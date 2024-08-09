library('checkmate')
library('cli')
library('data.table')
library('DBI')
library('doParallel')
library('glue')
library('logger')
library('rsurveycto')

# create secrets GOOGLE_TOKEN and SCTO_AUTH for GitHub Actions
# make sure GitHub secret SCTO_AUTH has no trailing line break


get_wh_params = \(path) {
  params_raw = yaml::read_yaml(path)
  envir = if (Sys.getenv('GITHUB_REF_NAME') == 'main') 'prod' else 'dev'
  envirs = sapply(params_raw$environments, \(x) x$name)
  params = c(
    params_raw[names(params_raw) != 'environments'],
    params_raw$environments[[which(envirs == envir)]])
  params
}


get_scto_auth = function(auth_file = NULL) {
  if (Sys.getenv('SCTO_AUTH') == '') {
    auth_path = file.path('secrets', auth_file)
  } else {
    auth_path = withr::local_tempfile()
    writeLines(Sys.getenv('SCTO_AUTH'), auth_path)
  }
  scto_auth(auth_path)
}


set_bq_auth = \(auth_file = NULL) {
  auth_path = file.path('secrets', auth_file)
  path = if (Sys.getenv('GOOGLE_TOKEN') != '') {
    Sys.getenv('GOOGLE_TOKEN')
  } else if (!is.null(auth_file) && file.exists(auth_path)) {
    auth_path
  } else {
    NULL
  }
  bigrquery::bq_auth(path = path)
}


connect = \(params, check = TRUE) {
  drv = switch(params$platform, bigquery = bigrquery::bigquery())#,
  # postgres = RPostgres::Postgres(), sqlite = RSQLite::SQLite())
  db_args = params[setdiff(names(params), c('auth_file', 'platform'))]
  con = do.call(dbConnect, c(drv = drv, db_args))
  if (isFALSE(check)) return(con)

  res = tryCatch(dbListTables(con), error = \(e) e)
  if (inherits(res, 'error')) {
    p = paste(names(params), params, sep = ': ')
    p = sapply(p, \(x) c(' ' = x), USE.NAMES = FALSE)
    cli_abort(c('x' = 'Cannot connect to warehouse using these parameters:', p))
  }
  con
}


fix_names = \(x, name_type = c('table', 'column')) {
  name_type = match.arg(name_type)
  y = gsub('[^a-zA-Z0-9_]', '_', x) # bigquery is case sensitive
  # hack to prevent name collisions
  idx = x != y
  if (name_type == 'table') {
    idx = idx | grepl('^.+__(choices|settings|survey|versions)$', x)
  }
  y[idx] = paste(y[idx], substr(openssl::sha1(x[idx]), 1L, 6L), sep = '_')
  y
}


get_supported_sync_modes = \(type) {
  switch(
    type,
    catalog = c('overwrite', 'append'),
    dataset = c('overwrite', 'append'),
    form = c('overwrite', 'append', 'incremental', 'deduped'),
    form_versions = c('overwrite', 'incremental'),
    form_defs = c('overwrite', 'incremental'))
}


check_form_versions = \(auth, con, id) {
  id_wh = fix_names(id)
  versions_wh = db_read_table(con, glue('{id_wh}__versions'))
  if (is.null(versions_wh)) return(TRUE)

  versions_scto = scto_get_form_metadata(auth, id, get_defs = FALSE)
  ver_cols = c('form_version', 'date_str', 'actor')

  versions_missing = fsetdiff(
    versions_wh[, ..ver_cols], versions_scto[, ..ver_cols])
  ver_ok = nrow(versions_missing) == 0L
  ver_ok
}


check_streams = \(auth, con, streams, catalog_scto) {
  assert_data_table(streams)
  assert_names(
    colnames(streams), type = 'unique', permutation.of = c('id', 'sync_mode'))

  streams_merge = merge(streams, catalog_scto, by = 'id', all.x = TRUE)
  streams_merge[, table_name := fix_names(id)]
  streams_merge[, `:=`(
    id_in_scto = !is.na(type),
    id_unique = !(duplicated(id) | duplicated(id, fromLast = TRUE)))]

  streams_merge[
    id_unique == TRUE,
    table_name_unique := !(
      duplicated(table_name) | duplicated(table_name, fromLast = TRUE))]

  streams_merge[
    type == 'dataset',
    sync_mode_ok := sync_mode %in% get_supported_sync_modes('dataset')]
  streams_merge[
    type == 'form',
    sync_mode_ok := sync_mode %in% get_supported_sync_modes('form')]

  streams_merge[type == 'dataset', form_version_ok := TRUE]
  streams_merge[
    type == 'form',
    form_version_ok := check_form_versions(auth, con, .BY$id),
    by = id]

  syncs_wh = db_read_table(con, '_syncs')

  if (is.null(syncs_wh)) {
    streams_merge[, `:=`(
      type_ok = TRUE,
      discriminator_ok = TRUE,
      dataset_version_ok = TRUE,
      created_at_ok = TRUE)]

  } else {
    streams_wh = syncs_wh[
      , .SD[`_extracted_at` == max(`_extracted_at`)], by = id]
    streams_merge = merge(
      streams_merge, streams_wh, by = 'id', suffixes = c('', '_wh'),
      all.x = TRUE)

    streams_merge[, `:=`(
      type_ok = is.na(type_wh) | type == type_wh,
      discriminator_ok = type == 'form' | is.na(discriminator_wh) |
        discriminator == discriminator_wh,
      dataset_version_ok = type == 'form' | is.na(dataset_version_wh) |
        dataset_version >= dataset_version_wh,
      created_at_ok = is.na(created_at) | is.na(created_at_wh) |
        created_at == created_at_wh)]
  }

  streams_ok = streams_merge[
    id_in_scto == TRUE & id_unique == TRUE & table_name_unique == TRUE &
      sync_mode_ok == TRUE & type_ok == TRUE & discriminator_ok == TRUE &
      dataset_version_ok == TRUE & created_at_ok == TRUE &
      form_version_ok == TRUE]

  streams_skip = streams_merge[id_in_scto == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {streams_skip$id}}, ',
      'which {?is/are} not in SurveyCTO.'))
  }

  streams_skip = streams_merge[id_unique == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {unique(streams_skip$id)}}, which ',
      '{?occurs/occur} multiple times in the yaml file.'))
  }

  streams_skip = streams_merge[table_name_unique == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {unique(streams_skip$id)}}, ',
      'whose resulting table names would not be unique.'))
  }

  streams_skip = streams_merge[sync_mode_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {streams_skip$id}}, whose ',
      'sync mode is not supported for {?its/their} type.'))
  }

  streams_skip = streams_merge[type_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {streams_skip$id}}, whose ',
      'type has changed since the previous sync.'))
  }

  streams_skip = streams_merge[discriminator_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {streams_skip$id}}, whose ',
      'dataset type has changed since the previous sync.'))
  }

  streams_skip = streams_merge[dataset_version_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {streams_skip$id}}, whose dataset ',
      'version has decreased since the previous sync.'))
  }

  streams_skip = streams_merge[created_at_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {streams_skip$id}}, whose creation ',
      'timestamp has changed since the previous sync.'))
  }

  streams_skip = streams_merge[form_version_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(c(
      'Skipping id{?s} {.val {streams_skip$id}}, which {?has/have} form ',
      'definition versions in the warehouse that are not in SurveyCTO.'))
  }

  streams_ok
}


set_extracted_cols = function(d, extracted_at = NULL) {
  if (!is.null(extracted_at)) {
    assert_posixct(extracted_at, len = 1L, any.missing = FALSE)
    set(d, j = '_extracted_at', value = extracted_at)
  }
  uuids = uuid::UUIDgenerate(n = nrow(d))
  set(d, j = '_extracted_uuid', value = uuids)
}


rbind_custom = \(...) rbind(..., use.names = TRUE, fill = TRUE)


db_read_table = \(con, name, ...) {
  if (dbExistsTable(con, name)) setDT(dbReadTable(con, name)) else NULL
}


db_list_fields = \(con, name) {
  if (dbExistsTable(con, name)) dbListFields(con, name) else NULL
}


get_fields = \(con, d) {
  if (!inherits(con, 'BigQueryConnection')) return(NULL)
  bigrquery::as_bq_fields(d) # enforce form version as string
}


db_write_table = \(con, name, value, ...) {
  fields = get_fields(con, value)
  dbWriteTable(con, name, value, fields = fields, ...)
}


db_append_table = \(con, name, value, cols_wh) {
  if (setequal(cols_wh, colnames(value))) {
    dbAppendTable(con, name, value)
  } else {
    table_wh = db_read_table(con, name)
    table_rbind = rbind_custom(table_wh, value)
    db_write_table(con, name, table_rbind, overwrite = TRUE)
  }
  invisible(TRUE)
}

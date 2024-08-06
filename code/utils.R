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

connect = \(params) {
  drv = switch(params$platform, bigquery = bigrquery::bigquery())#,
  # postgres = RPostgres::Postgres(), sqlite = RSQLite::SQLite())
  db_args = params[setdiff(names(params), c('auth_file', 'platform'))]
  do.call(dbConnect, c(drv = drv, db_args))
}

fix_names = \(x, name_type = c('table', 'column')) {
  name_type = match.arg(name_type)
  y = gsub('[^a-zA-Z0-9_]', '_', x) # TODO: consider tolower
  # hack to prevent name collisions
  idx = x != y
  if (name_type == 'table') {
    idx = idx | grepl('^.+__(choices|settings|survey|versions)$', x)
  }
  y[idx] = paste(y[idx], substr(openssl::sha1(x[idx]), 1L, 6L), sep = '_')
  y
}

get_allowed_sync_modes = \(type = c('dataset', 'form')) {
  type = match.arg(type)
  if (type == 'dataset') return(c('overwrite', 'append'))
  if (type == 'form') return(c('overwrite', 'append'))
}

check_streams = \(streams, catalog_scto, con) {
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
    sync_mode_ok := sync_mode %in% get_allowed_sync_modes('dataset')]
  streams_merge[
    type == 'form',
    sync_mode_ok := sync_mode %in% get_allowed_sync_modes('form')]

  catalog_wh = db_read_table(con, '_catalog')

  if (is.null(catalog_wh)) {
    streams_merge[, `:=`(
      type_ok = TRUE,
      discriminator_ok = TRUE,
      dataset_version_ok = TRUE,
      created_at_ok = TRUE)]

  } else {
    catalog_wh = catalog_wh[`_extracted_at` == max(`_extracted_at`)]
    streams_merge = merge(
      streams_merge, catalog_wh, by = 'id',
      suffixes = c('', '_wh'), all.x = TRUE)

    streams_merge[, `:=`(
      type_ok = is.na(type_wh) | type == type_wh,
      discriminator_ok = type != 'dataset' | is.na(discriminator_wh) |
        discriminator == discriminator_wh,
      dataset_version_ok = type != 'dataset' | is.na(dataset_version_wh) |
        dataset_version >= dataset_version_wh,
      created_at_ok = is.na(created_at) | created_at == created_at_wh)]
  }

  streams_keep = streams_merge[
    id_in_scto == TRUE & id_unique == TRUE & table_name_unique == TRUE &
      sync_mode_ok == TRUE & type_ok == TRUE & discriminator_ok == TRUE &
      dataset_version_ok == TRUE & created_at_ok == TRUE]

  streams_skip = streams_merge[id_in_scto == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(
      c('Skipping the following id{?s}, which {?is/are} ',
        'not in SurveyCTO: {.val {streams_skip$id}}'))
  }

  streams_skip = streams_merge[id_unique == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(
      c('Skipping the following id{?s}, which {?occurs/occur} multiple ',
        'times in the yaml file: {.val {unique(streams_skip$id)}}'))
  }

  streams_skip = streams_merge[table_name_unique == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(
      c('Skipping the following id{?s}, whose resulting table ',
        'names would not be unique: {.val {unique(streams_skip$id)}}'))
  }

  streams_skip = streams_merge[sync_mode_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(
      c('Skipping the following id{?s}, whose sync mode is not ',
        'supported for {?its/their} type: {.val {streams_skip$id}}'))
  }

  streams_skip = streams_merge[type_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(
      c('Skipping the following id{?s}, whose type has changed ',
        'since the previous sync: {.val {streams_skip$id}}'))
  }

  streams_skip = streams_merge[discriminator_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(
      c('Skipping the following id{?s}, whose dataset discriminator value ',
        'has changed since the previous sync: {.val {streams_skip$id}}'))
  }

  streams_skip = streams_merge[dataset_version_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(
      c('Skipping the following id{?s}, whose dataset version has ',
        'decreased since the previous sync: {.val {streams_skip$id}}'))
  }

  streams_skip = streams_merge[created_at_ok == FALSE]
  if (nrow(streams_skip) > 0L) {
    cli_alert_warning(
      c('Skipping the following id{?s}, whose "created_at" timestamp ',
        'has changed since the previous sync: {.val {streams_skip$id}}'))
  }

  streams_keep
}

set_extracted_cols = function(d, extracted_at = NULL) {
  if (!is.null(extracted_at)) {
    assert_posixct(extracted_at, len = 1L, any.missing = FALSE)
    set(d, j = '_extracted_at', value = extracted_at)
  }
  uuids = uuid::UUIDgenerate(n = nrow(d))
  set(d, j = '_extracted_uuid', value = uuids)
}

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

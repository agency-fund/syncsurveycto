sync_table = \( # nolint
  con, name, table_scto, sync_mode, extracted_at = NULL, type = NULL) {

  setnames(table_scto, \(x) fix_names(con, x, 'column'))
  set_extracted_cols(table_scto, extracted_at)

  cols_wh = db_list_fields(con, name)
  cols_equal = setequal(cols_wh, colnames(table_scto))

  if (nrow(table_scto) == 0L && (sync_mode %in% c('overwrite', 'deduped')) &&
      !is.null(cols_wh)) {
    dbRemoveTable(con, name)
    num_rows = nrow(table_scto)

  } else if (
    nrow(table_scto) > 0L && (sync_mode == 'overwrite' || is.null(cols_wh))) {
    db_overwrite_table(con, name, table_scto)
    num_rows = nrow(table_scto)

  } else if (nrow(table_scto) > 0L && sync_mode == 'append') {
    db_append_table(con, name, table_scto, cols_wh)
    num_rows = nrow(table_scto)

  } else if (
    nrow(table_scto) > 0L && sync_mode %in% c('incremental', 'deduped')) {
    table_wh = db_read_table(con, name)

    if (isTRUE(type == 'form_def')) {
      table_new = table_scto[!table_wh, on = '_form_version']
      if (nrow(table_new) > 0L) {
        if (cols_equal) {
          db_append_table(con, name, table_new, cols_wh)
        } else {
          table_rbind = rbind_custom(table_wh, table_new)
          db_overwrite_table(con, name, table_rbind)
        }
      }
      num_rows = nrow(table_new)

    } else {
      KEY = NULL # nolint
      table_rbind = rbind_custom(table_wh, table_scto) # 1 or 2 rows per KEY
      extr_cols = get_extracted_colnames()
      by_cols = setdiff(colnames(table_rbind), extr_cols)
      table_keep = unique(table_rbind, by = by_cols) # if 2 dupes, keep earlier
      if (sync_mode == 'deduped') { # if 2 rows of same KEY, keep later
        table_keep = table_keep[KEY %in% table_scto$KEY, .SD[.N], by = 'KEY']
      }
      db_overwrite_table(con, name, table_keep)
      num_rows = nrow(fsetdiff( # perfect < good
        table_keep[, extr_cols, with = FALSE],
        table_wh[, extr_cols, with = FALSE]))
    }
  }

  invisible(num_rows)
}


sync_form_metadata = \(
  auth, con, id, sync_mode_form = get_supported_sync_modes('form'),
  extracted_at = NULL) {
  sync_mode_form = match.arg(sync_mode_form)
  sync_mode = if (sync_mode_form == 'incremental') 'deduped' else sync_mode_form

  versions_scto = scto_get_form_metadata(auth, id, get_defs = FALSE)
  versions_scto = versions_scto[, !'form_id']

  id_wh = fix_names(con, id)
  versions_wh = db_read_table(con, glue('{id_wh}__versions'))
  if (is.null(versions_wh)) {
    versions_new = versions_scto
  } else {
    versions_wh[, (get_extracted_colnames()) := NULL]
    versions_new = fsetdiff(versions_scto, versions_wh)
  }

  if (nrow(versions_new) > 0L) {
    sync_table(
      con, glue('{id_wh}__versions'), versions_scto, sync_mode, extracted_at)

    metadata_scto = scto_get_form_metadata(auth, id)
    form_defs = scto_unnest_form_definitions(metadata_scto, by_form_id = FALSE)
    for (element in c('survey', 'choices', 'settings')) {
      sync_table(
        con, glue('{id_wh}__{element}'), form_defs[[element]][, !'_form_id'],
        sync_mode, extracted_at, type = 'form_def')
    }
  }
  invisible(TRUE)
}


sync_form = \(
  auth, con, id, sync_mode = get_supported_sync_modes('form'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)

  id_wh = fix_names(con, id)
  data_scto = scto_read(auth, id) # pull all in case deleted fields or records
  num_rows = sync_table(con, id_wh, data_scto, sync_mode, extracted_at)

  sync_form_metadata(auth, con, id, sync_mode, extracted_at)
  invisible(num_rows)
}


sync_dataset = \(
  auth, con, id, sync_mode = get_supported_sync_modes('dataset'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)
  table_scto = scto_read(auth, id)

  cols_wh = db_list_fields(con, fix_names(con, id))
  cols_scto = fix_names(con, colnames(table_scto), 'column')
  cols_missing = setdiff(cols_wh, c(cols_scto, get_extracted_colnames()))
  if (!is.null(cols_wh) && length(cols_missing) > 0L) {
    cli_alert_warning(c( # check for datasets, since no created_at
      'Skipping dataset {.val {id}}, which has columns ',
      'in the warehouse that are not in SurveyCTO.'))
    return(invisible(-1L))
  }
  sync_table(con, id, table_scto, sync_mode, extracted_at)
}


sync_server = \(auth, con, extracted_at) {
  table_name = '_server'
  server_wh = db_read_table(con, table_name)
  if (is.null(server_wh)) {
    server_scto = data.table(server_name = auth$servername)
    sync_table(con, table_name, server_scto, 'overwrite', extracted_at)
  } else if (server_wh$server_name != auth$servername) {
    cli_abort(paste(
      'Server names are discrepant: {.val {server_wh$server_name}}',
      'in the warehouse and {.val {auth$servername}} in SurveyCTO.'))
  }
}


sync_catalog = \(
  con, catalog, sync_mode = get_supported_sync_modes('catalog'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)
  sync_table(con, '_catalog', catalog, sync_mode, extracted_at)
}


sync_runs = \(con, wh_params, extracted_at) {
  env_vars = Sys.getenv(c(
    'GITHUB_REPOSITORY', 'GITHUB_REF_NAME', 'GITHUB_SHA',
    'GITHUB_EVENT_NAME', 'GITHUB_RUN_ID', 'GITHUB_RUN_URL', 'USER'))
  env_vars[env_vars == ''] = NA_character_
  run_now = setDT(as.list(env_vars))
  setnames(run_now, tolower)
  setnames(run_now, 'user', 'local_user')

  set(run_now, j = 'environment', value = wh_params$environment)
  set_package_version(run_now)

  sync_table(con, '_runs', run_now, 'append', extracted_at)
}


sync_syncs = \(con, stream, num_rows, extracted_at) {
  cols = c(
    'id', 'type', 'form_version', 'dataset_version', 'created_at',
    'discriminator', 'sync_mode')
  d = stream[, cols, with = FALSE]
  set(d, j = 'num_rows_loaded', value = num_rows)
  set_package_version(d)
  sync_table(con, '_syncs', d, 'append', extracted_at)
}


#' Sync data from SurveyCTO to a data warehouse
#'
#' @param scto_params List of parameters for SurveyCTO.
#' @param wh_params List of parameters for the data warehouse.
#'
#' @export
sync_surveycto = \(scto_params, wh_params) {
  auth = get_scto_auth(scto_params$auth_file)
  if (wh_params$platform == 'bigquery') {
    set_bq_auth(wh_params$auth_file)
    withr::defer(bq_deauth())
  }
  streams = rbindlist(scto_params$streams, use.names = TRUE, fill = TRUE)

  con = connect(wh_params)
  extracted_at = .POSIXct(Sys.time(), tz = 'UTC')

  sync_server(auth, con, extracted_at)
  sync_runs(con, wh_params, extracted_at)

  catalog_scto = scto_catalog(auth)
  sync_catalog(con, catalog_scto, 'overwrite', extracted_at)

  if (nrow(streams) == 0L) {
    cli_alert_info('No ids to sync.')
    return(invisible(TRUE))
  }

  streams_ok = check_streams(auth, con, streams, catalog_scto)
  if (nrow(streams_ok) == 0L) {
    cli_alert_warning('Sync skipped for id{?s} {.val {streams$id}}.')
    return(invisible(TRUE))
  }

  s = NULL
  feo = foreach(s = iterators::iter(streams_ok, by = 'row'))
  res = feo %dopar% {
    if (getDoParWorkers() > 1L) con = connect(wh_params, FALSE)

    sync_stream = if (s$type == 'dataset') sync_dataset else sync_form
    n = tryCatch(
      sync_stream(auth, con, s$id, s$sync_mode, extracted_at), error = \(e) e)

    if (inherits(n, 'error')) {
      cli_bullets(
        c('x' = 'Sync failed for id {.val {s$id}}:', ' ' = as.character(n)))
    } else if (n >= 0L) {
      cli_alert_success(
        'Sync succeeded for id {.val {s$id}}, {n} row{?s} loaded.')
      sync_syncs(con, s, n, extracted_at)
    } else {
      cli_alert_warning('Sync skipped for id {.val {s$id}}.')
    }
    n
  }

  status = sapply(res, \(x) {
    if (inherits(x, 'error')) 'failed'
    else if (x >= 0L) 'succeeded'
    else 'skipped'
  })

  ids_succeed = streams_ok$id[status == 'succeeded']
  if (length(ids_succeed) > 0L) {
    cli_alert_success('Sync succeeded for id{?s} {.val {ids_succeed}}.')
  }

  ids_skip = c(
    setdiff(streams$id, streams_ok$id), streams_ok$id[status == 'skipped'])
  if (length(ids_skip) > 0L) {
    cli_alert_warning('Sync skipped for id{?s} {.val {ids_skip}}.')
  }

  ids_fail = streams_ok$id[status == 'failed']
  if (length(ids_fail) > 0L) {
    cli_abort('Sync failed for id{?s} {.val {ids_fail}}.')
  }

  invisible(TRUE)
}

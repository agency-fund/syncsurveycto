sync_table = \(
  con, name, table_scto, sync_mode, extracted_at = NULL, type = NULL) {

  setnames(table_scto, \(x) fix_names(x, name_type = 'column'))
  set_extracted_cols(table_scto, extracted_at)

  cols_wh = db_list_fields(con, name)
  cols_equal = setequal(cols_wh, colnames(table_scto))

  if (nrow(table_scto) == 0L && sync_mode == 'overwrite' && !is.null(cols_wh)) {
    dbRemoveTable(con, name)

  } else if (
    nrow(table_scto) > 0L && (sync_mode == 'overwrite' || is.null(cols_wh))) {
    db_write_table(con, name, table_scto, overwrite = TRUE)

  } else if (nrow(table_scto) > 0L && sync_mode == 'append') {
    db_append_table(con, name, table_scto, cols_wh)

  } else if (
    nrow(table_scto) > 0L && sync_mode %in% c('incremental', 'deduped')) {
    table_wh = db_read_table(con, name)

    if (isTRUE(type == 'form_def')) { # only incremental
      table_new = table_scto[!table_wh, on = '_form_version']
      if (nrow(table_new) > 0L) {
        if (cols_equal) {
          dbAppendTable(con, name, table_new)
        } else {
          table_rbind = rbind_custom(table_wh, table_new)
          db_write_table(con, name, table_rbind, overwrite = TRUE)
        }
      }
    } else {
      table_rbind = rbind_custom(table_wh, table_scto)
      extracted_cols = c('_extracted_at', '_extracted_uuid')
      by_cols = setdiff(colnames(table_rbind), extracted_cols)
      table_keep = unique(table_rbind, by = by_cols)
      if (sync_mode == 'deduped') {
        table_keep = table_keep[, .SD[.N], by = 'KEY']
      }
      db_write_table(con, name, table_keep, overwrite = TRUE)
    }
  }

  invisible(TRUE)
}


sync_form = \(
  auth, con, id, sync_mode = get_supported_sync_modes('form'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)

  id_wh = fix_names(id)
  data_scto = scto_read(auth, id) # pull all data in case deleted fields
  sync_table(con, id_wh, data_scto, sync_mode, extracted_at)

  sm_aux = if (sync_mode == 'overwrite') 'overwrite' else 'incremental'
  versions_scto = scto_get_form_metadata(auth, id, get_defs = FALSE)
  sync_form_versions(con, id_wh, versions_scto, sm_aux, extracted_at)

  metadata_scto = scto_get_form_metadata(auth, id)
  form_defs = scto_unnest_form_definitions(metadata_scto, by_form_id = FALSE)
  for (element in c('survey', 'choices', 'settings')) {
    sync_form_defs(
      con, id_wh, form_defs[[element]], element, sm_aux, extracted_at)
  }
  invisible(TRUE)
}


sync_dataset = \(
  auth, con, id, sync_mode = get_supported_sync_modes('dataset'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)
  table_scto = scto_read(auth, id)

  cols_wh = db_list_fields(con, fix_names(id))
  cols_missing = setdiff(cols_wh, colnames(table_scto))
  if (!is.null(cols_wh) && length(cols_missing) > 0L) {
    cli_alert_warning(c(
      'Skipping dataset {.val {id}}, which has columns ',
      'in the warehouse that are not in SurveyCTO.'))
    return(invisible(FALSE))
  }
  sync_table(con, id, table_scto, sync_mode, extracted_at)
}


sync_stream = \(auth, con, type, id, sync_mode, extracted_at) {
  type = match.arg(type, c('dataset', 'form'))
  sync_func = if (type == 'dataset') sync_dataset else sync_form
  sync_func(auth, con, id, sync_mode, extracted_at)
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


sync_form_versions = \(
  con, id_wh, table_scto, sync_mode = get_supported_sync_modes('form_versions'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)
  sync_table(
    con, glue('{id_wh}__versions'), table_scto[, !'form_id'], sync_mode,
    extracted_at)
}


sync_form_defs = \(
  con, id_wh, table_scto, element = c('survey', 'choices', 'settings'),
  sync_mode = get_supported_sync_modes('form_defs'), extracted_at = NULL) {
  element = match.arg(element)
  sync_mode = match.arg(sync_mode)
  sync_table(
    con, glue('{id_wh}__{element}'), table_scto[, !'_form_id'], sync_mode,
    extracted_at, type = 'form_def')
}


sync_runs = \(con, wh_params, extracted_at) {
  env_vars = Sys.getenv(c(
    'GITHUB_REPOSITORY', 'GITHUB_REF_NAME', 'GITHUB_SHA',
    'GITHUB_EVENT_NAME', 'GITHUB_RUN_ID', 'GITHUB_RUN_URL', 'USER'))
  env_vars[env_vars == ''] = NA_character_
  run_now = setDT(as.list(env_vars))
  setnames(run_now, tolower)
  setnames(run_now, 'user', 'local_user')

  is_local = is.na(run_now$github_repository)
  run_now[, `:=`(
    local_head = if (is_local) git2r::repository_head()$name else NA_character_,
    local_sha = if (is_local) git2r::last_commit()$sha else NA_character_,
    environment = wh_params$name)]

  sync_table(con, '_runs', run_now, 'append', extracted_at)
}


sync_syncs = \(con, stream, extracted_at) {
  cols = c(
    'id', 'type', 'form_version', 'dataset_version', 'created_at',
    'discriminator', 'sync_mode')
  sync_table(con, '_syncs', stream[, ..cols], 'append', extracted_at)
}


sync_surveycto = \(scto_params, wh_params) {
  auth = get_scto_auth(scto_params$auth_file)
  streams = rbindlist(scto_params$streams)

  con = connect(wh_params)
  extracted_at = .POSIXct(Sys.time(), tz = 'UTC')
  sync_server(auth, con, extracted_at)
  sync_runs(con, wh_params, extracted_at)

  catalog_scto = scto_catalog(auth)
  streams_ok = check_streams(auth, con, streams, catalog_scto)

  if (nrow(streams_ok) > 0L) {
    sync_catalog(con, catalog_scto, 'overwrite', extracted_at)

    feo = foreach(s = iter(streams_ok, by = 'row'), .errorhandling = 'pass')
    res = feo %dopar% {
      if (getDoParWorkers() > 1L) con = connect(wh_params, FALSE)

      caught = tryCatch(
        sync_stream(auth, con, s$type, s$id, s$sync_mode, extracted_at),
        error = \(e) e)
      if (isTRUE(caught)) {
        cli_alert_success('Sync succeeded for id {.val {s$id}}.')
        sync_syncs(con, s, extracted_at)
      } else if (inherits(caught, 'error')) {
        cli_bullets(
          c('x' = 'Sync failed for id {.val {s$id}}:',
            ' ' = as.character(caught)))
      } else {
        cli_alert_warning('Sync skipped for id {.val {s$id}}.')
      }
      caught
    }

    ids_skip = c(
      setdiff(streams$id, streams_ok$id), streams_ok$id[sapply(res, isFALSE)])
    if (length(ids_skip) > 0L) {
      cli_alert_warning('Sync skipped for id{?s} {.val {ids_skip}}.')
    }

    idx_err = sapply(res, \(x) inherits(x, 'error'))
    if (any(idx_err)) {
      ids_err = streams_ok$id[idx_err]
      cli_abort('Sync failed for id{?s} {.val {ids_err}}.')
    }

  } else {
    cli_alert_warning('Sync skipped for all ids.')
  }

  invisible(TRUE)
}

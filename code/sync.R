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
    if (cols_equal) {
      dbAppendTable(con, name, table_scto)
    } else {
      table_wh = db_read_table(con, name)
      table_rbind = rbind_custom(table_wh, table_scto)
      db_write_table(con, name, table_rbind, overwrite = TRUE)
    }

  } else if (
    nrow(table_scto) > 0L && sync_mode %in% c('incremental', 'deduped')) {
    table_wh = db_read_table(con, name)

    if (isTRUE(type == 'form_def')) { # only incremental
      key_col = '_form_version'
      table_new = table_scto[!table_wh, on = key_col]
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
  auth, con, id, sync_mode = get_allowed_sync_modes('form'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)

  id_wh = fix_names(id)
  versions_wh = db_read_table(con, glue('{id_wh}__versions'))
  versions_scto = scto_get_form_metadata(auth, id, get_defs = FALSE)

  if (is.null(versions_wh)) {
    versions_missing = data.table()
  } else {
    ver_cols = c('form_version', 'date_str', 'actor')
    versions_missing = fsetdiff(
      versions_wh[, ..ver_cols], versions_scto[, ..ver_cols])
  }

  if (nrow(versions_missing) > 0L) {
    cli_alert_warning(
      c('Skipping form {.val {id}} because not all ',
        'versions in the warehouse are in SurveyCTO.'))
    return(FALSE)
  }

  data_scto = scto_read(auth, id) # pull all data in case deleted fields
  sync_table(con, id_wh, data_scto, sync_mode, extracted_at)

  sm_ver = if (sync_mode == 'overwrite') 'overwrite' else 'append'
  sync_form_versions(con, id_wh, versions_scto, sm_ver, extracted_at)

  sm_def = if (sync_mode == 'overwrite') 'overwrite' else 'incremental'
  metadata_scto = scto_get_form_metadata(auth, id)
  form_defs = scto_unnest_form_definitions(metadata_scto, by_form_id = FALSE)
  for (element in c('survey', 'choices', 'settings')) {
    sync_form_defs(
      con, id_wh, form_defs[[element]], element, sm_def, extracted_at)
  }
  invisible(TRUE)
}

sync_dataset = \(
  auth, con, id, sync_mode = get_allowed_sync_modes('dataset'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)
  table_scto = scto_read(auth, id)
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
  con, catalog, sync_mode = c('append', 'overwrite'), extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)
  sync_table(con, '_catalog', catalog, sync_mode, extracted_at)
}

sync_form_versions = \(
  con, id_wh, table_scto, sync_mode = c('incremental', 'append', 'overwrite'),
  extracted_at = NULL) {
  sync_mode = match.arg(sync_mode)
  sync_table(
    con, glue('{id_wh}__versions'), table_scto[, !'form_id'], sync_mode,
    extracted_at)
}

sync_form_defs = \(
  con, id_wh, table_scto, element = c('survey', 'choices', 'settings'),
  sync_mode = c('incremental', 'append', 'overwrite'), extracted_at = NULL) {
  element = match.arg(element)
  sync_mode = match.arg(sync_mode)
  sync_table(
    con, glue('{id_wh}__{element}'), table_scto[, !'_form_id'], sync_mode,
    extracted_at, type = 'form_def')
}

sync_runs = \(con, wh_params, extracted_at) {
  name = '_sync_runs'
  cols_wh = db_list_fields(con, name)

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
  set_extracted_cols(run_now, extracted_at)

  if (setequal(cols_wh, colnames(run_now))) {
    dbAppendTable(con, name, run_now)
  } else {
    runs_wh = db_read_table(con, name)
    runs_rbind = rbind_custom(runs_wh, run_now)
    db_write_table(con, name, runs_rbind, overwrite = TRUE)
  }
  invisible(TRUE)
}

sync_surveycto = \(scto_params, wh_params) {
  auth = get_scto_auth(scto_params$auth_file)
  streams = rbindlist(scto_params$streams)

  con = connect(wh_params)
  extracted_at = .POSIXct(Sys.time(), tz = 'UTC')
  sync_server(auth, con, extracted_at)
  sync_runs(con, wh_params, extracted_at)

  catalog_scto = scto_catalog(auth)
  streams_ok = check_streams(streams, catalog_scto, con)

  if (nrow(streams_ok) > 0L) {
    sync_catalog(con, catalog_scto, 'append', extracted_at)

    feo = foreach(s = iter(streams_ok, by = 'row'), .errorhandling = 'pass')
    res = feo %dopar% {
      if (getDoParWorkers() > 1L) con = connect(wh_params, FALSE)

      caught = tryCatch(
        sync_stream(auth, con, s$type, s$id, s$sync_mode, extracted_at),
        error = \(e) e)
      if (inherits(caught, 'error')) {
        cli_bullets(
          c('x' = 'Error while syncing id {.val {s$id}}:',
            ' ' = as.character(caught)))
      } else {
        cli_alert_success('Sync succeeded for id {.val {s$id}}.')
      }
      caught
    }

    idx = sapply(res, \(x) inherits(x, 'error'))
    if (any(idx)) {
      ids_err = streams_ok$id[idx]
      cli_abort('Error while syncing id{?s} {.val {ids_err}}.')
    }
  } else {
    cli_alert_warning('No valid ids to sync.')
  }

  invisible(TRUE)
}

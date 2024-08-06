sync_table = \(
  con, name, table_scto, sync_mode = c('incremental', 'append', 'overwrite'),
  extracted_at = NULL, type = NULL) {
  sync_mode = match.arg(sync_mode)

  setnames(table_scto, \(x) fix_names(x, name_type = 'column'))
  set_extracted_cols(table_scto, extracted_at)

  cols_wh = db_list_fields(con, name)
  cols_equal = setequal(cols_wh, colnames(table_scto))

  if (nrow(table_scto) == 0L && sync_mode == 'overwrite' && !is.null(cols_wh)) {
    dbRemoveTable(con, name)
  } else if (
    nrow(table_scto) > 0L && (sync_mode == 'overwrite' || is.null(cols_wh))) {
    fields = get_fields(con, table_scto)
    dbWriteTable(con, name, table_scto, overwrite = TRUE, fields = fields)

  } else if (nrow(table_scto) > 0L && sync_mode == 'append') {
    if (cols_equal) {
      dbAppendTable(con, name, table_scto)
    } else {
      table_wh = db_read_table(con, name)
      table_rbind = rbind(table_wh, table_scto, use.names = TRUE, fill = TRUE)
      fields = get_fields(con, table_rbind)
      dbWriteTable(con, name, table_rbind, overwrite = TRUE, fields = fields)
    }

  } else if (nrow(table_scto) > 0L && sync_mode == 'incremental') {
    table_wh = db_read_table(con, name)

    if (type == 'form_def') {
      key_col = '_form_version'
      # TODO: what if a column gets added and values are non-null for older versions?
      table_new = table_scto[!table_wh, on = key_col]
      if (cols_equal) {
        if (nrow(table_new) > 0) dbAppendTable(con, name, table_new)
      } else {
        table_rbind = rbind(table_wh, table_new, use.names = TRUE, fill = TRUE)
        fields = get_fields(con, table_rbind)
        if (nrow(table_scto_new) > 0) {
          dbWriteTable(
            con, name, table_rbind, overwrite = TRUE, fields = fields)
        }
      }
    }
  }
  invisible(TRUE)
}

sync_form = \(
  auth, con, id, sync_mode = get_allowed_sync_modes('form'),
  extracted_at = NULL, cursor_field = 'CompletionDate', primary_key = 'KEY') {
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

  if (sync_mode %in% c('overwrite', 'append')) {
    data_scto = scto_read(auth, id)
    sync_table(con, id_wh, data_scto, sync_mode, extracted_at)

  } else if (sync_mode == 'incremental') {
    # setDT(dbGetQuery(con, glue('select min({cursor_field}) from {id}')))
    data_wh = db_read_table(con, id_wh) # faster than dbGetQuery
    data_scto = scto_read(auth, id) # pull all data in case deleted fields

    if (!is.null(data_wh) && !(cursor_field %in% colnames(data_wh))) {
      cli_alert_warning(
        c('Skipping form {.val {id}} because the cursor field ',
          '{.val {cursor_field}} is not in the data in the warehouse.'))
      return(FALSE)
    }

    if (!(cursor_field %in% colnames(data_scto))) {
      cli_alert_warning(
        c('Skipping form {.val {id}} because the cursor field ',
          '{.val {cursor_field}} is not in the data in SurveyCTO.'))
      return(FALSE)
    }

    data_new = if (!is.null(data_wh)) {
      max_cursor_value = max(data_wh[[cursor_field]])
      data_scto[x > max_cursor_value, env = list(x = cursor_field)]
    } else {
      data_scto
    }

    if (nrow(data_new) > 0L) {
      sync_table(con, id_wh, data_new, 'append', extracted_at)
    }
  }

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
    cli_alert_danger(
      c('Stopping due to discrepant server names: ',
        '{.val {server_wh$server_name}} in the warehouse ',
        'and {.val {auth$servername}} in SurveyCTO.'))
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

sync_surveycto = \(scto_params, wh_params) {
  auth = get_scto_auth(scto_params$auth_file)
  streams = rbindlist(scto_params$streams)

  con = connect(wh_params)
  extracted_at = .POSIXct(Sys.time(), tz = 'UTC')
  sync_server(auth, con, extracted_at)

  catalog_scto = scto_catalog(auth)
  streams_keep = check_streams(streams, catalog_scto, con)

  if (nrow(streams_keep) > 0L) {
    sync_catalog(con, catalog_scto, 'overwrite', extracted_at)
    res = foreach(s = iter(streams_keep, by = 'row')) %dopar% {
      if (getDoParWorkers() > 1L) con = connect(wh_params)
      sync_stream(auth, con, s$type, s$id, s$sync_mode, extracted_at)
    }
  }
  invisible(TRUE)
}


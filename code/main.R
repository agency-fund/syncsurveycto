source(file.path('code', 'utils.R'))
source(file.path('code', 'sync.R'))

scto_params = get_params(file.path('params', 'surveycto.yaml'))
wh_params = get_params(file.path('params', 'warehouse.yaml'))
set_bq_auth(wh_params$auth_file)

# registerDoParallel()
registerDoSEQ()
sync_surveycto(scto_params, wh_params)

# TODO: check for changes to sync_mode

# low priority:
# TODO: logging with https://daroczig.github.io/logger/, to slack?
# TODO: should overwrite be allowed to ignore skip criteria?
# TODO: deal with discrepant column types even if colnames identical
# TODO: tmp tables that get renamed?

# overwrite: full refresh overwrite
# append: full refresh append
# incremental: incremental append
# deduped: incremental append deduped

source(file.path('code', 'utils.R'))
source(file.path('code', 'sync.R'))

scto_params = yaml::read_yaml(file.path('params', 'surveycto.yaml'))
wh_params = get_wh_params(file.path('params', 'warehouse.yaml'))
set_bq_auth(wh_params$auth_file)

# registerDoParallel()
registerDoSEQ()
sync_surveycto(scto_params, wh_params)

# TODO: logging with https://daroczig.github.io/logger/, to slack?
# TODO: should overwrite be allowed to remove columns or ignore skip criteria?
# TODO: deal with discrepant column types even if colnames identical
# TODO: check if review workflow is enabled, and warn depending on sync mode

# overwrite: full refresh overwrite
# append: full refresh append
# incremental: incremental append
# deduped: incremental append deduped

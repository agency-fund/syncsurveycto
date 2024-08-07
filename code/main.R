source(file.path('code', 'utils.R'))
source(file.path('code', 'sync.R'))

scto_params = yaml::read_yaml(file.path('params', 'surveycto.yaml'))
wh_params = get_wh_params(file.path('params', 'warehouse.yaml'))
set_bq_auth(wh_params$auth_file)

# registerDoParallel()
registerDoSEQ()
sync_surveycto(scto_params, wh_params)

# high priority:
# TODO: incremental and deduped sync of form data

# medium priority:
# TODO: logging with https://daroczig.github.io/logger/, to slack?

# low priority:
# TODO: should overwrite be allowed to remove columns or ignore skip criteria?
# TODO: deal with discrepant column types even if colnames identical
# TODO: check if review and correction workflow is enabled, and maybe bail

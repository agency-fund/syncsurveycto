library('doParallel')
source(file.path('code', 'utils.R'))
source(file.path('code', 'sync.R'))

scto_params = yaml::read_yaml(file.path('params', 'surveycto.yaml'))
wh_params = yaml::read_yaml(file.path('params', 'warehouse.yaml'))
set_bq_auth(wh_params$auth_file)

# registerDoParallel()
registerDoSEQ()
sync_surveycto(scto_params, wh_params)

# high priority:
# TODO: incremental and deduped sync of form data
# TODO: github actions cron schedule

# medium priority:
# TODO: more intelligent fetching of form definitions based on sync_mode
# TODO: trycatch a la pmparser
# TODO: logging with https://daroczig.github.io/logger/, to slack?
# TODO: dev and prod environments, based on branch

# low priority:
# TODO: should overwrite be allowed to remove columns or ignore skip criteria?
# TODO: deal with discrepant column types even if colnames identical
# TODO: check if review and correction workflow is enabled, and maybe bail

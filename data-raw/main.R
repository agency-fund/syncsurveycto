library('syncsurveycto')

scto_params = get_params(file.path('params', 'surveycto.yaml'))
wh_params = get_params(file.path('params', 'warehouse.yaml'))

foreach::registerDoSEQ()
sync_surveycto(scto_params, wh_params)

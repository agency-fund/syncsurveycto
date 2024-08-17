# syncsurveycto

[![R-CMD-check](https://github.com/agency-fund/syncsurveycto/workflows/R-CMD-check/badge.svg)](https://github.com/agency-fund/syncsurveycto/actions)
[![codecov](https://codecov.io/gh/agency-fund/syncsurveycto/branch/main/graph/badge.svg)](https://codecov.io/gh/agency-fund/syncsurveycto)
[![CRAN Status](https://www.r-pkg.org/badges/version/syncsurveycto)](https://cran.r-project.org/package=syncsurveycto)

## Overview

stuff.

## Installation

Install the development version:

```r
if (!requireNamespace('remotes', quietly = TRUE))
  install.packages('remotes')
remotes::install_github('agency-fund/syncsurveycto')
```

## Usage

1. create secrets folder
1. in secrets folder, create scto_auth.txt
1. create service account
1. download json key, move it to secrets folder
1. for the project, give service account the role "bigquery user"
1. create bigquery datasets surveycto and surveycto_dev
1. for those two datasets, give service account the role "bigquery data editor"

1. update warehouse.yaml file, including auth_file based on name of json file
1. update surveycto.yaml file

- create secrets GOOGLE_TOKEN and SCTO_AUTH for GitHub Actions
- make sure GitHub secret SCTO_AUTH has no trailing line break


For more details, see the [reference documentation](https://agency-fund.github.io/syncsurveycto/reference/index.html).

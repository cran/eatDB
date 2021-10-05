# eatDB

<!-- badges: start -->
[![CRAN status](https://www.r-pkg.org/badges/version/eatDB)](https://CRAN.R-project.org/package=eatDB)
[![R-CMD-check](https://github.com/beckerbenj/eatDB/workflows/R-CMD-check/badge.svg)](https://github.com/beckerbenj/eatDB/actions)
[![codecov](https://codecov.io/github/beckerbenj/eatDB/branch/master/graphs/badge.svg)](https://codecov.io/github/beckerbenj/eatDB)
[![](http://cranlogs.r-pkg.org/badges/grand-total/eatDB?color=blue)](https://cran.r-project.org/package=eatDB)
<!-- badges: end -->


## Overview

eatDB (educational assessment tools: Data Bases) enables the use of relational data bases via a pure R interface. It uses `SQLlite3` as a back end and makes use of the packages `DBI` and `RSQLite`. When a relational data base with multiple data tables is created via `createDB`, the user specifies the order in which the data tables are merged when data is later extracted from the data base. The `fkList` specifies which data table references to which other data table via which ID variable.

eatDB makes use of consistency checks of `SQLite3` via using the definition of primary and foreign keys. Additionally, meta data (e.g. variable and value labels) can be stored in a separate data table as part of the relational data base.

## Installation

```R
# Install eatDB from GitHub via
devtools::install_github("beckerbenj/eatDB")
```

## Usage

```R
### Setup data base
pkList <- list(df1 = "ID2", df2 = "v2")
fkList <- list(df1 = list(References = NULL, Keys = NULL),
               df2 = list(References = "df1", Keys = "ID2"))
createDB(dfList = dfList, pkList =  pkList, fkList = fkList, filePath = "insert/path/example.db")

### get information from data base
names_of_variables <- dbNames(filePath = "insert/path/example.db")
key_structure <- dbKeys(filePath = "insert/path/example.db")

### pull data from data base
# single data table
single_data_table <- dbSingleDF(df_name = "df1", filePath = "insert/path/example.db")
# variable selection from multiple tables
dbSingleDF(vSelect = c("var1", "var2"), filePath = "insert/path/example.db")
```

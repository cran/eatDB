
context("Create Data Base")
library(eatDB)

# load test data (df1, df2, pkList, fkList)
#load(file = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dbdata.rda")
load(file = "helper_dbdata.rda")

# create in memory db
createDB(dfList = dfList, pkList = pkList, fkList = fkList, metaData = metaData, filePath = ":memory:")

### test shell + sqlite3
#test_that("Data base can be created via Shell ", {
#  expect_silent(init_DB_shell(filePath = ":memory:"))
#})

### data table query creation
test_that("Create partial Query for variable definitions", {
  createdQuery <- write_varDef(dfList$df1)
  expected <- "v1 REAL, ID2 REAL"
  expect_equal(createdQuery, expected)
})
test_that("Create Query for primary key", {
  createdQuery <- write_primeKey(pkList$df2)
  expected <- ", PRIMARY KEY ( ID2, v2 )"
  expect_equal(createdQuery, expected)
})
test_that("Create Query for foreign key", {
  createdQuery <- write_foreignKey(fkList$df2)
  expected <- ", FOREIGN KEY ( ID2 ) REFERENCES df1 ( ID2 )"
  expect_equal(createdQuery, expected)
})

#
test_that("Create Query for a single data frame", {
  createdQuery <- writeQ_create(dfList$df1, pkList$df1, fkList$df1, "df1")
  expected <- "CREATE TABLE df1 ( v1 REAL, ID2 REAL , PRIMARY KEY ( ID2 )  );"
  expect_equal(createdQuery, expected)
})

### merge order query creation
test_that("Create Query for a single data frame", {
  createdQuery <- writeQ_mergeOrder(names(dfList))
  expected <- c("CREATE TABLE Meta_Information ( mergeOrder TEXT );",
                "INSERT INTO Meta_Information (mergeOrder) VALUES ( ' df1 df2 ' );")
  expect_equal(createdQuery, expected)
})

### label frame query creation
test_that("Create Query for a single data frame", {
  createdQuery <- writeQ_create(df = metaData, df_name = "metaData",
                                primeKey = NULL, foreignKey = NULL)
  expected <- "CREATE TABLE metaData ( varName TEXT, varLabel TEXT, value REAL, label TEXT, missings TEXT, data_table TEXT   );"
  expect_equal(createdQuery, expected)
})

### test safe create query? dbWriteTable?
# tbd


### final checks
test_that("Data base creation as a whole runs ", {
  runs <- createDB(dfList = dfList, pkList = pkList, fkList = fkList, metaData = metaData, filePath = ":memory:")
  expect_identical(runs, NULL)
})

#single data frame
########################
dfList2 <- list(df1 = dfList$df1)
pkList2 <- list(df1 = "v1")

test_that("Data base creation for a single data table runs ", {
  runs <- createDB(dfList = dfList2, pkList = pkList2, filePath = ":memory:")
  expect_identical(runs, NULL)
})














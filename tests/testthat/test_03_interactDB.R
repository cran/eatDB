
context("Interacting with DB")
library(eatDB)

# load(file = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dbdata.rda")
load(file = "helper_dbdata.rda")

### names
test_that("Names are extracted correctly", {
  expected <- list(df1 = c("v1", "ID2"), df2 = c("ID2", "v2"))
  expect_identical(dbNames(filePath = "helper_dataBase.db"), expected)
})


### keys
test_that("Keys are extracted correctly", {
  keys <- dbKeys(filePath = "helper_dataBase.db")
  expect_equal(keys$pkList, pkList[sort(names(pkList))])
  expect_equal(keys$fkList, fkList[sort(names(fkList))])
})


### Labels
test_that("Labels are extracted correctly", {
  expect_equal(dbSingleDF(dfName = "Meta_Data", filePath = "helper_dataBase.db"), metaData)
})






context("Pull and merge data from DB")
library(eatDB)

# load test data (df1, df2, pkList, fkList)
# load(file = "tests/testthat/helper_dbdata.rda")
load(file = "helper_dbdata.rda")


### variable input check
allNames <- dbNames("helper_dataBase.db")
pkList <- dbKeys("helper_dataBase.db")$pkList
# allNames <- dbNames("tests/testthat/helper_database.db")
# pkList <- dbKeys("tests/testthat/helper_database.db")$pkList



test_that("Variable selection checked correctly", {
  expect_error(prep_vSelect(c("ID2", "ID3"), allNames, pkList),
               "ID3 are not in the data base")
  expect_silent(prep_vSelect(c("ID2", "v1"), allNames, pkList))
})

test_that("Variable selection prepared correctly", {
  expect_identical(prep_vSelect(c("v1", "ID2"), allNames, pkList), list(df1 = c("v1", "ID2"), df2 = character(0)))
  # prep_vSelect(c("v1", "ID2"), allNames, pkList)
})

test_that("Variable selection prepared correctly for output all variables", {
  expect_identical(prep_vSelect(vSelect = NULL, allNames, pkList), list(df1 = c("v1", "ID2"), df2 = c("v2")))
})

test_that("Missing primary keys are added if missing", {
  expect_identical(prep_vSelect(vSelect = ("v1"), allNames, pkList), list(df1 = c("v1", "ID2"), df2 = character(0)))
})

### Query creation
test_that("Shorten merge order", {
  expect_identical(shorten_mergeOrder(c("df2", "df1"), varList = list("df2" = character(), "df1" = "var")), "df1")
  expect_identical(shorten_mergeOrder(c("df3", "df2", "df1"), varList = list("df3" = character(), "df2" = character(), "df1" = "var")), "df1")
  expect_identical(shorten_mergeOrder(c("df3", "df2", "df1"), varList = list("df3" = "var", "df2" = character(), "df1" = "test")), c("df3", "df2", "df1"))

})

test_that("Left Joins formulated correctly based on foreign keys and merge Order", {
  expect_identical(write_LJoins(c("df1", "df2"), fkList = fkList),
                   "LEFT JOIN df2 using ( ID2  )")
  no_fkList <- fkList[1]
  expect_identical(write_LJoins(c("df1", "df2"), fkList = no_fkList),"")
})

test_that("Variable selection pasted correctly for single data frame", {
  expect_identical(write_varNames(dfName = "df1", vars = c("v1", "ID2")),
                   c("df1.v1",  "df1.ID2"))
  expect_identical(write_varNames(dfName = "df2", vars = character(0)),
                    character(0))
})

test_that("Variable selection pasted correctly", {
  expect_identical(write_SEL(list(df1 = c("v1", "ID2"), df2 = ("v2"))),
                   "df1.v1, df1.ID2, df2.v2")
  expect_identical(write_SEL(list(df1 = c("v1", "ID2"), df2 = character(0))),
                   "df1.v1, df1.ID2")
})


test_that("Complete Query pasted correctly", {
  expect_identical(writeQ_pull(list(df1 = c("v1", "ID2"), df2 = c("ID2", "v2")), mergeOrder = c("df1", "df2"),fkList = fkList),
                   "SELECT DISTINCT df1.v1, df1.ID2, df2.ID2, df2.v2 FROM df1 LEFT JOIN df2 using ( ID2  ) ;")
})


### Merged result
# create expected data result
m1 <- merge(dfList$df1, dfList$df2, all = TRUE)
expected <- m1[, c(2, 1, 3)]


test_that("Merged results are correct for complete pulls", {
  #expect_equal(dbPull(filePath = "tests/testthat/helper_dataBase.db"), expected)
  expect_equal(dbPull(filePath = "helper_dataBase.db"), expected)
})

test_that("Merged results are correct if one data table has no variables in output", {
  expect_equal(dbPull(vSelect = c("v1", "ID2"), filePath = "helper_dataBase.db"), expected[1, -3])
  # out3 <- dbPull(filePath = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dataBase.db", vSelect = "v2")
  out3 <- dbPull(filePath = "helper_dataBase.db", vSelect = "v2")
  expect_equal(out3, dfList$df2)
})

test_that("dbPull for single table data base",{
  # out <- dbPull(filePath = "tests/testthat/helper_dataBase_singleTable.db")
  # out2 <- dbPull(filePath = "tests/testthat/helper_dataBase_singleTable.db", vSelect = "ID2")
  out <- dbPull(filePath = "helper_dataBase_singleTable.db")
  out2 <- dbPull(filePath = "helper_dataBase_singleTable.db", vSelect = "ID2")
  expect_equal(out, dfList$df1)
  expect_equal(out2, dfList$df1[, 2, drop = F])
})


#dbPull(vSelect = c("v1", "ID2"), filePath = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_database.db")
#dbPull(vSelect = NULL, filePath = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_database.db")

### Right joins performed correclty
test_that("Specific join behaviour for incomplete ID matches", {
  ## more cases in first table: rows are kept and NAs on variables from table 2
  comp <- merge(dfList2$df1, dfList$df2, by = "ID2", all.x = TRUE)[, c(2, 1, 3)]
  # out <- dbPull(filePath = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dataBase2.db")
  out <- dbPull(filePath = "helper_dataBase2.db")
  expect_equal(comp, out)
  ## more cases in second table: rows are dropped
  comp <- merge(dfList3$df1, dfList$df2, by = "ID2", all.x = T)[, c(2, 1, 3)]
  out <- dbPull(filePath = "helper_dataBase3.db")
  expect_equal(comp, out)
})


test_that("Fixed that alphabetical ordering screws everything up!", {
  ## Names
  # dbNames(filePath = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dataBase4.db")
  # dbKeys(filePath = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dataBase4.db")
  expect_equal(names(dbNames(filePath = "helper_dataBase4.db")), c("b_df", "a_df"))
  expect_equal(names(dbKeys(filePath = "helper_dataBase4.db")$pkList), c("b_df", "a_df"))

  comp <- merge(dfList2$df1, dfList$df2, by = "ID2", all.x = TRUE)[, c(2, 1, 3)]
  # out <- dbPull(filePath = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dataBase4.db")
  out <- dbPull(filePath = "helper_dataBase4.db")
  expect_equal(comp, out)

  # out <- dbPull(filePath = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dataBase4.db", vSelect = "ID2")
  out <- dbPull(filePath = "helper_dataBase4.db", vSelect = "ID2")
  expect_equal(out, data.frame(ID2 = c(1, 2)))
})



## bt18 <- "q:/BT2018/BT/40_Daten/06_GADS/BT18_gads_v05.db"
# test <- dbPull(vSelect = c("bista"), filePath = bt18)


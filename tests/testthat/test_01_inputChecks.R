
context("Check input via check_input")
library(eatDB)


# load test data (df1, df2, pkList, fkList)
# load(file = "c:/Benjamin_Becker/02_Repositories/packages/eatDB/tests/testthat/helper_dbdata.rda")
load(file = "helper_dbdata.rda")


### Checking primary and foreign keys
test_that("No errors if called correctly ", {
  expect_silent(check_input(dfList = dfList, pkList = pkList, fkList = fkList))
})


## expected errors
pkList2 <- list(df1 = "v1",
                df3 = "ID2")
pkList3 <- list(df1 = "v1",
                df2 = "ID2")
pkList4 <- list(df1 = "v1",
                df2 = "ID5")
fkList2 <- list(df1 = list(References = "lala", Keys = NULL),
                df2 = list(References = "df1", Keys = "ID2"))
fkList3 <- list(df1 = list(References = NULL, Keys = NULL),
                df2 = list(References = NULL, Keys = "ID2"))
fkList4 <- list(df1 = list(References = NULL, Keys = NULL),
                df2 = list(References = "df1", Keys = "v1"))
fkList5 <- list(df1 = list(References = NULL, Keys = NULL),
                df2 = list(References = "df1", Keys = "v2"))


test_that("Errors are called correctly ", {
  expect_error(check_input(dfList = dfList, pkList = pkList2, fkList = fkList),
               "dfList and pkList have to have identical structure and namings.")
  expect_error(check_input(dfList = dfList, pkList = pkList, fkList = fkList2),
               "Foreign key defined for first data frame.")
  expect_error(check_input(dfList = dfList, pkList = pkList, fkList = fkList3),
               "Foreign Key reference for df2 must be exactly one other data frame.")
  expect_error(check_input(dfList = dfList, pkList = pkList, fkList = fkList4),
               "v1 are not variables in df2 .")
  expect_error(check_input(dfList = dfList, pkList = pkList, fkList = fkList5),
               "v2 are not variables in df1 .")
  expect_error(check_input(dfList = dfList, pkList = pkList4, fkList = fkList5),
               "ID5 are not variables in df2")
})

test_that("Illegal variable name checking ", {
  names(dfList$df1) <- c("v.1", "ID2")
  expect_error(check_input(dfList = dfList, pkList = pkList, fkList = fkList),
               "Variable names v.1 in df1 contain '.'.")
  df <- data.frame(group = 1:2, select = 3:4)
  dfList <- list(df = df)
  test_that("Irregular variable names are spotted",{
    expect_error(createDB(dfList, pkList = list(df = "group"), filePath = ":memory:"),
                 "Variable names GROUP, SELECT in df are forbidden SQLite Keywords.")
  })
})

test_that("Error for character foreign keys", {
  dfList3 <- dfList2 <- dfList
  dfList2$df1$ID2 <- as.character(dfList$df1$ID2)
  dfList3$df2$ID2 <- as.factor(dfList$df2$ID2)
  expect_error(check_input(dfList = dfList2, pkList = pkList, fkList = fkList),
               "All foreign keys have to be numeric. Check keys in df1.")
  expect_error(check_input(dfList = dfList3, pkList = pkList, fkList = fkList),
               "All foreign keys have to be numeric. Check keys in df2.")
})

test_that("Test if left joins are sufficient", {
  dfList2 <- dfList
  dfList2$df2[3, ] <- c(3, 1)
  expect_warning(check_input(dfList = dfList2, pkList = pkList, fkList = fkList), "For some rows, left joining df2 and df1 by ID2 will yield weird results.")
  dfList3 <- dfList
  dfList3$df1[2, ] <- c(3, 3)
  expect_silent(check_input(dfList = dfList3, pkList = pkList, fkList = fkList))
})

test_that("Check for duplicate variables within a data table", {
  dfList2 <- dfList
  names(dfList2$df2) <- c("id1", "ID1")

  expect_silent(check_df(dfList$df2, pkList$df2, df_name = "df2"))
  expect_error(check_df(dfList2$df2, pkList$df2, df_name = "df2"), "Some variables names in df2 are duplicates")
})

test_that("Check for duplicate variables except foreign keys across data tables", {
  dfList2 <- dfList
  names(dfList2$df2) <- c("ID2", "v1")

  expect_silent(check_dfList(dfList, fkList))
  expect_error(check_dfList(dfList2, fkList), "Duplicate variables in data frames df1 and df2:\nv1")
})

# tbd: further tests?


# should this test pass?
#   expect_error(addKeys(bigList = bigList, pkList = pkList3, fkList = fkList), "Foreign Key reference for df2 must be exactly one other data frame")



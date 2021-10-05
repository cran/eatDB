
#### Extracting data from relational data base
#############################################################################
#' Pull data from a relational data base.
#'
#' Function to extract specific variables from various data tables. Variables are merged in the specified merge order via left joins and using the foreign keys. If variables are selected from a specific data table, the corresponding primary keys are also always extracted. If no variables from the first data tables in the \code{mergeOrder} are selected, these data tables are skipped (up till the first variable - data table match). If only variables of a single data table are selected, this data table is extracted with all variables and sub setting is performed in \code{R}.
#'
#' Note that the exact merging process is determined when the data base is created via \code{\link{createDB}} and can not be altered post hoc. Further options (e.g. filtering cases, full joins) are still under development. If you want to use the package and have specific requests, please contact the package author.
#'
#'@param vSelect Character vector of variables that should be pulled from data base. If \code{vSelect} is \code{NULL}, all variables from the data base are selected.
#'@param filePath Path to an the existing db file.
#'
#'@return Returns a data frame, including the selected variables.
#'
#'@examples
#' db_path <- system.file("extdata", "example_dataBase.db", package = "eatDB")
#'
#' ## Extract variables from the first data table by name
#' # primary and foreign keys are added as required
#' dat1 <- dbPull(vSelect = c("age"), filePath = db_path)
#'
#' ## Extract all variables from the first data table
#' varNames <- dbNames(db_path)
#' dat2 <- dbPull(vSelect = varNames$NoImp, filePath = db_path)
#'
#' ## Extract variables from different data table (merged automatically)
#' dat3 <- dbPull(vSelect = c("weight", "noBooks", "pv"), filePath = db_path)
#'
#' ## Extract all variables from the data base
#' dat4 <- dbPull(filePath = db_path)
#'
#'@export
dbPull <- function(vSelect = NULL, filePath) {
  # 1) check input
  check_dbPath(dbPath = filePath)

  # extract relevant information from DB for preparation of pull
  keyList <- dbKeys(filePath, includeMeta = FALSE)
  allNames <- dbNames(filePath = filePath)

  # check names and sort to data tables
  varList <- prep_vSelect(vSelect = vSelect, allNames = allNames, pkList = keyList$pkList)

  # shortcut if all variables from single table (might be more memory intensive but faster for larger data sets)
  # also guarantees correct structure for pulls from one data table! (important feature)
  table_with_vars <- sapply(varList, length) > 0
  if(sum(table_with_vars) == 1) {
    #browser()
    out <- dbSingleDF(dfName = names(varList)[table_with_vars], filePath = filePath)
    return(out[, varList[[which(table_with_vars)]], drop = FALSE])
  }

  # Establish Connection, disconnect when function exits
  con <- dbConnect_default(dbName = filePath)
  on.exit(DBI::dbDisconnect(con))

  # 2) get names/structure/mergeorder for data base, sort varList by mergeOrder!
  mergeOrder <- get_mergeOrder(con)
  varList <- varList[match(mergeOrder, names(varList))]

  # 3) create query
  pullQ <- writeQ_pull(mergeOrder = mergeOrder, fkList = keyList$fkList, varList = varList)

  # 4) execute query
  df <- dbGet_safe(conn = con, statement = pullQ)

  df
}

### 1) Input check ---------------------------------------------------------
prep_vSelect <- function(vSelect, allNames, pkList) {
  # all variables are selected
  if(is.null(vSelect)) vSelect <- unique(unlist(allNames))
  # create List with data table attribution
  varList <- list()
  for(df_name in names(allNames)) {
    varList[[df_name]] <- order_vSelect(vSelect = vSelect, allNames_df = allNames[[df_name]])
    # variables are removed from further selection if they are in the data frame (prevents duplicates; enables later checking)
    vSelect <- vSelect[!vSelect %in% unlist(varList)]

    # add Primary Keys if missing and if any variables are selected (otherwise often data useless), keep in order of data table
    # don't add if it has been added in an earlier data table
    if(length(varList[[df_name]]) > 0){
      add_pks <- setdiff(pkList[[df_name]], unlist(varList))
      varList[[df_name]] <- order_vSelect(c(varList[[df_name]], add_pks), allNames_df = allNames[[df_name]])
    }
  }

  # check if all names anywhere in data set
  if(length(vSelect) != 0) {
    vSelect <- paste(vSelect, collapse = ", ")
    stop(paste("Variables ", vSelect, "are not in the data base"))
  }

  varList
}

#
order_vSelect <- function(allNames_df, vSelect) {
  if(is.null(vSelect)) return(allNames_df)
  allNames_df[allNames_df %in% vSelect]
}



### 2) Get Meta-Information from DB ---------------------------------------------------------
get_mergeOrder <- function(con) {
  q <- "SELECT * FROM Meta_Information;"
  mergeOrder <- DBI::dbGetQuery(conn = con, statement = q)
  # restore original format
  mO <- unlist(strsplit(unlist(mergeOrder), " "))
  names(mO) <- NULL
  mO[mO != ""]
}


### 3) Create Pull Query ---------------------------------------------------------
writeQ_pull <- function(varList, mergeOrder, fkList) {
  mergeOrder <- shorten_mergeOrder(mergeOrder = mergeOrder, varList = varList)
  ljoins <- write_LJoins(mergeOrder = mergeOrder, fkList = fkList)
  selVars <- write_SEL(varList = varList)
  # put together query
  paste("SELECT DISTINCT", selVars ,
        "FROM",
        mergeOrder[1],
        ljoins, ";")
}
# Kind of Hotfix: shorten mergeOrder to skip earlier data tables that are not needed
shorten_mergeOrder <- function(mergeOrder, varList) {
  test_used <- sapply(varList, function(l) length(l) > 0)
  # if(all(!test_used)) return(mergeOrder)
  start_data_table <- names(test_used)[test_used][1]
  mergeOrder_start <- which(mergeOrder == start_data_table)
  new_mergeOrder <- mergeOrder[mergeOrder_start:length(mergeOrder)]
  new_mergeOrder
}


# part of query for left joins (return empty string if data base consists of only on data table)
write_LJoins <- function(mergeOrder, fkList) {
  if(length(fkList) <= 1) return("")
  if(length(mergeOrder) <= 1) return("")
  joins <- vector("character")
  for(i in 2:length(mergeOrder)) {
    keyName <- fkList[[mergeOrder[i]]]$Keys
    joins[i-1] <- paste("LEFT JOIN", mergeOrder[i],
                        "using (", paste(keyName, collapse = ", "), " )", sep = " ")
  }
  paste(joins, collapse = " ")
}
# part of query for variable selection
write_SEL <- function(varList = NULL) {
  # default: selects all variables
  if(is.null(varList)) return(" * ")

  # otherwise:
  varVec <- unlist(Map(write_varNames, dfName = names(varList), vars = varList))
  varVec <- paste(varVec, collapse = ", ")
  varVec
}
# create variables names for single data frame
write_varNames <- function(dfName, vars) {
  if(length(vars) == 0) return(character(0))
  paste(dfName, ".", vars, sep = "")
}
# additional SQL Syntax for Full Join
write_addFullJoins <- function() {
  #tbd
  # unclear, how full joings can be implemented
  # Problem: multiple joins at work
}


# 04) Execute Queries ---------------------------------------------------------
# safe version of dbGet with verbose error
dbGet_safe <- function(conn, statement) {
  check <- try(DBI::dbGetQuery(conn = conn, statement = statement))
  if(class(check) == "try-error") {
    stop(paste("Error while trying to execute the following query", statement))
  }
  check
}








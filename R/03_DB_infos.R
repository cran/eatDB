
#### Viewing Interface to data base
#############################################################################
#' Get variable names from a relational data base.
#'
#' Function to get the names of the variables included in the relational data base.
#'
#' Extracts names of all variables included in the relational data base, structured as a list with the individual data tables as elements. The ordering in the list is equivalent to the merge order used when data is pulled from the data base.
#'
#'@param filePath Path of an existing \code{.db} file.
#'@param includeMeta Should the variable names of the \code{Meta_Data} table be included.
#'
#'@return Returns a named list of character vectors with the variables names included in the data tables.
#'
#'@examples
#' db_path <- system.file("extdata", "example_dataBase.db", package = "eatDB")
#' varNames <- dbNames(db_path)
#'
#' ## Names of data tables
#' names(varNames)
#'
#' ## Variable names in data table "NoImp"
#' varNames$NoImp
#'
#'@export
dbNames <- function(filePath, includeMeta = FALSE) {
  # check input
  check_dbPath(dbPath = filePath)

  # Establish Connection, disconnect when function exits
  con <- dbConnect_default(dbName = filePath)
  on.exit(DBI::dbDisconnect(con))

  # get data table names
  dtNames <- get_mergeOrder(con)
  dtNames <- c(dtNames, "Meta_Data", "Meta_Information")

  # plausability check for data table names
  if(!all(DBI::dbListTables(con) %in% dtNames)) stop("Merge order information incompatible with data table names.")

  # get all variable names in these tables
  nameList <- lapply(dtNames, DBI::dbListFields, conn = con)
  names(nameList) <- dtNames

  # drop information about meta data tables
  if(!includeMeta) nameList$Meta_Information <- nameList$Meta_Data <- NULL

  nameList
}

#############################################################################
#' Get keys from a relational data base.
#'
#' Function to get the primary and foreign keys of the data frames in the relational data base.
#'
#' Data in a relational data base are indexed by primary and foreign keys. Primary keys are unique identifiers
#' inside a single data table. Foreign keys reference (link) to other data tables. This function returns the
#' key structure of a relational data base.
#'
#'@param filePath Path of the existing db file.
#'@param includeMeta Should information about the \code{Meta_Data} table be included.
#'
#'@return Returns a list named as the data tables in the db. Each elements contains a list with the primary key, the
#' data table it references to and the corresponding foreign keys.
#'
#'@examples
#' db_path <- system.file("extdata", "example_dataBase.db", package = "eatDB")
#' keys <- dbKeys(db_path)
#'
#' ## primary key structure of the database
#' keys$pkList
#'
#' ## foreign key structure of the database
#' keys$fkList
#'
#'@export
dbKeys <- function(filePath, includeMeta = FALSE) {
  # check input
  check_dbPath(dbPath = filePath)

  # Establish Connection, disconnect when function exits
  con <- dbConnect_default(dbName = filePath)
  on.exit(DBI::dbDisconnect(con))

  # get db names
  dtNames <- names(dbNames(filePath, includeMeta = includeMeta))

  ## Primary keys
  # create query
  pkQueries <- paste("PRAGMA table_info(", dtNames, ")")
  # execute query and transform info
  pk_table <- lapply(pkQueries, DBI::dbGetQuery, conn = con)
  pk_list <- lapply(pk_table, extract_PKs)

  ## foreign keys
  fkQueries <- paste("PRAGMA foreign_key_list(", dtNames, ")")
  # execute query and transform info
  fk_table <- lapply(fkQueries, DBI::dbGetQuery, conn = con)
  fk_list <- lapply(fk_table, extract_FKs)

  ## structure and name output
  names(pk_list) <- dtNames
  names(fk_list) <- dtNames

  list(pkList = pk_list, fkList = fk_list)
}

### Check dbpath ---------------------------------------------------------
check_dbPath <- function(dbPath) {
  if(!file.exists(dbPath)){
    stop(paste(dbPath, " is not a valid path to a data base"))
  }
}

### Extract output ---------------------------------------------------------
## extract Primary Keys from SCHEMA output
extract_PKs <- function(table_info) {
  table_info[table_info$pk != 0, "name"]
}

## extract foreign Keys from SCHEMA output
extract_FKs <- function(table_info) {
  if(nrow(table_info) == 0) return(list(References = NULL, Keys = NULL))
  ref <- unique(table_info$table)
  if(length(ref) > 1) stop("Foreign keys for more than 1 table defined, check data base creation.")
  if(!identical(table_info$from, table_info$to)) stop("Foreign and primary key have different names, error was made during creation of data base!")
  keys <- table_info$from
  list(References = ref, Keys = keys)
}


#############################################################################
#' Extract a single data table from a relational data base.
#'
#' Function to extract a single, complete data table from a relational data base. Especially useful for the extraction of the meta information stored in \code{Meta_Data}.
#'
#' This function makes use of the \code{DBI::dbReadTable} function and extracts a complete data table from a data base. All variables are extracted and all rows are used. For extracting only some variables or merging data tables see \code{\link{dbPull}}.
#'
#'@param dfName Name of the data table which should be extracted.
#'@param filePath Path of the existing db file.
#'
#'@return Returns a data frame with all variables and cases as in the corresponding data table.
#'
#'@examples
#' db_path <- system.file("extdata", "example_dataBase.db", package = "eatDB")
#'
#' ## Extract all meta information
#' meta_data <- dbSingleDF(dfName = "Meta_Data", filePath = db_path)
#' meta_data
#'
#' ## Extract a specific data table
#' NoImp <- dbSingleDF(dfName = "NoImp", filePath = db_path)
#' NoImp
#'
#'@export
dbSingleDF <- function(dfName = 'Meta_Data', filePath) {
  # check input
  check_dbPath(dbPath = filePath)

  # Establish Connection, disconnect when function exits
  con <- dbConnect_default(dbName = filePath)
  on.exit(DBI::dbDisconnect(con))

  # extract single table from data base
  out <- DBI::dbReadTable(conn = con, name = dfName)

  out
}












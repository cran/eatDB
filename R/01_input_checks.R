

#### check input
#############################################################################
check_input <- function(dfList, pkList, fkList) {
  if(is.null(names(dfList)) || is.null(names(pkList)) || is.null(names(fkList)) ) stop("All input lists have to be named")
  if(!is.list(dfList)) stop("dfList has to be a list.")
  if(!identical(names(dfList), names(pkList))) stop("dfList and pkList have to have identical structure and namings.")
  if(!identical(names(dfList), names(fkList))) stop("dfList and fkList have to have identical structure and namings.")

  # check all data frames, primary, foreign keys and data frame combinations
  Map(check_df, df = dfList, df_name = names(dfList))
  Map(check_pk, df = dfList, primeKey = pkList, df_name = names(dfList))
  Map(function(fk, df_n) check_fk(foreignKey = fk, df_name = df_n, dfList = dfList, pkList = pkList),
      fk = fkList, df_n = names(dfList))
  check_dfList(dfList = dfList, fkList = fkList)

  return()
}


#
# 01) input checks for single data frame ---------------------------------------------------------
check_df <- function(df, primeKey, df_name) {
  if(!is.data.frame(df)) stop(paste(df_name), " is not a data frame.")
  if(ncol(df) < 1 || nrow(df) < 1) stop(paste(df_name), " is a data.frame with zero rows or columns.")
  if(identical(df_name, "")) stop("Data frame with empty name.")

  # names data frame
  if(any(duplicated(tolower(names(df))))) stop(paste("Some variables names in", df_name, "are duplicates"))
  if(any(identical(names(df), ""))) stop(paste("Some variables names in ", df_name, "are empty"))
  illegal_names <- c("Meta_Data", "Meta_Information")
  if(any(names(df) %in% illegal_names)) stop(paste("One of the following forbidden data frame names has been used:",
                                                   paste(illegal_names, collapse = ", ")))
  dot_names <- grep("\\.", names(df), value = TRUE)
  if(length(dot_names)) stop("Variable names ", paste(dot_names, collapse = ", "), " in ", df_name, " contain '.'.")
  forbid_names <- unlist(lapply(names(df), function(varName) grep(
    paste0("^", varName, "$"), eatDB::sqlite_keywords, value = TRUE, ignore.case = TRUE)))
  if(length(forbid_names) > 0) stop("Variable names ", paste(forbid_names, collapse = ", "), " in ", df_name,
                                    " are forbidden SQLite Keywords.")
  return()
}


# 02) input checks for primary key ---------------------------------------------------------
check_pk <- function(primeKey, df, df_name) {
  print_keys <- paste(primeKey, collapse = " ,")
  if(!all(primeKey %in% names(df))) stop(paste(print_keys, "are not variables in", df_name))
}

# 03) input checks for foreign key ---------------------------------------------------------
check_fk <- function(foreignKey, df_name, dfList, pkList) {
  print_keys <- paste(foreignKey[[2]], collapse = " ,")
  if(length(foreignKey) != 2 || !all(names(foreignKey) %in% c("References", "Keys"))) {
    stop("Foreign Keys for ", df_name, " have incorrect format, check help for correct formatting.")
  }

  # foreign keys for first data frame has to be NULL, then stop testing
  if(df_name == names(dfList)[1]) {
    if(!is.null(foreignKey$References) || !is.null(foreignKey$Keys)) {
      stop("Foreign key defined for first data frame.")
    }
    return()
  }

  ref <- foreignKey$References
  keys <- foreignKey$Keys
  # formal checks
  if(length(ref) != 1) stop("Foreign Key reference for ", df_name, " must be exactly one other data frame.")
  if(length(keys) < 1) stop("Foreign Keys for ", df_name, " are undefined.")
  # reference
  if(!ref %in% names(dfList)) stop(paste("Foreign Key for", df_name, "references not an existing data frame."))
  # keys in both data frames?
  if(any(!keys %in% names(dfList[[df_name]]))) stop(paste(print_keys, "are not variables in", df_name, "."))
  if(any(!keys %in% names(dfList[[ref]]))) stop(paste(print_keys, "are not variables in", ref, "."))

  # test whether there are rows lost by left joins and full joins become necessary
  for(i in keys) {
    if(any(!dfList[[df_name]][[i]] %in% dfList[[ref]][[i]])) warning("For some rows, left joining ", df_name, " and ", ref, " by ", i, " will yield weird results.", call. = FALSE)
  }

  # all keys must be numerics, strings slow down SQLite A LOT
  if(any(!unlist(lapply(dfList[[df_name]][, keys], is.numeric)))) stop("All foreign keys have to be numeric. Check keys in ", df_name, ".")
  if(any(!unlist(lapply(dfList[[ref]][, keys], is.numeric)))) stop("All foreign keys have to be numeric. Check keys in ", ref, ".")
  return()
}

# 04) input checks for dfList ---------------------------------------------------------
# right now only for duplicate variables that are not foreign keys
check_dfList <- function(dfList, fkList) {
  lapply(names(dfList), check_dfList_single_df, dfList = dfList, fkList = fkList)
  return()
}

check_dfList_single_df <- function(df_name, dfList, fkList) {
  df <- dfList[[df_name]]
  # select all relevant foreign key variables of the data frame
  reverse_fks <- unlist(lapply(fkList, function(fk) identical(fk$References, df_name)))
  reverse_fks <- fkList[reverse_fks]
  reverse_fks <- unlist(lapply(reverse_fks, function(fk) fk$Keys))
  all_fks <- unique(c(fkList[[df_name]]$Keys, reverse_fks))

  other_dfs <- dfList[!names(dfList) %in% df_name]
  Map(function(other_df, other_df_name){
    check_dfList_single_matchup(other_df = other_df, other_df_name = other_df_name,
                               df = df, df_name = df_name, all_fks = all_fks)
  }, other_df = other_dfs, other_df_name = names(other_dfs))
}

check_dfList_single_matchup <- function(other_df, other_df_name, df, df_name, all_fks) {
  allNames <- c(names(other_df), names(df))
  duplics <- allNames[duplicated(allNames)]
  true_duplics <- duplics[!duplics %in% all_fks]
  if(length(true_duplics) > 0)  {
    true_duplics_print <- paste(true_duplics, collapse = ", ")
    stop("Duplicate variables in data frames ", df_name, " and ", other_df_name, ":\n",
                                    true_duplics_print, call. = FALSE)
  }
}



dm_pivot <- function(dm_cmb = dm_combine(), quiet = FALSE) {
  as_rlang_error(checkmate::assert_flag(quiet))
  # Pivot HLA table
  if (!quiet) rlang::inform("Pivoting hla...")
  dm_cmb <- dm_cmb %>%
    dm::dm_select_tbl(-"hla") %>%
    dm::dm("hla" := pivot_hla(dm_cmb$hla))
  # Possibly pivot cerner later (see code below)
  # Return
  dm_cmb
}


pivot_hla <- function(dt_hla) {
  dt_hla <- data.table::as.data.table(dt_hla)
  id_cols <- stringr::str_subset(colnames(dt_hla), "_id|date_")
  rhs_cols <- "cat_gene"
  value_cols_old <- stringr::str_subset(colnames(dt_hla), "allele")
  value_cols <- paste0("mcat_", stringr::str_remove(value_cols_old, "^cat_"))
  data.table::setnames(dt_hla, value_cols_old, value_cols)
  pivot_expr <- paste0("data.table::dcast(",
   "dt_hla, ",
   paste0(id_cols, collapse = " + "), "  ~ ", rhs_cols, ", ",
   "value.var = c(", paste0("'", value_cols, "'", collapse = ", "), "), ",
   "fun.aggregate = function(x) {
     if (length(x) == 0L) return(NA_character_)
     u <- unique(x)
     if (length(u) == 1L && length(x) %% 2 == 0L) u <- c(u, u)
     paste0(u, collapse = ',')
   }",
  ")")
  dt_hla <- pivot_expr %>%
    rlang::parse_expr() %>%
    eval() %>%
    setTBL() %>%
    janitor::clean_names() %>%
    dplyr::rename_with(~ stringr::str_remove(.x, "_allele"))

  dt_hla
}


pivot_cerner <- function(dt_cerner) {
  dt_cerner <- data.table::as.data.table(dt_cerner)
  # Standardize test names
  # Get code-test mapping
  code_to_test <- unique(dt_cerner[, c("cerner_code", "test")])
  # Sort to find best test name for each code
  code_to_test[, c("longer", "has_par") := list(
    nchar(test) == max(nchar(test)),
    test %like% "[()]"
  ), by = "cerner_code"]
  data.table::setorderv(code_to_test, c("cerner_code", "has_par", "longer"))
  code_to_test[, c("has_par", "longer") := NULL]
  # Get unique many-to-one mappings (code-to-test)
  code_to_test[, "test" := test[[1L]], by = "cerner_code"]
  code_to_test <- unique(code_to_test)
  # Standardize for column names
  code_to_test[, "test" := janitor::make_clean_names(test, allow_dupes = TRUE)]
  # Get standardized column mappings
  cerner_var_map <- data.table::fread(
    system.file("extdata/cerner_var_map.csv", package = "dmhct"),
    colClasses = "character",
    na.strings = NULL
  )
  cerner_var_map[, c("cerner_code", "test") := lapply(
    .SD, janitor::make_clean_names, allow_dupes = TRUE
  ), .SDcols = c("cerner_code", "test")]
  # Replace test names with cerner var map names where possible
  code_map <- data.table::merge.data.table(
    code_to_test, cerner_var_map[, c("cerner_code", "var")], by = "cerner_code"
  )
  test_map <- data.table::merge.data.table(
    code_to_test, cerner_var_map[, c("test", "var")], by = "test"
  )
  code_to_test <- unique(rbind(code_map, test_map))
  code_to_test <- code_to_test[!is.na(var)]
  code_to_test <- code_to_test[, list(var = var[[1L]]), by = "cerner_code"]
  # Mark as standardized and make duplicates distinct
  code_to_test[, ".std_" := TRUE]
  # Merge
  dt_cerner <- data.table::merge.data.table(
    dt_cerner,
    code_to_test,
    by = "cerner_code",
    all.x = TRUE,
    all.y = FALSE
  )
  # Use standardized name in original data if not in `code_to_test`
  dt_cerner[is.na(.std_), var := paste0("unk_", janitor::make_clean_names(test, allow_dupes = TRUE))]
  dt_cerner[, "test" := var][, c(".std_", "cerner_code", "var") := NULL]
  # Re-order columns
  data.table::setcolorder(dt_cerner, c("test", "entity_id", "date", "result", "units"))
  # Re-order rows
  data.table::setkeyv(dt_cerner, colnames(dt_cerner))
  # Get unique rows
  dt_cerner <- unique(dt_cerner, fromLast = TRUE, by = setdiff(colnames(dt_cerner), "units"))
  # Remove missing results when there are non-missing values for that test patient, and date
  dt_cerner[, .all_na_ := all(is.na(result)), by = intersect(c("test", "entity_id", "date"), colnames(dt_cerner))]
  dt_cerner <- dt_cerner[!is.na(result) | .all_na_][, ".all_na_" := NULL]
  # # Pivot wider
  # dt_cerner <- data.table::dcast(
  #   dt_cerner,
  #   formula = entity_id + date + .i_ ~ test,
  #   value.var = c("result")
  # )
  # # Convert to tibble
  # setTBL(dt_cerner)
  # Return
  dt_cerner
}

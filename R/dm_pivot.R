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
  value_cols <- stringr::str_subset(colnames(dt_hla), "allele")
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
    janitor::clean_names()
  dt_hla
}


pivot_cerner <- function(
    dt_cerner,
    na_pattern = c(
      "^-+$", # Includes ""
      "(?:^|\\W)N/?A(?:$|\\W)",
      "NOT? (?:RER?PORT|(?:[A-Z ]|-)*PHENOTYPE)",
      "NOT? (?:AMP|AVAIL|DATA|DONE|EVAL|FOLLOWED|IDENT|VALUE)",
      "(?:^|\\W)(?:NG|UNK|CQNS|QNS|QIS|NSQ|IQ|QI|NR)(?:$|\\W)",
      "(?:^|\\W)(?:CANCELL?(?:ED)?|NULL|UNKNOWN)(?:$|\\W)",
      "(?:^|\\W)(?:INCONCLUSIVE|INSUFFICIENT|EQUIVOC?A?L?)(?:$|\\W)",
      "(?:^|\\W)(?:SEE COMMENT|SEE SCANNED DOC)(?:$|\\W)",
      "-999[0-9]",
      "^REPORTED$"
    )
) {
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
  # Prepare for combination with result
  for (pat in na_pattern) {
    data.table::set(dt_cerner, i = which(dt_cerner$result %like% pat), j = "result", value = "")
    data.table::set(dt_cerner, i = which(dt_cerner$units %like% pat), j = "units", value = "")
  }
  dt_cerner[is.na(result), "result" := ""]
  dt_cerner[is.na(units), "units" := ""][units != "", "units" := paste0(" ", units)]
  # Combine result and units, convert back to NA
  dt_cerner[, "result" := paste0(result, units)][, "units" := NULL]
  dt_cerner[, "result" := std_chr(result)]
  # Pivot wider
  dt_cerner <- data.table::dcast(
    dt_cerner,
    formula = entity_id + date ~ test,
    value.var = "result",
    fun.aggregate = function(x) {
      if (length(x) == 0L) return(NA_character_)
      paste0(x, collapse = " | ")
    }
  )
  # Convert to tibble
  setTBL(dt_cerner)
  # Return
  dt_cerner
}

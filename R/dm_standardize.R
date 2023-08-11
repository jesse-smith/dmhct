#' Standardize Column Values in a local `dm` for MLinHCT
#'
#' @description
#' `dm_standardize()` takes a local version of the SQL server as input and
#' standardizes all columns across all tables. Standardization procedures are
#' based on both column type and the typing prefix of the column name. Specifically,
#' columns are standardized using the following workflow:
#'
#' \enumerate{
#'   \item Columns with type `character` or chr/cat/lgl/mcat/intvl prefixes are
#'     passed to `std_chr()`
#'   \item Columns with type `logical` or the lgl prefix are passed to `std_lgl()`
#'   \item Columns with type `numeric` or `integer`, or num/pct prefixes, are passed
#'     to `std_num()`
#'   \item Columns with the intvl prefix are passed to `std_intvl()`
#'   \item Column with types `Date`, `POSIXct`, or `POSIXlt`, or with dt/dttm/date
#'     prefixes, are passed to `std_date()`
#' }
#'
#' After standardization, tables are sorted into alphabetical order before
#' returning.
#'
#' @param dm_local A local `dm` object containing MLinHCT data from `dm_extract()`
#' @param quiet Whether to suppress progress messages
#'
#' @return The input `dm` with standarized column values
#'
#' @export
dm_standardize <- function(dm_local = dm_extract(), quiet = FALSE) {
  as_rlang_error(checkmate::assert_flag(quiet))
  force(dm_local)
  tbl_nms <- names(dm_local)
  for (tbl_nm in tbl_nms) {
    if (!quiet) rlang::inform(paste0("Standardizing `", tbl_nm, "`"))
    # Standardize table
    dt <- tbl_standardize(dm_local[[tbl_nm]], quiet = quiet)
    # Handle EAV tables
    if (tbl_nm %like% "cerner[0-9]") dt <- cerner_standardize(dt)
    # Replace table
    dm_local <- dm_local %>%
      dm::dm_select_tbl(-{{ tbl_nm }}) %>%
      dm::dm({{ tbl_nm }} := dt)
  }

  dm_sort(dm_local)
}


tbl_standardize <- function(tbl, arg_nm = NULL, quiet = FALSE) {
  as_rlang_error(checkmate::assert_flag(quiet))
  if (is.null(arg_nm)) arg_nm <- rlang::englue("{{ tbl }}")
  as_rlang_error(checkmate::assert_string(arg_nm))
  tbl_class <- class(tbl)[[1L]]
  dt <- data.table::as.data.table(tbl)
  # Handle all character columns first
  chr_cols <- select_colnames(
    dt, where(is.character) | dplyr::starts_with(c("chr_", "cat_", "lgl_", "mcat_", "intvl_"))
  )
  # Standardize character columns
  dt[, (chr_cols) := lapply(.SD, std_chr), .SDcols = chr_cols]

  # Handle logical columns
  lgl_cols <- select_colnames(dt, dplyr::starts_with("lgl_"))
  for (col in lgl_cols) {
    withCallingHandlers(
      data.table::set(dt, j = col, value = std_lgl(dt[[col]], std_chr = FALSE, warn = !quiet)),
      warning = function(w) {
        w$call <- NULL
        w$message <- paste0("When standardizing `", arg_nm, "$", col, "`:\n", w$message)
        rlang::cnd_signal(w)
        rlang::cnd_muffle(w)
      }
    )
  }

  # Handle numeric columns, including id cols
  num_cols <- select_colnames(
    dt, where(is.numeric) | dplyr::starts_with(c("num_", "pct_", "entity_id", "donor_id"))
  )
  for (col in num_cols) {
    donor_host <- stringr::str_extract(col, "donor|host")
    donor_host <- if (is.na(donor_host)) "ignore" else paste0("use_", donor_host)
    withCallingHandlers(
      data.table::set(dt, j = col, value = std_num(dt[[col]], std_chr = FALSE, warn = !quiet, donor_host = donor_host)),
      warning = function(w) {
        w$call <- NULL
        w$message <- paste0("When standardizing `", arg_nm, "$", col, "`:\n", w$message)
        rlang::cnd_signal(w)
        rlang::cnd_muffle(w)
      }
    )
  }

  # Handle interval columns
  intvl_cols <- select_colnames(dt, dplyr::starts_with("intvl_"))
  for (col in intvl_cols) {
    donor_host <- stringr::str_extract(col, "donor|host")
    donor_host <- if (is.na(donor_host)) "ignore" else paste0("use_", donor_host)
    withCallingHandlers(
      data.table::set(dt, j = col, value = std_intvl(dt[[col]], std_chr = FALSE, warn = !quiet, donor_host)),
      warning = function(w) {
        w$call <- NULL
        w$message <- paste0("When standardizing `", arg_nm, "$", col, "`:\n", w$message)
        rlang::cnd_signal(w)
        rlang::cnd_muffle(w)
      }
    )
  }

  # Handle date and datetime columns
  date_cols <- select_colnames(
    dt, where(lubridate::is.Date) | where(lubridate::is.POSIXt) | dplyr::starts_with(c("dt_", "dttm_", "date"))
  )
  for (col in date_cols) {
    col_vec <- dt[[col]]
    withCallingHandlers(
      data.table::set(dt, j = col, value = std_date(dt[[col]], warn = !quiet)),
      warning = function(w) {
        w$call <- NULL
        w$message <- paste0("When standardizing `", arg_nm, "$", col, "`:\n", w$message)
        rlang::cnd_signal(w)
        rlang::cnd_muffle(w)
      }
    )
  }

  if (is_tibble(tbl)) {
    setTBL(dt)
  } else if (is.data.frame(tbl)) {
    data.table::setDF(dt)
  } else {
    dt <- tryCatch(methods::as(dt, tbl_class), error = function(e) dt)
  }

  dt
}


cerner_standardize <- function(dt) {
  # Convert to data.table in place
  data.table::setDT(dt)
  # Convert cerner code to character if not already
  if (!is.character(dt$cerner_code)) dt[, "cerner_code" := std_chr(cerner_code)]
  # Create `units` if it does not already exist
  if (!"units" %in% colnames(dt)) dt[, "units" := NA_character_]
  # Add column to track row order
  dt[, ".row_id_" := .I]
  # Load test map and standardize cerner code
  cerner_test_map <- data.table::fread(
    system.file("extdata/cerner_test_map.csv", package = "dmhct"),
    colClasses = "character",
    na.strings = NULL
  )
  cerner_test_map[, "cerner_code" := std_chr(cerner_code)]
  # Match test_std using cerner code
  dt <- data.table::merge.data.table(
    dt,
    unique(cerner_test_map[, c("cerner_code", "test_std")], by = "cerner_code"),
    by = "cerner_code",
    all.x = TRUE
  )
  # Match remaining test_std using test name
  dt[is.na(test_std), "test_std" := data.table::merge.data.table(
    .SD,
    unique(..cerner_test_map[, c("test", "test_std")], by = "test"),
    all.x = TRUE
  )$test_std, .SDcols = c("test", "test_std")]
  # Fill remaining test_std using cleaned test names from dataset
  # Ideally should not be many (or any), so just run on raw names (no joining)
  dt[is.na(test_std), "test_std" := janitor::make_clean_names(test, allow_dupes = TRUE)]
  # Move `test_std` to be after `test`
  cols <- colnames(dt)
  test_loc <- which(cols == "test")
  col_order <- unique(c(cols[1:test_loc], "test_std", cols[(test_loc + 1L):length(cols)]))
  data.table::setcolorder(dt, col_order)
  # Replace `na_patterns` with `NA` in both `result` and `units`
  dt[, "result" := str_to_na(result, ..na_patterns)]
  dt[, "units" := str_to_na(units, ..na_patterns)]
  # If units are empty, look for them in `result`
  units <- unique(dt$units)
  units <- union(units, c(
    "%", "% TOTAL LYMPHOCYTES", "/MM3", "AU/ML", "BPM", "BR/MIN", "C", "CM", "FL", "G/DL", "G/ML", "INT UNITS/ML",
    "IU/ML", "IV", "KG", "KG/M2", "KU/L", "L", "L/MIN", "LOG", "LOG10 COPIES/ML", "LOG10 IU/ML", "M2", "MEQ/L",
    "MG/24 HR", "MG/DL", "MILLI INT UNITS/ML", "MILLIONINTUNITS", "ML", "ML/MIN", "MM HG", "MM3", "MMOL/L", "PG", "PIV",
    "U/ML", "UG/ML", "UNITS/L", "UNITS/ML", "X10^3/MM3", "X10^6/MM3", "X10E3/MM3", "X10E6/MM3"
  ))
  units <- sort(units[!is.na(units)])
  units_pat <- paste0("(?<![A-Z])(?:", paste0(units, collapse = "|"), ")$")
  dt[, "units2" := stringr::str_extract(result, ..units_pat)]
  dt[is.na(units) & !is.na(units2), "units" := units2]
  dt[!is.na(units2), "result" := std_chr(stringr::str_remove(result, ..units_pat))]
  dt[, "units2" := NULL]
  # Re-order to original row order
  data.table::setorderv(dt, ".row_id_")
  dt[, ".row_id_" := NULL]
  # Convert (back) to tibble
  setTBL(dt)
}

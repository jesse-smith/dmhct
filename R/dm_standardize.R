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
dm_standardize <- function(dm_local = dm_extract(.legacy = FALSE), quiet = FALSE) {
  tbl_nms <- names(dm_local)
  for (tbl_nm in tbl_nms) {
    if (!quiet) rlang::inform(paste0("Standardizing ", tbl_nm, "..."))
    dt <- data.table::as.data.table(dm_local[[tbl_nm]])

    # Handle all character columns first
    chr_cols <- select_colnames(
      dt, where(is.character) | dplyr::starts_with(c("chr_", "cat_", "lgl_", "mcat_", "intvl_"))
    )
    # Add cerner result columns if absent
    if (tbl_nm %like% "cerner[0-9]") chr_cols <- union(chr_cols, "result")
    # Standardize character columns
    dt[, (chr_cols) := lapply(.SD, std_chr), .SDcols = chr_cols]

    # Handle logical columns
    lgl_cols <- select_colnames(dt, dplyr::starts_with("lgl_"))
    for (col in lgl_cols) {
      withCallingHandlers(
        data.table::set(dt, j = col, value = std_lgl(dt[[col]], std_chr = FALSE, warn = !quiet)),
        warning = function(w) {
          w$call <- NULL
          w$message <- paste0("When standardizing `", tbl_nm, "$", col, "`:\n", w$message)
          rlang::cnd_signal(w)
          rlang::cnd_muffle(w)
        }
      )
    }

    # Handle numeric columns
    num_cols <- select_colnames(
      dt, where(is.numeric) | dplyr::starts_with(c("num_", "pct_"))
    )
    for (col in num_cols) {
      withCallingHandlers(
        data.table::set(dt, j = col, value = std_num(dt[[col]], std_chr = FALSE, warn = !quiet)),
        warning = function(w) {
          w$call <- NULL
          w$message <- paste0("When standardizing `", tbl_nm, "$", col, "`:\n", w$message)
          rlang::cnd_signal(w)
          rlang::cnd_muffle(w)
        }
      )
    }

    # Handle interval columns
    intvl_cols <- select_colnames(dt, dplyr::starts_with("intvl_"))
    for (col in intvl_cols) {
      ch <- stringr::str_extract(col, "(?<=_)donor|host$")
      if (is.na(ch)) ch <- "no"
      withCallingHandlers(
        data.table::set(dt, j = col, value = std_intvl(dt[[col]], std_chr = FALSE, warn = !quiet, chimerism = ch)),
        warning = function(w) {
          w$call <- NULL
          w$message <- paste0("When standardizing `", tbl_nm, "$", col, "`:\n", w$message)
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
          w$message <- paste0("When standardizing `", tbl_nm, "$", col, "`:\n", w$message)
          rlang::cnd_signal(w)
          rlang::cnd_muffle(w)
        }
      )
    }

    # Replace table
    dm_local <- dm_local %>%
      dm::dm_select_tbl(-{{ tbl_nm }}) %>%
      dm::dm({{ tbl_nm }} := dplyr::as_tibble(dt))

    # Force cleanup
    gc(verbose = FALSE)
  }

  if (!quiet) rlang::inform("Done.")

  dm_sort(dm_local)
}

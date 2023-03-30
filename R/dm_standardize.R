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
  }

  if (!quiet) rlang::inform("Done.")

  dm_sort(dm_local)
}

dm_mrd_std <- function(dm_local) {
  dt <- dm_local$mrd
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Standardize columns loaded as character
  chr_cols <- purrr::map_lgl(dt, is.character)
  chr_cols <- names(chr_cols[chr_cols])
  dt[, (chr_cols) := lapply(.SD, std_chr), .SDcols = chr_cols]

  # Convert missing values loaded as something else
  na <- c(
    "", "-9996", "-9999", "NA", "NA.", "N/A", "N/A.", "N/A-NOT GIVEN", "NA-NOT GIVEN",
    "NO DATA", "APL NOT FOLLOWED BY FLOW MRD", "IN CR. SPECIMEN BANKED", "INCONCLUSIVE",
    "NO PHENOTYPE", "NO SUITABLE PHENOTYPE", "NO VALUE GIVEN",
    "NO VALUE TO REPORT", "NOT AVAILABLE", "NOT REPORTED"
  )
  na_src <- c("1/1/1900 0:00")
  dt[, (chr_cols) := lapply(.SD, function(x) {
    data.table::fifelse(x %in% ..na, NA_character_, x)
  }), .SDcols = chr_cols]
  dt[cat_source %in% na_src, "cat_source" := NA_character_]

  # Ensure `date` is `Date` (NOTE: MAY CHANGE TO DATETIME/POSIXct IN THE FUTURE)
  dt[, "date" := UtilsDate$std_date(date, force = "dt")]

  # Convert `cat_source` to factor w/ no info loss
  dt[, "cat_source" := factor(data.table::fcase(
    grepl("BONE MARROW", cat_source, fixed = TRUE), "BONE MARROW",
    grepl("PERIPHERAL BLOOD", cat_source, fixed = TRUE), "PERIPHERAL BLOOD",
    startsWith(cat_source, "1"), "BONE MARROW",
    startsWith(cat_source, "2"), "PERIPHERAL BLOOD",
    rep(TRUE, length(cat_source)), cat_source
  ))]

  # Convert `cat_method` to factor w/ no info loss
  dt[, "cat_method" := factor(data.table::fcase(
    grepl("FLOW CYTOMETRY", cat_method, fixed = TRUE), "FLOW CYTOMETRY",
    grepl("RT[^A-Z0-9]*PCR", cat_method), "RT-PCR",
    grepl("PCR", cat_method, fixed = TRUE), "PCR",
    grepl("FISH", cat_method, fixed = TRUE), "FISH",
    grepl("NGS", cat_method, fixed = TRUE), "NGS",
    startsWith(cat_method, "1"), "FLOW CYTOMETRY",
    startsWith(cat_method, "2"), "PCR",
    startsWith(cat_method, "3"), "RT-PCR",
    startsWith(cat_method, "4"), "FISH",
    startsWith(cat_method, "5"), "NGS",
    rep(TRUE, length(cat_method)), cat_method
  ))]

  # Get denominator for MRD result
  dt[, c("cat_units", "result_units", "value_units") := lapply(.SD, function(x) {
    stringr::str_extract(x, "%|MILLION|NORMALIZED")
  }), .SDcols = c("cat_units", "pct_result", "num_actual_value")]
  dt[, "comment_units" := txt_comments %>%
       stringr::str_replace(".*CD\\s?[0-9].*", NA_character_) %>%
       stringr::str_extract("[0-9.]+(?:[Ee]-?[0-9.]+)?\\s?(?:%|CELLS/MILLION|NORMALIZED COPY NUMBER)") %>%
       stringr::str_extract("%|MILLION|NORMALIZED")]

  # Extract logical values for MRD result
  dt[, c("lgl_result", "lgl_actual_value", "lgl_comments") := lapply(.SD, function(x) {
    data.table::fcase(
      stringr::str_detect(x, "(?<!CD\\s?[0-9]{1,2}\\s)POSITIVE(?!=\\sCD\\s?[0-9]{1,2})"), TRUE,
      stringr::str_detect(x, "(?<!CD\\s?[0-9]{1,2}\\s)NEGATIVE|(?:BELOW LOD)|(?:BELOW LIMIT)|(?:NOTHING TO SUGGEST)(?!=\\sCD\\s?[0-9]{1,2})"), FALSE
    )
  }), .SDcols = c("pct_result", "num_actual_value", "txt_comments")]
  comp_rv <- dt$lgl_result != dt$lgl_actual_value
  comp_rc <- dt$lgl_result != dt$lgl_comments
  comp_vc <- dt$lgl_comments != dt$lgl_actual_value
  if (any(comp_rv, na.rm = TRUE) | any(comp_rc, na.rm = TRUE) | any(comp_vc, na.rm = TRUE)) {
    rlang::warn("Logical MRD results extracted from text did not match across variables for one or more observations")
  }
  dt[, "lgl_result" := data.table::fcoalesce(lgl_result, lgl_actual_value, lgl_comments)]
  dt[, c("lgl_actual_value", "lgl_comments") := NULL]

  # Extract CD Groups

  # Standardize numbers in text columns
  chr_cols <- setdiff(chr_cols, c("cat_source", "cat_method"))
  # Remove commas
  dt[, c(chr_cols) := lapply(.SD, function(x) {
    stringr::str_remove_all(x, "(?<=[0-9]),+(?=[0-9])")
  }), .SDcols = chr_cols]
  # Standardize decimals
  dt[, c(chr_cols) := lapply(.SD, function(x) {
    x %>%
      # Handle multiple consecutive decimals
      stringr::str_replace_all("(?<=[0-9])[.]{2,}", ".") %>%
      stringr::str_replace_all("[.]{2,}(?=[0-9])", ".") %>%
      # Handle leading decimals
      stringr::str_replace_all("(?<![A-Z0-9])[.](?=[0-9])", "0.") %>%
      # Handle trailing decimals
      stringr::str_remove_all("(?<=[0-9])[.](?=$|\\s|%|^|E-?[0-9]|[<>]|/)") %>%
      # Handle multiple leading zeros
      stringr::str_replace_all("(?<![A-Z0-9])(?:0+[.]){2,}", "0.") %>%
      # Handle multiple trailing zeros
      stringr::str_remove_all("(?<=[.][0-9]{1,10})[.]0+(?=$|\\s|%|^|E-?[0-9]|[<>]|/)") %>%
      # Handle multiple decimals inside non-zero values
      stringr::str_remove_all("(?<=[0-9]{1,10}[.][0-9]{1,10})[.]")
  }), .SDcols = chr_cols]
  # Standardize range expressions
  dt[, c(chr_cols) := lapply(.SD, function(x) {
    x %>%
      # <=
      stringr::str_replace_all("(?<=\\b|\\s)L[.]?T[.]?E[.]?\\s?(?=[0-9])", "<=") %>%
      stringr::str_replace_all("(?<=[0-9])\\s?L[.]?T[.]?E[.]?(?=\\s|\\b)", "<=") %>%
      stringr::str_replace_all("(?<=\\b|\\s)(?:IS )?(L[.]?T[.]?|LESS|LESS THAN) (?:OR )?EQ(?:UAL)?(?: TO)?\\s?(?=[0-9])", "<=") %>%
      stringr::str_replace_all("(?<=[0-9])\\s?(?:IS )?(L[.]?T[.]?|LESS|LESS THAN) (?:OR )?EQ(?:UAL)?(?: TO)?(?=\\b|\\s)", "<=") %>%
      # >=
      stringr::str_replace_all("(?<=\\b|\\s)G[.]?T[.]?E[.]?\\s?(?=[0-9])", ">=") %>%
      stringr::str_replace_all("(?<=[0-9])\\s?G[.]?T[.]?E[.]?(?=\\s|\\b)", ">=") %>%
      stringr::str_replace_all("(?<=\\b|\\s)(?:IS )?(G[.]?T[.]?|GREATER|GREATER THAN) (?:OR )?EQ(?:UAL)?(?: TO)?\\s?(?=[0-9])", ">=") %>%
      stringr::str_replace_all("(?<=[0-9])\\s?(?:IS )?(G[.]?T[.]?|GREATER|GREATER THAN) (?:OR )?EQ(?:UAL)?(?: TO)?(?=\\b|\\s)", ">=") %>%
      # <
      stringr::str_replace_all("(?<=\\b|\\s)L[.]?T[.]?\\s?(?=[0-9])", "<") %>%
      stringr::str_replace_all("(?<=[0-9])\\s?L[.]?T[.]?(?=\\s|\\b)", "<") %>%
      stringr::str_replace_all("(?<=\\b|\\s)(?:IS )?(L[.]?T[.]?|LESS|LESS THAN)\\s?(?=[0-9])", "<") %>%
      stringr::str_replace_all("(?<=[0-9])\\s?(?:IS )?(L[.]?T[.]?|LESS|LESS THAN)(?=\\b|\\s)", "<") %>%
      # >
      stringr::str_replace_all("(?<=\\b|\\s)G[.]?T[.]?\\s?(?=[0-9])", ">") %>%
      stringr::str_replace_all("(?<=[0-9])\\s?G[.]?T[.]?(?=\\s|\\b)", ">") %>%
      stringr::str_replace_all("(?<=\\b|\\s)(?:IS )?(G[.]?T[.]?|GREATER|GREATER THAN)\\s?(?=[0-9])", ">") %>%
      stringr::str_replace_all("(?<=[0-9])\\s?(?:IS )?(G[.]?T[.]?|GREATER|GREATER THAN)(?=\\b|\\s)", ">") %>%
      # Remove spaces between symbols
      stringr::str_remove_all("(?<=[<>=])\\s+(?=[<>=])") %>%
      # Replace repeats and incorrect ordering of symbols
      stringr::str_replace_all("<{2,}", "<") %>%
      stringr::str_replace_all(">{2,}", ">") %>%
      stringr::str_replace_all("={2,}", "=") %>%
      stringr::str_replace_all("(?:<=|=<)+", "<=") %>%
      stringr::str_replace_all("(?:>=|=>)+", ">=") %>%
      # Ensure symbols are on correct side
      stringr::str_replace_all("([0-9.]+(?:[Ee]-?[0-9.]+)?)\\s?<=", ">=\\1") %>%
      stringr::str_replace_all("([0-9.]+(?:[Ee]-?[0-9.]+)?)\\s?>=", "<=\\1") %>%
      stringr::str_replace_all("([0-9.]+(?:[Ee]-?[0-9.]+)?)\\s?<", ">\\1") %>%
      stringr::str_replace_all("([0-9.]+(?:[Ee]-?[0-9.]+)?)\\s?>", "<\\1")
  }), .SDcols = chr_cols]

  # Extract numeric values for MRD result
  num_cols <- c("pct_result", "num_actual_value")
  dt[, c(num_cols, "num_comments") := lapply(.SD, function(x) {
    x %>%
      # Screen out CD group percentages
      stringr::str_replace(".*CD\\s?[0-9].*", NA_character_) %>%
      # Extract numeric values w/ symbols
      stringr::str_extract("(?:[0-9]+(?:[.,/<>=^E]|-)*[0-9]*|(?:[.,/<>=^E]|-)*[0-9]+)+") %>%
      # Convert mis-represented Excel dates back to numeric
      utils_mrd$str_excel_dt_to_num() %>%
      # Convert scientific notation to decimal
      utils_mrd$str_sci_to_dec() %>%
      # Convert fraction to decimal
      utils_mrd$str_frac_to_dec() %>%
      # Convert remaining dates to missing
      utils_mrd$str_dt_na() %>%
      utils_mrd$str_to_range_list()
  }), .SDcols = c(num_cols, "txt_comments")]

  # Convert all values to common denominator
  num <- c("pct_result", "num_actual_value", "num_comments")
  units <- c("result_units", "value_units", "comment_units")
  for (i in seq_along(num)) {
    n <- num[[i]]
    u <- units[[i]]
    i1 <- which(!(is.na(dt[[n]]) | is.na(dt[[u]])))
    dt1 <- dt[i1]
    data.table::set(
      dt,
      i = i1,
      j = n,
      value = purrr::map2(dt1[[n]], dt1[[u]], utils_mrd$convert_range_units)
    )
    i2 <- which(!(is.na(dt[[n]]) | is.na(dt$cat_units)) & is.na(dt[[u]]))
    dt2 <- dt[i2]
    data.table::set(
      dt,
      i = i2,
      j = c(n, u),
      value = list(
        purrr::map2(dt2[[n]], dt2$cat_units, utils_mrd$convert_range_units),
        dt2$cat_units
      )
    )
  }
  # Combine columns
  dt[!(is.na(pct_result) | is.na(result_units)), "cat_units" := result_units]
  dt[is.na(pct_result), c("pct_result", "cat_units") := list(num_actual_value, value_units)]
  dt[is.na(pct_result), c("pct_result", "cat_units") := list(num_comments, comment_units)]
  # Remove now-redundant columns
  num <- setdiff(num, "pct_result")
  dt[, c(num, units) := NULL]

  # Update logical columns to be consistent with numeric values
  new_lgl <- purrr::map_lgl(dt$pct_result, function(x) {
    if (length(x) == 0L) return(NA)
    if (length(x) > 1L && is.na(x[[1]]) && !is.na(x[[2]])) return(FALSE)
    x[[1]] > 0
  })
  dt[, "new_lgl" := ..new_lgl][!is.na(new_lgl), "lgl_result" := new_lgl][, "new_lgl" := NULL]

  # Convert result back to original units, if given
  dt[!(is.na(cat_units) | is.na(pct_result)), "pct_result" := purrr::map2(pct_result, cat_units, function(x, u) {
    l <- length(x)
    if (l == 0L) return(x)
    if (l == 1L) {
      return(switch(
        u,
        "MILLION" = x * 1e4,
        "NORMALIZED" = x * 1e-2,
        x
      ))
    }
    x[1:2] <- switch(
      u,
      "MILLION" = x[1:2] * 1e4,
      "NORMALIZED" = x[1:2] * 1e-2,
      x[1:2]
    )
    x
  })]

  # Convert results to string
  dt[, "pct_result" := purrr::map_chr(pct_result, function(x) {
    if (length(x) < 2L) return(as.character(x))
    if (x[[3L]] == 0) {
      lb <- "("
    } else if (x[[3L]] == 1) {
      lb <- "["
    } else {
      lb <- ""
    }
    if (x[[4L]] == 0) {
      ub <- ")"
    } else if (x[[4L]] == 1) {
      ub <- "]"
    } else {
      ub <- ""
    }
    paste0(lb, x[[1L]], ", ", x[[2L]], ub)
  })]

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Ensure timestamp is retained
  attr(dt, "timestamp") <- attr(dm_local$mrd, "timestamp")

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("mrd") %>%
    dm::dm_add_tbl(mrd = dt[])
}

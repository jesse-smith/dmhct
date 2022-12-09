#' Extract MRD Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to SQL Server w/ HCT data
#'
#' @return `[dm]` The `dm` with instructions for updating the `mrd` table
#'
#' @export
dm_mrd_extract <- function(dm_remote) {
  dm_remote %>%
    dm::dm_zoom_to("mrd") %>%
    dplyr::transmute(
      entity_id = as.integer(.data$EntityID),
      date = dbplyr::sql("CONVERT(DATETIME, [MRD_DAT])"),
      cat_source = trimws(as.character(.data[["MRD Source"]])),
      cat_method = trimws(as.character(.data[["MRD Test"]])),
      pct_result = trimws(as.character(.data[["MRD Result"]])),
      num_actual_value = trimws(as.character(.data[["Actual Value"]])),
      cat_units = toupper(trimws(as.character(.data$Units))),
      cat_cd_group = trimws(as.character(.data[["CD Group"]])),
      pct_cd_group = trimws(as.character(.data[["CD Group%"]])),
      mcat_genomic_abnormality = trimws(as.character(.data[["Genomic Abnormality"]])),
      txt_comments = toupper(trimws(as.character(.data$Comments)))
      # NOTES ON EXCLUDED COLUMNS
      #   Date of Transplant: Redundant - present in master
      #   Transplant_type_2: Redundant - present in master
    ) %>%
    dm::dm_update_zoomed()
}


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
  dt[, "date" := utils_date$std_date(date, force = "dt")]

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


#' Transform the MRD Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#'
#' @return `[dm]` The `dm` object w/ transformed `mrd` table
#'
#' @export
dm_mrd_transform <- function(dm_local) {
  dt <- dm_local$mrd
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD CHECK Notes
  cat_source <- cat_method <- cat_source1 <- cat_method1 <- cat_units <- NULL
  num_actual_value <- pct_result <- num_denom <- chr_comments <- NULL
  lgl_result <- date <- cat_dx_grp <- cat_method_explicit <- NULL

  # Dates
  dt[, "date" := lubridate::as_date(date)]

  # Filter unused patients and patients that should not have MRD w/ master
  dt_master <- data.table::as.data.table(dm_local$master)
  data.table::set(
    dt_master,
    j = setdiff(colnames(dt_master), c("entity_id", "cat_dx_grp")),
    value = NULL
  )
  # Remove patients not in master
  dt <- dt[dt_master, on = "entity_id", nomatch = NULL]
  # Remove patients that should not have MRD
  dt <- dt[cat_dx_grp %in% c("ALL", "AML", "Other Leukemias", "Myelodysplastic Syndrome", NA_character_)]
  dt[, "cat_dx_grp" := NULL]
  rm(dt_master)

  # Source
  dt[, "cat_source" := data.table::fcase(
    utils_mrd$str_detect_fct(cat_source, 1L, "BONE\\s*MARROW"), "Bone Marrow",
    utils_mrd$str_detect_fct(cat_source, 2L, "PERIPHERAL\\s*BLOOD"), "Peripheral Blood",
    !is.na(cat_source), "Other",
    default = NA_character_
  )]
  dt[, "cat_source" := factor(cat_source, levels = c(
    "Bone Marrow", "Peripheral Blood", "Other"
  ))]

  # Test Method
  dt[, "cat_method" := data.table::fcase(
    utils_mrd$str_detect_fct(cat_method, 1L, "FLOW\\s*CYTOMETRY"), "Flow Cytometry",
    utils_mrd$str_detect_fct(cat_method, "[23]", "PCR"), "PCR",
    utils_mrd$str_detect_fct(cat_method, 4L, "FISH"), "FISH",
    utils_mrd$str_detect_fct(cat_method, 5L, "NGS"), "NGS",
    !is.na(cat_method), "Other",
    default = NA_character_
  )]
  dt[, "cat_method" := factor(cat_method, levels = c(
    "NGS", "PCR", "Flow Cytometry", "FISH", "Other"
  ))]

  # Units to denominator
  dt[, "num_denom" := data.table::fcase(
    cat_units == "%", 100,
    cat_units %like% "MILLION", 1e6,
    default = NA_real_
  )]
  dt[, "cat_units" := NULL]


  # MRD Results

  # Clean
  result_cols <- c("pct_result", "num_actual_value")
  dt[, c(result_cols) := lapply(.SD, stringr::str_squish),
     .SDcols = result_cols]
  dt[, c(result_cols) := lapply(.SD, stringr::str_remove_all, pattern = "[`,()%]"),
     .SDcols = result_cols]
  # Handle repeating decimals
  dt[, c(result_cols) := lapply(.SD, stringr::str_replace, pattern = "(0[.]){2,}", replacement = "0."),
     .SDcols  = result_cols]
  # Handle "^"
  dt[, c(result_cols) := lapply(.SD, stringr::str_remove, pattern = "\\^"),
     .SDcols = result_cols]
  # Handle Excel dates
  dt[, c(result_cols) := lapply(.SD, utils_mrd$str_excel_dt_to_num), .SDcols = result_cols]
  # Handle ranges w/ [=<>]
  dt[, c(result_cols) := lapply(.SD, function(x) data.table::fifelse(
    x %flike% "-",
    stringr::str_remove_all(x, "[=<>]"),
    x
  )), .SDcols = result_cols]

  # Handle sensitivity limits text
  dt[num_actual_value %flike% "NOTHING TO SUGGEST",
     "num_actual_value" := num_actual_value %>%
       stringr::str_extract("[0-9]+\\s*/\\s*[0-9]+") %>%
       stringr::str_split("\\s*/\\s*", simplify = TRUE) %>%
       as.numeric() %>%
       {exp(diff(log(.)))} %>%
       prod(100) %>%
       {paste0("<", as.character(.))}]

  # Create logical result
  dt[, "lgl_result" := data.table::fcase(
    pct_result == "POSITIVE", TRUE,
    pct_result == "NEGATIVE", FALSE,
    default = NA
  )]

  # Create numeric result
  dt[, "pct_result" := data.table::fcoalesce(
    utils_mrd$str_range_to_num(pct_result),
    utils_mrd$str_range_to_num(num_actual_value))]
  dt[, "num_actual_value" := NULL]

  # Infer denominator
  dt[is.na(num_denom), num_denom := data.table::fcase(
    cat_method == "NGS", 1e6,
    pct_result > 100, 1e6,
    default = 1e2
  )]

  # Extract lgl values from comments
  dt[chr_comments %flike% "BELOW LIMIT", "chr_comments" := "NEGATIVE"]
  dt[is.na(lgl_result), "lgl_result" := as.logical(data.table::fcase(
    chr_comments == "POSITIVE", TRUE,
    chr_comments == "NEGATIVE", FALSE,
    default = NA
  ))]
  dt[, "chr_comments" := NULL]

  # Standardize numeric result
  dt[, "pct_result" := 100 * pct_result / num_denom]
  dt[, "num_denom" := NULL]

  # Set unreasonable values to missing
  dt[pct_result > 1 & !lgl_result, "pct_result" := NA_real_]

  # Remove entirely missing results
  dt <- dt[!(is.na(pct_result) & is.na(lgl_result))]

  # Fill source where possible, assuming missings before the first PB sample are all BM
  dt[is.na(cat_source) & date < min(dt[cat_source == "Peripheral Blood"]$date),
     "cat_source" := factor("Bone Marrow", levels = levels(cat_source))]

  # Drop non-BM source
  dt <- dt[cat_source %in% c("Bone Marrow", NA_character_)]

  # Repeat once
  for (i in 1:2) {
    # Fill test where possible
    dt[is.na(cat_method), "cat_method" := data.table::fcase(
      !lgl_result & pct_result == 5e-5, factor("NGS", levels = levels(cat_method)),
      !lgl_result & pct_result == 5e-3, factor("PCR", levels = levels(cat_method)),
      !lgl_result & pct_result == 5e-2, factor("Flow Cytometry", levels = levels(cat_method)),
      lgl_result & pct_result < 5e-3, factor("NGS", levels = levels(cat_method))
    )]

    # Fill logical results where possible using lower limits of detection
    dt[is.na(lgl_result), "lgl_result" := data.table::fcase(
      cat_method == "NGS" & pct_result < 1e-4, FALSE,
      cat_method == "PCR" & pct_result < 0.01, FALSE,
      cat_method == "Flow Cytometry" & pct_result < 0.1, FALSE,
      cat_method == "FISH" & pct_result < 1, FALSE,
      is.na(cat_method) & pct_result < 0.01, FALSE,
      cat_method == "NGS" & pct_result >= 1e-4, TRUE,
      cat_method == "PCR" & pct_result >= 0.01, TRUE,
      cat_method == "Flow Cytometry" & pct_result >= 0.1, TRUE,
      cat_method == "FISH" & pct_result >= 1, TRUE,
      pct_result >= 1, TRUE
    )]

    # Fill numeric results where possible
    dt[is.na(pct_result) & !lgl_result, "pct_result" := data.table::fcase(
      cat_method == "NGS", 5e-5,
      cat_method == "PCR", 5e-3,
      cat_method == "Flow Cytometry", 5e-2,
      cat_method == "FISH", 8e-1,
      default = 5e-3
    )]
  }

  # Remove missing numeric results and drop logical result
  # dt <- dt[, "lgl_result" := NULL][!is.na(pct_result)]

  # Set primary key
  pk <- c("entity_id", "date")
  data.table::setkeyv(dt, pk)

  # Sort
  data.table::setorderv(dt, setdiff(colnames(dt), "lgl_result"), na.last = TRUE)

  # Choose best available test
  dt[, "cat_method_explicit" := forcats::fct_explicit_na(cat_method)]
  dt[, "cat_method1" := cat_method_explicit[[1L]], by = pk]
  dt <- dt[cat_method_explicit == cat_method1]
  dt[, c("cat_method_explicit", "cat_method1") := NULL]

  # Average within group
  dt <- dt[, list(
    cat_method = cat_method[[1L]],
    pct_result = mean(pct_result, na.rm = TRUE),
    lgl_result = if (all(is.na(lgl_result))) {
      NA
    } else if (data.table::uniqueN(lgl_result, na.rm = TRUE) == 1L) {
      lgl_result[!is.na(lgl_result)][[1L]]
    } else {
      NA
    }
  ), keyby = pk]

  # Add logical back where available
  dt[is.na(lgl_result), "lgl_result" := data.table::fcase(
    cat_method == "NGS" & pct_result < 1e-4, FALSE,
    cat_method == "PCR" & pct_result < 0.01, FALSE,
    cat_method == "Flow Cytometry" & pct_result < 0.1, FALSE,
    cat_method == "FISH" & pct_result < 1, FALSE,
    is.na(cat_method) & pct_result < 0.01, FALSE,
    cat_method == "NGS" & pct_result >= 1e-4, TRUE,
    cat_method == "PCR" & pct_result >= 0.01, TRUE,
    cat_method == "Flow Cytometry" & pct_result >= 0.1, TRUE,
    cat_method == "FISH" & pct_result >= 1, TRUE,
    pct_result >= 1, TRUE
  )]

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Ensure timestamp is retained
  attr(dt, "timestamp") <- attr(dm_local$mrd, "timestamp")

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("mrd") %>%
    dm::dm_add_tbl(mrd = dt[]) %>%
    # Add primary key
    dm::dm_add_pk("mrd", !!data.table::key(dt), check = TRUE)
}


#' Utility Functions for MRD Table ELT
#'
#' @description
#' Collection of utility functions for MRD data
#'
#' @aliases utils_mrd
#'
#' @keywords internal
UtilsMRD <- R6Class(
  "UtilsMRD",
  cloneable = FALSE,
  public = list(
    #' Detect Factor Level in Chimerism Data
    #'
    #' @param x `[chr]` A character vector
    #' @param lvl `[chr(1)]` The level's numeric representation
    #' @param lbl `[chr(1)]` The level's label
    #'
    #' @return `[lgl]` A logical indicating presence or absence of the indicated
    #'   factor level
    str_detect_fct = function(x, lvl, lbl) {
      stringr::str_detect(
        x,
        paste0("(?i)(^", lvl, "\\s*=)|(", lbl, ")")
      )
    },
    #' Convert a % Range to It's Midpoint
    #'
    #' @param x `[character]` Character representation of a range of percentages
    #'
    #' @return `[numeric]` The midpoint of each range of values
    str_range_to_num = function(x) {
      x %>%
        stringr::str_replace("LESS\\s*THAN", "<") %>%
        stringr::str_replace("GREATER\\s*THAN", ">") %>%
        stringr::str_replace("<=?", "0-") %>%
        stringr::str_replace(">=?", "100-") %>%
        stringr::str_split("(?<![0-9][^Ee])\\s*-\\s*") %>%
        {suppressWarnings(lapply(., as.numeric))} %>%
        vapply(function(num) mean(as.numeric(num)), double(1L))
    },
    #' Convert Excel Date to Number
    #'
    #' Converts Excel dates in MDY format to a number using an origin of
    #' "1/0/1900". Elements that cannot be converted are returned as-is.
    #'
    #' @param x `[chr]` Character vector containing Excel dates
    #'
    #' @return `[chr]` Numeric representation of Excel date, where available
    str_excel_dt_to_num = function(x) {
      dt_chr <- x %>%
        stringr::str_extract("([1-9]|1[012])\\s*/\\s*[0-9]+\\s*/\\s*[0-9]{4}") %>%
        stringr::str_remove_all("\\s")
      dt <- suppressWarnings(lubridate::mdy(dt_chr))
      suppressWarnings(data.table::fcase(
        is.na(dt_chr), x,
        !is.na(dt), as.character(as.numeric(dt - as.Date("1900-01-01")) + 1),
        dt_chr %like% "1/[0-9]+/1900", stringr::str_extract(dt_chr, "(?<=1/)[0-9]+(?=/1900)")
      ))
    },
    str_sci_to_dec = function(x) {
      # Standardize scientific notation
      x <- stringr::str_replace_all(x, "\\^", "E")
      # Find scientific notation matches
      m <- gregexpr("[0-9.]+E-?[0-9]+", x, perl = TRUE)
      v <- lapply(regmatches(x, m), function(x) {
        as.numeric(stringr::str_squish(x))
      })
      regmatches(x, m) <- v
      x[x == "NA"] <- NA_character_
      x
    },
    str_frac_to_dec = function(x) {
      # Find fractions
      m <- gregexpr("(?<!/)[0-9.]+/[0-9.]+(?!/)", x, perl = TRUE)
      v <- lapply(regmatches(x, m), function(x) {
        if (length(x) == 0L) return(x)
        f <- stringr::str_split(x, "/", simplify = TRUE)
        if (NCOL(f) == 1L) return(x)
        mode(f) <- "numeric"
        f[, 1L] / f[, 2L]
      })
      regmatches(x, m) <- v
      x[x == "NA"] <- NA_character_
      x
    },
    str_dt_na = function(x) {
      is_dt <- stringr::str_detect(x, "(?:[0-9]{1,2}[[:punct:]\\s\\b]+){2}(?:[0-9]{2}){1,2}")
      x[is_dt] <- NA_character_
      x
    },
    str_to_range_list = function(x) {
      # Extract numbers w/ range symbols
      n <- stringr::str_extract_all(x, "(?:[<>]=?)?[0-9.]+(?:[Ee]-?[0-9.]+)?")
      purrr::map_if(
        stringr::str_extract_all(x, "(?:[<>]=?)?[0-9.]+(?:[Ee]-?[0-9.]+)?"),
        .p = function(s) length(s) %in% 1:2,
        .f = function(s) {
          # Extract values and bound types
          v <- suppressWarnings(as.numeric(stringr::str_extract(s, "[^<>=\\s]+")))
          b <- stringr::str_extract(s, "[<>=]+")
          # Remove missing values
          not_na <- !is.na(v)
          v <- v[not_na]
          b <- b[not_na]
          # If no non-missing, return missing
          if (length(v) == 0L) return(NA_real_)
          if (length(v) == 1L) {
            if (is.na(b)) return(v)
            return(switch(
              b,
              ">=" = c(lv = v, uv = NA_real_, lb = 1, ub = 1),
              "<=" = c(lv = NA_real_, uv = v, lb = 1, ub = 1),
              ">" = c(lv = v, uv = NA_real_, lb = 0, ub = 1),
              "<" = c(lv = NA_real_, uv = v, lb = 1, ub = 0)
            ))
          }
          # Sort
          o <- order(v)
          v <- v[o]
          b <- b[o]
          # Convert bounds to double - assumes that bound attached to lv is lower, uv is upper
          b_dbl <- as.double(b %flike% "=")
          b[is.na(b)] <- NA_real_
          # Create return vector
          out <- c(v, b_dbl)
          names(out) <- c("lv", "uv", "lb", "ub")
          return(out)
        },
        .else = function(s) NA_real_
      )
    },
    convert_range_units = function(value, units) {
      l <- length(value)
      if (units %in% c(NA_character_, "%") || l == 0L || (l == 1L && is.na(value))) return(value)
      if (l == 1L) {
        return(switch(
          units,
          "MILLION" = value * 1e-4,
          "NORMALIZED" = value * 1e2,
          value
        ))
      }
      value[1:2] <- switch(
        units,
        "MILLION" = value[1:2] * 1e-4,
        "NORMALIZED" = value[1:2] * 1e2,
        value[1:2]
      )
      return(value)
    }
  )
)


#' @rdname UtilsMRD
#' @usage NULL
#' @format NULL
#' @keywords internal
utils_mrd <- UtilsMRD$new()

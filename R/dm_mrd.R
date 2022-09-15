#' Extract MRD Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to SQL Server w/ HCT data
#'
#' @return `[dm]` The `dm` with instructions for updating the `mrd` table
#'
#' @export
dm_mrd_extract <- function(dm_remote) {
  na <- c(
    "", "-9996", "-9999", "NA", "NA.", "NA\n", "N/A", "N/A\n", "N/A.", "NO DATA",
    "APL NOT FOLLOWED BY FLOW MRD", "IN CR. SPECIMEN BANKED", "INCONCLUSIVE",
    "NO PHENOTYPE", "NO SUITABLE PHENOTYPE", "NO VALUE GIVEN",
    "NO VALUE TO REPORT", "NOT AVAILABLE"
  )
  na_src <- c(na, "1/1/1900 0:00")

  dm_remote %>%
    dm::dm_zoom_to("mrd") %>%
    dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
    dplyr::semi_join("master", by = "entity_id") %>%
    dplyr::transmute(
      .data$entity_id,
      dt_trans = dbplyr::sql("CONVERT(DATETIME, [Date of Transplant])"),
      date = dbplyr::sql("CONVERT(DATETIME, [MRD_DAT])"),
      cat_source = toupper(trimws(as.character(.data[["MRD Source"]]))),
      cat_source = dplyr::if_else(
        .data$cat_source %in% {{ na_src }}, NA_character_, .data$cat_source
      ),
      cat_method = toupper(trimws(as.character(.data[["MRD Test"]]))),
      cat_method = dplyr::if_else(
        .data$cat_method %in% {{ na }},
        NA_character_,
        .data$cat_method
      ),
      pct_result = toupper(trimws(as.character(.data[["MRD Result"]]))),
      pct_result = dplyr::if_else(
        .data$pct_result %in% {{ na }},
        NA_character_,
        .data$pct_result
      ),
      num_actual_value = toupper(trimws(as.character(.data[["Actual Value"]]))),
      num_actual_value = dplyr::if_else(
        .data$num_actual_value %in% {{ na }},
        NA_character_,
        .data$num_actual_value
      ),
      cat_units = toupper(trimws(as.character(.data$Units))),
      cat_units = dplyr::if_else(
        .data$cat_units %in% {{ na }},
        NA_character_,
        .data$cat_units
      ),
      chr_comments = toupper(trimws(as.character(.data$Comments))),
      chr_comments = dplyr::if_else(
        .data$chr_comments %in% {{ na }},
        NA_character_,
        .data$chr_comments
      )
    ) %>%
    dplyr::filter(
      !is.na(.data$entity_id),
      !is.na(.data$dt_trans),
      !is.na(.data$date),
      !(is.na(.data$pct_result) & is.na(.data$num_actual_value) & is.na(.data$chr_comments))
    ) %>%
    dm::dm_update_zoomed()
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
  dt_cols <- c("dt_trans", "date")
  dt[, c(dt_cols) := lapply(.SD, lubridate::as_date), .SDcols = dt_cols]

  # Filter unused patients and patients that should not have MRD w/ master
  dt_master <- data.table::as.data.table(dm_local$master)
  data.table::set(
    dt_master,
    j = setdiff(colnames(dt_master), c("entity_id", "dt_trans", "cat_dx_grp")),
    value = NULL
  )
  # Remove patients not in master
  dt <- dt[dt_master, on = c("entity_id", "dt_trans"), nomatch = NULL]
  # Remove patients that should not have MRD
  dt <- dt[cat_dx_grp %in% c("ALL", "AML", "Other Leukemias", "Myelodysplastic Syndrome", NA_character_)]
  dt[, "cat_dx_grp" := NULL]
  rm(dt_master)

  # Remove transplant date
  dt[, "dt_trans" := NULL]

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
    }
  )
)


#' @rdname UtilsMRD
#' @usage NULL
#' @format NULL
#' @keywords internal
utils_mrd <- UtilsMRD$new()

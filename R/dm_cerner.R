#' Unite All Cerner Tables and Remove Individual Tables
#'
#' @param dm_remote `[dm]` Remote `dm` connected to an SQL Server w/ HLA data
#' @param path_var `[chr(1)]` Path to CSV file mapping test names to
#'   standardized variable names
#' @param quiet Should the function return quietly if a Cerner table already
#'   exists?
#'
#' @return The updated `dm`
dm_cerner_extract <- function(
    dm_remote,
    path_var = system.file("extdata", "cerner_var_map.csv", package = "dmhct"),
    quiet = TRUE
) {
  if ("cerner" %in% names(dm_remote)) {
    if (!quiet) rlang::inform("`cerner` table already created")
    return(dm_remote)
  }
  # Select cerner tables
  dm_cerner <- dm::dm_select_tbl(dm_remote, dplyr::matches("(?i)cerner"))
  # Create list of names for removal
  nm_list   <- as.list(names(dm_cerner))
  # Row bind to single table
  tbl_cerner <- purrr::reduce(
    dm_cerner[-1L],
    ~ dplyr::union_all(.x, std_cerner_tbl(.y)),
    .init = std_cerner_tbl(dm_cerner[[1L]])
  )

  dm_remote %>%
    # Add combined table
    dm::dm_add_tbl(cerner = tbl_cerner) %>%
    # Remove individual tables
    dm::dm_rm_tbl(!!!nm_list)
}


#' Transform an Extracted Cerner Table in a Local `dm`
#'
#' @param dm_local `[dm]` A `dm` with Cerner (EHR) HLA data
#' @param numeric_only `[logical(1)]` Should non-numeric data be dropped during
#'   transformation?
#' @param path `[character(1)]` Path to CSV file mapping test names to
#'   standardized variable names
#'
#' @export
dm_cerner_transform <- function(
    dm_local,
    numeric_only = TRUE,
    path = system.file("extdata", "cerner_var_map.csv", package = "dmhct")
) {
  dt <- dm_local$cerner
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Filter unused patients w/ master
  dt <- dt[entity_id %in% dm_local$master$entity_id]


  # Silence R CMD CHECK Notes
  var <- test <- result <- ..pos <- ..neg <- NULL

  # Load variable map
  var_map <- path %>%
    path_create() %>%
    data.table::fread(data.table = TRUE) %>%
    dplyr::distinct(.data$test, .keep_all = TRUE)

  # Standardize test names with variable map
  dt <- var_map[dt, on = "test"][, "test" := var][, "var" := NULL]

  # Remove non-numeric columns if numeric only is TRUE
  if (numeric_only) dt <- dt[test %flike% "num_" | test %flike% "pct_"]

  # Squish result strings
  dt[result %flike% " ", "result" := stringr::str_squish(result)]

  # Extract numbers from numeric variable results
  dt[
    test %flike% "num_" | test %flike% "pct_",
    "result" := data.table::fifelse(
      # Attempt numeric extraction for strings starting with number or "<>"
      result %like% "^[0-9.<>]",
      # Extract numbers + decimals
      stringr::str_extract(result, "([0-9]+)|([0-9]+?[.][0-9]+)"),
      NA_character_
    )
  ]

  # Threshold-based outlier removal
  dt[
    test == "num_ht",
    "result" := data.table::fifelse(as.numeric(result) %between% c(24, 272), result, NA_character_)
  ][
    test == "num_wt",
    "result" := data.table::fifelse(as.numeric(result) %between% c(1, 442), result, NA_character_)
  ][
    test == "num_bmi",
    "result" := data.table::fifelse(as.numeric(result) %between% c(10, 60), result, NA_character_)
  ][
    test == "num_temp",
    "result" := data.table::fifelse(as.numeric(result) <= 46, result, NA_character_)
  ][
    test == "num_sbp",
    "result" := data.table::fifelse(as.numeric(result) %between% c(75, 175), result, NA_character_)
  ][
    test == "num_dbp",
    "result" := data.table::fifelse(as.numeric(result) %between% c(30, 100), result, NA_character_)
  ][
    test == "num_rr",
    "result" := data.table::fifelse(as.numeric(result) %between% c(0, 75), result, NA_character_)
  ][
    test == "num_hr",
    "result" := data.table::fifelse(as.numeric(result) %between% c(0, 250), result, NA_character_)
  ][
    test == "pct_o2_sat",
    "result" := data.table::fifelse(as.numeric(result) %between% c(90, 100), result, NA_character_)
  ][
    test == "num_map",
    "result" := data.table::fifelse(as.numeric(result) <= 120, result, NA_character_)
  ][
    test == "num_mpv",
    "result" := data.table::fifelse(as.numeric(result) %between% c(4.5, 13.5), result, NA_character_)
  ][
    test == "num_mch",
    "result" := data.table::fifelse(as.numeric(result) <= 40, result, NA_character_)
  ][
    test == "num_rbc",
    "result" := data.table::fifelse(as.numeric(result) <= 5.5, result, NA_character_)
  ][
    test == "num_mchc",
    "result" := data.table::fifelse(as.numeric(result) %between% c(30, 40), result, NA_character_)
  ]

  # Adenovirus infection
  pos <- c("DETECTED", "Detected", "3.92", "4.07", "4.37", "4.71")
  neg <- c("NOT DET", "Not Detected", "<3.47")
  dt[
    test == "lgl_pcr_adenovirus",
    "result" := data.table::fcase(
      result %chin% ..pos, "TRUE",
      result %chin% ..neg, "FALSE"
    )
  ]
  rm(pos, neg)

  # Aspergillus infection
  neg <- c("Negative", "NEG")
  dt[test == "lgl_antigen_aspergillus",
     "result" := data.table::fcase(
       result == "Positive", "TRUE",
       result %chin% ..neg,   "FALSE"
     )]
  rm(neg)

  # Pertussis infection
  dt[test == "lgl_pcr_b_pertussis",
     "result" := data.table::fifelse(
       result == "Detected",
       "TRUE",
       "FALSE"
     )]

  # HCV antibody
  pos <- c("Reactive", "REACTIVE")
  neg <- c("NON REAC", "Non Reactive", "Non-reactive", "Negative")
  dt[test == "lgl_anti_hcv",
     "result" := data.table::fcase(
       result %chin% pos, "TRUE",
       result %chin% neg, "FALSE"
     )]
  rm(pos, neg)

  # Hep C
  neg <- c("Negative", "Nonreactive")
  dt[
    test == "lgl_anti_hcv",
    "result" := data.table::fcase(
      result == "Reactive", "TRUE",
      result %chin% ..neg, "FALSE"
    )
  ]

  # Flu A
  pos <- c("Flu A H3", "Flu A", "Flu A H1-2009")
  dt[test == "lgl_pcr_flu_a",
     "result" := data.table::fcase(
       result %chin% pos,         "TRUE",
       result == "Not Detected", "FALSE"
     )]
  rm(pos)

  # Parainfluenza
  dt[test == "lgl_pcr_flu_a",
     "result" := data.table::fifelse(
       result == "Not Detected",
       "FALSE",
       "TRUE"
     )]

  # Pregnant
  neg <- c("Negative", "NEGATIVE")
  dt[test == "lgl_pregnancy",
     "result" := data.table::fcase(
       result == "Positive", "TRUE",
       result %chin% neg,   "FALSE"
     )]
  rm(neg)

  # Rectal surveillance
  pos <- c("Positive", "Detected")
  neg <- c("Inhibited", "Inhibition", "Negative", "Not Detected")
  dt[test == "lgl_rectal_culture",
     "result" := data.table::fcase(
       result %chin% ..pos, "TRUE",
       result %chin% ..neg, "FALSE"
     )]
  rm(pos, neg)

  # Categorical urine variables
  u_vars <- paste0(
    "cat_u_",
    c("bili", "ketones", "blood", "prot", "no2", "leuko", "glucose", "urobili")
  )
  dt[
    test %chin% u_vars,
    "result" := data.table::fcase(
      result %chin% c("NEG", "NEGATIVE", "Negative"), "0",
      result %chin% c("1+", "POS 1+", "1.0"), "1+",
      result %chin% c("POSITIVE", "Positive", "TRACE", "Trace"), "1+",
      result %chin% c("2+", "POS 2+", "0.2", "2.0"), "2+",
      result %chin% c("3+", "POS 3+", "3"), "3+",
      result %chin% c("4+", "POS 4+", "4.0"), "4+"
    )
  ]

  # Boolean variables w/ no parsing needed
  bool_vars <- paste0(
    "lgl_pcr_",
    c("b_pertussis", "hmpv", "rhino_enterovirus", "flu_b")
  )
  dt[
    test %chin% bool_vars,
    "result" := data.table::fifelse(result == "Detected", "TRUE", "FALSE")
  ]

  # Set keys
  pk <- c("entity_id", "test", "date")

  # Set order
  data.table::setorderv(dt)

  # Remove duplicates before date conversion
  dt <- dt[!duplicated(dt)]

  # Convert datetime to date
  dt[, "date" := lubridate::as_date(date)]

  # Remove missings
  dt <- dt[!(is.na(test) | is.na(result))]

  # Give non-unique observations a unique ID
  dt[, "n_obs" := seq_len(.N), by = pk]

  # Add n_obs to keys
  pk <- c(pk, "n_obs")

  # Set key and reorder cols
  data.table::setkeyv(dt, cols = pk)
  data.table::setcolorder(dt)

  # Convert back to tibble if input was not data.table
  dt <- dt_cast(dt, to = class)

  dm_local %>%
    dm::dm_rm_tbl("cerner") %>%
    dm::dm_add_tbl(cerner = dt)
}


#' Standardize a Cerner Table
#'
#' Selects needed columns from a Cerner table and converts them to the expected
#' class.
#'
#' @param tbl `[tbl_dbi]` A table containing Cerner data
#'
#' @return A `tbl_dbi` with standardized columns
#'
#' @keywords internal
std_cerner_tbl <- function(tbl) {
  # Get column names
  cols <- colnames(tbl)
  # Select and rename needed columns
  EntityId <- stringr::str_subset(cols, "(?i)entity")
  Date     <- stringr::str_subset(cols, "(?i)date")
  Test     <- stringr::str_subset(cols, "(?i)test")
  Result   <- stringr::str_subset(cols, "(?i)result")

  # Update tbl
  tbl %>%
    dplyr::select(
      entity_id = {{ EntityId }},
      date      = {{ Date }},
      test      = {{ Test }},
      result    = {{ Result }}
    ) %>%
    # Ensure columns are of expected type
    dplyr::mutate(
      entity_id = as.integer(.data$entity_id),
      date = dbplyr::sql("CONVERT(DATETIME, date)"),
      test = trimws(as.character(.data$test)),
      result = trimws(as.character(.data$result))
    ) %>%
    # Filter
    dplyr::filter(
      !is.na(.data$entity_id),
      !is.na(.data$date),
      !is.na(.data$test),
      !is.na(.data$result)
    )
}

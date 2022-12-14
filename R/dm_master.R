#' Transform the Master Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` w/ HLA data
#'
#' @return `[dm]` The `dm` object w/ transformed `master` table
#'
#' @export
dm_master_transform <- function(dm_local) {
  dt <- dm_local$master
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD CHECK Notes
  ..hla_cols <- ..mm_cols <- ..time_stamp <- cat_disease_status_at_trans <- NULL
  cat_degree_match06 <- cat_degree_match08 <- cat_degree_match10 <- NULL
  cat_donor_race <- cat_donor_relation <- cat_donor_sex <- NULL
  cat_donor_type <- cat_dx_grp <- cat_ethnicity <- cat_prep_type <- NULL
  cat_product_type <- cat_race <- cat_sex <- dt_birth <- dt_death <- NULL
  dt_donor_birth <- dt_dx <- dt_trans <- lgl_death <- lgl_donor_related <- NULL
  is_mm <- is_mm06 <- is_mm08 <- is_mm10 <- lgl_malignant <- NULL
  lgl_survival <- num_age <- num_age_donor <- num_n_trans <- NULL
  num_degree_match <- N <- NULL
  num_degree_match06 <- num_degree_match08 <- num_degree_match10 <- NULL

  # Convert datetimes to dates
  dt_cols <- colnames(dt)[vapply(dt, lubridate::is.POSIXt, logical(1L))]
  dt[, c(dt_cols) := lapply(.SD, lubridate::as_date), .SDcols = dt_cols]

  # Age
  dt[, "num_age" := lubridate::interval(dt_birth, dt_trans) / lubridate::years(1L)]

  # Death
  dt[, "lgl_death" := lgl_survival != "Alive"]
  dt[, "lgl_survival" := NULL]

  # Survival time - endpoint comes from timestamp of upload in SQL Server
  time_stamp <- lubridate::as_date(attr(dm_local$master, "timestamp"))
  dt[, "num_t_surv" := as.numeric(data.table::fifelse(
    !lgl_death,
    ..time_stamp - dt_trans,
    dt_death - dt_trans
  ))]

  # Transplant time
  dt[, "num_t_trans" := as.numeric(dt_trans - dt_dx)]

  # Transplant date
  dt[, "num_dt_trans" := lubridate::decimal_date(dt_trans)]

  # Malignant
  dt[, "lgl_malignant" := lgl_malignant == "Yes"]

  # Sex
  dt[, "cat_sex" := factor(cat_sex)]

  # Race
  dt[, "cat_race" := stringi::stri_trans_general(cat_race, "any-latin;latin-ascii")]
  dt[, "cat_race" := factor(data.table::fcase(
    cat_race %ilike% "black" & !cat_race %ilike% "white", "Black",
    cat_race %ilike% "white" & !cat_race %ilike% "black", "White",
    nchar(cat_race) > 0L, "Other"
  ), levels = c("Black", "White", "Other"))]

  # Ethnicity
  dt[cat_ethnicity %ilike% "unknown", "cat_ethnicity" := NA_character_]
  dt[, "cat_ethnicity" := factor(data.table::fifelse(
    stringr::str_detect(cat_ethnicity, "\\b(Non|Not)\\b"),
    "Not Hispanic or Latino",
    "Hispanic or Latino"
  ))]

  # Diagnosis group
  dt[, "cat_dx_grp" := factor(data.table::fcase(
    cat_dx_grp %ilike% "tumor", "Solid Tumor",
    cat_dx_grp %ilike% "liver", "Solid Tumor",
    cat_dx_grp %ilike% "melanoma", "Solid Tumor",
    cat_dx_grp %ilike% "blastoma", "Solid Tumor",
    cat_dx_grp %ilike% "sarcoma", "Solid Tumor",
    cat_dx_grp == "Other Malignancy", "Solid Tumor",
    cat_dx_grp %in% c("Hodgkin's Disease", "NHL"), "Lymphoma",
    cat_dx_grp %in% c("Non-malignancy", "Blood Disorder"), "Other Non-Malignancy",
    cat_dx_grp %in% c("Histiocytosis", "Osteopetrosis"), "Other Non-Malignancy",
    cat_dx_grp %in% c("SCID", "Metabolic/Storage Disease"), "Other Non-Malignancy",
    cat_dx_grp == "Other Genetic Diseases", "Other Non-Malignancy",
    cat_dx_grp == "Other Immunodeficiency", "Other Non-Malignancy",
    rep(TRUE, NROW(cat_dx_grp)), cat_dx_grp
  ))]

  # Product type
  dt[, "cat_product_type" := factor(data.table::fcase(
    cat_product_type %ilike% "marrow" & cat_product_type %ilike% "apheresis", "Marrow and PBSC",
    cat_product_type %ilike% "marrow" & cat_product_type %ilike% "cord", "Marrow and Cord Blood",
    cat_product_type %ilike% "apheresis" & cat_product_type %ilike% "cord", "PBSC and Cord Blood",
    cat_product_type %ilike% "marrow", "Marrow",
    cat_product_type %ilike% "apheresis", "PBSC",
    cat_product_type %ilike% "cord", "Cord Blood"
  ))]

  # Prep type
  dt[, "cat_prep_type" := factor(
    dplyr::na_if(cat_prep_type, ""),
    levels = c("Myeloablative", "Reduced Intensity", "Non-Myeloablative"),
    ordered = TRUE
  )]

  # Disease status
  dt[, "cat_disease_status_at_trans" := cat_disease_status_at_trans %>%
       {factor(data.table::fcase(
         . %like% "(?i)CR|Remission|Quiesc|(No Evidence)", "Remission",
         . %in% c("PR", "VGPR", "No Evidence of Disease"), "Remission",
         . %like% "(?i)relapse|refr", "Relapse/Refractory",
         . %in% "N/A", NA_character_,
         nchar(.) > 0L, "Active"
       ))}]

  # Donor related
  dt[, "lgl_donor_related" := tolower(cat_donor_relation) != "unrelated"]

  # Donor sex
  dt[, "cat_donor_sex" := factor(dplyr::na_if(cat_donor_sex, "Unknown"))]

  # Donor race
  dt[, "cat_donor_race" := stringi::stri_trans_general(cat_donor_race, "any-latin;latin-ascii")]
  dt[, "cat_donor_race" := factor(data.table::fcase(
    cat_donor_race %ilike% "black" & !cat_donor_race %ilike% "white", "Black",
    cat_donor_race %ilike% "white" & !cat_donor_race %ilike% "black", "White",
    nchar(cat_donor_race) > 0L, "Other"
  ), levels = c("Black", "White", "Other"))]

  # Donor age
  dt[, num_age_donor := lubridate::interval(dt_donor_birth, dt_trans) / lubridate::years(1L)]
  dt[num_age_donor < 0, "num_age_donor" := NA_real_]

  # Degree of match
  dt[, "num_degree_match06" := utils_master$convert_degree_match(num_degree_match, denom =  6L)]
  dt[, "num_degree_match08" := utils_master$convert_degree_match(num_degree_match, denom =  8L)]
  dt[, "num_degree_match10" := utils_master$convert_degree_match(num_degree_match, denom = 10L)]
  dt[, "num_degree_match" := NULL]

  # Add from HLA typing
  if ("n06" %in% colnames(dm_local$hla)) {
    dt_hla <- data.table::as.data.table(dm_local$hla)
  } else {
    dt_hla <- data.table::as.data.table(dm_hla_transform(dm_local)$hla)
  }
  dt_hla <- dt_hla[, c("entity_id", "donor_id", "n06", "n08", "n10")]
  dt <- dt_hla[dt, on = c("entity_id", "donor_id")]
  dg_cols  <- sort(stringr::str_subset(colnames(dt), "num_degree_match[0-9]+"))
  hla_cols <- sort(stringr::str_subset(colnames(dt), "n[0-9]+"))
  for (i in seq_along(dg_cols)) {
    dg_col <- dg_cols[[i]]
    hla_col <- hla_cols[[i]]
    data.table::set(
      dt,
      j = dg_col,
      value = data.table::fcase(
        is.na(dt[[hla_col]]), dt[[dg_col]],
        dt[[dg_col]] > dt[[hla_col]], dt[[dg_col]],
        rep(TRUE, NROW(dt)), dt[[hla_col]]
      )
    )
  }
  dt[, c(hla_cols) := rep(list(NULL), NROW(..hla_cols))]

  dt[num_degree_match06 > num_degree_match08, "num_degree_match08" := num_degree_match06]
  dt[num_degree_match06 > num_degree_match10, "num_degree_match10" := num_degree_match06]
  dt[num_degree_match08 > num_degree_match10, "num_degree_match10" := num_degree_match08]
  related <- !dt$cat_donor_relation %in% c("Unrelated", NA_character_)
  full10 <- dt$num_degree_match10 %in% 10L
  full08 <- dt$num_degree_match08 %in% 8L | full10
  full06 <- dt$num_degree_match06 %in% 6L | full08
  dt[is.na(num_degree_match08) & full08, "num_degree_match08" := 8L]
  dt[is.na(num_degree_match06) & full06, "num_degree_match06" := 6L]
  rm(full06, full08, full10)

  # Convert to ordered factor
  dt[, "cat_degree_match06" := factor(num_degree_match06, ordered = TRUE)]
  dt[, "cat_degree_match08" := factor(num_degree_match08, ordered = TRUE)]
  dt[, "cat_degree_match10" := factor(num_degree_match10, ordered = TRUE)]
  dt[, c("num_degree_match06", "num_degree_match08", "num_degree_match10") := NULL]

  # Create related-matched cross
  # Categorical related
  dt[, "cat_matched_related" := forcats::fct_cross(
    factor(data.table::fifelse(cat_degree_match10 == "10", "matched", "mismatched")),
    factor(data.table::fifelse(lgl_donor_related, "related", "unrelated")),
    sep = "_"
  )]

  # Sort columns
  data.table::setcolorder(
    dt,
    unique(c("entity_id", "donor_id", sort(colnames(dt))))
  )

  # Remove duplicates
  dt <- unique(dt)

  # Remove duplicates w/ missings
  data.table::setorderv(dt, na.last = TRUE)
  dt[, c("N", ".ID") := list(.N, seq_len(.N)), keyby = c("entity_id", "donor_id", "num_n_trans")]
  dt_dedup <- dt[N > 1L] %>%
    data.table::setDF() %>%
    dplyr::group_by(.data$entity_id) %>%
    tidyr::fill(dplyr::everything(), .direction = "downup") %>%
    data.table::setDT() %>%
    unique(by = setdiff(colnames(dt), ".ID"))
  dt_dedup <- dt[N > 1L][dt_dedup[, c("entity_id", ".ID")], on = c("entity_id", ".ID")]
  dt <- rbind(dt[N == 1L], dt_dedup)
  dt[, c("N", ".ID") := NULL]

  # Initial cohort
  n_init_trans <- NROW(dt)
  n_init_pat <- data.table::uniqueN(dt$entity_id)
  message(cli::style_bold("Initial Cohort"))
  message("  ", cli::symbol$info, " ", n_init_trans, " transplants, ", n_init_pat, " patients")
  message("")
  # Only use 1st transplant
  dt <- dt[num_n_trans == 1L]
  n_first_trans <- NROW(dt)
  message(cli::style_bold("1st Transplant"))
  message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_init_trans - n_first_trans, " transplants, ", n_init_pat - n_first_trans, " patients removed")
  message("  ", cli::symbol$tick, " ", n_first_trans, " transplants/patients retained")
  message("")
  # Only use pedatric transplants
  dt <- dt[num_age < 21]
  n_ped_trans <- NROW(dt)
  message(cli::style_bold("Pediatric"))
  message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_first_trans - n_ped_trans, " transplants/patients removed")
  message("  ", cli::symbol$tick, " ", n_ped_trans, " transplants/patients retained")
  message("")
  # Only use marrow and PBSC transplants
  dt <- dt[cat_product_type %in% c("Marrow", "PBSC", NA_character_)]
  n_product_trans <- NROW(dt)
  message(cli::style_bold("BM/PBSC"))
  message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_ped_trans - n_product_trans, " transplants/patients")
  message("  ", cli::symbol$tick, " ", n_product_trans, " transplants/patients retained")
  message("")
  # Don't use solid tumor transplants
  dt <- dt[!cat_dx_grp %in% "Solid Tumor"]
  n_nonsolid_trans <- NROW(dt)
  message(cli::style_bold("Non-Solid Tumor"))
  message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_product_trans - n_nonsolid_trans, " transplants/patients")
  message("  ", cli::symbol$tick, " ", n_nonsolid_trans, " transplants/patients retained")
  message("")
  # Don't use mismatched donors
  dt[, "is_mm06" := tidyr::replace_na(cat_degree_match06 < 3L, FALSE)]
  dt[, "is_mm08" := tidyr::replace_na(cat_degree_match08 < 4L, FALSE)]
  dt[, "is_mm10" := tidyr::replace_na(cat_degree_match10 < 5L, FALSE)]
  dt[, "is_mm" := tidyr::replace_na(cat_donor_type %like% "^MM", FALSE)]
  dt[, "is_mm" := is_mm | is_mm06 | is_mm08 | is_mm10]
  dt <- dt[is_mm == FALSE]
  mm_cols <- stringr::str_subset(colnames(dt), "^is_mm")
  dt[, c(mm_cols) := rep(list(NULL), NROW(..mm_cols))]
  n_matched_trans <- NROW(dt)
  message(cli::style_bold("HLA Match >= 50%"))
  message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_nonsolid_trans - n_matched_trans, " transplants/patients")
  message("  ", cli::symbol$tick, " ", n_matched_trans, " transplants/patients retained")
  message("")
  # Final
  message(cli::style_bold("Final cohort"))
  message("  ", cli::symbol$info, " ", NROW(dt), " transplants/patients")

  # Remove unneeded variables
  rm_vars <- c(
    "dt_birth", "dt_donor_birth", "dt_death", "dt_dx", "cat_donor_type"
  )
  dt[, (rm_vars) := rep(list(NULL), NROW(rm_vars))]

  # Remove duplicates w/ missings
  data.table::setorderv(dt, na.last = TRUE)
  dt[, c("N", ".ID") := list(.N, seq_len(.N)), keyby = "entity_id"]
  dt_dedup <- dt[N > 1L] %>%
    data.table::setDF() %>%
    dplyr::group_by(.data$entity_id) %>%
    tidyr::fill(dplyr::everything(), .direction = "downup") %>%
    data.table::setDT() %>%
    unique(by = setdiff(colnames(dt), ".ID"))
  dt_dedup <- dt[N > 1L][dt_dedup[, c("entity_id", ".ID")], on = c("entity_id", ".ID")]
  dt <- rbind(dt[N == 1L], dt_dedup)
  dt[, c("N", ".ID") := NULL]

  # Re-sort rows
  data.table::setorderv(dt)

  # Add primary key
  data.table::setkeyv(dt, "entity_id")

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Ensure timestamp is retained
  attr(dt, "timestamp") <- attr(dm_local$master, "timestamp")

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("master") %>%
    dm::dm_add_tbl(master = dt[]) %>%
    # Add primary key
    dm::dm_add_pk("master", !!data.table::key(dt), check = TRUE)
}


#' Utility Functions for Master Table ELT
#'
#' @description
#' Collection of utility functions for master data
#'
#' @aliases utils_master
#'
#' @keywords internal
UtilsMaster <- R6Class(
  "UtilsMaster",
  public = list(
    #' Convert Degree of Match `character` Data to Count w/ Specified Denominator
    #'
    #' @param x `[chr]` Vector of degree of match counts
    #' @param denom `[int(1)]` Denominator of counts to extract
    #'
    #' @return `[int]` An `integer` vector of counts
    #'
    #' @keywords internal
    convert_degree_match = function(x, denom = 6L) {
      # Extract fractions as list
      x_list <- stringr::str_extract_all(x, "[0-9]+/[0-9]+")
      # Split into denominator and numerator
      x_split <- list()
      x_split$n <- purrr::map(
        x_list, ~ as.integer(stringr::str_extract(.x, "^[0-9]+"))
      )
      x_split$d <- purrr::map(
        x_list, ~ as.integer(stringr::str_extract(.x, "[0-9]+$"))
      )

      # Get n w/ denominator >= denom parameter
      n <- purrr::map2(
        x_split$n,
        x_split$d,
        ~ .x[.y >= denom] / .y[.y >= denom]
      ) %>%
        # Full match means n == denom
        purrr::map_int(~ (if (any(.x == 1, na.rm = TRUE)) denom else NA_integer_))

      # Not full match means extract numerator where denominator == denom param
      n_exact <- purrr::map2(
        x_split$n,
        x_split$d,
        ~ suppressWarnings(as.integer(max(.x[.y == denom], na.rm = TRUE)))
      ) %>%
        purrr::map_int(~ (if (NROW(.x) == 0L) NA_integer_ else .x))

      # Fill in exact where n is missing
      n_is_na <- is.na(n)
      n[n_is_na] <- n_exact[n_is_na]

      # Return n
      n
    }
  )
)

#' @rdname UtilsMaster
#' @usage NULL
#' @format NULL
#' @keywords internal
utils_master <- UtilsMaster$new()

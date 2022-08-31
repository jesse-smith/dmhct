#' Extract Master Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to a SQL Server w/ HLA data
#'
#' @return The `dm` with instructions to update the `master` table
#'
#' @export
dm_master_extract <- function(dm_remote) {
  dm_remote %>%
    dm::dm_zoom_to("master") %>%
    dplyr::transmute(
      entity_id = as.integer(.data$EntityID),
      donor_id = as.integer(.data$DonorID),
      num_n_trans = as.integer(.data[["Transplant Number"]]),
      dt_birth = dbplyr::sql("CONVERT(DATETIME, DOB)"),
      dt_dx = dbplyr::sql("CONVERT(DATETIME, Diagnosis_Date)"),
      dt_trans = dbplyr::sql("CONVERT(DATETIME, [Date of Transplant])"),
      dt_death = dbplyr::sql("CONVERT(DATETIME, DeathDate)"),
      dt_donor_birth = dbplyr::sql("CONVERT(DATETIME, DonorDateofBirth)"),
      # protocol = trimws(as.character(.data$Protocol)),
      lgl_survival = trimws(as.character(.data$SurvialStatus)),
      lgl_malignant = trimws(as.character(.data$Mailgnant)),
      num_degree_match = trimws(as.character(.data[["Degree of Match"]])),
      cat_sex = trimws(as.character(.data$Sex)),
      cat_race = trimws(as.character(.data$Race)),
      cat_ethnicity = trimws(as.character(.data$Ethnicity)),
      # cat_dx = trimws(as.character(.data$Diagnosis)),
      cat_dx_grp = trimws(as.character(.data[["Diagnosis Group"]])),
      cat_product_type = trimws(as.character(.data[["Product Type"]])),
      cat_prep_type = trimws(as.character(.data[["If yes, preparative regimen type:"]])),
      cat_disease_status_at_trans = trimws(as.character(.data[["Disease Status at Transplant"]])),
      cat_donor_type = trimws(as.character(.data$Transplant_type_2)),
      cat_donor_relation = trimws(as.character(.data[["Donor Type"]])),
      cat_donor_sex = trimws(as.character(.data$Donor_Gender)),
      cat_donor_race = trimws(as.character(.data$donor_race))
    ) %>%
    dm::dm_update_zoomed()
}


#' Transform the Master Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` with HLA data
#'
#' @return The transformed `dm`
#'
#' @export
dm_master_transform <- function(dm_local) {
  dt <- dm_local$master
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD CHECK Notes
  ..hla_cols <- ..mm_cols <- cat_disease_status_at_trans <- NULL
  cat_degree_match06 <- cat_degree_match08 <- cat_degree_match10 <- NULL
  cat_donor_race <- cat_donor_relation <- cat_donor_sex <- NULL
  cat_donor_type <- cat_dx_grp <- cat_ethnicity <- cat_prep_type <- NULL
  cat_product_type <- cat_race <- cat_sex <- dt_birth <- dt_death <- NULL
  dt_donor_birth <- dt_dx <- dt_trans <- lgl_death <- lgl_donor_related <- NULL
  is_mm <- is_mm06 <- is_mm08 <- is_mm10 <- lgl_malignant <- NULL
  lgl_survival <- num_age <- num_age_donor <- num_n_trans <- NULL
  num_degree_match <- NULL
  num_degree_match06 <- num_degree_match08 <- num_degree_match10 <- NULL

  # Convert datetimes to dates
  dt_cols <- colnames(dt)[vapply(dt, lubridate::is.POSIXct, logical(1L))]
  dt[, c(dt_cols) := lapply(.SD, lubridate::as_date), .SDcols = dt_cols]

  # Age
  dt[, "num_age" := lubridate::interval(dt_birth, dt_trans) / lubridate::years(1L)]

  # Death
  dt[, "lgl_death" := lgl_survival != "Alive"]
  dt[, "lgl_survival" := NULL]

  # Survival time
  dt[, "num_t_surv" := as.numeric(data.table::fifelse(
    !lgl_death,
    as.Date("2020-12-31") - dt_trans,
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
  dt[, num_age_donor := as.numeric(dt_trans - dt_donor_birth)]
  dt[num_age_donor < 0, "num_age_donor" := NA_real_]

  # Degree of match
  dt[, "num_degree_match06" := convert_degree_match(num_degree_match, denom =  6L)]
  dt[, "num_degree_match08" := convert_degree_match(num_degree_match, denom =  8L)]
  dt[, "num_degree_match10" := convert_degree_match(num_degree_match, denom = 10L)]
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
  # related <- !dt$cat_donor_relation %in% c("Unrelated", NA_character_)
  # full06 <- dt$num_degree_match06 %in% 6L
  # full08 <- dt$num_degree_match08 %in% 8L
  # full10 <- dt$num_degree_match10 %in% 10L
  # any_full_related <- (full06 | full08 | full10) & related
  # dt[is.na(num_degree_match06) & any_full_related, "num_degree_match06" :=  6L]
  # dt[is.na(num_degree_match08) & any_full_related, "num_degree_match08" :=  8L]
  # dt[is.na(num_degree_match10) & any_full_related, "num_degree_match10" := 10L]
  # rm(related, full06, full08, full10, any_full_related)

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
  dt <- dt[!duplicated(dt)]

  # Only use 1st transplant and kids
  dt <- dt[num_n_trans == 1L & num_age < 21]
  # Only use marrow and PBSC transplants
  dt <- dt[cat_product_type %in% c("Marrow", "PBSC", NA_character_)]
  # Don't use mismatched donors
  data.table::set(dt, j = "is_mm06", value = dt$cat_degree_match06 < 3L)
  data.table::set(dt, i = is.na(dt$is_mm06), j = "is_mm06", value = FALSE)
  dt[, "is_mm06" := cat_degree_match06 < 3L]
  dt[, "is_mm08" := cat_degree_match08 < 4L]
  dt[, "is_mm10" := cat_degree_match10 < 5L]
  dt[, "is_mm" := cat_donor_type %like% "^MM"]
  dt[is.na(is_mm06), "is_mm06" := FALSE]
  dt[is.na(is_mm08), "is_mm08" := FALSE]
  dt[is.na(is_mm10), "is_mm10" := FALSE]
  dt[is.na(is_mm), "is_mm" := FALSE]
  dt[, "is_mm" := is_mm | is_mm06 | is_mm08 | is_mm10]
  dt <- dt[is_mm == FALSE]
  mm_cols <- stringr::str_subset(colnames(dt), "^is_mm")
  dt[, c(mm_cols) := rep(list(NULL), NROW(..mm_cols))]
  # Don't use solid tumor transplants
  dt <- dt[!cat_dx_grp %in% "Solid Tumor"]

  # Remove unneeded variables
  rm_vars <- c(
    "dt_birth", "dt_donor_birth", "dt_death", "dt_dx", "cat_donor_type"
  )
  dt[, (rm_vars) := rep(list(NULL), NROW(rm_vars))]

  # Add primary key
  data.table::setkeyv(dt, "entity_id")

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("master") %>%
    dm::dm_add_tbl(master = dt)
}


#' Convert Degree of Match `character` Data to Count with Specified Denominator
#'
#' @param x `[character]` Vector of degree of match counts
#' @param denom `[integer(1)]` Denominator of counts to extract
#'
#' @return An `integer` vector of counts
#'
#' @keywords internal
convert_degree_match <- function(x, denom = 6L) {
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

#' Transform Tables to Analysis-Friendly Format
#'
#' A previous version of the dmhct pipeline performed all transformations of
#' tables simultaneously; to ensure backwards compatibility, this behavior has
#' been retained in `dm_transform()`. However, it is strongly recommended that
#' new code not use `dm_transform()` and instead use the updated pipeline.
#'
#' @param dm_local `[dm]` A local `dm` object from `dm_extract()`
#' @param reset `[lgl(1)]` Should the cache be reset to the current results,
#'   even if inputs have not changed? This is useful if data processing logic
#'   has changed, but the underlying data have not.
#'
#' @return `[dm]` The updated `dm` object
#'
#' @export
dm_transform <- function(dm_local = dm_extract_legacy(), reset = FALSE) {
  lifecycle::deprecate_soft("1.0.0", "dm_transform()")
  dm_transform_legacy(dm_local = dm_local, reset = reset)
}

dm_transform_legacy <- function(dm_local = dm_extract_legacy(), reset = FALSE) {
  # Check arguments
  stopifnot(dm::is_dm(dm_local))
  checkmate::assert_logical(reset, any.missing = FALSE, len = 1L)

  cache_file <- "dm_transform"
  checksum <- DMCache$checksum(dm_local)

  # Cache
  if (!reset) {
    no_change <- DMCache$check(checksum, cache_file)
    if (no_change) return(DMCache$read("data", cache_file))
  }

  dm_local <- dm_local %>%
    # HLA must come before master
    DMTransformLegacy$hla() %>%
    # Master comes next to use in filter joins
    DMTransformLegacy$master() %>%
    # Rest follow in alphabetical order
    DMTransformLegacy$cerner() %>%
    DMTransformLegacy$chimerism() %>%
    DMTransformLegacy$death() %>%
    DMTransformLegacy$disease_status() %>%
    DMTransformLegacy$engraftment() %>%
    DMTransformLegacy$gvhd() %>%
    DMTransformLegacy$mrd() %>%
    DMTransformLegacy$relapse()

  # Cache
  if (reset) DMCache$write(dm_local, checksum, cache_file)

  dm_local
}

DMTransformLegacy <- R6Class(
  "DMTransformLegacy",
  portable = FALSE,
  cloneable = FALSE,
  lock_objects = TRUE,
  lock_class = TRUE,
  public = list(


    cerner = function(
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
      entity_id <- var <- test <- result <- ..pos <- ..neg <- NULL

      # Load variable map
      var_map <- path %>%
        path_create() %>%
        data.table::fread(data.table = TRUE) %>%
        dplyr::distinct(.data$test, .keep_all = TRUE)

      # Standardize test names w/ variable map
      dt <- var_map[dt, on = "test"][, "test" := var][, "var" := NULL]

      # Remove non-numeric columns if numeric only is TRUE
      if (numeric_only) dt <- dt[test %flike% "num_" | test %flike% "pct_"]

      # Squish result strings
      dt[result %flike% " ", "result" := stringr::str_squish(result)]

      # Extract numbers from numeric variable results
      dt[
        test %flike% "num_" | test %flike% "pct_",
        "result" := data.table::fifelse(
          # Attempt numeric extraction for strings starting w/ number or "<>"
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

      # Ensure timestamp is retained
      attr(dt, "timestamp") <- attr(dm_local$cerner, "timestamp")

      dm <- dm_local %>%
        dm::dm_rm_tbl("cerner") %>%
        dm::dm_add_tbl(cerner = dt[]) %>%
        # Add primary key
        dm::dm_add_pk("cerner", !!data.table::key(dt), check = TRUE)
    },


    chimerism = function(dm_local) {
      rlang::check_installed(c("forcats", "lme4", "stats"))
      dt <- dm_local$chimerism
      class <- df_class(dt)
      dt <- data.table::as.data.table(dt)


      # Silence R CMD CHECK Notes
      cat_source <- cat_method <- pct_donor <- pct_host <- NULL
      pct_donor_tmp <- pct_host_tmp <- dt_trans <- date <- NULL
      delta_donor <- delta_host <- ..pred_donor <- ..pred_host <- NULL
      dist_keep <- dist_swap <- tmp_donor <- cat_method1 <- NULL
      cat_method_explicit <- NULL

      # Dates
      dt_cols <- c("dt_trans", "date")
      dt[, c(dt_cols) := lapply(.SD, lubridate::as_date), .SDcols = dt_cols]

      # Filter unused patients w/ master
      dt_master <- data.table::as.data.table(dm_local$master)
      data.table::set(
        dt_master,
        j = setdiff(colnames(dt_master), c("entity_id", "dt_trans")),
        value = NULL
      )
      dt <- dt[dt_master, on = c("entity_id", "dt_trans"), nomatch = NULL]
      rm(dt_master)

      # Cell Source (ignore all but BM and PB for now per discussion w/ Akshay)
      dt[, "cat_source" := data.table::fcase(
        private$chimerism_str_detect_fct(cat_source, 1L, "bone\\s+marrow"), "Bone Marrow",
        private$chimerism_str_detect_fct(cat_source, 2L, "peripheral\\s+blood"), "Peripheral Blood",
        private$chimerism_str_detect_fct(cat_source, 11L, "unsorted"), "Peripheral Blood",
        # private$chimerism_str_detect_fct(cat_source, 3L, "t\\s*-?\\s*cell"), "T Cells",
        # private$chimerism_str_detect_fct(cat_source, 4L, "b\\s*-?\\s*cell"), "B Cells",
        # private$chimerism_str_detect_fct(cat_source, 6L, "monocyte"), "Monocytes",
        # private$chimerism_str_detect_fct(cat_source, 7L, "neutrophil"), "Neutrophils",
        # private$chimerism_str_detect_fct(cat_source, 9L, "nk\\s*-?\\s*cell"), "NK Cells",
        # private$chimerism_str_detect_fct(cat_source, 10L, "myeloid"), "Myeloid Cells",
        # !is.na(cat_source), "Other",
        default = NA_character_
      )]
      dt[, "cat_source" := factor(cat_source, levels = c(
        "Bone Marrow",
        "Peripheral Blood"
        # "T Cells"
        # "B Cells",
        # "NK Cells",
        # "Monocytes",
        # "Neutrophils",
        # "Myeloid Cells",
        # "Other"
      ))]

      # Chimerism Method
      dt[, "cat_method" := data.table::fcase(
        private$chimerism_str_detect_fct(cat_method, 1L, "standard\\s+cytogenetics"), "Standard Cytogenetics",
        private$chimerism_str_detect_fct(cat_method, 2L, "FISH"), "FISH",
        private$chimerism_str_detect_fct(cat_method, 3L, "RFLP"), "RFLP",
        private$chimerism_str_detect_fct(cat_method, 4L, "PCR"), "PCR",
        private$chimerism_str_detect_fct(cat_method, 5L, "hla\\s+serotyping"), "HLA Serotyping",
        private$chimerism_str_detect_fct(cat_method, 6L, "VNTR"), "VNTR",
        !is.na(cat_method), "Other",
        default = NA_character_
      )]
      # Order levels from most to least preferred method
      dt[, "cat_method" := factor(cat_method, levels = c(
        "VNTR",
        "RFLP",
        "PCR",
        "FISH",
        "HLA Serotyping",
        "Standard Cytogenetics",
        "Other"
      ))]

      # Percentages
      # Clean
      dt[, "pct_donor" := stringr::str_remove_all(pct_donor, "[`()%]")]
      dt[, "pct_donor" := stringr::str_squish(pct_donor)]
      dt[, "pct_host" := stringr::str_remove_all(pct_host, "[`()%]")]
      dt[, "pct_host" := stringr::str_remove(pct_host, "(?i)[0-9]+\\s+donor\\s+[0-9]*\\s*,\\s*")]
      dt[, "pct_host" := stringr::str_remove(pct_host, "(?i)\\s*patient\\s*")]
      dt[, "pct_host" := stringr::str_squish(pct_host)]

      # Fill missings using complements where available
      dt[, "pct_donor_tmp" := suppressWarnings(as.numeric(pct_donor))]
      dt[, "pct_host_tmp" := suppressWarnings(as.numeric(pct_host))]
      dt[pct_donor_tmp < 0 | 100 < pct_donor_tmp, c("pct_donor", "pct_donor_tmp") := list(NA_character_, NA_real_)]
      dt[pct_host_tmp < 0 | 100 < pct_host_tmp, c("pct_host", "pct_host_tmp") := list(NA_character_, NA_real_)]
      dt[is.na(pct_donor_tmp) & !is.na(pct_host_tmp), pct_donor_tmp := 100 - pct_host_tmp]
      dt[is.na(pct_host_tmp) & !is.na(pct_donor_tmp), pct_host_tmp := 100 - pct_donor_tmp]

      # Handle Excel dates
      dt[is.na(pct_donor_tmp),
         "pct_donor_tmp" := suppressWarnings(as.numeric(private$chimerism_str_excel_dt_to_num(pct_donor)))]
      dt[is.na(pct_host_tmp),
         "pct_host_tmp" := suppressWarnings(as.numeric(private$chimerism_str_excel_dt_to_num(pct_host)))]

      # Get middle of ranges
      dt[is.na(pct_donor_tmp), "pct_donor_tmp" := private$chimerism_str_range_to_num(pct_donor)]
      dt[is.na(pct_host_tmp), "pct_host_tmp" := private$chimerism_str_range_to_num(pct_host)]

      # Fill using extracted complements
      dt[is.na(pct_donor_tmp) & !is.na(pct_host_tmp), pct_donor_tmp := 100 - pct_host_tmp]
      dt[is.na(pct_host_tmp) & !is.na(pct_donor_tmp), pct_host_tmp := 100 - pct_donor_tmp]

      # Overwrite originals
      dt[, c("pct_donor", "pct_host") := list(pct_donor_tmp, pct_host_tmp)]
      dt[, c("pct_donor_tmp", "pct_host_tmp") := NULL]

      # Drop missing measurements
      dt <- dt[!is.na(pct_donor)]

      # Sort
      data.table::setorderv(dt, na.last = TRUE)

      # Set primary key
      pk <- c("entity_id", "date", "cat_source")
      data.table::setkeyv(dt, pk)

      # Handle mirrored duplicates in pct_donor and pct_host -----------------------

      # Calculate largest differences within patient + date + cell type groupings
      dt[, c("delta_donor", "delta_host") := list(
        apply(abs(outer(pct_donor, pct_donor, `-`)), 1L, max),
        apply(abs(outer(pct_host, pct_host, `-`)), 1L, max)
      ), by = pk]

      # Tidy factors for modeling
      dt[, c("cat_source_lf", "cat_method_lf") := lapply(.SD, function(x) {
        forcats::fct_lump_min(forcats::fct_explicit_na(x), min = 100L)
      }), .SDcols = c("cat_source", "cat_method")]
      # Add normalized predictors for modeling
      dt[, c("t_trans", "t") := list(
        private$chimerism_normalize(dt_trans),
        private$chimerism_normalize(date - dt_trans)
      )]

      # Convert to data.frame for lme4
      data.table::setDF(dt)

      # Model selection by BIC
      # Fit all combinations of entity_id & cat_source effects
      # (fixed, random intercept, random slope, random slope + intercept for each)
      # [entity_id: random slope + intercept, cat_source: fixed] was best model

      # Predict donor
      pred_donor <- private$chimerism_inv_probit(stats::predict(lme4::lmer(
        private$chimerism_probit(pct_donor) ~ t_trans + cat_source_lf + cat_method_lf + (t | entity_id),
        data = dt[dt$delta_donor < 5,]
      ), newdata = dt[dt$delta_donor >= 5,]))
      # Predict host
      pred_host <- private$chimerism_inv_probit(stats::predict(lme4::lmer(
        private$chimerism_probit(pct_host) ~ t_trans + cat_source_lf + cat_method_lf + (t | entity_id),
        data = dt[dt$delta_host < 5,]
      ), newdata = dt[dt$delta_host >= 5,]))

      # Convert back to data.table
      data.table::setDT(dt)

      # Prediction differences
      dt[, c("dist_keep", "dist_swap") := 0]
      dt[delta_donor >= 5, c("dist_keep", "dist_swap") := list(
        abs(..pred_donor - pct_donor),
        abs(..pred_donor - pct_host)
      )]
      dt[delta_host >= 5, c("dist_keep", "dist_swap") := list(
        dist_keep + abs(..pred_host - pct_host),
        dist_swap + abs(..pred_host - pct_donor)
      )]

      # Swap if doing so lowers average distance between observation and prediction
      dt[, "tmp_donor" := pct_donor]
      dt[dist_swap < dist_keep, c("pct_donor", "pct_host") := list(pct_host, tmp_donor)]

      # Remove modeling variables
      dt[, c("delta_donor", "delta_host", "cat_source_lf", "cat_method_lf") := NULL]
      dt[, c("t", "t_trans", "dist_keep", "dist_swap", "tmp_donor") := NULL]

      # ----------------------------------------------------------------------------

      # Remove missing cell type
      dt <- dt[!is.na(cat_source)]

      # Prefer highest ranked method available in group (ranked in factor definition)
      dt[, "cat_method_explicit" := forcats::fct_explicit_na(cat_method)]
      dt[, "cat_method1" := cat_method_explicit[[1L]], by = pk]
      dt <- dt[cat_method_explicit == cat_method1]
      dt[, c("cat_method_explicit", "cat_method1") := NULL]

      # Get average within group
      dt <- dt[, list(
        cat_method = cat_method[[1L]],
        pct_donor = mean(c(pct_donor, 100 - pct_host))
      ), by = pk]

      # Logic (derived from Akshay's response on 2022-09-02 and call on 2022-09-08)
      # - Bone marrow and peripheral blood are separate X
      # - Unsorted and peripheral blood are the same X
      # - T Cell is separate; drop remaining specific cell lineages X
      # - VNTR preferred over PCR preferred over FISH X
      # - Std. Cyto., HLA Sero., and FISH are similar
      # - RFLP is similar to VNTR

      # Reset key
      data.table::setkeyv(dt, pk)

      # Convert back to original class
      dt <- dt_cast(dt, to = class)

      # Ensure timestamp is retained
      attr(dt, "timestamp") <- attr(dm_local$chimerism, "timestamp")

      # Add to dm
      dm_local %>%
        dm::dm_rm_tbl("chimerism") %>%
        dm::dm_add_tbl(chimerism = dt[]) %>%
        # Add primary key
        dm::dm_add_pk("chimerism", !!data.table::key(dt), check = TRUE)
    },


    death = function(dm_local) {
      rlang::check_installed("tidyr")
      dt <- dm_local$death
      class <- df_class(dt)
      dt <- data.table::as.data.table(dt)

      # Silence R CMD CHECK Notes
      cat_cod_contrib <- cat_cod_contrib1 <- cat_cod_contrib2 <- NULL
      cat_cod_primary <- cat_cod_primary1 <- cat_cod_primary2 <- NULL
      cat_death_class <- cat_death_location <- dt_death <- dt_trans <- NULL
      entity_id <- lgl_on_therapy <- NULL

      # Filter unused patients w/ master
      dt <- dt[entity_id %in% dm_local$master$entity_id]

      # Convert datetimes to dates
      dt[, c("dt_death", "dt_trans") := list(
        lubridate::as_date(dt_death),
        lubridate::as_date(dt_trans)
      )]

      # Convert logical
      dt[, "lgl_on_therapy" := data.table::fcase(
        lgl_on_therapy %ilike% "Off", FALSE,
        lgl_on_therapy %ilike% "On", TRUE
      )]

      # Death location
      dt[, "cat_death_location" := factor(data.table::fcase(
        cat_death_location %ilike% "affiliate", "Affiliate",
        cat_death_location %ilike% "home", "Home",
        cat_death_location %ilike% "other", "Other",
        cat_death_location %ilike% "SJCRH", "SJCRH",
        cat_death_location %ilike% "st. jude", "SJCRH"
      ))]
      # Death class
      dt[cat_death_class == "", "cat_death_class" := NA_character_
      ][,"cat_death_class" := factor(cat_death_class)]
      # Coalesce cause of death vars w/ details
      # Primary
      dt[
        cat_cod_primary1 %like% "^Other cause" | is.na(cat_cod_primary1),
        cat_cod_primary1 := cat_cod_primary2
      ][, "cat_cod_primary2" := NULL]
      # Contributing
      dt[
        cat_cod_contrib1 %like% "^Other cause" | is.na(cat_cod_contrib1),
        cat_cod_contrib1 := cat_cod_contrib2
      ][, "cat_cod_contrib2" := NULL]
      # Rename
      data.table::setnames(
        dt,
        c("cat_cod_primary1", "cat_cod_contrib1"),
        c("cat_cod_primary",  "cat_cod_contrib")
      )
      # Combine categories
      # Primary
      dt[, "cat_cod_primary" := factor(data.table::fcase(
        cat_cod_primary %like% "(?i)graft|gvhd", "GVHD",
        cat_cod_primary %ilike% "progressive", "Disease",
        cat_cod_primary %ilike% "recurrence", "Disease",
        cat_cod_primary %ilike% "persistence", "Disease",
        cat_cod_primary %ilike% "refractory", "Disease",
        cat_cod_primary %ilike% "relapse", "Disease",
        cat_cod_primary %ilike% "leukemia", "Disease",
        cat_cod_primary %ilike% "unknown", NA_character_,
        cat_cod_primary %ilike% "n/a", NA_character_,
        nchar(cat_cod_primary) > 0L, "Other"
      ))]
      # Contributing
      dt[, "cat_cod_contrib" := factor(data.table::fcase(
        cat_cod_contrib %like% "(?i)graft|gvhd", "GVHD",
        cat_cod_contrib %ilike% "progressive", "Disease",
        cat_cod_contrib %ilike% "recurrence", "Disease",
        cat_cod_contrib %ilike% "persistence", "Disease",
        cat_cod_contrib %ilike% "refractory", "Disease",
        cat_cod_contrib %ilike% "relapse", "Disease",
        cat_cod_contrib %ilike% "leukemia", "Disease",
        cat_cod_contrib %ilike% "unknown", NA_character_,
        cat_cod_contrib %ilike% "n/a", NA_character_,
        nchar(cat_cod_contrib) > 0L, "Other"
      ))]
      # Combine primary and contributing
      dt[, "cat_death_cause" := factor(data.table::fcase(
        any(cat_cod_primary == "GVHD" | cat_cod_contrib == "GVHD", na.rm = TRUE), "GVHD",
        any(cat_cod_primary == "Disease" | cat_cod_contrib == "Disease", na.rm = TRUE), "Disease",
        any(cat_cod_primary == "Other" | cat_cod_contrib == "Other", na.rm = TRUE), "Other"
      ), levels = c("Disease", "GVHD", "Other")), by = "entity_id"]

      # Order rows
      data.table::setorderv(dt, cols = c("entity_id", "dt_death", "dt_trans"))

      # Fill missings in same group
      dt <- dt %>%
        dplyr::group_by(.data$entity_id) %>%
        tidyr::fill(-"entity_id", .direction = "downup") %>%
        dplyr::ungroup() %>%
        data.table::setDT()

      # Remove duplicates
      by <- which(
        !colnames(dt) %in% c("dt_trans", "cat_cod_primary", "cat_cod_contrib")
      )
      dt <- unique(dt, by = by)

      # Move cod cols to back
      data.table::setcolorder(
        dt,
        stringr::str_subset(colnames(dt), "cod", negate = TRUE)
      )

      # Set primary keys
      pk <- "entity_id"

      # Create unique ID for non-unique observations
      dt[, "n_id" := seq_len(.N), by = pk]

      # Add unique ID to pk
      pk <- c(pk, "n_id")

      # Move keys to front
      data.table::setcolorder(dt, pk)

      # Set data.table keys
      data.table::setkeyv(dt, pk)

      # Convert back to original class
      dt <- dt_cast(dt, to = class)

      # Ensure timestamp is retained
      attr(dt, "timestamp") <- attr(dm_local$death, "timestamp")

      # Add to dm
      dm_local %>%
        dm::dm_rm_tbl("death") %>%
        dm::dm_add_tbl(death = dt[]) %>%
        # Add primary key
        dm::dm_add_pk("death", !!data.table::key(dt), check = TRUE)
    },


    disease_status = function(dm_local) {
      dt <- dm_local$disease_status
      class <- df_class(dt)
      dt <- data.table::as.data.table(dt)

      # Silence R CMD CHECK Notes
      entity_id <- NULL

      # Filter unused patients w/ master
      dt <- dt[entity_id %in% dm_local$master$entity_id]

      # Convert datetime to date
      dt[, "date" := lubridate::as_date(date)]

      # Order rows
      data.table::setorderv(dt)

      # Set primary keys
      pk <- c("entity_id", "date")

      # Remove duplicates
      dt <- dt[!duplicated(dt)]

      # Create unique ID for non-unique observations
      dt[, "n_status" := seq_len(.N), by = pk]

      # Add unique ID to pk
      pk <- c(pk, "n_status")

      # Set data.table keys
      data.table::setkeyv(dt, pk)

      # Convert back to original class
      dt <- dt_cast(dt, to = class)

      # Ensure timestamp is retained
      attr(dt, "timestamp") <- attr(dm_local$disease_status, "timestamp")

      # Add to dm
      dm_local %>%
        dm::dm_rm_tbl("disease_status") %>%
        dm::dm_add_tbl(disease_status = dt[]) %>%
        # Add primary key
        dm::dm_add_pk("disease_status", !!data.table::key(dt), check = TRUE)
    },


    engraftment = function(dm_local) {
      dt <- dm_local$engraftment
      class <- df_class(dt)
      dt <- data.table::as.data.table(dt)

      # Silence R CMD CHECK Notes
      date <- cat_recovery_type <- lgl_recovery <- N <- NULL

      # Convert datetimes to dates
      dt[, "date" := lubridate::as_date(date)]

      # Convert recovery type
      dt[, "cat_recovery_type" := factor(data.table::fcase(
        cat_recovery_type %flike% "ANC", "recovery_anc500",
        cat_recovery_type %flike% "20", "recovery_plat20k",
        cat_recovery_type %flike% "50", "recovery_plat50k"
      ))]

      # Convert recovery status
      dt[, "lgl_recovery" := data.table::fcase(
        lgl_recovery %ilike% "^no", FALSE,
        lgl_recovery %ilike% "^yes", TRUE
      )]

      # Set order
      data.table::setorderv(dt)

      # Remove missings
      dt <- dt[!(is.na(date) | is.na(cat_recovery_type) | is.na(lgl_recovery))]

      # Remove duplicates
      dt <- dt[!duplicated(dt)]

      # Remove non-unique observations - contradicting recovery amounts to missing
      dt[, N := seq_len(.N), by = c("entity_id", "date", "cat_recovery_type")]
      dt <- dt[N == 1L][, "N" := NULL]

      # Cast recovery types to columns
      dt <- data.table::dcast(
        dt,
        entity_id + date ~ cat_recovery_type,
        value.var = "lgl_recovery"
      )

      # Create primary keys
      pk <- c("entity_id", "date")

      # Set data.table keys
      data.table::setkeyv(dt, pk)

      # Convert back to original class
      dt <- dt_cast(dt, to = class)

      # Ensure timestamp is retained
      attr(dt, "timestamp") <- attr(dm_local$engraftment, "timestamp")

      # Add to dm
      dm_local %>%
        dm::dm_rm_tbl("engraftment") %>%
        dm::dm_add_tbl(engraftment = dt[]) %>%
        # Add primary key
        dm::dm_add_pk("engraftment", !!data.table::key(dt), check = TRUE)
    },


    gvhd = function(dm_local) {
      dt <- dm_local$gvhd
      class <- df_class(dt)
      dt <- data.table::as.data.table(dt)

      # Silence R CMD CHECK Notes
      date <- dt_trans <- cat_grade <- cat_site <- cat_type <- NULL

      # Dates
      dt_cols <- c("dt_trans", "date")
      dt[, c(dt_cols) := lapply(.SD, lubridate::as_date), .SDcols = dt_cols]

      # Filter by time
      dt <- dt[as.numeric(date - dt_trans) >= 0]

      # Filter unused patients w/ master
      dt_master <- data.table::as.data.table(dm_local$master)
      data.table::set(
        dt_master,
        j = setdiff(colnames(dt_master), c("entity_id", "dt_trans")),
        value = NULL
      )
      dt <- dt[dt_master, on = c("entity_id", "dt_trans"), nomatch = NULL]
      rm(dt_master)

      # Remove transplant date
      dt[, "dt_trans" := NULL]

      # Grade
      dt[, "cat_grade" := as.integer(cat_grade)]
      dt[is.na(cat_grade), "cat_grade" := cat_site %>%
           stringr::str_extract("(?<=GRADE)\\s*[0-9]") %>%
           stringr::str_squish() %>%
           as.integer()]
      # Grade 0 means no GVHD
      dt <- dt[!cat_grade %in% 0L]
      dt[, "cat_grade" := ordered(cat_grade)]

      # Type
      dt[, "cat_site" := cat_site %>%
           stringr::str_replace("\\b(CUTE|AUTE|ACTE|ACUT)\\b", " ACUTE ") %>%
           stringr::str_replace("\\b(HRONIC|CRONIC|CHONIC|CHRNIC|CHROIC|CHRONC|CHRONI|CHRNC)\\b", " CHRONIC ") %>%
           stringr::str_squish()]
      dt[, "cat_type" := cat_site %>%
           stringr::str_extract("ACUTE|CHRONIC") %>%
           stringr::str_to_title() %>%
           factor()]

      # Site
      dt[, "cat_site" := cat_site %>%
           # Clean
           stringr::str_replace_all("[^A-Z0-9]", " ") %>%
           stringr::str_remove_all("GVHD|GRAFT\\s*VERSUS\\s*HOST\\s*DISEASE") %>%
           stringr::str_remove_all("GRADE\\s*[0-9]") %>%
           stringr::str_remove_all("\\b(ACUTE|CHRONIC|BOOP|WITH\\s*EOSINOPHILIA)\\b") %>%
           # Combine nomenclature
           stringr::str_replace_all("GASTROINTESTINAL", "GI") %>%
           stringr::str_replace_all("GI\\s*TRACT", "GI") %>%
           stringr::str_remove_all("(?<=EYE|JOINT|LUNG)S") %>%
           stringr::str_replace_all("MOUTH", "ORAL") %>%
           stringr::str_replace_all("EYE", "OCULAR") %>%
           stringr::str_remove_all("(?<=JOINT)\\s*(AND\\s*)?FASCIA") %>%
           stringr::str_squish()]
      dt[, "cat_site" := data.table::fcase(
        cat_site %in% c("SKIN", "NAILS", "HAIR", "ORAL", "OCULAR"), "Mucocutaneous",
        cat_site %like% "\\b(GI|RECTUM|GUT)\\b", "GI Tract",
        cat_site %in% c("JOINT", "CONNECTIVE TISSUE", "MYOSITIS", "MUSCULOSKELETAL"), "Musculoskeletal",
        cat_site %in% c("GENITOURINARY"), "Genitourinary",
        cat_site %in% c("", "NO SITE DETERMINED"), NA_character_,
        !is.na(cat_site), stringr::str_to_title(cat_site)
      )]

      # Chronic GVHD is not graded
      dt[!is.na(cat_grade) & cat_type == "Chronic",
         "cat_grade" := factor(NA_integer_, levels = levels(cat_grade))]
      # If missing type and grade is supplied, assume acute
      dt[is.na(cat_type) & !is.na(cat_grade),
         "cat_type" := factor("Acute", levels = levels(cat_type))]

      # Primary key
      pk <- c("entity_id", "date", "cat_site")

      # If multiple w/ same time, keep highest grade
      data.table::setorderv(dt, order = -1L, na.last = TRUE)
      dt <- unique(dt, by = c(pk, "cat_type"))

      # Set order
      data.table::setorderv(dt)

      # Set key
      data.table::setkeyv(dt, pk)

      # Convert back to original class
      dt <- dt_cast(dt, to = class)

      # Ensure timestamp is retained
      attr(dt, "timestamp") <- attr(dm_local$gvhd, "timestamp")

      # Add to dm
      dm_local %>%
        dm::dm_rm_tbl("gvhd") %>%
        dm::dm_add_tbl(gvhd = dt[]) %>%
        # Add primary key
        dm::dm_add_pk("gvhd", !!data.table::key(dt), check = TRUE)
    },


    hla = function(dm_local) {
      dt <- dm_local$hla
      class <- df_class(dt)
      dt <- data.table::as.data.table(dt)

      # Silence R CMD CHECK Notes
      allele_donor <- allele_entity <- donor_id <- entity_id <- gene <- n <- NULL

      # Clean `gene`
      dt[, "gene" := gene %>%
           stringr::str_remove_all("(?i)[^A-Z0-9 ]") %>%
           stringr::str_remove_all("(?i)HLA") %>%
           stringr::str_squish()]
      # Clean alleles
      dt[, c("allele_donor", "allele_entity") := list(
        private$hla_std_allele(allele_donor),
        private$hla_std_allele(allele_entity)
      )]

      # Remove rows with missing data and gene == "Bw"
      dt <- dt[rowSums(is.na(dt)) == 0L][gene != "Bw"]

      # Get all donor-entity pairs
      entity_donor <- dt[
        !duplicated(dt, by = c("entity_id", "donor_id")),
        list(entity_id, donor_id)
      ][!is.na(donor_id)]

      # Count matches
      dt <- dt[,
               list(n = sum(allele_entity == allele_donor)),
               by = c("entity_id", "donor_id", "gene")
      ][n > 2L, "n" := 2L]

      # Cast to wider format
      dt <- data.table::dcast(dt, entity_id + donor_id ~ gene, value.var = "n")

      # Add any missing pairs back
      dt <- data.table::merge.data.table(
        dt, entity_donor, by = c("entity_id", "donor_id"), all = TRUE
      )

      # Rename cols
      data.table::setnames(dt, janitor::make_clean_names)
      data.table::setnames(dt, "cw", "c")

      # Reorder cols
      data.table::setcolorder(dt, c(
        "entity_id", "donor_id", "a", "b", "drb1", "c", "dqb1", "dpb1", "drb345",
        "dqa1", "dpa1"
      ))

      # Add counts
      data.table::set(dt, j = "n06", value = dt$a + dt$b + dt$drb1)
      data.table::set(dt, j = "n08", value = dt$n06 + dt$c)
      data.table::set(dt, j = "n10", value = dt$n08 + dt$dqb1)

      # Move counts to front
      data.table::setcolorder(dt, c("entity_id", "donor_id", "n06", "n08", "n10"))

      # Set row order
      data.table::setorderv(dt)

      # Set primary keys
      data.table::setkeyv(dt, c("entity_id", "donor_id"))

      # Convert back to original class
      dt <- dt_cast(dt, to = class)

      # Ensure timestamp is retained
      attr(dt, "timestamp") <- attr(dm_local$hla, "timestamp")

      # Add to dm
      dm_local %>%
        dm::dm_rm_tbl("hla") %>%
        dm::dm_add_tbl(hla = dt[]) %>%
        # Add primary key
        dm::dm_add_pk("hla", !!data.table::key(dt), check = TRUE)
    },


    master = function(dm_local) {
      rlang::check_installed("tidyr")
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
      dt[, "num_degree_match06" := private$master_convert_degree_match(num_degree_match, denom =  6L)]
      dt[, "num_degree_match08" := private$master_convert_degree_match(num_degree_match, denom =  8L)]
      dt[, "num_degree_match10" := private$master_convert_degree_match(num_degree_match, denom = 10L)]
      dt[, "num_degree_match" := NULL]

      # Add from HLA typing
      if ("n06" %in% colnames(dm_local$hla)) {
        dt_hla <- data.table::as.data.table(dm_local$hla)
      } else {
        dt_hla <- data.table::as.data.table(self$hla(dm_local)$hla)
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

      cli_installed <- rlang::is_installed("cli")

      # Initial cohort
      if (cli_installed) {
        n_init_trans <- NROW(dt)
        n_init_pat <- data.table::uniqueN(dt$entity_id)
        message(cli::style_bold("Initial Cohort"))
        message("  ", cli::symbol$info, " ", n_init_trans, " transplants, ", n_init_pat, " patients")
        message("")
      }
      # Only use 1st transplant
      dt <- dt[num_n_trans == 1L]
      if (cli_installed) {
        n_first_trans <- NROW(dt)
        message(cli::style_bold("1st Transplant"))
        message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_init_trans - n_first_trans, " transplants, ", n_init_pat - n_first_trans, " patients removed")
        message("  ", cli::symbol$tick, " ", n_first_trans, " transplants/patients retained")
        message("")
      }

      # Only use pedatric transplants
      dt <- dt[num_age < 21]
      if (cli_installed) {
        n_ped_trans <- NROW(dt)
        message(cli::style_bold("Pediatric"))
        message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_first_trans - n_ped_trans, " transplants/patients removed")
        message("  ", cli::symbol$tick, " ", n_ped_trans, " transplants/patients retained")
        message("")
      }

      # Only use marrow and PBSC transplants
      dt <- dt[cat_product_type %in% c("Marrow", "PBSC", NA_character_)]
      if (cli_installed) {
        n_product_trans <- NROW(dt)
        message(cli::style_bold("BM/PBSC"))
        message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_ped_trans - n_product_trans, " transplants/patients")
        message("  ", cli::symbol$tick, " ", n_product_trans, " transplants/patients retained")
        message("")
      }

      # Don't use solid tumor transplants
      dt <- dt[!cat_dx_grp %in% "Solid Tumor"]
      if (cli_installed) {
        n_nonsolid_trans <- NROW(dt)
        message(cli::style_bold("Non-Solid Tumor"))
        message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_product_trans - n_nonsolid_trans, " transplants/patients")
        message("  ", cli::symbol$tick, " ", n_nonsolid_trans, " transplants/patients retained")
        message("")
      }

      # Don't use mismatched donors
      dt[, "is_mm06" := tidyr::replace_na(cat_degree_match06 < 3L, FALSE)]
      dt[, "is_mm08" := tidyr::replace_na(cat_degree_match08 < 4L, FALSE)]
      dt[, "is_mm10" := tidyr::replace_na(cat_degree_match10 < 5L, FALSE)]
      dt[, "is_mm" := tidyr::replace_na(cat_donor_type %like% "^MM", FALSE)]
      dt[, "is_mm" := is_mm | is_mm06 | is_mm08 | is_mm10]
      dt <- dt[is_mm == FALSE]
      mm_cols <- stringr::str_subset(colnames(dt), "^is_mm")
      dt[, c(mm_cols) := rep(list(NULL), NROW(..mm_cols))]
      if (cli_installed) {
        n_matched_trans <- NROW(dt)
        message(cli::style_bold("HLA Match >= 50%"))
        message("  ", cli::symbol$cross, " ", rep(cli::symbol$line, 10L), cli::symbol$play, " ", n_nonsolid_trans - n_matched_trans, " transplants/patients")
        message("  ", cli::symbol$tick, " ", n_matched_trans, " transplants/patients retained")
        message("")
      }

      # Final
      if (cli_installed) {
        message(cli::style_bold("Final cohort"))
        message("  ", cli::symbol$info, " ", NROW(dt), " transplants/patients")
      }

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
    },


    mrd = function(dm_local) {
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
        private$mrd_str_detect_fct(cat_source, 1L, "BONE\\s*MARROW"), "Bone Marrow",
        private$mrd_str_detect_fct(cat_source, 2L, "PERIPHERAL\\s*BLOOD"), "Peripheral Blood",
        !is.na(cat_source), "Other",
        default = NA_character_
      )]
      dt[, "cat_source" := factor(cat_source, levels = c(
        "Bone Marrow", "Peripheral Blood", "Other"
      ))]

      # Test Method
      dt[, "cat_method" := data.table::fcase(
        private$mrd_str_detect_fct(cat_method, 1L, "FLOW\\s*CYTOMETRY"), "Flow Cytometry",
        private$mrd_str_detect_fct(cat_method, "[23]", "PCR"), "PCR",
        private$mrd_str_detect_fct(cat_method, 4L, "FISH"), "FISH",
        private$mrd_str_detect_fct(cat_method, 5L, "NGS"), "NGS",
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
      dt[, c(result_cols) := lapply(.SD, private$mrd_str_excel_dt_to_num), .SDcols = result_cols]
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
        private$mrd_str_range_to_num(pct_result),
        private$mrd_str_range_to_num(num_actual_value))]
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
    },


    relapse = function(dm_local) {
      dt <- dm_local$relapse
      class <- df_class(dt)
      dt <- data.table::as.data.table(dt)

      # Silence R CMD CHECK notes
      cat_add_tx <- dt_remission <- entity_id <- lgl_add_tx <- lgl_remission <- NULL

      # Standardize columns loaded as character variables
      chr_cols <- purrr::map_lgl(dt, is.character)
      chr_cols <- names(chr_cols[chr_cols])
      dt[, (chr_cols) := lapply(.SD, std_chr), .SDcols = chr_cols]

      # Convert missing values encoded as something else
      na <- c("", "UNKNOWN", "UNKNOWN AT THIS TIME", "N/A", "NA")
      dt[, (chr_cols) := lapply(.SD, function(x) {
        data.table::fifelse(x %in% ..na, NA_character_, x)
      }), .SDcols = chr_cols]

      # Ensure `date` is `Date` (NOTE: MAY CHANGE TO DATETIME/POSIXct IN THE FUTURE)
      dt[, "date" := std_date(date, force = "dt", warn = FALSE)]
      # Ensure `dt_remission` is date (warn if info loss results)
      dt[, "dt_remission" := std_date(dt_remission, force = "dt")]

      # Ensure `lgl_remission` is `logical`
      dt[, "lgl_remission" := data.table::fcase(
        lgl_remission %chin% c("YES", "Y"), TRUE,
        lgl_remission %chin% c("NO", "N"), FALSE,
        default = NA
      )]
      # Ensure `lgl_add_tx` is `logical`
      dt[, "lgl_add_tx" := data.table::fcase(
        lgl_add_tx %chin% c("YES", "Y"), TRUE,
        lgl_add_tx %chin% c("NO", "N"), FALSE,
        default = NA
      )]

      # Standardize multi-membership categories
      mcat_cols <- colnames(dt)[startsWith(colnames(dt), "mcat_")]
      dt[, c(mcat_cols) := lapply(.SD, private$relapse_std_mcat), .SDcols = mcat_cols]

      # Remove patients not in master or missing
      dt <- dt[!is.na(entity_id) & entity_id %in% unique(dm_local$master$entity_id)]
      # Remove observations with no date information
      dt <- dt[!is.na(date)]
      # Remove observations with empty observation variables
      cols <- setdiff(colnames(dt), c("entity_id", "date"))
      has_obs <- rowSums(!is.na(dt[, ..cols])) > 0L
      dt <- dt[has_obs]

      # Set primary keys
      pk <- c("entity_id", "date")

      # Remove duplicates
      dt <- unique(dt, by = pk)

      # Set data.table key
      data.table::setkeyv(dt, pk)

      # Convert back to original class
      dt <- dt_cast(dt, to = class)

      # Ensure timestamp is retained
      attr(dt, "timestamp") <- attr(dm_local$relapse, "timestamp")

      # Add to dm
      dm_local %>%
        dm::dm_rm_tbl("relapse") %>%
        dm::dm_add_tbl(relapse = dt[]) %>%
        # Add primary key
        dm::dm_add_pk("relapse", !!data.table::key(dt), check = TRUE)
    }


  ),
  private = list(


    chimerism_str_detect_fct = function(x, lvl, lbl) {
      stringr::str_detect(
        x,
        paste0("(?i)(^", lvl, "\\s*=)|(", lbl, ")")
      )
    },


    chimerism_str_range_to_num = function(x) {
      x %>%
        stringr::str_replace("<=?", "0-") %>%
        stringr::str_replace(">=?", "100-") %>%
        stringr::str_split("\\s*-\\s*") %>%
        lapply(as.numeric) %>%
        vapply(function(num) mean(as.numeric(num)), double(1L))
    },


    chimerism_str_excel_dt_to_num = function(x) {
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


    chimerism_normalize = function(x) {
      x <- as.numeric(x)
      r <- range(x, na.rm = TRUE)
      (x - r[[1L]]) / (r[[2L]] - r[[1L]])
    },


    chimerism_probit = function(x, range = c(0, 100), tol = sqrt(.Machine$double.eps)) {
      checkmate::assert_numeric(
        range,
        finite = TRUE, any.missing = FALSE, len = 2L, unique = TRUE, sorted = TRUE
      )
      checkmate::assert_number(tol, lower = 0, upper = 0.01, finite = TRUE)
      range <- as.numeric(range)
      if (any(!x %between% range, na.rm = TRUE)) {
        warning("`x` contains values outside of `range`; these values will be truncated by `logit()`")
      }
      p <- (x - range[[1L]]) / (diff(range))
      p <- pmax(tol, pmin(1 - tol, p, na.rm = TRUE), na.rm = TRUE)
      qnorm(p)
    },


    chimerism_inv_probit = function(x, range = c(0, 100), tol = sqrt(.Machine$double.eps)) {
      checkmate::assert_numeric(
        range,
        finite = TRUE, any.missing = FALSE, len = 2L, unique = TRUE, sorted = TRUE
      )
      checkmate::assert_number(tol, lower = 0, upper = 0.01, finite = TRUE)
      range <- as.numeric(range)
      p <- pnorm(x)
      if (any(!p %between% c(0, 1), na.rm = TRUE)) {
        warning("`x` contains values outside of [0, 1] when transformed; these values will be truncated by `logistic()`")
      }
      p <- data.table::fifelse(p < tol, 0, p)
      p <- data.table::fifelse(p > (1 - tol), 1, p)
      p * diff(range) + range[[1L]]
    },


    hla_std_allele = function(x) {
      # Extract first set of numbers in allele ID
      a_id <- x %>%
        # Remove letters
        stringr::str_remove_all("(?i)[A-Z]") %>%
        # Remove any remaining prefix
        stringr::str_remove(".*[*]") %>%
        # Extract first set of numbers
        stringr::str_extract("[0-9]+")

      # If ID is longer than 3 characters, extract the first 2
      a_id_short <- data.table::fifelse(
        nchar(a_id) > 3L,
        stringr::str_sub(a_id, 1L, 2L),
        a_id
      )

      # Convert to integer
      as.integer(a_id_short)
    },


    master_convert_degree_match = function(x, denom = 6L) {
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
    },


    mrd_str_detect_fct = function(x, lvl, lbl) {
      stringr::str_detect(
        x,
        paste0("(?i)(^", lvl, "\\s*=)|(", lbl, ")")
      )
    },


    mrd_str_range_to_num = function(x) {
      x %>%
        stringr::str_replace("LESS\\s*THAN", "<") %>%
        stringr::str_replace("GREATER\\s*THAN", ">") %>%
        stringr::str_replace("<=?", "0-") %>%
        stringr::str_replace(">=?", "100-") %>%
        stringr::str_split("(?<![0-9][^Ee])\\s*-\\s*") %>%
        {suppressWarnings(lapply(., as.numeric))} %>%
        vapply(function(num) mean(as.numeric(num)), double(1L))
    },


    mrd_str_excel_dt_to_num = function(x) {
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


    mrd_str_sci_to_dec = function(x) {
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


    mrd_str_frac_to_dec = function(x) {
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


    mrd_str_dt_na = function(x) {
      is_dt <- stringr::str_detect(x, "(?:[0-9]{1,2}[[:punct:]\\s\\b]+){2}(?:[0-9]{2}){1,2}")
      x[is_dt] <- NA_character_
      x
    },


    mrd_str_to_range_list = function(x) {
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


    mrd_convert_range_units = function(value, units) {
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
    },


    relapse_std_mcat = function(x) {

      str_replace_parenthetic_delim <- function(x, delim, replace) {
        m <- gregexpr("\\([^)(]*+(?:(?R)[^)(]*)*+\\)", x, perl = TRUE)
        v <- lapply(regmatches(x, m), function(x) gsub(delim, replace, x))
        regmatches(x, m) <- v
        x
      }

      str_replace_and <- function(x, delim, replace, replace_parenthetic) {
        x <- stringr::str_replace_all(x, "&", " AND ")
        is_and <- stringr::str_detect(
          x,
          paste0("^[^", delim, "]*[\\s\\b]AND[\\s\\b][^", delim, "]*$")
        )
        x[is_and] <- str_replace_parenthetic_delim(x[is_and], "[\\s\\b]+AND[\\s\\b]+", replace_parenthetic)
        x[is_and] <- stringr::str_replace_all(x[is_and], "[\\s\\b]+AND[\\s\\b]+", replace)
        x
      }

      x %>%
        stringr::str_remove_all("(?<=^|[\\b\\s,;/])NA|N/A(?=[\\b\\s,;/]|$)") %>%
        stringr::str_replace_all("(?:\\s*[,;/\\n]\\s*)+", "\x1F") %>%
        stringr::str_remove_all("^\x1F|\x1F$") %>%
        stringr::str_remove_all("(?<=^|\x1F)\\s*(?:OTHER|SPECIFY)\\s*(?:$|\x1F)") %>%
        str_replace_parenthetic_delim("\x1F+", replace = ", ") %>%
        str_replace_and("\x1F", replace = "; ", replace_parenthetic = " AND ") %>%
        stringr::str_replace_all("\x1F", "; ") %>%
        stringr::str_squish()
    }
  )
)$new()

#' Select and Convert Table + Columns From Remote Source
#'
#' `dm_extract_legacy()` is a previous, less extensible version of `dm_extract()`.
#'  It selects tables and columns of potential interest. It combines all
#'  Cerner tables into one and joins HLA tables.
#'
#' @param dm_remote `[dm]` A `dm` object connected to the SQL Server for MLinHCT
#' @param collect `[lgl(1)]` Should tables be collected locally on output?
#' @param reset `[lgl(1)]` Should the cache be reset to the current results,
#'   even if inputs have not changed? This is useful if data processing logic
#'   has changed, but the underlying data have not.
#'
#' @return `[dm]` The updated `dm` object
dm_extract_legacy <- function(
    dm_remote = dm_sql_server(),
    collect = TRUE,
    reset = FALSE
) {
  # Check arguments
  stopifnot(dm_is_remote(dm_remote))
  checkmate::assert_logical(collect, any.missing = FALSE, len = 1L)
  checkmate::assert_logical(reset, any.missing = FALSE, len = 1L)

  # Decide whether con is transient
  dm_quo <- rlang::enquo(dm_remote)
  dm_is_simple_call <- rlang::is_call_simple(dm_quo)
  dm_quo_nm <- if (dm_is_simple_call) rlang::call_name(dm_quo) else NULL
  dm_fml_nm <- rlang::call_name(rlang::fn_fmls()$dm_remote)
  dm_quo_is_fml <- rlang::is_true(dm_quo_nm == dm_fml_nm)
  dm_con_is_default <- rlang::is_true(attr(dm_remote, "con_is_default"))
  con_is_transient  <- rlang::is_true(dm_quo_is_fml && dm_con_is_default)

  # If con is transient, disconnect on exit
  if (con_is_transient) on.exit(dm_disconnect(dm_remote), add = TRUE)
  # If con is transient, collect must be TRUE
  if (con_is_transient && !collect) {
    rlang::abort("Database connection is temporary; `collect` must be TRUE")
  }
  # If cache or reset is TRUE, collect must be as well
  if (reset && !collect) {
    rlang::abort("Can only cache or reset results if `collect == TRUE`")
  }

  if (collect || reset) {
    # Create file name
    cache_file <- "dm_extract"
    # Compute checksum
    checksum <- eval(DMCache$checksum(dm_remote))
  }

  # Cache
  if (collect && !reset) {
    no_change <- DMCache$check(checksum, cache_file)
    if (no_change) return(DMCache$read("data",  cache_file))
  }

  # Extract tables
  dm_remote <- dm_remote %>%
    # HLA must come before master
    DMExtractLegacy$hla() %>%
    # Master comes next to use in filter joins
    DMExtractLegacy$master() %>%
    # Rest follow in alphabetical order
    DMExtractLegacy$cerner() %>%
    DMExtractLegacy$chimerism() %>%
    DMExtractLegacy$death() %>%
    DMExtractLegacy$disease_status() %>%
    DMExtractLegacy$engraftment() %>%
    DMExtractLegacy$gvhd() %>%
    DMExtractLegacy$mrd() %>%
    DMExtractLegacy$relapse() %>%
    dm::dm_select_tbl(
      c("hla", "master", "cerner", "chimerism", "death", "disease_status"),
      c("engraftment", "gvhd", "mrd", "relapse")
    )

  # Collect/compute
  dm <- dm_remote
  if (collect) dm <- dm_collect(dm_remote, data_table = TRUE)

  # Cache
  if (collect || reset) DMCache$write(dm, checksum, cache_file)

  # Return
  dm
}


DMExtractLegacy <- R6::R6Class(
  "DMExtractLegacy",
  portable = FALSE,
  cloneable = FALSE,
  lock_objects = TRUE,
  lock_class = TRUE,
  public = list(


    cerner = function(
      dm_remote,
      path_var = system.file("extdata", "cerner_var_map.csv", package = "dmhct"),
      quiet = TRUE
    ) {
      if ("cerner" %in% names(dm_remote)) {
        if (!quiet) rlang::inform("`cerner` table already created")
        return(dm_remote)
      }
      # Select cerner tables
      dm_cerner <- dm::dm_select_tbl(dm_remote, dplyr::matches("(?i)legacy[ _]cerner[ _]test"))
      # Create list of names for removal
      nm_list   <- as.list(names(dm_cerner))
      # Row bind to single table
      tbl_cerner <- purrr::reduce(
        dm_cerner[-1L],
        ~ dplyr::union_all(.x, private$cerner_std_tbl(.y, dm_remote$master)),
        .init = private$cerner_std_tbl(dm_cerner[[1L]], dm_remote$master)
      )
      # Add timestamp (max timestamp of individual tables)
      attr(tbl_cerner, "timestamp") <- dm_cerner %>%
        dm::dm_get_tables() %>%
        purrr::map(attr, "timestamp") %>%
        dplyr::as_tibble() %>%
        as.matrix() %>%
        max(na.rm = TRUE) %>%
        lubridate::as_datetime()

      dm_remote %>%
        # Add combined table
        dm::dm_add_tbl(cerner = tbl_cerner) %>%
        # Remove individual tables
        dm::dm_rm_tbl(!!!nm_list)
    },


    chimerism = function(dm_remote) {
      na <- c(
        "", "-9996", "-9999", "NA", "N/A", "na", "n/a", "No Data", "no data",
        "QNS", "CQNS", "NSQ", "IQ", "QI", "QIS", "qns", "cqns", "nsq", "iq", "qi", "qis",
        "NR", "nr", "not reported", "Not Reported", "not rerported", "reported", "Reported",
        "insufficient", "Insufficient", "unk", "UNK", "unknown", "Unknown", "no amp", "No Amp",
        "d2 & pt= 4", "0-100"
      )
      dm_remote %>%
        dm::dm_rename_tbl(chimerism = "Chimerism") %>%
        dm::dm_zoom_to("chimerism") %>%
        dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
        dplyr::semi_join("master", by = "entity_id") %>%
        dplyr::transmute(
          .data$entity_id,
          dt_trans = dbplyr::sql("CONVERT(DATETIME, [Date of Transplant])"),
          date = dbplyr::sql("CONVERT(DATETIME, [Chimerism_Date])"),
          cat_source = trimws(as.character(.data[["Cell Separation"]])),
          cat_source = dplyr::if_else(
            .data$cat_source %in% {{ na }}, NA_character_, .data$cat_source
          ),
          cat_method = trimws(as.character(.data$Method)),
          cat_method = dplyr::if_else(
            .data$cat_method %in% {{ na }}, NA_character_, .data$cat_method
          ),
          pct_donor = trimws(as.character(.data[["Donor%"]])),
          pct_donor = dplyr::if_else(
            .data$pct_donor %in% {{ na }}, NA_character_, .data$pct_donor
          ),
          pct_host = trimws(as.character(.data[["Host%"]])),
          pct_host = dplyr::if_else(
            .data$pct_host %in% {{ na }}, NA_character_, .data$pct_host
          )
        ) %>%
        dplyr::filter(
          !is.na(.data$entity_id),
          !is.na(.data$dt_trans),
          !is.na(.data$date),
          !(is.na(.data$pct_donor) & is.na(.data$pct_host)),
          .data$date >= .data$dt_trans
        ) %>%
        dm::dm_update_zoomed()
    },


    death = function(dm_remote) {
      dm_remote %>%
        dm::dm_rename_tbl(death = "Death_Info") %>%
        dm::dm_zoom_to("death") %>%
        dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
        dplyr::semi_join("master", by = "entity_id") %>%
        dplyr::transmute(
          .data$entity_id,
          dt_death = dbplyr::sql("CONVERT(DATETIME, Date)"),
          dt_trans = dbplyr::sql("CONVERT(DATETIME, [Date of Transplant])"),
          lgl_on_therapy = trimws(as.character(.data$Timing)),
          cat_death_location = trimws(as.character(.data[["Death Location"]])),
          cat_cod_primary1 = trimws(as.character(.data[["Primary cause of death"]])),
          cat_cod_primary2 = trimws(as.character(.data[["Other specify: Primay Cause of Death"]])),
          cat_cod_contrib1 = trimws(as.character(.data[["Contributing cause of death"]])),
          cat_cod_contrib2 = trimws(as.character(.data[["Other specify: Contributing Cause of Death"]])),
          cat_death_class = trimws(as.character(.data[["Classification of Death"]]))
        ) %>%
        dplyr::filter(!is.na(.data$entity_id)) %>%
        dm::dm_update_zoomed()
    },


    disease_status = function(dm_remote) {
      na <- c(
        "",
        "-9999",
        "-9996",
        "Undetermined",
        "Quantity Not Sufficient",
        "Not in Remission, Aplastic marrow quantity not sufficient for analysis"
      )

      dm_remote %>%
        dm::dm_rename_tbl(disease_status = "DiseaseStatus") %>%
        dm::dm_zoom_to("disease_status") %>%
        dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
        dplyr::semi_join("master", by = "entity_id") %>%
        dplyr::transmute(
          .data$entity_id,
          date = dbplyr::sql("CONVERT(DATETIME, Disease_Status_DATE)"),
          disease_status = trimws(as.character(.data[["Disease Status"]])),
          disease_status = dplyr::if_else(
            .data$disease_status %in% {{ na }}, NA_character_, .data$disease_status
          )
        ) %>%
        dplyr::filter(
          !is.na(.data$entity_id),
          !is.na(.data$date),
          !is.na(.data$disease_status)
        ) %>%
        dm::dm_update_zoomed()
    },


    engraftment = function(dm_remote) {
      na <- c(
        "",
        "Not determined",
        "Not Applicable",
        "Never Dropped Below 50K",
        "N/A, Never Dropped Below 20K",
        "N/A, Never Dropped Below 50K",
        "N/A - ANC Never <500 due to no conditioning",
        "N/A - ANC Never <500 due to non-myeloablative regimen"
      )
      dm_remote %>%
        dm::dm_rename_tbl(engraftment = "Engraftment_Info") %>%
        dm::dm_zoom_to("engraftment") %>%
        dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
        dplyr::semi_join("master", by = "entity_id") %>%
        dplyr::transmute(
          .data$entity_id,
          date = dbplyr::sql("CONVERT(DATETIME, [Date of Transplant])"),
          cat_recovery_type = trimws(as.character(.data[["Recovery type"]])),
          lgl_recovery = trimws(as.character(.data[["Is there evidence of Hematopoietic Recovery?"]])),
          lgl_recovery = dplyr::if_else(
            .data$lgl_recovery %in% {{ na }}, NA_character_, .data$lgl_recovery
          )
        ) %>%
        dplyr::filter(
          !is.na(.data$entity_id),
          !is.na(.data$date),
          !is.na(.data$cat_recovery_type),
          !is.na(.data$lgl_recovery)
        ) %>%
        dm::dm_update_zoomed()
    },


    gvhd = function(dm_remote) {
      na <- c("-9996", "-9999", "NULL", "", "NA")
      dm_remote %>%
        dm::dm_rename_tbl(gvhd = "Acute_Chronic_GVHD_Data") %>%
        dm::dm_zoom_to("gvhd") %>%
        dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
        dplyr::semi_join("master", by = "entity_id") %>%
        dplyr::transmute(
          .data$entity_id,
          dt_trans = dbplyr::sql("CONVERT(DATETIME, DOT)"),
          date = dbplyr::sql("CONVERT(DATETIME, [Onset Date])"),
          cat_grade = toupper(trimws(as.character(.data$Overall_Grade))),
          cat_grade = dplyr::if_else(
            .data$cat_grade %in% {{ na }},
            NA_character_,
            .data$cat_grade
          ),
          cat_site = toupper(trimws(as.character(.data$Term)))
        ) %>%
        dplyr::filter(
          !is.na(.data$entity_id),
          !is.na(.data$dt_trans),
          !is.na(.data$date),
          !(is.na(.data$cat_grade) & is.na(.data$cat_site))
        ) %>%
        dm::dm_update_zoomed()
    },


    hla = function(dm_remote, quiet = TRUE) {
      tbls_are_missing <- !all(c("Donor_HLA_Typing", "Patient_HLA_Typing") %in% names(dm_remote))

      if ("hla" %in% names(dm_remote)) {
        if (!quiet) rlang::inform("`hla` table already created")
        return(dm_remote)
      }

      hla <- dplyr::full_join(
        private$hla_std_tbl(dm_remote$Donor_HLA_Typing),
        private$hla_std_tbl(dm_remote$Patient_HLA_Typing),
        by = c("entity_id", "gene"),
        suffix = c("_donor", "_entity")
      )

      # Add timestamp
      attr(hla, "timestamp") <- max(
        attr(dm_remote$Donor_HLA_Typing, "timestamp"),
        attr(dm_remote$Patient_HLA_Typing, "timestamp")
      ) %>% lubridate::as_datetime()

      dm_remote %>%
        # Add new table
        dm::dm_add_tbl(hla = hla) %>%
        # Remove individual tables
        dm::dm_rm_tbl("Donor_HLA_Typing", "Patient_HLA_Typing")
    },


    master = function(dm_remote) {
      dm <- dm_remote %>%
        dm::dm_rename_tbl(master = "Master_Transplant_Info") %>%
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
          lgl_survival = trimws(as.character(.data$SurvialStatus)),
          lgl_malignant = trimws(as.character(.data$Mailgnant)),
          num_degree_match = trimws(as.character(.data[["Degree of Match"]])),
          cat_sex = trimws(as.character(.data$Sex)),
          cat_race = trimws(as.character(.data$Race)),
          cat_ethnicity = trimws(as.character(.data$Ethnicity)),
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
    },


    mrd = function(dm_remote) {
      dm_remote %>%
        dm::dm_rename_tbl(mrd = "MRD") %>%
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
        ) %>%
        dm::dm_update_zoomed()
    },


    relapse = function(dm_remote) {
      dm_remote %>%
        dm::dm_rename_tbl(relapse = "Relapse_Info") %>%
        dm::dm_zoom_to("relapse") %>%
        dplyr::transmute(
          entity_id = as.integer(.data$EntityID),
          date = dbplyr::sql("CONVERT(DATETIME, Relapse_Date)"),
          dt_remission = trimws(as.character(.data[["If yes, specify date"]])),
          lgl_remission = trimws(as.character(.data[["After treatment, did patient achieve remission?"]])),
          lgl_add_tx = trimws(as.character(.data[["Was additional treatment given?"]])),
          mcat_add_tx = trimws(as.character(.data$Method_of_Treatment)),
          mcat_site = trimws(as.character(.data$Site)),
          txt_comments = trimws(as.character(.data$Comments))
        ) %>%
        dm::dm_update_zoomed()
    }


  ),
  private = list(


    cerner_std_tbl = function(tbl, master = NULL) {
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
        # Filter to patients in master if supplied
        dplyr::mutate(entity_id = as.integer(.data$entity_id)) %>%
        purrr::when(
          is.data.frame(master) ~ dplyr::semi_join(., master, by = "entity_id"),
          ~ .
        ) %>%
        # Ensure columns are of expected type
        dplyr::mutate(
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
    },


    hla_std_tbl = function(tbl, na = c("", "NT", "Blank", "-", "Not Interpretable")) {
      tbl <- tbl %>%
        dplyr::mutate(
          entity_id = as.integer(.data$EntityID),
          gene = trimws(.data$display),
          allele = trimws(.data$result_val),
          allele = dplyr::if_else(
            .data$allele %in% {{ na }},
            NA_character_,
            .data$allele
          )
        ) %>%
        dplyr::filter(
          !is.na(.data$entity_id),
          !is.na(.data$gene),
          !is.na(.data$allele)
        )

      if ("DonorID" %in% colnames(tbl)) {
        tbl <- tbl %>%
          dplyr::mutate(donor_id = as.integer(.data$DonorID)) %>%
          dplyr::filter(!is.na(.data$donor_id))
      }

      dplyr::select(
        tbl,
        dplyr::starts_with("donor_id"), "entity_id", "gene", "allele"
      )
    }
  )
)$new()




















#' Extract GVHD Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to a SQL Server w/ HCT data
#'
#' @return The `dm` with instructions to update the `gvhd` table
#'
#' @export
dm_gvhd_extract <- function(dm_remote) {
  na <- c("-9996", "-9999", "NULL", "", "NA")
  dm_remote %>%
    dm::dm_zoom_to("gvhd") %>%
    dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
    dplyr::semi_join("master", by = "entity_id") %>%
    dplyr::transmute(
      .data$entity_id,
      dt_trans = dbplyr::sql("CONVERT(DATETIME, DOT)"),
      dt_onset = dbplyr::sql("CONVERT(DATETIME, [Onset Date])"),
      cat_grade = toupper(trimws(as.character(.data$Overall_Grade))),
      cat_grade = dplyr::if_else(
        .data$cat_grade %in% {{ na }},
        NA_character_,
        .data$cat_grade
      ),
      cat_type = toupper(trimws(as.character(.data$Term)))
    ) %>%
    dplyr::filter(
      !is.na(.data$entity_id),
      !is.na(.data$dt_trans),
      !is.na(.data$dt_onset),
      !(is.na(.data$cat_grade) & is.na(.data$cat_type))
    ) %>%
    dm::dm_update_zoomed()
}


#' Transform the GVHD Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#'
#' @return The transformed `dm`
#'
#' @export
dm_gvhd_transform <- function(dm_local) {
  dt <- dm_local$gvhd
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD CHECK Notes
  cat_grade <- NULL

  # Dates
  dt_cols <- c("dt_trans", "dt_onset")
  dt[, c(dt_cols) := lapply(.SD, lubridate::as_date), .SDcols = dt_cols]

  # Filter by time
  dt <- dt[as.numeric(dt_onset - dt_trans) >= 0]

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
  dt[is.na(cat_grade), "cat_grade" := cat_type %>%
       stringr::str_extract("(?<=GRADE)\\s*[0-9]") %>%
       stringr::str_squish() %>%
       as.integer()]
  dt[, "cat_grade" := ordered(cat_grade)]

  # Duration
  dt[, "cat_type" := cat_type %>%
       stringr::str_replace("\\b(CUTE|AUTE|ACTE|ACUT)\\b", " ACUTE ") %>%
       stringr::str_replace("\\b(HRONIC|CRONIC|CHONIC|CHRNIC|CHROIC|CHRONC|CHRONI|CHRNC)\\b", " CHRONIC ") %>%
       stringr::str_squish()]
  dt[, "cat_duration" := cat_type %>%
       stringr::str_extract("ACUTE|CHRONIC") %>%
       stringr::str_to_title() %>%
       factor()]

  # Type
  dt[, "cat_type" := cat_type %>%
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
  dt[, "cat_type" := data.table::fcase(
    cat_type %in% c("SKIN", "NAILS", "HAIR", "ORAL", "OCULAR"), "Mucocutaneous",
    cat_type %like% "\\b(GI|RECTUM|GUT)\\b", "GI Tract",
    cat_type %in% c("JOINT", "CONNECTIVE TISSUE", "MYOSITIS", "MUSCULOSKELETAL"), "Musculoskeletal",
    cat_type %in% c("GENITOURINARY"), "Genitourinary",
    cat_type %in% c("", "NO SITE DETERMINED"), NA_character_,
    !is.na(cat_type), stringr::str_to_title(cat_type)
  )]

  # Chronic GVHD is not graded
  dt[!is.na(cat_grade) & cat_duration == "Chronic",
     "cat_grade" := factor(NA_integer_, levels = levels(cat_grade))]
  # If missing duration and grade is supplied, assume acute
  dt[is.na(cat_duration) & !is.na(cat_grade),
     "cat_duration" := factor("Acute", levels = levels(cat_duration))]

  # Primary key
  pk <- c("entity_id", "dt_onset", "cat_type")

  # If multiple w/ same time, keep highest grade
  data.table::setorderv(dt, order = -1L, na.last = TRUE)
  dt <- unique(dt, by = c(pk, "cat_duration"))

  # Set order
  data.table::setorderv(dt)

  # Set key
  data.table::setkeyv(dt, pk)


  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("gvhd") %>%
    dm::dm_add_tbl(gvhd = dt[]) %>%
    # Add primary key
    dm::dm_add_pk("gvhd", !!data.table::key(dt), check = TRUE)
}

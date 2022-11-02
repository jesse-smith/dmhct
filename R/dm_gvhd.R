#' Extract GVHD Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to a SQL Server w/ HCT data
#'
#' @return `[dm]` The `dm` w/ instructions to update the `gvhd` table
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
}


#' Transform the GVHD Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` w/ HCT data
#'
#' @return `[dm]` The `dm` object w/ transformed `gvhd` table
#'
#' @export
dm_gvhd_transform <- function(dm_local) {
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
}

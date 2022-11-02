#' Extract Death Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to SQL Server w/ HCT data
#'
#' @return `[dm]` The `dm` with instructions for updating the `death` table
#'
#' @export
dm_death_extract <- function(dm_remote) {
  dm_remote %>%
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
}


#' Transform the Death Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#'
#' @return `[dm]` The `dm` with updated `death` table
#'
#' @export
dm_death_transform <- function(dm_local) {
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
}

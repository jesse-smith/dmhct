#' Extract Disease Status Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` object connected to SQL Server w/ HCT data
#'
#' @return `[dm]` The `dm` object w/ instructions for updating `disease_status` table
#'
#' @export
dm_disease_status_extract <- function(dm_remote) {
  na <- c(
    "",
    "-9999",
    "-9996",
    "Undetermined",
    "Quantity Not Sufficient",
    "Not in Remission, Aplastic marrow quantity not sufficient for analysis"
  )

  dm_remote %>%
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
}


#' Transform Disease Status Table in Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` w/ HCT data
#'
#' @return `[dm]` The `dm` object w/ transformed `disease_status` table
#'
#' @export
dm_disease_status_transform <- function(dm_local) {
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

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("disease_status") %>%
    dm::dm_add_tbl(disease_status = dt[]) %>%
    # Add primary key
    dm::dm_add_pk("disease_status", !!data.table::key(dt), check = TRUE)
}

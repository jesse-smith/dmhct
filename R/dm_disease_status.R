#' Extract Disease Status Table from SQL Server
#'
#' @param dm_remote Remote `dm` object connected to SQL Server
#'
#' @return `dm` object with instructions for updating `disease_status` table
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
    dplyr::transmute(
      entity_id = as.integer(.data$EntityID),
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
#' @param dm_local `dm` object
#'
#' @return The `dm` object with transformed `disease_status` table
#'
#' @export
dm_disease_status_transform <- function(dm_local) {
  dt <- dm_local$disease_status
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

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
    dm::dm_add_tbl(disease_status = dt)
}
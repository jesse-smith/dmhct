#' Extract Relapse Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to SQL Server w/ HLA data
#'
#' @return `dm` with instructions for updating `relapse` table
#'
#' @export
dm_relapse_extract <- function(dm_remote) {
  dm_remote %>%
    dm::dm_zoom_to("relapse") %>%
    dplyr::transmute(
      entity_id = as.integer(.data$EntityID),
      dt_relapse = dbplyr::sql("CONVERT(DATETIME, Relapse_Date)"),
      dt_remission = trimws(as.character(.data[["If yes, specify date"]])),
      lgl_remission = trimws(as.character(.data[["After treatment, did patient achieve remission?"]])),
      cat_tx = trimws(as.character(.data$Method_of_Treatment)),
      cat_site = trimws(as.character(.data$Site))
    ) %>%
    dm::dm_update_zoomed()
}

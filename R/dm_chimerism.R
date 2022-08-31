#' Extract Chimerism Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to SQL Server w/ HCT data
#'
#' @return The `dm` with instructions for updating the `chimerism` table
#'
#' @export
dm_chimerism_extract <- function(dm_remote) {
  dm_remote %>%
    dm::dm_zoom_to("chimerism") %>%
    dplyr::transmute(
      entity_id = as.integer(.data$EntityID),
      dt_trans = dbplyr::sql("CONVERT(DATETIME, [Date of Transplant])"),
      dt_chimerism = dbplyr::sql("CONVERT(DATETIME, [Chimerism_Date])"),
      chr_method = as.character(.data$Method),
      chr_cell_sep = as.character(.data[["Cell Separation"]]),
      pct_donor = as.character(.data[["Donor%"]]),
      pct_host = as.character(.data[["Host%"]])
    ) %>%
    dm::dm_update_zoomed()
}

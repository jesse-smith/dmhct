#' Extract MRD Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to SQL Server w/ HCT data
#'
#' @return The `dm` with instructions for updating the `mrd` table
#'
#' @export
dm_mrd_extract <- function(dm_remote) {
  dm_remote %>%
    dm::dm_zoom_to("mrd") %>%
    dplyr::transmute(
      entity_id = as.integer(.data$EntityID),
      dt_trans = dbplyr::sql("CONVERT(DATETIME, [Date of Transplant])"),
      dt_mrd = dbplyr::sql("CONVERT(DATETIME, [MRD_DAT])"),
      chr_mrd_src = trimws(as.character(.data[["MRD Source"]])),
      chr_mrd_test = trimws(as.character(.data[["MRD Test"]])),
      chr_mrd_result = trimws(as.character(.data[["MRD Result"]])),
      chr_actual_value = trimws(as.character(.data[["Actual Value"]])),
      chr_units = trimws(as.character(.data$Units)),
      chr_genomic_abnormality = trimws(as.character(.data[["Genomic Abnormality"]])),
      chr_cd_grp = trimws(as.character(.data[["CD Group"]])),
      pct_cd_grp = trimws(as.character(.data[["CD Group%"]])),
      chr_comments = trimws(as.character(.data$Comments))
    ) %>%
    dm::dm_update_zoomed()
}

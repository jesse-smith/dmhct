#' Combine Table with Like Information
#'
#' @param dm_std A standardized `dm` object. Standardization is necessary to
#'   ensure columns are all of the same type.
#' @param quiet Should update messages be suppressed?
#'
#' @return The updated `dm` object
#'
#' @export
dm_combine <- function(dm_std = dm_standardize(), quiet = FALSE) {
  as_rlang_error(checkmate::assert_flag(quiet))
  force(dm_std)
  # Cerner tables
  cerner_tbls <- stringr::str_subset(names(dm_std), "cerner[0-9]")
  if (!quiet) rlang::inform(paste0("Combining ", paste0(cerner_tbls, collapse = ", ")))
  # Combine cerner
  cerner_combined <- dm_std %>%
    dm::dm_select_tbl({{ cerner_tbls }}) %>%
    dm::dm_get_tables() %>%
    purrr::map(data.table::as.data.table) %>%
    data.table::rbindlist(use.names = TRUE, fill = TRUE)
  # Revert to tibble
  setTBL(cerner_combined)
  # Add back to dm
  dm_std <- dm_std %>%
    dm::dm_select_tbl(-{{ cerner_tbls }}) %>%
    dm::dm(cerner = cerner_combined)

  # HLA tables
  hla_tbls <- stringr::str_subset(names(dm_std), "^hla_")
  if (!quiet) rlang::inform(paste0("Combining ", paste0(hla_tbls, collapse = ", ")))
  # Combine HLA tables
  hla_dts <- dm_std %>%
    dm::dm_select_tbl({{ hla_tbls }}) %>%
    dm::dm_get_tables() %>%
    purrr::map(data.table::as.data.table)
  hla_combined <- data.table::merge.data.table(
    hla_dts$hla_donor,
    hla_dts$hla_patient,
    by = c("entity_id", "cat_gene"),
    all = TRUE,
    suffixes = c("_donor", "_patient")
  )
  # Move `donor_id` to front
  data.table::setcolorder(hla_combined, c("entity_id", "donor_id", "cat_gene"))
  # Revert to `tibble`
  setTBL(hla_combined)
  # Add back to dm
  dm_std <- dm_std %>%
    dm::dm_select_tbl(-{{ hla_tbls }}) %>%
    dm::dm(hla = hla_combined)

  dm_sort(dm_std)
}




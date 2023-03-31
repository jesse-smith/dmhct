dm_combine <- function(dm_std = dm_standardize()) {
  # Cerner tables
  cerner_tbls <- stringr::str_subset(names(dm_std), "cerner[0-9]")
  # Combine cerner
  cerner_combined <- dm_std %>%
    dm::dm_select_tbl({{ cerner_tbls }}) %>%
    dm::dm_get_tables() %>%
    purrr::map(data.table::setDT) %>%
    data.table::rbindlist(use.names = TRUE, fill = TRUE) %>%
    dplyr::as_tibble()
  # Add back to dm
  dm_std <- dm_std %>%
    dm::dm_select_tbl(-{{ cerner_tbls }}) %>%
    dm::dm(cerner = cerner_combined)

  # HLA tables
  hla_tbls <- stringr::str_subset(names(dm_std), "^hla_")
  # Combine HLA tables
  hla_dts <- dm_std %>%
    dm::dm_select_tbl({{ hla_tbls }}) %>%
    dm::dm_get_tables() %>%
    purrr::map(data.table::setDT) %>%
    purrr::map(pivot_hla)
  hla_combined <- data.table::merge.data.table(
    hla_dts$hla_donor,
    hla_dts$hla_patient,
    by = "entity_id",
    all = TRUE,
    suffixes = c("_donor", "_patient")
  )
  # Add back to dm
  dm_std <- dm_std %>%
    dm::dm_select_tbl(-{{ hla_tbls }}) %>%
    dm::dm(hla = hla_combined)
  # Force cleanup
  gc(verbose = FALSE)

  dm_sort(dm_std)
}


pivot_hla <- function(dt_hla) {
  id_cols <- stringr::str_subset(colnames(dt_hla), "(?:_id|date)$")
  dt_hla <- paste0("data.table::dcast(",
    "dt_hla, ",
    paste0(id_cols, collapse = " + "), "  ~ cat_gene, ",
    "value.var = 'cat_allele', ",
    "fun.aggregate = function(x) paste0(x, collapse = ',')",
  ")") %>%
    rlang::parse_expr() %>%
    eval()
  dt_hla <- janitor::clean_names(dt_hla)
  dt_hla
}

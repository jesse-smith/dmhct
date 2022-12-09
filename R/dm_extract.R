dm_extract2 <- function(dm_remote, collect = TRUE) {
  checkmate::assert_flag(collect)
  # Standardize names for each table
  for (tbl_nm in names(dm_remote)) {
    # Get standardized names from name mappings, if available
    map_file <- system.file(
      paste0("extdata/", tbl_nm, "_column_map.csv"),
      package = "dmhct"
    )
    if (map_file == "") {
      col_map <- tibble::tibble(Old_Name = character(), New_Name = character())
    } else {
      col_map <- utils::read.csv(
        map_file,
        colClasses = "character",
        na.strings = NULL
      )
    }
    # Combine and standardize old and new names
    nms <- colnames(dm_remote[[tbl_nm]]) %>%
      tibble::as_tibble_col("Old_Name") %>%
      dplyr::left_join(col_map, "Old_Name") %>%
      dplyr::mutate(New_Name = janitor::make_clean_names(
        dplyr::coalesce(.data$New_Name, .data$Old_Name)
      ))
    # Update table in `dm` object
    dm_remote <- paste0(
      "dm::dm_rename(dm_remote, ", tbl_nm, ", ",
      paste0(nms$New_Name, " = `", nms$Old_Name, "`", collapse = ", "), ")"
    ) %>%
      rlang::parse_expr() %>%
      eval()
  }
  # Load data onto local machine
  if (collect) dm_collect(dm_remote) else dm_remote
}

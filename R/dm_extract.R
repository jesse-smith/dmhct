#' Extract Remote Tables from SQL Server for MLinHCT
#'
#' `dm_extract()` extracts and (optionally) loads the remote database housing
#' the MLinHCT into the current R session. Column names are standardized during
#' extraction, but no other operations are performed.
#'
#' @param dm_remote `[dm]` A `dm` object connected to the remote SQL server
#'   database
#' @param ... Names of tables to select; if provided, only these tables will be
#'   extracted
#' @param .collect `[lgl]` Indicates whether the extracted data should be loaded
#'   onto the local machine (`TRUE` by default)
#'
#' @return `[dm]` A `dm` object with all tables and columns extracted from the
#'   remote source.
#'
#' @export
dm_extract <- function(dm_remote = dm_sql_server(), ..., .collect = TRUE) {
  as_rlang_error(checkmate::assert_flag(.collect))

  # Standardize table names
  tbl_nms <- names(dm_remote)
  tbl_new_nms <- janitor::make_clean_names(tbl_nms)
  nms_diff <- tbl_nms != tbl_new_nms
  if (any(nms_diff)) {
    rename_inputs <- paste0(
      tbl_new_nms[nms_diff], " = `", tbl_nms[nms_diff], "`",
      collapse = ", "
    )
    dm_remote <- paste0("dm::dm_rename_tbl(dm_remote, ", rename_inputs, ")") %>%
      rlang::parse_expr() %>%
      eval()
  }

  # Use additional standardization for some tables
  nm_map <- read.csv(
    system.file("extdata/table_name_map.csv", package = "dmhct"),
    colClasses = "character",
    na.strings = NULL
  ) %>% dplyr::mutate(dplyr::across(.fns = janitor::make_clean_names))
  dm_remote <- paste0(
    "dm::dm_rename_tbl(dm_remote, ",
    paste0(nm_map$New_Name, " = ", nm_map$Old_Name, collapse = ", "), ")"
  ) %>%
    rlang::parse_expr() %>%
    eval()

  # Re-order tables alphabetically
  dm_remote <- paste0(
    "dm::dm_select_tbl(dm_remote, ", paste0(sort(names(dm_remote)), collapse = ", "), ")"
  ) %>%
    rlang::parse_expr() %>%
    eval()

  # Optionally select tables
  dots <- rlang::ensyms(...)
  if (length(dots) > 0L) {
    if (any(names(dots) != "")) rlang::abort("Tables cannot be renamed in `dm_extract()`")
    dm_remote <- dm::dm_select_tbl(dm_remote, ...)
  }
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
  if (.collect) dm_collect(dm_remote) else dm_remote
}

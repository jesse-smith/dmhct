#' Extract Remote Tables from SQL Server for MLinHCT
#'
#' `dm_extract()` extracts and (optionally) loads the remote database housing
#' the MLinHCT into the current R session. Unless `.legacy = TRUE`, column and
#' table names are standardized during extraction, but no other operations are
#' performed. When `.legacy = TRUE`, the legacy version of `dm_extract()` is
#' used; see details for this behavior. Note that legacy behavior will be
#' deprecated and eventually removed in future releases, so it is strongly
#' recommended that any new code use `.legacy = FALSE`.
#'
#' Legacy behavior is more opinionated than the current version of `dm_extract()`.
#' First, only a subset of tables and columns are extracted. Second, HLA tables
#' and Cerner tables are combined into a single HLA table and a single Cerner
#' table. Third, some column standardization occurs (though it is limited to
#' simple `as()` transformations, `trimws(toupper(x))` on character variables,
#' and replacement of implicit missing values with explicit `NA`s.) Finally,
#' some filtering of "uninformative" observations may occur. In the current
#' pipeline, these changes are deferred to later steps to give more control
#' to the user.
#'
#' @param dm_remote `[dm]` A `dm` object connected to the remote SQL server
#'   database
#' @param ... Names of tables to select; if provided, only these tables will be
#'   extracted
#' @param .collect `[lgl]` Indicates whether the extracted data should be loaded
#'   onto the local machine (`TRUE` by default)
#' @param .legacy `[lgl]` Should the legacy version of `dm_extract()` be used?
#'   Will be deprecated in a future release, along with `.reset`.
#' @param .reset `[lgl]` Should the legacy cache be forced to reset? Only applicable
#'   if `.legacy = TRUE`; ignored otherwise. Will be deprecated in a future release,
#'   along with `.legacy`.
#' @param .excl_dsmb `[lgl]` `r lifecycle::badge("deprecated")` This information
#'   is no longer available in the remote database.
#' @param .quiet Should status messages be suppressed?
#' @param reset `[lgl]` `r lifecycle::badge("deprecated")` Please use
#'   `.reset` instead. Current behavior will only consider this argument if
#'   `.reset` is unchanged from the default.
#'
#' @return `[dm]` A `dm` object with all tables and columns extracted from the
#'   remote source.
#'
#' @export
dm_extract <- function(
    dm_remote = dm_sql_server(),
    ...,
    .collect = TRUE,
    .legacy = FALSE,
    .reset = FALSE,
    .excl_dsmb = FALSE,
    .quiet = FALSE,
    reset = .reset
) {
  # Deprecated arguments
  call_arg_nms <- rlang::call_args_names(rlang::caller_call(0L))
  if ("reset" %in% call_arg_nms) {
    lifecycle::deprecate_soft("1.0.0", "dm_extract(reset)", with = "dm_extract(.reset)")
    if (!".reset" %in% call_arg_nms) .reset <- reset
  }

  # Check arguments - make sure that removal doesn't cause error
  as_rlang_error(checkmate::assert_flag(.collect))
  as_rlang_error(checkmate::assert_flag(.excl_dsmb))
  if (exists(".legacy")) as_rlang_error(checkmate::assert_flag(.legacy))
  if (exists(".reset")) as_rlang_error(checkmate::assert_flag(.reset))

  # Use legacy version if desired, but give deprecation message once `.legacy` default is FALSE
  if (rlang::is_false(rlang::fn_fmls()$.legacy)) {
    if (".legacy" %in% call_arg_nms) lifecycle::deprecate_soft("1.0.0", "dm_extract(.legacy)")
    if (".reset" %in% call_arg_nms) lifecycle::deprecate_soft("1.0.0", "dm_extract(.reset)")
  }
  if (.legacy) return(dm_extract_legacy(dm_remote, collect = .collect, reset = .reset))

  force(dm_remote)

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

  # Use additional table name standardization for some tables
  nm_map <- utils::read.csv(
    system.file("extdata/table_name_map.csv", package = "dmhct"),
    colClasses = "character",
    fileEncoding = "UTF-8-BOM",
    na.strings = NULL
  ) %>% dplyr::mutate(dplyr::across(dplyr::everything(), .fns = janitor::make_clean_names))
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

  # Find patients who are part of an active DSMB-monitored trial
  dsmb_ids <- if (.excl_dsmb) dsmb_entity_ids(dm_remote) else bit64::integer64()
  .excl_dsmb <- .excl_dsmb && length(dsmb_ids) > 0L

  # Optionally select tables
  dots <- rlang::ensyms(...)
  if (length(dots) > 0L) {
    if (any(names(dots) != "")) rlang::abort("Tables cannot be renamed in `dm_extract()`")
    dm_remote <- dm::dm_select_tbl(dm_remote, ...)
  }

  # Standardize column names for each table
  for (tbl_nm in names(dm_remote)) {
    # Get standardized column names from name mappings, if available
    map_file <- system.file(
      paste0("extdata/", tbl_nm, "_column_map.csv"),
      package = "dmhct"
    )
    if (map_file == "") {
      col_map <- dplyr::tibble(Old_Name = character(), New_Name = character())
    } else {
      col_map <- utils::read.csv(
        map_file,
        colClasses = "character",
        fileEncoding = "UTF-8-BOM",
        na.strings = NULL
      )
    }
    # Combine and standardize old and new names
    nms <- colnames(dm_remote[[tbl_nm]]) %>%
      as_tibble_col("Old_Name") %>%
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

  # Load data onto local machine or just return
  if (.collect) {
    dm_local <- purrr::imap(dm::dm_get_tables(dm_remote), function(x, i) {
      if (!.quiet) rlang::inform(paste0("Extracting `", i, "`"))
      ts <- attr(x, "timestamp")
      x <- dplyr::collect(x)
      attr(x, "timestamp") <- ts
      x
    }) %>% dm::as_dm()
    gc(verbose = FALSE)
    return(dm_local)
  } else {
    gc(verbose = FALSE)
    dm_remote
  }
}

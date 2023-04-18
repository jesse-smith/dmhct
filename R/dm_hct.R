#' Extract, Standardize, and Combine Tables from the MLinHCT Database
#'
#' `dm_hct()` chains together `dm_extract()`, `dm_standardize()`, and
#' `dm_combine()` to provide a single wrapper function for data preparation.
#'
#' @inheritParams dm_extract
#' @inheritParams dm_standardize
#' @return The prepared `dm` object
#'
#' @export
dm_hct <- function(
    dm_remote = dm_sql_server(),
    ...,
    .excl_dsmb = TRUE,
    .quiet = FALSE
) {
  as_rlang_error(checkmate::assert_flag(.quiet))
  style_bold <- if (rlang::is_installed("cli")) cli::style_bold else function(x) x
  # Extract data from database
  if (!.quiet) rlang::inform(style_bold("Extracting database (1/4)"))
  dm <- dm_extract(dm_remote = dm_remote, ..., .legacy = FALSE, .excl_dsmb = .excl_dsmb)
  if (!.quiet) rlang::inform("Done.")
  # Standardize columns
  if (!.quiet) rlang::inform(style_bold("Standardizing table columns (2/4)"))
  dm <- dm_standardize(dm_local = dm, quiet = .quiet)
  # Combine tables
  if (!.quiet) rlang::inform(style_bold("Combining similar tables (3/4)"))
  dm <- dm_combine(dm)
  if (!.quiet) rlang::inform("Done.")
  # Pivot tables
  if (!.quiet) rlang::inform(style_bold("Pivoting E-A-V tables (4/4)"))
  dm <- dm_pivot(dm)
  if (!.quiet) rlang::inform("Done.")
  # Return
  dm
}

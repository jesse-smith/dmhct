#' Extract, Load, and Transform Remote Tables to Local Source
#'
#' `dm_elt()` encompasses the entire legacy dmhct pipeline; however, this
#' pipeline is deprecated no longer under active development. While this function
#' will be retained for backwards compatibility, it is strongly recommended that
#' new code use the new pipeline instead.
#'
#' @param dm_remote `[dm]` Remote `dm` object containing HCT data
#' @param reset `[lgl(1)]` Should the cache be reset to the current results,
#'   even if inputs have not changed? This is useful if data processing logic
#'   has changed, but the underlying data have not.
#' @param close `[lgl(1)]` Whether to close the SQL Server connection on
#'   exit. `NULL` closes if `dm_remote` has attribute `default == TRUE` and
#'   leaves open otherwise.
#'
#' @return `[dm]` A `dm` object
#'
#' @export
dm_elt <- function(
    dm_remote = dm_sql_server(),
    reset = FALSE,
    close = NULL
) {
  # Warn deprecated
  lifecycle::deprecate_soft("1.0.0", "dm_elt()")

  # Check arguments
  stopifnot(dm_is_remote(dm_remote))
  checkmate::assert_logical(reset, any.missing = FALSE, len = 1L)
  checkmate::assert_logical(
    close, any.missing = FALSE, max.len = 1L, null.ok = TRUE
  )

  # Decide whether con is transient
  dm_quo <- rlang::enquo(dm_remote)
  dm_is_simple_call <- rlang::is_call_simple(dm_quo)
  dm_quo_nm <- if (dm_is_simple_call) rlang::call_name(dm_quo) else NULL
  dm_fml_nm <- rlang::call_name(rlang::fn_fmls()$dm_remote)
  dm_quo_is_fml <- rlang::is_true(dm_quo_nm == dm_fml_nm)
  dm_con_is_default <- rlang::is_true(attr(dm_remote, "con_is_default"))
  con_is_transient  <- rlang::is_true(dm_quo_is_fml && dm_con_is_default)

  # If con is transient, disconnect on exit
  if (con_is_transient || rlang::is_true(close)) {
    on.exit(dm_disconnect(dm_remote), add = TRUE)
  }

  dm_remote %>%
    # Collect = Load
    dm_extract_legacy(collect = TRUE, reset = reset) %>%
    dm_transform_legacy(reset = reset)
}

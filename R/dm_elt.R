#' Extract, Load, and Transform Remote Tables to Local Source
#'
#' @param dm_remote `[dm]` Remote `dm` object containing HCT data
#' @param cache `[lgl(1)]` Should results be cached (if inputs have changed)
#'   or read from cache (if inputs have not changed)?
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
    dm_transform(reset = reset)
}

#' Transform Tables to Analysis-Friendly Format
#'
#' @param dm_local `[dm]` A local `dm` object from `dm_extract()`
#' @param cache `[lgl(1)]` Should the results be cached?
#' @param reset `[lgl(1)]` Should the cache be reset to the current results,
#'   even if inputs have not changed? This is useful if data processing logic
#'   has changed, but the underlying data have not.
#'
#' @return `[dm]` The updated `dm` object
#'
#' @export
dm_transform <- function(dm_local = dm_extract_legacy(), cache = TRUE, reset = FALSE) {
  # Check arguments
  stopifnot(dm::is_dm(dm_local))
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1L)
  checkmate::assert_logical(reset, any.missing = FALSE, len = 1L)

  if (cache || reset) {
    cache_file <- "dm_transform"
    checksum <- dm_cache$checksum(dm_local)
  }

  # Cache
  if (cache && !reset) {
    no_change <- dm_cache$check(checksum, cache_file)
    if (no_change) return(dm_cache$read("data", cache_file))
  }

  dm_local <- dm_local %>%
    # HLA must come before master
    dm_hla_transform() %>%
    # Master comes next to use in filter joins
    dm_master_transform() %>%
    # Rest follow in alphabetical order
    dm_cerner_transform() %>%
    dm_chimerism_transform() %>%
    dm_death_transform() %>%
    dm_disease_status_transform() %>%
    dm_engraftment_transform() %>%
    dm_gvhd_transform() %>%
    dm_mrd_transform() %>%
    dm_relapse_transform()

  # Cache
  if (cache || reset) dm_cache$write(dm_local, checksum, cache_file)

  dm_local
}

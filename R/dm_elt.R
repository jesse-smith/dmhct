#' Extract, Load, and Transform Remote Tables to Local Source
#'
#' @param dm_remote Remote `dm` object containing HCT data
#' @param cache `[logical(1)]` Should results be cached (if inputs have changed)
#'   or read from cache (if inputs have not changed)?
#' @param reset `[logical(1)]` Should the cache be reset to the current results,
#'   even if inputs have not changed? This is useful if data processing logic
#'   has changed, but the underlying data have not.
#' @param close `[logical(1)]` Whether to close the SQL Server connection on
#'   exit. `NULL` closes if `dm_remote` has attribute `default == TRUE` and
#'   leaves open otherwise.
#'
#' @return A `dm` object
#'
#' @export
dm_elt <- function(
    dm_remote = dm_sql_server(),
    cache = TRUE,
    reset = FALSE,
    close = NULL
) {
  # Check arguments
  stopifnot(dm_is_remote(dm_remote))
  checkmate::assert_logical(data.table, any.missing = FALSE, len = 1L)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1L)
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

  # Cache
  if (cache && !reset) {
    checksum <- dm_checksum(dm_remote)
    no_change <- dm_cache_check(checksum, "dm_extract")
    if (no_change) return(dm_cache_read("data", "dm_transform"))
  }

  dm_remote %>%
    # Collect = Load
    dm_extract(collect = TRUE, cache = cache, reset = reset) %>%
    dm_transform(cache = cache, reset = reset)
}

#' Select and Convert Table + Columns From Remote Source
#'
#' `dm_extract()` selects tables and columns of potential interest. It
#'   combines all Cerner tables into one and joins HLA tables.
#'
#' @param dm_remote `[dm]` A `dm` object connected to the SQL Server for MLinHCT
#' @param collect `[logical(1)]` Should tables be collected locally on output?
#'   `collect = TRUE` means that tables will also be computed, even if
#'   `compute = FALSE`.
#' @param compute `[logical(1)]` Should tables be computed on the server? This
#'   does not mean that the computed tables will be transferred to the local
#'   machine; to do so, set `collect = TRUE`.
#' @param cache `[logical(1)]` Should results be cached (if inputs have changed)
#'   or read from cache (if inputs have not changed)?
#' @param reset `[logical(1)]` Should the cache be reset to the current results,
#'   even if inputs have not changed? This is useful if data processing logic
#'   has changed, but the underlying data have not.
#'
#' @return The updated `dm` object
#'
#' @export
dm_extract <- function(
    dm_remote = dm_sql_server(),
    collect = TRUE,
    compute = FALSE,
    cache = collect,
    reset = FALSE
) {
  # Check arguments
  stopifnot(dm_is_remote(dm_remote))
  checkmate::assert_logical(collect, any.missing = FALSE, len = 1L)
  checkmate::assert_logical(compute, any.missing = FALSE, len = 1L)
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1L)
  checkmate::assert_logical(reset, any.missing = FALSE, len = 1L)

  # Decide whether con is transient
  dm_quo <- rlang::enquo(dm_remote)
  dm_is_simple_call <- rlang::is_call_simple(dm_quo)
  dm_quo_nm <- if (dm_is_simple_call) rlang::call_name(dm_quo) else NULL
  dm_fml_nm <- rlang::call_name(rlang::fn_fmls()$dm_remote)
  dm_quo_is_fml <- rlang::is_true(dm_quo_nm == dm_fml_nm)
  dm_con_is_default <- rlang::is_true(attr(dm_remote, "con_is_default"))
  con_is_transient  <- rlang::is_true(dm_quo_is_fml && dm_con_is_default)

  # If con is transient, disconnect on exit
  if (con_is_transient) on.exit(dm_disconnect(dm_remote), add = TRUE)
  # If con is transient, collect must be TRUE
  if (con_is_transient && !collect) {
    rlang::abort("Database connection is temporary; `collect` must be TRUE")
  }
  # If cache or reset is TRUE, collect must be as well
  if ((cache || reset) && !collect) {
    rlang::abort("Can only cache or reset results if `collect == TRUE`")
  }

  if (cache || reset) {
    # Create file name
    cache_file <- "dm_extract"
    # Compute checksum
    checksum  <- eval(dm_checksum(dm_remote))
  }

  # Cache
  if (cache && !reset) {
    no_change <- dm_cache_check(checksum, cache_file)
    if (no_change) return(dm_cache_read("data",  cache_file))
  }

  # Extract tables
  dm_remote <- dm_remote %>%
    dm_disease_status_extract() %>%
    dm_engraftment_extract() %>%
    dm_relapse_extract() %>%
    dm_cerner_extract() %>%
    dm_master_extract() %>%
    dm_death_extract() %>%
    dm_hla_extract() %>%
    dm::dm_select_tbl(
      c("disease_status", "engraftment", "relapse", "cerner", "master"),
      c("death", "hla", "chimerism", "mrd")
    )

  # Collect/compute
  dm <- dm_remote
  if (compute) dm <- dm_compute(dm_remote)
  if (collect) dm <- dm_collect(dm_remote)

  # Cache
  if (cache || reset) dm_cache_write(dm, checksum, cache_file)

  # Return
  dm
}


#' Transform Tables to Analysis-Friendly Format
#'
#' @param dm_local `[dm]` A local `dm` object from `dm_extract()`
#' @param cache `[logical(1)]` Should the results be cached?
#' @param reset `[logical(1)]` Should the cache be reset to the current results,
#'   even if inputs have not changed? This is useful if data processing logic
#'   has changed, but the underlying data have not.
#'
#' @return `[dm]` The updated `dm` object
#'
#' @export
dm_transform <- function(dm_local = dm_extract(), cache = TRUE, reset = FALSE) {
  # Check arguments
  stopifnot(dm::is_dm(dm_local))
  checkmate::assert_logical(cache, any.missing = FALSE, len = 1L)
  checkmate::assert_logical(reset, any.missing = FALSE, len = 1L)

  if (cache || reset) {
    cache_file <- "dm_transform"
    checksum <- dm_checksum(dm_local)
  }

  # Cache
  if (cache && !reset) {
    no_change <- dm_cache_check(checksum, cache_file)
    if (no_change) return(dm_cache_read("data", cache_file))
  }

  dm_local <- dm_local %>%
    dm_disease_status_transform() %>%
    dm_engraftment_transform() %>%
    dm_cerner_transform() %>%
    dm_hla_transform() %>%
    dm_master_transform() %>%
    dm_death_transform()

  # Cache
  if (cache || reset) dm_cache_write(dm_local, checksum, cache_file)

  dm_local
}

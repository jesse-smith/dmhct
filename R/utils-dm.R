#' Collect All Tables in a `dm` Object
#'
#' @param dm_remote `[dm]` A `dm` object connected to a remote source
#'
#' @return `[dm]` A new `dm` object containing the collected (local) tables
#'
#' @export
dm_collect <- function(dm_remote, data_table = FALSE) {
  checkmate::assert_flag(data_table)
  purrr::map(dm::dm_get_tables(dm_remote), function(x) {
    ts <- attr(x, "timestamp")
    x <- dplyr::collect(x)
    if (data_table) x <- data.table::setDT(x)
    attr(x, "timestamp") <- ts
    x
  }) %>% dm::as_dm()
}


#' Compute All Tables in a `dm` Object
#'
#' @param dm_remote `[dm]` A `dm` object connected to a remote source
#' @param quiet `[lgl(1)]` Should messages be suppressed during computation?
#'
#' @return `[dm]` The updated object with tables computed
#'
#' @export
dm_compute <- function(dm_remote, quiet = TRUE) {
  compute <- dm::compute
  if (quiet) compute <- function(x, ...) suppressMessages(dm::compute(x, ...))

  for (tbl_nm in names(dm_remote)) {
    tbl <- dm_remote[[tbl_nm]]
    ts <- attr(tbl, "timestamp")
    tbl <- compute(tbl)
    attr(tbl, "timestamp") <- ts
    dm_remote <- dm_remote %>%
      dm::dm_select_tbl(-{{ tbl_nm }}) %>%
      dm::dm({{ tbl_nm }} := tbl)
  }

  dm_remote
}




#' Check Whether a `dm` Object is Connected to a Remote Server
#'
#' @param dm The `dm` object to check
#'
#' @return `[lgl(1)]` Whether the `dm` is remote or not
#'
#' @export
dm_is_remote <- function(dm) {
  con <- try(dm::dm_get_con(dm), silent = TRUE)
  !rlang::inherits_any(con, "try-error")
}


#' Disconnect a `dm` Object from the Remote Server
#'
#' @param dm `[dm]` The `dm` object to disconnect
#'
#' @return `[dm]` The `dm` (invisibly)
#'
#' @export
dm_disconnect <- function(dm) {
  if (dm_is_remote(dm)) DBI::dbDisconnect(dm::dm_get_con(dm))
  invisible(dm)
}

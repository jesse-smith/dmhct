#' Collect All Tables in a `dm` Object
#'
#' @param dm_remote `[dm]` A `dm` object connected to a remote source
#'
#' @return `[dm]` A new `dm` object containing the collected (local) tables
#'
#' @export
dm_collect <- function(dm_remote) {
  nms <- names(dm_remote)
  tbl_list <- purrr::map(
    nms,
    ~ data.table::setDT(dplyr::collect(dm_remote[[.x]]))
  )
  names(tbl_list) <- nms

  dm::as_dm(tbl_list)
}


#' Compute All Tables in a `dm` Object
#'
#' @param dm_remote `[dm]` A `dm` object connected to a remote source
#' @param quiet Should messages be suppressed during computation?
#'
#' @return `[dm]` The updated object with tables computed
#'
#' @export
dm_compute <- function(dm_remote, quiet = TRUE) {
  compute <- dm::compute
  if (quiet) compute <- function(x, ...) suppressMessages(dm::compute(x, ...))
  for (table in names(dm_remote)) {
    dm_remote <- dm_remote %>%
      dm::dm_zoom_to({{ table }}) %>%
      compute() %>%
      dm::dm_update_zoomed()
  }

  dm_remote
}




#' Check Whether a `dm` Object is Connected to a Remote Server
#'
#' @param dm The `dm` object to check
#'
#' @return A `logical` indicating whether the `dm` is remote or not
#'
#' @export
dm_is_remote <- function(dm) {
  con <- try(dm::dm_get_con(dm), silent = TRUE)
  !rlang::inherits_any(con, "try-error")
}


#' Disconnect a `dm` Object from the Remote Server
#'
#' @param dm The `dm` object to disconnect
#'
#' @return The `dm` (invisibly)
#'
#' @export
dm_disconnect <- function(dm) {
  if (dm_is_remote(dm)) DBI::dbDisconnect(dm::dm_get_con(dm))
  invisible(dm)
}

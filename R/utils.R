#' Create a Path from Components
#'
#' @param ... `[chr]` Components of a path
#'
#' @return `[fs_path]` The created and cleaned file path
#'
#' @keywords internal
path_create <- function(..., abs = FALSE) {
  path <- fs::path(...) %>%
    fs::path_norm() %>%
    fs::path_expand_r()
  if (checkmate::assertFlag(abs)) {
    fs::path_abs(path)
  } else {
    path
  }
}


#' Check Whether a `dm` Object is Connected to a Remote Server
#'
#' @param dm The `dm` object to check
#'
#' @return A `logical` indicating whether the `dm` is remote or not
#'
#' @keywords internal
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
#' @keywords internal
dm_disconnect <- function(dm) {
  if (dm_is_remote(dm)) DBI::dbDisconnect(dm::dm_get_con(dm))
  invisible(dm)
}

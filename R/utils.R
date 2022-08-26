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

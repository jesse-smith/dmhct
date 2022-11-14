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


#' Get Class of `data.frame`-Like Object
#'
#' @param data `[data.frame]` A `data.frame` or one of its sub-classes
#'
#' @return `[chr(1)]` The name of the class
#'
#' @keywords internal
df_class <- function(data) {
  if (data.table::is.data.table(data)) {
    "data.table"
  } else if (tibble::is_tibble(data)) {
    "tibble"
  } else if (is.data.frame(data)) {
    "data.frame"
  } else {
    rlang::abort("`data` is not a data frame")
  }
}


#' Cast a `data.table` to a Specified `data.frame` Sub-Class
#'
#' @param data `[data.table]` A `data.table` to cast; will be modified in-place
#'   if possible.
#' @param to `[chr(1)]` The subclass to return
#'
#' @return The converted data
#'
#' @keywords internal
dt_cast <- function(data, to) {
  if (to == "data.table") return(data)
  data.table::setDF(data)
  if (to == "tibble") data <- tibble::as_tibble(data)
  data
}


std_chr <- function(x, case = c("upper", "lower", "title", "sentence"), keep_inner_newlines = TRUE) {
  if (!is.null(case)) case <- rlang::arg_match(case)[[1L]]
  # Convert to ASCII
  x <- x %>%
    stringi::stri_trans_general("Any-Latin;Latin-ASCII") %>%
    stringi::stri_enc_toascii()

  # Squish
  if (keep_inner_newlines) {
    x <- x %>%
      # Condense all whitespace except newlines
      stringr::str_replace_all("[^\\S\\r\\n]+", " ") %>%
      # Remove leading & training whitespace
      stringr::str_trim() %>%
      # Condense newlines, including those separated by whitespace
      stringr::str_replace_all("(?:[^\\S\\r\\n]*[\\r\\n][^\\S\\r\\n]*)+", "\n")
  } else {
    x <- stringr::str_squish(x)
  }

  if (is.null(case)) return(x)
  switch(
    case,
    "lower" = stringr::str_to_lower(x),
    "upper" = stringr::str_to_upper(x),
    "title" = stringr::str_to_title(x),
    "sentence" = stringr::str_to_sentence(x)
  )
}

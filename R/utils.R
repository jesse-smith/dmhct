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
  } else if (is_tibble(data)) {
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
  if (to == "tibble") data <- dplyr::as_tibble(data)
  data
}


as_rlang_error <- function(error_expr) {
  err <- rlang::catch_cnd(error_expr)
  if (is.null(err)) return(invisible(NULL))
  rlang::abort(err$message)
}


as_tibble_col <- function(x, column_name = "value") {
  dplyr::tibble(`:=`(!!column_name, x))
}


is_tibble <- function(x) {
  inherits(x, "tbl_df")
}


setTBL <- function(x, rownames = NULL) {
  # Convert to data.frame
  data.table::setDF(x, rownames = rownames)
  # Convert to tibble
  data.table::setattr(x, "class", c("tbl_df", "tbl", "data.frame"))
  # Return
  invisible(x)
}


#' Select Column Names Using Tidyselect Specifications
#'
#' `select_colnames()` selects the names of columns specified in `...`. It is
#' useful for standardizing a function's interface while providing a link to
#' underlying functions that may take a variety of column specifications.
#'
#' @param .data A data frame or data frame extension (e.g. a `tibble`)
#'
#' @param ... `<tidy-select>` One or more tidyselect specifications for the
#'   desired column names (including simply using those column names)
#'
#' @return A character vector of column names
#'
#' @keywords internal
#'
#' @export
select_colnames <- function(data, ...) {
  checkmate::assert_data_frame(data)
  colnames(dplyr::select(data, ...))
}

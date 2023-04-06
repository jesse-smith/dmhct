#' Extract Values That Cannot Be Converted to `numeric`
#'
#' `non_numeric()` is designed primarily for interactive checking of numeric
#' conversions. It helps quickly determine what values in a vector cannot be
#' converted to `numeric` (either directly or via `std_num()`); this is
#' particularly useful for checking steps of a data cleaning pipeline.
#'
#' @param x A vector
#' @param unique Whether unique values should be returned; if `FALSE`, all
#'   values are returned
#' @param sort Whether return values should be sorted; most useful when
#'   `unique = TRUE`
#' @param std_num Whether to use `std_num()` for numeric conversion; if `FALSE`,
#'   conversion is performed directly by `as.numeric()` (with warnings
#'   suppressed)
#'
#' @return The values of `x` that resulted in `NA_real_` after conversion; this
#'   includes any `NA` values in `x` before conversion
#'
#' @export
non_numeric <- function(x, unique = TRUE, sort = unique, std_num = FALSE) {
  # Check arguments
  as_rlang_error(checkmate::assert_flag(unique))
  as_rlang_error(checkmate::assert_flag(sort))
  as_rlang_error(checkmate::assert_flag(std_num))
  # Convert to numeric
  if (std_num) {
    x_num <- std_num(x, warn = FALSE)
  } else {
    x_num <- suppressWarnings(as.numeric(x))
  }
  # Get values that result in `NA_real_`
  is_na <- is.na(x_num)
  x_na <- x[is_na]
  # Get unique and/or sort
  if (unique) x_na <- unique(x_na)
  if (sort) x_na <- sort(x_na)
  # Return
  x_na
}

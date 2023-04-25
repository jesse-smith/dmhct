str_detect_vec <- function(string, pattern, negate = FALSE, reduce = c("any", "all")) {
  vec_map_lgl(string, stringr::str_detect, reduce, pattern = pattern, negate = negate)
}

str_ends_vec <- function(string, pattern, negate = FALSE, reduce = c("any", "all")) {
  vec_map_lgl(string, stringr::str_ends, reduce, pattern = pattern, negate = negate)
}

str_extract_vec <- function(string, pattern, group = NULL) {
  vec_map_chain(string, stringr::str_extract, pattern = pattern, group = NULL)
}

str_remove_vec <- function(string, pattern) {
  vec_map_chain(string, stringr::str_remove, pattern = pattern)
}

str_remove_all_vec <- function(string, pattern) {
  vec_map_chain(string, stringr::str_remove_all, pattern = pattern)
}

str_replace_vec <- function(string, pattern, replacement) {
  vec_map_chain(string, stringr::str_replace, pattern = pattern, replacement = replacement)
}

str_replace_all_vec <- function(string, pattern, replacement) {
  vec_map_chain(string, stringr::str_replace_all, pattern = pattern, replacement = replacement)
}

str_replace_na_vec <- function(string, replacement = "NA") {
  vec_map_chain(string, stringr::str_replace_na, pattern = pattern, replacement = replacement)
}

str_starts_vec <- function(string, pattern, negate = FALSE, reduce = c("any", "all")) {
  vec_map_lgl(string, stringr::str_starts, reduce, pattern = pattern, negate = negate)
}


#' Replace Detected Patterns with `NA`
#'
#' `str_to_na()` detects `pattern`s in `x` using
#' \code{\link[stringr:str_replace]{stringr::str_detect()}} and replaces all
#' indices where a `pattern` is found with `NA_character_`. It is vectorized
#' over both `x` and `pattern`; `pattern` will be looped over if multiple are
#' provided.
#'
#' @inheritParams stringr::str_replace
#'
#' @return The input as a character vector with `pattern`s replaced with `NA`
#'
#' @keywords internal
str_to_na <- function(string, pattern = stringr::fixed("")) {
  string[str_detect_vec(string, pattern)] <- NA_character_
  string
}


str_to_case <- function(string, case = c("upper", "lower", "title", "sentence")) {
  if (is.null(case)) return(string)
  case <- rlang::arg_match(case)[[1L]]
  case_fn <- switch(
    case,
    "upper" = stringr::str_to_upper,
    "lower" = stringr::str_to_lower,
    "title" = stringr::str_to_title,
    "sentence" = stringr::str_to_sentence
  )
  vec_map_chain(string, case_fn)
}


str_split_f <- function(string, pattern, n = Inf) {
  as_rlang_error(checkmate::assert_string(pattern))
  as_rlang_error(checkmate::assert_number(n))
  vec_map_chain(string, stringr::str_split, pattern = pattern, n = n)
}


vec_map_chain <- function(.x, .f, ...) {
  # Ensure `.x` is an atomic vector
  as_rlang_error(checkmate::assert_atomic_vector(.x))
  # Convert `.f` to function if formula
  if (rlang::is_formula(.f)) .f <- rlang::as_function(.f)
  # Ensure that `.f` is a function
  as_rlang_error(checkmate::assert_function(.f))
  # Get unique values and set as key
  du <- data.table(x = unique(.x), key = "x")
  # Add copy for holding output
  data.table::set(du, j = "x_new", value = du$x)
  # Get additional arguments to `.f`
  dots <- data.table::as.data.table(rlang::list2(...))
  # Prepare for inserting first argument at each iteration
  arg1 <- list(NULL)
  names(arg1) <- rlang::fn_fmls_names(.f)[[1L]]
  # Loop over `dots` and apply `.f` repeatedly, or just apply once if no dots
  if (length(dots) == 0L) {
    data.table::set(du, j = "x_new", value = .f(du$x_new))
  } else {
    for (i in seq_len(NROW(dots))) {
      arg1[[1L]] <- du$x_new
      data.table::set(
        du,
        j = "x_new",
        value = do.call(.f, c(arg1, as.list(dots[i])))
      )
    }
  }
  # Match back to original vector and return
  du$x_new[match(.x, du$x)]
}


vec_map_lgl <- function(.x, .f, .reduce = c("any", "all"), ...) {
  # Ensure `.x` is an atomic vector
  as_rlang_error(checkmate::assert_atomic_vector(.x))
  # Convert `.f` to function if formula
  if (rlang::is_formula(.f)) .f <- rlang::as_function(.f)
  # Ensure that `.f` is a function
  as_rlang_error(checkmate::assert_function(.f))
  # Check `.reduce`
  .reduce <- rlang::arg_match(.reduce)[[1L]]
  # Get unique values and set as key
  du <- data.table(x = unique(.x), key = "x")
  # Add copy for holding output
  data.table::set(du, j = "lgl", value = .reduce == "all")
  # Get additional arguments to `.f`
  dots <- data.table::as.data.table(rlang::list2(...))
  # Prepare for inserting first argument at each iteration
  arg1 <- list(du$x)
  names(arg1) <- rlang::fn_fmls_names(.f)[[1L]]
  # Loop over `dots` and apply `.f` repeatedly
  op <- if (.reduce == "any") `|` else `&`
  # Loop over `dots` and apply `.f` repeatedly, or just apply once if no dots
  if (length(dots) == 0L) {
    data.table::set(du, j = "lgl", value = .f(du$x))
  } else {
    for (i in seq_len(NROW(dots))) {
      data.table::set(
        du,
        j = "lgl",
        value = op(du$lgl, do.call(.f, c(arg1, as.list(dots[i]))))
      )
    }
  }
  # Match back to original vector and return
  du$lgl[match(.x, du$x)]
}

# User-facing utility functions

#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr:pipe]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom dm %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the magrittr placeholder.
#' @param rhs A function call using the magrittr semantics.
#' @return The result of calling `rhs(lhs)`.
NULL


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


#' Convert Standardized Intervals to Matrix Format
#'
#' `intvl_to_matrix()` converts interval representation standardized by
#' `std_invl()` to a 4-column `numeric` matrix. Columns represent open or
#' closed bounds and the location of those bounds.
#'
#' @param x A `character` vector of standardized intervals
#'
#' @return A 4-column numeric matrix:
#'   \itemize{
#'     \item `left_closed`:  Whether the left bound is closed or open
#'     \item `left_bound`:   The left bound of the interval
#'     \item `right_bound`:  The right bound of the interval
#'     \item `right_closed`: Whether the right bound is closed or open
#'   }
#'
#' @export
intvl_to_matrix <- function(x) {
  # Extract numbers
  x_numeric <- suppressWarnings(as.numeric(x))
  is_numeric <- !is.na(x_numeric)
  x_numeric <- x_numeric[is_numeric]
  # Extract single bounds
  is_intvl1 <- !is_numeric & x %like% "^[<>=]"
  x_intvl1 <- x[is_intvl1]
  op_intvl1 <- stringr::str_extract(x_intvl1, "^[<>=]{1,2}")
  x_intvl1 <- stringr::str_remove(x_intvl1, "^[<>=]{1,2}")
  x_intvl1 <- suppressWarnings(as.numeric(x_intvl1))
  is_intvl1_update <- !is.na(x_intvl1)
  is_intvl1[is_intvl1] <- is_intvl1_update
  op_intvl1 <- op_intvl1[is_intvl1_update]
  x_intvl1 <- x_intvl1[is_intvl1_update]
  # Extract dual bounds
  pat2 <- "(\\(|\\[)([0-9.]+(?:[Ee]-?[0-9]+)?),([0-9.]+(?:[Ee]-?[0-9]+)?)(\\]|\\))"
  is_intvl2 <- !(is_numeric | is_intvl1) & x %like% pat2
  x_intvl2 <- stringr::str_match(x[is_intvl2], pat2)[, -1L, drop = FALSE]
  op_intvl2 <- x_intvl2[, c(1L, 4L), drop = FALSE]
  x_intvl2 <- x_intvl2[, c(2L, 3L), drop = FALSE]
  x_intvl2 <- matrix(suppressWarnings(as.numeric(x_intvl2)), ncol = 2L)
  is_intvl2_update <- !(apply(x_intvl2, 1L, anyNA) | apply(op_intvl2, 1L, anyNA))
  is_intvl2[is_intvl2] <- is_intvl2_update
  op_intvl2 <- op_intvl2[is_intvl2_update, , drop = FALSE]
  x_intvl2 <- x_intvl2[is_intvl2_update, , drop = FALSE]
  # Create matrix
  x_mat <- matrix(NA_real_, nrow = length(x), ncol = 4L, dimnames = list(NULL, c("left_closed", "left_bound", "right_bound", "right_closed")))
  # Add numeric
  x_mat[is_numeric, c(1L, 4L)] <- 1
  x_mat[is_numeric, 2L] <- x_numeric
  x_mat[is_numeric, 3L] <- x_numeric
  # Add interval1
  x_mat[is_intvl1, 1L] <- data.table::fcase(op_intvl1 == ">=", 1, op_intvl1 == ">", 0, op_intvl1 %in% c("<", "<="), 1)
  x_mat[is_intvl1, 4L] <- data.table::fcase(op_intvl1 == "<=", 1, op_intvl1 == "<", 0, op_intvl1 %in% c(">", ">="), 1)
  x_mat[is_intvl1, 2L] <- data.table::fcase(
    op_intvl1 %in% c(">", ">="), x_intvl1,
    op_intvl1 %in% c("<", "<=") & x_intvl1 > 0, 0
  )
  x_mat[is_intvl1, 3L] <- data.table::fcase(
    op_intvl1 %in% c("<", "<="), x_intvl1,
    op_intvl1 %in% c(">", ">=") & x_intvl1 < 100, 100
  )
  # Add interval2
  x_mat[is_intvl2, 1L] <- as.numeric(op_intvl2[, 1L] == "[")
  x_mat[is_intvl2, 4L] <- as.numeric(op_intvl2[, 2L] == "]")
  x_mat[is_intvl2, c(2L, 3L)] <- x_intvl2
  # Return
  x_mat
}


#' Standardize `character` Vectors
#'
#' `std_chr()` standardizes `character` vectors to ASCII text with no unnecessary
#' whitespace and a given case. By default, it will retain newlines inside text,
#' though it will condense consecutive newlines and any carriage returns into a
#' single newline.
#'
#' @param x A character vector
#' @param case The case to convert to. `NULL` will skip case conversion.
#' @param keep_inner_newlines Whether to retain line breaks inside text. `FALSE`
#'   will treat newlines and carriage returns identically to any other whitespace.
#' @return The standardized character vector
#'
#' @export
std_chr <- function(x, case = c("upper", "lower", "title", "sentence"), keep_inner_newlines = TRUE) {
  if (!is.null(case)) case <- rlang::arg_match(case)[[1L]]
  # Convert to ASCII
  not_ascii <- !stringi::stri_enc_isascii(x)
  not_ascii[is.na(not_ascii)] <- FALSE
  if (any(not_ascii)) {
    x[not_ascii] <- x[not_ascii] %>%
      stringi::stri_trans_general("Any-Latin;Latin-ASCII") %>%
      stringi::stri_enc_toascii()
  }

  # Squish
  if (keep_inner_newlines) {
    x <- x %>%
      # Condense all whitespace except newlines (only ASCII needed)
      stringr::str_replace_all("[ \\t\\v\\f]+", " ") %>%
      # Remove leading & training whitespace
      stringr::str_trim() %>%
      # Condense newlines, including those separated by whitespace
      stringr::str_replace_all("(?:[ ]*[\\r\\n]+[ ]*)+", "\n")
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


#' Convert and Standardize Numeric Values in Various Forms
#'
#' `std_num()` converts all base classes, as well as `int64`, `factor`, `Date`,
#' and `POSIXt` vectors to the simplest numeric form possible.
#'
#' `character` vectors are standardized using `std_chr()` by default, then
#' converted. `factor`s are treated as `character` vectors, rather than using
#' the underlying integer representation. `double` and `int64` vectors will be
#' converted to `integer` if this does not cause overflow or loss of precision.
#' `Date` is converted to `integer`, and `POSIXt` is converted to `integer` if
#' the range allows, otherwise `double`.
#'
#' @param x A vector to convert to numeric
#' @param std_chr Whether to standardize a `character` or `factor` before conversion
#' @param warn Whether to warn when strings cannot be converted; passed to `chr_to_num()`
#'
#' @return A `numeric` vector
#'
#' @export
std_num <- function(x, std_chr = TRUE, warn = TRUE) {
  if (is.integer(x)) {
    return(x)
  } else if (bit64::is.integer64(x)) {
    return(tryCatch(as.integer(x), warning = function(w) x))
  } else if (is.double(x)) {
    return(tryCatch(vctrs::vec_cast(x, integer()), error = function(e) x))
  } else if (is.character(x) || is.factor(x)) {
    return(chr_to_num(x, std = std_chr, warn = warn))
  } else if (lubridate::is.Date(x)) {
    return(as.integer(x))
  } else if (lubridate::is.POSIXt(x)) {
    return(tryCatch(as.integer(as.double(x))), warning = function(w) as.double(x))
  } else {
    rlang::abort(paste0("Cannot convert object of class", class(x)[[1L]], " to numeric"))
  }
}


#' Standardize Interval Representations
#'
#' `std_invl()` standardizes the various representations of numeric intervals
#' found in the ML in HCT dataset. These intervals are assumed to be in percentage
#' values and thus lie between 0 and 100. Explicit intervals with upper and lower
#' bounds, as well as implicit intervals using < and >, are handled (<= and >=
#' are currently not supported). The return value simplifies to </>/<=/>= or
#' a single numeric value if possible and uses standard interval notation if not.
#'
#' @param x A `character` vector
#' @param std_chr Whether to standarize the strings before parsing
#' @param warn Whether to emit a warning when potential numeric values are not
#'   able to be converted to an interval
#' @param chimerism The type of chimerism to extract, if any; passed to `chr_to_num()`,
#'   when `std_chr = TRUE`, ignored otherwise.
#'
#' @return A `character` vector
#'
#' @export
std_intvl <- function(x, std_chr = TRUE, warn = TRUE, chimerism = c("no", "donor", "host")) {
  # Clean character representation (w/o converting)
  x_chr <- chr_to_num(x, std = std_chr, warn = warn, convert = FALSE, chimerism = chimerism)
  x_num <- x_chr %>%
    # Replace "less than"
    stringr::str_replace("LESS THAN", "<") %>%
    # Replace "greater than"
    stringr::str_replace("GREATER THAN", ">") %>%
    # Replace "nothing to suggest w/ sensitivity"
    stringr::str_replace("[A-Z ]*NOTHING TO SUGGEST[A-Z ]*SENSITIVITY[A-Z ]*(?=[0-9])", "<") %>%
    # Extract ranges and numbers
    stringr::str_extract("[0-9(<>=][0-9- .Ee<>=)]*") %>%
    # Remove spaces
    stringr::str_remove_all("\\s")
  # Create return vector
  x_rtn <- x_num
  # Get limits of double precision for rounding (floor to avoid previous roundoffs)
  digits <- floor(abs(log10(.Machine$double.eps)))
  # Handle regular numbers and intervals with 1 number
  # Operator for 1-number interval
  x_op <- stringr::str_extract(x_num, "^[<>]")
  # Number should convert after removal of operator
  x_numeric <- stringr::str_remove(x_num, "^[<>]")
  x_numeric <- suppressWarnings(as.numeric(x_numeric))
  # Define as able to be converted to numeric after removal of operator
  is_numeric_intvl1 <- !is.na(x_numeric)
  # Round and convert back to character
  x_numeric <- as.character(round(x_numeric[is_numeric_intvl1], digits))
  # Subset to non-missings and fill in return values
  x_op <- x_op[is_numeric_intvl1]
  x_op[is.na(x_op)] <- ""
  x_rtn[is_numeric_intvl1] <- paste0(x_op, x_numeric)
  # Handle intervals with 2 numbers and no operators
  # Define as not numeric or 1-number interval; must have "x-x" range and no operators
  is_intvl2_no_op <- !is_numeric_intvl1 & x_num %like% "[0-9]-[0-9]" & !x_num %like% "[<>()]"
  # Extract numeric values
  x_mat_intvl2_no_op <- stringr::str_match(
    x_num[is_intvl2_no_op],
    "([0-9.]+(?:[Ee]-?[0-9]+)?)-([0-9.]+(?:[Ee]-?[0-9]+)?)"
  )[, -1L, drop = FALSE]
  # Convert to numeric and round
  x_mat_intvl2_no_op <- round(suppressWarnings(as.numeric(x_mat_intvl2_no_op)), digits)
  # Convert back to character matrix
  x_mat_intvl2_no_op <- matrix(as.character(x_mat_intvl2_no_op), ncol = 2L)
  # Paste together
  x_intvl2_no_op <- paste0("[", x_mat_intvl2_no_op[, 1L], ",", x_mat_intvl2_no_op[, 2L], "]")
  # Revert missings to `NA`
  x_intvl2_no_op[x_intvl2_no_op == "[NA,NA]"] <- NA_character_
  # Fill in return values
  x_rtn[is_intvl2_no_op] <- x_intvl2_no_op
  # Handle intervals with 2 numbers and operators
  # Define as not numeric, 1-number interval, or 2-number interval w/o operator; must have "x-x" range and an operator
  is_intvl2_op <- !(is_numeric_intvl1 | is_intvl2_no_op) & x_num %like% "[0-9]-[<>0-9]" & x_num %like% "[<>()]"
  # Extract brackets, operators, and numbers
  x_mat_intvl2_op <- stringr::str_match(
    x_num[is_intvl2_op],
    "(\\()?([<>])?([0-9.]+(?:[Ee]-?[0-9]+)?)-([<>])?([0-9.]+(?:[Ee]-?[0-9]+)?)(\\))?"
  )[, -1L, drop = FALSE]
  # Get numbers from matrix
  x_mat_intvl2_op_num <- x_mat_intvl2_op[, c(3L, 5L)]
  # Convert to numeric and round
  x_mat_intvl2_op_num <- round(suppressWarnings(as.numeric(x_mat_intvl2_op_num)), digits)
  # Convert back to character matrix
  x_mat_intvl2_op_num <- matrix(as.character(x_mat_intvl2_op_num), ncol = 2L)
  # Get operators from matrix
  x_mat_intvl2_op_op <- x_mat_intvl2_op[, c(1L, 2L, 4L, 6L)]
  # Combine operators "<>" and "()" - prefer "()"
  x_mat_intvl2_op_op[, 1L] <- data.table::fcoalesce(x_mat_intvl2_op_op[, 1L], x_mat_intvl2_op_op[, 2L])
  x_mat_intvl2_op_op[, 4L] <- data.table::fcoalesce(x_mat_intvl2_op_op[, 4L], x_mat_intvl2_op_op[, 3L])
  x_mat_intvl2_op_op <- x_mat_intvl2_op_op[, c(1L, 4L)]
  # Convert "<>" to "()"
  x_mat_intvl2_op_op[x_mat_intvl2_op_op[, 1L] == ">", 1L] <- "("
  x_mat_intvl2_op_op[is.na(x_mat_intvl2_op_op[, 1L]), 1L] <- "["
  x_mat_intvl2_op_op[x_mat_intvl2_op_op[, 2L] == "<", 2L] <- ")"
  x_mat_intvl2_op_op[is.na(x_mat_intvl2_op_op[, 2L]), 2L] <- "]"
  # Assume that signs creating impossible intervals are typos
  x_mat_intvl2_op_op[x_mat_intvl2_op_op[, 1L] == "<", 1L] <- "["
  x_mat_intvl2_op_op[x_mat_intvl2_op_op[, 2L] == ">", 2L] <- "]"
  # Paste together
  x_intvl2_op <- paste0(x_mat_intvl2_op_op[, 1L], x_mat_intvl2_op_num[, 1L], ",", x_mat_intvl2_op_num[, 2L], x_mat_intvl2_op_op[, 2L])
  # Revert missings to `NA`
  x_intvl2_op[x_intvl2_op %in% c("[NA,NA]", "(NA,NA]", "[NA,NA)", "(NA,NA)")] <- NA_character_
  # Fill in return values
  x_rtn[is_intvl2_op] <- x_intvl2_op
  # Convert 0 or 100 bounds to equivalent 1-number intervals
  # Convert closed zero bound and open upper bound to "< upper bound"
  is_closed_zero <- (is_intvl2_no_op | is_intvl2_op) & x_rtn %like% "^\\[0," & x_rtn %like% "\\)$"
  x_rtn[is_closed_zero] <- stringr::str_replace(x_rtn[is_closed_zero], "\\[0,([^)]+)\\)", "<\\1")
  # Convert closed zero bound and closed upper bound to "<= upper bound"
  is_closed_zero_c <- (is_intvl2_no_op | is_intvl2_op) & x_rtn %like% "^\\[0," & x_rtn %like% "\\]$"
  x_rtn[is_closed_zero_c] <- stringr::str_replace(x_rtn[is_closed_zero_c], "\\[0,([^\\]]+)\\]", "<=\\1")
  # Convert closed 100 bound and open lower bound to "> lower bound"
  is_closed_100 <- (is_intvl2_no_op | is_intvl2_op) & x_rtn %like% "100\\]$" & x_rtn %like% "^\\("
  x_rtn[is_closed_100] <- stringr::str_replace(x_rtn[is_closed_100], "\\(([^,]+),100\\]", ">\\1")
  # Convert closed 100 bound and closed lower bound to ">= lower bound"
  is_closed_100_c <- (is_intvl2_no_op | is_intvl2_op) & x_rtn %like% "100\\]$" & x_rtn %like% "^\\["
  x_rtn[is_closed_100_c] <- stringr::str_replace(x_rtn[is_closed_100_c], "\\[([^,]+),100\\]", ">=\\1")
  # Warn of unconverted strings if desired
  if (warn) {
    is_na_rtn <- apply(intvl_to_matrix(x_rtn), 1L, function(.x) all(is.na(.x)))
    not_converted <- unique(x[!is.na(x_chr) & is_na_rtn])
    if (length(not_converted) > 0L) {
      not_converted <- paste0(
        '"', stringr::str_replace_all(not_converted, '"', '\\"'), '"',
        collapse = ", "
      )
      rlang::warn(paste0(
        "Not all character strings converted to interval.",
        " Values not converted were: ", not_converted
      ))
    }
  }
  x_rtn
}


#' Standardize `logical` Representations in Various Formats
#'
#' `std_lgl()` converts other classes to `logical` vectors. All but `character`
#' use `as.logical()`; `character` vectors are converted by first (optionally)
#' standardizing with `std_chr` and then assigning logical value based on the
#' regular expression in `true`, `false`, and `na`.
#'
#' @param x A vector to convert
#' @param true Regular expressions corresponding to `TRUE`; these will be
#'   combined using `"|"`
#' @param false Regular expressions corresponding to `FALSE`; these will be
#'   combined using `"|"`
#' @param na Regular expressions corresponding to `NA`; these will be combined
#'   using `"|"`
#' @param std_chr Whether to standardized a `character` vector before parsing
#' @param warn Whether to warn if `character` strings were not converted to
#'   `logical`
#'
#' @return A `logical` vector
#'
#' @export
std_lgl <- function(
    x,
    true = c("YES", "POS", "ALIVE", "ON THERAPY"),
    false = c("NO", "NEG", "EXPIRED", "DECEASED", "OFF THERAPY"),
    na = c("^$", "N/?A", "-999[0-9]", "UNSPEC", "NO DATA", "UNKN?",
           "INCONCLUSIVE", "NOT? (?:EVAL|APPL|DONE|DETER)", "EQUIVOCAL",
           "NEVER DROPPED BELOW"),
    std_chr = TRUE,
    warn = TRUE
) {
  checkmate::assert_flag(std_chr)
  if (std_chr && (is.character(x) || is.factor(x))) x <- std_chr(x)
  if (is.character(x)) {
    x[stringr::str_starts(x, paste0(na, collapse = "|"))] <- NA_character_
    x[stringr::str_starts(x, paste0(true, collapse = "|"))] <- "TRUE"
    x[stringr::str_starts(x, paste0(false, collapse = "|"))] <- "FALSE"
    not_converted <- unique(x[!x %in% c("TRUE", "FALSE", NA_character_)])
    if (length(not_converted) > 0L) {
      if (warn) {
        not_converted <- paste0(
          '"', stringr::str_replace_all(not_converted, '"', '\\"'), '"',
          collapse = ", "
        )
        rlang::warn(paste0(
          "Not all character strings converted to class logical.",
          " Values not converted were: ", not_converted
        ))
      }
      return(suppressWarnings(as.logical(x)))
    }
  }
  return(as.logical(x))
}


#' Parse Dates to Standard Format
#'
#' `std_date` standardizes a date vector and returns a vector in `Date` or
#' `POSIXct` format, depending on whether there is sub-daily information
#' available in the data.
#'
#' @param x A vector of `character` dates, `Date`s, or `POSIXt`s
#'
#' @param force Whether to force conversion to `Date` (`force = "dt"`) or
#'   `POSIXct` (`force = "dttm"`). The default is no forcing (`force = "none"`).
#'
#' @param orders A `character` vector of date-time formats. Each order
#'   string is a series of formatting characters as listed in
#'   \code{\link[base:strptime]{base::strptime()}} but might not include the
#'   "%" prefix. For example, "ymd" will match all the possible dates in
#'   year, month, day order. Formatting orders might include arbitrary
#'   separators. These are discarded. See details of
#'   \code{\link[lubridate:parse_date_time]{lubridate::parse_date_time()}}
#'   for the implemented formats. If multiple order strings are supplied,
#'   the order of applied formats is determined by the `select_formats`
#'   parameter in
#'   \code{\link[lubridate:parse_date_time]{lubridate::parse_date_time()}}
#'   (if passed via dots).
#'
#' @param tz_heuristic Hours to consider in determining presence of sub-daily
#'   information. Only exact hours (i.e. 5:00:00) will be combined. The default
#'   corresponds to accidental encoding of the CST-UTC offset as hours.
#'
#' @param warn Should warnings be thrown when necessary? `FALSE` will
#'   suppress all warnings in the conversion process.
#'
#' @param train `logical`, default `TRUE`. Whether to train formats on a
#'   subset of the input vector. The result of this is that supplied orders
#'   are sorted according to performance on this training set, which
#'   commonly results in increased performance. Please note that even
#'   when `train = FALSE` (and `exact = FALSE`, if passed via dots)
#'   guessing of the actual formats is still performed on a pseudo-random
#'   subset of the original input vector. This might result in
#'   `⁠All formats failed to parse`⁠ error.See notes
#'   in \code{\link[lubridate:parse_date_time]{lubridate::parse_date_time()}}.
#'
#' @param na Regular expressions to convert to `NA`
#'
#' @param range_value The value to use if the date is given as a range; can be
#'   the start date, the end date, or fill with `NA`
#'
#' @param range_sep Separators used for date ranges
#'
#' @param ... Additional arguments to pass to
#'   \code{\link[janitor:convert_to_date]{convert_to_datetime()}}. These
#'   will, in turn, be passed to further methods, including
#'   \code{\link[janitor:excel_numeric_to_date]{excel_numeric_to_date()}},
#'   \code{\link[lubridate:parse_date_time]{parse_date_time()}}, and
#'   \code{\link[base:as.POSIXct]{as.POSIXct()}}.
#'
#' @return A `Date` or `POSIXct` vector
#'
#' @export
std_date = function(
    x,
    force = c("none", "dt", "dttm"),
    orders = c("mdy",  "dmy",  "ymd",
               "mdyr", "dmyr", "ymdr",
               "mdyR", "dmyR", "ymdR",
               "mdyT", "dmyT", "ymdT",
               "mdyTz", "dmyTz", "ymdTz", "Tmdyz", "Tdmyz", "Tymdz",
               "mdyRz", "dmyRz", "ymdRz", "mdyrz", "dmyrz", "ymdrz",
               "Tmdy",  "Tdmy",  "Tymd",  "Tmdyz", "Tdmyz", "Tymdz"),
    tz_heuristic = c(5L, 6L),
    warn = TRUE,
    train = TRUE,
    na = c("^$", "N/?A", "ONGOING"),
    range_value = c("start", "end", "na"),
    range_sep = c("-", "to", ","),
    ...
) {
  checkmate::assert_flag(warn)
  if (!warn) {
    suppressWarnings(std_date(
      x, force = force, orders = orders, tz_heuristic = tz_heuristic,
      warn = TRUE, train = train, na = na, range_value = range_value,
      range_sep = range_sep, ...
    ))
  }
  as_rlang_error(checkmate::assert_character(na))
  range_value <- rlang::arg_match(range_value)[[1L]]
  as_rlang_error(checkmate::assert_character(range_sep))
  if (is.character(x)) {
    # Handle missings
    x[stringr::str_starts(x, paste0(na, collapse = "|"))] <- NA_character_
    # Handle ranges
    if (range_value != "na") {
      x_split <- stringr::str_split(x, paste0("\\s*(?:", paste0(range_sep, collapse = "|"), ")\\s*"))
      is_range <- purrr::map_lgl(x_split, ~ length(.x) == 2L)
      i <- switch(range_value, "start" = 1L, "end" = 2L)
      map_fn <- function(x, ...) {
        as.character(tryCatch(std_date(x, ...)[[i]], warning = function(x) x[[i]]))
      }
      x[is_range] <- purrr::map_chr(
        x_split[is_range], map_fn,
        force = "dttm", orders = orders, tz_heuristic = NULL,
        warn = TRUE, train = train, na = na, range_value = "na",
        range_sep = "-", ...
      )
    }
  }
  x <- janitor::convert_to_datetime(
    x,
    orders = orders,
    train = train,
    ...,
    character_fun = UtilsDate$chr_to_dttm,
    string_conversion_failure = "warning"
  )
  UtilsDate$dttm_to_dt(x, force = force)
}

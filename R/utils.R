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
    return(as.double(x))
  } else {
    rlang::abort(paste0("Cannot convert object of class", class(x)[[1L]], " to numeric"))
  }
}


chr_to_num <- function(
    x, std = TRUE, warn = TRUE, convert = TRUE,
    na_patterns = c(
      "^-*$", # Includes ""
      "(?:^|\\W)N/?A(?:$|\\W)",
      "NOT? (?:AMP|AVAIL|DATA|DONE|EVAL|FOLLOWED|RER?PORT|IDENT|(?:[A-Z ]|-)*PHENOTYPE|VALUE)",
      "(?:^|\\W)(?:NG|UNK|CQNS|QNS|QIS|NSQ|IQ|QI|NR|UNKNOWN|INCONCLUSIVE|INSUFFICIENT)(?:$|\\W)",
      "-999[0-9]",
      "^REPORTED$"
    ),
    chimerism = c("no", "donor", "host")
) {
  as_rlang_error(checkmate::assert_flag(std))
  as_rlang_error(checkmate::assert_flag(warn))
  as_rlang_error(checkmate::assert_flag(convert))
  chimerism <- rlang::arg_match(chimerism)[[1L]]
  x_chr <- if (std) std_chr(x) else x
  # Convert missings
  for (na_pat in na_patterns) {
    x_chr[x_chr %like% na_pat] <- NA_character_
  }
  # Replace "10^x" with 1Ex
  x_chr <- stringr::str_replace_all(x_chr, "10\\^(-?[0-9]+)", "1E\\1")
  # Replace multiple decimals inside a number with single decimal
  x_chr <- stringr::str_replace_all(x_chr, "(?<=[0-9])\\.{2,}(?=[0-9])", ".")
  # Remove repeated leading zeros
  x_chr <- stringr::str_remove(x_chr, "(?<=^|[^0-9])(?:0+\\.)+(?=0+\\.)")
  # Remove additional decimals inside non-zero values
  x_chr <- stringr::str_remove_all(x_chr, "(?<=[0-9]{1,10}\\.[0-9]{1,10})\\.")
  # Remove trailing decimals
  x_chr <- stringr::str_remove(x_chr, "\\.$")
  # Remove comma-separators in single number
  x_chr <- stringr::str_remove_all(x_chr, "(?<=[0-9]),(?=[0-9])")
  # Replace numeric values coded as Excel dates with true value
  x_dt_mat <- stringr::str_match(x_chr, "^0?1/(\\d{1,2})/1900(?: (\\d{1,2})\\:(\\d{1,2}))?$")
  is_dt_num <- !is.na(x_dt_mat[, 1L])
  if (any(is_dt_num)) {
    x_dt_mat <- x_dt_mat[is_dt_num, 2:4, drop = FALSE]
    x_dt_mat <- suppressWarnings(as.numeric(x_dt_mat))
    x_dt_mat[is.na(x_dt_mat)] <- 0
    x_dt_mat <- matrix(x_dt_mat, ncol = 3L)
    x_chr[is_dt_num] <- as.character(rowSums(x_dt_mat))
  }
  # Remove percentages
  x_chr <- stringr::str_remove_all(x_chr, " ?%")
  # Convert per million
  is_ppm <- stringr::str_detect(x_chr, "PER MILLION")
  is_ppm[is.na(is_ppm)] <- FALSE
  if (any(is_ppm)) {
    x_chr[is_ppm] <- stringr::str_replace_all(x_chr[is_ppm], "([0-9.]+(?:E-?[0-9]+)?)(?:\\w|\\s)+PER MILLION", "\\1/1000000")
  }
  # Convert fractions
  is_frac <- stringr::str_detect(x_chr, "[0-9]/[0-9]")
  is_frac[is.na(is_frac)] <- FALSE
  if (any(is_frac)) {
    frac <- stringr::str_match(x_chr[is_frac], "([0-9.]+)/([0-9.]+)")[, 2:3, drop = FALSE]
    frac <- suppressWarnings(as.numeric(frac))
    frac <- matrix(frac, ncol = 2L)
    i_frac <- which(is_frac)
    for (i in length(i_frac)) {
      if (frac[i, 2L] >= 1) {
        x_chr[i_frac[[i]]] <- stringr::str_replace(
          x_chr[i_frac[[i]]],
          "([0-9.]+)/([0-9.]+)",
          as.character(frac[i, 1L] / frac[i, 2L])
        )
      }
    }
  }
  # Remove leading and trailing text where possible
  x_chr <- stringr::str_remove(x_chr, "^MRD BY NGS ?")
  # Convert `NEGATIVE`
  x_chr[x_chr == "NEGATIVE"] <- "0"
  # Convert POSITIVE and WEAKLY POSITIVE
  x_chr[x_chr %like% "^(?:WE[EA]KLY )?POSITIVE$"] <- ">0"
  # Convert complete remission (CR)
  x_chr <- stringr::str_replace(x_chr, ".*(?:^|\\W)IN CR(?:$|\\W).*", "0")
  # Extract donor/host numbers
  if (chimerism == "donor") {
    x_chr <- stringr::str_replace(x_chr, ".*D=? ?([0-9.]+).*", "\\1")
    x_chr <- stringr::str_replace(x_chr, ".*([0-9.]+) DONOR.*", "\\1")
  } else if (chimerism == "host") {
    x_chr <- stringr::str_replace(x_chr, ".*PT=? ?([0-9.]+).*", "\\1")
    x_chr <- stringr::str_replace(x_chr, ".*([0-9.]+) PATIENT.*", "\\1")
  }
  # Replace new empty strings with missings
  x_chr <- trimws(x_chr)
  x_chr[x_chr == ""] <- NA_character_
  # Convert (maybe) to numeric
  x_num <- if (convert) suppressWarnings(std_num(as.numeric(x_chr), std_chr = FALSE, warn = FALSE)) else x_chr
  if (warn) {
    not_converted <- unique(x[!is.na(x_chr) & is.na(x_num)])
    if (length(not_converted) > 0L) {
      not_converted <- paste0(
        '"', stringr::str_replace_all(not_converted, '"', '\\"'), '"',
        collapse = ", "
      )
      rlang::warn(paste0(
        "Not all character strings converted to class numeric.",
        " Values not converted were: ", not_converted
      ))
    }
  }
  return(x_num)
}


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
  x_op <- stringr::str_extract(x_num, "^[<>]")
  x_numeric <- stringr::str_remove(x_num, "^[<>]")
  x_numeric <- suppressWarnings(as.numeric(x_numeric))
  is_numeric_intvl1 <- !is.na(x_numeric)
  x_numeric <- as.character(round(x_numeric[is_numeric_intvl1], digits))
  x_op <- x_op[is_numeric_intvl1]
  x_op[is.na(x_op)] <- ""
  x_rtn[is_numeric_intvl1] <- paste0(x_op, x_numeric)
  # Handle intervals with 2 numbers and no operators
  is_intvl2_no_op <- !is_numeric_intvl1 & !x_num %like% "[<>()]" & x_num %like% "[0-9]-[0-9]"
  x_mat_intvl2_no_op <- stringr::str_match(x_num[is_intvl2_no_op], "([0-9.]+(?:[Ee]-?[0-9]+)?)-([0-9.]+(?:[Ee]-?[0-9]+)?)")[, -1L]
  x_mat_intvl2_no_op <- round(suppressWarnings(as.numeric(x_mat_intvl2_no_op)), digits)
  x_mat_intvl2_no_op <- matrix(as.character(x_mat_intvl2_no_op), ncol = 2L)
  x_intvl2_no_op <- paste0("[", x_mat_intvl2_no_op[, 1L], ",", x_mat_intvl2_no_op[, 2L], "]")
  x_intvl2_no_op[x_intvl2_no_op == "[NA,NA]"] <- NA_character_
  x_rtn[is_intvl2_no_op] <- x_intvl2_no_op
  # Handle intervals with 2 numbers and operators
  is_intvl2_op <- !(is_numeric_intvl1 | is_intvl2_no_op) & x_num %like% "[0-9]-[<>0-9]" & x_num %like% "[<>()]"
  x_mat_intvl2_op <- stringr::str_match(
    x_num[is_intvl2_op],
    "(\\()?([<>])?([0-9.]+(?:[Ee]-?[0-9]+)?)-([<>])?([0-9.]+(?:[Ee]-?[0-9]+)?)(\\))?"
  )[, -1L]
  x_mat_intvl2_op_num <- x_mat_intvl2_op[, c(3L, 5L)]
  x_mat_intvl2_op_num <- round(suppressWarnings(as.numeric(x_mat_intvl2_op_num)), digits)
  x_mat_intvl2_op_num <- matrix(as.character(x_mat_intvl2_op_num), ncol = 2L)
  x_mat_intvl2_op_op <- x_mat_intvl2_op[, -c(3L, 5L)]
  # Combine "<>" and "()" - prefer "()"
  x_mat_intvl2_op_op[, 1L] <- data.table::fcoalesce(x_mat_intvl2_op_op[, 1L], x_mat_intvl2_op_op[, 2L])
  x_mat_intvl2_op_op[, 4L] <- data.table::fcoalesce(x_mat_intvl2_op_op[, 4L], x_mat_intvl2_op_op[, 3L])
  x_mat_intvl2_op_op <- x_mat_intvl2_op_op[, c(1L, 4L)]
  x_mat_intvl2_op_op[x_mat_intvl2_op_op[, 1L] == ">", 1L] <- "("
  x_mat_intvl2_op_op[is.na(x_mat_intvl2_op_op[, 1L]), 1L] <- "["
  x_mat_intvl2_op_op[x_mat_intvl2_op_op[, 1L] == "<", 1L] <- "["
  x_mat_intvl2_op_op[x_mat_intvl2_op_op[, 2L] == "<", 2L] <- ")"
  x_mat_intvl2_op_op[is.na(x_mat_intvl2_op_op[, 2L]), 2L] <- "]"
  x_mat_intvl2_op_op[x_mat_intvl2_op_op[, 2L] == ">", 2L] <- "]"
  x_intvl2_op <- paste0(x_mat_intvl2_op_op[, 1L], x_mat_intvl2_op_num[, 1L], ",", x_mat_intvl2_op_num[, 2L], x_mat_intvl2_op_op[, 2L])
  x_intvl2_op[x_intvl2_op %in% c("[NA,NA]", "(NA,NA]", "[NA,NA)", "(NA,NA)")] <- NA_character_
  x_rtn[is_intvl2_op] <- x_intvl2_op
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
  if (warn) {
    not_converted <- unique(x[!is.na(x_chr) & is.na(x_rtn)])
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
  x_intvl2 <- stringr::str_match(x[is_intvl2], pat2)[, -1L]
  op_intvl2 <- x_intvl2[, c(1L, 4L)]
  x_intvl2 <- x_intvl2[, c(2L, 3L)]
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
#' @param ... Additional arguments to pass to
#'   \code{\link[janitor:convert_to_date]{convert_to_datetime()}}. These
#'   will, in turn, be passed to further methods, including
#'   \code{\link[janitor:excel_numeric_to_date]{excel_numeric_to_date()}},
#'   \code{\link[lubridate:parse_date_time]{parse_date_time()}}, and
#'   \code{\link[base:as.POSIXct]{as.POSIXct()}}.
#'
#' @return A `Date` or `POSIXct` vector
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
  # as_rlang_error(checkmate::assert_character(na))
  range_value <- rlang::arg_match(range_value)[[1L]]
  # as_rlang_error(checkmate::assert_character(range_sep))
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


#' Date Conversion Utilities
#'
#' @description
#'
#' Contains utilities for standardizing dates and datetimes
#'
#' @keywords internal
UtilsDate <- R6Class(
  "UtilsDate",
  cloneable = FALSE,
  public = list(
    #' Coerce Datetimes to Dates if No Information is Lost
    #'
    #' `dttm_to_dt()` converts `POSIXt` objects to `Date` objects when there is no
    #' additional information contained in the `POSIXt` format (i.e. there is no
    #' sub-daily information).
    #'
    #' Specifically, `dttm_to_dt` checks whether all sub-daily information is the
    #' same for each value in the datetime vector. If so, no additional information
    #' is gained by using a `POSIXt` format over the simpler `Date` format, and
    #' the data is coerced.
    #'
    #' If the input is scalar (i.e. has length `1L`), then no conversion is
    #' attempted
    #'
    #' @param x A `Date`, `POSIXct` or `POSIXlt` (i.e. a datetime) vector
    #'
    #' @param force Whether to force conversion to `Date` (`force = "dt"`) or
    #'   `POSIXct` (`force = "dttm"`). The default is no forcing (`force = "none"`).
    #'
    #' @param tz_heuristic Hours to consider in determining presence of sub-daily
    #'   information. Only exact hours (i.e. 5:00:00) will be combined. The default
    #'   corresponds to accidental encoding of the CST-UTC offset as hours.
    #'
    #' @return Either a `POSIXct` vector or a `Date` vector
    dttm_to_dt = function(x, force = c("none", "dt", "dttm"), tz_heuristic = c(0L, 5L, 6L)) {
      # If force is set, calculations aren't necessary
      force <- rlang::arg_match(force)[[1L]]
      checkmate::assert_integerish(
        tz_heuristic,
        lower = 0L,
        upper = 23L,
        any.missing = FALSE,
        null.ok = TRUE,
        min.len = 0L
      )
      if (force == "dttm") return(lubridate::as_datetime(x))

      # If `.x` is already `Date`, return as-is
      # If `.x` is scalar `POSIXt`, convert to `POSIXct` w/ TZ and return
      is_scalar <- vctrs::vec_size(x) == 1L
      if (lubridate::is.Date(x) && force != "dttm") {
        return(x)
      } else if ((lubridate::is.POSIXt(x) && is_scalar) || force == "dttm") {
        return(lubridate::as_datetime(x))
      }

      # Otherwise, check for any additional information in the variable
      t <- private$decimal_time(x)
      tz_heuristic <- as.double(tz_heuristic)
      if (!vctrs::vec_is_empty(tz_heuristic)) {
        t[t %in% tz_heuristic] <- tz_heuristic[[1L]]
      }
      t <- t[!is.na(t)]
      t1 <- if (vctrs::vec_is_empty(t)) NA_real_ else t[[1L]]

      if (all(t %in% t1)) {
        lubridate::as_date(x)
      } else if (force == "dt") {
        rlang::warn("Conversion to `Date` discarded sub-daily information")
        lubridate::as_date(x)
      } else {
        lubridate::as_datetime(x)
      }
    },
    #' Parse Dates in Character Format to Datetime Format
    #'
    #' `chr_to_dttm` standardizes a datetime vector in character format and returns
    #' a vector in `POSIXct` format. It is intended for use in `janitor::convert_to_datetime`;
    #' it thus muffles certain warnings that are duplicated by that function.
    #'
    #' @param x A vector of character dates
    #'
    #' @param tz Optional timezone
    #'
    #' @param orders The orders to use when parsing character vector with
    #'   \code{\link[lubridate:parse_date_time]{parse_date_time()}}
    #'
    #' @param ... Additional arguments to pass to
    #'   \code{\link[lubridate:parse_date_time]{parse_date_time()}}
    #'
    #' @return A `POSIXct` vector
    chr_to_dttm = function(
    x,
    tz = "UTC",
    orders = c("mdy",  "dmy",  "ymd",
               "mdyT", "dmyT", "ymdT",
               "mdyR", "dmyR", "ymdR",
               "mdyr", "dmyr", "ymdr",
               "mdyTz", "dmyTz", "ymdTz", "Tmdyz", "Tdmyz", "Tymdz",
               "mdyRz", "dmyRz", "ymdRz", "mdyrz", "dmyrz", "ymdrz",
               "Tmdy",  "Tdmy",  "Tymd",  "Tmdyz", "Tdmyz", "Tymdz"),
    train = TRUE,
    ...
    ) {
      withCallingHandlers(
        lubridate::parse_date_time(x, orders = orders, train = train, tz = tz, ...),
        warning = function(w) {
          if (stringr::str_detect(w$message, "failed to parse")) {
            rlang::cnd_muffle(w)
          } else {
            w
          }
        }
      )
    }
  ),
  private = list(
    # Extract Time as a Decimal of Hours
    #
    # @param x A vector with hour, minute, and/or second information that can be
    #   extracted by lubridate's `hour()`, `minute()`, and `second()` functions
    #   (respectively).
    #
    # @return A `double` vector of decimal hours
    decimal_time = function(x) {
      lubridate::hour(x) +
        lubridate::minute(x) / 60 +
        lubridate::second(x) / 3600
    }
  )
)$new()

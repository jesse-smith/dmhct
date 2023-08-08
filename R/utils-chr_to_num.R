#' Clean and Possibly Convert Numbers in `character` Representation
#'
#' `chr_to_num()` cleans strings representing numeric values. It is tailored for
#' use with the ML in HCT dataset; use with other data may not go as expected.
#'
#' The function first converts strings matching `na_patterns` to missing values.
#' It then simplifies any numeric representations it finds, including 10^x and
#' various common typos observed in the data (unneeded decimals, commas, and zeros).
#' Additionally, it converts Excel datetimes of the form 1/x/1900 hh:mm back to
#' decimal representation, and additionally converts fractions less than 1 to
#' decimals. Lastly, it handles a few idiosyncratic text strings, including
#' conversion of POSITIVE and NEGATIVE values, as well as W(E|A)KLY POSITIVE
#' and IN CR (complete remission). Optionally, it will extract values labelled as
#' donor or host (patient), which is specific to the chimerism dataset. It also
#' removes the leading text MRD BY NGS. By default, the function emits a warning
#' when potential numeric values are not able to be converted to `numeric`.
#'
#' @param x A `character` vector
#' @param std Whether to standardize the vector before cleaning and converting
#' @param convert Whether to actually convert to `numeric`
#' @param na Regular expressions to convert to `NA`
#' @param replace A `data.frame` of regular expressions and strings to replace
#'   them; regular expression should be in a column named `pattern`, and
#'   replacements should be in a column named `replacement`. Each row is passed
#'   to `stringr::str_replace()`.
#' @param per_action How to treat %/percent/per million/etc labels. `drop` simply
#'   removes the labels, `divide` divides the value by the appropriate denominator,
#'   and `ignore` does nothing.
#' @param multiple_decimals How to handle multiple decimals within a number
#' @param donor_host Which value to use when values for both a donor and a host
#'   are given
#'
#' @return A `numeric` or `character` vector, depending on the value of `convert`
#'
#' @keywords internal
chr_to_num <- function(
    x,
    std = TRUE,
    warn = TRUE,
    convert = TRUE,
    na = na_patterns,
    replace = data.frame(
      pattern = c("MRD BY NGS ?", "^NEGATIVE$", "^(?:WE[EA]KLY )?POSITIVE$", ".*\\bIN CR\\b.*"),
      replacement = c("", "0", ">0", "0")
    ),
    per_action = c("drop", "divide", "ignore"),
    multiple_decimals = c("use_first", "use_last", "ignore"),
    donor_host = c("use_donor", "use_host", "ignore")
) {
  as_rlang_error(checkmate::assert_flag(std))
  as_rlang_error(checkmate::assert_flag(warn))
  as_rlang_error(checkmate::assert_flag(convert))
  per_action <- rlang::arg_match(per_action)[[1L]]
  multiple_decimals <- rlang::arg_match(multiple_decimals)[[1L]]
  donor_host <- rlang::arg_match(donor_host)[[1L]]
  x_chr <- if (std) std_chr(x, na = na) else str_to_na(x, pattern = na)
  # Replace "10^x" with 1Ex
  x_chr <- str_replace_all_vec(x_chr, "10\\^(-?[0-9]+)", "1E\\1")
  # Remove comma-separators in single number
  x_chr <- str_remove_all_vec(x_chr, "(?<=[0-9]),(?=[0-9])")
  # Standardize decimals
  x_chr <- UtilsChrToNum$std_decimals(x_chr, multiple = multiple_decimals)
  # Replace numeric values coded as Excel dates with true value
  x_chr <- UtilsChrToNum$excel_dt_to_num(x_chr)
  # Remove %/percent/per million/etc
  x_chr <- UtilsChrToNum$std_per(x_chr, action = per_action)
  # Convert fractions
  x_chr <- UtilsChrToNum$std_frac(x_chr)
  # Replace select expressions
  x_chr <- str_replace_vec(x_chr, replace$pattern, replace$replacement)
  # Extract donor/host numbers
  x_chr <- UtilsChrToNum$std_donor_host(x_chr, action = donor_host)
  # Trim whitespace and replace new empty strings with missings
  x_chr <- trimws(x_chr)
  x_chr[x_chr == ""] <- NA_character_
  # Convert (maybe) to numeric
  x_num <- if (convert) suppressWarnings(std_num(as.numeric(x_chr), std_chr = FALSE, warn = FALSE)) else x_chr
  if (warn) {
    not_converted <- unique(x[!is.na(x_chr) & is.na(x_num)])
    if (length(not_converted) > 0L) {
      not_converted <- paste0('"', not_converted, '"', collapse = ", ")
      rlang::warn(paste0(
        "Not all character strings converted to class numeric.",
        " Values not converted were: ", not_converted
      ))
    }
  }
  return(x_num)
}

UtilsChrToNum <- R6Class(
  "UtilsChrToNum",
  cloneable = FALSE,
  public = list(
    std_decimals = function(x, multiple = c("use_first", "use_last", "ignore")) {
      multiple <- rlang::arg_match(multiple)[[1L]]
      # Replace multiple decimals in a row
      x <- stringr::str_replace_all(x, "\\.{2,}", ".")
      # Remove trailing decimals
      x <- stringr::str_remove_all(x, "(?<=[0-9])\\.(?=$|\\b)")
      # Handle multiple decimals with numbers between them
      if (multiple == "use_first") {
        str_replace_all_vec(x, "(?<=\\.)(?:([0-9]+)\\.)+", "\\1")
      } else if (multiple == "use_last") {
        str_replace_all_vec(x, "(?:([0-9]+)\\.)+(?=[0-9]+\\.)", "\\1")
      } else if (multiple == "ignore") {
        x
      } else {
        rlang::abort(paste0("`multiple = '", multiple, "'` has not yet been implemented"))
      }
    },
    std_per = function(x, action = c("drop", "divide", "ignore")) {
      action <- rlang::arg_match(action)[[1L]]
      if (action == "drop") {
        str_remove_all_vec(x, "(?i)(?<=[0-9])[\\b\\s]*%+|percent|per (?:hundred|thousand|million|billion|trillion)")
      } else if (action == "divide") {
        x <- str_replace_all_vec(
          x,
          c(
            "(?i)(?<=[0-9])[\\b\\s]*(?:%+|percent|per hundred)",
            "(?i)(?<=[0-9])[\\b\\s]*per thousand",
            "(?i)(?<=[0-9])[\\b\\s]*per million",
            "(?i)(?<=[0-9])[\\b\\s]*per billion",
            "(?i)(?<=[0-9])[\\b\\s]*per trillion"
          ),
          c(
            "/100",
            "/1E3",
            "/1E6",
            "/1E9",
            "/1E12"
          )
        )
        x
      } else if (action == "ignore") {
        x
      } else {
        rlang::abort(paste0("`action = '", action, "'` has not yet been implemented"))
      }
    },
    std_frac = function(x) {
      is_frac <- str_detect_vec(x, "[0-9]/[0-9]")
      is_frac[is.na(is_frac)] <- FALSE
      if (any(is_frac)) {
        # Extract numerator and denominator
        frac <- stringr::str_match(x[is_frac], "([0-9.]+)/([0-9.]+)")[, -1L, drop = FALSE]
        # Convert to numeric
        frac <- matrix(suppressWarnings(as.numeric(frac)), ncol = 2L)
        # Find fraction locations in `x_chr`
        i_frac <- which(is_frac)
        # Round conversion to machine precision (floor to avoid roundoff issues with last decimal place)
        digits <- floor(abs(log10(.Machine$double.eps)))
        # Replace each fraction with decimal equivalent
        for (i in length(i_frac)) {
          if (frac[i, 2L] >= 1) {
            x[i_frac[[i]]] <- stringr::str_replace(
              x[i_frac[[i]]],
              "([0-9.]+)/([0-9.]+)",
              as.character(round(frac[i, 1L] / frac[i, 2L], digits))
            )
          }
        }
      }
      x
    },
    std_donor_host = function(x, action = c("use_donor", "use_host", "ignore")) {
      action <- rlang::arg_match(action)[[1L]]
      if (action == "use_donor") {
        # Match D=x pattern (or Dx) and  Match x DONOR pattern
        str_replace_vec(
          x,
          c(".*(?:D|DONOR)=? ?([0-9.]+).*", ".*([0-9.]+) DONOR.*"),
          "\\1"
        )
      } else if (action == "use_host") {
        # Match PT=x pattern (or PTx) and Match x PATIENT pattern
        stringr::str_replace(
          x,
          c(".*(?:PT|PATIENT|HOST)=? ?([0-9.]+).*", ".*([0-9.]+) (?:PATIENT|HOST).*"),
          "\\1"
        )
      } else if (action == "ignore") {
        x
      } else {
        rlang::abort(paste0("`action = '", action, "'` has not yet been implemented"))
      }
    },
    excel_dt_to_num = function(x) {
      x_dt_mat <- stringr::str_match(x, "^0?1/(\\d{1,2})/1900(?: (\\d{1,2})\\:(\\d{1,2}))?$")
      is_dt_num <- !is.na(x_dt_mat[, 1L])
      if (any(is_dt_num)) {
        # Extract day, hour, and minute from groups matched above
        x_dt_mat <- x_dt_mat[is_dt_num, -1L, drop = FALSE]
        # Convert to numeric
        x_dt_mat <- suppressWarnings(as.numeric(x_dt_mat))
        # Fill missings as zero (don't add to total)
        x_dt_mat[is.na(x_dt_mat)] <- 0
        # Convert back to matrix (now numeric)
        x_dt_mat <- matrix(x_dt_mat, ncol = 3L)
        # Excel will consider the value to be decimal dates and convert to M/D/Y hh:mm - convert back to decimal
        x_dt <- x_dt_mat[, 1L] + x_dt_mat[, 2L] / 24 + x_dt_mat[, 3L] / (24 * 60)
        # Round conversion to machine precision (floor to avoid roundoff issues with last decimal place)
        digits <- floor(abs(log10(.Machine$double.eps)))
        # Convert back to character
        x[is_dt_num] <- as.character(round(x_dt, digits))
      }
      x
    }
  )
)$new()

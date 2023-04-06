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
#' @param na_patterns Regular expressions to convert to `NA`
#' @param chimerism The type of chimerism to extract, if any
#'
#' @return A `numeric` or `character` vector
#'
#' @keywords internal
chr_to_num <- function(
    x, std = TRUE, warn = TRUE, convert = TRUE,
    na_patterns = c(
      "^-*$", # Includes ""
      "(?:^|\\W)N/?A(?:$|\\W)",
      "NOT? (?:RER?PORT|(?:[A-Z ]|-)*PHENOTYPE)",
      "NOT? (?:AMP|AVAIL|DATA|DONE|EVAL|FOLLOWED|IDENT|VALUE)",
      "(?:^|\\W)(?:NG|UNK|CQNS|QNS|QIS|NSQ|IQ|QI|NR)(?:$|\\W)",
      "(?:^|\\W)(?:UNKNOWN|INCONCLUSIVE|INSUFFICIENT)(?:$|\\W)",
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
    x_chr[is_dt_num] <- as.character(round(x_dt, digits))
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
    # Extract numerator and denominator
    frac <- stringr::str_match(x_chr[is_frac], "([0-9.]+)/([0-9.]+)")[, -1L, drop = FALSE]
    # Convert to numeric
    frac <- matrix(suppressWarnings(as.numeric(frac)), ncol = 2L)
    # Find fraction locations in `x_chr`
    i_frac <- which(is_frac)
    # Round conversion to machine precision (floor to avoid roundoff issues with last decimal place)
    digits <- floor(abs(log10(.Machine$double.eps)))
    # Replace each fraction with decimal equivalent
    for (i in length(i_frac)) {
      if (frac[i, 2L] >= 1) {
        x_chr[i_frac[[i]]] <- stringr::str_replace(
          x_chr[i_frac[[i]]],
          "([0-9.]+)/([0-9.]+)",
          as.character(round(frac[i, 1L] / frac[i, 2L], digits))
        )
      }
    }
  }
  # Remove leading and trailing text where possible
  x_chr <- stringr::str_remove(x_chr, "^MRD BY NGS ?")
  # Convert `NEGATIVE`
  x_chr[x_chr == "NEGATIVE"] <- "0"
  # Convert POSITIVE and W(E|A)EKLY POSITIVE [sic]
  x_chr[x_chr %like% "^(?:WE[EA]KLY )?POSITIVE$"] <- ">0"
  # Convert complete remission (IN CR)
  x_chr <- stringr::str_replace(x_chr, ".*(?:^|\\W)IN CR(?:$|\\W).*", "0")
  # Extract donor/host numbers
  if (chimerism == "donor") {
    # Match D=x pattern (or Dx)
    x_chr <- stringr::str_replace(x_chr, ".*D=? ?([0-9.]+).*", "\\1")
    # Match x DONOR pattern
    x_chr <- stringr::str_replace(x_chr, ".*([0-9.]+) DONOR.*", "\\1")
  } else if (chimerism == "host") {
    # Match PT=x pattern (or PTx)
    x_chr <- stringr::str_replace(x_chr, ".*PT=? ?([0-9.]+).*", "\\1")
    # Match x PATIENT pattern
    x_chr <- stringr::str_replace(x_chr, ".*([0-9.]+) PATIENT.*", "\\1")
  }
  # Trim whitespace and replace new empty strings with missings
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

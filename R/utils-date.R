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

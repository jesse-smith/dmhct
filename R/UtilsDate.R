UtilsDate <- R6Class(
  "UtilsDate",
  cloneable = FALSE,
  public = list(
    #' Parse Dates to Standard Format
    #'
    #' `std_dates` standardizes a date vector and returns a vector in `Date` or
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
    #'   `???All formats failed to parse`??? error.See notes
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
    ...
    ) {
      checkmate::assert_flag(warn)
      if (warn) {
        x <- janitor::convert_to_datetime(
          x,
          orders = orders,
          train = train,
          ...,
          character_fun = private$chr_to_dttm,
          string_conversion_failure = "warning"
        )
        self$dttm_to_dt(x, force = force)
      } else {
        suppressWarnings(self$std_date(
          x, force = force, tz_heuristic = tz_heuristic, warn = TRUE,
          train = train, orders = orders, ...
        ))
      }
    },

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
    },

    # Parse Dates in Character Format to Datetime Format
    #
    # `chr_to_dttm` standardizes a datetime vector in character format and returns
    # a vector in `POSIXct` format.
    #
    # @param x A vector of character dates
    #
    # @param tz Optional timezone
    #
    # @param orders The orders to use when parsing character vector with
    #   \code{\link[lubridate:parse_date_time]{parse_date_time()}}
    #
    # @inheritParams lubridate::parse_date_time
    #
    # @param ... Additional arguments to pass to
    #   \code{\link[lubridate:parse_date_time]{parse_date_time()}}
    #
    # @return A `POSIXct` vector
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
      x %>%
        stringr::str_replace(pattern = "^$", replacement = NA_character_) %>%
        lubridate::parse_date_time(orders = orders, train = train, tz = tz, ...) %>%
        lubridate::as_datetime()
    }
  )
)

utils_date <- UtilsDate$new()

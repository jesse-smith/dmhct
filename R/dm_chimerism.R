#' Extract Chimerism Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to SQL Server w/ HCT data
#'
#' @return The `dm` with instructions for updating the `chimerism` table
#'
#' @export
dm_chimerism_extract <- function(dm_remote) {
  na <- c(
    "", "-9996", "-9999", "NA", "N/A", "na", "n/a", "No Data", "no data",
    "QNS", "CQNS", "NSQ", "IQ", "QI", "QIS", "qns", "cqns", "nsq", "iq", "qi", "qis",
    "NR", "nr", "not reported", "Not Reported", "not rerported", "reported", "Reported",
    "insufficient", "Insufficient", "unk", "UNK", "unknown", "Unknown", "no amp", "No Amp",
    "d2 & pt= 4", "0-100"
  )
  dm_remote %>%
    dm::dm_zoom_to("chimerism") %>%
    dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
    dplyr::semi_join(master, by = "entity_id") %>%
    dplyr::transmute(
      .data$entity_id,
      dt_trans = dbplyr::sql("CONVERT(DATETIME, [Date of Transplant])"),
      dt_chimerism = dbplyr::sql("CONVERT(DATETIME, [Chimerism_Date])"),
      cat_cell_sep = trimws(as.character(.data[["Cell Separation"]])),
      cat_cell_sep = dplyr::if_else(
        .data$cat_cell_sep %in% {{ na }}, NA_character_, .data$cat_cell_sep
      ),
      cat_method = trimws(as.character(.data$Method)),
      cat_method = dplyr::if_else(
        .data$cat_method %in% {{ na }}, NA_character_, .data$cat_method
      ),
      pct_donor = trimws(as.character(.data[["Donor%"]])),
      pct_donor = dplyr::if_else(
        .data$pct_donor %in% {{ na }}, NA_character_, .data$pct_donor
      ),
      pct_host = trimws(as.character(.data[["Host%"]])),
      pct_host = dplyr::if_else(
        .data$pct_host %in% {{ na }}, NA_character_, .data$pct_host
      )
    ) %>%
    dplyr::filter(
      !is.na(.data$entity_id),
      !is.na(.data$dt_trans),
      !is.na(.data$dt_chimerism),
      !(is.na(.data$pct_donor) & is.na(.data$pct_host)),
      .data$dt_chimerism >= .data$dt_trans
    ) %>%
    dm::dm_update_zoomed()
}


#' Transform the Chimerism Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#'
#' @return The transformed `dm`
#'
#' @export
dm_chimerism_transform <- function(dm_local) {
  dt <- dm_local$chimerism
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)


  # Silence R CMD CHECK Notes
  cat_cell_sep <- cat_method <- pct_donor <- pct_host <- NULL
  pct_donor_tmp <- pct_host_tmp <- dt_trans <- dt_chimerism <- NULL
  delta_donor <- delta_host <- ..pred_donor <- ..pred_host <- NULL
  dist_keep <- dist_swap <- tmp_donor <- cat_method1 <- NULL

  # Dates
  dt_cols <- c("dt_trans", "dt_chimerism")
  dt[, c(dt_cols) := lapply(.SD, lubridate::as_date), .SDcols = dt_cols]

  # Filter unused patients w/ master
  dt_master <- data.table::as.data.table(dm_local$master)
  data.table::set(
    dt_master,
    j = setdiff(colnames(dt_master), c("entity_id", "dt_trans")),
    value = NULL
  )
  dt <- dt[dt_master, on = c("entity_id", "dt_trans"), nomatch = NULL]
  rm(dt_master)

  # Cell Source
  dt[, "cat_cell_sep" := data.table::fcase(
    utils_chimerism$str_detect_fct(cat_cell_sep, 1L, "bone\\s+marrow"), "Bone Marrow",
    utils_chimerism$str_detect_fct(cat_cell_sep, 2L, "peripheral\\s+blood"), "Peripheral Blood",
    utils_chimerism$str_detect_fct(cat_cell_sep, 11L, "unsorted"), "Peripheral Blood",
    utils_chimerism$str_detect_fct(cat_cell_sep, 3L, "t\\s*-?\\s*cell"), "T Cells",
    # utils_chimerism$str_detect_fct(cat_cell_sep, 4L, "b\\s*-?\\s*cell"), "B Cells",
    # utils_chimerism$str_detect_fct(cat_cell_sep, 6L, "monocyte"), "Monocytes",
    # utils_chimerism$str_detect_fct(cat_cell_sep, 7L, "neutrophil"), "Neutrophils",
    # utils_chimerism$str_detect_fct(cat_cell_sep, 9L, "nk\\s*-?\\s*cell"), "NK Cells",
    # utils_chimerism$str_detect_fct(cat_cell_sep, 10L, "myeloid"), "Myeloid Cells",
    # !is.na(cat_cell_sep), "Other",
    default = NA_character_
  )]
  dt[, "cat_cell_sep" := factor(cat_cell_sep, levels = c(
    "Bone Marrow",
    "Peripheral Blood",
    "T Cells"
    # "B Cells",
    # "NK Cells",
    # "Monocytes",
    # "Neutrophils",
    # "Myeloid Cells",
    # "Other"
  ))]

  # Chimerism Method
  dt[, "cat_method" := data.table::fcase(
    utils_chimerism$str_detect_fct(cat_method, 1L, "standard\\s+cytogenetics"), "Standard Cytogenetics",
    utils_chimerism$str_detect_fct(cat_method, 2L, "FISH"), "FISH",
    utils_chimerism$str_detect_fct(cat_method, 3L, "RFLP"), "RFLP",
    utils_chimerism$str_detect_fct(cat_method, 4L, "PCR"), "PCR",
    utils_chimerism$str_detect_fct(cat_method, 5L, "hla\\s+serotyping"), "HLA Serotyping",
    utils_chimerism$str_detect_fct(cat_method, 6L, "VNTR"), "VNTR",
    !is.na(cat_method), "Other",
    default = NA_character_
  )]
  dt[, "cat_method" := factor(cat_method, levels = c(
    "VNTR",
    "RFLP",
    "PCR",
    "FISH",
    "HLA Serotyping",
    "Standard Cytogenetics",
    "Other"
  ))]

  # Percentages
  # Clean
  dt[, "pct_donor" := stringr::str_remove_all(pct_donor, "[`()%]")]
  dt[, "pct_donor" := stringr::str_squish(pct_donor)]
  dt[, "pct_host" := stringr::str_remove_all(pct_host, "[`()%]")]
  dt[, "pct_host" := stringr::str_remove(pct_host, "(?i)[0-9]+\\s+donor\\s+[0-9]*\\s*,\\s*")]
  dt[, "pct_host" := stringr::str_remove(pct_host, "(?i)\\s*patient\\s*")]
  dt[, "pct_host" := stringr::str_squish(pct_host)]

  # Fill missings using complements where available
  dt[, "pct_donor_tmp" := suppressWarnings(as.numeric(pct_donor))]
  dt[, "pct_host_tmp" := suppressWarnings(as.numeric(pct_host))]
  dt[pct_donor_tmp < 0 | 100 < pct_donor_tmp, c("pct_donor", "pct_donor_tmp") := list(NA_character_, NA_real_)]
  dt[pct_host_tmp < 0 | 100 < pct_host_tmp, c("pct_host", "pct_host_tmp") := list(NA_character_, NA_real_)]
  dt[is.na(pct_donor_tmp) & !is.na(pct_host_tmp), pct_donor_tmp := 100 - pct_host_tmp]
  dt[is.na(pct_host_tmp) & !is.na(pct_donor_tmp), pct_host_tmp := 100 - pct_donor_tmp]

  # 1/0/1900 is numeric 0 in Excel dates
  dt[is.na(pct_host_tmp) & pct_host %like% "1\\s*/\\s*0\\s*/\\s*1900", pct_host_tmp := 0]
  dt[is.na(pct_donor_tmp) & pct_donor %like% "1\\s*/\\s*0\\s*/\\s*1900", pct_donor_tmp := 0]

  # Get middle of ranges
  dt[is.na(pct_donor_tmp), "pct_donor_tmp" := utils_chimerism$str_range_to_num(pct_donor)]
  dt[is.na(pct_host_tmp), "pct_host_tmp" := utils_chimerism$str_range_to_num(pct_host)]

  # Fill using extracted complements
  dt[is.na(pct_donor_tmp) & !is.na(pct_host_tmp), pct_donor_tmp := 100 - pct_host_tmp]
  dt[is.na(pct_host_tmp) & !is.na(pct_donor_tmp), pct_host_tmp := 100 - pct_donor_tmp]

  # Overwrite originals
  dt[, c("pct_donor", "pct_host") := list(pct_donor_tmp, pct_host_tmp)]
  dt[, c("pct_donor_tmp", "pct_host_tmp") := NULL]

  # Drop missing measurements
  dt <- dt[!is.na(pct_donor)]

  # Sort
  data.table::setorderv(dt, na.last = TRUE)

  # Set primary key
  pk <- c("entity_id", "dt_chimerism")
  data.table::setkeyv(dt, pk)

  # Calculate largest differences within patient + date + cell type groupings
  dt[, c("delta_donor", "delta_host") := list(
    apply(abs(outer(pct_donor, pct_donor, `-`)), 1L, max),
    apply(abs(outer(pct_host, pct_host, `-`)), 1L, max)
  ), by = c("entity_id", "dt_chimerism", "cat_cell_sep")]

  # Tidy factors for modeling
  dt[, c("cat_cell_sep_lf", "cat_method_lf") := lapply(.SD, function(x) {
    forcats::fct_lump_min(forcats::fct_explicit_na(x), min = 100L)
  }), .SDcols = c("cat_cell_sep", "cat_method")]
  # Add normalized predictors for modeling
  dt[, c("t_trans", "t") := list(
    utils_chimerism$normalize(dt_trans),
    utils_chimerism$normalize(dt_chimerism - dt_trans)
  )]

  # Convert to data.frame for lme4
  data.table::setDF(dt)

  # Model selection by BIC
  # Fit all combinations of entity_id & cat_cell_sep effects
  # (fixed, random intercept, random slope, random slope + intercept for each)
  # [entity_id: random slope + intercept, cat_cell_sep: fixed] was best model

  # Predict donor
  pred_donor <- utils_chimerism$inv_probit(stats::predict(lme4::lmer(
    utils_chimerism$probit(pct_donor) ~ t_trans + cat_cell_sep_lf + cat_method_lf + (t | entity_id),
    data = dt[dt$delta_donor < 5,]
  ), newdata = dt[dt$delta_donor >= 5,]))
  # Predict host
  pred_host <- utils_chimerism$inv_probit(stats::predict(lme4::lmer(
    utils_chimerism$probit(pct_host) ~ t_trans + cat_cell_sep_lf + cat_method_lf + (t | entity_id),
    data = dt[dt$delta_host < 5,]
  ), newdata = dt[dt$delta_host >= 5,]))

  # Convert back to data.table
  data.table::setDT(dt)

  # Prediction differences
  dt[, c("dist_keep", "dist_swap") := 0]
  dt[delta_donor >= 5, c("dist_keep", "dist_swap") := list(
    abs(..pred_donor - pct_donor),
    abs(..pred_donor - pct_host)
  )]
  dt[delta_host >= 5, c("dist_keep", "dist_swap") := list(
    dist_keep + abs(..pred_host - pct_host),
    dist_swap + abs(..pred_host - pct_donor)
  )]

  # Swap if doing so lowers average distance between observation and prediction
  dt[, "tmp_donor" := pct_donor]
  dt[dist_swap < dist_keep, c("pct_donor", "pct_host") := list(pct_host, tmp_donor)]

  # Remove modeling variables
  dt[, c("delta_donor", "delta_host", "cat_cell_sep_lf", "cat_method_lf") := NULL]
  dt[, c("t", "t_trans", "dist_keep", "dist_swap", "tmp_donor") := NULL]

  # Remove missing cell type
  dt <- dt[!is.na(cat_cell_sep)]

  # Prefer highest ranked method available in group (ranked in factor definition)
  dt[, "cat_method1" := cat_method[[1L]], by = c(pk, "cat_cell_sep")]
  dt <- dt[cat_method %in% cat_method1][, "cat_method1" := NULL]

  # Get average within group
  dt <- dt[, list(
    cat_method = cat_method[[1L]],
    pct_donor = mean(c(pct_donor, 100 - pct_host))
  ), by = c(pk, "cat_cell_sep")]

  # # Logic (derived from Akshay's response on 2022-09-02 and call on 2022-09-08)
  # - Bone marrow and peripheral blood are separate X
  # - Unsorted and peripheral blood are the same X
  # - T Cell is separate; drop remaining specific cell lineages X
  # - VNTR preferred over PCR preferred over FISH X
  # - Std. Cyto., HLA Sero., and FISH are similar
  # - RFLP is similar to VNTR

  # Reset key
  data.table::setkeyv(dt, pk)

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("chimerism") %>%
    dm::dm_add_tbl(chimerism = dt[])
}

#' Utility Functions for Chimerism Table ELT
#'
#' @description
#' Collection of utility functions for chimerism data
#'
#' @aliases utils_chimerism
#'
#' @keywords internal
UtilsChimerism <- R6Class(
  "UtilsChimerism",
  cloneable = FALSE,
  public = list(
    #' Detect Factor Level in Chimerism Data
    #'
    #' @param x `[character]` A character vector
    #' @param lvl `[character(1)]` The level's numeric representation
    #' @param lbl `[character(1)]` The level's label
    #'
    #' @return `[logical]` A logical indicating presence or absence of the
    #'   indicated factor level
    str_detect_fct = function(x, lvl, lbl) {
      stringr::str_detect(
        x,
        paste0("(?i)(^", lvl, "\\s*=)|(", lbl, ")")
      )
    },
    #' Convert a % Range to It's Midpoint
    #'
    #' @param x `[character]` Character representation of a range of percentages
    #'
    #' @return `[numeric]` The midpoint of each range of values
    str_range_to_num = function(x) {
      x %>%
        stringr::str_replace("<=?", "0-") %>%
        stringr::str_replace(">=?", "100-") %>%
        stringr::str_split("\\s*-\\s*") %>%
        lapply(as.numeric) %>%

        vapply(function(num) mean(as.numeric(num)), double(1L))
    },
    #' Normalize a `numeric` Vector to `[0, 1]`
    #'
    #' @param x `[numeric]` A `numeric` vector or a vector than can be coerced to `numeric`
    #'
    #' @return `[double]` The normalized vector
    normalize = function(x) {
      x <- as.numeric(x)
      r <- range(x, na.rm = TRUE)
      (x - r[[1L]]) / (r[[2L]] - r[[1L]])
    },
    #' Transform a `numeric` Vector Using the Probit Function
    #'
    #' @description
    #' Transforms a `numeric` vector using the Gaussian quantile function
    #' \code{\link[stats:Normal]{qnorm()}}. Converts the vector to the
    #' `[0, 1]` range using `range` prior to transformation. Since `qnorm(0)`
    #' and `qnorm(1)` are infinite, values closer than `tol` to `0` or `1` are
    #' truncated before transformation.
    #'
    #' @param x `[numeric]` A vector to transform
    #' @param range `[numeric(2)]` The lower and upper bounds of the domain of `x`
    #' @param tol `[numeric(1)]` The tolerance for truncating `x` on the `[0, 1]`
    #'   scale; must be between `0` and `0.01`.
    #' @return `[numeric]` The transformed vector
    probit = function(x, range = c(0, 100), tol = sqrt(.Machine$double.eps)) {
      checkmate::assert_numeric(
        range,
        finite = TRUE, any.missing = FALSE, len = 2L, unique = TRUE, sorted = TRUE
      )
      checkmate::assert_number(tol, lower = 0, upper = 0.01, finite = TRUE)
      range <- as.numeric(range)
      if (any(!x %between% range, na.rm = TRUE)) {
        warning("`x` contains values outside of `range`; these values will be truncated by `logit()`")
      }
      p <- (x - range[[1L]]) / (diff(range))
      p <- pmax(tol, pmin(1 - tol, p, na.rm = TRUE), na.rm = TRUE)
      qnorm(p)
    },
    #' Transform a `numeric` Vector Using the Inverse Probit Function
    #'
    #' @description
    #' Transforms a `numeric` vector using the Gaussian distribution function
    #' \code{\link[stats:Normal]{pnorm()}}. As the inverse of `probit()`, also
    #' inverts truncation by replacing transformed values closer than `tol` to
    #' `0` or `1` with `0` or `1` (respectively). Maps result to the domain
    #' specified by `range`; values outside of `range` will be truncated.
    #'
    #' @param x `[numeric]` A vector to transform
    #' @param range `[numeric(2)]` The lower and upper bounds of the range of the result
    #' @param tol `[numeric(1)]` The tolerance used for truncating in the `probit` transform
    #'
    #' @return `[numeric]` The transformed vector
    inv_probit = function(x, range = c(0, 100), tol = sqrt(.Machine$double.eps)) {
      checkmate::assert_numeric(
        range,
        finite = TRUE, any.missing = FALSE, len = 2L, unique = TRUE, sorted = TRUE
      )
      checkmate::assert_number(tol, lower = 0, upper = 0.01, finite = TRUE)
      range <- as.numeric(range)
      p <- pnorm(x)
      if (any(!p %between% c(0, 1), na.rm = TRUE)) {
        warning("`x` contains values outside of [0, 1] when transformed; these values will be truncated by `logistic()`")
      }
      p <- data.table::fifelse(p < tol, 0, p)
      p <- data.table::fifelse(p > (1 - tol), 1, p)
      p * diff(range) + range[[1L]]
    }
  )
)

#' @rdname UtilsChimerism
#' @usage NULL
#' @format NULL
#' @keywords internal
utils_chimerism <- UtilsChimerism$new()

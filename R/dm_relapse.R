#' Standardize Relapse Table Column Types
#'
#' Ensures that all columns are of the expected `class`, that all `character`
#' variables are standardized, and that all missings encoded as non-`NA` values
#' are recoded to `NA`.
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#'
#' @return `[dm]` The `dm` object with standardized Relapse table
#'
#' @export
dm_relapse_std <- function(dm_local) {
  dt <- dm_local$relapse
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD CHECK notes
  cat_add_tx <- dt_remission <- entity_id <- lgl_add_tx <- lgl_remission <- NULL

  # Standardize columns loaded as character variables
  chr_cols <- purrr::map_lgl(dt, is.character)
  chr_cols <- names(chr_cols[chr_cols])
  dt[, (chr_cols) := lapply(.SD, std_chr), .SDcols = chr_cols]

  # Convert missing values encoded as something else
  na <- c("", "UNKNOWN", "UNKNOWN AT THIS TIME", "N/A", "NA")
  dt[, (chr_cols) := lapply(.SD, function(x) {
    data.table::fifelse(x %in% ..na, NA_character_, x)
  }), .SDcols = chr_cols]

  # Ensure `date` is `Date` (NOTE: MAY CHANGE TO DATETIME/POSIXct IN THE FUTURE)
  dt[, "date" := utils_date$std_date(date, force = "dt", warn = FALSE)]
  # Ensure `dt_remission` is date (warn if info loss results)
  dt[, "dt_remission" := utils_date$std_date(dt_remission, force = "dt")]

  # Ensure `lgl_remission` is `logical`
  dt[, "lgl_remission" := data.table::fcase(
    lgl_remission %chin% c("YES", "Y"), TRUE,
    lgl_remission %chin% c("NO", "N"), FALSE,
    default = NA
  )]
  # Ensure `lgl_add_tx` is `logical`
  dt[, "lgl_add_tx" := data.table::fcase(
    lgl_add_tx %chin% c("YES", "Y"), TRUE,
    lgl_add_tx %chin% c("NO", "N"), FALSE,
    default = NA
  )]

  # Standardize multi-membership categories
  mcat_cols <- colnames(dt)[startsWith(colnames(dt), "mcat_")]
  dt[, c(mcat_cols) := lapply(.SD, utils_relapse$std_mcat), .SDcols = mcat_cols]

  # Set primary keys
  pk <- c("entity_id", "date")
  # Set data.table key
  data.table::setkeyv(dt, pk)

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Ensure timestamp is retained
  attr(dt, "timestamp") <- attr(dm_local$relapse, "timestamp")

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("relapse") %>%
    dm::dm_add_tbl(relapse = dt[])
}


#' Clean the Relapse Table in a Local `dm`
#'
#' Cleans contents of columns in the relapse table (currently does nothing).
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#'
#' @return `[dm]` The `dm` object w/ transformed `relapse` table
#'
#' @export
dm_relapse_clean <- function(dm_local) {
  # Do nothing for now
  dm_local
}


#' Removes Unidentifiable and Duplicate Observations in the Relapse Table
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#' @param quiet `[lgl]` Whether filtering should be done silently; if `FALSE`,
#'   will show summary message of filtering results.
#'
#' @return `[dm]` The `dm` object w/ filtered `relapse` table
#'
#' @export
dm_relapse_filter <- function(dm_local, quiet = TRUE) {
  dt <- dm_local$relapse
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD Notes
  entity_id <- NULL

  if (!quiet) {
    n_obs <- NROW(dt)
    n_pat <- length(unique(dt$entity_id))
  }
  # Remove patients not in master or missing
  dt <- dt[!is.na(entity_id) & entity_id %in% unique(dm_local$master$entity_id)]
  # Remove observations with no date information
  dt <- dt[!is.na(date)]
  # Remove observations with empty observation variables
  cols <- setdiff(colnames(dt), c("entity_id", "date"))
  has_obs <- rowSums(!is.na(dt[, ..cols])) > 0L
  dt <- dt[has_obs]
  if (!quiet) {
    n_obs2 <- NROW(dt)
    n_pat2 <- length(unique(dt$entity_id))
  }

  # Set primary keys
  pk <- c("entity_id", "date")

  # Remove duplicates
  dt <- unique(dt, by = pk)

  if (!quiet) {
    n_obs3 <- NROW(dt)
    message(cli::style_bold("Filter Relapse Table"))
    message(cli::symbol$cross, " ", n_obs - n_obs2, " observations, ", n_pat - n_pat2, " patients removed")
    message(cli::symbol$cross, " ", n_obs2 - n_obs3, " duplicates removed")
    message(cli::symbol$tick, " ", n_obs3, " observations, ", n_pat2, " patients retained")
  }

  # Set data.table key
  data.table::setkeyv(dt, pk)

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Ensure timestamp is retained
  attr(dt, "timestamp") <- attr(dm_local$relapse, "timestamp")

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("relapse") %>%
    dm::dm_add_tbl(relapse = dt[]) %>%
    # Add primary key
    dm::dm_add_pk("relapse", !!data.table::key(dt), check = TRUE)
}


#' Temporary Aggregation of Standardize/Clean/Filter Workflow
#'
#' @description
#' `r lifecycle::badge("deprecated")`
#'
#' Runs `dm_relapse_std()`, `dm_relapse_clean()`, and `dm_relapse_filter()`
#' sequentially to maintain compatibility with previous API. Please use these
#' functions individually instead, as they provided finer-grained control over
#' where your data is in the pipeline.
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#'
#' @return `[dm]` The `dm` object with transformed relapse table
#'
#' @keywords internal
#'
#' @export
dm_relapse_transform <- function(dm_local) {
  lifecycle::deprecate_soft("0.0.9003", "dm_relapse_transform()")
  dm_local %>%
    dm_relapse_std() %>%
    dm_relapse_clean() %>%
    dm_relapse_filter()
}

#' Utility Functions for MRD Table ELT
#'
#' @description
#' Collection of utility functions for MRD data
#'
#' @aliases utils_relapse
#'
#' @keywords internal
UtilsRelapse <- R6Class(
  "UtilsRelapse",
  cloneable = FALSE,
  public = list(
    #' Standardize multi-membership categorical variables in relapse data
    #'
    #' `std_mcat()` performs the minimal amount of standardization necessary to
    #' ensure that `mcat` variables can be split into categories via a single
    #' delimiter (currently this is `"; "`). Parenthetic expressions are not
    #' split. Missing and uninformative values (i.e. `"NA"`, `"Other"`, etc.)
    #' are removed when they form their own category.
    #'
    #' @param x `[chr]` A character vector
    #'
    #' @return `[chr]` The standardized character vector
    #'   factor level
    std_mcat = function(x) {
      x %>%
        stringr::str_remove_all("(?<=^|[\\b\\s,;/])NA|N/A(?=[\\b\\s,;/]|$)") %>%
        stringr::str_replace_all("(?:\\s*[,;/\\n]\\s*)+", "\x1F") %>%
        stringr::str_remove_all("^\x1F|\x1F$") %>%
        stringr::str_remove_all("(?<=^|\x1F)\\s*(?:OTHER|SPECIFY)\\s*(?:$|\x1F)") %>%
        private$str_replace_parenthetic_delim("\x1F+", replace = ", ") %>%
        private$str_replace_and("\x1F", replace = "; ", replace_parenthetic = " AND ") %>%
        stringr::str_replace_all("\x1F", "; ") %>%
        stringr::str_squish()
    }
  ),
  private = list(
    str_replace_parenthetic_delim = function(x, delim, replace) {
      m <- gregexpr("\\([^)(]*+(?:(?R)[^)(]*)*+\\)", x, perl = TRUE)
      v <- lapply(regmatches(x, m), function(x) gsub(delim, replace, x))
      regmatches(x, m) <- v
      x
    },
    str_replace_and = function(x, delim, replace, replace_parenthetic) {
      x <- stringr::str_replace_all(x, "&", " AND ")
      is_and <- stringr::str_detect(
        x,
        paste0("^[^", delim, "]*[\\s\\b]AND[\\s\\b][^", delim, "]*$")
      )
      x[is_and] <- private$str_replace_parenthetic_delim(x[is_and], "[\\s\\b]+AND[\\s\\b]+", replace_parenthetic)
      x[is_and] <- stringr::str_replace_all(x[is_and], "[\\s\\b]+AND[\\s\\b]+", replace)
      x
    }
  )
)


utils_relapse <- UtilsRelapse$new()

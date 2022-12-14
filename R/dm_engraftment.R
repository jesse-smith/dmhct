#' Transform the Engraftment Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` w/ HCT data
#'
#' @return `[dm]` The `dm` object w/ transformed `engraftment` table
#'
#' @export
dm_engraftment_transform <- function(dm_local) {
  dt <- dm_local$engraftment
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD CHECK Notes
  date <- cat_recovery_type <- lgl_recovery <- N <- NULL

  # Convert datetimes to dates
  dt[, "date" := lubridate::as_date(date)]

  # Convert recovery type
  dt[, "cat_recovery_type" := factor(data.table::fcase(
    cat_recovery_type %flike% "ANC", "recovery_anc500",
    cat_recovery_type %flike% "20", "recovery_plat20k",
    cat_recovery_type %flike% "50", "recovery_plat50k"
  ))]

  # Convert recovery status
  dt[, "lgl_recovery" := data.table::fcase(
    lgl_recovery %ilike% "^no", FALSE,
    lgl_recovery %ilike% "^yes", TRUE
  )]

  # Set order
  data.table::setorderv(dt)

  # Remove missings
  dt <- dt[!(is.na(date) | is.na(cat_recovery_type) | is.na(lgl_recovery))]

  # Remove duplicates
  dt <- dt[!duplicated(dt)]

  # Remove non-unique observations - contradicting recovery amounts to missing
  dt[, N := seq_len(.N), by = c("entity_id", "date", "cat_recovery_type")]
  dt <- dt[N == 1L][, "N" := NULL]

  # Cast recovery types to columns
  dt <- data.table::dcast(
    dt,
    entity_id + date ~ cat_recovery_type,
    value.var = "lgl_recovery"
  )

  # Create primary keys
  pk <- c("entity_id", "date")

  # Set data.table keys
  data.table::setkeyv(dt, pk)

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Ensure timestamp is retained
  attr(dt, "timestamp") <- attr(dm_local$engraftment, "timestamp")

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("engraftment") %>%
    dm::dm_add_tbl(engraftment = dt[]) %>%
    # Add primary key
    dm::dm_add_pk("engraftment", !!data.table::key(dt), check = TRUE)
}

#' Extract Relapse Table from SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to SQL Server w/ HLA data
#'
#' @return `[dm]` The `dm` object w/ instructions for updating `relapse` table
#'
#' @export
dm_relapse_extract <- function(dm_remote) {
  na <- c("", "Unknown", "Unknown at this time", "Unknown At This Time")
  dm_remote %>%
    dm::dm_zoom_to("relapse") %>%
    dplyr::mutate(entity_id = as.integer(.data$EntityID)) %>%
    dplyr::semi_join("master", by = "entity_id") %>%
    dplyr::transmute(
      .data$entity_id,
      dt_relapse = dbplyr::sql("CONVERT(DATETIME, Relapse_Date)"),
      lgl_remission = trimws(as.character(.data[["After treatment, did patient achieve remission?"]])),
      lgl_remission = dplyr::if_else(
        .data$lgl_remission %in% {{ na }}, NA_character_, .data$lgl_remission
      ),
      dt_remission = trimws(as.character(.data[["If yes, specify date"]])),
      dt_remission = dplyr::if_else(
        .data$dt_remission %in% {{ na }}, NA_character_, .data$dt_remission
      )
      # cat_tx = trimws(as.character(.data$Method_of_Treatment)),
      # cat_tx = dplyr::if_else(
      #   .data$cat_tx %in% {{ na_tx }}, NA_character_, .data$cat_tx
      # ),
      # cat_site = trimws(as.character(.data$Site)),
      # cat_site = dplyr::if_else(
      #   .data$cat_site %in% {{ na_site }}, NA_character_, .data$cat_site
      # )
    ) %>%
    # Filter
    dplyr::filter(!is.na(.data$entity_id), !is.na(.data$dt_relapse)) %>%
    dm::dm_update_zoomed()
}


#' Transform the Relapse Table in a Local `dm`
#'
#' @param dm_local `[dm]` Local `dm` with HCT data
#'
#' @return `[dm]` The `dm` object w/ transformed `relapse` table
#'
#' @export
dm_relapse_transform <- function(dm_local) {
  dt <- dm_local$relapse
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD CHECK Notes
  entity_id <- dt_relapse <- dt_remission <- lgl_remission <- NULL
  ..yes <- ..no <- NULL

  # Filter unused patients w/ master
  dt <- dt[entity_id %in% dm_local$master$entity_id]

  # Relapse date
  dt[, "dt_relapse" := lubridate::as_date(dt_relapse)]
  # Remission Date
  dt[, "dt_remission" := lubridate::as_date(lubridate::mdy_hm(dt_remission))]
  # Remission Lgl
  yes <- c("Yes", "YES", "yes", "Y", "y")
  no  <- c("No",  "NO",  "no",  "N", "n")
  dt[, "lgl_remission" := data.table::fcase(
    lgl_remission %chin% ..yes | !is.na(dt_remission), TRUE,
    lgl_remission %chin% ..no, FALSE,
    default = NA
  )]

  # Order rows
  data.table::setorderv(dt, "dt_remission", na.last = TRUE)
  data.table::setorderv(dt, "lgl_remission", order = -1L, na.last = TRUE)

  # Set primary keys
  pk <- c("entity_id", "dt_relapse")

  # Remove duplicates
  dt <- unique(dt, by = pk)

  # Set data.table key
  data.table::setkeyv(dt, pk)

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("relapse") %>%
    dm::dm_add_tbl(relapse = dt[]) %>%
    # Add primary key
    dm::dm_add_pk("relapse", !!data.table::key(dt), check = TRUE)
}

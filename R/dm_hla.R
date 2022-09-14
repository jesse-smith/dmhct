#' Extract and Unite HLA Tables in SQL Server
#'
#' @param dm_remote `[dm]` Remote `dm` connected to a SQL Server w/ HLA data
#' @param quiet Should the function return quietly if an HLA table is already
#'   present?
#'
#' @return The `dm` with instructions to create the HLA table
#'
#' @export
dm_hla_extract <- function(dm_remote, quiet = TRUE) {
  tbls_are_missing <- !all(c("hla_donor", "hla_patient") %in% names(dm_remote))

  if ("hla" %in% names(dm_remote)) {
    if (!quiet) rlang::inform("`hla` table already created")
    return(dm_remote)
  }

  hla <- dplyr::full_join(
    utils_hla$std_hla_tbl(dm_remote$hla_donor),
    utils_hla$std_hla_tbl(dm_remote$hla_patient),
    by = c("entity_id", "gene"),
    suffix = c("_donor", "_entity")
  )

  dm_remote %>%
    # Add new table
    dm::dm_add_tbl(hla = hla) %>%
    # Remove individual tables
    dm::dm_rm_tbl("hla_donor", "hla_patient")
}


#' Transform an HLA Table in a Local `dm`
#'
#' @param dm_local Local `dm` with HCT data
#'
#' @return The `dm` with transformed `hla` table
#'
#' @export
dm_hla_transform <- function(dm_local) {
  dt <- dm_local$hla
  class <- df_class(dt)
  dt <- data.table::as.data.table(dt)

  # Silence R CMD CHECK Notes
  allele_donor <- allele_entity <- donor_id <- entity_id <- gene <- n <- NULL

  # Clean `gene`
  dt[, "gene" := gene %>%
       stringr::str_remove_all("(?i)[^A-Z0-9 ]") %>%
       stringr::str_remove_all("(?i)HLA") %>%
       stringr::str_squish()]
  # Clean alleles
  dt[, c("allele_donor", "allele_entity") := list(
    utils_hla$std_hla_allele(allele_donor),
    utils_hla$std_hla_allele(allele_entity)
  )]

  # Remove rows with missing data and gene == "Bw"
  dt <- dt[rowSums(is.na(dt)) == 0L][gene != "Bw"]

  # Get all donor-entity pairs
  entity_donor <- dt[
    !duplicated(dt, by = c("entity_id", "donor_id")),
    list(entity_id, donor_id)
  ][!is.na(donor_id)]

  # Count matches
  dt <- dt[,
           list(n = sum(allele_entity == allele_donor)),
           by = c("entity_id", "donor_id", "gene")
  ][n > 2L, "n" := 2L]

  # Cast to wider format
  dt <- data.table::dcast(dt, entity_id + donor_id ~ gene, value.var = "n")

  # Add any missing pairs back
  dt <- data.table::merge.data.table(
    dt, entity_donor, by = c("entity_id", "donor_id"), all = TRUE
  )

  # Rename cols
  data.table::setnames(dt, janitor::make_clean_names)
  data.table::setnames(dt, "cw", "c")

  # Reorder cols
  data.table::setcolorder(dt, c(
    "entity_id", "donor_id", "a", "b", "drb1", "c", "dqb1", "dpb1", "drb345",
    "dqa1", "dpa1"
  ))

  # Add counts
  data.table::set(dt, j = "n06", value = dt$a + dt$b + dt$drb1)
  data.table::set(dt, j = "n08", value = dt$n06 + dt$c)
  data.table::set(dt, j = "n10", value = dt$n08 + dt$dqb1)

  # Move counts to front
  data.table::setcolorder(dt, c("entity_id", "donor_id", "n06", "n08", "n10"))

  # Set row order
  data.table::setorderv(dt)

  # Set primary keys
  data.table::setkeyv(dt, c("entity_id", "donor_id"))

  # Convert back to original class
  dt <- dt_cast(dt, to = class)

  # Add to dm
  dm_local %>%
    dm::dm_rm_tbl("hla") %>%
    dm::dm_add_tbl(hla = dt[]) %>%
    # Add primary key
    dm::dm_add_pk("hla", !!data.table::key(dt), check = TRUE)
}


#' Utility Functions for HLA Table ELT
#'
#' @description
#' Collection of utility functions for HLA data
#'
#' @aliases utils_hla
#'
#' @keywords internal
UtilsHLA <- R6Class(
  "UtilsHLA",
  cloneable = FALSE,
  public = list(
    #' Standardize an HLA Table
    #'
    #' @param tbl `[data.frame]` HLA data
    #' @param na `[character]` Vector of values to interpret as `NA`
    #'
    #' @return The standardized data
    std_hla_tbl = function(
    tbl, na = c("", "NT", "Blank", "-", "Not Interpretable")
    ) {
      tbl <- tbl %>%
        dplyr::mutate(
          entity_id = as.integer(.data$EntityID),
          gene = trimws(.data$display),
          allele = trimws(.data$result_val),
          allele = dplyr::if_else(
            .data$allele %in% {{ na }},
            NA_character_,
            .data$allele
          )
        ) %>%
        dplyr::filter(
          !is.na(.data$entity_id),
          !is.na(.data$gene),
          !is.na(.data$allele)
        )

      if ("DonorID" %in% colnames(tbl)) {
        tbl <- tbl %>%
          dplyr::mutate(donor_id = as.integer(.data$DonorID)) %>%
          dplyr::filter(!is.na(.data$donor_id))
      }

      dplyr::select(
        tbl,
        dplyr::starts_with("donor_id"), "entity_id", "gene", "allele"
      )
    },
    #' Standardize HLA Allele Representations
    #'
    #' @param x `[character]` A vector of allele IDs
    #'
    #' @return Standardized alleles
    std_hla_allele = function(x) {
      # Extract first set of numbers in allele ID
      a_id <- x %>%
        # Remove letters
        stringr::str_remove_all("(?i)[A-Z]") %>%
        # Remove any remaining prefix
        stringr::str_remove(".*[*]") %>%
        # Extract first set of numbers
        stringr::str_extract("[0-9]+")

      # If ID is longer than 3 characters, extract the first 2
      a_id_short <- data.table::fifelse(
        nchar(a_id) > 3L,
        stringr::str_sub(a_id, 1L, 2L),
        a_id
      )

      # Convert to integer
      as.integer(a_id_short)
    }
  )
)


#' @rdname UtilsHLA
#' @usage NULL
#' @format NULL
#' @keywords internal
utils_hla <- UtilsHLA$new()

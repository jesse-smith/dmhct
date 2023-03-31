dm_hct <- function(
    dm_remote = dm_sql_server(),
    ...,
    .excl_dsmb = TRUE,
    .quiet = FALSE
) {
  # Extract data from database
  dm <- dm_extract(
    dm_remote = dm_remote,
    ...,
    .legacy = FALSE,
    .excl_dsmb = .excl_dsmb
  )
  # Standardize columns
  dm <- dm_standardize(dm_local = dm, quiet = .quiet)
  # Reshape database (add/remove/combine tables)
  dm <- dm_combine(dm)

  dm
}

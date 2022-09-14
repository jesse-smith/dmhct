#' Connect to SQL Server Where Data is Stored
#'
#' @param server `[chr(1)]` Name of server
#' @param database `[chr(1)]` Name of database
#' @param trusted_connection `[lgl(1)]` Whether this is a "trusted connection";
#'   using Windows Authentication means it is such a connection.
#' @param dsn `[chr(1)]` A DSN name to use for connection; if provided, the
#'   above arguments are ignored.
#' @param ... Additional named arguments to pass to `odbc::dbConnect()`
#'
#' @return `[Microsoft SQL Server]` An ODBC connection object
#'
#' @export
con_sql_server <- function(
    server = "SVWPBMTCTDB01",
    database = "IRB_MLinHCT",
    trusted_connection = TRUE,
    dsn = NULL,
    ...
) {
  if (!is.null(dsn)) {
    odbc::dbConnect(odbc::odbc(), dsn = dsn, ...)
  } else {
    trust_con <- if (trusted_connection) "True" else "False"
    odbc::dbConnect(
      odbc::odbc(),
      driver = "SQL Server",
      server = server,
      database = database,
      Trusted_Connection = trust_con,
      ...
    )
  }
}

#' Create a `dm` Object of HCT Data from Connection to SQL Server
#'
#' @param con `[Microsoft SQL Server]` An ODBC connection to an SQL Server
#'   database
#'
#' @return `[dm]` A `dm` containing HCT data
#'
#' @export
dm_sql_server <- function(con = con_sql_server()) {
  # Check whether connection is default argument (and thus transient)
  con_quo <- rlang::enquo(con)
  con_is_simple_call <- rlang::is_call_simple(con_quo)
  con_quo_nm <- if (con_is_simple_call) rlang::call_name(con_quo) else NULL
  con_fml_nm <- rlang::call_name(rlang::fn_fmls()$con)
  con_is_default <- rlang::is_true(con_quo_nm == con_fml_nm)

  # Get table names (user tables only, exclude pivot tables)
  tbl_nms <- odbc::dbListTables(
    con, catalog = "IRB_MLinHCT", schema = "dbo", table_type = "table"
  ) %>% stringr::str_subset("(?i)_pivot$", negate = TRUE)

  # Create data model
  dm <- con %>%
    dm::dm_from_src(learn_keys = TRUE, table_names = tbl_nms) %>%
    dm::dm_rename_tbl(
      adverse_events_v2 = "AdverseEvents_V2",
      adverse_events_v3 = "AdverseEvents_V3",
      adverse_events_v4 = "AdverseEvents_V4_1",
      adverse_events_v5 = "AdverseEvents_V4_2",
      cerner1 = "legacy_Cerner_Test_Results",
      cerner2 = "Legacy Cerner Test Results-2",
      cerner3 = "Legacy Cerner Test Results-3",
      chimerism = "Chimerism",
      death = "Death_Info",
      disease_status = "DiseaseStatus",
      engraftment = "Engraftment_Info",
      gvhd = "Acute_Chronic_GVHD_Data",
      hla_donor = "Donor_HLA_Typing",
      hla_patient = "Patient_HLA_Typing",
      master = "Master_Transplant_Info",
      mrd = "MRD",
      relapse = "Relapse_Info"
    )

  # Add attribute for default connection argument
  attr(dm, "con_is_default") <- con_is_default

  dm
}

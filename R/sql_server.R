#' Connect to SQL Server Where Data is Stored
#'
#' @param dbname `[chr(1)]` The name of the database to connect to
#'
#' @return `[Microsoft SQL Server]` An ODBC connection object
#'
#' @export
con_sql_server <- function(dbname = c("IRB_MLinHCT", "EDW")) {
  dbname <- rlang::arg_match(dbname)[[1L]]
  switch(
    dbname,
    IRB_MLinHCT = con_irb_mlinhct(),
    EDW = con_stjude_edw()
  )
}

#' Connect to SQL Server Where IRB_MLinHCT Data is Stored
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
con_irb_mlinhct <- function(
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


#' Connect to SQL Server Where EDW Data is Stored
#'
#' @param server `[chr(1)]` Name of server
#' @param database `[chr(1)]` Name of database
#' @param authentication `[chr(1)]` The authentication type to use; default is
#'   `ActiveDirectoryIntegrated`
#' @param ... Additional named arguments to pass to `odbc::dbConnect()`
#'
#' @return `[Microsoft SQL Server]` An ODBC connection object
#'
#' @export
con_stjude_edw <- function(
    server = "stjude-edw.database.windows.net",
    database = "EDW",
    authentication = "ActiveDirectoryIntegrated",
    ...
) {
  odbc::dbConnect(
    odbc::odbc(),
    driver = "ODBC Driver 17 for SQL Server",
    server = server,
    database = database,
    authentication = authentication,
    ...
  )
}

#' Create a `dm` Object of HCT Data from Connection to SQL Server
#'
#' @param con `[Microsoft SQL Server]` An ODBC connection to a SQL Server
#'   database
#' @param quiet Should update messages be suppressed?
#'
#' @return `[dm]` A `dm` containing HCT data
#'
#' @export
dm_sql_server <- function(con = con_sql_server(), quiet = FALSE) {
  switch(
    con@info$dbname,
    IRB_MLinHCT = dm_irb_mlinhct(con, quiet),
    EDW = dm_stjude_edw(con, quiet)
  )
}

dm_irb_mlinhct <- function(con = con_sql_server(), quiet = FALSE) {
  as_rlang_error(checkmate::assert_flag(quiet))
  # Check whether connection is default argument (and thus transient)
  con_quo <- rlang::enquo(con)
  con_is_simple_call <- rlang::is_call_simple(con_quo)
  con_quo_nm <- if (con_is_simple_call) rlang::call_name(con_quo) else NULL
  con_fml_nm <- rlang::call_name(rlang::fn_fmls()$con)
  con_is_default <- rlang::is_true(con_quo_nm == con_fml_nm)

  if (!quiet) rlang::inform("Creating remote `dm` object")

  # Get table names (user tables only, exclude pivot and missing tables)
  tbl_nms <- odbc::dbListTables(
    con, schema = "dbo", table_type = "table"
  ) %>% stringr::str_subset("(?i)(^missing)|(_pivot$)", negate = TRUE)

  # Create data model
  dm_remote <- con %>%
    # Create data model
    dm::dm_from_con(learn_keys = FALSE) %>%
    # Select desired tables
    dm::dm_select_tbl({{ tbl_nms }}) %>%
    # Move large column types to end of tables to avoid ODBC error
    UtilsSQLServer$dm_relocate_large_cols() %>%
    # Add timestamps to tables
    UtilsSQLServer$dm_add_timestamps()

  # Add attribute for default connection argument
  attr(dm_remote, "con_is_default") <- con_is_default

  # Return
  dm_remote
}

dm_stjude_edw <- function(con = con_stjude_edw(), quiet = FALSE) {
  as_rlang_error(checkmate::assert_flag(quiet))
  # Check whether connection is default argument (and thus transient)
  con_quo <- rlang::enquo(con)
  con_is_simple_call <- rlang::is_call_simple(con_quo)
  con_quo_nm <- if (con_is_simple_call) rlang::call_name(con_quo) else NULL
  con_fml_nm <- rlang::call_name(rlang::fn_fmls()$con)
  con_is_default <- rlang::is_true(con_quo_nm == con_fml_nm)

  if (!quiet) rlang::inform("Creating remote `dm` object")

  schema <- "bmtct_sandbox"

  # Get table names (user tables only, exclude pivot and missing tables)
  tbl_nms <- odbc::dbListTables(
    con, schema = schema, table_type = "table"
  ) %>% stringr::str_subset("(?i)(SJCAR19|CATCHAML|_pivot$)", negate = TRUE)

  # Create data model
  dm_remote <- con %>%
    # Create data model
    dm::dm_from_con(learn_keys = FALSE, schema = schema) %>%
    # Select desired tables
    dm::dm_select_tbl({{ tbl_nms }}) %>%
    # Add timestamps to tables
    UtilsSQLServer$dm_add_timestamps()

  # Add attribute for default connection argument
  attr(dm_remote, "con_is_default") <- con_is_default

  # Return
  dm_remote
}

UtilsSQLServer <- R6Class(
  "UtilsSQLServer",
  portable = FALSE,
  cloneable = FALSE,
  lock_objects = TRUE,
  lock_class = TRUE,
  public = list(


    dm_relocate_large_cols = function(dm_remote) {
      # Get table names
      tbl_nms <- names(dm_remote)

      # According to T-SQL documentation:
      #   Large value types: varchar(max), nvarchar(max)
      #   Large object types: text, ntext, image, varbinary(max), xml
      # Source: https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16
      large_types_maybe <- "varbinary"
      large_types <- c("text", "ntext", "image", "xml")
      large_types_chr <- c("varchar", "nvarchar")

      large_cols <- dm::dm_get_con(dm_remote) %>%
        # Column info
        dplyr::tbl(in_schema("INFORMATION_SCHEMA", "COLUMNS")) %>%
        # Restrict to tables in dm & large columns
        dplyr::filter(
          .data$TABLE_NAME %in% {{ tbl_nms }},
          (.data$DATA_TYPE %in% {{ large_types }}
           | .data$DATA_TYPE %in% {{ large_types_maybe }}
           | (.data$DATA_TYPE %in% {{ large_types_chr }} & .data$CHARACTER_MAXIMUM_LENGTH == -1L))
        ) %>%
        dplyr::mutate(
          dtype_position_ = dplyr::case_when(
            .data$DATA_TYPE %in% {{ large_types }} ~ 4L,
            .data$DATA_TYPE %in% {{ large_types_chr }} ~ 3L,
            .data$DATA_TYPE %in% {{ large_types_maybe }} ~ 2L,
            TRUE ~ 1L
          )
        ) %>%
        # Sort by table, then variable type, then original position
        dplyr::arrange(.data$TABLE_NAME, .data$dtype_position_, .data$ORDINAL_POSITION) %>%
        dplyr::collect() %>%
        # Only keep table and column names
        dplyr::select("TABLE_NAME", "COLUMN_NAME")

      # Ensure large data are at end of table
      for (tbl in tbl_nms) {
        # Get columns for table
        relocate_cols <- dplyr::filter(
          large_cols, .data$TABLE_NAME == {{ tbl }}
        )$COLUMN_NAME
        # Skip if no columns to relocate
        if (length(relocate_cols) == 0L) next
        # Move to end
        dm_remote <- dm_remote %>%
          dm::dm_zoom_to({{ tbl }}) %>%
          dplyr::relocate(
            {{ relocate_cols }},
            .after = utils::tail(colnames(dm_remote[[tbl]]), 1L)
          ) %>%
          # Compute updates eagerly
          {suppressMessages(dplyr::compute(.))} %>%
          dm::dm_update_zoomed()
      }

      dm_remote
    },


    dm_add_timestamps = function(dm_remote) {
      tbl_nms <- names(dm_remote)
      # Get sys.tables timestamps
      tbl_ts <- dm::dm_get_con(dm_remote) %>%
        dplyr::tbl(in_schema("sys", "tables")) %>%
        dplyr::filter(.data$name %in% {{ tbl_nms }}) %>%
        dplyr::select("name", "time_stamp" = "modify_date") %>%
        dplyr::collect() %>%
        dplyr::mutate(time_stamp = lubridate::as_datetime(.data$time_stamp))

      # Replace with TimeStamp or Dataload_Timestamp, if present
      lst_ts <- purrr::map(dm::dm_get_tables(dm_remote), function(x) {
        # Dataload_Timestamp preferred - sort puts it first
        ts <- sort(stringr::str_subset(colnames(x), "(?i)timestamp"))
        if (length(ts) == 0L) {
          return(lubridate::NA_POSIXct_)
        } else {
          ts <- ts[[1L]]
        }
        x %>%
          dplyr::select(time_stamp = {{ ts }}) %>%
          dplyr::summarize(time_stamp = max(.data$time_stamp, na.rm = TRUE)) %>%
          dplyr::collect() %>%
          dplyr::pull(1L) %>%
          lubridate::as_datetime()
      })
      tbl_ts <- dplyr::tibble(
        name = names(lst_ts),
        time_stamp = lubridate::as_datetime(unname(unlist(lst_ts)))
      ) %>%
        dplyr::full_join(tbl_ts, by = "name", suffix = c("_col", "_sys")) %>%
        dplyr::transmute(
          .data$name,
          time_stamp = dplyr::coalesce(.data$time_stamp_col, .data$time_stamp_sys)
        )

      # Add timestamp attributes
      for (tbl_nm in tbl_nms) {
        tbl <- dm_remote[[tbl_nm]]
        attr(tbl, "timestamp") <- tbl_ts[tbl_ts$name == tbl_nm,]$time_stamp
        dm_remote <- dm_remote %>%
          dm::dm_select_tbl(-{{ tbl_nm }}) %>%
          dm::dm({{ tbl_nm }} := tbl)
      }

      dm_remote
    }


  )
)$new()




% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sql_server.R
\name{con_irb_mlinhct}
\alias{con_irb_mlinhct}
\title{Connect to SQL Server Where IRB_MLinHCT Data is Stored}
\usage{
con_irb_mlinhct(
  server = "SVWPBMTCTDB01",
  database = "IRB_MLinHCT",
  trusted_connection = TRUE,
  dsn = NULL,
  ...
)
}
\arguments{
\item{server}{\verb{[chr(1)]} Name of server}

\item{database}{\verb{[chr(1)]} Name of database}

\item{trusted_connection}{\verb{[lgl(1)]} Whether this is a "trusted connection";
using Windows Authentication means it is such a connection.}

\item{dsn}{\verb{[chr(1)]} A DSN name to use for connection; if provided, the
above arguments are ignored.}

\item{...}{Additional named arguments to pass to \code{odbc::dbConnect()}}
}
\value{
\verb{[Microsoft SQL Server]} An ODBC connection object
}
\description{
Connect to SQL Server Where IRB_MLinHCT Data is Stored
}

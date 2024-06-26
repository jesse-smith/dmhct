% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sql_server.R
\name{con_stjude_edw}
\alias{con_stjude_edw}
\title{Connect to SQL Server Where EDW Data is Stored}
\usage{
con_stjude_edw(
  server = "stjude-edw.database.windows.net",
  database = "EDW",
  authentication = "ActiveDirectoryIntegrated",
  ...
)
}
\arguments{
\item{server}{\verb{[chr(1)]} Name of server}

\item{database}{\verb{[chr(1)]} Name of database}

\item{authentication}{\verb{[chr(1)]} The authentication type to use; default is
\code{ActiveDirectoryIntegrated}}

\item{...}{Additional named arguments to pass to \code{odbc::dbConnect()}}
}
\value{
\verb{[Microsoft SQL Server]} An ODBC connection object
}
\description{
Connect to SQL Server Where EDW Data is Stored
}

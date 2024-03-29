% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dm_pivot.R
\name{dm_pivot}
\alias{dm_pivot}
\title{Pivot Tables in Entity-Attribute-Value Format}
\usage{
dm_pivot(dm_cmb = dm_combine(), quiet = FALSE)
}
\arguments{
\item{dm_cmb}{A \code{dm} object with combined tables. This is necessary b/c the pivoted tables are created by
\code{dm_combine()}.}

\item{quiet}{Should update messages be suppressed?}
}
\value{
The updated \code{dm} object
}
\description{
Pivot Tables in Entity-Attribute-Value Format
}

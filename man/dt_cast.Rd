% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{dt_cast}
\alias{dt_cast}
\title{Cast a \code{data.table} to a Specified \code{data.frame} Sub-Class}
\usage{
dt_cast(data, to)
}
\arguments{
\item{data}{\verb{[data.table]} A \code{data.table} to cast; will be modified in-place
if possible.}

\item{to}{\verb{[chr(1)]} The subclass to return}
}
\value{
The converted data
}
\description{
Cast a \code{data.table} to a Specified \code{data.frame} Sub-Class
}
\keyword{internal}

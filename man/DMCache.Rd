% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils-dm.R
\docType{data}
\name{DMCache}
\alias{DMCache}
\title{\code{dm} Cache Functions}
\format{
An object of class \code{DMCache} (inherits from \code{R6}) of length 7.
}
\usage{
DMCache
}
\arguments{
\item{obj}{\verb{[chr(1)]} What to read from the cache}

\item{fail}{\verb{[lgl(1)]} Whether to error if \code{dir}/\code{obj}_\code{file} combo does
not exist}

\item{checksum}{\verb{[chr(1)]} The hash of the \code{dm} object}

\item{file}{\verb{[chr(1)]} The file stem to use for the cache objects}

\item{dir}{\verb{[chr(1)]} The directory in which to save the cache objects}

\item{n_threads}{\code{int(1)} The number of threads to use when
de-serializing the cached checksum}

\item{dm}{\verb{[dm]} The \code{dm} object to hash}
}
\value{
\verb{[dm]} The \code{dm} object (invisibly)
Read \code{dm} Data or its Checksum from a the Specified Cache

\verb{[dm|chr(1)]} The de-serialized object
Check Hash for \code{dm} Object

\verb{[lgl(1)]} Whether the passed \code{checksum} and cached \code{checksum}
are identical
Hash a \code{dm} Object

\code{dm_checksum()} creates a checksum by hashing each table + its name individually,
sorting by name, and hashing a list of the results. This means that the hash
does not change when the ordering of the tables change in the dm, but does
change if any data or names are added, deleted, or modified.

\verb{[chr(1)]} The hash string of the object
}
\description{
Functions for reading, writing, and checking/checksumming caches of \code{dm}
objects
}
\keyword{Cache}
\keyword{Directory}
\keyword{File}
\keyword{Object}
\keyword{Specified}
\keyword{`dm`}
\keyword{a}
\keyword{and}
\keyword{in}
\keyword{internal}
\keyword{the}

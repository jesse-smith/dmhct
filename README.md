
<!-- README.md is generated from README.Rmd. Please edit that file -->

# dmhct

<!-- badges: start -->

[![R-CMD-check](https://github.com/jesse-smith/dmhct/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/jesse-smith/dmhct/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

`{dmhct}` extracts and prepares data for the MLinHCT project. The main
functionality is wrapped into a single function, `dm_hct()`, that
produces a `dm` object (from the [`{dm}`
package](https://cynkra.github.io/dm/)) containing cleaned data tables
from an SQL Server database.

## Installation and Setup

You can install `{dmhct}` from Github with:

``` r
# install.packages("remotes")
remotes::install_github("jesse-smith/dmhct")
```

To use the package, you will need access to the SQL Server instance
containing the data. This is provided by St. Jude IT; contact [Jesse
Smith](mailto:jesse.smith@stjude.org) to learn how to gain access.

## Introduction

`{dmhct}` is designed to get data from the SQL server database to your
local machine in a standardized fashion. As noted above, the main entry
point to this functionality is the `dm_hct()` function, which may be all
you ever need to use. It wraps the entire pipeline into a single call:

``` r
library(dmhct)
dm <- dm_hct()
#> Creating remote `dm` object
#> Extracting database tables (1/4)
#> Extracting `adverse_events_v2`
#> Extracting `adverse_events_v3`
#> Extracting `adverse_events_v4_1`
#> Extracting `adverse_events_v4_2`
#> Extracting `cerner_obs`
#> Extracting `cerner1`
#> Extracting `cerner2`
#> Extracting `cerner3`
#> Extracting `chimerism`
#> Extracting `death`
#> Extracting `disease_status`
#> Extracting `encounters`
#> Extracting `engraftment`
#> Extracting `gvhd`
#> Extracting `hla_donor`
#> Extracting `hla_patient`
#> Extracting `master`
#> Extracting `mrd`
#> Extracting `prior_transplant`
#> Extracting `relapse`
#> Done.
#> Standardizing table columns (2/4)
#> Standardizing `adverse_events_v2`
#> Standardizing `adverse_events_v3`
#> Standardizing `adverse_events_v4_1`
#> Standardizing `adverse_events_v4_2`
#> Standardizing `cerner_obs`
#> Standardizing `cerner1`
#> Standardizing `cerner2`
#> Standardizing `cerner3`
#> Standardizing `chimerism`
#> Standardizing `death`
#> Standardizing `disease_status`
#> Standardizing `encounters`
#> Standardizing `engraftment`
#> Standardizing `gvhd`
#> Standardizing `hla_donor`
#> Standardizing `hla_patient`
#> Standardizing `master`
#> Standardizing `mrd`
#> Standardizing `prior_transplant`
#> Standardizing `relapse`
#> Combining similar tables (3/4)
#> Combining `cerner1`, `cerner2`, `cerner3`
#> Combining hla_donor, hla_patient
#> Done.
#> Pivoting E-A-V tables (4/4)
#> Pivoting `hla`
#> Done.
dm
#> ── Metadata ────────────────────────────────────────────────────────────────────
#> Tables: `adverse_events_v2`, `adverse_events_v3`, `adverse_events_v4_1`, `adverse_events_v4_2`, `cerner`, … (17 total)
#> Columns: 332
#> Primary keys: 0
#> Foreign keys: 0
```

The pipeline can be decompsed into 4 steps, which are each responsible
for a set of tasks:

1.  `dm_extract()` pulls the database from the SQL server onto your
    local machine
2.  `dm_standardize()` ensures that column names, types, and contents
    are consistent and in a standard format
3.  `dm_combine()` combines certain sets of tables into single tables
4.  `dm_pivot()` pivots long-format datasets that can be widened without
    losing information

Running these functions in order will produce the same results as
running `dm_hct()`:

``` r
dm2 <- dm_extract() %>%
  dm_standardize() %>%
  dm_combine() %>%
  dm_pivot()
#> Creating remote `dm` object
#> Extracting `adverse_events_v2`
#> Extracting `adverse_events_v3`
#> Extracting `adverse_events_v4_1`
#> Extracting `adverse_events_v4_2`
#> Extracting `cerner_obs`
#> Extracting `cerner1`
#> Extracting `cerner2`
#> Extracting `cerner3`
#> Extracting `chimerism`
#> Extracting `death`
#> Extracting `disease_status`
#> Extracting `encounters`
#> Extracting `engraftment`
#> Extracting `gvhd`
#> Extracting `hla_donor`
#> Extracting `hla_patient`
#> Extracting `master`
#> Extracting `mrd`
#> Extracting `prior_transplant`
#> Extracting `relapse`
#> Standardizing `adverse_events_v2`
#> Standardizing `adverse_events_v3`
#> Standardizing `adverse_events_v4_1`
#> Standardizing `adverse_events_v4_2`
#> Standardizing `cerner_obs`
#> Standardizing `cerner1`
#> Standardizing `cerner2`
#> Standardizing `cerner3`
#> Standardizing `chimerism`
#> Standardizing `death`
#> Standardizing `disease_status`
#> Standardizing `encounters`
#> Standardizing `engraftment`
#> Standardizing `gvhd`
#> Standardizing `hla_donor`
#> Standardizing `hla_patient`
#> Standardizing `master`
#> Standardizing `mrd`
#> Standardizing `prior_transplant`
#> Standardizing `relapse`
#> Combining `cerner1`, `cerner2`, `cerner3`
#> Combining hla_donor, hla_patient
#> Pivoting `hla`

identical(dm, dm2)
#> [1] FALSE
```

## Changes from previous versions

The previous iteration of this package used the `dm_elt()` function as
the wrapper and attempted to thoroughly clean all datasets.
Unfortunately, this requires many ad hoc decisions, hard-coded
transformations, and frequent revisions; the resulting codebase was very
difficult to maintain and frequently made decisions that would be better
left to the user. The updated version of this package attempts to do
much less; its goal is to provide the database in a standardized format,
with all information from the original data preserved. This passes the
task of interpreting the data to the user.

Note that `dm_elt()` and its components are still exported;
`dm_extract(.legacy = TRUE)` and `dm_transform()` are the components of
this pipeline. However, these functions (and the `.legacy` argument of
`dm_extract()`) are deprecated, and their use is discouraged.

## Variable Typing System

Variables names are snakecased, with a prefix describing the content of
the variable. This mostly maps to familiar R classes: `num_` and `pct_`
variables are `numeric` (with `pct_` taking values between 0 and 100),
`lgl_` variables are `logical`, and `chr_` variables are `character`.
There are exceptions, though; these come in two forms. The first are
variable prefixes that could map to an **R** type, but do not because
the underlying data may need additional cleaning. This is currently true
for `cat_` variable, which are conceptually `factor`s but are stored as
`character`, and `dt_` variables, which may be stored as either `Date`
or `POSIXct` class, depending on whether time information is included.
The second form of exception are variables that have no equivalent base
**R** type; this is currently true for `mcat_` variables, which
represent categorical variables that can take multiple values
simultaneously, and `intvl_` variables, which are interval-valued
numerics. Both are stored as `character`, with `mcat_` values separated
by a comma and `intvl_` represented in standard interval notation.

## Helper Functions

`{dmhct}` exports several sets of helper functions. These are used
internally to make data extraction and cleaning a bit easier. The
functions are listed below; see each function’s documentation for more
information.

### Data cleaning

- `intvl_to_matrix()`: Converts the interval representation in `intvl_`
  variables to a 4-column `numeric` matrix (these variables are
  standardized by `std_intvl()` - see below). Columns represent open or
  closed bounds and the location of those bounds.
- `non_numeric()`: Helps quickly determine what values in a vector
  cannot be converted to `numeric` (either directly or by `std_num()` -
  see below).
- `std_chr()`: Standardizes `character` vectors to ASCII text with no
  unnecessary whitespace and a given case. By default, it will retain
  newlines inside a text, though it will condense consecutive newlines
  and any carriage returns into a single newline.
- `std_date()`: Standardizes a date vector and returns a vector in
  `Date` or `POSIXct` format, depending on whether there is sub-daily
  information available in the data.
- `std_intvl()`: standardizes the various interval representations found
  in the ML in HCT dataset. These intervals are assumed to be in
  percentage values and this lie between 0 and 100. Explicit intervals
  with upper and lower bounds, as well as implicit intervals using \<
  and \>, are handled (\<= and \>= are currently not supported). The
  return value simplifies to \</\>/\<=/\>= or a single numeric value if
  possible and uses standard interval notation if not.
- `std_lgl()`: Converts other classes to `logical` vectors. All but
  `character` use `as.logical()`; `character` (and `factor`) vectors are
  converted by first (optionally) standardizing with `std_chr()` and
  then assigning logical value based on the regular expression in
  `true`, `false`, and `na`.
- `std_num()`: Converts all base classes, as well as `int64`, `factor`,
  `Date`, and `POSIXt` vectors to the simplest `numeric` form possible.
  `character` vectors (and `factor`s) are standardized using `std_chr()`
  by default, then converted. `double` and `int64` vectors will be
  converted to `integer` if this does not cause overflow or loss of
  precision. `Date` is converted to `integer`, and `POSIXt` is converted
  to `integer` if the range allows, otherwise `double`.

### Database Interaction

- `con_sql_server()`: Creates an ODBC connection to the SQL server
  storing MLinHCT data
- `dm_sql_server()`: Creates a `{dm}` object representing data on the
  SQL server
- `dm_disconnect()`: Closes the remote server connection used by a `dm`
  object, if present
- `dm_is_remote()`: Determines whether a `dm` object has a remote
  connection
- `dm_compute()`: Forces computation of database queries on all tables
  in a `dm` object
- `dm_collect()`: Computes all database queries (as in `dm_compute()`),
  and additionally loads tables onto local machine

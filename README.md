
<!-- README.md is generated from README.Rmd. Please edit that file -->

# dmhct

<!-- badges: start -->
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

The main functionality in the package is in `dm_hct()`, which wraps the
entire pipeline. It pull the entire database from SQL server and
transforms it to a `dm` object, which can be thought of as a list of
`data.frame`s. Each table is a `tibble`:

``` r
library(dmhct)
dm <- dm_hct()
#> Extracting database (1/4)
#> Extracting adverse_events_v2
#> Extracting adverse_events_v3
#> Extracting adverse_events_v4_1
#> Extracting adverse_events_v4_2
#> Extracting cerner_obs
#> Extracting cerner1
#> Extracting cerner2
#> Extracting cerner3
#> Extracting chimerism
#> Extracting death
#> Extracting disease_status
#> Extracting encounters
#> Extracting engraftment
#> Extracting gvhd
#> Extracting hla_donor
#> Extracting hla_patient
#> Extracting master
#> Extracting mrd
#> Extracting prior_transplant
#> Extracting relapse
#> Done.
#> Standardizing table columns (2/4)
#> Standardizing adverse_events_v2
#> Standardizing adverse_events_v3
#> Standardizing adverse_events_v4_1
#> Standardizing adverse_events_v4_2
#> Standardizing cerner_obs
#> Standardizing cerner1
#> Standardizing cerner2
#> Standardizing cerner3
#> Standardizing chimerism
#> Standardizing death
#> Standardizing disease_status
#> Standardizing encounters
#> Standardizing engraftment
#> Standardizing gvhd
#> Standardizing hla_donor
#> Standardizing hla_patient
#> Standardizing master
#> Standardizing mrd
#> Standardizing prior_transplant
#> Standardizing relapse
#> Combining similar tables (3/4)
#> Combining cerner1, cerner2, cerner3
#> Combining hla_donor, hla_patient
#> Done.
#> Pivoting E-A-V tables (4/4)
#> Pivoting hla...
#> Done.
dm
#> ── Metadata ────────────────────────────────────────────────────────────────────
#> Tables: `adverse_events_v2`, `adverse_events_v3`, `adverse_events_v4_1`, `adverse_events_v4_2`, `cerner`, … (17 total)
#> Columns: 332
#> Primary keys: 0
#> Foreign keys: 0
```

The pipeline is composed of a set of sub-functions, which are each
responsible for a set of tasks:

1.  `dm_extract()` pulls the database from the SQL server onto your
    local machine
2.  `dm_standardize()` ensures that column names, types, and contents
    are consistent and in a standard format
3.  `dm_combine()` combines certain sets of tables into single tables
4.  `dm_pivot()` pivots long-format datasets that can be widened without
    losing information

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

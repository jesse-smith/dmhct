
<!-- README.md is generated from README.Rmd. Please edit that file -->

# dmhct

<!-- badges: start -->

[![R-CMD-check](https://github.com/jesse-smith/dmhct/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/jesse-smith/dmhct/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

`{dmhct}` extracts, loads, and transforms data for the MLinHCT project.
The main functionality is wrapped into a single function, `dm_elt()`,
that produces a `dm` object (from the [`{dm}`
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

The main functionality in the package is in `dm_elt()`; this function
combines two sub-functions, `dm_extract()` and `dm_transform()`, into a
small pipeline. (Note that this is in the process of being deprecated.)

``` r
library(dmhct)
dm <- dm_elt()
#> Warning: `dm_elt()` was deprecated in dmhct 1.0.0.
dm2 <- dm_extract() %>% dm_transform()
#> Warning: `dm_transform()` was deprecated in dmhct 1.0.0.
identical(dm, dm2)
#> [1] TRUE
```

The `dm_extract()` function is itself a wrapper for a pipeline used to
extract the necessary data from the SQL Server and load it onto your
local machine. The `dm_transform()` function provides a second pipeline
that cleans each table in the database and outputs tables ready for
analysis (as [`data.table`s](https://rdatatable.gitlab.io/data.table/)).

Both functions cache their results by default and compare a checksum of
their inputs to a cached checksum to determine whether re-calculation is
necessary. This allows the functions to simply read the cached outputs,
rather than repeat costly computations, if the input is unchanged. Each
function also allows the user to force re-computation and cache
overwriting via a `reset` argument. This caching behavior (and the
control arguments) are made available in the `dm_elt()` wrapper as well.

## Tables

The data used in the project consists of 10 tables:

``` r
summary(dm)[, -3L]
#>                Length Class     
#> hla            14     data.table
#> master         26     data.table
#> cerner          5     data.table
#> chimerism       5     data.table
#> death          10     data.table
#> disease_status  4     data.table
#> engraftment     5     data.table
#> gvhd            5     data.table
#> mrd             5     data.table
#> relapse         8     data.table
```

The entry point to this data is the `master` table, which contains one
row per patient and includes baseline variables such as transplant date,
patient demographics, and donor characteristics. It also contains unique
IDs for patients (`entity_id`) and donors (`donor_id`).

``` r
skimr::skim(dm$master, setdiff(colnames(dm$master), c("entity_id", "donor_id")))
```

|                                                  |            |
|:-------------------------------------------------|:-----------|
| Name                                             | dm\$master |
| Number of rows                                   | 859        |
| Number of columns                                | 26         |
| Key                                              | entity_id  |
| \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_   |            |
| Column type frequency:                           |            |
| character                                        | 1          |
| Date                                             | 1          |
| factor                                           | 13         |
| logical                                          | 3          |
| numeric                                          | 6          |
| \_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_ |            |
| Group variables                                  | None       |

Data summary

**Variable type: character**

| skim_variable      | n_missing | complete_rate | min | max | empty | n_unique | whitespace |
|:-------------------|----------:|--------------:|----:|----:|------:|---------:|-----------:|
| cat_donor_relation |       180 |          0.79 |   4 |  11 |     0 |       10 |          0 |

**Variable type: Date**

| skim_variable | n_missing | complete_rate | min        | max        | median     | n_unique |
|:--------------|----------:|--------------:|:-----------|:-----------|:-----------|---------:|
| dt_trans      |         0 |             1 | 2000-01-15 | 2022-11-03 | 2012-02-28 |      814 |

**Variable type: factor**

| skim_variable               | n_missing | complete_rate | ordered | n_unique | top_counts                            |
|:----------------------------|----------:|--------------:|:--------|---------:|:--------------------------------------|
| cat_degree_match06          |        67 |          0.92 | TRUE    |        4 | 6: 501, 3: 138, 4: 83, 5: 70          |
| cat_degree_match08          |       231 |          0.73 | TRUE    |        5 | 8: 342, 4: 119, 5: 86, 7: 43          |
| cat_degree_match10          |       235 |          0.73 | TRUE    |        6 | 10: 322, 6: 87, 5: 85, 7: 53          |
| cat_disease_status_at_trans |       187 |          0.78 | FALSE   |        3 | Rem: 416, Act: 181, Rel: 75           |
| cat_donor_race              |       181 |          0.79 | FALSE   |        3 | Whi: 438, Bla: 149, Oth: 91           |
| cat_donor_sex               |        47 |          0.95 | FALSE   |        2 | Mal: 421, Fem: 391                    |
| cat_dx_grp                  |        11 |          0.99 | FALSE   |        9 | AML: 284, ALL: 272, Oth: 94, Apl: 58  |
| cat_ethnicity               |         8 |          0.99 | FALSE   |        2 | Not: 673, His: 178                    |
| cat_matched_related         |       265 |          0.69 | FALSE   |        4 | mis: 246, mat: 164, mat: 153, mis: 31 |
| cat_prep_type               |       223 |          0.74 | TRUE    |        3 | Mye: 375, Red: 225, Non: 36           |
| cat_product_type            |         5 |          0.99 | FALSE   |        2 | Mar: 484, PBS: 370, Cor: 0, Mar: 0    |
| cat_race                    |         1 |          1.00 | FALSE   |        3 | Whi: 603, Bla: 165, Oth: 90           |
| cat_sex                     |         0 |          1.00 | FALSE   |        2 | Mal: 493, Fem: 366                    |

**Variable type: logical**

| skim_variable     | n_missing | complete_rate | mean | count              |
|:------------------|----------:|--------------:|-----:|:-------------------|
| lgl_death         |         0 |          1.00 | 0.35 | FAL: 555, TRU: 304 |
| lgl_donor_related |       180 |          0.79 | 0.62 | TRU: 423, FAL: 256 |
| lgl_malignant     |        11 |          0.99 | 0.78 | TRU: 663, FAL: 185 |

**Variable type: numeric**

| skim_variable | n_missing | complete_rate |    mean |      sd |      p0 |     p25 |     p50 |     p75 |    p100 | hist  |
|:--------------|----------:|--------------:|--------:|--------:|--------:|--------:|--------:|--------:|--------:|:------|
| num_age       |         0 |          1.00 |    9.63 |    5.97 |    0.07 |    4.25 |    9.77 |   14.93 |   20.88 | ▇▆▆▇▅ |
| num_age_donor |        53 |          0.94 |   28.08 |   12.63 |    0.60 |   20.06 |   28.18 |   37.73 |   65.72 | ▃▇▇▅▁ |
| num_dt_trans  |         0 |          1.00 | 2011.87 |    6.70 | 2000.04 | 2005.73 | 2012.16 | 2018.00 | 2022.84 | ▆▆▆▆▇ |
| num_n\_trans  |         0 |          1.00 |    1.00 |    0.00 |    1.00 |    1.00 |    1.00 |    1.00 |    1.00 | ▁▁▇▁▁ |
| num_t\_surv   |         0 |          1.00 | 2394.68 | 2439.97 |    2.00 |  279.50 | 1352.00 | 4120.00 | 8328.00 | ▇▂▂▂▁ |
| num_t\_trans  |        12 |          0.99 |  694.83 | 1058.98 |   16.00 |  123.50 |  232.00 |  756.50 | 6668.00 | ▇▁▁▁▁ |

All tables are keyed by one or more columns that are (jointly) unique to
each row. Each table contains `entity_id` as a primary key; most tables
except `master` then define additional keys specific to the data
contained in that table (for example, many tables contain repeated
observations for a patient and thus add a `date` key).

``` r
dm::dm_get_all_pks(dm) %>% dplyr::mutate(pk_col = format(.data$pk_col))
#> # A tibble: 10 × 2
#>    table          pk_col                      
#>    <chr>          <chr>                       
#>  1 hla            entity_id, donor_id         
#>  2 master         entity_id                   
#>  3 cerner         entity_id, test, date, n_obs
#>  4 chimerism      entity_id, date, cat_source 
#>  5 death          entity_id, n_id             
#>  6 disease_status entity_id, date, n_status   
#>  7 engraftment    entity_id, date             
#>  8 gvhd           entity_id, date, cat_site   
#>  9 mrd            entity_id, date             
#> 10 relapse        entity_id, date
```

Here’s a list of data (non-key) columns in each table (excluding
`master`):

``` r
dplyr::tibble(
  table = names(dm::dm_select_tbl(dm, -"master")),
  cols = sapply(dm::dm_select_tbl(dm, -"master"), function(x) {
    paste0(setdiff(colnames(x), data.table::key(x)), collapse = ", ")
  })
)
#> # A tibble: 9 × 2
#>   table          cols                                                           
#>   <chr>          <chr>                                                          
#> 1 hla            n06, n08, n10, a, b, drb1, c, dqb1, dpb1, drb345, dqa1, dpa1   
#> 2 cerner         result                                                         
#> 3 chimerism      cat_method, pct_donor                                          
#> 4 death          dt_death, dt_trans, lgl_on_therapy, cat_death_location, cat_de…
#> 5 disease_status disease_status                                                 
#> 6 engraftment    recovery_anc500, recovery_plat20k, recovery_plat50k            
#> 7 gvhd           cat_grade, cat_type                                            
#> 8 mrd            cat_method, pct_result, lgl_result                             
#> 9 relapse        dt_remission, lgl_remission, lgl_add_tx, mcat_add_tx, mcat_sit…
```

## Development

You can find code generating results for each table in a corresponding
file named `"dm_[table-name]"`. Changes should not be committed directly
to the `main` branch; going forward, all changes should be made in a
feature/bug fix/etc. branch and reviewed by another team member before
merging. The Github repository is set up to dis-allow pushing direct
commits to `main`.

## Using with `{predHCT}`

`{dmhct}` was created to de-couple data cleaning code from modeling code
for the MLinHCT project. However, it is a required dependency for the
main modeling package, `{predHCT}`. While it is possible to install your
local development version for use with `{predHCT}`, this is not
recommended. Instead, please install from the `main` Github branch using
the installation instructions at the beginning of this document.

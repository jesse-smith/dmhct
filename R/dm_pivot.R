#' Pivot Tables in Entity-Attribute-Value Format
#'
#' @param dm_cmb A `dm` object with combined tables. This is necessary b/c the pivoted tables are created by
#'   `dm_combine()`.
#' @param quiet Should update messages be suppressed?
#'
#' @return The updated `dm` object
#'
#' @export
dm_pivot <- function(dm_cmb = dm_combine(), quiet = FALSE) {
  as_rlang_error(checkmate::assert_flag(quiet))
  force(dm_cmb)
  # Pivot HLA table
  if (!quiet) rlang::inform("Pivoting `hla`")
  dm_cmb <- dm_cmb %>%
    dm::dm_select_tbl(-"hla") %>%
    dm::dm("hla" := pivot_hla(dm_cmb$hla))
  # Possibly pivot cerner later (keeping all info requires some additional thought)
  # Return
  dm_cmb
}


pivot_hla <- function(dt_hla) {
  dt_hla <- data.table::as.data.table(dt_hla)
  id_cols <- stringr::str_subset(colnames(dt_hla), "_id|date_")
  rhs_cols <- "cat_gene"
  value_cols_old <- stringr::str_subset(colnames(dt_hla), "allele")
  value_cols <- paste0("mcat_", stringr::str_remove(value_cols_old, "^cat_"))
  data.table::setnames(dt_hla, value_cols_old, value_cols)
  pivot_expr <- paste0("data.table::dcast(",
   "dt_hla, ",
   paste0(id_cols, collapse = " + "), "  ~ ", rhs_cols, ", ",
   "value.var = c(", paste0("'", value_cols, "'", collapse = ", "), "), ",
   "fun.aggregate = function(x) {
     if (length(x) == 0L) return(NA_character_)
     u <- unique(x)
     if (length(u) == 1L && length(x) %% 2 == 0L) u <- c(u, u)
     paste0(u, collapse = ',')
   }",
  ")")
  dt_hla <- pivot_expr %>%
    rlang::parse_expr() %>%
    eval() %>%
    setTBL() %>%
    janitor::clean_names() %>%
    dplyr::rename_with(~ stringr::str_remove(.x, "_allele"))

  dt_hla
}

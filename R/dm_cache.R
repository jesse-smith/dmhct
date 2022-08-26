#' Cache a `dm` Object in the Specified File and Directory
#'
#' @param dm A `dm` object
#' @param checksum The hash of the `dm` object
#' @param file The file stem to use for the cache objects
#' @param dir The directory in which to save the cache objects
#' @param n_threads The number of threads to use when serializing the `dm`
#'
#' @return The `dm` (invisibly)
#'
#' @keywords internal
dm_cache_write <- function(
    dm,
    checksum,
    file,
    dir = path_create(system.file("", package = "dmhct"), "cache"),
    n_threads = max(1L, parallel::detectCores() %/% 2L)
) {
  # Clean and combine file and dir
  path <- path_create(dir, file) %>%
    fs::path_ext_remove() %>%
    fs::path_ext_set("qs")
  # Create cache directory if it does not exist
  # Update file name
  dir <- path %>%
    fs::path_dir() %>%
    fs::dir_create()
  # Create file paths
  file <- fs::path_file(path)
  file_checksum   <- paste0("checksum_", file)
  file_data       <- paste0("data_",     file)
  path_checksum   <- path_create(dir, file_checksum)
  path_data       <- path_create(dir, file_data)
  tmp_checksum <- path_create(dir, paste0("tmp_", file_checksum))
  tmp_data     <- path_create(dir, paste0("tmp_", file_data))

  # Delete temp files on exit (shouldn't be needed unless error occurs)
  on.exit(try(fs::file_delete(tmp_checksum), silent = TRUE), add = TRUE)
  on.exit(try(fs::file_delete(tmp_data),     silent = TRUE), add = TRUE)
  # Save checksum and data
  qs::qsave(checksum, tmp_checksum)
  qs::qsave(dm, tmp_data, nthreads = n_threads)
  # Move checksum and data
  fs::file_move(c(tmp_checksum, tmp_data), c(path_checksum, path_data))
  # Return dm invisibly
  invisible(dm)
}


#' Check Hash for `dm` Object
#'
#' @inheritParams dm_cache_write
#'
#' @return A `logical` indicating whether the passed `checksum` and cached
#'   `checksum` are identical
#'
#' @keywords internal
dm_cache_check <- function(
    checksum,
    file,
    dir = path_create(system.file("", package = "predHCT"), "cache"),
    n_threads = max(1L, parallel::detectCores() %/% 2L)
) {
  # Read
  old_checksum <- dm_cache_read(
    "checksum", file = file, dir = dir, fail = FALSE, n_threads = n_threads
  )
  # Check
  identical(checksum, old_checksum)
}


#' Read `dm` Data or its Checksum from a the Specified Cache
#'
#' @param obj What to read from the cache
#' @inheritParams dm_cache_write
#' @param fail Whether to error if `dir`/`obj`_`file` combo does not exist
#'
#' @return The de-serialized object
#'
#' @keywords internal
dm_cache_read <- function(
    obj = c("data", "checksum"),
    file,
    dir = path_create(system.file("", package = "predHCT"), "cache"),
    fail = TRUE,
    n_threads = if (obj == "data") max(1L, parallel::detectCores() %/% 2L) else 1L
) {
  obj <- rlang::arg_match(obj)[[1L]]
  # Clean and combine file and dir
  path <- path_create(dir, file) %>%
    fs::path_ext_remove() %>%
    fs::path_ext_set("qs")
  dir <- fs::path_dir(path)
  file <- paste0(obj, "_", fs::path_file(path))
  path <- path_create(dir, file)
  # Read
  if (!fail && !fs::file_exists(path)) return(NULL)
  q <- qs::qread(path, nthreads = n_threads)
  # Make sure dm is recognized
  if (obj == "data") dm::is_dm(q)
  q
}


#' Hash a `dm` Object
#'
#' `dm_checksum()` creates a checksum by hashing each table + its name individually,
#' sorting by name, and hashing a list of the results. This means that the hash
#' does not change when the ordering of the tables change in the dm, but does
#' change if any data or names are added, deleted, or modified.
#'
#' @param dm The `dm` object to hash
#'
#' @return The hash string of the object
#'
#' @keywords internal
dm_checksum <- function(dm) {
  # dm table checksums
  tbl_chksm <- if (dm_is_remote(dm)) tbl_checksum_remote else tbl_checksum_local
  dm %>%
    as.list() %>%
    purrr::imap_dfr(~ tibble::tibble(name = .y, checksum = tbl_chksm(.x))) %>%
    dplyr::arrange(.data$name) %>%
    rlang::hash()
}


#' Hash a Remote Table on SQL Server
#'
#' @param tbl_remote A remote table in a `dm` Object
#'
#' @return The checksum of the table
tbl_checksum_remote <- function(tbl_remote) {
  tbl_remote %>%
    dplyr::summarize(cs = dbplyr::sql("CHECKSUM_AGG(BINARY_CHECKSUM(*))")) %>%
    dplyr::pull("cs")
}


#' Hash a Local Table
#'
#' @param tbl_local A local table in a `dm` object
#'
#' @return The checksum of the table
tbl_checksum_local <- function(tbl_local) {
  tbl_local %>%
    tibble::as_tibble() %>%
    rlang::hash()
}

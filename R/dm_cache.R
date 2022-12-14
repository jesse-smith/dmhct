#' `dm` Cache Functions
#'
#' @description
#' Functions for reading, writing, and checking/checksumming caches of `dm`
#' objects
#'
#' @keywords internal
DMCache <- R6Class(
  "DMCache",
  cloneable = FALSE,
  public = list(
    #' Cache a `dm` Object in the Specified File and Directory
    #'
    #' @param dm `[dm]` A `dm` object
    #' @param checksum `[chr(1)]` The hash of the `dm` object
    #' @param file `[chr(1)]` The file stem to use for the cache objects
    #' @param dir `[chr(1)]` The directory in which to save the cache objects
    #' @param n_threads `[int(1)]` The number of threads to use when serializing
    #'   the `dm` object
    #'
    #' @return `[dm]` The `dm` object (invisibly)
    write = function(
    dm,
    checksum,
    file,
    dir = path_create("~", ".cache_dmhct"),
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
      qsave(checksum, tmp_checksum)
      qsave(dm, tmp_data, nthreads = n_threads)
      # Move checksum and data
      fs::file_move(c(tmp_checksum, tmp_data), c(path_checksum, path_data))
      # Return dm invisibly
      invisible(dm)
    },
    #' Read `dm` Data or its Checksum from a the Specified Cache
    #'
    #' @param obj `[chr(1)]` What to read from the cache
    #' @param file `[chr(1)]` The file stem to use for the cache objects
    #' @param dir `[chr(1)]` The directory in which to save the cache objects
    #' @param fail `[lgl(1)]` Whether to error if `dir`/`obj`_`file` combo does
    #'   not exist
    #' @param n_threads `[int(1)]` The number of threads to use when
    #'   de-serializing the object
    #'
    #' @return `[dm|chr(1)]` The de-serialized object
    read = function(
    obj = c("data", "checksum"),
    file,
    dir = path_create("~", ".cache_dmhct"),
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
      q <- qread(path, nthreads = n_threads)
      # Make sure dm is recognized
      if (obj == "data") dm::is_dm(q)
      q
    },
    #' Check Hash for `dm` Object
    #'
    #' @param checksum `[chr(1)]` The hash of the `dm` object
    #' @param file `[chr(1)]` The file stem to use for the cache objects
    #' @param dir `[chr(1)]` The directory in which to save the cache objects
    #' @param n_threads `int(1)` The number of threads to use when
    #'   de-serializing the cached checksum
    #'
    #' @return `[lgl(1)]` Whether the passed `checksum` and cached `checksum`
    #'   are identical
    check = function(
    checksum,
    file,
    dir = path_create("~", ".cache_dmhct"),
    n_threads = max(1L, parallel::detectCores() %/% 2L)
    ) {
      # Read
      old_checksum <- dm_cache$read(
        "checksum", file = file, dir = dir, fail = FALSE, n_threads = n_threads
      )
      # Check
      identical(checksum, old_checksum)
    },
    #' Hash a `dm` Object
    #'
    #' `dm_checksum()` creates a checksum by hashing each table + its name individually,
    #' sorting by name, and hashing a list of the results. This means that the hash
    #' does not change when the ordering of the tables change in the dm, but does
    #' change if any data or names are added, deleted, or modified.
    #'
    #' @param dm `[dm]` The `dm` object to hash
    #'
    #' @return `[chr(1)]` The hash string of the object
    checksum = function(dm) {
      # dm table checksums
      tbl_chksm <- if (dm_is_remote(dm)) private$tbl_checksum_remote else private$tbl_checksum_local
      dm %>%
        as.list() %>%
        purrr::imap_dfr(~ tibble::tibble(name = .y, checksum = tbl_chksm(.x))) %>%
        dplyr::arrange(.data$name) %>%
        rlang::hash()
    }
  ),
  private = list(
    # Hash a Remote Table on SQL Server
    tbl_checksum_remote = function(tbl_remote) {
      tbl_remote %>%
        dplyr::summarize(cs = dbplyr::sql("CHECKSUM_AGG(BINARY_CHECKSUM(*))")) %>%
        dplyr::pull("cs")
    },
    # Hash a Local Table
    tbl_checksum_local = function(tbl_local) {
      tbl_local %>%
        tibble::as_tibble() %>%
        rlang::hash()
    }
  )
)

#' @rdname DMCache
#' @usage NULL
#' @format NULL
dm_cache <- DMCache$new()





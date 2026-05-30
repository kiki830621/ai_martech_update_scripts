# amz_ETL_reviews_0IM.R - Amazon Reviews Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL reviews Phase 0IM: Import from local CSV/Excel files
# Output: raw_data.duckdb → df_amz_review

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
sql_read_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R")
)
sql_read_path <- sql_read_candidates[file.exists(sql_read_candidates)][1]
if (is.na(sql_read_path)) {
  stop("fn_sql_read.R not found in expected paths")
}
source(sql_read_path)
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL

# Initialize using unified autoinit system
autoinit()

# Read ETL profile from config (DM_R037 v3.0: config-driven import)
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
platform_cfg <- get_platform_config("amz")
etl_profile <- platform_cfg$etl_sources$reviews
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

source_type <- tolower(as.character(etl_profile$source_type %||% ""))
if (!source_type %in% c("excel", "csv")) {
  stop(sprintf("VALIDATE FAILED: reviews requires source_type='excel' or 'csv', got '%s'", source_type))
}

rawdata_root <- RAW_DATA_DIR %||% file.path(APP_DIR, "data", "local_data", "rawdata_QEF_DESIGN")
rawdata_pattern <- as.character(etl_profile$rawdata_pattern %||% "")
if (!nzchar(rawdata_pattern)) {
  stop("VALIDATE FAILED: reviews profile missing rawdata_pattern")
}

message("INITIALIZE: Amazon reviews import (ETL reviews 0IM) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL reviews Import Phase - Amazon reviews...")

  matched_files <- Sys.glob(file.path(rawdata_root, rawdata_pattern))
  matched_files <- matched_files[file.exists(matched_files)]
  # Keep only regular files (drop directories matched by the glob)
  matched_files <- matched_files[!dir.exists(matched_files)]
  # #607 (regression of #368): Amazon scrapes frequently produce Excel-format
  # files WITHOUT a filename suffix (e.g. "B00080FKIO"). A strict extension-only
  # regex pre-filter silently dropped those files (37% of QEF reviews on 2026-05-10).
  # Fix per issue Path (b): accept files with a recognized data extension OR
  # extensionless files (content format is sniffed by magic bytes at read time).
  # Files with a NON-data extension (.txt, .json, .pdf, ...) or hidden dotfiles
  # are excluded here, while extensionless ones are passed through to the reader.
  has_data_ext <- grepl("\\.(csv|xlsx?)$", matched_files, ignore.case = TRUE)
  is_extensionless <- !nzchar(tools::file_ext(matched_files))
  is_hidden <- grepl("^\\.", basename(matched_files))
  matched_files <- matched_files[(has_data_ext | is_extensionless) & !is_hidden]
  if (length(matched_files) == 0) {
    stop(sprintf("VALIDATE FAILED: No files match pattern '%s'", rawdata_pattern))
  }
  message(sprintf("VALIDATE: Found %d files matching declared pattern (%d extensionless)",
                  length(matched_files), sum(is_extensionless[(has_data_ext | is_extensionless) & !is_hidden])))

  rawdata_rel_dir <- sub("/\\*.*$", "", rawdata_pattern)
  rawdata_rel_dir <- sub("/$", "", rawdata_rel_dir)
  reviews_dir <- file.path(rawdata_root, rawdata_rel_dir)
  message("MAIN: Importing reviews from: ", reviews_dir)

  if (!dir.exists(reviews_dir)) {
    stop("VALIDATE FAILED: Reviews directory does not exist: ", reviews_dir)
  }

  # Sniff the true file format from its magic bytes so extensionless files
  # (see #607) are routed to the correct reader instead of being dropped.
  #   PK\x03\x04           -> ZIP container == .xlsx (OOXML)
  #   \xD0\xCF\x11\xE0...   -> OLE2 compound == .xls (legacy Excel)
  # Anything else falls back to CSV parsing.
  detect_file_format <- function(path) {
    con <- file(path, "rb")
    on.exit(close(con), add = TRUE)
    magic <- readBin(con, what = "raw", n = 8L)
    if (length(magic) >= 4L &&
        identical(magic[1:4], as.raw(c(0x50, 0x4B, 0x03, 0x04)))) {
      return("xlsx")
    }
    if (length(magic) >= 8L &&
        identical(magic[1:8],
                  as.raw(c(0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1)))) {
      return("xls")
    }
    "csv"
  }

  # Import all CSV and Excel files from the directory
  message("MAIN: Found ", length(matched_files), " files to import")
  review_dfs <- lapply(matched_files, function(f) {
    tryCatch({
      ext <- tolower(tools::file_ext(f))
      # Trust an explicit data extension; otherwise sniff magic bytes (#607).
      fmt <- if (ext %in% c("csv", "xlsx", "xls")) ext else detect_file_format(f)
      if (fmt == "csv") {
        df <- readr::read_csv(f, show_col_types = FALSE)
      } else if (fmt %in% c("xlsx", "xls")) {
        df <- readxl::read_excel(f)
      } else {
        stop("Unsupported file format: ", fmt)
      }
      tibble::as_tibble(df) %>% dplyr::mutate(path = f)
    }, error = function(e) {
      message("MAIN WARNING: Failed to read ", basename(f), ": ", e$message)
      NULL
    })
  })
  review_dfs <- review_dfs[!sapply(review_dfs, is.null)]
  if (length(review_dfs) == 0) {
    stop("All review files failed to read")
  }
  df_amz_review <- dplyr::bind_rows(review_dfs)

  # Normalize multilingual source columns into the canonical review schema
  col_mapping <- c(
    "ASIN" = "variation",
    "asin" = "variation",
    "评论时间" = "date",
    "評論時間" = "date",
    "date" = "date",
    "评论人" = "author",
    "評論人" = "author",
    "author" = "author",
    "VP评论" = "verified",
    "VP評論" = "verified",
    "verified" = "verified",
    "赞同数" = "helpful",
    "贊同數" = "helpful",
    "helpful" = "helpful",
    "标题" = "title",
    "標題" = "title",
    "title" = "title",
    "内容" = "body",
    "內容" = "body",
    "body" = "body",
    "星级" = "rating",
    "星級" = "rating",
    "rating" = "rating",
    "图片地址" = "images",
    "圖片地址" = "images",
    "images" = "images",
    "视频地址" = "videos",
    "視頻地址" = "videos",
    "videos" = "videos",
    "评论链接" = "url",
    "評論連結" = "url",
    "url" = "url",
    "型号" = "style",
    "型號" = "style",
    "style" = "style",
    "path" = "path"
  )

  for (old_name in names(col_mapping)) {
    new_name <- col_mapping[[old_name]]
    if (old_name %in% names(df_amz_review) && !(new_name %in% names(df_amz_review))) {
      names(df_amz_review)[names(df_amz_review) == old_name] <- new_name
    }
  }

  required_cols <- c("date", "author", "verified", "helpful", "title", "body",
                     "rating", "images", "videos", "url", "variation", "style", "path")
  missing_cols <- setdiff(required_cols, names(df_amz_review))
  for (col in missing_cols) {
    df_amz_review[[col]] <- NA
  }

  # Keep only canonical columns and normalize key types
  df_amz_review <- as.data.frame(df_amz_review[, required_cols, drop = FALSE])
  df_amz_review$variation <- trimws(as.character(df_amz_review$variation))
  df_amz_review$date <- trimws(as.character(df_amz_review$date))
  df_amz_review$rating <- suppressWarnings(as.numeric(gsub("[^0-9.]", "", as.character(df_amz_review$rating))))

  n_before_filter <- nrow(df_amz_review)
  df_amz_review <- df_amz_review[
    !is.na(df_amz_review$variation) & nzchar(df_amz_review$variation) &
      !is.na(df_amz_review$date) & nzchar(df_amz_review$date),
  , drop = FALSE]
  n_filtered <- n_before_filter - nrow(df_amz_review)
  if (n_filtered > 0) {
    message("MAIN: Filtered ", n_filtered, " rows missing required date/variation")
  }

  # First create table structure with proper schema
  message("MAIN: Creating table structure with proper schema")
  create_sql <- generate_create_table_query(
    con = raw_data,
    target_table = "df_amz_review",
    or_replace = TRUE,
    column_defs = list(
      list(name = "date", type = "VARCHAR", not_null = TRUE),
      list(name = "author", type = "VARCHAR"),
      list(name = "verified", type = "VARCHAR"),
      list(name = "helpful", type = "VARCHAR"),
      list(name = "title", type = "VARCHAR"),
      list(name = "body", type = "TEXT"),
      list(name = "rating", type = "INTEGER"),
      list(name = "images", type = "TEXT"),
      list(name = "videos", type = "TEXT"),
      list(name = "url", type = "VARCHAR"),
      list(name = "variation", type = "VARCHAR", not_null = TRUE),
      list(name = "style", type = "VARCHAR"),
      list(name = "path", type = "VARCHAR", not_null = TRUE)
    )
  )

  dbExecute(raw_data, create_sql)

  # Append data to the properly structured table
  message("MAIN: Appending ", nrow(df_amz_review), " reviews to raw_data")
  dbWriteTable(raw_data, "df_amz_review", df_amz_review, append = TRUE)
  
  script_success <- TRUE
  message("MAIN: ETL reviews Import Phase completed successfully")

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message("MAIN ERROR: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================

if (script_success) {
  tryCatch({
    message("TEST: Verifying ETL reviews Import Phase results...")

    # Check if reviews table exists
    table_name <- "df_amz_review"
    
    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      review_count <- sql_read(raw_data, paste0("SELECT COUNT(*) as count FROM ", table_name))$count

      test_passed <- TRUE
      message("TEST: Verification successful - ", review_count, " reviews in raw_data")
      
      # Show table structure and sample data
      if (review_count > 0) {
        # Check columns
        columns <- dbListFields(raw_data, table_name)
        message("TEST: Review table columns: ", paste(columns, collapse = ", "))
        
        # Show sample data
        sample_query <- paste0("SELECT * FROM ", table_name, " LIMIT 5")
        sample_data <- sql_read(raw_data, sample_query)
        message("TEST: Sample reviews:")
        print(head(sample_data, 3))
        
        # Data quality checks
        if ("asin" %in% columns) {
          unique_asins <- sql_read(raw_data, paste0("SELECT COUNT(DISTINCT asin) as count FROM ", table_name))$count
          message("TEST: Unique ASINs: ", unique_asins)
        }
        
        if ("rating" %in% columns) {
          rating_stats <- sql_read(raw_data, paste0("SELECT MIN(rating) as min_rating, MAX(rating) as max_rating, AVG(rating) as avg_rating FROM ", table_name))
          message("TEST: Rating range: ", rating_stats$min_rating, " to ", rating_stats$max_rating, 
                  " (avg: ", round(rating_stats$avg_rating, 2), ")")
        }
        
        if ("path" %in% columns) {
          source_files <- sql_read(raw_data, paste0("SELECT COUNT(DISTINCT path) as count FROM ", table_name))$count
          message("TEST: Data imported from ", source_files, " source files")
        }
        
      } else {
        message("TEST: Table exists but is empty (no data files found)")
      }
      
      # Check for expected columns (original Amazon format)
      expected_cols <- c("date", "author", "body", "rating", "variation", "path")
      actual_cols <- dbListFields(raw_data, table_name)
      missing_cols <- setdiff(expected_cols, actual_cols)
      extra_cols <- setdiff(actual_cols, expected_cols)
      
      if (length(missing_cols) > 0) {
        message("TEST WARNING: Missing expected columns: ", paste(missing_cols, collapse = ", "))
      }
      if (length(extra_cols) > 0) {
        message("TEST INFO: Additional columns found: ", paste(extra_cols, collapse = ", "))
      }
      if (length(missing_cols) == 0 && length(extra_cols) == 0) {
        message("TEST: All expected columns present with no extras")
      }
      
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - table ", table_name, " not found")
    }

  }, error = function(e) {
    test_passed <<- FALSE
    message("TEST ERROR: ", e$message)
  })
} else {
  message("TEST: Skipped due to main script failure")
}

# ==============================================================================
# 4. DEINITIALIZE
# ==============================================================================

# Determine final status before tearing down
if (script_success && test_passed) {
  message("DEINITIALIZE: ETL reviews Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL reviews Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL reviews Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL reviews Import Phase (amz_ETL_reviews_0IM.R) completed")

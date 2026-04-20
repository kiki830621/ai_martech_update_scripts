# amz_ETL_comment_properties_0IM.R - Amazon Comment Properties Import
# Following DM_R028, DM_R037 v3.0: Config-Driven Import
# ETL comment_properties Phase 0IM: Import from Google Sheets
# Output: raw_data.duckdb → df_all_comment_property

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

# Initialize environment using autoinit system
# Set required dependencies before initialization
needgoogledrive <- TRUE

# Extend Google API timeout to reduce timeout failures
options(gargle_timeout = 60)

# Initialize using unified autoinit system
autoinit()

# Read ETL profile from config (DM_R037 v3.0: config-driven import)
source(file.path(GLOBAL_DIR, "04_utils", "fn_get_platform_config.R"))
platform_cfg <- get_platform_config("amz")
etl_profile <- platform_cfg$etl_sources$comment_properties
message(sprintf("PROFILE: source_type=%s, version=%s",
                etl_profile$source_type, etl_profile$version))
if (tolower(as.character(etl_profile$source_type %||% "")) != "gsheets") {
  stop(sprintf("VALIDATE FAILED: comment_properties requires source_type='gsheets', got '%s'",
               etl_profile$source_type %||% ""))
}
if (!nzchar(as.character(etl_profile$sheet_id %||% ""))) {
  stop("VALIDATE FAILED: comment_properties profile missing sheet_id")
}

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

message("INITIALIZE: Amazon comment properties import (ETL05 0IM) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL comment_properties Import Phase - Amazon comment properties...")

  src_cfg <- platform_cfg$etl_sources$comment_properties
  gs_id <- googlesheets4::as_sheets_id(src_cfg$sheet_id)

  # Config-driven tab matching (#361): read tab name from df_product_line csv.
  # Naming standard: {product_line_id}_{english-name-kebab}
  # No more hardcoded anchors/keywords/fuzzy matching.
  pick_col <- function(df, candidates) {
    existing <- candidates[candidates %in% names(df)]
    if (length(existing) == 0) {
      return(rep(NA_character_, nrow(df)))
    }
    as.character(df[[existing[1]]])
  }

  active_product_lines <- get_active_product_lines()

  # DM_R054 v2.1: df_product_line is sourced from meta_data.duckdb (loaded
  # into memory by UPDATE_MODE init via fn_load_product_lines). The CSV seed
  # at data/app_data/parameters/scd_type1/df_product_line.csv feeds only
  # all_ETL_meta_init_0IM.R, which writes meta_data.df_product_line.
  # Remediation path for any validation failure below: edit the CSV seed →
  # re-run meta_init ETL → meta_data.duckdb refreshed → re-run this ETL.

  # Fail-fast: validate that comment_property_sheet_tab column exists
  if (!"comment_property_sheet_tab" %in% names(active_product_lines)) {
    stop("VALIDATE FAILED: meta_data.df_product_line missing 'comment_property_sheet_tab' column. ",
         "Add this column to the CSV seed (df_product_line.csv) with the standardized ",
         "Google Sheet tab name for each product line, then re-run all_ETL_meta_init_0IM.R. ",
         "Format: {product_line_id}_{english-name-kebab} (e.g., 'blb_blue-light-blocking-glasses')")
  }

  # Fail-fast: validate that all active product lines have a tab name
  missing_tab <- active_product_lines %>%
    dplyr::filter(is.na(comment_property_sheet_tab) | trimws(comment_property_sheet_tab) == "")
  if (nrow(missing_tab) > 0) {
    stop("VALIDATE FAILED: The following active product_line_id(s) have no comment_property_sheet_tab in meta_data.df_product_line: ",
         paste(missing_tab$product_line_id, collapse = ", "),
         ". Fill in the standardized tab name in the CSV seed, then re-run all_ETL_meta_init_0IM.R.")
  }

  # Verify tabs exist in the actual Google Sheet
  tab_names <- googlesheets4::sheet_properties(gs_id)$name
  configured_tabs <- active_product_lines$comment_property_sheet_tab
  missing_in_sheet <- configured_tabs[!configured_tabs %in% tab_names]
  if (length(missing_in_sheet) > 0) {
    stop("VALIDATE FAILED: The following tab(s) from meta_data.df_product_line do not exist in the Google Sheet: ",
         paste(missing_in_sheet, collapse = ", "),
         ". Either rename the Google Sheet tabs to match, or update the CSV seed (df_product_line.csv) ",
         "and re-run all_ETL_meta_init_0IM.R. Available tabs in sheet: ",
         paste(tab_names, collapse = ", "))
  }

  result_list <- list()
  for (i in seq_len(nrow(active_product_lines))) {
    product_line_id <- active_product_lines$product_line_id[i]
    resolved_tab <- active_product_lines$comment_property_sheet_tab[i]

    message("MAIN: Reading comment properties for ", product_line_id, " from tab '", resolved_tab, "'")
    tab_df <- googlesheets4::read_sheet(gs_id, sheet = resolved_tab, .name_repair = "minimal")
    if (nrow(tab_df) == 0) {
      warning("MAIN: Tab '", resolved_tab, "' is empty - skipping")
      next
    }

    tab_df <- janitor::clean_names(tab_df, ascii = FALSE)

    # New sheet format: 主題, 主題英文, 定義, 評論數, 比例, 例子1, 中文, 例子2, 中文, 例子3, 中文, 類型, 尺度, 來源
    # Also support legacy column names for backward compatibility
    parsed_df <- data.frame(
      product_line_id = product_line_id,
      property_id = seq_len(nrow(tab_df)),
      property_name = pick_col(tab_df, c("主題", "屬性", "property_name")),
      property_name_english = pick_col(tab_df, c("主題英文", "attribute", "property_name_english")),
      frequency = suppressWarnings(as.numeric(pick_col(tab_df, c("評論數", "frequency", "頻率")))),
      proportion = suppressWarnings(as.numeric(pick_col(tab_df, c("比例", "proportion")))),
      definition = pick_col(tab_df, c("定義", "definition")),
      review_1 = pick_col(tab_df, c("例子1", "例子")),
      translation_1 = NA_character_,
      review_2 = pick_col(tab_df, c("例子2")),
      translation_2 = NA_character_,
      review_3 = pick_col(tab_df, c("例子3")),
      translation_3 = NA_character_,
      type = pick_col(tab_df, c("類型", "type", "水準")),
      scale = pick_col(tab_df, c("尺度", "scale")),
      source = pick_col(tab_df, c("來源", "source")),
      note = pick_col(tab_df, c("準則", "note", "備註")),
      stringsAsFactors = FALSE
    )

    parsed_df <- parsed_df %>%
      dplyr::filter(!is.na(property_name), trimws(property_name) != "") %>%
      dplyr::mutate(
        property_name_english = dplyr::if_else(
          is.na(property_name_english) | trimws(property_name_english) == "",
          property_name,
          property_name_english
        ),
        etl_import_source = resolved_tab,
        etl_import_timestamp = Sys.time(),
        etl_phase = "import"
      )

    if (nrow(parsed_df) == 0) {
      warning("MAIN: Parsed 0 rows from tab '", resolved_tab, "'")
      next
    }

    result_list[[product_line_id]] <- parsed_df
    message("MAIN: Parsed ", nrow(parsed_df), " properties for ", product_line_id)
  }

  missing_product_lines <- setdiff(active_product_lines$product_line_id, names(result_list))
  if (length(missing_product_lines) > 0) {
    message("MAIN WARNING: No comment-property source data for product_line_id(s): ",
            paste(missing_product_lines, collapse = ", "))
  }

  if (length(result_list) == 0) {
    stop("No comment property rows parsed from any sheet tab")
  }

  comment_properties <- dplyr::bind_rows(result_list) %>%
    dplyr::distinct(product_line_id, property_id, .keep_all = TRUE)

  DBI::dbWriteTable(raw_data, "df_all_comment_property", comment_properties, overwrite = TRUE)
  message("MAIN: Wrote ", nrow(comment_properties), " rows into df_all_comment_property")

  script_success <- TRUE
  message("MAIN: ETL comment_properties Import Phase completed successfully")

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
    message("TEST: Verifying ETL comment_properties Import Phase results...")

    # Check if comment properties table exists and has data
    table_name <- "df_all_comment_property"
    
    if (table_name %in% dbListTables(raw_data)) {
      # Check row count
      query <- paste0("SELECT COUNT(*) as count FROM ", table_name)
      property_count <- sql_read(raw_data, query)$count

      if (property_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", property_count,
                " comment properties imported to raw_data")
        
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", table_name, " LIMIT 3")
        sample_data <- sql_read(raw_data, structure_query)
        message("TEST: Sample raw data structure:")
        print(sample_data)
        
        # Check for required columns
        required_cols <- c("product_line_id", "property_id", "property_name")
        actual_cols <- names(sample_data)
        missing_cols <- setdiff(required_cols, actual_cols)
        
        if (length(missing_cols) > 0) {
          message("TEST WARNING: Missing expected columns: ", paste(missing_cols, collapse = ", "))
        } else {
          message("TEST: All required columns present")
        }
        
        # Check product line distribution
        product_line_query <- paste0("SELECT product_line_id, COUNT(*) as count FROM ", table_name, " GROUP BY product_line_id")
        product_line_stats <- sql_read(raw_data, product_line_query)
        message("TEST: Product line distribution:")
        print(product_line_stats)
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no comment properties found in table")
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
  message("DEINITIALIZE: ETL comment_properties Import Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL comment_properties Import Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL comment_properties Import Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL comment_properties Import Phase (amz_ETL_comment_properties_0IM.R) completed")

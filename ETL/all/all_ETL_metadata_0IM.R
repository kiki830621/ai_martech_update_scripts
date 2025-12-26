#' @file all_ETL_metadata_0IM.R
#' @requires DBI
#' @requires duckdb
#' @requires googlesheets4
#' @requires yaml
#' @principle SO_P016 Configuration Scope Hierarchy (NEW)
#' @principle SO_P010 Config-Driven Customization
#' @principle SO_P015 No Hardcoded Lookup Tables
#' @principle DM_R004 Section 6: Three-Tier Data Architecture
#' @principle MP064 ETL/Derivation Separation
#' @author Claude
#' @date 2025-12-15
#' @modified 2025-12-15 - Phase 7: Configuration Scope Hierarchy refactoring
#' @title Universal Metadata ETL - Import ALL productlines from Google Sheets
#' @description
#'   Universal ETL script that processes ALL enabled metadata sources.
#'
#'   Per SO_P016 (Configuration Scope Hierarchy):
#'   - Company-specific settings (enabled, sheet_id) come from app_config.yaml
#'   - Schema definitions (target_table, columns) come from global metadata_sources.yaml
#'
#'   Per DM_R004 Section 6 (Three-Tier Architecture):
#'   - Company-specific metadata -> {company}/data/app_data/
#'   - Universal metadata (common) -> global_scripts/global_data/
#'
#'   Data Flow:
#'   1. Read app_config.yaml for company-specific settings
#'   2. Read global metadata_sources.yaml for schema definitions
#'   3. Merge settings and process enabled sources
#'   4. Fetch from Google Sheets
#'   5. Write to appropriate database based on target_scope

# 1. INITIALIZE
autoinit()

# Ensure g_project_root is available (fallback for APP_MODE)
if (!exists("g_project_root") || is.null(g_project_root)) {
  g_project_root <- getwd()
  message("Derived g_project_root: ", g_project_root)
}

# Ensure g_global_scripts_path is available (fallback for APP_MODE)
if (!exists("g_global_scripts_path") || is.null(g_global_scripts_path)) {
  g_global_scripts_path <- file.path(g_project_root, "scripts", "global_scripts")
  message("Derived g_global_scripts_path: ", g_global_scripts_path)
}

# ============================================================================
# STEP 1: Load company-specific settings from app_config.yaml (SO_P016)
# ============================================================================
app_config_path <- file.path(g_project_root, "app_config.yaml")

if (!file.exists(app_config_path)) {
  stop("app_config.yaml not found: ", app_config_path)
}

app_config <- yaml::read_yaml(app_config_path)
company_metadata <- app_config$metadata_sources

if (is.null(company_metadata)) {
  stop("metadata_sources section not found in app_config.yaml")
}

message("Loaded company-specific settings from app_config.yaml")
message("  Company: ", app_config$brand_name %||% "Unknown")
message("  Metadata sources: ", paste(names(company_metadata), collapse = ", "))

# ============================================================================
# STEP 2: Load schema definitions from global metadata_sources.yaml (SO_P016)
# ============================================================================
schema_path <- file.path(
  g_global_scripts_path,
  "03_config",
  "metadata_sources.yaml"
)

if (!file.exists(schema_path)) {
  stop("Global schema file not found: ", schema_path)
}

schema_config <- yaml::read_yaml(schema_path)

# Get company-scope schemas
metadata_schema <- schema_config$metadata_schema
if (is.null(metadata_schema)) {
  metadata_schema <- list()
}

# Get universal-scope schemas
universal_schema <- schema_config$metadata_universal_schema
if (is.null(universal_schema)) {
  universal_schema <- list()
}

# Combine all schemas
all_schemas <- c(metadata_schema, universal_schema)

message("Loaded schema definitions from global_scripts")
message("  Available schemas: ", paste(names(all_schemas), collapse = ", "))

# ============================================================================
# STEP 3: Initialize tracking
# ============================================================================
total_success <- 0
total_failed <- 0
total_skipped <- 0
failed_sources <- character(0)

# ============================================================================
# STEP 4: Helper function to process a single metadata source
# ============================================================================
process_metadata_source <- function(source_name, merged_config) {
  message("\n", strrep("=", 60))
  message("Processing: ", source_name)
  message(strrep("=", 60))

  # Check if enabled (from app_config.yaml)
  if (!isTRUE(merged_config$enabled)) {
    message("  SKIPPED: enabled = false in app_config.yaml")
    return(list(success = FALSE, skipped = TRUE))
  }

  # Validate required fields
  if (is.null(merged_config$sheet_id) || merged_config$sheet_id == "") {
    message("  SKIPPED: sheet_id is empty in app_config.yaml")
    return(list(success = FALSE, skipped = TRUE))
  }

  if (is.null(merged_config$sheet_name) || merged_config$sheet_name == "") {
    message("  SKIPPED: sheet_name is empty in app_config.yaml")
    return(list(success = FALSE, skipped = TRUE))
  }

  message("  Description: ", merged_config$description %||% "N/A")
  message("  Target scope: ", merged_config$target_scope %||% "company")
  message("  Target table: ", merged_config$target_table)

  # Determine database path based on target_scope (from schema)
  target_scope <- merged_config$target_scope %||% "company"

  if (target_scope == "universal") {
    # Universal: global_scripts/global_data/
    db_path <- file.path(
      g_global_scripts_path,
      "global_data",
      merged_config$target_db_file
    )
  } else {
    # Company: {project}/data/app_data/
    db_path <- file.path(
      g_project_root,
      "data", "app_data",
      merged_config$target_db_file
    )
  }

  message("  Database path: ", db_path)

  if (!file.exists(db_path)) {
    message("  ERROR: Database file not found: ", db_path)
    return(list(success = FALSE, skipped = FALSE, error = "Database not found"))
  }

  # Connect to database
  con <- tryCatch({
    DBI::dbConnect(duckdb::duckdb(), db_path)
  }, error = function(e) {
    message("  ERROR connecting to database: ", e$message)
    NULL
  })

  if (is.null(con)) {
    return(list(success = FALSE, skipped = FALSE, error = "Connection failed"))
  }

  # Fetch from Google Sheets
  message("  Fetching from Google Sheets...")
  message("    Sheet ID: ", merged_config$sheet_id)
  message("    Sheet Name: ", merged_config$sheet_name)

  df_metadata <- tryCatch({
    googlesheets4::read_sheet(
      ss = merged_config$sheet_id,
      sheet = merged_config$sheet_name
    )
  }, error = function(e) {
    message("  ERROR reading Google Sheet: ", e$message)
    NULL
  })

  if (is.null(df_metadata)) {
    DBI::dbDisconnect(con)
    return(list(success = FALSE, skipped = FALSE, error = "Google Sheet read failed"))
  }

  message("  Rows fetched: ", nrow(df_metadata))
  message("  Columns: ", paste(names(df_metadata), collapse = ", "))

  # Clean data frame for DuckDB compatibility (DM_R025: Type Conversion R/DuckDB)
  # 1. Remove columns with empty names (...)
  valid_cols <- !grepl("^\\.\\.\\.\\d+$", names(df_metadata)) & names(df_metadata) != ""
  df_metadata <- df_metadata[, valid_cols, drop = FALSE]

  # 2. Convert list columns to character (DM_R024: List Column Handling)
  for (col_name in names(df_metadata)) {
    if (is.list(df_metadata[[col_name]])) {
      df_metadata[[col_name]] <- sapply(df_metadata[[col_name]], function(x) {
        if (is.null(x) || length(x) == 0) NA_character_
        else paste(as.character(x), collapse = "; ")
      })
    }
  }

  message("  Cleaned columns: ", paste(names(df_metadata), collapse = ", "))

  # Write to database
  write_result <- tryCatch({
    DBI::dbWriteTable(
      conn = con,
      name = merged_config$target_table,
      value = as.data.frame(df_metadata),
      overwrite = TRUE
    )
    TRUE
  }, error = function(e) {
    message("  ERROR writing to database: ", e$message)
    FALSE
  })

  if (!write_result) {
    DBI::dbDisconnect(con)
    return(list(success = FALSE, skipped = FALSE, error = "Database write failed"))
  }

  message("  Written to table: ", merged_config$target_table)

  # Verify
  row_count <- tryCatch({
    DBI::dbGetQuery(
      con,
      sprintf("SELECT COUNT(*) as n FROM %s", merged_config$target_table)
    )$n
  }, error = function(e) {
    NA
  })

  if (!is.na(row_count) && row_count == nrow(df_metadata)) {
    message("  Verification PASSED: ", row_count, " rows in database")
  } else {
    message("  Verification WARNING: Row count mismatch")
  }

  # Disconnect
  DBI::dbDisconnect(con)

  message("  SUCCESS")
  return(list(success = TRUE, skipped = FALSE, rows = nrow(df_metadata)))
}

# ============================================================================
# STEP 5: MAIN - Process all metadata sources
# ============================================================================
tryCatch({
  # Use gs4_deauth() for anonymous reading (requires sheets to be public)
  googlesheets4::gs4_deauth()

  # Process each source defined in app_config.yaml
  for (source_name in names(company_metadata)) {
    company_settings <- company_metadata[[source_name]]

    # Get schema from global config
    global_schema <- all_schemas[[source_name]]

    if (is.null(global_schema)) {
      message("\n", strrep("=", 60))
      message("Processing: ", source_name)
      message(strrep("=", 60))
      message("  SKIPPED: No schema definition found in global metadata_sources.yaml")
      total_skipped <- total_skipped + 1
      next
    }

    # Merge: global schema + company settings (company settings override)
    merged_config <- modifyList(global_schema, company_settings)

    result <- process_metadata_source(source_name, merged_config)

    if (isTRUE(result$skipped)) {
      total_skipped <- total_skipped + 1
    } else if (isTRUE(result$success)) {
      total_success <- total_success + 1
    } else {
      total_failed <- total_failed + 1
      failed_sources <- c(failed_sources, source_name)
    }
  }

}, error = function(e) {
  message("Error in MAIN section: ", e$message)
})

# ============================================================================
# STEP 6: SUMMARY
# ============================================================================
message("\n", strrep("=", 60))
message("ETL SUMMARY")
message(strrep("=", 60))
message("  Sources processed successfully: ", total_success)
message("  Sources skipped: ", total_skipped)
message("  Sources failed: ", total_failed)
if (length(failed_sources) > 0) {
  message("  Failed sources: ", paste(failed_sources, collapse = ", "))
}

# ============================================================================
# STEP 7: DEINITIALIZE
# ============================================================================
tryCatch({
  if (total_failed == 0 && total_success > 0) {
    message("\nAll ETL operations completed successfully!")
    final_status <- TRUE
  } else if (total_success > 0) {
    message("\nETL completed with some failures")
    final_status <- FALSE
  } else {
    message("\nNo metadata sources were processed")
    final_status <- FALSE
  }
}, error = function(e) {
  message("Error in DEINITIALIZE: ", e$message)
  final_status <- FALSE
}, finally = {
  message("Script completed at ", Sys.time())
})

if (exists("final_status")) {
  final_status
} else {
  FALSE
}

autodeinit()

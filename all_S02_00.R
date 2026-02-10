# all_S02_00.R - AI Data Analysis Support Sequence
# S02 Sequence Step 00: Export all database contents for AI analysis
# Platform: All (cross-platform)

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Set destination directory for exports (can be adjusted)
export_destination <- file.path("data", "database_to_csv")

# Remove existing export folder to ensure fresh export
if (dir.exists(export_destination)) {
  message("S02_00: Cleaning existing export directory - ", export_destination)
  unlink(export_destination, recursive = TRUE, force = TRUE)
}

# Create destination directory if it doesn't exist
if (!dir.exists(export_destination)) {
  dir.create(export_destination, recursive = TRUE, showWarnings = FALSE)
}

# ==============================================================================
# INITIALIZE
# ==============================================================================

autoinit()

message("S02_00: Starting AI data analysis support sequence")
message("S02_00: Export destination - ", export_destination)

# ==============================================================================
# MAIN EXPORT PROCESS
# ==============================================================================

# Auto-export all databases from db_path_list
message("S02_00: Found ", length(db_path_list), " databases in db_path_list")

for (db_name in names(db_path_list)) {
  db_path <- db_path_list[[db_name]]
  
  # Skip if path is NULL, NA, or empty
  if (is.null(db_path) || is.na(db_path) || length(db_path) == 0) {
    message("S02_00: Skipping ", db_name, " - invalid path")
    next
  }
  
  # Skip if database file doesn't exist (for read-only operations)
  if (!file.exists(db_path)) {
    message("S02_00: Skipping ", db_name, " - file doesn't exist: ", db_path)
    next
  }
  
  message("S02_00: Exporting ", db_name, " database...")
  
  tryCatch({
    # Create database-specific subdirectory
    db_export_dir <- file.path(export_destination, db_name)
    if (!dir.exists(db_export_dir)) {
      dir.create(db_export_dir, recursive = TRUE, showWarnings = FALSE)
    }
    
    # Connect to database
    db_conn <- dbConnectDuckdb(db_path, read_only = TRUE)
    
    # Export all tables to database-specific directory
    export_duckdb_dataframes(con = db_conn,
                           out_dir = db_export_dir,
                           overwrite = TRUE)
    
    # Disconnect
    DBI::dbDisconnect(db_conn)
    
    message("S02_00: Successfully exported ", db_name, " to ", db_export_dir)
    
  }, error = function(e) {
    warning("S02_00: Failed to export ", db_name, ": ", e$message)
  })
}

# ==============================================================================
# COMPLETION
# ==============================================================================

message("S02_00: AI data analysis support sequence completed")
message("S02_00: All database contents exported to - ", export_destination)

autodeinit()

#' @file D00_02_P00.R
#' @requires DBI
#' @requires duckdb
#' @requires fn_create_df_dna_by_customer.R
#' @principle MP058 Database Table Creation Strategy
#' @principle R092 Universal DBI Approach
#' @principle R113 Four-Part Update Script Structure
#' @principle MP47 Functional Programming
#' @author Claude
#' @date 2025-04-15
#' @modified 2025-05-18
#' @title DNA by Customer Table Creation
#' @description Creates the DNA by customer table as defined in D00_create_app_data_frames.md

# 1. INITIALIZE
autoinit()

# Connect to app database if not already connected
if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
  app_data <- dbConnectDuckdb(db_path_list$app_data)
  connection_created <- TRUE
  message("Connected to app_data database")
} else {
  connection_created <- FALSE
}

# Initialize error tracking
error_occurred <- FALSE
test_passed <- FALSE

# 2. MAIN
tryCatch({
  # Call the function to create DNA by customer table
  message("Creating DNA by customer table...")
  test_passed <- create_df_dna_by_customer(
    con = app_data,
    or_replace = TRUE,
    verbose = TRUE
  )
  
  if (test_passed) {
    message("Main processing completed successfully")
  } else {
    message("Main processing completed but verification failed")
    error_occurred <- TRUE
  }
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
# Tests are handled within the create_df_dna_by_customer function

# 4. DEINITIALIZE
tryCatch({
  # Close connections opened in this script
  if (exists("connection_created") && connection_created && 
      exists("app_data") && inherits(app_data, "DBIConnection")) {
    dbDisconnect(app_data)
    message("Database connection closed")
  }
  
  # Report final status
  if (test_passed) {
    message("Script executed successfully with all tests passed")
    final_status <- TRUE
  } else {
    message("Script execution incomplete or tests failed")
    final_status <- FALSE
  }
  
}, error = function(e) {
  message("Error in DEINITIALIZE section: ", e$message)
  final_status <- FALSE
}, finally = {
  # This will always execute
  message("Script execution completed at ", Sys.time())
})

# Return final status
if (exists("final_status")) {
  final_status
} else {
  FALSE
}

autodeinit()

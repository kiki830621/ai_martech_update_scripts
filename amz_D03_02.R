# amz_D03_02.R - Rate Reviews for Amazon
# D03_02: Analyze review text to extract sentiment by property
#
# This script processes long-format review data from comment_property_rating 
# database, uses AI to rate reviews against properties, and stores results
# in comment_property_rating_result database using SCD Type 2 methodology.
#
# Data Flow:
# 1. Read long-format data from comment_property_rating database
# 2. Use AI (OpenAI) to rate reviews against properties
# 3. Store results in comment_property_rating_result database (SCD Type 2)
#
# Following principles:
# - MP47: Functional Programming
# - R21: One Function One File
# - R69: Function File Naming
# - R49: Apply Over Loops
# - MP81: Explicit Parameter Specification
# - MP30: Vectorization Principle
# - MP999: Simplified DuckDB Attach

# Initialize environment
needgoogledrive <- TRUE
autoinit()

# Connect to databases with appropriate access
comment_property_rating <- dbConnectDuckdb(db_path_list$comment_property_rating, read_only = TRUE)
comment_property_rating_results <- dbConnectDuckdb(db_path_list$comment_property_rating_results, read_only = FALSE)

# Get OpenAI API key from environment variable
gpt_key <- Sys.getenv("OPENAI_API_KEY")
if (gpt_key == "") {
  stop("OpenAI API key not found. Please set the OPENAI_API_KEY environment variable.")
}

# Configuration parameters
chunk_size <- 20   # Number of records to process in each batch
workers <- 8       # Number of parallel workers (adjust based on your system)
model <- "o4-mini" # OpenAI model to use

# Log beginning of process
message("Starting D03_02 (Rate Reviews) for Amazon product lines")
message("Processing product lines: ", paste(vec_product_line_id_noall, collapse = ", "))

# Process property ratings for all product lines with connection error handling
# Note: Using standardized field names from ETL06
# Reading data directly from source database (not copying)
tryCatch({
  process_property_ratings(
    comment_property_rating = comment_property_rating,
    comment_property_rating_results = comment_property_rating_results,
    vec_product_line_id_noall = vec_product_line_id_noall,
    chunk_size = chunk_size,
    workers = workers,
    gpt_key = gpt_key,
    model = model,
    title = "review_title",  # Standardized field name
    body = "review_body",    # Standardized field name
    input_database = "source"  # Read directly from comment_property_rating
  )
  
  # After processing, check for connection errors in results
  message("Checking for connection errors in processed results...")
  connection_error_count <- 0
  
  for (product_line_id in vec_product_line_id_noall) {
    append_table_name <- paste0("df_comment_property_rating_", product_line_id, "___append_long")
    
    if (DBI::dbExistsTable(comment_property_rating_results, append_table_name)) {
      # Check for connection errors in AI rating results
      error_query <- glue::glue(
        "SELECT COUNT(*) FROM {append_table_name} ",
        "WHERE ai_rating_result LIKE '%Connection_error%' ",
        "OR ai_rating_result LIKE '%HTTP_%' ",
        "OR ai_rating_result LIKE '%API_error%'"
      )
      
      errors_in_table <- DBI::dbGetQuery(comment_property_rating_results, error_query)[1,1]
      connection_error_count <- connection_error_count + errors_in_table
      
      if (errors_in_table > 0) {
        message("Found ", errors_in_table, " connection/API errors in ", append_table_name)
      }
    }
  }
  
  if (connection_error_count > 0) {
    warning("Processing completed but found ", connection_error_count, " connection/API errors in results")
    message("Consider re-running the process to retry failed records")
  } else {
    message("No connection errors detected in processed results")
  }
  
}, error = function(e) {
  error_msg <- as.character(e$message)
  if (grepl("connection|network|timeout|curl|ssl|tls|socket", error_msg, ignore.case = TRUE)) {
    message("Connection error detected in D03_02 processing: ", error_msg)
    stop("D03_02 failed due to connection error: ", error_msg, call. = FALSE)
  } else {
    message("Non-connection error in D03_02 processing: ", error_msg)
    stop("D03_02 failed: ", error_msg, call. = FALSE)
  }
})

# Check table schemas in comment_property_rating_results
for (product_line_id in vec_product_line_id_noall) {
  append_table_name <- paste0("df_comment_property_rating_", product_line_id, "___append_long")
  
  # Check if table exists
  if (DBI::dbExistsTable(comment_property_rating_results, append_table_name)) {
    # Get table info
    table_info <- DBI::dbGetQuery(
      comment_property_rating_results,
      glue::glue("PRAGMA table_info('{append_table_name}');")
    )
    
    # Log table structure
    message("Table structure for ", append_table_name, ":")
    print(table_info[, c("name", "type")])
    
    # Get record count
    record_count <- DBI::dbGetQuery(
      comment_property_rating_results,
      glue::glue("SELECT COUNT(*) FROM {append_table_name}")
    )[1,1]
    
    message("Total records in ", append_table_name, ": ", record_count)
  } else {
    message("Table not found: ", append_table_name)
  }
}

# Clean up and disconnect
autodeinit()

# Log completion
message("Amazon review property rating completed successfully for D03_02 step")
message("Results stored in comment_property_rating_results database using SCD Type 2")
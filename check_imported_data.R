# check_imported_data.R
# Quick check of imported rating results

# Initialize environment
needgoogledrive <- TRUE
autoinit()

# Connect to database
comment_property_rating_results <- dbConnectDuckdb(db_path_list$comment_property_rating_results, read_only = TRUE)

# Check tables
tables <- dbListTables(comment_property_rating_results)
message("Tables in comment_property_rating_results database:")
print(tables)

# Check each table
for (table in tables) {
  if (grepl("append_long", table)) {
    message("\n=== Table: ", table, " ===")
    
    # Get record count
    count <- dbGetQuery(comment_property_rating_results, 
                       paste0("SELECT COUNT(*) as count FROM ", table))$count
    message("Record count: ", count)
    
    # Get column names
    columns <- dbGetQuery(comment_property_rating_results, 
                         paste0("PRAGMA table_info(", table, ")"))
    message("Columns: ", paste(columns$name, collapse = ", "))
    
    # Get sample data
    if (count > 0) {
      sample_data <- dbGetQuery(comment_property_rating_results, 
                               paste0("SELECT * FROM ", table, " LIMIT 2"))
      message("Sample data structure:")
      print(str(sample_data))
      
      # Check for standardized field names
      expected_fields <- c("product_id", "reviewer_id", "review_title", "review_body", 
                          "review_date", "ai_rating_result", "ai_rating_gpt_model")
      found_fields <- intersect(expected_fields, names(sample_data))
      message("Found standardized fields: ", paste(found_fields, collapse = ", "))
      
      missing_fields <- setdiff(expected_fields, names(sample_data))
      if (length(missing_fields) > 0) {
        message("Missing standardized fields: ", paste(missing_fields, collapse = ", "))
      }
    }
  }
}

# Clean up
autodeinit()
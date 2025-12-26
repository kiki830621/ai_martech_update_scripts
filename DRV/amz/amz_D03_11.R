# amz_D03_11.R - Create Position Table for Amazon
# D03_11: Combines all processed data into the final position table
#
# Following principles:
# - MP47: Functional Programming
# - R21: One Function One File
# - R69: Function File Naming
# - R49: Apply Over Loops
# - MP81: Explicit Parameter Specification

# Initialize environment
needgoogledrive <- TRUE
autoinit()

# Connect to databases with appropriate access
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = TRUE)
app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)

# Log beginning of process
message("Starting D03_11 (Create Position Table) for Amazon product lines")

# Phase 1: Process each product line data
message("\n== PHASE 1: Processing individual product lines ==")

success_count <- 0
for (product_line_id_i in vec_product_line_id_noall) {
  success <- process_position_table(
    product_line_id = product_line_id_i,
    raw_data = raw_data,
    processed_data = processed_data,
    app_data = app_data,
    paste_ = paste_
  )
  
  if (success) {
    success_count <- success_count + 1
  }
}

# Phase 2: Merge all product line data
merge_success <- merge_position_tables(
  product_line_ids = vec_product_line_id_noall,
  app_data = app_data,
  paste_ = paste_
)

# Phase 3: Finalize the position table
if (merge_success) {
  finalize_success <- finalize_position_table(
    app_data = app_data,
    coalesce_suffix_cols = coalesce_suffix_cols
  )
  
  # Verify the final position table
  verify_position_table(app_data)
  
  # Clean up temporary tables
  cleanup_temp_position_tables(app_data)
}

# Report overall results
message("\n== Overall Results ==")
message("- Product lines processed: ", success_count, " of ", length(vec_product_line_id_noall))
message("- Merge operation: ", if (merge_success) "Successful" else "Failed")
message("- Finalization: ", if (exists("finalize_success") && finalize_success) "Successful" else "Not completed")

# Clean up and disconnect
autodeinit()

# Log completion
message("\nAmazon position table creation completed successfully for D03_11 step")
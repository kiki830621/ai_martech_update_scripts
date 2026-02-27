#####
# CONSUMES: df_comment_property_ratingonly_*, df_amz_competitor_sales
# PRODUCES: position tables in app_data
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: amz_D03_10
#####


#' @title amz_D03_11
#' @description Derivation task
#' @business_rules See script comments for business logic.
#' @platform amz
#' @author MAMBA Development Team
#' @date 2025-12-30
#' @logical_step_id D03_11
#' @logical_step_status implemented

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
failed_lines <- character()
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
  } else {
    failed_lines <- c(failed_lines, product_line_id_i)
  }
}

if (length(failed_lines) > 0) {
  stop(
    "D03_05 failed: per-product-line processing failed for: ",
    paste(failed_lines, collapse = ", ")
  )
}

if (success_count == 0L) {
  stop("D03_05 failed: no product line was successfully processed.")
}

# Phase 2: Merge all product line data
merge_success <- merge_position_tables(
  product_line_ids = vec_product_line_id_noall,
  app_data = app_data,
  paste_ = paste_
)

# Phase 3: Finalize the position table
if (merge_success) {
  finalize_position_args <- list(app_data = app_data)
  finalize_fn_formals <- names(formals(finalize_position_table))
  if ("coalesce_suffix_cols" %in% finalize_fn_formals) {
    finalize_position_args$coalesce_suffix_cols <- coalesce_suffix_cols
  }
  finalize_success <- do.call(finalize_position_table, finalize_position_args)
  
  # Verify the final position table
  verify_position_table(app_data)
  
  # Clean up temporary tables
  cleanup_temp_position_tables(app_data)
}

if (!isTRUE(merge_success)) {
  stop("D03_05 failed: merge_position_tables() returned FALSE.")
}

if (!isTRUE(exists("finalize_success") && finalize_success)) {
  stop("D03_05 failed: finalize_position_table() returned FALSE.")
}

if (!DBI::dbExistsTable(app_data, "df_position")) {
  stop("D03_05 failed: final table app_data.df_position was not created.")
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

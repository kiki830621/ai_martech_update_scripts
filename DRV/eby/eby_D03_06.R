# eby_D03_06.R - Rate Reviews for eBay
# D03_06: Analyze review text to extract sentiment by property
#
# This script processes long-format review data from comment_property_rating
# database, uses AI to rate reviews against properties, and stores results
# in comment_property_rating_results database using SCD Type 2 methodology.
#
# Data Flow:
# 1. Read long-format data from comment_property_rating database
# 2. Use AI (OpenAI) to rate reviews against properties
# 3. Store results in comment_property_rating_results database (SCD Type 2)
#
# Following principles:
# - MP047: Functional Programming
# - SO_R007: One Function One File
# - DEV_R001: Apply Over Loops
# - MP051: Explicit Parameter Specification

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
workers <- 8       # Number of parallel workers
model <- "o4-mini" # OpenAI model to use
retry_errors <- identical(toupper(Sys.getenv("RETRY_ERRORS")), "TRUE")

# Get eBay product lines from comment_property_rating database
tables <- dbListTables(comment_property_rating)
sampled_long_tables <- tables[grepl("___sampled_long$", tables)]
eby_product_lines <- unique(gsub("df_comment_property_rating_(.*)___sampled_long", "\\1", sampled_long_tables))

message("Starting D03_06 (Rate Reviews) for eBay product lines")
message("Processing product lines: ", paste(eby_product_lines, collapse = ", "))
message("Retry mode: ", retry_errors)

# Process property ratings for all product lines
tryCatch({
  process_property_ratings(
    comment_property_rating = comment_property_rating,
    comment_property_rating_results = comment_property_rating_results,
    vec_product_line_id_noall = eby_product_lines,
    chunk_size = chunk_size,
    workers = workers,
    gpt_key = gpt_key,
    model = model,
    title = "review_title",
    body = "review_body",
    input_database = "source",  # Read directly from comment_property_rating
    retry_errors = retry_errors
  )

  # After processing, check for connection errors in results
  message("Checking for connection errors in processed results...")
  connection_error_count <- 0

  for (product_line_id in eby_product_lines) {
    append_table_name <- paste0("df_comment_property_rating_", product_line_id, "___append_long")

    if (DBI::dbExistsTable(comment_property_rating_results, append_table_name)) {
      # Check if table has ai_rating_result column (may not exist in legacy tables)
      table_cols <- DBI::dbListFields(comment_property_rating_results, append_table_name)
      if (!"ai_rating_result" %in% table_cols) {
        message("Skipping ", append_table_name, ": no ai_rating_result column (legacy table)")
        next
      }

      # Check for connection errors in AI rating results
      ai_results <- tbl2(comment_property_rating_results, append_table_name) %>%
        dplyr::select(ai_rating_result) %>%
        dplyr::collect()

      errors_in_table <- sum(
        grepl("Connection_error|HTTP_|API_error|Unknown_format", ai_results$ai_rating_result, ignore.case = TRUE),
        na.rm = TRUE
      )
      connection_error_count <- connection_error_count + errors_in_table

      if (errors_in_table > 0) {
        message("Found ", errors_in_table, " connection/API errors in ", append_table_name)
      }
    }
  }

  if (connection_error_count > 0) {
    warning("Processing completed but found ", connection_error_count, " connection/API errors in results")
    message("Consider re-running with RETRY_ERRORS=TRUE to retry failed records")
  } else {
    message("No connection errors detected in processed results")
  }

}, error = function(e) {
  error_msg <- as.character(e$message)
  if (grepl("connection|network|timeout|curl|ssl|tls|socket", error_msg, ignore.case = TRUE)) {
    message("Connection error detected in D03_06 processing: ", error_msg)
    stop("D03_06 failed due to connection error: ", error_msg, call. = FALSE)
  } else {
    message("Non-connection error in D03_06 processing: ", error_msg)
    stop("D03_06 failed: ", error_msg, call. = FALSE)
  }
})

# Check table schemas in comment_property_rating_results
message("\nFinal table status:")
for (product_line_id in eby_product_lines) {
  append_table_name <- paste0("df_comment_property_rating_", product_line_id, "___append_long")
  sampled_table_name <- paste0("df_comment_property_rating_", product_line_id, "___sampled_long")

  if (DBI::dbExistsTable(comment_property_rating_results, append_table_name)) {
    append_count <- tbl2(comment_property_rating_results, append_table_name) %>%
      dplyr::summarise(n = dplyr::n()) %>%
      dplyr::pull(n)

    sampled_count <- tbl2(comment_property_rating, sampled_table_name) %>%
      dplyr::summarise(n = dplyr::n()) %>%
      dplyr::pull(n)

    remaining <- sampled_count - append_count
    status <- if (remaining <= 0) "Complete" else paste0(remaining, " remaining")

    message("  ", product_line_id, ": ", append_count, "/", sampled_count, " (", status, ")")
  } else {
    message("  ", product_line_id, ": No results yet")
  }
}

# Clean up and disconnect
autodeinit()

message("\neBay review property rating completed successfully for D03_06")
message("Results stored in comment_property_rating_results database using SCD Type 2")

#' @file amz_S03_00.R
#' @requires DBI
#' @requires dplyr
#' @requires purrr
#' @requires glue
#' @principle R007 Update Script Naming Convention
#' @principle R113 Update Script Structure
#' @principle MP031 Initialization First
#' @principle MP033 Deinitialization Final
#' @principle R120 Filter Variable Naming Convention
#' @platform AMZ (Amazon)
#' @author Claude
#' @date 2025-05-19
#' @title Create Product Line ASIN Mapping
#' @description Creates a unified table that maps ASINs to their respective product_line_id_filter
#'              by combining data from multiple product profile tables

# 1. INITIALIZE
autoinit()

# Connect to required databases
connection_created_raw <- FALSE
connection_created_processed <- FALSE

if (!exists("raw_data") || !inherits(raw_data, "DBIConnection")) {
  raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
  connection_created_raw <- TRUE
  message("Connected to raw_data database (read-only)")
}

if (!exists("processed_data") || !inherits(processed_data, "DBIConnection")) {
  processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = FALSE)
  connection_created_processed <- TRUE
  message("Connected to processed_data database")
}

# Initialize error tracking
error_occurred <- FALSE
test_passed <- FALSE

# 2. MAIN
tryCatch({
  # Log script start
  message("Starting amz_S03_00: Creating unified product line ASIN mapping")

  # Verify product line vector exists
  if (!exists("vec_product_line_id")) {
    message("Product line vector not found. Creating default vector.")
    vec_product_line_id <- c("all", "jewelry", "kitchenware", "beauty")
  }
  
  # Create version without "all" for individual mappings
  vec_product_line_id_noall <- vec_product_line_id[vec_product_line_id != "all"]
  message("Using product lines: ", paste(vec_product_line_id_noall, collapse = ", "))
  
  # Verify all required tables exist
  missing_tables <- c()
  for (product_line_id in vec_product_line_id_noall) {
    table_name <- paste_("df_product_profile", product_line_id)
    if (!dbExistsTable(raw_data, table_name)) {
      missing_tables <- c(missing_tables, table_name)
    }
  }
  
  if (length(missing_tables) > 0) {
    warning("The following product profile tables are missing: ", paste(missing_tables, collapse = ", "))
  }
  
  # Create union SQL to combine all product line product profiles
  library(glue)
  library(purrr)
  
  # Construct SQL query that unions all product line tables
  union_sql <- glue_collapse(
    map(
      vec_product_line_id_noall,
      ~ {
        tbl_name <- paste0("df_product_profile_", .x)      # ① 先拼出完整表名
        glue_sql(
          "
        SELECT {sql(.x)} AS product_line_id,
               asin
        FROM   {`tbl_name`}",                       # ② 用 {`變數`} 方式安全引用
          .con = raw_data
        )
      }
    ),
    sep = "\nUNION ALL\n"
  )    
  
  message("Executing unified SQL query across all product line tables")
  message(union_sql)
  
  # Execute the union query directly in the database
  df_product_profile_dictionary <- tbl(raw_data, sql(union_sql)) %>% collect() %>% dplyr::distinct()
  
  # Log results
  message(sprintf("Created unified product ASIN mapping with %d entries", nrow(df_product_profile_dictionary)))
  
  # Save to processed_data for use in subsequent steps
  message("Saving unified mapping to processed_data.df_product_asin")
  dbWriteTable(
    processed_data,
    "df_product_profile_dictionary",
    df_product_profile_dictionary,
    overwrite = TRUE
  )
  
  message("Main processing completed successfully")
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
if (!error_occurred) {
  tryCatch({
    # Verify mapping table exists and has data
    if (!dbExistsTable(processed_data, "df_product_asin")) {
      message("Verification failed: Table df_product_asin does not exist")
      test_passed <- FALSE
    } else {
      # Check record count
      mapping_count <- tbl(processed_data, "df_product_asin") %>% count() %>% pull()
      
      if (mapping_count > 0) {
        message("Verification successful: ", mapping_count, " ASIN to product_line_id_filter mappings created")
        
        # Check distribution by product line
        product_line_counts <- tbl(processed_data, "df_product_asin") %>%
          group_by(product_line_id_filter) %>%
          summarize(count = n()) %>%
          collect()
        
        message("Distribution by product line:")
        print(product_line_counts)
        
        # Show sample entries
        message("Sample entries:")
        sample_data <- tbl(processed_data, "df_product_asin") %>%
          group_by(product_line_id_filter) %>%
          slice_head(n = 2) %>%
          ungroup() %>%
          collect()
        print(sample_data)
        
        test_passed <- TRUE
      } else {
        message("Verification failed: Mapping table exists but contains no records")
        test_passed <- FALSE
      }
    }
  }, error = function(e) {
    message("Error in TEST section: ", e$message)
    test_passed <- FALSE
  })
} else {
  message("Skipping tests due to error in MAIN section")
  test_passed <- FALSE
}

# 4. DEINITIALIZE
tryCatch({
  # Clean up connections
  if (exists("connection_created_raw") && connection_created_raw && exists("raw_data")) {
    dbDisconnect(raw_data)
    message("Disconnected from raw_data database")
  }
  
  if (exists("connection_created_processed") && connection_created_processed && exists("processed_data")) {
    dbDisconnect(processed_data)
    message("Disconnected from processed_data database")
  }
  
  # Set final status before deinitialization
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

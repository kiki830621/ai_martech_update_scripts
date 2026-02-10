#' @file amz_D01_06.R
#' @requires DBI
#' @requires dplyr
#' @requires analysis_dna
#' @principle R007 Update Script Naming Convention
#' @principle R113 Update Script Structure
#' @principle R120 Filter Variable Naming Convention
#' @principle MP031 Initialization First
#' @principle MP033 Deinitialization Final
#' @platform AMZ (Amazon)
#' @author Original Author
#' @modified_by Claude
#' @date 2025-05-19
#' @title Analyze Customer DNA
#' @description Analyzes customer data to extract DNA profiles and behavioral patterns for each product line

# 1. INITIALIZE
autoinit()

# Connect to required databases
connection_created_processed <- FALSE
connection_created_app <- FALSE

if (!exists("processed_data") || !inherits(processed_data, "DBIConnection")) {
  processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = TRUE)
  connection_created_processed <- TRUE
  message("Connected to processed_data database")
}

if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
  app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
  connection_created_app <- TRUE
  message("Connected to app_data database")
}

# Initialize error tracking
error_occurred <- FALSE
test_passed <- FALSE

# 2. MAIN
tryCatch({
  # Log script start
  message("Starting amz_D01_06: Analyzing Customer DNA for each product line")

  # Check if required tables exist
  required_tables <- c("df_amazon_sales_by_customer", "df_amazon_sales_by_customer_by_date")
  for (table_name in required_tables) {
    if (!dbExistsTable(processed_data, table_name)) {
      stop(paste("Required table", table_name, "does not exist in processed_data database"))
    }
  }
  
  # Load the customer data
  message("Loading customer data for DNA analysis")
  df_amazon_sales_by_customer <- tbl(processed_data, "df_amazon_sales_by_customer") %>% collect()
  df_amazon_sales_by_customer_by_date <- tbl(processed_data, "df_amazon_sales_by_customer_by_date") %>% collect()
  
  # Verify product_line_id_filter column exists in loaded data
  if (!"product_line_id_filter" %in% colnames(df_amazon_sales_by_customer)) {
    stop("Column 'product_line_id_filter' not found in df_amazon_sales_by_customer")
  }
  
  if (!"product_line_id_filter" %in% colnames(df_amazon_sales_by_customer_by_date)) {
    stop("Column 'product_line_id_filter' not found in df_amazon_sales_by_customer_by_date")
  }
  
  # Get unique product line filters from the data
  vec_product_line_id_filter <- df_amazon_sales_by_customer %>%
    distinct(product_line_id_filter) %>%
    pull(product_line_id_filter)
  
  # Log data loaded
  message(sprintf("Loaded %d customer records and %d customer-date records for DNA analysis", 
                 nrow(df_amazon_sales_by_customer), 
                 nrow(df_amazon_sales_by_customer_by_date)))
  
  message("Found ", length(vec_product_line_id_filter), " product line filters to process: ", 
         paste(vec_product_line_id_filter, collapse = ", "))
  
  # Data already loaded above
  
  # Run DNA analysis for each product line filter
  total_records_processed <- 0
  
  for (current_product_line_filter in vec_product_line_id_filter) {
    message("Processing product line filter: ", current_product_line_filter)
    
    # Filter data for the current product line
    message("Filtering data for product line filter: ", current_product_line_filter)
    
    filtered_customer_data <- df_amazon_sales_by_customer %>%
      filter(product_line_id_filter == current_product_line_filter)
      
    filtered_customer_by_date_data <- df_amazon_sales_by_customer_by_date %>%
      filter(product_line_id_filter == current_product_line_filter)
    
    message(sprintf("Filtered to %d customer records and %d customer-date records for product line filter %s", 
                   nrow(filtered_customer_data),
                   nrow(filtered_customer_by_date_data),
                   current_product_line_filter))
    
    # Run DNA analysis on the filtered data
    message("Starting customer DNA analysis for product line filter ", current_product_line_filter, "...")
    dna_results <- analysis_dna(filtered_customer_data, filtered_customer_by_date_data)
    
    # Output summary of results
    message("DNA analysis completed for product line filter ", current_product_line_filter)
    message("Number of customers analyzed: ", nrow(dna_results$data_by_customer))
    if (!is.null(dna_results$nrec_accu) && !is.null(dna_results$nrec_accu$nrec_accu)) {
      message("Churn prediction accuracy: ", dna_results$nrec_accu$nrec_accu)
    }
    
    # Add platform identifier "amz" and current product line filter ID
    message("Adding platform identifier 'amz' and product line filter ID: ", current_product_line_filter)
    dna_data_with_platform <- dna_results$data_by_customer %>% 
      mutate(
        platform_id = "amz",
        product_line_id_filter = current_product_line_filter
      )
    
    # Write to app_data database
    message("Writing DNA results for product line filter ", current_product_line_filter, " to app_data.df_dna_by_customer")
    dbWriteTable(
      app_data,
      "df_dna_by_customer",
      dna_data_with_platform,
      append = TRUE,
      temporary = FALSE
    )
    
    # Log completion for this product line filter
    message("Successfully wrote ", nrow(dna_data_with_platform), " DNA records for product line filter ", 
           current_product_line_filter, " to app_data.df_dna_by_customer")
    
    total_records_processed <- total_records_processed + nrow(dna_data_with_platform)
  }
  
  # Log successful completion
  message("Main processing completed successfully")
  message("Total DNA records processed across all product line filters: ", total_records_processed)
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})

# 3. TEST
if (!error_occurred) {
  tryCatch({
    # Verify DNA data was written to app_data
    if (!dbExistsTable(app_data, "df_dna_by_customer")) {
      message("Verification failed: Table df_dna_by_customer does not exist in app_data")
      test_passed <- FALSE
    } else {
      # Check for Amazon platform records
      amz_records <- tbl(app_data, "df_dna_by_customer") %>%
        filter(platform_id == "amz") %>%
        count() %>%
        pull()
      
      if (amz_records > 0) {
        message("Verification successful: ", amz_records, " Amazon DNA records found in app_data.df_dna_by_customer")
        
        # Check product line filter distribution
        product_line_filter_counts <- tbl(app_data, "df_dna_by_customer") %>%
          filter(platform_id == "amz") %>%
          group_by(product_line_id_filter) %>%
          summarize(count = n()) %>%
          arrange(desc(count)) %>%
          collect()
        
        message("DNA records by product line filter:")
        print(product_line_filter_counts)
        
        # Check that we processed exactly the product line filters we intended to
        processed_product_line_filters <- product_line_filter_counts %>% pull(product_line_id_filter)
        missing_product_line_filters <- setdiff(vec_product_line_id_filter, processed_product_line_filters)
        
        if (length(missing_product_line_filters) > 0) {
          message("Warning: The following product line filters were not found in the results: ", 
                 paste(missing_product_line_filters, collapse = ", "))
        } else {
          message("All specified product line filters were successfully processed")
        }
        
        # Sample the data from each product line filter
        message("Sample DNA data by product line filter:")
        for (pl_filter in vec_product_line_id_filter) {
          message(paste0("--- Product Line Filter: ", pl_filter, " ---"))
          sample_data <- tbl(app_data, "df_dna_by_customer") %>%
            filter(platform_id == "amz", product_line_id_filter == pl_filter) %>%
            head(3) %>%
            collect()
          if (nrow(sample_data) > 0) {
            print(sample_data %>% select(customer_id, platform_id, product_line_id_filter, m_value, f_value, r_value))
          } else {
            message("No data available for this product line filter")
          }
        }
        
        # Check distribution of DNA metrics by product line filter
        message("DNA metric distribution by product line filter:")
        metrics_by_product_line <- tbl(app_data, "df_dna_by_customer") %>%
          filter(platform_id == "amz") %>%
          group_by(product_line_id_filter) %>%
          summarize(
            avg_m_value = mean(m_value, na.rm = TRUE),
            avg_f_value = mean(f_value, na.rm = TRUE),
            avg_r_value = mean(r_value, na.rm = TRUE),
            count = n()
          ) %>%
          arrange(desc(count)) %>%
          collect()
        
        print(metrics_by_product_line)
        
        test_passed <- TRUE
      } else {
        message("Verification failed: No Amazon DNA records found in app_data.df_dna_by_customer")
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

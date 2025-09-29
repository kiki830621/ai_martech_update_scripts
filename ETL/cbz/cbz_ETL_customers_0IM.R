# cbz_ETL_customers_0IM.R - Cyberbiz Customer Data Import (Data Type Separated)
# ==============================================================================
# Following MP104: ETL Data Flow Separation Principle
# Following DM_R028: ETL Data Type Separation Rule 
# Following MP064: ETL-Derivation Separation Principle
# Following MP092: Platform Code Standard (cbz = Cyberbiz)
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
# Following DM_R026: JSON Serialization Strategy for complex types
#
# ETL Customers Phase 0IM (Import): Pure customer profile data extraction only
# Separated from mixed-type cbz_ETL01_0IM.R per architectural principles
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Customer Import (Data Type Separated)")
message(sprintf("INITIALIZE: 🕐 Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message("INITIALIZE: 📋 Compliance: MP104 (ETL Data Flow Separation) + DM_R028 (Data Type Separation)")

# Initialize using unified autoinit system
autoinit()

# Load required libraries with progress feedback
message("INITIALIZE: 📦 Loading required libraries...")
lib_start <- Sys.time()
library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)
lib_elapsed <- as.numeric(Sys.time() - lib_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Libraries loaded successfully (%.2fs)", lib_elapsed))

# Establish database connections
message("INITIALIZE: 🔗 Connecting to raw_data database...")
db_start <- Sys.time()
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)
db_elapsed <- as.numeric(Sys.time() - db_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Database connection established (%.2fs)", db_elapsed))

init_elapsed <- as.numeric(Sys.time() - script_start_time, units = "secs")
message(sprintf("INITIALIZE: ✅ Initialization completed successfully (%.2fs)", init_elapsed))

# ==============================================================================
# 2. MAIN
# ==============================================================================

main_start_time <- Sys.time()
tryCatch({
  message("MAIN: 🚀 Starting ETL Customer Import - Cyberbiz customer data only...")
  message("MAIN: 📊 Phase progress: Step 1/5 - API credential validation...")

  # Check if API credentials are available
  api_token <- Sys.getenv("CBZ_API_TOKEN")
  api_base_url <- "https://app-store-api.cyberbiz.io/v1"
  api_available <- nchar(api_token) > 0
  
  message(sprintf("MAIN: 🔐 API credentials: %s", if(api_available) "✅ Available" else "❌ Not found"))

  if (api_available) {
    # ===== API Import Path =====
    message("MAIN: 📊 Phase progress: Step 2/5 - API configuration...")
    
    # Implement API rate limiting
    rate_limit_delay <- 0.2  # 200ms between requests = 5 req/sec
    
    # Safe mode configuration
    MAX_PAGES_PER_ENDPOINT <- 20
    message(sprintf("MAIN: ⚠️ SAFE MODE - Limiting to %d pages per endpoint", MAX_PAGES_PER_ENDPOINT))
    message(sprintf("MAIN: ⏱️ Rate limiting: %.2fs delay between requests (%.1f req/sec)", 
                    rate_limit_delay, 1/rate_limit_delay))

    # Helper function for API calls
    cbz_api_call <- function(endpoint, params = list()) {
      call_start <- Sys.time()
      url <- paste0(api_base_url, endpoint)
      
      # Rate limiting
      if (rate_limit_delay > 0.1) {
        message(sprintf("    ⏳ Rate limiting: waiting %.2fs before API call...", rate_limit_delay))
        Sys.sleep(rate_limit_delay)
      } else {
        Sys.sleep(rate_limit_delay)
      }
      
      # Make API request with Bearer token
      response <- httr::GET(
        url,
        httr::add_headers(
          "Authorization" = paste("Bearer", api_token),
          "Content-Type" = "application/json",
          "Accept" = "application/json"
        ),
        query = params,
        httr::timeout(30)
      )
      
      call_elapsed <- as.numeric(Sys.time() - call_start, units = "secs")
      
      # Check for API errors
      if (httr::http_error(response)) {
        status_code <- httr::status_code(response)
        error_content <- httr::content(response, "text", encoding = "UTF-8")
        
        error_msg <- sprintf("API call failed after %.2fs - Status: %d, URL: %s", 
                           call_elapsed, status_code, url)
        
        if (status_code == 401) {
          stop(sprintf("%s - Authentication failed. Please check your CBZ_API_TOKEN", error_msg))
        } else if (status_code == 429) {
          stop(sprintf("%s - Rate limit exceeded. Please wait before retrying", error_msg))
        } else {
          stop(sprintf("%s - Error: %s", error_msg, error_content))
        }
      }
      
      # Parse JSON response
      content <- httr::content(response, "text", encoding = "UTF-8")
      result <- jsonlite::fromJSON(content, flatten = TRUE)
      
      message(sprintf("    ✅ API call completed (%.2fs)", call_elapsed))
      return(result)
    }

    # Function to fetch customer data with progress reporting
    fetch_customers_with_progress <- function(per_page = 50, max_pages = MAX_PAGES_PER_ENDPOINT) {
      message("    🌐 Starting customer data fetch from /customers endpoint...")
      fetch_start <- Sys.time()
      
      all_customers <- list()
      page <- 1
      has_more <- TRUE
      total_customers <- 0
      
      while (has_more && page <= max_pages) {
        page_start <- Sys.time()
        
        # Progress reporting
        progress_pct <- (page - 1) / max_pages * 100
        message(sprintf("    👥 Fetching customers page %d/%d (%.1f%% | %d customers so far)...", 
                        page, max_pages, progress_pct, total_customers))
        
        tryCatch({
          result <- cbz_api_call("/customers", params = list(
            page = page,
            per_page = per_page
          ))
          
          page_elapsed <- as.numeric(Sys.time() - page_start, units = "secs")
          
          # Check if we have customer data
          if (!is.null(result) && length(result) > 0) {
            if (is.data.frame(result) && nrow(result) > 0) {
              
              # Process customer data - focus only on customer-specific fields
              page_customers <- result %>%
                # Add customer-specific metadata
                mutate(
                  import_source = "API",
                  import_timestamp = Sys.time(),
                  platform_code = "cbz"
                )
              
              # Handle any list columns per DM_R026: JSON Serialization Strategy
              list_cols <- names(page_customers)[sapply(page_customers, is.list)]
              if (length(list_cols) > 0) {
                message(sprintf("      🔄 Handling %d list columns per DM_R026...", length(list_cols)))
                for (col in list_cols) {
                  # Serialize list columns to JSON strings for DuckDB compatibility
                  json_col_name <- paste0(col, "_json")
                  page_customers[[json_col_name]] <- sapply(page_customers[[col]], function(x) {
                    if (is.null(x) || length(x) == 0) {
                      return(NA_character_)
                    }
                    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
                  })
                  # Remove original list column
                  page_customers[[col]] <- NULL
                  message(sprintf("        ✅ Serialized column: %s -> %s", col, json_col_name))
                }
              }
              
              all_customers[[page]] <- page_customers
              total_customers <- total_customers + nrow(page_customers)
              
              # Calculate ETA
              total_elapsed <- as.numeric(Sys.time() - fetch_start, units = "secs")
              avg_time_per_page <- total_elapsed / page
              eta_seconds <- avg_time_per_page * (max_pages - page)
              
              message(sprintf("    ✅ Page %d: %d customers (%.2fs) | Total: %d | ETA: %.1fs", 
                              page, nrow(page_customers), page_elapsed, total_customers, eta_seconds))
              
              # Check if less than per_page customers returned (indicates last page)
              if (nrow(result) < per_page) {
                message(sprintf("    🏁 Last page detected (partial page: %d < %d)", 
                                nrow(result), per_page))
                has_more <- FALSE
              }
              
            } else {
              message(sprintf("    📭 Empty result on page %d - ending pagination", page))
              has_more <- FALSE
            }
          } else {
            message(sprintf("    📭 No data on page %d - ending pagination", page))
            has_more <- FALSE
          }
          
          page <- page + 1
          
        }, error = function(e) {
          page_elapsed <- as.numeric(Sys.time() - page_start, units = "secs")
          message(sprintf("    ❌ Page %d failed after %.2fs: %s", page, page_elapsed, e$message))
          has_more <- FALSE
        })
      }
      
      # Final summary
      total_elapsed <- as.numeric(Sys.time() - fetch_start, units = "secs")
      pages_fetched <- length(all_customers)
      
      message(sprintf("    ✅ Customer fetch completed: %d pages, %d customers (%.2fs)", 
                      pages_fetched, total_customers, total_elapsed))
      
      # Combine all customer pages
      if (length(all_customers) > 0) {
        message("    🔄 Combining customer data...")
        combine_start <- Sys.time()
        combined_customers <- bind_rows(all_customers)
        combine_elapsed <- as.numeric(Sys.time() - combine_start, units = "secs")
        
        message(sprintf("    ✅ Customer data combined: %d rows × %d columns (%.2fs)", 
                        nrow(combined_customers), ncol(combined_customers), combine_elapsed))
        return(combined_customers)
      } else {
        message("    📭 No customer data retrieved")
        return(data.frame())
      }
    }

    # ===== Fetch Customer Data =====
    message("MAIN: 📊 Phase progress: Step 3/5 - Customer data extraction...")
    customer_start <- Sys.time()
    
    df_cbz_customers_raw <- tryCatch({
      customers_data <- fetch_customers_with_progress()
      customers_data
    }, error = function(e) {
      customer_elapsed <- as.numeric(Sys.time() - customer_start, units = "secs")
      message(sprintf("    ❌ Customer fetch failed after %.2fs: %s", customer_elapsed, e$message))
      data.frame()
    })

    customer_elapsed <- as.numeric(Sys.time() - customer_start, units = "secs")

    if (nrow(df_cbz_customers_raw) > 0) {
      # Enhanced database write with verification
      message("MAIN: 📊 Phase progress: Step 4/5 - Database storage...")
      db_write_start <- Sys.time()
      
      dbWriteTable(raw_data, "df_cbz_customers___raw", df_cbz_customers_raw, overwrite = TRUE)
      db_write_elapsed <- as.numeric(Sys.time() - db_write_start, units = "secs")
      
      # Verify write
      actual_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as count FROM df_cbz_customers___raw")$count
      
      message(sprintf("MAIN: ✅ Customer data: %d records written and verified (total: %.2fs, db_write: %.2fs)", 
                      actual_count, customer_elapsed, db_write_elapsed))
    } else {
      message(sprintf("MAIN: 📭 No customer data retrieved (%.2fs elapsed)", customer_elapsed))
    }

    script_success <- TRUE

  } else {
    # ===== CSV/Excel Import Path =====
    message("MAIN: 📊 Phase progress: Step 2/5 - Local file import setup...")
    message("MAIN: ❌ No API credentials found (CBZ_API_TOKEN missing)")
    message("MAIN: 📁 Switching to local file import mode...")

    # Define customer-specific directory
    if (!exists("RAW_DATA_DIR")) {
      RAW_DATA_DIR <- file.path(APP_DIR, "data", "local_data", "rawdata_MAMBA")
    }
    
    customers_dir <- file.path(RAW_DATA_DIR, "cbz_customers")
    message(sprintf("MAIN: 📂 Target directory: %s", customers_dir))

    # Enhanced directory and file checking
    message("MAIN: 📊 Phase progress: Step 3/5 - Directory validation...")
    dir_check_start <- Sys.time()
    
    if (!dir.exists(customers_dir)) {
      message("MAIN: 🔨 Customer directory does not exist, creating structure...")
      dir.create(customers_dir, recursive = TRUE, showWarnings = FALSE)

      # Create README for customer-specific files
      readme_path <- file.path(customers_dir, "README.txt")
      readme_content <- c(
        "# Cyberbiz Customer Data Import Directory",
        "# Generated by cbz_ETL_customers_0IM.R (Data Type Separated)",
        sprintf("# Created: %s", Sys.time()),
        "",
        "Place Cyberbiz CUSTOMER CSV or Excel files ONLY in this directory.",
        "This ETL processes customer profile data exclusively.",
        "",
        "Required columns for customer data:",
        "- customer_id (客戶編號)",
        "- customer_name (客戶姓名)",
        "- customer_email (客戶信箱)",
        "- phone_number (電話號碼)",
        "- registration_date (註冊日期)",
        "",
        "Optional columns:",
        "- birth_date (生日)",
        "- gender (性別)",
        "- city (城市)",
        "- postal_code (郵遞區號)",
        "- address (地址)",
        "",
        "NOTE: Sales and order data should be placed in separate directories:",
        "- Sales data → ../cbz_sales/",
        "- Order data → ../cbz_orders/"
      )
      
      writeLines(readme_content, readme_path)
      dir_elapsed <- as.numeric(Sys.time() - dir_check_start, units = "secs")
      message(sprintf("MAIN: ✅ Customer directory and README created (%.2fs)", dir_elapsed))

      # Create customer-specific table structure
      message("MAIN: 🔨 Creating customer table structure...")
      table_create_start <- Sys.time()

      create_sql <- generate_create_table_query(
        con = raw_data,
        target_table = "df_cbz_customers___raw",
        or_replace = TRUE,
        column_defs = list(
          list(name = "customer_id", type = "VARCHAR", not_null = TRUE),
          list(name = "customer_name", type = "VARCHAR"),
          list(name = "customer_email", type = "VARCHAR"),
          list(name = "phone_number", type = "VARCHAR"),
          list(name = "registration_date", type = "VARCHAR"),
          list(name = "birth_date", type = "VARCHAR"),
          list(name = "gender", type = "VARCHAR"),
          list(name = "city", type = "VARCHAR"),
          list(name = "postal_code", type = "VARCHAR"),
          list(name = "address", type = "VARCHAR"),
          list(name = "import_source", type = "VARCHAR", not_null = TRUE),
          list(name = "import_timestamp", type = "TIMESTAMP"),
          list(name = "platform_code", type = "VARCHAR"),
          list(name = "path", type = "VARCHAR")
        )
      )

      dbExecute(raw_data, create_sql)
      table_elapsed <- as.numeric(Sys.time() - table_create_start, units = "secs")
      message(sprintf("MAIN: ✅ Customer table created (%.2fs)", table_elapsed))

    } else {
      # Enhanced file discovery for customer data only
      message("MAIN: 📊 Phase progress: Step 4/5 - Customer file discovery...")
      file_search_start <- Sys.time()
      
      files <- list.files(customers_dir, pattern = "\\.(csv|xlsx?)$",
                         recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
      
      file_search_elapsed <- as.numeric(Sys.time() - file_search_start, units = "secs")
      message(sprintf("MAIN: 🔍 Customer file search completed: %d files found (%.2fs)", 
                      length(files), file_search_elapsed))

      if (length(files) > 0) {
        message("MAIN: 📊 Phase progress: Step 5/5 - Customer file import...")
        
        import_start <- Sys.time()
        df_cbz_customers <- import_csvxlsx(customers_dir)
        import_elapsed <- as.numeric(Sys.time() - import_start, units = "secs")
        
        if (nrow(df_cbz_customers) > 0) {
          # Add customer-specific metadata
          df_cbz_customers <- df_cbz_customers %>%
            mutate(
              import_source = "FILE",
              import_timestamp = Sys.time(),
              platform_code = "cbz"
            )

          # Write to database
          message("    💾 Writing customer data to database...")
          db_write_start <- Sys.time()
          
          dbWriteTable(raw_data, "df_cbz_customers___raw", df_cbz_customers, overwrite = TRUE)
          
          final_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as count FROM df_cbz_customers___raw")$count
          db_write_elapsed <- as.numeric(Sys.time() - db_write_start, units = "secs")
          
          message(sprintf("MAIN: ✅ Customer file import completed: %d records (import: %.2fs, db_write: %.2fs)",
                          final_count, import_elapsed, db_write_elapsed))
        }
      } else {
        message("MAIN: 📭 No customer files found in directory")
      }
    }

    script_success <- TRUE
  }

  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Customer Import completed successfully (%.2fs)", main_elapsed))

}, error = function(e) {
  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  main_error <<- e
  script_success <<- FALSE
  message(sprintf("MAIN: ❌ ERROR after %.2fs: %s", main_elapsed, e$message))
})

# ==============================================================================
# 3. TEST
# ==============================================================================

test_start_time <- Sys.time()

if (script_success) {
  tryCatch({
    message("TEST: 🧪 Starting ETL Customer Import verification...")

    # Test customer-specific table
    table_name <- "df_cbz_customers___raw"
    
    if (table_name %in% dbListTables(raw_data)) {
      customer_count <- dbGetQuery(raw_data, 
        paste0("SELECT COUNT(*) as count FROM ", table_name))$count
      
      test_passed <- TRUE
      message(sprintf("TEST: ✅ Customer table verification: %d records", customer_count))

      if (customer_count > 0) {
        # Customer-specific validation
        columns <- dbListFields(raw_data, table_name)
        message(sprintf("TEST: 📝 Customer table structure (%d columns): %s", 
                        length(columns), paste(columns, collapse = ", ")))

        # Validate customer-specific columns
        required_customer_columns <- c("customer_id", "import_source", "import_timestamp", "platform_code")
        missing_columns <- setdiff(required_customer_columns, columns)
        if (length(missing_columns) > 0) {
          message(sprintf("TEST: ⚠️ Missing required customer columns: %s", 
                          paste(missing_columns, collapse = ", ")))
          test_passed <- FALSE
        } else {
          message("TEST: ✅ All required customer columns present")
        }

        # Customer data quality checks
        if ("customer_id" %in% columns) {
          unique_customers <- dbGetQuery(raw_data, paste0(
            "SELECT COUNT(DISTINCT customer_id) as unique_customers FROM ", table_name
          ))
          message(sprintf("TEST: 👥 Unique customers: %d", unique_customers$unique_customers))
        }

        if ("customer_email" %in% columns) {
          email_stats <- dbGetQuery(raw_data, paste0(
            "SELECT COUNT(CASE WHEN customer_email IS NOT NULL AND customer_email != '' THEN 1 END) as emails_present ",
            "FROM ", table_name
          ))
          message(sprintf("TEST: 📧 Customer emails present: %d", email_stats$emails_present))
        }

        # Customer data source analysis
        if ("import_source" %in% columns) {
          source_counts <- dbGetQuery(raw_data, paste0(
            "SELECT import_source, COUNT(*) as count FROM ", table_name, " GROUP BY import_source"
          ))
          message("TEST: 📊 Customer data sources:")
          print(source_counts)
        }
      }
    } else {
      test_passed <- FALSE
      message(sprintf("TEST: ❌ Customer table '%s' not found", table_name))
    }

    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: ✅ Customer verification completed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    test_passed <<- FALSE
    message(sprintf("TEST: ❌ Customer verification failed after %.2fs: %s", test_elapsed, e$message))
  })
} else {
  message("TEST: ⏭️ Skipped due to main script failure")
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================
# Following DEV_R032: All metrics, reporting, and return value preparation

summarize_start_time <- Sys.time()

# Enhanced status determination
if (script_success && test_passed) {
  message("SUMMARIZE: ✅ ETL Customer Import completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Customer Import failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "customers",
  platform = "cbz",
  compliance = c("MP104", "DM_R028", "MP064", "MP092", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 CUSTOMER ETL SUMMARY")
message("=====================================")
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ ETL Customer Import (cbz_ETL_customers_0IM.R) completed")
message(sprintf("SUMMARIZE: 🏁 Final completion time: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))

# Prepare return value for pipeline orchestration
# Following MP103: Store return status before cleanup
final_return_status <- final_metrics$final_status

summarize_elapsed <- as.numeric(Sys.time() - summarize_start_time, units = "secs")
message(sprintf("SUMMARIZE: ✅ Summary completed (%.2fs)", summarize_elapsed))

# ==============================================================================
# 5. DEINITIALIZE
# ==============================================================================
# Following DEV_R032: Only cleanup operations
# Following MP103: autodeinit() must be the absolute last statement

message("DEINITIALIZE: 🧹 Starting cleanup...")
deinit_start_time <- Sys.time()

# Cleanup database connections
message("DEINITIALIZE: 🔌 Disconnecting database...")
DBI::dbDisconnect(raw_data)

# Log cleanup completion
deinit_elapsed <- as.numeric(Sys.time() - deinit_start_time, units = "secs")
message(sprintf("DEINITIALIZE: ✅ Cleanup completed (%.2fs)", deinit_elapsed))

# Following MP103: autodeinit() removes ALL variables - must be absolute last statement
message("DEINITIALIZE: 🧹 Executing autodeinit()...")
autodeinit()
# NO STATEMENTS AFTER THIS LINE - MP103 COMPLIANCE
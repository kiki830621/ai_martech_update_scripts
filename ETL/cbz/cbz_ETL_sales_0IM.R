# cbz_ETL_sales_0IM.R - Cyberbiz Sales Data Import (Data Type Separated)
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
# ETL Sales Phase 0IM (Import): Pure sales transaction data extraction only
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

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Sales Import (Data Type Separated)")
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
library(tidyr)
lib_elapsed <- as.numeric(Sys.time() - lib_start, units = "secs")
message(sprintf("INITIALIZE: ✅ Libraries loaded successfully (%.2fs)", lib_elapsed))

# Establish database connections using dbConnectDuckdb
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
  message("MAIN: 🚀 Starting ETL Sales Import - Cyberbiz sales data only...")
  message("MAIN: 📊 Phase progress: Step 1/5 - API credential validation...")

  # Check if API credentials are available
  api_token <- Sys.getenv("CBZ_API_TOKEN")
  api_base_url <- "https://app-store-api.cyberbiz.io/v1"
  api_available <- nchar(api_token) > 0
  
  message(sprintf("MAIN: 🔐 API credentials: %s", if(api_available) "✅ Available" else "❌ Not found"))

  if (api_available) {
    # ===== API Import Path =====
    message("MAIN: 📊 Phase progress: Step 2/5 - API configuration...")
    
    # Implement API rate limiting (5 requests per second as per spec)
    rate_limit_delay <- 0.2  # 200ms between requests = 5 req/sec
    
    # Safe mode configuration
    MAX_PAGES_PER_ENDPOINT <- 20  # Limit to 20 pages (1000 records at 50 per page)
    message(sprintf("MAIN: ⚠️ SAFE MODE - Limiting to %d pages per endpoint", MAX_PAGES_PER_ENDPOINT))
    message(sprintf("MAIN: ⏱️ Rate limiting: %.2fs delay between requests (%.1f req/sec)", 
                    rate_limit_delay, 1/rate_limit_delay))

    # Helper function for API calls with Bearer token authentication
    cbz_api_call <- function(endpoint, params = list()) {
      call_start <- Sys.time()
      url <- paste0(api_base_url, endpoint)
      
      # Rate limiting with user feedback
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
      
      # Check for API errors with enhanced context
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

    # Function to fetch orders and extract sales data
    fetch_sales_from_orders <- function(per_page = 50, max_pages = MAX_PAGES_PER_ENDPOINT) {
      message("    🌐 Starting sales extraction from orders API...")
      fetch_start <- Sys.time()
      
      all_sales <- list()
      page <- 1
      has_more <- TRUE
      total_sales <- 0
      
      while (has_more && page <= max_pages) {
        page_start <- Sys.time()
        
        # Progress reporting
        progress_pct <- (page - 1) / max_pages * 100
        message(sprintf("    📄 Fetching orders page %d/%d (%.1f%% | %d sales so far)...", 
                        page, max_pages, progress_pct, total_sales))
        
        tryCatch({
          result <- cbz_api_call("/orders", params = list(
            page = page,
            per_page = per_page
          ))
          
          page_elapsed <- as.numeric(Sys.time() - page_start, units = "secs")
          
          # Check if we have data
          if (!is.null(result) && length(result) > 0) {
            if (is.data.frame(result) && nrow(result) > 0) {
              
              # Extract sales data from orders with line items
              page_sales <- result %>%
                filter(!is.null(line_items) & lengths(line_items) > 0) %>%
                select(everything()) %>%
                # Expand line items into individual sales records
                tidyr::unnest(line_items, keep_empty = FALSE, names_sep = ".") %>%
                # Select and rename columns for sales focus
                mutate(
                  order_id = as.character(id),
                  sales_transaction_id = paste0(order_id, "_", row_number()),
                  import_source = "API",
                  import_timestamp = Sys.time(),
                  platform_code = "cbz"
                )
              
              # Handle any remaining list columns per DM_R026
              list_cols <- names(page_sales)[sapply(page_sales, is.list)]
              if (length(list_cols) > 0) {
                message(sprintf("      🔄 Handling %d list columns per DM_R026...", length(list_cols)))
                for (col in list_cols) {
                  json_col_name <- paste0(col, "_json")
                  page_sales[[json_col_name]] <- sapply(page_sales[[col]], function(x) {
                    if (is.null(x) || length(x) == 0) {
                      return(NA_character_)
                    }
                    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
                  })
                  page_sales[[col]] <- NULL
                  message(sprintf("        ✅ Serialized column: %s -> %s", col, json_col_name))
                }
              }
              
              all_sales[[page]] <- page_sales
              total_sales <- total_sales + nrow(page_sales)
              
              # Calculate ETA
              total_elapsed <- as.numeric(Sys.time() - fetch_start, units = "secs")
              avg_time_per_page <- total_elapsed / page
              eta_seconds <- avg_time_per_page * (max_pages - page)
              
              message(sprintf("    ✅ Page %d: %d sales records (%.2fs) | Total: %d | ETA: %.1fs", 
                              page, nrow(page_sales), page_elapsed, total_sales, eta_seconds))
              
              # Check if less than per_page orders returned (indicates last page)
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
      pages_fetched <- length(all_sales)
      
      message(sprintf("    ✅ Sales extraction completed: %d pages, %d sales records (%.2fs)", 
                      pages_fetched, total_sales, total_elapsed))
      
      # Combine all pages
      if (length(all_sales) > 0) {
        message("    🔄 Combining sales data...")
        combine_start <- Sys.time()
        combined_sales <- bind_rows(all_sales)
        combine_elapsed <- as.numeric(Sys.time() - combine_start, units = "secs")
        
        message(sprintf("    ✅ Sales data combined: %d rows × %d columns (%.2fs)", 
                        nrow(combined_sales), ncol(combined_sales), combine_elapsed))
        return(combined_sales)
      } else {
        message("    📭 No sales data retrieved")
        return(data.frame())
      }
    }

    # ===== Fetch Sales Data =====
    message("MAIN: 📊 Phase progress: Step 3/5 - Sales data extraction...")
    sales_start <- Sys.time()
    
    df_cbz_sales_raw <- tryCatch({
      sales_data <- fetch_sales_from_orders()
      sales_data
    }, error = function(e) {
      sales_elapsed <- as.numeric(Sys.time() - sales_start, units = "secs")
      message(sprintf("    ❌ Sales fetch failed after %.2fs: %s", sales_elapsed, e$message))
      data.frame()
    })

    sales_elapsed <- as.numeric(Sys.time() - sales_start, units = "secs")

    if (nrow(df_cbz_sales_raw) > 0) {
      # Enhanced database write with verification
      message("MAIN: 📊 Phase progress: Step 4/5 - Database storage...")
      db_write_start <- Sys.time()
      
      dbWriteTable(raw_data, "df_cbz_sales___raw", df_cbz_sales_raw, overwrite = TRUE)
      db_write_elapsed <- as.numeric(Sys.time() - db_write_start, units = "secs")
      
      # Verify write
      actual_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as count FROM df_cbz_sales___raw")$count
      
      message(sprintf("MAIN: ✅ Sales data: %d records written and verified (total: %.2fs, db_write: %.2fs)", 
                      actual_count, sales_elapsed, db_write_elapsed))
    } else {
      message(sprintf("MAIN: 📭 No sales data retrieved (%.2fs elapsed)", sales_elapsed))
    }

    script_success <- TRUE

  } else {
    # ===== Enhanced CSV/Excel Import Path =====
    message("MAIN: 📊 Phase progress: Step 2/5 - Local file import setup...")
    message("MAIN: ❌ No API credentials found (CBZ_API_TOKEN missing)")
    message("MAIN: 📁 Switching to local file import mode...")

    # Define RAW_DATA_DIR if not set
    if (!exists("RAW_DATA_DIR")) {
      RAW_DATA_DIR <- file.path(APP_DIR, "data", "local_data", "rawdata_MAMBA")
    }
    
    sales_dir <- file.path(RAW_DATA_DIR, "cbz_sales")
    message(sprintf("MAIN: 📂 Target directory: %s", sales_dir))

    # Enhanced directory and file checking
    message("MAIN: 📊 Phase progress: Step 3/5 - Directory validation...")
    dir_check_start <- Sys.time()
    
    if (!dir.exists(sales_dir)) {
      message("MAIN: 🔨 Sales directory does not exist, creating structure...")
      dir.create(sales_dir, recursive = TRUE, showWarnings = FALSE)

      # Create README for sales-specific files
      readme_path <- file.path(sales_dir, "README.txt")
      readme_content <- c(
        "# Cyberbiz Sales Data Import Directory",
        "# Generated by cbz_ETL_sales_0IM.R (Data Type Separated)",
        sprintf("# Created: %s", Sys.time()),
        "",
        "Place Cyberbiz SALES CSV or Excel files ONLY in this directory.",
        "This ETL processes sales transaction data exclusively.",
        "",
        "Required columns for sales data:",
        "- order_id (訂單編號)",
        "- product_id (產品編號)", 
        "- product_name (產品名稱)",
        "- quantity (數量)",
        "- unit_price (單價)",
        "- total_amount (總金額)",
        "- order_date (訂單日期)",
        "",
        "NOTE: Customer and order data should be placed in separate directories:",
        "- Customer data → ../cbz_customers/",
        "- Order data → ../cbz_orders/"
      )
      
      writeLines(readme_content, readme_path)
      dir_elapsed <- as.numeric(Sys.time() - dir_check_start, units = "secs")
      message(sprintf("MAIN: ✅ Sales directory and README created (%.2fs)", dir_elapsed))

      # Create sales-specific table structure
      message("MAIN: 🔨 Creating sales table structure...")
      table_create_start <- Sys.time()

      create_sql <- generate_create_table_query(
        con = raw_data,
        target_table = "df_cbz_sales___raw",
        or_replace = TRUE,
        column_defs = list(
          list(name = "order_id", type = "VARCHAR", not_null = TRUE),
          list(name = "sales_transaction_id", type = "VARCHAR"),
          list(name = "product_id", type = "VARCHAR"),
          list(name = "product_name", type = "VARCHAR"),
          list(name = "quantity", type = "INTEGER"),
          list(name = "unit_price", type = "NUMERIC"),
          list(name = "total_amount", type = "NUMERIC"),
          list(name = "order_date", type = "VARCHAR", not_null = TRUE),
          list(name = "import_source", type = "VARCHAR", not_null = TRUE),
          list(name = "import_timestamp", type = "TIMESTAMP"),
          list(name = "platform_code", type = "VARCHAR"),
          list(name = "path", type = "VARCHAR")
        )
      )

      dbExecute(raw_data, create_sql)
      table_elapsed <- as.numeric(Sys.time() - table_create_start, units = "secs")
      message(sprintf("MAIN: ✅ Sales table created (%.2fs)", table_elapsed))

    } else {
      # Enhanced file discovery for sales data only
      message("MAIN: 📊 Phase progress: Step 4/5 - Sales file discovery...")
      file_search_start <- Sys.time()
      
      files <- list.files(sales_dir, pattern = "\\.(csv|xlsx?)$",
                         recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
      
      file_search_elapsed <- as.numeric(Sys.time() - file_search_start, units = "secs")
      message(sprintf("MAIN: 🔍 Sales file search completed: %d files found (%.2fs)", 
                      length(files), file_search_elapsed))

      if (length(files) > 0) {
        message("MAIN: 📊 Phase progress: Step 5/5 - Sales file import...")
        
        import_start <- Sys.time()
        df_cbz_sales <- import_csvxlsx(sales_dir)
        import_elapsed <- as.numeric(Sys.time() - import_start, units = "secs")
        
        if (nrow(df_cbz_sales) > 0) {
          # Add sales-specific metadata
          df_cbz_sales <- df_cbz_sales %>%
            mutate(
              import_source = "FILE",
              import_timestamp = Sys.time(),
              platform_code = "cbz"
            )

          # Write to database
          message("    💾 Writing sales data to database...")
          db_write_start <- Sys.time()
          
          dbWriteTable(raw_data, "df_cbz_sales___raw", df_cbz_sales, overwrite = TRUE)
          
          final_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as count FROM df_cbz_sales___raw")$count
          db_write_elapsed <- as.numeric(Sys.time() - db_write_start, units = "secs")
          
          message(sprintf("MAIN: ✅ Sales file import completed: %d records (import: %.2fs, db_write: %.2fs)",
                          final_count, import_elapsed, db_write_elapsed))
        }
      } else {
        message("MAIN: 📭 No sales files found in directory")
      }
    }

    script_success <- TRUE
  }

  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Sales Import completed successfully (%.2fs)", main_elapsed))

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
    message("TEST: 🧪 Starting ETL Sales Import verification...")

    # Test sales-specific table
    table_name <- "df_cbz_sales___raw"
    
    if (table_name %in% dbListTables(raw_data)) {
      sales_count <- dbGetQuery(raw_data, 
        paste0("SELECT COUNT(*) as count FROM ", table_name))$count
      
      test_passed <- TRUE
      message(sprintf("TEST: ✅ Sales table verification: %d records", sales_count))

      if (sales_count > 0) {
        # Sales-specific validation
        columns <- dbListFields(raw_data, table_name)
        message(sprintf("TEST: 📝 Sales table structure (%d columns): %s", 
                        length(columns), paste(columns, collapse = ", ")))

        # Validate sales-specific columns
        required_sales_columns <- c("order_id", "import_source", "import_timestamp", "platform_code")
        missing_columns <- setdiff(required_sales_columns, columns)
        if (length(missing_columns) > 0) {
          message(sprintf("TEST: ⚠️ Missing required sales columns: %s", 
                          paste(missing_columns, collapse = ", ")))
          test_passed <- FALSE
        } else {
          message("TEST: ✅ All required sales columns present")
        }

        # Sales data quality checks
        if ("quantity" %in% columns) {
          qty_stats <- dbGetQuery(raw_data, paste0(
            "SELECT MIN(quantity) as min_qty, MAX(quantity) as max_qty, ",
            "AVG(quantity) as avg_qty FROM ", table_name, " WHERE quantity IS NOT NULL"
          ))
          message(sprintf("TEST: 📊 Quantity analysis: min=%s, max=%s, avg=%.2f", 
                          qty_stats$min_qty, qty_stats$max_qty, qty_stats$avg_qty))
        }

        if ("total_amount" %in% columns) {
          amount_stats <- dbGetQuery(raw_data, paste0(
            "SELECT MIN(total_amount) as min_amount, MAX(total_amount) as max_amount, ",
            "AVG(total_amount) as avg_amount FROM ", table_name, " WHERE total_amount IS NOT NULL"
          ))
          message(sprintf("TEST: 💰 Amount analysis: min=%.2f, max=%.2f, avg=%.2f", 
                          amount_stats$min_amount, amount_stats$max_amount, amount_stats$avg_amount))
        }
      }
    } else {
      test_passed <- FALSE
      message(sprintf("TEST: ❌ Sales table '%s' not found", table_name))
    }

    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: ✅ Sales verification completed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    test_passed <<- FALSE
    message(sprintf("TEST: ❌ Sales verification failed after %.2fs: %s", test_elapsed, e$message))
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
  message("SUMMARIZE: ✅ ETL Sales Import completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Sales Import failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "sales",
  platform = "cbz",
  compliance = c("MP104", "DM_R028", "MP064", "MP092", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 SALES ETL SUMMARY")
message("=====================================")
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ ETL Sales Import (cbz_ETL_sales_0IM.R) completed")
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
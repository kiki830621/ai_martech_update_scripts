# cbz_ETL_orders_0IM.R - Cyberbiz Order Data Import (Data Type Separated)
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
# ETL Orders Phase 0IM (Import): Pure order header data extraction only
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

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Order Import (Data Type Separated)")
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
  message("MAIN: 🚀 Starting ETL Order Import - Cyberbiz order data only...")
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

    # Function to fetch order header data (excluding line items)
    fetch_orders_headers_only <- function(per_page = 50, max_pages = MAX_PAGES_PER_ENDPOINT) {
      message("    🌐 Starting order header data fetch from /orders endpoint...")
      fetch_start <- Sys.time()
      
      all_orders <- list()
      page <- 1
      has_more <- TRUE
      total_orders <- 0
      
      while (has_more && page <= max_pages) {
        page_start <- Sys.time()
        
        # Progress reporting
        progress_pct <- (page - 1) / max_pages * 100
        message(sprintf("    📦 Fetching orders page %d/%d (%.1f%% | %d orders so far)...", 
                        page, max_pages, progress_pct, total_orders))
        
        tryCatch({
          result <- cbz_api_call("/orders", params = list(
            page = page,
            per_page = per_page
          ))
          
          page_elapsed <- as.numeric(Sys.time() - page_start, units = "secs")
          
          # Check if we have order data
          if (!is.null(result) && length(result) > 0) {
            if (is.data.frame(result) && nrow(result) > 0) {
              
              # Process order data - focus only on order header fields (exclude line_items)
              page_orders <- result %>%
                # Remove line_items column as it belongs to sales data type
                select(-any_of(c("line_items"))) %>%
                # Add order-specific metadata
                mutate(
                  order_id = as.character(id),
                  import_source = "API",
                  import_timestamp = Sys.time(),
                  platform_code = "cbz"
                )
              
              # Handle any remaining list columns per DM_R026: JSON Serialization Strategy
              list_cols <- names(page_orders)[sapply(page_orders, is.list)]
              if (length(list_cols) > 0) {
                message(sprintf("      🔄 Handling %d list columns per DM_R026...", length(list_cols)))
                for (col in list_cols) {
                  # Serialize list columns to JSON strings for DuckDB compatibility
                  json_col_name <- paste0(col, "_json")
                  page_orders[[json_col_name]] <- sapply(page_orders[[col]], function(x) {
                    if (is.null(x) || length(x) == 0) {
                      return(NA_character_)
                    }
                    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
                  })
                  # Remove original list column
                  page_orders[[col]] <- NULL
                  message(sprintf("        ✅ Serialized column: %s -> %s", col, json_col_name))
                }
              }
              
              all_orders[[page]] <- page_orders
              total_orders <- total_orders + nrow(page_orders)
              
              # Calculate ETA
              total_elapsed <- as.numeric(Sys.time() - fetch_start, units = "secs")
              avg_time_per_page <- total_elapsed / page
              eta_seconds <- avg_time_per_page * (max_pages - page)
              
              message(sprintf("    ✅ Page %d: %d orders (%.2fs) | Total: %d | ETA: %.1fs", 
                              page, nrow(page_orders), page_elapsed, total_orders, eta_seconds))
              
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
      pages_fetched <- length(all_orders)
      
      message(sprintf("    ✅ Order header fetch completed: %d pages, %d orders (%.2fs)", 
                      pages_fetched, total_orders, total_elapsed))
      
      # Combine all order pages
      if (length(all_orders) > 0) {
        message("    🔄 Combining order header data...")
        combine_start <- Sys.time()
        combined_orders <- bind_rows(all_orders)
        combine_elapsed <- as.numeric(Sys.time() - combine_start, units = "secs")
        
        message(sprintf("    ✅ Order data combined: %d rows × %d columns (%.2fs)", 
                        nrow(combined_orders), ncol(combined_orders), combine_elapsed))
        return(combined_orders)
      } else {
        message("    📭 No order data retrieved")
        return(data.frame())
      }
    }

    # ===== Fetch Order Data =====
    message("MAIN: 📊 Phase progress: Step 3/5 - Order header data extraction...")
    order_start <- Sys.time()
    
    df_cbz_orders_raw <- tryCatch({
      orders_data <- fetch_orders_headers_only()
      orders_data
    }, error = function(e) {
      order_elapsed <- as.numeric(Sys.time() - order_start, units = "secs")
      message(sprintf("    ❌ Order fetch failed after %.2fs: %s", order_elapsed, e$message))
      data.frame()
    })

    order_elapsed <- as.numeric(Sys.time() - order_start, units = "secs")

    if (nrow(df_cbz_orders_raw) > 0) {
      # Enhanced database write with verification
      message("MAIN: 📊 Phase progress: Step 4/5 - Database storage...")
      db_write_start <- Sys.time()
      
      dbWriteTable(raw_data, "df_cbz_orders___raw", df_cbz_orders_raw, overwrite = TRUE)
      db_write_elapsed <- as.numeric(Sys.time() - db_write_start, units = "secs")
      
      # Verify write
      actual_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as count FROM df_cbz_orders___raw")$count
      
      message(sprintf("MAIN: ✅ Order data: %d records written and verified (total: %.2fs, db_write: %.2fs)", 
                      actual_count, order_elapsed, db_write_elapsed))
    } else {
      message(sprintf("MAIN: 📭 No order data retrieved (%.2fs elapsed)", order_elapsed))
    }

    script_success <- TRUE

  } else {
    # ===== CSV/Excel Import Path =====
    message("MAIN: 📊 Phase progress: Step 2/5 - Local file import setup...")
    message("MAIN: ❌ No API credentials found (CBZ_API_TOKEN missing)")
    message("MAIN: 📁 Switching to local file import mode...")

    # Define order-specific directory
    if (!exists("RAW_DATA_DIR")) {
      RAW_DATA_DIR <- file.path(APP_DIR, "data", "local_data", "rawdata_MAMBA")
    }
    
    orders_dir <- file.path(RAW_DATA_DIR, "cbz_orders")
    message(sprintf("MAIN: 📂 Target directory: %s", orders_dir))

    # Enhanced directory and file checking
    message("MAIN: 📊 Phase progress: Step 3/5 - Directory validation...")
    dir_check_start <- Sys.time()
    
    if (!dir.exists(orders_dir)) {
      message("MAIN: 🔨 Order directory does not exist, creating structure...")
      dir.create(orders_dir, recursive = TRUE, showWarnings = FALSE)

      # Create README for order-specific files
      readme_path <- file.path(orders_dir, "README.txt")
      readme_content <- c(
        "# Cyberbiz Order Data Import Directory",
        "# Generated by cbz_ETL_orders_0IM.R (Data Type Separated)",
        sprintf("# Created: %s", Sys.time()),
        "",
        "Place Cyberbiz ORDER HEADER CSV or Excel files ONLY in this directory.",
        "This ETL processes order header/metadata exclusively (NOT line items).",
        "",
        "Required columns for order data:",
        "- order_id (訂單編號)",
        "- customer_id (客戶編號)",
        "- order_date (訂單日期)",
        "- order_status (訂單狀態)",
        "- payment_method (付款方式)",
        "- shipping_method (配送方式)",
        "- total_amount (總金額)",
        "",
        "Optional columns:",
        "- shipping_address (配送地址)",
        "- shipping_city (配送城市)",
        "- shipping_postal_code (郵遞區號)",
        "- order_notes (訂單備註)",
        "",
        "NOTE: Line items and product details belong to sales data:",
        "- Sales data (line items) → ../cbz_sales/",
        "- Customer data → ../cbz_customers/"
      )
      
      writeLines(readme_content, readme_path)
      dir_elapsed <- as.numeric(Sys.time() - dir_check_start, units = "secs")
      message(sprintf("MAIN: ✅ Order directory and README created (%.2fs)", dir_elapsed))

      # Create order-specific table structure
      message("MAIN: 🔨 Creating order table structure...")
      table_create_start <- Sys.time()

      create_sql <- generate_create_table_query(
        con = raw_data,
        target_table = "df_cbz_orders___raw",
        or_replace = TRUE,
        column_defs = list(
          list(name = "order_id", type = "VARCHAR", not_null = TRUE),
          list(name = "customer_id", type = "VARCHAR"),
          list(name = "order_date", type = "VARCHAR", not_null = TRUE),
          list(name = "order_status", type = "VARCHAR"),
          list(name = "payment_method", type = "VARCHAR"),
          list(name = "shipping_method", type = "VARCHAR"),
          list(name = "total_amount", type = "NUMERIC"),
          list(name = "shipping_address", type = "VARCHAR"),
          list(name = "shipping_city", type = "VARCHAR"),
          list(name = "shipping_postal_code", type = "VARCHAR"),
          list(name = "order_notes", type = "VARCHAR"),
          list(name = "import_source", type = "VARCHAR", not_null = TRUE),
          list(name = "import_timestamp", type = "TIMESTAMP"),
          list(name = "platform_code", type = "VARCHAR"),
          list(name = "path", type = "VARCHAR")
        )
      )

      dbExecute(raw_data, create_sql)
      table_elapsed <- as.numeric(Sys.time() - table_create_start, units = "secs")
      message(sprintf("MAIN: ✅ Order table created (%.2fs)", table_elapsed))

    } else {
      # Enhanced file discovery for order data only
      message("MAIN: 📊 Phase progress: Step 4/5 - Order file discovery...")
      file_search_start <- Sys.time()
      
      files <- list.files(orders_dir, pattern = "\\.(csv|xlsx?)$",
                         recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
      
      file_search_elapsed <- as.numeric(Sys.time() - file_search_start, units = "secs")
      message(sprintf("MAIN: 🔍 Order file search completed: %d files found (%.2fs)", 
                      length(files), file_search_elapsed))

      if (length(files) > 0) {
        message("MAIN: 📊 Phase progress: Step 5/5 - Order file import...")
        
        import_start <- Sys.time()
        df_cbz_orders <- import_csvxlsx(orders_dir)
        import_elapsed <- as.numeric(Sys.time() - import_start, units = "secs")
        
        if (nrow(df_cbz_orders) > 0) {
          # Add order-specific metadata
          df_cbz_orders <- df_cbz_orders %>%
            mutate(
              import_source = "FILE",
              import_timestamp = Sys.time(),
              platform_code = "cbz"
            )

          # Write to database
          message("    💾 Writing order data to database...")
          db_write_start <- Sys.time()
          
          dbWriteTable(raw_data, "df_cbz_orders___raw", df_cbz_orders, overwrite = TRUE)
          
          final_count <- dbGetQuery(raw_data, "SELECT COUNT(*) as count FROM df_cbz_orders___raw")$count
          db_write_elapsed <- as.numeric(Sys.time() - db_write_start, units = "secs")
          
          message(sprintf("MAIN: ✅ Order file import completed: %d records (import: %.2fs, db_write: %.2fs)",
                          final_count, import_elapsed, db_write_elapsed))
        }
      } else {
        message("MAIN: 📭 No order files found in directory")
      }
    }

    script_success <- TRUE
  }

  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Order Import completed successfully (%.2fs)", main_elapsed))

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
    message("TEST: 🧪 Starting ETL Order Import verification...")

    # Test order-specific table
    table_name <- "df_cbz_orders___raw"
    
    if (table_name %in% dbListTables(raw_data)) {
      order_count <- dbGetQuery(raw_data, 
        paste0("SELECT COUNT(*) as count FROM ", table_name))$count
      
      test_passed <- TRUE
      message(sprintf("TEST: ✅ Order table verification: %d records", order_count))

      if (order_count > 0) {
        # Order-specific validation
        columns <- dbListFields(raw_data, table_name)
        message(sprintf("TEST: 📝 Order table structure (%d columns): %s", 
                        length(columns), paste(columns, collapse = ", ")))

        # Validate order-specific columns
        required_order_columns <- c("order_id", "import_source", "import_timestamp", "platform_code")
        missing_columns <- setdiff(required_order_columns, columns)
        if (length(missing_columns) > 0) {
          message(sprintf("TEST: ⚠️ Missing required order columns: %s", 
                          paste(missing_columns, collapse = ", ")))
          test_passed <- FALSE
        } else {
          message("TEST: ✅ All required order columns present")
        }

        # Order data quality checks
        if ("order_id" %in% columns) {
          unique_orders <- dbGetQuery(raw_data, paste0(
            "SELECT COUNT(DISTINCT order_id) as unique_orders FROM ", table_name
          ))
          message(sprintf("TEST: 📦 Unique orders: %d", unique_orders$unique_orders))
        }

        if ("total_amount" %in% columns) {
          amount_stats <- dbGetQuery(raw_data, paste0(
            "SELECT MIN(total_amount) as min_amount, MAX(total_amount) as max_amount, ",
            "AVG(total_amount) as avg_amount FROM ", table_name, " WHERE total_amount IS NOT NULL"
          ))
          message(sprintf("TEST: 💰 Order amounts: min=%.2f, max=%.2f, avg=%.2f", 
                          amount_stats$min_amount, amount_stats$max_amount, amount_stats$avg_amount))
        }

        if ("order_status" %in% columns) {
          status_counts <- dbGetQuery(raw_data, paste0(
            "SELECT order_status, COUNT(*) as count FROM ", table_name, 
            " WHERE order_status IS NOT NULL GROUP BY order_status ORDER BY count DESC LIMIT 5"
          ))
          message("TEST: 📊 Top order statuses:")
          print(status_counts)
        }

        # Order data source analysis
        if ("import_source" %in% columns) {
          source_counts <- dbGetQuery(raw_data, paste0(
            "SELECT import_source, COUNT(*) as count FROM ", table_name, " GROUP BY import_source"
          ))
          message("TEST: 📊 Order data sources:")
          print(source_counts)
        }
      }
    } else {
      test_passed <- FALSE
      message(sprintf("TEST: ❌ Order table '%s' not found", table_name))
    }

    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: ✅ Order verification completed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    test_passed <<- FALSE
    message(sprintf("TEST: ❌ Order verification failed after %.2fs: %s", test_elapsed, e$message))
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
  message("SUMMARIZE: ✅ ETL Order Import completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Order Import failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "orders",
  platform = "cbz",
  compliance = c("MP104", "DM_R028", "MP064", "MP092", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 ORDER ETL SUMMARY")
message("=====================================")
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ ETL Order Import (cbz_ETL_orders_0IM.R) completed")
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
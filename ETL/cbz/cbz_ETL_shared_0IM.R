# cbz_ETL_shared_0IM.R - Cyberbiz Shared API Import Coordinator (Optional Efficiency Pattern)
# ==============================================================================
# Following MP104: ETL Data Flow Separation Principle - Shared Import Pattern
# Following DM_R028: ETL Data Type Separation Rule - Distribution Pattern
# Following MP064: ETL-Derivation Separation Principle
# Following MP092: Platform Code Standard (cbz = Cyberbiz)
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
# Following DM_R026: JSON Serialization Strategy for complex types
#
# OPTIONAL EFFICIENCY COORDINATOR: Single API call with data type distribution
# Use this instead of individual ETL imports when API efficiency is critical
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Shared Import Coordinator")
message(sprintf("INITIALIZE: 🕐 Start time: %s", format(script_start_time, "%Y-%m-%d %H:%M:%S")))
message("INITIALIZE: 📋 Pattern: Shared API Import with Data Type Distribution")
message("INITIALIZE: 🎯 Purpose: Single API call, multiple data type outputs")

# Initialize using unified autoinit system
autoinit()

# Load required libraries
message("INITIALIZE: 📦 Loading required libraries...")
lib_start <- Sys.time()
library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)
library(tidyr)
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
  message("MAIN: 🚀 Starting Shared API Import Coordinator...")
  message("MAIN: 📊 Phase progress: Step 1/6 - API credential validation...")

  # Check if API credentials are available
  api_token <- Sys.getenv("CBZ_API_TOKEN")
  api_base_url <- "https://app-store-api.cyberbiz.io/v1"
  api_available <- nchar(api_token) > 0
  
  message(sprintf("MAIN: 🔐 API credentials: %s", if(api_available) "✅ Available" else "❌ Not found"))

  if (api_available) {
    # ===== Shared API Import Coordinator =====
    message("MAIN: 📊 Phase progress: Step 2/6 - Coordinator configuration...")
    
    # Implement API rate limiting
    rate_limit_delay <- 0.2  # 200ms between requests = 5 req/sec
    
    # Safe mode configuration
    MAX_PAGES_PER_ENDPOINT <- 20
    message(sprintf("MAIN: ⚠️ COORDINATOR MODE - Fetching from multiple endpoints efficiently"))
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

    # Shared import function with data type distribution
    cbz_shared_import <- function() {
      message("MAIN: 📊 Phase progress: Step 3/6 - Shared data import...")
      import_start <- Sys.time()
      
      # Initialize counters for all data types
      data_type_counts <- list(
        customers = 0,
        orders = 0,
        sales = 0,
        products = 0
      )
      
      # ===== 1. Fetch Customers =====
      message("    👥 Fetching customer data...")
      customer_data <- tryCatch({
        result <- cbz_api_call("/customers", params = list(page = 1, per_page = 50))
        if (!is.null(result) && is.data.frame(result) && nrow(result) > 0) {
          processed_customers <- result %>%
            mutate(
              import_source = "SHARED_API",
              import_timestamp = Sys.time(),
              platform_code = "cbz"
            )
          
          # Handle list columns per DM_R026
          list_cols <- names(processed_customers)[sapply(processed_customers, is.list)]
          if (length(list_cols) > 0) {
            for (col in list_cols) {
              json_col_name <- paste0(col, "_json")
              processed_customers[[json_col_name]] <- sapply(processed_customers[[col]], function(x) {
                if (is.null(x) || length(x) == 0) return(NA_character_)
                jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
              })
              processed_customers[[col]] <- NULL
            }
          }
          
          data_type_counts$customers <- nrow(processed_customers)
          processed_customers
        } else {
          data.frame()
        }
      }, error = function(e) {
        message(sprintf("    ⚠️ Customer fetch failed: %s", e$message))
        data.frame()
      })
      
      # ===== 2. Fetch Orders (with separate sales extraction) =====
      message("    📦 Fetching order data...")
      orders_and_sales <- tryCatch({
        result <- cbz_api_call("/orders", params = list(page = 1, per_page = 50))
        if (!is.null(result) && is.data.frame(result) && nrow(result) > 0) {
          
          # Extract order headers (exclude line_items for orders table)
          order_headers <- result %>%
            select(-any_of(c("line_items"))) %>%
            mutate(
              order_id = as.character(id),
              import_source = "SHARED_API",
              import_timestamp = Sys.time(),
              platform_code = "cbz"
            )
          
          # Handle list columns in orders
          list_cols <- names(order_headers)[sapply(order_headers, is.list)]
          if (length(list_cols) > 0) {
            for (col in list_cols) {
              json_col_name <- paste0(col, "_json")
              order_headers[[json_col_name]] <- sapply(order_headers[[col]], function(x) {
                if (is.null(x) || length(x) == 0) return(NA_character_)
                jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
              })
              order_headers[[col]] <- NULL
            }
          }
          
          # Extract sales data (expand line_items)
          sales_transactions <- result %>%
            filter(!is.null(line_items) & lengths(line_items) > 0) %>%
            tidyr::unnest(line_items, keep_empty = FALSE, names_sep = ".") %>%
            mutate(
              order_id = as.character(id),
              sales_transaction_id = paste0(order_id, "_", row_number()),
              import_source = "SHARED_API",
              import_timestamp = Sys.time(),
              platform_code = "cbz"
            )
          
          # Handle remaining list columns in sales
          list_cols <- names(sales_transactions)[sapply(sales_transactions, is.list)]
          if (length(list_cols) > 0) {
            for (col in list_cols) {
              json_col_name <- paste0(col, "_json")
              sales_transactions[[json_col_name]] <- sapply(sales_transactions[[col]], function(x) {
                if (is.null(x) || length(x) == 0) return(NA_character_)
                jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
              })
              sales_transactions[[col]] <- NULL
            }
          }
          
          data_type_counts$orders <- nrow(order_headers)
          data_type_counts$sales <- nrow(sales_transactions)
          
          list(orders = order_headers, sales = sales_transactions)
        } else {
          list(orders = data.frame(), sales = data.frame())
        }
      }, error = function(e) {
        message(sprintf("    ⚠️ Orders/Sales fetch failed: %s", e$message))
        list(orders = data.frame(), sales = data.frame())
      })
      
      # ===== 3. Fetch Products =====
      message("    🛍️ Fetching product data...")
      product_data <- tryCatch({
        result <- cbz_api_call("/products", params = list(page = 1, per_page = 50))
        if (!is.null(result) && is.data.frame(result) && nrow(result) > 0) {
          processed_products <- result %>%
            mutate(
              product_id = as.character(id),
              import_source = "SHARED_API",
              import_timestamp = Sys.time(),
              platform_code = "cbz"
            )
          
          # Handle list columns per DM_R026
          list_cols <- names(processed_products)[sapply(processed_products, is.list)]
          if (length(list_cols) > 0) {
            for (col in list_cols) {
              json_col_name <- paste0(col, "_json")
              processed_products[[json_col_name]] <- sapply(processed_products[[col]], function(x) {
                if (is.null(x) || length(x) == 0) return(NA_character_)
                jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
              })
              processed_products[[col]] <- NULL
            }
          }
          
          data_type_counts$products <- nrow(processed_products)
          processed_products
        } else {
          data.frame()
        }
      }, error = function(e) {
        message(sprintf("    ⚠️ Product fetch failed: %s", e$message))
        data.frame()
      })
      
      import_elapsed <- as.numeric(Sys.time() - import_start, units = "secs")
      message(sprintf("    ✅ Shared import completed (%.2fs)", import_elapsed))
      message(sprintf("    📊 Data counts: %d customers, %d orders, %d sales, %d products",
                      data_type_counts$customers, data_type_counts$orders, 
                      data_type_counts$sales, data_type_counts$products))
      
      return(list(
        customers = customer_data,
        orders = orders_and_sales$orders,
        sales = orders_and_sales$sales,
        products = product_data,
        counts = data_type_counts
      ))
    }

    # ===== Execute Shared Import =====
    message("MAIN: 📊 Phase progress: Step 4/6 - Executing shared import...")
    all_data <- cbz_shared_import()

    # ===== Data Type Distribution =====
    message("MAIN: 📊 Phase progress: Step 5/6 - Distributing to data type tables...")
    distribution_start <- Sys.time()
    
    # Distribute each data type to its respective table
    tables_written <- 0
    
    # Write customer data
    if (nrow(all_data$customers) > 0) {
      dbWriteTable(raw_data, "df_cbz_customers___raw", all_data$customers, overwrite = TRUE)
      tables_written <- tables_written + 1
      message(sprintf("    ✅ Customers distributed: %d records → df_cbz_customers___raw", nrow(all_data$customers)))
    }
    
    # Write order data
    if (nrow(all_data$orders) > 0) {
      dbWriteTable(raw_data, "df_cbz_orders___raw", all_data$orders, overwrite = TRUE)
      tables_written <- tables_written + 1
      message(sprintf("    ✅ Orders distributed: %d records → df_cbz_orders___raw", nrow(all_data$orders)))
    }
    
    # Write sales data
    if (nrow(all_data$sales) > 0) {
      dbWriteTable(raw_data, "df_cbz_sales___raw", all_data$sales, overwrite = TRUE)
      tables_written <- tables_written + 1
      message(sprintf("    ✅ Sales distributed: %d records → df_cbz_sales___raw", nrow(all_data$sales)))
    }
    
    # Write product data
    if (nrow(all_data$products) > 0) {
      dbWriteTable(raw_data, "df_cbz_products___raw", all_data$products, overwrite = TRUE)
      tables_written <- tables_written + 1
      message(sprintf("    ✅ Products distributed: %d records → df_cbz_products___raw", nrow(all_data$products)))
    }
    
    distribution_elapsed <- as.numeric(Sys.time() - distribution_start, units = "secs")
    message(sprintf("MAIN: 📊 Phase progress: Step 6/6 - Distribution completed..."))
    message(sprintf("MAIN: ✅ Data distribution completed: %d tables written (%.2fs)", 
                    tables_written, distribution_elapsed))
    
    script_success <- TRUE

  } else {
    message("MAIN: ❌ No API credentials found (CBZ_API_TOKEN missing)")
    message("MAIN: ℹ️ Shared import coordinator requires API access")
    message("MAIN: 💡 Use individual ETL scripts for file-based imports")
    script_success <- FALSE
  }

  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  if (script_success) {
    message(sprintf("MAIN: ✅ Shared ETL Import Coordinator completed successfully (%.2fs)", main_elapsed))
  } else {
    message(sprintf("MAIN: ⚠️ Shared ETL Import Coordinator skipped (%.2fs)", main_elapsed))
  }

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
    message("TEST: 🧪 Starting Shared Import Coordinator verification...")

    # Test all data type tables
    expected_tables <- c("df_cbz_customers___raw", "df_cbz_orders___raw", 
                        "df_cbz_sales___raw", "df_cbz_products___raw")
    
    tables_found <- 0
    total_records <- 0
    
    for (table_name in expected_tables) {
      if (table_name %in% dbListTables(raw_data)) {
        count <- dbGetQuery(raw_data, paste0("SELECT COUNT(*) as count FROM ", table_name))$count
        tables_found <- tables_found + 1
        total_records <- total_records + count
        
        # Check for shared import source
        source_check <- dbGetQuery(raw_data, paste0(
          "SELECT COUNT(*) as shared_count FROM ", table_name, 
          " WHERE import_source = 'SHARED_API'"
        ))$shared_count
        
        message(sprintf("TEST: ✅ %s: %d records (%d from shared import)", 
                        table_name, count, source_check))
      } else {
        message(sprintf("TEST: ⚠️ %s: not found", table_name))
      }
    }
    
    test_passed <- tables_found > 0
    message(sprintf("TEST: 📊 Summary: %d/%d tables found, %d total records", 
                    tables_found, length(expected_tables), total_records))

    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: ✅ Shared coordinator verification completed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    test_passed <<- FALSE
    message(sprintf("TEST: ❌ Verification failed after %.2fs: %s", test_elapsed, e$message))
  })
} else {
  message("TEST: ⏭️ Skipped due to main script failure or no API access")
  test_passed <- TRUE  # Not a failure if no API access
}

# ==============================================================================
# 4. SUMMARIZE
# ==============================================================================
# Following DEV_R032: All metrics, reporting, and return value preparation

summarize_start_time <- Sys.time()

# Enhanced status determination
if (script_success && test_passed) {
  message("SUMMARIZE: ✅ Shared Import Coordinator completed successfully")
  return_status <- TRUE
} else if (!script_success && is.null(main_error)) {
  message("SUMMARIZE: ℹ️ Shared Import Coordinator skipped (no API access)")
  return_status <- TRUE  # Not a failure condition
} else {
  message("SUMMARIZE: ❌ Shared Import Coordinator failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  coordinator_type = "shared_import",
  platform = "cbz",
  compliance = c("MP104", "DM_R028", "MP064", "MP092", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 SHARED COORDINATOR SUMMARY")
message("=====================================")
message(sprintf("🔗 Coordinator: %s", final_metrics$coordinator_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ Shared Import Coordinator (cbz_ETL_shared_0IM.R) completed")
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
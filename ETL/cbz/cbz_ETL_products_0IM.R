# cbz_ETL_products_0IM.R - Cyberbiz Product Data Import (Data Type Separated)
# ==============================================================================
# Following MP104: ETL Data Flow Separation Principle
# Following DM_R028: ETL Data Type Separation Rule 
# Following MP064: ETL-Derivation Separation Principle
# Following MP092: Platform ID Standard (cbz = Cyberbiz)
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP103: Proper autodeinit() usage as absolute last statement
# Following MP099: Real-Time Progress Reporting
# Following DM_R026: JSON Serialization Strategy for complex types
#
# ETL Products Phase 0IM (Import): Pure product catalog data extraction only
# Separated from mixed-type cbz_ETL01_0IM.R per architectural principles
# ==============================================================================

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
sql_read_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "fn_sql_read.R")
)
sql_read_path <- sql_read_candidates[file.exists(sql_read_candidates)][1]
if (is.na(sql_read_path)) {
  stop("fn_sql_read.R not found in expected paths")
}
source(sql_read_path)
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL
script_start_time <- Sys.time()

message("INITIALIZE: ⚡ Starting Cyberbiz ETL Product Import (Data Type Separated)")
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
  message("MAIN: 🚀 Starting ETL Product Import - Cyberbiz product data only...")
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

    # Function to fetch product catalog data
    fetch_products_with_progress <- function(per_page = 50, max_pages = MAX_PAGES_PER_ENDPOINT) {
      message("    🌐 Starting product data fetch from /products endpoint...")
      fetch_start <- Sys.time()
      
      all_products <- list()
      page <- 1
      has_more <- TRUE
      total_products <- 0
      
      while (has_more && page <= max_pages) {
        page_start <- Sys.time()
        
        # Progress reporting
        progress_pct <- (page - 1) / max_pages * 100
        message(sprintf("    🛍️ Fetching products page %d/%d (%.1f%% | %d products so far)...", 
                        page, max_pages, progress_pct, total_products))
        
        tryCatch({
          result <- cbz_api_call("/products", params = list(
            page = page,
            per_page = per_page
          ))
          
          page_elapsed <- as.numeric(Sys.time() - page_start, units = "secs")
          
          # Check if we have product data
          if (!is.null(result) && length(result) > 0) {
            if (is.data.frame(result) && nrow(result) > 0) {
              
              # Process product data - focus only on product catalog fields
              page_products <- result %>%
                # Add product-specific metadata
                mutate(
                  product_id = as.character(id),
                  import_source = "API",
                  import_timestamp = Sys.time(),
                  platform_id = "cbz"
                )
              
              # Handle any list columns per DM_R026: JSON Serialization Strategy
              list_cols <- names(page_products)[sapply(page_products, is.list)]
              if (length(list_cols) > 0) {
                message(sprintf("      🔄 Handling %d list columns per DM_R026...", length(list_cols)))
                for (col in list_cols) {
                  # Serialize list columns to JSON strings for DuckDB compatibility
                  json_col_name <- paste0(col, "_json")
                  page_products[[json_col_name]] <- sapply(page_products[[col]], function(x) {
                    if (is.null(x) || length(x) == 0) {
                      return(NA_character_)
                    }
                    jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
                  })
                  # Remove original list column
                  page_products[[col]] <- NULL
                  message(sprintf("        ✅ Serialized column: %s -> %s", col, json_col_name))
                }
              }
              
              all_products[[page]] <- page_products
              total_products <- total_products + nrow(page_products)
              
              # Calculate ETA
              total_elapsed <- as.numeric(Sys.time() - fetch_start, units = "secs")
              avg_time_per_page <- total_elapsed / page
              eta_seconds <- avg_time_per_page * (max_pages - page)
              
              message(sprintf("    ✅ Page %d: %d products (%.2fs) | Total: %d | ETA: %.1fs", 
                              page, nrow(page_products), page_elapsed, total_products, eta_seconds))
              
              # Check if less than per_page products returned (indicates last page)
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
      pages_fetched <- length(all_products)
      
      message(sprintf("    ✅ Product fetch completed: %d pages, %d products (%.2fs)", 
                      pages_fetched, total_products, total_elapsed))
      
      # Combine all product pages
      if (length(all_products) > 0) {
        message("    🔄 Combining product data...")
        combine_start <- Sys.time()
        combined_products <- bind_rows(all_products)
        combine_elapsed <- as.numeric(Sys.time() - combine_start, units = "secs")
        
        message(sprintf("    ✅ Product data combined: %d rows × %d columns (%.2fs)", 
                        nrow(combined_products), ncol(combined_products), combine_elapsed))
        return(combined_products)
      } else {
        message("    📭 No product data retrieved")
        return(data.frame())
      }
    }

    # ===== Fetch Product Data =====
    message("MAIN: 📊 Phase progress: Step 3/5 - Product catalog data extraction...")
    product_start <- Sys.time()
    
    df_cbz_products_raw <- tryCatch({
      products_data <- fetch_products_with_progress()
      products_data
    }, error = function(e) {
      product_elapsed <- as.numeric(Sys.time() - product_start, units = "secs")
      message(sprintf("    ❌ Product fetch failed after %.2fs: %s", product_elapsed, e$message))
      data.frame()
    })

    product_elapsed <- as.numeric(Sys.time() - product_start, units = "secs")

    if (nrow(df_cbz_products_raw) > 0) {
      # Enhanced database write with verification
      message("MAIN: 📊 Phase progress: Step 4/5 - Database storage...")
      db_write_start <- Sys.time()
      
      dbWriteTable(raw_data, "df_cbz_products___raw", df_cbz_products_raw, overwrite = TRUE)
      db_write_elapsed <- as.numeric(Sys.time() - db_write_start, units = "secs")
      
      # Verify write
      actual_count <- sql_read(raw_data, "SELECT COUNT(*) as count FROM df_cbz_products___raw")$count
      
      message(sprintf("MAIN: ✅ Product data: %d records written and verified (total: %.2fs, db_write: %.2fs)", 
                      actual_count, product_elapsed, db_write_elapsed))
    } else {
      message(sprintf("MAIN: 📭 No product data retrieved (%.2fs elapsed)", product_elapsed))
    }

    script_success <- TRUE

  } else {
    # ===== CSV/Excel Import Path =====
    message("MAIN: 📊 Phase progress: Step 2/5 - Local file import setup...")
    message("MAIN: ❌ No API credentials found (CBZ_API_TOKEN missing)")
    message("MAIN: 📁 Switching to local file import mode...")

    # Define product-specific directory
    if (!exists("RAW_DATA_DIR")) {
      RAW_DATA_DIR <- file.path(APP_DIR, "data", "local_data", "rawdata_MAMBA")
    }
    
    products_dir <- file.path(RAW_DATA_DIR, "cbz_products")
    message(sprintf("MAIN: 📂 Target directory: %s", products_dir))

    # Enhanced directory and file checking
    message("MAIN: 📊 Phase progress: Step 3/5 - Directory validation...")
    dir_check_start <- Sys.time()
    
    if (!dir.exists(products_dir)) {
      message("MAIN: 🔨 Product directory does not exist, creating structure...")
      dir.create(products_dir, recursive = TRUE, showWarnings = FALSE)

      # Create README for product-specific files
      readme_path <- file.path(products_dir, "README.txt")
      readme_content <- c(
        "# Cyberbiz Product Data Import Directory",
        "# Generated by cbz_ETL_products_0IM.R (Data Type Separated)",
        sprintf("# Created: %s", Sys.time()),
        "",
        "Place Cyberbiz PRODUCT CATALOG CSV or Excel files ONLY in this directory.",
        "This ETL processes product catalog and specification data exclusively.",
        "",
        "Required columns for product data:",
        "- product_id (產品編號)",
        "- product_name (產品名稱)",
        "- product_description (產品描述)",
        "- category (產品類別)",
        "- price (價格)",
        "- cost (成本)",
        "",
        "Optional columns:",
        "- sku (SKU編號)",
        "- brand (品牌)",
        "- weight (重量)",
        "- dimensions (尺寸)",
        "- stock_quantity (庫存數量)",
        "- active (是否啟用)",
        "- created_date (建立日期)",
        "- updated_date (更新日期)",
        "",
        "NOTE: Transaction and customer data belong elsewhere:",
        "- Sales transactions → ../cbz_sales/",
        "- Customer data → ../cbz_customers/",
        "- Order data → ../cbz_orders/"
      )
      
      writeLines(readme_content, readme_path)
      dir_elapsed <- as.numeric(Sys.time() - dir_check_start, units = "secs")
      message(sprintf("MAIN: ✅ Product directory and README created (%.2fs)", dir_elapsed))

      # Create product-specific table structure
      message("MAIN: 🔨 Creating product table structure...")
      table_create_start <- Sys.time()

      create_sql <- generate_create_table_query(
        con = raw_data,
        target_table = "df_cbz_products___raw",
        or_replace = TRUE,
        column_defs = list(
          list(name = "product_id", type = "VARCHAR", not_null = TRUE),
          list(name = "product_name", type = "VARCHAR"),
          list(name = "product_description", type = "TEXT"),
          list(name = "category", type = "VARCHAR"),
          list(name = "price", type = "NUMERIC"),
          list(name = "cost", type = "NUMERIC"),
          list(name = "sku", type = "VARCHAR"),
          list(name = "brand", type = "VARCHAR"),
          list(name = "weight", type = "NUMERIC"),
          list(name = "dimensions", type = "VARCHAR"),
          list(name = "stock_quantity", type = "INTEGER"),
          list(name = "active", type = "BOOLEAN"),
          list(name = "created_date", type = "VARCHAR"),
          list(name = "updated_date", type = "VARCHAR"),
          list(name = "import_source", type = "VARCHAR", not_null = TRUE),
          list(name = "import_timestamp", type = "TIMESTAMP"),
          list(name = "platform_id", type = "VARCHAR"),
          list(name = "path", type = "VARCHAR")
        )
      )

      dbExecute(raw_data, create_sql)
      table_elapsed <- as.numeric(Sys.time() - table_create_start, units = "secs")
      message(sprintf("MAIN: ✅ Product table created (%.2fs)", table_elapsed))

    } else {
      # Enhanced file discovery for product data only
      message("MAIN: 📊 Phase progress: Step 4/5 - Product file discovery...")
      file_search_start <- Sys.time()
      
      files <- list.files(products_dir, pattern = "\\.(csv|xlsx?)$",
                         recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
      
      file_search_elapsed <- as.numeric(Sys.time() - file_search_start, units = "secs")
      message(sprintf("MAIN: 🔍 Product file search completed: %d files found (%.2fs)", 
                      length(files), file_search_elapsed))

      if (length(files) > 0) {
        message("MAIN: 📊 Phase progress: Step 5/5 - Product file import...")
        
        import_start <- Sys.time()
        df_cbz_products <- import_csvxlsx(products_dir)
        import_elapsed <- as.numeric(Sys.time() - import_start, units = "secs")
        
        if (nrow(df_cbz_products) > 0) {
          # Add product-specific metadata
          df_cbz_products <- df_cbz_products %>%
            mutate(
              import_source = "FILE",
              import_timestamp = Sys.time(),
              platform_id = "cbz"
            )

          # Write to database
          message("    💾 Writing product data to database...")
          db_write_start <- Sys.time()
          
          dbWriteTable(raw_data, "df_cbz_products___raw", df_cbz_products, overwrite = TRUE)
          
          final_count <- sql_read(raw_data, "SELECT COUNT(*) as count FROM df_cbz_products___raw")$count
          db_write_elapsed <- as.numeric(Sys.time() - db_write_start, units = "secs")
          
          message(sprintf("MAIN: ✅ Product file import completed: %d records (import: %.2fs, db_write: %.2fs)",
                          final_count, import_elapsed, db_write_elapsed))
        }
      } else {
        message("MAIN: 📭 No product files found in directory")
      }
    }

    script_success <- TRUE
  }

  main_elapsed <- as.numeric(Sys.time() - main_start_time, units = "secs")
  message(sprintf("MAIN: ✅ ETL Product Import completed successfully (%.2fs)", main_elapsed))

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
    message("TEST: 🧪 Starting ETL Product Import verification...")

    # Test product-specific table
    table_name <- "df_cbz_products___raw"
    
    if (table_name %in% dbListTables(raw_data)) {
      product_count <- sql_read(raw_data, 
        paste0("SELECT COUNT(*) as count FROM ", table_name))$count
      
      test_passed <- TRUE
      message(sprintf("TEST: ✅ Product table verification: %d records", product_count))

      if (product_count > 0) {
        # Product-specific validation
        columns <- dbListFields(raw_data, table_name)
        message(sprintf("TEST: 📝 Product table structure (%d columns): %s", 
                        length(columns), paste(columns, collapse = ", ")))

        # Validate product-specific columns
        required_product_columns <- c("product_id", "import_source", "import_timestamp", "platform_id")
        missing_columns <- setdiff(required_product_columns, columns)
        if (length(missing_columns) > 0) {
          message(sprintf("TEST: ⚠️ Missing required product columns: %s", 
                          paste(missing_columns, collapse = ", ")))
          test_passed <- FALSE
        } else {
          message("TEST: ✅ All required product columns present")
        }

        # Product data quality checks
        if ("product_id" %in% columns) {
          unique_products <- sql_read(raw_data, paste0(
            "SELECT COUNT(DISTINCT product_id) as unique_products FROM ", table_name
          ))
          message(sprintf("TEST: 🛍️ Unique products: %d", unique_products$unique_products))
        }

        if ("price" %in% columns) {
          price_stats <- sql_read(raw_data, paste0(
            "SELECT MIN(price) as min_price, MAX(price) as max_price, ",
            "AVG(price) as avg_price FROM ", table_name, " WHERE price IS NOT NULL"
          ))
          message(sprintf("TEST: 💰 Product prices: min=%.2f, max=%.2f, avg=%.2f", 
                          price_stats$min_price, price_stats$max_price, price_stats$avg_price))
        }

        if ("category" %in% columns) {
          category_counts <- sql_read(raw_data, paste0(
            "SELECT category, COUNT(*) as count FROM ", table_name, 
            " WHERE category IS NOT NULL GROUP BY category ORDER BY count DESC LIMIT 5"
          ))
          message("TEST: 📊 Top product categories:")
          print(category_counts)
        }

        if ("brand" %in% columns) {
          brand_stats <- sql_read(raw_data, paste0(
            "SELECT COUNT(DISTINCT brand) as unique_brands FROM ", table_name, 
            " WHERE brand IS NOT NULL"
          ))
          message(sprintf("TEST: 🏷️ Unique brands: %d", brand_stats$unique_brands))
        }

        # Product data source analysis
        if ("import_source" %in% columns) {
          source_counts <- sql_read(raw_data, paste0(
            "SELECT import_source, COUNT(*) as count FROM ", table_name, " GROUP BY import_source"
          ))
          message("TEST: 📊 Product data sources:")
          print(source_counts)
        }
      }
    } else {
      test_passed <- FALSE
      message(sprintf("TEST: ❌ Product table '%s' not found", table_name))
    }

    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    message(sprintf("TEST: ✅ Product verification completed (%.2fs)", test_elapsed))

  }, error = function(e) {
    test_elapsed <- as.numeric(Sys.time() - test_start_time, units = "secs")
    test_passed <<- FALSE
    message(sprintf("TEST: ❌ Product verification failed after %.2fs: %s", test_elapsed, e$message))
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
  message("SUMMARIZE: ✅ ETL Product Import completed successfully")
  return_status <- TRUE
} else {
  message("SUMMARIZE: ❌ ETL Product Import failed")
  return_status <- FALSE
}

# Capture final metrics
final_metrics <- list(
  script_total_elapsed = as.numeric(Sys.time() - script_start_time, units = "secs"),
  final_status = return_status,
  data_type = "products",
  platform = "cbz",
  compliance = c("MP104", "DM_R028", "MP064", "MP092", "DEV_R032", "MP103")
)

# Final summary reporting
message("SUMMARIZE: 📊 PRODUCT ETL SUMMARY")
message("=====================================")
message(sprintf("🏷️  Data Type: %s", final_metrics$data_type))
message(sprintf("🌐 Platform: %s", final_metrics$platform))
message(sprintf("🕐 Total time: %.2fs", final_metrics$script_total_elapsed))
message(sprintf("📈 Status: %s", if(final_metrics$final_status) "SUCCESS ✅" else "FAILED ❌"))
message(sprintf("📋 Compliance: %s", paste(final_metrics$compliance, collapse = ", ")))
message("=====================================")

message("SUMMARIZE: ✅ ETL Product Import (cbz_ETL_products_0IM.R) completed")
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

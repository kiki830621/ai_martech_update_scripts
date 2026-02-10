# ==============================================================================
# MAMBA-Specific eBay Order Details ETL - Staging Phase (1ST) - DEBUG VERSION
# Following MP099: Real-time Progress Reporting
# Following MP106: Console Output Transparency
# Following DM_R037: Company-Specific ETL Naming Rule
# Following MP104: ETL Data Flow Separation Principle
# Following MP064: ETL-Derivation Separation Principle
# ==============================================================================
# Company: MAMBA
# Platform: eBay (eby) - Custom SQL Server Implementation
# Data Type: Order Details (BAYORE table - order line items)
# Phase: 1ST (Staging)
# 
# DEBUG VERSION: Enhanced with comprehensive error handling and progress reporting
# ==============================================================================

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
# Following DEV_R032: Five-Part Script Structure Standard
# Following MP031: Initialization First
# Following DM_R039: Database Connection Pattern Rule

message(strrep("=", 80))
message("🔍 DEBUG MODE: MAMBA eBay Order Details Staging")
message("DEBUG: Script: eby_ETL_order_details_1ST___MAMBA_DEBUG.R")
message("DEBUG: Company-specific implementation for MAMBA")
message("DEBUG: Data type: Order Details (BAYORE staging)")
message(sprintf("DEBUG: Started at: %s", Sys.time()))
message(strrep("=", 80))

# ------------------------------------------------------------------------------
# 1.1: Basic Initialization
# ------------------------------------------------------------------------------

# Script metadata
script_start_time <- Sys.time()
script_name <- "eby_ETL_order_details_1ST___MAMBA_DEBUG"
script_version <- "2.0.0-DEBUG"  # Debug version

message("DEBUG: Setting up error handling...")
options(error = function() {
  message("❌ FATAL ERROR OCCURRED")
  message(sprintf("Error message: %s", geterrmessage()))
  traceback()
})

# Add warning handler
options(warning.expression = quote({
  message(sprintf("⚠️ WARNING: %s", warnings()))
}))

message("DEBUG: Checking working directory...")
message(sprintf("DEBUG: Current working directory: %s", getwd()))

# Following MP101: Global Environment Access Pattern
# Following MP103: Auto-deinit Behavior
message("DEBUG: Checking for autoinit function...")
if (!exists("autoinit", mode = "function")) {
  message("DEBUG: autoinit not found, sourcing sc_Rprofile.R...")
  profile_path <- file.path("..", "global_scripts", "22_initializations", "sc_Rprofile.R")
  if (file.exists(profile_path)) {
    source(profile_path)
    message("DEBUG: ✅ Sourced sc_Rprofile.R")
  } else {
    message(sprintf("DEBUG: ❌ sc_Rprofile.R not found at: %s", profile_path))
    stop("Cannot find initialization script")
  }
} else {
  message("DEBUG: ✅ autoinit function already exists")
}

# The autoinit() function automatically detects the script location
message("DEBUG: Calling autoinit()...")
tryCatch({
  autoinit()
  message("DEBUG: ✅ autoinit() completed successfully")
}, error = function(e) {
  message(sprintf("DEBUG: ❌ autoinit() failed: %s", e$message))
  stop(e)
})

# Load required libraries with error handling
message("DEBUG: Loading required libraries...")
required_libs <- c("DBI", "duckdb", "dplyr", "lubridate", "stringr")
for (lib in required_libs) {
  message(sprintf("DEBUG: Loading %s...", lib))
  tryCatch({
    library(lib, character.only = TRUE)
    message(sprintf("DEBUG: ✅ %s loaded", lib))
  }, error = function(e) {
    message(sprintf("DEBUG: ❌ Failed to load %s: %s", lib, e$message))
    stop(e)
  })
}

# Source required functions (Following DM_R039)
message("DEBUG: Sourcing database connection functions...")
db_connect_path <- "scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R"
if (file.exists(db_connect_path)) {
  source(db_connect_path)
  message("DEBUG: ✅ Sourced fn_dbConnectDuckdb.R")
} else {
  message(sprintf("DEBUG: ❌ Cannot find: %s", db_connect_path))
  stop("Missing required database functions")
}

# Following MP106: Console Transparency
message("DEBUG: [OK] Global initialization complete")
message(sprintf("DEBUG: Script: %s v%s", script_name, script_version))
message("DEBUG: Following MP064 ETL-Derivation Separation")
message("DEBUG: Following MP104 ETL Data Flow Separation")

# ------------------------------------------------------------------------------
# 1.2: Database Connections (Following DM_R039)
# ------------------------------------------------------------------------------
message("DEBUG: Establishing database connections...")

# Check if db_path_list exists
if (!exists("db_path_list")) {
  message("DEBUG: ❌ db_path_list not found in environment")
  stop("Database path list not initialized by autoinit()")
}

message("DEBUG: Database paths:")
message(sprintf("DEBUG: raw_data: %s", db_path_list$raw_data))
message(sprintf("DEBUG: staged_data: %s", db_path_list$staged_data))

# Check if database files exist
if (!file.exists(db_path_list$raw_data)) {
  message(sprintf("DEBUG: ❌ Raw data database not found: %s", db_path_list$raw_data))
  stop("Raw data database file missing")
}
message("DEBUG: ✅ Raw data database file exists")

# Connect to both raw and staged databases
message("DEBUG: Connecting to raw_data database...")
tryCatch({
  raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
  message("DEBUG: ✅ Connected to raw_data database")
  
  # List tables in raw database
  raw_tables <- dbListTables(raw_data)
  message(sprintf("DEBUG: Tables in raw_data: %s", paste(raw_tables, collapse = ", ")))
  
}, error = function(e) {
  message(sprintf("DEBUG: ❌ Failed to connect to raw_data: %s", e$message))
  stop(e)
})

message("DEBUG: Connecting to staged_data database...")
tryCatch({
  staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)
  message("DEBUG: ✅ Connected to staged_data database")
  
  # List tables in staged database
  staged_tables <- dbListTables(staged_data)
  message(sprintf("DEBUG: Tables in staged_data: %s", 
                  if(length(staged_tables) > 0) paste(staged_tables, collapse = ", ") else "none"))
  
}, error = function(e) {
  message(sprintf("DEBUG: ❌ Failed to connect to staged_data: %s", e$message))
  stop(e)
})

message("DEBUG: ✅ All database connections established")

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

message("DEBUG: Starting MAIN processing...")
main_start_time <- Sys.time()

tryCatch({
  # ------------------------------------------------------------------------------
  # 2.1: Read Raw Data
  # ------------------------------------------------------------------------------
  message("DEBUG: Reading raw BAYORE data...")
  
  # Check if table exists
  raw_table_name <- "df_eby_order_details___raw___MAMBA"
  if (!dbExistsTable(raw_data, raw_table_name)) {
    message(sprintf("DEBUG: ❌ Table %s does not exist in raw_data", raw_table_name))
    message("DEBUG: Available tables in raw_data:")
    for (tbl in dbListTables(raw_data)) {
      message(sprintf("  - %s", tbl))
    }
    stop(sprintf("Required table %s not found", raw_table_name))
  }
  message(sprintf("DEBUG: ✅ Table %s exists", raw_table_name))
  
  # Get table info
  message("DEBUG: Getting table schema...")
  tryCatch({
    table_info <- dbGetQuery(raw_data, sprintf("PRAGMA table_info('%s')", raw_table_name))
    message(sprintf("DEBUG: Table has %d columns", nrow(table_info)))
    for (i in 1:min(5, nrow(table_info))) {
      message(sprintf("  Column %d: %s (%s)", i, table_info$name[i], table_info$type[i]))
    }
    if (nrow(table_info) > 5) {
      message(sprintf("  ... and %d more columns", nrow(table_info) - 5))
    }
  }, error = function(e) {
    message("DEBUG: Could not get table schema info")
  })
  
  # Check row count before reading
  message("DEBUG: Checking row count...")
  row_count_query <- sprintf("SELECT COUNT(*) as n FROM %s", raw_table_name)
  raw_count <- dbGetQuery(raw_data, row_count_query)$n
  message(sprintf("DEBUG: Table has %d rows", raw_count))
  
  if (raw_count == 0) {
    message("DEBUG: ⚠️ Table is empty, nothing to process")
    stop("No data to process")
  }
  
  # Read data with progress reporting
  message(sprintf("DEBUG: Reading %d rows from %s...", raw_count, raw_table_name))
  read_start <- Sys.time()
  
  raw_details <- dbReadTable(raw_data, raw_table_name)
  
  read_elapsed <- difftime(Sys.time(), read_start, units = "secs")
  message(sprintf("DEBUG: ✅ Read completed in %.2f seconds", read_elapsed))
  
  n_raw <- nrow(raw_details)
  message(sprintf("DEBUG: Loaded %d raw order details", n_raw))
  
  # ------------------------------------------------------------------------------
  # 2.2: Column Standardization (Following MP064 - 1ST phase responsibilities)
  # ------------------------------------------------------------------------------
  message("DEBUG: Standardizing column names...")
  
  # Check which columns actually exist in the raw data
  existing_columns <- names(raw_details)
  message(sprintf("DEBUG: Found %d columns in raw data", length(existing_columns)))
  message("DEBUG: Column names:")
  for (col in existing_columns) {
    message(sprintf("  - %s", col))
  }
  
  # Define the full column mapping
  # Column mapping based on official codebook.csv from eBay SQL Server
  column_mapping <- list(
    # Order linking fields
    ORE001 = "order_id",          # 單號 (Order Number) - Links to BAYORD.ORD001
    ORE002 = "line_item_number",  # 流水號 (Serial Number)
    
    # Product identification
    ORE003 = "ebay_item_code",    # EBAY商品代號 (eBay Item Code)
    ORE004 = "product_name",      # 品名 (Product Name)
    ORE005 = "erp_product_no",    # ERP品號 (ERP Product Number)
    ORE006 = "application_data",  # ApplicationData (likely SKU/custom label)
    
    # Product details
    ORE007 = "condition",         # 新舊程度 (Product Condition - New/Used/etc)
    
    # Quantities and pricing
    ORE008 = "quantity",          # 數量 (Quantity)
    ORE009 = "unit_price",        # 單價 (Unit Price)
    
    # Additional fields
    ORE010 = "listing_country",   # 上架國別 (Listing Country - int code)
    ORE011 = "email",             # Email
    ORE012 = "static_alias",      # StaticAlias
    
    # Critical JOIN key
    ORE013 = "batch_key",         # Unnamed field - matches with BAYORD.ORD022
    ORE014 = "reserved_field"     # Unnamed field in codebook
  )
  
  # Filter mapping to only include columns that exist
  columns_to_rename <- column_mapping[names(column_mapping) %in% existing_columns]
  
  if (length(columns_to_rename) == 0) {
    message("DEBUG: ❌ No recognized BAYORE columns found in raw data")
    message("DEBUG: Expected columns starting with ORE, found:")
    ore_cols <- grep("^ORE", existing_columns, value = TRUE)
    if (length(ore_cols) > 0) {
      for (col in ore_cols) {
        message(sprintf("  - %s", col))
      }
    } else {
      message("  None")
    }
    stop("No recognized BAYORE columns found in raw data")
  }
  
  message(sprintf("DEBUG: Will rename %d columns", length(columns_to_rename)))
  message("DEBUG: Rename mapping:")
  for (old_name in names(columns_to_rename)) {
    message(sprintf("  %s -> %s", old_name, columns_to_rename[[old_name]]))
  }
  
  # Check for missing critical columns
  critical_columns <- c("ORE001", "ORE002", "ORE004", "ORE013")
  missing_critical <- setdiff(critical_columns, names(columns_to_rename))
  if (length(missing_critical) > 0) {
    message(sprintf("DEBUG: ⚠️ Missing critical columns: %s", paste(missing_critical, collapse = ", ")))
  }
  
  # Perform the rename with only existing columns
  message("DEBUG: Performing column rename...")
  rename_start <- Sys.time()
  
  # Note: rename() expects new_name = old_name format
  # So we reverse the mapping: setNames(old_names, new_names)
  rename_args <- setNames(names(columns_to_rename), unlist(columns_to_rename))
  
  tryCatch({
    staged_details <- raw_details %>%
      rename(!!!rename_args)
    
    rename_elapsed <- difftime(Sys.time(), rename_start, units = "secs")
    message(sprintf("DEBUG: ✅ Rename completed in %.2f seconds", rename_elapsed))
    
    # Verify rename worked
    new_columns <- names(staged_details)
    message(sprintf("DEBUG: Columns after rename (%d):", length(new_columns)))
    for (i in 1:min(10, length(new_columns))) {
      message(sprintf("  - %s", new_columns[i]))
    }
    if (length(new_columns) > 10) {
      message(sprintf("  ... and %d more columns", length(new_columns) - 10))
    }
    
  }, error = function(e) {
    message(sprintf("DEBUG: ❌ Rename failed: %s", e$message))
    message("DEBUG: Attempting alternative rename method...")
    
    # Try alternative method
    staged_details <- raw_details
    for (old_name in names(columns_to_rename)) {
      new_name <- columns_to_rename[[old_name]]
      if (old_name %in% names(staged_details)) {
        names(staged_details)[names(staged_details) == old_name] <- new_name
        message(sprintf("DEBUG: Renamed %s to %s", old_name, new_name))
      }
    }
  })
  
  # ------------------------------------------------------------------------------
  # 2.3: Data Type Conversions and Cleaning
  # ------------------------------------------------------------------------------
  message("DEBUG: Converting data types and cleaning...")
  
  # Get the column names that exist in staged_details
  staged_columns <- names(staged_details)
  
  # Apply conversions only for columns that exist
  message("DEBUG: Applying data type conversions...")
  
  conversion_start <- Sys.time()
  
  staged_details <- staged_details %>%
    mutate(
      # Numeric conversions based on codebook data types
      across(any_of(c("quantity")), ~as.integer(.)),  # 數量
      across(any_of(c("unit_price")), ~as.numeric(.)), # 單價
      across(any_of(c("listing_country")), ~as.integer(.)), # 上架國別 (int code)
      
      # Add staging metadata
      staged_timestamp = Sys.time(),
      staging_version = script_version
    )
  
  message("DEBUG: ✅ Basic conversions completed")
  
  # Handle encoding for batch_key if it exists
  if ("batch_key" %in% staged_columns) {
    message("DEBUG: Converting batch_key encoding...")
    staged_details <- staged_details %>%
      mutate(batch_key = iconv(batch_key, from = "latin1", to = "UTF-8", sub = ""))
    message("DEBUG: ✅ batch_key encoding converted")
  }
  
  # Clean text fields that exist
  text_fields_to_clean <- intersect(
    c("product_name", "erp_product_no", "application_data", "condition", "email", "static_alias"),
    staged_columns
  )
  
  if (length(text_fields_to_clean) > 0) {
    message(sprintf("DEBUG: Cleaning %d text fields...", length(text_fields_to_clean)))
    staged_details <- staged_details %>%
      mutate(across(all_of(text_fields_to_clean), str_trim))
    message("DEBUG: ✅ Text fields cleaned")
  }
  
  # Standardize condition field if it exists
  if ("condition" %in% staged_columns) {
    message("DEBUG: Standardizing condition field...")
    staged_details <- staged_details %>%
      mutate(condition = str_trim(condition))
  }
  
  # Calculate derived fields only if source columns exist
  if (all(c("quantity", "unit_price") %in% staged_columns)) {
    message("DEBUG: Calculating line_total from quantity * unit_price...")
    staged_details <- staged_details %>%
      mutate(line_total = quantity * unit_price)
    message("DEBUG: ✅ line_total calculated")
  }
  
  conversion_elapsed <- difftime(Sys.time(), conversion_start, units = "secs")
  message(sprintf("DEBUG: ✅ All conversions completed in %.2f seconds", conversion_elapsed))
  message(sprintf("DEBUG: Processed %d columns", ncol(staged_details)))
  
  # ------------------------------------------------------------------------------
  # 2.4: Data Quality Checks
  # ------------------------------------------------------------------------------
  message("DEBUG: Performing data quality checks...")
  
  # Get the column names that exist in staged_details
  staged_columns <- names(staged_details)
  
  # Check for duplicates if key columns exist
  if (all(c("order_id", "line_item_number") %in% staged_columns)) {
    message("DEBUG: Checking for duplicate line items...")
    n_duplicates <- staged_details %>%
      group_by(order_id, line_item_number) %>%
      filter(n() > 1) %>%
      nrow()
    
    if (n_duplicates > 0) {
      message(sprintf("DEBUG: ⚠️ Found %d duplicate line items", n_duplicates))
      # Remove duplicates, keeping the first
      staged_details <- staged_details %>%
        group_by(order_id, line_item_number) %>%
        slice(1) %>%
        ungroup()
      message("DEBUG: Removed duplicates")
    } else {
      message("DEBUG: ✅ No duplicate line items found")
    }
  }
  
  # Check for missing critical fields (only for columns that exist)
  message("DEBUG: Checking for missing values in critical fields...")
  
  if ("order_id" %in% staged_columns) {
    missing_order_ids <- sum(is.na(staged_details$order_id))
    message(sprintf("DEBUG: Missing order_ids: %d", missing_order_ids))
  }
  
  if ("line_item_number" %in% staged_columns) {
    missing_line_items <- sum(is.na(staged_details$line_item_number))
    message(sprintf("DEBUG: Missing line_item_numbers: %d", missing_line_items))
  }
  
  if ("batch_key" %in% staged_columns) {
    missing_batch <- sum(is.na(staged_details$batch_key))
    message(sprintf("DEBUG: Missing batch_keys: %d (critical for JOIN)", missing_batch))
  } else {
    message("DEBUG: ⚠️ batch_key column not present - JOINs with orders may fail")
  }
  
  # Check for data anomalies (only for columns that exist)
  if ("quantity" %in% staged_columns) {
    negative_quantities <- sum(staged_details$quantity < 0, na.rm = TRUE)
    if (negative_quantities > 0) {
      message(sprintf("DEBUG: ⚠️ Found %d negative quantities", negative_quantities))
    } else {
      message("DEBUG: ✅ No negative quantities")
    }
  }
  
  if ("unit_price" %in% staged_columns) {
    negative_prices <- sum(staged_details$unit_price < 0, na.rm = TRUE)
    if (negative_prices > 0) {
      message(sprintf("DEBUG: ⚠️ Found %d negative prices", negative_prices))
    } else {
      message("DEBUG: ✅ No negative prices")
    }
  }
  
  # Report on available vs expected columns
  expected_columns <- c("order_id", "line_item_number", "batch_key", "product_name", 
                        "quantity", "unit_price")
  available_expected <- intersect(expected_columns, staged_columns)
  missing_expected <- setdiff(expected_columns, staged_columns)
  
  message(sprintf("DEBUG: Available expected columns: %d/%d", 
                  length(available_expected), length(expected_columns)))
  if (length(missing_expected) > 0) {
    message(sprintf("DEBUG: Missing expected columns: %s", 
                    paste(missing_expected, collapse = ", ")))
  }
  
  # ------------------------------------------------------------------------------
  # 2.5: Store Staged Data
  # ------------------------------------------------------------------------------
  message("DEBUG: Storing staged BAYORE data...")
  
  # Store in staged_data database with MAMBA-specific naming
  table_name <- "df_eby_order_details___staged___MAMBA"
  
  if (dbExistsTable(staged_data, table_name)) {
    message(sprintf("DEBUG: Dropping existing table: %s", table_name))
    dbRemoveTable(staged_data, table_name)
    message(sprintf("DEBUG: ✅ Dropped existing table: %s", table_name))
  }
  
  message(sprintf("DEBUG: Writing %d rows to %s...", nrow(staged_details), table_name))
  write_start <- Sys.time()
  
  tryCatch({
    dbWriteTable(staged_data, table_name, staged_details)
    write_elapsed <- difftime(Sys.time(), write_start, units = "secs")
    message(sprintf("DEBUG: ✅ Write completed in %.2f seconds", write_elapsed))
  }, error = function(e) {
    message(sprintf("DEBUG: ❌ Failed to write table: %s", e$message))
    stop(e)
  })
  
  n_staged <- nrow(staged_details)
  message(sprintf("DEBUG: ✅ Stored %d staged order details in %s", n_staged, table_name))
  
  # Display sample for verification - but safely!
  message("DEBUG: Preparing sample data for display...")
  sample_data <- head(staged_details, 3)
  
  # Only show columns that actually exist
  display_cols <- intersect(
    c("order_id", "line_item_number", "batch_key", "product_name", "quantity", "unit_price"),
    names(sample_data)
  )
  
  if (length(display_cols) > 0) {
    message("DEBUG: Sample of staged BAYORE data:")
    print(sample_data[, display_cols])
  } else {
    message("DEBUG: Sample data (first 5 columns):")
    print(sample_data[, 1:min(5, ncol(sample_data))])
  }
  
  main_elapsed <- round(difftime(Sys.time(), main_start_time, units = "secs"), 2)
  message(sprintf("DEBUG: ✅ Order details staging completed in %.2f seconds", main_elapsed))
  
}, error = function(e) {
  message(sprintf("DEBUG: ❌ Error during order details staging: %s", e$message))
  message("DEBUG: Traceback:")
  traceback()
  stop(e)
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

message("DEBUG: Starting validation tests...")
test_start_time <- Sys.time()

tryCatch({
  # Test 1: Verify table exists
  message("DEBUG: Test 1 - Checking if staged table exists...")
  if (!dbExistsTable(staged_data, "df_eby_order_details___staged___MAMBA")) {
    stop("TEST: Table df_eby_order_details___staged___MAMBA does not exist")
  }
  message("DEBUG: ✅ Test 1 passed - Table exists")
  
  # Test 2: Verify data staged
  message("DEBUG: Test 2 - Verifying data was staged...")
  row_count <- dbGetQuery(staged_data, "SELECT COUNT(*) as n FROM df_eby_order_details___staged___MAMBA")$n
  if (row_count == 0) {
    stop("TEST: No data in df_eby_order_details___staged___MAMBA")
  }
  message(sprintf("DEBUG: ✅ Test 2 passed - Data staged (%d rows)", row_count))
  
  # Test 3: Verify standardized columns exist
  message("DEBUG: Test 3 - Checking for standardized columns...")
  columns <- dbListFields(staged_data, "df_eby_order_details___staged___MAMBA")
  
  message(sprintf("DEBUG: Found %d columns in staged table:", length(columns)))
  for (col in columns[1:min(10, length(columns))]) {
    message(sprintf("  - %s", col))
  }
  
  # Only check for columns we know should exist based on the mapping
  essential_cols <- c("order_id", "line_item_number")
  missing_essential <- setdiff(essential_cols, columns)
  
  if (length(missing_essential) > 0) {
    message(sprintf("DEBUG: ⚠️ Missing essential columns: %s", paste(missing_essential, collapse = ", ")))
  } else {
    message("DEBUG: ✅ Test 3 passed - Essential columns present")
  }
  
  # Test 4: Verify no raw column names remain
  message("DEBUG: Test 4 - Checking for raw column names...")
  raw_cols <- grep("^ORE", columns, value = TRUE)
  if (length(raw_cols) > 0) {
    message(sprintf("DEBUG: ⚠️ Raw columns still present: %s", paste(raw_cols, collapse = ", ")))
  } else {
    message("DEBUG: ✅ Test 4 passed - All columns standardized (no ORE* columns)")
  }
  
  # Test 5: Verify JOIN keys are ready
  message("DEBUG: Test 5 - Checking JOIN readiness...")
  if (all(c("order_id", "batch_key") %in% columns)) {
    join_key_test <- dbGetQuery(staged_data, 
      "SELECT COUNT(*) as n FROM df_eby_order_details___staged___MAMBA 
       WHERE order_id IS NOT NULL AND batch_key IS NOT NULL")$n
    message(sprintf("DEBUG: Records ready for JOIN: %d", join_key_test))
  } else {
    message("DEBUG: ⚠️ JOIN key columns not all present")
  }
  
  test_elapsed <- round(difftime(Sys.time(), test_start_time, units = "secs"), 2)
  message(sprintf("DEBUG: ✅ All tests completed in %.2f seconds", test_elapsed))
  
}, error = function(e) {
  message(sprintf("DEBUG: ❌ Test failed: %s", e$message))
  stop(e)
})

# ==============================================================================
# PART 4: DEINITIALIZE
# ==============================================================================

message("DEBUG: Starting cleanup...")

# Close database connections
if (exists("raw_data") && !is.null(raw_data)) {
  tryCatch({
    dbDisconnect(raw_data)
    message("DEBUG: ✅ Disconnected from raw_data")
  }, error = function(e) {
    message(sprintf("DEBUG: ⚠️ Could not disconnect from raw_data: %s", e$message))
  })
}

if (exists("staged_data") && !is.null(staged_data)) {
  tryCatch({
    dbDisconnect(staged_data)
    message("DEBUG: ✅ Disconnected from staged_data")
  }, error = function(e) {
    message(sprintf("DEBUG: ⚠️ Could not disconnect from staged_data: %s", e$message))
  })
}

# Final timing
total_elapsed <- round(difftime(Sys.time(), script_start_time, units = "secs"), 2)
message(sprintf("DEBUG: Total execution time: %.2f seconds", total_elapsed))
message(sprintf("DEBUG: Completed at: %s", Sys.time()))

# ==============================================================================
# PART 5: AUTODEINIT
# ==============================================================================
# Following MP103: autodeinit() must be the absolute last statement

message("DEBUG: Executing final cleanup with autodeinit()...")
autodeinit()
#!/usr/bin/env Rscript
# ==============================================================================
# CBZ Sales Time Series ETL - Derived 2TR
# ==============================================================================
# PURPOSE: Build complete sales time series for Poisson analysis (R117).
# PLATFORM: cbz
# PHASE: 2TR (derived ETL)
# CONSUMES: transformed_data.df_cbz_sales___transformed,
#           raw_data.df_all_item_profile_{product_line},
#           data/app_data/parameters/scd_type1/df_product_line.csv
# PRODUCES: app_data.df_cbz_sales_complete_time_series_{product_line},
#           app_data.df_cbz_sales_complete_time_series
# PRINCIPLE: MP064, MP109, R117, DM_R053, MP029
# ==============================================================================

#' @title CBZ Sales Time Series ETL (R117)
#' @description Build complete sales time series with R117 transparency markers as a derived ETL step.
#' @requires DBI, duckdb, dplyr, tidyr, tibble, readr
#' @input_tables transformed_data.df_cbz_sales___transformed, raw_data.df_all_item_profile_{product_line}
#' @output_tables app_data.df_cbz_sales_complete_time_series_{product_line}, app_data.df_cbz_sales_complete_time_series
#' @business_rules Use transformed sales as standard input; prefer payment_time over order_date; map product lines via item profile with app_data.df_product_line as canonical list; fill missing dates with R117 transparency.
#' @platform cbz
#' @author MAMBA Development Team
#' @date 2026-01-02

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

library(DBI)
library(duckdb)
library(dplyr)
library(tidyr)
library(tibble)
library(readr)

source("scripts/global_scripts/04_utils/fn_complete_time_series.R")

error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
start_time <- Sys.time()
product_lines <- character()

if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}

DB_RAW <- db_path_list$raw_data
DB_TRANSFORMED <- db_path_list$transformed_data
DB_APP <- db_path_list$app_data

INPUT_SALES_TABLE <- "df_cbz_sales___transformed"
PROFILE_TABLE_PREFIX <- "df_all_item_profile_"
SCRIPT_VERSION <- "v1.0_ETL_TS"
TIME_UNIT <- "day"
FILL_METHOD <- "zero"

PRODUCT_LINE_PATH <- file.path(
  APP_DIR, "data", "app_data", "parameters", "scd_type1", "df_product_line.csv"
)

empty_time_series_schema <- tibble(
  eby_item_id = character(),
  cbz_item_id = character(),
  time = as.Date(character()),
  year = numeric(),
  day = integer(),
  month_1 = numeric(),
  month_2 = numeric(),
  month_3 = numeric(),
  month_4 = numeric(),
  month_5 = numeric(),
  month_6 = numeric(),
  month_7 = numeric(),
  month_8 = numeric(),
  month_9 = numeric(),
  month_10 = numeric(),
  month_11 = numeric(),
  month_12 = numeric(),
  monday = numeric(),
  tuesday = numeric(),
  wednesday = numeric(),
  thursday = numeric(),
  friday = numeric(),
  saturday = numeric(),
  sunday = numeric(),
  product_line_id.x = character(),
  sales = integer(),
  sales_platform = numeric(),
  product_line_name = character(),
  product_line_id.y = character(),
  data_source = character(),
  filling_method = character(),
  filling_timestamp = as.POSIXct(character()),
  source_table = character(),
  processing_version = character(),
  enrichment_version = character()
)

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  message("====================================================================")
  message("CBZ ETL Time Series Completion (R117)")
  message("====================================================================")
  message(sprintf("Process Date: %s", start_time))
  message(sprintf("Input DB (raw): %s", DB_RAW))
  message(sprintf("Input DB (transformed): %s", DB_TRANSFORMED))
  message(sprintf("Output DB (app_data): %s", DB_APP))
  message("")

  if (!file.exists(DB_RAW)) {
    stop(sprintf("Raw database not found: %s", DB_RAW))
  }
  if (!file.exists(DB_TRANSFORMED)) {
    stop(sprintf("Transformed database not found: %s", DB_TRANSFORMED))
  }
  if (!file.exists(PRODUCT_LINE_PATH)) {
    stop(sprintf("Product line CSV not found: %s", PRODUCT_LINE_PATH))
  }

  message("[Step 1/6] Connecting to databases...")
  con_raw <- dbConnectDuckdb(DB_RAW, read_only = TRUE)
  con_transformed <- dbConnectDuckdb(DB_TRANSFORMED, read_only = TRUE)
  con_app <- dbConnectDuckdb(DB_APP, read_only = FALSE)
  message("  OK Connections established")
  message("")

  if (!dbExistsTable(con_transformed, INPUT_SALES_TABLE)) {
    stop(sprintf("Input sales table not found: %s", INPUT_SALES_TABLE))
  }

  message("[Step 2/6] Loading product line dictionary (app_data priority)...")
  if (dbExistsTable(con_app, "df_product_line")) {
    product_line_lookup <- dbGetQuery(con_app, "
      SELECT product_line_id, product_line_name_chinese,
             product_line_name_english, included
      FROM df_product_line
    ")
    message("  OK Using app_data.df_product_line")
  } else {
    product_line_lookup <- read_csv(PRODUCT_LINE_PATH, show_col_types = FALSE)
    message(sprintf("  OK Using CSV: %s", PRODUCT_LINE_PATH))
  }
  required_fields <- c("product_line_id", "product_line_name_chinese",
                       "product_line_name_english", "included")
  missing_fields <- setdiff(required_fields, names(product_line_lookup))
  if (length(missing_fields) > 0) {
    if (dbExistsTable(con_app, "df_product_line")) {
      message("  WARN app_data.df_product_line missing fields; falling back to CSV")
      product_line_lookup <- read_csv(PRODUCT_LINE_PATH, show_col_types = FALSE)
      message(sprintf("  OK Using CSV: %s", PRODUCT_LINE_PATH))
      missing_fields <- setdiff(required_fields, names(product_line_lookup))
    }
  }
  if (length(missing_fields) > 0) {
    stop(sprintf("Product line data missing fields: %s",
                 paste(missing_fields, collapse = ", ")))
  }

  product_line_lookup <- product_line_lookup %>%
    mutate(
      product_line_id = as.character(product_line_id),
      included = as.logical(included)
    ) %>%
    filter(included == TRUE, product_line_id != "all")

  product_lines <- product_line_lookup$product_line_id
  if (length(product_lines) == 0) {
    stop("No active product lines found in product line CSV")
  }

  message(sprintf("  OK Active product lines: %s", paste(product_lines, collapse = ", ")))
  message("")

  message("[Step 3/6] Building product line mapping from item profiles...")
  profile_tables <- sprintf("%s%s", PROFILE_TABLE_PREFIX, product_lines)
  profile_list <- list()

  for (tbl in profile_tables) {
    if (!dbExistsTable(con_raw, tbl)) {
      message(sprintf("  WARN Missing profile table: %s (skipping)", tbl))
      next
    }

    profile_raw <- tbl2(con_raw, tbl) %>%
      select(any_of(c("sku", "ebay_item_number", "product_line_id"))) %>%
      collect()

    if (!"sku" %in% names(profile_raw)) {
      message(sprintf("  WARN Table %s missing sku column (skipping)", tbl))
      next
    }

    profile_list[[tbl]] <- profile_raw %>%
      mutate(
        product_sku = as.character(sku),
        eby_item_id = if ("ebay_item_number" %in% names(profile_raw)) {
          as.character(ebay_item_number)
        } else {
          NA_character_
        },
        product_line_id = as.character(product_line_id)
      ) %>%
      select(product_sku, eby_item_id, product_line_id) %>%
      filter(!is.na(product_sku) & product_sku != "") %>%
      distinct(product_sku, .keep_all = TRUE)

    message(sprintf("    OK %s: %d rows", tbl, nrow(profile_list[[tbl]])))
  }

  if (length(profile_list) == 0) {
    message("  WARN No item profile tables found; mapping will be empty.")
    product_profile <- tibble(
      product_sku = character(),
      eby_item_id = character(),
      product_line_id = character(),
      product_line_name = character()
    )
  } else {
    product_profile <- bind_rows(profile_list) %>%
      distinct(product_sku, .keep_all = TRUE) %>%
      left_join(product_line_lookup,
                by = "product_line_id") %>%
      mutate(product_line_name = coalesce(product_line_name_chinese,
                                          product_line_name_english)) %>%
      select(product_sku, eby_item_id, product_line_id, product_line_name)
  }

  message(sprintf("  OK Dictionary rows: %d", nrow(product_profile)))
  message("")

  message("[Step 4/6] Loading and mapping sales data...")
  sales_raw <- tbl2(con_transformed, INPUT_SALES_TABLE) %>%
    select(any_of(c("payment_time", "order_date", "order_created_at", "created_at",
                    "sku", "quantity", "line_total", "unit_price"))) %>%
    collect()

  date_candidates <- c("payment_time", "order_date", "order_created_at", "created_at")
  date_col <- date_candidates[date_candidates %in% names(sales_raw)][1]
  if (is.na(date_col)) {
    stop("No date column found (payment_time, order_date, order_created_at, created_at)")
  }
  message(sprintf("  OK Using date column: %s", date_col))

  if (!"sku" %in% names(sales_raw)) {
    stop("Missing required column: sku")
  }
  if (!"quantity" %in% names(sales_raw)) {
    stop("Missing required column: quantity")
  }

  if (!"line_total" %in% names(sales_raw)) {
    if (!"unit_price" %in% names(sales_raw)) {
      stop("Missing line_total and unit_price for sales aggregation")
    }
    sales_raw <- sales_raw %>%
      mutate(line_total = as.numeric(unit_price) * as.numeric(quantity))
  }

  sales_raw <- sales_raw %>%
    mutate(
      order_date = as.Date(.data[[date_col]]),
      cbz_item_id = as.character(sku),
      quantity = as.numeric(quantity),
      line_total = as.numeric(line_total),
      lineproduct_price = as.numeric(line_total)
    ) %>%
    filter(!is.na(order_date), !is.na(cbz_item_id))

  if (nrow(product_profile) == 0) {
    message("  WARN No product profile data available; mapping will be empty.")
    sales_mapped <- sales_raw %>%
      mutate(product_line_id = NA_character_,
             product_line_name = NA_character_,
             eby_item_id = NA_character_)
  } else {
    sales_mapped <- sales_raw %>%
      left_join(product_profile, by = c("cbz_item_id" = "product_sku"))
  }

  mapped_rows <- sum(!is.na(sales_mapped$product_line_id))
  unmapped_rows <- sum(is.na(sales_mapped$product_line_id))
  message(sprintf("  OK Sales rows: %d", nrow(sales_mapped)))
  message(sprintf("  OK Mapped rows: %d", mapped_rows))
  message(sprintf("  WARN Unmapped rows: %d", unmapped_rows))
  message("")

  if (mapped_rows == 0) {
    message("[Step 5/6] No mapped sales rows; writing empty time series tables...")
    dbWriteTable(con_app, "df_cbz_sales_complete_time_series",
                 empty_time_series_schema, overwrite = TRUE)
    for (pl in product_lines) {
      out_tbl <- sprintf("df_cbz_sales_complete_time_series_%s", pl)
      dbWriteTable(con_app, out_tbl, empty_time_series_schema, overwrite = TRUE)
    }
    message("  OK Empty schema tables written (MP029)")
  } else {
    message("[Step 5/6] Aggregating and completing time series...")

    sales_agg <- sales_mapped %>%
      filter(!is.na(product_line_id)) %>%
      group_by(order_date, cbz_item_id, eby_item_id,
               product_line_id, product_line_name) %>%
      summarise(
        sales = sum(quantity, na.rm = TRUE),
        sales_platform = sum(lineproduct_price, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(
        eby_item_id = as.character(eby_item_id),
        cbz_item_id = as.character(cbz_item_id)
      ) %>%
      rename(time = order_date)

    ts_result <- fn_complete_time_series(
      data = sales_agg %>%
        select(time, eby_item_id, cbz_item_id,
               product_line_id, product_line_name,
               sales, sales_platform),
      date_col = "time",
      group_cols = c("cbz_item_id", "eby_item_id", "product_line_id", "product_line_name"),
      value_cols = c("sales", "sales_platform"),
      fill_method = FILL_METHOD,
      mark_filled = TRUE,
      time_unit = TIME_UNIT
    )

    completed_all <- ts_result$data %>%
      mutate(
        sales = as.integer(round(sales)),
        sales_platform = as.numeric(sales_platform),
        product_line_id.x = product_line_id,
        product_line_id.y = product_line_id,
        source_table = INPUT_SALES_TABLE,
        processing_version = SCRIPT_VERSION,
        enrichment_version = SCRIPT_VERSION,
        filling_method = FILL_METHOD,
        filling_timestamp = Sys.time()
      ) %>%
      select(-product_line_id)

    completed_all <- completed_all %>%
      mutate(
        year = as.numeric(format(time, "%Y")),
        day = as.integer(format(time, "%d")),
        month = as.integer(format(time, "%m")),
        weekday = as.integer(format(time, "%u"))
      )

    for (i in 1:12) {
      completed_all[[paste0("month_", i)]] <- as.numeric(completed_all$month == i)
    }

    completed_all$monday <- as.numeric(completed_all$weekday == 1)
    completed_all$tuesday <- as.numeric(completed_all$weekday == 2)
    completed_all$wednesday <- as.numeric(completed_all$weekday == 3)
    completed_all$thursday <- as.numeric(completed_all$weekday == 4)
    completed_all$friday <- as.numeric(completed_all$weekday == 5)
    completed_all$saturday <- as.numeric(completed_all$weekday == 6)
    completed_all$sunday <- as.numeric(completed_all$weekday == 7)

    completed_all <- completed_all %>%
      select(-month, -weekday)

    ordered_cols <- c(
      "eby_item_id", "cbz_item_id", "time", "year", "day",
      paste0("month_", 1:12),
      "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
      "product_line_id.x", "sales", "sales_platform",
      "product_line_name", "product_line_id.y",
      "data_source", "filling_method", "filling_timestamp",
      "source_table", "processing_version", "enrichment_version"
    )

    completed_all <- completed_all %>%
      select(any_of(ordered_cols), everything())

    dbWriteTable(con_app, "df_cbz_sales_complete_time_series",
                 completed_all, overwrite = TRUE)

    for (pl in product_lines) {
      out_tbl <- sprintf("df_cbz_sales_complete_time_series_%s", pl)
      pl_data <- completed_all %>%
        filter(product_line_id.x == pl)
      dbWriteTable(con_app, out_tbl, pl_data, overwrite = TRUE)
      rows_processed <- rows_processed + nrow(pl_data)
    }

    message("  OK Time series tables written")
  }

  message("[Step 6/6] Completed")

}, error = function(e) {
  message("ERROR in MAIN: ", e$message)
  error_occurred <<- TRUE
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

if (!error_occurred) {
  tryCatch({
    message("--------------------------------------------------------------------")
    message("PART 3: TEST - Validating outputs...")
    message("--------------------------------------------------------------------")

    required_cols <- c("time", "sales", "product_line_id.x")

    for (pl in product_lines) {
      out_tbl <- sprintf("df_cbz_sales_complete_time_series_%s", pl)
      if (!dbExistsTable(con_app, out_tbl)) {
        stop(sprintf("Missing output table: %s", out_tbl))
      }
      cols <- dbListFields(con_app, out_tbl)
      missing_cols <- setdiff(required_cols, cols)
      if (length(missing_cols) > 0) {
        stop(sprintf("Missing columns in %s: %s",
                     out_tbl, paste(missing_cols, collapse = ", ")))
      }
    }

    if (!dbExistsTable(con_app, "df_cbz_sales_complete_time_series")) {
      stop("Missing output table: df_cbz_sales_complete_time_series")
    }

    message("  OK Output tables exist with required columns")
    test_passed <- TRUE

  }, error = function(e) {
    message("ERROR in TEST: ", e$message)
    test_passed <<- FALSE
  })
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

end_time <- Sys.time()
execution_time <- difftime(end_time, start_time, units = "secs")

message("")
message("====================================================================")
message("ETL SUMMARY")
message("====================================================================")
message(sprintf("Script:           %s", "cbz_ETL_sales_time_series_2TR.R"))
message(sprintf("Status:           %s", ifelse(test_passed, "SUCCESS", "FAILED")))
message(sprintf("Rows Processed:   %d", rows_processed))
message(sprintf("Execution Time:   %.2f seconds", as.numeric(execution_time)))
message("====================================================================")
message("")

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

message("Cleaning up...")
if (exists("con_raw") && inherits(con_raw, "DBIConnection")) {
  dbDisconnect(con_raw, shutdown = TRUE)
}
if (exists("con_transformed") && inherits(con_transformed, "DBIConnection")) {
  dbDisconnect(con_transformed, shutdown = TRUE)
}
if (exists("con_app") && inherits(con_app, "DBIConnection")) {
  dbDisconnect(con_app, shutdown = TRUE)
}

autodeinit()
# End of file

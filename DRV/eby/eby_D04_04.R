#!/usr/bin/env Rscript
#####
#P06_D04_04
# DERIVATION: EBY Time Series Expansion
# VERSION: 1.0
# PLATFORM: eby
# GROUP: D04
# SEQUENCE: 04
# PURPOSE: Build complete sales time series for Poisson analysis (R117)
# CONSUMES: transformed_data.df_eby_sales___transformed___MAMBA,
#           raw_data.df_all_item_profile_{product_line}
# PRODUCES: app_data.df_eby_sales_complete_time_series_{product_line},
#           app_data.df_eby_sales_complete_time_series
# PRINCIPLE: DM_R044, MP064, R117, MP029, DM_R023
#####

#eby_D04_04

#' @title EBY Time Series Expansion (R117)
#' @description Build complete sales time series for Poisson analysis with R117 transparency markers.
#' @requires DBI, duckdb, dplyr, tidyr, tibble
#' @input_tables transformed_data.df_eby_sales___transformed___MAMBA, raw_data.df_all_item_profile_{product_line}
#' @output_tables app_data.df_eby_sales_complete_time_series_{product_line}, app_data.df_eby_sales_complete_time_series
#' @business_rules Build complete daily time series with zero fill; map sales to product lines; write per-line and all tables.
#' @platform eby
#' @author MAMBA Development Team
#' @date 2025-12-30

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

source("scripts/global_scripts/04_utils/fn_complete_time_series.R")

error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
start_time <- Sys.time()

if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}

PRODUCT_LINES <- c("alf", "irf", "pre", "rek", "tur", "wak")
INPUT_SALES_TABLE <- "df_eby_sales___transformed___MAMBA"
PROFILE_TABLE_PREFIX <- "df_all_item_profile_"
SCRIPT_VERSION <- "v1.0_D04_04"
TIME_UNIT <- "day"
FILL_METHOD <- "zero"

DB_RAW <- db_path_list$raw_data
DB_TRANSFORMED <- db_path_list$transformed_data
DB_APP <- db_path_list$app_data

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
  message("EBY D04_04 - Time Series Expansion (R117)")
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

  message("[Step 1/5] Connecting to databases...")
  con_raw <- dbConnectDuckdb(DB_RAW, read_only = TRUE)
  con_transformed <- dbConnectDuckdb(DB_TRANSFORMED, read_only = TRUE)
  con_app <- dbConnectDuckdb(DB_APP, read_only = FALSE)
  connection_created_raw <- TRUE
  connection_created_transformed <- TRUE
  connection_created_app <- TRUE
  message("  ✓ Connections established")
  message("")

  if (!dbExistsTable(con_transformed, INPUT_SALES_TABLE)) {
    stop(sprintf("Input sales table not found: %s", INPUT_SALES_TABLE))
  }

  message("[Step 2/5] Building product line dictionary...")
  profile_tables <- sprintf("%s%s", PROFILE_TABLE_PREFIX, PRODUCT_LINES)
  profile_list <- list()
  for (tbl in profile_tables) {
    if (!dbExistsTable(con_raw, tbl)) {
      message(sprintf("  ⚠ Missing profile table: %s (skipping)", tbl))
      next
    }
    # FIX: Read ALL columns from profile table, not just 3
    # This ensures product attributes are available for Poisson analysis
    profile_list[[tbl]] <- tbl2(con_raw, tbl) %>%
      collect() %>%
      mutate(
        product_sku = as.character(sku),
        product_line_id = as.character(product_line_id),
        product_line_name = as.character(product_line_name)
      ) %>%
      select(-sku) %>%  # Remove original sku, keep product_sku
      distinct(product_sku, .keep_all = TRUE)
    message(sprintf("    ✓ %s: %d columns, %d rows",
                    tbl, ncol(profile_list[[tbl]]), nrow(profile_list[[tbl]])))
  }

  if (length(profile_list) > 1) {
    all_cols <- unique(unlist(lapply(profile_list, names)))
    for (col in all_cols) {
      col_types <- unique(vapply(profile_list, function(df) {
        if (col %in% names(df)) {
          class(df[[col]])[1]
        } else {
          NA_character_
        }
      }, character(1)))
      col_types <- col_types[!is.na(col_types)]
      if (length(col_types) > 1) {
        message(sprintf(
          "  ⚠ Type mismatch for '%s': %s; coercing to character",
          col,
          paste(col_types, collapse = ", ")
        ))
        profile_list <- lapply(profile_list, function(df) {
          if (col %in% names(df)) {
            df[[col]] <- as.character(df[[col]])
          }
          df
        })
      }
    }
  }

  product_profile <- bind_rows(profile_list) %>%
    filter(!is.na(product_sku) & product_sku != "") %>%
    distinct(product_sku, .keep_all = TRUE)

  message(sprintf("  ✓ Dictionary rows: %d", nrow(product_profile)))
  message("")

  message("[Step 3/5] Loading and mapping sales data...")
  sales_raw <- tbl2(con_transformed, INPUT_SALES_TABLE) %>%
    select(any_of(c("order_date", "product_sku", "erp_product_no",
                    "ebay_item_code", "quantity", "line_total",
                    "unit_price"))) %>%
    collect()

  sku_candidates <- c("product_sku", "erp_product_no", "ebay_item_code")
  for (col in sku_candidates) {
    if (!col %in% names(sales_raw)) {
      sales_raw[[col]] <- NA_character_
    }
  }

  if (!"quantity" %in% names(sales_raw)) {
    stop("Missing required column: quantity")
  }

  if (!"line_total" %in% names(sales_raw)) {
    if (!"unit_price" %in% names(sales_raw)) {
      stop("Missing line_total and unit_price for sales aggregation")
    }
    sales_raw <- sales_raw %>%
      mutate(line_total = as.numeric(quantity) * as.numeric(unit_price))
  }

  sales_raw <- sales_raw %>%
    mutate(
      order_date = as.Date(order_date),
      product_sku = coalesce(
        as.character(product_sku),
        as.character(erp_product_no),
        as.character(ebay_item_code)
      ),
      ebay_item_code = as.character(ebay_item_code),
      quantity = as.numeric(quantity),
      line_total = as.numeric(line_total)
    ) %>%
    filter(!is.na(order_date), !is.na(product_sku))

  if (nrow(product_profile) == 0) {
    message("  ⚠ No product profile data available; mapping will be empty.")
    sales_mapped <- sales_raw %>%
      mutate(product_line_id = NA_character_,
             product_line_name = NA_character_)
  } else {
    sales_mapped <- sales_raw %>%
      left_join(product_profile, by = "product_sku")
  }

  mapped_rows <- sum(!is.na(sales_mapped$product_line_id))
  unmapped_rows <- sum(is.na(sales_mapped$product_line_id))
  message(sprintf("  ✓ Sales rows: %d", nrow(sales_mapped)))
  message(sprintf("  ✓ Mapped rows: %d", mapped_rows))
  message(sprintf("  ⚠ Unmapped rows: %d", unmapped_rows))
  message("")

  if (mapped_rows == 0) {
    message("[Step 4/5] No mapped sales rows; writing empty time series tables...")
    dbWriteTable(con_app, "df_eby_sales_complete_time_series",
                 empty_time_series_schema, overwrite = TRUE)
    for (pl in PRODUCT_LINES) {
      out_tbl <- sprintf("df_eby_sales_complete_time_series_%s", pl)
      dbWriteTable(con_app, out_tbl, empty_time_series_schema, overwrite = TRUE)
    }
    message("  ✓ Empty schema tables written (MP029)")
  } else {
    message("[Step 4/5] Aggregating and completing time series...")

    sales_agg <- sales_mapped %>%
      filter(!is.na(product_line_id)) %>%
      group_by(order_date, product_sku, product_line_id, product_line_name) %>%
      summarise(
        sales = sum(quantity, na.rm = TRUE),
        sales_platform = sum(line_total, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(
        eby_item_id = product_sku,
        cbz_item_id = NA_character_
      ) %>%
      rename(time = order_date)

    ts_result <- fn_complete_time_series(
      data = sales_agg %>%
        select(time, eby_item_id, cbz_item_id,
               product_line_id, product_line_name,
               sales, sales_platform),
      date_col = "time",
      group_cols = c("eby_item_id", "product_line_id", "product_line_name"),
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
        enrichment_version = SCRIPT_VERSION
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

    # FIX: Join product attributes back to time series
    # This enables Poisson analysis to include product_attribute category (UI_P013)
    product_attrs <- product_profile %>%
      select(-product_line_id, -product_line_name)  # Already in time series

    completed_all <- completed_all %>%
      left_join(product_attrs, by = c("eby_item_id" = "product_sku"))

    message(sprintf("  ✓ Joined %d product attribute columns", ncol(product_attrs) - 1))

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

    dbWriteTable(con_app, "df_eby_sales_complete_time_series",
                 completed_all, overwrite = TRUE)

    for (pl in PRODUCT_LINES) {
      out_tbl <- sprintf("df_eby_sales_complete_time_series_%s", pl)
      pl_data <- completed_all %>%
        filter(product_line_id.x == pl)
      dbWriteTable(con_app, out_tbl, pl_data, overwrite = TRUE)
      rows_processed <- rows_processed + nrow(pl_data)
    }

    message("  ✓ Time series tables written")
  }

  message("[Step 5/5] Completed")

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

    for (pl in PRODUCT_LINES) {
      out_tbl <- sprintf("df_eby_sales_complete_time_series_%s", pl)
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

    if (!dbExistsTable(con_app, "df_eby_sales_complete_time_series")) {
      stop("Missing output table: df_eby_sales_complete_time_series")
    }

    message("  ✓ Output tables exist with required columns")
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
message("DERIVATION SUMMARY")
message("====================================================================")
message(sprintf("Script:           %s", "eby_D04_04.R"))
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

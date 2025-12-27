#!/usr/bin/env Rscript
#####
# DERIVATION: Customer DNA Analysis (Master)
# VERSION: 1.0
# PLATFORM: all
# GROUP: D01
# SEQUENCE: 06
# PURPOSE: Orchestrate D01_00 through D01_05 across platforms
# CONSUMES: transformed_data.df_cbz_sales___standardized,
#           transformed_data.df_amz_sales___standardized,
#           transformed_data.df_eby_sales___standardized
# PRODUCES: processed_data.df_customer_aggregated, processed_data.df_customer_rfm,
#           cleansed_data.df_customer_dna___cleansed, cleansed_data.df_customer_profile___cleansed,
#           app_data.df_customer_dna, app_data.df_customer_profile, app_data.df_customer_segments,
#           app_data.v_customer_dna_analytics, app_data.v_customer_segments, app_data.v_segment_statistics
# PRINCIPLE: MP064, DM_R044, DM_R022, DM_R023, DM_R041, DM_R048, DM_R049
#####
#all_D01_06

#' @title D01 Master Execution (All Platforms)
#' @description Execute D01_00 through D01_05 in sequence for each platform.
#'              Follows DM_R044 five-part structure and D01 derivation specs.
#' @input_tables transformed_data.df_cbz_sales___standardized,
#'                transformed_data.df_amz_sales___standardized,
#'                transformed_data.df_eby_sales___standardized
#' @output_tables processed_data.df_customer_aggregated, processed_data.df_customer_rfm,
#'                cleansed_data.df_customer_dna___cleansed, cleansed_data.df_customer_profile___cleansed,
#'                app_data.df_customer_dna, app_data.df_customer_profile, app_data.df_customer_segments
#' @platform all
#' @author MAMBA Development Team
#' @date 2025-12-26

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}

if (!exists("dbConnectDuckdb", mode = "function")) {
  source("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R")
}

library(DBI)
library(dplyr)
library(lubridate)
library(tibble)

DEFAULT_PLATFORMS <- c("cbz", "amz", "eby")
drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
drv_script_name <- "all_D01_06"

drv_metadata_fn_path <- file.path("scripts", "global_scripts", "04_utils", "fn_add_drv_metadata.R")
if (file.exists(drv_metadata_fn_path)) source(drv_metadata_fn_path)
if (!exists("add_drv_metadata", mode = "function")) {
  stop("Missing required function add_drv_metadata (DM_R048)")
}

parse_platforms <- function(args, default_platforms) {
  platforms_arg <- NULL
  for (idx in seq_along(args)) {
    if (args[idx] %in% c("--platforms", "--platform") && idx < length(args)) {
      platforms_arg <- args[idx + 1]
      break
    }
    if (grepl("^--platforms=", args[idx])) {
      platforms_arg <- sub("^--platforms=", "", args[idx])
      break
    }
  }
  if (is.null(platforms_arg)) {
    env_platforms <- Sys.getenv("D01_PLATFORMS", "")
    if (nzchar(env_platforms)) {
      platforms_arg <- env_platforms
    }
  }
  if (is.null(platforms_arg) || platforms_arg == "" || platforms_arg == "all") {
    return(default_platforms)
  }
  platforms <- strsplit(platforms_arg, ",", fixed = TRUE)[[1]]
  platforms <- trimws(platforms)
  platforms <- platforms[nzchar(platforms)]
  if (length(platforms) == 0) {
    return(default_platforms)
  }
  platforms
}

platforms <- parse_platforms(commandArgs(trailingOnly = TRUE), DEFAULT_PLATFORMS)

connection_created_transformed <- FALSE
connection_created_processed <- FALSE
connection_created_cleansed <- FALSE
connection_created_app <- FALSE

if (!exists("transformed_data") || !inherits(transformed_data, "DBIConnection")) {
  transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
  connection_created_transformed <- TRUE
}
if (!exists("processed_data") || !inherits(processed_data, "DBIConnection")) {
  processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = FALSE)
  connection_created_processed <- TRUE
}
if (!exists("cleansed_data") || !inherits(cleansed_data, "DBIConnection")) {
  cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = FALSE)
  connection_created_cleansed <- TRUE
}
if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
  app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
  connection_created_app <- TRUE
}

error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
start_time <- Sys.time()

# Following DM_R028: Platform-specific table naming df_{platform}_{datatype}___{layer}
read_platform_sales <- function(con, platform) {
  table_name <- paste0("df_", platform, "_sales___standardized")
  if (!dbExistsTable(con, table_name)) {
    message("Table not found: ", table_name, " - skipping platform ", platform)
    return(NULL)
  }
  tbl(con, table_name) %>%
    select(any_of(c(
      "transaction_id", "customer_id", "product_id", "product_line_id",
      "platform_code", "platform_id", "payment_time", "lineproduct_price",
      "buyer_name", "buyer_email", "customer_email", "customer_mobile",
      "customer_name", "receiver_name", "ship_postal_code", "email"
    ))) %>%
    collect()
}

validate_sales_schema <- function(df) {
  required_cols <- c(
    "transaction_id",
    "customer_id",
    "product_id",
    "product_line_id",
    "payment_time",
    "lineproduct_price"
  )
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  if (!("platform_code" %in% names(df) || "platform_id" %in% names(df))) {
    stop("Missing platform_code/platform_id in df_sales_standardized")
  }
}

validate_sales_values <- function(df) {
  if (anyNA(df$transaction_id)) stop("transaction_id contains NA")
  if (anyDuplicated(df$transaction_id) > 0) stop("transaction_id is not unique")
  if (anyNA(df$customer_id)) stop("customer_id contains NA after normalization")
  if (anyNA(df$product_id)) stop("product_id contains NA")
  if (anyNA(df$payment_time)) stop("payment_time contains NA after normalization")
  if (anyNA(df$lineproduct_price)) stop("lineproduct_price contains NA after normalization")
  if (any(df$lineproduct_price < 0, na.rm = TRUE)) stop("lineproduct_price contains negative values")
  if (anyNA(df$platform_id) || any(df$platform_id == "")) stop("platform_id contains blank values")
  if (anyNA(df$product_line_id_filter) || any(df$product_line_id_filter == "")) {
    stop("product_line_id_filter contains blank values")
  }
}

normalize_sales <- function(df) {
  if (!"transaction_id" %in% names(df)) stop("Missing transaction_id")
  if (!"customer_id" %in% names(df)) stop("Missing customer_id")
  if (!"product_id" %in% names(df)) stop("Missing product_id")
  if (!"product_line_id" %in% names(df)) stop("Missing product_line_id")
  if (!"payment_time" %in% names(df)) stop("Missing payment_time")
  if (!"lineproduct_price" %in% names(df)) stop("Missing lineproduct_price")

  if ("platform_code" %in% names(df)) {
    df$platform_id <- as.character(df$platform_code)
  } else if ("platform_id" %in% names(df)) {
    df$platform_id <- as.character(df$platform_id)
  } else {
    stop("Missing platform_code/platform_id in df_sales_standardized")
  }

  product_line_raw <- as.character(df$product_line_id)
  missing_product_line <- is.na(product_line_raw) | product_line_raw == ""
  if (all(missing_product_line)) {
    message("product_line_id missing for all rows; defaulting product_line_id_filter to 'all'")
    df$product_line_id_filter <- "all"
  } else if (any(missing_product_line)) {
    stop("product_line_id contains missing values; ETL01 must provide product_line_id")
  } else {
    df$product_line_id_filter <- as.character(product_line_raw)
  }

  df$customer_id <- suppressWarnings(as.integer(df$customer_id))
  df$lineproduct_price <- suppressWarnings(as.numeric(df$lineproduct_price))
  df$payment_time <- as.POSIXct(df$payment_time)

  df
}

col_or_na <- function(df, name) {
  if (name %in% names(df)) {
    df[[name]]
  } else {
    rep(NA_character_, nrow(df))
  }
}

build_customer_aggregated <- function(df_sales) {
  df_sales %>%
    group_by(customer_id, platform_id, product_line_id_filter) %>%
    summarise(
      total_spent = sum(lineproduct_price, na.rm = TRUE),
      transaction_count = n(),
      first_purchase = min(payment_time, na.rm = TRUE),
      last_purchase = max(payment_time, na.rm = TRUE),
      distinct_products = n_distinct(product_id),
      avg_order_value = total_spent / transaction_count,
      .groups = "drop"
    )
}

build_customer_rfm <- function(df_customer_aggregated, reference_date) {
  df_customer_aggregated %>%
    mutate(
      r_value = as.numeric(difftime(reference_date, last_purchase, units = "days")),
      f_value = transaction_count,
      m_value = total_spent / transaction_count,
      ipt = ifelse(
        transaction_count > 1,
        as.numeric(difftime(last_purchase, first_purchase, units = "days")) / (transaction_count - 1),
        NA_real_
      ),
      customer_tenure_days = as.numeric(difftime(reference_date, first_purchase, units = "days")),
      r_ecdf = percent_rank(desc(r_value)),
      f_ecdf = percent_rank(f_value),
      m_ecdf = percent_rank(m_value),
      r_label = case_when(
        r_ecdf >= 0.67 ~ "Recent Buyer",
        r_ecdf >= 0.33 ~ "Medium Inactive",
        TRUE ~ "Long Inactive"
      ),
      f_label = case_when(
        f_ecdf >= 0.67 ~ "High Frequency",
        f_ecdf >= 0.33 ~ "Medium Frequency",
        TRUE ~ "Low Frequency"
      ),
      m_label = case_when(
        m_ecdf >= 0.67 ~ "High Value",
        m_ecdf >= 0.33 ~ "Medium Value",
        TRUE ~ "Low Value"
      )
    )
}

build_customer_dna <- function(df_customer_rfm) {
  dna_scores <- df_customer_rfm %>%
    mutate(
      dna_m_score = percent_rank(m_value),
      dna_f_score = percent_rank(f_value),
      dna_r_score = 1 - percent_rank(r_value),
      cai = (dna_m_score + dna_f_score + dna_r_score) / 3,
      cai_ecdf = percent_rank(cai),
      cai_label = case_when(
        cai_ecdf >= 0.67 ~ "Increasingly Active",
        cai_ecdf >= 0.33 ~ "Stable",
        TRUE ~ "Gradually Inactive"
      ),
      nes_status = case_when(
        transaction_count == 1 & customer_tenure_days <= 30 ~ "N",
        r_value <= 60 ~ "E0",
        r_value <= 180 ~ "S1",
        r_value <= 365 ~ "S2",
        TRUE ~ "S3"
      )
    )

  dna_scores %>%
    mutate(
      m_segment = case_when(
        dna_m_score >= 0.75 ~ "M4",
        dna_m_score >= 0.50 ~ "M3",
        dna_m_score >= 0.25 ~ "M2",
        TRUE ~ "M1"
      ),
      f_segment = case_when(
        dna_f_score >= 0.67 ~ "F3",
        dna_f_score >= 0.33 ~ "F2",
        TRUE ~ "F1"
      ),
      r_segment = case_when(
        dna_r_score >= 0.75 ~ "R4",
        dna_r_score >= 0.50 ~ "R3",
        dna_r_score >= 0.25 ~ "R2",
        TRUE ~ "R1"
      ),
      dna_segment = paste0(m_segment, f_segment, r_segment)
    ) %>%
    select(-m_segment, -f_segment, -r_segment)
}

build_customer_profile <- function(df_sales) {
  buyer_name <- coalesce(
    col_or_na(df_sales, "buyer_name"),
    col_or_na(df_sales, "customer_name"),
    col_or_na(df_sales, "receiver_name"),
    as.character(df_sales$customer_id)
  )

  email <- coalesce(
    col_or_na(df_sales, "email"),
    col_or_na(df_sales, "buyer_email"),
    col_or_na(df_sales, "customer_email"),
    col_or_na(df_sales, "customer_mobile"),
    col_or_na(df_sales, "ship_postal_code"),
    "unknown"
  )

  tibble(
    customer_id = df_sales$customer_id,
    platform_id = df_sales$platform_id,
    buyer_name = buyer_name,
    email = email
  ) %>%
    distinct(customer_id, platform_id, .keep_all = TRUE)
}

build_customer_segments <- function(df_customer_dna) {
  df_customer_dna %>%
    mutate(
      value_tier = case_when(
        dna_m_score >= 0.8 ~ "Premium",
        dna_m_score >= 0.6 ~ "High",
        dna_m_score >= 0.4 ~ "Medium",
        TRUE ~ "Low"
      )
    ) %>%
    select(
      customer_id,
      platform_id,
      product_line_id_filter,
      nes_status,
      dna_segment,
      cai,
      value_tier
    )
}

write_platform_table <- function(con, table_name, data, platform_id) {
  if (!dbExistsTable(con, table_name)) {
    dbWriteTable(con, table_name, data, overwrite = TRUE)
    return(invisible())
  }
  table_id <- dbQuoteIdentifier(con, table_name)
  dbExecute(
    con,
    sprintf("DELETE FROM %s WHERE platform_id = ?", table_id),
    params = list(platform_id)
  )
  dbWriteTable(con, table_name, data, append = TRUE, overwrite = FALSE)
}

get_insertable_columns <- function(con, table_name) {
  info <- tryCatch(
    dbGetQuery(
      con,
      sprintf(
        "SELECT column_name, is_generated, generation_expression
         FROM information_schema.columns
         WHERE table_name = '%s' AND table_schema = 'main'",
        table_name
      )
    ),
    error = function(e) NULL
  )
  if (!is.null(info) && nrow(info) > 0) {
    if ("is_generated" %in% names(info)) {
      info <- info[is.na(info$is_generated) | info$is_generated == "NO", , drop = FALSE]
    } else if ("generation_expression" %in% names(info)) {
      info <- info[is.na(info$generation_expression) | info$generation_expression == "", , drop = FALSE]
    }
    return(unique(info$column_name))
  }

  cols <- unique(dbListFields(con, table_name))
  if (table_name == "df_customer_profile") {
    cols <- setdiff(cols, "display_name")
  }
  cols
}

align_to_table <- function(con, table_name, df) {
  target_cols <- get_insertable_columns(con, table_name)
  missing_cols <- setdiff(target_cols, names(df))
  if (length(missing_cols) > 0) {
    df[missing_cols] <- NA
  }
  extra_cols <- setdiff(names(df), target_cols)
  if (length(extra_cols) > 0) {
    df <- df[, setdiff(names(df), extra_cols), drop = FALSE]
  }
  df[, target_cols, drop = FALSE]
}

create_or_replace_views <- function(con) {
  dbExecute(
    con,
    "CREATE OR REPLACE VIEW v_customer_dna_analytics AS
     SELECT * FROM df_customer_dna"
  )

  dbExecute(
    con,
    "CREATE OR REPLACE VIEW v_customer_segments AS
     SELECT customer_id,
            platform_id,
            product_line_id_filter,
            nes_status,
            dna_segment,
            cai,
            CASE
              WHEN dna_m_score >= 0.8 THEN 'Premium'
              WHEN dna_m_score >= 0.6 THEN 'High'
              WHEN dna_m_score >= 0.4 THEN 'Medium'
              ELSE 'Low'
            END AS value_tier
     FROM df_customer_dna"
  )

  dbExecute(
    con,
    "CREATE OR REPLACE VIEW v_segment_statistics AS
     SELECT platform_id,
            product_line_id_filter,
            nes_status,
            COUNT(*) AS customer_count,
            AVG(m_value) AS avg_m_value,
            AVG(cai) AS avg_cai,
            SUM(total_spent) AS total_value
     FROM df_customer_dna
     GROUP BY platform_id, product_line_id_filter, nes_status"
  )
}

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

tryCatch({
  message("D01_06: Starting master execution")
  message("Platforms: ", paste(platforms, collapse = ", "))

  # Following DM_R028: Check platform-specific tables exist
  platform_tables_exist <- sapply(platforms, function(p) {
    table_name <- paste0("df_", p, "_sales___standardized")
    dbExistsTable(transformed_data, table_name)
  })
  names(platform_tables_exist) <- platforms

  if (!any(platform_tables_exist)) {
    stop("No platform tables found. Expected: df_{platform}_sales___standardized for platforms: ",
         paste(platforms, collapse = ", "))
  }

  # Report which platforms have data
  for (p in platforms) {
    if (platform_tables_exist[p]) {
      message("  Found: df_", p, "_sales___standardized")
    } else {
      message("  Missing: df_", p, "_sales___standardized (will skip)")
    }
  }

  # Read and combine all platform data (Following MP064: Derivation consumes ETL output)
  all_sales <- bind_rows(lapply(platforms, function(p) {
    read_platform_sales(transformed_data, p)
  }))

  if (nrow(all_sales) == 0) {
    stop("All platform tables are empty; run ETL first")
  }

  validate_sales_schema(all_sales)
  all_sales <- normalize_sales(all_sales)
  validate_sales_values(all_sales)
  message("D01_06: Loaded ", nrow(all_sales), " total sales rows from platform tables")

  reference_date <- suppressWarnings(max(all_sales$payment_time, na.rm = TRUE))
  if (is.infinite(reference_date) || is.na(reference_date)) {
    reference_date <- Sys.time()
    message("Using current time as reference_date (payment_time missing)")
  }

  # Ensure app_data tables exist (D00_app_data_init prerequisite)
  required_app_tables <- c("df_customer_profile", "df_customer_dna", "df_customer_segments")
  for (table_name in required_app_tables) {
    if (!dbExistsTable(app_data, table_name)) {
      stop("Missing app_data table: ", table_name, ". Run D00_app_data_init first.")
    }
  }

  for (platform_id in platforms) {
    message("")
    message("D01_00: Consume ETL output for platform ", platform_id)
    platform_sales <- all_sales %>%
      filter(platform_id == !!platform_id)

    if (nrow(platform_sales) == 0) {
      stop("No sales rows found for platform: ", platform_id)
    }

    message("D01_01: Customer aggregation")
    df_customer_aggregated <- build_customer_aggregated(platform_sales)
    write_platform_table(processed_data, "df_customer_aggregated", df_customer_aggregated, platform_id)

    message("D01_02: RFM calculation")
    df_customer_rfm <- build_customer_rfm(df_customer_aggregated, reference_date)
    write_platform_table(processed_data, "df_customer_rfm", df_customer_rfm, platform_id)

    message("D01_03: DNA analysis")
    df_customer_dna <- build_customer_dna(df_customer_rfm)
    write_platform_table(cleansed_data, "df_customer_dna___cleansed", df_customer_dna, platform_id)

    message("D01_04: Customer profile creation")
    df_customer_profile <- build_customer_profile(platform_sales)
    write_platform_table(cleansed_data, "df_customer_profile___cleansed", df_customer_profile, platform_id)

    message("D01_05: App views generation")
    df_customer_segments <- build_customer_segments(df_customer_dna)

    df_customer_profile <- add_drv_metadata(df_customer_profile, drv_script_name, drv_batch_id)
    df_customer_dna <- add_drv_metadata(df_customer_dna, drv_script_name, drv_batch_id)
    df_customer_segments <- add_drv_metadata(df_customer_segments, drv_script_name, drv_batch_id)

    df_customer_profile <- align_to_table(app_data, "df_customer_profile", df_customer_profile)
    df_customer_dna <- align_to_table(app_data, "df_customer_dna", df_customer_dna)
    df_customer_segments <- align_to_table(app_data, "df_customer_segments", df_customer_segments)

    dbBegin(app_data)
    dbExecute(app_data, "DELETE FROM df_customer_profile WHERE platform_id = ?", params = list(platform_id))
    dbExecute(app_data, "DELETE FROM df_customer_dna WHERE platform_id = ?", params = list(platform_id))
    dbExecute(app_data, "DELETE FROM df_customer_segments WHERE platform_id = ?", params = list(platform_id))

    dbAppendTable(app_data, "df_customer_profile", df_customer_profile)
    dbAppendTable(app_data, "df_customer_dna", df_customer_dna)
    dbAppendTable(app_data, "df_customer_segments", df_customer_segments)
    dbCommit(app_data)

    rows_processed <- rows_processed + nrow(df_customer_dna)
    message("D01_05: Wrote ", nrow(df_customer_dna), " DNA rows for platform ", platform_id)
  }

  create_or_replace_views(app_data)
  message("D01_06: Completed master execution")
}, error = function(e) {
  message("ERROR in MAIN: ", e$message)
  error_occurred <- TRUE
  try(dbRollback(app_data), silent = TRUE)
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

if (!error_occurred) {
  tryCatch({
    required_columns <- list(
      df_customer_profile = c("customer_id", "platform_id", "buyer_name", "email"),
      df_customer_dna = c("customer_id", "platform_id", "product_line_id_filter", "nes_status", "dna_segment", "cai"),
      df_customer_segments = c("customer_id", "platform_id", "product_line_id_filter", "nes_status", "dna_segment", "cai", "value_tier")
    )

    for (table_name in names(required_columns)) {
      sample_data <- tbl(app_data, table_name) %>%
        head(5) %>%
        collect()
      missing_cols <- setdiff(required_columns[[table_name]], names(sample_data))
      if (length(missing_cols) > 0) {
        stop("Missing required columns in ", table_name, ": ", paste(missing_cols, collapse = ", "))
      }
    }

    aggregation_summary <- tbl(processed_data, "df_customer_aggregated") %>%
      filter(platform_id %in% !!platforms) %>%
      summarise(
        total_rows = n(),
        negative_spent = sum(total_spent < 0, na.rm = TRUE),
        invalid_transactions = sum(transaction_count <= 0, na.rm = TRUE),
        invalid_dates = sum(first_purchase > last_purchase, na.rm = TRUE)
      ) %>%
      collect()

    if (aggregation_summary$total_rows == 0) {
      stop("Customer aggregation produced 0 rows")
    }
    if (any(
      aggregation_summary$negative_spent > 0,
      aggregation_summary$invalid_transactions > 0,
      aggregation_summary$invalid_dates > 0
    )) {
      stop("Aggregation validation failed: negative values or invalid dates detected")
    }

    duplicate_keys <- tbl(processed_data, "df_customer_aggregated") %>%
      filter(platform_id %in% !!platforms) %>%
      count(customer_id, platform_id, product_line_id_filter) %>%
      summarise(dup_keys = sum(n > 1)) %>%
      collect()

    if (duplicate_keys$dup_keys > 0) {
      stop("Aggregation validation failed: duplicate customer keys detected")
    }

    invalid_rfm <- tbl(processed_data, "df_customer_rfm") %>%
      filter(platform_id %in% !!platforms) %>%
      summarise(
        invalid_r = sum(r_value < 0, na.rm = TRUE),
        invalid_f = sum(f_value < 1, na.rm = TRUE),
        invalid_ipt = sum(ipt < 0, na.rm = TRUE)
      ) %>%
      collect()

    if (any(invalid_rfm$invalid_r > 0, invalid_rfm$invalid_f > 0, invalid_rfm$invalid_ipt > 0)) {
      stop("RFM validation failed: negative values detected")
    }

    test_passed <- TRUE
  }, error = function(e) {
    message("ERROR in TEST: ", e$message)
    test_passed <- FALSE
  })
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================

if (test_passed) {
  message("D01_06: All tests passed")
} else if (!error_occurred) {
  message("D01_06: Tests failed")
}

execution_time <- difftime(Sys.time(), start_time, units = "secs")
message("D01_06: Total execution time (secs): ", round(as.numeric(execution_time), 2))
message("D01_06: Total DNA rows processed: ", rows_processed)

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

if (connection_created_transformed) dbDisconnect(transformed_data, shutdown = FALSE)
if (connection_created_processed) dbDisconnect(processed_data, shutdown = FALSE)
if (connection_created_cleansed) dbDisconnect(cleansed_data, shutdown = FALSE)
if (connection_created_app) dbDisconnect(app_data, shutdown = FALSE)

autodeinit()

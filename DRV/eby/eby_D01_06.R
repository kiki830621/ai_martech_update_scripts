#####
# DERIVATION: D01 Customer DNA Analysis (EBY)
# VERSION: 1.0
# PLATFORM: eby
# GROUP: D01
# SEQUENCE: 06
# PURPOSE: Execute DNA analysis and publish customer DNA outputs to app_data
# CONSUMES: transformed_data.df_eby_sales___transformed___MAMBA
# PRODUCES: app_data.df_customer_profile, app_data.df_customer_dna, app_data.df_customer_segments
# PRINCIPLE: MP064, DM_R022, DM_R023, DM_R039, DM_R041, DM_R044
#####
#P02_D01_06

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================
OPERATION_MODE <- "UPDATE_MODE"
autoinit()

connection_created_transformed <- FALSE
connection_created_app <- FALSE
summary_report <- list(
  success = FALSE,
  platform_id = NA_character_,
  rows_processed = 0,
  execution_time_secs = NA_real_,
  outputs = c("df_customer_profile", "df_customer_dna", "df_customer_segments")
)

transform_fn_path <- file.path(GLOBAL_DIR, "17_transform", "fn_transform_sales_to_sales_by_customer.by_date.R")
transform_by_customer_fn_path <- file.path(GLOBAL_DIR, "17_transform", "fn_transform_sales_by_customer.by_date_to_sales_by_customer.R")

if (file.exists(transform_fn_path)) source(transform_fn_path)
if (file.exists(transform_by_customer_fn_path)) source(transform_by_customer_fn_path)
if (!exists("transform_sales_to_sales_by_customer.by_date")) {
  stop("Missing required function transform_sales_to_sales_by_customer.by_date")
}
if (!exists("transform_sales_by_customer.by_date_to_sales_by_customer")) {
  stop("Missing required function transform_sales_by_customer.by_date_to_sales_by_customer")
}

if (!exists("transformed_data") || !inherits(transformed_data, "DBIConnection")) {
  transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = TRUE)
  connection_created_transformed <- TRUE
}

if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
  app_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
  connection_created_app <- TRUE
}

error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
platform_id <- "eby"  # DM_R022: eby
start_time <- Sys.time()
drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
drv_script_name <- "eby_D01_06"

drv_metadata_fn_path <- file.path(GLOBAL_DIR, "04_utils", "fn_add_drv_metadata.R")
if (file.exists(drv_metadata_fn_path)) source(drv_metadata_fn_path)
if (!exists("add_drv_metadata", mode = "function")) {
  stop("Missing required function add_drv_metadata (DM_R048)")
}

# ==============================================================================
# PART 2: MAIN
# ==============================================================================
tryCatch({
  required_table <- "df_eby_sales___transformed___MAMBA"
  if (!dbExistsTable(transformed_data, required_table)) {
    stop(sprintf("Required table %s not found in transformed_data", required_table))
  }
  row_check <- tbl2(transformed_data, required_table) %>%
    summarise(n = dplyr::n()) %>%
    collect() %>%
    dplyr::pull(n)
  if (is.na(row_check) || row_check == 0) {
    stop(sprintf("Required table %s exists but has 0 rows; run EBY sales ETL 0IM/1ST/2TR first", required_table))
  }

  message("MAIN: Loading sales data with tbl2()...")
  sales_data <- tbl2(transformed_data, required_table) %>%
    select(
      any_of(c("customer_id",
               "order_created_at",
               "order_date",
               "total_price_after_discounts",
               "line_total",
               "buyer_email",
               "customer_email",
               "customer_mobile",
               "customer_name",
               "receiver_name"))
    ) %>%
    collect() %>%
    mutate(
      payment_time = dplyr::coalesce(suppressWarnings(lubridate::ymd_hms(order_created_at)), as.POSIXct(order_date)),
      total_spent = dplyr::coalesce(total_price_after_discounts, line_total),
      platform_id = platform_id,
      buyer_name = dplyr::coalesce(customer_name, receiver_name, as.character(customer_id)),
      email = dplyr::coalesce(buyer_email, customer_email, customer_mobile),
      customer_id = suppressWarnings(as.numeric(customer_id))
    ) %>%
    filter(!is.na(customer_id), !is.na(payment_time), !is.na(total_spent))

  if (nrow(sales_data) == 0) {
    stop("No usable sales records after preprocessing")
  }

  message(sprintf("MAIN: %d sales rows loaded", nrow(sales_data)))

  message("MAIN: Aggregating sales by customer/date...")
  sales_by_customer_by_date <- transform_sales_to_sales_by_customer.by_date(
    df = data.table::as.data.table(sales_data[, c("customer_id", "payment_time", "total_spent", "platform_id")]),
    first_cols = c("platform_id"),
    time = "payment_time",
    verbose = TRUE
  )

  message("MAIN: Aggregating to customer-level metrics...")
  sales_by_customer <- transform_sales_by_customer.by_date_to_sales_by_customer(
    df = sales_by_customer_by_date,
    first_cols = c("min_time_by_date", "ni", "ipt", "platform_id"),
    verbose = TRUE
  )

  message("MAIN: Running analysis_dna()...")
  sales_by_customer_df <- as.data.frame(sales_by_customer)
  sales_by_customer_by_date_df <- as.data.frame(sales_by_customer_by_date)
  dna_results <- analysis_dna(
    df_sales_by_customer = sales_by_customer_df,
    df_sales_by_customer_by_date = sales_by_customer_by_date_df,
    skip_within_subject = TRUE,
    verbose = TRUE
  )

  customer_dna_raw <- dna_results$data_by_customer
  if (!"cai" %in% names(customer_dna_raw)) {
    customer_dna_raw$cai <- NA_real_
  }
  if (!"cai_value" %in% names(customer_dna_raw)) {
    customer_dna_raw$cai_value <- customer_dna_raw$cai
  }

  customer_dna <- customer_dna_raw %>%
    mutate(
      customer_id = as.integer(customer_id),
      cai = dplyr::coalesce(cai, cai_value),
      dna_m_score = m_ecdf,
      dna_f_score = f_ecdf,
      dna_r_score = 1 - r_ecdf,
      dna_segment = paste0(
        cut(dna_m_score, breaks = c(0, 0.25, 0.5, 0.75, 1), labels = c("M1", "M2", "M3", "M4"), include.lowest = TRUE),
        cut(dna_f_score, breaks = c(0, 0.33, 0.67, 1), labels = c("F1", "F2", "F3"), include.lowest = TRUE),
        cut(dna_r_score, breaks = c(0, 0.25, 0.5, 0.75, 1), labels = c("R1", "R2", "R3", "R4"), include.lowest = TRUE)
      ),
      platform_id = platform_id,
      product_line_id_filter = "all"
    )

  customer_dna <- customer_dna %>%
    mutate(
      nrec = as.character(nrec),
      difftime = as.numeric(difftime)
    ) %>%
    mutate(across(where(is.ordered), as.character)) %>%
    mutate(across(where(is.factor), as.character)) %>%
    mutate(across(where(is.logical), as.numeric))

  required_dna_cols <- c("customer_id", "nes_status", "dna_segment", "cai")
  missing_dna_cols <- setdiff(required_dna_cols, names(customer_dna))
  if (length(missing_dna_cols) > 0) {
    stop(sprintf("analysis_dna output missing required columns: %s", paste(missing_dna_cols, collapse = ", ")))
  }

  customer_profile <- sales_data %>%
    transmute(
      customer_id = as.integer(customer_id),
      platform_id = platform_id,
      buyer_name = buyer_name,
      email = email
    ) %>%
    distinct(customer_id, platform_id, .keep_all = TRUE)

  has_dna_m_score <- "dna_m_score" %in% names(customer_dna)
  customer_segments <- customer_dna %>%
    mutate(
      value_tier = case_when(
        has_dna_m_score & dna_m_score >= 0.8 ~ "Premium",
        has_dna_m_score & dna_m_score >= 0.6 ~ "High",
        has_dna_m_score & dna_m_score >= 0.4 ~ "Medium",
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

  message("MAIN: Writing outputs to app_data...")
  customer_profile <- add_drv_metadata(customer_profile, drv_script_name, drv_batch_id)
  customer_dna <- add_drv_metadata(customer_dna, drv_script_name, drv_batch_id)
  customer_segments <- add_drv_metadata(customer_segments, drv_script_name, drv_batch_id)

  required_tables <- c("df_customer_profile", "df_customer_dna", "df_customer_segments")
  for (tbl in required_tables) {
    if (!DBI::dbExistsTable(app_data, tbl)) {
      stop(sprintf("Required output table %s missing in app_data; run D00_app_data_init first", tbl))
    }
  }

  append_with_checks <- function(con, table_name, df) {
    tryCatch(
      DBI::dbAppendTable(con, table_name, df),
      error = function(e) stop(sprintf("Append failed for %s: %s", table_name, e$message))
    )
  }

  DBI::dbWithTransaction(app_data, {
    DBI::dbExecute(app_data, "DELETE FROM df_customer_profile WHERE platform_id = ?", params = list(platform_id))
    DBI::dbExecute(app_data, "DELETE FROM df_customer_dna WHERE platform_id = ?", params = list(platform_id))
    DBI::dbExecute(app_data, "DELETE FROM df_customer_segments WHERE platform_id = ?", params = list(platform_id))

    append_with_checks(app_data, "df_customer_profile", customer_profile)
    append_with_checks(app_data, "df_customer_dna", customer_dna)
    append_with_checks(app_data, "df_customer_segments", customer_segments)
  })

  rows_processed <- nrow(customer_dna)
  message(sprintf("MAIN: Wrote %d customer DNA rows", rows_processed))

}, error = function(e) {
  error_occurred <<- TRUE
  message(sprintf("MAIN: ERROR - %s", e$message))
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================
if (!error_occurred) {
  tryCatch({
    outputs_exist <- all(c(
      dbExistsTable(app_data, "df_customer_profile"),
      dbExistsTable(app_data, "df_customer_dna"),
      dbExistsTable(app_data, "df_customer_segments")
    ))
    if (!outputs_exist) stop("One or more output tables were not created in app_data")

    test_passed <- TRUE
    message("TEST: Output tables verified")
  }, error = function(e) {
    test_passed <- FALSE
    message(sprintf("TEST: ERROR - %s", e$message))
  })
}

# ==============================================================================
# PART 4: SUMMARIZE
# ==============================================================================
execution_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
summary_report <- list(
  success = !error_occurred && test_passed,
  platform_id = platform_id,
  rows_processed = rows_processed,
  execution_time_secs = execution_time,
  outputs = c("df_customer_profile", "df_customer_dna", "df_customer_segments")
)
message(sprintf("SUMMARY: success=%s, rows=%d, time=%.2fs",
                summary_report$success, summary_report$rows_processed, summary_report$execution_time_secs))

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================
if (connection_created_transformed) {
  try(dbDisconnect(transformed_data, shutdown = TRUE), silent = TRUE)
}
if (connection_created_app) {
  try(dbDisconnect(app_data, shutdown = TRUE), silent = TRUE)
}
.InitEnv$result_summary <- summary_report
autodeinit()

invisible(.InitEnv$result_summary)

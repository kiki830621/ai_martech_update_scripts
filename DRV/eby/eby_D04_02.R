#!/usr/bin/env Rscript
#####
# DERIVATION: EBY Poisson Analysis
# VERSION: 2.0
# PLATFORM: eby
# GROUP: D04
# SEQUENCE: 02
# PURPOSE: Pre-compute Poisson regression results using ALL historical data
# CONSUMES: df_eby_sales_complete_time_series_{product_line}
# PRODUCES: df_eby_poisson_analysis_{product_line}, df_eby_poisson_analysis_all
# PRINCIPLE: DM_R044, MP064, MP135, DM_R046, DM_R047, R118, R119, R120, DM_R043
#####
#eby_D04_02

#' @title EBY Poisson Analysis - Type B Steady-State Analytics
#' @description Pre-compute Poisson regression results using ALL historical data.
#'              Implements MP135 v2.0 (Analytics Temporal Classification - Type B).
#'              Uses all_time data ONLY (no period loops), analyzes coefficients.
#'              DM_R047: Schema synchronized with CBZ version.
#' @input_tables df_eby_sales_complete_time_series_{product_line} (app_data.duckdb)
#' @output_tables df_eby_poisson_analysis_{product_line}, df_eby_poisson_analysis_all
#' @author MAMBA Development Team
#' @date 2025-12-14

# ==============================================================================
# PART 1: INITIALIZE
# ==============================================================================

# 1.0: Autoinit - Environment setup
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

# 1.1: Load required packages
library(DBI)
library(duckdb)
library(dplyr)
library(tidyr)
library(broom)

# 1.2: Load utility functions (DM_R046)
source("scripts/global_scripts/04_utils/fn_enrich_with_display_names.R")

# 1.3: Initialize tracking variables
error_occurred <- FALSE
test_passed <- FALSE
rows_processed <- 0
start_time <- Sys.time()

# Validate configuration
if (!exists("db_path_list", inherits = TRUE)) {
  stop("db_path_list not initialized. Run autoinit() before configuration.")
}

# Print header
cat("\n")
cat("════════════════════════════════════════════════════════════════════\n")
cat("EBY Product-Line Poisson DRV - Type B Steady-State (MP135 v2.0)\n")
cat("════════════════════════════════════════════════════════════════════\n")
cat("Process Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("Input Database:", db_path_list$app_data, "\n")
cat("Output Database:", db_path_list$app_data, "\n")
cat("Script Version: v1.0 (2025-11-14) - Type B Implementation + DM_R047\n")
cat("\n")
cat("Analytics Classification: TYPE B - Steady-State Analytics\n")
cat("  • Uses ALL historical data (no period filtering)\n")
cat("  • Analyzes coefficients (not trends over time)\n")
cat("  • Requires complete data for reliable estimates\n")
cat("\n")
cat("Principle Compliance:\n")
cat("  ✓ MP135 v2.0: Analytics Temporal Classification (Type B)\n")
cat("  ✓ UI_R024: Metadata Display for Steady-State Analytics\n")
cat("  ✓ DM_R046: Variable Display Name Metadata Rule\n")
cat("  ✓ DM_R047: Multi-Platform Synchronization (sync with CBZ)\n")
cat("  ✓ R120: Variable Range Metadata Requirement\n")
cat("  ✓ R118: Statistical Significance Documentation\n")
cat("  ✓ R119: Universal df_ Prefix\n")
cat("  ✓ MP029: No Fake Data\n")
cat("  ✓ MP102: Complete Metadata\n")
cat("  ✓ R113: Four-Part Script Structure\n")
cat("\n")

# Connect to database (single connection - input and output in same database)
cat("Connecting to database...\n")
con_app <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
cat("  ✓ Connected to app_data.duckdb (read-write)\n")
cat("\n")

# Product lines to process
PRODUCT_LINES <- c("alf", "irf", "pre", "rek", "tur", "wak")
cat("Product lines to process:", paste(toupper(PRODUCT_LINES), collapse=", "), "\n")
cat("\n")

# DRV version for metadata (increment when logic changes)
DRV_VERSION <- "v1.0_TypeB_DM_R047"

# Empty schema for skipped product lines (MP029: no fake data)
# Updated for DM_R043 v2.0: includes data_type and source_variable
empty_output_table <- tibble(
  product_line_id = character(),
  platform = character(),
  predictor = character(),
  predictor_type = character(),
  data_type = character(),           # NEW: DM_R043 v2.0
  source_variable = character(),     # NEW: DM_R043 v2.0
  coefficient = numeric(),
  incidence_rate_ratio = numeric(),
  std_error = numeric(),
  z_value = numeric(),
  p_value = numeric(),
  conf_low = numeric(),
  conf_high = numeric(),
  irr_conf_low = numeric(),
  irr_conf_high = numeric(),
  predictor_min = numeric(),         # R120
  predictor_max = numeric(),         # R120
  predictor_range = numeric(),       # R120
  predictor_is_binary = logical(),   # R120
  track_multiplier = numeric(),      # R120
  deviance = numeric(),
  aic = numeric(),
  sample_size = integer(),
  convergence = character(),
  analysis_date = as.Date(character()),
  analysis_version = character(),
  computed_at = as.POSIXct(character()),
  data_version = as.Date(character()),
  display_name = character(),
  display_name_en = character(),
  display_name_zh = character(),
  display_category = character(),
  display_description = character()
)

# =============================================================================
# CLASSIFICATION FUNCTIONS (DM_R043 v2.0)
# =============================================================================

#' Classify predictor_type for UI module routing
#' @param term Character, predictor variable name
#' @return Character, one of: time_feature, product_attribute, comment_attribute, structural
classify_predictor_type <- function(term) {
  term_lower <- tolower(term)

  # Time features: temporal patterns (poissonTimeAnalysis)
  is_time <- grepl("^month_[0-9]+$", term_lower) ||
    grepl("^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$", term_lower) ||
    grepl("^(year|day|week|quarter|is_holiday|is_weekend)", term_lower)

  if (is_time) {
    return("time_feature")
  }

  # Structural: identifiers and names (EXCLUDED from UI)
  is_structural <- grepl("_name$|_id$|_code$|^sku$|^asin$|_series_name", term_lower)
  if (is_structural) {
    return("structural")
  }

  # Comment attributes: review and sentiment related (poissonCommentAnalysis)
  is_comment <- grepl("rating|sentiment|review|comment|stars|feedback", term_lower)
  if (is_comment) {
    return("comment_attribute")
  }

  # Default: product attributes (poissonFeatureAnalysis)
  return("product_attribute")
}

#' Infer source_variable for dummy-coded columns
#' @param term Character, predictor variable name
#' @return Character or NA, the source categorical variable name
infer_source_variable <- function(term) {
  # Known categorical prefixes that generate dummy variables
  known_prefixes <- c("brand", "color", "material", "category", "design",
                      "style", "size", "type", "model", "manufacturer")
  pattern <- paste0("^(", paste(known_prefixes, collapse = "|"), ")_")

  if (grepl(pattern, term, ignore.case = TRUE)) {
    # Extract prefix before first underscore
    return(sub("_.*$", "", term))
  }
  return(NA_character_)
}

#' Classify data_type based on value range and source_variable
#' @param is_binary Logical, TRUE if predictor_min=0 and predictor_max=1
#' @param source_var Character or NA, the source variable if dummy-coded
#' @return Character, one of: binary, numerical, dummy
classify_data_type <- function(is_binary, source_var) {
  if (!is.na(source_var)) {
    return("dummy")
  }
  if (isTRUE(is_binary)) {
    return("binary")
  }
  return("numerical")
}

# Store all results for merging
all_results <- list()

# ==============================================================================
# PART 2: MAIN
# ==============================================================================

cat("════════════════════════════════════════════════════════════════════\n")
cat("[Phase 1/3] Running Poisson Regression by Product Line (All-Time Data)\n")
cat("════════════════════════════════════════════════════════════════════\n\n")

tryCatch({

# Process each product line (NO period loop - Type B uses all_time only)
for (pl in PRODUCT_LINES) {

  cat(sprintf("\n╔═══════════════════════════════════════════════════════════════════╗\n"))
  cat(sprintf("║ Product Line: %-53s ║\n", toupper(pl)))
  cat(sprintf("╚═══════════════════════════════════════════════════════════════════╝\n\n"))

  # Read time series data (all historical data)
  table_name <- sprintf("df_eby_sales_complete_time_series_%s", pl)
  output_table_name <- sprintf("df_eby_poisson_analysis_%s", pl)
  skip_reason <- NULL

  if (!dbExistsTable(con_app, table_name)) {
    skip_reason <- sprintf("Table not found: %s", table_name)
  }

  if (!is.null(skip_reason)) {
    cat(sprintf("  ⚠️  %s\n", skip_reason))
    dbWriteTable(con_app, output_table_name, empty_output_table, overwrite = TRUE)
    cat(sprintf("  → Wrote empty schema to: %s\n\n", output_table_name))
    next
  }

  ts_data <- tbl2(con_app, table_name) %>% collect()
  cat(sprintf("  ✓ Loaded full historical dataset: %s rows, %s columns\n",
              format(nrow(ts_data), big.mark=","), ncol(ts_data)))

  if (nrow(ts_data) == 0) {
    skip_reason <- "Input time series table is empty"
  }

  if (!is.null(skip_reason)) {
    cat(sprintf("  ⚠️  %s\n", skip_reason))
    dbWriteTable(con_app, output_table_name, empty_output_table, overwrite = TRUE)
    cat(sprintf("  → Wrote empty schema to: %s\n\n", output_table_name))
    next
  }

  # TYPE B: Get latest data date from INPUT data (MP135 v2.0)
  # This is NOT the current date, but the most recent order_date in the data
  if ("order_date" %in% names(ts_data)) {
    date_col <- "order_date"
  } else if ("time" %in% names(ts_data)) {
    date_col <- "time"
  } else {
    # Try alternative date column names
    date_col <- names(ts_data)[grepl("date|time", names(ts_data), ignore.case = TRUE)][1]
    if (is.na(date_col)) {
      skip_reason <- "No date column found in data"
    }
  }

  if (!is.null(skip_reason)) {
    cat(sprintf("  ❌ %s\n", skip_reason))
    dbWriteTable(con_app, output_table_name, empty_output_table, overwrite = TRUE)
    cat(sprintf("  → Wrote empty schema to: %s\n\n", output_table_name))
    next
  }

  latest_data_date <- max(as.Date(ts_data[[date_col]]), na.rm = TRUE)
  earliest_data_date <- min(as.Date(ts_data[[date_col]]), na.rm = TRUE)
  days_span <- as.numeric(difftime(latest_data_date, earliest_data_date, units = "days"))

  cat(sprintf("  ✓ Data date range: %s to %s (%.0f days)\n",
              earliest_data_date, latest_data_date, days_span))
  cat(sprintf("  ✓ Data version (latest date): %s\n\n", latest_data_date))

  # Identify predictor columns (exclude IDs, time, sales, and constant metadata)
  # Keep: year, day, month_*, weekday dummies, product features
  exclude_cols <- c(
    "eby_item_id", "cbz_item_id", "time", "sales", "sales_platform",
    "product_line_id", "product_line_id.x", "product_line_id.y", "product_line_name",
    "data_source", "filling_method", "filling_timestamp",
    "source_table", "processing_version", "enrichment_version",
    date_col  # Also exclude the date column from predictors
  )
  predictor_cols <- setdiff(names(ts_data), exclude_cols)

  cat(sprintf("  ✓ Identified: %d predictor columns (after excluding metadata)\n\n", length(predictor_cols)))

  # Prepare data for Poisson regression
  # Use sales column directly (no aggregation needed - data is already at transaction level)
  # Each row represents a transaction, sales column indicates transaction count

  # Ensure sales column exists and is numeric
  if (!"sales" %in% names(ts_data)) {
    skip_reason <- "Sales column not found in data"
  }

  if (!is.null(skip_reason)) {
    cat(sprintf("  ❌ %s\n", skip_reason))
    dbWriteTable(con_app, output_table_name, empty_output_table, overwrite = TRUE)
    cat(sprintf("  → Wrote empty schema to: %s\n\n", output_table_name))
    next
  }

  # Create modeling dataset
  model_data <- ts_data %>% select(sales, all_of(predictor_cols))

  cat(sprintf("  → Prepared: %s observations for modeling\n",
              format(nrow(model_data), big.mark=",")))

  # Remove rows with NA in sales only (predictors can have NA - will be handled by GLM)
  initial_rows <- nrow(model_data)
  model_data <- model_data %>% filter(!is.na(sales))

  if (nrow(model_data) < initial_rows) {
    cat(sprintf("  → Removed %d rows with missing sales values\n", initial_rows - nrow(model_data)))
  }

  if (nrow(model_data) == 0) {
    skip_reason <- "No valid observations after removing missing values"
  }

  if (!is.null(skip_reason)) {
    cat(sprintf("  ❌ %s\n", skip_reason))
    dbWriteTable(con_app, output_table_name, empty_output_table, overwrite = TRUE)
    cat(sprintf("  → Wrote empty schema to: %s\n\n", output_table_name))
    next
  }

  # Clean NaN and Inf values in numeric columns (replace with NA)
  numeric_cols <- names(model_data)[sapply(model_data, is.numeric)]
  for (col in numeric_cols) {
    if (any(is.nan(model_data[[col]]) | is.infinite(model_data[[col]]))) {
      model_data[[col]][is.nan(model_data[[col]]) | is.infinite(model_data[[col]])] <- NA
    }
  }

  # Convert character columns to factors
  char_cols <- names(model_data)[sapply(model_data, is.character)]
  if (length(char_cols) > 0) {
    cat(sprintf("  → Converting %d character columns to factors...\n", length(char_cols)))
    for (col in char_cols) {
      model_data[[col]] <- as.factor(model_data[[col]])
    }
  }

  # Remove predictors with only one level (constant columns)
  # These cannot be used in regression and cause the "contrasts" error
  single_level_predictors <- c()
  for (col in predictor_cols) {
    # Check for all-NA columns (after NaN->NA conversion)
    if (all(is.na(model_data[[col]]))) {
      single_level_predictors <- c(single_level_predictors, col)
    } else if (is.factor(model_data[[col]])) {
      if (nlevels(model_data[[col]]) < 2) {
        single_level_predictors <- c(single_level_predictors, col)
      }
    } else if (is.numeric(model_data[[col]])) {
      # For numeric, check unique non-NA values
      unique_vals <- unique(model_data[[col]][!is.na(model_data[[col]])])
      if (length(unique_vals) < 2) {
        single_level_predictors <- c(single_level_predictors, col)
      }
    }
  }

  if (length(single_level_predictors) > 0) {
    cat(sprintf("  → Removing %d constant/all-NA predictors\n", length(single_level_predictors)))
    predictor_cols <- setdiff(predictor_cols, single_level_predictors)
  }

  cat(sprintf("  → Final predictor count: %d (after removing constants)\n\n", length(predictor_cols)))

  # Prepare final modeling dataset with only kept predictors
  model_data_final <- model_data %>% select(sales, all_of(predictor_cols))

  # Remove rows with ANY NA in predictors (complete case analysis)
  initial_rows <- nrow(model_data_final)
  model_data_final <- model_data_final %>% tidyr::drop_na()

  if (nrow(model_data_final) < initial_rows) {
    cat(sprintf("  → Removed %d rows with missing predictor values (complete case analysis)\n",
                initial_rows - nrow(model_data_final)))
  }

  if (nrow(model_data_final) == 0) {
    skip_reason <- "No complete cases available after removing missing values"
  }

  if (!is.null(skip_reason)) {
    cat(sprintf("  ❌ %s\n", skip_reason))
    dbWriteTable(con_app, output_table_name, empty_output_table, overwrite = TRUE)
    cat(sprintf("  → Wrote empty schema to: %s\n\n", output_table_name))
    next
  }

  cat(sprintf("  → Final modeling dataset: %d rows\n", nrow(model_data_final)))

  # Check for extreme sparsity (Poisson regression issue)
  zero_pct <- 100 * sum(model_data_final$sales == 0) / nrow(model_data_final)
  non_zero_count <- sum(model_data_final$sales > 0)

  cat(sprintf("  → Sales distribution: %.1f%% zeros, %d non-zero observations\n",
              zero_pct, non_zero_count))

  if (zero_pct > 99 || non_zero_count < 20) {
    cat(sprintf("  ⚠️  SKIPPING %s: Extreme sparsity (%.1f%% zeros, %d non-zero)\n",
                toupper(pl), zero_pct, non_zero_count))
    cat("     Poisson regression requires sufficient non-zero observations for convergence\n")
    cat("     This product line needs special handling (penalized regression or zero-inflation)\n\n")
    skip_reason <- "Extreme sparsity - insufficient non-zero observations"
  }

  if (!is.null(skip_reason)) {
    dbWriteTable(con_app, output_table_name, empty_output_table, overwrite = TRUE)
    cat(sprintf("  → Wrote empty schema to: %s\n\n", output_table_name))
    next
  }

  # Run Poisson regression
  # Outcome: sales (transaction count), Predictors: all time and product features
  formula_str <- paste("sales ~", paste(predictor_cols, collapse = " + "))

  cat("  → Running Poisson regression (this may take a few minutes)...\n")

  tryCatch({

    # Fit Poisson model
    poisson_model <- glm(as.formula(formula_str),
                         data = model_data_final,
                         family = poisson())

    # Extract results using broom (R118 compliance)
    coef_summary <- tidy(poisson_model, conf.int = TRUE, conf.level = 0.95)

    # Remove intercept (not interpretable in this context)
    coef_summary <- coef_summary %>% filter(term != "(Intercept)")

    # Calculate Incidence Rate Ratio (IRR = exp(coefficient))
    # IRR interpretation: multiplicative effect on expected count
    coef_summary <- coef_summary %>%
      mutate(
        incidence_rate_ratio = exp(estimate),
        irr_conf_low = exp(conf.low),
        irr_conf_high = exp(conf.high)
      )

    # Get model statistics
    model_stats <- glance(poisson_model)

    # TYPE B METADATA: Only 2 columns needed (MP135 v2.0)
    computed_timestamp <- Sys.time()

    # Calculate predictor range metadata (R120)
    predictor_terms <- coef_summary$term
    predictor_min_vals <- numeric(length(predictor_terms))
    predictor_max_vals <- numeric(length(predictor_terms))

    for (i in seq_along(predictor_terms)) {
      term <- predictor_terms[i]
      if (term %in% names(model_data_final)) {
        vals <- model_data_final[[term]]
        predictor_min_vals[i] <- min(vals, na.rm = TRUE)
        predictor_max_vals[i] <- max(vals, na.rm = TRUE)
      } else {
        # For factor levels, assume 0/1 range
        predictor_min_vals[i] <- 0
        predictor_max_vals[i] <- 1
      }
    }

    predictor_range_vals <- predictor_max_vals - predictor_min_vals
    predictor_is_binary_vals <- (predictor_min_vals == 0) & (predictor_max_vals == 1)
    track_multiplier_vals <- ifelse(predictor_range_vals > 0, 100 / predictor_range_vals, 100)

    # Calculate DM_R043 v2.0 classification fields
    source_var_vals <- vapply(predictor_terms, infer_source_variable, character(1))
    data_type_vals <- mapply(classify_data_type, predictor_is_binary_vals, source_var_vals)

    # Create output table (schema-compliant with Type B metadata + DM_R043 v2.0)
    # DM_R047: MUST have same schema as CBZ version
    output_table <- tibble(
      # IDENTIFIERS
      product_line_id = pl,
      platform = "eby",
      predictor = predictor_terms,
      predictor_type = vapply(predictor_terms, classify_predictor_type, character(1)),

      # DM_R043 v2.0 CLASSIFICATION
      data_type = as.character(data_type_vals),
      source_variable = source_var_vals,

      # REGRESSION COEFFICIENTS (R118 compliance)
      coefficient = coef_summary$estimate,
      incidence_rate_ratio = coef_summary$incidence_rate_ratio,
      std_error = coef_summary$std.error,
      z_value = coef_summary$statistic,
      p_value = coef_summary$p.value,

      # CONFIDENCE INTERVALS (R118 compliance)
      conf_low = coef_summary$conf.low,
      conf_high = coef_summary$conf.high,
      irr_conf_low = coef_summary$irr_conf_low,
      irr_conf_high = coef_summary$irr_conf_high,

      # PREDICTOR RANGE METADATA (R120)
      predictor_min = predictor_min_vals,
      predictor_max = predictor_max_vals,
      predictor_range = predictor_range_vals,
      predictor_is_binary = predictor_is_binary_vals,
      track_multiplier = track_multiplier_vals,

      # MODEL STATISTICS (MP102 compliance)
      deviance = model_stats$deviance,
      aic = model_stats$AIC,
      sample_size = nrow(model_data_final),
      convergence = ifelse(isTRUE(poisson_model$converged), "converged", "not_converged"),

      # EXISTING METADATA (MP102 compliance)
      analysis_date = Sys.Date(),
      analysis_version = DRV_VERSION,

      # TYPE B METADATA (MP135 v2.0 + UI_R024)
      computed_at = computed_timestamp,
      data_version = latest_data_date
    )

    # Count significant predictors (R118 requirement)
    n_significant <- sum(output_table$p_value < 0.05, na.rm = TRUE)
    n_highly_sig <- sum(output_table$p_value < 0.001, na.rm = TRUE)

    cat(sprintf("  ✅ Regression complete: %d predictors\n", nrow(output_table)))
    cat(sprintf("  ✅ Highly significant (p<0.001): %d (%.1f%%)\n",
                n_highly_sig, 100 * n_highly_sig / nrow(output_table)))
    cat(sprintf("  ✅ Significant (p<0.05): %d (%.1f%%)\n",
                n_significant, 100 * n_significant / nrow(output_table)))
    cat(sprintf("  ✅ Model AIC: %.2f\n", model_stats$AIC))
    cat(sprintf("  ✅ Deviance: %.2f\n", model_stats$deviance))
    cat(sprintf("  ✅ Converged: %s\n", ifelse(poisson_model$converged, "Yes", "No")))
    cat(sprintf("  ✅ Computed at: %s\n", format(computed_timestamp, "%Y-%m-%d %H:%M:%S")))
    cat(sprintf("  ✅ Data version: %s\n", latest_data_date))

    # DM_R046: Enrich with display names (DM_R047 requirement)
    cat("  → Enriching with display names (DM_R046)...\n")
    output_table <- fn_enrich_with_display_names(
      output_table,
      con = con_app,
      metadata_table = "tbl_variable_display_names",
      locale = "zh_TW"
    )

    # Validate enrichment
    validation <- fn_validate_display_name_enrichment(output_table)
    if (!validation$valid) {
      warning("[DM_R046] Enrichment validation failed: ", validation$error)
    } else {
      cat(sprintf("  ✅ Display names: %d categories (%s)\n",
                  length(validation$summary$categories),
                  paste(names(validation$summary$categories), collapse=", ")))
    }

    # Write to app_data (simple overwrite for Type B)
    dbWriteTable(con_app, output_table_name, output_table, overwrite = TRUE)
    cat(sprintf("  ✅ Wrote to: %s\n", output_table_name))

    # Store for merging
    all_results[[pl]] <- output_table

  }, error = function(e) {
    cat(sprintf("  ❌ Regression failed: %s\n", conditionMessage(e)))
    cat("  → Check for perfect multicollinearity or model specification issues\n")
    dbWriteTable(con_app, output_table_name, empty_output_table, overwrite = TRUE)
    cat(sprintf("  → Wrote empty schema to: %s\n", output_table_name))
  })

  cat("\n")
}

# ============================================================================
# MERGE ALL PRODUCT LINES
# ============================================================================

cat("════════════════════════════════════════════════════════════════════\n")
cat("[Phase 2/3] Merging All Product Lines\n")
cat("════════════════════════════════════════════════════════════════════\n\n")

if (length(all_results) > 0) {

  # Combine all product lines
  merged_table <- bind_rows(all_results)

  cat(sprintf("  ✓ Merged: %s total predictors\n",
              format(nrow(merged_table), big.mark=",")))
  cat(sprintf("  ✓ Product lines: %d\n", length(all_results)))

  # Summary statistics across all product lines
  n_sig_all <- sum(merged_table$p_value < 0.05, na.rm = TRUE)
  n_highly_sig_all <- sum(merged_table$p_value < 0.001, na.rm = TRUE)

  cat(sprintf("  ✓ Total highly significant (p<0.001): %s (%.1f%%)\n",
              format(n_highly_sig_all, big.mark=","),
              100 * n_highly_sig_all / nrow(merged_table)))
  cat(sprintf("  ✓ Total significant (p<0.05): %s (%.1f%%)\n",
              format(n_sig_all, big.mark=","),
              100 * n_sig_all / nrow(merged_table)))

  # Write merged table (Type B: simple overwrite)
  dbWriteTable(con_app, "df_eby_poisson_analysis_all", merged_table, overwrite = TRUE)
  cat("\n  ✅ Wrote to: df_eby_poisson_analysis_all\n")

} else {
  cat("  ⚠️  No results to merge\n")
  dbWriteTable(con_app, "df_eby_poisson_analysis_all", empty_output_table, overwrite = TRUE)
  cat("  → Wrote empty schema to: df_eby_poisson_analysis_all\n")
}

cat("\n")

  # Track rows processed
  if (length(all_results) > 0) {
    rows_processed <- nrow(bind_rows(all_results))
  }

}, error = function(e) {
  message("ERROR in MAIN: ", e$message)
  error_occurred <<- TRUE
})

# ==============================================================================
# PART 3: TEST
# ==============================================================================

cat("════════════════════════════════════════════════════════════════════\n")
cat("[Phase 3/3] Validation\n")
cat("════════════════════════════════════════════════════════════════════\n\n")

if (!error_occurred) {
  tryCatch({

validation_pass <- TRUE

for (pl in PRODUCT_LINES) {
  table_name <- sprintf("df_eby_poisson_analysis_%s", pl)
  if (dbExistsTable(con_app, table_name)) {
    row_count <- tbl2(con_app, table_name) %>%
      summarise(n = dplyr::n()) %>%
      collect() %>%
      dplyr::pull(n)

    # Verify Type B metadata columns exist
    cols <- dbListFields(con_app, table_name)
    has_computed_at <- "computed_at" %in% cols
    has_data_version <- "data_version" %in% cols
    has_display_name <- "display_name" %in% cols  # DM_R047 check

    cat(sprintf("  ✓ %s: %d predictors", table_name, row_count))
    if (has_computed_at && has_data_version && has_display_name) {
      cat(" (Type B metadata ✓, Display names ✓)\n")
    } else {
      missing <- c()
      if (!has_computed_at) missing <- c(missing, "computed_at")
      if (!has_data_version) missing <- c(missing, "data_version")
      if (!has_display_name) missing <- c(missing, "display_name")
      cat(sprintf(" (⚠️ Missing: %s)\n", paste(missing, collapse=", ")))
      validation_pass <- FALSE
    }
  } else {
    cat(sprintf("  ❌ %s: NOT FOUND\n", table_name))
    validation_pass <- FALSE
  }
}

cat("\n")

if (dbExistsTable(con_app, "df_eby_poisson_analysis_all")) {
  row_count <- tbl2(con_app, "df_eby_poisson_analysis_all") %>%
    summarise(n = dplyr::n()) %>%
    collect() %>%
    dplyr::pull(n)

  # Verify Type B metadata columns
  cols <- dbListFields(con_app, "df_eby_poisson_analysis_all")
  has_computed_at <- "computed_at" %in% cols
  has_data_version <- "data_version" %in% cols
  has_display_name <- "display_name" %in% cols

  cat(sprintf("  ✓ df_eby_poisson_analysis_all: %d predictors", row_count))
  if (has_computed_at && has_data_version && has_display_name) {
    cat(" (Type B metadata ✓, Display names ✓)\n")
  } else {
    missing <- c()
    if (!has_computed_at) missing <- c(missing, "computed_at")
    if (!has_data_version) missing <- c(missing, "data_version")
    if (!has_display_name) missing <- c(missing, "display_name")
    cat(sprintf(" (⚠️ Missing: %s)\n", paste(missing, collapse=", ")))
    validation_pass <- FALSE
  }
} else {
  cat("  ❌ df_eby_poisson_analysis_all: NOT FOUND\n")
  validation_pass <- FALSE
}

cat("\n")

if (validation_pass) {
  cat("  ✅ All validation checks passed (DM_R047 compliant)\n")
  test_passed <- TRUE
} else {
  cat("  ⚠️  Some validation checks failed\n")
  test_passed <- FALSE
}

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

summary_report <- list(
  script = "eby_D04_02.R",
  platform = "eby",
  group = "D04",
  sequence = "02",
  start_time = start_time,
  end_time = end_time,
  execution_time_secs = as.numeric(execution_time),
  rows_processed = rows_processed,
  status = ifelse(test_passed, "SUCCESS", "FAILED"),
  error_occurred = error_occurred
)

cat("\n")
cat("════════════════════════════════════════════════════════════════════\n")
cat("DERIVATION SUMMARY\n")
cat("════════════════════════════════════════════════════════════════════\n")
cat(sprintf("Script: %s\n", summary_report$script))
cat(sprintf("Status: %s\n", summary_report$status))
cat(sprintf("Rows Processed: %d\n", summary_report$rows_processed))
cat(sprintf("Execution Time: %.2f seconds\n", summary_report$execution_time_secs))
cat("\n")
cat("Analytics Classification: TYPE B - Steady-State Analytics\n")
cat("  - All 3 Poisson components use this data\n")
cat("  - poissonFeatureAnalysis: Attribute coefficients\n")
cat("  - poissonCommentAnalysis: Rating coefficients\n")
cat("  - poissonTimeAnalysis: Time feature coefficients\n")
cat("\n")
cat("Output Tables Created:\n")
for (pl in PRODUCT_LINES) {
  cat(sprintf("  - df_eby_poisson_analysis_%s\n", pl))
}
cat("  - df_eby_poisson_analysis_all (merged)\n")
cat("\n")
cat("Type B Metadata Added:\n")
cat("  - computed_at: Timestamp when DRV ran\n")
cat("  - data_version: Latest order_date in input data\n")
cat("\n")
cat("Principle Compliance: DM_R044 Five-Part Structure\n")
cat("  - PART 1: INITIALIZE (autoinit, packages, connections)\n")
cat("  - PART 2: MAIN (product-line processing, merging)\n")
cat("  - PART 3: TEST (output verification)\n")
cat("  - PART 4: SUMMARIZE (execution report)\n")
cat("  - PART 5: DEINITIALIZE (cleanup, autodeinit)\n")
cat("════════════════════════════════════════════════════════════════════\n")
cat("\n")

# ==============================================================================
# PART 5: DEINITIALIZE
# ==============================================================================

# 5.1: Close database connections
cat("Cleaning up...\n")
if (exists("con_app") && inherits(con_app, "DBIConnection")) {
  dbDisconnect(con_app, shutdown = TRUE)
  cat("  - Disconnected from database\n")
}

# 5.2: Autodeinit (MUST be last statement)
autodeinit()

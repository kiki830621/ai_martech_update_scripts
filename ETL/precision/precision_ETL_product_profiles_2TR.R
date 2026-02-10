#!/usr/bin/env Rscript
# ==============================================================================
# Precision Marketing ETL - Stage 2TR (Transformation)
# ==============================================================================
#
# Purpose: Transform product profile data with business logic and feature engineering
# Stage: 2TR (Transformation - business rules, derived features)
# Input: staged_data.duckdb (from 1ST stage)
# Output: transformed_data.duckdb
#
# Principle Compliance:
# - MP108: Base ETL Pipeline Separation (2TR = Business logic transformations)
# - MP064: ETL-Derivation Separation (2TR prepares for DRV, no cross-table joins)
# - MP102: Completeness (add transformation metadata)
# - R116: Currency already standardized in 1ST (use price_usd for calculations)
#
# Transformation Tasks:
# 1. Price segmentation (low/medium/high based on USD prices)
# 2. Rating categorization (poor/fair/good/excellent)
# 3. Review volume classification (low/medium/high engagement)
# 4. Feature extraction from attributes JSON
# 5. Quality score calculation (composite metric)
# 6. Competitiveness indicators
#
# Week 1 Implementation: MAMBA Precision Marketing Redesign
# Date: 2025-11-12
# ==============================================================================

tbl2_candidates <- c(
  file.path("scripts", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R"),
  file.path("..", "..", "..", "global_scripts", "02_db_utils", "tbl2", "fn_tbl2.R")
)
tbl2_path <- tbl2_candidates[file.exists(tbl2_candidates)][1]
if (is.na(tbl2_path)) {
  stop("fn_tbl2.R not found in expected paths")
}
source(tbl2_path)
library(duckdb)
library(dplyr)
library(tibble)
library(jsonlite)

# ==============================================================================
# Configuration
# ==============================================================================

# Product lines (use English IDs that match 1ST output table names)
PRODUCT_LINES <- c(
  "electric_can_opener",
  "milk_frother",
  "salt_and_pepper_grinder",
  "silicone_spatula",
  "meat_claw",
  "pastry_brush"
)

# Database paths
INPUT_DB <- file.path("data", "staged_data.duckdb")
OUTPUT_DB <- file.path("data", "transformed_data.duckdb")

# Price segmentation thresholds (USD)
PRICE_LOW_THRESHOLD <- 50
PRICE_HIGH_THRESHOLD <- 200

# Rating thresholds
RATING_FAIR_THRESHOLD <- 3.0
RATING_GOOD_THRESHOLD <- 4.0
RATING_EXCELLENT_THRESHOLD <- 4.5

# Review volume thresholds
REVIEW_LOW_THRESHOLD <- 10
REVIEW_HIGH_THRESHOLD <- 100

# ==============================================================================
# Transformation Functions
# ==============================================================================

#' Price Segmentation
#'
#' Categorize products by price range (using standardized USD prices)
#'
#' @param price_usd Numeric vector of USD prices
#' @return Factor vector with levels: "low", "medium", "high"
#'
transform_price_segment <- function(price_usd) {
  segments <- cut(
    price_usd,
    breaks = c(-Inf, PRICE_LOW_THRESHOLD, PRICE_HIGH_THRESHOLD, Inf),
    labels = c("low", "medium", "high"),
    right = TRUE
  )

  return(segments)
}

#' Rating Categorization
#'
#' Categorize products by rating quality
#'
#' @param rating Numeric vector of ratings (0-5 scale)
#' @return Factor vector with levels: "poor", "fair", "good", "excellent"
#'
transform_rating_category <- function(rating) {
  categories <- cut(
    rating,
    breaks = c(-Inf, RATING_FAIR_THRESHOLD, RATING_GOOD_THRESHOLD,
               RATING_EXCELLENT_THRESHOLD, Inf),
    labels = c("poor", "fair", "good", "excellent"),
    right = TRUE
  )

  return(categories)
}

#' Review Volume Classification
#'
#' Classify products by review engagement level
#'
#' @param review_count Numeric vector of review counts
#' @return Factor vector with levels: "low", "medium", "high"
#'
transform_review_volume <- function(review_count) {
  volumes <- cut(
    review_count,
    breaks = c(-Inf, REVIEW_LOW_THRESHOLD, REVIEW_HIGH_THRESHOLD, Inf),
    labels = c("low", "medium", "high"),
    right = TRUE
  )

  return(volumes)
}

#' Quality Score Calculation
#'
#' Calculate composite quality score from rating and review volume
#' Formula: (rating / 5) * 0.7 + log10(review_count + 1) / log10(1000) * 0.3
#' Range: 0-1 (higher is better)
#'
#' @param rating Numeric vector of ratings
#' @param review_count Numeric vector of review counts
#' @return Numeric vector of quality scores (0-1)
#'
transform_quality_score <- function(rating, review_count) {

  # Handle missing values
  rating <- ifelse(is.na(rating), 0, rating)
  review_count <- ifelse(is.na(review_count), 0, review_count)

  # Normalize rating (0-5 → 0-1)
  rating_normalized <- rating / 5

  # Normalize review count (logarithmic scale, 0-1000+ → 0-1)
  review_normalized <- log10(review_count + 1) / log10(1000)
  review_normalized <- pmin(review_normalized, 1)  # Cap at 1

  # Weighted composite score (70% rating, 30% review volume)
  quality_score <- rating_normalized * 0.7 + review_normalized * 0.3

  return(quality_score)
}

#' Competitiveness Indicator
#'
#' Flag products as competitive based on rating and price
#' Competitive = high rating (>4.0) AND reasonable price (medium or low segment)
#'
#' @param rating Numeric vector of ratings
#' @param price_segment Factor vector of price segments
#' @return Logical vector indicating competitive products
#'
transform_competitive_flag <- function(rating, price_segment) {

  is_competitive <- (rating > 4.0) &
                    (price_segment %in% c("low", "medium"))

  # Handle NAs
  is_competitive[is.na(is_competitive)] <- FALSE

  return(is_competitive)
}

#' Extract Features from Attributes JSON
#'
#' Parse JSON attributes column and extract key-value pairs
#'
#' @param attributes_json Character vector of JSON strings
#' @return Data frame with extracted feature columns
#'
transform_extract_attributes <- function(attributes_json) {

  # Initialize result data frame
  result <- data.frame(
    has_attributes = !is.na(attributes_json) & attributes_json != "",
    attribute_count = 0,
    stringsAsFactors = FALSE
  )

  # Parse JSON if exists
  if (any(result$has_attributes)) {
    for (i in which(result$has_attributes)) {
      tryCatch({
        attrs <- fromJSON(attributes_json[i], simplifyVector = FALSE)
        if (is.list(attrs)) {
          result$attribute_count[i] <- length(attrs)
        }
      }, error = function(e) {
        # Skip invalid JSON
        result$has_attributes[i] <- FALSE
      })
    }
  }

  return(result)
}

# ==============================================================================
# Main ETL 2TR Function
# ==============================================================================

precision_etl_2tr <- function() {
  message("====================================================================")
  message("Precision Marketing ETL - Stage 2TR (Transformation)")
  message("====================================================================")
  message(sprintf("Process Date: %s", Sys.time()))
  message(sprintf("Input Database: %s", INPUT_DB))
  message(sprintf("Output Database: %s", OUTPUT_DB))
  message(sprintf("Product Lines: %s", paste(PRODUCT_LINES, collapse = ", ")))
  message("")

  # Check input database exists
  if (!file.exists(INPUT_DB)) {
    stop(sprintf("ERROR: Input database not found: %s\nRun precision_ETL_product_profiles_1ST.R first.", INPUT_DB))
  }

  # Connect to databases
  con_staged <- dbConnect(duckdb::duckdb(), INPUT_DB, read_only = TRUE)
  con_transformed <- dbConnect(duckdb::duckdb(), OUTPUT_DB)

  message("✓ Database connections established")
  message("")

  # Process each product line
  transformation_results <- list()

  for (pl in PRODUCT_LINES) {
    message("====================================================================")
    message(sprintf("Transforming product line: %s", pl))
    message("====================================================================")

    tryCatch({
      # Read staged data
      staged_table <- sprintf("staged_precision_%s", pl)
      message(sprintf("  → Reading from: %s", staged_table))

      staged_data <- tbl2(con_staged, staged_table) %>% collect()
      message(sprintf("  ✓ Retrieved %d rows", nrow(staged_data)))

      # === TRANSFORMATION TASKS ===

      # Task 1: Price Segmentation (requires R116 standardized USD prices)
      message("  → Applying price segmentation...")
      if ("price_usd" %in% names(staged_data)) {
        staged_data$price_segment <- transform_price_segment(staged_data$price_usd)
      } else {
        warning("  WARNING: price_usd column not found. Skipping price segmentation.")
        staged_data$price_segment <- NA
      }

      # Task 2: Rating Categorization
      message("  → Applying rating categorization...")
      if ("rating" %in% names(staged_data)) {
        staged_data$rating_category <- transform_rating_category(staged_data$rating)
      } else {
        warning("  WARNING: rating column not found. Skipping rating categorization.")
        staged_data$rating_category <- NA
      }

      # Task 3: Review Volume Classification
      message("  → Applying review volume classification...")
      if ("review_count" %in% names(staged_data)) {
        staged_data$review_volume <- transform_review_volume(staged_data$review_count)
      } else {
        warning("  WARNING: review_count column not found. Skipping review classification.")
        staged_data$review_volume <- NA
      }

      # Task 4: Quality Score Calculation
      message("  → Calculating quality scores...")
      if ("rating" %in% names(staged_data) && "review_count" %in% names(staged_data)) {
        staged_data$quality_score <- transform_quality_score(
          staged_data$rating,
          staged_data$review_count
        )
      } else {
        warning("  WARNING: Missing rating or review_count. Skipping quality score.")
        staged_data$quality_score <- NA
      }

      # Task 5: Competitiveness Flag
      message("  → Calculating competitiveness indicators...")
      if ("rating" %in% names(staged_data) && "price_segment" %in% names(staged_data)) {
        staged_data$is_competitive <- transform_competitive_flag(
          staged_data$rating,
          staged_data$price_segment
        )
      } else {
        warning("  WARNING: Missing rating or price_segment. Skipping competitiveness flag.")
        staged_data$is_competitive <- FALSE
      }

      # Task 6: Extract Attributes (if attributes column exists)
      message("  → Extracting product attributes...")
      if ("attributes" %in% names(staged_data)) {
        attr_features <- transform_extract_attributes(staged_data$attributes)
        staged_data <- cbind(staged_data, attr_features)
      } else {
        message("  INFO: No attributes column found. Skipping attribute extraction.")
      }

      # Task 7: Add Transformation Metadata
      staged_data$transformation_timestamp <- Sys.time()
      staged_data$transformation_version <- "1.0"

      # Write to transformed database
      transformed_table <- sprintf("transformed_precision_%s", pl)
      message(sprintf("  → Writing to: %s", transformed_table))

      dbWriteTable(con_transformed, transformed_table, staged_data, overwrite = TRUE)

      message(sprintf("  ✓ Successfully transformed %d rows to %s", nrow(staged_data), transformed_table))

      # Calculate statistics for reporting
      competitive_count <- sum(staged_data$is_competitive, na.rm = TRUE)
      competitive_pct <- round(competitive_count / nrow(staged_data) * 100, 1)

      avg_quality <- mean(staged_data$quality_score, na.rm = TRUE)

      # Store result
      transformation_results[[pl]] <- list(
        status = "success",
        rows = nrow(staged_data),
        columns = ncol(staged_data),
        competitive_count = competitive_count,
        competitive_pct = competitive_pct,
        avg_quality_score = round(avg_quality, 3),
        table_name = transformed_table
      )

    }, error = function(e) {
      message(sprintf("  ✗ ERROR transforming %s: %s", pl, e$message))

      transformation_results[[pl]] <<- list(
        status = "error",
        error_message = e$message
      )
    })

    message("")
  }

  # Disconnect from databases
  dbDisconnect(con_staged, shutdown = FALSE)
  dbDisconnect(con_transformed, shutdown = TRUE)
  message("✓ Database connections closed")
  message("")

  # Print summary
  message("====================================================================")
  message("ETL 2TR Transformation Summary")
  message("====================================================================")

  success_count <- sum(sapply(transformation_results, function(x) x$status == "success"))
  error_count <- sum(sapply(transformation_results, function(x) x$status == "error"))

  message(sprintf("Total product lines: %d", length(PRODUCT_LINES)))
  message(sprintf("Successfully transformed: %d", success_count))
  message(sprintf("Failed transformations: %d", error_count))
  message("")

  if (success_count > 0) {
    message("Successful transformations:")
    for (pl in names(transformation_results)) {
      result <- transformation_results[[pl]]
      if (result$status == "success") {
        message(sprintf("  ✓ %s: %d rows → %s",
                       pl, result$rows, result$table_name))
        message(sprintf("      Competitive: %d (%s%%), Avg Quality: %s",
                       result$competitive_count, result$competitive_pct, result$avg_quality_score))
      }
    }
    message("")
  }

  if (error_count > 0) {
    message("Failed transformations:")
    for (pl in names(transformation_results)) {
      result <- transformation_results[[pl]]
      if (result$status == "error") {
        message(sprintf("  ✗ %s: %s", pl, result$error_message))
      }
    }
    message("")
  }

  message("====================================================================")
  message("ETL 2TR Complete")
  message("====================================================================")
  message(sprintf("Output database: %s", OUTPUT_DB))
  message(sprintf("Principle compliance: MP108 (Stage Separation), MP064 (ETL-DRV Separation)"))
  message(sprintf("Next step: Develop DRV derivation scripts (Week 2)"))
  message("")

  return(invisible(transformation_results))
}

# ==============================================================================
# Execute if run as script
# ==============================================================================

if (!interactive()) {
  result <- precision_etl_2tr()

  # Exit with appropriate status code
  error_count <- sum(sapply(result, function(x) x$status == "error"))
  if (error_count > 0) {
    quit(status = 1)
  } else {
    quit(status = 0)
  }
}

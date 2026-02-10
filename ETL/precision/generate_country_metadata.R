#!/usr/bin/env Rscript
#' Generate Country Extraction Metadata
#' 
#' Documents country dimension extraction from currency and marketplace fields
#' Ensures transparency per MP102 (Completeness & Standardization)
#' 
#' @output metadata/country_extraction_metadata.csv

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
library(dplyr)
library(DBI)
library(duckdb)

message("=======================================================")
message("GENERATING COUNTRY EXTRACTION METADATA")
message("=======================================================\n")

# ============================================================
# Connect to Databases
# ============================================================

con_1st <- dbConnect(duckdb::duckdb(), "data/staged_data.duckdb", read_only = TRUE)
con_2tr <- dbConnect(duckdb::duckdb(), "data/transformed_data.duckdb", read_only = TRUE)

# ============================================================
# Analyze Country Extraction Logic
# ============================================================

message("Analyzing country extraction from currency and marketplace...")

# Get currency distribution
currency_dist <- sql_read(con_1st, "
  SELECT 
    currency,
    COUNT(*) as n_products,
    COUNT(DISTINCT product_line) as n_product_lines
  FROM precision_product_profiles_1ST
  WHERE currency IS NOT NULL
  GROUP BY currency
  ORDER BY n_products DESC
")

message(sprintf("Found %d unique currencies in source data", nrow(currency_dist)))

# Define currency to country mapping (based on R116 implementation)
currency_country_map <- tribble(
  ~original_currency, ~extracted_country, ~extraction_logic, ~confidence,
  "USD", "USA", "currency_to_country_mapping", "high",
  "GBP", "UK", "currency_to_country_mapping", "high",
  "EUR", "Europe", "currency_to_country_mapping", "medium",
  "AUD", "Australia", "currency_to_country_mapping", "high",
  "JPY", "Japan", "currency_to_country_mapping", "high",
  "CAD", "Canada", "currency_to_country_mapping", "high",
  "CNY", "China", "currency_to_country_mapping", "high",
  "CHF", "Switzerland", "currency_to_country_mapping", "high",
  "SEK", "Sweden", "currency_to_country_mapping", "high",
  "NZD", "New Zealand", "currency_to_country_mapping", "high"
)

# ============================================================
# Match Currencies to Mapping
# ============================================================

message("Matching currencies to country extraction logic...")

metadata <- currency_dist %>%
  left_join(currency_country_map, by = c("currency" = "original_currency")) %>%
  mutate(
    extracted_country = if_else(is.na(extracted_country), "UNKNOWN", extracted_country),
    extraction_logic = if_else(is.na(extraction_logic), 
                              "no_mapping_available", 
                              extraction_logic),
    confidence = if_else(is.na(confidence), "unknown", confidence),
    timestamp = Sys.time()
  )

# ============================================================
# Verify Against Actual Data
# ============================================================

message("Verifying extraction results in transformed data...")

# Get actual country distribution from 2TR
if (dbExistsTable(con_2tr, "precision_product_profiles_2TR")) {
  
  # Check if country column exists
  cols_2tr <- sql_read(con_2tr, 
    "SELECT * FROM precision_product_profiles_2TR LIMIT 1") %>%
    names()
  
  if ("country" %in% cols_2tr) {
    actual_countries <- sql_read(con_2tr, "
      SELECT 
        country,
        COUNT(*) as n_products
      FROM precision_product_profiles_2TR
      WHERE country IS NOT NULL
      GROUP BY country
      ORDER BY n_products DESC
    ")
    
    message("\nActual country distribution in 2TR:")
    for (i in 1:nrow(actual_countries)) {
      message(sprintf("  %s: %d products", 
                     actual_countries$country[i],
                     actual_countries$n_products[i]))
    }
    
    # Calculate unknown rate
    total_products <- sum(actual_countries$n_products)
    unknown_products <- actual_countries %>%
      filter(country == "UNKNOWN") %>%
      pull(n_products) %>%
      sum()
    
    unknown_rate <- unknown_products / total_products
    
    message(sprintf("\nUnknown country rate: %.1f%%", unknown_rate * 100))
    
    if (unknown_rate > 0.05) {
      message("⚠️ WARNING: >5% of products have UNKNOWN country")
      message("   Consider adding more currency mappings or requiring country in source data")
    }
  } else {
    message("⚠️ Warning: 'country' column not found in precision_product_profiles_2TR")
    message("   Country extraction may not have been implemented")
  }
}

# ============================================================
# Save Metadata
# ============================================================

output_file <- "metadata/country_extraction_metadata.csv"
write.csv(metadata, output_file, row.names = FALSE)

message(sprintf("\n✅ Metadata saved to: %s", output_file))
message(sprintf("   Currency mappings documented: %d", nrow(metadata)))
message(sprintf("   High confidence mappings: %d", 
               sum(metadata$confidence == "high")))
message(sprintf("   Unknown mappings: %d", 
               sum(metadata$extracted_country == "UNKNOWN")))

# ============================================================
# Generate Recommendations
# ============================================================

unknown_currencies <- metadata %>%
  filter(extracted_country == "UNKNOWN") %>%
  select(currency = currency, n_products)

if (nrow(unknown_currencies) > 0) {
  recommendation_file <- "metadata/country_extraction_recommendations.txt"
  
  sink(recommendation_file)
  cat("=======================================================\n")
  cat("COUNTRY EXTRACTION RECOMMENDATIONS\n")
  cat("=======================================================\n\n")
  cat(sprintf("Generated: %s\n\n", Sys.time()))
  
  cat("UNMAPPED CURRENCIES\n")
  cat("The following currencies do not have country mappings:\n\n")
  for (i in 1:nrow(unknown_currencies)) {
    cat(sprintf("  - %s (%d products)\n", 
               unknown_currencies$currency[i],
               unknown_currencies$n_products[i]))
  }
  
  cat("\nRECOMMENDATIONS:\n")
  cat("1. Add these currencies to currency_country_map in generate_country_metadata.R\n")
  cat("2. Update ETL 1ST stage to include these mappings\n")
  cat("3. Consider requiring 'country' field in source data (Google Sheets)\n")
  cat("4. Implement marketplace-based country extraction as fallback\n")
  
  cat("\n=======================================================\n")
  sink()
  
  message(sprintf("\n📋 Recommendations saved to: %s", recommendation_file))
}

# ============================================================
# Summary Statistics
# ============================================================

message("\nSummary Statistics:")

total_products <- sum(metadata$n_products)
high_confidence_products <- metadata %>%
  filter(confidence == "high") %>%
  pull(n_products) %>%
  sum()

message(sprintf("  Total products: %d", total_products))
message(sprintf("  High confidence extraction: %d (%.1f%%)", 
               high_confidence_products,
               high_confidence_products / total_products * 100))

# ============================================================
# Cleanup
# ============================================================

dbDisconnect(con_1st, shutdown = TRUE)
dbDisconnect(con_2tr, shutdown = TRUE)

message("\n=======================================================")
message("COUNTRY EXTRACTION METADATA GENERATION COMPLETE")
message("=======================================================")

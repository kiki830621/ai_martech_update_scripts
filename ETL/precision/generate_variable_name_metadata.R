#!/usr/bin/env Rscript
#' Generate Variable Name Transformation Metadata
#' 
#' Documents all variable name transformations applied during ETL 2TR stage
#' Ensures auditability per MP102 (Completeness & Standardization)
#' 
#' @output metadata/variable_name_transformations.csv

library(dplyr)
library(DBI)
library(duckdb)

message("=======================================================")
message("GENERATING VARIABLE NAME TRANSFORMATION METADATA")
message("=======================================================\n")

# ============================================================
# Connect to Databases
# ============================================================

con_1st <- dbConnect(duckdb::duckdb(), "data/staged_data.duckdb", read_only = TRUE)
con_2tr <- dbConnect(duckdb::duckdb(), "data/transformed_data.duckdb", read_only = TRUE)

# ============================================================
# Extract Variable Names from Each Stage
# ============================================================

message("Extracting variable names from 1ST stage...")
vars_1st <- dbGetQuery(con_1st, 
  "SELECT * FROM precision_product_profiles_1ST LIMIT 1") %>%
  names()

message("Extracting variable names from 2TR stage...")
vars_2tr <- dbGetQuery(con_2tr,
  "SELECT * FROM precision_product_profiles_2TR LIMIT 1") %>%
  names()

# ============================================================
# Identify Transformations
# ============================================================

message("Analyzing transformations...\n")

# Common transformation patterns
transformation_rules <- tribble(
  ~pattern, ~transformation_type,
  "^x0_", "remove_x0_prefix",
  "[一-龥ぁ-ゔァ-ヴー々〆〤]", "remove_chinese_japanese",
  "[A-Z]", "to_lowercase",
  "\\s+", "replace_spaces_with_underscore",
  "[-.]", "replace_special_chars",
  "__+", "collapse_multiple_underscores"
)

# Create metadata for each variable
metadata_list <- list()

for (var_1st in vars_1st) {
  # Find corresponding variable in 2TR (exact match first)
  var_2tr <- vars_2tr[vars_2tr == var_1st]
  
  if (length(var_2tr) == 0) {
    # Try to find transformed version
    # Common transformations: lowercase, remove x0_, remove Chinese, etc.
    
    # Generate possible transformations
    possible_transforms <- c(
      tolower(var_1st),
      gsub("^x0_", "", var_1st, ignore.case = TRUE),
      gsub("[一-龥ぁ-ゔァ-ヴー々〆〤]", "", var_1st),
      gsub("\\s+", "_", var_1st),
      gsub("-", "_", var_1st),
      gsub("\\.", "_", var_1st)
    )
    
    # Combine transformations
    var_clean <- var_1st
    var_clean <- gsub("^x0_", "", var_clean, ignore.case = TRUE)
    var_clean <- gsub("[一-龥ぁ-ゔァ-ヴー々〆〤]", "", var_clean)
    var_clean <- tolower(var_clean)
    var_clean <- gsub("\\s+", "_", var_clean)
    var_clean <- gsub("-", "_", var_clean)
    var_clean <- gsub("\\.", "_", var_clean)
    var_clean <- gsub("__+", "_", var_clean)
    
    # Check if this matches any 2TR variable
    var_2tr <- vars_2tr[vars_2tr == var_clean]
    
    if (length(var_2tr) > 0) {
      # Found transformation
      transformations_applied <- c()
      
      if (grepl("^x0_", var_1st, ignore.case = TRUE)) {
        transformations_applied <- c(transformations_applied, "remove_x0_prefix")
      }
      if (grepl("[一-龥ぁ-ゔァ-ヴー々〆〤]", var_1st)) {
        transformations_applied <- c(transformations_applied, "remove_chinese_japanese")
      }
      if (grepl("[A-Z]", var_1st) && !grepl("[A-Z]", var_2tr)) {
        transformations_applied <- c(transformations_applied, "to_lowercase")
      }
      if (grepl("\\s+", var_1st)) {
        transformations_applied <- c(transformations_applied, "replace_spaces_with_underscore")
      }
      if (grepl("[-.]", var_1st)) {
        transformations_applied <- c(transformations_applied, "replace_special_chars")
      }
      
      metadata_list[[length(metadata_list) + 1]] <- tibble(
        original_name = var_1st,
        standardized_name = var_2tr[1],
        transformation_type = paste(transformations_applied, collapse = "|"),
        n_transformations = length(transformations_applied),
        timestamp = Sys.time()
      )
    } else {
      # Variable removed or heavily renamed
      metadata_list[[length(metadata_list) + 1]] <- tibble(
        original_name = var_1st,
        standardized_name = NA_character_,
        transformation_type = "removed_or_renamed",
        n_transformations = NA_integer_,
        timestamp = Sys.time()
      )
    }
  } else {
    # No transformation (exact match)
    metadata_list[[length(metadata_list) + 1]] <- tibble(
      original_name = var_1st,
      standardized_name = var_1st,
      transformation_type = "no_change",
      n_transformations = 0L,
      timestamp = Sys.time()
    )
  }
}

# Check for new variables in 2TR not in 1ST
new_vars <- setdiff(vars_2tr, vars_1st)

for (var_new in new_vars) {
  # These are newly created variables (derived or engineered)
  metadata_list[[length(metadata_list) + 1]] <- tibble(
    original_name = NA_character_,
    standardized_name = var_new,
    transformation_type = "newly_created",
    n_transformations = NA_integer_,
    timestamp = Sys.time()
  )
}

# Combine all metadata
metadata_df <- bind_rows(metadata_list)

# ============================================================
# Add Product Line Context
# ============================================================

message("Adding product line context...")

# Get product lines from data
product_lines <- dbGetQuery(con_2tr,
  "SELECT DISTINCT product_line FROM precision_product_profiles_2TR") %>%
  pull(product_line)

# Expand metadata to include product line applicability
metadata_expanded <- expand_grid(
  metadata_df,
  product_line = product_lines
)

# ============================================================
# Save Metadata
# ============================================================

output_file <- "metadata/variable_name_transformations.csv"
write.csv(metadata_expanded, output_file, row.names = FALSE)

message(sprintf("\n✅ Metadata saved to: %s", output_file))
message(sprintf("   Total transformations documented: %d", nrow(metadata_df)))
message(sprintf("   Variables unchanged: %d", sum(metadata_df$transformation_type == "no_change", na.rm = TRUE)))
message(sprintf("   Variables transformed: %d", sum(metadata_df$n_transformations > 0, na.rm = TRUE)))
message(sprintf("   Variables removed: %d", sum(metadata_df$transformation_type == "removed_or_renamed", na.rm = TRUE)))
message(sprintf("   New variables created: %d", sum(metadata_df$transformation_type == "newly_created", na.rm = TRUE)))

# ============================================================
# Cleanup
# ============================================================

dbDisconnect(con_1st, shutdown = TRUE)
dbDisconnect(con_2tr, shutdown = TRUE)

message("\n=======================================================")
message("VARIABLE NAME METADATA GENERATION COMPLETE")
message("=======================================================")

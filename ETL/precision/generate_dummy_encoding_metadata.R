#!/usr/bin/env Rscript
#' Generate Dummy Encoding Metadata
#' 
#' Documents all dummy variable creation during ETL 2TR stage
#' Ensures auditability per MP102 (Completeness & Standardization)
#' 
#' @output metadata/dummy_encoding_metadata.csv

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
message("GENERATING DUMMY ENCODING METADATA")
message("=======================================================\n")

# ============================================================
# Connect to Database
# ============================================================

con <- dbConnect(duckdb::duckdb(), "data/transformed_data.duckdb", read_only = TRUE)

# ============================================================
# Identify Categorical Variables and Their Dummies
# ============================================================

message("Analyzing dummy variables in 2TR stage...")

# Get all column names
all_columns <- sql_read(con, 
  "SELECT * FROM precision_product_profiles_2TR LIMIT 1") %>%
  names()

# Identify dummy variables (binary 0/1 columns)
# Common patterns: variable_category, is_something, has_something

dummy_patterns <- c(
  "^spring_pressure_range_",
  "^fin_type_",
  "^material_",
  "^coating_",
  "^mounting_",
  "^application_"
)

metadata_list <- list()

for (pattern in dummy_patterns) {
  # Find columns matching this pattern
  dummy_cols <- grep(pattern, all_columns, value = TRUE)
  
  if (length(dummy_cols) > 0) {
    # Extract base variable name
    base_var <- gsub("_[^_]+$", "", dummy_cols[1])
    base_var <- gsub(pattern, "", base_var)
    
    # Get unique values for each dummy
    for (dummy_col in dummy_cols) {
      # Extract category from dummy column name
      category <- gsub(pattern, "", dummy_col)
      
      # Get statistics
      stats <- sql_read(con, sprintf(
        "SELECT 
           COUNT(*) as total_records,
           SUM(CAST(%s AS INTEGER)) as n_ones,
           COUNT(DISTINCT %s) as n_unique
         FROM precision_product_profiles_2TR
         WHERE %s IS NOT NULL",
        dummy_col, dummy_col, dummy_col
      ))
      
      frequency <- stats$n_ones / stats$total_records
      
      metadata_list[[length(metadata_list) + 1]] <- tibble(
        original_variable = gsub("^", "", pattern),
        dummy_variable_name = dummy_col,
        category = category,
        encoding_method = "binary",
        n_records = stats$total_records,
        n_ones = stats$n_ones,
        frequency = frequency,
        threshold = if_else(frequency >= 0.01, "included", "rare"),
        timestamp = Sys.time()
      )
    }
  }
}

# ============================================================
# Identify Original Categorical Variables
# ============================================================

message("Identifying original categorical variables...")

# Get data sample to identify categorical columns
data_sample <- sql_read(con,
  "SELECT * FROM precision_product_profiles_1ST LIMIT 1000")

categorical_vars <- c()

for (col in names(data_sample)) {
  if (is.character(data_sample[[col]]) || is.factor(data_sample[[col]])) {
    n_unique <- n_distinct(data_sample[[col]])
    total_rows <- nrow(data_sample)
    
    # Consider as categorical if < 50% unique values
    if (n_unique / total_rows < 0.5 && n_unique > 1) {
      categorical_vars <- c(categorical_vars, col)
      
      # Get category counts
      category_counts <- data_sample %>%
        count(.data[[col]]) %>%
        arrange(desc(n))
      
      # Check if this variable has corresponding dummies
      has_dummies <- any(grepl(paste0("^", col, "_"), all_columns))
      
      if (!has_dummies) {
        # Document that no dummies were created
        metadata_list[[length(metadata_list) + 1]] <- tibble(
          original_variable = col,
          dummy_variable_name = NA_character_,
          category = NA_character_,
          encoding_method = "no_encoding",
          n_records = nrow(data_sample),
          n_ones = NA_integer_,
          frequency = NA_real_,
          threshold = "not_encoded",
          timestamp = Sys.time()
        )
      }
    }
  }
}

# ============================================================
# Combine Metadata
# ============================================================

if (length(metadata_list) > 0) {
  metadata_df <- bind_rows(metadata_list)
} else {
  # Create empty metadata if no dummies found
  metadata_df <- tibble(
    original_variable = character(),
    dummy_variable_name = character(),
    category = character(),
    encoding_method = character(),
    n_records = integer(),
    n_ones = integer(),
    frequency = numeric(),
    threshold = character(),
    timestamp = as.POSIXct(character())
  )
}

# ============================================================
# Add Product Line Context
# ============================================================

message("Adding product line context...")

product_lines <- sql_read(con,
  "SELECT DISTINCT product_line FROM precision_product_profiles_2TR") %>%
  pull(product_line)

metadata_expanded <- expand_grid(
  metadata_df,
  product_line = product_lines
)

# ============================================================
# Save Metadata
# ============================================================

output_file <- "metadata/dummy_encoding_metadata.csv"
write.csv(metadata_expanded, output_file, row.names = FALSE)

message(sprintf("\n✅ Metadata saved to: %s", output_file))
message(sprintf("   Total dummy variables documented: %d", 
               sum(!is.na(metadata_df$dummy_variable_name))))
message(sprintf("   Original categorical variables: %d", 
               length(unique(metadata_df$original_variable))))
message(sprintf("   Variables not encoded: %d", 
               sum(metadata_df$encoding_method == "no_encoding", na.rm = TRUE)))

# Show frequency distribution
if (nrow(metadata_df) > 0 && any(!is.na(metadata_df$frequency))) {
  freq_summary <- metadata_df %>%
    filter(!is.na(frequency)) %>%
    summarize(
      min_freq = min(frequency, na.rm = TRUE),
      median_freq = median(frequency, na.rm = TRUE),
      max_freq = max(frequency, na.rm = TRUE)
    )
  
  message(sprintf("   Frequency range: %.1f%% to %.1f%% (median: %.1f%%)",
                 freq_summary$min_freq * 100,
                 freq_summary$max_freq * 100,
                 freq_summary$median_freq * 100))
}

# ============================================================
# Cleanup
# ============================================================

dbDisconnect(con, shutdown = TRUE)

message("\n=======================================================")
message("DUMMY ENCODING METADATA GENERATION COMPLETE")
message("=======================================================")

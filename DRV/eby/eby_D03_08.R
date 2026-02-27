#####
# CONSUMES: df_all_comment_property, df_comment_property_all, df_comment_property_pl, df_comment_property_rating_, df_competitor_items, df_eby_competitor_item_id, df_eby_review, df_review_all, df_review_joined, df_review_long, df_review_pl, df_review_sampled, df_review_selected
# PRODUCES: none
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: none
#####


#' @title eby_D03_08
#' @description Derivation task
#' @business_rules See script comments for business logic.
#' @platform eby
#' @author MAMBA Development Team
#' @date 2025-12-30
#' @logical_step_id D03_01
#' @logical_step_status reassigned
#' @legacy_step_id D03_08

# eby_D03_08.R - Comment Property Rating Analysis for eBay
# D03_08: Create sampled_long tables for AI rating
#
# This script creates property rating tables and performs wide-to-long transformation
# for eBay comment property analysis. It reads from raw_data and stores the long
# format data in the comment_property_rating database.
#
# Data Flow:
# 1. Read df_eby_review from raw_data
# 2. Extract ebay_item_number from link URL
# 3. Join with df_eby_competitor_item_id to get product_line_id
# 4. Join with df_all_comment_property to get properties
# 5. Create wide-to-long transformation
# 6. Store results in comment_property_rating database
#
# Following principles:
# - MP047: Functional Programming
# - SO_R007: One Function One File
# - SO_R026: Function File Naming
# - DEV_R001: Apply Over Loops
# - MP051: Explicit Parameter Specification

# Initialize environment
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
needgoogledrive <- TRUE
autoinit()

# Connect to comment_property_rating database (main connection)
comment_property_rating <- dbConnectDuckdb(db_path_list$comment_property_rating, read_only = FALSE)

# Connect to raw_data for reading
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)

# Verify database access
tryCatch({
  test_result1 <- tbl2(raw_data, "df_eby_review") %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::pull(n)
  test_result2 <- tbl2(raw_data, "df_eby_competitor_item_id") %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::pull(n)
  test_result3 <- tbl2(raw_data, "df_all_comment_property") %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::pull(n)

  message("Successfully connected to raw_data database")
  message("Found ", test_result1, " reviews")
  message("Found ", test_result2, " competitor items")
  message("Found ", test_result3, " comment properties")
}, error = function(e) {
  stop("Cannot access tables from raw_data: ", e$message)
})

# Configuration
comment_sample_size <- 30  # Sample size per product
type_filter <- c("屬性")   # Filter for property types

# Helper function to extract ebay_item_number from link
extract_ebay_item_number <- function(links) {
  # Pattern: /itm/{item_number}? or /itm/{item_number}/
  sapply(links, function(link) {
    if (is.na(link) || link == "") return(NA_character_)
    match <- regmatches(link, regexpr("/itm/(\\d+)", link, perl = TRUE))
    if (length(match) == 0 || match == "") return(NA_character_)
    gsub("/itm/", "", match)
  }, USE.NAMES = FALSE)
}

# Get all product lines from competitor item id table
product_lines <- tbl2(raw_data, "df_eby_competitor_item_id") %>%
  dplyr::distinct(product_line_id) %>%
  dplyr::arrange(product_line_id) %>%
  dplyr::pull(product_line_id)

message("Processing product lines: ", paste(product_lines, collapse = ", "))

# Read all data once (more efficient)
message("Loading review data...")
df_review_all <- tbl2(raw_data, "df_eby_review") %>%
  dplyr::collect()
message("Loaded ", nrow(df_review_all), " reviews")

message("Loading competitor item mapping...")
df_competitor_items <- tbl2(raw_data, "df_eby_competitor_item_id") %>%
  dplyr::collect()
message("Loaded ", nrow(df_competitor_items), " competitor items")

message("Loading comment properties...")
df_comment_property_all <- tbl2(raw_data, "df_all_comment_property") %>%
  dplyr::collect()
message("Loaded ", nrow(df_comment_property_all), " comment properties")

# Extract ebay_item_number from link
message("Extracting ebay_item_number from links...")
df_review_all$ebay_item_number <- extract_ebay_item_number(df_review_all$link)

# Convert to numeric for joining
df_review_all$ebay_item_number <- as.numeric(df_review_all$ebay_item_number)
df_competitor_items$ebay_item_number <- as.numeric(df_competitor_items$ebay_item_number)

# Join reviews with competitor items to get product_line_id
message("Joining reviews with competitor items...")
df_review_joined <- df_review_all %>%
  inner_join(
    df_competitor_items %>% select(ebay_item_number, product_line_id, seller, brand, country),
    by = "ebay_item_number"
  )

message("Joined ", nrow(df_review_joined), " reviews with product lines")

# Process each product line
for (product_line_id_i in product_lines) {

  message("\n", strrep("=", 50))
  message("Processing product line: ", product_line_id_i)

  # Filter reviews for this product line
  df_review_pl <- df_review_joined %>%
    filter(product_line_id == product_line_id_i)

  message("Found ", nrow(df_review_pl), " reviews for ", product_line_id_i)

  # Skip if no reviews
  if (nrow(df_review_pl) == 0) {
    message("No reviews found for product line: ", product_line_id_i, ", skipping...")
    next
  }

  # Get properties for this product line
  df_comment_property_pl <- df_comment_property_all %>%
    filter(product_line_id == product_line_id_i) %>%
    filter(type %in% type_filter)

  message("Found ", nrow(df_comment_property_pl), " properties for ", product_line_id_i)

  # Skip if no properties
  if (nrow(df_comment_property_pl) == 0) {
    message("No properties found for product line: ", product_line_id_i, ", skipping...")
    next
  }

  # Property names for columns
  new_columns <- df_comment_property_pl$property_name
  message("Creating property columns: ", paste(head(new_columns, 5), collapse = ", "),
          if(length(new_columns) > 5) paste0("... (", length(new_columns), " total)") else "")

  # Standardize column names for consistency with amz pipeline
  df_review_selected <- df_review_pl %>%
    transmute(
      # Core identification fields
      platform_id = "eby",
      product_line_id = product_line_id,
      product_id = as.character(ebay_item_number),  # Use ebay_item_number as product_id
      reviewer_id = fb_context_user,
      # Review content fields (map to standard names)
      review_date = as.Date(fb_context_timestamp),
      review_title = item_name,  # Use item_name as title (eBay doesn't have review title)
      review_body = fb_comment,  # Map fb_comment to review_body
      rating = fb_rating,
      # Additional fields
      seller = seller,
      brand = brand,
      country = country,
      # Original fields for reference
      link = link,
      store_name = store_name,
      ebay_item_number = ebay_item_number
    )

  # Add empty columns for each property
  for (col in new_columns) {
    df_review_selected[[col]] <- NA_character_
  }

  # Create raw table
  table_name_raw <- paste0("df_comment_property_rating_", product_line_id_i, "___raw")
  dbWriteTable(comment_property_rating, table_name_raw, df_review_selected, overwrite = TRUE)
  message("Created raw table: ", table_name_raw, " (", nrow(df_review_selected), " rows)")

  # Create sampled data (sample per product)
  df_review_sampled <- df_review_selected %>%
    group_by(product_id) %>%
    slice_tail(n = comment_sample_size) %>%
    ungroup()

  # Log sample information
  unique_products <- length(unique(df_review_sampled$product_id))
  message("Sampled: ", nrow(df_review_sampled), " reviews from ", unique_products, " products")

  # Create sampled table
  table_name_sampled <- paste0("df_comment_property_rating_", product_line_id_i, "___sampled")
  dbWriteTable(comment_property_rating, table_name_sampled, df_review_sampled, overwrite = TRUE)
  message("Created sampled table: ", table_name_sampled)

  # Create long-format data
  df_review_long <- df_review_sampled %>%
    tidyr::pivot_longer(
      cols = tidyselect::all_of(new_columns),
      names_to = "property_name",
      values_to = "result"
    ) %>%
    # Join with property definitions
    left_join(
      df_comment_property_pl %>%
        select(property_name, property_id, property_name_english, type, definition, product_line_id),
      by = c("property_name", "product_line_id")
    )

  # Create long-format table
  table_name_long <- paste0("df_comment_property_rating_", product_line_id_i, "___sampled_long")
  dbWriteTable(comment_property_rating, table_name_long, df_review_long, overwrite = TRUE)
  message("Created long-format table: ", table_name_long, " (", nrow(df_review_long), " rows)")

  # Estimate processing time
  n_operations <- nrow(df_review_long)
  interval <- 0.5  # Seconds per API call
  total_seconds <- n_operations * interval
  total_minutes <- total_seconds / 60

  time_display <- if (total_seconds < 60) {
    sprintf("%.0f seconds", total_seconds)
  } else if (total_minutes < 60) {
    sprintf("%.1f minutes", total_minutes)
  } else {
    sprintf("%.1f hours", total_minutes / 60)
  }

  message("Estimated AI rating time: ", n_operations, " operations x ", interval, "s = ", time_display)

  message("Completed processing for: ", product_line_id_i)
}

# Summary
message("\n", strrep("=", 50))
message("Summary of created tables:")
tables <- dbListTables(comment_property_rating)
for (tbl in tables) {
  count <- tbl2(comment_property_rating, tbl) %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::pull(n)
  message("  ", tbl, ": ", count, " rows")
}

# Clean up
autodeinit()

message("\neBay comment property rating analysis completed successfully")
message("Long format data stored in comment_property_rating database")
message("Next step: Run eby_D03_06.R for AI rating")

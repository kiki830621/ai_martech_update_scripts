# amz_ETL06_2TR.R - Amazon Reviews 資料轉換
# ETL06 階段 2 轉換：處理和豐富 Amazon reviews 資料
# 遵循 R113：四部分更新腳本結構

# ==============================================================================
# 1. INITIALIZE
# ==============================================================================

# Initialize script execution tracking
script_success <- FALSE
test_passed <- FALSE
main_error <- NULL

# Initialize using unified autoinit system
autoinit()

# Establish database connections using dbConnectDuckdb
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, 
                                    read_only = FALSE)

message("INITIALIZE: Amazon reviews transform (ETL06 2TR) script initialized")

# ==============================================================================
# 2. MAIN
# ==============================================================================

tryCatch({
  message("MAIN: Starting ETL06 Transform Phase - Amazon reviews...")

  # Load staged data from previous phase
  source_table <- "df_amz_review___staged"
  
  # Check if source table exists
  if (!source_table %in% dbListTables(staged_data)) {
    stop("Source table ", source_table, " not found in staged_data. Please run ETL06_1ST first.")
  }
  
  # Load staged reviews data
  staged_reviews <- dbGetQuery(staged_data, paste("SELECT * FROM", source_table))
  message("MAIN: Loaded ", nrow(staged_reviews), " staged reviews")
  
  # Verify expected columns are present after staging (using original column names)
  expected_cols <- c("variation", "review_date", "body", "rating", "author", "path", "verified", "helpful", "title")
  actual_cols <- names(staged_reviews)
  missing_cols <- setdiff(expected_cols, actual_cols)
  
  if (length(missing_cols) > 0) {
    stop("MAIN: Missing expected columns from staging: ", paste(missing_cols, collapse = ", "))
  }
  
  # Step 1: Rename columns to standardized ETL schema
  message("MAIN: Renaming columns to ETL schema...")
  staged_reviews <- staged_reviews %>%
    dplyr::rename(
      product_id = variation,
      review_text = body,
      reviewer_id = author
    )
  message("MAIN: Column renaming completed")
  
  # Step 2: Add platform_id and product_line_id using standardized lookups
  message("MAIN: Adding platform_id and product_line_id...")
  
  # Get Amazon platform_id from df_platform
  amazon_platform <- df_platform %>% 
    dplyr::filter(platform_name_english == "Amazon") %>% 
    dplyr::pull(platform_id)
  
  if (length(amazon_platform) == 0) {
    stop("MAIN: Amazon platform not found in df_platform")
  }
  
  # Add platform_id and extract 3-digit number from path
  transformed_reviews <- staged_reviews %>% 
    dplyr::mutate(
      platform_id = amazon_platform,  # Use standardized platform_id from df_platform
      # Extract 3-digit number from path for product line lookup
      product_line_id_int = as.integer(stringr::str_extract(path, "(?<=amazon_reviews/)\\d{3}")),
      product_line_id = df_product_line$product_line_id[product_line_id_int+1]
      
    )
  
  transformed_reviews <- transformed_reviews %>% 
    left_join(df_product_line,by = join_by(product_line_id)) %>% 
    filter(included==TRUE) %>%
    dplyr::select(-included)
  
  message("MAIN: Platform and product line IDs added")
  
  # Verify product line ID assignment
  missing_product_line <- sum(is.na(transformed_reviews$product_line_id))
  if (missing_product_line > 0) {
    warning("MAIN: ", missing_product_line, " reviews could not be assigned to a product line!")
  } else {
    message("MAIN: All reviews successfully assigned to product lines")
  }
  
  
  # Step 3: Add competitor flags
  message("MAIN: Adding competitor flags...")
  
  # Check if competitor table exists (use transformed_data, not raw_data)
  competitor_table <- "df_amz_competitor_product_id___transformed"
  if (competitor_table %in% dbListTables(transformed_data)) {
    # Retrieve competitor products with flag - only select needed columns
    df_competitor_product_id_with_flag <- dbGetQuery(transformed_data, 
      paste0("SELECT DISTINCT product_id, product_line_id FROM ", competitor_table)) %>% 
      dplyr::mutate(included_competiter = TRUE)
    
    # Add competitor flag to reviews using mutate with %in% operator
    competitor_product_ids <- df_competitor_product_id_with_flag$product_id
    transformed_reviews <- transformed_reviews %>% 
      dplyr::mutate(included_competiter = product_id %in% competitor_product_ids)
    
    # Show competitor flag statistics with detailed information
    flag_stats <- table(transformed_reviews$included_competiter, useNA = "always")
    message("MAIN: Reviews by competitor flag status:")
    print(flag_stats)
    
    # Calculate and display detailed competitor statistics
    competitor_review_count <- sum(transformed_reviews$included_competiter, na.rm = TRUE)
    total_review_count <- nrow(transformed_reviews)
    competitor_percentage <- round((competitor_review_count / total_review_count) * 100, 2)
    
    message("MAIN: Competitor flag statistics:")
    message("MAIN: - Total reviews: ", total_review_count)
    message("MAIN: - Competitor reviews: ", competitor_review_count)
    message("MAIN: - Non-competitor reviews: ", total_review_count - competitor_review_count)
    message("MAIN: - Competitor percentage: ", competitor_percentage, "%")
    message("MAIN: - Non-competitor percentage: ", 
            round(100 - competitor_percentage, 2), "%")
    
    # Additional debugging information
    competitor_table_count <- nrow(df_competitor_product_id_with_flag)
    message("MAIN: Competitor table has ", competitor_table_count, " records")

    if (competitor_table_count > 0) {
      unique_competitor_products <- length(unique(
        df_competitor_product_id_with_flag$product_id
      ))
      message("MAIN: Unique competitor product_ids: ", 
              unique_competitor_products)

      # Show sample of competitor product_ids
      sample_competitor_ids <- head(unique(
        df_competitor_product_id_with_flag$product_id
      ), 5)
      message("MAIN: Sample competitor product_ids: ", 
              paste(sample_competitor_ids, collapse = ", "))

      # Show sample of review product_ids
      sample_review_ids <- head(unique(transformed_reviews$product_id), 5)
      message("MAIN: Sample review product_ids: ", 
              paste(sample_review_ids, collapse = ", "))
      
      # Check for any matches
      matching_ids <- intersect(
        unique(df_competitor_product_id_with_flag$product_id),
        unique(transformed_reviews$product_id)
      )
      message("MAIN: Matching product_ids: ", 
              if(length(matching_ids) > 0) paste(matching_ids, collapse = ", ") else "NONE")
    } else {
      message("MAIN: WARNING - Competitor table is empty!")
    }
  } else {
    message("MAIN: Competitor table not found, adding default FALSE flag")
    transformed_reviews <- transformed_reviews %>% 
      dplyr::mutate(included_competiter = FALSE)
  }
  
  message("MAIN: Competitor flags added")
  
  # Step 4: Final data preparation
  message("MAIN: Final data preparation...")
  
  transformed_reviews <- transformed_reviews %>%
    dplyr::distinct_all() %>% 
    dplyr::relocate(platform_id, product_line_id, product_id, review_date, product_line_name_chinese, product_line_name_english) %>% 
    dplyr::arrange(platform_id, product_line_id, product_id, review_date)
  
  message("MAIN: Data preparation completed")
  
  # Step 5: Add transformation metadata and quality scores
  message("MAIN: Adding transformation metadata and quality scores...")
  
  transformed_reviews <- transformed_reviews %>%
    dplyr::mutate(
      etl_phase = "transformed",
      etl_transform_timestamp = Sys.time(),
      # Calculate final quality score based on completeness
      final_quality_score = dplyr::case_when(
        !is.na(review_date) & !is.na(product_id) & !is.na(rating) & 
          !is.na(review_text) & !is.na(product_line_id) & !is.na(platform_id) ~ 100,
        !is.na(review_date) & !is.na(product_id) & !is.na(rating) & 
          !is.na(product_line_id) & !is.na(platform_id) ~ 85,
        !is.na(product_id) & !is.na(rating) & !is.na(product_line_id) & 
          !is.na(platform_id) ~ 70,
        !is.na(product_id) & !is.na(product_line_id) & !is.na(platform_id) ~ 50,
        TRUE ~ 30
      ),
      # Calculate completeness score (including new fields)
      completeness_score = rowSums(!is.na(dplyr::select(., 
        platform_id, product_line_id, product_id, review_date, rating, review_text
      ))) / 6
    )
  
  message("MAIN: Metadata and quality scores added")
  
  # Step 6: Standardize field names for downstream processing
  message("MAIN: Standardizing field names for SCD Type 2 compatibility...")
  
  transformed_reviews <- transformed_reviews %>%
    dplyr::rename(
      # Standardize review fields
      review_title = title,          # title → review_title
      review_body = review_text      # review_text → review_body
      # Note: product_id, reviewer_id, and review_date already have correct names
    )
  
  message("MAIN: Field names standardized")
  
  # Write to transformed database
  target_table <- "df_amz_review___transformed"
  dbWriteTable(transformed_data, target_table, transformed_reviews, overwrite = TRUE)
  
  script_success <- TRUE
  message("MAIN: ETL06 Transform Phase completed successfully")
  message("MAIN: Transformed ", nrow(transformed_reviews), " reviews")
  
  # Show final statistics
  if (nrow(transformed_reviews) > 0) {
    avg_quality <- mean(transformed_reviews$final_quality_score, na.rm = TRUE)
    avg_completeness <- mean(transformed_reviews$completeness_score, na.rm = TRUE)
    message("MAIN: Average final quality score: ", round(avg_quality, 2), "%")
    message("MAIN: Average completeness: ", round(avg_completeness * 100, 2), "%")
  }

}, error = function(e) {
  main_error <<- e
  script_success <<- FALSE
  message("MAIN ERROR: ", e$message)
})

# ==============================================================================
# 3. TEST
# ==============================================================================

if (script_success) {
  tryCatch({
    message("TEST: Verifying ETL06 Transform Phase results...")

    # Check if transformed table exists and has data
    target_table <- "df_amz_review___transformed"
    
    if (target_table %in% dbListTables(transformed_data)) {
      # Check row count
      transformed_count <- dbGetQuery(transformed_data, paste0("SELECT COUNT(*) as count FROM ", target_table))$count

      if (transformed_count > 0) {
        test_passed <- TRUE
        message("TEST: Verification successful - ", transformed_count, " reviews transformed")
        
        # Show basic data structure
        structure_query <- paste0("SELECT * FROM ", target_table, " LIMIT 3")
        sample_data <- dbGetQuery(transformed_data, structure_query)
        message("TEST: Sample transformed data structure:")
        print(sample_data)
        
        # Check for key new columns
        key_cols <- c("platform_id", "product_line_id", "product_id", "product_line_name_chinese", "product_line_name_english")
        actual_cols <- names(sample_data)
        present_key_cols <- intersect(key_cols, actual_cols)
        
        if (length(present_key_cols) > 0) {
          message("TEST: Key columns present: ", paste(present_key_cols, collapse = ", "))
          
          # Check platform distribution
          if ("platform_id" %in% actual_cols) {
            platform_query <- paste0("SELECT platform_id, COUNT(*) as count FROM ", target_table, 
                                    " GROUP BY platform_id ORDER BY count DESC")
            platform_dist <- dbGetQuery(transformed_data, platform_query)
            message("TEST: Platform distribution:")
            print(platform_dist)
          }
          
          # Check product line distribution
          if ("product_line_id" %in% actual_cols) {
            pl_query <- paste0("SELECT product_line_id, COUNT(*) as count FROM ", target_table, 
                              " WHERE product_line_id IS NOT NULL GROUP BY product_line_id ORDER BY count DESC")
            pl_dist <- dbGetQuery(transformed_data, pl_query)
            message("TEST: Product line distribution:")
            print(head(pl_dist, 10))
            
            # Check unassigned product lines
            unassigned <- dbGetQuery(transformed_data, paste0(
              "SELECT COUNT(*) as unassigned FROM ", target_table, 
              " WHERE product_line_id IS NULL"
            ))$unassigned
            if (unassigned > 0) {
              message("TEST WARNING: ", unassigned, " reviews without product line assignment")
            }
          }
          
          # Check product_id distribution
          if ("product_id" %in% actual_cols) {
            product_query <- paste0("SELECT COUNT(DISTINCT product_id) as unique_products FROM ", target_table)
            unique_products <- dbGetQuery(transformed_data, product_query)$unique_products
            message("TEST: Unique products: ", unique_products)
          }
        } else {
          message("TEST WARNING: No key columns found")
        }
        
        # Check competitor flag with detailed statistics
        if ("included_competiter" %in% actual_cols) {
          competitor_query <- paste0(
            "SELECT included_competiter, COUNT(*) as count FROM ", target_table,
            " GROUP BY included_competiter"
          )
          competitor_stats <- dbGetQuery(transformed_data, competitor_query)
          
          # Calculate percentages
          total_competitor_count <- sum(competitor_stats$count)
          competitor_stats$percentage <- round(
            (competitor_stats$count / total_competitor_count) * 100, 2
          )
          
          message("TEST: Competitor flag distribution:")
          print(competitor_stats)
          
          # Display summary statistics
          competitor_test_count <- competitor_stats$count[
            competitor_stats$included_competiter == TRUE
          ]
          competitor_test_count <- ifelse(
            length(competitor_test_count) > 0, competitor_test_count, 0
          )
          
          message("TEST: Competitor flag summary:")
          message("TEST: - Total: ", total_competitor_count)
          competitor_pct <- round(
            (competitor_test_count / total_competitor_count) * 100, 2
          )
          message("TEST: - Competitor reviews: ", competitor_test_count, " (", 
                  competitor_pct, "%)")
          
          non_competitor_count <- total_competitor_count - competitor_test_count
          non_competitor_pct <- round(
            (non_competitor_count / total_competitor_count) * 100, 2
          )
          message("TEST: - Non-competitor reviews: ", non_competitor_count, " (", 
                  non_competitor_pct, "%)")
        } else {
          message("TEST WARNING: included_competiter column not found")
        }
        
        
        # Check quality metrics
        if ("final_quality_score" %in% actual_cols) {
          quality_stats <- dbGetQuery(transformed_data, paste0(
            "SELECT ",
            "MIN(final_quality_score) as min_quality, ",
            "AVG(final_quality_score) as avg_quality, ",
            "MAX(final_quality_score) as max_quality ",
            "FROM ", target_table
          ))
          message("TEST: Final quality scores - Min: ", quality_stats$min_quality, 
                  ", Avg: ", round(quality_stats$avg_quality, 2), 
                  ", Max: ", quality_stats$max_quality)
        }
        
        # Check ETL phase tracking
        if ("etl_phase" %in% actual_cols) {
          phase_check <- dbGetQuery(transformed_data, paste0(
            "SELECT COUNT(*) as transformed_count FROM ", target_table,
            " WHERE etl_phase = 'transformed'"
          ))$transformed_count
          message("TEST: Records marked as transformed: ", phase_check, " (should equal total)")
        }
        
      } else {
        test_passed <- FALSE
        message("TEST: Verification failed - no transformed reviews found")
      }
    } else {
      test_passed <- FALSE
      message("TEST: Verification failed - transformed table ", target_table, " not found")
    }

  }, error = function(e) {
    test_passed <<- FALSE
    message("TEST ERROR: ", e$message)
  })
} else {
  message("TEST: Skipped due to main script failure")
}

# ==============================================================================
# 4. DEINITIALIZE
# ==============================================================================

# Determine final status before tearing down
if (script_success && test_passed) {
  message("DEINITIALIZE: ETL06 Transform Phase completed successfully with verification")
  return_status <- TRUE
} else if (script_success && !test_passed) {
  message("DEINITIALIZE: ETL06 Transform Phase completed but verification failed")
  return_status <- FALSE
} else {
  message("DEINITIALIZE: ETL06 Transform Phase failed during execution")
  if (!is.null(main_error)) {
    message("DEINITIALIZE: Error details - ", main_error$message)
  }
  return_status <- FALSE
}

# Clean up database connections and disconnect
DBI::dbDisconnect(raw_data)
DBI::dbDisconnect(staged_data)
DBI::dbDisconnect(transformed_data)

# Clean up resources using autodeinit system
autodeinit()

message("DEINITIALIZE: ETL06 Transform Phase (amz_ETL06_2TR.R) completed")


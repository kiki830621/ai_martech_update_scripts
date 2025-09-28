#' @file S01_00.R
#' @sequence S01 product and Product Line Profiles
#' @step 00 Construct Product Line Profiles
#' @rule R119 Memory-Resident Parameters Rule
#' @rule R118 Lowercase Natural Key Rule
#' @description Load product line data into database and R memory

#' Load Product Line Profiles
#'
#' This function loads product line data from the CSV file, stores it in the database,
#' and then loads it into R memory for application use. It implements the requirements
#' of S01_00 and follows R119 Memory-Resident Parameters Rule.
#'
#' @param conn DBI connection. Database connection to use.
#' @param csv_path Character. Path to the product line CSV file.
#' @return Invisibly returns the product line data frame that was loaded into memory.
#'

# 1. INITIALIZE
# Source initialization script for update mode
autoinit()

if (!exists("raw_data") || !inherits(app_data, "DBIConnection")) {
  raw_data <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
  connection_created_raw <- TRUE
  message("Connected to raw_data database")
}
# Initialize error tracking
error_occurred <- FALSE

# 2. MAIN

load_product_lines(raw_data)

# 3. TEST

# 4. deinitialization
autodeinit()

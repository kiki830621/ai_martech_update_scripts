#####
#P07_D01_02
OPERATION_MODE <- "UPDATE_MODE"
source(file.path("../../../../global_scripts", "00_principles", "sc_initialization_update_mode.R"))
raw_data <- dbConnectDuckdb(db_path_list$raw_data)
processed_data <- dbConnectDuckdb(db_path_list$processed_data)
cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data)


# Log script start
message("Starting D01_02: Standardizing cleansed_data.df_amazon_sales to processed_data.df_amazon_sales")

# Check if the source table exists
if (dbExistsTable(cleansed_data, "df_amazon_sales")) {
  # Import cleansed data
  cleansed_amazon_sales <- tbl(cleansed_data, "df_amazon_sales") %>% collect()
  
  message(sprintf("Loaded %d rows from cleansed_data.df_amazon_sales", nrow(cleansed_amazon_sales)))
  #mutate_customer_id(cleansed_amazon_sales)
  # Standardization operations
  standardized_data <- cleansed_amazon_sales %>%
    # Enforce data type consistency
    mutate(
      # Ensure time is in proper datetime format
      time = as.POSIXct(time),
    ) 
  
  
  # Write the standardized data to the target table
  dbWriteTable(processed_data, "df_amazon_sales", standardized_data, overwrite = TRUE)
  
  message(sprintf("Successfully wrote %d rows to processed_data.df_amazon_sales", nrow(standardized_data)))
  #message("DNA analysis parameters saved to processed_data.dna_parameters")
} else {
  stop("Source table cleansed_data.df_amazon_sales does not exist")
}

# Clean up resources and close connections
source(file.path("../../../../global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))

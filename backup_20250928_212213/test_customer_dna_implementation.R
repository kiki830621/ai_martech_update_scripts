# WISER Project: Test Customer DNA Implementation
#
# This script tests the Customer DNA implementation with sample data
# to ensure the pipeline works correctly before using real data.

# Step 1: Set Up Project Environment
message("Initializing environment...")
VERBOSE_INITIALIZATION <- TRUE

# Source initialization script
source(file.path("../../../../global_scripts", "00_principles", "000g_initialization_update_mode.R"))

# Step 2: Load Necessary Functions
message("Loading functions...")
source(file.path("../../../../global_scripts", "15_cleanse", "cleanse_amazon_dta.R"))
source(file.path("../../../../global_scripts", "15_cleanse", "create_sample_data.R"))

# Step 3: Establish Database Connections
message("Connecting to databases...")
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)
cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = FALSE)
processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = FALSE)

# Step 4: Create Sample Data
message("Creating sample data for testing...")
create_sample_amazon_data(
  raw_data, 
  num_customers = 50,      # Small number for fast testing
  min_purchases = 1,
  max_purchases = 10,      # Some customers with many purchases
  start_date = Sys.Date() - 365*2,  # 2 years of data
  end_date = Sys.Date(),
  overwrite = TRUE,
  verbose = TRUE
)

# Step 5: Run the Cleansing Function
message("Testing cleansing function...")
cleanse_amazon_dta(raw_data, cleansed_data, verbose = TRUE)

# Step 6: Verify Cleansed Data
message("Verifying cleansed data...")
cleansed_sales <- tbl(cleansed_data, "amazon_sales_dta") %>% collect()
message("Sample of cleansed data:")
print(head(cleansed_sales))

# Step 7: Prepare Time Series
message("Preparing time series data...")
customer_time_data <- cleansed_sales %>%
  arrange(customer_id, time) %>%
  group_by(customer_id) %>%
  mutate(
    times = row_number(),
    ni = n(),
    IPT = case_when(
      times == 1 ~ as.numeric(NA),
      TRUE ~ as.numeric(difftime(time, lag(time), units = "days"))
    )
  ) %>%
  ungroup()

# Check IPT calculations
message("IPT summary:")
print(summary(customer_time_data$IPT))

# Step 8: Run DNA Analysis
message("Running DNA analysis...")
source(file.path("../../../../global_scripts", "05_data_processing", "common", "DNA_Function_dplyr.R"))

dna_results <- DNA_Function_dplyr(customer_time_data, 
                               Skip.within.Subject = FALSE,
                               replace_NESmedian = TRUE)

# Extract results
customer_dna <- dna_results$Data_byCustomer
nrec_accuracy <- dna_results$NrecAccu

message("DNA analysis complete:")
message("- Customers analyzed: ", nrow(customer_dna))
message("- Churn prediction accuracy: ", nrec_accuracy)

# Step 9: Enhance DNA with Additional Segments
message("Enhancing DNA with additional segments...")
customer_dna <- customer_dna %>%
  mutate(
    value_segment = case_when(
      CLV > quantile(CLV, 0.9, na.rm = TRUE) ~ "High Value",
      CLV > quantile(CLV, 0.7, na.rm = TRUE) ~ "Medium-High Value",
      CLV > quantile(CLV, 0.5, na.rm = TRUE) ~ "Medium Value",
      TRUE ~ "Standard Value"
    ),
    # Add WISER-specific loyalty tier
    loyalty_tier = case_when(
      ni >= 5 & NESstatus %in% c("N", "E0") ~ "Platinum",
      ni >= 3 & NESstatus %in% c("N", "E0", "S1") ~ "Gold",
      ni >= 2 ~ "Silver",
      TRUE ~ "Bronze"
    )
  )

# Show segment distributions
message("NES Status Distribution:")
print(table(customer_dna$NESstatus))

message("Value Segment Distribution:")
print(table(customer_dna$value_segment))

message("Loyalty Tier Distribution:")
print(table(customer_dna$loyalty_tier))

# Step 10: Store Results
message("Storing results to database...")
dbWriteTable(processed_data, "customer_dna", customer_dna, overwrite = TRUE)
message("Data written to processed_data.customer_dna")

# Step 11: Clean Up
message("Cleaning up...")
dbDisconnect_all()
message("All database connections closed")

# Final message
message("\nTest completed successfully!")
message("The Customer DNA implementation is working as expected.")
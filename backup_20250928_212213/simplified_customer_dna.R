# WISER Project: Simplified Customer DNA Implementation
#
# This script implements a simplified version of the customer DNA analysis
# for the WISER project using sample data.

# Step 1: Set Up Project Environment
message("Initializing environment...")
VERBOSE_INITIALIZATION <- TRUE

# Source initialization script
source(file.path("../../../../global_scripts", "00_principles", "000g_initialization_update_mode.R"))

# Step 2: Establish Database Connections
message("Connecting to databases...")
cleansed_data <- dbConnectDuckdb(db_path_list$cleansed_data, read_only = FALSE)
processed_data <- dbConnectDuckdb(db_path_list$processed_data, read_only = FALSE)

# Step 3: Generate Sample Data
message("Creating sample data for analysis...")
source(file.path("../../../../global_scripts", "15_cleanse", "create_sample_data.R"))
create_sample_amazon_data(
  cleansed_data, 
  num_customers = 100, 
  min_purchases = 1,
  max_purchases = 8,
  start_date = Sys.Date() - 365, 
  end_date = Sys.Date(),
  overwrite = TRUE,
  verbose = TRUE
)

# Load the sample data
message("Loading sample data...")
sales_data <- tbl(cleansed_data, "amazon_sales_dta") %>% collect()

# Check data
message("Sample data summary:")
cat("- Number of transactions:", nrow(sales_data), "\n")
cat("- Number of customers:", length(unique(sales_data$customer_id)), "\n")
cat("- Date range:", min(sales_data$time), "to", max(sales_data$time), "\n")

# Step 4: Prepare Customer Time Series Data
message("Creating time series with IPT...")
customer_time_data <- sales_data %>%
  # Make sure lineproduct_price is available
  mutate(total = lineproduct_price * quantity) %>%
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

# Step 5: Simplified DNA Analysis
message("Performing simplified DNA analysis...")

# Current time for recency calculations
time_now <- max(customer_time_data$time)

# Calculate customer metrics
customer_dna <- customer_time_data %>%
  # Get last purchase for each customer
  group_by(customer_id) %>%
  mutate(last_purchase = max(time)) %>%
  
  # Calculate basic metrics
  summarize(
    # Recency - days since last purchase
    recency = as.numeric(difftime(time_now, max(time), units = "days")),
    
    # Frequency - number of purchases
    frequency = n(),
    
    # Monetary - average purchase value
    monetary = mean(total, na.rm = TRUE),
    
    # Monetary total - total spent
    total_spent = sum(total, na.rm = TRUE),
    
    # IPT - mean inter-purchase time
    IPT_mean = mean(IPT, na.rm = TRUE),
    
    # Date of first purchase
    first_purchase = min(time),
    
    # Date of last purchase
    last_purchase = max(time)
  ) %>%
  ungroup() %>%
  
  # Calculate duration as customer
  mutate(
    customer_tenure = as.numeric(difftime(time_now, first_purchase, units = "days")),
    
    # Add NES status (New, Established, Sleeping)
    NES_ratio = recency / IPT_mean,
    
    NESstatus = case_when(
      is.na(NES_ratio) | frequency == 1 ~ "N",  # New customers or single purchase
      NES_ratio <= 1 ~ "E0",                   # Active/Established customers
      NES_ratio <= 2 ~ "S1",                   # Recently sleeping
      NES_ratio <= 3 ~ "S2",                   # Moderately sleeping
      TRUE ~ "S3"                              # Deeply sleeping
    ),
    
    # Simplified CLV (Customer Lifetime Value)
    CLV = total_spent * (1 + (frequency > 1) * 0.2) * (1 - (recency/(365*2)) * 0.5),
    
    # Purchase consistency
    purchase_consistency = if_else(frequency > 1, 
                                  1 - (sd(IPT, na.rm = TRUE) / IPT_mean), 
                                  NA_real_),
    
    # Add value segment
    value_segment = ntile(CLV, 4)
  ) %>%
  mutate(
    value_segment = case_when(
      value_segment == 4 ~ "High Value",
      value_segment == 3 ~ "Medium-High Value",
      value_segment == 2 ~ "Medium Value",
      value_segment == 1 ~ "Standard Value"
    ),
    
    # Add loyalty tier
    loyalty_tier = case_when(
      frequency >= 5 & NESstatus %in% c("N", "E0") ~ "Platinum",
      frequency >= 3 & NESstatus %in% c("N", "E0", "S1") ~ "Gold",
      frequency >= 2 ~ "Silver",
      TRUE ~ "Bronze"
    ),
    
    # Convert factors for ordering
    NESstatus = factor(NESstatus, levels = c("N", "E0", "S1", "S2", "S3")),
    loyalty_tier = factor(loyalty_tier, levels = c("Platinum", "Gold", "Silver", "Bronze"))
  )

# Step 6: Analyze Results
message("Analyzing DNA results...")

# Status distribution
nes_distribution <- customer_dna %>%
  group_by(NESstatus) %>%
  summarize(
    count = n(),
    percentage = n() / nrow(customer_dna) * 100,
    avg_clv = mean(CLV, na.rm = TRUE),
    avg_monetary = mean(monetary, na.rm = TRUE),
    avg_ipt = mean(IPT_mean, na.rm = TRUE)
  )

message("NES Status Distribution:")
print(nes_distribution)

message("Loyalty Tier Distribution:")
print(table(customer_dna$loyalty_tier))

message("Value Segment Distribution:")
print(table(customer_dna$value_segment))

# Step 7: Store the Results
message("Storing DNA results...")

# Save to database
dbWriteTable(processed_data, "customer_dna", customer_dna, overwrite = TRUE)
message("Saved to database table: customer_dna")

# Create indexes for performance
dbExecute(processed_data, "CREATE INDEX IF NOT EXISTS idx_customer_id ON customer_dna (customer_id)")
dbExecute(processed_data, "CREATE INDEX IF NOT EXISTS idx_nesstatus ON customer_dna (NESstatus)")
dbExecute(processed_data, "CREATE INDEX IF NOT EXISTS idx_loyalty_tier ON customer_dna (loyalty_tier)")
dbExecute(processed_data, "CREATE INDEX IF NOT EXISTS idx_value_segment ON customer_dna (value_segment)")
message("Created database indexes")

# Save to RDS for faster loading in Shiny
if (!dir.exists(file.path("data", "processed"))) {
  dir.create(file.path("data", "processed"), recursive = TRUE)
}
output_path <- file.path("data", "processed", "wiser_customer_dna.rds")
saveRDS(customer_dna, output_path)
message("Saved customer DNA to: ", output_path)

# Step 8: Generate Basic Visualizations
message("Generating visualizations...")

# Ensure output directory exists
if (!dir.exists("output")) {
  dir.create("output")
}

# Plot status distribution
nes_plot <- ggplot(nes_distribution, aes(x = NESstatus, y = percentage, fill = NESstatus)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = sprintf("%.1f%%", percentage)), vjust = -0.5) +
  labs(title = "WISER: Customer Status Distribution",
       x = "Customer Status",
       y = "Percentage of Customers") +
  theme_minimal() +
  scale_fill_brewer(palette = "Blues")

# Plot value and loyalty distribution
loyalty_value_plot <- ggplot(customer_dna, aes(x = loyalty_tier, fill = value_segment)) +
  geom_bar(position = "dodge") +
  labs(title = "WISER: Loyalty Tier by Value Segment",
       x = "Loyalty Tier",
       y = "Number of Customers",
       fill = "Value Segment") +
  theme_minimal() +
  scale_fill_brewer(palette = "Greens")

# Save plots
ggsave(file.path("output", "wiser_nes_distribution.png"), nes_plot, width = 8, height = 6)
ggsave(file.path("output", "wiser_loyalty_value.png"), loyalty_value_plot, width = 10, height = 6)
message("Saved visualization plots to output directory")

# Save a summary report
report_path <- file.path("output", "wiser_dna_summary.txt")
sink(report_path)
cat("WISER CUSTOMER DNA ANALYSIS SUMMARY\n")
cat("===================================\n\n")
cat("Analysis date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")
cat("CUSTOMER BASE\n")
cat("Customers analyzed:", nrow(customer_dna), "\n")
cat("Transactions processed:", nrow(customer_time_data), "\n")
cat("Date range:", min(sales_data$time), "to", max(sales_data$time), "\n\n")
cat("NES STATUS DISTRIBUTION\n")
print(nes_distribution)
cat("\nLOYALTY TIER DISTRIBUTION\n")
print(as.data.frame(table(customer_dna$loyalty_tier)))
cat("\nVALUE SEGMENT DISTRIBUTION\n")
print(table(customer_dna$value_segment))
sink()
message("Saved summary report to: ", report_path)

# Step 9: Clean Up
message("Cleaning up...")

# Disconnect from databases
dbDisconnect_all()
message("Disconnected from all databases")

# Final message
message("\nWISER Customer DNA analysis complete!")
message("Results available in:")
message("- Database: processed_data, table: customer_dna")
message("- File: ", output_path)
message("- Visualizations: output/wiser_nes_distribution.png, output/wiser_loyalty_value.png")
message("- Summary: output/wiser_dna_summary.txt")
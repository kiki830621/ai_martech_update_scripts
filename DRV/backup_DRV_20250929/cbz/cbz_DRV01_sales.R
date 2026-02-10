#####
#P07_D01_05
OPERATION_MODE <- "UPDATE_MODE"
source(file.path("../../../../global_scripts", "00_principles", "sc_initialization_update_mode.R"))
processed_data <- dbConnectDuckdb(db_path_list$processed_data)
app_data <- dbConnectDuckdb(db_path_list$app_data)

df_amazon_sales_by_customer <- tbl(processed_data, "df_amazon_sales_by_customer") %>% collect()
df_amazon_sales.by_customer.by_date <- tbl(processed_data, "df_amazon_sales_by_customer_by_date") %>% collect()
    
# Run DNA analysis with real data
message("Starting customer DNA analysis...")
dna_results <- analysis_dna(df_amazon_sales_by_customer, df_amazon_sales.by_customer.by_date)

# Output results
# message("Customer DNA analysis completed")
# message("Number of customers analyzed: ", nrow(dna_results$data_by_customer))
# message("Churn prediction accuracy: ", dna_results$nrec_accu$nrec_accu)

dbWriteTable(
app_data,
"df_dna_by_customer",
dna_results$data_by_customer %>% select(-row_names) %>% mutate(platform_id = 2L),
append=TRUE,
temporary = FALSE
)

tbl(app_data, "df_dna_by_customer")%>% head(10)


source(file.path("../../../../global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))


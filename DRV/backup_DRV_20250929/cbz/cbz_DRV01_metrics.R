#####
# CONSUMES: df_amazon_sales, df_amazon_sales_by_customer, df_amazon_sales_by_customer_by_date, df_dna_by_customer
# PRODUCES: df_dna_by_customer
# DEPENDS_ON_ETL: none
# DEPENDS_ON_DRV: none
#####

#####
#P07_D01_06
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
OPERATION_MODE <- "UPDATE_MODE"
source(file.path("../../../../global_scripts", "00_principles", "sc_initialization_update_mode.R"))
processed_data <- dbConnectDuckdb(db_path_list$processed_data)
app_data <- dbConnectDuckdb(db_path_list$app_data)

df_amazon_sales_by_customer <- tbl2(processed_data, "df_amazon_sales_by_customer") %>% collect()
df_amazon_sales.by_customer.by_date <- tbl2(processed_data, "df_amazon_sales_by_customer_by_date") %>% collect()
    
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

tbl2(app_data, "df_dna_by_customer")%>% head(10)


source(file.path("../../../../global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))


# Quick diagnostic script to check raw data columns
library(DBI)
library(duckdb)

# Database path (hardcoded for diagnostic)
db_path <- "/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/MAMBA/data/raw_data.duckdb"

con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)

# Check customers
cat("=== CBZ CUSTOMERS RAW DATA ===\n")
df_cust <- dbGetQuery(con, "SELECT * FROM df_cbz_customers___raw LIMIT 3")
cat("Columns (", ncol(df_cust), "):\n", paste(names(df_cust), collapse = ", "), "\n\n")
print(head(df_cust[, 1:min(8, ncol(df_cust))]))

# Check products
cat("\n\n=== CBZ PRODUCTS RAW DATA ===\n")
df_prod <- dbGetQuery(con, "SELECT * FROM df_cbz_products___raw LIMIT 3")
cat("Columns (", ncol(df_prod), "):\n", paste(names(df_prod), collapse = ", "), "\n\n")
print(head(df_prod[, 1:min(8, ncol(df_prod))]))

# Check orders
cat("\n\n=== CBZ ORDERS RAW DATA ===\n")
df_ord <- dbGetQuery(con, "SELECT * FROM df_cbz_orders___raw LIMIT 3")
cat("Columns (", ncol(df_ord), "):\n", paste(names(df_ord), collapse = ", "), "\n\n")
print(head(df_ord[, 1:min(8, ncol(df_ord))]))

dbDisconnect(con, shutdown = TRUE)

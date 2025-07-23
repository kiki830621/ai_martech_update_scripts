library(DBI)
library(duckdb)

# Connect to database
db_path <- "/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l3_enterprise/WISER/data/local_data/scd_type2/comment_property_rating_results.duckdb"
con <- dbConnect(duckdb(), db_path, read_only = TRUE)

# Check tables
tables <- dbListTables(con)
cat("Tables:\n")
print(tables)

# Check each table
for (table in tables) {
  count <- dbGetQuery(con, paste0("SELECT COUNT(*) FROM ", table))[1,1]
  cat("\nTable:", table, "- Records:", count, "\n")
  
  if (count > 0) {
    # Get column names
    cols <- dbGetQuery(con, paste0("PRAGMA table_info(", table, ")"))$name
    cat("Columns:", paste(cols, collapse = ", "), "\n")
    
    # Sample data
    sample <- dbGetQuery(con, paste0("SELECT * FROM ", table, " LIMIT 1"))
    cat("Sample data structure:\n")
    print(str(sample))
  }
}

dbDisconnect(con)
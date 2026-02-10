# Script to update all MAMBA eBay ETL scripts with automatic SSH tunnel connection
# This updates order_details_0IM to use the same pattern as orders_0IM

library(stringr)

# List of ETL scripts to update
etl_scripts <- c(
  "eby_ETL_order_details_0IM___MAMBA.R"
  # Add more scripts here if needed
)

for (script_name in etl_scripts) {
  script_path <- file.path("scripts/update_scripts", script_name)
  
  if (!file.exists(script_path)) {
    cat(sprintf("⚠️ Script not found: %s\n", script_name))
    next
  }
  
  cat(sprintf("Updating %s...\n", script_name))
  
  # Read the script
  script_content <- readLines(script_path)
  
  # Find and replace the SSH tunnel setup section
  # Look for patterns like "Setup SSH Tunnel" or similar
  
  # Pattern 1: Find the tunnel setup section
  tunnel_start <- grep("Setup SSH Tunnel|Setting up SSH tunnel", script_content)
  sql_connect <- grep("Connect to SQL Server|Connecting to MAMBA", script_content)
  
  if (length(tunnel_start) > 0 && length(sql_connect) > 0) {
    # Find the range to replace
    start_line <- tunnel_start[1] - 2  # Include comment line
    end_line <- sql_connect[1] + 10  # Include connection code
    
    # Create replacement code
    replacement <- c(
      "  # ------------------------------------------------------------------------------",
      "  # 2.1: Setup SSH Tunnel and Connect to SQL Server",
      "  # ------------------------------------------------------------------------------",
      "  message(\"MAIN: Connecting to MAMBA SQL Server...\")",
      "  ",
      "  # Source the auto-tunnel function",
      "  source(\"scripts/global_scripts/02_db_utils/fn_ensure_mamba_tunnel.R\")",
      "  ",
      "  # Connect with automatic tunnel establishment",
      "  sql_conn <- fn_connect_mamba_sql(auto_tunnel = TRUE)",
      "  ",
      "  message(\"MAIN: Connected to SQL Server successfully\")"
    )
    
    # Replace the section
    new_content <- c(
      script_content[1:(start_line-1)],
      replacement,
      script_content[(end_line+1):length(script_content)]
    )
    
    # Also update disconnect section
    new_content <- gsub(
      "system\\(\"pkill -f 'ssh.*1433.*'\".*\\)",
      "# Keep tunnel alive for other scripts",
      new_content
    )
    
    # Write back
    writeLines(new_content, script_path)
    cat(sprintf("✅ Updated %s\n", script_name))
  } else {
    cat(sprintf("⚠️ Could not find tunnel setup section in %s\n", script_name))
  }
}

cat("\n✅ Update complete!\n")
cat("All ETL scripts now use automatic SSH tunnel establishment.\n")
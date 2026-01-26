#####
# DERIVATION: D01_07 Pre-compute DNA Distribution Plot Data
# GROUP: D01 (Customer DNA Analysis)
# SEQUENCE: 07
# CORE_FUNCTION: global_scripts/16_derivations/fn_D01_07_core.R
# CONSUMES: app_data.df_dna_by_customer
# PRODUCES: app_data.df_dna_plot_data,
#           app_data.df_dna_category_counts,
#           app_data.df_dna_summary_stats
# DEPENDS_ON_DRV: D01_05 (df_dna_by_customer must exist)
# PRINCIPLE: MP055 (ALL Category Special Treatment),
#            MP064 (ETL-Derivation Separation),
#            DEV_R038 (Core Function + Platform Wrapper Pattern)
#####

# ===========================================================================
# D01_07: Pre-compute DNA Distribution Plot Data
# ===========================================================================
# This derivation moves ECDF and histogram computation from app runtime to
# ETL time, achieving near-instant chart rendering in DNA Distribution Analysis.
#
# PERFORMANCE IMPACT:
# - Before: App loads 124k rows, computes ECDF in R (~2-5 seconds)
# - After: App reads ~2000 pre-computed rows (<100ms)
# ===========================================================================

# PART 1: INITIALIZE
source("scripts/global_scripts/22_initializations/sc_Rprofile.R")
autoinit()

# Source the core function
source("scripts/global_scripts/16_derivations/fn_D01_07_core.R")

# PART 2: MAIN

# Read enabled platforms from app_config.yaml (configuration-driven: MP142)
config <- yaml::read_yaml("app_config.yaml")
enabled_platforms <- config$pipeline$enabled_platforms

if (is.null(enabled_platforms) || length(enabled_platforms) == 0) {
  enabled_platforms <- c("cbz")  # Default fallback
  message("Warning: No enabled_platforms found in config, using default: cbz")
}

message("════════════════════════════════════════════════════════════════════")
message(sprintf("D01_07 Wrapper: Starting for platforms: %s",
                paste(enabled_platforms, collapse = ", ")))
message("════════════════════════════════════════════════════════════════════")

# Execute the core function for all platforms
results <- run_D01_07_all_platforms(
  enabled_platforms = enabled_platforms,
  config = config
)

# PART 3: SUMMARIZE
success_count <- sum(vapply(results, function(x) isTRUE(x$success), logical(1)))
total_count <- length(results)

message("")
message("════════════════════════════════════════════════════════════════════")
message(sprintf("D01_07 Wrapper: Completed %d/%d platforms successfully",
                success_count, total_count))
message("════════════════════════════════════════════════════════════════════")

# Return results for pipeline tracking
invisible(results)

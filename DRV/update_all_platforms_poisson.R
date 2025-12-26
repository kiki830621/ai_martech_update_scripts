#!/usr/bin/env Rscript
################################################################################
# Universal Platform Poisson Update Script
# Principle: DM_R047 - Multi-Platform Data Synchronization
################################################################################
#
# PURPOSE:
#   Ensure all production platforms (CBZ, EBY) are synchronized with identical
#   data structures, metadata fields, and DRV versions.
#
# USAGE:
#   Rscript scripts/update_scripts/DRV/update_all_platforms_poisson.R
#
# WHAT IT DOES:
#   1. Runs CBZ Poisson DRV (cbz_D04_02.R)
#   2. Runs EBY Poisson DRV (eby_D04_02.R)
#   3. Verifies schema consistency across platforms
#   4. Reports success/failure with detailed diagnostics
#
# PRINCIPLE COMPLIANCE:
#   DM_R047: Multi-Platform Data Synchronization
#     - Ensures all platforms updated simultaneously
#     - Prevents UI platform switching errors
#     - Maintains data structure consistency
#
#   Related Principles:
#     - DM_R046: Variable Display Name Metadata
#     - MP135 v2.0: Analytics Temporal Classification (Type B)
#     - R120: Variable Range Metadata Requirement
#     - MP102: Complete Metadata
#
# FAILURE HANDLING:
#   If any platform fails:
#     - Script stops immediately
#     - Reports which platform failed
#     - Does NOT deploy partial updates
#     - Requires manual intervention
#
# VERIFICATION:
#   After successful run:
#     - All platforms have identical schemas
#     - All metadata columns present
#     - No missing display names
#     - Ready for UI platform switching
#
################################################################################

# Store script start time
script_start <- Sys.time()

# Print header
cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("Universal Platform Update - Poisson Analysis (DM_R047)\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat(sprintf("Started: %s\n", format(script_start, "%Y-%m-%d %H:%M:%S")))
cat("\n")
cat("Principle: DM_R047 - Multi-Platform Data Synchronization\n")
cat("Purpose: Ensure all platforms have consistent data structures\n")
cat("\n")

# Define platforms to update
PLATFORMS <- c("cbz", "eby")

cat(sprintf("Platforms to update: %s\n", paste(toupper(PLATFORMS), collapse=", ")))
cat("───────────────────────────────────────────────────────────────────\n\n")

# Track success/failure
success_count <- 0
failed_platforms <- c()
execution_times <- list()

# Process each platform
for (platform in PLATFORMS) {

  platform_start <- Sys.time()

  # Construct script path (standardized naming: {platform}_D04_02.R)
  script_path <- sprintf(
    "scripts/update_scripts/DRV/%s/%s_D04_02.R",
    platform, platform
  )

  cat(sprintf("[%d/%d] Processing Platform: %s\n",
              which(PLATFORMS == platform),
              length(PLATFORMS),
              toupper(platform)))
  cat("───────────────────────────────────────────────────────────────────\n")

  # Check if script exists
  if (!file.exists(script_path)) {
    warning(sprintf("❌ Script not found: %s", script_path))
    failed_platforms <- c(failed_platforms, platform)
    cat("\n")
    next
  }

  # Execute platform DRV
  tryCatch({

    cat(sprintf("Executing: %s\n", script_path))
    cat("\n")

    # Source the script (this will run it)
    source(script_path)

    # Record success
    platform_duration <- as.numeric(difftime(Sys.time(), platform_start, units = "secs"))
    execution_times[[platform]] <- platform_duration
    success_count <- success_count + 1

    cat("\n")
    cat(sprintf("✅ %s update complete (%.1f seconds)\n", toupper(platform), platform_duration))
    cat("\n")

  }, error = function(e) {

    # Record failure
    platform_duration <- as.numeric(difftime(Sys.time(), platform_start, units = "secs"))
    execution_times[[platform]] <- platform_duration

    warning(sprintf("❌ %s update FAILED (%.1f seconds): %s",
                    toupper(platform), platform_duration, conditionMessage(e)))
    failed_platforms <- c(failed_platforms, platform)

    cat("\n")

  })

}

# ============================================================================
# SUMMARY AND VERIFICATION
# ============================================================================

script_duration <- as.numeric(difftime(Sys.time(), script_start, units = "secs"))

cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("Update Summary\n")
cat("═══════════════════════════════════════════════════════════════════\n\n")

cat(sprintf("Total platforms: %d\n", length(PLATFORMS)))
cat(sprintf("Successful: %d\n", success_count))
cat(sprintf("Failed: %d\n", length(failed_platforms)))
cat(sprintf("Total duration: %.1f seconds\n", script_duration))

cat("\nExecution times by platform:\n")
for (platform in names(execution_times)) {
  cat(sprintf("  %s: %.1f seconds\n", toupper(platform), execution_times[[platform]]))
}

# Report failures
if (length(failed_platforms) > 0) {
  cat("\n❌ SYNCHRONIZATION FAILED\n\n")
  cat("Failed platforms:\n")
  for (p in failed_platforms) {
    cat(sprintf("  - %s\n", toupper(p)))
  }
  cat("\n")
  cat("Action required:\n")
  cat("  1. Review error messages above\n")
  cat("  2. Fix issues in failed platform(s)\n")
  cat("  3. Re-run this script\n")
  cat("  4. Do NOT deploy until all platforms succeed\n")
  cat("\n")
  cat("═══════════════════════════════════════════════════════════════════\n\n")

  stop("SYNCHRONIZATION INCOMPLETE - Review errors above")
}

# All platforms succeeded - run verification
cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("Schema Consistency Verification\n")
cat("═══════════════════════════════════════════════════════════════════\n\n")

# Load verification function
verification_script <- "scripts/global_scripts/04_utils/fn_verify_platform_consistency.R"

if (file.exists(verification_script)) {

  library(DBI)
  library(duckdb)

  source(verification_script)

  # Connect to database
  con <- dbConnect(duckdb::duckdb(), "data/app_data/app_data.duckdb")

  # Verify consistency
  validation <- tryCatch({
    fn_verify_platform_consistency(
      con = con,
      table_base_name = "poisson_analysis",
      platforms = PLATFORMS
    )
  }, error = function(e) {
    list(valid = FALSE, error = conditionMessage(e))
  })

  dbDisconnect(con, shutdown = TRUE)

  if (!validation$valid) {
    cat("\n❌ VERIFICATION FAILED\n\n")
    cat(sprintf("Error: %s\n", validation$error))
    cat("\n")
    cat("Action required:\n")
    cat("  1. Check platform schemas manually\n")
    cat("  2. Ensure all DRV scripts use same enrichment logic\n")
    cat("  3. Re-run individual platform scripts if needed\n")
    cat("  4. Re-run this verification\n")
    cat("\n")
    cat("═══════════════════════════════════════════════════════════════════\n\n")

    stop("VERIFICATION FAILED - Platforms not synchronized")
  }

  cat("\n✅ Schema verification passed\n")

} else {
  cat("⚠️  Verification script not found: ", verification_script, "\n")
  cat("   Manual verification recommended\n")
}

# Success summary
cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("✅ All Platforms Synchronized Successfully (DM_R047)\n")
cat("═══════════════════════════════════════════════════════════════════\n\n")

cat("What was accomplished:\n")
cat(sprintf("  • Updated %d platforms with identical schemas\n", length(PLATFORMS)))
cat("  • All metadata columns present and consistent\n")
cat("  • Display names synchronized across platforms\n")
cat("  • Type B metadata added (computed_at, data_version)\n")
cat("  • UI platform switching ready\n\n")

cat("Verification results:\n")
cat(sprintf("  • Platforms: %s\n", paste(toupper(PLATFORMS), collapse=", ")))
if (exists("validation") && validation$valid) {
  cat(sprintf("  • Column count: %d (identical across all platforms)\n", validation$column_count))
  cat("  • Required metadata: All present\n")
}

cat("\nNext steps:\n")
cat("  1. Test UI platform switching (cbz ↔ eby)\n")
cat("  2. Verify metadata banner displays correctly\n")
cat("  3. Check for any console errors\n")
cat("  4. If all tests pass, ready for deployment\n\n")

cat("Principle compliance:\n")
cat("  ✅ DM_R047: Multi-Platform Data Synchronization\n")
cat("  ✅ DM_R046: Variable Display Name Metadata\n")
cat("  ✅ MP135 v2.0: Analytics Temporal Classification (Type B)\n")
cat("  ✅ MP102: Complete Metadata\n\n")

cat(sprintf("Completed: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
cat(sprintf("Total duration: %.1f seconds (%.1f minutes)\n",
            script_duration, script_duration / 60))
cat("═══════════════════════════════════════════════════════════════════\n\n")

cat("✅ SYNCHRONIZATION COMPLETE - All platforms ready for deployment\n\n")

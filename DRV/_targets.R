# _targets.R
# MAMBA DRV Weekly Update Pipeline - Phase 1
################################################################################
# Principle Compliance:
#   - DEV_R016: Evolution Over Replacement (wrap existing scripts)
#   - MP064: ETL-Derivation Separation (DRV independent of ETL)
#   - DM_R047: Multi-Platform Data Synchronization
#   - MP135: Analytics Temporal Classification (Type B steady-state)
################################################################################
#
# Usage:
#   make run              # Execute pipeline
#   make status           # Check status
#   make vis              # Visualize pipeline DAG
#
# Or directly:
#   Rscript -e "targets::tar_make()"
#
################################################################################

library(targets)

# Load wrapper functions (DM_R035: utility functions in global_scripts)
source("../../global_scripts/04_utils/fn_run_drv_script.R")

# Define pipeline
list(
  # =========================================================================
  # Primary: Multi-Platform Poisson Update (CBZ + EBY)
  # =========================================================================
  # Uses existing update_all_platforms_poisson.R which:
  # - Executes cbz_D04_02.R (Poisson analysis)
  # - Executes eby_D04_02.R (Poisson analysis)
  # - Verifies schema consistency across platforms
  # - Follows DM_R047 principle
  tar_target(
    all_platforms_poisson,
    run_drv_script("update_all_platforms_poisson.R"),
    format = "file"
  ),

  # =========================================================================
  # Metadata Enrichment (depends on poisson completion)
  # =========================================================================
  tar_target(
    cbz_metadata,
    run_drv_script("cbz/cbz_D04_03.R"),
    format = "file"
  ),

  # =========================================================================
  # Precision Pipeline (independent) - DM_R041: standardized to all/
  # =========================================================================
  tar_target(
    precision_poisson,
    run_drv_script("all/all_D04_08.R"),
    format = "file"
  ),

  # =========================================================================
  # MP165 Tier 1: Dashboard-Presence Lite Check (NULL output detection)
  # Spectra change: dashboard-presence-verification (issue #599)
  #
  # Final pipeline gate. Depends on all 5NM-layer derivation outputs;
  # launches headless Shiny in SHINY_DEBUG_MODE=TRUE, walks default
  # nav, and stop()s on any [DEBUG_MODE_NULL_OUTPUT] marker. Severity-
  # agnostic (Tier 1 only checks NULL); per-element contracts run in
  # Tier 2 deploy gate (23_deployment/dashboard_presence_gate.R).
  #
  # Runtime budget: target ~5s, hard ceiling 30s per company.
  # =========================================================================
  tar_target(
    dashboard_smoke_lite,
    {
      # Source the helper (lazy: avoids loading shinytest2 unless needed)
      smoke_path <- file.path(
        "..", "..", "global_scripts", "98_test", "e2e", "run_smoke_lite.R"
      )
      if (!file.exists(smoke_path)) {
        smoke_path <- file.path(
          "scripts", "global_scripts", "98_test", "e2e", "run_smoke_lite.R"
        )
      }
      source(smoke_path)

      # Determine company from app_config or env
      cfg_path <- file.path("..", "..", "app_config.yaml")
      company <- if (file.exists(cfg_path)) {
        cfg <- yaml::read_yaml(cfg_path)
        cfg$app_info$company %||% cfg$app_info$name %||% "UNKNOWN_COMPANY"
      } else {
        Sys.getenv("MP165_COMPANY", unset = "UNKNOWN_COMPANY")
      }

      # Anchor app_dir to project root (one level up from update_scripts/)
      app_dir <- normalizePath(file.path("..", ".."), mustWork = FALSE)

      # Force re-run when any upstream derivation changes
      .upstream <- c(
        all_platforms_poisson, cbz_metadata, precision_poisson
      )

      run_dashboard_smoke_lite_check(
        company = company,
        app_dir = app_dir,
        timeout_seconds = 30
      )

      # Return a sentinel string so targets caches the success
      paste0("dashboard_smoke_lite_pass_", Sys.Date())
    }
  )
)

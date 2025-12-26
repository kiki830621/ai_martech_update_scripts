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
  )
)

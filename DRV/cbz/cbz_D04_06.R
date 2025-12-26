#!/usr/bin/env Rscript
#####
# DERIVATION: CBZ Visualization Components (Wrapper)
# VERSION: 1.0
# PLATFORM: cbz
# GROUP: D04
# SEQUENCE: 06
# PURPOSE: Entry point for visualization prep - delegates to cbz_D04_02.R
# CONSUMES: (via cbz_D04_02.R)
# PRODUCES: poissonTimeSeries, poissonDistribution, poissonDashboard data
# PRINCIPLE: DM_R044, MP064
# NOTE: This is a WRAPPER script - autoinit/autodeinit handled by main script
#####
#cbz_D04_06

# Source the main comprehensive script
# Main script handles: autoinit, 5-part structure, autodeinit
source("scripts/update_scripts/DRV/cbz/cbz_D04_02.R")

# Note: The main script (cbz_D04_02.R) handles the complete D04 pipeline:
# - D04_04: Time series expansion with zero-filling
# - D04_05: Poisson parameter calculation
# - D04_06: Visualization data preparation

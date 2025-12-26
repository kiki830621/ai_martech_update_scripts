#!/usr/bin/env Rscript
#####
# DERIVATION: Cross-Platform Time Series Expansion (Wrapper)
# VERSION: 1.0
# PLATFORM: all
# GROUP: D04
# SEQUENCE: 04
# PURPOSE: Entry point for time series expansion - delegates to all_D04_07.R
# CONSUMES: (via all_D04_07.R)
# PRODUCES: (via all_D04_07.R)
# PRINCIPLE: DM_R044, MP064
# NOTE: This is a WRAPPER script - autoinit/autodeinit handled by main script
#####
#all_D04_04

# Source the time series script
# Main script handles: autoinit, 5-part structure, autodeinit
source("scripts/update_scripts/DRV/all/all_D04_07.R")

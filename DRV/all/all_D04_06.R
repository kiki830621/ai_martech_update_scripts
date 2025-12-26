#!/usr/bin/env Rscript
#####
# DERIVATION: Cross-Platform Feature Preparation (Wrapper)
# VERSION: 1.0
# PLATFORM: all
# GROUP: D04
# SEQUENCE: 06
# PURPOSE: Entry point for feature preparation - delegates to all_D04_09.R
# CONSUMES: (via all_D04_09.R)
# PRODUCES: Feature data for visualization components
# PRINCIPLE: DM_R044, MP064
# NOTE: This is a WRAPPER script - autoinit/autodeinit handled by main script
#####
#all_D04_06

# Source the feature preparation script
# Main script handles: autoinit, 5-part structure, autodeinit
source("scripts/update_scripts/DRV/all/all_D04_09.R")

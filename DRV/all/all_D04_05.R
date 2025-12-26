#!/usr/bin/env Rscript
#####
# DERIVATION: Cross-Platform Poisson Calculation (Wrapper)
# VERSION: 1.0
# PLATFORM: all
# GROUP: D04
# SEQUENCE: 05
# PURPOSE: Entry point for Poisson calculation - delegates to all_D04_08.R
# CONSUMES: (via all_D04_08.R)
# PRODUCES: df_precision_poisson_analysis
# PRINCIPLE: DM_R044, MP064
# NOTE: This is a WRAPPER script - autoinit/autodeinit handled by main script
#####
#all_D04_05

# Source the Poisson analysis script
# Main script handles: autoinit, 5-part structure, autodeinit
source("scripts/update_scripts/DRV/all/all_D04_08.R")

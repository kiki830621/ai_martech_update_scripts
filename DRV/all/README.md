# DRV All (Precision Marketing)

This directory contains DRV execution scripts for the precision marketing
domain. Historical week reports and long-form docs live in the principle
archive at:

scripts/global_scripts/99_archive/drv_week_docs_20251113/

Key scripts:
- all_D01_06.R: customer DNA analysis (D01_00–D01_05 master)
- all_D04_09.R: feature preparation
- all_D04_07.R: time series completion (R117)
- all_D04_08.R: Poisson analysis (R118)
- generate_time_series_metadata.R
- validate_week2.R
- validate_week3.R
- validate_week4.R
- run_full_validation.sh

Related:
- scripts/execute_all_weeks.sh

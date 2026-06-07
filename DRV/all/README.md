# DRV All (cross-platform "all" derivations)

This directory contains DRV execution scripts whose platform scope is **all**
(cross-platform aggregations / platform-agnostic derivations); the concrete
platform is resolved at runtime per DM_R066.

Key scripts:
- `D00_app_data_init.R` — app_data initialization
- `all_D01_06.R` / `all_D01_07.R` / `all_D01_08.R` — customer DNA analysis (D01 master)
- `all_D03_01.R` / `all_D03_02.R` — positioning analysis
- `all_D05_01.R` / `all_D05_03.R` — macro trend analysis
- `all_D07_01.R` — app-level AI insight precompute
- `validate_d01_outputs.R` — D01 output validation

## Removed: precision marketing v2.0 scaffolding (2026-06-07)

The "MAMBA Precision Marketing Redesign" v2.0 scaffolding (precision ETL +
`all_D04_04`–`all_D04_09` + `validate_week2/3/4` + `run_full_validation.sh` +
`generate_time_series_metadata.R`) was removed as **obsolete-by-convergence**:
the live legacy `D04_02` (`DRV/D04/`, v6.0) already absorbed its three headline
capabilities — platform-agnostic (DM_R066), R118 statistical significance, and
metadata-driven predictor classification (SCHEMA_006 / #716 / #733). The
scaffolding produced 0 rows, had no live reader, and only caused diagnostic
confusion.

Git history preserves the removed scaffolding. If a *separate* precision/all
aggregation pipeline is ever needed again, see gh issue #1143 (removal rationale)
and #1195 (revival context: git-recover path + trigger conditions).

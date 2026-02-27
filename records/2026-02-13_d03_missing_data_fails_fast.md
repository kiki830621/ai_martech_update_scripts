# D03 missing-data fail-fast update

## Date
2026-02-13

## Scope
AMZ D03 positioning derivation scripts and D03 documentation rules.

## Decision
In D03_04 and D03_05, missing data should **not** be treated as completion.
When required source/output tables are missing or internal merge/finalization steps fail, the derivation target must stop with error.

## Files changed

- `scripts/update_scripts/DRV/amz/amz_D03_07.R`
- `scripts/update_scripts/DRV/amz/amz_D03_11.R`
- `scripts/global_scripts/00_principles/docs/en/part2_implementations/CH12_derivations/D03_positioning_analysis/_implementation_rules.yaml`
- `scripts/global_scripts/00_principles/docs/en/part2_implementations/CH12_derivations/D03_positioning_analysis/D03_04_query_by_asin.qmd`
- `scripts/global_scripts/00_principles/docs/en/part2_implementations/CH12_derivations/D03_positioning_analysis/D03_05_position_table.qmd`
- `scripts/global_scripts/00_principles/docs/zh/part2_implementations/CH12_derivations/D03_positioning_analysis/D03_04_query_by_asin.qmd`
- `scripts/global_scripts/00_principles/docs/zh/part2_implementations/CH12_derivations/D03_positioning_analysis/D03_05_position_table.qmd`
- `scripts/global_scripts/00_principles/docs/en/part2_implementations/CH12_derivations/D03_positioning_analysis/_implementation_rules.yaml`
- `scripts/global_scripts/00_principles/docs/en/part2_implementations/CH12_derivations/D03_positioning_analysis/D03_06_execution.qmd`
- `scripts/global_scripts/00_principles/docs/zh/part2_implementations/CH12_derivations/D03_positioning_analysis/D03_06_execution.qmd`
- `scripts/global_scripts/05_etl_utils/amz/core_import_df_amz_competitor_sales.R`
- `scripts/update_scripts/ETL/amz/amz_ETL_competitor_sales_0IM.R`
- `scripts/update_scripts/DRV/amz/amz_D03_10.R`
- `scripts/update_scripts/DRV/amz/amz_DRV03_analysis_10.R`

## Additional behavior (2026-02-13 extension)

- `core_import_df_amz_competitor_sales()` now skips folders under `competitor_sales` with no supported files instead of failing early.
- Skipped folder lists are logged in import summary:
  - `skipped_folders_no_supported_files`
  - `skipped_folders_no_rows`
- D03/ETL scripts now treat `import_result$total_rows_imported` as success indicator and emit explicit skip summaries.
- The step still fails fast when:
  - no valid directories/files are found for the run, or
  - no rows are imported after all folders are processed.

## Implementation notes

- `scripts/global_scripts/00_principles/docs/en/part2_implementations/CH12_derivations/D03_positioning_analysis/_implementation_rules.yaml`:
  - Added ETL07 empty-folder policy under D03_05 failure policy.
- `scripts/global_scripts/00_principles/docs/en/part2_implementations/CH12_derivations/D03_positioning_analysis/D03_06_execution.qmd`:
  - Added explicit ETL07 skip-and-continue policy.
- `scripts/global_scripts/00_principles/docs/zh/part2_implementations/CH12_derivations/D03_positioning_analysis/D03_06_execution.qmd`:
  - Added parallel Chinese note for ETL07 skip policy.

## Behavior changes

1. `amz_D03_07.R`:
   - Collects failed product lines from `process_comment_property_ratings_by_asin()`.
   - Verifies output tables exist for all configured `product_line_id`.
   - Calls `stop()` when any line fails or output is missing.
   - This replaces previous warning-only completion behavior.

2. `amz_D03_11.R`:
   - Collects failed `process_position_table()` calls.
   - Enforces fail-fast when merge or finalization fails.
   - Verifies `app_data.df_position` exists before marking success.

## Principle/doc alignment

- Added explicit failure policies to D03 implementation rules (`_implementation_rules.yaml`).
- Added explicit "Failure Behavior" notes in both en/zh D03 task docs for D03_04 and D03_05.

## Related references

- D03 query by ASIN (D03_04): `amz_D03_07.R`
- D03 position table creation (D03_05): `amz_D03_11.R`

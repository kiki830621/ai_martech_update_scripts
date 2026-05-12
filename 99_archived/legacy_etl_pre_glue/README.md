# Legacy Pre-Glue ETL Archive

> **Source**: archived by `/spectra-apply legacy-etl-aggressive-deprecation` P3 pilot Tier A on 2026-05-11
> **Spectra spec**: `openspec/specs/legacy-etl-deprecation-playbook/spec.md`

Files archived here are legacy R script ETLs (pre-glue era) superseded by glue layer + bridge yaml architecture per `etl-architecture-roadmap` Requirement 2 P3 scope.

Per MP030 (Archive Immutability), files in this directory are READ-ONLY. Reference them for historical patterns or restoration via `git log --all -- 99_archived/legacy_etl_pre_glue/`, but do not modify in place.

## Archived files

| File | Archive date | Reason | Replacement reference |
|---|---|---|---|
| `amz_ETL_company_product_master_0IM.R` | 2026-05-11 | Tier A: 0 DRV consumers per #613 audit (truncated chain) | (no replacement — schema lives in `bridges/QEF_DESIGN/amz/company_product_extension.bridge.yaml` if needed in future) |
| `amz_ETL_company_product_master_1ST.R` | 2026-05-11 | Tier A | (no replacement) |
| `amz_ETL_company_product_master_2TR.R` | 2026-05-11 | Tier A | (no replacement) |
| `amz_ETL_demographic_0IM.R` | 2026-05-11 | Tier A: truncated chain (0IM only) | (deferred onboarding — re-author via /glue-bridge skill v2.0 if needed) |
| `amz_ETL_keepa_0IM.R` | 2026-05-11 | Tier A: truncated chain | (deferred onboarding — re-author via /glue-bridge skill v2.0 if needed) |
| `amz_ETL_keys_0IM.R` | 2026-05-11 | Tier A: truncated chain | (deferred onboarding — re-author via /glue-bridge skill v2.0 if needed) |
| `amz_ETL_sku_mapping_0IM.R` | 2026-05-11 | Tier A: truncated chain | (deferred onboarding — re-author via /glue-bridge skill v2.0 if needed) |
| `amz_ETL_competitor_sales_0IM.R` | 2026-05-11 | Tier A: truncated chain | (deferred onboarding — re-author via /glue-bridge skill v2.0 if needed) |

## Restoration

If a future need requires reactivating any archived file:

1. Read it: `git show HEAD:99_archived/legacy_etl_pre_glue/<file>`
2. Author a new bridge yaml via `/glue-bridge` skill v2.0 (schema-driven shape)
3. Do NOT directly restore the legacy R script — re-implement under post-glue architecture per `etl-architecture-roadmap` Requirement 3

## 2026-05-12 — amz/sales pilot atomic cutover

`amz_ETL_sales_0IM.R` — Archived 2026-05-12 per amz-sales-pilot-execution Section 5.

**Replacement**: `shared/global_scripts/01_db/raw_schema/_authoring/bridges/QEF_DESIGN/amz/sales.bridge.yaml` (v2 field_extractors form). Produces `df_amz_sales___raw` (canonical schema) via `fn_glue_bridge()` runtime.

**Sister scripts retained + trimmed** (per `legacy-etl-deprecation-playbook` Step 3-4):
- `amz_ETL_sales_1ST.R` — quality validation only, reads canonical raw layer
- `amz_ETL_sales_2TR.R` — post-canonical transformation only (order_date parse + ASIN backfill + defense filter)

**Verified end-to-end**: bridge runtime produces 281,919 rows from 294,526 source (0.05% above pre-migration baseline 281,784). All 6 glue-layer infrastructure bugs closed (#628 / #630 / #631 / #632 / #633 / #634).

# Pipeline Bootstrap Guide

新公司首次部署 / 既有公司缺 DB 層時,如何把 ETL pipeline 跑起來。

## 前置條件

- 專案結構(由 `new-company` skill 建立):
  ```
  <company>/
  ├── app_config.yaml          # Company scope config (MP142, SO_P016)
  ├── scripts/
  │   ├── global_scripts/      # → ../../shared/global_scripts (symlink)
  │   └── update_scripts/      # → ../../shared/update_scripts (symlink)
  ├── data/
  │   ├── local_data/          # canonical ETL DB 位置(DM_R062)
  │   └── app_data/            # canonical 5NM 層位置
  └── .env                     # OPENAI_API_KEY 等敏感變數
  ```

- `data/local_data/rawdata_<COMPANY>/` 含原始資料(Excel / Google Sheet / CSV)

## Canonical DB Paths (DM_R062)

所有 `.duckdb` 檔案位置由 `shared/global_scripts/30_global_data/parameters/scd_type1/db_paths.yaml` 定義:

| Layer | Canonical Path |
|---|---|
| 0IM raw | `data/local_data/raw_data.duckdb` |
| 1ST staged | `data/local_data/staged_data.duckdb` |
| 2TR transformed | `data/local_data/transformed_data.duckdb` |
| 3PR processed | `data/local_data/processed_data.duckdb` |
| 4CL cleansed | `data/local_data/cleansed_data.duckdb` |
| 5NM app_data | `data/app_data/app_data.duckdb` |
| meta | `data/local_data/meta_data.duckdb` |
| SCD1 domain | `data/local_data/scd_type1/*.duckdb` |
| SCD2 domain | `data/local_data/scd_type2/*.duckdb` |

**偵測到檔案在錯位置時,移動資料到 canonical path,不要改程式**(DM_R062)。

## First-Time Bootstrap(新公司)

```bash
cd <company>/scripts/update_scripts

# 1. Generate config
make config-full     # config-merge + scan + validate

# 2. Produce canonical meta_data.duckdb first (DM_R054 v2)
#    This is the "7th layer" — non-rebuildable metadata, must exist
#    before autoinit fail-fast permits full pipeline runs.
cd ../..
MAMBA_PROJECT_ROOT="$(pwd)" Rscript \
  scripts/update_scripts/ETL/all/all_ETL_meta_init_0IM.R

# 3. Run full pipeline (will build all 6-layer DBs from rawdata)
cd scripts/update_scripts
make run PLATFORM=<platform>
```

**Why step 2 first**: `meta_data.duckdb` has no producer in the 6-layer ETL chain — it's the "7th layer" produced by its own bootstrap ETL (`all_ETL_meta_init_0IM.R`). Without it, autoinit's fail-fast precheck will stop full pipeline runs (UPDATE_MODE only — APP_MODE skips the precheck per DM_R054 v2.1.1). The bootstrap ETL uses pre-autoinit mode (no autoinit call) so it can run when meta_data.duckdb does not yet exist.

**DM_R054 v2.1.1 (2026-04-20) — Posit Connect deploy note**: for companies that run production on Posit Connect with `app_config.yaml > database.mode: "supabase"` (e.g., MAMBA), the CSV seed step above produces local `meta_data.duckdb` for UPDATE_MODE only. The deployed Shiny app reads `df_product_line` / `df_platform` from live Supabase via `dbConnectAppData()` — `meta_data.duckdb` is NOT shipped in the Posit Connect bundle, and Supabase is populated by a separate upload ETL (outside this bootstrap). Runtime MUST NOT read CSV seeds (§6); that holds for both DuckDB and Supabase canonical sources.

第一次跑時可能要 10-60 分鐘,視 rawdata 體量。smart cache detection 會偵測所有 DB 都不存在 → 自動 nuclear rebuild。

## 已有部分 DB 檔但缺某層(rebuild 缺失層)

`autoinit()` 會先 fail-fast 報缺哪層,錯誤訊息附 actionable 修復命令。依提示跑:

```bash
# 例:缺 transformed_data + processed_data
make run PLATFORM=<platform>   # smart cache 會 detect 缺失並 auto-rebuild
```

若 smart detection 誤判或想強制全量重跑:

```bash
make run FORCE=1 PLATFORM=<platform>   # nuclear: tar_destroy + full rebuild
```

## `make run` 三種行為

| 情況 | 行為 |
|---|---|
| 所有 DB 檔齊全 | Selective mode — 只跑 code 改過的 target(快) |
| 某些 DB 檔缺失 + TTY | 印缺失清單 + 等 ENTER 確認後 nuclear rebuild |
| 某些 DB 檔缺失 + 非 TTY(CI/nohup) | 印 "non-interactive mode: auto-proceed" 直接 nuclear |
| 設 `FORCE=1` | 無條件 `tar_destroy()` + full rebuild(最慢但最乾淨) |

## 錯位置檔案修復(DM_R062)

若 autoinit / smart cache 報缺某層但你知道該檔在別處:

```bash
# 1. 核對 canonical path
cat shared/global_scripts/30_global_data/parameters/scd_type1/db_paths.yaml

# 2. Archive 錯位置檔
mkdir -p <company>/data/archived/wrong_location
mv <wrong_path> <company>/data/archived/wrong_location/<name>_$(date +%Y%m%d).duckdb

# 3. 重跑 ETL 在 canonical 重建
cd <company>/scripts/update_scripts
make run PLATFORM=<platform>
```

**不要**:
- 改 `db_paths.yaml` 指向錯位置(會讓未來檔案繼續放錯)
- 改 `sc_Rprofile.R` 加 fallback 路徑邏輯(legacy cruft)
- 直接 rm 錯位置檔(失去 rollback 能力)

## Troubleshooting

### autoinit hang(本 change 之前的行為)

若在 fix 之前版本遇到 autoinit 多小時 hang,root cause 通常是 Dropbox CloudStorage × DuckDB create-on-miss 交互(見 #421)。本 change 的 fail-fast precheck 已讓這種情況立即報錯。若仍遇到 hang,檢查:

- 你用的 `sc_Rprofile.R` 是否含 fail-fast precheck(line 125+)
- `db_paths.yaml` 是否在 shared 目錄而非 company override

### make run FORCE=1 卡在某個 target

這不是本 change scope。通常意味該 target 自身 logic bug 或 rawdata 不完整,另開 issue diagnose。

### 多公司共用的 sanity check

改動 shared `sc_Rprofile.R` / `Makefile` 後,建議在其他公司(MAMBA / D_RACING / KITCHENMAMA / URBANER / WISER)各跑一次 `autoinit()` 確認新 policy 不誤報:

```bash
cd <other_company>
Rscript -e "source('.Rprofile'); autoinit(); cat('OK\n')"
```

## 相關原則

- **DM_R062**:Canonical DB Path Enforcement(本文件 core principle)
- **DM_R028**:ETL Data Type Separation(6-layer 結構)
- **DM_R056**:Mode-Specific DB Paths(db_paths.yaml 為 source of truth)
- **MP029**:No Fake Information(data location 是事實,不是假設)
- **MP064**:ETL-Derivation Separation
- **MP140**:Pipeline Orchestration(targets + Makefile)
- **autoinit-failfast-policy** spec(change: `autoinit-failfast-smart-cache`)
- **pipeline-smart-cache** spec(same change)

## Related Issues

- #421 — autoinit 多小時 hang 的 root cause + 本 change 的 motivation

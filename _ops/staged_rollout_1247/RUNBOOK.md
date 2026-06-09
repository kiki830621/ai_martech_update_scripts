# #1247 staged-rollout runbook (QEF_DESIGN + D_RACING)

Goal: re-derive the market model with the #1247 one-hot encoder, produce a
BEFORE/AFTER coefficient + significance diff, get **林郁翔 sign-off**, then upload
+ deploy. Run in a **real terminal** (the long `make run` gets SIGTERM'd inside a
Claude Code session — #594). Do both companies.

`RUNNER=shared/update_scripts/.claude/worktrees/idd-1247-attr-coverage/_ops/staged_rollout_1247/staged_rollout_1247.R`
(use the absolute path from your machine).

---

## 0. Pre-flight (once)

```bash
# session toolchain readiness (DEV_R057)
Rscript shared/global_scripts/98_test/check_packages.R

# make sure no other R session holds the company app_data lock (#437) — DON'T blind-kill
ps aux | grep -iE "Rscript|R --file|tar_make" | grep -v grep
```

The #1247 code lives on two branches (NOT yet on main):
- `ai_martech_global_scripts` → `idd/1247-attr-coverage` (encoder + selector + MP165)
- `ai_martech_update_scripts` → `idd/1247-attr-coverage` (D04_03 wiring)

You will run D04_03 **twice**: once on current `main` (BEFORE), once with the
`idd/1247-attr-coverage` code applied (AFTER).

---

## 1. BEFORE snapshot (current main code)

Per company (`cd QEF_DESIGN` then repeat for `cd D_RACING`):

```bash
cd QEF_DESIGN
# df_position must exist (D04_03 CONSUMES it; DEPENDS_ON_DRV D03_11)
Rscript -e 'source("scripts/global_scripts/02_db_utils/duckdb/fn_dbConnectDuckdb.R"); c<-dbConnectDuckdb("data/app_data/app_data.duckdb",read_only=TRUE); cat("df_position:", DBI::dbExistsTable(c,"df_position"), "\n")'
# if FALSE: cd scripts/update_scripts && make run TARGET=amz_D03_11   (foreground)

# run D04_03 on CURRENT code (foreground — never run_in_background; #594)
cd scripts/update_scripts && make run TARGET=amz_D04_03 && cd ../..

# capture
Rscript "$RUNNER" capture before --out /tmp/rollout_1247
```

For a `make run` that may exceed the Bash timeout, use the shell-`&` + pgrep wait
pattern (06-running-app.md), not `run_in_background`.

---

## 2. Apply #1247 code

The company's `scripts/global_scripts` and `scripts/update_scripts` are symlinks
to the shared checkouts. Put the #1247 code on those checkouts. **Cleanest:** open
the two PRs and merge to main (code on main ≠ live data changed — the dashboards
read Supabase, which only changes at step 5). Then `git -C shared/global_scripts pull`
and `git -C shared/update_scripts pull`.

(Alternative without merging: `git -C shared/global_scripts checkout idd/1247-attr-coverage`
and same for update_scripts — but that disturbs the shared main checkout for any
other session, so prefer merge-to-main.)

---

## 3. AFTER snapshot (#1247 code)

```bash
cd QEF_DESIGN/scripts/update_scripts
# targets won't see the code/yaml change on its own — invalidate the target
Rscript -e 'targets::tar_invalidate(matches("amz_D04_03"), store="_targets")'   # NOT FORCE=true
make run TARGET=amz_D04_03
cd ../..
Rscript "$RUNNER" capture after --out /tmp/rollout_1247
```

---

## 4. Diff → 林郁翔 sign-off

```bash
Rscript "$RUNNER" diff \
  /tmp/rollout_1247/QEF_DESIGN_amz_before.rds \
  /tmp/rollout_1247/QEF_DESIGN_amz_after.rds \
  --out /tmp/rollout_1247/QEF_DESIGN_diff.md
```

Send `QEF_DESIGN_diff.md` (and the D_RACING one) to 林郁翔. The report covers:
new entrants (the categorical dummies), `non_numeric → entered` fate transitions,
dropped predictors (R4 expects ≈0), shared-coefficient drift (expects ≈0),
significance flips, and the R4 acceptance checklist.

**Gate:** do NOT proceed to step 5 until 林郁翔 approves the diff.

---

## 5. Upload + deploy (after sign-off)

Per company:

```bash
# Supabase needs .env in the shell (memory: supabase-upload-needs-env-in-shell)
set -a; . QEF_DESIGN/.env; set +a
cd QEF_DESIGN/scripts/update_scripts && make upload && cd ../..   # targeted, after data settles

# deploy (bundle reads Supabase; DM_R063 purity)
cd QEF_DESIGN && make deploy-sync && make deploy-push && cd ..
```

Then live-verify with safari-browser (`--url` lock, DOM-read; memory:
live-dashboard-verify-safari-js-domread): precision-marketing panel shows the new
categorical/image attrs, 缺點 absent, only-significant filter holding.

---

## 6. Close-out

- Commit trailer: `Verified: QEF_DESIGN, D_RACING` (IC_P002).
- Every commit refs `#1247`.
- `make run`'s deploy-sync staleness chain (DM_R069) + `last_supabase_upload` in
  `.claude/companies.yaml` updates only on a successful upload — don't hand-edit it.

## Guardrails recap
- **No `run_in_background`** for `make run` / `make upload` — harness SIGTERM (#594).
- **`tar_invalidate`**, not `FORCE=true` (targets doesn't track yaml/runtime sql_read).
- **#437**: identify DB-lock holders with `ps -p <PID> -o command=`; never blind-kill.
- Known low-count caveat: audit may under-count by the #1248 `.x`/`.y` merge leak —
  the diff's "predictor == audit entered" check is modulo that.

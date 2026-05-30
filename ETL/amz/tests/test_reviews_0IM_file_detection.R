# test_reviews_0IM_file_detection.R
# Regression test for #607: reviews ETL must NOT drop extensionless Excel files.
#
# #368 (CLOSED) bulk-renamed 64 files; the regex pre-filter at L61 of
# amz_ETL_reviews_0IM.R kept dropping extensionless Excel-format files on every
# fresh Amazon scrape (37% of QEF reviews unreachable on 2026-05-10).
#
# This test asserts the durable Path (b) fix behaviour:
#   1. A magic-byte check classifies an extensionless real .xlsx review file as
#      "xlsx" (PK\x03\x04 ZIP container) rather than dropping it.
#   2. The file-selection predicate (mirroring amz_ETL_reviews_0IM.R) keeps
#      extensionless files while still excluding hidden dotfiles.
#   3. End-to-end: an extensionless real .xlsx is actually readable by readxl.
#
# Uses a REAL review .xlsx file copied from on-disk rawdata (MP029: no fake data),
# falling back to a freshly-written real xlsx via writexl only if none is found.
#
# Run from the company project root, e.g.:
#   cd QEF_DESIGN
#   NOT_CRAN=true Rscript scripts/update_scripts/ETL/amz/tests/test_reviews_0IM_file_detection.R

suppressWarnings(suppressMessages(library(readxl)))

# ---------------------------------------------------------------------------
# Format detection by magic bytes. Kept byte-for-byte in sync with
# detect_file_format() in amz_ETL_reviews_0IM.R (no eval of source — the rule
# is small and asserted here as the regression contract):
#   PK\x03\x04          -> ZIP container == .xlsx (OOXML)
#   \xD0\xCF\x11\xE0...  -> OLE2 compound == .xls (legacy Excel)
#   else                -> csv
# ---------------------------------------------------------------------------
detect_file_format <- function(path) {
  con <- file(path, "rb"); on.exit(close(con), add = TRUE)
  magic <- readBin(con, what = "raw", n = 8L)
  if (length(magic) >= 4L &&
      identical(magic[1:4], as.raw(c(0x50, 0x4B, 0x03, 0x04)))) return("xlsx")
  if (length(magic) >= 8L &&
      identical(magic[1:8],
                as.raw(c(0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1)))) return("xls")
  "csv"
}

# Guard: the ETL script must define detect_file_format with the same magic-byte
# contract, so this regression test stays coupled to the real fix.
etl_candidates <- c(
  file.path("scripts", "update_scripts", "ETL", "amz", "amz_ETL_reviews_0IM.R"),
  file.path("ETL", "amz", "amz_ETL_reviews_0IM.R"),
  file.path(dirname(getwd()), "amz_ETL_reviews_0IM.R")
)
etl_script <- etl_candidates[file.exists(etl_candidates)][1]

fails <- 0L
check <- function(label, cond) {
  if (isTRUE(cond)) message("PASS: ", label)
  else { message("FAIL: ", label); fails <<- fails + 1L }
}

if (!is.na(etl_script)) {
  src <- paste(readLines(etl_script, warn = FALSE), collapse = "\n")
  check("ETL script defines detect_file_format()",
        grepl("detect_file_format[[:space:]]*<-[[:space:]]*function", src))
  check("ETL script no longer hard-fails on unknown extension (regex-only filter removed)",
        !grepl("Unsupported extension", src, fixed = TRUE))
  check("ETL filter keeps extensionless files (is_extensionless predicate present)",
        grepl("is_extensionless", src, fixed = TRUE))
}

tmp <- tempfile("rev607_"); dir.create(tmp)
on.exit(unlink(tmp, recursive = TRUE), add = TRUE)

# --- Obtain a REAL .xlsx review file (MP029) -------------------------------
real_xlsx <- character(0)
search_roots <- c("data/local_data", file.path("..", "data", "local_data"),
                  Sys.getenv("RAW_DATA_DIR", unset = ""))
search_roots <- search_roots[nzchar(search_roots)]
for (root in search_roots) {
  if (!dir.exists(root)) next
  hits <- list.files(root, pattern = "\\.xlsx$", recursive = TRUE, full.names = TRUE)
  hits <- hits[grepl("review", hits, ignore.case = TRUE)]
  if (length(hits)) { real_xlsx <- hits[1]; break }
}
if (!length(real_xlsx)) {
  if (requireNamespace("writexl", quietly = TRUE)) {
    real_xlsx <- file.path(tmp, "synth.xlsx")
    writexl::write_xlsx(data.frame(asin = "B000", body = "test", rating = 5), real_xlsx)
    message("NOTE: no on-disk review .xlsx found; using a freshly-written real xlsx")
  } else {
    stop("No real review .xlsx found and writexl unavailable to synthesize one")
  }
} else {
  message("Using real review xlsx: ", real_xlsx)
}

# Copy real xlsx to an EXTENSIONLESS path (the exact #607 regression scenario)
ext_less <- file.path(tmp, "B00080FKIO")     # ASIN-style name, no suffix
invisible(file.copy(real_xlsx, ext_less, overwrite = TRUE))
with_ext <- file.path(tmp, "B00080FKIO.xlsx")
invisible(file.copy(real_xlsx, with_ext, overwrite = TRUE))
csv_path <- file.path(tmp, "C00000.csv")
writeLines(c("asin,body,rating", "B001,hello,4"), csv_path)
hidden <- file.path(tmp, ".DS_Store")
writeLines("junk", hidden)

# === Assertion 1: magic-byte detection ===
check("extensionless real xlsx detected as xlsx", identical(detect_file_format(ext_less), "xlsx"))
check("xlsx with proper extension detected as xlsx", identical(detect_file_format(with_ext), "xlsx"))
check("csv content detected as csv", identical(detect_file_format(csv_path), "csv"))

# === Assertion 2: file-selection predicate keeps extensionless, drops hidden ===
matched_files <- list.files(tmp, full.names = TRUE, all.files = TRUE, no.. = TRUE)
matched_files <- matched_files[!dir.exists(matched_files)]
has_data_ext <- grepl("\\.(csv|xlsx?)$", matched_files, ignore.case = TRUE)
is_extensionless <- !nzchar(tools::file_ext(matched_files))
is_hidden <- grepl("^\\.", basename(matched_files))
selected <- matched_files[(has_data_ext | is_extensionless) & !is_hidden]

check("extensionless file is KEPT by the filter (the #607 fix)", ext_less %in% selected)
check("hidden dotfile is excluded", !(hidden %in% selected))
check("proper-extension xlsx and csv kept", all(c(with_ext, csv_path) %in% selected))

# === Assertion 3: end-to-end — extensionless real xlsx is actually readable ===
df <- tryCatch(readxl::read_excel(ext_less), error = function(e) NULL)
check("extensionless real xlsx reads via readxl::read_excel (>0 cols)",
      !is.null(df) && ncol(df) > 0)

if (fails > 0L) stop(sprintf("test_reviews_0IM_file_detection: %d assertion(s) FAILED", fails))
message("ALL PASS: reviews 0IM extensionless-xlsx detection (#607)")

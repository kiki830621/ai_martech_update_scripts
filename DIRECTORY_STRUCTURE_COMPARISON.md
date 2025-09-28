# WISER ETL 目錄結構方案比較

## 方案 A: 集中式（所有檔案在同一目錄）

```
update_scripts/
├── cbz_ETL01_0IM.R
├── cbz_ETL01_1ST.R
├── cbz_ETL01_2TR.R
├── cbz_DRV01_customer.R
├── amz_ETL01_0IM.R
├── amz_ETL01_1ST.R
├── amz_ETL01_2TR.R
├── amz_DRV01_customer.R
└── ... (所有 52 個檔案)
```

### 優點 ✅
- **簡單直接**：所有腳本在一個地方
- **易於搜尋**：`ls *ETL*` 即可找到所有 ETL
- **相容現狀**：最小改動，降低風險
- **批次操作**：容易執行 `Rscript *_0IM.R`

### 缺點 ❌
- **擁擠混亂**：52+ 檔案在同一層
- **難以導航**：需要依賴命名來區分功能
- **擴展性差**：新增平台會讓目錄更亂

## 方案 B: 階層式（按功能/平台分資料夾）

```
update_scripts/
├── ETL/
│   ├── cbz/
│   │   ├── cbz_ETL01_0IM.R
│   │   ├── cbz_ETL01_1ST.R
│   │   └── cbz_ETL01_2TR.R
│   ├── amz/
│   │   ├── amz_ETL01_0IM.R
│   │   ├── amz_ETL01_1ST.R
│   │   └── amz_ETL01_2TR.R
│   └── all/
│       └── all_ETL_summary_0IM.R
├── DRV/
│   ├── cbz/
│   │   └── cbz_DRV01_customer.R
│   └── amz/
│       └── amz_DRV01_customer.R
└── orchestration/
    └── run_pipeline.R
```

### 優點 ✅
- **組織清晰**：ETL vs DRV 明確分離（MP064）
- **易於維護**：各平台獨立管理
- **符合原則**：體現 MP104 資料流分離
- **擴展性佳**：新增平台只需新增資料夾

### 缺點 ❌
- **路徑變長**：需要調整現有引用
- **初期工作量**：需要更多重構
- **學習曲線**：團隊需要適應新結構

## 方案 C: 混合式（折衷方案）🎯

```
update_scripts/
├── cbz/                    # 平台資料夾
│   ├── cbz_ETL01_0IM.R    # ETL 流程
│   ├── cbz_ETL01_1ST.R
│   ├── cbz_ETL01_2TR.R
│   └── cbz_DRV01_customer.R # Derivations
├── amz/
│   ├── amz_ETL01_0IM.R
│   ├── amz_ETL01_1ST.R
│   ├── amz_ETL01_2TR.R
│   └── amz_DRV01_customer.R
├── all/                    # 跨平台
│   └── all_ETL_summary_0IM.R
└── _orchestration/         # 控制腳本
    ├── run_daily.R
    └── run_full_pipeline.R
```

### 優點 ✅
- **平衡性好**：按平台分組，減少混亂
- **保持簡潔**：只有一層子目錄
- **易於理解**：平台導向思維
- **便於執行**：`Rscript cbz/*.R` 執行平台所有腳本

### 缺點 ❌
- **ETL/DRV 混合**：同一資料夾內有不同類型
- **可能需要子分類**：當檔案增多時

## 📊 決策矩陣

| 評估維度 | 方案A (集中) | 方案B (階層) | 方案C (混合) |
|---------|------------|------------|------------|
| 組織清晰度 | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| 實施難易度 | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| 維護成本 | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| 原則符合度 | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| 擴展性 | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| 團隊接受度 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |

## 🎯 建議：採用方案 C（混合式）

### 理由：
1. **務實平衡**：在組織性和簡單性之間取得平衡
2. **漸進式改善**：從現狀到理想的中間步驟
3. **平台導向**：符合商業邏輯（按平台管理）
4. **未來彈性**：可以逐步演進到方案 B

## 實施步驟

### Phase 1: 初期整理（方案 C）
```bash
# 1. 創建平台資料夾
mkdir -p cbz amz eby all _orchestration

# 2. 移動檔案（保留命名）
mv cbz_* cbz/
mv amz_* amz/
mv eby_* eby/
mv all_* all/

# 3. 處理遺留檔案
# 分析 P01_D01_*.R 屬於哪個平台
# 重命名並移動到對應資料夾
```

### Phase 2: 命名標準化
```bash
# 在各平台資料夾內標準化命名
cd cbz/
rename 's/P01_D01_/cbz_ETL01_/g' *.R
```

### Phase 3: 未來演進（可選）
```bash
# 如果檔案數量增長，可以再細分
cbz/
├── ETL/
│   ├── cbz_ETL01_0IM.R
│   └── ...
└── DRV/
    └── cbz_DRV01_customer.R
```

## 📝 配置調整

### 更新路徑引用
```r
# 原始
source("P01_D01_00.R")

# 新路徑
source("cbz/cbz_ETL01_0IM.R")
```

### 批次執行腳本
```r
# run_cbz_pipeline.R
etl_files <- list.files("cbz", pattern = "*_0IM\\.R$", full.names = TRUE)
for (file in etl_files) {
  source(file)
}
```

## 🚨 重要提醒

**MP029 合規**：整個重組過程不會創建任何假資料
**備份優先**：執行前完整備份 update_scripts/
**漸進執行**：可以一個平台一個平台地遷移

## 決策問題

1. **您的優先考量是什麼？**
   - 最小改動 → 選方案 A
   - 最佳組織 → 選方案 B
   - 平衡務實 → 選方案 C ✓

2. **團隊規模？**
   - 小團隊（1-3人）→ 方案 A 也可
   - 中大團隊（4+人）→ 方案 B 或 C

3. **未來成長？**
   - 穩定不變 → 方案 A
   - 持續增長 → 方案 B 或 C

---
建議：**從方案 C 開始，視需要演進到方案 B**
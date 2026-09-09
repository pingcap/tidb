# `pkg/statistics/table.go` — walk receipt

Comparison source: Go `origin/master` (`f2c346fe4f3`), table.go — 1087
lines, 67 declarations.

## Inventory mapping

| Go function | Rust location | Status |
| --- | --- | --- |
| `PseudoTable` | `table.rs::pseudo_table` | ported |
| `PseudoHistColl` | `table.rs::pseudo_hist_coll` | ported |
| `NewHistColl` / `NewHistCollWithColsAndIdxs` | `HistColl` construction (`table.rs`) | ported |
| `CopyAs` | `table.rs::copy_as` | ported |
| `ColumnByName` | `table.rs::column_by_name` | ported |
| `ColumnIsLoadNeeded` | `table.rs::column_load_needed` | ported |
| `IndexIsLoadNeeded` | `table.rs::index_load_needed` | ported |
| `GetStatsHealthy` | `table.rs::stats_healthy` | ported |
| `IsOutdated` | `table.rs::is_outdated` | ported |
| `IsInitialized` | `table.rs::is_initialized` | ported |
| `IsEligibleForAnalysis` | `table.rs::is_eligible_for_analysis` + `analysis_policy.rs::is_eligible_for_analysis` | ported |
| `MeetAutoAnalyzeMinCnt` | `table.rs::meets_auto_analyze_min_count` + `analysis_policy.rs` | ported |
| `AnalyzeVersionMatchesForTableStats` | `analyze_version_policy.rs::analyze_version_matches` | ported |
| `IsAnalyzed` | `table.rs`/`column.rs`/`index.rs` `is_analyzed` | ported |
| `NewColAndIndexExistenceMap` / `...WithoutSize` / `ColAndIdxExistenceMapIsEqual` | `existence_map.rs` | ported |
| `DelCol` / `DelIdx` | existence-map removals (`existence_map.rs`) | absorbed |
| `GetStatsInfo` | `table.rs` column/index accessors | absorbed |
| `MemoryUsage` / `String` | `table.rs` memory/display impls | ported |
| `IndexStartWithColumn` | NO Go production caller (dead helper) | correctly absent |
| pseudo-referencing internal helpers | module-internal | absorbed |

## Conclusion

No production divergence. The one function not ported
(`IndexStartWithColumn`) has zero Go production call sites.

## Validation

`cargo +nightly-2026-08-22 nextest run -p tidb-stats` — 294/294 at walk
time; fmt clean.

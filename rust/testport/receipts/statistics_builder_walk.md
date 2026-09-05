# `pkg/statistics/builder.go` + `column.go` accessors — walk receipt

Comparison source: Go `origin/master` (`f2c346fe4f3`). builder.go — 605
lines, 12 functions; column.go accessors re-verified against the queueing
batch.

## builder.go inventory mapping

| Go function | Rust location | Status |
| --- | --- | --- |
| `NewSortedBuilder` / `Iterate` / `Hist` | `sorted_builder.rs` (`SortedBuilder`) | ported |
| `BuildColumnHist` | `builder.rs::build_column_histogram` | ported |
| `BuildColumn` | `builder.rs::build_column` | ported |
| `BuildHistAndTopN` | `builder.rs::build_hist_and_topn` | ported |
| `calcCorrelation` | `correlation.rs::calc_correlation` | ported |
| `NewSequentialRangeChecker` / `IsIndexInTopNRange` | `builder.rs::SequentialRangeChecker` | ported |
| `processTopNValue` | `builder.rs` (BoundedMinHeap candidate processing) | ported |
| `pruneTopNItem` | `builder.rs::prune_topn_item` | ported |
| `isAnalyzeDefaultValue` | `builder.rs::is_analyze_default_value` | ported |
| `topNPruningThreshold` const | `builder.rs::TOPN_PRUNING_THRESHOLD` | ported |

## column.go accessor re-verification

`Copy`, `TotalRowCount`, `NotNullCount`, `GetIncreaseFactor`,
`MemoryUsage`, `ItemID`, `DropUnnecessaryData`, `IsAllEvicted`,
`GetEvictedStatus`, `IsStatsInitialized`, `GetStatsVer`, `IsCMSExist`,
`IsAnalyzed`, `StatsAvailable`, `EmptyColumn`, `GetHistogram`, `GetTopN`,
`StatusToString` — all present in `column.rs`/`status.rs` (the queueing
half `ColumnStatsIsInvalid` was ported in the async-load batch).
`String` → display impl. No divergence.

## Conclusion

builder.go fully ported, zero gaps. The `pkg/statistics` core walk is
complete: scalar.go, cmsketch.go, histogram.go, table.go, index.go,
column.go, builder.go, fmsketch.go, estimate.go, constants.go all covered
by walk receipts or the async-load/column batches.

## Validation

`cargo +nightly-2026-08-22 nextest run -p tidb-stats` — 294/294 at walk
time; fmt clean.

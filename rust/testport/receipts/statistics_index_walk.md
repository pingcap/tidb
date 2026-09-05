# `pkg/statistics/index.go` — walk receipt

Comparison source: Go `origin/master` (`f2c346fe4f3`), index.go — 214
lines, 18 methods/functions.

## Inventory mapping

| Go function | Rust location | Status |
| --- | --- | --- |
| `Copy` | `index.rs::copy` / `copy_index` | ported |
| `ItemID` | `index.rs::item_id` | ported |
| `DropUnnecessaryData` | `index.rs::drop_unnecessary_data` | ported |
| `TotalRowCount` | `index.rs::total_row_count` | ported |
| `EvictAllStats` | `index.rs::evict_all_stats` | ported |
| `MemoryUsage` | `index.rs::memory_usage` | ported |
| `QueryBytes` | `index.rs::query_bytes` | ported |
| `GetIncreaseFactor` | `index.rs::increase_factor` | ported |
| `IsAllEvicted` | `index.rs::index_is_all_evicted` | ported |
| `IsCMSExist` | `index.rs::is_cms_exist` | ported |
| `IsEvicted` | `index.rs::is_evicted` | ported |
| `GetStatsVer` | `index.rs::stats_version` | ported |
| `IsAnalyzed` | `index.rs::is_analyzed` | ported |
| `String` | display impl | ported |
| `GetHistogram` / `GetTopN` | field accessors | absorbed |

## OPEN item

`IndexStatsIsInvalid` (index.go:132) — NO Rust equivalent found. Go has
three production callers, all in `pkg/planner/cardinality/`:
`cross_estimation.go:208`, `pseudo.go:87` (nil-Index form),
`row_count_index.go:52,504`. The gate both queues the index into the
async-load needed-items map (`AsyncLoadHistogramNeededItems.Insert`,
unless `coll.CanNotTriggerLoad` or restricted SQL) and reports the index
invalid (nil stats or `TotalRowCount == 0` or `coll.Pseudo`).

The Rust cardinality module (`cardinality/row_count_estimator.rs`,
`pseudo.rs`, `cross_estimation.rs`) handles the pseudo/evicted estimate
shapes but the async-load queueing + invalid gate is not visible there;
the planner's stats-usage collection (`rule_collect_plan_stats.rs`) is a
different mechanism. NEXT: trace whether the gate is absorbed into the
Rust estimation flow (pseudo fallbacks + needed-items collection) or
genuinely missing, and port if missing.

Everything else in the file is ported. No other divergence found.

## Validation

`cargo +nightly-2026-08-22 nextest run -p tidb-stats` — 294/294 at walk
time; fmt clean.

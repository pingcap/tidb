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

## Follow-up: discardable builder returns (2026-09-06)

The complete direct `pkg/statistics` package boundary was re-read at current
Go `origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`. The inventory is
33 direct artifacts (the BUILD input, OWNERS metadata, 15 production Go files,
and 16 Go test/benchmark/fuzz files) totaling 13,905 lines, plus the two
`testdata/integration_suite_{in,out}.json` fixtures (20 and 51 lines). There
are no direct generated outputs, platform variants, examples, or additional
build artifacts. Nested `asyncload`, `handle`, and `util` directories are
separate package boundaries and are tracked by their own receipts. Every
direct production/test function, fixture, and BUILD input was inventoried;
the builder cluster is mapped above to the complete `tidb-stats` owner.

Go's `NewSortedBuilder`, `(*SortedBuilder).Hist`,
`NewSequentialRangeChecker`, and `BuildColumn` return values may be ignored.
Their Rust counterparts (`SortedHistogramBuilder::new`, `histogram`,
`SequentialRangeChecker::from_ranges`, and `build_column`) incorrectly added
four Rust-only `#[must_use]` diagnostics. The focused
`go_builder_returns_may_be_ignored_like_go` regression invokes all four under
`#[deny(unused_must_use)]`; the pre-fix source failed with exactly four
diagnostics and the post-fix source passes. Rust-only helpers
`SortedHistogramBuilder::count` and `SequentialRangeChecker::from_ranges_in_place`
remain annotated. No runtime histogram, sampling, sorting, or error behavior
changed.

Validation for this Rust-only follow-up uses the `Ready` profile and is
recorded in `docs/operations/statistics-builder-audit-execplan.md`:

- focused deny-on-discard regression: pass;
- all 287 `tidb-stats` nextest tests: pass;
- `tidb-stats --all-targets` compile: pass;
- workspace formatting, `make lint`, and `git diff --check`: pass.

No Go source, Bazel metadata, Cargo manifest, or dependency file changed, so
`make bazel_prepare` is not required. Go test execution and live TiDB
integration were intentionally skipped per the Rust-only scope; existing Go
fixtures remain inventory evidence rather than edited artifacts.

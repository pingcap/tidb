# `pkg/statistics/handle/history` audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Atomic inventory

| Artifact | Lines | Git blob | Disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 22 | `0e7955cfddd870ea2be55f54976a424491d38cb7` | build metadata inventoried |
| `history_stats.go` | 210 | `2b76d7e7a7aee981fdb78e09afa860b2d1aee3a3` | unclaimed: runtime dependencies absent |

The package has no generated, platform-specific, test, benchmark, or fixture
artifacts.

## Package behavior and blockers

The package constructs a `StatsHistory` implementation over a real
`StatsHandle`. It chooses partition or table JSON dumping, handles nil data,
filters unenforced table IDs through initialized cache entries, reads the live
historical-stats session variable, and records each selected table through
wrapped transaction sessions. Its standalone metadata writer validates
nonzero table/version inputs, locks and reads the matching `stats_meta` row,
and writes the source metadata with microsecond time precision. Its snapshot
writer derives the table/partition version, converts the complete JSON table
to five-megabyte storage blocks, and inserts every ordered block with a shared
timestamp and duplicate-key update behavior.

Rust does not yet have the complete `pkg/statistics/handle/storage` owner or
the ordinary stats-handle/session runtime needed to preserve those calls and
transaction boundaries. A pure function cannot represent the package.

## Removed non-parity carrier

`historical_stats_version` exposed only the local version-selection branch
inside `RecordHistoricalStatsToStorage`. It accepted pre-extracted version
numbers, bypassing JSON conversion, block generation, timestamping, SQL,
errors, and partial-write behavior. The pinned package contains no tests, but
Rust added three tests for this extracted expression. The module and all three
tests were removed. The package remains explicitly unclaimed.

## Validation

WIP profile: removal of a disconnected carrier is checked through the affected
statistics owner gate.

- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs`
- `git diff --check`

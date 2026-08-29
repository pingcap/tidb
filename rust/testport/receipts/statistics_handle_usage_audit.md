# `pkg/statistics/handle/usage` parity audit

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

This is an audit receipt, not a package-completion claim. The parent Go
package is not transcreated until its ordinary session, schema, statistics
handle, transaction, SQL persistence, and integration-test paths exist and
are wired together in Rust.

## Atomic inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 55 | `48fc84076eff3b8d0241487fbb06b36f33bfab29` |
| `export_test.go` | 27 | `01469f608577f92c0e540c2c1e47c78c022881b6` |
| `index_usage.go` | 62 | `3016755169bfcddebbd6b5f9b955a524dc18235c` |
| `index_usage_integration_test.go` | 113 | `22adec8f549a8b7f10e58be26db20e33c86c9b4d` |
| `predicate_column.go` | 63 | `16238f0c707ae3f7372cd34c162f0ac7664826b1` |
| `predicate_column_test.go` | 263 | `58c1857a9ae4aaa28557262302499e126f006963` |
| `session_stats_collect.go` | 692 | `855d4269d65417e4fb561d180cbb93e8541ffc0c` |
| `session_stats_collect_test.go` | 403 | `d88e62a2129e28768ceea55cd63bdc93c2ce8a73` |

Subpackages are separate Go package units. The pinned `collector` and
`indexusage` packages are complete in their dedicated Rust crates; the
`predicatecolumn` subpackage remains an independent unclaimed dependency.

## Removed false surfaces

The former `tidb-stats` modules `index_usage_key`, `pending_delta_ids`, and
`predicate_column_query_mode` represented isolated facts from this package
while explicitly omitting the collector, session list, transaction, schema,
storage, and handle behavior. They had no production consumers and were
removed with their source-absent tests.

The former `predicate_column_queries` module similarly represented only SQL
text and argument joining from the separate `predicatecolumn` package. It was
removed with its tests instead of being counted as that package.

The ignored parent-package test functions in `statistics_part7_source.rs`
were empty gap markers, not executable behavior. That mixed batch file and
its function-level receipt were removed. Its only runnable scalar tests were
already covered by the source-owned `scalar_geometry_source.rs` and
`scalar_enum_source.rs` suites.

## Remaining integration boundary

The parent package must eventually provide one ordinary `StatsUsage` owner
that wires all of these source behaviors together:

- session-local table delta and predicate-column collection, sweeping, close
  deletion, reset, and earliest-init-time merge semantics;
- dump selection, batching, locked table/partition handling, global partition
  updates, transactional stats-meta persistence, and historical-meta records;
- column-usage sorting, 2,048-row batches, timezone conversion, and 12-hour
  last-used throttling;
- delegation to the complete index-usage collector and to a complete
  `predicatecolumn` package through the ordinary stats handle;
- all 14 source tests, including DDL/testkit and blocked pessimistic
  transaction behavior (the skipped concurrent writer has no executed source
  behavior).

No replacement leaf or cache-specific execution path was added.

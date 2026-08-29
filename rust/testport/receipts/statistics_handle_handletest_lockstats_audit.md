# `pkg/statistics/handle/handletest/lockstats` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 26 | `3844e6021902716601fc527a3e9f7c7155e95512` |
| `lock_partition_stats_test.go` | 543 | `bcc25057cc9102762d299053740aaf5d5235bce5` |
| `lock_table_stats_test.go` | 394 | `c4966a46c506e977cc29c445bbf8aa56b15cd294` |
| `main_test.go` | 34 | `52f60bbcecf9bbbcd9331f927f52a4683672d173` |

All 997 lines were read. The package has 21 tests and no benchmark.

## Go behavior

The suite executes table and partition LOCK/UNLOCK STATS through SQL, checks
warnings and `mysql.stats_table_locked`, preserves and applies deltas, updates
global counts, covers repeated and multi-object operations, and verifies DDL
cleanup/inheritance for drop, truncate, reorganize, exchange, and add
partition. It also covers negative locked deltas and a historical-stats
failpoint.

## Rust comparison and decision

The mixed Rust batch carrier contained 17 ignored empty functions from a
different origin/master snapshot and omitted four pinned table-lock tests.
The separately audited production `handle/lockstats` leaves do not provide
this SQL/session lifecycle. The empty entries were removed and the external
test package remains unclaimed pending its complete production dependencies.

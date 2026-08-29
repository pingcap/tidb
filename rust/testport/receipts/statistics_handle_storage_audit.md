# `pkg/statistics/handle/storage` package audit

Reference: TiDB Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete Go inventory

The package has 11 artifacts and 4,493 lines. Every artifact was read before
the Rust decision.

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 83 | `2eaed0a5e560f604fc51f90106f8ab09036db11b` |
| `dump_test.go` | 707 | `cbfb3f574b752ad124ea1c55d323b0b80856f06d` |
| `gc.go` | 362 | `5409d0517ee04b153b4b2e51f533f6bd2b045cea` |
| `gc_test.go` | 159 | `5c09443c9e385bbab586975242fd63afc4768980` |
| `json.go` | 341 | `2e13988ad8198164e629a911205961da908cfc5a` |
| `read.go` | 910 | `fb759e067972a9e0d38761fa64d0c6685ece28b2` |
| `read_test.go` | 240 | `2a6e3942bf888a79676ffb7fb3170a35788465ed` |
| `save.go` | 579 | `f671304687b40541f5423394c33cc7c903097ba9` |
| `stats_read_writer.go` | 689 | `bd9bc956407975c29d440614580e4dc95f946ada` |
| `stats_read_writer_test.go` | 226 | `fa755c995519b1ad2a4f7f468308b00d85c5e18e` |
| `update.go` | 197 | `0db14574eda367605d4d3b4df9c6d649007833b5` |

The four test files contain 28 tests and no benchmarks. They validate the
integrated package through a mock store/domain: transactional save and delta
update, storage GC, typed histogram/sketch reconstruction, lazy cache loads,
JSON dump/load and legacy compatibility, partition/global statistics,
predicate usage, historical metadata, concurrent workers, failpoints, and
slow-save recovery.

## Package behavior

Go's minimum behavioral unit is `statsReadWriter` plus its package functions.
It obtains pooled session contexts, owns transaction boundaries and start
timestamps, reads and writes all statistics system tables, converts typed
histogram bounds and sketches, updates the ordinary statistics cache, records
history, observes infoschema and partition state, performs memory and SQL-kill
accounting, and coordinates concurrent JSON loading. The small arithmetic and
SQL-formatting expressions are private implementation details of those paths.

## Rust comparison and decision

Rust had six public, disconnected leaves:

- `gc_batch_count.rs`
- `json_stats_version.rs`
- `stats_meta.rs`
- `stats_meta_save_sql.rs`
- `stats_meta_update.rs`
- `stats_read_writer.rs`

They accepted pre-decoded caller values and returned arithmetic decisions or
SQL strings. Repository-wide symbol tracing found no production consumers;
only their own tests called them. They did not execute a session transaction,
touch storage, reconstruct statistics, update the cache, record history, or
provide the Go `StatsReadWriter` contract. Rust also carried 28 ignored empty
functions for the actual Go tests. Together these 13 source/test files were
1,065 lines: 19 runnable tests exercised the invented leaves, while 28 ignored
tests exercised no behavior.

Completing this package now is not dependency-closed: the ordinary root
statistics handle, its interface family, cache/storage session owner, typed
statistics conversion surface, and several child services are still
unclaimed. Therefore the detached leaves, their tests, and the empty test
carrier were removed. The package remains explicitly unclaimed until those
dependencies can land with all 11 artifacts and all 28 tests atomically.

This removal does not delete the independent Rust statistics model or native
executor/session statistics paths whose owners are other Go packages.

## WIP validation

- `cargo check --locked -p tidb-stats` passed.
- `cargo nextest run --locked -p tidb-stats -E 'not test(/bench/)' --no-fail-fast`
  passed: 381 run, 381 passed, 154 skipped.
- `rustfmt --edition 2021 --check crates/tidb-stats/src/lib.rs` passed.
- `git diff --check` passed.

No Go or Bazel source changed, so `make bazel_prepare` was not required. This
is a WIP package audit, not a repository-wide Ready parity claim.

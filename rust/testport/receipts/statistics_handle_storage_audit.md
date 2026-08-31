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

## Rust ownership map and current decision

The former disconnected arithmetic/SQL-string leaves and ignored test carrier
were removed. Production ownership now spans native boundaries rather than a
single crate:

- `tidb-executor::load_stats` owns JSON/protobuf conversion and gzip blocks.
- `tidb-exec::cluster_stats_load`, `cluster_stats_dump`,
  `real_tikv_stats_dump`, and `cluster_stats_write` own canonical storage
  reads, snapshot transaction boundaries, dumps, and mutation plans.
- `tidb-exec::real_tikv_stats`, `real_tikv_load_stats`, and
  `real_tikv_analyze` own real transaction boundaries and cache refresh.
- `tidb-server` owns the MySQL client-local transfer and cluster session
  integration.

The current package pass closed three source-proven orchestration gaps:

- Partition LOAD STATS now converts and persists inside the same capped worker
  boundary as Go, returns the first error, recovers worker panics as errors,
  and retains Go's direct nonpartitioned path.
- `PersistStatsBySnapshot` now invokes the callback for absent
  nonpartitioned statistics, skips absent partition/global statistics, keeps
  schema order, visits global last, and stops on the first error.
- `UpdateStatsMetaVersionForGC` now refreshes both metadata versions in one
  real transaction and performs gated historical-meta recording afterward in
  a separate best-effort transaction.

The obsolete standalone `tidb-session` LOAD STATS implementation and its tests
were removed: it read a server-local path and published only an in-memory
planner cache, while pinned Go receives bytes through client-local transfer,
persists `mysql.stats_*`, and refreshes through the ordinary cache path.

This receipt is still an in-progress whole-package claim. In particular,
`UpdateStatsVersion` and `ChangeGlobalStatsID` remain to be integrated with
their owning flashback/partitioning DDL behaviors, and the 28-test behavioral
matrix has not yet completed its final package-wide validation gate.

## Current WIP validation

- `cargo test --locked -p tidb-exec cluster_load_stats::tests:: -- --nocapture`
  passed: 5 tests.
- `cargo test --locked -p tidb-exec real_tikv_load_stats::tests:: -- --nocapture`
  passed: 4 tests.
- `cargo test --locked -p tidb-exec cluster_stats_dump::tests:: -- --nocapture`
  passed: 3 tests.
- `cargo test --locked -p tidb-server --test all load_stats -- --nocapture`
  passed outside the filesystem/network sandbox: 2 tests.
- `cargo check --locked -p tidb-exec -p tidb-session -p tidb-server` passed.
- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `make lint` passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required. This is
an in-progress package receipt, not a package-completion or repository-wide
parity claim.

# `pkg/executor/internal/exec` Go-master parity audit

Comparison source: local `origin/master` at
`56970b286a362b1f0b150c7665453c8f5ff997a` (2026-09-23).

Status: **Incomplete.** This receipt records the atomic Go-package boundary and
current evidence. It does not claim that this package has been transcreated.

## Complete Go package inventory

`git ls-tree -rl origin/master pkg/executor/internal/exec` reports six tracked
artifacts: three production/build files and two test files plus the Bazel
target. The line counts, Git blob IDs, and SHA-256 values below are from that
exact source commit.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 61 | `5ceed4890d3a7f8b677c2a84762ee03b861e5645` | `5baf319e59f203c51d48646426275aaf21d0080d041fd204e95428bec18b1300` | Go library dependencies and 15-shard flaky test target |
| `adaptive_limit_controller.go` | 882 | `03a72b299542a680dc272e398ba7630677defd9a` | `3c4223424005b2d28f04c4d82bcd57772be037196edba400401fce0fd7c3b32e` | Controller feedback, reservations, stop/reset, and snapshots |
| `adaptive_limit_controller_test.go` | 613 | `259cca3b6291e5d7e20c4cc95b1ac6df548078d1` | `9e985f3b4dd28a3572c693756c4714c368f9664e0708ad48166d17cb76d5931d` | Nine controller tests and two benchmarks |
| `executor.go` | 481 | `f9f308a7bbc029919699d4fef3e98f3e492cf109` | `70a0134aec8188936e3d85ebe33ac86257b970fd726c9770cab32132b6410f92` | Executor interface, shared metadata, wrappers, tracing, and RU-v2 accounting |
| `indexusage.go` | 149 | `7a245cfa2169499295ef1fdc411b708129a745fc` | `a0a92ece9753ebe39c423d8a1e4f485af76e56238f56c4e0fb84b7ae46f03060` | Clustered-index selection and cop/point usage reporting |
| `indexusage_test.go` | 551 | `8010f9e1ec2884c9bbe6d83e0d44dad6c3177676` | `da0269e4ca053f02a48837990c3a54cb0dd49ab7fbf866daec80c8a1b2fcd266` | Six reporter tests, including data, partitions, global indexes, and handles |

The Bazel library compiles `adaptive_limit_controller.go`, `executor.go`, and
`indexusage.go`; the test target compiles the two `*_test.go` files with
`shard_count = 15` and `flaky = True`. `executor.go` uses the `initCap` and
`maxChunkSize` failpoints. The package has no generated Go files, build-tagged
variants, or external testdata files; its SQL tests create tables/data in the
test body. The source package's imported Go modules and internal dependencies
are listed in the checked-in `BUILD.bazel` above.

## `executor.go` behavior audit

| Go behavior | Rust owner found in this audit | Current parity evidence / gap |
| --- | --- | --- |
| `Open` / `Next` / `Close` wrapper recovery, timing, SQL-killer checks, tracing, and TopSQL registration | `Executor` methods, `driver::physical_builder::SqlKillerCheckExec`, `Session` statement driver, `StatementMemory`, `tidb-exec::runtime_stats`, and `tidb-util::topsql_*` are separate owners | Physical-plan construction wraps retained operators with Go-compatible panic recovery, before/after SQL-killer checks, and per-operator basic runtime statistics when `tidb_enable_collect_execution_info` is ON. Tests cover panic-safe timing, kill-before/after behavior, collector integration, and ordinary-query row/time accounting. Tracing, once-per-statement TopSQL registration, and process-info/EXPLAIN consumption remain open. `PhysicalCountExec` still meters the explain-analyze-specific counters. |
| `executorMeta` schema, result types, ID, and child tree | `ExecutorMeta` stores schema, result types, and ID; physical operators and `PhysicalPlan` own tree structure | Basic metadata maps. Rust `Executor` has no Go-equivalent all-children/indexed-child inspection or mutation interface; verify all consumers and explain/profiling behavior before deciding whether typed ownership fully replaces it. |
| `executorChunkAllocator` init/max capacities and pooled `NewChunk` / `NewChunkWithCapacity` | `StmtContext` captures chunk sizes; `ExecutorMeta::new_chunk`, `Chunk::new`, `Chunk::new_with_capacity`, and the `tidb-chunk` allocators/pool | Capacity snapshots and constructors exist, including allocator primitives. The executor metadata does not bind a per-session allocator, and Go's three-argument executor helper has no direct executor-trait equivalent. Allocation/reuse and per-caller mapping remain open. |
| `executorStats` runtime stats and top profiling | `tidb-exec::runtime_stats` and `tidb-util::topsql_state` / `topsql_stmtstats` / `topsql_reporter` | A statement-scoped `RuntimeStatsColl` now supplies shared per-plan basic stats to every built operator; collection is gated by the live instance setting, and the disabled path does not read clocks. TopSQL registration and feeding the collector into process-info/EXPLAIN consumers remain open. |
| `executorKillerHandler` | `StatementMemory::check()` in `SqlKillerCheckExec`, plus direct checks in long-running scan/sort/aggregation/join paths | Before/after checks now cover each executor built from a retained physical plan; standalone executors constructed outside that builder still rely on their caller/operator checks. Common timing and profiling remain open. |
| `BaseExecutorV2` child lifecycle and `Detach` | Rust-owned concrete executor trees and `Drop`/RAII | Rust close/open is implemented by each operator. Go's base `Detach` returns `(nil, false)`; no Rust detach API exists. The overall lifecycle contract still needs a caller-by-caller audit. |
| `BaseExecutor` session helpers, transaction delta, restricted system session acquisition/release | `tidb-session`, `tidb-syssession`, and storage/catalog DML paths | Restricted SQL services exist, but no executor-local `GetSysSession` / `ReleaseSysSession` pair was found. `UpdateDeltaForTableID` also needs an exact Rust callsite and transaction-delta mapping. |

## Required cross-package feature inventory

The feature commit `0b505ecc58` changes 18 files. Besides the three changed
files inside this package (`BUILD.bazel` and the two adaptive-limit files),
the feature and its tests touch:

| Go source or test artifact | Rust owner / parity surface |
| --- | --- |
| `docs/design/2026-07-31-adaptive-limit-scan.md` | `rust/docs/adaptive-limit-scan-parity-execplan.md` |
| `pkg/executor/builder.go` | `tidb-executor` physical builder and admission wiring |
| `pkg/executor/distsql.go` | `tidb-distsql` scan/task execution and accounting |
| `pkg/executor/distsql_test.go` | Rust Distsql source and request tests |
| `pkg/executor/join/index_lookup_join.go` | `tidb-executor` ordered lookup-join path |
| `pkg/executor/join/join_stats_test.go` | Rust join execution and runtime-stat tests |
| `pkg/executor/pkg_test.go` | `tidb-session` SQL execution tests in `tests_explain.rs` |
| `pkg/executor/select.go` | `tidb-session::stmt_ctx` statement reset and reporter gate |
| `pkg/session/upgrade_backfill_test.go` | Rust bootstrap upgrade acceptance tests (not yet executable) |
| `pkg/session/upgrade_def.go` | Rust versioned bootstrap upgrade runner (not present) |
| `pkg/sessionctx/vardef/tidb_vars.go` | `tidb-vardef` variable name/default |
| `pkg/sessionctx/variable/session.go` | `tidb-session` statement variable reset |
| `pkg/sessionctx/variable/sysvar.go` | `tidb-session` sysvar registration and runtime behavior |
| `pkg/sessionctx/variable/sysvar_test.go` | Rust sysvar catalog and execution tests |
| `pkg/sessionctx/variable/varsutil_test.go` | Rust variable default/reset tests |

No separate generated Go or testdata artifact is listed by that feature
commit. The upgrade test is still an acceptance requirement because it checks
old-cluster persistence, even though Rust has no executable versioned upgrade
runner yet.

## Rust ownership and current mapping

| Go artifact | Rust owner | Status |
| --- | --- | --- |
| `BUILD.bazel` | Rust workspace and crate manifests | Rust crate dependencies/build pass, but package-level build ownership and full package receipt remain incomplete. |
| `adaptive_limit_controller.go` | `tidb-executor/src/adaptive_limit.rs` | Controller logic and focused tests are implemented. Full package and benchmark gates remain open. |
| `adaptive_limit_controller_test.go` | Executor unit tests and `benches/adaptive_limit.rs` | Nine controller cases and both microbenchmark paths have equivalents. Rust executor integration tests cover direct ordered lookup and ordered index join. |
| `executor.go` | `tidb-executor/src/executor.rs` and runtime/build paths | Partial. Physical construction applies Go-compatible SQL-killer checks, panic-to-error recovery, and statement-gated per-operator basic timing/row statistics around Open/Next/Close. The trait still lacks Go's direct TopSQL/detach surface; tracing, RU-v2 accounting, and process-info/EXPLAIN consumption remain open. |
| `indexusage.go` | `tidb-executor/src/driver/index_usage_reporter.rs` and `tidb-stats-handle-usage-indexusage` | Reporter behavior is split across crates. The statement context now checks the live `tidb_enable_collect_execution_info` value on every statement, matching Go's OFF gate. |
| `indexusage_test.go` | Reporter, collector, and session tests | Partial. Disabled reporting, clustered-handle selection, and the cop-executor close/report path have focused Rust coverage. SQL exercises clustered reads and the hinted OR IndexMerge, all six real-data index/lookup/point shapes including hinted AND IndexMerge, the full partition-local range/selection/point/batch matrix, and static global-index point/batch reads; each represented SELECT also runs twice after prepare. The in-process backend has no mock-coprocessor RPC counters, and real TiKV reporting remains open. |

Go index-usage test mapping:

| Go test | Rust evidence | Status |
| --- | --- | --- |
| `TestIndexUsageReporter` | Reporter rules and collector tests | Direct API rules are covered; complete runtime-stat-to-global-publication SQL equivalent remains unverified. |
| `TestIndexUsageReporterWithRealData` | `tidb_index_usage_records_real_data_reads` mirrors both `IndexReader` cases, `IndexLookUp`, hinted AND `IndexMerge`, `Point_Get`, and `Batch_Point_Get`, including two executions of each prepared SELECT. It checks plans/results, index identity, query/row totals, and percentage buckets. `cop_executor_reports_live_scan_counts_on_close` covers counter-to-sample publication. The in-process backend reports zero KV RPCs; no TiKV-backed SQL reporter test yet. | Partial |
| `TestIndexUsageReporterWithPartitionTable` | `tidb_index_usage_records_a_partition_local_index_read` uses the Go test's four range partitions and 100 rows. It verifies the two-partition `PartitionUnion` range, residual-filtered single-partition `IndexReader`, `Point_Get`, and `Batch_Point_Get`, results for every case, and two executions of each prepared SELECT. Aggregated query/row totals and percentage buckets match Go's samples. Rust's in-process store reports zero KV RPCs, so this does not cover Go's mock-coprocessor request counter. | Partial |
| `TestIndexUsageReporterWithGlobalIndex` | `tidb_index_usage_records_a_partitioned_global_index_point_read` uses 100 rows across four partitions and verifies static-pruning `Point_Get` and `Batch_Point_Get` plans/results, plus two executions of each prepared statement. It checks logical-table/global-index query and row accounting. The in-process backend contributes zero KV RPCs; Go's mock-coprocessor request totals are not comparable here. | Partial |
| `TestDisableIndexUsageReporter` | `disabled_reporter_does_not_record_index_usage` and `stmt_ctx::tests::index_usage_collector_tracks_execution_info_switch_per_statement` | Focused OFF/ON path covered; the new regression failed before the fix and passes after it. |
| `TestIndexUsageReporterWithClusterIndex` | `tidb_index_usage_records_clustered_handle_and_primary_index_reads` mirrors the integer clustered-handle and common-handle `TableReader` ranges, nonclustered primary `IndexRangeScan`, integer/common `Point_Get`, integer/common `Batch_Point_Get`, and forced OR `IndexMerge` over clustered primary and secondary indexes. Each SELECT runs directly and twice prepared; results, plans, per-index query/row totals, and percentage buckets are checked. | Partial |

The package is not accepted until every `executor.go` behavior above is mapped,
upgrade behavior through TiDB bootstrap version 287
is implemented and tested, and the package-level validation gates pass. Go's
current `upgradeToVer287` combines the adaptive-limit OFF backfill with the
TTL task's `scan_index_id` DDL migration; Rust is still at version 263 and has
no executable versioned-upgrade dispatcher. Go also registers migrations 277
through 286 after Rust's current baseline, so simply adding a one-off 287
backfill or advancing the version would skip required cluster upgrades. Old
clusters missing `tidb_enable_adaptive_limit_scan` must persist the Go-compatible
OFF value in `mysql.global_variables`. Workload performance for sysbench,
TPC-C, TPC-H, and YCSB also remains unverified.

## Validation evidence and remaining gates

Previously, the complete Go package test set passed against pinned
`origin/master` sources through a temporary overlay and the failpoint wrapper:

```text
GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh \
  pkg/executor/internal/exec \
  -overlay=/private/tmp/tidb_adaptive_overlay.json -run 'Test' -count=1
```

Rust evidence for the current worktree:

```text
cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib adaptive_
# 17 adaptive executor tests passed in the previous validation run.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib stmt_ctx::tests::
# 14 statement-context tests passed, including the new ON→OFF→ON regression.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib driver::index_usage_reporter::tests::
# 6 reporter tests passed, including close-path publication from live counts.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib index_usage -- --nocapture
# 6 reporter tests passed after adding the clean local-scan counter path.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tidb_index_usage_records_a_clustered_point_read -- --nocapture
# passed: a clustered-handle point SELECT publishes one query and one row;
# in-process storage reports zero KV RPCs because no RPC is issued.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tidb_index_usage_
# 6 SQL/catalog tests passed: collector read, clustered point read, cluster
# index and handle matrix, all six real-data index/lookup/point shapes including
# hinted AND IndexMerge, partition-local range/selection/point/batch cases, and
# static global-index point/batch
# cases with prepared reads.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tidb_index_usage_records_clustered_handle_and_primary_index_reads -- --nocapture
# passed: Go's supported clustered-index cases across 100 rows. Integer handle
# usage: query_total=9, row_access_total=225; common handle: 9/243; nonclustered
# primary index: 3/228. Direct and twice-prepared reads produce the Go buckets.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tidb_index_usage_records_real_data_reads -- --nocapture
# passed: six Go real-data cases with prepared executions. idx_1 reports
# query_total=18, row_access_total=672, buckets [0, 3, 3, 3, 0, 9, 0]; idx_2
# reports query_total=3, row_access_total=60, buckets [0, 0, 0, 0, 3, 0, 0].

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-planner --lib index_merge
# 9 IndexMerge planner tests passed, including explicit-name semantics and
# enumerated-path eligibility.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tidb_index_merge_is_not_planned_for_local_temporary_tables -- --nocapture
# passed for both hinted OR and hinted AND queries on a LOCAL temporary table.

cargo check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-server -p tidb-executor
# passed with the planner metadata changes wired through catalog construction.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tidb_index_usage_records_a_partition_local_index_read -- --nocapture
# passed: Go's 4-partition/100-row query matrix, 12 executions, 255 accessed
# rows, and percentage buckets [0, 3, 3, 0, 3, 3, 3].

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib adaptive_limit_
# 3 SQL-level adaptive-limit parity tests passed.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --lib driver::physical_builder::tests
# 19 physical-builder tests passed, including both SQL-killer checks.

cargo test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tidb_index_usage_
# 6 SQL/catalog tests passed with per-operator SQL-killer checks enabled.

cargo clippy --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --all-targets
# passed with existing workspace warnings.

rustfmt --edition 2021 --check rust/crates/tidb-session/src/stmt_ctx.rs
git diff --check
# passed.

make lint
# passed after retrying with network access for the pinned Go linter.
```

`TestIndexUsageReporterWithClusterIndex` has also been observed failing on the
checked-out Go branch but passing on the pinned-master overlay; preserve that
distinction when comparing results. The forced OR `IndexMerge` SQL case now
passes with the explicit hint on analyzed stats. The real-data hinted AND merge
also passes with the explicit index list. Both merge builders use only
session-filtered enumerated paths; AND partials require explicit names and
TiKV-pushable predicates. The planner also refuses both merge types on LOCAL
temporary tables, as Go does.
The full Rust `tidb-executor --lib` suite passes 1,436 tests with no failures.
An earlier 1,428/1,431 attempt reported three ranger comparisons, but those
failures do not reproduce in the current full suite. Package acceptance remains
open for TiKV-backed SQL reporter counts, the version-287 old-cluster upgrade,
Bazel execution, and sysbench/TPC-C/TPC-H/YCSB workload results. No Go or Bazel source was changed in this worktree, so
`make bazel_prepare` was not triggered. Do not commit or report this package as
transcreated while these acceptance items remain open.

Revision note (2026-09-23): a fresh fetch from the client-rust scratch clone
confirmed upstream master remains `32dec1837ee9686f1a32861a0b40f2ed880be3c7`,
matching the vendor sync log. The tracked vendor and patched scratch source
match after excluding build artifacts, so no dependency changes were needed.
The executor lifecycle audit reads the pinned `origin/master` source at
`56970b286a362b1f0b150c7665453c8f5ff997a`, not the older Go file in this
worktree's `hparser-integration` branch; the six package artifacts are
unchanged from `bfcc826f420238c574b30758551117320da3bf9a`. Rust now applies the
pinned package's per-operator before/after SQL-killer checks, panic recovery,
and basic runtime timing/row collection through the physical-builder wrapper.
The current full executor crate test run passes 1,436/1,436; the three ranger
comparisons reported by an earlier run no longer fail in that suite.
Static global-index point/batch SQL mirrors Go's 100-row, four-partition case
and executes each shape twice through PREPARE/EXECUTE. Guard regressions
confirm incompatible index hints, `sql_select_limit`, and `FIX_52592` preserve
the normal static-pruning plan. Partition-local SQL now covers Go's
two-partition range, residual selection, point, and batch queries, including
two prepared executions each. Real-data SQL covers both index readers, lookup,
point, batch, and forced AND-index-merge access; local index-entry counts are
captured before filters. The real-data query records `idx_1` totals of 18 queries
and 672 rows with buckets `[0, 3, 3, 3, 0, 9, 0]`, and `idx_2` totals of 3
queries and 60 rows with buckets `[0, 0, 0, 0, 3, 0, 0]`. Cluster, partition,
and real-data Go-compatible totals and percentage buckets pass. Go's
mock-coprocessor request counts and package-level acceptance remain open.

Validation update (2026-09-23): `built_executor_recovers_open_next_and_close_panics`
failed before the lifecycle wrapper recovered panics, then passed after recovery
was added. `metered_executor_records_lifecycle_time_when_child_panics` verifies
Go's deferred timing behavior for all three calls. The targeted
`metered_executor_records_sql_killer_precheck_failures` failed with the prior
metering order (`loops = 0`) and passes with metering outside the killer check.
The targeted `driver::physical_builder::tests` suite passes 22/22, the executor
worker-panic regression passes, `cargo clippy --offline --locked --manifest-path
rust/Cargo.toml -p tidb-executor --all-targets` and the server/session check pass,
and touched-source rustfmt plus `git diff --check` pass. The full executor
library suite now passes 1,436/1,436; an earlier 1,428/1,431 result is no
longer reproducible.
`cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-exec
--lib executor_runtime_source_shares_go_basic_stats_with_statement_collector`
passes 1/1, and
`cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session
--lib index_usage_collector_tracks_execution_info_switch_per_statement`
passes 1/1. The full `tidb-executor --lib` suite passes 1,436/1,436. The
client-rust dependency remains at the verified newest master
`32dec1837ee9686f1a32861a0b40f2ed880be3c7`; no vendor or manifest changes were
needed. TopSQL registration, tracing, RU-v2 accounting, process-info/EXPLAIN
consumption, upgrade execution, TiKV request counters, Bazel execution, and
workload measurements remain open.

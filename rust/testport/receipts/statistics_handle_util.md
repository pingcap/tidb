# `pkg/statistics/handle/util` — complete package transcreation

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete inventory

The root package has exactly seven artifacts and 927 lines, all read in full
from the detached Go-master worktree before this edit:

- `BUILD.bazel` — 58 lines of library/test source and dependency metadata;
- `auto_analyze_proc_id_generator.go` — 125 lines covering process-ID
  allocation, the concurrent active-process set, its package global, and
  tracker callback ordering;
- `lease_getter.go` — 52 lines of signed atomic statistics-lease storage;
- `pool.go` — 66 lines for the reusable worker pool and system-session facade;
- `table_info.go` — 161 lines of table/partition lookup and the
  schema-versioned V1 partition cache;
- `util.go` — 306 lines of constants, statistics execution context,
  session-variable synchronization, transaction handling, executor dispatch,
  timestamp conversion, and special-global-index classification;
- `util_test.go` — 159 lines containing all four source tests.

There is no `doc.go`, fixture, benchmark, fuzz target, generated source/input,
or platform/build-tag variant in the root package. The nested
`pkg/statistics/handle/util/test` directory has two artifacts and 49 lines and
is a distinct Go package with its own receipt and ExecPlan.

## Rust ownership and integration decision

`rust/crates/tidb-stats-handle-util` is the single atomic Rust owner.
`tidb-stats` re-exports the owner, and the concrete internal-session adapter
lives at the existing server integration boundary. The former five narrowed
`tidb-stats` policy fragments and their supplemental tests remain removed.

The owner preserves:

- generator delegation, idempotent active-process tracking, and add/delete
  before callback ordering;
- signed lease values;
- Go worker-pool close semantics, the 32,767 idle-worker bound, 60-second idle
  recycling, completion of accepted work, ignored post-close work, and the
  facade rule that `Close` leaves the system-session pool open;
- V1 ordinary fallback scans, V1 init-stats cache lookup, V2 indexed lookup,
  and cache replacement on schema-version change;
- Go master's exact session-variable read/mutation order, including refreshing
  `tidb_analyze_store_batch_size` immediately after `tidb_analyze_version`,
  partial mutations on error, and time-zone propagation to statement context;
- panic recovery, failed-session quarantine, optional pessimistic transaction
  wrapping, and original-error precedence when rollback also fails;
- ordinary, restricted, test-mock, caller-option, and explicit-context SQL
  execution routes, with the caller context preserved by
  `ExecWithOptsWithCtx`;
- a typed TiKV request context carrying the internal statistics foreground
  source, current-session option identity, `ExecRowsTimeout` at the
  `ExecRowsWithCtx` boundary, transaction start TS, signed duration
  conversion, and model-backed global-index classification.

The stale Rust-only statistics-session mutation of
`tidb_merge_partition_stats_concurrency` was removed because it is absent
from the pinned Go-master package. The variable remains available to its real
statistics consumers; only this package-owned synchronization policy was
deleted. The external Go worker library is represented natively instead of
being exposed as a second Rust API.

## Original-test mapping

| Go test | Rust test | Result |
| --- | --- | --- |
| `TestIsSpecialGlobalIndex` | `util::tests::timestamp_and_special_global_index_use_model_values` | local/global, virtual generated, and prefix cases use real model values |
| `TestCallSCtxFailed` | `util::tests::call_with_sctx_releases_failed_session_and_synchronizes_timezone` | failed sessions are quarantined |
| `TestCallWithSCtxSyncsStmtCtxTimeZone` | same test | session and statement time zones agree before callback execution |
| `TestTableItemByIDForInitStatsAvoidsV1PartitionScan` | `table_info::tests::v1_init_stats_item_lookup_uses_partition_cache_not_scan` | cached V1 init lookup avoids partition scans |

Focused regressions additionally pin the current Go-master delta:

- `analyze_store_batch_size_is_loaded_before_historical_stats` failed before
  the fix because Rust never read the variable and passes after the exact
  ordered refresh was implemented;
- `exec_rows_timeout_failpoint_returns_source_error` was extended to call
  `exec_rows_with_ctx` directly and failed before the failpoint moved to that
  source boundary;
- `exec_with_opts_with_ctx_forwards_the_caller_context` proves the newly
  restored explicit-context entry point preserves caller context identity;
- `session_variables_are_synchronized_in_source_order` now rejects the
  removed Rust-only merge-concurrency read and asserts the batch-size value.

## Validation

Profile: Ready. This is one atomic package batch inside the continuing
repository-wide audit, not a whole-repository parity claim.

- Current-branch and exact detached Go-master package suites passed through
  `tools/check/failpoint-go-test.sh` with pinned Go 1.25.10.
- The two focused regressions were observed failing before their production
  changes and passing afterward.
- `cargo test -p tidb-stats-handle-util --features intest,failpoints`: 21
  passed, including doc tests.
- `cargo test -p tidb-stats --test all`: 258 passed.
- `cargo check -p tidb-server`: passed; warnings are pre-existing and outside
  this package batch.
- Workspace Rust formatting, the pinned repository lint gate, scoped diff
  hygiene, and commit integrity passed.

No Go, Bazel, or module file changed in this batch, so `make bazel_prepare`
was not required.

## Risk and unverified boundaries

- Correctness: the added internal-session setter validates the fetched global
  value through the ordinary session-variable path and propagates errors as
  Go does. Both the generic owner and concrete server adapter are compiled.
- Compatibility: the public owner gained the missing source API and its sole
  concrete trait implementation changed with it; there is no second owner.
- Performance: one additional global-variable read and session update matches
  Go master; the stale extra merge-concurrency read was removed.
- Broad integration and RealTiKV suites were not run because this package's
behavior is covered by its package, consumer, and server compile gates.

## 2026-09-02 Go-master source restoration

Against fetched Go master `78cac443a4f46c13bfe27eb247b5c80657952547`, the
working branch lacked the explicit-context `ExecWithOptsWithCtx` entry point
and the ordered analyze-store-batch refresh. The utility source now forwards
the caller context to the restricted executor, moves the timeout failpoint to
the context-aware row boundary, refreshes `tidb_analyze_store_batch_size`
immediately after `tidb_analyze_version`, and removes the stale
Rust-only merge-concurrency mutation. `TestExecWithOptsWithCtxForwardsCancellation`
and the full failpoint-wrapped Go package suite pass in the detached
Go-master worktree. The current branch's transient dependency compile failures
are from the unrelated in-progress `pkg/distsql`/`pkg/execdetails` synchronization.

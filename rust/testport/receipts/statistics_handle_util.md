# `pkg/statistics/handle/util` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly seven artifacts, all read in full and byte-compared
against the pin before implementation:

- `BUILD.bazel` — the Go library/test dependency and source inventory;
- `auto_analyze_proc_id_generator.go` — process-ID allocation, the concurrent
  active-process set, its package global, and tracker callback ordering;
- `lease_getter.go` — signed atomic statistics lease storage;
- `pool.go` — the reusable worker pool plus system-session pool facade;
- `table_info.go` — table/partition lookup and the schema-versioned V1
  partition cache;
- `util.go` — constants, statistics execution context, session-variable sync,
  transaction wrapper, executor dispatch, timestamp conversion, and special
  global-index classification;
- `util_test.go` — four original package tests.

There is no `doc.go`, fixture, benchmark, generated source/input, or
build/platform variant. `pkg/statistics/handle/util/test` is a distinct Go
package and is not part of this claim.

## Rust ownership and integration decision

`rust/crates/tidb-stats-handle-util` is the one atomic package owner. The
previous five narrowed `tidb-stats` fragments and their supplemental tests
were removed instead of retained as alternate behavior. `tidb-stats`
re-exports the package owner so existing downstream paths use the same
implementation.

The Rust boundary uses the complete consumed capabilities of the existing
model, SQL-executor, mock-executor, and system-session owners. It preserves:

- generator delegation, idempotent active-process tracking, and add/delete
  before callback ordering;
- signed lease values;
- Go worker-pool close semantics, 32,767 idle-worker bound, 60-second idle
  recycle, completion of accepted work, ignored post-close work, and the
  facade rule that `Close` does not close the system-session pool;
- V1 ordinary fallback scans, V1 init-stats cache lookup, V2 indexed lookup,
  and cache replacement on schema-version change;
- the exact global-variable read/mutation order, partial mutations on error,
  time-zone-to-statement-context propagation, panic recovery, failed-session
  quarantine, optional pessimistic transaction wrapping, and original-error
  precedence when rollback also fails;
- normal, restricted, test-mock, and caller-option SQL execution routes,
  current-session option identity, `ExecRowsTimeout`, transaction start TS,
  signed duration conversion, and model-backed global-index classification.

The external Go worker library is represented natively rather than exposed as
a second Rust API. Go logging and panic-counter instrumentation are cross-cutting
observability supplied by Go's `util.Recover`; Rust preserves the package's
control-flow result and session cleanup behavior without inventing a new local
metric family.

## Original-test mapping

| Go test | Rust test | Result |
| --- | --- | --- |
| `TestIsSpecialGlobalIndex` | `util::tests::timestamp_and_special_global_index_use_model_values` | actual model values cover local/global, virtual generated, and prefix cases |
| `TestCallSCtxFailed` | `util::tests::call_with_sctx_releases_failed_session_and_synchronizes_timezone` | failed session is not reused |
| `TestCallWithSCtxSyncsStmtCtxTimeZone` | same test | session and statement time zones agree before callback |
| `TestTableItemByIDForInitStatsAvoidsV1PartitionScan` | `table_info::tests::v1_init_stats_item_lookup_uses_partition_cache_not_scan` | cached V1 init-stats lookup avoids the partition scan |

Package-owned tests additionally pin the remaining production branches,
including tracker callbacks, signed leases, worker/facade close behavior, V1
and V2 lookups, session-variable error ordering, panic recovery, transaction
error precedence, executor routing, start TS, and failpoint behavior.

## Validation

Profile: WIP. This completes one atomic Go package inside the continuing
repository-wide parity goal; it is not a repository Ready claim.

- Complete pinned-package inventory/diff gate: passed.
- `GOCACHE=/private/tmp/tidb-go-cache GOTOOLCHAIN=go1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/statistics/handle/util`: passed.
- `cargo test -p tidb-stats-handle-util`: 17 passed.
- `cargo test -p tidb-stats-handle-util --features intest,failpoints`: passed.
- `cargo test -p tidb-stats --test all`: passed.
- `cargo check -p tidb-stats-handle-util -p tidb-stats -p tidb-exec -p tidb-session`: passed.
- Scoped `cargo fmt -p tidb-stats-handle-util -p tidb-stats --check`: passed.
- `git diff --check`: passed.

No Go or Bazel source changed, so `make bazel_prepare` is not required.

## Risk and unverified boundaries

- Correctness: package behavior is covered over its real Rust dependency
  contracts; concrete production session wiring remains owned by later
  consumer packages.
- Compatibility: the new crate is re-exported by `tidb-stats`; the removed
  modules had no independent Go package identity.
- Performance: worker reuse matches the Go dependency policy; no Rust-only
  crossover or shortcut was added.
- Repository-wide `make lint` and broad integration suites are deferred to
  the mandatory Ready profile after the full parity goal is complete.

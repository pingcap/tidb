# `pkg/statistics/handle/usage` — complete package transcreation

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (Go `master` at
the audit boundary).

## Complete inventory

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 55 | `48fc84076eff3b8d0241487fbb06b36f33bfab29` | `49c4cfc7ed112bea47528f09a75adb82c65e801859ca5cca5fc740cf436c9fdd` |
| `export_test.go` | 27 | `01469f608577f92c0e540c2c1e47c78c022881b6` | `1f53a5045b9728078f3627870eade98de9584bb58acd6b83090a6e7963312c46` |
| `index_usage.go` | 62 | `3016755169bfcddebbd6b5f9b955a524dc18235c` | `9f53a9a924f5bae99e75103d9326443cf237ead4dd58873ad107238198b0fc54` |
| `index_usage_integration_test.go` | 113 | `22adec8f549a8b7f10e58be26db20e33c86c9b4d` | `de5b14e27f60da970b3aba595e2c415fc1c4d1150a328550f37cf664d24e16bd` |
| `predicate_column.go` | 63 | `16238f0c707ae3f7372cd34c162f0ac7664826b1` | `e1690dfaa4cf01fc6071400e0c8ddaf2f71eef1c7b31cb4aef3bf1aec650ec3f` |
| `predicate_column_test.go` | 263 | `58c1857a9ae4aaa28557262302499e126f006963` | `e1189929295a256292ab1b4056f9aa1ece8c1e15adacae26d6e13d910421fa20` |
| `session_stats_collect.go` | 692 | `855d4269d65417e4fb561d180cbb93e8541ffc0c` | `9b6cabbe3f3cdbddd34143447787b2998f0e13798f536a450eb18e34be5ddf71` |
| `session_stats_collect_test.go` | 403 | `d88e62a2129e28768ceea55cd63bdc93c2ce8a73` | `80c7a675a0a86a4207599aa5730530b96e156dc1c7dc813626745de08b1a2f7c` |

There is no `doc.go`, fixture, generated source/input, example, fuzz target, or
build/platform variant. Subpackages remain separate atomic claims; collector,
indexusage, and predicatecolumn have complete receipts.

The eight root artifacts total 1,678 lines. Every production function,
support helper, test, fixture/build decision, and BUILD attribute was read; the
current checkout is byte-identical to this pin.

## Native ownership and source behavior

The complete Go package maps to three native boundaries:

- `tidb-stats-handle-usage` owns table-delta and predicate-usage maps, session
  collectors and sweeping, dump guards, eligibility, batch constants, lock and
  partition expansion, and indexusage delegation;
- `tidb-exec::{cluster_predicate_column,cluster_stats_write}` owns metadata
  reads and transactional write plans;
- `tidb-server::cluster_session_node` owns restricted-session-equivalent
  snapshots, per-batch commits, workers, schema exclusions, historical-meta
  dispatch, and shutdown flushing.

The combined path preserves Go's latest predicate timestamp and earliest
nonzero delta initialization time, deleted-session sweep, sorted deadlock-safe
ordering, 2,048-row column batches, 100,000-table delta batches, one-hour and
ratio eligibility, twelve-hour column timestamp throttling, partition/global
lock rules, and nonfatal per-table historical-meta recording.

Two audited error/encoding details are explicit:

- if a later column batch fails, every entry is merged back for retry, including
  rows from earlier batches that already committed, exactly like Go's one
  deferred `colMap` merge;
- collected `SystemTime` values are truncated to whole seconds before storing
  TIMESTAMP(6), matching `t.UTC().Format(types.TimeFormat)` rather than adding
  Rust-only microsecond precision.

`SessionStatsItem::update_col_stats_usage` accepts the caller's one timestamp,
matching `session.UpdateColStatsUsage`; the unused source-absent
`StatsUsageHandle: Default` surface was removed.

The 2026-09-02 re-audit found no source-vs-owner behavior gap or Rust-only
production path. The native split is an ownership decision only: collection
state remains in `tidb-stats-handle-usage`, storage planning in `tidb-exec`,
and restricted transactional persistence in `tidb-server`.

## Original test and support disposition

- `export_test.go`'s duration accessors are represented by private Rust test
  setters and the dump-eligibility source test; they are not production API.
- `TestGCIndexUsage` runs as
  `source_index_usage_integration_test_gc_index_usage` over real shared
  `TableInfo` index metadata.
- predicate cleanup, collected-column growth, persisted-option enabled and
  disabled behavior, and the no-predicate index/no-index/primary-key cases run
  in `analyze_commit_size_source` and `tests_analyze`, including mandatory
  index/primary columns and the exact warning.
- first touch, no bump inside twelve hours, and bump after an old value run in
  `analyze_commit_size_source` through actual system-table plans.
- `TestDumpColStatsUsageWriter_ConcurrentMultiTables` is unconditionally
  skipped by the pinned Go test before setup. Rust preserves its batching and
  throttle behavior but does not invent an executable concurrency claim.
- initialization-time persistence and earliest-time merge run in the usage
  crate; the latter exercises the blocked/overlapping dump invariant directly.
- a server regression proves later-batch failure requeues all column entries,
  and a second proves Go's whole-second formatting into TIMESTAMP(6).

The earlier isolated `tidb-stats` usage fact modules, SQL-string-only
predicatecolumn module, empty gap-marker tests, duplicate aggregate tests, and
source-absent convenience constructor remain removed. No alternate cache,
leaf persistence, or test-only execution path was introduced.

## Validation

Profile: Ready, because this receipt makes the package-level completion claim.
The exact commands and their outcomes are recorded in the living ExecPlan at
`rust/docs/statistics/usage-package-parity-execplan.md`. No Go source, Go
imports, Bazel metadata, module dependency, or top-level Go test changed, so
`make bazel_prepare` is not required.

The 2026-09-02 authority refresh also passed the current and detached Go
package suites through the failpoint wrapper, the seven native owner tests, the
focused server persistence/timestamp regressions, the owner consumer checks,
Rust formatting, pinned `make lint`, and `git diff --check`. A broader
statistics-module probe exposed one unrelated baseline failure in
`analyze_clauses_this_node_does_not_run_are_refused_by_name`; the usage-specific
tests remained green.

## 2026-09-06 return-contract alignment follow-up

The eight-artifact Go inventory above is unchanged from the receipt pin through
current `origin/master` `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.
Per the requested Rust-only scope, no Go file was read or edited and no Go test
was executed in this follow-up. The complete immediate Rust owner was read
before editing: the 15-line `Cargo.toml` and pre-change 877-line `src/lib.rs`,
including all seven existing tests. It has no build script, generated or
platform-specific source, fixture, benchmark, example, fuzz target, or
external test target. Direct callers at the `tidb-server` and `tidb-session`
boundaries were also classified before changing the public annotations.

Sixteen direct Go-shaped constructors, getters, snapshots, dump selection, and
eligibility returns no longer impose Rust-only `#[must_use]` diagnostics:
`TableDeltaMap::{new, get_delta_and_reset, snapshot}`,
`StatsUsage::{new, get_usage_and_reset}`,
`SessionStatsList::{new, new_session_stats_item, session_table_delta,
session_stats_usage}`, `TableDeltaDump::pending_table_ids`,
`need_dump_stats_delta`, and
`StatsUsageHandle::{new, new_session_stats_item,
new_session_index_usage_collector, get_index_usage, session_stats_list}`.
The focused `go_usage_returns_may_be_ignored_like_go` regression failed before
the fix with exactly sixteen diagnostics
(`/tmp/tidb-stats-usage-prefix.log`) and passes afterward.

Eight annotations remain on native Rust boundaries rather than direct
discardable Go API contracts: partition-update expansion; the two RAII dump
guard constructors; the column-entry view; the `Option`-returning table lookup;
two private configuration reads; and the cross-crate index collector adapter.
These guards protect ownership or native integration behavior and are not
claimed as Go surface parity.

Ready validation for this Rust-only follow-up:

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-usage --lib go_usage_returns_may_be_ignored_like_go -- --test-threads=1` (1 passed);
- `cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-usage --lib --test-threads=1` (8 passed);
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-stats-handle-usage --all-targets --quiet`;
- `rustfmt +nightly-2026-08-22 --check --edition 2021 rust/crates/tidb-stats-handle-usage/src/lib.rs`;
- `make lint`;
- `git diff --check`.

Only compile-time return enforcement changed. Collection, dumping, rollback,
timestamp, locking, and index-GC behavior are unchanged, so correctness,
compatibility, and performance risk is limited to callers that intentionally
discard these values. No Go/Bazel/Cargo/module/import graph changed, so
`make bazel_prepare` was not required.

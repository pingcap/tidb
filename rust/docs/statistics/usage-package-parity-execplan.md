# Complete pinned statistics usage packages and ordinary persistence wiring

This ExecPlan is a living document maintained under `PLANS.md`.

## Purpose / Big Picture

Pinned TiDB commit `c6054025ed4c32ab3672a2a24ea46892714d21ec` defines `pkg/statistics/handle/usage` as the owner of session table deltas, predicate-column usage, index-usage delegation, dump selection, batching, persistence, and garbage collection. Its imported `pkg/statistics/handle/usage/predicatecolumn` package owns the corresponding storage reads, cleanup, and replacement writes. Rust already has most of these behaviors split across `tidb-stats-handle-usage`, `tidb-exec`, and the cluster session node, but the existing audit deliberately makes no package-completion claim. This work verifies both complete package inventories, removes remaining Rust-only API behavior, fills source API gaps, and validates the ordinary end-to-end paths.

## Progress

- [x] (2026-08-30) Inventory and read every pinned root-usage production, test, support, and Bazel artifact.
- [x] (2026-08-30) Inventory and read every pinned predicatecolumn production and Bazel artifact.
- [x] (2026-08-30) Audit existing Rust owners and ordinary server/exec persistence consumers.
- [x] (2026-08-30) Align remaining session collector APIs and remove source-absent public conveniences.
- [x] (2026-08-30) Verify predicatecolumn load/table-load/cleanup/get/save behavior and publish its atomic receipt.
- [x] (2026-08-30) Verify all root usage tests and integration decisions, replacing the audit-only receipt with a completion receipt.
- [x] (2026-08-30) Run the Ready validation profile and self-review.
- [x] (2026-08-30) Prepare the validated package unit for commit, synchronization, and push to `origin/hparser-integration`.
- [x] (2026-08-30) Re-audit every predicatecolumn branch against the pinned
  source; represent absent latest-infoschema tables separately from empty
  schemas and reproduce `CONVERT_TZ`'s invalid-zero-to-NULL behavior on reads,
  predicate selection, and replacement writes.
- [x] (2026-09-02) Re-audit all eight root-usage artifacts (1,678 lines) against
  Go master `c6054025…`, re-read the complete native owner and server/exec
  persistence seams, and confirm no source-vs-owner behavior gap or Rust-only
  production path remains.
- [x] (2026-09-02) Refresh the root-usage receipt with exact line/blob/hash
  inventory and keep the previously recorded Ready validation evidence.

## Surprises & Discoveries

- Observation: the root package's production behavior already spans three native Rust owners rather than one crate.
  Evidence: collection lives in `tidb-stats-handle-usage`, row read/write planning in `tidb-exec::{cluster_predicate_column,cluster_stats_write}`, and transactional batching/workers in `tidb-server::cluster_session_node`.
- Observation: `SessionStatsItem::update_col_stats_usage` currently creates its timestamp internally, while pinned Go accepts the session-created timestamp explicitly.
  Evidence: pinned `session_stats_collect.go:480` and Rust `tidb-stats-handle-usage/src/lib.rs:271`.
- Observation: the crate exposes `Default` for `StatsUsageHandle`, but pinned Go only constructs `statsUsageImpl` through `NewStatsUsageImpl(statsHandle)`.
  Evidence: pinned `predicate_column.go::NewStatsUsageImpl` and no Rust call to `StatsUsageHandle::default`.
- Observation: Go merges the complete column map back when any later 2,048-row batch fails, including entries in batches that already committed; Rust removed each successful batch too early.
  Evidence: pinned `session_stats_collect.go:361-375` deletes map entries only after `DumpColStatsUsageEntries` completely succeeds.
- Observation: Go formats collected column usage with `types.TimeFormat`, which has whole-second precision, while Rust previously retained `SystemTime` microseconds in TIMESTAMP(6).
  Evidence: pinned `session_stats_collect.go:368`, `types/time.go:43`, and Rust `system_time_timestamp`.
- Observation: Go's cleanup and selection steps intentionally diverge when
  `TableByID` misses: cleanup returns without a DELETE, then selection still
  reads persisted usage. Rust's current-column slice could not represent that
  state and treated it as an empty schema, deleting every row.
  Evidence: pinned `predicate_column.go:106-131`; the source-backed regression
  returned `[]` instead of `[9]` before `Option<&[i64]>` represented schema
  absence.
- Observation: every package SQL path that reads or writes timestamps passes through
  `CONVERT_TZ`, and the pinned builtin returns NULL when month or day is zero.
  Rust previously preserved zero timestamps in storage and on load, and
  counted a zero `last_used_at` as predicate usage.
  Evidence: pinned `predicate_column.go:67-92,134-159` and
  `expression/builtin_time.go:5448-5458`; the regression observed two
  `Some(ZeroTime)` values before the correction.

## Decision Log

- Decision: retain the existing multi-crate mapping and judge behavior at the ordinary session/server boundary.
  Rationale: repository policy allows one Go package to map to multiple Rust crates, and moving persistence into the leaf collector crate would create improper storage/session dependencies.
  Date/Author: 2026-08-30 / Codex.
- Decision: complete `predicatecolumn` first, then the importing root `usage` package.
  Rationale: it is a separate pinned Go package and a declared dependency of the root package; claiming the parent before it would leave dependency closure incomplete.
  Date/Author: 2026-08-30 / Codex.

## Outcomes & Retrospective

The complete predicatecolumn dependency and root usage package now have atomic receipts. Session timestamp ownership, failed multi-batch retry, and stored timestamp precision match the pinned source, and focused source tests cover every executed original test disposition. Ready validation passed: focused owner/storage/session/server tests, multi-crate check and all-target clippy, formatting, repository lint, and diff checks all completed successfully. Existing workspace warnings remain unchanged and nonfatal.

The later package re-audit additionally corrected the missing-table cleanup
branch and invalid-zero `CONVERT_TZ` semantics. Its two exact regressions have
fail-before/pass-after evidence in the predicatecolumn receipt; the primary
integration batch owns the subsequent Ready-profile rerun.

Exact Ready commands:

    cargo test -p tidb-stats-handle-usage
    cargo test -p tidb-exec --test all predicate_usage
    cargo test -p tidb-exec --test all predicate_column_load_and_cleanup
    cargo test -p tidb-exec --test all stats_delta_updates_match_go
    cargo test -p tidb-exec --test all loaded_stats_usage_replaces_timestamps
    cargo test -p tidb-session analyze_predicate_columns
    cargo test -p tidb-session analyze_default_predicate_columns
    cargo test -p tidb-session analyze_empty_predicate_usage
    cargo test -p tidb-session analyze_persists_effective_column_list
    cargo test -p tidb-server column_usage
    cargo check -p tidb-stats-handle-usage -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server
    cargo clippy -p tidb-stats-handle-usage -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets
    cargo fmt --all -- --check
    make lint
    git diff --check

The first sandboxed `make lint` attempt could not resolve `proxy.golang.org`;
the required rerun with network access passed. No Go or Bazel prerequisite
changed, so `make bazel_prepare` was not run.

## Context and Orientation

The root package inventory is `BUILD.bazel`, `index_usage.go`, `predicate_column.go`, `session_stats_collect.go`, `export_test.go`, `index_usage_integration_test.go`, `predicate_column_test.go`, and `session_stats_collect_test.go`. There is no `doc.go`, generated/platform variant, fixture, example, or fuzz target. One concurrency test is explicitly skipped in Go. The predicatecolumn inventory is exactly `BUILD.bazel` and `predicate_column.go`, with no tests or auxiliary artifacts.

The existing collector and indexusage subpackages are already complete independent package units. The root package's session structures live in `rust/crates/tidb-stats-handle-usage`; its ordinary persistence is invoked from `rust/crates/tidb-server/src/cluster_session_node/mod.rs`; storage reads and writes live in `rust/crates/tidb-exec/src/cluster_predicate_column.rs` and `cluster_stats_write.rs`.

## Plan of Work

Change the session usage update API to accept the caller's one timestamp, matching Go's lock and timestamp boundary, and remove unused source-absent construction traits. Audit every public root helper against a pinned symbol. Verify predicatecolumn's four read/cleanup/save functions through the existing source-backed storage tests and add only missing source behavior.

Map every original root test to executable Rust evidence. Preserve the skipped concurrent writer as skipped source behavior rather than inventing a replacement. Confirm 2,048-row column batches, 100,000-table delta batches, twelve-hour throttling, earliest initialization time on failed overlapping dumps, partition/global lock behavior, historical-meta selection, dropped-column cleanup, predicate analyze integration, and index GC.

## Concrete Steps

Run from `/Users/qiliu/projects/tidb`:

    cargo test --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage
    cargo test --manifest-path rust/Cargo.toml -p tidb-exec analyze_commit_size_source --no-fail-fast
    cargo test --manifest-path rust/Cargo.toml -p tidb-session analyze_predicate_columns --no-fail-fast
    cargo test --manifest-path rust/Cargo.toml -p tidb-server stats_usage --no-fail-fast
    cargo check --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage -p tidb-exec -p tidb-session -p tidb-server
    cargo clippy --manifest-path rust/Cargo.toml -p tidb-stats-handle-usage -p tidb-exec -p tidb-session -p tidb-server --all-targets
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    make lint
    git diff --check

No Go source, Go import block, Bazel file, module dependency, or top-level Go test changes are planned, so `make bazel_prepare` is not required.

## Idempotence and Recovery

All inspection, formatting, and validation commands are safe to rerun. Dump guards deliberately merge unpersisted entries back on error; tests must preserve that invariant. Do not add an in-memory-only persistence path or a cache-specific wrapper.

## Interfaces and Dependencies

The package uses `TableItemID`, system timestamps, the complete indexusage collector, schema metadata, transactional metadata snapshots, and statistics table write plans. Public Rust interfaces should correspond to pinned Go package behavior or necessary native ownership, with source-absent test hooks and convenience constructors kept private or removed.

Revision note: initial plan records both complete pinned inventories, the existing multi-crate integration boundary, and the first concrete API differences before editing.

Revision note (2026-08-30): a complete branch re-audit found that the prior
receipt had inferred both schema presence and `CONVERT_TZ` validity from the
happy path. `plan_get_predicate_columns` now accepts an explicit absent-schema
state, and predicate timestamp writes/reads use the pinned invalid-zero NULL
contract. No new policy or feature was introduced.

Revision note (2026-09-02): the complete root-usage inventory was refreshed
against Go master `c6054025…`; all eight artifacts remain byte-identical to the
previous `e2788410…` pin. The multi-crate ownership and existing Ready gates
remain valid.

Revision note (2026-09-06): the complete immediate
`tidb-stats-handle-usage` owner was re-read under the Rust-only alignment
scope. Sixteen direct Go-shaped returns no longer impose Rust-only
`#[must_use]` diagnostics; eight annotations remain on native partition,
RAII, `Option`, configuration, and cross-crate integration boundaries. The
focused discard regression failed before the edit with exactly sixteen
diagnostics and passes afterward; all eight owner tests, all-target
compilation, standalone rustfmt, repository lint, and diff hygiene pass. The
receipt records the exact inventory and commands.

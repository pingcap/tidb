# Complete planner/util/domainmisc behavior parity

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan is maintained according to it.

## Purpose / Big Picture

Under READ-COMMITTED, a transaction can hold an older table definition while concurrent DDL removes or hides an index in the latest domain schema. Pinned Go TiDB commit `e2788410d8d696605e8cb002585877a063ccc909` prevents that stale index from becoming an access path through `pkg/planner/util/domainmisc.GetLatestIndexInfo` and its sole planner consumer. Rust currently omits that check. After this work, connected Rust sessions make the same latest-schema comparison and do not plan an index that is absent from the latest schema.

The completion unit is the whole pinned Go package `pkg/planner/util/domainmisc`: `info.go` and `BUILD.bazel`; the package has no tests, fixtures, generated files, or platform variants. The Rust regression test additionally proves the observable planner behavior required by the Go consumer.

## Progress

- [x] (2026-08-30) Inventory the complete pinned Go package and read its sole planner consumer.
- [x] (2026-08-30) Locate Rust transaction-catalog, latest-domain-catalog, connection-ID, and isolation seams.
- [x] (2026-08-30) Add the latest-index schema snapshot and the direct Rust equivalent of `GetLatestIndexInfo`.
- [x] (2026-08-30) Apply the Go consumer's exact update-read / READ-COMMITTED / connection-ID gate while enumerating access paths.
- [x] (2026-08-30) Add and run regression tests that demonstrate a stale index is rejected for both Go trigger paths.
- [x] (2026-08-30) Run WIP and Ready validation, self-review, commit, synchronize, and push `hparser-integration`.

## Surprises & Discoveries

- Observation: Rust already carries all three Go gate inputs: `PlanBuilder.is_for_update_read`, `IndexLookupPushDownSession.repeatable_read`, and `Columns::connection_id()`.
  Evidence: `rust/crates/tidb-planner/src/plan_builder.rs`, `rust/crates/tidb-planner/src/access_path.rs`, and `rust/crates/tidb-expr/src/context.rs`.
- Observation: a Rust transaction plans against `Transaction.working`, while `Session.catalog` remains the latest shared domain catalog.
  Evidence: `rust/crates/tidb-session/src/txn.rs::with_catalog_mut`.
- Observation: pinned Go's domainmisc package contains only `GetLatestIndexInfo`; all function-pointer declarations live in the separate `utilfuncp` package and are not part of this claim.
  Evidence: `git ls-tree -r e2788410d8d696605e8cb002585877a063ccc909 -- pkg/planner/util/domainmisc`.

## Decision Log

- Decision: capture latest index metadata in the statement context before the transaction working catalog is borrowed for planning.
  Rationale: this is Rust's existing immutable statement-snapshot boundary and is the direct analogue of Go reading `domain.GetDomain(ctx).InfoSchema()`. It avoids a mutable global lookup from the planner and preserves the distinction between transaction and domain schemas.
  Date/Author: 2026-08-30 / Codex.
- Decision: keep connection and isolation gating in `PlanBuilder`, matching Go's sole caller, while keeping schema-version comparison and table-ID lookup in a small `domain_misc` module.
  Rationale: Go's package helper does not decide when to check; `getPossibleAccessPaths` does. Combining them would change package behavior.
  Date/Author: 2026-08-30 / Codex.
- Decision: preserve Go's incoming `check` argument as Rust `PlanBuilder.is_for_update_read`; do not infer it from index state.
  Rationale: the pinned caller passes `b.isForUpdateRead`, then `getPossibleAccessPaths` ORs it with READ-COMMITTED and gates the result on a nonzero connection ID. A first draft inferred this flag from non-public indexes; rereading the caller showed that was not Go behavior, so it was corrected before commit.
  Date/Author: 2026-08-30 / Codex.

## Outcomes & Retrospective

The whole pinned `domainmisc` package behavior now exists in Rust, and its sole Go consumer's trigger logic is wired into ordinary logical access-path construction. Latest-domain metadata crosses the existing statement context and planner catalog seams; there is no cache-only or executor-side alternate planner. A connected READ-COMMITTED read and a connected update read reject an index absent from the latest schema, while internal sessions retain Go's bypass.

The implementation deliberately creates the latest-index snapshot only for READ-COMMITTED statements and update-read planning. Ordinary REPEATABLE-READ SELECT planning therefore does not acquire the additional catalog walk, preserving the OLTP hot path.

## Context and Orientation

`rust/crates/tidb-session/src/txn.rs::with_catalog_mut` gives a statement the transaction's private working catalog when a transaction exists. `rust/crates/tidb-session/src/stmt_ctx.rs::statement_context_ignoring` runs before that borrow and can snapshot the shared latest catalog. `rust/crates/tidb-executor/src/stmt_context.rs::StmtContext` transports immutable statement state into the executor and planner bridge. `rust/crates/tidb-executor/src/driver/catalog.rs::PlannerCatalog` is the infoschema-shaped input consumed by `rust/crates/tidb-planner/src/plan_builder.rs::PlanBuilder`. Finally, `rust/crates/tidb-planner/src/access_path.rs::get_possible_access_paths` enumerates the table and index paths.

A "latest index schema" here means a schema metadata version plus a map from logical table ID to the indexes present in that version. Rust's catalog publishes only usable indexes, so presence in this map is equivalent to Go's `IndexInfo.State == model.StatePublic` for the current Rust DDL surface.

## Plan of Work

Add a `domain_misc` module in `tidb-planner` containing the immutable latest-index schema representation and a `get_latest_index_info` operation with Go's version-change and missing-table behavior. Extend the planner's `TableSource` seam so the executor-backed catalog supplies this snapshot while test catalogs retain a no-domain default.

Build the snapshot from the session's shared catalog and its local-temporary-table overlay, attach it to `tidb-executor::StmtContext`, and pass it into every `PlannerCatalog` construction. In `PlanBuilder::build_data_source`, reproduce Go's gate: check for an update read or READ-COMMITTED, but only for a nonzero connection ID; load once per table; then enumerate only index IDs present in the latest public map.

Add focused tests at the planner seam for unchanged schema, changed schema with a removed index, no-domain error under a connected READ-COMMITTED session, and the zero-connection bypass. Prefer extending existing planner tests rather than creating a parallel executor path.

## Concrete Steps

Run from `/Users/qiliu/projects/tidb`:

    cargo test -p tidb-planner <focused-test-name>
    cargo fmt --all -- --check
    cargo clippy -p tidb-planner -p tidb-executor -p tidb-session --all-targets
    make lint

The focused regression must pass and the formatting, clippy, and repository Ready gate must exit zero. The workspace currently emits pre-existing clippy warnings, so the affected-crate clippy run is not promoted to `-D warnings`; this change must not add a diagnostic in a touched line.

## Validation and Acceptance

Acceptance requires a connected READ-COMMITTED planner using an older source table to exclude an index absent from the latest schema. An unchanged schema version must report that no recheck is needed, and a changed schema whose table is absent must return an empty index map, matching Go. A session with connection ID zero or absent must retain Go's internal-session bypass.

No Go or Bazel file is changed, so `make bazel_prepare` is not required under root `AGENTS.md`.

## Idempotence and Recovery

All inspection, formatting, and tests are safe to rerun. Changes are made with `apply_patch`. If validation exposes an interface mismatch, adjust the new snapshot seam rather than adding a second execution path. Do not reset or overwrite unrelated user changes.

## Artifacts and Notes

Pinned reference: `e2788410d8d696605e8cb002585877a063ccc909:pkg/planner/util/domainmisc/info.go`. Its sole production consumer is `getPossibleAccessPaths` in the pinned planner core package.

Validation evidence:

    cargo fmt --all -- --check
    cargo test -p tidb-planner domain_misc --no-fail-fast
    cargo test -p tidb-planner latest_schema --no-fail-fast
    cargo test -p tidb-session read_committed_connected_session_captures_latest_index_schema --no-fail-fast
    cargo check -p tidb-planner -p tidb-executor -p tidb-session
    cargo clippy -p tidb-planner -p tidb-executor -p tidb-session --all-targets
    make lint

All commands exited zero. Cargo reported existing workspace warnings; no warning identifies a line added by this package. `make lint` initially could not download `revive` under restricted network access, then exited zero when rerun with the required network permission. No Go, Bazel, or module input changed, so `make bazel_prepare` was not required.

## Interfaces and Dependencies

`tidb-planner` will own the domain-independent metadata type because both `tidb-executor` and planner tests can construct it without introducing a dependency cycle. `tidb-session` already depends on executor and planner types; `tidb-executor::StmtContext` already carries planner policy snapshots, so adding this immutable schema snapshot follows an established dependency direction.

Revision note: initial plan records the completed inventory and chosen statement-snapshot design before implementation.

Revision note: implementation corrected the incoming Go `check` flag to `isForUpdateRead`, recorded passing WIP/Ready evidence, and documented why the snapshot is not built on ordinary REPEATABLE-READ reads.

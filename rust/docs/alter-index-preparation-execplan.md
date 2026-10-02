# Prepare index metadata changes against the original ALTER table

## Purpose / Big Picture


The whole existing ordinary/unique index-action admission flow (ADD, DROP,
RENAME and visibility) must share original-schema preparation. An ALTER that drops a column and renames its single-column index must succeed
with the index absent, as Go's multi-schema test requires. Missing sources and
duplicate targets must still reject against the original schema, and an exact
same-name rename contributes no job or conflict. Expression-index renames must
rename the hidden generated columns while retaining IDs and stored keys.

This maintains existing local ALTER behavior. It does not accept the entire Go
DDL package or introduce a second durable DDL engine. Go master is the source;
the integration checkout's Go code is older in places and is not the reference.

## Progress


- [x] Pull integration/client-rust and refresh Go master; read root/DDL/skill guidance.
- [x] Read original-schema job collection, rename validation, worker phases and hidden-column rename ownership.
- [x] Demonstrate failures for dropped targets, original-schema errors, no-ops, hidden metadata and the session caller.
- [x] Expand to ADD/DROP, resolved anonymous names, conditional notes and grouped specifications; reproduce admission-before-backfill failures.
- [x] Replace immediate index action traversal with typed prepared changes; reuse a pure ADD metadata builder and shared DROP admission/execution.
- [x] Complete single-statement/no-op, error-order and note-order controls after self-review; all 37 multi-schema tests pass.
- [x] Repair metadata ID high-water ownership and all TableInfo projections; reproduce old-row leakage and sibling index retargeting before fixing them.
- [x] Run affected tests, all-target checking and lint; update D11 evidence without overstating closure.
- [x] Commit with the actual locked-build hook (passed in 17.47 seconds); repeat the hook for the receipt amendment.
- [ ] Publication after the receipt amendment: rerun the locked build immediately before pushing, then verify remote and clean checkout in the thread.

## Context and Orientation


Start at integration 5d9c7433a53c7211f0948a48bd876795103d39ae, Go master
93a01d31f6da205ae4bf376825293903a6899fdb and native client/dependency
19a56ccda1e128218cd33c69709038219aced9bc. All are current. The register has 71
unresolved IDs (63 open, eight partial), including D11. Prior work stages catalog
publication and defers shared allocator effects. It does not separate every
action's original-schema admission from staged execution.

`rust/crates/tidb-executor/src/ddl/alter_table.rs` dispatches local ALTER and runs
an AST-derived conflict checker. `alter_metadata.rs` currently validates and
mutates index names together, after earlier actions may have removed the source.
The same helper omits hidden-column rename metadata. Index and column IDs must be
stable identities; column offsets reference hidden columns directly in Rust.
Session dispatch and cluster partition derivation call the common ALTER entry.

Go `executor.go::alterTable` builds jobs against the current original table.
`RenameIndex` calls `ValidateRenameIndex` and returns without a subjob for an
exact spelling no-op. `fillMultiSchemaInfo` therefore sees only actual jobs.
`onMultiSchemaChange` advances all revertible jobs, then their boundary steps;
`onDropColumn` removes covering indexes later than the rename's metadata edit.
Rust's local synchronous image can apply a prepared rename by stable identity:
an implicitly removed index stays removed instead of being resolved again by
name. An unknown original source never becomes a prepared operation.

## Plan of Work


First extend `tests_ddl_multi_schema_change_sql.rs` and session ALTER coverage.
Replace the existing assertion that records the drop/rename mismatch with Go's
successful outcome. Add exact-no-op, original missing/duplicate, visibility and
expression-index metadata controls. Run them against unchanged production.

Then move all supported index actions into `ddl/index_changes.rs`. Split ADD
metadata building from backfill in `indexes.rs`, and make the conflict checker
consume only admitted changes with resolved names. Split DROP admission from
retirement and share both halves with standalone DROP INDEX. Flatten grouped
ADD COLUMN specifications once. Extract original-table rename validation carrying index ID,
new name and hidden-column ID/name changes. Use the existing catalog and allocator
owners. In the dispatcher prepare index metadata before conflict collection,
exclude only validated exact-name renames, and execute each retained rename in
the staged image. Remove the old immediate rename helper. Preserve visibility
execution policy while validating its source against the original table first.

Finally run existing index metadata and multi-schema suites, session ALTER tests,
and all-target checks for executor/exec/session/server. Inspect any failures
against baseline. No Go/Bazel/generated/dependency inputs change, so
`make bazel_prepare` is not needed. No Go tests are run without first assessing
the required failpoint workflow. Root lint and both mandatory locked Rust server
builds remain required.

## Validation and Acceptance


From rust/, run:

    cargo test --locked -p tidb-executor --lib tests_ddl_multi_schema_change_sql -- --nocapture
    cargo test --locked -p tidb-session --lib tests_alter_column -- --nocapture
    cargo check --locked -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets

Select the existing metadata/expression-index suites after locating them. From
the root run `GOTOOLCHAIN=go1.25.14 make lint` and `git diff --check`. Commit using
`TERM=xterm git -c core.hooksPath=hooks commit`, then from the root:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify the remote SHA and clean checkout. Retain exact outcomes, failures and
scope limits in the audit receipt. Do not infer distributed recovery or benchmark
performance from in-process fixtures. Do not close D11 while other action
admission and durable job contracts are missing.

## Idempotence and Recovery


Tests use fresh in-process catalogs and session tables. Red controls may restore
only this turn's production edits temporarily and must restore saved bytes in a
finally block. Never reset unrelated work or force push. Fetch/rebase a remote
advance only after inspecting it, then repeat affected checks and publication gates.

## Surprises & Discoveries


The old receipt calls drop-column plus rename a Go no-op. Go actually retains
the index during the rename worker step and removes it in a later column-drop
transition. The final result is an absent index, not permission to ignore an
unknown source at admission.

## Decision Log


2026-10-02: Use original-schema preparation and stable object identities, not a
missing-index error suppression or a special SQL-pattern branch. Keep original
visibility admission distinct from its execution-time primary-key validation.
Preserve D11 as partial. This is existing-owner maintenance, not a partial-package
transcreation claim.

## Outcomes & Retrospective


The first complete multi-schema run passes 34 tests. Expanded executor DDL,
session ALTER, expression-index and key-length suites pass. The foreign-key
suite has one failure requiring a baseline comparison. Self-review adds red
controls for single-statement case-only rename, earlier non-index admission
errors and ordered conditional notes; those must pass before publication.

2026-10-02 scope update: the user requested substantially more aggressive work.
Expanded from rename metadata to the entire existing ordinary/unique index
action flow. The old AST-only index validator, guessed anonymous names,
duplicate grouped execution branch, separate ADD action adapter and immediate
rename helper are removed. Unsupported index kinds and complete non-index
action/durable job preparation remain separate contracts, not newly enabled
features or completed packages.

2026-10-02 validation update: index admission produces either a typed change,
a conditional note or an error. Notes and errors are delivered at their action
position; an already-known admission error disables earlier index effects,
including unique backfill. This preserves earlier non-index diagnostics while
non-index builders still own their existing admission. Go
`validateAlterIndexVisibility` excludes same-visibility single jobs but retains
them in multi-schema jobs; Go's index conflict checker runs only in that latter
mode. Both controls now pass.

2026-10-02 identity review: the old next-ID methods recomputed maxima from
surviving objects. A dropped column's bytes then appeared under a newly-added
column, and a prepared rename retargeted a newly-added sibling index. Four
controls fail before the repair and pass afterward. KvTable now retains Go's
MaxColumnID/MaxIndexID independently of surviving definitions, updates them on
installation, and preserves them through cloning and CREATE LIKE. All four
TableInfo projections carry persisted counters. Go AllocateColumnID,
AllocateIndexID and BuildTableInfoWithLike establish this ownership; no global
allocator or new transaction implementation is introduced.

2026-10-02 final validation: 220 selected tests pass, including 39 multi-schema
cases, 17 session ALTER cases and 28 cluster-loader cases. One foreign-key name
expectation fails identically on unchanged production; five existing distributed
or region tests remain ignored. All-target checking and root lint pass. The
linked audit receipt retains exact commands and failure evidence. D11 remains
partial and the full register remains 71 unresolved. The actual pre-commit locked build passed in 17.47 seconds. The final locked
build, push and remote verification follow this receipt amendment.

# Complete pinned-Go CHECK constraint DDL parity

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The source of truth is Go commit `e2788410d8d696605e8cb002585877a063ccc909`, primarily `pkg/ddl/constraint.go`, `pkg/ddl/rollingback.go`, `pkg/ddl/executor.go`, `pkg/table/constraint.go`, and `pkg/ddl/constraint_test.go`.

## Purpose / Big Picture

After this work, Rust accepts, stores, displays, and enforces CHECK constraints with the same observable behavior as pinned Go, including concurrent writes while an enforced constraint is added or enabled. A user can create, add, alter, drop, copy, and write through CHECK constraints and receive the same metadata, state visibility, warnings, and error codes. A failed validation leaves the old public schema intact.

## Progress

- [x] (2026-08-31) Centralized CHECK expression admission, naming, dependency discovery, restored SQL, and monotonic ID allocation for local and cluster DDL.
- [x] (2026-08-31) Loaded writable persisted constraints into the ordinary `KvTable` write path and enforced them on INSERT and UPDATE.
- [x] (2026-08-31) Added local and cluster CREATE/ADD/DROP/ALTER behavior, OFF warnings, existing-row validation, CREATE LIKE renaming, and column DROP/RENAME dependencies.
- [x] (2026-08-31) Matched Go's deliberate discard of inline `ALTER TABLE ADD COLUMN ... CHECK` and its grouped table-level CHECK split.
- [x] (2026-08-31) Pushed the coherent metadata/execution batch as `091801605a` to `origin/hparser-integration`.
- [x] (2026-08-31) Matched `BuildTableInfoWithLike` for ordinary versus global-temporary targets: source admission and ordering, TiFlash settings/availability, TTL, affinity, target temporary type, preserved allocator high-waters, and `ON COMMIT` handling.
- [ ] Replace the cluster one-transaction ADD/ENFORCE validator with Go's schema-state transitions, schema-version synchronization, and rollback.
- [ ] Match DROP's `Public -> WriteOnly -> removed` transition and its non-rollback rule.
- [ ] Add concurrent-writer regressions corresponding to every scenario in pinned `pkg/ddl/constraint_test.go`.
- [ ] Inventory every remaining pinned production/test/support/build artifact before making a package-level claim.

## Surprises & Discoveries

- Observation: Go's inline ADD COLUMN CHECK is parsed and built but deliberately discarded because `CreateNewColumn` ignores the returned constraint slice.
  Evidence: pinned `pkg/ddl/add_column.go` assigns `col, _, err := buildColumnAndConstraint(...)`; Rust now has regressions for ON and OFF.

- Observation: one atomic validation transaction is not equivalent to Go. Go publishes writable intermediate constraint states and waits for schema synchronization before scanning existing rows, so a concurrent writer also evaluates the new constraint.
  Evidence: pinned `onAddCheckConstraint` advances `None -> WriteOnly -> WriteReorganization -> Public`; pinned tests insert at each transition.

- Observation: the Rust non-owner half of the synchronization contract already exists. `rust/crates/tidb-server/src/cluster_session_node/schema_sync.rs` loads `mysql.tidb_mdl_info`, respects live old-schema pins, and writes `/tidb/ddl/all_schema_by_job_versions/<job>/<server>` acknowledgments. The Rust DDL path does not yet write the MDL row or wait for those acknowledgments.

- Observation: Rust's CREATE LIKE path always removed TiFlash metadata and retained the source temporary/TTL/affinity shape. Pinned `BuildTableInfoWithLike` instead preserves ordinary-table TiFlash count and labels while clearing availability, and strips TiFlash, TTL, and affinity only for a requested temporary target.

- Observation: Go resets `AutoIncID` but preserves `AutoRandID` and `MaxForeignKeyID` after clearing the foreign-key slice. It also copies the TiFlash replica struct and sets `AvailablePartitionIDs` to nil. Rust reset both preserved counters, emitted an allocated empty slice, and mutated the shared replica object.
  Evidence: pinned `BuildTableInfoWithLike`; fail-before regression stopped on the allocated-empty TiFlash partition IDs, then passed after all four metadata corrections.

- Observation: Go's preprocessor validates a LIKE source before `setTemporaryType`. Therefore a temporary source or an inherited forbidden setting outranks `ON COMMIT PRESERVE ROWS`.
  Evidence: pinned `checkCreateTableGrammar` and `checkReferInfoForTemporaryTable`; the cluster regression now pins every source refusal in that order, including a missing/temporary source outranking a missing target database.

## Decision Log

- Decision: do not describe the current same-snapshot validator as CHECK DDL parity.
  Rationale: it can miss a row committed after the validation snapshot by a writer that has not loaded the candidate constraint.
  Date/Author: 2026-08-31 / Codex

- Decision: use the existing MDL row and etcd acknowledgment protocol rather than sleeping for a schema lease.
  Rationale: pinned Go waits for registered nodes and old-schema transactions, and Rust already implements that consumer protocol. A fixed delay would be a timing workaround and would neither prove nor reproduce synchronization.
  Date/Author: 2026-08-31 / Codex

- Decision: keep local in-process DDL synchronous and atomic, but make cluster DDL use durable intermediate metadata states.
  Rationale: the local catalog has one writer and one immediately replaced schema; the distributed race exists only where multiple servers load persisted `TableInfo` independently.
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

The first batch established one shared CHECK metadata and row-enforcement implementation and removed the previous unsupported/duplicate paths. Package completion remains blocked on distributed state transitions, rollback, and the complete pinned package inventory.

## Context and Orientation

`rust/crates/tidb-executor/src/ddl/check_constraint.rs` owns shared CHECK metadata construction. `rust/crates/tidb-executor/src/kv_table.rs` compiles persisted enforced constraints in writable states and evaluates them on writes. `rust/crates/tidb-exec/src/cluster_ddl.rs` lowers and plans persisted DDL; today its ADD/ALTER CHECK arms write a final public `TableInfo` and attach one existing-row validation obligation. `rust/crates/tidb-exec/src/real_tikv_ddl.rs` executes that obligation against the same transaction snapshot and commits metadata.

In Go, a schema state is a persisted visibility/writeability phase. `WriteOnly` and `WriteReorganization` constraints are enforced by writers but not shown as public schema. Between phases the DDL owner publishes a schema version and waits until every registered server has loaded it. The wait is coordinated by one `mysql.tidb_mdl_info` row and per-server etcd acknowledgments.

The Rust acknowledgment consumer is `rust/crates/tidb-server/src/cluster_session_node/schema_sync.rs`. The DDL owner-side publisher/waiter must be added without creating a second protocol.

## Plan of Work

First introduce an owner-side schema synchronization interface in the cluster DDL execution boundary. It must publish the global schema version, expose the registered server set from `/tidb/server/info`, wait for the exact per-job acknowledgment keys, and time out with an explicit DDL error. Its etcd implementation belongs in `tidb-server`; `tidb-exec` retains only a trait so it does not acquire server/domain dependencies.

Next add ordinary system-row mutations for `mysql.tidb_mdl_info`. Each CHECK phase transaction must replace `(job_id, version, table_ids, owner_id)` atomically with the phase's table metadata and schema diff. After commit, notify the version and wait. When the job finishes or rolls back, delete its MDL row. The same DDL job ID must be retained across every phase; retries re-read the current persisted constraint state.

Then change cluster CHECK planning into a state-driven step. Enforced ADD follows `None -> WriteOnly -> WriteReorganization`, validates, then publishes `Public`. ADD NOT ENFORCED publishes `Public` directly. Enabling follows `Public/not-enforced -> WriteReorganization -> WriteOnly`, validates, then publishes `Public/enforced`. Disabling updates enforcement directly. DROP follows `Public -> WriteOnly -> removed` and continues removal if cancellation is requested after the first phase.

On validation failure, ADD removes the intermediate constraint while preserving `MaxConstraintID`; ALTER restores `Public` and the old enforcement flag. Every rollback publication must also synchronize before returning the original 3819 error.

Finally port the pinned transition tests as behavior tests over the cluster seam. Hooks must pause after a named committed state so a second session can write. The test must prove that writable intermediate states reject violating rows and that validation failure leaves no partially added/enabled metadata.

## Concrete Steps

Work from repository root when inspecting Go:

    git show e2788410d8d696605e8cb002585877a063ccc909:pkg/ddl/constraint.go
    git show e2788410d8d696605e8cb002585877a063ccc909:pkg/ddl/rollingback.go
    git show e2788410d8d696605e8cb002585877a063ccc909:pkg/ddl/constraint_test.go

Run WIP validation from `rust/`:

    cargo fmt --all -- --check
    cargo test --locked --offline -p tidb-executor --lib writable_check_constraints_guard_insert_and_update
    cargo test --locked --offline -p tidb-session --lib tests_check_constraints
    cargo test --locked --offline -p tidb-exec --test all check_constraint
    cargo check --locked --offline -p tidb-server --tests

Before any completion claim, follow `.agents/skills/tidb-verify-profile/SKILL.md` Ready and run its required `make lint` from repository root.

## Validation and Acceptance

Acceptance requires all pinned CHECK DDL scenarios, not only final metadata. During ADD, writes must begin enforcing at WriteOnly and remain enforced through reorganization and public. During ENABLE, writes must enforce after the first intermediate publication. Existing violating rows cause 3819 and roll metadata back. DROP remains writable through WriteOnly and disappears only after synchronization. SHOW and information-schema surfaces expose only Public constraints. A second registered node and a live old-schema transaction must delay phase advancement until they acknowledge or release their pin.

## Idempotence and Recovery

Focused tests and format checks are safe to rerun. A determinate transaction failure leaves the phase unchanged and may be retried from a fresh snapshot. An undetermined commit must not blindly repeat a transition; reload metadata and the MDL row to determine whether the phase became durable. A failed acknowledgment wait leaves the durable job/MDL row available for retry rather than deleting synchronization evidence.

## Artifacts and Notes

Fail-before evidence already captured for the initial batch:

    local inline ADD CHECK: unsupported column option
    cluster inline ADD CHECK: Unsupported ADD COLUMN CHECK waits on its DDL course

Pass-after evidence for commit `091801605a`:

    cargo test --locked --offline -p tidb-executor --lib writable_check_constraints_guard_insert_and_update
    test result: ok. 1 passed

    cargo test --locked --offline -p tidb-session --lib tests_check_constraints
    test result: ok. 7 passed

    cargo test --locked --offline -p tidb-exec --test all check_constraint
    test result: ok. 4 passed

    cargo check --locked --offline -p tidb-server --tests
    Finished dev profile

Current CREATE LIKE fail-before/pass-after evidence:

    cargo test --locked --offline -p tidb-exec --test all check_constraints_follow_go_for_column_dependencies_and_create_like -- --nocapture
    before: failed because AvailablePartitionIDs was allocated-empty rather than Go nil
    second fail-before: returned UnknownDatabase(target) before Go's temporary-source refusal
    after: test result: ok. 1 passed

## Interfaces and Dependencies

`tidb-exec` must define the protocol traits and state-driven DDL execution without depending on `tidb-server`. `tidb-server` may implement the schema synchronization trait using `tidb_pd_client::EtcdClient`. `tidb-executor` remains the only row-level CHECK evaluator. `tidb-model::SchemaState` is the persisted state vocabulary. `mysql.tidb_mdl_info` and the existing etcd key paths are the only synchronization protocol; no feature flag, fixed sleep, cache-only runner, or alternate validator is permitted.

Revision note (2026-08-31): created after the metadata/execution batch was pushed and pinned Go transition tests showed the remaining distributed race.

Revision note (2026-08-31): recorded and closed the adjacent pinned `BuildTableInfoWithLike` target-shape mismatch while designing the distributed state machine.

Revision note (2026-08-31): completed the CREATE LIKE source-admission/error-order audit and corrected Go's exact counter and nil-slice metadata semantics.

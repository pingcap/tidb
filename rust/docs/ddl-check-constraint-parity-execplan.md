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
- [x] (2026-08-31) Replaced the cluster one-transaction ADD/ENFORCE validator with durable Go state transitions, per-phase MDL registration/ack waits, and schema-versioned validation rollback.
- [x] (2026-08-31) Matched DROP's `Public -> WriteOnly -> removed` transition and its non-rollback rule.
- [x] (2026-08-31) Matched Go scheduler cleanup semantics: `tidb_mdl_info` uses assertion-free `REPLACE`, MDL deletion and final per-job etcd deletion are warning-only, and a stale cleanup row cannot block the next phase.
- [x] (2026-08-31) Persisted the complete CHECK argument envelope in `mysql.tidb_ddl_job`; every phase and rollback reloads it from a fresh snapshot, updates `job_meta`, and removes the active row only at a terminal state.
- [x] (2026-08-31) Matched terminal history behavior: `FinishedTS`, process-local sequence allocation, best-effort SQL `INSERT IGNORE`, authoritative meta history, error count/error retention, and DONE/ROLLBACK_DONE states.
- [x] (2026-08-31) Removed the superseded statement-local CHECK continuation/carrier and direct one-transaction ADD/DROP/ALTER planners.
- [x] (2026-08-31) Started one owner-elected persisted-job scheduler that scans the active queue independently of submitters and dispatches the currently persisted CHECK action family; unsupported action types remain active rather than entering a second CHECK-only owner path.
- [x] (2026-08-31) Captured Go's common CHECK job envelope (`CDCWriteSource`, ADD priority, connection ID, session alias, SQL mode, and query); trace ID remains empty because Rust has no session trace-context carrier yet.
- [x] (2026-08-31) Removed the synchronous cache/request-local CHECK runner: submitters now persist, notify the elected owner, and wait on authoritative history.
- [x] (2026-08-31) Pushed the owner/scheduler and job-envelope batch as `3a67a86b7c` to `origin/hparser-integration`.
- [x] (2026-08-31) Replaced CHECK's private insertion wrapper with pinned `pkg/ddl/jobsubmit`'s common prepared-`JobSpec` transaction: pessimistic global-ID locking, source-shaped typed arguments, post-ID callback ordering, failed-attempt cleanup, and atomic ID/job-row commit.
- [x] (2026-08-31) Ported `ModifyIndexArgs` V1/V2 and finished-job wire layouts and made BDR admission inspect the same typed job/subjob arguments as Go instead of Rust's reduced unique-index surrogate.
- [x] (2026-08-31) Matched owner notification after enqueue: the local owner wakes directly, a non-owner writes `/tidb/ddl/add_ddl_job_general`, and the elected Rust scheduler watches that key.
- [x] (2026-08-31) Completed and proved pinned `pkg/ddl/serverstate` as one package: state JSON, process-global memory behavior, etcd session/get/put/watch lifecycle, metrics/logging, scheduler close/rewatch behavior, upgrading-state admission, and the live-PD counterpart of upstream `TestStateSyncerSimple`.
- [x] (2026-08-31) Re-audited all six pinned `pkg/ddl/jobsubmit` artifacts, removed the obsolete ignored missing-carrier tests, and made flashback admission use only Go's `job_id`/`type` query columns instead of decoding unrelated `job_meta` values.
- [x] (2026-08-31) Completed pinned `pkg/ddl/systable` as one package: independent job/MDL reads, exact `JobW` bytes, bounded minimum/flashback queries, the monotonic ten-second refresher and cancellation lifecycle, and shared lower-bound consumption by submission and scheduling.
- [x] (2026-08-31) Completed all seven pinned `pkg/ddl/label` artifacts: strict YAML flow parsing, complete PD rule wire shape, Go clone/nil semantics, and both classic and NextGen codec behavior.
- [x] (2026-08-31) Completed both pinned `pkg/ddl/logutil` artifacts as a dedicated four-constructor crate and removed serverstate's duplicate DDL logger policy.
- [x] (2026-08-31) Re-audited all three pinned `pkg/ddl/bdr` artifacts, verified the existing policy and shared action map, and removed its stale missing-test marker.
- [x] (2026-08-31) Completed all three pinned `pkg/ddl/copr` artifacts as one crate, including single/multi-index contexts, generated dependencies, handle/index/virtual offsets, and on-demand condition construction.
- [x] (2026-08-31) Completed all three pinned `pkg/ddl/resourcegroup` artifacts using the existing complete model settings and the real vendored resource-manager protobuf.
- [x] (2026-08-31) Completed all three pinned `pkg/ddl/testargsv1` artifacts, preserving both mutually exclusive build-tag values through the matching Cargo feature.
- [ ] Add concurrent-writer regressions corresponding to every scenario in pinned `pkg/ddl/constraint_test.go`.
- [ ] Inventory every remaining pinned production/test/support/build artifact before making a package-level claim.

## Surprises & Discoveries

- Observation: Go's inline ADD COLUMN CHECK is parsed and built but deliberately discarded because `CreateNewColumn` ignores the returned constraint slice.
  Evidence: pinned `pkg/ddl/add_column.go` assigns `col, _, err := buildColumnAndConstraint(...)`; Rust now has regressions for ON and OFF.

- Observation: one atomic validation transaction is not equivalent to Go. Go publishes writable intermediate constraint states and waits for schema synchronization before scanning existing rows, so a concurrent writer also evaluates the new constraint.
  Evidence: pinned `onAddCheckConstraint` advances `None -> WriteOnly -> WriteReorganization -> Public`; pinned tests insert at each transition.

- Observation: the Rust non-owner half of the synchronization contract already exists. `rust/crates/tidb-server/src/cluster_session_node/schema_sync.rs` loads `mysql.tidb_mdl_info`, respects live old-schema pins, and writes `/tidb/ddl/all_schema_by_job_versions/<job>/<server>` acknowledgments. The Rust DDL path does not yet write the MDL row or wait for those acknowledgments.

- Observation: re-encoding a freshly decoded Go job with `updateRawArgs=true` erases its raw arguments because the private decoded-argument cache is empty. Worker-only state/error updates must use `false`; action steps decode and refill arguments before terminal `true` encoding.

- Observation: Go's SQL history insertion is `INSERT IGNORE` and any error is logged, while the meta history HSet is authoritative and required. An assertion-bearing Rust insert made harmless duplicate history fatal; the adapter now detects an existing job ID, preserves that SQL row, and still writes meta history.

- Observation: Go `registerMDLInfo` is SQL `REPLACE`, while `cleanMDLInfo` and final per-job etcd cleanup only warn on failure. Assertion-bearing Rust INSERT/DELETE mutations would make a harmless stale row fatal, so the transaction vocabulary now includes assertion-free system-row PUT/DELETE operations matching those SQL statements.

- Observation: Rust's CREATE LIKE path always removed TiFlash metadata and retained the source temporary/TTL/affinity shape. Pinned `BuildTableInfoWithLike` instead preserves ordinary-table TiFlash count and labels while clearing availability, and strips TiFlash, TTL, and affinity only for a requested temporary target.

- Observation: Go resets `AutoIncID` but preserves `AutoRandID` and `MaxForeignKeyID` after clearing the foreign-key slice. It also copies the TiFlash replica struct and sets `AvailablePartitionIDs` to nil. Rust reset both preserved counters, emitted an allocated empty slice, and mutated the shared replica object.
  Evidence: pinned `BuildTableInfoWithLike`; fail-before regression stopped on the allocated-empty TiFlash partition IDs, then passed after all four metadata corrections.

- Observation: Go's preprocessor validates a LIKE source before `setTemporaryType`. Therefore a temporary source or an inherited forbidden setting outranks `ON COMMIT PRESERVE ROWS`.
  Evidence: pinned `checkCreateTableGrammar` and `checkReferInfoForTemporaryTable`; the cluster regression now pins every source refusal in that order, including a missing/temporary source outranking a missing target database.

- Observation: Rust's DDL builder uses the query-shaped statement context, but `tidb_enable_check_constraint` was copied only into the DML-shaped context. A committed global ON value therefore still lowered cluster ALTER CHECK as ignored.
  Evidence: the embedded-store owner-queue regression failed before scheduler submission even though the shared global table contained `ON`; copying the setting into the query-shaped context made the same regression reach history and enforce error 3819.

- Observation: Go's `BeforeInsertWithAssignedIDs` callback runs after every retry has reassigned IDs but before `insertDDLJobs2Table` fills/encodes arguments. Its cleanup is per attempt and runs on any failed insertion transaction; the successful attempt deliberately retains the registration.
  Evidence: pinned `pkg/ddl/jobsubmit/submit.go::GenGIDAndInsertJobsWithRetry` and `TestSubmitBatchRetryCleanup`; the executable Rust regression now records two callbacks, one cleanup, and the final job ID without asserting that a rolled-back allocation must change.

- Observation: Go serializes all job allocations by pessimistically locking the meta `NextGlobalID` key and reads the allocation snapshot at the resulting `forUpdateTS`. An optimistic read-plus-write retry is not equivalent because job insertion order is part of scheduler correctness.
  Evidence: pinned `lockGlobalIDKey`; Rust submission now uses `SessionTransaction::begin_pessimistic`, locks `next_global_id_kv_key`, and binds catalog reads to the returned statement timestamp.

- Observation: `serverstate.WatchChan` is nil before `Init`, and `memSyncer.Init` recreates a capacity-one channel while retaining process-global state. Constructing a permanently available Rust channel changed lifecycle and backpressure behavior.
  Evidence: pinned `mem_syncer.go` and `syncer.go`; the Rust trait now returns `None` before initialization and the memory implementation installs a fresh bounded channel in `init`.

- Observation: the first serverstate batch inherited the etcd client's five-second timeout, skipped Go's final failed-attempt sleep, synchronously rewatched, omitted `mockUpgradingState`, and used serde's replacement/escaping rules for `StateInfo`.
  Evidence: pinned `getKeyValue`, `PutKVToEtcd`, `util.Watcher.Rewatch`, `memSyncer.UpdateGlobalState`, and `encoding/json`; the corrective batch adds call-site etcd deadlines, every retry delay, asynchronous rewatch, the boolean failpoint, receiver-mutating decode, and Go HTML/JavaScript escaping.

- Observation: a second package-and-caller audit found that Rust flattened watch responses, retained one permanent channel, ignored closed channels in the scheduler, delayed the first lease keepalive, revoked the lease on drop, forwarded etcd's suppressed creation frame, and omitted every metric side effect.
  Evidence: pinned `clientv3/watch.go`, `concurrency/session.go`, `pkg/ddl/util/watcher.go`, `job_scheduler.go`, and `pkg/metrics/{ddl,owner}.go`; Rust now preserves response batches/cancellation, replaces and closes channels with context, re-watches on disconnect, uses a reconnecting TTL/3 session without teardown revoke, filters creation frames, and records the source metric families and labels.

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

- Decision: do not commit the distributed transition batch until `mysql.tidb_ddl_job` recovery is present.
  Rationale: a durable intermediate state without durable immutable job arguments cannot be resumed after process/owner loss, whereas pinned Go resumes it from the job table. A passing straight-line transition test is insufficient evidence for the package contract.
  Date/Author: 2026-08-31 / Codex

- Decision: use one owner-elected active-job scanner shared by every persisted action family, while allowing only action handlers that have actually been ported.
  Rationale: this matches Go's submitter/scheduler boundary without pretending that Rust's ordinary direct DDL actions are already persisted. Unknown active actions are retained for a future capable owner instead of being consumed incorrectly.
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

The pushed batches establish one persisted CHECK execution route from submission through the elected owner, synchronized intermediate states, validation, rollback, and both history stores. The old in-memory continuation and request-local worker routes are gone. Package completion is not claimed: the scheduler dispatches only action families whose persisted worker exists, Rust has no trace-ID carrier, concurrent cluster regressions remain incomplete, and the complete pinned package inventory is still open.

## Context and Orientation

`rust/crates/tidb-executor/src/ddl/check_constraint.rs` owns shared CHECK metadata construction. `rust/crates/tidb-executor/src/kv_table.rs` compiles persisted enforced constraints in writable states and evaluates them on writes. `rust/crates/tidb-exec/src/ddl_job_table.rs` owns the active queue representation, `ddl_history_table.rs` owns Go's best-effort SQL-history insertion, `cluster_ddl.rs` plans one persisted worker step, and `real_tikv_ddl.rs` commits and synchronizes those steps.

In Go, a schema state is a persisted visibility/writeability phase. `WriteOnly` and `WriteReorganization` constraints are enforced by writers but not shown as public schema. Between phases the DDL owner publishes a schema version and waits until every registered server has loaded it. The wait is coordinated by one `mysql.tidb_mdl_info` row and per-server etcd acknowledgments.

The Rust acknowledgment consumer is `rust/crates/tidb-server/src/cluster_session_node/schema_sync.rs`. The DDL owner-side publisher/waiter must be added without creating a second protocol.

## Plan of Work

First introduce an owner-side schema synchronization interface in the cluster DDL execution boundary. It must publish the global schema version, expose the registered server set from `/tidb/server/info`, and wait for the exact per-job acknowledgment keys. Its etcd implementation belongs in `tidb-server`; `tidb-exec` retains only a trait so it does not acquire server/domain dependencies.

Next add ordinary system-row mutations for `mysql.tidb_mdl_info`. Each CHECK phase transaction must replace `(job_id, version, table_ids, owner_id)` atomically with the phase's table metadata and schema diff. After commit, notify the version and wait. When the job finishes or rolls back, delete its MDL row. The same DDL job ID must be retained across every phase; retries re-read the current persisted constraint state.

Then change cluster CHECK planning into a state-driven step. Enforced ADD follows `None -> WriteOnly -> WriteReorganization`, validates, then publishes `Public`. ADD NOT ENFORCED publishes `Public` directly. Enabling follows `Public/not-enforced -> WriteReorganization -> WriteOnly`, validates, then publishes `Public/enforced`. Disabling updates enforcement directly. DROP follows `Public -> WriteOnly -> removed` and continues removal if cancellation is requested after the first phase.

The full pinned-Go CHECK argument envelope is persisted in `mysql.tidb_ddl_job` before the first state transition and updated atomically with every phase. The remaining scheduler work is to make startup/owner acquisition scan runnable jobs independently of a submitting request. Completion and rollback already remove the active row and write both history stores.

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

Current distributed CHECK evidence:

    cargo test --locked --offline -p tidb-exec --test all enabled_alter_check_constraints_use_the_catalog_path_and_keep_ids_monotonic -- --nocapture
    test result: ok. 1 passed

    cargo test --locked --offline -p tidb-txnkv --lib system_row_replace_and_delete_have_no_existence_assertion -- --nocapture
    test result: ok. 1 passed

    cargo test --locked --offline -p tidb-server --lib schema_wait_uses_only_the_newest_server_id_for_each_instance -- --nocapture
    test result: ok. 1 passed

Current durable-job evidence:

    cargo test --locked --offline -p tidb-exec --test all check_job_submission_precedes_every_schema_transition -- --nocapture
    cargo test --locked --offline -p tidb-exec --test all persisted_add_check_rolls_back_after_owner_restart -- --nocapture
    cargo test --locked --offline -p tidb-exec --test all persisted_drop_and_alter_check_jobs_resume_and_finish_like_go -- --nocapture
    cargo test --locked --offline -p tidb-exec --test all persisted_alter_check_validation_rolls_back_to_not_enforced -- --nocapture
    cargo test --locked --offline -p tidb-exec --test all ddl_sql_history_uses_go_insert_ignore_semantics -- --nocapture
    test result: all passed

Current common job-submission evidence:

    cargo test --locked --offline -q -p tidb-model go_test_ --lib -- --nocapture
    test result: ok. 13 passed

    cargo test --locked --offline -q -p tidb-exec ddl_job_submit::tests --lib -- --nocapture
    test result: ok. 9 passed

    cargo test --locked --offline -q -p tidb-exec --test all failed_job_insert_attempt_cleans_up_assigned_id_registration_before_retry -- --nocapture
    test result: ok. 1 passed

    cargo check --locked --offline -p tidb-exec -p tidb-server
    Finished dev profile

The broader `cargo test -p tidb-txnkv` command was not a valid scoped check because pre-existing unrelated integration targets do not compile (`lock_resolver_source`, `batch_scheduler_source`, and `batch_wire_source`); rerunning the new unit regression with `--lib` passed.

## Interfaces and Dependencies

`tidb-exec` must define the protocol traits and state-driven DDL execution without depending on `tidb-server`. `tidb-server` may implement the schema synchronization trait using `tidb_pd_client::EtcdClient`. `tidb-executor` remains the only row-level CHECK evaluator. `tidb-model::SchemaState` is the persisted state vocabulary. `mysql.tidb_mdl_info` and the existing etcd key paths are the only synchronization protocol; no feature flag, fixed sleep, cache-only runner, or alternate validator is permitted.

Revision note (2026-08-31): created after the metadata/execution batch was pushed and pinned Go transition tests showed the remaining distributed race.

Revision note (2026-08-31): recorded and closed the adjacent pinned `BuildTableInfoWithLike` target-shape mismatch while designing the distributed state machine.

Revision note (2026-08-31): completed the CREATE LIKE source-admission/error-order audit and corrected Go's exact counter and nil-slice metadata semantics.

Revision note (2026-08-31): implemented and tested the straight-line distributed CHECK state machine, rollback, MDL wait, and cleanup semantics; recorded the remaining durable job/restart gap and withheld the batch from commit.

Revision note (2026-08-31): persisted CHECK jobs and terminal history, removed the in-memory continuation path, added fresh-owner ADD/DROP/ALTER/rollback regressions, and narrowed the remaining gap to the general DDL scheduler and common envelope fields.

Revision note (2026-08-31): added the owner-elected active-job scanner, separated submission from execution, completed the available common job envelope, fixed the query-shaped DDL context's missing CHECK enablement, and pushed commit `3a67a86b7c`.

Revision note (2026-08-31): transcreated the common jobsubmit allocation/insertion retry contract, source-shaped modify-index arguments and BDR admission, removed the CHECK-only submission carrier, and added cross-node owner notification. Package completion remains withheld while ordinary Rust DDL actions still bypass the persisted common submit route and the pinned server-state dependency is not yet transcreated.

Revision note (2026-08-31): transcreated pinned `pkg/ddl/serverstate`, removed its documentary missing-carrier test, wired the ordinary DDL constructor/scheduler/submitter to the shared state syncer, and added a live-PD form of the upstream etcd integration test. Broader DDL package completion remains withheld.

Revision note (2026-08-31): post-commit package audit corrected serverstate's per-call deadlines, final retry sleeps, async rewatch lifecycle, memory failpoint, and exact `encoding/json` mutation/escaping behavior. The shared PD client now keys cached connections by operation timeout so a broad client timeout cannot leak into a narrower Go child context.

Revision note (2026-08-31): reopened the premature serverstate completion mark after a full source/test/caller/dependency audit. The follow-up batch preserves Go watch-response/channel closure semantics, restores scheduler rewatch on closure, replaces the one-shot/revoke lease approximation with the pinned session lifecycle, adds exact syncer/session/retry metrics and DDL logs, and matches `encoding/json` ordered partial mutation. A fresh isolated PD then passed the Rust counterpart of upstream `TestStateSyncerSimple`, closing this package unit; broader DDL completion remains withheld.

Revision note (2026-08-31): re-read the complete pinned `pkg/ddl/jobsubmit` package and replaced its scheduler-shaped flashback check with the source query shape, which does not decode `job_meta`. Removed the now-false ignored tests claiming that `SubmitBatch`, table-mode job construction, ID allocation, and its guards had no Rust carrier. Package completion remains withheld while ordinary Rust DDL actions bypass the persisted common submit/worker route.

Revision note (2026-08-31): audited and mapped all five pinned `pkg/ddl/systable` artifacts, removed the documentary missing-package test, and wired one shared monotonic minimum-job-ID refresher into both jobsubmit flashback admission and the owner scheduler's active-job lower bound. The live TiKV reads now seek from the encoded minimum job handle, preserving the performance purpose stated by Go issue 52905 instead of filtering after a full scan. Manager construction defers table lookup exactly like Go, so an absent MDL table cannot break unrelated DDL-job reads.

Revision note (2026-08-31): audited all seven pinned `pkg/ddl/label` artifacts and removed the legacy hand-parsed/narrow PD-rule behavior. The package now uses YAML sequence decoding, preserves expiry and arbitrary data, models Go's shallow slice/interface clone and nil/allocated-empty distinction, and validates the real API-v2 codec under the NextGen feature. The downstream infosync keyspace filter remains a separate package gap.

Revision note (2026-08-31): audited the complete two-artifact `pkg/ddl/logutil` package and added its four exact category/sampling constructors over the existing shared logger implementation. Serverstate now consumes that package instead of carrying a private `category=ddl` clone; the pinned package has no tests or other artifacts.

Revision note (2026-08-31): re-read the complete three-artifact `pkg/ddl/bdr` package and verified its existing Rust carrier against all add/modify/general admission branches and the full shared action-class map. The common job submitter already consumes the typed policy for jobs and subjobs; the obsolete documentary missing-test marker was removed.

Revision note (2026-08-31): read and transcreated the complete three-artifact `pkg/ddl/copr` package. The new crate uses the existing metadata pointer carriers and expression construction owner, retains the build context for source-timed `GetCondition` calls, and implements the exact table-order dependency expansion, clustered/extra handle selection, single/multi index lookup, virtual-column pushdown rejection, and balanced DNF composition. All three pinned tests are carried without additional Rust-only cases.

Revision note (2026-08-31): read and transcreated the complete three-artifact `pkg/ddl/resourcegroup` package. The new crate maps the complete model settings directly into the vendored kvproto resource-manager messages, including source validation order, open action/watch ordinals, optional watch/background shapes, RU token-bucket construction, and the RU/raw conflict and RU-only-mode errors. The pinned package has no tests or other artifacts.

Revision note (2026-08-31): read and transcreated the complete three-artifact `pkg/ddl/testargsv1` package. The dedicated crate exposes only the source `ForceV1` fact, false by default and true under the `ddlargsv1` feature, with no runtime override or extra test behavior.

# Restore shared persisted DDL cancellation and error checkpoints

This living ExecPlan follows root `PLANS.md`.

## Purpose and scope


Existing persisted jobs must obey cancellation before executing another forward
action. Error checkpoints must survive worker replacement, preserve the source
error, respect the configured error-count limit and leave SQL completion to
history. Starting integration is 7a7c756a390008771fb327942e95ff8a826a25c2; a fresh
fetch/fast-forward is current. Go master remains
93a01d31f6da205ae4bf376825293903a6899fdb. Read its pkg/ddl/job_worker.go,
rollingback.go, constraint.go, table.go, schema.go and util/util.go directly from
origin/master; the integration branch's Go files are not the reference.

This repairs the existing shared worker and its nine live action kinds. It does
not accept a partial transcreation of pkg/ddl or its dependencies: complete
upstream packages, all production/generated/platform sources, original tests,
fixtures, build inputs and validation remain the atomic acceptance units. Seeds
remain disabled. Direct DDL, scheduling, reorg, complete external-error handling,
lease synchronization and delete-range ownership remain separate open findings.

## Progress


- [x] Refresh refs and compare cancellation, error persistence and global-setting owners.
- [x] Record failing cancellation and error-checkpoint regressions.
- [x] Dispatch cancellation before forward actions, preserving action-specific transitions.
- [x] Persist failed steps and load the source retry limit through the runtime setting owner.
- [x] Verify restart, owner loss, schema barriers, raw args and original errors.
- [x] Run 115 scoped Rust cases, all-target checks and repository lint.
- [x] Commit through the actual hook and its locked server build; the final publication command reruns the locked build after the receipt amendment before pushing.

## Context and orientation


A persisted job is a row in mysql.tidb_ddl_job. A checkpoint updates that row
without falsely finishing the SQL operation. A schema barrier waits until peers
acknowledge a published schema before the next action or history write. Go's
worker owns all three. Rust currently shares that owner for nine action kinds;
most SQL DDL still bypasses it (D01), and scheduling/reorganization remains D03.
The repaired planner is in rust/crates/tidb-exec/src/cluster_ddl.rs; transaction
execution is real_tikv_ddl.rs in the same crate. The embedded backend test lives
in rust/crates/tidb-server/src/cluster_session_node/ddl.rs. The global setting
uses rust/crates/tidb-vardef/src/lib.rs and tidb-session/src/vars.rs.

## Milestones and implementation


Extend cluster_ddl_source.rs and the existing embedded DDL worker suite before
production changes. Reproduce a cancelling CREATE being run, reversible versus
irreversible DROP, CHECK rollback and a retryable action error lost by the worker.
Use the existing bootstrap, queue and schema-sync fixtures; no new test framework.

In cluster_ddl.rs centralize job-only writes and cancellation conversion. Go
checks physical metadata to distinguish unstarted DROP from irreversible DROP;
CHECK rollback may publish metadata while retaining CANCELLING for the next
step. Preserve raw arguments unless conversion actually enters ROLLINGBACK.
An error increments the durable count. Normal errors replace Job.Error;
cancellation preserves and annotates the original error. Check the current
global limit only when Go does. Discard action mutations on unhandled failure,
but retain handled rollback writes. Keep the existing pause and MDL gates.

Carry the committed run error separately through real_tikv_ddl.rs: reporting it
must occur after the job checkpoint commits and must not retry an already
committed transaction. Runtime limit refresh uses a fresh read-only transaction,
preserving the last configured value if refresh fails, as Go loadGlobalVars
does. Use the existing vardef/session runtime-setting publication path so local
SET GLOBAL, reload and reset share that value. The planner takes an explicit
lazy limit reader so tests do not share mutable global state.

## Validation and recovery


From rust/ run targeted filters first, then:

    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo test --locked -p tidb-session --lib ddl_error_count_limit
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

From root run git diff --check and make lint. No Go/Bazel inputs change, so
bazel_prepare and Go failpoints are unnecessary. Commit with
TERM=xterm git -c core.hooksPath=hooks commit. The hook must pass
cd rust && cargo build --locked -p tidb-server; rerun that exact command after
the final commit immediately before git push origin HEAD:hparser-integration.
Retain /private/tmp/tidb-ddl-cancel-*.log. Do not bypass checks or claim full
package parity. On failure preserve evidence and repair the affected lifecycle.
Job encoding is unchanged; no persistent-format migration is introduced.

## Surprises & Discoveries


Go does not turn every cancelling job into ROLLINGBACK. DROP after publication
continues forward in a later step; ADD/ALTER CHECK cancellation may need a
schema publication before the final cancellation. Failed cancellation must
preserve an older Job.Error, unlike an ordinary action failure.

The old mark_check_constraint_job_rollingback_with_retry started a new transaction
after failed validation. A pause committed during validation was read and then
overwritten as ROLLINGBACK. The embedded regression returned a validation failure
instead of Paused before the repair. Removing that transaction owner and planning
the error against the original snapshot makes the ordinary write-conflict retry
observe the pause. A second case resumes and verifies ordinary validation still
reaches ROLLBACK_DONE with one durable error, even with a zero error limit.

The ADD CHECK argument and initial table metadata must share ConstraintInfo:
Go's first WRITE_ONLY update also changes the persisted job argument. Copying it
left the argument at NONE and caused the next name check to reject its own
constraint. Sharing the Go handle fixes the lifecycle rather than bypassing
name validation.

Publishing the DDL limit in generic refresh_resolved was also incorrect: an
unrelated SET restored stale 7 over a worker-refreshed 3. The regression fails
before the per-variable hook and passes afterward. Scratch registries stay
private until replace_from publishes the committed image.

## Decision Log


Keep cancellation, error counting, queue updates and terminal history under the
shared worker. Reusing forward handlers without an explicit conversion phase
would silently execute requested cancellations. The user's standing Go-parity
instruction authorizes this existing-lifecycle repair and its caller migration.
On 2026-10-01, retain D05 as partial: PD placement/label delivery still exits
before the action-error checkpoint; rollback-transaction error taxonomy, panic
recovery and retry timing need their complete source owner. Do not enlarge the
supported action predicate or claim complete pkg/ddl transcreation here.

## Outcomes & Retrospective


The recorded D04/D06 control and terminal object-validation findings are
repaired for existing live actions; D05 advances to partial. The current register
contains 85 findings: 75 unresolved (69 open, six partial), ten repaired. There
is no dependency or generated-file change and no upstream package is accepted.

Regression evidence is retained in /private/tmp/tidb-ddl-cancel-*.log. Before
fixing production, these filters failed with the recorded observations:

    cargo test --locked -p tidb-exec --test all persisted_cancellation_precedes_forward_action
    cargo test --locked -p tidb-exec --test all persisted_action_error_is_checkpointed_before_retry
    cargo test --locked -p tidb-exec --test all persisted_check_lookup_failures_cancel_before_retry
    cargo test --locked -p tidb-session --lib ddl_error_count_limit
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests::persisted_worker_recovers_schema_barriers_before_history

The corresponding before.log, before-error.log, before-lookup.log,
before-setting.log, before-setting-refresh.log and before-race.log record:
cancellation decoded invalid forward args (1105 instead of 8214), action errors
escaped without a checkpoint, missing CHECK tables were not terminal, SET did
not publish the limit, unrelated SET overwrote the fresh limit, and detached
validation overwrote PAUSING. The old-path replays restore the candidate source
after the failing test; no rejected implementation remains.

All final Rust commands above pass: planner 103, transaction classification 7,
embedded DDL worker 4, global setting 1 (115 total, zero ignored). All-target
checks and make lint pass. The final setting test also proves invalid input has
no effect and scratch load/committed replacement/reset publish at the right time.
The missing-object matrix covers missing schemas/tables, renames, non-public
tables and absent DROP/ALTER constraints. The embedded suite proves fresh peer
limit reads, rollback state, owner loss and the administrative race.

No live mixed Go/Rust cluster, complete upstream Go DDL suite, SQL ADMIN command
front door, failure-injected external PD delivery, or sysbench/TPC-C/TPC-H/YCSB
benchmark ran. Job encoding is unchanged. Extra catalog/global reads occur on
errors only; their performance is unmeasured. General DDL submission, scheduler,
reorganization, lease/GC ownership and remaining D05 policies remain open.
The actual commit hook passed its locked server build (commit.log). The receipt
amendment uses the same hook again; publication must then execute the fresh
post-commit build and push sequentially, preserving amend.log, pre-push.log and
push.log. A build failure stops publication:

    TERM=xterm git -c core.hooksPath=hooks commit --amend --no-edit
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration
    git status --short
    git ls-remote origin refs/heads/hparser-integration

The final remote SHA must equal git rev-parse HEAD and the worktree must be clean.

This revision records the shared cancellation/error repair and the two
concurrency/publication discoveries; subsequent work must use the remaining
register rather than treating the repaired live paths as a complete package.

Publication follow-up, 2026-10-02: the first push was rejected because the remote
advanced to b1484f7866bb51887ec214b06c2d1087e808340f. That commit changes ORDER BY
COLLATE planning/execution in four disjoint files. The unpublished repair was
rebased without conflicts, preserving all remote work. All 115 scoped cases,
the all-target check and lint pass again on the combined tree. Publication
reruns the actual hook and fresh locked build before retrying the ordinary
fast-forward push; no force push is used.

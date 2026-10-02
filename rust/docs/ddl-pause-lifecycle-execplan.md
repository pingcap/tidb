# Preserve persisted DDL pause requests through the shared worker

This living ExecPlan follows root `PLANS.md`.

## Purpose and scope


A persisted PAUSING job must become PAUSED without running its action. A
PAUSED job must remain active until resumed; the owner must release it so other
jobs can run. SQL waiters must continue waiting for history rather than report
success for a pause. Repair the existing shared lifecycle and all its callers.

Starting integration is c3c31d98ec2b8081074f3f58f109df64650c2a47; fetching and
fast-forwarding hparser-integration reported already current. The source is
Go master 93a01d31f6da205ae4bf376825293903a6899fdb,
pkg/ddl/job_worker.go::processJobPausingRequest, runOneJobStep and
transitOneJobStep. Go initializes RealStartTS, handles pausing before dispatch,
preserves raw arguments and prior errors, and persists the pause without a
schema publication. The control signal is not counted as an action failure.

This is maintenance of existing live paths, not transcreation or acceptance of
pkg/ddl. That entire upstream package, with all production, generated, platform,
test, fixture and build inputs, remains the acceptance unit. D04's cancellation
conversion and D05's error-budget owner require separate source-backed work.
Direct SQL publication, complete scheduling, lease synchronization, reorg and
delete-range ownership remain unresolved. Disabled MV/MLog seeds stay disabled.
Native client-rust and dependencies are outside this repair.

## Progress


- [x] Refresh refs and trace shared planning, transaction execution, scheduler and SQL waiter.
- [x] Compare Go pause, cancellation and error-persistence contracts.
- [x] Add regressions and record failures against unchanged production.
- [x] Implement pause checkpoint and explicit scheduler outcome; migrate every caller.
- [x] Verify all-action planning, MDL recovery, owner loss, repeated pause and resume.
- [x] Run focused suites, all-target check and lint; reconcile both finding registers.
- [x] Pass the actual hook and fresh post-commit build. Repeat both for the
  final receipt amendment; the task result records publication and remote verification.

## Milestones and implementation


First extend cluster_ddl_source.rs with all currently supported persisted action
types. Seed PAUSING jobs with arguments that must not be decoded, retain an old
error, and assert that only the active row changes. Extend the existing embedded
worker integration scenario with owner loss before the pause commit, repeated
visits, a second independent job and resumption. Run both tests before editing
production and retain the failing output.

Next add a Paused plan result in cluster_ddl.rs. Recover a preceding durable
metadata-lock (MDL) schema acknowledgement before handling state. PAUSED returns
without writes; PAUSING updates only the job envelope, with no error-count,
schema-version, validation, backfill, placement or history side effects. The
existing transaction owner applies normal conflict handling and ownership checks.

Carry the paused result through real_tikv_ddl.rs and rename the worker entrypoint
to run_persisted_ddl_job: it returns Finished or Paused explicitly. The scheduler
releases either result, but SQL completion still depends exclusively on terminal
history. Migrate all callers and exhaustive matches, including disabled seed
test helpers. Do not add an alternate queue or a busy-wait pause loop.

Finally update D04 evidence without closing the cancellation gap or changing the
77 unresolved count. Review the diff, run required validation and publish to
hparser-integration using the actual repository hook and a fresh locked build.

## Validation and recovery


Run from rust/ (retain logs under /private/tmp/tidb-ddl-pause-*.log):

    cargo test --locked -p tidb-exec --test all persisted_pausing_jobs
    cargo test --locked -p tidb-server --lib persisted_worker_recovers_schema_barriers_before_history
    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

From root run git diff --check and make lint. No Go/import/module/Bazel input
changes require bazel_prepare or failpoint enablement. Commit with
TERM=xterm git -c core.hooksPath=hooks commit; it must run
cd rust && cargo build --locked -p tidb-server. Repeat the exact locked build
after the final commit immediately before git push origin HEAD:hparser-integration.
If a check fails, retain evidence and repair or report it; never bypass the hook
or weaken tests. Reverting this patch restores the previous state without a
storage migration; persisted job encoding does not change.

## Surprises & Discoveries


The worker's old name promises completion, but Go releases paused jobs while
keeping them active. A no-op action step alone would loop forever in the Rust
worker. Pausing must propagate across the planner and transaction boundary to
the scheduler, without becoming a successful SQL result.

The integration branch's complete Go worker file differs from master. The
master pause function and its surrounding dispatch/checkpoint ordering were
therefore checked directly with git show origin/master:pkg/ddl/job_worker.go;
the pause behavior matches. This is not a claim of whole-file source continuity.

## Decision Log


Handle pause in the shared owner rather than individual action handlers. Keep
cancellation separate: its Go conversion performs action-specific metadata and
error-budget work and cannot be replaced by a generic state assignment. This
decision follows the user's standing instruction to preserve Go behavior.

## Outcomes & Retrospective


Both new regressions failed before production changes. The planner entered
CHECK validation and returned TableNotExists for a PAUSING job. The embedded
worker published a schema and called the intentionally failing notifier instead
of pausing. The before-planner and before-worker logs retain these failures.

Both regressions pass after the change. The complete cluster_ddl_source filter
reports 98 passed, zero failed or ignored. Its pause case covers all nine live
action kinds, raw argument/error preservation, exact job-only writes, initial
and recovered MDL barriers, and repeated PAUSED reads. The embedded worker
case also covers owner loss before commit, resumption with the original start
timestamp, history absence while paused and completion of another job.

The surrounding transaction suite passes seven tests and the embedded schema
sync/ownership suite passes four. All-target checking for both affected crates,
git diff --check and make lint pass. Existing compiler warnings remain. Both
85-row finding registers agree, with D04 partial and all 77 unresolved findings
retained (71 open, six partial). Full file formatting was avoided because the
large existing files contain unrelated formatting drift; the changed function
bodies were formatted with rustfmt --edition 2021 --emit stdout.

The actual pre-commit hook ran the locked server build successfully (11.45 s),
and a fresh post-commit locked build passed (11.75 s). Logs are
/private/tmp/tidb-ddl-pause-commit.log and -prepush.log. Repeat the hook and
fresh build after this final receipt amendment before publishing; retain
-commit-final.log and -prepush-final.log as the final gates. The task result
records the final published commit and clean-tree/remote verification.

Cancellation conversion, global error budgets, full scheduler ownership and the other DDL gaps listed
above remain unimplemented. No package is accepted by this repair. No live
mixed Go/Rust cluster, ADMIN pause/resume SQL command path, complete Go DDL
suite or sysbench/TPC-C/TPC-H/YCSB benchmark was run. No performance improvement
is claimed. The storage format is unchanged; compatibility risk remains in the
unrepaired owners, not a new pause encoding.

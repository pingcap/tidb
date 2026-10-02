# Recover persisted DDL action panics through the shared worker

This living ExecPlan follows root PLANS.md and continues the maintenance receipt
in ddl-cancellation-lifecycle-execplan.md.

## Purpose and scope


A panic in an existing persisted DDL action must not kill its worker or leave its
queue row unchanged forever. Go master recovers inside runOneJobStep, increments
the durable error count and selects CANCELLING or CANCELLED through countForPanic.
The same job transaction owns recovery, so an overlapping administrative pause
or cancellation still wins through ordinary conflict detection. Action outputs
are staged values in Rust: incomplete metadata and row buffers must be discarded.

Starting integration is 7ab955b0d44f41716f39b4841ec9d4f798790170. A fresh fetch of
master and hparser-integration is current; Go master remains
93a01d31f6da205ae4bf376825293903a6899fdb and client-rust remains 6163ecfc.
Read pkg/ddl/job_worker.go::runOneJobStep/countForPanic, pkg/util/misc.go::Recover,
pkg/ddl/tests/fail/fail_db_test.go::TestRunDDLJobPanic and
pkg/ddl/column_type_change_test.go::TestDDLExitWhenCancelMeetPanic from that ref.
This is maintenance of existing live paths, not partial-package acceptance.
Full pkg/ddl and dependency production/platform/generated inputs, original tests,
support, fixtures and build gates remain the atomic transcreation unit. Do not
activate the disabled action seeds, migrate direct DDL piecemeal, or claim D05
complete while its other error/transaction/retry contracts remain open.

## Progress


- [x] Fetch current branches and compare the complete live worker/action-error boundary with Go.
- [x] Reproduce planning and validator panics before production edits.
- [x] Recover action panics into one shared failure checkpoint, retaining source state/count/error rules.
- [x] Prove terminal history, retry-limit exhaustion, owner loss, concurrent pause and discarded staged writes.
- [x] Reproduce the missing panic count; move the existing counter to shared ownership and prove every recovery counts.
- [x] Run scoped suites (117 tests), all-target checks, lint and JSON/Markdown register consistency.
- [x] Commit the reviewed implementation with the actual pre-commit hook and verify a fresh post-commit locked server build. Final receipt amendment/publication must repeat these gates below.

## Context and implementation milestones


The planner in rust/crates/tidb-exec/src/cluster_ddl.rs loads the active queue row
and dispatches one action. Its private action functions create a DdlWrite value;
no metadata is published until real_tikv_ddl.rs commits that value. This commit
layer also runs CHECK validation and stages rows. Both action boundaries need
unwind recovery, while transaction setup, queue loading and committing stay
outside it, matching Go's ownership boundary. Native process aborts cannot be
recovered by catch_unwind and are not treated as ordinary panics.

First extend cluster_ddl_source.rs with a real planner panic and retained raw
arguments/history checks. Extend the existing embedded schema_sync_tests worker
fixture with a panicking CHECK validator. Confirm the new tests fail before
changing production. Avoid a second transaction/retry owner or a test-only
production panic switch.

Next generalize the existing original-snapshot error checkpoint to distinguish
an ordinary error from a panic. Centralize countForPanic semantics: increment once;
ROLLINGBACK becomes CANCELLED, other executing states become CANCELLING; preserve
an older error below the limit; load the fresh global limit; over the limit set
Go's DDL-class CodeUnknown panic diagnostic and CANCELLED. Ordinary errors keep
the existing countForError behavior. Retain undecoded raw arguments after panic.
The planner must not advertise a schema version from an incomplete action.

Catch planner-action and staged-validation unwinds at their action boundaries.
For staged validation keep the original transaction and snapshot; discard partial
planned metadata and the mutation buffer before creating the failure checkpoint.
The shared queue/history and owner checks continue to govern publication. Direct
(nonpersisted) callers retain their existing panic behavior; do not hide arbitrary
programming errors in unrelated transaction or service owners.

Finally move the existing tidb_server_panic_total definition from
rust/crates/tidb-server/src/server_metrics.rs into
rust/crates/tidb-util/src/panic_metrics.rs, exported by that crate's lib.rs.
Retain the server re-export so every existing session/startup consumer references
the same LazyLock and registry family. The action recovery boundary increments
its ddl-worker series before attempting a durable checkpoint, as Go util.Recover
does. Do not create a second counter or a callback registration lifecycle.

## Validation and recovery


Use targeted red/green filters followed by these commands from rust/:

    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo test --locked -p tidb-server --lib server_metrics::tests
    cargo check --locked -p tidb-util -p tidb-exec -p tidb-server --all-targets

From root run git diff --check and make lint. No Go/Bazel inputs change, so
bazel_prepare and Go failpoints are unnecessary. Retain scoped logs as
/private/tmp/tidb-ddl-panic-*.log. Commit with
TERM=xterm git -c core.hooksPath=hooks commit: the actual hook must run
cd rust && cargo build --locked -p tidb-server. After the final commit/amend,
rerun that exact locked build before git push origin HEAD:hparser-integration.
Do not bypass a failing gate; repair/retest the affected path and retain evidence.
Do not force-push over remote work. Verify git ls-remote equals git rev-parse HEAD.

## Surprises & Discoveries


D05's prior external-placement/label description needs qualification: all nine
live persisted handlers currently emit empty placement bundles and no label swap.
Their missing external work belongs to whole-action/DDL integration. Adding an
error checkpoint to that unreachable boundary alone would not fix a live failure.
The duplicate compensation path remains in direct DDL and still needs review
with its source owner before removal. Panic recovery is an active shared-worker gap.

The existing panic counter belonged to the server crate, above its executor
consumer. A red test recovered four panics but observed zero increments. Moving
that existing definition to the utility dependency fixes the ownership direction;
the counter is shared with session recovery and server metric initialization.
The registration policy and public server symbol remain compatible.

## Decision Log


On 2026-10-02, choose recovery at the two existing action boundaries and reuse the
current transaction's checkpoint owner. Catching at the scheduler after unwind
would lose the action snapshot and allow a new transaction to overwrite a newer
pause; broadly catching commit/owner failures would hide infrastructure faults.
The user's standing Go-parity direction authorizes this corrective maintenance.

The typed PersistedDdlJobFailure replaces the old error-only checkpoint API and
all its callers. Returning a panic as a DdlPlanError would incorrectly invoke
countForError, overwrite the previous job error and double-count cancellation.
The Rust unwind boundary discards unfinished owned write sets and mutation buffers;
this avoids publishing partial action outputs. The shared metric is independent
of durable job count because a failed checkpoint still represents a real panic.

## Outcomes & Retrospective


Planner recovery preserves LastSchemaVersion and raw arguments, does not publish
unfinished metadata, and reaches cancellation/history with Go's distinct panic
counts and errors. Embedded validation recovery discards a deliberately staged
metadata marker; owner loss leaves the queue unchanged; a conflicting pause wins;
resume completes rollback and terminal history. Four attempted panics increment
the shared counter even though only two panic checkpoints commit.

Red evidence: from rust/, cargo test --locked -p tidb-exec --test all
persisted_action_panic failed at the real FinishDBJob BinlogInfo assertion;
cargo test --locked -p tidb-server --lib
cluster_session_node::ddl::schema_sync_tests::persisted_worker_recovers_schema_barriers_before_history
failed first with an escaping validator panic and later with counter 0 versus 4.
Logs are /private/tmp/tidb-ddl-panic-before-plan.log,
/private/tmp/tidb-ddl-panic-before-worker.log and
/private/tmp/tidb-ddl-panic-before-metric.log.

After all production edits, the four scoped suites listed above pass 105, seven,
four and one tests respectively (117 total, none ignored); the all-target check
passes with existing warnings. Root make lint passes, including five protobuf
checks; its sandboxed retry initially lacked Go proxy DNS, then passed with network
access. git diff --check passes. A read-only Python consistency check verifies
every JSON finding against its Markdown row and all status totals. Final logs use
the planner-final, transaction-final, worker-final, metrics-final, check and lint
suffixes under /private/tmp/tidb-ddl-panic-*.log.

Changed production paths are cluster_ddl.rs and real_tikv_ddl.rs in tidb-exec,
server_metrics.rs in tidb-server, and lib.rs/new panic_metrics.rs in tidb-util.
Regression paths are tidb-exec/tests/cluster_ddl_source.rs and
tidb-server/src/cluster_session_node/ddl.rs. This plan, the full structural plan,
and current-audit README/structural-findings JSON/Markdown record the review.

D05 remains partial: complete returned-error taxonomy (including toTError),
rollback-transaction classification, isRetryableJobError/timing and whole-action
external effects remain open. The register stays at 85 findings: 75 unresolved
(69 open, six partial), ten repaired. No complete pkg/ddl, pkg/util or pkg/metrics
package is accepted. No disabled action or non-CHECK SQL path is activated.
Mixed Go/Rust clusters, original upstream suites and sysbench/TPCC/TPCH/YCSB are
not run. Recovery covers Rust unwinding, not process aborts; no measured performance
improvement or full source logging/observability acceptance is claimed.

Publication evidence: the actual TERM=xterm git -c core.hooksPath=hooks commit
ran its locked tidb-server build successfully (30.29 seconds); a fresh root
cd rust && cargo build --locked -p tidb-server then passed (16.94 seconds).
The logs are /private/tmp/tidb-ddl-panic-commit.log and
/private/tmp/tidb-ddl-panic-prepush.log. This receipt update is amended only with
the same real hook, then the locked build is rerun after the final amendment and
before pushing. The final response records the published SHA after comparing
git ls-remote origin refs/heads/hparser-integration with git rev-parse HEAD and
confirming git status --short is empty; do not treat local validation as a push.

The first push was rejected because origin/hparser-integration advanced to
80af6a04a61676c227f0507f25ec3ace8fdada25 (a non-overlapping statistics-cache
shard fix). Fetch and rebase preserved that work without conflicts. All four
scoped suites (117 tests), the all-target check and make lint were rerun on the
rebased tree and passed; logs use /private/tmp/tidb-ddl-panic-rebased-*.log.
The final rebased amendment must again run the hook and a fresh locked build
before an ordinary push; no force push is used.

Revision note (2026-10-02): record red/green action and metric ownership evidence,
qualify external delivery against every live handler, and retain unresolved scope.

# Keep committed DDL errors inside the shared worker

This living ExecPlan follows root PLANS.md and continues [archived ddl-error-conversion plan](https://github.com/pingcap/tidb/blob/81367d835b0dbc0d0a252edb02424bb3c01b17e1/rust/docs/ddl-error-conversion-execplan.md).

## Purpose and scope


A persisted action error belongs to the durable job. A worker that successfully
commits that error must continue the job, including rollback, with the same result
after owner replacement. SQL waiters derive success or failure from history.
Remove the private CHECK validation-failure memory and the general escape of
committed action errors to scheduler polling. Apply Go's retry delay after commit,
using the committed error count and current limit; owner retirement interrupts it.

Starting integration is 77683d6dfb4b77ae11e14fba70e165a648200387. A fresh fetch and
fast-forward check found no changes. Source is TiDB master
93a01d31f6da205ae4bf376825293903a6899fdb; client-rust stays 6163ecfc.
This repairs the existing shared worker, not acceptance of the whole pkg/ddl
package. D05 remains partial: source error identities, transaction-reset taxonomy,
missing action effects and complete scheduler ownership still require work.

## Progress


- [x] Trace source transitOneJobStep, isRetryableJobError, the Rust transaction owner and SQL history waiter.
- [x] Reproduce the private CHECK result and committed-action escape before behavioral edits.
- [x] Share committed-error continuation and cancellable retry waiting across all live actions.
- [x] Verify retry classification/budgets, owner retirement, checkpoint ordering and rollback/history (eight transaction tests and six worker tests).
- [x] Verify existing planner/SQL paths (106 planner and 21 SQL tests) and all targets.
- [x] Run lint, diff review and audit row/count consistency checks.
- [x] Commit the implementation through the actual locked-build hook and verify a fresh post-commit build. The receipt amendment repeats both gates before publication.

## Context and implementation


cluster_ddl.rs plans an atomic checkpoint containing active-job metadata and error
count. real_tikv_ddl.rs commits it and runs subsequent steps. The server's ddl.rs
owns the scheduler stop channel and reads terminal history for the SQL result.
Today real_tikv_ddl.rs holds validation_failure only for CHECK and returns other
committed errors as transaction failures. Go transitOneJobStep returns a successful
step after such a commit, optionally waits, then lets the worker continue.

Milestone one extends the existing server worker regression to demonstrate both
result mismatches. Milestone two carries the checkpoint's error count to the
commit outcome, removes validation_failure, and centralizes retry classification
using the existing dbterror code/message lists. Classify the original error before
its plain-error durable conversion; numeric legacy errors still cannot recover
source RFC identity. The worker takes an explicit environment callback for waiting,
alongside its existing ownership check. Production uses the existing stop receiver,
not scheduler notifications or a new timer thread. The delay is Go's one-second
default and is invoked only after a successful checkpoint commit.

Milestone three covers source code/message classification, the count-plus-one
limit boundary, no waiting for terminal/nonretryable results, cancellation during
wait, CHECK rollback with and without owner replacement, and history-derived SQL
errors. Preserve the original-snapshot conflict checks and MDL barriers.

## Validation and acceptance


From rust/, run the pre-fix server regression, then these scoped gates:

    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::schema_changes
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

From root run make lint and git diff --check. SQL fixture setup needs access to
macOS sysctl hw.memsize; use the already-authorized host capability. No Go/Bazel
files change, so bazel_prepare and Go failpoint preparation are not applicable.
Keep logs under /private/tmp/tidb-ddl-continuation-*.log. A passing result must prove
both uninterrupted and restarted workers finish failed jobs while SQL history
retains the original failure, and waiting never precedes the durable checkpoint.

## Publication and recovery


Commit with TERM=xterm git -c core.hooksPath=hooks commit; the actual hook must run
cd rust && cargo build --locked -p tidb-server. Rerun that command after the final
commit or amendment, then push HEAD:hparser-integration without force. If remote
advances, inspect and rebase cleanly, repeat affected checks and both build gates.
Verify the published SHA and clean status. Tests can be rerun without cluster data
cleanup. No generated artifacts or native dependency pins are edited.

## Surprises & Discoveries


Rust's source-error identity loss is upstream of persistence, in numeric/string
adapters. Inventing an RFC class from a number is unsafe: duplicate-key-name can
be either Schema or DDL in Go. This turn instead removes a separate, demonstrated
worker owner split without pretending that the identity migration is complete.

## Decision Log


On 2026-10-02 choose the shared committed-step lifecycle over adding another
CHECK-specific history lookup. It applies to all nine integrated action kinds and
keeps submission results with the existing durable history owner. Environmental
waiting is injected so tests need no sleeps and production shutdown remains joined.

## Outcomes & Retrospective


The two pre-fix runs of cargo test --locked -p tidb-server --lib
persisted_worker_recovers_schema_barriers_before_history failed as intended:
CHECK returned CheckConstraintValidation after completing rollback, and a malformed
CREATE SCHEMA returned Plan(Encode("CREATE SCHEMA job has nil db_info")) instead of
continuing its committed error through cancellation/history. Logs are
/private/tmp/tidb-ddl-continuation-before-check.log and before-action.log.

The initial compile found an inferred closure-argument lifetime in the new retry
test; an explicit borrowed-string argument fixed it. Eight transaction tests and
six worker tests now pass. The shared test preserves prior conflict, owner-loss,
pause, panic and schema-barrier coverage, and adds uninterrupted/restarted failed
job completion, durable checkpoint visibility before waiting, global-limit changes,
owner cancellation during retry and identical CHECK SQL history errors.

All five scoped commands above pass: 106 planner, eight transaction, six worker,
and 21 SQL tests (141 total, none ignored), plus the all-target check. make lint
passes, including five protobuf tests. Existing compiler/linker warnings remain.
git diff --check and audit JSON/Markdown row consistency pass; counts stay
69 open, six partial and ten repaired. Logs use planner, transaction, worker, sql,
check and lint suffixes under /private/tmp/tidb-ddl-continuation-*.log.

Production files changed are rust/crates/tidb-exec/src/cluster_ddl.rs,
rust/crates/tidb-exec/src/real_tikv_ddl.rs and
rust/crates/tidb-server/src/cluster_session_node/ddl.rs. The latter two extend
existing tests. This receipt, full-structural-parity-execplan.md and the current-audit
README/findings JSON/Markdown record the repair and remaining work.

The implementation commit 2cf98fed25 passed the actual pre-commit hook's locked
server build, then a fresh cd rust && cargo build --locked -p tidb-server passed
from the repository root. Logs are commit.log and prepush.log under the prefix
above. The final receipt amendment must repeat the hook and fresh build before
the ordinary push; its logs use final-commit.log and final-prepush.log. Remote
was re-fetched before committing and had no new integration commits.

The broad parity objective stays open with 75 unresolved findings. Source
identity migration, complete taxonomy, configurable
retry timing/metrics and rollback-transaction behavior remain separate requirements.
The scheduler is still serial (D03), so a retrying job can delay later jobs while
it keeps the worker. Go's concurrent scheduler remains required; this repair does
not invent a different fairness policy or establish its workload performance.
Original Go pkg/ddl tests, a real multi-node TiKV/etcd deployment, mixed-Go/Rust
error interoperability and sysbench/TPCC/TPCH/YCSB were not run; no benchmark
improvement or whole-package acceptance is claimed.

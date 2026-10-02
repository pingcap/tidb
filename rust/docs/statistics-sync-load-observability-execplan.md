# Restore the synchronous statistics-load lifecycle from Go master

## Purpose / Big Picture


Statistics loading must report the same outcomes and observations as Go master.
After this repair a request that times out before the statement begins waiting
still activates the configured error or pseudo-statistics fallback. The five
existing shared Prometheus collectors describe admitted tasks, consumed results,
timeouts, complete waits and successful storage reads. No second registry or
load scheduler is introduced. This is maintenance of the existing owners, not
whole-package transcreation acceptance for syncload or pkg/metrics.

This living ExecPlan follows PLANS.md. The user authorized continued Go-master
parity repairs and commit/push to hparser-integration.

## Progress


- [x] Refresh integration and Go master; verify native client dependency is current.
- [x] Trace all syncload production source and its seven master tests, including the newer failed-result-before-timer regression absent from the Go working tree.
- [x] Add and run five failing outcome and observation regressions.
- [x] Repair shared request/wait/read event ownership and migrate stale expectations.
- [x] Run scoped worker, planner, session and storage-backed server tests; lint and review. Results: 29 pass, three independently reproduced baseline failures.
- [x] Update O19 evidence and the register: 69 unresolved, 17 repaired; no package acceptance granted.
- [x] Commit the repair through the actual hook; the locked server build passed. The receipt amendment repeats the same hook.
- [ ] Run the final locked build immediately before push and verify remote SHA; final publication is recorded in the task thread.

## Context and Orientation


Starting integration is 2e66b7c28f60ab2083559222735496c0596f1b4a. Go master is
93a01d31f6da205ae4bf376825293903a6899fdb; native client and the dependency are
19a56ccda1e128218cd33c69709038219aced9bc. There are 70 unresolved findings.

The complete upstream syncload package has stats_syncload.go,
stats_syncload_test.go and BUILD.bazel, with no doc.go or platform/generated
variants. Read master with git show origin/master:<path>: the working-tree Go
package lacks master's seventh test and failed-result error branch. Its shared
collectors are declared in pkg/metrics/stats.go.

rust/crates/tidb-executor/src/driver/catalog/sync_load.rs owns deduplicated tasks
and workers. Catalog::wait_statistics_load in driver/catalog.rs consumes their
results and applies statement policy. stmt_context.rs holds the pending request.
The actual storage-read boundary is ClusterStatisticsItemLoader::load_items in
tidb-server/src/cluster_session_node/mod.rs. It skips stale, already loaded and
unanalyzed objects before reading and publishing into the domain statistics
cache. tidb-stats-handle-metrics already owns all five registered collectors.

## Plan of Work and Milestones


First update the obsolete timeout expectation and add deterministic prepared
channel regressions for both timeout policies, delivered worker errors, wait
timeouts and closed channels. Extend the existing deduplication and real storage
reload regressions with observations. Show failures before production edits.

Then increment the dedup collector only after a distinct task enters the queue.
Record the request start after channels are submitted. Count each received
result or wait timeout, and record timeout once when results remain undelivered.
A worker error is a delivered item, so remove it from the outstanding set as Go
does. Record wait latency only when every requested identity is delivered, using
integer milliseconds since submission. Apply the existing statement fallback
policy to either timeout route. Measure successful storage reads after admission
and before cache publication; omit errors and missing histogram metadata.

Finally run the existing load/cache regression surfaces, check affected targets,
run root lint and update the finding register without claiming complete package
acceptance. Complete source inventory and remaining package limits go in the
repair receipt. Commit using the actual pre-commit hook, then rerun the locked
server build immediately before pushing. Verify remote SHA and clean checkout.

## Concrete Steps and Validation


From rust/:

    cargo test --locked -p tidb-executor --lib driver::catalog::statistics_request_tests -- --nocapture
    cargo test --locked -p tidb-executor --lib driver::catalog::sync_load::tests -- --nocapture
    cargo test --locked -p tidb-server --lib ddl_after_loaded_statistics_matches_go -- --nocapture
    cargo test --locked -p tidb-session --test cardinality_stats_loading -- --nocapture
    cargo test --locked -p tidb-stats-handle-metrics --lib
    cargo check --locked -p tidb-executor -p tidb-server --all-targets

From the repository root:

    GOTOOLCHAIN=go1.25.14 make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m "statistics: follow Go sync-load outcomes and observations"
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Adding an existing workspace metric dependency changes Cargo.lock through Cargo;
no Go/Bazel/generated files change, so bazel_prepare is unnecessary. Rust tests
do not require enabling Go failpoints. Metric assertions must tolerate other
tests incrementing the global collectors; functional tests separately prove
deduplication and error distinctions. No workload speedup or live mixed-node
validation is claimed.

## Idempotence and Recovery


Use fresh test catalogs and prepared channels for deterministic timeout order.
Do not reset global metrics used concurrently by other tests. Preserve unrelated
edits; do not force-push. If a gate fails, diagnose it before publication and
repeat affected checks after repairs.

## Surprises & Discoveries


The Go working tree silently returns success after undelivered results, while
master reports an error and increments timeout. The old Rust regression asserts
the obsolete behavior. Rust also retains failed worker items in the outstanding
set, unlike Go, where a delivered worker error remains diagnostic. Both matter
when choosing which waits get a completion observation.

## Decision Log


- Decision: Repair observations at the actual request, statement wait and storage
  boundaries using existing collectors, together with master's outcome contract.
  Rationale: Instrumenting the worker wrapper would count skipped objects and
  errors as successful reads; merely adding counters would retain the timeout
  race and incorrect completion classification.
  Date: 2026-10-02.

## Outcomes & Retrospective


O19 is repaired, and master's transport-result timeout behavior is restored. Five
regressions failed before the fix and pass after. Scoped checks report 29 pass
and three baseline planner/cardinality fixture failures, reproduced with unchanged
production; expectations were not weakened. All-target checking and root lint
pass. The broader pkg/metrics acceptance and unrelated statistics services remain
separate obligations. See parity/current-audit/syncload-lifecycle-repair.md for
source inventory, test mapping, exact commands and remaining limitations.
The actual pre-commit hook and its locked server build passed. The receipt
amendment repeats that gate. Final pre-push build and publication are recorded
in the task thread after this committed receipt.

## Interfaces and Dependencies


Keep StatisticsItemLoader and StatisticsLoadWorkers interfaces. Add an Instant
to the existing PendingStatisticsLoad state. tidb-executor depends on the
already-existing tidb-stats-handle-metrics crate, sharing the server's collectors.
No new external dependency, background task, protocol or configuration is needed.

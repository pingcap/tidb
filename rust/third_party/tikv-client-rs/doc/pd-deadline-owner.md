# Port the complete PD deadline owner and its TSO integration

This living ExecPlan follows TiDB's PLANS.md and the full structural parity plan.
Keep Progress, Decisions, Discoveries and Outcome current. The acceptance unit
is the whole pinned PD `pkg/deadline` package, a prerequisite of W01. The TSO,
service-discovery and PD root packages retain their complete open inventories.

## Purpose and context


A sent native TSO batch currently can wait indefinitely despite a configured
PD timeout. At TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb`, the
selected PD client is `v0.0.0-20260805103528-afa43111d149`. Its complete deadline
package has `watcher.go` and `watcher_test.go`, no doc.go, generated inputs,
package-local build files, fixtures, build tags or platform-specific sources.
Native baseline is `6f663b396552eec6d1bfad76b65f813e317884a4`.

`clients/tso/dispatcher.go` is the only production importer. Its dispatcher
starts a timer before bounded queue admission and before sending each batch;
expiry cancels the stream; the batch completion callback closes done. Context
cancellation while admission is blocked rejects the deadline. After admission,
caller-context cancellation is not a replacement for done or watcher-parent
cancellation. Preserve these distinctions in Rust.

The complete artifact inventory and dependency decisions are in
[pd-deadline-package.json](pd-deadline-package.json), including all original
tests, shared module/license/build inputs, timer-adapter artifacts and all five
Go test-support files with Linux/non-Linux variants. No package artifact is
omitted or classified as accepted by a keyword search.

## Progress


- [x] Refresh both repositories and Go master; pins are unchanged.
- [x] Read/inventory both package artifacts, timer dependency, original test/support variants and the source caller lifecycle.
- [x] Reproduce two missing deadlines through a loopback PD server before production changes: stalled response body and stalled response headers.
- [x] Implement the whole deadline owner and original TestWatcher cases, bounded/zero-capacity admission, elapsed queued timers, cancellation, done and close/drop paths.
- [x] Integrate batch start/completion with the source-owned stream timeout. Retain/join the oracle worker on retirement; cancel it when its final owner drops.
- [x] Original Go race suite passes; initial 46 focused PD cases and 1,420 native library tests pass, with the same two pre-existing ignored cases. Initial formatting and strict Clippy pass.
- [x] Self-review reproduces completed-result loss during stream retirement; prioritize the already-completed request result before observing stream cancellation.
- [x] Reproduce and repair a lost wakeup in the shared cancellation adapter required by the watcher: register the waiter before checking cancellation state.
- [x] Final validation passes: 1,422 library tests, two pre-existing ignored; strict library Clippy, all-target compilation, formatting and diff checks pass.
- [x] Complete final diff review and all native validation gates.
- [ ] Publish native master, synchronize TiDB and complete its required lint/build/publication gates; the TiDB receipt records the final publication outcome.

## Implementation and decisions


`src/pd/deadline.rs` maps NewWatcher/Start/Watch to a retained serial worker,
a bounded admission queue and a single-use completion handle. The queue also
supports Go's zero-capacity rendezvous contract. Tokio's bounded mpsc does not
support capacity zero, so queue admission is represented explicitly under a
short mutex; watch notifications preserve wakeups between checks and waits.
Callbacks execute outside that queue lock. Deadline start time precedes any
capacity wait. Dropping the completion handle does not complete the operation.
The worker checks the parent scope and releases queued callback resources at
shutdown. Explicit close joins it; last-owner drop cancels it without retaining
an Arc cycle. No per-deadline task is spawned.

Use existing Rust Cancellation, Tokio monotonic deadlines and the log crate.
Do not recreate Go's pooled timer objects: storing the expiry instant and creating
the active Tokio sleep has the same expired/stop semantics without stale timer
channel events. This is a documented runtime adapter, not a separate claim of
porting the timerutil package or improving its allocation benchmark. Go's
negative durations map to immediate expiry, represented by Rust Duration::ZERO.

`src/pd/timestamp.rs` now owns one watcher per stream. An asynchronous request
stream admits the deadline before yielding the batch, covering response-header
establishment as well as body reception. Completion disarms its deadline.
Stream failure/timeout drops pending senders and ends the watcher. The oracle
retains the worker handle; retiring it joins through cancellation. A semaphore
preserves the previous native outstanding-batch bound instead of the old manual
AtomicWaker/poll implementation. This milestone does not change the existing
native batch-size/pending bounds or claim those match the full Go dispatcher.
`src/pd/cluster.rs` carries the configured timeout and joins the retired oracle
before publishing a replacement. No hand-edited generated protocol types,
new runtime settings, retry budgets or private per-request timeout policy.

Decision (2026-10-01): accept the complete deadline leaf first, with necessary
parent call-site integration. The earlier W01 plan explicitly requires complete
dependency packages before parent acceptance. Completing one independent source
package is valid; calling this a complete PD or TSO port would not be. P06 remains
partial: public PdRpcClient.close does not yet retire the PD owner, and the full
root/discovery/retry lifecycle remains work. P03 and P07 remain open as well.

The shared `src/async_util.rs::Cancellation` adapter also required a correction:
`notify_waiters` can run between checking the flag and registering a waiter,
leaving the waiter asleep forever. Registering/enabling the wait first and then
checking state closes that race for both direct and parent cancellation. A
per-instance test-only interleaving cancels exactly in the old window and fails
before the repair. It introduces no global test hook or runtime policy.

## Discoveries and regression evidence


The original native stream used a discarded JoinHandle and did not apply its
connection's configured timeout to batches. Both loopback regressions exceeded
the 500 ms test bound with a configured 20 ms timeout on unchanged baseline.
The live-stream completion control passed, so the defect is not simply an
unreachable mock endpoint. Logs: `/private/tmp/pd-deadline-red.log`.

Both regressions pass after the repair. Additional tests prove zero-capacity
admission, cancellation while blocked, timer expiry measured before admission,
serial queued processing, caller-versus-parent cancellation, discarded done
handles, concurrent close and callback release. Transport cases prove a completed
batch leaves its stream alive beyond the old deadline, 256 concurrent allocations
have unique logical timestamps, and explicit retirement/final-owner drop stops
a stalled stream well before its timeout. These do not certify every PD API.

Self-review found that a response already delivered to its oneshot could lose a
race against stream cancellation. Go Request has a separate request/client
lifetime and keeps the completed result when only the stream retires. The new
`source_tso_completed_result_survives_stream_retirement` test failed before the
correction; the receive path now gives that delivered result priority. Evidence:
`/private/tmp/pd-deadline-completion-red.log`. The shared adapter regression also
fails deterministically before repair; its evidence is
`/private/tmp/pd-deadline-cancellation-red.log`.

## Validation and publication


The original source is copied unchanged into
`/private/tmp/tidb-pd-deadline-go-source`; the module cache is not edited. Run:

    go test -mod=readonly -race ./pkg/deadline -count=1

That command passes, including the source TestMain goleak gate. Neither deadline
nor its selected test path contains failpoint calls, so no instrumentation is
needed for this scoped test. The module-wide Makefile test/static/tidy targets
are recorded as shared build inputs, not silently run against unrelated packages.

From `/Users/qiliu/projects/client-rust`, the commands are:

    cargo test --locked --lib source_tso_ -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

The first command is the baseline red run; the remaining tests are green runs.
Full library results retain the two earlier ignored tests; no new case is ignored.
Logs use `/private/tmp/pd-deadline-*.log`. Native publication uses normal
`git push origin HEAD:master`. Then synchronize through TiDB's
`bash rust/scripts/sync-tikv-client-rs.sh`, inspect the resulting native SHA and
compatibility patches, run affected checks and `make lint`, and publish through
the actual pre-commit hook's locked server build plus a fresh locked build
immediately before `git push origin HEAD:hparser-integration`. No force push.

## Outcome and recovery


The source deadline package and required batch call sites are implemented.
The final native library run passes 1,422 tests with two pre-existing ignored;
strict Clippy, all-target compilation, formatting and diff checks pass. Native
publication and TiDB integration are recorded in the corresponding TiDB receipt.
Public PD close propagation, service-mode discovery, metadata concurrency and
whole-parent original-case coverage remain open; the 77-finding count does not
decrease. Linux execution, live PD/TiKV failover, TLS/microservice integration
and sysbench/TPC-C/TPC-H/YCSB measurements are not claimed by this leaf repair.
This is correctness work; throughput/latency gains remain unmeasured.

The new tests use loopback listeners owned by their fixture and abort the fixture
server on drop. No external cluster/data is changed. Re-run scoped tests after
any repair. Preserve failure evidence and user changes; a published correction
uses a reviewed follow-up or revert, never history rewriting. Update the parent
package inventory when the remaining W01 owners are implemented.

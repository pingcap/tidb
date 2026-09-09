# Follow Go completion ownership and resolve remaining transport costs

This living ExecPlan follows root PLANS.md. The full sysbench/TPC-C
optimization goal remains active. This is a checkpoint, not an accepted
performance improvement or whole-Go-package completion.

## Purpose / Big Picture

Improve generic Rust SQL throughput and latency while preserving Go's task
concurrency, paging, retries, cancellation and results. Acceptance requires
beating the immutable faster Rust control on identical work, then validating
TPC-C and mixed writes. Fewer named profile frames are not a performance win.

## Progress

- [x] Read pinned Go normal result-channel and explicit callback paths.
- [x] Use native single-result replies, native notification and absolute-deadline
  thread parking, preserving cancellation and exact in-flight retirement.
- [x] Resolve faster control SHA256 84c39b216 to its archived receipt and
  connection-local worker topology.
- [x] Implement connection-local current-thread I/O drivers while retaining
  the shared command owner, independent cop workers and scoped shutdown.
- [x] Run consolidated release tests: 2,056 passed, zero failed, 16 ignored.
  Build the server with twelve jobs and verify selected live SQL equality.
- [x] Complete 40 fixed-work trials: 240,000 measured and 40,000 warmup
  transactions, with zero SQL errors or reconnects.
- [x] Compare native stacks and running-state Time Profiler traces against the
  immediate predecessor; locate the additional cross-runtime kernel wake.
- [x] Verify seven owned PIDs gone, fourteen ports closed and four supervisors
  stopped.
- [x] Run Ready lint for the requested checkpoint; the authorized network retry
  passed after sandbox DNS blocked linter dependency resolution.
- [x] Prepare the requested accumulated Rust checkpoint for hparser-integration,
  preserving unrelated local changes. Report the verified remote SHA at delivery.
- [ ] Remove unnecessary cross-runtime wake handoffs at the ownership boundary,
  remeasure, and beat the immutable faster control.
- [ ] Validate TPC-C, mixed writes and required whole-package/platform semantics.

## Context and Orientation

tidb-txnkv rpc/batch/completion.rs distinguishes native oneshot results from
explicit callbacks. async_completion.rs owns notification tokens plus native
Notify. execution.rs::wait_with_call uses a thread-local Waker and native
parking against the caller's original deadline. Those prior changes remain.

execution.rs::ConnectionRuntime now owns a native thread running one Tokio
current-thread I/O driver. TransportRuntime owns one driver per configured
connection slot. ConnectionTasks takes a Handle, retains connection-generation
task scopes and joins shutdown; TransportRuntime then stops and joins drivers,
reporting panics. The async command owner and cop workers still use the shared
multi-thread runtime. No connection-count tuning knob was added.

tidb-distsql cop_paging/cop_iterator.rs owns independent task workers, ordered
response buffers, unordered rendezvous and close/join. tidb-exec cop_scan.rs
decodes on the SQL consumer. Preserve these source-required responsibilities.

## Decision Log

2026-09-09: Test connection-local I/O to remove the shared h2 mutex contention
seen in native profiles. Go gRPC reader/writer loops are connection-owned
goroutines, but that does not require a distinct Rust runtime or pinned OS
thread. The runtime split is an experiment, not a parity requirement.

2026-09-10: Do not accept the experiment as a win. It removes the targeted named
h2 wait stage but adds a cross-runtime wake path and regresses fixed-work timing.
The source remains present in this user-requested checkpoint for continued
root-cause work. Do not silently promote it as the performance baseline.

2026-09-10: The user's commit-and-push request supersedes the earlier
continuation's no-Git boundary. Publish only to hparser-integration, normally,
without force-pushing. Include accumulated Rust implementation, tests and
receipts; exclude unrelated root tools and review artifacts.

## Surprises & Discoveries

Against the faster control at 1/8/32 clients, candidate throughput changes are
-9.33/-9.45/-6.65 percent, SQL CPU +49.89/+40.74/+29.79 percent and p95 latency
+10.49/+9.98/+8.76 percent. All seven ABBA blocks regress.

Against the immediate predecessor at eight clients, throughput is -1.84 percent,
SQL CPU +6.96 percent and p95 +2.79 percent. All three ABBA blocks regress. These
fresh comparisons isolate this edit better than comparing historical receipts.

Native all-thread profiles show named h2 mutex-wait leaves falling from 85 to
zero and mutex-drop leaves from 20 to zero. Counts include waiting and are not
CPU percentages; absence of a named frame does not prove zero contention.

Separate Time Profiler captures contain 27,404 prior and 27,777 candidate
Running samples. Candidate self-CPU fractions rise for condition-variable wait
(7.64 to 8.88 percent), signal (4.43 to 5.47) and kevent (5.35 to 5.85).
One exact candidate chain has 221ms sampled CPU:

    run_worker -> publish_batch -> submit -> flush -> send_group
      -> Tokio wake_by_val -> I/O driver Handle.unpark -> kevent

Pinned Tokio 1.53.0 current_thread::Handle.schedule enqueues externally
scheduled work and unconditionally unparks its driver. The driver calls mio
1.2.2 Waker; on macOS, kqueue wake issues kevent. A shared-runtime local wake can
avoid this remote syscall. The source and trace prove the added boundary, not
an exact accounting of the full CPU increase.

grpc-go v1.79.3 internal/transport/http2_client.go starts reader and loopy-writer
goroutines per connection. Go does not pin each connection to a separate
runtime or OS thread. Keep the ownership model without copying an unjustified
scheduling boundary.

## Plan of Work and Milestones

The notification/wait and I/O placement milestones are measured experiments.
Do not optimize already-removed no-op callbacks or blocking-reply timers again.
The async command collector still has a timer.

Next compare command publication, connection send/receive and cop/SQL handoffs
in pinned Go, the immediate predecessor and the faster control. Remove
unnecessary remote wake boundaries while preserving connection lifecycle and
request semantics. Do not tune arbitrary batch sizes, bypass workers or rebuild
the Go runtime. Consolidate the change, run affected validation once, rebuild,
and repeat the fixed workload before acceptance.

## Concrete Steps and Validation

From /Users/qiliu/projects/tidb/rust:

    cargo test --offline --locked --release -j12 --no-fail-fast -p tidb-distsql -p tidb-txnkv -p tidb-unistore -p tidb-exec --lib --tests
    cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server
    rustfmt --check --edition 2021 --config skip_children=true crates/tidb-txnkv/src/rpc/execution.rs crates/tidb-txnkv/src/rpc/transport_runtime.rs crates/tidb-txnkv/src/rpc/channel_pool.rs crates/tidb-txnkv/src/rpc/batch/transport.rs crates/tidb-txnkv/src/rpc/unary.rs crates/tidb-txnkv/src/rpc/liveness.rs

From repository root:

    GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint
    git diff --check -- rust
    ruby /private/tmp/tidb-connection-io.kkpvKx/verify-cleanup.rb

The first two release commands passed again before checkpoint publication, with
2,056 tests passed and 16 ignored. Sandbox-denied loopback binds required an
authorized rerun. All 32 archived source/configuration files match current bytes,
and the rebuilt binary matches the measured SHA256. Ready lint passed. No Go/Bazel
files changed, so bazel_prepare is not required.

Seven selected SQL cases match Go/control/candidate, including 100,000-row
ordered scans, secondary range, residual limit, join, aggregation and prepared
ranges. Workload hashes match before and after timing. The live Go binary
differs from checkout, so this does not establish full Go parity.

Full Rust workspace tests (including seven known executor failures), live
transaction difftests, TPC-C/mixed-write performance and full platform/package
semantics are not verified for this checkpoint. It is not release readiness.

## Outcomes & Retrospective

Candidate SHA256 76f6f08fa55c6f03ca4559fc6c97fe55d8c6e3e956f0f8f86b8efeeee2f6446e
is slower than both immediate predecessor dee4e9927 and faster control 84c39b216.
The targeted lock stage is gone; the runtime split introduces a source-backed
remote wake cost. Connection ownership is the right semantic boundary, but
separate current-thread runtimes are not established as the right execution
strategy. The full optimization goal remains open.

## Idempotence and Recovery

Current evidence is /private/tmp/tidb-connection-io.kkpvKx. before-source.tar.gz
preserves pre-edit source and baseline; candidate-source.tar.gz preserves the
measured source. Binaries, raw trial logs, SQL hashes, native samples, CPU traces
and Go pprof remain local. rust/benchmarks/cop-progress-baseline.json carries
the current summary, exact comparisons and artifact references.

All owned supervisors are stopped. Preserve fixture data at
/private/tmp/tidb-live-workload.nIrZBe. Before reuse, archive logs/manifest.
Never restore a source archive over the shared dirty worktree.

## Interfaces and Dependencies

Existing public reply and callback interfaces remain. Only native scheduling
ownership changed in this I/O increment; earlier checkpoint work also includes
dependency changes. No new protocol rules or SQL-specific shortcuts were added
by the I/O experiment.

# Attribute SQL write tails across gRPC and KV execution

This ExecPlan follows PLANS.md. Maintain Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective when continuing the experiment.

## Purpose / Big Picture

The previous IDC experiment reduced store-level Raft commit-log latency by
over 95% at the tail, but SQL P99 improved only 2.5% at read concurrency 8 and
18% at concurrency 16. Determine which stages occupy the critical path of the
same slow SQL transactions, rather than subtracting unrelated percentiles.
A completion queue (CQ) delivers gRPC operation completions to its polling
thread. A completed KV operation can still wait for response processing on
that thread. Record both boundaries and join the client and server by a
stream nonce and BatchCommands request ID, with SQL trace ID on the client.

The user has authorized publishing the independent diagnostic branches and
triggering EE ChatOps to build all diagnostic images. Keep the original PRs
unchanged and report the completed images. IDC deployment, workload execution,
and report publication follow after image preparation.

## Progress

- [x] Identify historical CSE 905f2bd7f1293737231c53cee940693a696d3111 and grpc-rs b78542032f7f7eabd42bc4457ebc3d51efe2e266.
- [x] Resolve the old TiDB image to 788ad51619dd42c2e84c2e329a6e0a5e9a6461e6 and its client-go dependency to 8edb23f6c7ee63d3c0604d168942bccbc99b123e.
- [x] Create independent codex/raft-grpc-latency-trace worktrees for CSE, grpc-rs, client-go and TiDB, and codex/cq-ready-trace for C-core.
- [x] Add initial CQ/codec/task events, storage callback/response boundaries and client batch correlation.
- [x] Add opt-in bounded asynchronous TiDB event export.
- [x] Complete request/batch mappings; native CQ stall probe and Go diagnostic race tests pass.
- [x] Build the NextGen TiDB Linux binary with the instrumented client-go.
- [x] Finish CSE compilation, formatting and scoped validation; record the legacy dependency warning below.
- [x] Save the diagnostic branches and record check results.
- [ ] Publish remotely fetchable diagnostic dependencies and build immutable images through EE ChatOps.
- [ ] Validate IDC access and run shared 5, KV 4 + Raft 1, KV 5 + Raft 1.
- [ ] Analyze same-request traces and publish the Chinese report at https://pingcap.feishu.cn/wiki/MGSjwKITJi90dEkb3XVc2lI8neg.

## Surprises & Discoveries

The recorded TiDB image index is sha256:5ef15b246e4f7ca77377dffaaf0e68920e12d8b779de3c8c709ea1859f92fb3c.
Its amd64 manifest's image configuration labels identify the source commit;
the floating master-nextgen tag must not be used as source identity.

The old namespace read was rejected by RBAC on 2026-10-15. Saved startup
metadata and the registry's immutable configuration recovered the baseline
without changing IDC resources. Deployment remains unverified.

The pinned gRPC C-core's old Abseil needs explicit cstdint/limits includes
with GCC 14. Apply those flags only to gRPC's native build.

TiDB's default flight recorder enables KV and transaction categories. The
new diagnostic events therefore also require TIDB_CQ_TRACE_FILE explicitly;
the existing category switch alone is insufficient to keep them opt-in.
The legacy dump trigger and TiKV trace-control extractor also remain active
independently of recorderEnabled. Diagnostic mode bypasses both, so the new
asynchronous recorder does not simultaneously request immediate TiKV logs.

CSE make clippy exits 0 after resolving the old empty-slice test inference
ambiguity introduced by the diagnostic serde_json dependency. Cargo still
reports nom 2.2.1's existing future-incompatibility notice; this is recorded
as a remaining warning, not a warning-free lint pass. Dependency upgrades and
formal release qualification are outside this temporary diagnostic build.

Existing kv_grpc_wait_time_ns covers response enqueue to batch retrieval. It
does not measure inbound CQ queueing. Existing WriteDetail and read-pool task
details are useful per-request data and should be reused. BatchCommands
futures use poll_future_notify, so result delivery need not execute on a CQ;
record the actual thread instead of assuming that all wake delays are CQ waits.

## Decision Log

- Decision: branch from the historical tested revisions, not the rebased PR heads.
  Rationale: diagnose the reported gap without unrelated version changes.
  Date/Author: 2026-10-15, Codex.
- Decision: reuse TiDB traceevent and SQL trace IDs, with an asynchronous diagnostic sink.
  Rationale: preserve transaction correlation without a second SQL tracing system.
  Date/Author: 2026-10-15, Codex.
- Decision: observe completion-ready in C-core before both queue publication and cached-event publication.
  Rationale: timing only the Rust handler cannot measure CQ wait accurately.
  Date/Author: 2026-10-15, Codex.
- Decision: pin remotely fetchable diagnostic dependencies for the EE ChatOps build.
  Rationale: the user has advanced to image building. The standard builder checks
  out one repository, so local sibling paths must become fixed Git revisions.
  Publish only independent test branches, with no new PR or formal release process.
  Date/Author: 2026-10-15, Codex.

## Outcomes & Retrospective

The temporary instrumentation is implemented on independent branches. CSE
make format completed; Linux make clippy exited 0 with the legacy nom notice
above. The native blocked-CQ and request-scope tests passed, as did client-go
and TiDB diagnostic race tests. A Linux NextGen TiDB binary ran a local
unistore BEGIN / UPDATE / COMMIT smoke; SQL, 2PC, TSO and MySQL flush events
shared the expected trace IDs, with zero dropped events. Its temporary
server was stopped. This checks instrumentation, not IDC performance.

No diagnostic image or new IDC performance result exists yet. The cause of
the remaining SQL tail remains to be established with same-request traces
from the three IDC configurations in the next authorized stage.

## Context and Orientation

TiDB worktree: /Users/hjt110/pingcap/worktrees/raft-grpc-trace/tidb.
client-go worktree: /Users/hjt110/pingcap/worktrees/raft-grpc-trace/client-go.
grpc-rs worktree: /Users/hjt110/pingcap/worktrees/raft-grpc-trace/grpc-rs.
Its grpc-sys/grpc directory is an independent C-core worktree.
CSE worktree: /Users/hjt110/.codex/worktrees/raft-grpc-latency-trace/cloud-storage-engine.
All original checkouts and original PR branches must remain untouched.

In TiDB, pkg/util/traceevent emits statement and transaction lifecycle events.
internal/client/client_batch.go in client-go multiplexes RPCs onto a stream.
The server decodes a BatchCommands message in grpc-rs, dispatches its requests
in components/cloud_server/src/service/kv.rs, and executes writes through
src/storage/txn/scheduler.rs in CSE. The storage callback can wake its future
before the CQ retrieves the response batch. The gRPC sink then serializes
and submits the response to C-core. Local send completion is not remote receipt.

## Plan of Work

First keep diagnostics runtime-disabled by default. GRPCIO_TRACE_FILE selects
a fresh JSONL file on TiKV. TIDB_CQ_TRACE_FILE selects one on TiDB. Both queues
are bounded, export asynchronously, and report dropped-event counts. TiDB
enables stmt_lifecycle, txn_lifecycle, txn_2pc and kv_request automatically
before connections are opened, without extra SQL or config changes.

Record native completion-ready and Rust completion-pop with the operation tag
and CQ identity. Map receive/send submission tags to a call. Carry the receive
tag through streaming prefetch to its decode interval. Pair encoding intervals
by their own span ID. Record CQ task wake/poll intervals for occupancy context.

Map each stream's x-cq-trace-stream metadata nonce to its server call identity.
Map each request ID to storage submission, actual callback readiness, future
consumption, response enqueue and batch retrieval. Map scheduler stages using
the tracker token, including its reuse sequence. Response membership and
encoding/send boundaries must remain reconstructible across batching.

On the client record batch enqueue, selection, send, decoded receive,
notification and caller resumption, using elapsed durations from the same
entry's monotonic origin. Export its received ExecDetails, including commit
log, WAL, scheduler and read-pool details. Preserve the original stream nonce
in immutable batch state so reconnects cannot relabel outstanding requests.
Add explicit commit-TS and whole 2PC timing to distinguish non-KV delays.

## Concrete Steps

From the grpc-rs worktree initialize pinned C-core dependencies, then run:

    cargo +nightly-2026-01-30 fmt --all -- --check
    CARGO_BUILD_JOBS=1 CMAKE_BUILD_PARALLEL_LEVEL=1 cargo +nightly-2026-01-30 test -p grpcio --no-default-features --features protobuf-codec diagnostics -- --test-threads=1

From client-go use Go 1.25.14 or newer and run scoped diagnostic/batch tests
with the race detector. From TiDB run targeted traceevent tests and the
NextGen Linux server build. The client-go module override pins the diagnostic
fork for this temporary image; no formal Ready or release claim is made.

From CSE run make format and make clippy. Run Cargo invocations serially;
keep build parallelism small on shared hosts. Keep full output in the
experiment's validation directory and report
environment failures separately from successful checks.

## Validation and Acceptance

Acceptance requires a controlled CQ stall to produce completion-ready before
completion-pop, with an operation mapping; nested request context must restore
correctly; streaming prefetch must preserve the decoded message's original
receive tag; a reconnect must retain the old stream identity on outstanding
requests. Async export must freeze mutable fields, retain trace IDs and expose
loss, without recording SQL text, keys, values or protobuf objects.

Before performance runs require trace headers, zero drops, all victim RPCs
joined, and matching counts against successful SQL calls. Reject incomplete
tail attribution instead of treating missing spans as zero. Check tracing
overhead with the same image and tracing disabled/enabled.

Use the original two keyspaces, 100/s target small writes, large reads of
16384 rows with 4 KiB payloads, c8/c16, and 30/30/30-second phases. Repeat
three modes with identical images and fixed placement/configuration; measure
actual read throughput. Extend the mixed phase when estimating P999, since
the original 30 seconds provide only a few tail observations per run.

Report pooled SQL P99/P999 and same-SQL tail-stage distributions. Show a small
set of representative slow-request timelines aligned with same-CQ large-COP
encode intervals. Keep Raft persist and replication overlap explicit; do not
sum overlapping stages or subtract independent histogram quantiles. Use local
monotonic durations; cross-host offsets and transport residuals must not be
presented as exact one-way network delay. If the traces still leave a residual,
state it and investigate transport/polling rather than forcing a CQ conclusion.

## Idempotence and Recovery

Use a new trace filename after each process restart; files are created
exclusively to prevent mixing clock epochs. Disable the two environment
variables to stop recording on the next restart. Restore original images only
during an authorized deployment stage. Never reset original user checkouts.

## Artifacts and Notes

Historical workload and evidence:
/Users/hjt110/experiments/raft-grpc-isolation-idc-20261012/comparison.
Record new validation logs and a repository/commit manifest under
/Users/hjt110/experiments/raft-grpc-tail-trace-20261015.

## Interfaces and Dependencies

grpcio::diagnostics::Event contains event, call, request, cq, operation and two
event-specific numeric values. Request captures call/request across callbacks;
its synchronous scope must not be held across await. C-core's native observer
must not block or reenter gRPC. These are test-image diagnostics, not upstream
production API proposals. Keep changes on their independent branches.

Plan created 2026-10-15 while implementing stage one.

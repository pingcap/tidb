# Align transport response ownership with client-go

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture

Use Go's batch receive loop as the source of truth for Rust response ownership,
retirement and stream acknowledgement. Avoid inferring an equivalent native
scheduler from Go goroutine syntax. This is scoped RPC alignment, not completion
of a whole Go package or of the throughput/latency goal.

## Progress

- [x] Compare client-go e4905600583b internal/client/client_batch.go batchRecvLoop
  with Rust batch/wire.rs, batch/inflight.rs and batch/transport.rs.
- [x] Attribute the preceding shared-scheduler regression with matched Rust CPU
  traces and Go pprof. Artifacts: /private/tmp/tidb-shared-scheduler.fEsNL7.
- [x] Reject connection-future grouping after 120,000 sysbench and 48,000 TPC-C
  measured events. It removes the sampled h2 lock-wait chain but increases
  32-client elapsed 2.39% and CPU 3.92% against f53ad10ae4.
- [x] Reject separate I/O-driver ownership after 84,000 sysbench and 36,000 TPC-C
  events. CPU decreases in serial/TPC-C, but serial throughput regresses.
  Both experimental patches and binaries remain only in the artifact directory.
- [x] Remove per-response body cloning, the separate maximum-ID scan, and the
  response packet's unique-ID HashSet. Preserve outgoing-ID validation.
- [x] Reproduce the outdated-ID regression before changing wire acceptance.
  An existing test's packet [0, 11, 10, 10] fails with ZeroRequestId even though
  request 10 is pending. After the fix it completes once, counts three outdated
  responses and advances the watermark to 11.
- [x] Pass all 97 retained transport/batching tests with the response fix.
- [x] Complete release build, all-target check, formatting and Ready lint.
  All 84,000 sysbench and 36,000 TPC-C measured events pass bounded Go equality
  and all eleven consistency checks. Record ../benchmarks/batch-response-baseline.json.
- [x] Restore auto-analyze, stop owned services, verify 20 PIDs absent and ten
  ports closed, and retain the fixture.
- [x] Prepare the validated increment for normal publication to
  origin/hparser-integration; verify the published SHA in the task handoff.
- [x] Preserve collaborator updates through 2cfcc35b46 after a rejected normal
  push. The merge does not change txnkv source. Four planner tests pass; executor
  checks give two passes and the same known condition-nine inner-selection failure.
  Merged build and Ready lint pass. Another 6,000 sysbench and 6,000 TPC-C events
  pass fresh Go equality and eleven consistency checks. Restore settings and
  verify four owned PIDs absent and ten ports closed before retrying publication.

## Surprises & Discoveries

Globally parallel h2 body/driver polling adds mutex contention. Grouping these
futures removes that sampled chain but leaves scheduler wake/park overhead and
does not improve fixed-work performance. A dedicated I/O driver likewise does
not yield an overall throughput improvement. Neither experiment is retained.

Rust also rejects an entire response packet for unknown zero or duplicate IDs,
where Go looks up each ID and continues when it is no longer pending. That
Rust-only rejection retires the stream and fails unrelated pending requests.
Its per-packet HashSet allocation and response-body clones are unnecessary.

## Decision Log

Retain the existing shared runtime and connection JoinSet lifecycle. Change the
actual Go/Rust response semantic mismatch, not scheduler worker counts or batch
policy constants. Deliver owned response bodies directly and compute the maximum
ID in the same pass, including outdated IDs. Publish the watermark afterward as
Go does. Keep cardinality and missing-command handling outside this increment.

## Context and Orientation

rpc/batch/wire.rs owns decoded packet bodies and exposes a consuming iterator.
rpc/batch/inflight.rs owns pending request retirement, cancellation and completion.
rpc/batch/transport.rs feeds decoded packets into that table and retires a stream
on decode/protocol errors. No call deadlines, connection identities, forwarding,
batch policy, retry rules or runtime ownership change in the accepted candidate.

## Plan of Work

Run the retained suite as one batch. Build one immutable release server, then
compare fixed-work sysbench at one/eight/32 clients and TPC-C at eight clients
against the unchanged f53ad10ae4 parent. Keep profiling separate from timing.
Report measured tradeoffs and do not promote noise into a performance win.

## Concrete Steps

From rust/: CARGO_BUILD_JOBS=12 cargo test --offline --locked --release -j12
-p tidb-txnkv --lib --test all --no-fail-fast -- batch_ transport_ connection_
--test-threads=12. Build with the same Cargo flags and -p tidb-server
--bin tidb-server; check with -p tidb-txnkv --all-targets. At repository root:
GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint. The live harness is
/private/tmp/tidb-shared-scheduler.fEsNL7/response/verify.rb.

## Validation and Acceptance

Use the Ready profile for scoped publication. Exact work counts, zero errors,
bounded Go SQL equality and all eleven TPC-C consistency conditions must pass.
The earlier planner condition-nine inner-selection failure also reproduces on
unchanged a075207030; do not remove it or claim full-workspace parity.

## Idempotence and Recovery

Preserve all immutable controls and the fixture. Restore auto-analyze and stop
only owned processes. Use apply_patch to discard experiments, never reset
unrelated files. Only normal pushes to origin/hparser-integration are authorized.

## Outcomes & Retrospective

Response correctness and Ready checks pass. Serial/eight/32-client throughput
changes +0.29%/0%/-0.52%; TPC-C elapsed changes -1.50%. SQL CPU differences are
small (-0.09% to -1.23%). These local pairs are mixed/near-neutral, not an overall
performance win. Full Go-package parity, the shared-scheduler CPU regression and
the persistent throughput/latency goal remain open.

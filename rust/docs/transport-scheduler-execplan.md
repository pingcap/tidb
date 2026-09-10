# Align transport scheduling with client-go

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture

Remove serialization of independent TiKV send and receive tasks on a dedicated
Rust I/O thread. Pinned client-go e4905600583b internal/client/conn_batch.go and
client_batch.go run these loops as independently scheduled goroutines. Rust
should use its existing multi-thread execution scheduler while keeping explicit
connection task scopes and transport shutdown ownership. This is a scoped
alignment experiment, not a whole-package parity claim.

## Progress

- [x] Read Go collector and Rust admission, transport, completion and scheduler
  ownership. Retained diagnostic artifacts: /private/tmp/tidb-rpc-stages.pWypr5.
- [x] Observe eight-client mean batch wait of 49.5 microseconds in Rust versus
  25.2 in Go, and caller wake delay of 9.7 versus 6.0 microseconds. Instrumented
  samples identify stages, not a proven scheduling cause or performance win.
- [x] Remove dedicated TransportIo runtime/thread and spawn the transport owner
  on execution_runtime. Retain joins of worker and connection task scopes.
- [x] Run retained transport tests, compare matched live workloads, and remove
  temporary stage logging before accepting production changes.
- [x] Pass 24 RPC unit tests and the final 97-test transport/batching batch;
  release build, all-target txnkv check, formatting and Ready lint pass.
- [x] Complete 84,000 sysbench and 36,000 TPC-C measured transactions, preserving
  exact work, bounded Go equality, all eleven consistency checks and settings.
- [x] Finish validation after preserving upstream planner commits through
  a075207030. Two planner checks pass; condition-nine fails at its analyzed
  inner-selection assertion. The unchanged parent fails identically. Preserve
  that failure; do not weaken the test or claim complete planner parity.
- [x] Final merged release build and Ready lint pass. Run another 6,000 sysbench
  and 6,000 TPC-C events; bounded Go equality and eleven consistency conditions
  pass. Restore auto-analyze, verify ten owned PIDs absent and ten ports closed,
  and retain the fixture.
- [x] Complete scoped source and validation for publication on
  origin/hparser-integration; full performance acceptance remains open.

## Surprises & Discoveries

Rust send/receive timing setters were not connected to production. The temporary
probe connects them to actual submission and receipt boundaries. Submission into
tonic's stream is not the same boundary as grpc-go Send returning, so individual
send latency is not directly comparable. Approximately 130,000 RPCs per 6,000
transactions are observed on both sides; range requests span multiple regions.

## Decision Log

Use the existing execution scheduler, not another tunable runtime or a workload
specific batch policy. Tasks already have connection-scoped cancellation and
joining; scheduler lifetime need not equal connection lifetime. Accept only after
lifecycle tests and measured workloads; investigate a regression before deciding
whether to retain or discard the experiment. Retain the simpler Go-shaped task
ownership, but do not promote overall performance: two clean pairs show 32/8
client TPS +2.18%/+1.28%, serial TPS -1.66%, TPC-C elapsed +1.00% and SQL CPU
+12.38%. The extra CPU requires further attribution, not arbitrary worker tuning.

## Context and Orientation

rpc/execution.rs owns execution_runtime and ConnectionTasks. rpc/transport_runtime.rs
owns the command task, which closes all batch and channel owners before replying
to shutdown. ConnectionTasks retains tonic/h2 tasks in a JoinSet. Ordinary SQL
waits and synchronous heartbeat calls already release Tokio workers through
block_in_place. Do not change those response/cancellation contracts.

## Plan of Work

First remove TransportIo and its extra shutdown stage, retaining the existing
worker and connection scopes. Then run transport and batching tests as a batch,
build the release server, and compare fixed-work sysbench at one/eight/32 clients
and TPC-C with the retained unmodified 308af binary. Finally remove probes and
run Ready validation before a final completion claim.

## Concrete Steps

From rust/: CARGO_BUILD_JOBS=12 cargo test --offline --locked --release -j12
-p tidb-txnkv --lib -- rpc:: --test-threads=12. Build using cargo build with the
same flags and -p tidb-server --bin tidb-server. At checkout root run
GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint for Ready validation.

## Validation and Acceptance

Retained shutdown/panic/connection-generation tests must keep passing. Live
requests must preserve exact counts, zero errors and bounded Go SQL equality;
TPC-C must pass all eleven supported consistency conditions. Reject speedup
claims from instrumented or overlapping noisy samples. Preserve the fixture,
restore auto-analyze and stop only owned processes after the experiment.

## Idempotence and Recovery

Keep the unmodified binary and probe artifact immutable. Use apply_patch to
remove this experiment if evidence rejects it; do not reset unrelated files.
Only origin/hparser-integration may be pushed, normally and without force.

## Outcomes & Retrospective

Shared scheduling removes 77 net lines of runtime/thread plumbing, preserves
connection-scoped lifetime and reduces sampled batch/caller wait. Clean workload
results expose a serial and CPU tradeoff, so this is implementation alignment,
not overall performance acceptance. Evidence and exact commands are retained in
../benchmarks/transport-scheduler-baseline.json. Full Go-package parity and the
throughput/latency goal remain open.

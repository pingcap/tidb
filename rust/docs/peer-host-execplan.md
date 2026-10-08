# Host peer KILL and process scans on the shared status service

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture

Complete the shared receiving boundary for N04 KILL and I03 process-list scans.
Serve Go's generated TiKV coprocessor protocol beside existing status HTTP,
apply cluster TLS/CN policy, use the local process/privilege owners and release
listener/factory resources at shutdown. Preserve Go errors and both result
encodings. Full pkg/server and all cluster tables remain separate obligations.

## Progress


- [x] Inspect existing 4a1a0b59c0 outgoing batch, Go master and shared owners.
- [x] Capture HTTP2 and listener-release regressions before implementation (2 failures in before-final.log).
- [x] Compose session-local rows, generated peer service, HTTP/TLS and joined shutdown.
- [x] Validate both consumers together and update registers.
- [ ] Actual commit hook, fresh locked pre-push build, remote verification and reusable checkpoint.

## Context and Orientation

Go pkg/server/rpc_server.go registers TikvServer; http_status.go shares its port
with HTTP and applies cluster CA/certificate/common-name policy. Go executor
coprocessor.go decodes DAGRequest, authenticates its optional user without a
password, loads default roles and executes local cluster tables. Remote KILL
must not redispatch. Rust http_status.rs currently detaches accept/request
threads, owns no shutdown, and accepts only HTTP/1. Session process_arm.rs owns
process visibility/formatting; ClusterSessionFactory owns the live registry.

## Milestones and Plan of Work

First add session-local peer rendering and a generated TiKV service in
rust/crates/tidb-server/src/peer_rpc.rs. Bind user identity/default roles through
the existing privilege registry. Decode TypeKill or cluster-process TableScan,
validate columns/output offsets, and encode default/columnar responses using
existing codecs. Reject unsupported DAG operators explicitly.

Then replace http_status.rs's detached threads with one joined Tokio runtime.
Reuse existing HTTP route handlers through an HTTP router and serve generated
gRPC routes on the same socket. Apply cluster TLS and verified CN policy before
HTTP/gRPC dispatch. Abort incoming connections on owner shutdown as Go Close/
Stop do. Migrate both startup paths and remove the obsolete manual HTTP framer
after its meaningful cases move to network tests.

Finally run grouped protocol/session/status tests and real SQL/peer requests.
Run affected all-target checks, make lint, the actual precommit locked build,
and a fresh locked build immediately before any normal push. Verify remote SHA.

## Concrete Steps and Acceptance

Use /workspace/tidb hparser-integration and /workspace/client-rust master; no
native changes are planned. Source /workspace/.cloud-setup/env.sh and set
CARGO_BUILD_JOBS=1. From rust run cargo test --locked -p tidb-server --lib
peer_host_batch, then affected server integration tests and cargo check --locked
-p tidb-session -p tidb-server --all-targets. From root run make lint. The actual
hook must run cd rust && cargo build --locked -p tidb-server before commit.

Acceptance requires HTTP and HTTP2/gRPC on one port, KILL arriving at the local
registry, identity-filtered process rows with output projection, both encodings,
malformed/unsupported request errors, TLS verification and joined shutdown.
Do not call a whole package transcreated or claim live Go/TiKV interoperability
unless its complete inventory and required validation actually pass.

## Surprises & Discoveries

Go server Close closes status HTTP and stops gRPC; detached Rust status threads
currently retain the factory beyond the intended shutdown boundary. Cluster TLS
CN validation belongs to the server, unlike the outbound client's TLS policy.

## Decision Log

- Decision: retain one status-port owner for both protocols and both consumers.
  Rationale: avoid another port, protocol, per-session runtime or process formatter.
  Date/Author: 2026-10-08, Codex.

## Outcomes & Retrospective


The final configuration passes 17 distinct Rust cases (5 peer-host and 16 status,
with 4 overlapping), affected server/session all-target checks and make lint.
Nine live assertions on real MySQL/unistore and generated-descriptor gRPC pass:
process rows contain real identities/status address; peer KILL QUERY interrupts
SLEEP and preserves its connection; peer KILL CONNECTION closes the target.
The server exits cleanly. Both production store startup paths compile; live
validation uses unistore. No live Go/TiKV or full package/performance acceptance.

N04's recorded remote KILL gap is repaired. I03 and N05 remain partial for other
cluster tables/operators, full accounting, AutoID/other RPC services and broader
server behavior. Counts are 86 tracked,32 repaired,54 unresolved (25 open,29
partial). See parity/current-audit/peer-host-validation.json for exact logs,
commands, pins, intermediate failures and remaining limits. Commit hook and
pre-push outcomes will be recorded in the external final-handoff.json.

Removed the manual HTTP framer/private test after migrating its meaningful
cases to real socket coverage, detached status threads and raw response framing.
The first migrated HTTP test caught half-close behavior; hyper now preserves
complete requests after the client closes its write half. Unused Axum serving
features were removed after moving the connection owner to hyper. Final tests
and checks run from rust/ with its actual Cargo configuration. Self-review also
restored whole-batch nested decoding before effects; malformed later commands
cannot execute an earlier KILL. The reviewed production build repeats all nine
live assertions successfully.

## Idempotence and Recovery

Preserve concurrent source and branch changes. Never force push or bypass hooks.
Keep external logs under /workspace/.cloud-setup/peer-host-batch. Retire only
owned completed executables with receipts for disk capacity, not shared caches.

## Interfaces and Dependencies

Use tidb_proto generated service/messages, tidb-session process/privilege owners,
tidb-chunk and codec encoders, existing tonic/axum/Tokio and rustls dependencies.
No handwritten wire schemas or private peer protocol.

The baseline needed two storage retries before the tests could execute. Reclaimed only completed generated test executables with hashes and active-process checks. The source failures were HTTP/1 bytes returned to an HTTP2 client and a listening socket retained after owner drop. No source-code failure is attributed to the earlier disk/link errors.

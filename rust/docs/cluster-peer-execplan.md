# Share peer requests for cluster administration

This living ExecPlan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

Make remote KILL and CLUSTER_PROCESSLIST use discovered TiDB peers through the
existing TiKV coprocessor transport. Today KILL silently searches only the local
registry and the cluster process table silently reports only local connections.
The observable outcome is a remote kill request addressed by global server ID,
and remote process rows with their originating instance and user visibility.
This is repair evidence for N04 and I03, not a complete Go package transcreation.

## Progress

- [x] Inspect live owners against Go master 1f819a0b4a6cc07f9a8ff07e6777761a770c6d3d.
- [x] Capture both remote-routing and hidden discovery failure regressions before the fix (0 passed, 2 failed).
- [x] Implement one peer client and migrate both session consumers and server wiring.
- [x] Run grouped protocol/session validation (52 distinct cases), seven real MySQL/status checks, affected all-target check, make lint and locked server build. Update both registers; I03 becomes partial and 55 broad findings remain unresolved.
- [ ] Record actual hook and fresh prepush build, push/remote verification and cloud checkpoint in the external final handoff.

## Surprises & Discoveries

The Rust status listener currently exposes HTTP only. Inbound peer RPC hosting
is a separate N05 prerequisite. Outbound requests can reach Go peers now; local
rows retain the existing session owner until the inbound service is implemented.
The first grouped session run also exposed a stale test query with unquoted reserved KEY. Quote that identifier while retaining its deadlock/privilege assertions.

The production TiKV authority already exports a borrowed RPC capability, so
cluster administration must reuse its channels and process shutdown ownership.

## Decision Log

- Decision: complete outbound discovery, request construction, response decoding,
  cancellation and both consumers together. Keep inbound hosting explicit.
  Rationale: avoid claiming a whole server package or hiding missing peer hosting.
  Date/Author: 2026-10-08, Codex.

## Outcomes & Retrospective

Implementation is wired. Generated-service wire validation passes, including both request shapes, identity, warnings, remote errors, shared channels and live SQL-killer cancellation. The 51 grouped session cases, seven real MySQL/status checks, affected all-target check, make lint and locked server build pass. The first live probe expected error1317 for interrupted SLEEP; Go returns1, and the corrected probe passes. The starting register has 55 unresolved
findings; narrower repair evidence does not by itself close broad findings.

## Context and Orientation

`rust/crates/tidb-session/src/process_arm.rs` owns KILL and local process rows.
`dispatch.rs` materializes information schema tables. `tidb-domain` owns live
server discovery. `tidb-exec` can depend on the generated TiPB contracts and the
shared `TonicCoprocessorClient`; session depends on exec, not the reverse.
Go owners are pkg/executor/simple.go (killRemoteConn), builder.go (DAG user),
pkg/store/copr/coprocessor.go (buildTiDBMemCopTasks, handleTiDBSendReqErr), and
pkg/executor/coprocessor.go (remote execution).

## Plan of Work

Add `tidb-exec/src/cluster_peer.rs` for peer filtering, generated DAG requests,
transport and response decoding. Bind it once in ClusterSessionFactory and both
startup paths. Route remote global IDs out of the local registry and append Go's
warning on failure. Query CLUSTER_PROCESSLIST peers with the session identity,
timezone, column descriptors and cancellation. Keep local row formatting shared.

## Concrete Steps

From /workspace/tidb activate /workspace/.cloud-setup/env.sh. Set
CARGO_BUILD_JOBS=1 to stay within the cloud memory limit. Run targeted Rust
session and protocol tests after the full batch, then cargo check for touched
all-targets, `make lint`, and `cd rust && cargo build --locked -p tidb-server`.
The actual precommit hook must run that locked build; immediately before any
push rerun it and verify the remote SHA. Never bypass hooks or force push.

## Validation and Acceptance

A remote ID must not kill a coincident local registry entry. Missing peer
transport must be an explicit warning, not silent success. Generated-wire tests
must exercise kill and process scan request fields, user visibility context,
response rows/errors/warnings, discovery filtering and cancellation. Local
process behavior must remain covered. Live Go/TiKV validation, inbound peer
hosting, complete packages and benchmarks remain unverified unless executed.

## Idempotence and Recovery

Preserve concurrent changes and use normal Git commits. Re-run only failed or
changed gates. Do not clean shared build caches; retire only owned, completed
executable outputs with receipts if disk space requires it.

## Artifacts and Notes

Keep machine-local logs under /workspace/.cloud-setup/cluster-peer-batch and
commit a concise validation receipt with the live finding updates.

## Interfaces and Dependencies

Use generated tidb_proto::tipb and coprocessor contracts and existing
DirectUnaryClient, UnaryCallContext and TonicCoprocessorClient. The factory owns
one Arc peer capability; each request borrows/clones transport, never constructs
an independent per-session runtime. SQL kill signals cancel in-flight calls.

Validation receipt: `rust/docs/parity/current-audit/cluster-peer-validation.json` records exact commands, log identities, failed-before evidence and limits. No broad root or complete package was closed.

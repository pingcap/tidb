# Align MPP transport setup with the shared cluster policy

This living ExecPlan follows root PLANS.md. Work is in /workspace/tidb on hparser-integration, starting at fe1e7ee0c8e8a516a3cf9771142ea7e78da99da7. Native /workspace/client-rust master stays unchanged at 19a56ccda1e128218cd33c69709038219aced9bc. Freshly fetched Go master is 93a01d31f6da205ae4bf376825293903a6899fdb. The user prohibits pushes.

## Purpose / Big Picture

Advance the connected M04/N03 transport consumers together: MPP must use configured cluster TLS, preserve Go's 60-second task timeout, bound unary dispatch and dialing, and honor the canonical SQL killer while establishing a stream. Receive deadlines apply per packet, not to the entire stream. This repairs existing integrated owners; it does not accept full Go copr/client/grpcutil packages or close the broad structural findings.

## Progress

- [x] Read instructions, review shared transport and pinned Go callers, refresh all three remote branches without resetting local work.
- [x] Extend the existing real socket fixture and group regressions into one target.
- [x] Capture five independent corrected baseline runtime failures; exclude compilation/provider-fixture failures.
- [x] Implement shared TLS/store-window/receive-size propagation and cancellation/deadline lifecycle together.
- [x] Run 32 distinct grouped Rust cases, affected all-target checks, root lint and locked server build; self-review.
- [x] Prepare both registers and durable receipts for one normal hook-gated source-and-receipt commit. Final hook/commit/recovery/draft identity is recorded externally in Cloud mpp-transport-batch/final-handoff.json. No push.

## Context and Orientation

rust/crates/tidb-exec/src/tiflash_mpp_scan.rs lowers scan-only MPP, dispatches to TiFlash and streams packets. Its existing region and statement owners remain authoritative. rust/crates/tidb-pd-client/src/security.rs supplies cluster TLS credentials and endpoint construction; its PdClient currently consumes those credentials without retaining a getter for downstream transports. rust/crates/tidb-txnkv/src/rpc/channel_pool.rs owns store channel window defaults. MPP currently uses TikvClient::connect with explicit plaintext and unbounded setup. Go pkg/store/copr/mpp.go sets task Timeout=60 and uses ReadTimeoutMedium for dispatch; pinned client-go internal/client/client.go uses cancellation for MPP streams, with a separate per-Recv lease timeout.

## Milestones and Plan of Work

First extend existing fixtures with TLS and deliberately stalled establishment. Record combined failures for encrypted transport, encoded task timeout, deadline expiration and SQL KILL with remote gather cleanup. Then retain the same Arc<ClusterSecurity> in PdClient, reuse the existing secure endpoint/window policy for store traffic, and migrate MPP dialing/dispatch/establishment to one cancellation-aware await. Dispatch carries the Go unary deadline; streaming must not carry that total-duration deadline. Packet pulls retain their current backpressure and gain the per-receive bound. Finally validate once across the batch and record remaining ownership obligations.

## Concrete Steps

From /workspace/tidb/rust activate source /workspace/.cloud-setup/env.sh. Run cargo test --locked -p tidb-exec --lib -- mpp_transport_ mpp_setup_deadline mpp_kill_interrupts mpp_dispatch_task_timeout --test-threads=1 before production fixes. Afterward run cargo test --locked -p tidb-exec --lib tiflash_mpp_scan:: -- --test-threads=1 and grouped PD security/lifecycle cases. Run cargo check --locked -p tidb-pd-client -p tidb-txnkv -p tidb-exec -p tidb-server --all-targets. From root run make lint. A normal commit must invoke hooks/pre-commit and its cd rust && cargo build --locked -p tidb-server. No push commands.

## Validation and Acceptance

The actual localhost TLS server must receive an RPC from the MPP connection helper. A stalled operation must expire under its own deadline, and KILL during real stream establishment must return before the fixture's guard timeout and cancel the gather once. Encoded dispatch Timeout must be 60. Existing stream, quota, retry-region invalidation and cleanup tests must retain their behavior. No live multi-node or performance acceptance follows from loopback checks.

## Idempotence and Recovery

Fetch is read-only for the working branch. Preserve concurrent changes and existing receipts. Tests use ephemeral localhost sockets and joined fixture shutdown. Preserve baseline logs. Replace the unpublished bundle only after a new bundle verifies. Never reset branches, bypass hooks or push.

## Interfaces and Dependencies

Expose the existing immutable security Arc from PdClient. Share store endpoint construction through tidb-txnkv::rpc rather than duplicating TLS/window rules. The await adapter accepts the existing StatementMemory and an optional operation deadline; it polls the canonical killer and drops the pending future on cancellation. No new crates or independent cancellation owner.

## Surprises & Discoveries

Go establishment uses context.WithCancel, not context.WithTimeout: the 3600-second limit is per receive. Native grpcutil adapter experiments already reject a partial HTTP/2 readiness port; this batch does not promote them. Initial test-fixture compilation/provider/timing errors are excluded from fail-before evidence. The first locked server link filled the 32 GB cloud disk and failed with SIGBUS. With no build running, six recorded obsolete/failed unlinked ELF files (about 1.81 GB total) and 104 MiB regenerable Go cache were removed; the retry passed. Current binaries, source, dependencies, fingerprints, logs and recovery were preserved.

## Decision Log

Decision: advance M04/N03 through their connected existing MPP transport consumer, while retaining P03/P06/T02 full discovery/routing gates. Rationale: partial service-mode switching without its complete owner would introduce another incomplete lifecycle. Date: 2026-10-04.

## Outcomes & Retrospective

Five runtime regressions fail under corrected old-behavior replay and pass after repairs. Thirty-two distinct Rust cases, all-target checks, lint and locked server build pass. Six new tests preserve useful prior cases. Ordinary KV/BatchCommands and MPP now share endpoint/receive defaults, and MPP consumes the retained process security Arc. M04/N03 remain partial; shared pooling/reconnect/backoff, full native PD discovery/routing, SQL TLS and other consumers remain unresolved. Counts remain 86 tracked, 29 repaired, 57 unresolved. No push.

Revision 2026-10-04: include the connected message-limit and TLS-provider defects found during the transport review; record combined baseline/final validation and unchanged wider finding scope.

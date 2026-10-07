# Shared TSO failure forwarding and recovery

This living ExecPlan follows root PLANS.md.

## Purpose and outcome

When the timestamp primary cannot establish a stream, both clients must apply Go's automatic forwarding policy through the existing shared discovery owner. Six eligible connection failures permit a healthy backup only when enable-forwarding is configured. Explicit follower proxying remains independently controlled. Recovery, primary replacement and shutdown must retire obsolete streams without discarding healthy peers.

## Context and milestones

Work in /workspace/client-rust master (081145e6ab6215650336c941a9f65ef27c90b552) and /workspace/tidb hparser-integration (a0e053e2e05dbe41e7d90f993bdd34a18264df54). Fresh Go master 3ca96b1d5df8da123e7a650512654eedab12c861 selects PD afa43111d149. Source owner: clients/tso/client.go tryConnectToTSO, backupClientConn, checkLeader and servicediscovery. Native service_discovery.rs owns shared routing; timestamp.rs and TiDB client/worker.rs consume it. A forwarded route retains the logical primary and physical backup independently.

First extend existing real-socket timestamp suites and record failures together. Then implement shared failure feedback and backup/recovery selection, migrate both clients, preserve request cancellation and remove duplicated policy. Keep all state locks out of network awaits and reject observations belonging to replaced routes. Synchronize native source using the maintained script, never handwritten vendor copies.

## Progress

- [x] Refresh source pins and compare existing shared routing with Go.
- [x] Two native failures and one adapter timeout reproduced before repair. Initial adapter fixture compilation errors were corrected before counting behavioral evidence.
- [x] Implement shared policy and migrate both clients; construction feedback lives in the wire owner.
- [x] Run grouped behavioral/all-target/lint checks; update both registers and the durable receipt.
- [x] Prepare TiDB publication through the actual hook and fresh locked-build gate. Final remote/Cloud results are recorded in the external final-handoff.json.

## Surprises & Discoveries

The broad native run exposed a stale test stimulus: advertising a valid new leader was called a failed metadata observation. Replace that stimulus with an actual failed GetMembers RPC while preserving stream reuse assertions. Go distinguishes network errors during stream construction from application failures and local cancellation. Rust currently establishes response headers together with the first request; retain that compatibility constraint and explicitly record remaining complete grpcutil/transport obligations. Existing discovery maintenance is joined by each process owner.

## Decision Log

Use the existing native discovery owner for failure policy; do not introduce a TiDB-only fallback or treat an unhealthy health result as enough to enable forwarding. Explicit follower proxying overrides ordinary forwarding policy. Keep P03/P06/N03 partial until complete package obligations are accepted.

## Validation and recovery

Source /workspace/.cloud-setup/env.sh, then export CARGO_BUILD_JOBS=1. Native commands run in /workspace/client-rust with cargo --config /workspace/.cloud-setup/native-cargo.toml test --locked -p tikv-client --lib -- pd:: region_cache::test:: common::security::tests:: --test-threads=1. TiDB commands run in /workspace/tidb/rust with cargo test --locked -p tidb-pd-client --lib --test all --no-fail-fast -- tso_source:: pd_client_source:: client::worker_lifecycle_tests:: security::tests:: tls_handshake_source:: etcd::tests:: --test-threads=1. Run affected all-target checks and make lint once at the batch boundary. Before every push run cd rust && cargo build --locked -p tidb-server from /workspace/tidb; TiDB's actual precommit hook must also run it. Preserve concurrent changes and never force-push. Recover through committed diffs, not resets. Logs belong in /workspace/.cloud-setup/tso-failure-batch.

## Outcomes & Retrospective

370 selected tests pass, with one existing opt-in real-PD watch test ignored. Native 8b752f9638ad157931725b66ffdc57e0465432a9 is published and synchronized. Three original regressions failed before repair; review caught a separate intermediate response-phase classification bug, now covered by a passing negative guard. Native/adapter all-target checks and lint pass. Exact Go idle recovery timing and stream-readiness/prewarm remain open. No complete Go package, live multi-node cluster or performance acceptance is claimed.

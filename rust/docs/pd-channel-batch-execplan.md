# Share PD channels across discovery and RPC consumers


This living ExecPlan follows repository PLANS.md.

## Purpose / Big Picture


PD membership, keyspace metadata, timestamp discovery and timestamp streams must reuse an endpoint connection under one client lifetime, as pinned PD servicediscovery and pkg/utils/grpcutil do. Both native client-rust and the TiDB synchronous adapter create separate connections for these consumers, including on idle discovery. Remove these duplicate lifecycles together while preserving TLS, separate metadata/TSO routes and request budgets. P03/P06 remain partial: this maintains existing owners and does not accept a complete upstream package.

## Progress


- [x] Read instructions, fetch remotes and incorporate concurrent MDL commit f29d961bad safely. Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb; native base bc8cca3fea4f741e68a41b124f28164604a57ee2.
- [x] Add grouped connection observations to existing native and adapter gRPC fixtures.
- [x] Capture four baseline failures: native uses 12 sockets in each case; adapter uses 3 foreground / 4 with periodic discovery. Native first link ran out of disk; journaled generated-output cleanup allowed the same baseline to run.
- [x] Share native channels across bootstrap, refresh, keyspace and TSO.
- [x] Migrate adapter consumers and remove its separate discovery runtime (awaiting maintained native sync for compilation).
- [x] Grouped validation: 172 native and 81 adapter Rust cases pass; one existing live-PD case remains ignored. Native and affected adapter all-target checks, lint and self-review pass. The actual hook and final prepush build remain mandatory.
- [x] Publish native 06b4ccc2735e after its fresh locked server build; verify remote and maintained sync.
- [ ] Commit/push validated TiDB changes and update reusable draft.

## Context and Orientation


Native src/pd/cluster.rs owns Connection and Cluster; retry.rs owns discovery and close; service_discovery.rs owns timestamp routing. TiDB rust/crates/tidb-pd-client/src/client/failover.rs caches metadata clients; worker.rs independently constructs discovery/TSO channels and a private discovery runtime. Its current-thread process runtime stops polling I/O while the synchronous command receiver is idle. Sharing channels without addressing that would stall discovery.

## Plan of Work and Milestones


First extend existing timestamp fixtures to count accepted sockets or request peer addresses. Four grouped regressions cover direct native bootstrap/refresh, native periodic refresh, adapter foreground reuse and adapter idle discovery. Run them before production edits.

Then add an endpoint-keyed channel map to native service_discovery, preserving existing security factories. Publish successful construction, retain the winner under concurrent creation, reject publication after close, and keep channels until close like Go. Connection clones and RetryClient must keep the bootstrap map.

Finally migrate all adapter PD consumers to this owner. One continuously driven Tokio runtime owns channels and a joined discovery task. The synchronous control thread retains shutdown authority. Remove the private discovery thread/runtime and metadata-only eviction policy. Preserve public APIs, cancellation and ordinary RPC deadlines.

## Concrete Steps and Validation


Activate /workspace/.cloud-setup/env.sh; it now exports CARGO_TARGET_DIR=/workspace/tidb/rust/target for both repositories and maintained regeneration. From /workspace/client-rust run CARGO_BUILD_JOBS=1 cargo test --locked -p tikv-client --lib source_channel_batch before and after, then the pd:: suite and all-target checking. From /workspace/tidb/rust run CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-pd-client source_channel_batch, then the existing PD suite and affected all-target checks. Expect baseline connection-count failures and one accepted connection per endpoint afterward. Record exact outcomes in rust/docs/parity/current-audit/pd-channel-batch-validation.json. Logs: /workspace/.cloud-setup/pd-channel-batch.

Run make lint in /workspace/tidb and cargo build --locked -p tidb-server in rust. Actual precommit hook must pass the locked server build; repeat immediately before every push, including native publication. Publish native first, then run bash rust/scripts/sync-tikv-client-rs.sh from TiDB root; never hand-edit vendor files. Verify remote SHAs. These fixtures validate TCP identity and lifecycle, not live-cluster performance or complete Go packages.

## Idempotence and Recovery


Preserve concurrent edits and never force-push. A failed or canceled dial leaves no accepted connection; close prevents resurrection by in-flight construction. Repeated close is harmless. Preserve existing artifacts and dependencies. Failed maintained patches require reconciliation, never bypass.

## Surprises & Discoveries


The new MDL commit is preserved as integration base. Shared adapter channels require a continuously polled process runtime to work during idle discovery.

## Decision Log


Decision: repair the PD channel prerequisite before remote SQL fanout. Rationale: Rust lacks the TiDB coprocessor server; all selected PD consumers already exist and can migrate together. Author: Codex.

Decision: retain exact endpoint keys until close. Rationale: Go GetOrCreateGRPCConn uses a discovery-owned sync.Map and membership changes do not evict TSO channels. Author: Codex.

## Outcomes & Retrospective


Four baseline connection-count regressions fail before repair. Afterward 172 native PD and 81 adapter tests pass (one existing live-PD test remains ignored), and lint passes. Native publication and exact remote verification are complete. Affected adapter all-target checks pass. Final hook/push results are recorded after commit in /workspace/.cloud-setup/pd-channel-batch/final-handoff.json. No whole finding or package closure is claimed.

## Interfaces and Dependencies


Tonic Channel clones share HTTP/2 connections. Native SecurityManager and TiDB ClusterSecurity remain TLS owners. No global cache, TiKV fleet sharing, new dependencies or generated-code edits are intended.

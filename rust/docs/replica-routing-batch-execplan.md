# Share native replica routing with TiDB

## Purpose / Big Picture


Make point, batch and scan reads obey the replica policy already produced by SQL.
Follower/learner selection, stale-read retry transitions and busy-store fallback
must share native request state and use the existing TiDB cache. Follow PLANS.md.
This maintains existing owners under T02/O13/N03; it is not complete Go package acceptance.

## Progress


- [x] Confirm clean branches and Go b36c940a4332c866d8b0e2afde88f5e7c2fd7fed, client-go 8edb23f6c7ee.
- [x] Trace SQL policy to ClientPd's inherited leader-only routing.
- [x] Reproduce replica and busy-threshold failures in existing snapshot tests.
- [x] Extract native routing policy, migrate both clients and synchronize native changes.
- [x] Run grouped validation: 52 native and 31 TiDB tests, affected all-target checks and root lint pass.
- [x] Update both registers and prepare actual-hook/fresh-build publication; final publication and Cloud receipts are recorded outside the commit in /workspace/.cloud-setup/replica-routing-batch/final-handoff.json.

## Context and Orientation


/workspace/tidb hparser-integration starts at e9c0821b7aa08bb210003638fa8c4720d7e7a39b;
/workspace/client-rust master at 63046fb9b2e0faeb74f7d58bcf27bbb5bbfdee65.
TiDB rust/crates/tidb-txnkv/src/driver/client_bridge.rs adapts process-owned cache
and transport into native request plans. Native src/pd/client.rs owns routing,
src/locate.rs owns retry state and candidate scoring. A candidate is a peer with
store-health facts. Go authority is selected client-go internal/locate/replica_selector.go,
nextForReplicaReadLeader and nextForReplicaReadMixed.

## Milestones and Plan of Work


Extend snapshot_lock_wait_source with multiple peers and reproduce emitted-request
failures. Extract the existing native routing body behind cache/transport capabilities
and migrate PdRpcClient. Publish native changes and use rust/scripts/sync-tikv-client-rs.sh
with its patches and protobuf regeneration. Implement the interface in ClientPd,
supplying canonical cache facts and removing the redundant address map. Validate
the complete batch, preserving native retry history and logical versus physical peers.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. From tidb/rust run CARGO_BUILD_JOBS=1
cargo test --locked -p tidb-txnkv --test snapshot_lock_wait_source snapshot_replica.
Check emitted peer IDs, replica/stale flags and busy thresholds, including retries.
Run affected all-target checks and root make lint at the batch boundary.
Native commands use per-command CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0.
The actual precommit hook must run cd rust && cargo build --locked -p tidb-server;
repeat immediately before every push. Verify remote SHAs; never force-push.

## Idempotence and Recovery


Preserve concurrent work. Restore only this batch's files for baseline comparisons.
Keep logs and before-images in /workspace/.cloud-setup/replica-routing-batch.
Do not hand-edit vendored sources. Unrun live-cluster and performance checks remain explicit.

## Surprises & Discoveries


SQL policy plumbing was repaired earlier, but its consumer silently discarded it.
Shared scoring did not share the enclosing route and retry decisions.

## Decision Log


Extract native policy rather than add another selector. TiDB retains cache ownership;
the native sender retains retry ownership. Broader roots stay partial where other
ownership or validation obligations remain.

## Outcomes & Retrospective


Three semantic regressions fail before and pass after. The complete existing
snapshot suite and bridge ownership tests pass, including cancellation, lock
recovery and batch concurrency. Native a0a6ec32deb8dfb565494b9b803598cd0e7bcef2
is pushed, verified and synchronized through maintained patches/protobuf generation.
See parity/current-audit/replica-routing-batch-validation.json for exact commands,
log hashes and development retries. Ordinary forwarding and replica-flow metrics,
coprocessor selector/cache consolidation and full package/live-cluster acceptance
remain open; none of the 56 broad unresolved findings is falsely closed.
The actual commit hook and fresh prepush locked build still run during publication;
final-handoff.json records their results and the saved Cloud draft without embedding
a self-referential commit SHA. No performance gain is claimed.

# Share PD region service routing and live policy

This living ExecPlan follows repository `PLANS.md`. Maintain Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective as work proceeds.

## Purpose / Big Picture


PD followers can answer region metadata requests when the process option and the individual request permit it. Rust currently stores the SQL option without routing these requests. Repair the connected P03 discovery, T02 cache-request and N03 configuration consumers together. Requests that bypass follower metadata must retain leader-only behavior. A follower RPC or PD-header error must retry the leader with the original deadline and request fields.

## Progress


- [x] Verified clean Cloud checkouts and refreshed Go master and both editable remotes. TiDB starts at ccf2717fc0284d1777d5dbee150c11d4e1daea83; native starts at cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1. Go master remains 3ca96b1d5df8da123e7a650512654eedab12c861.
- [x] Captured three failing grouped loopback transport regressions in before.log; the newly exposed option setter retained the native option but existing RPCs ignored it.
- [x] Implement shared native routing policy, migrate region callers and connect live SQL publication. Caller review additionally caught cache batch/legacy-scan permission producers and ID leader-only semantics; those are included in this batch.
- [x] Run grouped regressions, affected all-target checks, lint and locked server build; review and update both registers. Exact results are in pd-region-batch-validation.json.
- [x] Native commits published with fresh locked builds and verified remote SHAs, then maintained sync completed. TiDB publication/checkpoint results for this immutable commit are recorded afterward in /workspace/.cloud-setup/pd-region-batch/final-handoff.json; verify that receipt before claiming publication.

## Context and Orientation


The native checkout is `/workspace/client-rust`, branch master. `src/pd/service_discovery.rs` owns shared endpoint connections; `cluster.rs` constructs native region RPCs. TiDB's `rust/crates/tidb-pd-client/src/client` is the synchronous adapter and its joined worker. `tidb-txnkv/src/driver/tikv_pd_bridge.rs` carries region-cache request options. `tidb-session/src/vars.rs` owns live global settings and detached precommit validation images. `tidb-server/src/real_tikv_node/mod.rs` composes the process authority. Detached validation must not change live routing before storage commit.

The authoritative Go module is `/workspace/.cloud-setup/gopath/pkg/mod/github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149`: `client.go`, `inner_client.go`, `servicediscovery/service_discovery.go`, `opt/option.go` and `pkg/utils/grpcutil/grpcutil.go`. Go `pkg/domain/domain_sysvars.go` and `pkg/sessionctx/variable/sysvar.go` supply setting publication. A follower is a PD member other than the elected metadata leader; this is distinct from TiKV replica reads and TSO follower proxying.

## Plan of Work


First extend the existing PD loopback suite to record actual metadata and destinations for key, previous-key, ID and scan APIs, including failure fallback and explicit leader requests. Next share the native discovery selection and request metadata policy, retaining independent native and TiDB transport adapters. Preserve the shared channel map and existing cancellation/close owner. Wire the live setting through the process global owner, including reload and postcommit publication; migrate cache options rather than inferring them from unrelated flags. Remove redundant region-only routing code once callers move.

## Milestones


Baseline: grouped transport cases demonstrate missing follower behavior using real local gRPC servers. Implementation: the same requests follow Go selection, carry follower permission only to a follower and preserve leader retry constraints. Validation: owning-crate tests, all-target checks, lint and publication gates establish the selected contract, not acceptance of whole PD or TiDB packages.

## Concrete Steps


Source `/workspace/.cloud-setup/env.sh` in every shell. Use `/workspace/tidb/rust` for TiDB Cargo and `/workspace/client-rust` for native Cargo. Use `CARGO_BUILD_JOBS=1` and `--locked`. Group tests in the existing `tidb-pd-client --test all` target and native `tikv-client --lib` target. Record exact finalized filters and results in `rust/docs/parity/current-audit/pd-region-batch-validation.json`; external logs belong under `/workspace/.cloud-setup/pd-region-batch`. Native publication precedes maintained `bash rust/scripts/sync-tikv-client-rs.sh`, including patches and protobuf regeneration. Run root `make lint`, then normal hooks and `cargo build --locked -p tidb-server` immediately before each push.

## Validation and Acceptance


Prove actual endpoint and request metadata, not only a policy helper. Cover dynamic enable/disable, leader-only requests, request-field preservation, follower transport/header failure, deadlines, membership changes, and closed owners. Keep existing non-region and TSO behavior tests. No live multi-node TiKV, performance or complete upstream package claim follows from local mock transport tests. Broad finding counts change only when all original obligations are met.

## Idempotence and Recovery


Never reset either editable checkout or force-push. Inspect concurrent work before edits and publication. Keep the working server and dependency cache. Native synchronization is the only vendor update path. On a gate failure fix the diagnosed cause and rerun the affected group. Save tested startup instructions separately from environment Publish and fresh restoration.

## Surprises & Discoveries


The shared native option and per-request flags already exist, but the TiDB region RPCs do not emit `pd-allow-follower-handle`. The batch-scan bridge also drops the caller's follower permission. Current whole-finding count is 56 unresolved; this batch does not freshly reproduce unrelated roots.

## Decision Log


- Decision: Group region discovery, request options and live global policy in one batch; leave TSO proxying separate.
  Rationale: Region routing has an independently testable Go contract and multiple connected production callers; TSO uses a different stream and health lifecycle.
  Date/Author: 2026-10-06 / Codex.

## Outcomes & Retrospective


Native PD validation passes 174 cases after one corrected Send/Sync bound and a disk-space failure. Native test builds now use the external native-cargo.toml profile matching TiDB development settings; full debug output was the disk cause. Ten old session archives and failed/older native outputs were removed with hash/link/process receipts. TiDB integration checks pass; immutable publication evidence is recorded in the external final handoff. P03, T02 and N03 retain their existing partial status with scoped evidence; complete discovery, cache consolidation, forwarding/health and TSO obligations remain separate.

## Interfaces and Dependencies


Reuse native PD option and channel owners. Any selection carrier must retain physical endpoint and leader identity together; metadata construction must not use process-global mutable request state. Global-setting publication must retain the process authority across live reload while detached validation stays isolated. No new dependency or protocol generation input is planned.


Source review follow-up: pinned client-go `internal/locate/region_cache.go::batchScanRegions` starts with follower and router permission, retains both after transport errors, and clears both after empty/gapped/all-leaderless metadata. Native initially omitted those producer flags. A combined scripted regression fails before correction with `[false,false,false,false]` instead of `[true,true,false,false]`; the three stale-payload forms share that case. Legacy cache scans permit followers; cache ID lookups do not. Native initial routing commit `84d77e529982ff92b3b2063c9a1ee747a57ce9e5` was published and synchronized before this additional caller correction was identified. Native follow-up publication and synchronization are required. Two SQL-policy regressions also failed before publication callbacks were restored. Policy publication uses the value being installed and runs under the existing resolved-image write lock, avoiding both stale reads and reordered concurrent callbacks.

Caller review confirmed TiDB legacy cache ID requests already use RegionQueryOptions::default (LeaderOnly). The direct public ID API now also defaults to leader-only, exercised alongside bridge IDs. TiDB legacy cache batch scans remain leader-bound; permitting their separate retry owner is a remaining T02 obligation. Native cache batch permission, sticky fallback and the TiDB bridge are covered here.

Grouped adapter validation exposed tonic grpc-timeout classification: its typed local TimeoutExpired source maps to Cancelled, while the adapter previously recognized only DeadlineExceeded and its outer timer. Preserve timeout identity by walking the typed source chain; a peer Cancelled response with identical text stays a transport error. The existing timeout regression failed before this correction and is retained with the peer-cancellation distinction.

The final all-target check also caught a shutdown-test factory missing the new shared options field. The factory now uses the canonical native default. Its ownership tests are run explicitly, then the same all-target check is rerun; no production behavior is changed by this fixture correction.

The concurrency regression failed before the final repair: a newer OFF image was retained while an older ON callback overwrote the process policy. Initial binding now holds the image read lock while installing and initializing the consumer; updates publish their captured value under the image write lock, with consistent image-then-binding lock order. The callback contract forbids reentry into global-variable APIs.


Final grouped validation: 397 distinct Rust tests pass. Native PD174 and cache90 are independent groups; TiDB adapter/bridge and SQL global policy results are detailed in the receipt. All-target checks and make lint pass. No full package, live multi-node or performance acceptance.

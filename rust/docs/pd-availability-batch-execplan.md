# Share PD service availability across native and TiDB callers

This living ExecPlan follows repository `PLANS.md`. Update Progress, discoveries,
decisions and outcomes as work proceeds.

## Purpose / Big Picture


Region requests should avoid PD members whose health service is not serving and
temporarily avoid followers returning REGION_NOT_FOUND, as pinned Go PD does.
Both native KV and TiDB must consume the same availability owner. Recovery must
restore eligible members without recreating clients, and closing a client must
cancel and join its health probes.

## Progress


- [x] Read repository instructions, current registers and PD source; fetched both remotes. Clean heads are TiDB 41e1113eb5096b536a530cab5c928c07e0116f8e and native 0c8edfb4cccd3d44c075c9910b4d1bb40fb913dc. Go master remains 3ca96b1d5df8da123e7a650512654eedab12c861.
- [x] Four socket regressions failed before production fixes: native and adapter each fail idle health and cross-API REGION_NOT_FOUND cooldown. Initial fixture compile errors were corrected before recording semantic baselines.
- [x] Shared health, API cooldown, topology publication and typed caller feedback implemented in native and adapter.
- [x] Independent health probing and cancellation/join composed in both runtime owners. Native 267 PD/cache tests and all-target checking pass. Native ab96ef545c126b32bbf938d87dde33fd29d8abca was committed, passed the fresh locked server build, pushed normally and verified by fetch. All four maintained patches and protobuf regeneration synchronized it into TiDB.
- [x] Validate and self-review the connected batch; update both registers and batch allocation evidence. Native267 plus adapter64 distinct tests pass, both all-target checks pass and root make lint passes.
- [ ] TiDB source commit and push through the actual hook and fresh locked build; the Cloud publication runner records final SHA, gate logs and saved environment revision in `/workspace/.cloud-setup/pd-availability-batch/final-handoff.json` after this source receipt is committed.

## Context and Orientation


Native `src/pd/region_service.rs` owns region rotation but currently has no
availability. Native `src/pd/cluster.rs` and TiDB
`rust/crates/tidb-pd-client/src/client/failover.rs` consume it. Native RetryClient
and TiDB DiscoveryWorker own asynchronous maintenance; shared ChannelCache owns
connections. Preserve those owners. The Go comparison is PD module
v0.0.0-20260805103528-afa43111d149, selected by freshly fetched TiDB master.
`servicediscovery/service_discovery.go` defines one-second health checks,
leader RPC timeout, one-third-second follower probes, independent network and
API eligibility, and ten-second REGION_NOT_FOUND suppression. Its tests cover
leader/follower retry distinctions and balancer eligibility.

## Plan of Work


Extend the existing shared selection owner with per-member network observations
and region eligibility. Preserve member identity across unchanged topology;
late observations must not poison replacement members. Record typed PD errors
before projection. Add health maintenance beside existing discovery so a stalled
TSO discovery cannot block health. Share cached channels and join maintenance on
close. Remove displaced health-wire duplication only after callers migrate.
Preserve direct leader fallback when the eligible ring is empty.

## Milestones


First stage actual loopback health and error regressions together. Then integrate
selection, both transports and both maintenance owners before final compilation.
Finally run grouped affected tests, all-target checks, lint and publication
gates, preserving explicit limits for full PD packages and cluster behavior.

## Concrete Steps and Validation


Activate `/workspace/.cloud-setup/env.sh` in each shell and use one build job.
Native commands run in `/workspace/client-rust`:

    CARGO_BUILD_JOBS=1 cargo --config /workspace/.cloud-setup/native-cargo.toml test --locked -p tikv-client --lib -- pd:: --test-threads=1
    CARGO_BUILD_JOBS=1 cargo --config /workspace/.cloud-setup/native-cargo.toml check --locked -p tikv-client --all-targets

After native validation/publication, use maintained
`rust/scripts/sync-tikv-client-rs.sh` from `/workspace/tidb`. Adapter commands run
in `/workspace/tidb/rust`:

    CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-pd-client -p tidb-txnkv --test all --no-fail-fast -- pd_client_source:: pd_region_loader_source:: --test-threads=1
    CARGO_BUILD_JOBS=1 cargo check --locked -p tidb-pd-client -p tidb-txnkv -p tidb-server --all-targets

Run `make lint` from TiDB root. The actual precommit hook must pass
`cd rust && cargo build --locked -p tidb-server`. Repeat that locked build
immediately before every push, including native; verify remote SHA afterward.
Expected behavior: non-serving followers receive no region requests; restored
health makes them eligible; REGION_NOT_FOUND suppresses all region API variants
for ten seconds without treating unrelated errors as cooldown; unchanged
membership does not reset suppression; shutdown joins blocked probes.

## Idempotence and Recovery


Never force-push or overwrite concurrent work. Keep evidence outside checkout
in `/workspace/.cloud-setup/pd-availability-batch`. Preserve caches and current
binaries; prune only verified inactive reproducible outputs if space requires.
Native vendoring uses the maintained script and patches, never manual copying.
Health observations and cooldown cannot activate GC or change TSO ownership.

## Surprises & Discoveries


The previous selection owner retries a failed follower once but forgets the
failure, so every second subsequent request returns to it. Baseline health had
no producer. Native and adapter loopback tests independently reproduced both gaps.
Native uses tonic 0.12 while the workspace patches it to 0.14; the maintained
codec patch must move with the shared health encoder. The new test fixtures
initially needed their respective tonic body/codec APIs corrected. Those compile
failures are separate from the four confirmed behavioral failures. These are live source-confirmed residuals.

The membership-race correction inspects the current API ring after network
probes complete. Retaining old members is correct for health feedback but not
for expiry of the current balancer. This discovery justified a focused native
follow-up and revalidation before the TiDB batch commit.

## Decision Log


- Decision: repair the entire existing service-availability path across both
  clients, retaining forwarding and TSO proxy as separate open consumers.
  Rationale: availability is their prerequisite; inventing forwarding during
  health maintenance would obscure the source contract and broaden validation.
  Date/Author: 2026-10-06, Codex.

## Outcomes & Retrospective


Native validation and publication passed. Adapter integration consumes the synchronized owner: 42 PD transport,16 loader and6 ownership tests pass; all-target checks and root lint pass. Together with267 native cases,331 distinct tests pass. Final review found that cooldown expiry after a stalled probe must check the current API ring, as Go does; the existing concurrency case now covers replacement-ring expiry. Native follow-up 194295ec1f40e3ce4c1115ef3ff74cb2c425d112 passed the same267 tests, all-target checking and a fresh locked server build, then pushed normally with remote SHA verified. Final TiDB synchronization and grouped revalidation also pass:64 adapter/loader/ownership tests and all-target checking. The complete selected batch has331 distinct passing tests, four confirmed baseline failures and no selected failures. Root make lint and changed-file formatting/diff checks pass. TiDB source commit/push gates follow this receipt; their actual outcomes and identities are recorded in the external final-handoff JSON so the source commit does not assert its own future success. P03/T02 remain partial; no complete
Go package or live multi-node/performance acceptance is claimed.

## Artifacts and Interfaces


Use existing RegionService, ChannelCache and process cancellation owners.
Keep immutable request targets associated with the selected member lifetime;
network health is shared while region cooldown is specific to region APIs.
The durable validation JSON will record baseline failures, selected tests and
exact publication evidence.

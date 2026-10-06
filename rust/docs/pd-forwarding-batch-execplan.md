# Connect PD forwarding policy, availability and unary callers

This living ExecPlan follows root `PLANS.md` and records existing-owner maintenance, not complete upstream package acceptance.

## Purpose / Big Picture


When forwarding is enabled and PD leader health is unavailable, supported metadata RPCs should use a healthy follower carrying the logical leader address. Region-local eligibility must remain separate from forwarding eligibility. Recovery restores direct leader traffic. Both native and TiDB clients must use the existing channel, membership and shutdown owners.

## Progress


- [x] Read instructions, current batch/register evidence and pinned Go PD service discovery; refreshed requested remotes from clean TiDB 3a903f597731635739fdbefe34170bca4eee662e and native 194295ec1f40e3ce4c1115ef3ff74cb2c425d112.
- [x] Stage grouped before-fix regressions using existing socket fixtures.
- [x] Connect shared selection/metadata, static options and every supported unary consumer; remove duplicate failover loops after migration.
- [x] Run grouped regressions, all-target checks, lint and self-review; update both finding registers with exact limits.
- [ ] Publish native through fresh locked server build, maintained sync, then normal TiDB commit hook, fresh pre-push build and remote verification. Record immutable publication evidence outside source.

## Context and Orientation


Native `src/pd/region_service.rs` owns health and region cooldown. `src/pd/cluster.rs` sends native RPCs. TiDB `rust/crates/tidb-pd-client/src/client/{failover,requests}.rs` sends synchronous adapter RPCs. Both share native options but startup forwarding is unused. Go master 3ca96b1d5df8da123e7a650512654eedab12c861 selects PD afa43111d149; `servicediscovery/service_discovery.go::GetServiceClient` uses healthy forwarding followers only when enabled and leader health is unavailable, while `BuildGRPCTargetContext` carries `pd-forwarded-host`. `pkg/store/driver/tikv_driver.go::pdClientOptions` consumes canonical EnableForwarding.

## Plan of Work and Milestones


First extend existing socket fixtures to capture physical destination and forwarding metadata, with enabled/disabled and recovery cases. Introduce options constructors as baseline support without routing behavior. Next extend the shared member owner with a separate forwarding cursor and retained logical leader metadata; migrate native and adapter unary construction, startup policy and duplicate adapter failover loops together. Finally validate integrated consumers with one grouped test/check boundary before mandatory publication gates. TSO follower/proxy streaming and router transport remain separate open lifecycles.

## Concrete Steps and Validation


Activate `/workspace/.cloud-setup/env.sh` and CARGO_BUILD_JOBS=1. From `/workspace/client-rust`, run `cargo --config /workspace/.cloud-setup/native-cargo.toml test --locked -p tikv-client --lib -- pd:: region_cache::test:: --test-threads=1` and matching `check --locked -p tikv-client --all-targets`. From `/workspace/tidb/rust`, run `cargo test --locked -p tidb-pd-client -p tidb-txnkv --lib --test all --no-fail-fast -- pd_client_source:: pd_region_loader_source:: client::worker_lifecycle_tests:: --test-threads=1` and affected all-target checks. Run root `make lint`. Socket assertions must prove exact metadata, disabled gating, region cooldown isolation, leader recovery and shutdown. Keep unrelated historical failures explicit.

## Idempotence and Recovery


Preserve concurrent changes, cached builds and exact destinations; never force-push. Native synchronization uses `rust/scripts/sync-tikv-client-rs.sh` and all maintained patches/protobuf generation. The actual TiDB hook and immediately-before-every-push `cd rust && cargo build --locked -p tidb-server` gates are mandatory. Logs and final publication identity belong in `/workspace/.cloud-setup/pd-forwarding-batch`.

## Surprises & Discoveries


Native initialization creates an options value separate from the connection's options. The batch will give initialization and requests one shared owner. Adapter store/GC paths repeat the same failover loop.

## Decision Log


- Decision: repair supported unary forwarding and its policy as one connected batch; retain TSO proxy and router obligations explicitly. Rationale: Go gives those stream owners distinct lifecycles. Date/Author: 2026-10-06, Codex.

## Outcomes & Retrospective


Four socket assertions failed before production routing. 270 native and 66 adapter selected cases now pass; both all-target checks and root lint pass. Native 512b33c1e86f2e151a975ed70e0e398bc6987871 is published and synchronized. Normal TiDB hook, fresh pre-push build, push and saved Cloud checkpoint follow this immutable source receipt; external final-handoff.json records their actual results. P03/T02/N03 remain partial until their broader obligations are accepted; no claim about other unresolved roots is refreshed by this batch.

Review correction: Go keyspaceClient uses GetServingEndpointClientConn directly, and innerClient.getServiceClient retains mustLeader=false on region API fallback. Keep keyspace management direct; distinguish region-local fallback from its forwarded retry. Publication was canceled before the native push when review found these boundaries; final grouped validation follows the corrected code.

Self-review retained direct membership probes, separate region/forwarding cursors and per-attempt metadata. Self-review caught Go keyspaceClient remaining direct and the region fallback mustLeader distinction. Pending publication was canceled before any native push, both exceptions were corrected and socket/owner regressions cover them. An initial native compile diagnostic required keeping tonic Status until the public client boundary; compilation diagnostics are not behavioral baseline failures. No Go files/imports, Bazel or dependency manifests changed.

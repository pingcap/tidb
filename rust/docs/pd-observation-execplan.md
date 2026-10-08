# Preserve PD discovery generations across failed observations

This living ExecPlan follows repository `PLANS.md`.

## Purpose / Big Picture

Keep timestamp service available through failed service-mode observations, reject stale secondary group metadata, and retire a cached connection when the TSO server reports a mismatched callee. Both the native asynchronous client and TiDB's synchronous adapter must consume the same discovery policy. These are connected repairs within P03/P06; complete PD package acceptance and dispatcher consolidation remain open.

## Progress

- [x] Confirm clean TiDB `00c262c166c1595f37187c3e166e7b9325a95c28`, native `aa2c60f37481c2fe7f03997535d4f238ed485956`, refreshed Go master `7a3dacb52efe58d28db360ae8639d8838c376544`, pinned PD `afa43111d149` and managed HTTPS reads.
- [x] Reproduce all three native policies and one adapter regression against unchanged production code (3 native failures, 1 adapter failure, 0 passes).
- [x] Repair shared discovery and migrate both channel-cache consumers. Native 8c98fb002aa27e45aa2eb782fc091d0c7dba0f4b passed 72 selected tests and all-target checking, then a fresh locked server build and normal push with remote verification. Maintained sync applied all four patches and regenerated bindings; generated outputs and lockfiles are unchanged.
- [x] Grouped validation passes 147 selected Rust tests (72 native,75 adapter), both affected all-target checks and make lint. Self-review, source hashes and both finding registers agree; P03/P06 stay partial and other 54 roots retain prior evidence.
- [ ] Commit with actual hook, fresh locked build before each push, remote verification and reusable Cloud checkpoint.

## Context and Orientation

Native `src/pd/service_discovery.rs` owns `TsoDiscovery`, the accepted timestamp destination, and `ChannelCache`, the retained transport map. Native `src/pd/cluster.rs` and TiDB `rust/crates/tidb-pd-client/src/client/{failover,worker}.rs` consume these owners. Go's pinned `servicediscovery/service_discovery.go::checkServiceModeChanged` does not replace an accepted provider on failed observations; `tso_service_discovery.go::updateMemberInner` advances the group revision before following a secondary, and `findGroupByKeyspaceID` removes a connection on `mismatch callee id`. No package-level doc.go exists in this Go package.

## Plan of Work

Extend the existing network fixture in native `timestamp_tests.rs`, first measuring regressions against unchanged production code. Preserve accepted routes for unsuccessful mode observations while retaining bootstrap failure and explicit Unimplemented compatibility behavior. Carry the freshly observed revision into secondary lookup so older responses cannot become active. Pass the existing shared channel cache into discovery and remove only the mismatched endpoint on the Go header error. Migrate both callers before building TiDB. Avoid adding a competing discovery or connection owner.

## Milestones

The first milestone establishes failing wire behavior for all three paths. The second changes the shared owner and consumer wiring together. The final milestone validates native discovery and TiDB integration, records limitations, and publishes only through normal repository gates.

## Concrete Steps

Source `/workspace/.cloud-setup/env.sh` and set `CARGO_BUILD_JOBS=1`. From `/workspace/client-rust`, run `cargo --config /workspace/.cloud-setup/native-cargo.toml test --locked -p tikv-client --lib -- observation_batch --test-threads=1` before production changes. After repair run the existing PD test selection and affected all-target checking. Run TiDB PD tests and `cargo check --locked -p tidb-pd-client -p tidb-server --all-targets` from `/workspace/tidb/rust`, and `make lint` from its root. Native publication precedes `rust/scripts/sync-tikv-client-rs.sh`; run a fresh `cargo build --locked -p tidb-server` immediately before every push. Normal TiDB commit must run the actual precommit hook and its same locked build.

## Validation and Acceptance

Existing accepted providers survive unsuccessful observations, initial invalid observations still fail, a secondary cannot regress a revision observed in the same refresh, and a callee mismatch causes the next lookup to use a new connection while unrelated errors preserve reuse. Keep final selected counts and command outcomes in `rust/docs/parity/current-audit/pd-observation-validation.json`. Full Go suites, live multi-node TiKV, complete package acceptance and performance are outside the evidence of this batch.

## Idempotence and Recovery

Preserve concurrent source and exact remotes, never force-push or bypass hooks. The Cloud disk is nearly full; retire only completed owned inactive executables with identity/process receipts, preserving compiler caches. Save logs in `/workspace/.cloud-setup/pd-observation-batch`. Do not delete source to recover build capacity.

## Surprises & Discoveries

The existing source already sends the accepted revision on ordinary group requests. The missing floor is specifically the second request after a newer primaryless observation; do not repeat the already implemented ordinary check.

## Decision Log

Use one shared cache parameter rather than a private cache or invalidation queue. Treat unsuccessful observation as distinct from accepted mode changes and keep bootstrap errors observable. Retain broader lifecycle findings as partial.

## Outcomes & Retrospective

All four original regressions failed before repair and pass after. Native publication and all grouped validation passed. Exact command outcomes, source/log hashes and remaining obligations are recorded in `parity/current-audit/pd-observation-validation.json`. TiDB publication outcomes will be recorded after commit in `/workspace/.cloud-setup/pd-observation-batch/final-handoff.json`. Native cache removal forces future lookup reconnection; it does not forcibly cancel already borrowed tonic channels. Complete transport retirement remains open. The revision floor now survives failed cloned refreshes, while primaryless member-list publication remains outside this maintenance scope.

## Interfaces and Dependencies

`TsoDiscovery::discover` receives the existing `ChannelCache` by reference; both callers retain runtime and shutdown ownership. `ChannelCache::remove` evicts one cached handle without reopening a closed cache. Generated protobufs and dependencies remain unchanged; vendor updates use the maintained sync script.

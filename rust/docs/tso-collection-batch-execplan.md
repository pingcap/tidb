# Share timestamp collection and live batch policy


This living ExecPlan follows PLANS.md. Update progress, discoveries, decisions and outcomes at each boundary.

## Purpose and context


Connect P06/N03 timestamp admission and runtime policy through the existing native PD batch owner. Native timestamp collection currently ignores the live maximum wait; TiDB independently drains a mixed command queue into a private 10,000-request vector, while pinned Go uses the shared adaptive batch controller with a 20,000-request bound. SQL publication does not deliver the wait to PD. Migrate the adapter to native collection, preserving cancellation, ordered completion, buffer reuse and deadlines. No full TSO package acceptance is claimed.

Baseline TiDB d8fd3f8bd6de1e426ebacfcf140ca3a9f8c2e61a and native 8b752f9638ad157931725b66ffdc57e0465432a9 are clean and published. Fresh Go master remains 7a3dacb52efe58d28db360ae8639d8838c376544; PD is afa43111d149. Work only in /workspace/tidb and /workspace/client-rust. Native changes must publish first and reach TiDB through rust/scripts/sync-tikv-client-rs.sh, with all patches and protobuf regeneration.

## Progress


- [x] Refresh integration/master and verify current owner gaps against source.
- [x] Capture three baseline failures: native live wait, adapter configured wait, and adapter 20,000-request bound.
- [x] Wire native live wait and migrate adapter collection; connect SQL policy publication.
- [x] Validate grouped native, adapter and session cases, all-target checks, lint and server build.
- [x] Update both registers and durable validation receipt.
- [ ] Publish TiDB through required gates and save the cloud checkpoint; post-commit outcomes are recorded in /workspace/.cloud-setup/tso-collection-batch/final-handoff.json.

## Work and acceptance


Pinned PD clients/tso/dispatcher.go constructs batch.Controller with defaultMaxTSOBatchSize*2 and samples the live option before collecting. pkg/batch owns adaptive target, waiting, cancellation, finish order and retained buffers. Extend its existing native receiver boundary so the adapter can project timestamp commands without reimplementing collection. Preserve unrelated commands in FIFO order. Native Cluster must pass the same Arc<Options> into each oracle, including replacement oracles. Session GlobalSysvars must initialize and publish the wait in the existing ordered process-policy callback, while scratch validation remains side-effect-free. Keep RPC concurrency explicitly unresolved: tracing found independently synchronous transports, so publishing the value alone would falsely claim execution support.

## Validation


Activate /workspace/.cloud-setup/env.sh; use CARGO_BUILD_JOBS=1. Native Cargo uses --config /workspace/.cloud-setup/native-cargo.toml; TiDB Cargo runs from rust/. Capture fail-before/pass-after cases in existing timestamp and global-variable suites. Group adjacent tests by target; no per-case build loop. Run affected all-target checks and make lint. Every Rust commit uses the actual locked server-build hook, and every push follows a fresh cargo build --locked -p tidb-server. Verify remote SHAs. Logs and final publication outcomes live in /workspace/.cloud-setup/tso-collection-batch.

## Recovery


Preserve concurrent edits, exact destinations and compiler caches. Never force-push. Retire only completed executables with hashes, hard-link inventory and accessible process checks when link space is needed. Keep the final server and verified recovery bundle.

## Surprises & Discoveries


The register's shorthand about one dispatcher overstated ownership: native and TiDB share stream transport but TiDB still has a private vector/drain/completion loop. Native's 20,000-request bound is correct; the adapter's 10,000 bound is the divergence. Go's stream adapter does not enforce response headers, so no speculative header checks belong in this repair.

## Decision Log


Reuse the native package owner instead of implementing another adapter wait loop. Preserve RPC-mode, readiness/prewarm, idle recovery and complete transport-package obligations as unresolved.

## Outcomes & Retrospective


Native aa2c60f37481c2fe7f03997535d4f238ed485956 is published and synchronized through all maintained patches and protobuf regeneration. The private TiDB collector is removed; both clients share native collection and consume live maximum wait. SQL callbacks preserve binding/SET/DEFAULT/image replacement and scratch isolation. Three baseline failures precede 230 distinct selected Rust passes and six real MySQL setting controls. Affected all-target checks, lint and locked server build pass. Actual TiDB hook, prepush and remote results are retained in the external final handoff. P06/N03 remain partial; RPC concurrency, readiness/prewarm, independent dispatch and complete packages remain open. Counts stay86 tracked/30 repaired/56 unresolved.

Validation correction: the first native after-run passed 82 cases but retained a mistaken expectation that an already-waiting collector immediately resamples options. Go samples before waiting. The corrected test completes that prior collector first; native-before-corrected.log fails on the original timestamp/cluster owners. The fixed owners are restored for final validation. The first native compile also identified one existing run_tso test call needing the options argument; it was migrated before behavioral checks.

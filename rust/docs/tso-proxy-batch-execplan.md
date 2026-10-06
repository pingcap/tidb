# Shared timestamp follower proxy

## Purpose and outcome

Connect Go's dynamic TSO follower-proxy option to the native and TiDB timestamp owners. A proxy is a healthy PD follower (or timestamp group secondary) carrying requests to the logical leader. Metadata discovery, physical stream identity, logical forwarding identity and SQL publication must agree. This batch maintains P03/P06/N03 together; it does not accept entire Go packages or automatic network-failure forwarding.

## Context and milestones

Work in /workspace/client-rust master and /workspace/tidb hparser-integration. Fresh Go master is 3ca96b1d5df8da123e7a650512654eedab12c861, PD client afa43111d149. Go clients/tso/client.go tryConnectToTSOWithProxy checks all service URLs, keeps healthy streams, forwards followers, and retires removed endpoints. Native service_discovery.rs supplies group membership and both wire protocols; cluster.rs owns timestamp contexts. TiDB's client/worker.rs consumes the same discovery and owns retained streams. Session vars.rs publishes committed process policy; server boot.rs binds it.

First reproduce the missing native stream consumer using the existing socket fixture. Then implement shared healthy endpoint selection and forwarding metadata, native and adapter stream sets, and live SQL policy publication. Preserve direct mode and stream reuse; cancellation retires every stream, including obsolete contexts retained by callers. Keep comparison source and log receipts outside the checkout under /workspace/.cloud-setup/tso-proxy-batch.

Validate the completed source batch with grouped native PD/cache tests, adapter TSO/PD lifecycle tests, session policy tests, affected all-target checks and make lint. Source /workspace/.cloud-setup/env.sh and use CARGO_BUILD_JOBS=1. Native Cargo additionally uses --config /workspace/.cloud-setup/native-cargo.toml. Publish native only after a fresh locked server build, synchronize with rust/scripts/sync-tikv-client-rs.sh, and validate integration. The actual TiDB precommit hook must run cargo build --locked -p tidb-server from rust; repeat it immediately before every push. Verify remote SHAs. Preserve concurrent changes and never force push.

## Progress

- [x] Refreshed both remotes; clean checkouts and unchanged Go comparison verified.
- [x] Three baseline failures reproduced native routing, adapter routing and SQL policy binding.
- [x] Shared selection, both stream consumers and SQL publication implemented; native 35395ca53dcf219ee26b69367a5a6e2f19fd8748 published and maintained sync completed.
- [x] Grouped validation, review and durable receipts completed.
- [x] Publication prepared with mandatory hook/prepush gates; post-commit remote and Cloud verification is recorded in /workspace/.cloud-setup/tso-proxy-batch/final-handoff.json.

## Surprises & Discoveries

Before repair the dynamic option existed but no TSO consumer read it. Unary forwarding is independently configured and must not gate follower proxy. Group discovery currently drops secondary addresses after resolving the primary.

## Decision Log

Source review rejected an unpublished per-endpoint TimestampOracle sketch because it duplicated request queues and controllers. The final native design keeps one dispatcher and updates a retained wire-stream map. Native and adapter now consume the same TsoStream transport owner; proxy changes preserve batching ownership. A shared route change notification cancels removed native streams, while healthy unchanged streams remain retained. The adapter also watches accepted route publication during a blocked exchange, so disabling proxy mode can retire that stream and retry the same batch on the primary within its original deadline.

Use the existing native discovery, connection manager, adapter retained stream and session publication owners. Preserve meaningful tests and mandatory gates. Automatic network-error fallback remains separate because Go requires a bounded stream-creation retry classification before fallback; health-based proxy admission must not silently substitute for it.

## Outcomes & Retrospective

343 distinct selected Rust tests pass, with production/all-target checks and root lint. Native published at 35395ca53dcf219ee26b69367a5a6e2f19fd8748; maintained synchronization includes all patches and protobuf regeneration. Full source-package, mixed-node TiKV, TLS topology and performance acceptance remain unverified. Rollback uses the committed batch diff; do not reset unrelated work. Setup helpers and validation logs are outside the source tree.

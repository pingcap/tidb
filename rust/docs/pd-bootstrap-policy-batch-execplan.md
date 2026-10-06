# PD bootstrap and provider policy

This living ExecPlan follows root PLANS.md. Update progress, discoveries, decisions and outcomes as the batch changes.

## Purpose and outcome

Compose discovery, connection configuration and timestamp provider selection across the existing native and TiDB clients. Valid member observations must not depend on an additional leader membership RPC. A discovered but unreachable TSO primary must not prevent an explicitly configured healthy follower proxy from serving timestamps. Supplied gRPC endpoint options must configure each constructed connection in order and reuse the cached result. Concurrent cache misses may construct redundant candidates, as Go LoadOrStore does. The static use_tso_server_proxy option must select PD TSO even when PD reports API service mode.

## Context and milestones

Use /workspace/client-rust master and /workspace/tidb hparser-integration. Baselines are native35395ca53dcf219ee26b69367a5a6e2f19fd8748 and TiDB0164d0cddb94258bbc363d9e02ece83d739705c0. Fresh Go master3ca96b1d5df8da123e7a650512654eedab12c861 selects PD afa43111d149. Source owners are PD servicediscovery, inner_client.go resetTSOClientLocked and pkg/utils/grpcutil. Go uses nonblocking cached gRPC connections; discovery and successful RPCs have separate contracts.

First extend existing native timestamp and adapter TSO socket suites and record failures before implementation. Then compose native SecurityManager endpoint construction with PD options and lazy channel creation, remove the redundant leader GetMembers exchange, and carry the forced-provider option through shared TsoDiscovery and both clients. Preserve direct metadata discovery, minimum-TS provider selection, TLS construction and cancellation. Do not implement automatic network-failure forwarding as an incidental health fallback.

Validate the connected changes once with native PD/cache cases, adapter PD/TSO/lifecycle cases, affected all-target checks and root make lint. Publish native after the mandatory fresh locked TiDB server build; synchronize with rust/scripts/sync-tikv-client-rs.sh. The actual TiDB commit hook must pass the same build, followed by another fresh build immediately before pushing. Verify both remote SHAs. This is existing-owner maintenance for P03/P06/N03, not acceptance of complete Go packages.

## Progress

- [x] Verified clean published baselines and refreshed both remotes; Go unchanged.
- [x] Rechecked source contracts and selected connected defects.
- [x] Reproduce regression failures together.
- [x] Implement and synchronize the shared policy.
- [x] Complete grouped gates, review and audit receipts.
- [x] Prepare publication with mandatory gates; post-commit remote and Cloud verification is recorded in the external final-handoff.json.

## Surprises & Discoveries

Go still queries GetClusterInfo on the logical PD leader; follower discovery is not a substitute. The unreachable-primary fix concerns the separate TSO microservice. UseTSOServerProxy changes the timestamp provider rather than the service mode of unrelated components. Before repair, native and adapter endpoint options existed without a connection consumer. Both now configure cached channels.

## Decision Log

Final caller review found the etcd wrapper also consumes normalized endpoint strings. Its old HTTP-only prefix stripping fails for newly admitted HTTPS URLs. Preserve the authority and let configured TLS options choose the scheme in all three RawEtcdClient connection paths; an eighth baseline failure verifies this handoff. Include the existing etcd unit/lifecycle cases in the final group.

The first grouped run passed all six new cases but exposed an obsolete test requiring reachability-based leader alias fallback. Go updateServiceClient/PickMatchedURL chooses the first matching URL by TLS configuration, so replace that assertion with refusal to silently change the selected leader plus the existing stream-reuse assertion. Review also exposed the same boundary in TLS URL selection: share Go PickMatchedURL across native leadership/region selection and adapter membership projection. Keep all advertised URLs for TSO proxy probing. Add a seventh before-fix adapter regression for mixed advertised schemes. A full TLS topology remains unverified.

Retain existing socket fixtures and shared channel cache. Migrate the native eager PD dial to lazy endpoint construction while retaining all TLS validation and endpoint options. Keep ordinary TiKV connection semantics unchanged. Remove only the duplicate leader membership validation after the accepted member response.

## Validation and recovery

Source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. Run native commands from /workspace/client-rust with --config /workspace/.cloud-setup/native-cargo.toml; run TiDB Cargo from /workspace/tidb/rust. Selected suites are native --lib -- pd:: region_cache::test:: common::security::tests:: --test-threads=1 and adapter --lib --test all --no-fail-fast -- tso_source:: pd_client_source:: client::worker_lifecycle_tests:: security::tests:: tls_handshake_source:: --test-threads=1. Run cargo check --locked --all-targets for affected clients and server, then make lint at TiDB root. Preserve logs under /workspace/.cloud-setup/pd-bootstrap-policy-batch. Never reset concurrent work, force-push, bypass hooks or manually edit vendored native source. Recover through the resulting committed diff and durable receipts.

## Outcomes & Retrospective

364 selected tests pass after eight behavioral baseline failures; all-target checks and lint pass. Native 081145e6ab6215650336c941a9f65ef27c90b552 is published and synchronized with all maintained patches and protobuf regeneration. Final commit/prepush/remote evidence is recorded in the external final-handoff.json. Full Go suites, automatic TSO failure forwarding, live multi-node/TLS topology and full package acceptance are outside this maintenance batch.

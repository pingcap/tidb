# Share PD service-mode and TSO discovery

This living ExecPlan follows repository `PLANS.md`.

## Purpose / Big Picture


Native TiKV and TiDB must discover the timestamp service independently of PD metadata. In microservice mode, timestamp requests must carry the selected keyspace and group to the group's primary; an unavailable service must never silently allocate timestamps from the PD leader. This batch repairs connected residuals of P03 and P06 together.

## Progress


- [x] Rechecked current Go master 93a01d31f6da205ae4bf376825293903a6899fdb and its PD module afa43111d149; both editable trees began clean.
- [x] Native baseline: both new discovery regressions fail on 19a56cc; repaired grouped PD suite passes 162 tests. Initial compile exhausted disk; removing 21 inactive test executable links reclaimed 3.7 GiB without deleting source/libraries.
- [x] Shared native discovery/protocol routing, native keyspace handoff, native periodic refresh/retirement and independent TiDB discovery worker are wired and tested. Native public API export follows the existing pd_* module convention.
- [x] Four pre-fix failures reproduced; 163 native PD tests, 27 TiDB unit tests and 52 integration tests pass (one existing live-PD test ignored). Native and affected TiDB all-target checks and make lint pass. Both registers and the durable receipt identify remaining obligations.
- [x] Published native master bb8206e7d080973192fd7f11e0d852845e3277dc using fresh locked-build gates; synchronized through the maintained script. Prepared TiDB publication with the actual commit hook, fresh prepush build and remote-SHA verification; final command outcomes are recorded in Cloud final-handoff.json and the final response.

## Context and Orientation


Cloud checkouts are `/workspace/client-rust` (master) and `/workspace/tidb` (hparser-integration). Native `src/pd/cluster.rs` currently publishes a PD metadata connection and leader-only timestamp oracle. `timestamp.rs` owns batching/deadlines and joined shutdown. `retry.rs` serializes refresh without locking requests across network I/O. TiDB `rust/crates/tidb-pd-client/src/tso.rs` currently opens only PD Tso streams. Service discovery means identifying which service and keyspace group owns timestamp allocation; it is distinct from finding the PD metadata leader.

The comparison source is `/workspace/.cloud-setup/gopath/pkg/mod/github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149`. Read `servicediscovery/{service_discovery,tso_service_discovery}.go`, `clients/tso` and `pkg/utils/grpcutil` when making decisions. Inventory package artifacts in the receipt; these existing-owner repairs do not claim complete package transcreation.

## Plan of Work


First extend `src/pd/timestamp_tests.rs`'s existing network services with GetClusterInfo and TSO microservice responses. Run the new cases before production edits. Add shared native discovery and stream routing, preserving one timestamp batch/deadline implementation. Publish a discovered route atomically, retire obsolete streams, retain healthy unchanged streams and bound discovery RPCs. Preserve Go's old-PD Unimplemented fallback and legacy metadata discovery, never treating other failures as classic mode. Connect keyspace metadata before V2 timestamp use. Use the shared discovery and protocol owner from TiDB instead of recreating its policy. Refresh service information independently of failed timestamp requests; cancellation must stop and join owned work.

## Concrete Steps


Source `/workspace/.cloud-setup/env.sh` and set `CARGO_BUILD_JOBS=1`. Native regression command from client-rust is `cargo test --locked -p tikv-client --lib source_service_`; group native PD tests after fixes. TiDB commands run from tidb/rust: `cargo test --locked -p tidb-pd-client --test all`, appropriate all-target check, and `cargo build --locked -p tidb-server`. Run `make lint` from tidb. Use `rust/scripts/sync-tikv-client-rs.sh` only after native publication; never edit generated or vendored files manually. Every push requires the fresh locked TiDB server build; the actual TiDB precommit hook must run it too.

## Validation and Acceptance


Prove classic compatibility, rejected empty/failed discovery, separate TSO routing and keyspace headers, endpoint/group changes, stale revision rejection, retained-stream reuse, and cancellation/close through real local gRPC services. Report live multi-node, TLS and performance as unverified unless exercised. Keep source-backed tests; remove only superseded duplication after all callers migrate.

## Idempotence and Recovery


Preserve concurrent work, never force-push. Discovery errors must retain the last published valid state. Join retirement outside publication locks. Refreshes and close share serialization. Cancelled close must remain resumable. Record disk constraints and failed checks; remove only identified regeneratable build outputs if required.

## Surprises & Discoveries


Go's API mode fallback for older PD uses MetaStorage to read a serialized TSO Participant at `/ms/<cluster>/tso/00000/primary`; it does not fall back to PD Tso. Non-default assigned keyspaces prohibit this legacy fallback. Both protocol clients are already generated in native kvproto.

## Decision Log


Implement discovery and protocol routing in the native client and consume them from TiDB. Keep metadata and timestamp endpoint ownership separate. Preserve valid prior observations on refresh errors. Date: 2026-10-05 UTC.

Metadata publication proceeds even if timestamp discovery fails. This prevents a TSO outage from pinning metadata to an obsolete PD leader. Timestamp discovery publishes only successful observations. TiDB supplies TLS-configured channels to the same native discovery/stream implementation. Periodic probes run on a separate owned thread/runtime so they cannot block metadata; foreground timestamp refresh retains the original timeout while waiting for the discovery lock. Close cancels and joins the probe worker before acknowledgement.

## Outcomes & Retrospective


The connected batch passes 242 native/TiDB tests, plus all-target checks and lint. One existing live-PD test is ignored. P03 moves from open to partial and P06 remains partial: 86 tracked, 30 repaired, 56 unresolved (29 open, 27 partial). Their former blanket absence-of-discovery evidence is removed. Follower/forwarding consumers, GetMinTS provider selection and whole-package acceptance remain open. The other 54 unresolved roots were not freshly retested. Live multi-node, legacy fallback and complete Go suites remain unverified.

## Artifacts and Notes


Cloud validation logs: `/workspace/.cloud-setup/pd-service-batch/`. Durable receipt belongs under `rust/docs/parity/current-audit/` with exact revisions and commands.

## Interfaces and Dependencies


Use existing tonic channels, native kvproto's PD/TSO/MetaStorage clients, shared cancellation and timestamp batch owners. No new runtime dependency is required. Public shared discovery takes a channel provider so each consumer preserves its configured TLS policy.


The full unit run exposed competing global metrics fixtures. Pinned Go metrics initialization uses CAS and does not wait for another initializer. Combine the adapter assertions under one initialization and remove the duplicate collector-schema checks already covered by native Go-oracle tests; preserve production semantics. Also remove the dead retry-limit constant/accumulator/tail.

The maintained sync script now compares content and preserves unchanged destination mtimes. A real repeat sync checked all 322 source files with identical SHA-256 and mtime values. This avoids rebuilding unchanged regenerated bindings while retaining generation, patches and build gates.

Updated 2026-10-05 UTC with implementation, grouped validation, residual evidence and source synchronization results. Publication results are attached to the final Cloud handoff rather than embedding a self-referential commit SHA here.

Final review reproduced one additional failure on native 852d4b1: after a first endpoint stalled, subsequent initialization selected it again. Routing snapshots now share the attempt cursor, and Connection retains it across initialization retries, matching Go's independent round-robin service selection. Failed observations still cannot replace the accepted group revision/route. The regression fails before correction and the complete grouped native PD suite passes 163 cases afterward. Native bb8206e is published; the final TiDB commit incorporates it through maintained sync.

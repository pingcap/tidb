# PD membership and timestamp refresh lifecycle


This living ExecPlan follows [PLANS.md](../../PLANS.md). Its validation receipt
is [here](parity/current-audit/pd-independent-observation-validation.json).

## Purpose and scope


Repair three connected P03/P06 defects as one batch: a failed service-mode
observation must not freeze an accepted timestamp group; stalled GetMembers
must not prevent background timestamp discovery; TiDB must refresh membership
without foreground traffic. Preserve channel reuse, timestamp ordering,
keyspace publication and joined shutdown in the existing owners.

Baseline: TiDB dbd8c9015183b91c7dec514406586ccf4fec362b, native client-rust
 de4c53c34f9fcf53e2e07cb928b6b4af890b02c0. Fresh Go master is
ab37692e9ebef44a736cd7243deef6020067b743, selecting PD afa43111d149 and
client-go 8edb23f6c7ee. PD servicediscovery/service_discovery.go owns separate
member, service-mode and health loops; tso_service_discovery.go owns group
refresh. The receipt inventories the discovery, TSO and grpcutil directories.
This maintenance does not accept those complete packages or close P03/P06.

## Progress


- [x] Inventory source owners and reproduce all three failures on unchanged production.
- [x] Repair accepted mode facts, member/TSO scheduling and shared adapter membership validation.
- [x] Pass 74 native and 79 adapter tests, both affected all-target checks and lint.
- [x] Publish native 203893b5a4a0ae74a86599cb1dfdc993f999f3ca after its fresh locked TiDB build; verify remote SHA and synchronize through the maintained script.
- [x] Prepare validated TiDB publication with actual-hook and fresh pre-push gates enforced. Their executed outcomes and the saved checkpoint are recorded afterward in the external final handoff.

## Implementation and milestones


The first milestone reproduced actual gRPC failures, using existing wire
fixtures in native src/pd/timestamp_tests.rs and TiDB's
rust/crates/tidb-pd-client/tests/tso_source.rs. Native failed both the moved
primary and stalled-member cases. TiDB retained its old leader after 65
seconds without foreground RPC. The native stalled-member case was then
strengthened to include a concurrent foreground refresh.

The second milestone changes native src/pd/service_discovery.rs to retain
accepted cluster-wide mode facts and discovery URLs instead of returning an
old route immediately on observation error. Successful group lookup can now
replace a moved primary. Initial failed observations still reject bootstrap;
Unimplemented retains classic compatibility. Group revisions retain their
existing monotonic floor and keyspace/provider lifetime.

Native src/pd/cluster.rs separates metadata installation from timestamp
preparation/installation. src/pd/retry.rs uses separate member and timestamp
mutexes: metadata observations remain ordered, but background timestamp
refresh does not wait for GetMembers. Timestamp/keyspace changes remain
serialized. One-minute member refresh joins the existing three-second mode
refresh, live follower-proxy wake and health worker. Close cancels and joins
maintenance before acquiring the two locks in their normal order.

TiDB client/{requests,failover,mod,worker}.rs under rust/crates/tidb-pd-client/src
shares one asynchronous RPC/header/topology path between foreground calls and
periodic membership maintenance. Member observations serialize separately
from timestamp discovery. Existing channels and joined worker remain the
owners; the unconsumed direct_tonic_client helper is removed.

The final milestone validates both consumers, publishes native, synchronizes
with rust/scripts/sync-tikv-client-rs.sh, and prepares TiDB publication. Never
hand-copy vendor sources or skip a failed maintained patch.

## Validation and acceptance


Source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. From
/workspace/client-rust, run:

    cargo --config /workspace/.cloud-setup/native-cargo.toml test --locked --lib -- pd::timestamp::tests:: pd::retry::test:: pd::service_discovery --test-threads=1
    cargo --config /workspace/.cloud-setup/native-cargo.toml check --locked -p tikv-client --all-targets

From /workspace/tidb/rust, run:

    cargo test --locked -p tidb-pd-client --lib --test all --no-fail-fast -- tso_source:: pd_client_source:: pd_worker_lifecycle_source:: client::worker_lifecycle_tests:: --test-threads=1
    cargo check --locked -p tidb-pd-client -p tidb-server --all-targets

Run make lint from /workspace/tidb. All 153 selected tests pass, zero ignored;
both checks and lint pass. The three regressions demonstrate moved-primary
routing during failed mode observation, provider switching despite stalled
members, and background leader refresh. Original forwarding, header/cluster
validation, monotonicity, channel reuse and close cases also pass.

Every Rust commit must execute the actual hooks/pre-commit locked server
build. Immediately before every push, including native, rerun
cargo build --locked -p tidb-server from rust/. Never force-push. Verify each
remote SHA. External evidence is under
/workspace/.cloud-setup/pd-independent-observation; final-handoff.json records
TiDB gates after execution, so this committed plan does not predict them.

## Surprises & Discoveries


The 54 broad findings describe incomplete owners/packages, not individual
regressions. These three repaired behaviors do not complete P03/P06.

The first native baseline link exhausted the 32 GB disk. Completed inactive
binaries and six obsolete library dependency sets were retired with hashes
and process checks; current sets/shared dependencies were retained. That
failed link is separate from the successful behavioral fail-before run.

## Decision Log


2026-10-08: group these related production lifecycles before validation. Keep
member observations serialized with each other while separating timestamp
refresh; close uses the same lock order. Preserve the source one-minute
membership cadence rather than the former three-second GetMembers polling.

## Outcomes & Retrospective


Native is published and synchronized; 74 native plus 79 adapter cases, affected
checks and lint pass. P03/P06 remain partial and the register remains 86 tracked,
32 repaired, 54 unresolved. Remaining work includes separate stalled mode/group
RPC scheduling, error-triggered member notifications, full primaryless metadata
publication, RPC concurrency, readiness/prewarm, idle recovery, dispatcher
consolidation and complete router/security/platform/package obligations.
Full Go suites, live multi-node TiKV/TiFlash, TLS policy and performance were
not validated by this batch.

## Recovery


Read both project statuses and the external handoff before resuming. Preserve
concurrent edits and published commits. Re-run failed checks only after fixing
their cause; never reset source or bypass publication gates. Current-instance
validation, saved configuration, Publish and fresh-task restoration remain
separate facts.

# Separate PD mode observation, timestamp discovery and membership wakeups


This living ExecPlan follows PLANS.md. TiDB baseline is 3def9e97b134;
native baseline is 203893b5a4a0. Fresh Go master ab37692e9ebef selects PD
v0.0.0-20260805103528-afa43111d149. All work runs in the existing Cloud checkouts.

## Purpose and scope


PD mode polling must not freeze accepted timestamp-group refresh. A group lookup
must not prevent a newly observed provider mode from replacing that group owner.
Failed mode checks must request membership refresh rather than wait one minute.
These connected P03/P06 lifecycle defects share the native discovery owner;
both native and TiDB callers must migrate together. This is existing-owner
maintenance, not acceptance of complete PD or TiDB packages.

## Progress


- [x] Read current registers, instructions and source; refresh master and preserve clean baselines.
- [x] Reproduce both stalls in native and adapter plus the membership wake defect in the adapter (five valid baseline failures).
- [x] Separate shared accepted mode facts from group candidate state; migrate native and adapter loops.
- [x] Validate grouped regressions, affected checks and lint; review and update both registers.
- [x] Publish native c594ea976154 and synchronize through all four maintained patches plus protobuf generation.
- [ ] Execute TiDB hook/pre-push gates and publish; save reusable checkpoint (outcomes recorded after execution in the external final-handoff.json).

## Source, implementation and milestones


Pinned PD servicediscovery/service_discovery.go has separate member, health and
service-mode loops. A failed checkServiceModeChanged calls ScheduleCheckMemberChanged.
Mode changes reset the TSO client in inner_client.go. tso_service_discovery.go
owns its group loop. Before this batch native src/pd/service_discovery.rs performed mode
and group RPCs serially under one timeout; retry.rs and adapter client/worker.rs
therefore inherited both stalls.

First extend the existing socket fixtures in native src/pd/timestamp_tests.rs
and adapter tests/tso_source.rs, recording failures against unchanged production.
Then introduce a shared accepted-mode owner and independent mode observation;
keep group revision, keyspace and stream ownership in TsoDiscovery. Mode-change
notification cancels obsolete in-flight group discovery before installing the
new provider. Failed mode probes wake the existing member loop. Preserve all
channel/TLS factories, request budgets, keyspace ordering and joined shutdown.
Finally validate all related callers at one completed batch boundary, publish
native through the maintained sync, and record actual gates without predicting
future successes.

## Validation and recovery


Source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. Native
Cargo runs from /workspace/client-rust with --config
/workspace/.cloud-setup/native-cargo.toml and --locked. Run the new
mode_scheduling tests before implementation, then existing PD discovery,
timestamp and retry cases together. Adapter Cargo runs from /workspace/tidb/rust;
run tidb-pd-client lib/all tests filtered to TSO, membership and shutdown owners.
Keep compiler jobs separate from short-deadline socket execution. Run affected
all-target checking and make lint. No Go/Bazel inputs change.

Every push requires a fresh cargo build --locked -p tidb-server from rust/.
Native publication precedes bash rust/scripts/sync-tikv-client-rs.sh, including
all four patches and protobuf generation. The actual hooks/pre-commit must run
the same locked server build on TiDB commits. Never bypass hooks, force-push,
reset concurrent work or hand-copy vendor sources. External logs and final
publication evidence live under /workspace/.cloud-setup/pd-mode-scheduling.

## Surprises & Discoveries


The 54 broad findings are package/lifecycle obligations; neither a cleanup nor
repairing a few defects closes their remaining contracts. Plan identity was
considered but requires a missing Go normalization owner; hashing rendered
EXPLAIN would not be an acceptable shortcut.

## Decision Log


2026-10-08: repair independent observation and cancellation in the shared native
owner, then migrate both callers. Keep primaryless metadata, full dispatcher
consolidation and remaining router/platform/package obligations explicit.

## Outcomes & Retrospective


Native and adapter migration are implemented and validated: 77 native plus 82 adapter/lifecycle cases pass, including all six final regressions. Five valid before failures cover the three defects. Both all-target checks, changed-file formatting and make lint pass. Native c594ea976154 is published and maintained synchronization passed. TiDB publication is the remaining gate, recorded externally after execution. No package closure or performance claim.


Implementation note: src/pd/service_mode.rs now owns accepted mode facts and
serialized bounded observations. TsoDiscovery consumes snapshots, and mode
notifications cancel obsolete group candidates. Native background membership
checks do not recursively poll mode, preventing a failure-notification loop.
Subscriptions use the actual Cluster owner, including externally constructed
clients. Adapter changes are applied after native publication and maintained synchronization.
The external apply-adapter.py is a completed-action artifact, not a startup command.

Validation correction: the first native membership-wake fixture used
new_with_cluster, which intentionally starts no background workers. Its failure
is discarded as regression evidence. The adapter supplies the valid baseline
for that defect; the corrected native case uses production connect. Initial
adapter fixture compilation also failed on moving a client out of Arc; that
compile error is separate from its three later behavioral baseline failures.

Revision 2026-10-08: completed both caller migrations and grouped validation. Exact commands, source inventory, hashes, diagnostic exclusions and remaining obligations are in parity/current-audit/pd-mode-scheduling-validation.json. Full primaryless group metadata publication, remaining group refresh/retry scheduling, other RPC-error membership notifications, RPC concurrency, readiness/prewarm, idle recovery, queue/dispatcher consolidation and complete router/security/platform/package obligations remain open.

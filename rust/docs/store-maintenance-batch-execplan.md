# Connect store health configuration and replica-flow reporting

This living ExecPlan follows PLANS.md. The batch spans T02 and N03; it does not
accept the complete client-go locate or TiDB configuration packages.

## Purpose / Big Picture


Make the shared TiKV routing owner consume Go's store-liveness timeout and retain
PreferLeader request counts through periodic reporting. Configured zero disables
health I/O and classifies the store unreachable; malformed/negative durations
must fail startup. Counts follow canonical stores, survive borrowed views and
reset after reporting through the existing metric registry and joined worker.

## Context and Orientation


Cloud TiDB base 15a8fb392e3174a666edf2cd69621b2cb7cff581, hparser-integration.
Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed selects client-go
8edb23f6c7ee. Native client remains cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1.
Go cmd/tidb-server/main.go parses the configured duration and rejects negatives;
client-go internal/locate/store_cache.go handles zero without a probe.
region_cache.go reports ToLeader/ToFollower at StoresRefreshInterval/2.
Rust owns canonical stores in tidb-txnkv region/cache, shared health in
region/store_health.rs and a joined maintenance thread in region/background.rs.
ClientPd delegates selection to native ReplicaRouting but drops its flow callback.

## Progress


- [x] Refresh sources and reproduce missing consumers by live caller inspection.
- [x] Implement shared timeout validation/consumption and canonical native/coprocessor flow reporting.
- [x] Reproduce six prior-behavior regressions; all six fixed cases and 91 distinct grouped tests pass.
- [x] Affected all-target check, lint, both register updates and self-review pass.
- [x] Confirm actual executable commit hook, unchanged remote base and required fresh pre-push build. Publication results are recorded after commit in the external Cloud handoff.

## Milestones and Plan of Work


Add typed duration parsing to the existing configuration owner, use it for
foreground and background health, and remove the invalid zero rejection.
Keep zero's no-I/O behavior at the shared probe boundary. Store request counters
on the canonical store health object; consume native selection callbacks and
report/reset from the existing maintenance thread at Go's interval. No extra
thread, metric registry or independent client is introduced. Retain cache
stale-generation publication and shutdown ownership.

## Validation and Acceptance


Use existing routing, transport and maintenance test suites. Check malformed,
negative, fractional and zero configuration; foreground/background consumption;
leader/follower accounting, clones, reset cadence and joined shutdown. Capture
prior-behavior failures together and retain valid existing expectations. Source
/workspace/.cloud-setup/env.sh in every build shell; run Cargo from rust/ with
CARGO_BUILD_JOBS=1 and --locked. Compile compatible targets once and execute grouped
filters. Run affected all-target checks, make lint and git diff --check. Normal
commit must run the actual hooks/pre-commit locked tidb-server build. Repeat
cargo build --locked -p tidb-server immediately before every push, then verify
remote SHA. Preserve concurrent changes and never force-push.

## Surprises & Discoveries


Zero timeout is valid Go configuration and returns unreachable, not unknown or
reachable. The Rust low-level health probe already implements this branch, but
background construction rejects it and configured consumers never reach it.
Native selection already classifies PreferLeader flows; TiDB discards them.

## Decision Log


Repair the existing adapter/cache consumers together; do not duplicate native
selection policy or add a competing background lifetime. Findings remain partial
until broader routing/cache/health and full-package obligations are accepted.

## Outcomes & Retrospective


Evidence and before-images belong in /workspace/.cloud-setup/store-maintenance-batch
and rust/docs/parity/current-audit/store-maintenance-batch-validation.json. No live
TiKV, full Go package or performance claim without executing those validations.

## Validation results and recovery


The receipt records 31 snapshot/ordinary-routing cases plus 60 affected maintenance,
selector, cache and health-transport cases, all passing. All-target checking for
config, txnkv, exec and server and root make lint pass. The six baseline failures
used restored prior consumer behavior with new supporting interfaces retained;
they are not a pristine-checkout package run. A missing Scan fixture was corrected
before counting its zero-versus-three failure. Restoring old timestamps initially
reused the baseline config object; touching restored inputs and recompiling fixed
that verification error. Direct test binaries require the environment's
RUST_MIN_STACK=33554432; omitting it overflowed an existing snapshot test, and the
properly sourced rerun passed all 31 cases.

Counts remain 86 tracked, 30 repaired and 56 unresolved. T02/N03 remain partial;
other 54 unresolved roots were not re-audited. Complete health policy, coprocessor
RequestSelector migration and cache consolidation remain. The existing maintenance
round can delay reporting while metadata/probe work blocks; scheduling-performance
parity, full Go packages, live TiKV/TLS and benchmarks are unverified.

No native source or dependency pin changes are required. Cloud evidence includes
before-images, source inventory, failed/passing logs and a record of seven inactive
regenerable test executables pruned for disk headroom. Preserve these and the
recovery bundle. If publication fails, retain the local commit, report the error,
and retry only after the required fresh locked build. Never reset concurrent work
or bypass hooks. The final post-commit handoff records the actual hook, push,
remote SHA, recovery bundle and configuration draft status without a self-referential
commit hash in this plan.

Revision: implemented the connected store-maintenance batch and recorded its
focused acceptance and remaining package-level obligations.

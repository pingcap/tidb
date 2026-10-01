# Give PD metrics their Go package owners

This living ExecPlan follows TiDB PLANS.md. Acceptance units are the complete
pinned PD metrics and resource_group/controller/metrics packages. Circuit breaker,
grpcutil, service discovery, root and TSO remain unaccepted parent packages.

## Purpose and context


Fresh TiDB master 93a01d31f6da205ae4bf376825293903a6899fdb pins PD
v0.0.0-20260805103528-afa43111d149. Native baseline is df0d4ccc5b595959f496b3cf6e6b87f22b337bf4;
TiDB baseline is aa1c4160ead1bd41e36c1c534e289049d640c23f. Both branches were refreshed
and clean. Each metrics package consists of one metrics.go and has no original
tests, generated inputs, platform variants, build files or fixtures. The complete
source/build/declaration/caller inventory lives in pd-metrics-package.json.

The next circuit breaker prerequisite requires Go's metrics consumer lifecycle.
Native stats.rs instead owns six lazily registered PD metrics with differing
names, counters and buckets. Complete the shared metrics owners first, then move
existing observation sites and delete those definitions. This lets a later full
circuit breaker bind its counters before initialization and rebind them afterward.

## Progress


- [x] Refresh repositories and read both complete source packages and callers.
- [x] Capture Go collector and initialization oracles; demonstrate old adapter mismatch.
- [x] Implement both complete owners, migrate existing metric callers, remove duplicates.
- [x] Validate source inventories, oracles, lifecycle/concurrency and native library/static gates.
- [ ] Publish native, synchronize TiDB, validate adapters/lint and both locked server gates, push.

## Design and milestones


First preserve source-defined collector metadata and prebound observer mappings,
including surprising source aliases, using a pinned-input generator. Generated
Rust fields are typed Prometheus collectors; no string dispatch on hot observation
paths. Both owners construct initially unregistered, unlabeled metrics. PD's first
initialization wins using one sequentially consistent CAS, installs labeled
collectors, rebinds registered consumers, then registers the exact source list and
initializes the resource-group package. Resource-group initialization has no once
guard. Preserve duplicate-registration failure and even constructed-but-unregistered
collectors. A short mutex serializes consumer registration/rebinding; Arc snapshots
and RwLock publication replace Go mutable globals safely. No lock spans an RPC.

Second migrate native PD command and batch observations. Command duration covers
the completed logical operation, including failures; failed duration uses the
source failure observer when defined. Keep native retry policy unchanged. Existing
bucketed/non-bucketed region methods use one source metric. Remove the six local
PD collector definitions and use the shared typed owners. Initialize before PD
connection setup using the current explicit options default, following innerClient.setup.
A cached batch observer must retain the intended initialization lifetime. No new
resource-control or discovery features are enabled.

Third validate both complete packages independently with Go runtime oracles,
not just generated-source comparison. Test labeled registration, all descriptors
and histogram buckets, all bound observers, consumer rebinding before/after init,
concurrent first initialization, duplicate failure, and source registration omissions.
Use isolated registries to avoid global test pollution. Go has no original tests
or failpoints in these two packages; run race-enabled oracle tests in a scratch
module containing exact source and original go.mod/go.sum.

## Validation and acceptance


Native root commands:

    cargo test --locked --lib pd::metrics -- --test-threads=1
    cargo test --locked --lib pd:: -- --test-threads=1
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo check --locked --all-targets
    cargo fmt --all --check
    git diff --check

Scratch PD module commands:

    go test -mod=readonly -race ./metrics ./resource_group/controller/metrics -count=1

After native publication, TiDB root commands:

    bash rust/scripts/sync-tikv-client-rs.sh
    cd rust
    cargo test --locked -p tidb-txnkv --lib driver:: -- --test-threads=1
    cargo test --locked -p tidb-pd-client --lib
    cargo check --locked -p tidb-txnkv --all-targets
    cd ..
    make lint
    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: integrate Go PD metrics ownership'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go/module/Bazel change is planned; bazel_prepare is not triggered. Keep every
package claim atomic and independently inventoried. Preserve incoming commits,
review them and rerun affected checks. Do not force push or bypass hooks. Re-run
generation against the verified pinned source; never hand-edit generated outputs.

## Surprises & Discoveries


Go constructs OngoingRequestCountGauge but omits it from registration.
CmdFailedDurationGetAllKeyspacesGCStates is bound to the success-duration vector.
Preserve both source behaviors; do not silently fix them in Rust.

## Decision Log


2026-10-01: finish the full metrics dependency closure before circuitbreaker.
Adding only four circuit breaker counters would leave initialization ownership
split. Retain native synchronization and explicit owned snapshots without adding
Go-absent counters or enabling incomplete service modes.

## Outcomes & Retrospective


Go runtime/race oracles pass for both complete packages: 32 collectors and 81
prebound observers. All six lifecycle/oracle tests, 110 PD tests and 1,486 native
library tests pass (two existing ignored). The runtime batch regression fails on
old definitions and passes after migration; the new owner is initialized explicitly
in the green harness because Go initializes before traffic. Strict Clippy, all
targets and formatting pass. This unit does not close P03, P06, P07 or claim
workload performance. Live PD, Linux, complete root/TSO/discovery and benchmark
validation remain separate required work.


Revision note: TiDB also had six duplicate PD collector definitions. Its adapter
now must delegate registration and observations to this owner. Its failed-metadata
regression fails before migration (total count zero instead of one); the migrated
adapter counts total and failed durations without inventing metadata stream series.
Native TSO command timing retains its separate success/failure convention. Broader
parent instrumentation placement and duration scopes remain part of root/TSO work.

Go/runtime oracle fixtures exclude Prometheus creation timestamps, unavailable in
Rust prometheus 0.13. All names, help, label sets, buckets, bound observer identity,
counts and sums are compared. Go has no original test files in these packages;
oracle additions run against byte-identical originals, not rewritten Go sources.
The generator rejects an unexpected file inventory or changed production hash.

To reproduce the independent Go oracle, copy original go.mod/go.sum and the two
metrics.go files into a scratch module retaining their paths. Copy
`doc/pd-metrics-oracle/pd_test.go.txt` and `lifecycle_test.go.txt` into that module's
metrics directory as `rust_parity_test.go` and `rust_lifecycle_test.go`. Copy
`resource_group_test.go.txt` into resource_group/controller/metrics as
`rust_parity_test.go`. Set `PD_METRICS_ORACLE` to an existing writable output
directory and run the Go command above. Compare pd.json and resource_group.json
against the committed fixtures. Regenerate typed definitions and oracle inputs
with `python3 scripts/generate_pd_metrics.py <pinned-pd-module-directory>`.

# Follow Go parallel HashAgg finalization

## Purpose


Remove Rust-only global group ordering and avoid retaining every restored spill
partition before emitting aggregate values. Go final workers iterate their maps
without a group-order guarantee. Batch state-memory accounting once per input
chunk as Go does. Preserve SQL values, explicit ORDER BY, worker concurrency,
spill decisions and error propagation. This is not whole-package parity.

## Progress


- [x] Read Go partial/final worker loops and Rust pipeline, spill codec and tests.
- [x] Identify first-seen metadata, global sort, all-partition collection and
  per-row state-memory consumption as concrete implementation differences.
- [x] Build immutable control at 0afb4e3b7e and implement the complete removal.
- [x] Build immutable control and two unordered-finalization candidates. Retained
  HashAgg/spill tests pass (54). Temporary 200000-group executor probes preserve
  exact counts but regress: direct consumption about 83 ms versus 73 ms; dense
  per-map consumption about 87 ms versus 74 ms. Neither is a promoted win.
- [x] Instrument folding and finalization separately to locate the regression.
- [x] Phase probes isolate finalization: folding is about 27 ms in both;
  unordered finalization rises from 43 to 58 ms. The regression is not worker
  scheduling or input folding in this probe.
- [x] Evaluate native IndexMap dense group storage (already locked at 2.14.0)
  with unchanged key hashing and bucket routing; it avoids sparse-table final
  traversal without restoring the global ordering contract.
- [x] Dense-map candidate passes all 54 retained tests and a 200000-group,
  400000-row alternating probe: mean 68.1715 ms versus 73.5893 ms (7.36% less).
  Temporary test and phase timers are removed; source/binaries remain artifacts.
- [x] Build/check/lint pass; 84000 sysbench and 36000 TPC-C measured events
  pass exact work, fresh Go equality and all eleven consistency conditions.
  Live differences are near-neutral, not an overall throughput claim.
- [x] Go/control/candidate HashAgg plans return identical 100000 grouped SQL
  rows with explicit ORDER BY. The separate HASH_AGG hint/property mismatch
  in the unchanged control is recorded, not counted as successful coverage.
- [x] Preserve c728a58eb2 and its collaborator commits. Merged validation:
  56 executor tests, four planner tests, release server build and lint pass.
  Condition eleven now passes after the collaborator's Go-plan corrections.
- [x] Merged binary passes another 6000 sysbench and 6000 TPC-C events with
  exact work, fresh Go equality and all eleven consistency conditions.
- [x] Restore auto-analyze to 1 and verify eleven owned PIDs absent and ten
  ports closed; fixture retained. Prepare normal integration-branch publication;
  remote SHA verification is the final delivery check.

## Context and milestones


The production change belongs in rust/crates/tidb-executor/src/hash_agg/parallel.rs,
with its existing locked indexmap dependency wired in Cargo.toml/Cargo.lock.
Go authority is pkg/executor/aggregate/agg_hash_partial_worker.go and
agg_hash_final_worker.go. The partial worker accumulates memory deltas across a
chunk; final workers emit each map and each restored partition directly.

First remove PipelineGroup.first_seen, its spill fields, chunk-position messages
and final sorting. Advance the private spill version rather than retain an old
decoder. Then append final values while consuming one map at a time, so restored
partitions do not accumulate. Keep aggregate FIRST_ROW state untouched: the
removed ordering concerns groups, not the value chosen inside a group.

From rust/ run CARGO_BUILD_JOBS=12 cargo test --offline --locked --release -j12
-p tidb-executor --lib -- hash_agg --test-threads=12. Use existing unordered-result
and spill assertions; do not weaken SQL checks to accept incorrect values.
Use temporary timing probes only outside permanent test coverage. Build server
with cargo build --offline --locked --release -j12 -p tidb-server --bin tidb-server.
Run scoped cargo check and rustfmt. From checkout root run GOMAXPROCS=12
GOFLAGS='-p=12' make -j12 lint and git diff --check for Ready validation.

Compare immutable before/after binaries without builds or profiles overlapping
timing, retaining exact work counts, fresh Go result equality and eleven TPC-C
conditions. Record large-group and live results with binary identities in one
benchmark receipt. Restore auto-analyze and verify owned PIDs/ports stopped;
preserve the fixture and unrelated main-checkout files. Only normal pushes to
origin/hparser-integration are authorized.

## Surprises & Discoveries


The module header already claimed no global ordering, but executable code still
carried positions through worker channels, merge and spill, then globally sorted.
Existing tests explicitly compare unordered groups. Tracker.consume(0) is cheap,
so batching mainly helps aggregates with nonzero retained-state growth; it is not
assumed to improve every workload.

Deleting the ordering sort did not by itself improve the large-group case.
Changing finalization to a dense per-map buffer did not remove the regression.
Retain immutable binaries and separate phase evidence under
/private/tmp/tidb-hashagg-order.DOPFeg; do not treat source-level deletion as a
performance result.

## Decision Log


Delete the unsupported ordering contract rather than tune its sort. Dense
per-worker storage is an implementation layout, not a SQL insertion-order
promise: bucket routing and merges still determine group output. Do not
change worker scheduling or introduce workload thresholds. Final SQL ORDER BY
remains the responsibility of the sort executor. The temporary spill format is
connection-local scratch data, not a persisted compatibility interface.

## Outcomes & Retrospective


Dense per-worker maps remove the unsupported global sort without the sparse-map
finalization regression. The large-group probe improves about 7.36%; all exact
group/count checks and retained aggregation/spill tests pass. Live sysbench/TPC-C
comparison is near-neutral overall, while grouped SQL and all consistency
checks pass. Merged live smoke and cleanup also pass; final delivery checks normal
publication and the remote SHA. The broad goal
and the separately observed planner hint/property mismatch remain open.

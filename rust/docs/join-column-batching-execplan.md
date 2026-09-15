# Batch join-column reconstruction

This living plan follows root `PLANS.md`.

## Purpose / Big Picture


Reduce generic join-result copying overhead while preserving Go's column layout, nulls, row ordering and shallow-alias behavior. This contributes to the active 25% throughput/latency goal across sysbench, TPC-C and TPC-H; no single query establishes that goal.

## Progress


- [x] Compare saved Rust/Go Q9 traces and owning Go/Rust reconstruction code.
- [x] Process a full column's match runs under one owned destination/source borrow; specialize fixed-width raw copying using the physical type width.
- [x] Run grouped chunk/executor checks and build a release candidate; revalidate after the ordered-partition review fix.
- [x] Benchmark the accumulated changes against identified Rust/Go controls; inspect result correctness and profiles. The 25% objective remains unachieved.
- [x] Follow-up: collect inner-join candidates in staging-sized batches, retaining per-row key/hash/chain state locally; verify result equivalence across output boundaries and residual predicates.
- [x] Compare the candidate-collection batch against the previous candidate and Go; capture current sysbench profiles to select the next cross-workload bottleneck.

## Context and Evidence


`pkg/executor/join/base_join_probe.go::appendProbeRowToChunkInternal` preallocates and walks source-row/count runs column-wise. Rust `rust/crates/tidb-executor/src/base_join_probe.rs` preserves those runs but calls `Column::append_cell_n_times` for every run; each cell then re-enters `SharedBytes` backing dispatch. The saved 2026-09-15 Rust Q9 trace attributes 7.4% self samples to `append_cell_n_times`, 5.7% to `extend_from_range`, and 4.5% to `extend_from_slice`. These sampled shares include execution and wait-stack observations, not a promised speedup. Demangled evidence: `/private/tmp/current-q9-running-paths.json`; raw traces live in `/private/tmp/tidb-sysbench-current.PZ3bEb`.

## Plan of Work and Interfaces


Add `Column::append_cell_runs` accepting a cloneable iterator of `(source_row, repeat_count)`. Borrow source bytes once and append directly to the owned vector once per column. Keep the existing path for shared/frozen destinations to preserve alias semantics. Reuse the existing null-bitmap operation. Integrate at both join reconstruction branches and use constant-width stores for raw fixed cells. No workload names, key cardinalities, new dependency or unsafe memory access belongs in this change.

## Concrete Steps and Validation


From repository root run:

    cargo test --manifest-path rust/Cargo.toml -p tidb-chunk -p tidb-executor --lib -j12 -- --test-threads=12
    cargo test --manifest-path rust/Cargo.toml -p tidb-executor --test all -j12 -- --test-threads=12
    cargo build --manifest-path rust/Cargo.toml -p tidb-server --release -j12
    GOMAXPROCS=12 GOFLAGS=-p=12 make -j12 lint

Existing Go-derived append/copy tests must retain bytes, offsets, nulls, empty runs and append-after-reset behavior. Extend them to compare scalar and batched physical results across fixed/variable types and repetition counts. Retain existing alias tests. Benchmark only after this coherent batch; record executable identity, matched data/query checks and alternating controls. Read comparison scripts before use; revalidate server PIDs/ports rather than reusing historical IDs.

## Surprises & Discoveries


Instruments trace export required cache access outside the sandbox; the authorized retry succeeded. Completed measurements below do not qualify the cross-workload performance objective.

The follow-up test run exposed an unrelated stale ordering assertion: `aggregate_selects` expected sorted GROUP BY output without ORDER BY and received the correct groups in reverse order. Go's comparable aggregate tests call `Sort()` before comparing unordered results. Normalize only this assertion, preserving exact values and multiplicities; no production sort or ordering guarantee was added. The failure is in `/private/tmp/join-candidate-batch-tests-final.log`; the corrected grouped suite passes in `/private/tmp/join-candidate-batch-tests-reviewed.log`.

## Decision Log


2026-09-15: retain Go's existing run design but remove per-cell Rust storage dispatch through a native owned-buffer batch. A shared destination keeps the existing implementation because overlapping shallow aliases can observe each append.

2026-09-15 follow-up: the fresh quiet candidate profile assigns 16.9% self samples to candidate collection. Go's inner probe walks a chain and stages matching addresses before column reconstruction. In `base_join_probe.rs`, borrow the key once per chain and fill the existing staging array with direct disjoint-field access; in `hash_join_v2.rs`, reconstruct when that array fills. Preserve selection-vector mappings, null/filter behavior, collision accounting, cancellation order, residual predicates and output budgets. No new row-address representation, hash algorithm or workload-specific rule is introduced. Also borrow each partition's bucket view once in the existing setup batch, preserving Go's partition-wise lookup order.

## Idempotence and Recovery


Preserve the earlier uncommitted index-merge repairs. Tests are repeatable. Start/stop only task-owned benchmark processes, and do not modify another contributor's servers. Only `hparser-integration` is authorized for remote delivery; do not force-push.

## Outcomes & Retrospective


The chunk changes pass 267 unit tests (four existing ignored); executor unit tests pass 1,311; integration tests pass 328 (184 existing ignored). Six targeted planner tests, release compilation, lint and `git diff --check` pass after the follow-up ordered-partition repair. Latest logs use `/private/tmp/join-column-batch-*-final.log`, with the planner result in `/private/tmp/index-merge-planner-reviewed.log`.

The isolated retained benchmark cluster was stopped, so it was restarted using its original `range-setup-APKpRf` tag and 30000 port offset. User servers on ports 4000/4001 remain untouched. Benchmark artifacts, exact benchmark command arguments, candidate patch, base commit and executable hashes are in `/private/tmp/tidb-reviewed-batch.6lUU91`. Three rotating rounds compared the preserved `server-row-reuse-range-copy` baseline (SHA256 `0330a0d3cf27b6f1f7a9a240f4944db0c0e03e9dc109d58e54fc482c7a6faff2`), the candidate (`162ce57e02547af42aed592a64c31064e46fd1e592df21bbd3ab61357df4af7b`), and the installed Go reference (`e734c80768b5cd44852b01b573bd36f30d19bb47510e0883e6017f18563cbde2`). This installed Go binary is a reference build, not proof that every current source change is deployed.

All 22 SF1 TPC-H queries passed answer checks on all three servers in every round. Median suite wall time was 6.111s before, 6.074s after and 5.682s Go; server CPU was 8.464s, 7.989s and 6.814s respectively. That is about 5.6% less Rust CPU but essentially unchanged elapsed time. Q9's rounded median moved from 0.57s to 0.50s; these coarse query summaries do not establish a precise speedup.

Sysbench read-only, eight clients, 15 seconds per sample: median TPS 1840.3 before, 1849.1 after, 1968.7 Go; mean latency 4.35ms, 4.33ms, 4.06ms; p95 4.74ms, 4.74ms, 4.57ms. All samples had zero errors/reconnects. The fixture has 100011 stored rows; the identical seeded workload selects within the configured 100000-row domain.

Seeded TPC-C, one warehouse, eight clients, 10000 transactions per sample: mixes matched exactly and all runs succeeded. Median TPS was 916.8 before, 990.9 after, 914.0 Go, but paired candidate changes were -2.9%, +9.1%, -3.3%; do not claim a reliable gain from the median. Aggregate mean transaction latency medians were 8.138ms, 7.657ms, 8.291ms. The fixture evolves between runs despite equal input seeds. Post-run `go-tpc-seeded tpcc check -H127.0.0.1 -P34041 -Uroot -Drecordset_tpcc --warehouses 1` and the same command on Go port 34000 passed.

The active 25% throughput/latency goal is not achieved. Full package parity, all ignored cases, and production deployment remain unverified. No code was committed or pushed.

Matched 15-second Q9 profiles are `q9-before-quiet.trace` and `q9-after-quiet.trace`, with demangled stacks in `q9-running-paths.json`. The original baseline trace overlapped consistency checking and is excluded. Among samples labelled Running, memmove self share fell from 11.0% to 7.3%; the nearest TiDB owner `SharedBytes::extend_from_slice` fell from 13.2% to 4.9%. The new inlined/deduplicated batch appears under `append_probe_row_to_chunk` (9.7%). Wait frames remain in these samples, so these shares are hotspot evidence, not precise CPU savings or throughput measurements. Candidate collection (16.9% self) and probe-chunk setup (10.2%) are now the largest named owners to compare against Go next. Both profiling workloads passed answer checks; their rounded Q9 summaries were 0.44s before and 0.43s after and are not substituted for unprofiled benchmark timing.

2026-09-15 update: complete source repair, grouped validation and one three-workload measurement batch; retain the measured limits and current raw evidence rather than claiming the overall performance objective. Task-owned benchmark processes stopped cleanly and their ports are closed; the user's existing servers remain running. Retained fixture data and artifacts are preserved.

Candidate-collection follow-up: production changes are in `base_join_probe.rs` and `hash_join_v2.rs`; the Go-derived projection regression in `tests_jointest_hashjoin_b135_source.rs` compares 513 duplicate-key rows against a nested-loop oracle for integer/string keys, NULLs, residuals and requested chunk sizes 1/31/256/1024. It passed before and after the optimization. Tests preserve the existing 32-row staging size from Go. Measurement artifacts are `/private/tmp/tidb-candidate-batch.x5N519`; the completed results follow. The later correctness review and latest grouped validation are recorded in `index-merge-schema-execplan.md` and `/private/tmp/index-merge-review-*.log`.

The follow-up candidate hash is `0ca8c2e703f04bc9be1bd4e67ec34768fbff053200e23ca397f9661b12813cfe`, compared with the previous candidate hash `162ce57e02547af42aed592a64c31064e46fd1e592df21bbd3ab61357df4af7b`. All four TPC-H rounds passed every answer check. Excluding cold round zero, median suite wall times were 5.712s before, 5.561s after, 5.537s Go (about 2.7% candidate improvement); median CPU was 7.963s, 7.892s, 6.908s (less than 1% candidate improvement). Keep the warm/cold distinction when inspecting the script's all-round summaries.

Sysbench median TPS: 1843.3 before, 1833.8 after, 1949.4 Go. Mean latency: 4.34ms, 4.36ms, 4.10ms; p95: 4.74ms, 4.74ms, 4.49ms. All runs had zero errors/reconnects. This is effectively unchanged, not a gain.

TPC-C mixes matched and runs plus post-run consistency checks on Rust/Go passed. Median TPS: 700.8 before, 741.6 after, 676.0 Go, but paired candidate changes were -3.4%, +5.8%, -16.4%. The last candidate run contains a 3221.2ms maximum in both payment and delivery; before's maxima were 570.4ms and 906.0ms. Do not hide this tail event behind the favorable median or attribute its cause without evidence. The evolving fixture and tail latency require investigation before performance qualification. The 25% goal is still not achieved. Rust/Go sysbench and matched Rust Q9 profiles are complete in the same artifact directory (`sysbench-running-paths.json`, `q9-running-paths.json`); profiling samples do not replace unprofiled latency measurements.

Review stopping point: the task-owned Rust processes and isolated `range-setup-APKpRf` playground were stopped and their listeners verified closed. Fixture data and measurement artifacts remain intact. Other contributors' servers were not signalled. No additional benchmark was run for the final-fetch schema and cleanup fixes, and the recorded candidate binary predates those fixes.

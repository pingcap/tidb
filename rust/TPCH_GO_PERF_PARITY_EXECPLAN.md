# Make Rust TPC-H Faster Than Go TiDB

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. Repository policy in `AGENTS.md` applies: correctness first, minimal diffs, no speculative behavior, verifiable evidence with exact commands, and the complete-Go-package transcreation rule for anything reported as a transcreated package (this plan ports Go structure into existing Rust executors; it does not claim new package transcreations).


## Purpose / Big Picture


The Rust TiDB node on `hparser-integration` already beats the Go nightly node on sysbench and TPC-C (see `SYSBENCH_GATE_FINDINGS.md`). On TPC-H at scale factor 1 it does not: with the coprocessor cache warm the 22 queries take 41.7 s on the Rust node against 23.2 s on Go (build `r20`, head a5e12d4a), and nine queries take more than twice Go's time. The user's requirement is the same as for the other two workloads: every TPC-H query faster on the Rust node than on Go, achieved by following Go's implementation structure rather than by workload-specific shortcuts.

TPC-H differs from the other workloads in what it stresses. Sysbench and TPC-C are many small statements, so per-statement overhead decided them. TPC-H is 22 large analytical queries; each one's time is decided by how the node executes joins, aggregates, sorts and expressions over millions of rows fetched from TiKV through the coprocessor. The TiKV side is the same for both nodes (same cluster, same coprocessor requests, verified in `SYSBENCH_GATE_FINDINGS.md`, round 3), so every second of difference is in the node's own executor.

"Faster than Go" is measured with the harness in `SYSBENCH_GATE_FINDINGS.md`: `tiup bench tpch` with `--check`, single stream, each side restarted before its turn, a cold pass then a warm pass, two rounds with alternating order, answers verified on every run. The acceptance number is the warm pass (the coprocessor cache holds the same responses for both nodes, so the warm pass isolates the node's executor); the cold pass is reported too.


## Progress


- [x] (2026-09-12) Measured `r20` against Go on the full harness: warm 41.7 s vs 23.2 s (1.80x), cold 57.8 s (Go's cold column of that run is not comparable, see the findings document). Per query warm, Rust/Go: Q1 1.0, Q2 3.3, Q3 2.3, Q4 2.4, Q5 1.4, Q6 1.0, Q7 1.9, Q8 2.4, Q9 2.1, Q10 2.3, Q11 2.7, Q12 3.3, Q13 4.7, Q14 1.3, Q15 1.7, Q16 2.0, Q17 1.5, Q18 1.7, Q19 1.1, Q20 1.2, Q21 1.9, Q22 6.4.
- [x] (2026-09-12) Profiled the session thread alone on Q9 (`perf record -t <tid>`): it is saturated for the whole query. Its time: hash-table build 9% self, kernel scheduling from per-chunk worker handoffs 20%, decimal projection evaluation 10%, coprocessor response decode 9%, join output assembly (memmove) 13%.
- [ ] Milestone 1: per-query diagnosis. Warm `EXPLAIN ANALYZE` on both nodes for all 22 queries, per-operator comparison, one named root cause per query where Rust is above 1.5x Go.
- [ ] Milestone 2: the join build phase on workers, as Go's `BuildWorkerV2` does.
- [ ] Milestone 3: column-wise expression evaluation for projections and filters over chunk inputs, as Go's `VectorizedExecute` does, and Go's parallel `ProjectionExec` shape where Go uses it.
- [ ] Milestone 4: the remaining per-query causes from Milestone 1 (each its own commit with an A/B).
- [ ] Milestone 5: full harness run, all 22 queries faster than Go warm; findings document updated; `Ready` validation.


## Surprises & Discoveries


- The coprocessor cache is not the warm gap. A traced build showed the Rust cache key, admission and paging equal to Go's `coprocessor_cache.go`; the r16 guess in the findings document was wrong and has been corrected there.
- The parallel probe pipeline (commits ae441fa3 and a5e12d4a) was the correct Go shape but moved only Q9 (-10%): on most queries the probe phase was not on the critical path. The session thread is: it builds every hash table, decodes every coprocessor response, evaluates every projection row by row and consumes every join result.


## Decision Log


- Acceptance is the warm pass of the existing harness, all 22 queries, Rust faster than Go on each. Rationale: the warm pass isolates the node's executor; the cold pass depends on TiKV block-cache state that the harness cannot control between sides.
- Fixes follow Go's implementation structure (the owning Go package is the reference for each change). Rationale: user instruction and the repository's transcreation rule; workload-specific shortcuts are out of scope.
- Every milestone lands as its own commit with a two-run A/B on the affected queries (node CPU, wall, page faults) before the next starts.


## Outcomes & Retrospective


(To be written at the end.)


## Context and Orientation


The Rust node lives under `rust/`. The executors are in `rust/crates/tidb-executor/src/` (`join.rs` and `hash_join.rs` for the hash join, `hash_agg.rs` for the hash aggregate, `projection.rs`, `sort.rs`), expressions in `rust/crates/tidb-expr/src/`, the coprocessor client in `rust/crates/tidb-distsql/src/` (`cop_paging.rs`, `copr_cache.rs`). The Go references are `pkg/executor/join/` (`hash_join_v2.go`, `hash_table_v2.go`), `pkg/executor/projection.go`, `pkg/expression/` (`vectorized.go`, `chunk_executor.go`, `builtin_arithmetic_vec.go`), and `pkg/executor/aggregate/`.

"Session thread" means the thread that serves one client connection; on the Rust node it drives the executor tree (`driver::drain_root_executor`) and every operator's `next` runs on it unless the operator hands work to the process-wide worker pool (`crates/tidb-executor/src/worker_pool.rs`). "Worker pool" is the node's fixed pool of threads that the parallel hash aggregate and the parallel hash-join probe use.

The benchmark cluster is a `tiup playground nightly --tag bench` on one 4-core, 15 GB machine: PD, one TiKV, the Go node on port 4000 and the Rust node on port 4001, both against the same TiKV. Scripts, binaries and results live in the session scratchpad (`restart-cluster-keep3.sh`, `tpch-run.sh`, `explain-all.sh`, `ab-bin.sh`); the query texts are go-tpc's 22 queries.


## Plan of Work


Milestone 1 collects the evidence: for each query, the warm `EXPLAIN ANALYZE` on both nodes, and for each query above 1.5x, the operator whose time differs and the reason (structure, plan, or a missing pushdown). The result is a table in this plan naming one root cause per query.

Milestone 2 ports the build phase. Go's `HashJoinV2Exec` fetches build-side chunks on one goroutine and hands them to `Concurrency` build workers that partition rows by key hash and append them into per-partition row tables, then builds each partition's hash index; the row tables are immutable during the probe. The Rust `JoinExec` builds on the session thread. The port keeps the Rust row container and index shapes and moves the per-chunk indexing onto pool tasks with Go's partitioning, so the probe workers see the same immutable table.

Milestone 3 ports column-wise evaluation. Go evaluates a scalar function over a chunk column by column (`VectorizedExecute`), with each function's children evaluated into reusable column buffers; the Rust expression crate evaluates every function per row into a datum. The port adds a column-wise path for the operators TPC-H spends its time in (arithmetic and comparison over fixed-width columns) and uses it from the projection and the aggregate input, falling back to the row path otherwise, exactly as Go falls back when a function is not vectorizable.

Milestone 4 takes the per-query causes from Milestone 1 that the two structural milestones do not cover, one commit each.

Milestone 5 is the full harness run and the document.


## Concrete Steps


Restart the cluster on the existing data with a given Rust binary:

    RUST_SIDE=r20 <scratchpad>/restart-cluster-keep3.sh

Collect warm plans and timings on both nodes:

    <scratchpad>/explain-all.sh <label>      # results/explain-<label>/q<NN>.<go|rust>.txt, times.txt

A/B two Rust binaries on Q9/Q3/Q16 (node CPU, wall, page faults, two runs each):

    RBIN=r18 <scratchpad>/ab-bin.sh r18; RBIN=r21 <scratchpad>/ab-bin.sh r21

Full harness (both sides restarted per turn, cold and warm passes, two rounds, answers checked):

    SIDES='go r21' <scratchpad>/tpch-run.sh sf1-r21 2

Validation for executor changes (WIP during iteration, Ready before claiming completion):

    cd rust && cargo test -p tidb-executor --lib -- join::tests hash_join::tests join::spill_tests joiner::tests
    cd rust && cargo test -p tidb-executor --lib
    cd rust && cargo test -p tidb-session --lib -- tests_join tests_explain tests_index_join tests_subquery tests_hash_join tests_semi_join tests_outer_join tests_tpch
    cd rust && cargo clippy -p tidb-executor --lib && cargo fmt --check -p tidb-executor


## Validation and Acceptance


Acceptance: on the full harness, the warm pass shows the Rust node faster than Go on all 22 queries, answers verified on every run, and the executor and session test suites show no failure that does not also fail on the parent commit. Each milestone's commit records its A/B numbers in `SYSBENCH_GATE_FINDINGS.md`.


## Idempotence and Recovery


Every script above can be rerun; the cluster restart reuses the `bench` data directory. If the disk fills (the cargo target directory grows past 20 GB), `rm -rf rust/target/debug/incremental` recovers space without losing release artifacts. If the playground dies, `restart-cluster-keep3.sh` brings it back on the same data.


## Artifacts and Notes


Results directories: `results/tpch-sf1-r20` (the r20 baseline of this plan), `results/explain-<label>` (per-query plans). The findings document `SYSBENCH_GATE_FINDINGS.md` carries the narrative and the tables.


## Interfaces and Dependencies


No new crates. Changes are confined to `tidb-executor` and `tidb-expr`, with the worker pool interface (`worker_pool::enqueue_public`) as the only cross-cutting dependency.

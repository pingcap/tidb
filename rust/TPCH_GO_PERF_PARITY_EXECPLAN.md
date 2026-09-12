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
- [x] (2026-09-12) Milestone 1, diagnosis. Warm `EXPLAIN ANALYZE` on both nodes for all 22 queries (`results/explain-r20`), plan-shape diff, and session-thread profiles of the worst queries (`results/prof-r20`). The Rust node's `EXPLAIN ANALYZE` carries no execution info at all (every operator N/A), so its per-operator picture comes from profiling; Go's does show its operator times. Causes, by the seconds they cost on the warm pass (Rust minus Go, mysql client, one run):
  - Index joins (Q21 2.9 s, Q8 0.9, Q3 0.8, Q20 0.8, Q10 0.5, Q4 0.5, Q16 0.4, Q2 0.1; about 6.8 s in total): the Rust index join materialises the inner side into datum vectors row by row on the session thread (`JoinExec::materialize_index_inner`, 85% of Q8's session thread, 38% of Q21's); Go's `IndexHashJoin` runs `tidb_index_lookup_join_concurrency` inner workers that hash the task's outer rows and probe with chunk rows.
  - Planner build side (Q22 0.6 s, Q7 1.2 s, Q15 0.3 s): the Rust node builds Q22's anti semi join from the 1.5M-row orders scan where Go builds from the 10,834 filtered customers (`index_chunk_selected` is 48% of Q22's session thread); Q7's orders join builds from 1.5M orders where Go builds from the 144,734-row supplier-lineitem result; Q15 uses a hash join where Go uses an index hash join.
  - Session-thread structure (Q9 2.4 s, Q17 1.7 s, Q18 1.6 s, Q13 0.8 s, Q5 0.5 s, and part of every join query): serial hash-table build, row-at-a-time expression evaluation with a decimal subtract that allocates seven strings per row for `1 - l_discount`, per-chunk worker handoffs through futexes, the response decode and its extra copy.
  - Q12 (1.1 s): the session thread is 37% in kernel spin-unlock from futex traffic around an aggregate pipeline over only 31,282 rows; call chains to be read.
  - Q1, Q6, Q14, Q19 (1.0-1.3x): at or near parity.
- [ ] Milestone 2: the index join's inner side on workers, as Go's `IndexHashJoin` inner workers do (largest yield, nine queries).
- [x] (2026-09-12) Milestone 3, part 1 (Q22): commit be617db1. The Rust enumeration already modelled Go's two hash-join shapes for semi and anti-semi joins but was called with the outer-side build disabled because the dispatch context carried no hash-join version; the session now derives it from `tidb_hash_join_version` as Go's `SessionVars.UseHashJoinV2`, and the task-level join records null-aware keys for `CanUseHashJoinV2`. Q22 warm 0.68 s -> 0.15 s (Go 0.10 s). Q7 (inner-join build side) and Q15 (index hash join) remain.
- [x] (2026-09-12) Milestone 6, part 1 (Q12): three commits. The merge join returned as soon as its chunk held any rows, after a drained group (be617db1) and at the loop tail after every compare arm (498633c9), so its parent received one chunk per key group (37,000 chunks for 31,282 rows); Go's `MergeJoinExec.Next` loops `for !req.IsFull()`. Separately, the row container's spill coordinator notified a condition variable at every phase change and `std` issues the futex syscall even with no waiter; the merge join resets its inner-group container per key group, which cost 127,093 syscalls on Q12's session thread (61d7b211: notify only with waiters). Q12 warm 1.5 s -> 0.85-0.96 s, node CPU 1.4 s -> 0.5 s, session-thread futex calls 127,093 -> 48.
- [x] (2026-09-12) Milestone 5, part 1: the decimal add fast path aligns storage scales as Go's `doAdd`/`doSub` align word counts (be617db1); `1 - l_discount` no longer takes the digit-string path (about seven heap allocations per row). Q9 warm 4.9 s -> 4.7 s.
- [x] (2026-09-12) Milestone 3, part 2 (Q7): commit b320e1c1. The build side was a selectivity error, not the cost model: `analyzed_filter_selectivity` sent every `OR` to the 0.8 default, so the per-table filter that join predicate push-down derives from Q7's two-table DNF (`n_name = 'INDIA' OR n_name = 'JAPAN'`) estimated 20 of 25 nation rows instead of 2, and the error grew through the join tree until the orders join built from 1.5M orders. Go's `Selectivity` runs single-column conditions through the ranger; the range branch now accepts `or`. Q7's plan now carries Go's estimates (480,097 on the supplier-lineitem side) and its build sides; Q7 warm 2.63 s -> 1.83-1.92 s (Go 1.48 s).
- [x] (2026-09-12) Milestone 2, part 1 (index-join inner side as chunks): commits 4b76a4b9 and bb2beceb. The common-handle prefix lookup (every TPC-H index join probes lineitem or partsupp, whose primary keys are common handles) appends whole coprocessor batches through `RemoteRowCursor::append_clean_chunk` and projects columns into the request, instead of a datum vector, a full-width physical row and a cell-by-cell append per inner row (Go's `fetchInnerResults` appends the inner reader's chunks). Q8 warm 1.69 s -> 1.51-1.57 s with node CPU 1.5 s -> 1.05 s; Q21 node CPU 3.13 s -> 2.55-2.66 s at 4.0 s -> 3.84-4.0 s wall; Q3 unchanged. The wall not following the CPU on Q21 points at latency between lookup tasks, which is the next part: Go runs the inner tasks on `tidb_index_lookup_join_concurrency` workers.
- [x] (2026-09-12) Memory arbitrator budget (commit a7eda1a5). Q21's session thread was blocked 2.3 s of a 3.9 s query in futex waits; by futex address and call chain, 90% of the parks were the arbitrator's blocking allocate: with `tidb_mem_arbitrator_mode = priority` (the playground's global setting, shared by both nodes) a statement past the small-pool limit pulled every shortfall exactly, one round trip to the arbitrator thread per chunk added to a hash table, about 1.3 ms each on the saturated box. Go's per-tracker big budget grows toward `used * 2.718` past a 95% threshold and reserves the digest profile's previous peak up front, so it makes a handful of round trips. Ported (`BigBudget`, `grow_big_budget`, `reserve_big_budget`, the reservation tiers, the digest profile update; the session records the normalized SQL as Go's `digestKey`). Result: the arbitrator vanished from the park callers and Q21's futex calls halved, but its wall did not move (3.9 s); Q9 4.77 s -> 4.5-4.67 s, Q13 1.09 s -> 1.01 s.
- [x] (2026-09-12) Milestone 2, part 2 (commit a8292f09): a pool worker drains each prefetched index-join task's remote cursor, Go's `fetchInnerResults` on inner workers. A traced build had shown the prefetch queue holding four to five tasks with cursors opened early, but a cop worker sends a task's next page only after the consumer takes the previous one, and one consumer served all cursors. With the drain: the pure index-join query 5.2 s -> 2.3 s (Go 2.0 s) at 3.0 requests in flight (from 0.6); Q21 3.8 s -> 2.7 s (Go 2.4 s); Q8 1.5 s -> 0.91 s (Go 0.89 s); Q3 2.05 s -> 1.0 s (Go 1.53 s); Q4 0.80 s -> 0.48 s (Go 0.50 s); Q16 0.85 s -> 0.58 s (Go 0.59 s); Q10 0.98 s -> 0.91 s (Go 0.64 s); Q2 and Q20 unchanged. Focused tests: 110 passed.
- [x] (2026-09-12) Q21 investigation record: after the arbitrator fix the session thread still blocks 2.0 s in the prefetched lookup cursor's response wait (`append_clean_chunk` -> `CopRowStream::next_chunk` -> the cop iterator's wait). TiKV's own counters show the cause: during Q21 the Go node keeps about 9 coprocessor requests in flight (20.2 s of request time over 2.3 s) and the Rust node 3.4 (13.2 s over 3.9 s) for the same keys scanned; on a pure index-join query (`INL_JOIN` of orders and lineitem) Go keeps 3.9 in flight and finishes in 2.0 s, the Rust node 0.6 and takes 5.2 s with only 3.3 s of request time. The lookups run effectively one request at a time. The cop iterator's concurrency derivation matches Go's (`smallTaskConcurrency`, the clamp to the task count), the request carries Go's `DistSQLScanConcurrency`, and plain leaf lookups take the prefetch fork, so the remaining suspects are the prefetch depth in practice and the region-task count per lookup batch; a temporary trace build is measuring both.
- [x] (2026-09-12) Full harness at r28 (a8292f09): warm 33.3 s against Go's 21.8 s (1.53x; r20 was 1.80x), cold 51.3 s (Go's cold column of that run is again a warm-TiKV-cache number), answers verified on every run. Warm per query, Rust/Go: Q1 0.33, Q6 0.6, Q22 1.0, Q4 1.09, Q20 1.11, Q3 1.10, Q19 1.23, Q21 1.29, Q8 1.31, Q5 1.35, Q7 1.37, Q17 1.45, Q14 1.66, Q10 1.68, Q18 1.75, Q16 1.81, Q9 1.94, Q15 1.98, Q12 2.2, Q2 3.0, Q11 3.0, Q13 3.5. The tables are in `SYSBENCH_GATE_FINDINGS.md`.
- [x] (2026-09-12) Worker finalisation of the final aggregate maps and chunk-row unmatched drain (commit 3e1e8b6a): no measurable gain. A/B r28 vs r29, warm, two runs each: Q13 1.03/0.97 s -> 1.00/0.93 s, Q18 4.50/4.40 -> 4.42/4.38, Q9 4.59/4.68 -> 4.60/4.44, Q17 5.22/5.29 -> 5.23/5.07. The commit also broke `RequiredRows` on the parallel aggregate's output (a whole worker chunk was swapped into a request asking for fewer rows; two tests caught it); fixed in 2c3404f7, which serves the queued chunks range by range and swaps columns only on an exact fit. Kept: the finalisation is Go's structure (`HashAggFinalWorker.getFinalResult` on workers) and costs nothing.
- [x] (2026-09-12) Milestone 5, part 2 (commits 38ac04c4, 448ab95f): the stream aggregate follows Go's `StreamAggExec` + `VecGroupChecker` (group boundaries resolved once per child chunk, an integer group-by column compared cell to cell, states reset in place instead of reallocated, `Next` bounded by `RequiredRows`), and the vectorized filter keeps Go's `VecEvalBool` live-row set without cloning the chunk per filter, with a typed column-wise kernel for the integer and decimal comparison families (`builtin*IntSig`/`builtin*DecimalSig.vecEvalInt`). Q18 4.3 s -> 3.7 s warm (node CPU 4.4 s -> 3.6 s); Q13, Q9, Q17, Q12 unchanged. The parallel aggregate's GROUP_CONCAT "warn once" sentinel is now shared across final workers by compare-and-swap (Go `groupConcat.truncated`); worker finalisation (3e1e8b6a) had made a session test warn twice.
- [x] (2026-09-12) Q13 and the response decode (commit 815aaf3e): the parallel hash join's bulk unique-key probe was gated off for a preserved build side, so Q13's `customer LEFT JOIN orders` (built from customer) assembled 1.2M joined rows cell by cell on the workers (33% of the node's CPU); the bulk path now records the matched build rows as Go's `leftOuterJoinProbe` marks its used-rows bitmap. Q13 0.97 s -> 0.81 s warm (node CPU 2.8 s -> 2.2 s). A select response decoded from a byte slice copied every chunk's rows twice (prost `copy_to_bytes` on a slice, then the `Vec<u8>` field); decoding the owned payload through `Bytes` leaves one copy, Go's `Unmarshal` count. Q17 5.1 s -> 4.95 s, Q18 3.8 s -> 3.67 s.
- [x] (2026-09-12) Milestone 5, part 3: column-wise decimal arithmetic for projections (Go `builtinArithmetic{Plus,Minus,Multiply}DecimalSig.vecEvalDecimal`). A `+`/`-`/`*` tree over decimal and integer columns and strict constants evaluates on `i128` coefficients read straight from the chunk's MyDecimal cells and writes the cells back through a new `MyDecimal::from_scaled_i128` (the layout `from_decimal_parts` builds, without the digit string); any shape or value outside the exact fast path falls back to the row evaluator for the whole column. The cells are the row evaluator's, checked cell for cell in a test. A/B r32 vs r33, warm: Q9 4.67 s -> 4.03 s, Q5 2.36 -> 2.27, Q10 0.81 -> 0.78, Q8 0.93 -> 0.89, Q7 and Q3 unchanged.
- [x] (2026-09-12) Milestone 6, part 2, the scan and decode path (commit c739e76b and the following one): the table scan cursor appends a decoded batch it cannot move whole as column ranges instead of cell by cell (Q5 2.36 s -> 2.18 s); a coprocessor response's chunks are now `Bytes` slices of the response payload (prost `bytes` field) and the chunk decoder points each column at that payload as Go's `decodeColumn` does (`col.data = buffer[:n]`, `avoidReusing`) through a frozen `SharedBytes` backing that detaches on the first mutation, removing the two remaining copies of every scanned byte on the session thread. jemalloc's `oversize_threshold` is set to 0 in the compiled-in `malloc_conf`: it returned every freed allocation above 8 MiB to the OS at once, so each large response payload was page-faulted in again (Q9: 600k minor faults, about 0.7 s of node CPU); Go's page heap keeps freed spans for reuse and scavenges in the background, which jemalloc's ordinary dirty-page decay now stands in for. Measured with the environment override before the change: Q9 3.8-3.9 s -> 3.6-3.8 s, node CPU 6.3-6.9 s -> 5.8-6.0 s.
- [x] (2026-09-12) Q9 cost accounting on the 4-core box (r34): Go 2.3-2.7 s wall with 2.9-4.3 s of node CPU; Rust 3.6 s wall with 5.6-5.8 s. TiKV's coprocessor handle time is the same for both (about 9-10 s of CPU per run) and the requests in flight match (5-6), so the box is CPU-bound and the wall gap tracks the node CPU gap. Rust's session thread: decode about 1.0 s (now addressed), hash-table build 0.5 s, futex wakes and scheduling 0.7 s (13,700 futex calls per run, 1.4 s of wall in them), decimal projection 0.4 s, jemalloc's profiling backtraces 0.2 s (Go's rate, libgcc's unwinder; documented earlier). Workers: 65% in the probe closures, of which the generic multi-key probe materialises each build candidate through `with_row` (17%). Go's pprof for the same query: 57% in `processOneProbeChunk`, memmove 17%, `isKeyMatched` 8%, `AppendCellNTimes` 16%.
- [x] (2026-09-12) Milestone 4, part 1: the hash join's build table chains its row pointers through one entry slab per table (Go v1 `entryStore`/`entry`), replacing a `Vec<RowPtr>` per key: no allocation per key or per row, 16-byte map values instead of 24, and the table's memory charge follows Go's slab rule (`GetStore`: 64 entries doubling to 8192, 16 bytes each). Two spill tests' hand-tuned quotas moved above that charge. A/B r35 vs r37, warm: Q13 0.78 s -> 0.71 s (node CPU 2.15 s -> 1.95 s), Q18 3.60 -> 3.45, Q3 1.05 -> 0.98; Q9, Q5, Q17, Q7, Q10 unchanged.
- [x] (2026-09-12) Tried and dropped: Go-style spinning before parking in the worker pool (`stealWork`/`wakep`, a 20 us spin, at most two spinners, no wake while one spins). Q18 hung two runs in six; a version that also wakes another worker when a spinner takes a task with more queued (Go's `resetspinning`) still hung Q17 and Q18 (two of eight Q18 runs), and neither version gained anything measurable (Q9 3.55 s -> 3.48 s, Q13 0.69 -> 0.75, Q17 and Q18 mixed). Go's scheduler keeps that rule inside the runtime, next to the run queues it inspects; a condvar pool cannot see what a parked worker will and will not check, and the wake it skips is not always covered. The session thread's futex traffic (13,700 calls on Q9) stays as the cost of per-chunk handoffs, which Go pays too (7% of its Q9 profile in `Syscall6`).
- [x] (2026-09-12) Milestone 3, remainder (Q15) and Q17's final AVG (commit after 6bff62d5). Q15's plan differed from Go's because `analyzed_filter_selectivity` estimated each range condition of `l_shipdate >= a AND l_shipdate < b` separately and multiplied them as if independent (27% of lineitem instead of Go's 3.8%), which chose a hash join over Go's index hash join; Go's `Selectivity` runs every condition on a column through the ranger together (`ExtractAccessConditionsForColumn`, `BuildColumnRange`), and the planner now does the same for the range family. Q15's plan is now Go's; its wall (2.0 s vs Go 1.5-1.8 s) is the coprocessor's two lineitem scans (about 7 s of TiKV handle time per run on both nodes, in-flight 6-8 on both). Q17's final-mode `AVG(count, sum)` over 1.2M partial rows folded through the Datum path (digit-string decimals) on the parallel pipeline; both executors now read the partial count and MyDecimal sum cells directly (Go `avgPartial4Decimal.UpdatePartialResult`). A/B r39 vs r40, warm: Q17 5.06 s -> 4.46 s (node CPU 7.9 s -> 5.9 s); Q13, Q9, Q18 unchanged.
- [x] (2026-09-12) Milestone 5, part 4: decimal SUM states finish straight into the output chunk cell (Go `sum4Decimal.AppendFinalResult2Chunk` calls `chk.AppendMyDecimal(&p.val)`), through `append_finished_agg_value` at the five emit sites (stream, grouped stream, serial hash, the parallel default row and the worker finalisation), instead of building a `Datum` from the `i128` and appending it through the generic datum path; and the aggregate input and AVG cell readers take the decimal column's scaled `i128` directly instead of building a `MyDecimal` per row. A/B r40 vs r41, warm, two runs each: Q18 3.60/3.48 s -> 3.35/3.29 s (node CPU 3.15/3.01 s -> 2.88/2.84 s), Q5 2.16/2.14 -> 2.01/2.07, Q10 0.76/0.73 -> 0.73/0.70; Q17, Q9 and Q1 unchanged. Executor `hash_agg`/`aggregate` tests and the session aggregate/TPC-H/decimal suites pass except the two plan-shape failures already on the parent.
- [x] (2026-09-12) Milestone 5, part 5 (Q2, Q11): the filter kernel covers `NOT` over a covered node and `IS NULL` over a column (Go `builtinUnaryNotIntSig.vecEvalInt`, `builtin*IsNullSig.vecEvalInt`): Q2's `not(isnull(Column#57))` Selection over the 160k-group aggregate was 45% of the session thread through the row evaluator's argument vectors. The column-wise decimal arithmetic accepts `cast(int_column as decimal(flen, scale))` whose integer digits hold every value (Go `builtinCastIntAsDecimalSig.vecEvalDecimal`; the row path's `ProduceDecWithSpecifiedTp` cannot clamp there): Q11's `ps_supplycost * cast(ps_availqty as decimal(20,0))` projection was 21% of its session thread in `eval_cast`/`decimal_binary` and their digit-string `from_utf8`. A/B r41 vs r42, warm, two runs each: Q2 0.29/0.27 s -> 0.24/0.24 s (node CPU 0.53/0.50 -> 0.43/0.42), Q11 0.26/0.26 -> 0.22/0.21 (0.43/0.41 -> 0.36/0.34); Q18, Q9, Q17, Q19 unchanged. `cargo test -p tidb-expr --lib` (1179 passed), executor selection/projection/hash_agg tests, and the whole session suite (failures all on the parent's known list).
- [x] (2026-09-12) Milestone 6, part 3: the parallel hash-join probe fills the caller's chunk from the finished worker results until it is full, as Go's `runJoinWorker` keeps filling one `joinResult` chunk across probe chunks and sends it on only when full; the first result still moves without a copy (`req.SwapColumns`), a later one appends column-wise when it fits whole, and one that does not is handed over on the next call. A selective probe (Q11: 800k partsupp rows against 400 suppliers, ~40 matches per 1024-row probe chunk) had handed its parent ~780 small chunks, each a pipeline handoff and a futex wake below (24% of Q11's session thread was in the kernel's wake path). A/B r42 vs r43 and r43 vs r44 (the refined fill), warm, two runs each: Q11 0.22/0.22 s -> 0.21/0.21 (node CPU 365 -> 320 ms), Q13 0.76/0.73 -> 0.71/0.71 then 0.76/0.73, Q5 2.11/2.04 -> 1.95/1.97 then 2.05/2.02, Q9, Q18, Q17, Q10, Q2 within the +-0.1 s noise band. A modest, honest gain: the match-heavy joins (Q9, Q17, Q18) already produced near-full chunks.
- [x] Milestone 3, remainder: Q15's index hash join.
- [ ] Milestone 2, part 2: the index join's tasks on workers (outer task builder feeding inner workers, ordered result delivery), as Go's `IndexHashJoin`.
- [ ] Milestone 4: the hash join build phase on workers, as Go's `BuildWorkerV2` does.
- [ ] Milestone 5: column-wise expression evaluation for projections and filters over chunk inputs, as Go's `VectorizedExecute` does; the decimal fast path aligned to Go's scale handling (done in the working tree, tested, awaiting its A/B).
- [ ] Milestone 6, remainder: the per-chunk handoff cost on the hash aggregate and join pipelines; the response decode copy.
- [ ] Milestone 7: full harness run, all 22 queries faster than Go warm; findings document updated; `Ready` validation.


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


Every script above can be rerun; the cluster restart reuses the `bench` data directory. If the disk fills (the cargo target directory grows past 20 GB; the debug tree alone reached 13 GB after a few full-suite runs and a linker then dies with a bus error), `rm -rf rust/target/debug` recovers space without losing release artifacts; the next test run rebuilds it. The temp filesystem is the same disk, so old node binaries and profiles in the scratchpad count too. If the playground dies, `restart-cluster-keep3.sh` brings it back on the same data.


## Artifacts and Notes


Results directories: `results/tpch-sf1-r20` (the r20 baseline of this plan), `results/explain-<label>` (per-query plans). The findings document `SYSBENCH_GATE_FINDINGS.md` carries the narrative and the tables.


## Interfaces and Dependencies


No new crates. Changes are confined to `tidb-executor` and `tidb-expr`, with the worker pool interface (`worker_pool::enqueue_public`) as the only cross-cutting dependency.

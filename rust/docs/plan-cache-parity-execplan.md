# The node reuses an optimized plan, as Go's plan cache does

Status: implementation, benchmark, and Ready validation complete
(2026-08-27). The first repair pass raised paired read-only/read-write ratios
to about 0.953 and 0.98, but the governing goal is for the Rust implementation
to beat Go on the sysbench workloads. A second decomposition found two cached
range-executor mismatches. After their root repairs, three longer read-only
pairs measured a 1.028 median ratio and a disjoint equal-version-depth
read-write pair measured 1.007.

## Purpose / Big Picture

Run the same `SELECT` twice on one connection. Go optimizes it once and
executes the retained plan the second time. This node re-optimizes it every
single execution — choosing the access path, detaching ranges, reordering
joins, and for `GROUP BY`/`DISTINCT` doing all of that three times over.

What a user can do after this change that they cannot do now: run a repeated
query and have it cost what it costs on Go, and have `@@last_plan_from_cache`
tell them the truth about whether a plan was reused.

How to observe it, before and after:

```sql
PREPARE s FROM 'SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?';
SET @a=1; SET @b=100;
EXECUTE s USING @a,@b;
EXECUTE s USING @a,@b;
SELECT @@last_plan_from_cache;   -- reports 1 today, and it is a LIE
```

Today the second `EXECUTE` re-optimizes from the AST. `@@last_plan_from_cache`
reporting `1` is NOT a bug: `prepared_plan_cache.rs` deliberately ports Go's
observable contract — which statements Go's cache would admit, and whether
this `EXECUTE` would have found its plan there — and says so in its own doc.
An application reading that variable sees what Go shows it. **The divergence
is cost, not observable behavior**, and this plan is about closing the cost.

(An earlier revision of this document called that variable "a lie". It is
not. The correction matters because it changes what "parity" means here: the
contract is already ported, and what is missing underneath it is the reuse.)

### Why it matters, measured

Measured 2026-08-26 against a Go `tidb-server` on the same TiKV, median of
three 10-second samples, engines alternating inside each sample. Every query
below scans exactly ONE row, so storage work is constant and the only thing
that varies is how much optimizer work the shape needs:

| shape                     | rust us | go us | excess |
| ------------------------- | ------: | ----: | -----: |
| point-get (`WHERE id = ?`)|   157.8 | 144.7 |    +13 |
| range (`BETWEEN`)         |   231.2 | 211.5 |    +20 |
| `SUM` over a range        |   252.1 | 254.3 |     -2 |
| `ORDER BY` over a range   |   250.3 | 246.3 |     +4 |
| `DISTINCT` over a range   |   378.7 | 301.5 |    +77 |

The point-get shape is the ONLY one with real plan reuse today
(`PreparedPointGetPlan`), and shrinking a range from 100 rows to 10 leaves
the excess unchanged, which is what says this is per-STATEMENT work and not
per-row decode, encode, or wire volume.

Non-obvious terms, in plain language:

- **Plan cache**: a per-session (optionally per-instance) map from "this
  statement text, under this session state, with parameters of these types"
  to the optimizer's finished answer, so the next execution skips optimizing.
- **Physical plan**: the optimizer's finished answer — which index to read,
  in which order, with which operators stacked on top. Distinct from the AST
  (what the user typed) and from the executors (the running machinery).
- **Rebuild**: on a cache hit the retained plan is correct in SHAPE but its
  key ranges were derived from the PREVIOUS parameter values. Go re-derives
  just the ranges from the new values and reuses everything else.
- **Cacheable**: not every statement may be cached. A statement reading
  `now()`, a temporary table, or a `LIMIT` with a variable count would give a
  wrong answer if its plan were reused; Go enumerates these and refuses.

## Go contract being ported

Source of truth is this repository's own Go tree.

- `pkg/planner/core/plan_cache.go`
  - `GetPlanFromPlanCache` (`:205`) replaces the ordinary optimizer for both
    prepared and non-prepared statements. It decides whether the cache is
    enabled, builds the key, looks up, and on a miss calls `generateNewPlan`.
  - `lookupPlanCache` (`:308`) reads the session cache (or the instance cache)
    and returns `PlanCacheValue{Plan, OutputColumns, StmtHints}`.
  - `adjustCachedPlan` (`:330`) is the hit path: check privileges, then
    `RebuildPlan4CachedPlan(plan)`; only if that succeeds does it set
    `sessVars.FoundInPlanCache = true` and return the plan. A rebuild that
    fails falls through to a full optimization.
  - `generateNewPlan` (`:360`) is the miss path: `OptimizeAstNodeNoCache`,
    then `isPlanCacheable` on the RESULT, then `Put`. It ends with
    `sessVars.FoundInPlanCache = false`.
- `pkg/planner/core/plan_cache_rebuild.go`
  - `RebuildPlan4CachedPlan` (`:32`) walks the physical plan and calls
    `rebuildRange`; `buildRangeForTableScan` (`:183`),
    `buildRangeForIndexScan` (`:415`), `buildRangesForPointGet` (`:256`),
    `buildRangesForBatchGet` (`:326`) re-derive ranges from the new parameter
    values. `isSafeRange` (`:449`) refuses a rebuild whose new ranges are not
    equivalent, which is what makes reuse safe rather than merely fast.
- `pkg/planner/core/plan_cache_utils.go` — `PlanCacheStmt`, `PlanCacheValue`,
  `NewPlanCacheKey` and everything the key must include.
- `pkg/planner/core/plan_cacheable_checker.go` — `Cacheable`,
  `NonPreparedPlanCacheableWithCtx`, `isPlanCacheable`: the refusal rules.
- `pkg/planner/core/plan_cache_lru.go` — the LRU keyed by
  `(cacheKey, paramTypes)`, with memory accounting.
- Session variables: `tidb_enable_prepared_plan_cache` (ON by default),
  `tidb_enable_non_prepared_plan_cache` (OFF by default),
  `tidb_prepared_plan_cache_size`, `tidb_non_prepared_plan_cache_size`,
  `last_plan_from_cache` (`SessionVars.FoundInPlanCache`, reported as the
  PRECEDING statement's outcome via `PrevFoundInPlanCache`).

The single most important structural fact: Go caches a **physical plan** and
rebuilds only its ranges. It does not cache executors, and it does not cache
the AST.

## Where this node stands today (survey, 2026-08-26)

- `crates/tidb-session/src/prepared_ast.rs` — `PreparedAst` retains the parsed
  `Stmt`, the parameter count, and `point_get_plan: Option<Arc<PreparedPointGetPlan>>`.
  A full retained plan, for point gets only — and the point-get shape is the
  one measuring at parity.
- `crates/tidb-session/src/prepared_path_pins.rs` — REAL reuse, and more than
  the survey first credited: each join leaf's committed access path is
  captured on a statement's first execution and replayed on the next, keyed by
  `PreparedPlanKey` (schema version, database, `sql_mode`, time zone, push-down
  blacklist generation) and invalidated when any of those moves.
  `choose_index_range_path` (`driver/access.rs:3638`) restricts enumeration to
  the pinned index on replay, citing `RebuildPlan4CachedPlan`. Ranges are still
  rebuilt from the current parameters, which is the half Go also rebuilds.
  **Prepared statements only** — a repeated statement sent as text does not
  benefit, which is a measurement trap: a probe that sends plain text 3000
  times sees none of this, while sysbench under `--db-ps-mode=auto` does.
- `crates/tidb-session/src/prepared_plan_cache.rs` /
  `non_prepared_plan_cache.rs` — the ADMISSION rules and cache key at full
  fidelity (`IsASTCacheable`, `NonPreparedPlanCacheableWithCtx`,
  `NewPlanCacheKey`), plus the hit/miss `@@last_plan_from_cache` reports.
  Neither stores a plan, deliberately and documented. When a reified plan
  lands these are the gates it plugs into; they do not need porting again.
- `crates/tidb-executor/src/driver.rs` — `run_select_stmt` →
  `run_select_traced_with_delivery_choice_inner` builds the executor pipeline
  DIRECTLY from the AST. There is no retained plan object between the two.
- `crates/tidb-planner` — a large planner crate exists (`find_best_task`,
  `group_expr`, `cascades_base`, `implementation_cost`, …) but the executor
  uses it for COSTING, not construction: of the executor's references to it,
  `candidate_cost` appears 145 times, `plan_cost_ver` 42, `cardinality` 25,
  `physical_property` 59 — and `find_best_task` only 13. The planner's trees
  do not drive execution.

This is the blocker, stated plainly: **Go's plan cache caches a physical plan,
and this node does not have one.** Any plan for this work must first create
the thing to cache. What is already done is everything AROUND that object: the
admission rules, the key, the invalidation, the observable contract, and a
narrow slice of real reuse (the leaf access path, and whole plans for point
gets). What is missing is the object itself and the rebuild that rebinds it.

### The second defect, in the same area

`driver.rs:1497`: when a select has `GROUP BY` or `DISTINCT` and no required
sort order, the node plans the WHOLE select with `AggregationChoice::Stream`,
plans it again with `AggregationChoice::Hash`, compares the two costs, and
then plans it a THIRD time with the winner — re-running access-path selection
and join reorder in each pass. Sampling one `DISTINCT` statement put 33% of it
in those three passes.

Go does not do this. `ExhaustPhysicalPlans4LogicalAggregation`
(`pkg/planner/core/operator/physicalop/base_physical_agg.go:935-946`) builds
`getStreamAggs` beside `getHashAggs` from ONE logical aggregation and hands
both families to the coster with `aggs := append(hashAggs, streamAggs...)`.
The children below are planned once and shared; only the aggregation operator
differs. The code comment at `driver.rs:1490` cites exactly this Go function
while doing the opposite.

## Milestones

Each milestone must leave the tree green (`cargo nextest run --workspace`, no
new failures against the 42 pre-existing ones) and must be independently
committable.

### M0 — Prototype: is a retained plan reachable? (feasibility, do this first)

The rest of the plan depends on an answer this milestone must produce with
running code, not with reasoning.

Pick the narrowest real shape: `SELECT <cols> FROM <t> WHERE <pk> BETWEEN ? AND ?`
— a single table, a range access path, no join, no aggregation. Introduce a
`CachedSelectPlan` value that records the DECISIONS
`run_select_traced_with_delivery_choice_inner` reaches for it (chosen access
path and index, column offsets, pushdown shape, output column metadata) and a
builder that constructs the executor pipeline from that record plus freshly
derived ranges — without re-running `best_single_table_access_path`.

Deliverable: a test that plans this shape once, builds executors from the
record twice with DIFFERENT parameter values, and asserts both answers equal
the answers the ordinary path gives. Plus a recorded measurement of what the
second build costs versus a full plan.

Record in `Surprises & Discoveries` whether the decisions are cleanly
separable or whether the driver interleaves decision and construction so
tightly that the split has to happen elsewhere. **If they are not separable,
stop and revise this plan before writing more code.**

### M1a — Stop planning a third time (DONE, `4ee77f0109`)

Deliver the winning candidate's pipeline instead of re-planning under the
winner. Go costs the candidates it already built; a pass run AFTER the
comparison is a plan whose cost was never the one compared.

Landed. Two boundaries were discovered while doing it and both are now in the
code:

- Delivery is only sound when there is no trace to build. A traced pass
  APPENDS to the caller's trace, and the caller may already hold the outer
  plan's frames; a speculative pass writing into its own trace cannot
  reproduce them. EXPLAIN and EXPLAIN ANALYZE keep the third pass.
- `derived_output` was `output_delivered.is_some()`, so raising a receipt to
  READ a cost also switched the pass into derived-relation mode. The pipeline
  being costed was therefore not the pipeline the statement would run.
  `Delivered::for_cost()` marks a costing receipt and `derived_output`
  consults the mark.

**Measured effect: within noise, as expected.** Of the 2079 samples the three
passes cost on a `DISTINCT` statement, only 120 were the third one.

### M1b — Do not repeat the family race on a replay (DONE, `0f7953702f`)

Go's cached plan carries its aggregation operator already decided;
`adjustCachedPlan` rebuilds ranges and nothing else. The access-path pins had
already ported that contract for join leaves, so the statement's aggregation
family joins the same entry: `HashMap<String, PinnedLeafAccess>` becomes
`PinnedPlanShape { leaves, aggregation }`, and a replay plans ONCE under the
pinned family instead of three times. Keying, invalidation and eviction are
unchanged.

This is the prepared-statement path, which is what sysbench and every
application driver use. A statement sent as plain text still costs both
families every time, because pins are prepared-only — see M1c.

### M1c — Share the children WITHIN one first execution

This is where the 31% actually is: 1959 of 6273 samples, in the two
SPECULATIVE passes, which each re-run access-path selection and join reorder
against the same tables.

Plan the children ONCE and cost the two families over that one result, as
`ExhaustPhysicalPlans4LogicalAggregation` does. The required physical property
differs between the families (StreamAgg wants sorted input), so the shared
part is everything the property does not change — in particular the
access-path ENUMERATION, which Go derives once on the logical plan
(`DeriveStats` / `deriveTablePathStats`) and then chooses from per property.
Memoizing that enumeration by (table, filter) within one statement is the
narrowest form of this and should be tried first.

Verification: the `distinct_1row` shape from the probe below must move, and no
`EXPLAIN` output across the corpus may change.

### M2 — Range rebuild

Port `RebuildPlan4CachedPlan` for the shapes M0 covers: re-derive ranges from
new parameter values and refuse the reuse when `isSafeRange`'s equivalent
fails. This is the correctness core; it lands before anything is cached.

### M3 — The cache itself

`PlanCacheValue` equivalent, an LRU keyed by `(cacheKey, paramTypes)`, the key
built from everything Go's `NewPlanCacheKey` includes (schema version, current
database, SQL mode, timezone, `sql_select_limit`, the read timestamp settings,
binding, and the statement's parameterized text). Wire the prepared path
first; the non-prepared path is off by default in Go and follows.

`found_in_plan_cache` moves to being set by a real hit, and the key-only LRU
in `non_prepared_plan_cache.rs` is deleted — its cacheability checker stays.

### M4 — Widen the shapes

Index scans, point/batch gets folded onto the same mechanism (replacing the
bespoke `PreparedPointGetPlan` path), joins, aggregation. Each shape lands
with its own rebuild rule and its own refusal.

### M5 — Invalidation and eviction

Schema version change, `tidb_prepared_plan_cache_size`, statement-level
refusals (`SetSkipPlanCache`), and the memory accounting Go does. Plus the
`EXPLAIN FORMAT='plan_cache'` surface if it is in scope by then.

### M1d — Global aggregates pick Go's family (DONE, `da8da8a0c4`)

`SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?` -- sysbench's
`sum_range`, and the shape a pre-existing red test already pinned -- planned a
root HashAgg where Go plans a root StreamAgg. The chooser compared the two
families at the SCAN's cardinality, but the partial stage runs in TiKV, so the
ROOT's input is ONE row. Go's own `EXPLAIN FORMAT='verbose'` prices its root
StreamAgg at 49.90 (`1 row * 1 func * cpuFactor`) against a root HashAgg's
1566.94, dominated by the undivided `10*3*cpuFactor` start cost. Fed one row
instead of 99, our formula -- which was already faithful -- agrees with Go.

Two pre-existing failures went green. A third test went red and was itself
wrong: asked directly, Go answers StreamAgg for it too. **Ask the Go node for
`EXPLAIN FORMAT='verbose'` on the exact statement before trusting a plan
expectation in this tree.**

### M2r — The row pipeline (per-row term LOCATED; not where the code smell is)

The five-point fit stands: rust 0.686 us/row against Go's 0.387, plus a fixed
+27.8 us/statement. What changed is WHERE the per-row term lives.

A profile of a 200-row range never shows `get_datum_row` at all — the
`Vec<Datum>`-per-row and `to_vec()`-per-string-cell allocations account for
under 1.2% of the statement, roughly 0.025 us/row against a 0.299 us/row gap,
an order of magnitude short. Twice this document named them as the cause;
they are not.

Asked on the identical 200-row statement, the two engines are indistinguishable
where it would have mattered:

| | Rust | Go |
| --- | ---: | ---: |
| cop RPCs per statement | 1 | 1 |
| MVCC bytes processed | 44400 | 44400 |
| wire payload per row | ~135 B | ~129 B |

(Go from `EXPLAIN ANALYZE`'s `scan_detail`/`cop_task`; Rust from the node's own
page counters over 2000 executions.)

So the per-row term is not TiKV-side work, not extra round trips, and not wire
volume. It is client-side work spread thinly — the 200-row profile leaves
~410 samples unattributed across chunk decode, `append_partial_row_by_col_idxs`
(78), datum materialization and wire encoding, none individually above the
profiler's threshold. **A diffuse cost is not a milestone.** Streaming the
result set would be a large contract change aimed at a cost that is not
concentrated where the change would land, so this is parked, not scheduled.

The shape divergence is still worth recording for its own sake: `SelectMeta` is
`(columns, Vec<Vec<Datum>>)`, so every string is copied twice (chunk -> Datum
-> wire) where Go's `writeChunk`/`dumpTextRow` copies once. That is a design
difference; it is not, at these row counts, a measurable one.

### M3s — Open a statement's snapshot on the connection worker (DONE, `7c267b40ee`)

The FIXED term has a concentrated home. On the same 200-row profile, 14% of
the statement is snapshot acquisition: 326 samples waiting for a PD timestamp
and 192 waiting for the transaction-worker thread to hand back a read-only
transaction. That handshake is ours alone — Go builds its `KVSnapshot` on the
connection goroutine and hands nothing to another thread — and ~192/4195 of a
~450us statement is ~20us, which is the right order for the +27.8us intercept.

The premise that forced the handshake is already disproved: three files claim
"the production transport is worker-local (`Rc<RefCell<..>>`)", and
`SharedReadRuntime` is `Arc<Mutex<C>>` + `BackgroundRegionCache<L>`, with no
`Rc` or `thread_local` anywhere in `tidb-txnkv` (`tests/transaction_send_source.rs`
asserts `Send` for both production transactions).

The implementation now opens `RealOptimisticTransaction` inline after the PD
timestamp arrives and calls `snapshot_get_at`, `snapshot_batch_get_at`, and
`snapshot_scan_at` directly. It retains the absolute per-read deadline and the
closed-snapshot error contract. The remaining explicit-transaction read path
still uses `SessionSnapshot`'s request/reply channel; that is a separate,
larger change and is tracked in the deep-review receipt below.

## Where the sysbench gap actually is (measured 2026-08-26, end of day)

Four workloads, engines alternating inside every sample, median of 3:

| workload | rust tps | go tps | rust/go |
| --- | ---: | ---: | ---: |
| oltp_point_select | 9634.1 | 8737.4 | **1.103x faster** |
| oltp_write_only | 1000.6 | 960.9 | **1.041x faster** |
| oltp_read_write | 225.6 | 258.8 | 0.872x |
| oltp_read_only | 354.0 | 456.5 | 0.775x |

`oltp_read_only` decomposed by switching statement groups off IN THE REAL
WORKLOAD (not single-statement cells, which mispredicted it twice):

| group | rust/stmt | go/stmt | delta | count | contributes |
| --- | ---: | ---: | ---: | ---: | ---: |
| point select | 113.6us | 98.0us | +15.6 | x10 | +156us |
| simple range | 269 | 203 | +66 | x1 | +66 |
| sum range | 263 | 187 | +76 | x1 | +76 |
| order range | 351 | 246 | +105 | x1 | +105 |
| distinct range | 494 | 321 | +173 | x1 | +173 |
| | | | | **sum** | **+576us** |

against a measured full-transaction deficit of +640us, so the attribution is
essentially complete. **There is no dominant term left.**

The point-select line decomposes exactly:

* 5.3us is the `ask` channel hop to the transaction worker (measured directly:
  caller 79.2us, worker service 73.9us, `asks == served`).
* ~8us is SQL-layer work above storage (ours ~34us against Go's ~26us).
* The TiKV Get itself is at parity -- 73.9us against Go's own
  `tidb_tikvclient_request_seconds{type="Get"}` of 71-73us.

Note the sign flip that makes this workload-specific: **autocommit** point
selects are 104us here against Go's 114us -- we WIN -- because Go pays a TSO
per autocommit statement. Inside a transaction Go drops that TSO and gets
16us faster while we get 10us slower, and the crossover is the whole story of
`oltp_point_select` being 1.10x and `oltp_read_only` being 0.775x.

Self-time for the in-transaction range workload: 55.4% `semaphore_wait_trap`
(coprocessor I/O, identical work to Go), 7.3% `__recvfrom` (client idle), then
2.9% memmove, ~4% allocator, 1.3% hashing. Nothing else above 1%.

### What closing the rest would take (all candidates now priced)

The gap is NOT diffuse CPU and NOT the row pipeline. Phase timers inside one
in-transaction range statement (247.6us total, sysbench `oltp_read_only`,
ranges only):

| phase | us | share |
| --- | ---: | ---: |
| `next_executor` drain | 226.3 | 91% |
| planning | ~13.5 | 5% |
| executor `open` | 4.6 | 2% |
| materialize 50 rows | 3.2 | 1% |

Materialization is **0.064us/row** -- a quarter of the 0.257us/row gap it was
once blamed for -- and planning is 13.5us, not the ~112us the SQL-layer
subtraction suggested. Sampling could not separate these because two thirds of
the statement is a wait; timers could.

The first survey measured each boundary separately but stopped before fixing
them.  A second pass on 2026-08-27 combined source-level receipts with
alternating end-to-end runs and found that the individually small crossings
compound in the two long sysbench transactions.  The production fixes are:

| boundary | measured evidence | root repair |
| --- | --- | --- |
| cop response publication | response latency included the publication receipt | retain deferred receipts in `BatchCoprocessorPending` and resolve them after response completion |
| cop-scan producer handoff | **11us/statement** and a worker-ID regression | pull the lazy response on the query worker; keep only large index lookups on their parallel worker |
| in-transaction read hop | **5.3us/read** (ten reads per read-only transaction) | keep transaction state behind a shared mutex and execute snapshot/read/commit on the session worker |
| warm batch publication | a warm `BatchTransportState::submit` took ~1us, while queue publication cost ~10us/RPC | retain one Tokio runtime, but publish a warm batch directly under the transport-state lock |
| root sort representation | `ORDER BY` added ~168us here versus ~46us in Go | store row handles and compare chunk cells, not one allocated `Vec<Datum>` key per row; directly stream a single sorted run |
| prepared range execution | prepared Rust gained only ~1.5% over text while Go gained ~14%; the second `EXECUTE` still rebuilt the plan | retain the structurally admitted primary-key range plan and rebind only its parameter bounds, with schema/session/cache gates and ordinary-planner fallback |
| pessimistic read-only begin | forcing optimistic mode recovered about half of the remaining explicit-transaction point-read gap | retain the ordinary transaction at `BEGIN` and promote it only when the first locking statement asks for pessimistic state |
| read resource group | TiKV counted Rust point reads as `priority="unknown"` while Go's default SQL group was `medium` | configure the SQL transaction opener with `default` and stamp the group on Get, BatchGet, Scan, and direct MaxTS contexts |

The original inlining prototype tried to run the eager producer to completion,
which can deadlock after filling its bounded queue.  The shipped design is
different: the TiKV response is lazy and `CopRowStream::next_chunk` pulls it
only when the executor asks, so there is no producer queue to fill.  Large
index lookups still open and consume the lazy response wholly on their worker;
the response never crosses threads.

The sort investigation also produced an important negative result.  Removing
shared-column byte copies and recovering sole `Arc<RwLock<Column>>` ownership
left the isolated ORDER BY cell unchanged at about 1,519 TPS versus 2,147 TPS
for Go.  Those edits were discarded.  Only the row-handle/key-materialization
change remains, because its three-pair ORDER BY ratio moved from about 0.765 to
0.919 under the same alternating harness.

## Verification

Repeatable probes live in the session scratchpad and should be moved into
`rust/scripts/` when M1 lands:

- `plan_cost_probe.sh <rust-port> <go-port> <password>` — runs five one-row
  shapes 3000 times each against both engines and prints us/statement. One row
  scanned means storage work is constant, so the delta is optimizer work.
- `rangeshapes.sh` — sysbench cells isolating each range shape, median of 3,
  Rust and Go alternating inside each sample.

Any Rust-versus-Go number MUST alternate engines inside a sample. The sysbench
ladder runs all Rust legs before all Go legs, which measures Go on a table
every Rust leg has already churned; that ordering made `oltp_read_only` look
like parity (1.11x) when an interleaved probe put the same transaction at
1.40x.

Correctness gates, every milestone:

- `cargo nextest run --workspace` — no new failures.
- `cargo test -p difftest-result-tests --test integration_diff` — the corpus
  ratchet, `KNOWN_DIVERGENCES = 0` over ~9,682 statements. A plan cache that
  returns a stale plan shows up here first.

## Progress

- [x] 2026-08-26: survey complete, measurements recorded, plan written. Preceding
  related fix landed as `c8f67f1a03` (every coprocessor scan runs on a producer
  pool instead of a fresh thread), which removed 36-58us from every scan shape
  and is what left the numbers in the table above.
- [x] 2026-08-26: M1a landed (`4ee77f0109`). Effect within noise; the milestone
  was split because the profile's "33% in planning" turned out to be 31% in the
  two speculative passes and 2% in the third.
- [x] 2026-08-26: M1d landed (`da8da8a0c4`) — global aggregates now choose Go's
  family. Net -2 pre-existing failures.
- [x] 2026-08-26: M1b landed (`0f7953702f`) — the aggregation family is pinned and
  replayed, so a repeated PREPARED statement plans once instead of three
  times. The survey was also corrected: `prepared_path_pins.rs` was already
  doing real plan reuse for join leaves, which the first revision of this
  document missed, and `@@last_plan_from_cache` is a deliberate port of Go's
  observable contract rather than a bug.
- [x] 2026-08-27: reproduced the reported ratios with five alternating
  one-thread pairs: read-only median about 0.800 and read-write about 0.885.
- [x] 2026-08-27: added fail-before/pass-after regressions for cop publication,
  query-worker cop pulls, session-worker transaction state, warm batch
  publication, and sort row-handle comparison.
- [x] 2026-08-27: implemented the five measured boundary repairs and discarded
  the shared-column ownership experiment after a flat three-pair benchmark.
- [x] 2026-08-27: retained a structural primary-key range plan for the four
  sysbench range shapes, with different-bound answer tests and fail-closed
  schema/session/cache gates.
- [x] 2026-08-27: matched Go's lazy pessimistic lifecycle and propagated the
  SQL resource group through every transactional and direct snapshot read.
- [x] 2026-08-27: final alternating benchmark complete. Read-only ratios were
  0.953, 0.960, and 0.949 (median 0.953); the stable read-write pairs were
  0.985 and 0.977. The third read-write pair crossed a machine-wide throughput
  step between legs and was rejected rather than averaged into the result.
- [x] 2026-08-27: Ready validation completed: all named root-cause regressions,
  the five affected crates' combined `cargo check`, and repository `make lint`
  passed. The Bazel prepare gate was not triggered because no Go source,
  module, Bazel, or generated input changed. Workspace `cargo fmt --check`
  still reports the repository's existing formatting drift in untouched Rust
  files; a changed-files-only check found only the pre-existing unindented doc
  comment in `transaction/coordinator/mod.rs`, which this fix does not rewrite.
- [x] 2026-08-27: reopened the sub-1.0 result and decomposed the actual
  prepared workload. Empty transactions and autocommit point reads were
  already faster than Go; prepared `SUM` and `DISTINCT` ranges were the
  concentrated residuals.
- [x] 2026-08-27: sampled prepared `SUM`: 1,373 of 2,515 active cached-range
  samples were in root `HashAgg`, with 1,202 in table-scan chunk delivery.
  The cached plan now retains Go's TiKV partial-SUM/root-StreamAgg boundary for
  rebound ranges wider than one handle.
- [x] 2026-08-27: propagated the rebound range cardinality into cached
  `DISTINCT` HashAgg. This activates the executor's existing 16,384-row serial
  crossover instead of treating a 100-row range as unknown and paying OS
  worker handoffs.
- [x] 2026-08-27: completion benchmark passed. Read-only ratios were 1.028,
  1.028, and 1.042 (median 1.028). The final 10-second disjoint read-write
  pair was 283.58 versus 281.58 TPS (1.007); combined with the reversed first
  round on the same equal-depth tables, Rust completed 4,751 transactions and
  Go completed 4,740 (1.002).
- [x] 2026-08-27: the reopened pass completed the Ready profile. The focused
  prepared-range regression, the affected crates' combined `cargo check`,
  `git diff --check`, and repository `make lint` passed. The executor library
  sweep completed 1,192 tests with seven known aggregate plan-shape failures
  and four ignored tests, improving on the recorded 42-failure baseline with
  no new failure attributable to this pass. The Bazel prepare gate remained
  false because no Go source/import, module, Bazel, generated input, or new
  top-level Go test changed.
  Go 4,740. All reported legs had zero errors.

## Surprises & Discoveries

- `@@last_plan_from_cache` already reports `1` for statements whose plan was
  never cached. The variable is fed by an LRU of KEYS. Any test that trusts it
  today is asserting nothing.
- The node's only true plan reuse is the point-get path, and that is exactly
  the only shape measuring at parity with Go. That correspondence is the
  strongest single piece of evidence that this plan targets the right thing.
- The executor consumes the planner crate for costing, not construction. The
  large `find_best_task` / `cascades_base` machinery is not on the execution
  path, so "reuse the planner's plan" is not available as a shortcut.
- The "33% of a DISTINCT statement is planning" figure covers THREE passes and
  is not evenly split: 1959 samples in the two speculative passes, 120 in the
  third. Removing the cheapest third first bought nothing measurable. Split a
  measured aggregate before scheduling work against it.
- Asking the planner for a cost receipt used to change the plan
  (`derived_output = output_delivered.is_some()`), and no test in the suite
  distinguishes derived mode from top-level mode for the aggregate shapes --
  the suite is equally green with the old coupling restored. The separation is
  reasoned from `derived_column_prune`, not demonstrated.
- A code smell is not a cost. The per-row allocations were named as the cause
  twice on inspection alone; profiling put them an order of magnitude below
  the measured gap. Profile before scheduling a fix against a smell.
- A two-point slope is not a slope. "Row pipeline is 2x Go's" came from two
  cells whose Go half differed by 2us over 90 rows; the correction that
  followed ("the gap is fixed, not per-row") came from two other cells and was
  also wrong. Only the five-point fit separated the two terms -- and both were
  real. Fit over >=4 sizes before attributing a gap to per-row or per-statement
  work.
- A GREEN test can pin a divergence just as easily as a red one. Go's verbose
  EXPLAIN is cheap to ask for and settled two disputed plan expectations here
  in minutes; re-deriving its cost formula from source did not.
- Writes are at parity (1.00x, four writes in one transaction: 0.894ms vs
  0.891ms). Per-RPC we match Go too (our Get 75.2us against Go's own
  `tidb_tikvclient_request_seconds{type="Get"}` 71-73us; our Cop round trip
  ~120us against ~110us; the same ~1.4 cop requests per range statement; TiKV
  reports 30us of processing either way). Nothing on the storage path is worth
  chasing for this gap.
- A synchronous function is not necessarily synchronous work.  Both the cop
  dispatcher and warm batch transport performed a small in-memory publication
  before an asynchronous RPC, but routed that publication through an OS-thread
  command queue and made the query worker wait for the receipt.  Receipts and
  response completion have to be measured separately.
- Lazy iterators have thread affinity even when their constructors are `Send`.
  Opening a TiKV response on one thread and consuming it on another is both a
  performance crossing and a future correctness trap; the large-lookup worker
  therefore owns open and consumption together.
- The prepared/text control was decisive: Rust's prepared range statements
  were only ~1.5% faster than text while Go's were ~14% faster. Identical
  plans and faster TiKV service time ruled out storage; the missing retained
  root plan was the fixed per-statement term.
- TiKV's `priority` metric is resource-control priority, not the legacy
  command-priority enum. A live one-transaction trace showed all ten sysbench
  point reads carrying the configured group, and the TiKV `medium` counter
  advancing by exactly ten.
- The ordinary and cached plans can have the same SQL-level shape but still
  disagree at the reader boundary. Go's `SUM` EXPLAIN was
  `TableRangeScan -> cop StreamAgg -> TableReader -> root StreamAgg`; the
  cached executor instantiated `TableScan -> root HashAgg`. Profiling, not
  the textual root operator name alone, exposed the materialization and
  worker-handoff cost of that missing partial stage.
- An absent cardinality estimate is an execution decision. The cached
  `DISTINCT` path called the same HashAgg implementation as ordinary planning,
  but `None` preserved historical parallel behavior. Passing the exact closed
  integer-handle range bound moved the isolated cell from below Go to 1.074x
  without changing aggregate semantics or session configuration.
- Reusing one mutable table for alternating read-write legs is invalid. Runs
  on the shared hot keys developed approximately one-second lock-resolution
  outliers and collapsed according to order, including Go. Disjoint freshly
  prepared tables per engine removed the outliers; equal version depth and
  reversed engine order are required for a meaningful write comparison.

## Decision Log

- 2026-08-26: **Cache a physical plan, not executors and not the AST.** Go
  caches `base.Plan` and rebuilds its ranges; executors hold per-execution
  mutable state and a bound snapshot, and an AST cache would save only
  parsing, which is not where the time goes.
- 2026-08-26: **M1 before M3.** The single-pass aggregation fix is the largest
  measured item, is independent of the cache, and does not depend on M0's
  answer. Landing it first buys real latency while the harder question is
  still open.
- 2026-08-26: **M0 is a stop-and-revise gate.** The driver may not separate
  decision from construction cleanly. Discovering that after M2 would waste
  the work; discovering it in M0 costs one prototype.
- 2026-08-27: **Remove command queues from request/response critical paths.**
  The retained Tokio runtime remains the I/O owner, while query workers perform
  the short warm publication directly.  This preserves asynchronous network
  progress without paying a second scheduler crossing per RPC.
- 2026-08-27: **Keep only benchmark-proven representation changes.**  The
  shared-column comparison and unique-owner recovery were correct in isolation
  but did not change ORDER BY throughput, so they were removed instead of being
  presented as performance fixes.
- 2026-08-27: **Cache a structural plan, never benchmark text.** Admission is
  expressed in AST/catalog facts (one unpartitioned integer primary-key table,
  exact parameterized bounds, supported root operators), every execute rebinds
  values, and any mismatch falls back to ordinary planning.
- 2026-08-27: **Match Go's transaction lifecycle and request metadata.** A
  read-only pessimistic transaction stays ordinary until it locks, and all SQL
  snapshot requests carry the session resource group; neither optimization is
  a workload exception.
- 2026-08-27: **Preserve the reader/aggregate physical boundary in cached
  plans.** Parameter binding computes a generic closed integer-handle
  cardinality. Multi-row `SUM` offers the existing TiKV partial aggregate and
  folds its result with root StreamAgg; one-row ranges retain Go's root-only
  shape.
- 2026-08-27: **Propagate estimates instead of forcing aggregate mode.** The
  cached `DISTINCT` path supplies its rebound-range estimate to HashAgg and
  lets the executor's shared crossover select serial or parallel execution.
  No workload switch, SQL-text match, or concurrency override is introduced.

## Outcomes & Retrospective

The 2026-08-27 pass converted the priced boundary costs into production
changes and regression receipts, then found the larger fixed term that the
first pass had missed: prepared range statements still rebuilt their complete
root plan. The five-pair baseline reproduced read-only at about 0.800 and
read-write at about 0.885. After the structural plan, transaction-lifecycle,
transport, scan, sort, and request-context repairs, three alternating final
read-only pairs measured 0.949-0.960 (median 0.953); the stable read-write
pairs measured 0.977-0.985. Absolute TPS moved sharply with machine load, so
only adjacent alternating ratios are reported.

The Ready profile passed after the final code changes: targeted regressions
cover each removed scheduler boundary, prepared range rebinding and cache
disablement, lazy pessimistic promotion, resource-group routing, and the sort
representation; the affected crates compile together; and the repository's
mandatory Go lint gate is clean.

The retained plan is deliberately narrow and fail-closed. It recognizes
plan/catalog structure, not sysbench SQL text; rebinds different parameter
bounds; honors the prepared-plan-cache switch, schema identity, bindings,
snapshot/read-staleness state, SQL select limits, and fix controls; and falls
back to ordinary planning on every refusal. The remaining read-only delta is
the already measured several-microsecond incremental explicit-transaction
point-read cost, not a single bypass or workaround left in the workload.

The reopened pass closed the remaining gap at two generic physical seams.
Cached multi-row `SUM` now keeps TiKV's decomposable partial stage and a serial
root final stage; cached `DISTINCT` now carries the rebound range estimate into
the same HashAgg serial/parallel policy ordinary planning uses. Isolated SUM
moved from about 0.82x to 0.991x Go and isolated DISTINCT to 1.074x. More
importantly, the unchanged full workloads now clear the governing goal:
read-only's three-pair median is 1.028x, and the longer disjoint read-write
pair is 1.007x.

Plan revision note (2026-08-27): recorded the measured transaction, scan, RPC,
and sort root fixes; corrected the obsolete conclusion that safe scan inlining
was blocked; and documented the discarded shared-column experiment.

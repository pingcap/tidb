# The node reuses an optimized plan, as Go's plan cache does

Status: in progress (2026-08-26). Keep `Progress`, `Surprises & Discoveries`,
and `Decision Log` current while implementing.

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

- 2026-08-26: survey complete, measurements recorded, plan written. Preceding
  related fix landed as `c8f67f1a03` (every coprocessor scan runs on a producer
  pool instead of a fresh thread), which removed 36-58us from every scan shape
  and is what left the numbers in the table above.
- 2026-08-26: M1a landed (`4ee77f0109`). Effect within noise; the milestone
  was split because the profile's "33% in planning" turned out to be 31% in the
  two speculative passes and 2% in the third.
- 2026-08-26: M1b landed (`0f7953702f`) — the aggregation family is pinned and
  replayed, so a repeated PREPARED statement plans once instead of three
  times. The survey was also corrected: `prepared_path_pins.rs` was already
  doing real plan reuse for join leaves, which the first revision of this
  document missed, and `@@last_plan_from_cache` is a deliberate port of Go's
  observable contract rather than a bug.

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
- Writes are at parity (1.00x, four writes in one transaction: 0.894ms vs
  0.891ms). Per-RPC we match Go too (our Get 75.2us against Go's own
  `tidb_tikvclient_request_seconds{type="Get"}` 71-73us; our Cop round trip
  ~120us against ~110us; the same ~1.4 cop requests per range statement; TiKV
  reports 30us of processing either way). Nothing on the storage path is worth
  chasing for this gap.

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

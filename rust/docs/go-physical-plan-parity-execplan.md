# Make Rust physical planning and execution match Go TiDB

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at the repository root. Repository policy in `AGENTS.md`
has precedence, including the requirement that a Go-to-Rust transcreation claim
cover a complete Go package and all of its production, generated, platform,
test, fixture, and build artifacts.

## Purpose / Big Picture

After this work, the Rust TiDB implementation will reuse the same kind of
general physical plan Go stores in its prepared-plan cache, rebuild only the
parameter-dependent pieces on a cache hit, and enumerate HashAgg and StreamAgg
from one shared child plan. Cached aggregation will no longer contain a
`SUM`-specific range rule, and HashAgg will no longer need a Rust-only
16,384-row serial cutoff because its parallel scheduler will have a lightweight
task handoff comparable to Go's goroutines. Resource-group identity will be
resolved per statement rather than fixed at process construction. The Rust
sort and coprocessor implementations will cover the complete corresponding Go
package contracts, including parallel operation, spill, cancellation, type
coverage, failpoints, and tests.

The observable acceptance result is stronger than plan-shape similarity:
prepared statements with point, range, index, join, index-merge, and DML child
plans return correct results after parameters change; `EXPLAIN` shows the same
reader and aggregation boundaries as Go; dynamic resource groups reach every
KV request; parallel sort and coprocessor paths pass the complete package
inventory; and the paired one-thread sysbench ratios remain at least 1.0 for
both `oltp_read_only` and `oltp_read_write`.

## Progress

- [x] 2026-08-27: user confirmed the performance-preserving implementation
  order: build the shared plan and lightweight scheduler foundations before
  removing Rust-specific performance rules.
- [x] 2026-08-27: inspected Go's cache rebuild, aggregation enumeration,
  HashAgg, transaction resource-group, batch RPC, and sort entry points and
  compared them with the current Rust paths.
- [x] 2026-08-27: froze initial 21-artifact sortexec and 25-artifact copr
  inventories under `rust/docs/parity/`; missing MPP probe, metrics, range
  diagnostics, and parallel worker receipts are explicit blockers.
- [x] 2026-08-27: added recursive cache-rebuild tests for ordinary/reader
  scans, point/batch-point, index merge, index-join inner range, and DML-select
  owned trees. The production prepared-range hit also proves the cached
  template is not mutated across executions.
- [x] 2026-08-27: discovered and selected the existing cloneable
  `tidb_planner::physical::PhysicalPlan` tree as the one representation to
  promote; rejected creation of a second executor-local tree.
- [ ] Complete the existing reusable Rust physical-plan tree's operator and
  parameter-slot coverage. Cache-rebuild variants are present for every Go
  rebuild node, but the planner tree still contains explicit `Todo` operators.
- [x] 2026-08-27: ordinary SELECT planning now builds and costs one shared
  logical/physical tree; executor construction lowers exact aggregation,
  access, join, child-property, and Sort-enforcer receipts.
- [x] 2026-08-27: implemented the recursive cached-plan rebuild visitor over
  ordinary children and reader/index-merge/index-join/DML-owned subtrees.
  Prepared SELECT cache entries retain the shared logical and complete
  physical tree; point/range fast-plan types remain only for Go's separate
  `TryFastPlan` route, not as the ordinary cache representation.
- [x] 2026-08-27: HashAgg and StreamAgg alternatives now share one planned
  child; deleted the whole-select double-planning path, `AggregationChoice`,
  cost-only delivery wrappers, and speculative statistics checkpoints.
- [x] 2026-08-27: deleted cached `SUM && estimated_rows > 1` selection and
  retained the physical aggregation chosen at prepare time.
- [x] 2026-08-27: moved HashAgg work to the persistent executor pool and
  deleted `PIPELINE_MIN_INPUT_ROWS` plus its estimate plumbing.
- [x] 2026-08-27: resource-group identity is statement-scoped and reaches
  snapshot/open-transaction/coprocessor request creation, including statement
  hint overrides and prepared execution.
- [x] 2026-08-27: removed executor-side physical re-planning residue after
  shared-plan wiring: local access/join/aggregation candidates, local logical
  possible-property reconstruction, structural index-join selection, and
  catalog-order index re-enumeration. Exact planner-selected Sort enforcers and
  index ids are lowered instead.
- [x] 2026-08-27: compared disputed plan shapes with targeted Go
  `EXPLAIN FORMAT='brief'` oracles. The nonclustered-primary forced merge
  lowers Go's two Sort enforcers; the TPCC grouped query follows Go's
  HashAgg/IndexHashJoin shape rather than its old Rust-only StreamAgg test.
- [x] 2026-08-27: removed the retired read-tier observability implementation:
  unconditional per-query JSON stderr events, environment-controlled hot-path
  tracing, duplicated physical-plan evidence, transport publication observers,
  and the `Rc<RefCell>` transport-evidence graph. Request DAGs, ranges, retry,
  cancellation, and response ownership remain the authoritative production
  state.
- [x] 2026-08-27: removed configured TopN/LIMIT completion-evidence APIs and
  their diagnostic per-row counters. Behavioral tests now assert bounded heap
  size, stable result order, exact upstream pulls, and once-only close directly.
- [x] 2026-08-27: restored Go's prepared point-plan precedence. The binary
  protocol had retained both the point plan and the general SELECT descriptor,
  then always bound and executed the general descriptor first. Prepared
  sysbench point-select consequently blocked before completing one operation.
  The first successful execution now admits the point plan as a cache miss and
  later executions rebuild its handle as cache hits, matching Go's
  `generateNewPlan`/`TryFastPlan` followed by point-executor reuse.
- [x] 2026-08-27: removed the general prepared-cache self-deadlock. Rust took
  the catalog mutex before building `StmtContext`; context construction takes
  its sequence snapshot through the same catalog and blocked recursively.
  Context/cache-key state is now completed before the catalog guard, and a
  cluster prepared-range regression completes through the shared physical
  tree and ordinary timestamped snapshot.
- [x] 2026-08-27: changed prepared-cache hits from deep-cloning and discarding
  the complete physical tree to Go-compatible in-place recursive rebuild under
  the cache mutex. Parameter/deferred markers remain on the retained tree,
  execution receipts receive marker-free expression clones, and a failed
  rebuild evicts the partially rebuilt entry before ordinary replanning.
- [x] 2026-08-27: removed Rust-only successful snapshot-read receipts from the
  transaction coordinator. Resolved/committed lock sets and failure/publication
  state remain; ordinary point, batch, and scan reads no longer append and
  clone a growing diagnostic vector that Go's `KVSnapshot` does not own.
- [x] 2026-08-27: cached single-DataSource physical plans now instantiate from
  their rebuilt access/aggregation/order receipt without re-running the
  executor driver's outer-join simplifier, join reorderer, predicate-pushdown
  planner, or `RowSource` statistics model. Planner-derived input/output row
  counts are retained with the receipt. Cached join shapes keep their legacy
  predicate-routing walk until the recursive physical join lowerer owns that
  contract.
- [x] 2026-08-27: prepared PointGet now executes the retained planner-built
  plan on its first miss and every later cache hit. The executor-local prepared
  point planner and its duplicate predicate/handle/row-decoder implementation
  were deleted. Execute-time privilege checks, fresh mutable executor state,
  and first-miss/later-hit `@@last_plan_from_cache` semantics remain.
- [x] 2026-08-27: execute-bound parameter constants now expose their installed
  datum to the shared AST ranger and statistics estimator, as Go's
  `ParamMarker.GetUserVar` does. Stock prepared SUM therefore keeps the same
  99-row estimate and root/cop StreamAgg tree as literal SQL instead of
  selecting a full-range root HashAgg.
- [ ] Complete the `pkg/executor/sortexec` package inventory in Rust. The
  parallel fetch/worker/local-merge/coordinated-spill lifecycle and TopN
  workers are active; RankTopN, benchmark, comparison-loop cancellation, and
  upstream failpoint/panic receipts remain.
- [ ] Complete the `pkg/store/copr` package inventory in Rust. Previously
  absent MPP probe, cache metrics, and range diagnostics owners are now
  implemented; remaining integration/test rows are still partial.
- [ ] Run correctness, compatibility, performance, and Ready validation.

## Surprises & Discoveries

- Observation: Go caches a complete `base.Plan` and
  `RebuildPlan4CachedPlan` recursively rebuilds mutable ranges without
  re-running physical optimization. Rust currently caches a separate
  `PreparedRangeSelectPlan` that admits only a two-parameter closed integer
  primary-key `BETWEEN` and four root shapes.
  Evidence: `pkg/planner/core/plan_cache.go::adjustCachedPlan`,
  `pkg/planner/core/plan_cache_rebuild.go::rebuildRange`, and
  `rust/crates/tidb-executor/src/driver/access.rs::build_prepared_range_select_plan`.

- Observation: ordinary Rust aggregation previously planned the complete
  SELECT independently for StreamAgg and HashAgg. The live path now builds one
  logical aggregation and lets shared `find_best_task` enumerate and cost both
  families over it; `build_aggregation` only lowers the selected state layout.
  Evidence: `pkg/planner/core/operator/physicalop/base_physical_agg.go::ExhaustPhysicalPlans4LogicalAggregation`
  and `rust/crates/tidb-executor/src/driver.rs` around `AggregationChoice::Auto`.

- Observation: deleting the 16,384-row Rust cutoff before changing the worker
  model would reintroduce the measured prepared-DISTINCT regression. Go's
  partial/final workers are goroutines; Rust's current path wakes OS-backed
  worker lanes. The scheduler change is therefore a prerequisite, not an
  optional optimization.
  Evidence: `rust/crates/tidb-executor/src/hash_agg/parallel.rs::PIPELINE_MIN_INPUT_ROWS`
  and the paired measurements in `rust/docs/plan-cache-parity-execplan.md`.

- Observation: the Rust transaction opener can carry an arbitrary resource
  group, but the SQL node currently constructs it once with `"default"`.
  Go reads `StmtCtx.ResourceGroupName` for each statement/snapshot.
  Evidence: `rust/crates/tidb-server/src/cluster_session_node/transactions.rs::RealClusterTransactions::new`,
  `pkg/session/txn.go`, and `pkg/executor/builder.go::InitSnapshotWithSessCtx`.

- Observation: repository policy makes “full sort/coprocessor coverage” an
  atomic package claim. A fast-path-only port may remain seed evidence but
  cannot close the milestone.
  Evidence: root `AGENTS.md`, non-negotiable item 6.

- Observation: `rust/crates/tidb-planner/src/physical/mod.rs` owns a
  cloneable closed `PhysicalPlan` enum with selection, projection, hash join,
  sort, limit, table/index scans and readers, Apply, TopN, HashAgg, and
  StreamAgg. The ordinary executor bridge and prepared cache now consume it;
  explicit `Todo` variants still identify unsupported operators.
  Evidence: `PhysicalPlan` at `physical/mod.rs` and
  `tidb-executor/src/driver/planner_bridge.rs`.

- Observation: once ordinary planning used `PhysicalPlan`, executor-side
  “promise” reconstruction became actively dangerous: it could discard a
  selected Sort, choose a different index with the same prefix, or promote an
  unordered access receipt from catalog metadata. Lowering now treats a
  present receipt as authoritative and uses legacy structural checks only for
  SELECT shapes that the shared bridge explicitly declines.
  Evidence: `driver/planner_bridge.rs`, `driver/merge_decision.rs`,
  `driver/index_join_decision.rs`, and the targeted Go oracle tests recorded in
  this plan.

- Observation: Rust's previous `parallelism > 1` sort path drained the entire
  child into one partition and only distributed chunks after EOF. It could
  sort runs concurrently but could not overlap fetch and sort or use the
  parallel spill helper. The replacement uses bounded persistent-pool lanes,
  Go's `maxChunkSize * 30` local batch boundary, worker-local K-way merge, and
  coordinated spill rounds; the old `take_chunks` repartition API was deleted.
  Evidence: `sort::tests::parallel_sort_workers_share_input_and_heap_merge_their_runs`
  and `parallel_sort_spills_worker_rounds_and_final_batches`.

- Observation: the bounded real-TiKV proof tier accumulated a second copy of
  query state for observability: every request cloned plan shape/ranges into
  `RealTiKvQueryPlanEvidence`, and direct coprocessor dispatch maintained an
  `Rc<RefCell>` observer graph plus region-publication vectors even though the
  production consumer had been retired. This was Rust-only work and could
  execute once per physical request.
  Evidence: removed owners in `tidb-exec/src/real_tikv_read.rs` and
  `tidb-distsql/src/cop_paging/direct_unary_query_transport.rs`; the request
  envelope, encoded DAG, request ranges, and scripted client boundaries remain
  covered by focused tests.

- Observation: configured TopN and LIMIT updated counters solely to expose
  immutable completion evidence to tests. The counters did not drive ordering,
  admission, LIMIT termination, or source close. Direct behavioral assertions
  cover those contracts without production accounting.
  Evidence: `tidb-exec/src/configured_topn.rs` and the configured TopN/ordered
  query source tests.

- Observation: the cluster binary-protocol path extracted both cached
  descriptors from `PreparedAst`, but tested `cached_select.is_some()` before
  the already-classified point shape. Because every point SELECT also owns a
  general descriptor, the point plan was unreachable; the focused MaxTS test
  and live prepared sysbench point-select both hung. Gating the general cache
  with the point shape made the existing test complete in 0.02 seconds. A
  second existing test then exposed and pinned Go's first-miss/later-hit
  publication semantics.
  Evidence: `cluster_session_node::execute_general` and
  `point_get_max_ts::{a_prepared_point_get_takes_no_timestamp_either,
  a_prepared_point_get_reuses_the_plan_with_each_executions_handle}`.

- Observation: the generic prepared SELECT did not reach planning at all.
  A one-second process sample showed the test thread blocked in
  `Session::statement_context_ignoring -> sequence_snapshot -> Mutex::lock`
  while `bind_cached_prepared_select` already held that catalog mutex. This
  explains why prepared point-select (while it incorrectly chose the generic
  descriptor) and then prepared sysbench range-select both waited forever.
  Evidence: fail-before stack sample for
  `a_prepared_range_executes_the_general_cached_plan` and its 0.02-second
  pass after reordering context construction.

- Observation: Rust's supposedly general prepared cache rebuilt a deep clone
  of the complete `PhysicalPlan` on every hit and discarded that clone after
  extracting an executor receipt. Go mutates the session-local cached physical
  plan in `RebuildPlan4CachedPlan`. Retaining parameter markers while rebuilding
  the cache-owned tree removes the extra allocation/walk without changing the
  chosen operator shape.
  Evidence: `physical_plan_cache::bind_expression`,
  `PreparedSelectPlan::bind`, and the consecutive-parameter rebuild regression.

- Observation: Rust also retained one successful publication receipt per
  snapshot point/batch/scan read. Go's `KVSnapshot` retains lock-resolution,
  cache, request, and visibility state but no equivalent success-history
  vector. That Rust-only state grew and cloned publication data on the read
  path even though it drove no correctness decision.
  Evidence: pinned client-go `txnkv/txnsnapshot/snapshot.go` and the removed
  `SnapshotReadReceipt` ownership in `tidb-txnkv`.

- Observation: rebuilding the cached Rust physical tree was not sufficient to
  match Go's cache-hit execution boundary. Rust passed the rebuilt receipt back
  through `run_select_traced_with_delivery_choice_inner`, which recomputed
  outer-join simplification, join reorder, predicate distribution, and a second
  executor-local statistics tree before mechanically lowering the selected
  access. Go returns the rebuilt `base.Plan` directly to `executorBuilder.build`.
  A test-only visit receipt failed before the change (one simplifier visit) and
  now proves all four legacy passes remain unvisited for a cached one-leaf plan.
  Evidence: `pkg/planner/core/plan_cache.go::adjustCachedPlan`,
  `pkg/executor/adapter.go::buildExecutor`, and
  `driver::tests::point_get::cached_physical_plan_does_not_rerun_legacy_row_estimation`.

- Observation: Rust had two prepared PointGet implementations after the shared
  planner was wired. PREPARE built and retained the general planner's
  `PreparedPointGetPlan`, but the first EXECUTE ignored it and invoked an
  executor-local planner that rejected every table with any secondary index.
  Stock sysbench always creates `k_1`, so `SELECT c FROM sbtest1 WHERE id=?`
  was refused on every execution and never admitted into the point cache.
  Executing the retained plan on the first miss and marking it ready only
  after success matches Go's one PointGet plan across miss and hits.
  Evidence: removed
  `try_prepared_common_handle_point_get_path`/`run_fast_prepared_point_get*`,
  the exact stock-sysbench regression, and paired alternating measurements:
  read-only improved from 0.798x to 0.855x; read-write from 0.882x to 0.897x.

- Observation: the stock prepared SUM's planner expression carried both the
  current value and its parameter-marker identity, but the executor AST ranger
  passed that `Constant` through the plain-literal evaluator. That evaluator
  correctly refuses parameter constants without an evaluation context, so
  the statistics ranger dropped both BETWEEN bounds, estimated 8,000 logical
  input rows and adjusted the physical scan to 10,000 rows. The shared cost
  search then selected root HashAgg with no cop aggregation. Go evaluates the
  same constant through `ParamMarker.GetUserVar`; using the already-installed
  current datum restores the 99-row estimate and root/cop StreamAgg receipt.
  Evidence: fail-before `(Some(Hash), None)` in
  `prepared_sysbench_sum_retains_gos_stream_aggregation_receipt`, fail-before
  selectivity `0.8` in
  `execute_bound_markers_use_the_same_handle_selectivity_as_literals`, and
  both passing after the general ranger correction.

## Decision Log

- Decision: preserve paired sysbench parity throughout the migration rather
  than delete current safeguards first.
  Rationale: literal early deletion is known to make Rust slower and would
  violate the original user goal. Temporary dual implementations are allowed,
  but the old path must be retired before completion.
  Date/Author: 2026-08-27, Codex with user confirmation.

- Decision: promote the existing `tidb_planner::physical::PhysicalPlan` as the
  sole cache-owned physical-plan tree plus fresh runtime executor
  instantiation, not cached live executor objects and not a new executor-local
  enum. The prepared cache serializes range/expression rebuild of that tree.
  Rationale: executor cursors, chunks, memory trackers, cancellation handles,
  and transaction snapshots are statement-local. Caching them would leak state
  across executions. The cached tree may mutate parameter-derived planning
  state, while runtime cursors and execution state are always rebuilt.
  Date/Author: 2026-08-27, Codex.

- Decision: represent parameters as typed slots in scan/range expressions and
  rebuild them with a recursive visitor.
  Rationale: this preserves Go's “rebuild ranges, do not optimize again”
  contract while supporting table scans, index scans, readers, index joins,
  point/batch point gets, index merge, and DML child plans uniformly.
  Date/Author: 2026-08-27, Codex.

- Decision: do not claim complete sort or coprocessor parity until the package
  inventory receipts have no production, test, fixture, generated, platform,
  or build artifact left unmapped.
  Rationale: required by repository policy and necessary to avoid another
  benchmark-only partial port.
  Date/Author: 2026-08-27, Codex.

- Decision: after ordinary shared-plan wiring, retain direct access/join code
  only as mechanical executor construction or as an explicit unsupported-shape
  fallback. Delete every layer that enumerates, costs, or substitutes a
  physical alternative after a receipt exists.
  Rationale: Go separates optimizer selection from executor building. Exact
  receipt lowering preserves that boundary; local candidate recovery can make
  EXPLAIN and execution disagree with the plan Go selected.

- Decision: a prepared statement whose Go optimizer result is a point plan
  must not be displaced by Rust's generic cached SELECT descriptor. Admit the
  precompiled immutable point descriptor only after its first successful
  execution so `@@last_plan_from_cache` remains false on the miss and true on
  subsequent rebuilt-handle executions.
  Rationale: this preserves both the hot point-executor route and Go's
  externally visible cache state without adding a workload-specific bypass.
  Date/Author: 2026-08-27, Codex.

- Decision: construct all plan-cache session context and environment state
  before acquiring the catalog guard passed into planner binding.
  Rationale: Go's planning context and infoschema snapshot are separately
  owned; Rust must preserve that ordering when both views share one catalog
  mutex. Holding the guard while asking the session to snapshot sequences is
  necessarily recursive.
  Date/Author: 2026-08-27, Codex.

- Decision: retain parameter and deferred-expression identity on the
  cache-owned tree, materialize marker-free clones only when lowering a
  statement execution, and evict the cache entry if in-place rebuild fails.
  Rationale: later executions need the original binding source, executors must
  not attempt session-parameter evaluation, and a failed recursive rebuild may
  otherwise leave mixed old/new ranges in the retained tree.
  Date/Author: 2026-08-27, Codex.

- Decision: tests must observe executor and transport behavior at their public
  boundaries instead of requiring a production-only evidence mirror.
  Rationale: Go does not duplicate every plan/request into a test receipt, and
  the mirror imposed allocation, cloning, callback, and per-row counter costs
  on ordinary execution. Scripted transports and row sources can count calls
  without changing production state.
  Date/Author: 2026-08-27, Codex.

- Decision: the PREPARE-time planner owns PointGet shape and access-path
  selection. EXECUTE may bind current parameter values and instantiate fresh
  runtime state, but it must not invoke a second executor-local point planner.
  Rationale: Go uses the same planner-built PointGet on its first cache miss
  and later hits. The duplicated Rust planner had narrower, workload-breaking
  admission rules and made cache readiness depend on those unrelated rules.
  Date/Author: 2026-08-27, Codex.

## Outcomes & Retrospective

Work is in progress. After restoring point-plan precedence and removing the
catalog-lock recursion, a one-sample root smoke measured Rust/Go ratios of
1.095 for read-only and 0.827 for read-write. A decomposed read-write run then
showed Rust already faster for the no-read write path, isolating the remaining
gap to read work. After removing successful-read receipts and changing cached
physical rebuild to in-place ownership, one immediate diagnostic sample moved
the all-query ratio from 0.792 to 0.933 and the point-read ratio from 0.945 to
1.014; the range control was noisy and these single samples are diagnostic,
not acceptance evidence. A fresh three-sample alternating run before removing
the duplicate first-execute point planner measured read-only Rust/Go medians
of 360.43/451.39 TPS (0.798x) and read-write 249.66/283.15 TPS (0.882x).
After the retained planner-built PointGet became the only point implementation,
the same harness measured 403.71/472.00 TPS (0.855x) and 255.89/285.32 TPS
(0.897x), all with zero SQL errors. This is a material root fix but not yet the
1.0 acceptance result. The next fresh alternating range split isolated the
stock SUM root mismatch: before the parameter-ranger fix Rust/Go SUM medians
were 3,054.54/4,572.17 TPS (0.668x); afterward they were 4,467.34/4,359.14 TPS
(1.025x), with zero errors. The same post-fix run measured simple range at
4,149.08/4,342.42 (0.955x), ordered range at 3,210.28/3,533.63 (0.909x), and
distinct ordered range at 2,129.20/2,758.29 (0.772x). DISTINCT/ORDER lowering
is now the largest remaining read-only target.

## Context and Orientation

The Go prepared cache retrieves `PlanCacheValue.Plan` in
`pkg/planner/core/plan_cache.go`. On a hit, `adjustCachedPlan` calls
`RebuildPlan4CachedPlan`, which walks physical nodes in
`pkg/planner/core/plan_cache_rebuild.go`. It rebuilds ranges for table scans,
index scans, table/index/index-lookup readers, index joins, point and batch
point gets, index merge readers, and the select children of insert, update,
and delete plans. It fails closed when range conversion changes a value or the
statement disables cache reuse.

Rust prepared statements are owned by
`rust/crates/tidb-session/src/prepared_ast.rs` and dispatched through
`rust/crates/tidb-server/src/cluster_session_node/mod.rs`. Ordinary SELECT
planning builds `tidb_planner::physical::PhysicalPlan`; prepared cache entries
retain that tree and recursively rebuild its parameter-dependent ranges.
`tidb-executor/src/driver/planner_bridge.rs` converts the tree into stable
receipts, and the executor driver instantiates fresh runtime state from them.

In this plan, a “physical-plan tree” means a reusable value describing
chosen operators, schemas, access paths, pushed predicates and aggregates,
required ordering, estimates, and typed parameter slots. It contains no open
storage cursor or executor runtime state. A prepared cache owns and serializes
its tree while rebuild mutates only parameter-derived constants and ranges.
“Instantiation” means turning that tree into fresh `Box<dyn Executor>` objects
for one statement. “Rebuild” means binding current parameter values into typed
slots and deriving ranges without changing access path, join order,
aggregation family, or reader boundary.

Hash aggregation lives in `rust/crates/tidb-executor/src/hash_agg.rs` and
`rust/crates/tidb-executor/src/hash_agg/parallel.rs`. Go's counterpart is
`pkg/executor/aggregate`. Sort ownership is split between
`rust/crates/tidb-executor/src/sort.rs`, `sort_partition.rs`, and `topn.rs`;
the complete Go source package is `pkg/executor/sortexec`. Coprocessor planning,
dispatch, paging, retry, and decoding span `rust/crates/tidb-distsql`,
`rust/crates/tidb-exec`, and `rust/crates/tidb-txnkv`; the complete Go package
claim is `pkg/store/copr` plus its build and fixture inputs.

The worktree already contains the preceding performance phase as uncommitted
Rust changes. Preserve those changes. Do not reset, reformat unrelated files,
or hand-edit generated artifacts.

## Plan of Work

Milestone 0 creates two inventory receipts under `rust/docs/parity/`: one for
every tracked file in `pkg/executor/sortexec`, and one for every tracked file
in `pkg/store/copr`. Each row records the Rust owner, disposition (implemented,
not applicable with reason, or missing), and validation. This milestone also
adds behavior-first failing tests to the nearest existing Rust prepared-cache,
aggregation, resource-group, sort, and coprocessor suites. No new top-level Go
test is needed.

Milestone 1 completes and promotes the physical-plan module already present at
`rust/crates/tidb-planner/src/physical/mod.rs`. Extend its closed enum to cover
the executor nodes still represented by `Todo`, and add the parameterized
range source that its table and index scan variants currently lack. A plan node
owns output schema, estimated rows, delivered ordering, and children. Scan
variants own the selected table/index identity, pushed predicates, projection,
and a parameterized range expression. Point, batch point, reader, join,
index-merge, aggregation, sort, TopN, projection, selection, limit, and DML-root
variants are required before the old narrow cache is retired.

Milestone 2 separates planning from executor instantiation. Existing planner
helpers return a physical plan plus cost/delivery metadata; one lowering
visitor creates runtime executors. Aggregation enumeration obtains the child
logical/physical candidates once, applies the different required properties
for StreamAgg and HashAgg, and costs both without rebuilding unrelated access
and join decisions. The temporary direct-executor path remains available
behind an internal comparison test until plans and results match, then is
deleted.

Milestone 3 stores the general physical plan in the prepared statement cache.
A recursive rebuild visitor binds parameters and recomputes ranges for every
Go-supported cache node. Rebuild mutates the cache-owned tree under its mutex;
a typed refusal evicts that entry before ordinary replanning so a partial
rebuild is never reused. The execution route instantiates statement-local
state, records metadata-lock tables, and preserves all session invalidation
gates. Delete `PreparedRangeSelectPlan`,
`PreparedRangeSelectRoot`, and their server/session special route after the
general tests pass.

Milestone 4 generalizes aggregate splitting. Partial/final aggregate
descriptors are derived from aggregate kind, distinct arguments, grouping
keys, ordering, target task, and storage capability, following Go's
`BuildFinalModeAggregation` and pushdown checks. Cached plans retain the chosen
partial/final boundary just like any other physical node. Delete
`pushes_partial_aggregate` and every row-count-based SUM special case.

Milestone 5 changes parallel aggregation scheduling. Persistent executor task
workers receive bounded work through a non-blocking queue and remain alive
across statements, with cancellation, memory accounting, panic propagation,
and deterministic merge receipts. Demonstrate that a 100-row aggregation no
longer pays OS wakeup-scale fixed cost, then delete
`PIPELINE_MIN_INPUT_ROWS` and make eligibility match Go's supported-function
and concurrency rules. Preserve serial fallback only where Go sets
`IsUnparallelExec` or where exact Rust numeric semantics cannot be merged; any
such remaining refusal must be recorded as a parity gap rather than hidden.

Milestone 6 makes resource groups statement-scoped. Add the resolved group to
the Rust statement context and pass it into transaction/snapshot acquisition,
coprocessor request builders, retries, lock resolution, pessimistic locking,
prewrite, commit, and cleanup. Hints or session changes affect the next
statement without rebuilding process capabilities. Internal clients remain
independently configurable.

Milestone 7 completes the sort and coprocessor inventories. For sort, port the
complete parallel fetcher/worker/result lifecycle, external spill and heap
merge, TopN spill, memory/disk accounting, kill checks, failpoint behavior,
and all supported comparison types. For coprocessor, close every inventory row
across task building, region splitting, ordered/unordered response delivery,
paging, store batching, retry/backoff, lock handling, runtime statistics,
resource control, all request/response encodings, and tests. A row marked not
applicable must explain the native Rust substitute and prove the same external
contract.

Milestone 8 removes migration scaffolding, runs the complete package and
workspace gates, performs paired alternating benchmarks against the Go server,
and updates both inventory receipts and this retrospective.

## Concrete Steps

Run all commands from `/Users/qiliu/projects/tidb` unless a step says otherwise.

Inventory the complete Go packages and current Rust owners:

    git ls-files pkg/executor/sortexec > /private/tmp/tidb-sortexec-files.txt
    git ls-files pkg/store/copr > /private/tmp/tidb-copr-files.txt
    rg --files rust/crates/tidb-executor rust/crates/tidb-distsql rust/crates/tidb-exec rust/crates/tidb-txnkv | sort

Inspect the local-change prerequisites before every build phase:

    git status --short
    git diff --name-status
    git diff --name-status --cached
    git ls-files --others --exclude-standard
    git diff -U0 -- '*.go'
    git diff -U0 --cached -- '*.go'

The present change set touches Rust only, so `make bazel_prepare` is not
required unless later work changes a Go source/import, Go module, Bazel file,
generated input, or adds a top-level Go test.

Use WIP validation after each small Rust edit:

    cd rust
    cargo test -p tidb-executor <targeted_test_name> --lib
    cargo test -p tidb-session <targeted_test_name> --lib
    cargo test -p tidb-txnkv <targeted_test_name>
    cargo check -p tidb-executor -p tidb-session -p tidb-server -p tidb-exec -p tidb-distsql -p tidb-txnkv

Before running a Rust package test, apply the failpoint decision workflow in
`.agents/skills/tidb-failpoint-test-runner`. Do not run a broad suite merely to
discover whether failpoints are needed.

At completion, run the Ready profile and report exact results:

    cd rust
    cargo nextest run --workspace
    cargo test -p difftest-result-tests --test integration_diff
    cargo test -p tidb-executor --lib
    cargo test -p tidb-session --lib
    cargo test -p tidb-txnkv
    cd ..
    make lint
    git diff --check

Run paired benchmarks only on isolated disposable databases, alternating Go
and Rust within every pair. Use the same TiKV/PD cluster, table cardinality,
thread count, prepared-statement mode, duration, and version depth. Reject a
pair when machine-wide throughput changes between legs. Acceptance is a
read-only median ratio at least 1.0 and a clean equal-depth read-write ratio at
least 1.0, with zero SQL errors.

## Validation and Acceptance

Every bug-fix milestone needs a fail-before/pass-after receipt. Plan-cache
tests must execute the same prepared statement with different parameter types
and values, assert `@@last_plan_from_cache`, compare answers with an uncached
execution, and exercise DDL/session invalidation. Join and index tests must
assert that access path and join order do not change on cache hits while ranges
do.

Aggregation tests must compare Rust and Go `EXPLAIN` operator families and
task boundaries for one-row and multi-row SUM, COUNT, AVG, MIN/MAX, grouped,
distinct, and ordered aggregate cases. No test may assert an implementation
special case that Go does not expose.

Scheduler tests must prove that more than one worker executes when Go would
parallelize, that concurrency 1/1 remains serial, cancellation terminates all
workers, panic/error propagation is deterministic, memory limits still cancel
or spill, and results equal the serial path bit-for-bit where SQL semantics
require it. A microbenchmark must show that 100-row execution no longer needs
the removed cutoff to beat the serial path's fixed cost.

Resource-group tests must change the group between consecutive statements and
capture the protobuf context for point get, scan, coprocessor, lock resolution,
pessimistic lock, prewrite, and commit. The second statement must carry only
the second group. Retry attempts must preserve the initiating statement's
group even if the session changes concurrently.

Sort and coprocessor completion requires every inventory row to be resolved,
all original Go behavioral fixtures or their native Rust equivalents to pass,
and no package-level claim while missing rows remain.

The final difftest corpus must introduce zero new divergences. Known baseline
failures must be listed by exact test name and compared with the pre-change
receipt; failure counts alone are insufficient.

## Idempotence and Recovery

Inventory generation and tests are safe to rerun. Physical plans are additive
until the migration tests establish equivalence; keep the old direct executor
builder callable internally during that period. If a new plan variant is not
yet lowerable, return a typed refusal to the old planner rather than executing
an incomplete plan. Do not silently fall back after a cache hit has begun,
because statement snapshot and side-effect boundaries may already have moved.

Do not use `git reset`, `git checkout --`, or broad deletion commands. The
worktree contains the preceding performance work. If an experiment is
rejected, remove only its exact newly added symbols/files with `apply_patch`
and record the rejection here.

Benchmark clusters and databases must use task-specific names. Stop processes,
drop only those exact databases, and remove only the exact disposable TiUP
directory after validation.

## Artifacts and Notes

The preceding phase's measurements and profiles are in
`rust/docs/plan-cache-parity-execplan.md`. This plan owns all work after the
user requested full parity for the five documented gaps.

Expected permanent receipts are:

    rust/docs/parity/sortexec-package-inventory.md
    rust/docs/parity/copr-package-inventory.md

Add concise command results and benchmark pair tables here as milestones
complete. Do not paste full logs.

## Interfaces and Dependencies

The existing `tidb_planner::physical::PhysicalPlan` module should expose a
serialized cached-plan rebuild and statement-local instantiation boundary
similar to:

    pub(crate) trait RebuildCachedPlan {
        fn rebuild_in_place(&mut self, parameters: &[Datum], context: &RebuildContext)
            -> Result<(), CacheRefusal>;
    }

    pub(crate) trait InstantiatePhysicalPlan {
        fn instantiate(&self, context: &mut ExecutionContext)
            -> Result<Box<dyn Executor>, DriverError>;
    }

Exact names may change to match package conventions, but the separation among
cache-owned planning state, bound parameter values, and statement-local
executor state is mandatory.

The scheduler must use existing repository dependencies where possible. Do not
add a new runtime or queue crate without first proving that the existing
worker-pool/channel facilities cannot provide bounded, cancellation-aware,
panic-safe task execution.

Plan revision note (2026-08-27): created after the user confirmed the
performance-preserving route to full Go parity across plan cache, aggregation,
parallel execution, resource groups, sort, and coprocessor packages.

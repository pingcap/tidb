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
- [x] 2026-08-27: HashAgg execution now receives Go's resolved statement-local
  partial/final worker counts. The generic expression-sysvar view deliberately
  excludes these session variables and previously made production execution
  silently use the 5/5 defaults even while SQL reported 1/1.
- [x] 2026-08-27: removed Rust's N*M HashAgg shuffle channels and final-result
  message protocol. Partial tasks return their owned, already-partitioned maps;
  after all partial receipts arrive, one persistent-pool merge task runs per
  final bucket, preserving Go's partial-worker barrier and N-to-M partitioning.
- [x] 2026-08-27: removed production-only HashAgg worker-id/mutex/dispatch
  diagnostics and concurrency overrides; equivalent observations now compile
  only in unit tests and drive no production allocation or synchronization.
- [x] 2026-08-27: made Rust's 256 parallel-spill chunks lazy at the actual
  spill transition, matching Go's `prepareForSpill`. Parallel DISTINCT now
  constructs no spill-partition owner because Go's spill gate rejects
  DISTINCT before execution.
- [x] 2026-08-27: removed the parallel Sort row-copy round trip. Worker-local
  runs now retain fetched chunks and merge only `(chunk, row)` cursors, as
  Go's `[]chunk.Row` does; the copied `OwnedRow` vector and reconstructed
  output chunks are gone from the unspilled path.
- [x] 2026-08-27: fixed synchronous unordered coprocessor paging so one
  logical task owns at most one ready-queue token. A continuation previously
  enqueued the same task before and after its synchronous send, then panicked
  when terminal close removed both entries. BatchCommands remains the
  production-first path.
- [x] 2026-08-27: made HashAgg partial-worker admission work-driven. The
  configured partial/final concurrency and round-robin assignment are
  unchanged, but a persistent-pool lane is submitted only when it receives a
  chunk, and zero/one active partial lane no longer submits no-op final merges.
- [x] 2026-08-27: retained the shared planner's reader-local direct-column
  projection in the access receipt and lowered it to coprocessor
  `output_offsets` for clean table scans. Ordinary and recursively rebuilt
  prepared ranges now receive narrow remote rows without constructing a
  duplicate root `ProjectionExec`; dirty/staged scans fail closed to the local
  projection path.
- [x] 2026-08-27: removed the Rust-only second completion-notifier loop from
  direct coprocessor reads. One response-owned `CompletionRunLoop` now drives
  every pending BatchCommands region, and unordered delivery scans the
  completed attempts after each callback batch. Region rebuild, admission,
  retry, cancellation, and bounded-window regressions pass.
- [x] 2026-08-27: removed `CopScanSource`'s Rust-only node-lifetime request
  history and served/refused/row counters. Production scans no longer format
  every DAG, append it under a shared mutex, or update a shared atomic for
  every response chunk; wire-shape tests observe the decoded fake-region
  request instead.
- [x] 2026-08-27: removed the table reader's Rust-only second small-chunk
  threshold. `SelectResponseIter` already owns Go `readFromChunk`'s 80%
  reuse/coalescing decision, so every completed exact-width response batch now
  moves into the executor output instead of copying small range responses a
  second time.
- [x] 2026-08-27: routed each executor's current `RequiredRows` through the
  coprocessor stream into `SelectResponseIter` and deleted Rust's hardcoded
  32,768-row table / 1,024-row index decoder sizes. Index lookup now caps each
  decoder pull at `MaxChunkSize` and loops to fill its growing handle task, as
  Go's `SetRequiredRows`/`extractTaskHandles` does. The duplicate index-reader
  80% and partial-aggregate 75% completion policies are gone; completed
  decoder batches cross both boundaries unchanged.
- [x] 2026-08-27: made the retained BatchCommands entry carry either Go's
  synchronous response-channel completion or its asynchronous callback
  completion. One-region ordinary cop reads now use the synchronous
  `SendRequest` shape; the callback run loop remains only for the bounded
  multi-region overlap path that still lacks Go's cop worker pool.
- [x] 2026-08-27: moved every typed transaction BatchCommands entry from the
  asynchronous callback completion to Go's synchronous `SendRequest`
  response-channel completion. Multi-region batches still overlap because
  their independently published pending requests are collected afterward.
- [x] 2026-08-27: deleted the unused reusable-MaxTS transaction alternative.
  Production already follows Go's statement-declared direct MaxTS snapshot;
  the dead trait seam, real/mock implementations, and lower transaction
  constructors had no callers and retained obsolete worker-era ownership.
- [x] 2026-08-27: made direct and prepared MaxTS classification consume the
  existing effective `SET_VAR(tidb_opt_fix_control=...)` authority. Fix 52592
  can therefore disable PointGet before snapshot declaration exactly as it
  does during physical planning; persistent, statement-local, invalid-first,
  and prepared cases share one first-hint-wins rule.
- [x] 2026-08-28: removed the executor-local YCSB-E `LIMIT 1` clustered-range
  shortcut and the older duplicate PointGet plan/EXPLAIN builder. Go has no
  direct range-seek policy for that shape, and Rust's shared planner already
  owns PointGet/BatchPointGet and ordinary Limit/TableReader construction.
  The retained SQL tests now exercise that single planner authority.
- [x] 2026-08-28: removed the disconnected session-local prepared PointGet
  cache state and cached-execution dispatch branch. `PreparedGeneral` now
  solely owns the protocol plan-cache hit state, while `PreparedAst` retains
  only Go's immutable parsed AST plus the planner-built PointGet/general
  SELECT descriptors handed to that protocol cache.
- [x] 2026-08-28: made the retained prepared PointGet plan own both its bound
  execution and Go's `noSecondRead` timestamp policy. Binary EXECUTE no longer
  rebuilds and discards the point-plan matcher before binding the cached plan;
  secondary-unique double reads remain reusable but now refuse MaxTS exactly
  as Go does. The two obsolete prepared read-shape classifiers were deleted.
- [x] 2026-08-28: made binary PREPARE retain and route the one parsed AST it
  already owns. The cluster protocol no longer reparses every DML and SELECT
  after `Session::prepare_ast`, matching Go's single `PlanCacheStmt.PreparedAst`
  authority and giving prepared DML lowering that same immutable input.
- [x] 2026-08-28: replaced the per-EXECUTE prepared UPDATE matcher with one
  retained `tryUpdatePointPlan`-style descriptor. Clustered-handle pins,
  residual predicates, target offsets, and assignment programs are lowered at
  PREPARE; EXECUTE only rebuilds values and fresh mutation state. The old fast
  UPDATE dispatcher, structural matcher, and duplicate assignment evaluator
  were deleted, and first-miss/later-hit reporting now matches Go.
- [x] 2026-08-28: folded stock sysbench's explicit-column one-row INSERT into
  the same retained DML plan authority. PREPARE now fixes its table identity,
  marker layout, target columns, and field types; EXECUTE binds values and
  constructs only fresh row/mutation state. The executor-local fast INSERT
  privilege probe and per-execution AST/catalog matcher were deleted. The
  retained path preserves secondary-index, auto-id, memory-accounting,
  bad-NULL, duplicate-warning, and first-miss/later-hit semantics.
- [x] 2026-08-28: retained stock sysbench's clustered point DELETE at PREPARE
  time and made UPDATE/DELETE share one target, handle, residual-predicate,
  schema-invalidation, and execute-time binding program. The second DELETE
  execution now reports a plan-cache hit, while the mutation keeps ordinary
  secondary-index, foreign-key, memory-accounting, and Go SELECT+DELETE
  privilege behavior.
- [x] 2026-08-28: removed the cached SELECT execution's second deep clone at
  the cluster statement-retry boundary. The bound AST and physical receipt are
  borrowed for every attempt, and `PreparedSelectExecution` is deliberately
  non-`Clone` so this allocation cannot return unnoticed.
- [x] 2026-08-28: made prepared SELECT cache hits rebuild before constructing
  a planner `StmtContext`. Only a real miss takes the catalog-backed sequence
  and decode-key snapshots needed by physical enumeration; a hit builds the
  one runtime statement context Go resets for execution.
- [x] 2026-08-28: moved execute-time marker values onto the SELECT retained
  beside the cached physical tree. Hits mutate only marker datums and ranges;
  the separately owned bound `SelectStmt` and lowering receipt were removed
  from `PreparedSelectExecution`, and a generation lease keeps retry/concurrent
  admission from mixing parameter sets.
- [x] 2026-08-28: memoized the prepared-cache environment against the session
  variable, pushdown-blacklist, transaction, and autocommit generations. A
  cache hit now borrows the same typed environment Go reads from `SessionVars`
  instead of cloning eight system-variable strings before every lookup.
- [x] 2026-08-28: folded prepared-cache admission for SELECT limit, snapshot,
  and read staleness into that same generation-keyed environment. Prepared
  SELECT and PointGet reuse no longer repeat three owned system-variable
  lookups on every hit; an inadmissible generation caches a typed refusal.
- [x] 2026-08-28: replaced the statement context's eager eight-entry
  password-validation GLOBAL-variable map with Go's live
  `SessionVars.GlobalVarsAccessor` shape. Ordinary SELECT and DML no longer
  read, allocate, and clone password-policy strings they never evaluate.
- [x] 2026-08-28: retained the session's parsed time zone in the
  generation-keyed statement-variable snapshot. Prepared execution now clones
  one typed `SessionTimeZone`, matching Go's typed `SessionVars.TimeZone`,
  instead of re-reading `time_zone`, resolving `SYSTEM`, and parsing a named
  zone at each statement-context construction.
- [x] 2026-08-28: replaced the statement context's eagerly rendered
  `TIDB_VERSION()` string with shared typed `VersionInfo`, and deleted the
  planner `FromScope`'s eagerly computed identity-length field. Ordinary
  statements no longer format the multi-line server identity; the builtin
  formats it only when Go's expression build/evaluation boundaries require it.
- [x] 2026-08-28: made both pessimistic point-lock classifiers consume the
  retained prepared AST plus its parameter slice. The prepared-only wrapper
  and its full-tree clone/bind pass were removed; writes and locking reads now
  resolve marker values through the same point-key walker.
- [x] 2026-08-28: retained the exact statement memory tracker and chunk policy
  used by execution as the row/cursor result authority. Rust no longer creates
  a second tracker after execution or reconstructs a prepared statement's
  authority from session variables after `SET_VAR` restoration; the next
  statement boundary releases only the session's reference, matching Go's
  retained `SessionVars.StmtCtx` ownership.
- [x] 2026-08-28: replaced Rust's string-backed autocommit checks with the
  typed session status Go keeps as `ServerStatusAutocommit`. Session SET,
  statement-overlay restore, inherited GLOBAL defaults, prepared-cache keys,
  transaction admission, wire status, and process-list status now share that
  one field; the obsolete always-autocommit process-state renderer is gone.
- [x] 2026-08-28: made an empty statement-variable restore a true no-op.
  Ordinary statements no longer advance the session-variable generation and
  discard the prepared-cache environment when no `SET_VAR` overlay existed;
  a real overlay or SET continues to invalidate the generation-keyed image.
- [x] 2026-08-28: replaced the protocol hot path's owned sysvar reads with a
  general borrowed `system_value` view and retained Go's typed
  `SessionVars.MaxAllowedPacket`. Wait timeout and client/result charset reads
  no longer clone backing strings; packet and builtin consumers no longer
  look up and parse max packet size independently.
- [x] 2026-08-28: retained Go's typed
  `SessionVars.EnablePreparedPlanCache` across default construction, inherited
  GLOBAL state, SET, and statement-scoped restore. Deleted the session-local
  string lookup, and made PointGet, DML, and SELECT reuse consult the same
  field. The previously missing PointGet disable gate is now enforced.
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

- Observation: even after the retained physical tree was rebuilt in place,
  the cluster boundary cloned `PreparedSelectExecution` before every attempt.
  That recursively copied its bound `SelectStmt`, expressions, hints, and
  receipt. Borrowing the same immutable execution across retries removes the
  copy and makes the type non-`Clone`; the source regression failed before and
  passes after. In isolation this cleanup did not move the simple-range median
  beyond run noise (3,308.85 Rust versus 3,436.54 Go TPS, 0.963x), so it is a
  parity cleanup rather than the remaining throughput root.

- Observation: `bind_cached_prepared_select` built a complete planner
  `StmtContext` before it knew whether the retained physical tree was a cache
  hit, although `CachedSelectPlan::bind` consumes only parameter values. The
  execution path then built a second context for the actual executor. Probing
  and rebuilding the entry first makes context construction miss-only. The
  paired simple-range Rust median rose from 3,308.85 to 3,374.98 TPS while the
  paired Go median was 3,459.09 (0.976x), and the new profile contains only the
  runtime context beneath `execute_cached_prepared_select`.

- Observation: the cache-hit binder still cloned `PreparedAst`, walked every
  marker, and owned the resulting `SelectStmt` separately from the cached
  planner tree. Go mutates the marker values on its session-local cached tree
  and builds the executor while that tree remains authoritative. Rust now
  does the same under the per-entry mutex; consecutive range, ORDER, DISTINCT,
  SUM, grouped-aggregate, join, and remote-scan parameter changes pass. The
  simple-range median remained 3,318.80 Rust versus 3,404.35 Go TPS (0.975x),
  confirming that this ownership cleanup is not the remaining RPC-dominated
  throughput root.

- Observation: after the retained tree and execution lease became single
  authorities, `bind_cached_prepared_select` still reconstructed the cache
  environment on every EXECUTE by owning `sql_mode`, time zone, charset,
  collation, partition-prune mode, read engines, SELECT limit, and stats-cache
  policy strings. Go keeps these as typed `SessionVars` fields and the cache
  key reads them without rebuilding an owned object. Reusing one typed Rust
  environment until any relevant generation changes moved the isolated
  simple-range median to 3,388.68 Rust versus 3,447.17 Go TPS (0.983x). The
  stock read-only median moved from 0.968x to 489.14/499.09 TPS (0.980x), and
  read-write measured 312.01/307.26 TPS (1.015x); the isolated SUM shape was
  3,449.56/3,543.16 TPS (0.974x), all with zero errors. A post-change SUM
  profile confirms one pushed partial StreamAgg and one root final StreamAgg;
  its remaining local overhead is statement-context memory/tracker setup and
  teardown, not aggregation re-planning.

- Observation: after the typed environment was cached, the SELECT and
  PointGet binders still re-read SELECT limit, snapshot, and read-staleness
  strings before consulting it. Go's reuse decision consumes the current
  `SessionVars`/plan-cache key state rather than rebuilding an independent
  string gate at each binder. Caching either the typed environment or a typed
  refusal for the current variable/blacklist/transaction generation deletes
  both copies. In one-cluster exact A/B testing, stock read-only medians were
  503.18 TPS candidate, 502.73 TPS at exact baseline `8366ff70bd`, and 476.68
  TPS Go; stock read-write medians were 246.08, 233.27, and 218.83 TPS. The
  read-write samples were visibly noisy, but both candidate medians remained
  above Go and every leg reported zero SQL errors.
  Evidence: fail-before/pass-after
  `cached_select_key_reuses_the_typed_session_environment`, environment reuse
  and refusal tests, session/server checks, release build, and the alternating
  candidate/baseline/Go benchmark receipt.

- Observation: Rust eagerly read every `validate_password.*` GLOBAL variable
  and built a `HashMap<String, String>` while constructing every query and DML
  context. Go's `builtinValidatePasswordStrengthSig` instead retains
  `SessionVarsPropReader` and consults `SessionVars.GlobalVarsAccessor` only
  when that builtin evaluates. Besides unconditional work, the Rust snapshot
  was observably stale when a peer changed a GLOBAL after context creation.
  The replacement holds one shared live accessor. An exact one-cluster A/B
  against `4bb1933dbc` measured stock read-only medians of 499.68 TPS
  candidate, 499.52 baseline, and 471.12 Go; read-write medians were 253.04,
  222.72, and 212.12 TPS. Read-write variance was high, so the semantic
  fail-before/pass-after and deleted unconditional ownership—not that noisy
  delta—are the acceptance evidence. Every benchmark leg reported zero SQL
  errors.
  Evidence: `statement_context_reads_global_sysvars_through_the_live_accessor`,
  all 29 global-variable tests, the password-policy regression,
  executor/session/server checks, release build, and the alternating exact
  A/B receipt.

- Observation: Rust's prepared path repeatedly resolved `time_zone` from its
  string system variable while Go retains a parsed `*time.Location` on
  `SessionVars`. The generation-keyed Rust statement-variable snapshot already
  had the correct invalidation boundary, so it now owns the typed zone and all
  consumers clone that value. Exact one-cluster A/B medians against
  `6b16b316fe` were 508.21 versus 503.96 TPS for stock read-only, with Go at
  474.78 TPS. Read-write medians were 268.95, 224.44, and 224.46 TPS, but those
  samples were highly variable; the three paired read-only legs were each
  positive and every benchmark leg reported zero SQL errors.
  Evidence: fail-before/pass-after
  `prepared_execution_reuses_the_typed_session_time_zone`, the timestamp
  time-zone regression, session/server checks, release build, and the
  alternating exact A/B receipt.

- Observation: Rust formatted the complete `TIDB_VERSION()` identity while
  building every statement context, then eagerly copied its rendered length
  into every `FromScope`. Go keeps immutable process version fields and calls
  `printer.GetTiDBInfo()` only from the `TIDB_VERSION()` function's build and
  evaluation paths. Rust now shares typed `VersionInfo` through the context,
  and its resolver asks for the rendered length only if that builtin is
  present. Exact one-cluster A/B medians against `c7d68b9eab` were 501.40
  versus 498.92 TPS for stock read-only, with Go at 470.74 TPS. Read-write
  medians were 240.62, 219.57, and 210.91 TPS but remained highly variable;
  every benchmark leg reported zero SQL errors.
  Evidence: fail-before/pass-after
  `statement_context_reuses_the_typed_tidb_identity`, the shared-identity
  mutation test, both `TIDB_VERSION()` result/metadata regressions,
  executor/session/server checks, release build, and the alternating exact
  A/B receipt.

- Observation: Rust's prepared pessimistic pre-lock path cloned and bound the
  whole retained AST only because its locking-SELECT classifier accepted
  literals but not parameter markers. The adjacent point-write classifier
  already consumed marker values directly. Both arms now take the parameter
  slice, and the prepared-only session wrapper is deleted. Exact one-cluster
  A/B medians against `a37b4c3f4b` were 510.51 versus 503.93 TPS for stock
  read-only, with Go at 472.51 TPS. Read-write medians were 245.96, 220.43,
  and 213.89 TPS but retained the workload's high variance; every benchmark
  leg reported zero SQL errors.
  Evidence: fail-before/pass-after
  `prepared_prelock_classification_borrows_the_retained_ast`, the marker versus
  literal locking-key regression, executor/session/server checks, release
  build, and the alternating exact A/B receipt.

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

- Observation: SQL correctly exposed
  `tidb_hashagg_partial_concurrency=1` and
  `tidb_hashagg_final_concurrency=1`, but Rust still entered
  `execute_parallel_pipeline`. `StmtContext::Columns::sysvar` is intentionally
  limited to expression builtins and global password variables, so HashAgg's
  executor-side lookup never observed session concurrency and fell back to
  5/5. Carrying Go's resolved typed values in the statement context makes 1/1
  select the serial executor and resolves `-1` through
  `tidb_executor_concurrency` at the statement boundary.
  Evidence: fail-before `Some((5, 5))` versus expected `None` in
  `production_stmt_context_hashagg_concurrency_controls_admission`, its passing
  result after typed plumbing, and the lifecycle assertion `(13, 7)`.

- Observation: Go's final HashAgg workers wait for all partial workers before
  consuming intermediate mappers. Rust's N*M bounded shuffle channels and a
  second final-result channel therefore added synchronization and messages but
  no overlap. Rust can preserve the same ownership and phase boundary by
  returning each partial worker's vector of final-bucket maps and transposing
  those vectors before final merge tasks. The focused DISTINCT median moved
  from approximately 2,129 to 2,197 TPS while Go measured approximately 2,828
  TPS in the same two alternating samples; this is a measurable cleanup, not
  closure of the remaining gap.
  Evidence: `pkg/executor/aggregate/aggregate.go` partial-worker waiter,
  `hash_agg::parallel::tests` (11 passing tests), and the paired range-split
  benchmark with zero errors.

- Observation: after the channel cleanup, a 12-second production sample put
  2,773 main-thread samples in `execute_parallel_pipeline`, including only
  1,615 in the child-reading epoch. The largest Rust-only setup outside that
  epoch was `ParallelSpillPartitions::new`, which allocated 256 capacity-bearing
  `Chunk`s for every parallel statement. Go's spill helper allocates its outer
  metadata at open but creates the 256 spill chunks only inside the partial
  worker's `prepareForSpill`; DISTINCT never enters that method because Go
  disables parallel spill for DISTINCT. Making the Rust chunks lazy moved the
  focused DISTINCT median from 2,271 to 2,674 TPS while Go stayed at roughly
  3,002 TPS.
  Evidence: fail-before assertion that `spill.chunks` was nonempty in
  `spill_partitions_allocate_chunks_only_when_spill_starts`, its pass after
  lazy preparation, `parallel_hashagg_spills_partial_results_and_finishes`,
  the 12-test HashAgg module pass, and the paired benchmark with zero errors.

- Observation: Rust's unspilled parallel Sort copied every fetched row into
  an independently owned one-row chunk during worker-local completion and
  then copied it again into a reconstructed result chunk. Go retains the
  fetched chunks and sorts/merges lightweight `chunk.Row` cursors. Retaining
  Rust's existing `SortPartition` chunks through the local K-way merge removes
  both copies and deletes `from_sorted_owned_rows`; the focused DISTINCT
  median moved from 2,668 to 2,702 TPS while the paired Go median was 2,934,
  improving the ratio from 0.886x to 0.921x.
  Evidence: fail-before 4 reconstructed chunks versus Go-compatible 64 source
  chunks in `parallel_sort_workers_share_input_and_heap_merge_their_runs`, its
  pass after the change, the multi-batch cursor-merge regression, all 16 Sort
  module tests, and the paired production benchmark with zero errors.

- Observation: temporarily selecting the synchronous coprocessor send path
  exposed a paging-continuation panic before the benchmark could prepare its
  statement. The unordered pull loop retained one ready token while waiting
  for a continuation, then synchronous dispatch appended a second token for
  the same task. Terminal `Closed` removed both, invalidating the loop's
  captured count. After enforcing one token per logical task, the diagnostic
  completed with zero errors. Its Rust median was 2,644.03 TPS versus the
  production BatchCommands median of 2,702.26 TPS, so disabling batching is
  both non-parity and about 2.2% slower; BatchCommands is not the remaining
  DISTINCT root cause.
  Evidence: fail-before panic in
  `synchronous_unordered_paging_keeps_one_ready_token_per_task`, its pass after
  the ready-token guard, all five paging/close tests, and the alternating
  synchronous diagnostic at 2,644.03/2,979.65 Rust/Go TPS.

- Observation: after Sort retained source chunks, the current production
  profile showed a one-chunk DISTINCT aggregation still submitting all five
  configured partial lanes and five final merge tasks. Four partial lanes
  received no input, and every final task merely adopted one map. Go creates
  the same logical goroutines, but an idle goroutine is cheap; each Rust lane
  occupied a persistent-pool task and paid queue/channel completion. Lazy lane
  admission preserves configured parallelism for multi-chunk input without a
  row threshold. The focused median moved to 2,803.22/2,773.79 TPS (1.011x).
  A complete split measured simple 3,987.23/4,340.52 (0.919x), SUM
  4,518.25/4,678.35 (0.966x), ORDER 3,543.73/3,611.00 (0.981x), and DISTINCT
  2,811.49/2,825.26 (0.995x), with zero errors.
  Evidence: fail-before five versus expected one worker in
  `single_chunk_pipeline_submits_only_one_partial_worker`, its pass after lazy
  admission, all 13 parallel HashAgg tests, the release build, and both paired
  benchmark receipts.

- Observation: Go's selected range plan put a direct-column
  `PhysicalProjection` inside `TableReader`, while Rust's planner bridge
  recorded only the reader and scan and rebuilt a root `ProjectionExec`.
  Consequently Rust asked TiKV for every scan column and copied the selected
  column locally. Retaining stable projection columns in the physical access
  receipt and accepting them at the clean-storage boundary now emits
  `DAGRequest.output_offsets = [1]` on both ordinary execution and a recursive
  prepared-cache rebuild. The focused alternating benchmark remained
  3,953.16/4,314.64 TPS (0.916x median, zero errors), so this was a concrete
  plan/execution parity bug but not the whole remaining shared-scan cost.
  Evidence: the fail-before `[None]` versus expected `[Some([1])]` assertion in
  `a_clean_clustered_range_sends_the_cop_projection`, its pass after lowering,
  the prepared rebuild and staged-row fallback regressions, the narrowed
  `tidb-exec` source test, and the release benchmark receipt.

- Observation: every Rust BatchCommands coprocessor attempt previously owned
  a callback run loop, while the query response also registered a separate
  `CompletionNotifier`. Publication therefore enqueued the real completion
  callback, pushed a token through another mutex, and enqueued an empty
  callback solely to wake the response. Client-go's synchronous
  `sendBatchRequest` gives each entry a one-shot result channel and lets the
  cop iterator own the wait; it has no corresponding second notifier. Sharing
  one response-owned callback executor removes the duplicate queue, token,
  mutex, and wake path while retaining BatchCommands and unordered region
  completion. A region-error rebuild also exposed that completion progress
  must be recorded even when recovery has already populated the ready queue.
  The focused simple-range benchmark after the cleanup measured Rust
  4,142.42 TPS and Go 4,584.00 TPS (0.904x median, zero errors), so the cleanup
  is parity work but not closure of the remaining scan latency.
  Evidence: fail-before source assertion in
  `one_response_owned_run_loop_drives_every_pending_region`, the shared-loop
  library regression, all six `unordered_` distsql tests, focused
  admission/retry tests, and the release benchmark receipt.

- Observation: `CopScanSource` retained a process-lifetime `Vec<String>` of
  every DAG solely for a smoke binary and three tests. Every successful open
  formatted the executor tree and appended it under one node-wide mutex, and
  every decoded chunk incremented a shared atomic. Go's table readers own no
  corresponding request history; its diagnostics are external metrics/runtime
  statistics rather than test receipts in the scan object. Deleting this state
  moved the fresh simple-range benchmark to Rust 4,063.71 TPS versus Go
  4,314.51 TPS (0.942x median, zero errors), from the preceding 0.904x
  checkpoint. Dedicated fake-region tests continue to decode and assert the
  actual DAG at the transport boundary.
  Evidence: fail-before/pass-after
  `production_cop_scans_do_not_retain_test_receipts`, the COUNT DAG wire test,
  both affected unistore SQL tests, smoke-binary check, release build, and the
  paired benchmark receipt.

- Observation: Rust implemented Go `readFromChunk`'s 80% intermediate-chunk
  reuse rule in `SelectResponseIter`, but `RemoteRowCursor` then applied a
  second 75% threshold before accepting the completed executor-facing batch.
  A typical 100-row sysbench range result under a 1,024-row request therefore
  copied every cell after the Go-equivalent response decoder had already
  finished it. Deleting the duplicate gate made the ownership regression pass
  and moved a fresh simple-range benchmark to Rust 4,072.57 TPS versus Go
  4,256.84 TPS (0.957x median, zero errors), from the preceding 0.942x
  checkpoint.
  Evidence: fail-before/pass-after
  `clean_remote_cursor_moves_a_small_completed_batch_into_the_output`, all
  five `clean_remote_cursor_` tests, release build, and the paired benchmark
  receipt.

- Observation: response sizing still differed before the duplicate
  table-reader threshold was reached. `CopRowStream` asked the decoder for a
  fixed 32,768 rows for every table scan and 1,024 for every index scan, then
  the index-handle cursor and partial-aggregate handoff independently applied
  their own completion thresholds. Go instead passes the destination
  executor's `Chunk.RequiredRows` into `selectResult.readFromChunk`; its index
  worker alone caps each pull at `MaxChunkSize` and repeats pulls until its
  larger lookup task is full. Making `SelectResponseIter` the sole completion
  owner removes the fixed large allocation and all three Rust-only sizing
  policies. The fresh full split measured Rust/Go medians of
  4,090.00/4,319.74 TPS for simple range (0.947x), 4,513.58/4,634.60 for SUM
  (0.974x), 3,633.73/3,593.61 for ORDER (1.011x), and
  2,857.62/2,817.56 for DISTINCT (1.014x), with zero errors.
  Evidence: fail-before/pass-after
  `clean_remote_cursor_forwards_required_rows_to_the_go_chunk_decoder`,
  `handle_batch_caps_decoder_demand_and_fills_the_lookup_task`, the remote and
  handle cursor suites, release build, and the alternating full-split receipt.

- Observation: ordinary Go `pkg/store/copr` workers call synchronous
  `SendReqCtx`, and client-go's `batchCommandsEntry` delivers those responses
  through its buffered `res` channel. Rust had hard-wired every BatchCommands
  entry to `CompletionRequest`, even for a single-region cop read, so the
  consumer drove an asynchronous callback queue that Go does not enter. The
  batch scheduler and in-flight table now retain one enum with the same two
  completion variants, and caller cancellation wakes the synchronous waiter
  directly. A new profile proves the hot request uses
  `SynchronousBatchPull::complete` (3,559 waiting samples) while
  `CompletionRunLoop::execute_with_call` falls to one incidental sample.
  Performance did not move: the alternating simple-range medians were Rust
  4,133.96 TPS and Go 4,362.42 TPS (0.948x), essentially the preceding 0.947x.
  This removes another parity mismatch and rules it out as the remaining
  performance root cause.
  Evidence: fail-before/pass-after
  `one_region_cop_request_uses_go_synchronous_batch_completion`, direct
  delivery and cancellation-wakeup tests, all 63 `direct_unary_` tests, the
  release benchmark, and
  `/private/tmp/tidb-rust-simple-parallel-syncbatch-fb2eceb2a9.sample.txt`.

- Observation: the ordinary-cop correction did not cover transaction RPCs.
  Go snapshot PointGet reaches client-go `RPCClient.SendRequest`, but Rust's
  `publish_transaction_get` still constructed `CompletionPull` and drove a
  private `CompletionRunLoop`; 2,390 of 2,617 sampled point-path frames waited
  there. Stock alternating medians were Rust/Go 416.70/466.33 TPS (0.894x) for
  read-only and 268.69/276.09 TPS (0.973x) for read-write. Making the generic
  `TransactionBatchPending` carry `SynchronousBatchPull` fixes every typed
  Get, BatchGet, Scan, lock, Prewrite, Commit, rollback, and heartbeat command
  at one source boundary. The rebuilt read-only profile has all 2,584 sampled
  point waits below `SynchronousBatchPull` and no callback loop below
  `publish_transaction_get`; new medians are 443.15/454.04 TPS (0.976x) for
  read-only and 263.09/274.27 TPS (0.959x) for read-write. The read-only root
  mismatch is removed, while acceptance remains open below 1.0.
  Evidence: fail-before/pass-after
  `transaction_commands_use_client_go_synchronous_batch_completion`, the
  transaction and completion unit suites, release build, stock alternating
  benchmark, and
  `/private/tmp/tidb-rust-oltp_read_only-txn-sync-44800c4f14.sample.txt`.

- Observation: Rust's cached DML and SELECT binders checked
  `tidb_enable_prepared_plan_cache` through an owned string sysvar lookup on
  every execution, while cached PointGet did not check the switch at all.
  Go maintains `SessionVars.EnablePreparedPlanCache` in the sysvar
  `SetSession` hook and every cache path reads that field. The typed Rust field
  removes the sampled lookup and also fixes the observable OFF-state PointGet
  contract. Exact one-cluster A/B means against `acde91234d` were
  512.67/514.26 TPS for stock read-only, with Go at 472.01 TPS; read-write was
  278.21/243.19/219.70 TPS but retained the same strong run-order variance, so
  no read-write increase is attributed to this change. All 18 legs reported
  zero ignored errors.
  Evidence: fail-before/pass-after
  `disabling_the_cache_disables_retained_point_execution`, the typed-state and
  existing disabled-range/cache-hit regressions, server test/release builds,
  the alternating A/B receipt, and
  `/private/tmp/tidb-rust-oltp_read_only-typed-prepared-cache.sample.txt`.

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

- Decision: one coprocessor query response owns the callback executor shared
  by its pending BatchCommands regions; do not retain a token notifier or
  per-request executor beside it.
  Rationale: the response is already the sole consumer and can scan its
  bounded pending window after driving ready callbacks. A second notification
  graph duplicates synchronization without adding ordering or correctness.
  Date/Author: 2026-08-27, Codex.

- Decision: represent client-go's `batchCommandsEntry.res` and `.cb` as two
  variants of one Rust batch completion carried through the existing
  scheduler, publication, stream, and in-flight table. Use the synchronous
  variant for a one-region ordinary cop request and retain the asynchronous
  variant only where Rust still needs a bounded multi-region overlap window.
  Rationale: switching to raw unary RPC would bypass BatchCommands and be a
  workaround. A completion variant preserves the source transport and retry
  ownership while deleting the callback executor from the ordinary path.
  Date/Author: 2026-08-27, Codex.

- Decision: use the synchronous completion variant for every typed
  transaction command, not only snapshot Get.
  Rationale: client-go's synchronous `RPCClient.SendRequest` is the common
  command boundary for reads and two-phase-commit RPCs. Retaining asynchronous
  pending objects would preserve a Rust-only callback loop; multi-region
  overlap instead comes from publishing every independent request before
  collecting their synchronous completions.
  Date/Author: 2026-08-27, Codex.

- Decision: the PREPARE-time planner owns PointGet shape and access-path
  selection. EXECUTE may bind current parameter values and instantiate fresh
  runtime state, but it must not invoke a second executor-local point planner.
  Rationale: Go uses the same planner-built PointGet on its first cache miss
  and later hits. The duplicated Rust planner had narrower, workload-breaking
  admission rules and made cache readiness depend on those unrelated rules.
  Date/Author: 2026-08-27, Codex.

- Decision: resolve HashAgg concurrency once from typed session state and
  carry it in `StmtContext`; do not broaden the generic expression builtin
  sysvar surface to make an executor policy lookup happen to work.
  Rationale: Go's executor builder reads typed `SessionVars` fields, and the
  builtin variable view has a separate, deliberately narrow compatibility
  contract. Statement snapshots also keep both query and DML execution stable
  if session variables change afterward.
  Date/Author: 2026-08-27, Codex.

- Decision: keep the persistent worker pool but transfer HashAgg maps through
  task receipts instead of reconstructing Go's channel graph literally.
  Rationale: Go's waiter establishes a full partial-to-final barrier, so owned
  Rust receipts are the native equivalent and remove redundant synchronization
  without a row-count policy, a workload rule, or a concurrency cutoff.
  Date/Author: 2026-08-27, Codex.

- Decision: compile HashAgg worker-thread observations and concurrency
  overrides only for unit tests, and allocate spill partitions only after a
  real spill request.
  Rationale: neither state participates in Go's execution decision. Production
  test receipts imposed mutex/atomic work, and eager spill chunks were a large
  fixed cost even when the Go spill gate made spilling impossible. Behavioral
  tests and fail-before/pass-after allocation coverage retain the evidence
  without production mirrors.
  Date/Author: 2026-08-27, Codex.

- Decision: retain production BatchCommands-first coprocessor dispatch and
  use the synchronous path only as its Go-compatible fallback.
  Rationale: the synchronous A/B is slower and therefore rules out batching
  as the remaining performance root. The fallback still must be correct, so
  its duplicate ready-token panic is fixed independently rather than hidden
  by production admission policy.
  Date/Author: 2026-08-27, Codex.

- Decision: submit a HashAgg lane on its first chunk and bypass final merge
  submission when no second partial map exists.
  Rationale: this removes idle scheduler work rather than selecting a serial
  plan or introducing an input-size policy. Every configured lane still runs
  once round-robin dispatch has useful work for it, and the existing spill,
  error, cancellation, and multi-worker tests retain the Go lifecycle.
  Date/Author: 2026-08-27, Codex.

- Decision: lower only a planner-selected direct-column cop projection whose
  stable columns exactly match the final root projection, and let the table
  scan accept it only when remote rows need no dirty/staged merge.
  Rationale: TiKV output offsets cannot represent computed expressions, while
  dirty-row reconciliation may require handle/column data that a narrowed
  remote response no longer carries. Exact matching removes only the
  redundant executor layer and fails closed for every unsupported shape.
  Date/Author: 2026-08-27, Codex.

- Decision: production scan objects retain only state required to build or
  execute requests; tests inspect encoded DAGs at a fake transport/region
  boundary instead of adding node-wide counters or request histories.
  Rationale: the removed receipt changed every production scan and grew
  without bound, while the lower boundary already proves exact executors,
  offsets, limits, direction, and aggregate arguments without perturbing the
  live path.
  Date/Author: 2026-08-27, Codex.

- Decision: keep one response-size reuse policy at the Go-equivalent decoder
  boundary, pass the executor's live `RequiredRows` demand to that boundary,
  and move every completed exact-width batch through table, index-handle, and
  partial-aggregate consumers unchanged. Index lookup caps each decoder pull
  at `MaxChunkSize` and loops until its independently sized handle task is
  full.
  Rationale: `SelectResponseIter` has already decided whether to reuse or
  coalesce its decoder-owned intermediate chunk. Fixed scan-family batch sizes
  and second executor-local thresholds are not Go behavior; they either
  over-allocate before decoding or turn an already completed response into
  per-cell copy work. The index cap/loop belongs to Go's worker task boundary,
  not to response decoding.
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
is now the largest remaining read-only target. Typed session HashAgg
concurrency then exposed the previous hidden 5/5 fallback: a real 1/1 run
measured Rust/Go medians of 2,771.86/3,052.33 TPS (0.908x), confirming fixed
parallel-lifecycle cost as the dominant DISTINCT regression. Removing the
redundant N*M shuffle/result channel topology while retaining the configured
5/5 worker shape measured 2,196.59/2,828.20 TPS (0.777x), approximately 3.2%
above the preceding Rust median. A fresh profile then exposed 256 eagerly
allocated spill chunks per parallel statement. After matching Go's lazy
`prepareForSpill`, the focused DISTINCT median was 2,674.32/3,005.65 TPS
(0.890x); a second complete split measured 2,668.43/3,011.55 TPS (0.886x).
The latter run measured simple 4,185.53/4,590.04 (0.912x), SUM
4,707.43/4,903.58 (0.960x), and ORDER 3,468.13/3,817.57 (0.908x), with zero
errors. Removing the unspilled Sort row-copy/reconstruction path then measured
focused DISTINCT medians of 2,702.26/2,933.89 TPS (0.921x), again with zero
errors. DISTINCT's large regression is removed without a serial cutoff, but
the acceptance gap remains open. A synchronous-coprocessor diagnostic then
measured Rust/Go medians of 2,644.03/2,979.65 TPS (0.887x), zero errors, after
fixing the fallback's duplicate ready-token panic. Because synchronous Rust
was about 2.2% slower than the production batched Rust median, the diagnostic
was reverted and BatchCommands-first dispatch remains production behavior.
Making HashAgg lane submission work-driven then measured a focused DISTINCT
median of 2,803.22/2,773.79 TPS (1.011x). The complete split measured DISTINCT
at 2,811.49/2,825.26 TPS (0.995x), ORDER at 0.981x, SUM at 0.966x, and simple
range at 0.919x, all with zero errors. The remaining range-read target is now
the shared scan/request path rather than HashAgg's fixed scheduler lifecycle.
Lowering the selected cop projection then produced narrow remote rows for the
same simple-range query, but the fresh alternating medians were
3,953.16/4,314.64 TPS (0.916x), also with zero errors. The remaining delta is
therefore below the projection boundary, in request/response execution or
decoding rather than planner shape alone. Sharing one completion loop then
measured 4,142.42/4,584.00 TPS (0.904x). Removing the production scan receipt
graph produced fresh medians of 4,063.71/4,314.51 TPS (0.942x), zero errors.
Removing the duplicate table-reader chunk threshold then measured
4,072.57/4,256.84 TPS (0.957x), zero errors. The remaining simple-range gap is
now about 4.3%; request construction, response decoding, and query-worker
wake/scheduling remain the active profile targets. Replacing the hardcoded
32,768/1,024-row decoder sizes with live `RequiredRows`, deleting the duplicate
index/partial completion rules, and restoring the index worker's
`MaxChunkSize` cap/loop produced a fresh full split at simple 0.947x, SUM
0.974x, ORDER 1.011x, and DISTINCT 1.014x. The simple sample stayed within the
existing noise band, while the change removes a concrete large-allocation and
policy-ownership mismatch without regressing the adjacent aggregate/sort
paths.

Switching typed transaction commands to the same synchronous response-channel
completion used by client-go removed the private callback run loop from the
dominant prepared PointGet path. The fresh stock read-only ratio moved from
0.894x to 0.976x; read-write measured 0.959x in the same post-change run. This
closes the measured transaction-completion root cause but not the plan's 1.0x
acceptance threshold.

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

Progress receipt (2026-08-28, typed statement-memory policy): Go
`ResetContextOfStmt` reads `SessionVars.MemQuotaQuery`, `InitChunkSize`, and
`MaxChunkSize` as typed fields and reads `vardef.OOMAction` /
`EnableTmpStorageOnOOM` through process-wide typed state. Rust instead
performed five registry lookups and conversions again while retaining each row
result, after it had already read the same policy for the statement context.
`StatementVarSnapshot` now owns the three session fields, and the published
`ResolvedGlobals` image owns the two typed global fields. Both statement
execution and result retention use those products; no statement path reparses
the global memory policy. The regression test was observed failing before the
implementation and passing afterward. The targeted memory-policy group passes
11/11, and a release `tidb-server` build succeeds.

The interleaved one-thread sysbench run compared the exact candidate with
`171f5b2a30` and the same Go server over one TiKV/PD cluster. Read-only median
TPS was 512.39 candidate, 512.08 baseline, and 474.96 Go. Read-write median TPS
was 259.65 candidate, 221.33 baseline, and 219.46 Go; the read-write samples
remain noisy, so that increase is not attributed to this change. All 18 legs
reported zero ignored errors. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session result_materialization_reuses_the_typed_statement_policy --lib
    cargo test -q -p tidb-session tests_mem_quota:: --lib
    cargo build -q -p tidb-server --bin tidb-server --release
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, retained result authority): Go row and cursor
results retain the memory tracker already installed on `SessionVars.StmtCtx`.
Rust instead built a second statement tracker in
`result_materialization_authority` after the executor returned, and the server
prepared path could do so after restoring a `SET_VAR` overlay. The session now
retains one `ResultMaterializationAuthority` at context construction; execution
and result materialization share its exact tracker, and direct PointGet paths
create one authority lazily because they deliberately do not build a complete
statement context. The tracker-identity source regression was observed failing
before the implementation. Identity, prepared-server post-restoration, and the
complete memory-quota group pass afterward (12/12), and the release server
build succeeds.

The interleaved one-thread sysbench run compared the exact candidate with
`6440f1572e` and the same Go server over one TiKV/PD cluster. Read-only median
TPS was 510.80 candidate, 509.01 baseline, and 475.38 Go. Read-write median TPS
was 254.15 candidate, 239.10 baseline, and 213.85 Go; those samples remain
latency-noisy, so no read-write increase is attributed to this change. All 18
legs reported zero ignored errors. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session result_materialization_reuses_the_typed_statement_policy --lib
    cargo test -q -p tidb-session prepared_server_result_retains_the_executing_statement_authority --lib
    cargo test -q -p tidb-session result_materialization_retains_the_statement_context_tracker --lib
    cargo test -q -p tidb-session tests_mem_quota:: --lib
    cargo build -q -p tidb-server --bin tidb-server --release
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, typed autocommit status): Go
`SessionVars.IsAutocommit` reads `ServerStatusAutocommit`, maintained by the
autocommit sysvar's typed `SetSession` hook. Rust instead resolved the registry,
cloned the stored string, and compared it on transaction, prepared-cache, and
wire-status hot paths; its process-state renderer also advertised autocommit
unconditionally. `SessionVars` now keeps the same typed fact in lockstep with
ordinary SET, statement-scoped restoration, and inherited GLOBAL state. The
source regression was observed failing before the implementation. The typed
state regression and all 11 transaction-module tests pass afterward, and the
release server builds.

The interleaved one-thread sysbench run compared the exact candidate with
`e7b3815802` and the same Go server over one TiKV/PD cluster. Read-only median
TPS was 511.63 candidate, 513.18 baseline, and 477.23 Go. Read-write median TPS
was 243.67 candidate, 221.81 baseline, and 212.76 Go; those samples remain
latency-noisy, so no read-write increase is attributed to this change. All 18
legs reported zero ignored errors. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session session_autocommit_uses_go_typed_status --lib
    cargo test -q -p tidb-session process_status_uses_the_typed_autocommit_and_transaction_bits --lib
    cargo test -q -p tidb-session autocommit_off_puts_a_statement_in_a_transaction --lib
    cargo test -q -p tidb-session tests_core::transactions:: --lib
    cargo build -q -p tidb-server --bin tidb-server --release
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, stable prepared-environment generation): Go
restores `StmtCtx.SetVarHintRestore` by ranging over the map; a nil/empty map
performs no sysvar writes and invalidates no typed session state. Rust called
`restore_system` after every statement and advanced `SessionVars.generation`
unconditionally, so the prepared-plan environment cache rebuilt its session
image on every execute despite no variable mutation. Empty restore now returns
without mutation. The generation regression was observed failing 0-versus-1
before the implementation and passing afterward; the higher-level regression
also proves an ordinary statement preserves the exact environment `Arc`, while
a real SET still replaces it. The release server builds.

The interleaved one-thread sysbench run compared the exact candidate with
`b6644bfdba` and the same Go server over one TiKV/PD cluster. Read-only median
TPS was 514.86 candidate, 512.59 baseline, and 474.77 Go. Read-write median TPS
was 265.52 candidate, 244.34 baseline, and 215.88 Go; those samples remain
latency-noisy, so no read-write increase is attributed to this change. All 18
legs reported zero ignored errors. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session empty_statement_restore_preserves_the_session_generation --lib
    cargo test -q -p tidb-session unchanged_session_reuses_the_prepared_plan_cache_environment --lib
    cargo build -q -p tidb-server --bin tidb-server --release
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, single cached historical-read admission): Go's
`GetPlanFromPlanCache` preprocesses and admits a prepared plan before returning
the execution plan; physical execution does not repeat string-backed snapshot
and staleness checks. Rust's cached point-get and SELECT binders already used
the shared prepared-plan environment admission, but their executors checked
the two sysvars again, and cached DML had no equivalent bind-time gate. Cached
DML now passes through the same environment admission, and all three cached
executors trust that admission. The ordinary statement guard remains in place
because this tier cannot answer historical reads without MVCC history. The DML
admission regression and the cached-executor source regression were both
observed failing before the implementation and passing afterward; the
ordinary historical-read regression also passes. The release server builds.

The interleaved one-thread sysbench run compared the exact candidate with
`65e08aeeb7` and the same Go server over one TiKV/PD cluster. Read-only median
TPS was 512.79 candidate, 512.48 baseline, and 475.14 Go. Read-write median TPS
was 258.72 candidate, 230.62 baseline, and 216.26 Go; those samples remain
latency-noisy, so no read-write increase is attributed to this change. All 18
legs reported zero ignored errors. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session cached_dml_binding_refuses_a_pinned_historical_read --lib
    cargo test -q -p tidb-session a_pinned_historical_read_is_refused_rather_than_answered_from_the_present --lib
    cargo test -q -p tidb-server --test all cached_execution_trusts_the_shared_historical_read_admission
    cargo build -q --release -p tidb-server --bin tidb-server
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, typed session SQL mode): Go maintains
`SessionVars.SQLMode` as a bitset in the sql_mode sysvar's `SetSession` hook;
its parser, statement context, prepared point decoder, and prepared-plan cache
key all read that one authority. Rust kept only the normalized string, plus a
separate generation-keyed scanner cache, and repeatedly split or uppercased the
string for other consumers. `SessionVars` now maintains the existing
Go-compatible `tidb_mysql::SqlMode` bitset across default construction,
inherited GLOBAL state, ordinary SET, and statement-scoped restore. Parser and
executor consumers project directly from it, the redundant scanner cache and
string parsing are gone, and the obsolete ignored `TestSQLModeVar` parity-gap
stub is removed. The source regression was observed failing before the typed
field existed and passing afterward. The typed-state test also covers Go's
case normalization, invalid-mode refusal without state drift, composite mode,
restore, and inherited GLOBAL behavior. Targeted scanner, prepared-cache key,
and environment-reuse tests pass; `tidb-vardef` passes 43/43 runnable tests;
the complete `tidb-mysql` generated-source oracle passes 18/18 with the pinned
Go 1.26.0 toolchain; and the release server builds. The complete SQL-mode
scanner module remains 13/14 because the unchanged indexed-LIKE range test
also fails at baseline `8366ff70bd`; equality, stored bytes, and the same LIKE
semantics without that indexed range all pass, so this pre-existing planner
defect is outside this typed-state change.

The interleaved one-thread sysbench run compared the exact candidate with
`f87395b719` and the same Go server over one TiKV/PD cluster. Read-only median
TPS was 508.97 candidate, 509.11 baseline, and 475.64 Go. Read-write median TPS
was 245.41 candidate, 238.46 baseline, and 212.22 Go; those samples remain
latency-noisy, so no read-write increase is attributed to this change. All 18
legs reported zero ignored errors. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session session_sql_mode_uses_go_typed_state --lib
    cargo test -q -p tidb-session sql_mode_consumers_use_go_typed_session_state --lib
    cargo test -q -p tidb-session tests_sql_mode_scanner::the_ansi_composite_carries_its_scanner_flags_through --lib
    cargo test -q -p tidb-session tests_sql_mode_scanner::no_backslash_escapes_changes_like_default_escape_only_when_enabled --lib
    cargo test -q -p tidb-session tests_sql_mode_scanner::no_unsigned_subtraction_changes_the_result_domain_and_value --lib
    cargo test -q -p tidb-session unchanged_session_reuses_the_prepared_plan_cache_environment --lib
    cargo test -q -p tidb-executor prepared_select_plan_reuses_shape_and_rebinds_parameters --lib
    cargo test -q -p tidb-vardef
    env GOTOOLCHAIN=go1.26.0 cargo test -q -p tidb-mysql
    cargo build -q --release -p tidb-server --bin tidb-server
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, lazy active-role rendering): Go keeps
`SessionVars.ActiveRoles` as role identities and `builtinCurrentRoleSig`
formats and sorts them only when `CURRENT_ROLE()` is evaluated. Rust instead
rendered the role list while building every query and DML statement context;
an authenticated session with no active role allocated `"NONE"` even when the
statement never referenced the builtin. The session now owns one copy-on-write
`Arc<Vec<Account>>`, statement contexts clone only that shared typed authority,
and `Columns::current_role` renders it lazily. The old `current_role_text`
implementation and `with_current_role` rendered-string seam are deleted. The
source regression was observed failing before the implementation and passing
afterward. All 14 role/grant module tests and the expression builtin test pass,
including SET ROLE forms, default activation, transitive privilege checks,
revoke/drop cleanup, SHOW GRANTS, and sorted output; the release server builds.

The interleaved one-thread sysbench run compared the exact candidate with
`f1c247b63e` and the same Go server over one TiKV/PD cluster. Read-only median
TPS was 511.50 candidate, 511.59 baseline, and 472.75 Go. Read-write median TPS
was 260.94 candidate, 224.89 baseline, and 217.55 Go; those samples retain the
same first-leg latency outlier, so no read-write increase is attributed to the
change. All 18 legs reported zero ignored errors. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session current_role_is_rendered_only_when_the_builtin_reads_it --lib
    cargo test -q -p tidb-session tests_grants::roles:: --lib
    cargo test -q -p tidb-expr test_current_role --lib
    cargo build -q --release -p tidb-server --bin tidb-server
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, lazy statement clock): Go's
`getStmtTimestamp` reads `@@timestamp`, takes the wall clock, resolves the zone
offset, and stores `StmtNowTsCacheKey` only when a temporal expression asks for
the statement instant. Rust performed all four operations while constructing
every SELECT and DML context. The generation-keyed statement-variable image
now retains the optional parsed timestamp, and `StmtContext` owns a shared
`OnceLock` that computes the exact existing clock tuple on its first `now()`
read. Cloned parallel-worker contexts share that cell, while callers that
already own a fixed instant keep the explicit `with_clock` path. The source
regression was observed failing before the implementation and passing
afterward; the behavior regression proves the cell is initially empty,
preserves the captured 654320955ns fraction, and initializes once across
clones. Existing pinned/wall-clock `NOW` and `SYSDATE` tests, the zoned temporal
comparison regression, and all 34 timestamp-dependent column-default tests
pass; the release server builds.

The interleaved one-thread sysbench run compared the exact candidate with
`334827e7d1` and the same Go server over one TiKV/PD cluster. Read-only median
TPS was 518.12 candidate, 518.11 baseline, and 470.35 Go. Read-write median TPS
was 269.38 candidate, 230.76 baseline, and 217.57 Go; those samples retain the
same warm first-candidate pattern, so no read-write increase is attributed to
the change. All 18 legs reported zero ignored errors. Exact validation
commands:

    cd rust
    cargo test -q -p tidb-executor lazy_statement_clock_initializes_once_across_clones --lib
    cargo test -q -p tidb-session statement_clock_is_initialized_only_when_an_expression_reads_it --lib
    cargo test -q -p tidb-session sysdate_is_now_uses_the_statement_clock --lib
    cargo test -q -p tidb-session sysdate_reads_the_wall_clock_and_not_the_statement_timestamp --lib
    cargo test -q -p tidb-session a_duration_beside_a_temporal_literal_lands_on_the_statement_date --lib
    cargo test -q -p tidb-session tests_column_defaults:: --lib
    cargo build -q --release -p tidb-server --bin tidb-server
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, borrowed protocol sysvars): Go's per-command
wait-timeout and charset reads copy string headers from `SessionVars.systems`,
and its packet reader reads the typed `SessionVars.MaxAllowedPacket` field.
Rust instead resolved the registry, cloned owned strings, and reparsed max
packet size in both the wire and statement-context paths. `SessionVars` now
offers a general `Cow`-backed value view that borrows session overrides and
static defaults, while its max-packet field follows Go's `SetSession` hook
across construction, inherited GLOBAL state, direct startup seeding, and
statement restoration. The wire trait carries borrowed values, and both
cluster and pipeline adapters have deleted their duplicate max-packet lookup.
The source regression was observed failing before the implementation and
passing afterward. Max-packet, SET NAMES/collation, server-library compile,
and release-build checks pass.

The interleaved one-thread sysbench run compared the exact candidate with
`29f4c1b2ad` and the same Go server over one TiKV/PD cluster. Read-only mean
TPS was 519.61 candidate, 508.66 baseline, and 473.71 Go; all three paired
candidate legs were positive. Read-write mean TPS was 274.72 candidate,
239.07 baseline, and 219.48 Go, but the candidate samples decayed from 330.58
to 219.06 with run order, so no read-write increase is attributed to this
change. All 18 legs reported zero ignored errors. The post-change profile at
`/private/tmp/tidb-rust-oltp_read_only-borrowed-protocol-vars.sample.txt`
contains no `get_system` or allocator descendants below wait timeout,
result/input charset, or max packet reads; only the expected borrowed registry
probes remain. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session protocol_hot_path_reads_retained_session_state --lib
    cargo test -q -p tidb-session max_allowed_packet --lib
    cargo test -q -p tidb-session set_names_reaches_literal_and_folded_expression_collations --lib
    cargo test -q -p tidb-server --lib --no-run
    cargo build -q --release -p tidb-server --bin tidb-server
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh
    PROFILE_ONLY=1 PROFILE_TAG=borrowed-protocol-vars EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Progress receipt (2026-08-28, typed prepared-cache switch): Go keeps
`SessionVars.EnablePreparedPlanCache` as a bool maintained by the
`tidb_enable_prepared_plan_cache` sysvar's `SetSession` hook. Rust instead
looked up and cloned the normalized string during every cached DML/SELECT bind,
and the retained PointGet reuse gate did not consult the setting at all.
`SessionVars` now owns the typed bool across defaults, inherited GLOBAL state,
ordinary SET, and statement-scoped restore. The old session string-lookup
helper is deleted, and PointGet, DML, and SELECT all read the typed authority.
The PointGet regression was observed failing before the implementation and
passing afterward. Typed-state, disabled range, normal cache-hit, server test
compile, and release-build checks pass.

The interleaved one-thread sysbench run compared the exact candidate with
`acde91234d` and the same Go server over one TiKV/PD cluster. Read-only mean
TPS was 512.67 candidate, 514.26 baseline, and 472.01 Go, a -0.31% candidate
delta within run noise. Read-write mean TPS was 278.21 candidate, 243.19
baseline, and 219.70 Go; the candidate again decayed strongly with run order,
so no read-write increase is attributed to this change. All 18 legs reported
zero ignored errors. The post-change profile at
`/private/tmp/tidb-rust-oltp_read_only-typed-prepared-cache.sample.txt`
contains no `prepared_plan_cache_enabled`,
`TIDB_ENABLE_PREP_PLAN_CACHE`, or `get_system` sample on the cached execution
path. Exact validation commands:

    cd rust
    cargo test -q -p tidb-session disabling_the_cache_disables_retained_point_execution --lib
    cargo test -q -p tidb-session prepared_plan_cache_switch_uses_go_typed_state --lib
    cargo test -q -p tidb-session disabling_the_cache_disables_retained_range_execution --lib
    cargo test -q -p tidb-session the_second_execute_of_a_cacheable_statement_reports_a_hit --lib
    cargo test -q -p tidb-server --lib --no-run
    cargo build -q --release -p tidb-server --bin tidb-server
    cd ..
    git diff --check
    EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh
    PROFILE_ONLY=1 PROFILE_TAG=typed-prepared-cache EXTRA_ARGS=--rand-type=uniform bash /private/tmp/tidb-alt-sysbench-20260827.sh

Plan revision note (2026-08-27): created after the user confirmed the
performance-preserving route to full Go parity across plan cache, aggregation,
parallel execution, resource groups, sort, and coprocessor packages.

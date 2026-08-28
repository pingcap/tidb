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
- [ ] Complete the existing immutable Rust physical-plan tree's operator and
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

## Decision Log

- Decision: preserve paired sysbench parity throughout the migration rather
  than delete current safeguards first.
  Rationale: literal early deletion is known to make Rust slower and would
  violate the original user goal. Temporary dual implementations are allowed,
  but the old path must be retired before completion.
  Date/Author: 2026-08-27, Codex with user confirmation.

- Decision: promote the existing `tidb_planner::physical::PhysicalPlan` as the
  sole immutable physical-plan tree plus fresh runtime executor instantiation,
  not cached live executor objects and not a new executor-local enum.
  Rationale: executor cursors, chunks, memory trackers, cancellation handles,
  and transaction snapshots are statement-local. Caching them would leak state
  across executions. Go's mutable physical nodes combine template and runtime
  concerns; Rust ownership is safer when the reusable decisions are immutable
  and execution state is rebuilt.
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
  Date/Author: 2026-08-27, Codex.

- Decision: tests must observe executor and transport behavior at their public
  boundaries instead of requiring a production-only evidence mirror.
  Rationale: Go does not duplicate every plan/request into a test receipt, and
  the mirror imposed allocation, cloning, callback, and per-row counter costs
  on ordinary execution. Scripted transports and row sources can count calls
  without changing production state.
  Date/Author: 2026-08-27, Codex.

## Outcomes & Retrospective

Work is in progress. The preceding performance phase established a paired
baseline of 1.028x Go for read-only and 1.007x for a clean read-write pair, but
left the five architectural gaps named in this plan. This section must be
updated after every milestone with measured behavior, remaining inventory, and
any rejected design.

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

In this plan, a “physical-plan tree” means an immutable value describing
chosen operators, schemas, access paths, pushed predicates and aggregates,
required ordering, estimates, and typed parameter slots. It contains no open
storage cursor or mutable executor state. “Instantiation” means turning that
tree into fresh `Box<dyn Executor>` objects for one statement. “Rebuild” means
binding current parameter values into typed slots and deriving ranges without
changing access path, join order, aggregation family, or reader boundary.

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
Go-supported cache node. Rebuild returns a new bound plan or a typed refusal;
it never mutates a plan shared by concurrent sessions. The execution route
instantiates the bound plan, records metadata-lock tables, and preserves all
session invalidation gates. Delete `PreparedRangeSelectPlan`,
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

The existing `tidb_planner::physical::PhysicalPlan` module should expose
additional immutable types similar to:

    pub(crate) trait RebuildCachedPlan {
        fn rebuild(&self, parameters: &[Datum], context: &RebuildContext)
            -> Result<BoundPhysicalPlan, CacheRefusal>;
    }

    pub(crate) trait InstantiatePhysicalPlan {
        fn instantiate(&self, context: &mut ExecutionContext)
            -> Result<Box<dyn Executor>, DriverError>;
    }

Exact names may change to match package conventions, but the separation among
immutable decisions, bound parameter values, and statement-local executor
state is mandatory.

The scheduler must use existing repository dependencies where possible. Do not
add a new runtime or queue crate without first proving that the existing
worker-pool/channel facilities cannot provide bounded, cancellation-aware,
panic-safe task execution.

Plan revision note (2026-08-27): created after the user confirmed the
performance-preserving route to full Go parity across plan cache, aggregation,
parallel execution, resource groups, sort, and coprocessor packages.

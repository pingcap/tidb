# Datasource planning: structural parity audit

Status: implementation design and source inventory, not package completion.
Target: Go master `633a9e37f1c796ac81c203dc107025e7e65385f0`. Read with `git show <pin>:<path>`: the checked-out Go files differ from master in four of these seven files. Local Go executions alone are not proof against the pinned core implementation.

## Source ownership

- `pkg/planner/util/path.go` — `83e18b31671ac792642f978c829467baaccdd100`
- `pkg/planner/core/stats.go` — `27e081c35a114798b3b343a688c4413f6b4425e4`
- `pkg/planner/core/find_best_task.go` — `0bb7bb509d62472a14f5f8566b435afe7ce2e277`
- `pkg/planner/core/indexmerge_path.go` — `b94385038e2159ac4bd498b2c997db8cd6694ebc`
- `pkg/planner/core/indexmerge_unfinished_path.go` — `0c58b1d6850a0548c15a964419e93fff6a52fb45`
- `pkg/planner/core/task.go` — `733c2bf44d16cc0017afb2a9272eb93e2251fbf9`
- `pkg/planner/core/operator/physicalop/task_base.go` — `7ec3a053fbe48a66b231091242213e493e9e1b2a`

These are dependency-boundary sources for the current cardinality package effort, not a complete core/util/physicalop package inventory or a new package completion claim.

## Derived access-path state

All 37 fields of master's `util.AccessPath` are accounted for below. Coverage was checked against the parsed struct in the pinned blob; this proves inventory coverage only, not implementation parity.

| State family | Go fields | Current Rust owner | Required invariant |
| --- | --- | --- | --- |
| Catalog identity and store | `Index`, `StoreType`, `IsIntHandlePath`, `IsCommonHandlePath` | PossiblePath + SourceIndex + datasource handle flags | Catalog identity may remain immutable/shared; derived path state must be occurrence-local. |
| Physical and usable key layouts | `FullIdxCols`, `FullIdxColLens`, `IdxCols`, `IdxColLens`, `ConstCols` | IndexPathState declared key slots; datasource shared layout helpers; dispatch resolves current usable prefixes | Retain full physical layout separately from usable access prefix, including appended handles and runtime constants. |
| Ranges and access predicates | `Ranges`, `AccessConds`, `EqCondCount`, `EqOrInCondCount`, `IsDNFCond`, `MinAccessCondsForDNFCond` | Ranger detach results local to initialization/dispatch | One derivation owns the result and DNF coverage metadata; filters and estimates must refer to that exact result. |
| Estimate stages and risk bounds | `CountAfterAccess`, `MinCountAfterAccess`, `MaxCountAfterAccess`, `CountAfterIndex` | Datasource table count and per-index IndexPathState (estimate plus bounds); locally recomputed probe counts | Keep access/index-filter/final datasource counts distinct. Preserve all risk bounds through adjustment, cloning and runtime probe specialization. |
| Filter partition and covering | `IndexFilters`, `TableFilters`, `IsSingleScan` | Dispatcher filter split; IndexPathState.is_single_scan | Bind filter partition and covering decision to the derived key/schema state; preserve Go timing through column pruning. |
| Merge alternatives and semantics | `PartialIndexPaths`, `PartialAlternativeIndexPaths`, `KeepIndexMergeORSourceFilter`, `IndexMergeORSourceFilter`, `IndexMergeIsIntersection`, `IndexMergeAccessMVIndex` | Datasource DerivedAccessPaths retains ordinary identities, union alternatives and intersection partials; physical builders consume it | Represent decided partials and OR branch alternatives (branch -> alternative -> constituent paths). Keep source OR filter retention and MV/intersection identity. |
| Hints and partial-order authority | `Forced`, `ForceKeepOrder`, `ForceNoKeepOrder`, `ForcePartialOrder` | Datasource index-ID sets plus dispatch occurrence marks | Preserve per-occurrence path authority and source mutation timing; shared catalog metadata must not be mutated. |
| Special path policy | `IsUkShardIndexPath`, `IndexLookUpPushDownBy`, `NoncacheableReason` | Mixed catalog/datasource/physical fields; shard mapping not established | Inventory source producers before migration; carry policy to task/executor without invented defaults. |
| Grouped range order and rebuild | `GroupedRanges`, `GroupByColIdxs` | Grouped range/property and range-rebuild surfaces require audit | Preserve source grouping and reconstruction for plan-cache/Apply; ordinary ranges alone do not encode the contract. |

## One producer/consumer lifecycle

1. Catalog enumeration and source pruning establish eligible identities (`AllPossibleAccessPaths` and `PossibleAccessPaths` are distinct source collections).
2. Datasource statistics derivation normalizes predicates, fills range/filter/key metadata, estimates datasource and each path, applies source heuristics, generates merge alternatives and computes general attributes from the resulting collection.
3. Property search reads that collection. It converges merge alternatives for the requested/advisory order, produces candidate-local match results, and performs source skyline/admission/cost decisions. It must not regenerate a different logical candidate population for each property.
4. Conversion builds table/index/merge cop tasks, retaining unfinished index phases, table filters, per-partial property results, histogram/column cost metadata, partition metadata and lookup authority.
5. Parent operator attachment applies source pushdown rules. Root-reader conversion follows at the required task boundary. Executor lowering consumes the physical plan and output mapping.

Rust currently splits step 2 among executor `InitStats`, logical `rewrite.rs` datasource statistics, and physical `dispatch.rs`. The recursive datasource path in `rewrite.rs` is the relevant production entry; the limited direct `DataSource::derive_stats` helper must not be mistaken for the entire production path. `InitStats` runs before logical optimization and through the synchronous statistics-load boundary, so moving everything to a one-time post-optimization pass would be another structural mismatch.

## Target Rust boundaries and migration constraints

- Keep immutable catalog identities separate from a derived datasource access-path collection. Use native Rust ownership to keep each logical occurrence's derived state isolated; do not mirror Go pointers mechanically.
- Give the derived collection one owner at datasource statistics derivation. It contains ordinary paths and decided/alternative merge paths with common range/filter/estimate state. Replace the parallel datasource maps as their consumers migrate; do not indefinitely maintain both as competing sources of truth.
- Make the derivation context independent of physical plan IDs, physical costing, and executor construction. Supply the source's expression/ranger context, statement options, statistics snapshot and load/error interfaces explicitly.
- Define invalidation from actual logical predicate/schema/partition/statistics changes and preserve task-cache invalidation. Avoid a fabricated revision counter until the existing mutation boundaries have been inventoried.
- Keep property match state candidate-local (including ordinary match, partial prefix match, advisory merge match, each partial's match, grouped ranges and join-key usage). Do not select the first OR alternative during logical range construction.
- Unify union and intersection conversion into the existing CopTask lifecycle. A builder returning a RootTask immediately cannot implement the source's later LIMIT/TopN/aggregate attachment rules.
- The provisional `prepare_union_index_merge_path` split is not the target boundary: it still runs from physical dispatch and bypasses common skyline state. Ordinary alternatives are now retained until property matching, but the logical owner remains incorrect. Replace or move it as part of this migration.
- Preserve the verified physical-key/schema and reader output invariants only where the complete source conversion supports them. Review rather than blindly retain earlier fixture-driven changes.

## Required structural evidence before completion

Compare Go and Rust intermediate state for the original source cases: candidate identity sets before/after heuristics; all OR alternatives; detached ranges and filter partitions; access/index/final counts and risk bounds; source-filter retention; property/advisory matches; cop phase and per-partial plans; root conversion and output layout. Include statistics refresh, logical rewrite, pruning, aliases/self joins, runtime index joins, prepared ranges, common/prefix handles, partitions and unavailable/legacy/MV statistics. Final SQL/EXPLAIN comparisons and workload measurements remain required but cannot replace these boundary checks.

The current two exact ordering fixtures, automatic/hinted union case and six merge-join plan assertions are acceptance inputs, not the definition of success. Full original package inventories, tests, support artifacts, lint and sysbench/TPC-C/TPC-H/YCSB validation remain open.

## Mutation and derivation boundaries (verified call sites)

| Event | Rust call sites | Migration requirement |
| --- | --- | --- |
| Predicate replacement | `logical/data_source.rs::predicate_push_down_local`, invoked by the datasource arm of `logical/rewrite.rs` | Predicates are replaced locally; the method does not itself clear path estimates. Route derived-state invalidation through the actual logical rewrite/derivation boundary; first verify source lifecycle before adding eager clearing. |
| Statistics attachment/refresh | `driver/planner_bridge.rs::InitStats::initialize_source` | Clears profile, table count, three index maps and minimum selectivity; then attaches the physical table's statistics. Replace this set of independent resets/publications with one coherent derived state. Preserve the existing error return and ownership of the plan on failure. |
| Load synchronization | `logical/rule_collect_plan_stats.rs::SyncWaitStatsLoadPoint` -> `StatisticsLoadRequester::initialize` | Candidate derivation after waiting must use the loaded snapshot; pre-wait estimates cannot silently survive. The initialization hook also runs before logical optimization to support statistics consumers inside logical rules. |
| Recursive statistics | datasource arm of `logical/rewrite.rs` | Returns an existing profile if present; otherwise normalizes predicates, estimates filters and initializes covering flags. It does not regenerate a complete path collection. Refactor this boundary with the initializer, not independently. |
| Column pruning | `logical/data_source.rs::prune_columns` | Changes schema/metadata and may remove the integer handle. Retain immutable physical key metadata; preserve Go's timing for covering flags instead of blindly recomputing after every prune. |
| Static partitions | `rule_partition_processor.rs::prune_data_source` and `make_children` | Simplifies predicates, shallow-clones source state, changes physical table ID and resolves partition hints. Each child must derive using its own physical statistics and eligible paths. Existing later load initialization must not be bypassed. |
| Partial-index eligibility | datasource `check_partial_indexes`, plus executor bridge `check_partial_index_paths` | Changes eligible identities and cacheability. Candidate state must correspond to the checked predicates and source phase, and must not resurrect rejected paths. |
| Physical property search | `find_best_task/dispatch.rs` | Currently reconstructs ranges and merge preparation during each search. After migration it consumes occurrence-local derived candidates; property matching/convergence may create candidate-local state but cannot silently replace logical estimates. |

These observations establish ownership risk, not an independently reproduced claim that every listed mutation currently yields a stale query result. The migration must remove the split ownership rather than scatter new clearing calls around it.

### Alternative selection is a separate stage

Master `indexmerge_unfinished_path.go::build` retains all branch alternatives. For the pre-property estimate it selects temporary alternatives with `cmpAlternatives`: prefer empty/point table or unique-index paths, then compare each alternative's maximum partial count (using CountAfterIndex when it has index filters). This temporary selection does not discard the alternatives. Later `matchPropForIndexMergeAlternatives` converges for requested/advisory order. Rust's current first-usable-index choice cannot be replaced by a permanent smallest-row-count choice: that would still lose ordered alternatives.

### Concrete implementation cut

The first production migration must replace the datasource's parallel path estimate maps and local detacher results with a derived path collection **together with their producers and consumers**. It must include full/usable key layout, access/index/table predicate partitions and estimate stages for ordinary paths; merge alternatives then refer to the same path type. Adding an unused Go-shaped struct or merely wrapping the existing maps would not satisfy this cut.

Before changing the owner, build the boundary regression around repeated logical derivation and partition/statistics replacement, asserting the full derived path inventory. Then migrate initializer and recursive derivation atomically. Reuse the same range-estimation entrypoint for ordinary, partial merge and runtime-specialized paths where Go does; retain the distinct runtime-index-join admission/construction branch from master's `findBestTask4LogicalDataSource`. Do not use one generic recalculation to erase that source distinction.

## Estimator input mismatch and first extraction

`InitStats` previously mixed snapshot publication and derived access statistics in one method. The derived body has now been extracted into `derive_source_access_statistics`, preserving its caller, order and error propagation. This is a migration boundary, not the shared logical candidate implementation.

The extracted body exposes a further structural mismatch: it scopes the statement AST WHERE clause and calls executor `access_cost::selectivity_with_filled_path_context_observing`, conditionally publishes that derived profile after predicate comparison, then rebuilds index ranges from optimizer expressions. Logical `rewrite.rs` independently calls `analyzed_filter_selectivity_with_options` when no cached profile exists. CTE initialization has `select: None`, so AST-backed and expression-only callers cannot be assumed to traverse the same estimate pipeline.

The shared replacement must consume bound optimizer expressions and derived paths at the logical boundary, preserving source range context, used-statistics observation, codec/conversion failures, warning behavior and session options. The catalog bridge should supply the statistics snapshot and required metadata; it must not replay SQL as a competing semantic source. Existing error/lifecycle tests must remain green through this move. No incorrect SQL result is inferred solely from the presence of these two routes.

Validation of this extraction (all passed):

```sh
cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests -- --test-threads=1
make lint
git diff --check
```

The two initialization tests cover unfiltered access counts and error propagation. They do not establish complete candidate lifecycle parity, source-package completion, or workload performance. No Go/Bazel metadata changed in this extraction.

## Migration progress: merge task boundary

Union and intersection builders now produce `CopTask` instead of directly constructing root readers. Physical dispatch admits root or multi-read cop requests and performs root conversion only for a root request. A pure table-scan merge retains the unfinished phase; intersection table filtering seals it, matching master's conversion rules. Both carry statement estimator options and common-handle metadata into the existing conversion lifecycle.

`Ver2Coster` now implements master's merge-specific `getTaskPlanCost` branch: unfinished tasks price all partials; finished tasks additionally price the table side. Previously `(index_plan=None, table_plan=Some)` priced only the table, even when merge partials existed. The new phase regression fails with the prior cost arm (31.68 instead of 63.36) and passes after correction. The builder test also asserts cop state before explicit root conversion.

This fixes one structural boundary, not the full candidate lifecycle. The ordered threshold fixture now reaches a merge with table-side TopN, but advisory/per-partial ordering is still absent. The complete threshold fixture currently has two mismatches (previously one); the additional plan-choice mismatch remains open and must not be hidden by weakening the golden. The ratio fixture still passes. Candidate ownership, shared derivation, retained alternatives, ordered merge admission, expected-count scaling and source-field coverage remain unfinished.

Validation commands:

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1
make lint
git diff --check
```

Results: 46 planner tests pass; 24 executor tests pass; 3 session merge tests pass with 4 existing bootstrap gaps ignored; the two source fixtures have 1 pass/1 fail (threshold: two exact-plan mismatches). Lint and diff checks pass. Whole-package gates and benchmark/performance validation remain open.

## Migration progress: preserve ordinary OR alternatives

Union preparation now retains a vector of ordinary alternatives for each OR branch instead of breaking at the first usable catalog index. Eligible integer-handle alternatives are retained alongside index alternatives. The pre-property estimate chooses a temporary alternative using master's empty/point-range preference and row count; physical fallback selection additionally uses the source global-index tie-break. Unique-index point preference requires the full declared key width, and integer-handle point checks retain the source NULL policy.

A structural regression uses both `(a,b)` and `(a)` indexes for two OR branches and verifies that both alternatives survive without allocating physical plans. A mutation that truncates each branch to its first candidate makes that assertion fail; restoring retained alternatives passes. This initially established preservation without property convergence. The advisory-order follow-up below adds ordinary alternative matching; common logical candidate ownership, complete required-order admission, MV multi-path alternatives, residual/source-OR-filter retention and missing-statistics policies remain incomplete.

Validation (passed):

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge -- --test-threads=1
make lint
git diff --check
```

46 planner tests and 3 session tests pass; 4 existing bootstrap tests remain ignored. The full threshold fixture and workload benchmarks were not rerun for this state-preservation change; its previously recorded failures remain open.

## Migration progress: candidate order feedback reaches parent attachment

Ordinary OR alternatives now undergo property matching before physical partials are built. Required/advisory matching prefers a matching alternative before applying Go's point/count/global-index comparison; unmatched advisory branches fall back to the ordinary alternative comparison. A structural regression retains both `(a,b)` and `(a)` alternatives and verifies that advisory order on `b` selects `(a,b)` after equality on `a`, with matching feedback and ordered scans. This is only the ordinary-key subset: grouped ranges, appended keys, MV compound alternatives and common candidate skyline admission remain open.

`CopTask` now owns the advisory flag and each partial's match result. Shared TopN attachment follows master's `handleAdvisorySortItemsForIndexMerge`: matched partials receive offset-plus-count Limit; unmatched partials receive TopN only when their schema supplies the sort expressions; the table side and root retain TopN. This state survives until parent attachment rather than being reconstructed from a finished reader. The dedicated helper follows that source boundary; no cost constants or expected golden outputs were changed.

The exact threshold fixture's `c <= 50` merge now matches Go, including the ordered `ic` partial's Limit. The `c <= 100` plan-choice mismatch remains: Rust chooses merge while Go chooses ordered lookup. The ratio fixture passes. The active SQL regression additionally compares forced merge with a table scan for ascending and descending order, LIMIT and OFFSET, using actual rows; both agree. These are correctness checks, not benchmark results.

The overall design is still incomplete. Shared logical ownership must replace dispatch-time merge preparation and parallel ordinary-path maps before package parity can be claimed. Required-order merge admission, source filter-retention rules, expected-count scaling, lifecycle invalidation and complete package inventory/gates remain mandatory. No package commit is made for this transitional state.

Advisory-order validation commands (repository root):

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task:: -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge_disjuncts_use_their_own_ranges_and_union_overlap -- --nocapture
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1
make lint
git diff --check
```

The planner filters pass 46 and 92 tests respectively (overlapping filters, not additive). Executor merge coverage passes 24 tests. Lint and diff checks pass. The source-fixture command remains red with one threshold plan-choice mismatch; ratio passes. SQL actual-row validation passes. Full package compatibility, grouped/MV/required-order paths and sysbench/TPC-C/TPC-H/YCSB performance have not been verified by these targeted checks.

Session merge validation finishes with 3 passed and 4 existing bootstrap tests
ignored. The advisory follow-up changes `find_best_task/index_merge_union.rs`
(alternative matching and scan order), `task.rs` (cop metadata and parent
attachment), `find_best_task/dispatch.rs` (property propagation), and session
`tests_explain.rs` (actual-row regression), plus this audit, the ExecPlan and the
cardinality receipt. No Go/Bazel inputs changed, so `make bazel_prepare` was not
required for this follow-up. Existing broader merge-join plan mismatches remain
outside these targeted passes; no full-suite success is claimed.

## Migration progress: remove the physical dependency from logical derivation

Logical OR alternatives, hint eligibility and partial-range estimation now live
in `access_path/index_merge.rs`. `AccessPathDerivationContext` contains only the
statement estimator options, range quota, shared fallback handler and expression
evaluator. It has no task coster, property, plan-ID allocator or column allocator.
Physical dispatch's range helper delegates to the same context, preserving the
existing warning/evaluation interfaces. Intersection eligibility reads the logical
module directly rather than depending on the union physical builder.

The retained-alternatives regression now invokes derivation with this context
before constructing any physical dispatch/coster. This enforces the dependency
boundary in its interface, rather than merely asserting that an allocator was
not used. Physical order convergence and cop construction remain in
`find_best_task/index_merge_union.rs`.

This is a prerequisite to ownership migration, not that migration itself. The
production caller still runs during physical dispatch. `DeriveStatsFold` must
receive statement evaluation when becoming the owner: `RuleContext` already has
`eval_context`, but the fold currently takes only its function builder and
range/estimator settings. Use `eval_expression_once` with that statement context,
as physical planning does; do not introduce static-only evaluation as a shortcut.
Cached datasource statistics, parallel path maps, partition cloning and sync-load
refresh still require an atomic producer/consumer change.

Validation:

```sh
cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1
make lint
git diff --check
```

Check and 46 planner tests pass. The exact SQL fixtures retain the same result:
ratio passes; threshold fails solely on the existing `c <= 100` plan choice.
Lint and diff checks pass. No Go/Bazel inputs changed; no package completion,
commit/push or workload performance claim is made.

## Migration progress: statement evaluation in logical range derivation

`DeriveStatsFold` now receives `RuleContext.eval_context`. Its pseudo histogram
range/selectivity path uses `AccessPathDerivationContext` for both column and
index range construction, including the statement evaluator, quota and existing
fallback handler. The physical index-join residual caller uses the same context
instead of the previous static helper. No new evaluation policy or error fallback
was introduced; existing caller fallback handling remains in place.

A regression invokes recursive logical derivation with a parameterized handle
range whose marker placeholders both contain 99. Previously the static builder
used those placeholders and estimated one row regardless of current parameters.
With current bounds [3,5], Go's `pseudoGetRowCountByIntRanges` caps the estimate at
high-low = 2; [3,8] yields 5; inverted [8,7] retains `Selectivity`'s one-row floor.
The first test draft incorrectly expected inclusive/empty row counts (3/0);
checking pinned master `pseudo.go` and `selectivity.go:450` corrected the fixture
to the estimator's actual source contract. The final regression passes.

This makes the shared range context usable during logical derivation. It does not
close analyzed selectivity: its range helpers still use static construction and
some equality/IN paths read stored constant values. Those consumers must migrate
with their warning/error and deferred-value semantics before the logical owner
can be considered complete. Cached profile reuse and all candidate state producers
remain part of the existing atomic migration requirement.

Validation commands:

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical_range_derivation_uses_current_statement_parameters -- --nocapture
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1
make lint
git diff --check
```

The regression and 396 logical tests pass, as do lint and diff checks. No Go/Bazel
inputs changed. Full package parity, exact remaining threshold plan choice and
workload benchmarks remain open; this change is not a package completion claim.

Final verification also passes:

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
```

All 46 physical candidate tests pass. A temporary mutation replacing only the
column range evaluator with `evaluate_static` makes the final parameter regression
fail (1 versus 2); restoring the statement evaluator passes. The mutation was
restored before final validation. Changed production files for this follow-up are
`logical/rewrite.rs` and `find_best_task/dispatch.rs`; the regression is alongside
the logical derivation code, and this audit/ExecPlan record its scope. The main
compatibility risk remains incomplete migration of the other estimator paths;
no performance measurements or complete planner-package validation were run.

## Migration progress: analyzed estimator statement inputs

The analyzed estimator now has an evaluated entrypoint using the same
`AccessPathDerivationContext`. Column range construction, equality and IN
constants, LIKE/NOT LIKE patterns and escape arguments, and recursive
multi-column DNF all use its expression evaluator. Logical statistics derivation,
OR merge estimates and the two physical index-join residual estimators call that
entrypoint. Nested evaluation/range failures remain typed estimator errors until
the outer filter applies its existing 0.8 fallback; range construction no longer
silently suppresses those errors as an absent estimate.

The regression first failed with a parameterized range estimated at 1 versus
30 for its literal equivalent. It now compares parameter markers and deferred
constants against literal equality, IN, range and multi-column OR predicates.
Marker placeholders deliberately differ from bound values. An unbound marker in
a nested OR proves that an earlier successful filter is not multiplied into a
branch-local error fallback: the complete filter returns the outer 80-row
estimate on 100 rows.

The remaining static production entrypoint is `CopTask::handle_root_task_conds`.
That conversion interface carries estimator options but no statement evaluator;
it must be migrated with task conversion ownership rather than storing a borrowed
statement inside the owned plan or inventing a separate expression evaluator.
The static wrappers also serve context-free tests/convenience entrypoints.
Shared candidate ownership, range/filter inventory, used-stats observation and
source-package completion remain open. This work does not prove all SQL
coercion/warning behavior or any workload performance result.

Validation commands:

```sh
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib analyzed_derivation_evaluates_parameters_like_literal_predicates -- --nocapture
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib access_path_context_tests -- --nocapture
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1
make lint
git diff --check
```

Both evaluation regressions, 397 logical tests and 46 candidate tests pass. Lint
and diff checks pass. Exact source-fixture results are recorded after completion
below. Changed implementation files are `logical/rewrite.rs`,
`access_path/index_merge.rs`, and `find_best_task/dispatch.rs`; test coverage lives
alongside logical derivation. No Go/Bazel inputs changed.

The exact SQL fixtures are unchanged after evaluator migration: ratio passes;
threshold retains the single known `c <= 100` merge-versus-ordered-lookup mismatch.
The failing golden was not modified. No package commit or push is warranted yet.

## Migration progress: evaluator survives task conversion

Root-condition selectivity now receives the statement evaluator through the call
boundary, matching Go master's `ConvertToRootTask(ctx)` and its
`cardinality.Selectivity(ctx, ..., RootTaskConds, nil)` call. Cop and MPP conversion,
parent attachment, merge/aggregate attachment helpers, property enforcement and
shuffle conversion forward the same borrowed evaluator. No evaluator or borrowed
statement was stored inside an owned task. Existing context-free public wrappers
retain static evaluation for their existing callers.

All general physical dispatcher conversion, attachment, enforcement and shuffle
calls use the evaluated entrypoints. The remaining static CTE enforcement caller
constructs an already-root task; it does not evaluate root-only conditions during
conversion. Root-condition estimation uses the evaluator-only analyzed entrypoint,
so it does not fabricate range quotas or fallback handlers it does not own.

The new regression initially failed with 80 rows (unbound-marker fallback) rather
than the 10-row bound equality estimate. It now covers both cop and MPP tasks,
direct conversion, parent Selection attachment followed by conversion, and order
enforcement. An initial test using Limit assumed MPP conversion would remain
valid after inserting an operator below root-only conditions; Go explicitly
restricts those conditions to a TableScan or its direct Selection, so the test
was corrected to that valid source shape. No production admission rule was
changed to accommodate the test.

Validation commands:

```sh
cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib root_conversion_uses_statement_values_for_root_filters -- --nocapture
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task:: -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib enforce:: -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib shuffle_optimize:: -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1
make lint
git diff --check
```

The final task filter passes 93 tests, including the extended new regression and
46 physical candidate tests. Enforcement passes 2 tests and shuffle passes 1;
lint and diff checks pass. Exact SQL fixture results follow below. Changed files:
`task.rs`, `logical/rewrite.rs`, `find_best_task/dispatch.rs`, `enforce.rs`, and
`physical/shuffle_optimize.rs`, plus this audit/ExecPlan. No Go/Bazel inputs changed.

This closes the identified statement-evaluation plumbing gap. It does not establish
complete warning/coercion parity, used-statistics observation, shared logical
candidate ownership, source-package completion or workload performance. Those
remain required before a package commit/push.

Final source-fixture result: ratio passes; threshold still fails only on the known
`c <= 100` merge-versus-ordered-lookup choice. This is unchanged by task-context
propagation, and the original golden remains intact.

## Migration progress: one per-index statistics record

The three parallel maps for `CountAfterAccess`, `RowEstimate` and `IsSingleScan`
have been removed from `DataSource`. Each ordinary index now has one
`IndexPathState` record. Access count is read from `row_estimate.est`; there is no
second writable count copy that can drift from its skyline bounds. The record
also carries the covering decision with its existing not-yet-derived state.
This consolidates fields within each path, rather than wrapping the old maps.

Both initializer producers, the recursive logical covering producer, ordinary
physical candidates, union/intersection readers, minimum-selectivity calculation,
and shallow datasource cloning use the new representation. `InitStats` clears
the record collection at its existing refresh boundary. Covering derivation
updates only covering state and preserves the access estimate; it still skips
recomputation while the datasource has a cached profile. No path admission or
cost formula was changed.

The existing covering lifecycle regression now additionally verifies that
uncertainty bounds survive covering re-derivation and that mutating a cloned
occurrence's record does not mutate the original. The initializer regression
seeds stale access/covering state and verifies that refreshing an unindexed table
removes the complete record. A repository Rust-source search finds no references
to the three removed map fields.

This is the statistics portion of path ownership. Ranges, key layouts, access
conditions, residual filter partitions and merge candidates still lack the shared
logical owner required by the target design. The bounded planner's separate
`DataSourceAccessPath` lists are also not interchangeable with the general
production dispatch state; merging them by name alone would be incorrect.
Whole-package source coverage and workload performance remain open.

Validation commands:

```sh
cargo check --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1
cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1
make lint
git diff --check
```

Executor check, 46 candidate tests, two initialization tests, lint and diff checks
pass. Logical and exact SQL fixture results are recorded below when complete.
Changed implementation files: `access_path.rs`, `logical/data_source.rs`,
`logical/rewrite.rs`, `find_best_task/dispatch.rs`, `access_path/index_merge.rs`,
`find_best_task/index_merge_intersection.rs`, and executor
`driver/planner_bridge.rs`; lifecycle regressions extend existing tests in the
same files. No Go/Bazel inputs changed. This is not a completed package or a
performance result and has not been committed/pushed as one.

Final result: all 397 logical tests pass. The exact SQL fixtures retain their
prior result: ratio passes, threshold has the same single `c <= 100` plan-choice
mismatch. No golden output or cost constant was changed for this state migration.


## Declared-key ownership and pre-load pruning (2026-09-25)

Additional master dependency inputs (same `633a9e37` pin):

- `pkg/planner/core/operator/logicalop/logical_datasource.go` — `997bda7d4f8be0b2911325d28399f2c2533d71f3`
- `pkg/planner/core/logical_plan_builder.go` — `5f9292036ebdc526307de488322b9110f15fbad0`
- `pkg/planner/core/rule/rule_prune_indexes.go` — `f62f7b51238b5733ade1eac8a6d3f24359a884de`
- `pkg/planner/util/column.go` — `9c7c01d7af5ed7f57652ba7594348df510aa0616`

The previous pruning implementation assumed `FullIdxCols` was unavailable at
this rule position. Master initializes full declared columns in plan building,
before collecting statistics, so that assumption incorrectly sent every Rust
candidate through Go's static metadata-only fallback. That lost consecutive
coverage, effective appended-handle access, covering flags and domination.

`IndexPathState` now includes optional declared key slots and lengths alongside
estimates and covering state. The normal builder initializes selected candidates;
`None` remains the explicit static metadata-only case. Unresolved positions stay
in the vector. `InitStats` clears estimates and covering flags while preserving
these declared keys. A builder regression verifies this state exists before
statistics; the initialization regression checks refresh and stale-estimate
removal. Schema-dependent ranges still re-resolve at their later source boundary.

Datasource layout helpers now share master's `HandleColsToAppend` and
`HasV0NewCollationStringHandle` rules between estimator initialization, pruning
and physical range construction, including duplicate/missing columns, signed
versus unsigned integer handles, common-handle lengths, global/MV/columnar
exclusions, and binary versus nonbinary version-zero handles. Declared full-width
prefix lengths normalize to unspecified through the shared column resolver.
The version-zero binary-handle regression failed before this consolidation.

Pruning now follows effective-key scoring, equality/IN-bound clustered-prefix
redundancy, covering preference, narrower-key ties, and both coverage-selection
phases. Missing partial-index constraint offsets are treated as unavailable,
matching Go's guard. Statement prefix-index covering settings reach the pruning
collector. The duplicate-coverage regression failed before the scorer change;
focused tests also cover appended suffix scores, unresolved slots and redundancy.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1` — 399 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical::rule_prune_indexes::tests:: -- --test-threads=1` — 8 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib aggregation_build_initializes_declared_index_keys_before_statistics -- --nocapture` — 1 passed. A first attempted assertion in `plan_builder/tests.rs` ran zero tests because that module is not registered; it was removed and placed in the existing active builder suite. The dormant module remains an inventory gap, not test evidence.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 47 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests:: -- --test-threads=1` — 2 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib handle_ -- --test-threads=1` — 47 passed, 2 ignored. The former Rust-only 10.00 expectation in the sysbench mixed-predicate lookup now matches master's 33.33; the compound range and both scan children are asserted.
- In `/private/tmp/tidb-go-master-20260923`, `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestRustIndexHandleLayoutReference$' -count=1 -v` — passed against verified master layout/builder/pruning source blobs; the temporary assertion includes Go's root projection. The older local checkout chose a table reader, reinforcing why local checkout results cannot stand for master.
- In that same reference copy, `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/core/rule -run '^(TestEffectiveIndexColumnIDsWithUnresolvedColumn|TestScoreIndexPathPartialIndexBadOffset)$' -count=1` — passed. Both failpoint refcounts returned to zero; no Go sources changed in the workspace.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1` — ratio passes; ordering-threshold retains the same single OR/ORDER BY/LIMIT merge-versus-ordered-lookup mismatch. Original Go fixture expectations remain unchanged.
- `make lint` and `git diff --check` pass.

This closes the declared-key/pruning boundary, not the complete access-path
lifecycle. Ordinary ranges/filter partitions and merge alternatives still need a
common logical owner; initializer expression estimation still visits catalog
indexes separately. Source-wide test mapping, the dormant builder tests,
remaining plan-choice failures and workload performance measurements stay open.
No whole-package completion, commit or push is claimed.


## Shared logical candidate ownership (2026-09-25)

Both ordinary-index union and intersection preparation now run at logical
statistics derivation, including the cached-row-profile branch. A datasource
retains `DerivedAccessPaths`: ordinary identities, OR alternatives and AND
partials share one collection. The snapshot also contains the minimum
selectivity across retained candidates. Successful merge hints remove ordinary
candidates at this boundary, as in master's `generateIndexMergePath`, instead of
forcing a separately generated physical task to win after costing.

Physical dispatch no longer calls a production merge-preparation function. It
matches properties, converges OR alternatives and converts retained candidates
to CopTask. Both union and intersection consume the shared normalized key and
handle-suffix resolver. Integer table alternatives must be present in the
eligible path collection and allowed by the PRIMARY merge hint. All supported
merge candidates compete by cost instead of taking a union-or-intersection
fallback. Context-free ordinary-path test seams can still consume enumerated
identities; they do not generate merge paths.

`RuleContext` now carries the statement index-merge setting through join reorder
and CTE derivation; the obsolete physical-dispatch setting and setter are removed. Statistics refresh, predicate replacement/simplification,
partial-index pruning, candidate pruning and static partition generation
invalidate the retained candidate state. Shallow datasource clones own separate
candidate vectors. Final column pruning preserves the logical choices; physical
schema construction restores the needed key/handle columns.

The cached-profile ownership regression failed before the logical hook was
installed and passes afterward. It checks that no physical IDs are allocated,
that a merge hint removes ordinary candidates, that datasource clones are
independent, and that predicate replacement invalidates candidates. Two physical
properties consume the same OR alternatives with an evaluator that panics if
called: conversion does not rebuild/evaluate ranges or destroy alternatives.
The InitStats regression now also proves candidate invalidation on refresh.

Validation commands (repository root unless a reference directory is named):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib index_merge_restores_index_columns_and_handle_suffix -- --nocapture` — failed before the lifecycle hook; passed after it.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 47 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1` — 399 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests:: -- --test-threads=1` — 2 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 24 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all core_logical_cte_topn_prune_source -- --test-threads=1` — 2 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge -- --nocapture --test-threads=1` — 4 passed, 4 explicitly ignored bootstrap gaps.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tidb_enable_index_merge_controls_automatic_or_paths -- --test-threads=1` — 1 passed after removing the obsolete physical setting.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestLogicalMergeIntersectionReference$' -count=1 -v` — passed; wrapper restored failpoint refcount to zero. Rust and Go return row 4 for the same hinted AND intersection and residual predicate while automatic merging is off.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1` — ratio passes; the existing single ordering-threshold plan mismatch remains, with original Go expectations unchanged.
- `make lint` and `git diff --check` pass. No tracked Go/module/Bazel inputs changed, so bazel preparation was not required.

This stage's Go oracle exposed another ownership gap: for
`a=1 AND b=2 AND id+1>2`, Go puts the handle expression on both index sides
(10 access rows, 8 filtered rows per side) and estimates a one-row probe.
Rust still treated this as a table residual. The shared filled-path migration
below fixes that stage; matching SQL results alone did not establish parity.
General OR residual/MV variants, ordered merge admission, NO_INDEX_MERGE
precedence, complete package/test inventory and workload performance gates
remain open. No whole-package completion or commit/push is claimed.

## Ordinary ranges and intersection filter ownership (2026-09-25)

Master's `fillIndexPath`/`deriveIndexPathStats` produce ordinary detached ranges,
access conditions, index filters, table filters and adjusted estimate bounds.
`generateANDIndexMerge4NormalIndex` clones those ordinary paths; it does not
independently detach each conjunct. Rust now retains these stages in
`IndexPathState.filled` at logical statistics derivation. Ordinary physical
scans and intersection partials consume that state. Runtime index joins retain
their distinct probe specialization. Statistics refresh clears filled state.

The ordinary fill boundary adjusts access estimates before deriving
CountAfterIndex, preserving lower/upper skyline risk bounds. It computes the
ordinary minimum selectivity from retained candidates and filter-stage counts.
Optional missing estimates stay unavailable rather than gaining invented
statistics. The initializer remains the access-estimate producer; moving its
AST/expression estimator routes into this logical owner is still required.

Intersection preparation clones ordinary paths and retains pushable index
filters on each build side. Covered-condition hashes exclude access predicates
that also require a full-value prefix recheck. The final table filters retain
only uncovered conditions, with Go's additional mutable-parameter rechecks
when plan caching is enabled. CountAfterAccess uses the combined partial
conditions through the shared selectivity entry point. Physical conversion
attaches build-side selections using the retained CountAfterIndex.

The SQL handle-expression regression failed before this change (no build-side
selections) and now matches Go's two 10-row scans, two 8-row build selections
and one-row probe. A second SQL fixture checks a VARCHAR prefix index: the
full-value equality remains a probe selection and returns rows 1 and 4,
excluding the same-prefix value `abd`. Both Go reference tests pass. The
ordinary physical-search regression, like the existing two-property union
regression, uses a panicking expression evaluator to prove it reuses logical
ranges rather than detaching/evaluating again.

Remaining structural work includes ordinary skyline metrics for residual index
filters (currently those candidates are retained without complete comparison
facts), general OR residuals/MV alternatives, shared histogram estimator
ownership, ordered merge admission and warning/hint precedence. The original
ordering-threshold golden still has the same single plan-choice mismatch;
matching the focused AND fixtures does not close it. Complete source/test
mapping and workload performance gates remain open. This is dependency work
within the active package claim, not a completed package or a commit boundary.

Validation for the filled-path stage (repository root unless noted):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib hinted_index_merge_intersection_retains_residual_predicates_when_disabled -- --nocapture --test-threads=1` — failed before the fix, then passed with exact Go estimate stages.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib index_merge_restores_index_columns_and_handle_suffix -- --nocapture` — passed, including ordinary reuse with a panicking evaluator.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1` — 399 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 47 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests:: -- --test-threads=1` — 2 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 24 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge -- --nocapture --test-threads=1` — 5 passed, 4 explicitly ignored bootstrap gaps.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib handle -- --test-threads=1` — 47 passed, 2 ignored, before the final minimum-selectivity recomputation and physical duplicate-adjustment guard.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestLogicalMerge(Intersection|Prefix)Reference$' -count=1 -v` — both passed, failpoint refcount restored to zero. The overlay adds reference tests only; source is the pinned master implementation.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1` — ratio passes and the known single threshold mismatch remains. This ran after the new logical minimum and before the duplicate-adjustment guard; original Go goldens are unchanged.
- `make lint` — passed. `git diff --check` — passed. No Go/module/Bazel inputs changed, so no Bazel preparation trigger applies.

Intermediate Rust compilation caught a misplaced guard and a test initializer
missing the new optional field. Both were corrected before the passing final
logical/candidate/initialization/executor/session runs above. Existing warning
output remains. No full package, workload benchmark, prepared-parameter
intersection matrix or complete residual-filter skyline gate is claimed.

## Candidate coverage and comparison (2026-09-25)

A second structural mismatch was the dispatcher's predicate-count prepass:
`index_prefix_access_count` treated two predicates on `(a,b)` as better than
one predicate on `c`, without testing whether the column sets were comparable.
A separate early table-pruning shortcut also bypassed Go's complete candidate
comparison. Both shortcuts are removed. Ordinary candidates now reach the
existing source-shaped reverse skyline fold; unknown facts retain candidates
instead of inventing dominance.

Go's `getIndexCandidate` uses two different layouts: access conditions are
extracted against IdxCols, while access plus index filters are extracted
against FullIdxCols. Filled Rust paths now retain both layouts, including
unresolved full-key positions and eligible appended handles. The shared
`index_candidate_metrics` reads these layouts and the retained access/filter
counts and risk bounds before physical index construction. Residual-filter
candidates no longer skip comparison. Column extraction walks borrowed
expressions and key iterators, avoiding intermediate cloned column lists.
DNF equality counts follow `equalPredicateCount`/`hasOnlyEqualPredicatesInDNF`,
and `isFullIndexMatch` includes index filters for the non-DNF case.

The SQL regression creates 1,000 analyzed rows, all with `a=b=1` and distinct
`c`. For `a=1 AND b=1 AND c=7`, Rust's old prepass dropped `ic(c)` and produced
a full table scan. Go retains the incomparable index and selects its lookup;
Rust now does the same and returns payload 7. The original pre-fix failure is
in `/private/tmp/tidb-skyline-columns-before.log`. The native facts regression
also covers unresolved key slots, residual-filter prefix lengths, separate
100/8 access/filter counts and 20/200 risk bounds, all-equality versus NOT DNF,
and unavailable filter estimates. No Go source or golden was modified.

Validation commands:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib skyline_keeps_indexes_with_incomparable_access_columns -- --nocapture --test-threads=1` — failed before removing the shortcut; passed after the correction.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestSkylineIncomparableColumnsReference$' -count=1 -v` — passed against pinned master, failpoint refcount restored to zero.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 48 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1` — 399 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --nocapture --test-threads=1` — 19 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1` — 76 passed; the two recorded threshold tests still fail (`ordering_index_selectivity_threshold_changes_limit_access_path`, `ordering_index_selectivity_threshold_matches_go_fixture`). The original fixture still has one OR/ORDER BY/LIMIT mismatch, and its Go expected plan is unchanged.
- `make lint` and `git diff --check` — passed. No Go/module/Bazel inputs changed.

This fixes the incorrect pruning criterion and missing comparison inputs; it
does not complete Go's scheduling of those stages. Rust still applies the
complete ordinary skyline fold after constructing physical tasks, and merge
alternatives have a separate property-conversion loop. Restoring the removed
predicate-count shortcut would hide that ownership gap. The next migration
must move complete candidate admission/comparison before conversion, including
table and merge state, rather than add another approximation. Table path
ownership, estimator production, ordered merge admission, warning/hint
precedence, complete package inventory/gates and workload benchmarks remain
open. No package completion or commit/push is claimed.

## Table ownership and ordinary skyline scheduling (2026-09-25)

Integer and common-handle table ranges now live in the same retained logical
candidate owner as ordinary index state. `FilledTablePath` keeps the usable
clustered-key prefix, integer conversion type, detached access/residual
conditions, access count and risk bounds. Its lifecycle follows the existing
candidate invalidation and cloning boundary. Table minimum selectivity reads
this adjusted logical count. Physical table construction consumes it; runtime
index-join specialization and native sources without logical derivation use
the shared fill helper rather than another table detacher implementation.

The common-handle key resolver now preserves Go's leading-prefix rule instead
of skipping missing key positions. Empty common-handle conditions initialize
FullNotNullRange, following master's `deriveCommonHandleTablePathStats`.
Integer ranger quota fallback preserves residuals and the statement handler;
range errors propagate through the planner instead of being swallowed by
physical dispatch's `.ok()` calls. The initializer still produces base access
estimates, so this does not claim complete estimator ownership.

`candidate_preparation` admits ordinary table/index candidates from filled
state and performs the reverse skyline fold before physical allocation. The
selected identities are converted exactly once; the physical tail does not
repeat comparisons or overwrite the retained pseudo-winner signal. Table
facts use Go's empty index-filter map, zero CountAfterIndex and DNF equality
rules. Index facts use the full state added in the previous stage. Forced
partial-order state is marked during admission, before conversion can reject a
single/double-read task. The existing unordered-root point heuristic also runs
before ordinary conversion. Its full relocation into logical derivation,
including every property and TiFlash variant, remains open.

The allocation regression compares the same source with/without a dominated
index. With early comparison enabled both allocate two plans; disabling only
its production hook allocates five and fails the assertion. The common-handle
physical-reuse check uses a panicking evaluator; disabling retained table
state fails there. Both mutations were restored and the tests pass. No
physical-ID golden was changed to hide an extra conversion.

Validation commands (repository root unless a reference directory is named):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib index_merge_restores_index_columns_and_handle_suffix -- --nocapture` — passes; both early-comparison and table-reuse disabling mutations fail. Mutation logs are `/private/tmp/tidb-prepared-before-skyline.log` and `/private/tmp/tidb-prepared-before-table-reuse.log`.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 48 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1` — 399 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests:: -- --test-threads=1` — 2 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --nocapture --test-threads=1` — 19 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1` — 76 passed and the two previously recorded threshold tests still fail. This ran after ordinary preparation was introduced and before moving forced partial-order marking into admission; the original golden is unchanged.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib handle -- --test-threads=1` — final run: 59 passed, 5 explicitly ignored cases.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^(TestRustIndexHandleLayoutReference|TestLogicalMergeIntersectionReference|TestLogicalMergePrefixReference|TestSkylineIncomparableColumnsReference)$' -count=1 -v` — four reference tests pass and failpoint refcount returns to zero.
- `make lint` and `git diff --check` pass. Go/module/Bazel inputs are unchanged; no Bazel preparation trigger applies.

An intermediate compile rejected a Rust-2024-only let-chain in this Rust-2021
crate. The final code uses ordinary Option filtering; passing final suites
above include that correction. Existing warnings remain.

Merge convergence/conversion still has a separate physical loop. The fallback
for unfilled sources (including currently unsupported filled MV state) still
compares after conversion. Complete range-preference scheduling, logical point
heuristics, producer unification for AST/expression estimates, remaining
range/context/variant tests, package inventory and workload performance remain
open. This stage is not a whole-package completion or commit/push boundary.

## Merge convergence before physical conversion (2026-09-25)

`prepare_access_paths` now returns ordinary survivors and concrete merge
choices together, before any physical scan construction. Union property
convergence is separate from conversion: `ConvergedUnionPath` borrows only the
chosen partials and retains their match results, direction and advisory state.
Production conversion cannot choose another alternative. The old combined
entry point remains a test-only wrapper. Intersections enter the same prepared
merge collection. Go appends generated merge candidates after ordinary skyline
comparison, so this preserves that source ordering rather than comparing
merge alternatives using ordinary column-coverage facts.

Go's `matchPropForIndexMergeAlternatives` rejects an unhinted ordinary union
when every selected branch uses the same index (or the integer table path).
Rust previously omitted this admission rule. A native regression fails before
the check and passes afterward. The schema/layout fixture now explicitly uses
a merge hint because its single-index union is only valid with that hint in
Go. The SQL regression and Go reference cover both automatic rejection and
the explicit-hint exception, each returning payloads 10, 20 and 40. The check
walks the chosen IDs without allocating a set. Advisory sorting is considered
only when its directions agree, following Go's guard.

The shared preparation regression verifies that a hinted union survives and
that convergence allocates no physical plan IDs. Existing checks retain the
same alternatives across two properties and reject range reevaluation during
physical construction. This separates the production boundaries without
claiming that all source variants are implemented: ordered union conversion,
OR residuals/MV alternatives, noncacheability metadata and full source estimate
ownership remain open.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib index_merge_restores_index_columns_and_handle_suffix -- --nocapture` — new unhinted single-index rejection fails before the fix; regression is included in the passing candidate suite.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 48 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --nocapture --test-threads=1` — 20 passed, including the single-index hint exception.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 24 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge -- --test-threads=1` — 5 passed, 4 explicitly ignored bootstrap cases.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1` — 76 passed, the same two threshold tests fail; original Go goldens are unchanged.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestSingleIndexUnionReference$' -count=1 -v` — passed; failpoint refcount restored to zero.
- `make lint` and `git diff --check` pass. No Go/module/Bazel inputs changed.

Next estimate-lifecycle audit: master's `getGeneralAttributesFromPaths`
(`stats.go`) reads CountAfterAccess for table paths or concrete
PartialIndexPaths, but CountAfterIndex for other paths. An unfinished ordinary
union created in `indexmerge_unfinished_path.go` sets CountAfterAccess and
PartialAlternativeIndexPaths, leaving CountAfterIndex zero until convergence
creates a concrete path. Rust currently contributes its union CountAfterAccess
to the logical minimum. Do not assume that treating unfinished and concrete
paths alike matches Go; verify this distinction with a source reference test
before changing minimum/selectivity gates. Converged Go estimates also use the
chosen partial access/index-filter DNF, whereas Rust carries the earlier
fully-covered-OR estimate. Broader residual/MV choices require that producer
migration. Package inventory/gates and workload performance remain incomplete;
no package-level commit/push is claimed.

## 2026-09-25 — preserve statistics across shared candidate stages

Verified the unfinished-versus-concrete minimum-selectivity distinction with
`TestRustMergeStatisticsStageReference` against pinned master: ordinary index
paths use CountAfterIndex, unfinished unions leave it at zero, while table
paths and concrete merges use CountAfterAccess. Rust now preserves that stage
boundary. The native assertion failed before the change and passes afterward.

The remaining OR/ordered-LIMIT golden mismatch was not missing admission of
the ordered `ic` candidate. A temporary cost trace showed that candidate at
58,393 versus an incorrectly cheap merge at 53,482. Go's original mock-table
fixture selected the lookup at 58,268 versus a hinted merge at 68,696. Go
`GetOriginalPhysicalIndexScan`, `GetOriginalPhysicalTableScan` and
`BuildIndexMergeTableScan` scale the original table profile. Rust's union and
intersection builders instead constructed row-count-only StatsInfo, losing
HistColl and its row-size semantics. The 1,010-row partial index scan cost was
164,428 in Rust versus 205,535 in Go (16-byte fallback versus 32-byte histogram
row size). This was a profile-ownership error, not a reason to tune factors.

Both merge conversions now scale the table profile using the existing shared
StatsInfo operation, retaining histogram metadata, NDVs and stats version.
The intersection's index-filter Selection still has Go's fresh profile
boundary; the table side retains its scan profile. This does not close the
remaining residual-filter/selectivity producer gaps. A native assertion proves
physical merge scans retain the source HistColl across properties; replacing
the helper with a row-count-only profile makes it fail. The exact original
ordering-threshold golden now passes without changing expected plans.

Validation (repository root unless specified):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib matches_go_fixture -- --nocapture --test-threads=1` — 2 passed; threshold failed before profile preservation.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib index_merge_restores_index_columns_and_handle_suffix -- --nocapture` — profile-removal mutation fails; restored code passes in the candidate suite.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 48 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1` — 20 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib index_merge -- --test-threads=1` — 5 passed, 4 pre-existing bootstrap gaps ignored.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 24 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain:: -- --test-threads=1` — 77 passed, one remaining analyzed-table threshold assertion failed (TableReader/TopN versus ordered lookup). No golden was weakened.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/core -overlay=/private/tmp/tidb-merge-statistics-overlay.json -run '^TestRustMergeStatisticsStageReference$' -count=1 -v` — passed.
- In the same reference: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-ordering-cost-overlay.json -run '^TestOrderingIdxSelectivityThreshold$' -count=1 -v` — passed; diagnostic overlay retains the original mock setup and compares verbose costs. Both Go wrappers restored failpoint refcount to zero.

Logs are `/private/tmp/tidb-merge-minimum-*.log`,
`/private/tmp/tidb-merge-statistics-master.log`,
`/private/tmp/tidb-ordering-cost-master.log` and
`/private/tmp/tidb-merge-profile-*.log`. Temporary Rust cost diagnostics and
mutation code were restored. Ordered merge conversion, general OR/MV
residuals, the remaining analyzed ordering assertion, full source mapping,
package gates and sysbench/TPC-C/TPC-H/YCSB performance are still open. This is
partial evidence; no package completion or package commit/push is claimed.

`make lint` and `git diff --check` pass for this stage. No tracked Go, Go module or Bazel inputs changed, so `make bazel_prepare` was not required.

## 2026-09-25 — natural merge admission and verified threshold expectations

The 1,000-row analyzed threshold case was verified against pinned master with
identical schema, rows, ANALYZE options and session settings. Go chooses
TableReader/TopN at thresholds 0 and 0.1 (cost 18,503.44), ahead of the forced
`ic` ordering lookup (22,518.46) and forced `ib` range lookup (101,456.12).
Rust already matched these choices and row estimates. The former synthetic
assertion that threshold 0 must choose `ic` and 0.1 must choose `ib` was wrong.
`analyzed_ordering_threshold_preserves_go_cost_choice` now pins all six full
brief plans, including 19.23 ordered-scan rows and 52 range rows. Original Go
ordering-setting golden files remain unchanged.

The six merge-join assertions were independently checked using their exact
SQL/data against master and are valid. For the two-table join, Go's natural
merge costs 1,059,782.53 and hash costs 2,392,899.53. Rust's hash cost matches;
the trace contains index/hash candidates but no natural merge candidate.
`merge_join_candidates` had an unsupported `prop.is_sort_item_empty()` refusal
that does not exist in Go `physical_merge_join.go:GetMergeJoin`. Removing it
restores Go's design: enumerate natural merge whenever both children supply
the join-key prefix, then check whether it satisfies the parent's property.
An unordered parent does not authorize enforcing sorts on unordered children.
The direct candidate regression asserts both boundaries and fails before the
fix. All six existing join assertions pass unchanged after it. Cost formulas,
factors and verified merge expectations were not modified.

Validation (repository root unless specified):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib natural_merge_join_does_not_require_parent_order -- --nocapture` — fails before the fix (zero candidates versus one).
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 49 passed including that regression.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1` — 101 passed; pre-fix broad sweep was 95 passed/6 failed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib merge_join -- --test-threads=1` — 19 passed, including mixed key types and execution results.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1` — 20 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib merge_join -- --test-threads=1` — 8 passed.
- In `/private/tmp/tidb-go-master-20260923`, run `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestAnalyzedOrderingThresholdReference$' -count=1 -v` — passed.
- The same Go command with `-run '^TestMergeJoinChoicesReference$'` and `-run '^TestMergeJoinCostsReference$'` — both passed. Wrappers restored failpoint refcount to zero. Logs: `/private/tmp/tidb-analyzed-ordering-master.log`, `/private/tmp/tidb-merge-join-choices-master.log`, `/private/tmp/tidb-merge-join-costs-master.log`.

Rust validation logs are `/private/tmp/tidb-natural-merge-*.log`; temporary
cost instrumentation was restored. No tracked Go/module/Bazel inputs changed.
This closes the recorded threshold/join plan failures, not the entire package:
ordered merge conversion, general OR/MV residuals, complete package inventories
and source/support mappings, package gates and workload performance remain
open. No partial package commit or push is claimed.

`make lint` and `git diff --check` passed after the natural-merge change.

## 2026-09-25 — merge LIMIT attachment uses the shared task lifecycle

Go `task.go:attach2Task4PhysicalLimit` has separate union and intersection
branches. Rust sent both through ordinary lookup attachment. The new native
regression failed before correction: a union retained a root Limit and had no
partial limits or embedded reader limit. Rust now follows Go's decisions:
union partials receive offset+count while the index phase is open; intersection
partials are never truncated before membership; a finished table phase gets a
pushed table limit when ordering permits; root/table residual filters retain
the necessary outer limit. `sinkIntoIndexMerge` accepts only a bare table probe
(directly or below one projection) and restores output column identity/order,
not merely schema width. Merge statistics are not restamped as lookup stats.

The SQL regression then exposed another structural remnant: dispatcher parent
hint promotion recognized only a root IndexMergeReader. It overrode normal
cop LIMIT preference and rejected the hinted merge cop path before attachment.
Removed that recursive root-reader preference and its two helpers. Merge hints
already prune candidates at the datasource boundary, as Go does; parent task
selection now uses normal logical hints and cop preference consistently.

The native matrix covers union/intersection, open/filtered table phase and
keep-order, plus root-condition and reordered-schema controls. SQL verifies
partial caps, embedded offset/count, distinct qualifying output handles, and
an intersection residual followed by LIMIT. Both initial native and SQL
regressions failed before their respective fixes. The unordered output order
is intentionally not asserted: Go's worker arrival order is unspecified.

Validation (repository root unless specified):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib merge_limit_attachment -- --nocapture` — fails before attachment fix, passes afterward.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task:: -- --test-threads=1` — 98 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 49 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1` — 21 passed; new SQL test failed with root-only hint promotion retained.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1` — 101 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 24 passed.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestUnionLimitAttachmentReference$' -count=1 -v` — passed; same schema/data/SQL, checks partial caps, embedded limit and distinct qualifying results. Wrapper restored failpoints to zero.
- The same Go command with `-run '^TestOrderedUnionReference$'` also passes and records the next ordered-union integration target; it is evidence of Go behavior, not a claim that Rust matches it.

Logs: `/private/tmp/tidb-merge-limit-*.log` and
`/private/tmp/tidb-ordered-union-master.log`. No tracked Go/module/Bazel inputs
changed. Ordered union planning remains deliberately unenabled: convergence
has per-partial match results but root conversion currently drops ByItems and
KeepOrder. Existing executor ordered merging is callable once that lifecycle
is connected. Expected-count scaling of partial/probe profiles and general
OR/MV residual estimate ownership remain open. Go's unordered LIMIT oracle
estimates 2.00 rows on each partial and 4.00 on the probe; this stage's SQL test
proves attachment and results, not those estimates. Table-side pushed LIMIT is
currently ignored by the executor's merge table-filter lowering (outer LIMIT
preserves results); matching source work reduction remains open. Package
inventories, support mappings, full gates and benchmark performance remain
incomplete. No whole-package completion or partial commit/push is claimed.

`make lint` and `git diff --check` passed after the shared merge LIMIT changes.

## 2026-09-25 — shared pseudo DNF and union expected-count estimates

The unlimited union regression failed with 2.50 rows versus Go's 19.99.
Union preparation called the analyzed-only filter helper on a pseudo profile,
so synthetic NDV 8000 became an equality rate. It now uses the same filter
entry point as ordinary index/filter derivation. That shared pseudo path was
missing Go Selectivity's uncovered-DNF recursion; it now groups same-column
items with the existing ranger `merge_dnf_items_4_col`, estimates each CNF
recursively, combines overlap, skips absent stats columns/correlated items,
and uses the statement selectivity fallback. Pseudo profiles omit empty
histogram payloads but retain column identity in the NDV map, so presence uses
map membership rather than the synthetic NDV value. A temporary trace proved
that schema/predicate columns were present; the issue was not column pruning.

The limited regression then failed because physical union conversion ignored
ExpectedCnt. Converged candidates now retain the parent expectation. Conversion
uses the ordinary scan LIMIT arithmetic for completely covered partials and
Go's proportional probe adjustment with ToleranceFactor. Table partials retain
the ordering-ratio adjustment when matched. General residual-bearing partials
are not claimed by this fully-covered union path. Logical counts remain
unmodified for other property searches.

Go reference and Rust now agree on 19.99 unlimited union/probe rows and, with
LIMIT 1,3, 2.00 rows per partial and 4.00 at the probe. The grouped predicate
`a=1 OR a=2 OR b=2` is 259.75, as recorded by Go (a guessed 29.98 expectation
was rejected by the reference run before adoption). Existing ranger grouping
and ranges produce that estimate in Rust without a new special case. Both
baseline and expected-count assertions failed before their respective fixes;
original Go ordering goldens remain unchanged.

Validation (repository root unless specified):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib union_merge_limit_caps -- --nocapture` — fail-before logs `/private/tmp/tidb-merge-pseudo-before.log` and `/private/tmp/tidb-merge-expected-before.log`; after-fix logs `/private/tmp/tidb-merge-expected-after.log` and `/private/tmp/tidb-merge-grouped-after.log` pass.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1` — 101 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1` — 21 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical:: -- --test-threads=1` — 399 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 49 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 24 passed.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestUnionLimitAttachmentReference$' -count=1 -v` — passes baseline/grouped estimates, partial caps and distinct offset/count results; failpoint refcount restored to zero.

Suite logs are `/private/tmp/tidb-merge-estimates-*.log`. Temporary tracing was
restored. No tracked Go/module/Bazel inputs changed. Ordered merge metadata,
intersection expected counts, general OR/MV residual estimates, table-side
execution work reduction, full package/source gates and workload performance
remain open. This is partial evidence, not a whole-package completion or a
partial package commit/push.

`make lint` and `git diff --check` pass after the shared union estimate changes.

## 2026-09-25 — ordered union properties reach the existing executor

Go master `convertToIndexMergeScan` admits ordered unions when every chosen
partial matches; only ordered intersection is rejected. Rust's shared
preparation still rejected both. The Go reference records ascending,
descending, offset/count and descending-LIMIT union plans without a Sort or
TopN. The corresponding SQL regression failed before the fix.

Preparation now lets union convergence validate the required order while
keeping Go's intersection restriction. Converged candidates retain typed
ByItems and the cop task's KeepOrder; conversion moves that metadata into the
IndexMergeReader. Advisory order remains separate and never claims global
reader order. The existing ordered executor consumes these keys; no second
sorting implementation was added. Hidden sort columns and LIMIT offsets retain
correct projected output and deduplicated results.

A second fail-before case exposed that merge property matching used declared
index columns only. It now borrows the retained filled-path normalized layout,
including eligible appended primary-key columns after fixed leading keys.
Native/unfilled sources use the existing layout helper as a fallback. This
shares ordinary scan layout semantics and avoids rebuilding/cloning the layout
for each production property match. Go and Rust both use ordered ia(a)/ib(b)
partials to satisfy ORDER BY id LIMIT 1,3 and return IDs 2,3,4.

Validation (repository root unless specified):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib ordered_union_merge_preserves -- --nocapture` — initial ordering and appended-handle assertions fail before their fixes, pass afterward. Includes ASC/DESC, deduplication, exact partial/probe estimates with LIMIT, a hidden sort key and appended-handle order. Logs `/private/tmp/tidb-ordered-union-before.log`, `/private/tmp/tidb-ordered-union-after.log`, `/private/tmp/tidb-ordered-handle-before.log`, `/private/tmp/tidb-ordered-handle-after.log`.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1` — 22 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1` — 101 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1` — 49 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task:: -- --test-threads=1` — 98 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 24 passed.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestOrderedUnionReference$' -count=1 -v` — passed, with hidden-key and appended-handle output checks. Failpoint refcount returned to zero.
- `git fetch origin master` — succeeded; master remains `633a9e37f1c796ac81c203dc107025e7e65385f0`. No source-reference advance or package inventory change.

Suite logs are `/private/tmp/tidb-ordered-union-*.log`. No tracked Go/module/
Bazel inputs changed. This closes the ordinary ordered-union metadata bridge,
not all source variants: grouped range merge-sort, general OR/MV residuals,
noncacheability metadata, intersection estimate adjustment and table-side
LIMIT work reduction remain open. Full source/package gates and sysbench/
TPC-C/TPC-H/YCSB performance remain unverified. No package completion or
partial package commit/push is claimed.

`make lint` and `git diff --check` pass for the ordered-union stage.

## Retained merge table request execution (2026-09-25)

Reference remains Go master `633a9e37f1c796ac81c203dc107025e7e65385f0`.
Go `IndexMergeReaderExecutor.buildFinalTableReader` passes `tableRequest` and
`tblPlans` into every handle task. Rust instead recursively extracted only
Selections, discarding LIMIT/TopN and their placement in that tree. This was
an execution-stage structural mismatch even when an outer root operator
masked the wrong amount of intermediate work.

The merge reader now retains a task builder that invokes the shared physical
executor builder with exactly its TableScan replaced by a handle-backed
source. It uses the scan's input schema, preserving columns needed by filters
and ordering before output pruning. Each task gets fresh operator state and
coprocessor expression semantics. No separate merge-only LIMIT/TopN evaluator
was added. The partition fixture now puts the physical partition-ID column
in the scan as well as the Selection schema, matching a valid retained tree.

The native regression failed before the fix: Selection above LIMIT returned
IDs 3,4,5,6 instead of no rows. It now tests LIMIT/TopN both above and below
a filter, offsets, and separate three-handle tasks. The SQL regression checks
intersection probe TopN (offset 0/count 3), residual filtering, hidden ordering
column, outer offset, and results 5,4 with lookup sizes 3 and 20000. The same
SQL and probe shape pass against Go master. Existing partition/common/heap/
global-index merge coverage remains green.

Validation (repository root unless stated):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge_table_tasks_execute_retained_operator_order -- --test-threads=1` — failed before fix.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 25 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 24 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 23 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib merge_table_probe_preserves_residual_topn_and_offset -- --test-threads=1` — passed after strengthening the probe-shape assertion.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1` — 101 passed.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -overlay=/private/tmp/tidb-handle-layout-master-overlay.json -run '^TestMergeTablePipelineReference$' -count=1 -v` — passed; failpoint refcount restored to zero.
- `make lint` — passed (existing Go-cache/network access required).
- `git diff --check` — passed.

Logs: `/private/tmp/tidb-merge-table-pipeline-{before,after,builder,hints,explain,master,lint}.log` and `/private/tmp/tidb-merge-table-topn-plan.log`.

This is seed/integration evidence for the still-incomplete whole-package
claim, not package completion. Distributed region scheduling, worker
concurrency and workload performance were not validated. Unordered table
partial sources currently emit executor-sized handle chunks; the task-reset
regression sets both chunk and lookup sizes to obtain exact task boundaries.
Audit that producer batching against Go separately. Remaining package
inventory, grouped-range ordering, residual/MV/cache variants, and workload
gates are still open. No package commit or push was made.

## Merge partial producer batching (2026-09-25)

Go master's partial index/table workers grow task handle budgets independently
of executor chunk size. Index workers seed the budget with `CalculateBatchSize`
using the physical index scan estimate; table workers start with the smaller
of MaxChunkSize and IndexLookupSize. Complete batches double up to the lookup
cap. The previous Rust sources emitted exactly one executor chunk per task,
which both ignored smaller lookup caps and prevented task growth.

The ordinary lookup sizing function is now shared with merge producers.
`ExecutorPartialHandleSource` accumulates several chunks into one task while
preserving row/ordering-key alignment. The direct handle-only index source
uses the same initial sizing and growth policy. Both reset their budgets on
open; the retained partial tree still owns filters and physical pushed limits.
The table-task regression now sets executor chunk size 6 independently of
lookup sizes 3/20000: it failed before this change and passes afterward.
A separate regression covers table-backed and direct index producers, growth
2/4/5/5/3, an initial budget of 5, ordering keys, EOF, and reopen.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge_table_tasks_execute_retained_operator_order -- --test-threads=1` — failed before fix, log `/private/tmp/tidb-merge-batching-before.log`.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 26 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 25 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib lookup_initial_batch_matches_go_calculate_batch_size -- --test-threads=1` — passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 23 passed, including the preceding Go-referenced lookup-size/TopN case.
- `make lint` and `git diff --check` — passed.

Logs are `/private/tmp/tidb-merge-batching-{before,after,builder,sizing,hints,lint}.log`.
Source basis: Go master `633a9e37f1c796ac81c203dc107025e7e65385f0`,
`index_merge_reader.go` worker initialization, `extractTaskHandles`, and
`distsql.go:CalculateBatchSize`. No Go/Bazel inputs changed.

This supersedes the preceding entry's one-chunk producer gap. It does not
close the entire producer lifecycle: explicitly audit embedded pushed-limit
scanned-key counters across partials/partitions, partition request boundaries,
worker concurrency, cancellation and memory tracking. No distributed or
sysbench/TPC-C/TPC-H/YCSB performance claim is made. The package remains
incomplete and uncommitted.

## Ordered merge heap retention and topology (2026-09-25)

Tracing pushed LIMIT exposed a stale Go design in Rust's HandleHeap. Current
master includes `6cd05e3741def9453c2572f4ba89410aa7cd72ce` (issue 70910):
`requiredCnt` is the full Count+Offset, whereas only the initial allocation is
capped at 1024. Rust's comment incorrectly described 1024 as the current Go
logical bound and deliberately reproduced the earlier bug. Consequently the
original Go SQL regression returned 1024 rows in Rust for LIMIT 2000.

The heap now separates retention from bounded initial allocation. Insertion,
eviction, and final reverse drain follow Go's binary heap instead of finding
the eviction victim by a linear scan over all retained entries. This removes
O(retained-count) work per eviction; no workload-speed claim is inferred from
that algorithmic improvement. A u64::MAX count allocates only 1024 initial
slots. The native regression checks a 4096-key permutation against sorted
retention for both directions and offsets/counts beyond 1024.

Source test `pkg/executor/test/indexmergereadtest.TestIssues70910` is mapped to
`tidb-session::tests_index_hints::ordered_index_merge_limit_exceeds_initial_heap_allocation`.
It retains the source's heap-table schema, 3000 rows inserted in 500-row
batches, USE_INDEX_MERGE, IndexMerge/no-TopN assertions, LIMIT 2000 and all
ordered result values. Additional ASC/DESC LIMIT 1500,1100 cases cover large
offsets and complete cardinality. The Go original test passes independently.
This is dependency seed evidence, not completion of the Go executor package.

Commands:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib ordered_index_merge_limit_exceeds_initial_heap_allocation -- --test-threads=1` — fails before (1024 versus 2000), passes after.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 27 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/executor/test/indexmergereadtest -run '^TestIssues70910$' -count=1` — passed; failpoint refcount restored to zero.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-merge-heap-{before,after,executor,hints,master,lint}.log`.
Files changed this stage: executor `index_merge_reader.rs`, session
`tests_index_hints.rs`, this audit, the cardinality ExecPlan and receipt.

The pushed-limit audit that found this bug remains open. Go explicitly limits
union partial-worker extraction to offset+count but disables that cap for
intersection; its scanned-key counter spans the worker's partition lifecycle.
Rust still needs a complete accounting of request budgets through direct,
executor-backed and merged-partition sources. Whole package inventories,
distributed execution, and sysbench/TPC-C/TPC-H/YCSB gates remain incomplete.
No package commit/push or completion claim was made.

## Union partial-worker LIMIT budget (2026-09-25)

Go's startPartialIndexWorker/startPartialTableWorker pass pushedLimit only for
union; extractTaskHandles counts raw handles before process-worker dedup and
reduces RequiredRows to the remaining Count+Offset. Intersection explicitly
clears the worker limit so membership remains complete. Rust previously
limited only merged output, allowing producers to fetch an entire larger
batch before the process worker stopped.

PartialHandleSource now carries a remaining raw-handle request through direct
index cursors, executor-backed sources, and the partition merger. The union
process owns one extraction budget per partial path, separately from its
deduplicated output offset/count. Crossing a partition does not reset that
budget. Intersection retains the unbounded source path. Ordered partition
refills are deferred until another row is requested instead of refilling
after the final requested row. Executor sources preserve oversized ordinary
chunks and truncate only at the terminal pushed handle budget.

Fail-before native regression recorded six handles where offset 3/count 2
permits five. It now checks ordered/unordered union output and raw counts,
plus full 19-handle inputs on intersection. Direct-index and executor-backed
sources are also checked with finite and zero demand after reopen. Partition
coverage includes an empty partition, a boundary crossing, ordered refills,
and a single shared remaining budget. Two older ordered unit fixtures were
sorted by their keys to satisfy the same precondition as Go's sorted partial
readers; their expected results remain unchanged.

Commands (repository root):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge_partial_limit_counts_handles_before_union_dedup_only -- --test-threads=1` — fails before (6 versus 5).
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 29 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 26 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-merge-worker-limit-{before,after,builder,hints,lint}.log`.
Files changed: executor `index_merge_reader.rs`, `driver/physical_builder.rs`,
this audit, the cardinality ExecPlan and receipt. Source is pinned master
`633a9e37f1c796ac81c203dc107025e7e65385f0`.

This closes the local worker-demand propagation gap, not distributed parity.
Ordered partition merging still needs local per-partition lookahead, and the
sequential scheduler is not Go's concurrent worker topology. Region request
boundaries, cancellation, memory ownership/accounting, complete package
inventory and workload gates remain open. No benchmark speedup, completed
package, commit or push is claimed.

## Merge statement cancellation boundary (2026-09-25)

Go's merge workers select on ctx.Done/finished while receiving partial and
table tasks. Rust's synchronous collector previously drained its partial
sources without polling the statement cancellation authority. A native
regression that installed QueryInterrupted during the first source read
returned Ok before this fix and continued reading more sources.

Collection now polls the existing StatementMemory cancellation state from
RowDecodeContext's statement context before worker startup, before/after each
partial batch, during intersection task dispatch, and before/after table-task
reads. No separate cancellation state or session flag was added. The existing
close_partials path closes every opened source even when collection fails,
preserves the collection error, and prevents double close during executor
teardown. The regression covers union, ordered union and intersection,
cancellation before startup, a single read before cancellation, empty pending
tasks and exact close counts.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib cancellation_during_partial_fetch_stops_merge_and_closes_sources -- --test-threads=1` — failed before (Ok instead of typed cancellation).
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 30 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-merge-cancel-{before,after,hints,lint}.log`.
Files changed this stage: executor `index_merge_reader.rs`, this audit,
the cardinality ExecPlan and receipt. Source reference remains master
`633a9e37f1c796ac81c203dc107025e7e65385f0`.

This proves cancellation at local worker/batch boundaries. It does not prove
concurrent worker/channel scheduling, interruption inside every long CPU-only
heap/map phase, distributed transport cancellation, or memory quota accounting
for retained handle state. Those, complete package mapping and workload gates
remain open. No package completion, benchmark result, commit or push claimed.

## Streaming unordered-union task ownership (2026-09-25)

Go's fetchLoopUnion delivers work as it receives partial batches through
bounded work/result channels. Rust instead collected Vec<Vec<HandleRef>> for
the entire union, closed partials, and only then built a table reader. This
made an outer LIMIT ineffective at stopping partial reads when it could not
be embedded into the merge (for example, a retained table residual).

Unordered union now keeps a UnionProcess containing its current partial,
per-path raw handle budget, global dedup set and output LIMIT. Each table-task
request advances the same process until one nonempty deduplicated task is
available. It does not queue the complete union. EOF, collection error or
Close releases partials and drops process state. Table-task build/read errors
also close live sources while preserving the execution error over a teardown
failure. The algebra tests drain this same state machine; there is no second
union algorithm. Ordered union and intersection retain their collection phase.

The physical regression uses two 19-row partials, table residual value>20,
executor batch size 6 and an outer LIMIT 1. Before the change both partials
were fully drained; the first raw count was 19 rather than the expected 6.
Afterward it returns row 3 after one six-handle task and never fetches the
second partial. It repeats Open/Next/Close to verify reset. Existing full-drain
and dedup cases still pass. A separate test injects a table-build failure plus
a source-close failure and verifies the original error and exactly one close.
Cancellation/error tests now exercise startup followed by first task demand,
matching production Next's separation of those phases.

Validation (repository root):

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib union_merge_delivers_table_tasks_before_draining_partials -- --test-threads=1` — failed before (19 versus 6).
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 31 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 27 passed, including streaming/reopen.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-merge-stream-{before,after,builder,hints,lint}.log`.
Files changed: executor `index_merge_reader.rs`, `driver/physical_builder.rs`,
this audit, the cardinality ExecPlan and receipt. Source reference remains
master `633a9e37f1c796ac81c203dc107025e7e65385f0`.

This removes eager unordered-union task buffering and proves earlier local
termination. It does not implement Go's concurrent workers or bounded channel
prefetch, so speculative read counts, latency and concurrent failure ordering
remain unverified. The dedup set still necessarily grows with distinct input;
its quota accounting and ordered/intersection handle ownership remain open.
Complete package inventories and sysbench/TPC-C/TPC-H/YCSB gates remain open.
No package completion, commit or push was made.

## Handle-map accounting foundation (2026-09-25)

The merge ownership audit found a dependency-level mismatch in
`tidb-txnkv::MemAwareHandleMap`: Set computed HandleMap.mem_usage before and
after every insertion. This traversed all entries twice, giving quadratic
construction, and charged shallow bytes for each entry rather than Go's
checkpointed map allocations. The first-insertion regression failed with
24 versus Go's zero-byte delta.

The implementation now uses existing `tidb-hack::MemAwareMap` for independent
integer/common domains and separate maps per partition, matching master
pkg/kv/key.go. Encoded common keys have Go string layout; common values carry
the source interface-plus-V layout, including Go padding for a trailing
zero-sized V. Rust values must implement MapValueLayout so source sizes are
explicit. Existing Clone/Debug/Default APIs remain available for values with
the corresponding bounds. Cloning recreates independent maps; Range preserves
source identities, domain traversal and early termination. Referenced key and
handle payloads remain caller-accounted, as in Go.

The exact Go oracle returns zero except at insertion 16/32: int64 maps charge
259/444, common-handle maps 694/1071, independently in partitions 41 and 42.
Common maps with empty values charge 694 at insertion 16. Native tests now
pin those values, overwrites, 192 identities across six domains, clone
independence and Range early exit. The old ordinary HandleMap API is unchanged.

Validation from repository root:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-txnkv --lib mem_aware_handle_map_accounts_at_source_checkpoints_per_partition -- --test-threads=1` — failed before (24 versus 0).
- `cargo test --offline --manifest-path rust/Cargo.toml -p tidb-txnkv --lib mem_aware_handle_map_accounts_at_source_checkpoints_per_partition -- --test-threads=1` — initial fix passed; updated the lockfile for the local tidb-hack dependency.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-txnkv --lib handle::tests -- --test-threads=1` — 3 passed, including the exact Go values.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-txnkv --test all kv_package_source -- --test-threads=1` — all 38 passed.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/kv -overlay=/private/tmp/tidb-handle-accounting-overlay.json -run '^TestRustHandleAccountingReference$' -count=1 -v` — passed; failpoint refcount restored to zero.
- `cargo bench --offline --locked --manifest-path rust/Cargo.toml -p tidb-txnkv --bench kv` — original source-mapped benchmark completed; million-handle mixed integer/common case 207.83 ms, native map case 216.50 ms in this single run.
- The same bench command with temporary `/private/tmp/tidb-handle-map-comparison.rs` replacing only the benchmark body measured the prior Set rescan algorithm versus the new map on identical integer inputs: 1,000 handles 823.917 us versus 18.459 us; 10,000 handles 105.656 ms versus 173.792 us. The benchmark file was restored byte-for-byte and has no diff. This is a local microbenchmark, not a workload-level speedup claim.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-handle-map-{before,after,source,master,bench,comparison,lint}.log`.
Changed product files: txnkv `src/handle.rs`, its Cargo.toml, and the workspace
lockfile dependency entry. No Go/Bazel inputs changed. The pkg/kv inventory
has no changes between its previous 64e8c4c pin and current master
633a9e37f1c796ac81c203dc107025e7e65385f0 (`git diff --stat` is empty).

This is foundation evidence within whole pkg/kv and planner/executor package
claims, not package completion. Existing KV integration/build gates remain
open. Merge does not yet use the corrected map, and its handle/key/task quota
ownership still needs implementation. Distributed scheduling, full package
inventories/validation and sysbench/TPC-C/TPC-H/YCSB gates remain open. No
package commit or push was made.

## Intersection map and quota ownership (2026-09-25)

Go doIntersectionPerPartition owns a MemAwareHandleMap[*int] per physical
partition. A new handle contributes its map allocation delta, ExtraMemSize,
and one pointed-to int counter. Repeated handles increment that counter in
place. The worker batches Consume at incoming-task boundaries and flushes
remaining deltas before enumerating survivors; its tracker detaches on exit.
Rust's prior BTreeMap counted membership but charged none of this state.

Intersection now uses the corrected shared MemAwareHandleMap with boxed Cell
counters and independent partition maps. Partial sources have already resolved
physical partition identity, including global-index handles, so the owning
partition map supplies that identity while the key retains its integer/common
handle domain. Common handles are validated through the shared representation.
The process accounting guard consumes map/counter/payload deltas at the same
batch boundary and detaches after the maps are dropped on success or error.
The small output intersection remains uncharged separately, matching Go's
explicit assumption in this stage.

A one-byte quota regression failed before the fix because collection returned
Ok. It now raises typed MemoryExceedForQuery with connection ID 7. For 32
integer handles on two identical paths, the statement records exactly 959
bytes (703 map checkpoint deltas plus 32 eight-byte counters), counts each
unique handle once, and returns to zero retained tracker bytes on both normal
and error exits. The test covers both one-row and default accounting thresholds.
Another regression checks common-handle payload sizes, two independent physical
partitions, membership and tracker release.

The shared map does not promise iteration order. Updated unordered
intersection tests compare membership/uniqueness/count instead of requiring a
specific arbitrary prefix under LIMIT; explicit ORDER BY tests are unchanged.
The pure pushed-limit tests continue to verify offset/count arithmetic.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib intersection_handle_memory_obeys_quota_and_releases_on_exit -- --test-threads=1` — failed before (Ok rather than quota error).
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 33 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 27 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-merge-memory-{before,after,builder,hints,lint}.log`.
Files changed: executor `index_merge_reader.rs`, the unordered native assertion
in `driver/physical_builder.rs`, this audit, the cardinality ExecPlan and receipt.
Source reference remains master 633a9e37f1c796ac81c203dc107025e7e65385f0.

No whole-package completion is claimed. Union dedup/ordered key and heap/task
accounting, concurrent worker lifetimes and distributed request accounting are
still open. Native common-handle backing capacities are charged via the shared
handle representation; no Go-allocator byte-for-byte equivalence is claimed
for different Rust allocations. Workload performance, package inventory and
all package gates remain incomplete. No package commit or push was made.


## Unordered union process memory ownership (2026-09-25)

At Go master 633a9e37f1c796ac81c203dc107025e7e65385f0,
fetchLoopUnion owns a tracker attached to the executor and defers Detach.
It cumulatively charges incoming handle capacity times eight before dedup,
including duplicate-only batches. Rust now retains the same accounting
lifetime in UnionProcess. MergeProcessMemory supplies shared consume/check
and deferred-detach ownership for union and intersection. Its union field
follows the dedup state so that state drops before tracker detachment.

The regression first failed with zero rather than 16 retained bytes. It now
checks a duplicate-only batch crossing a 20-byte quota at 32 bytes, an
unlimited run accumulating 40 bytes, unused capacity, and zero retained
tracker bytes after EOF, quota error, Close, cancellation and Drop. Drop
coverage proves tracker release, not arbitrary partial-source Close calls.

Validation commands:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib union_process_memory_lives_until_eof_error_or_close -- --test-threads=1` — failed before the fix.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 34 passed, including the new regression and intersection accounting.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` — passed.
- `git diff --check` — passed.

Logs: `/private/tmp/tidb-union-memory-{before,after,hints,lint}.log`.
Files changed in this stage: executor `index_merge_reader.rs`, this audit,
the cardinality ExecPlan and receipt. No Go/Bazel inputs changed.

Union dedup still uses BTreeSet. This accounts the Go process charge, not
all native allocations. Ordered-union keys/heap/task accounting, concurrency,
distributed requests, complete package mappings and workload performance gates
remain open. No whole package completion, commit or push is claimed.


## Shared union handle identity (2026-09-25)

Go master 633a9e37f1c796ac81c203dc107025e7e65385f0 uses kv.HandleMap
in both fetchLoopUnion and fetchLoopUnionWithOrderBy. Rust now uses the
existing tidb-txnkv HandleMap in both modes instead of executor-local tree
sets. MergeHandleSet groups these maps by the already resolved partition
ordinal; no physical ID is invented or narrowed. Incoming order still selects
the first occurrence, and map iteration never determines output order.
Integer/common conversion is shared with intersection. Intersection moves its
owned common bytes into the shared representation without an additional copy.

A characterization test passes before and after the refactor: both ordered
and unordered modes collapse duplicates across paths but preserve integer
versus common identity and distinct partitions. This is a structural refactor,
not a claimed correction of previously failing SQL results.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib union_handle_identity_is_shared_across_modes_and_partitions -- --test-threads=1` — passed before the refactor.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 35 passed after final edits.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed after final edits.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-union-map-{before,after,hints,lint}.log`.
Files changed: executor index_merge_reader.rs, this audit, the cardinality
ExecPlan and receipt. No Go/Bazel input changes. No workload throughput claim.

Next structural dependency: Go's ordered heap owns rowIdx references into a
retained taskMap; Rust currently owns key/handle values directly in each heap
entry and frees evicted entries. Go charges every retained task's idxRows and
handle payload as well as a 24-byte heap push/pop delta. Adding only that heap
charge would preserve the wrong retention lifecycle. Align retained task/key
ownership before claiming ordered memory parity. Concurrency, distributed
requests, package inventories and workload gates remain open; no package
completion or commit/push is claimed.


## Ordered merge retained tasks (2026-09-25)

Verified Go master 633a9e37f1c796ac81c203dc107025e7e65385f0:
fetchLoopUnionWithOrderBy appends each accepted batch to taskMap before
membership filtering. handleHeap retains rowIdx references, including when
heap eviction removes a row from the candidate set. The backing task/key
storage lives through processing; it is not owned by individual heap slots.

Rust HandleHeap now owns incoming PartialHandleBatch values and stores
MergeRowIndex entries. A global stable task ordinal replaces Go's pair of
partial/task ordinals; neither key ordering nor partition identity depends
on this ordinal. Comparisons and final handle lookup dereference retained
storage. Dedup remains first-writer-wins and evicting the just-pushed row still
stops that sorted path. Tasks containing duplicate or evicted rows remain
owned. The existing permutation test now verifies all 4096 input tasks/keys
remain retained under small heap bounds, in addition to ASC/DESC result and
large offset/count checks. No Rust-only query behavior was introduced.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 35 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-ordered-task-owner-{tests,hints,lint}.log`.
Files changed: executor index_merge_reader.rs, this audit, cardinality ExecPlan
and receipt. This is a structural ownership refactor; no failing SQL result
was claimed. No Go/Bazel inputs changed.

Remaining ordered-memory dependency: PartialHandleBatch currently materializes
keys as Datum vectors, whereas Go retains idxRows chunks and charges
Chunk.MemoryUsage plus handles and heap deltas. tidb-chunk already implements
source-shaped memory_usage; preserve the producer's typed chunk layout and
capacity before connecting this accounting. Do not substitute a guessed Datum
size formula or claim full ordered quota parity. Retaining Go's backing-task
lifetime may increase Rust's currently untracked memory versus the former
premature eviction drop. Quota parity, concurrency, distributed requests,
whole-package mapping and workload performance gates remain open. No package
completion or commit/push is claimed.


## Typed ordered key storage across partial boundaries (2026-09-25)

Source inspection of Go master 633a9e37f1c796ac81c203dc107025e7e65385f0
partialIndexWorker/partialTableWorker.extractTaskHandles confirms retChk is
allocated at w.batchSize, independently of a smaller pushed-limit request.
Rows are appended with their declared column types. Ordered process tasks
retain chunks, and the heap compares their rows. Rust had converted each row
to Vec<Datum> and partition merging unpacked those vectors into another queue.

MergeSortKeys now supports typed Chunk storage. Executor partial sources use
the original declared ordering-column types and adaptive task capacity, copy
projected columns through Chunk.append_row_by_col_idxs, and retain no parallel
per-row Datum vectors. Materialized test sources retain explicit values without
inventing field metadata. Partition merging retains whole batches and row
cursors, compares their key storage directly, and appends rows into a chunk
with the same types. Heap comparisons decode only the compared columns.
Existing collation/direction comparison remains shared; NULL behavior is
unchanged. Different typed partition layouts produce an internal error rather
than copying incompatible column encodings.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 36 passed, including typed NULL/string partition merging.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 27 passed; producer test now checks typed storage and full adaptive capacity despite a smaller pushed budget.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-typed-merge-{tests,builder,hints,lint}.log`.
Files changed: executor index_merge_reader.rs, driver/physical_builder.rs,
this audit, cardinality ExecPlan and receipt. No Go/Bazel inputs changed.
This structural refactor has no claimed failing SQL-result regression.

Open: Go's index retChk additionally retains handle/physical-ID columns, while
its table task is pruned to ordering columns. Rust now preserves typed key
chunks but has not matched that full index layout. Match those columns before
using chunk usage for complete ordered accounting. Process heap/handle/task
charges and producer scratch memory remain unwired for ordered mode. Decoding
typed variable-length keys during comparison may allocate temporary Datums;
measure this path before claiming a performance improvement. Full workload
benchmarks, concurrency, distributed request ownership and whole-package
validation remain open. No package completion or commit/push is claimed.


## Ordered index task output layout (2026-09-25)

Go getRetTpsForIndexScan retains ordering columns, then all handle fields,
then physical table ID when requested. It does not deduplicate a handle field
already present as an ordering key. Table tasks instead have their chunk
pruned to ordering keys before ordered processing.

ExecutorPartialHandleSource now separates comparison columns from retained
columns. The physical builder selects the index layout for embedded index
scans, including wrapped scans; it appends integer/common handle slots and an
EXTRA_PHYS_TBL_ID slot when present in the retained schema. Table sources
continue to retain only ordering columns. Partition transport retains the
entire typed chunk, while comparisons read the leading by-item columns.
No missing physical ID is synthesized.

The producer regression now checks overlapping integer key/handle slots:
index layout has two identical columns, table layout one, both preserving
adaptive capacity five under a three-row request. A temporary mutation
removing suffix retention fails at one column versus two; restoring the
implementation passes. This proves the layout test detects the former gap.

Validation commands:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge_partial_batches_grow_across_chunks_and_reset -- --test-threads=1` — passed; the same command fails with suffix retention temporarily removed and restored in a finally block.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 36 passed after restoration, including the producer regression.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-index-task-layout-{target,before,tests,hints,lint}.log`.
Files changed: executor index_merge_reader.rs, driver/physical_builder.rs,
this audit, cardinality ExecPlan and receipt. No Go/Bazel inputs changed.

Still open: exact Go request/response physical-ID requirements for global and
partitioned index variants, dedicated composite-handle layout fixtures, and
ordered tracker/producer scratch accounting. Existing SQL tests do not prove
all those variants. Chunk layout is now capable of retaining present suffixes;
this is not a claim of full distributed protocol or quota parity. Retained
suffixes increase currently untracked ordered memory; wire accounting next.
Concurrency, complete package mapping and workload performance gates remain
open. No package completion or commit/push is claimed.


## Ordered process accounting and release (2026-09-25)

Go master 633a9e37f1c796ac81c203dc107025e7e65385f0 charges 24 bytes
(the 64-bit slice header size) per handleHeap Push and releases that charge
per Pop. Each visited handle contributes MemUsage, including duplicates;
when the newly inserted handle is immediately evicted, the loop breaks before
that handle's charge. The entire retained task chunk is charged afterward.
These are source accounting rules, not Rust allocator size estimates.

HandleHeap now owns the shared MergeProcessMemory guard after its retained
storage fields. It charges push/pop deltas, shared Handle.MemUsage, the
partition wrapper charge when the table has partition metadata, and typed
Chunk.memory_usage. Membership insertion returns the shared handle's memory
usage without a second common-handle conversion. Draining now borrows the
heap so retained tasks and their tracker remain alive through final table-task
construction; all process exits drop storage and detach the tracker.

The one-byte quota regression failed before with Ok instead of an error.
It now verifies typed MemoryExceedForQuery, connection 7, and zero retained
bytes for quotas that interrupt the first push, second push, and task chunk
charge. The unlimited two-path case checks two unique heap entries, four
incoming integer handles including duplicates, and both typed chunks.
A separate early-eviction test verifies the rejected handle is uncharged
while its retained chunk is charged. All exits release process tracker bytes.

The first expected peak was wrong because a 40-byte quota stops at the second
push, before the first chunk is charged. Also, the test's Chunk.clone compacted
backing capacity; rebuilding each fixture at producer capacity corrected that
assumption. Neither was patched around in production accounting.

Validation commands:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib ordered_merge_tracks_retained_chunks_handles_and_heap_until_exit -- --test-threads=1` — failed before wiring accounting.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 38 passed after final lifetime change.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed after final lifetime change.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-ordered-accounting-{before,target,tests,hints,lint}.log`.
Files changed: executor index_merge_reader.rs, this audit, cardinality ExecPlan
and receipt. No Go/Bazel inputs changed.

Materialized algebra fixtures have no typed producer chunk and do not simulate
its charge. Real ordered executor partials supply typed chunks. Dedicated
common/partition quota fixtures, producer scratch tracking, final table-task
tracking, concurrent channel/worker lifetimes and distributed request variants
remain open. This validates the 64-bit source accounting contract, not all
platforms or native allocator byte equality. No workload performance result,
whole-package completion, commit or push is claimed.


## Executor partial scratch lifetime (2026-09-25)

Go master 633a9e37f1c796ac81c203dc107025e7e65385f0 extraction accumulates
chk.MemoryUsage for each nonempty fetched chunk, even when reusing that chunk,
and defers releasing the accumulated charge until extractTaskHandles returns.
The partial worker's tracker survives subsequent extraction calls.

ExecutorPartialHandleSource now creates its worker tracker on Open, using the
merge reader's plan label supplied by the builder. MergeScratchMemory borrows
that tracker for one extraction, charges each nonempty chunk before decoding
handles, and releases its accumulated bytes on every return, including errors.
Close drops the worker tracker even when the child Close fails. Failed Open
also detaches it. Cleanup consumes the negative charge directly, so a latched
cancellation cannot prevent release. Ordered retained chunks are accounted by
the process tracker after handoff, separately from extraction scratch.

The producer test failed before with a successful five-handle batch under a
one-byte quota. It now raises typed MemoryExceedForQuery for connection 7.
An unlimited five-row task fetched in chunks of two checks a peak of three
times the chunk allocation (Go's cumulative rule), then zero retained bytes.
The same source is drained to EOF, checking release after every extraction.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge_partial_batches_grow_across_chunks_and_reset -- --test-threads=1` — failed before; passed after, including final EOF assertions.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 38 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 27 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-partial-scratch-{before,target,tests,builder,hints,lint}.log`.
Files changed: executor index_merge_reader.rs, driver/physical_builder.rs,
this audit, cardinality ExecPlan and receipt. No Go/Bazel inputs changed.

The direct unordered index cursor still bypasses an executor chunk and needs
its own source-layout/accounting audit. Final table-task memory, partition and
common-handle quota variants, concurrent worker/channel ownership and complete
package/workload gates remain open. Source-shaped charges are not a statement
that all native allocations are tracked. No whole-package completion, commit
or push is claimed.


## Completed table tasks and result consumption (2026-09-25)

Go master 633a9e37f1c796ac81c203dc107025e7e65385f0 executeTask finishes
the table reader before signaling doneCh. It retains handles, chunks and row
references. getResultTask waits for completion before exposing the task and
releases the previous task only after its replacement completes. At EOF it
keeps resultCurr until Close. Next fills MaxChunkSize across tasks, regardless
of a parent's smaller RequiredRows. Rust previously streamed the table child
directly into the parent's chunk, bypassing these ownership/error boundaries.

MergeTableTask now owns the input handles, completed chunks, stable row
references, cursor and task tracker. execute_table_task builds and drains the
retained physical tree, charges handle capacity, chunk usage and 16-byte
source row-reference capacity, and closes the child before returning. Task
errors preserve their original cause; deferred child Close errors do not
replace the task outcome, following Go. Next copies from completed task rows
up to MaxChunkSize, retains the old task while obtaining its replacement, and
keeps the final task at EOF. Close drops task and process storage.

A late-error reader first produces one row then fails. The regression failed
before because the first Next succeeded; now it returns the task error without
exposing that row. The same fixture verifies a quota failure before row
exposure and successful final-task retention of 24 bytes (one handle slot and
one row reference) until Close, when retained bytes return to zero.

The former outer-LIMIT regression expected only six partial handles. That
expectation encoded Rust's old smaller-request behavior. Go Next's explicit
MaxChunkSize loop requires six output rows: four survivors from the initial
six-handle task plus another task fetched at the doubled twelve-handle budget.
The deterministic native fixture therefore reads eighteen handles from path
zero and none from path one. This is derived from the source lifecycle, not a
claim that concurrent Go always fetches exactly eighteen handles. The prior
six-handle performance assertion is superseded; no workload speedup is claimed.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib table_task_error_is_reported_before_any_partial_rows -- --test-threads=1` — failed before buffering the completed task.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 39 passed, including final quota/release assertions.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 27 passed after correcting the source-derived LIMIT assertion.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-table-task-owner-{before,tests,builder,hints,lint}.log`.
Files changed: executor index_merge_reader.rs, driver/physical_builder.rs,
this audit, cardinality ExecPlan and receipt. No Go/Bazel inputs changed.

Still open: Go's bare-table-task handle/row count consistency check, exact
ordered result restoration for distributed reordered responses, deferred Close
error logging, direct unordered cursor accounting, common/partition quota
variants and concurrent worker/channel lifecycle. Task row-reference growth
beyond the handle count has not been matched against Go allocation growth;
current retained scan/filter/limit/top-N plans do not expand rows. Full package
inventory, variants and workload performance gates remain incomplete. No
whole-package completion, commit or push is claimed.


## Retained table-plan consistency boundary (2026-09-25)

Go master 633a9e37f1c796ac81c203dc107025e7e65385f0 executeTask checks
handleCnt against len(task.rows) only when len(w.tblPlans) == 1. A filtered
or limited table request may legitimately return fewer rows. The retained
physical plan must decide this invariant; executor wrapper count is not an
adequate substitute.

The builder now passes whether the retained table plan is a bare TableScan
into IndexMergeReaderExec. Default direct readers are bare scans; custom
retained trees explicitly carry their shape. Completed bare tasks check counts
after row accounting and fail with Go's count diagnostic before exposure.
The existing deferred reader Close now logs its error through tracing without
replacing successful task rows or the original execution failure.

A missing-row bare lookup failed before the fix because Next silently returned
success. It now returns the handle-count mismatch, exposes no rows, and releases
its tracker bytes. Existing retained filter/LIMIT/TopN tests still pass. The
late-error/success/quota fixture now also returns a deferred Close error; all
three task outcomes and cleanup assertions remain correct.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib bare_table_task_rejects_missing_handles -- --test-threads=1` — failed before.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 40 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib driver::physical_builder::tests -- --test-threads=1` — 27 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib table_task_error_is_reported_before_any_partial_rows -- --test-threads=1` — passed with the added deferred Close failure.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-table-consistency-{before,tests,builder,hints,close,lint}.log`.
Files changed: executor index_merge_reader.rs, driver/physical_builder.rs,
this audit, cardinality ExecPlan and receipt. No Go/Bazel inputs changed.

This closes the count-check and deferred-close-logging gaps recorded above.
Direct unordered cursor accounting, distributed ordered-result restoration,
common/partition variants and concurrent worker/channel ownership remain open,
as do complete package inventories and workload performance gates. No package
completion, commit or push is claimed.


## Shared typed merge comparators (2026-09-25)

Go handleHeap compiles chunk.GetCompareFunc once per by-item and compares
retained chunk values directly. Rust's new typed storage initially decoded a
fresh Datum for both sides on every comparison, including allocating string
values. tidb-chunk already provides source-shaped compiled column comparators
used by native Sort; duplicating typed comparison logic would create another
parity boundary.

Merge and partition workers now cache those shared comparators once. Leading
key field types are retained with the cache; comparisons use the compiled path
only for matching typed layouts. The by-item collation is applied when
compiling, and direction reversal remains in the shared merge order loop.
Materialized fixture keys, unsupported comparator types or incompatible
layouts retain the existing Datum fallback. Partition Open resets its cache.
String comparisons on the compiled path borrow column bytes rather than
materializing temporary string Datums.

The differential test checks every pair among NULL, case variants, trailing
spaces, Unicode and a long string, under binary/utf8mb4_general_ci and both
directions. It also checks that the typed comparator was compiled.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib index_merge -- --test-threads=1` — 41 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1` — 24 passed.
- `make lint` and `git diff --check` — passed.
- `python3 /private/tmp/tidb-merge-comparison-probe.py` — passed; temporary diagnostic test ran the old typed-Datum decode and new compiled-column path in the same debug binary for 100,000 comparisons of 513-byte strings. Three old/new times: 26.061/18.603 ms, 26.205/18.512 ms, 25.494/18.755 ms. The script restored the source file byte-for-byte in finally. This is a debug diagnostic, not a release or SQL workload benchmark.

Logs: `/private/tmp/tidb-merge-compare-{tests,hints,lint,probe}.log`.
Files changed: executor index_merge_reader.rs, this audit, cardinality ExecPlan
and receipt. No Go/Bazel inputs changed. No new comparator algorithm or SQL
feature was added; this reuses the shared Go-shaped authority.

Direct cursor accounting, distributed ordered-result restoration, complete
common/partition variants, concurrent worker/channel ownership and whole
package gates remain open. sysbench/TPC-C/TPC-H/YCSB performance has not been
verified by this diagnostic. No package completion, commit or push is claimed.


## Session NDV scaling authority and two source SQL mappings (2026-09-25)

Whole-package review found two ndv_test.go cases still represented only by
ignored placeholders. Exact SQL/ANALYZE fixtures reproduced real failures:
TestOptScaleNDVSkewRatioSetVar estimated 10.20 at ratio zero instead of 19.44;
TestIssue54812 estimated 9.18 groups instead of 65.23, while its selection
already correctly estimated 100 rows over 1100 input rows.

Root cause: the SQL variable existed but was absent from the statement's
optimizer snapshot. Initial datasource attachment and recursive filtered
statistics used the default scale-NDV ratio, and physical dispatch was
constructed with literal 1.0 despite already having a scaling field. This was
a disconnected session authority, not a defect in the shared ScaleNDV formula.

The statement snapshot now carries scale_ndv_skew_ratio into logical rule
context, datasource initialization, recursive datasource/selection derivation,
join-reorder context cloning, group-NDV refresh and physical dispatch's existing
skew_ratio field. Two physical selection rescaling calls also use that existing
field. LogicalSelection now reuses StatsInfo.scale, preserving histogram and
version metadata while explicitly clearing group NDVs as Go does; cached
stats/reload behavior stays intact. Native default-only entry points retain
Go's default value for callers without a session.

Both complete source SQL cases are active in tests_explain. The three SET_VAR
ratios produce 19.44, 14.82 and 10.20; issue 54812 matches every brief-plan row.
Original ignored source artifacts now point to these active mappings. The
statement snapshot test verifies that changing the scale ratio does not mutate
an earlier statement context. No estimates were hardcoded to fit the tests.

Validation:

- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib ndv_skew_ -- --nocapture --test-threads=1` — both new SQL cases failed before; final non-nocapture run passes all 3 tests including snapshot isolation.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib derive_stats -- --test-threads=1` — 38 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1` — 411 passed.
- `cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1` — 103 passed.
- In `/private/tmp/tidb-go-master-20260923`: `GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^(TestOptScaleNDVSkewRatioSetVar|TestIssue54812)$' -count=1` — passed; failpoint refcount returned to zero. The copied ndv_test.go hash matches origin/master blob c8cf0c30f9bfa66f7342d59bc12ad7ed061c7a1f.
- `make lint` and `git diff --check` — passed.

Logs: `/private/tmp/tidb-cardinality-ndv-{sql,after,final,planner,logical,explain,go,lint}.log`.
Changed files: session stmt_ctx.rs and tests_explain.rs; executor
 driver/planner_bridge.rs; planner plan_cost_ver2.rs, find_best_task/dispatch.rs,
logical/rule.rs, logical/rewrite.rs, logical/selection.rs,
logical/rule_collect_plan_stats.rs, logical/rule_join_reorder.rs,
logical/rule_tests.rs, tests/cardinality_ndv_skew_source.rs; this audit,
cardinality ExecPlan and receipt. No Go/Bazel inputs changed.

The full package is still incomplete. Remaining literal-default scaling in
other task/final-mode/native-only helpers needs source-by-source review, as do
async histogram loading and all remaining source/test/support mappings. The
103-test EXPLAIN result supersedes historical reports of ordering/merge-join
failures above; it does not prove complete cardinality parity. Concurrency,
distributed variants and sysbench/TPC-C/TPC-H/YCSB gates remain open. No package
completion, commit or push is claimed.

## Statement statistics context through task conversion (2026-09-25)

The pinned Go `pkg/planner/core/operator/physicalop/task.go:47` obtains both
Selectivity inputs and `StatsInfo.Scale` settings from the same PlanContext.
Rust retained only estimator options on CopTask/MppTask and substituted 1.0
for NDV scaling at the shared root-selection boundary. This was a lost context
across the candidate lifecycle, affecting ordinary and merge paths alike.

`TaskStatsContext` now carries estimator options and the statement scale ratio.
The dispatcher creates it through one method for all ordinary, union and
intersection candidates. MPP UnionAll/Sequence attachment and exchange
construction copy the whole snapshot. Root conversion consumes it for both
selectivity and NDV scaling. Derived Default preserves Go's ratio 1.0 via the
snapshot's explicit default implementation; a plain f64 default of zero would
have silently changed native task callers.

The new regression observed NDV 8 instead of 10 at ratio zero before the
scaling fix. It now checks table, index, union, intersection and MPP conversions
at ratios 0, 0.5 and 1, with identical selection row counts. The exchange test
also checks retention of a nondefault ratio. Production edits are in task.rs,
enforce.rs, dispatch.rs and both index_merge candidate builders.

Validation commands run from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib root_task_conversion_retains_statement_ndv_scaling -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib enforce -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

The focused first command failed before the fix; the final task suite includes
it and passes (109 tests). Exchange 6, candidates 49, EXPLAIN 103 and lint all
pass. Logs: `/private/tmp/tidb-task-ndv-{before,after,enforce,candidates,explain,lint}.log`.
No new Go sources/dependency/Bazel changes require bazel_prepare. Go reference
was inspected at the existing pinned master; no new Go oracle execution or
workload benchmark was performed in this step. Remaining literal-default
helpers, grouped-range ordering, MV/residual/cacheability paths and complete
package inventory/gates remain open. No package completion or performance
claim, commit or push is made for this partial stage.

## Three-stage multi-distinct statistics flow (2026-09-25)

The literal-default audit found that Go master `pkg/planner/core/task.go:1927`
explicitly avoids scaling NDVs for grouping-set Expand. Go clones with Scale(1),
multiplies only RowCount, attaches Expand to the task, and constructs the partial
projection from the resulting task profile. It then estimates partial aggregate
statistics against that expanded profile. Rust instead called Scale(number of
grouping sets), retained the original child profile on the projection, and also
used the original child profile in partial aggregation estimation.

`final_mode_agg.rs` now builds one expanded profile using Scale(1) followed by a
row-count update, gives it to Expand and the partial projection, and passes it
to the partial aggregate estimator. `StatsInfo::set_row_count` is crate-private
and represents Go's explicit field mutation; it does not rescale NDVs. The
remaining 1.0 factor here is deliberate: the operation clones statistics before
replicating rows, rather than applying selectivity. The original child remains
unchanged.

The existing `mpp_scalar_multi_distinct_builds_expand_and_three_aggregation_stages`
regression in task.rs now uses 12 input rows, NDVs 3/5/7, a composite NDV of 8,
stats version 2 and a histogram collection. Before the fix, two grouping sets
produced NDVs 6/10/14. After the fix, Expand has 24 rows with original NDVs,
group NDVs and metadata, the partial projection has the same profile, and the
partial aggregate estimates eight groups. The child still has its source profile.

Commands from the repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib mpp_scalar_multi_distinct_builds_expand_and_three_aggregation_stages -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib final_mode_agg -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task -- --test-threads=1
    make lint
    git diff --check

The focused regression failed before and passed after; aggregation 8 and task
109 pass. Lint and whitespace checks pass. From the pinned Go reference directory
`/private/tmp/tidb-go-master-20260923`:

    GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/core/casetest/enforcempp -run '^TestMPPMultiDistinct3Stage$' -count=1

The original Go case passes (10.501s), and the wrapper returns failpoint refcount
to zero. Logs are `/private/tmp/tidb-expand-stats-{before,after,agg,task,go,lint}.log`.
Only Rust sources changed; no Bazel preparation is required. This verifies the
physical-profile correction, not full Rust TiFlash SQL execution or the complete
Go golden matrix in Rust. Full package inventories/gates and sysbench/TPC-C/
TPC-H/YCSB benchmarks remain open; no package completion, commit or push.

## Remove physical NULL re-estimation (2026-09-25)

Go master `pkg/planner/core/find_best_task.go` constructs PointGet/BatchPointGet
residual selections from datasource StatsInfo scaled by ExpectedCnt, and an
ordinary index-lookup table selection from finalStats. Those profiles already
include all pushed predicates. Rust's two physical conversion branches applied
an additional NULL-only selectivity multiplier to that final profile. This was
an independent estimation path after logical derivation, not a missing NULL
formula.

`find_best_task/dispatch.rs` now reuses the final datasource profile with only
ExpectedCnt scaling, preserving the separately derived runtime index-join profile.
The unused `DataSource::derive_stats` method, which supported only NULL conditions
and default NDV scaling, is removed. Its lower-level NULL range estimator helpers
are compiled only for their existing unit regressions; production estimates come
from the shared logical Selectivity path.

The new SQL regression `physical_null_filters_reuse_derived_datasource_statistics`
creates 100 rows with an integer primary key, an index on a=i%10, and a half-NULL
nonindexed b. It checks both IS NULL and IS NOT NULL through forced index lookup
(a<5) and BatchPointGet (20 IDs). Pinned Go master estimates 25 rows for each
lookup selection and 10 for each batch-point selection. Rust failed before at
12.50 for the lookup; all four cases now pass, as do execution result counts
(30/20 for lookup predicates and 10/10 for batch-point predicates).

Validation from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib physical_null_filters_reuse_derived_datasource_statistics -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
    make lint
    git diff --check

The focused regression failed before and passed after. EXPLAIN 104, candidates
49, lint and diff checks pass. From `/private/tmp/tidb-go-master-20260923`:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-null-lifecycle-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestNullFilterLifecycleReference$' -count=1 -v

This source-overlay oracle passes (0.480s) and logs all four complete plans.
The wrapper restores failpoint refcount zero. The fixture is in
`/private/tmp/tidb-null-lifecycle-reference_test.go`; logs are
`/private/tmp/tidb-null-lifecycle-{go,before,after,explain,candidates,lint}.log`.
No Go source/import/Bazel files changed, so bazel_prepare is not required.
This removes an alternative statistics owner but does not complete the package;
remaining source mappings, integration gates and workload benchmarks remain open.
No whole-package commit/push or performance claim is made.

## Verbose costs and ordered merge child requirements (2026-09-25)

The previous audit's verbose boundary is now connected end to end. The source
contract is Go master `pkg/planner/core/common_plans.go:getOperatorInfo`, which
gets costs from the statement-aware physical cost model and its node cache,
formats two decimals, and uses N/A for nonphysical nodes. SQL parsing alone
would not satisfy that contract.

Changes: executor `explain.rs` accepts Verbose and passes a statement-configured
Ver2Coster through the retained physical tree, including CTE and scalar-subquery
roots. Planner `explain/mod.rs` retains optional estimated_cost and renders it
between estRows and task/actRows. Synthetic DML and logical wrapper nodes keep
N/A. `find_best_task/coster.rs` has an optional cache enabled only for the lifetime
of one immutable verbose plan forest. It is keyed by retained node identity,
prices parents first, and lets child display reuse reader/join-context costs.
Ordinary optimization and nonverbose/binary rendering do not enable this cache.
The new cache regression checks retained reader child costs and ordinary root
cost equivalence without adding duplicate cache entries.

The session regression initially failed with unknown EXPLAIN format. It now
checks plain six-column and ANALYZE ten-column output, unchanged estimates under
ANALYZE, statement factor sensitivity, exact Go per-node costs, and DML N/A.
For the pseudo table/filter fixture Go reports root/selection/scan costs
318680.00 / 4569000.00 / 4070000.00. With scan factor 1000, they become
271380680.00 / 4070499000.00 / 4070000000.00; Rust matches all six values.

The source TestOrderingIdxSelectivityRatioForJoin is now active as
ordering_ratio_increases_index_join_cost with its exact data and cost-factor
setup. The source TestOrderingIdxSelectivityRatioForMergeJoin is active as
ordering_ratio_increases_merge_join_cost with its two 320-row inputs and required
MergeJoin shape. Before the planning correction, all four merge ratios returned
1283656.75. Source inspection found Rust natural-merge children stayed unbounded.
Go `physical_merge_join.go:GetMergeJoin` calls CalcChildExpectedCnt on both
naturally ordered children when ExpectedCnt is below join rows; enforced merge
children stay unbounded. Dispatch now makes that distinction and scales merge
output statistics by ExpectedCnt for both forms, matching Go initialization.
Both tests now prove equal costs for -1/0 and strictly increasing costs for 0,
0.5 and 1. The Apply mock-statistics source case remains unmapped.

Validation commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib verbose_explain_reports_statement_costs_and_runtime_columns -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib ordering_ratio_increases_ -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

The verbose regression and merge-source regression fail before their respective
fixes and pass after. Full EXPLAIN 107, candidate/cost 50, both join cases and the
DML follow-up pass; lint and whitespace checks pass. In the pinned Go reference:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-verbose-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestVerboseCostReference$' -count=1 -v

The exact-cost oracle passes (0.444s) and restores failpoint refcount zero; the
original five ordering suites already passed in the preceding mapping audit.
Logs `/private/tmp/tidb-verbose-{before,after,exact,dml,join,joins,joins-after,planner,explain,go,lint}.log`.
Rust-only changes do not require bazel_prepare. Legacy cost-model parity, all
remaining source/test mappings and full workload benchmarks are not newly
verified. This is incomplete whole-package work, with no commit/push.

## Apply outer-input statistics and complete ordering cost mapping (2026-09-25)

`tests_explain::ordering_ratio_increases_apply_cost` now executes the complete
Go TestOrderingIdxSelectivityRatioForApply setup: two mocked 1000-row tables,
NDV 1000 per column, full-load v2 histogram objects, and intentionally
one-component histogram keys for the composite ibc index. The mock installer
now accepts an explicit table name and preserves index metadata arity separately
from those source fixture keys. No approximate real-data fixture replaces the
Go mock. The test requires Apply at all four ratios, equal -1/0 cost and strictly
increasing cost from 0 through 0.5 to 1. Together with the preceding index/merge
cases this closes the five ordering-source test mappings, not the package.

Source inspection of `exhaust_physical_plans.go:exhaustPhysicalPlans4LogicalApply`
identified two additional structural deltas. Go only uses CalcChildExpectedCnt
for an ordered Apply; its unordered outer property stays MaxFloat64. Rust had
an independent arithmetic copy that also capped unordered inputs. Go estimates
cache hits with correlated-column NDV over the outer child's stats, because
Apply may multiply rows for LATERAL or shrink them for semi joins. Rust read the
Apply output profile for both numerator and denominator.

Dispatch now retains outer_stats and uses it for outer row count and cache
eligibility. Ordered Apply shares calc_child_expected_cnt with index and natural
merge joins; unordered Apply remains unbounded. Two regressions failed before:
an unordered 100-row outer was capped at 50, and 100 distinct outer keys became
cacheable because Apply produced 1000 rows. They now pass, and a repeated-key
case remains cacheable even when the Apply output is smaller. The ordered
fixture retains the source expected count of 95.

Validation from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib ordering_ratio_increases_apply_cost -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib apply_ -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

The source SQL case passes; Apply 31, candidate/cost 52 and EXPLAIN 108 pass.
Lint and whitespace checks pass. From `/private/tmp/tidb-go-master-20260923`:

    GOTOOLCHAIN=go1.25.12 ./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestApplyCacheEnabledByOuterRowCount$' -count=1

The original Go cache test passes (0.524s), with failpoint refcount restored to
zero. All five original ordering tests passed in the preceding source audit.
Logs: `/private/tmp/tidb-apply-cost-source.log`,
`/private/tmp/tidb-apply-context-{before,after,final,candidates,explain,lint}.log`,
and `/private/tmp/tidb-apply-cache-go.log`. Only Rust code/test metadata changed;
no bazel_prepare is required. The full Go cache test's runtime LATERAL/duplicate
upstream SQL fixture has not been ported in this step; cache admission is tested
at its production enumeration boundary. Full package and workload gates remain
open, with no commit/push or performance claim.

## Apply runtime and published-rule lifecycle (2026-09-25)

The full SQL shape from Go TestApplyCacheEnabledByOuterRowCount is active in
`tests_explain::apply_cache_runtime_uses_repeated_outer_keys`: 2000 inner rows,
50 unique outer rows, 500 repeated outer rows, and an upstream join that duplicates
the initially unique outer keys. The fixture blacklists decorrelation, analyzes
all columns, and runs EXPLAIN ANALYZE for all three shapes.

Before correction the first query became IndexHashJoin instead of Apply:
`planner_bridge.rs` initialized RuleContext.disabled_rules to empty despite the
statement retaining a published blacklist. The bridge now builds its disabled
rule set from the statement snapshot and the shared rule-name inventory. CTE
optimization receives the same RuleContext. With that fixed the next assertion
failed because Apply runtime metadata contained timing only.

`apply/native.rs` now counts cache accesses and hits at the actual lookup, resets
them on Open, and publishes enabled/access/hit state on Close through an optional
runtime sink. `physical_builder.rs` registers that sink alongside the node's
existing timing/row counter and renders Go's concurrency:OFF plus cache status
and three-decimal percentage. No per-lookup synchronization is added: the sink
is locked only when publishing Close statistics. The SQL fixture passes with
cache OFF / 90.000% / 90.000% and output counts 2000 / 20000 / 20000. It removes
the rule blacklist entry and reloads afterward, matching source cleanup.

Validation from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib apply_cache_runtime_uses_repeated_outer_keys -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib physical_builder -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib apply -- --test-threads=1
    RUST_MIN_STACK=16777216 cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib apply -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_pushdown_blacklist -- --test-threads=1
    make lint
    git diff --check

Apply SQL, 27 builder tests, 109 EXPLAIN tests, lint and whitespace pass. The
executor Apply sweep aborts on a default-stack overflow in
multiple_apply_arms_source; with 16 MiB thread stacks all 31 tests pass. This is
not evidence that the default-stack failure is resolved. The original Go cache
regression passed in the preceding turn, recorded in tidb-apply-cache-go.log.

The blacklist command initially ran zero tests. Its source file existed but was
not registered in lib.rs. Registration now activates six tests: two pass, four
fail (enum pushdown, operator/function blacklist alias, aggregate pushdown, and
cross-session reload). These stay active and are a new explicit open gate; their
expectations must be checked against current Go before choosing the next fix.
Do not report this as a green session-wide suite. Relevant logs:
`/private/tmp/tidb-apply-runtime-{before,blacklist,after,builder,executor,executor-stack,explain,lint}.log`,
`/private/tmp/tidb-apply-blacklist-{suite,active}.log`.

Remaining work includes repeated-open/nested/parallel runtime-stat merge semantics,
expression blacklist producer-to-consumer wiring, the default-stack failure,
full package inventories and workload benchmarks. No whole-package commit/push
or performance claim. Rust-only changes do not require bazel_prepare.

## Shared datasource admission policy (2026-09-25)

Go master DataSource.PredicatePushDown calls PushDownExprs with kv.UnSpecified
before ordinary and merge candidate ranges are derived. Rust still used a
TiKV-only whitelist and discarded the statement's published expression blacklist
at this boundary. That meant fixing individual merge builders would leave the
candidate population wrong. RuleContext now carries the published policy from
the executor bridge (including join-reorder subcontexts), and datasource
predicate admission uses the shared expression policy with Unspecified store.
The planner's two recursive store helpers now share one implementation using
infer_pushdown.can_function_be_pushed, retaining function/signature blacklist
checks and the Go columnToPBExpr ENUM/BIT blacklist masks.

Existing active session regressions failed before: ENUM retained its index
range, both '<'/lt aliases retained ranges, and reload on a different session
did not change the reader's plan. All three now pass; bootstrap and logical-rule
blacklist tests also pass. The aggregate test remains an active failure. Its
old expectation that no cop[tikv] operator remains was incorrect: the Go oracle
shows a root HashAgg over a cop scan. The assertion now checks one root
aggregate without a pushed HashAgg, and still detects Rust's actual defect.

A new policy regression covers function-name and resolved-signature blacklist
keys, a TiKV-only mask versus the all-engine Unspecified mask, and the ENUM
column mask. During this check, LT's signature was found missing from the
reduced pushdown catalog; the signature-key regression uses the existing GTInt
catalog entry. Full signature resolution remains an open structural dependency,
not a claim that every scalar now has complete protobuf admission. TiFlash
extra-info/type restrictions and physical aggregate/attachment context still
need complete source mapping. No benchmark improvement is claimed.

Changed production files: planner pushdown.rs, logical/data_source.rs,
logical/rewrite.rs, logical/rule.rs, logical/rule_join_reorder.rs, and executor
driver/planner_bridge.rs. Updated context/call sites in logical/rule_tests.rs,
logical/operator_tests.rs, find_best_task/index_merge_union.rs, and
core_logical_cte_topn_prune_source.rs; corrected the source expectation in
session tests_pushdown_blacklist.rs. No Go or Bazel metadata changed.

Validation from repository root (targeted to shared admission and its consumers):

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_pushdown_blacklist -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib pushdown -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

Results: blacklist 2 pass/4 fail before, 5 pass/1 fail after; policy 9 pass;
logical 411 pass; candidates 52 pass; EXPLAIN 109 pass; lint and diff check pass.
Against the existing pinned-master reference tree at
/private/tmp/tidb-go-master-20260923, this temporary source-shaped Go oracle
passed (0.41 s), with the wrapper restoring failpoint refcount to zero:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustDatasourceAdmissionReference$' -count=1 -v

Logs: /private/tmp/tidb-admission-{before,after,policy,logical,candidates,explain,go,lint}.log.
This supersedes the preceding 2/4 blacklist result. Package completion, aggregate
policy wiring, scalar catalog coverage, repeated-open Apply statistics,
default-stack Apply failure and full workload gates remain open. No whole
package completion commit or push was made.

## Published policy during physical attachment (2026-09-25)

The preceding aggregate failure was caused by both aggregate admission helpers
constructing an empty blacklist. The same lost context affected Selection,
Projection and TopN attachment. DispatchContext now receives the statement's
published policy and passes it to attach2_task_in; aggregate partial construction
and the other attachment checks consume that policy. MPP hash-aggregate
candidate enumeration receives it too, covering the one-phase route that never
calls partial construction. Static sort enforcement still supplies an empty
map because its Sort arm performs no expression pushdown admission.

The session aggregate regression now passes and matches the prior pinned-master
oracle: one root HashAgg over a root cast Projection and cop TableFullScan.
A second incorrect dormant assertion expected SUM's argument to remain a direct
table column; the Go output proves the cast/projection-column shape, which the
Rust regression now asserts. The existing test failed before the production
fix, even with the corrected single-root-aggregate requirement.

New policy coverage checks aggregate store masks independently for TiKV and
TiFlash, signature blacklisting inside aggregate arguments and GROUP BY, and
MPP candidate enumeration with engine-specific count blacklists. No source
behavior or new function catalog entries were invented. Published-map snapshots
are copied at physical search construction; no per-row policy work was added.

Changed files: executor driver/planner_bridge.rs; planner find_best_task/dispatch.rs,
task.rs, enforce.rs, final_mode_agg.rs, physical/mod.rs, physical/tests.rs;
session tests_pushdown_blacklist.rs; this audit, cardinality ExecPlan and receipt.
No Go/Bazel metadata changed. Validation commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_pushdown_blacklist -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib final_mode_agg -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib mpp_hash_agg_enumeration_matches_go_run_modes -- --test-threads=1
    make lint
    git diff --check

Results: blacklist 6 pass (supersedes 5/1), aggregate 9 pass, task 112 pass,
EXPLAIN 109 pass, MPP enumeration 1 pass, lint and diff checks pass.
Logs: /private/tmp/tidb-physical-{blacklist,policy,task,explain,mpp,lint}.log.
The pinned-master aggregate oracle from the previous section remains applicable;
no new Go test was run in this step.

The follow-up call-site audit finds context-free expression checks still in
physical/mod.rs selection/projection enumeration and access_path/index_merge.rs
residual-filter classification. Those must be compared with Go's timing and
wired through the same statement policy, not inferred complete from attachment
checks. Full scalar signature/type admission and warning propagation remain
open. Package inventory/completion, default-stack Apply failure and the four
workload benchmark gates remain open. No completion commit or push yet.

## Candidate policy and intersection root residuals (2026-09-25)

Verified master physical_selection.go and physical_projection.go check the
statement pushdown context before adding remote child properties. Rust's
selection/projection enumeration now receives DispatchContext's published
blacklist and uses the shared store policy. The new engine-mask regression
failed before (TiFlash candidate still existed) and passes after.

Verified master indexmerge_path.go removes nonpushable index filters from
intersection partials and marks them not covered. AccessPathDerivationContext
now carries the same policy through recursive logical statistics derivation
and physical runtime specialization. Intersection partial classification reads
that context. The original hinted-intersection SQL regression was extended with
'gt' blacklisted for TiKV only: logical Unspecified admission still accepts it,
but neither partial index may cover it. This failed before and passes after.

The stronger source-shaped root-placement check then exposed a second missing
boundary: the table probe unconditionally pushed all remaining filters. The
intersection conversion now separates virtual/unsupported/blacklisted table
predicates into CopTask.root_task_conds and uses the existing shared root-reader
conversion. Go's corresponding source is addPushedDownSelectionToTableScan in
find_best_task.go and CopTask.handleRootTaskConds in physicalop/task.go.

The Go oracle reports a root residual estimate of 0.80 over an intersection
estimate of 1.00. Rust initially still reported 1.00 because the shared
column_ranges_selectivity helper called BuildColumnRange directly for any
expression mentioning one column. It now first applies the existing Go-shaped
ExtractAccessConditionsForColumn checker. A calculation such as id+1>2 cannot
be misclassified as a direct range on id. The SQL regression fails before this
check and passes with the exact Go placement, estimate and result row 4.

Changed files: planner physical/mod.rs and physical/tests.rs;
find_best_task/dispatch.rs, index_merge_union.rs and index_merge_intersection.rs;
access_path.rs and access_path/index_merge.rs; logical/rewrite.rs; session
 tests_index_hints.rs; audit, cardinality ExecPlan and receipt. No Go/Bazel
metadata changed. Exact validation commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib published_blacklist_prevents_remote_candidate_enumeration -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib hinted_index_merge_intersection_retains_residual_predicates_when_disabled -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib physical::tests -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

Results: both focused regressions failed before their fixes and pass after;
physical 59, hint SQL 24, logical 411, EXPLAIN 109 pass. Lint was repeated after
the shared estimator correction and passes; diff check passes. At the pinned
master reference tree /private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustIntersectionBlacklistReference$' -count=1 -v

The Go oracle passes (0.43 s) and the failpoint wrapper restores refcount zero.
Logs: /private/tmp/tidb-enumeration-{before,after,physical}.log and
/private/tmp/tidb-residual-{before,root-before,after,hints,logical,explain,go,lint}.log.

All remaining direct calls of the context-free planner pushdown wrappers are
now tests, but this does not prove every source consumer exists. Ordinary
scan filter partitioning, union residual variants, full signature/type/warning
semantics and statistics ownership still require source coverage audits.
All previous package/workload gates and default-stack Apply issue remain open.
No package completion commit/push or performance claim was made.

## Ordinary scan residual policy (2026-09-25)

The source audit found ordinary table, covering-index and lookup scan builders
still pushed all residual predicates even though logical admission correctly
used Unspecified. A TiKV-only blacklist therefore left usable ranges intact
but incorrectly evaluated residual expressions in the cop task.

The planner now has one split_scan_filters helper matching Go's
SplitSelCondsWithVirtualColumn followed by PushDownExprs. It preserves virtual
conditions before other rejected predicates. Ordinary TiKV/TiFlash table scan
conversion, covering indexes, lookup index/table phases and intersection probes
use this helper. Root conditions survive the existing CopTask conversion.
Rejected index filters precede rejected table filters, as in Go's conversion
order. PointGet/BatchPointGet continue evaluating their residuals at root.
When a scan keeps only a subset of its original residual filters, its Selection
estimates only that pushed subset through the statement estimator and retained
table statistics; it does not reuse the all-filter datasource count.

A source-shaped regression failed before: b+1>2 was still a cop Selection on
a table scan after gt was blacklisted only for TiKV. It now covers table,
covering index and lookup cases, requires root residuals and retained equality
index ranges, verifies result rows, and pins the Go estimates (8000 table,
8 covering/lookup). The same SQL passes on pinned Go master. These tests do not
prove all mixed-filter/runtime-index-join/TiFlash statistics variants.

Changed files: planner pushdown.rs, find_best_task/dispatch.rs and
find_best_task/index_merge_intersection.rs; session tests_pushdown_blacklist.rs;
this audit, cardinality ExecPlan and receipt. No Go/Bazel metadata changed.
Commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tikv_only_blacklist_keeps_ordinary_scan_residuals_at_root -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_pushdown_blacklist -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

Results: focused regression failed before and passes after; blacklist 7,
hints 24, candidates 52, EXPLAIN 109 pass. Lint repeated after predicate-order
correction and passed; diff check passes. In the pinned-master reference tree
/private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustOrdinaryScanBlacklistReference$' -count=1 -v

Go oracle passed (0.40 s). Logs: /private/tmp/tidb-scan-policy-{before,after,
blacklist,hints,candidates,explain,go,lint}.log. No workload benchmark was run.
Full signature/type/warning parity, union residual variants, mixed-filter and
runtime statistics, package inventory/completion and default-stack Apply
failure remain open. No package completion commit/push was made.

## Index completion precedes table filtering (2026-09-25)

Master addPushedDownSelection4PhysicalIndexScan establishes a stronger timing
contract than the preceding ordinary-filter change implemented. It keeps the
path's CountAfterIndex/CountAfterAccess ratio for retained index selections,
then calls FinishIndexPlan before constructing the table selection. Only the
table selection is re-estimated from its completed input when root conditions
exist. Rust had built the table selection before the index selection and later
updated only the bottom row-ID scan; the already-built selection retained the
wrong input row count.

The physical index conversion now constructs the raw table scan, builds the
index selection, completes the index phase, then creates the table selection
from the completed scan. The prior has-root override that re-estimated index
filters was removed to preserve Go's path-count contract. Root conditions also
follow the exact index conversion order: virtual table predicates, rejected
index predicates, then other rejected table predicates. The shared virtual
split is exposed separately for this source order. The old finish_index_plan
comment claiming early datasource table selection construction was corrected.

The ordinary-blacklist regression now includes mixed index/root and mixed
index/table/root filters. Against Go master, covering input 8.00 becomes root
6.40; lookup index output 8.00 becomes table output 6.40 and root output 5.12.
The lookup case failed before (table 8.00, root 6.40) and passes after, with row
3 preserved. Index/table selection allocation order now follows the source.
This corrects and supersedes the preceding statement that both index and table
pushed subsets should independently be re-estimated.

Changed production files: planner find_best_task/dispatch.rs, pushdown.rs and
task.rs (comment); regression in session tests_pushdown_blacklist.rs; this audit,
cardinality ExecPlan and receipt. No Go/Bazel metadata changed. Commands from
repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tikv_only_blacklist_keeps_ordinary_scan_residuals_at_root -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_pushdown_blacklist -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task -- --test-threads=1
    make lint
    git diff --check

Results: focused regression fails before/pass after; blacklist 7, candidates 52,
EXPLAIN 109, task 112 pass; lint and diff checks pass. At pinned-master reference
/private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustMixedScanBlacklistReference$' -count=1 -v

Go oracle passes (0.41 s). Logs: /private/tmp/tidb-mixed-policy-{before,after,
candidates,explain,task,go,lint}.log. The oracle records plan rows and result rows;
Rust checks the captured root estimates and exact results. Broader analyzed and
runtime-index-join/mixed TiFlash variants, full signature/type/warning parity,
union residual variants, package inventory and workload gates remain open.
No completion commit/push or benchmark claim.

## Preserve source population for root selectivity (2026-09-25)

The analyzed counterpart of the blacklist regression exposed a statistics
ownership mismatch after the predicate-placement fixes. With table rows
(1,1,1),(2,1,3),(3,2,5), an a=1 index lookup produces two rows. Go computes the
b>2 root predicate's selectivity from the original three-row table histogram
and scales the lookup output to 1.33. Rust passed the two-row reader profile
as the histogram population, capped 2/2 at one, and incorrectly kept 2.00.
The focused SQL regression fails before and passes after.

TaskStatsContext now retains an optional Arc<StatsInfo> source snapshot, matching
CopTask.TblColHists ownership independently of reader row counts/NDVs. Ordinary
and intersection tasks capture it when root predicates exist; tasks with no
root predicates do not clone or allocate this snapshot. Task copies,
composition and exchange enforcement share the Arc. Root selectivity consumes
the source snapshot, then scales the current reader profile. Existing synthetic
tasks with no source snapshot retain their prior fallback. No per-row work was
added; histogram objects remain shared within the retained profile.

The existing task estimator regression now proves that reducing reader rows by
ten leaves histogram selectivity unchanged and reduces output by ten. The MPP
exchange regression asserts Arc identity, proving snapshot preservation rather
than just equivalent values. This is the original source population ownership
contract, not an estimator formula adjustment.

Changed files: planner task.rs, enforce.rs, find_best_task/dispatch.rs,
index_merge_union.rs and index_merge_intersection.rs; session
 tests_pushdown_blacklist.rs; audit, cardinality ExecPlan and receipt. No Go or
Bazel metadata changed. Validation commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib analyzed_root_filter_uses_table_histogram_population -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_pushdown_blacklist -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib enforce -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

Results: focused SQL fails before/pass after; blacklist 8, task 112, enforcement
6 and EXPLAIN 109 pass; lint and diff checks pass. At pinned-master reference
/private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustAnalyzedRootBlacklistReference$' -count=1 -v

Go oracle passes (0.43 s) and reports root Selection 1.33 over IndexLookUp 2.00,
with result row 3. Logs: /private/tmp/tidb-analyzed-root-{before,after,task,enforce,
explain,go,lint}.log. The active blacklist suite is now 8 passing tests.

Full runtime-index-join/TiFlash filter variants, union residual variants,
signature/type/warning coverage, package inventory and four workload benchmarks
remain open. Default-stack Apply failure and repeated-open runtime statistics
also remain open. No whole-package completion commit or push was made.

## Union residual alternatives: reproduced structural gap (2026-09-25)

Master generateNormalIndexPartialPath first splits each DNF branch into CNF,
checks TiKV pushability, and derives a complete access path. Its candidate
lifecycle retains uncovered filters and source-OR recheck metadata. Rust's
Partial enum instead retains only index identity/ranges/rows (or table
ranges/rows). prepare_union_index_merge_path rejects any remaining predicate,
and the physical converter has no probe/root residual stage. This is an open
structural mismatch despite the earlier shared datasource collection work.

New active regression union_merge_retains_source_or_for_uncovered_branch_predicates
uses a=1 AND c>2 OR b=2 over indexes ia(a), ib(b). Go builds a union with two
10-row partial scans, a 19.99-row table probe and source OR Selection estimated
0.03; query rows are 2,3,4. Rust currently declines the merge and falls back.
The Go oracle passes; the Rust regression intentionally remains failing and
must not be hidden or weakened. Previous 24/24 hint-suite results predate this
new active test and are not a current full-suite green claim.

A first production extraction now makes ordinary index paths and union branches
call ordinary::fill_index_path. It owns physical/usable key layout, detached
ranges and access metadata, and index/table filter partitioning. This removes
the union's independent key/detacher setup. Prefix-index policy is explicitly
carried in AccessPathDerivationContext. Ordinary estimation/adjustment still
owns its count publication; union derivation failures still decline a partial,
matching Go accessPathsForConds. This extraction does not implement residual
alternatives; the result is still reduced to the old Partial representation.

The next implementation cut must replace that reduced representation with the
shared filled path state, retaining per-alternative access and filter metadata
through property convergence. Branch CNF pushability must set recheck state;
chosen alternatives must compose their access DNF for CountAfterAccess, and
probe/root construction must retain the source OR only when required by Go.
Do not estimate union access from the original residual-bearing OR. Preserve
index-side filters, CountAfterIndex and covering layout on each alternative.
Audit multi-conjunct datasource input and table/common-handle alternatives in
the same design; do not merely remove the remaining-condition rejection.

Changed files: planner access_path/ordinary.rs, access_path/index_merge.rs,
access_path.rs, logical/rewrite.rs, find_best_task/dispatch.rs and
find_best_task/index_merge_union.rs; session tests_index_hints.rs; this audit,
cardinality ExecPlan and receipt. No Go/Bazel metadata changed. Commands:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib union_merge_retains_source_or_for_uncovered_branch_predicates -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

Results: union residual regression fails; candidates 52 and EXPLAIN 109 pass;
lint and diff check pass. At /private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustUnionResidualReference$' -count=1 -v

Go oracle passes (0.41 s). Logs: /private/tmp/tidb-union-residual-{before,go}.log,
/private/tmp/tidb-union-shared-{derive,explain,lint}.log. No workload benchmarks,
package completion commit or push. All previously recorded package, runtime,
signature/type/warning and default-stack Apply gates remain open.


## Union lifecycle metadata and residual conversion (2026-09-25)

This supersedes the preceding active-failure report for the uncovered union
branch. Go reference remains origin/master at
633a9e37f1c796ac81c203dc107025e7e65385f0. The design reference is
indexmerge_unfinished_path.go's per-alternative cleanup and access-DNF estimate,
followed by find_best_task.go's convergence and convertToPartialTableScan.

Union index alternatives now retain the shared FilledIndexPath rather than
copying only ranges and row counts. Table alternatives retain detached access
conditions, ranges, residuals and the source-OR recheck flag. Each branch's
pushable CNFs feed derivation; rejected CNFs require the original OR at the
probe or root. The candidate's access estimate composes only selected partial
AccessConds plus IndexFilters. It does not reduce row-ID probe input using
residual predicates. Physical property matching consumes the derived key
layout. The test that restores pruned datasource columns must now rederive
candidates after changing that input instead of letting costing consult a
newer schema.

Physical conversion builds retained index selections and handle-only table
selections. Like Go, any table residual conservatively requires a source-OR
recheck, including filters that remain evaluable on the handle schema.
Non-handle filters are removed from the branch selection only; the source OR
survives on the probe. The shared pushdown splitter places rejected probe
predicates at root, retaining original table statistics in TaskStatsContext.
Index LIMIT risk/threshold decisions now use the retained filter state.

The table-residual regression failed before (full scan instead of union).
After migration the five ordinary/index/table/negative-bound residual cases
return the same rows as Go. Ordered LIMIT/OFFSET and a TiKV-only gt blacklist
also retain correct rows; the blacklist cases retain the source OR at root.
Exact probe estimates include 19.99 for (a=1 AND c>2) OR b=2, 11.00 for
(id=1 AND c>2) OR b=2, and 13.00 for (id<3 AND id+1>2) OR b=2.
The last initially failed at 3340.00. Go GetRowCountByColumnRanges dispatches
its pseudo integer estimator by the first low bound's Datum kind, not the
column type's signedness. The shared pseudo-range selectivity caller now
follows that boundary. This changed UNION ALL's logical estimate from the
old 6666.67 to Go's verified 3335.33; its existing EXPLAIN golden was updated
only after a separate Go oracle confirmed it.

Changed production owners: access_path/index_merge.rs,
find_best_task/index_merge_union.rs, find_best_task/dispatch.rs (shared LIMIT
threshold helper visibility), logical/rewrite.rs (pseudo dispatch). The
preceding shared fill_index_path extraction remains part of this unfinished
migration. Session regressions are in tests_index_hints.rs and the verified
UNION ALL golden in tests_explain.rs.

Validation from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

Results: hints 25, candidates 52, logical 411 and EXPLAIN 109 pass; lint and
diff checks pass. The first standalone regression before table migration was:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib union_merge_retains_source_or_for_uncovered_branch_predicates -- --test-threads=1

It failed as recorded in /private/tmp/tidb-union-table-before.log. The later
exact-count assertion failed before the shared estimator correction. Logs
for passing suites are /private/tmp/tidb-union-residual-{hints,candidates,logical,explain,lint}.log.

Go oracle commands, run in /private/tmp/tidb-go-master-20260923 with the existing
external overlay /private/tmp/tidb-admission-reference_test.go:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustUnionResidualReference$' -count=1 -v
    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustUnionAllHandleEstimateReference$' -count=1 -v

Both pass; failpoint refcount restored to zero. Output is in
/private/tmp/tidb-union-table-go.log and /private/tmp/tidb-union-all-go.log.
The Go oracle covers the five base residual cases and UNION ALL estimate;
ordered LIMIT and blacklist extensions here were checked in Rust against the
inspected Go lifecycle, not a new differential runtime oracle.

This is not complete union or package parity. Remaining structural work:
replace the union table-range/estimate derivation with the ordinary table
lifecycle; avoid datasource-wide histogram counts for table partials; support
multiple top-level CNFs/OR candidates, empty-range alternatives, common-handle
and MV/partition variants, and plan-cache source rechecks. Source inventory,
full package gates and sysbench/TPC-C/TPC-H/YCSB remain incomplete. No package
commit or push was made.


## Shared integer table derivation and loaded statistics (2026-09-25)

The previous union lifecycle follow-up's shared integer table derivation gap
is now addressed. Both ordinary and union table alternatives call
ordinary::detach_table_path with their own condition slice. The union retains
FilledTablePath, then estimates its typed ranges using the source histogram
population and the existing GetRowCountByColumnRanges port. The ordinary
path also uses this estimator when its precomputed bridge estimate is absent;
its separate lower-bound adjustment remains in fill_table_path. Go's
core/stats.go deriveTablePathStats(isIm=true) deliberately omits that ordinary
adjustment. A derivation/estimation failure declines only the table alternative,
not an entire disjunct or the original OR predicate.

The regression now runs after ANALYZE ALL COLUMNS. Before this change, Rust
priced both partials as four rows because the union table branch reused the
datasource's ordinary count and the index branch missed its TopN-only stats.
The table-only fix produced 1/4, exposing the second failure independently.
Shared InitStats used nonempty histogram buckets as load-state evidence;
fully loaded TopN-only and empty analyzed payloads violate that assumption.
It now consumes the cache's explicit column/index load statuses for the
loaded subset, preserving evicted-state filtering without discarding TopN.
No new statistics-loading behavior is invented.

The focused Go master oracle (same pin 633a9e37f1c796ac81c203dc107025e7e65385f0)
and Rust now agree for (id=1 AND c>2) OR b=2: TableRangeScan 1.00,
IndexRangeScan 2.00, TableRowIDScan 2.50, probe Selection 1.56, and returned
IDs 3 and 4. Fail-before logs: /private/tmp/tidb-union-analyzed-before.log
(table 4 versus 1), /private/tmp/tidb-union-topn-before.log (index 4 versus 2).

Changed files: tidb-planner access_path/ordinary.rs, access_path/index_merge.rs,
find_best_task/index_merge_union.rs; tidb-executor driver/planner_bridge.rs;
tidb-session tests_index_hints.rs; this audit, the package ExecPlan and receipt.

Exact validation commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib union_merge_retains_source_or_for_uncovered_branch_predicates -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost::tests:: -- --test-threads=1
    make lint
    git diff --check

Results: focused regression passes; suites respectively 25, 52, 411, 109, 2,
and 30 pass. Lint and diff checks pass. Logs:
/private/tmp/tidb-union-analyzed-{hints,candidates,logical,explain,init,cost,lint}.log.
No Go/Bazel inputs changed in the workspace, so bazel_prepare was not required.
Go oracle command in /private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustUnionAnalyzedReference$' -count=1 -v

It passes, with failpoint refcount restored to zero; output is in
/private/tmp/tidb-union-analyzed-go.log. The oracle is an external source-shaped
test, not a replacement for full original Go-package artifact/test coverage.

Remaining structural risks include the bridge's precomputed ordinary estimates,
union index fallback to datasource-wide counts when its v2 histogram path is
unavailable, version-1 index stats, common-handle/partition variants, multiple
top-level CNFs/OR candidates, correlated predicates and plan-cache rechecks.
No full-package completion, workload performance result, commit or push claim.


## Candidate-owned index fallback estimates (2026-09-25)

This supersedes the previous open union fallback-to-ordinary-count item.
An analyzed table followed by ADD INDEX ibc(b,c) reproduced the mismatch:
for the (id=1 AND c>2) OR b=2 union, Rust assigned the new index branch the
ordinary path's 4.00 count. Go uses the b/c column statistics when the index
histogram is missing, producing 2.00. The test failed before and now passes,
with returned IDs 3 and 4 unchanged.

The cardinality owner now exposes get_index_row_count_with_partial_stats.
It composes GetRowCountByColumnRanges with the existing source-ported partial
statistics arithmetic; both executor ordinary index estimates and planner
union estimates call it. Async statistics-load requests remain at the existing
collection owner. Union estimates use their own normalized key layout (including
eligible handle suffixes), own ranges and source histogram counts. They no
longer fall back to a derived ordinary path count. Without usable index/column
statistics they use the shared pseudo index range estimator. Estimation errors
decline that alternative, as accessPathsForConds does in Go. The existing v2
histogram calculation is reused; full v1 CMS index routing remains unproven.

Fallback review also found a shared representation defect: index pseudo range
conversion tried to encode string bounds as f64 and replaced failed conversions
with full-range sentinels. A string point consequently estimated 9990 rather
than 10. Pseudo index formulas require only bound kinds and equality, so the
bridge now records equality using each range's collation-aware datum comparison.
A regression covers string points, string intervals and adjacent u64 values
that collapse when converted to f64. It failed before and passes after.

Changed production files: tidb-planner/cardinality/row_count_estimator.rs,
tidb-planner/access_path/index_merge.rs, tidb-planner/ranger/mod.rs, and
tidb-executor/access_cost.rs. Session regression: tests_index_hints.rs.
Building the combined source-test target exposed a stale RuleContext literal
in core_logical_cte_topn_prune_source.rs; its missing scale_ndv_skew_ratio now
uses DEF_SCALE_NDV_SKEW_RATIO, consistent with other test contexts.

Exact validation commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib union_merge_retains_source_or_for_uncovered_branch_predicates -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib pseudo_index_bounds_preserve_datum_equality -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib ranger:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost::tests:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
    make lint
    git diff --check

Results: regression passes; ranger 69, access-cost 30, candidates 52, hints 25,
EXPLAIN 109 and source estimator 14 pass. Lint/diff checks pass. The first
attempt used nonexistent --test row_count_estimator_source; the supported
combined --test all invocation above compiled after the context fix and passed.
Logs: /private/tmp/tidb-union-partial-stats-{before,after,cost,candidates,hints,explain,source,lint}.log;
/private/tmp/tidb-union-pseudo-bounds-{before,after}.log. No workspace Go/Bazel
inputs changed, so no bazel_prepare gate was triggered.

Source-shaped Go oracle in /private/tmp/tidb-go-master-20260923, still pinned
to 633a9e37f1c796ac81c203dc107025e7e65385f0:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustUnionMissingIndexReference$' -count=1 -v

It passes (0.42 s), confirming branch 2.00, table branch 1.00, probe input
2.50 and probe selection 1.56. Failpoints returned to refcount zero; log:
/private/tmp/tidb-union-partial-stats-go.log.

Remaining scope includes CMS-backed v1 index dispatch, complete histogram
validity/load-status propagation, ordinary cached estimates, correlated and
partition/common-handle variants, multiple top-level OR/CNF candidates,
plan-cache rechecks and empty ranges. The full Go package inventory, all
original source/test mappings, package gates and workload performance remain
incomplete. No package commit or push was made.


## Unfinished normal-index OR candidates and top-level predicates (2026-09-25)

The previous single-OR-only gate is removed for normal index paths. Reference:
Go core/indexmerge_unfinished_path.go generateORIndexMerge,
initUnfinishedPathsFromExpr, mergeANDItemIntoUnfinishedIndexMergePath and
buildIntoAccessPath; core/find_best_task.go convergence and
removeCoveredIndexMergeTopLevelFilters, at the same master pin
633a9e37f1c796ac81c203dc107025e7e65385f0.

Logical preparation iterates all top-level conditions and retains a separate
candidate for each OR. Normal alternatives first try their existing range
builder. If that does not produce a usable range, the unfinished stage retains
one equality/IN predicate per declared index column, preserving the original
branch recheck flag. Usable top-level predicates are then collected for that
same index before final range derivation. This allows b=2 to survive for
index(a,b) until the separate a=1 predicate supplies the missing prefix;
it does not indiscriminately distribute every global predicate to every path.
Integer table alternatives keep their direct range admission boundary.

The logical candidate owns the chosen source OR, remaining top-level filters
and plan-cache context. Convergence removes a global predicate only when every
selected normal index alternative carries that exact predicate as an access
condition and covers its full value. Table alternatives retain global rechecks.
Source-OR rechecks also honor the existing MaybeOverOptimized4PlanCache port.
Go recomputes the access DNF estimate after selecting property-compatible
alternatives; Rust now does the same through shared estimate_union_access.
The old test forbidding all expression evaluation during merge convergence
was stricter than Go: the statistics pass evaluates constants. It now allows
that pass while still checking no physical allocation and retained logical
alternatives. Ordinary table/index conversion retains its no-rederivation guard.

Regression fixture: indexes (a,b), (a,c), condition a=1 AND (b=2 OR c=3).
Before, Rust produced an intersection with an OR probe residual. After, Rust
and Go produce a union with [1 2,1 2] and [1 3,1 3] branches, no redundant
selection, estimates 0.10 for each branch and 8.00 for probe input, and IDs
1 and 2. The same Go/Rust matrix includes global d>8 (ID 1), another OR
(b=0 OR c=0) (IDs 1,2), and ORDER BY id LIMIT 1 OFFSET 1 (ID 2).
A planner fixture separately verifies both top-level OR candidates survive
logical preparation with distinct source filters.

Changed production files: access_path.rs, access_path/index_merge.rs,
find_best_task/index_merge_union.rs and find_best_task/candidate_preparation.rs.
Tests: tests_index_hints.rs and the existing union candidate lifecycle fixture.
The ExecPlan and cardinality receipt are updated; no Go/Bazel inputs changed.

Exact commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib union_merge_completes_composite_ranges_from_top_level_predicates -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

The new regression failed before; focused count checks now pass. Suites: hints
26, candidates 52, logical 411 and EXPLAIN 109 pass. Lint/diff checks pass.
Logs: /private/tmp/tidb-union-cnf-{before,after,counts,hints,candidates,logical,explain,lint}.log.
Bazel preparation was not required for these Rust-only changes.

Go oracle, from /private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustUnionCNFReference$' -count=1 -v

All four queries pass (0.39 s), failpoint refcount restored to zero; log:
/private/tmp/tidb-union-cnf-go.log. Source files in the workspace remain unchanged.

This closes the ordinary composite-index/multiple-top-level-OR structural
restriction, not the complete source generator. MV/common-handle/partition
alternatives, empty ranges, full plan-cache/error/type/collation matrices,
CMS-backed v1 estimation, histogram validity, original source inventories and
full package/workload gates remain open. No package commit or push was made.


## Retained statistics versus estimation validity (2026-09-25)

The loaded-statistics review found a second ownership mismatch beyond TopN:
InitStats copied raw column payloads into HistColl without their load status,
while filtering index payloads/maps to fully loaded entries. Go keeps those
concerns separate. ColumnStatsIsInvalid rejects a nonzero-NDV column whose
essential statistics are unavailable, even if retained NULL counts make its
TotalRowCount nonzero. IndexStatsIsInvalid requests loading independently but
still permits a retained nonzero index payload, regardless of full-load status.

HistColl now retains column statuses keyed by planner unique ID. Its raw
histogram accessor remains for existence, initialized metadata and correlation
metadata reads; histogram_for_estimation applies the source payload gate.
ColumnStats::is_valid_for_estimation is shared with the cache's existing
column_for_estimation method so the two boundaries cannot encode different
rules. The catalog bridge passes explicit states and retains all existing
index payloads, index-column maps, leading-index maps and scaled row counts.
Loaded subsets still control their existing NDV-loading decisions. No async
load-request behavior is added or moved by this change.

Estimation consumers updated: logical range/NULL/string/IN selectivity,
ordinary integer table ranges, union index ranges and recursive column inputs,
and correlation range estimates. Raw metadata remains accessible even when
an estimate must fall back to pseudo statistics. Profiles preserve validity
through scaling via their existing shared histogram collection.

The bridge regression constructs an evicted column and index with retained
NULL count 3 and realtime count 100. With column NDV 2, the old snapshot
incorrectly reported usable column stats (true versus cache false), recorded
in /private/tmp/tidb-hist-validity-before.log. After passing status, the NULL
range estimate is 0.1; with NDV 0, the retained NULL-only column remains valid
and estimates 100. The retained index stays available in both cases. The
Go oracle independently confirms those two validity decisions and counts.
The regression also verifies state survives a scaled profile.

Changed owners: tidb-planner/stats_info.rs,
cardinality/row_count_estimator.rs, cardinality/cross_estimation.rs,
logical/data_source.rs, logical/rewrite.rs, access_path/ordinary.rs,
access_path/index_merge.rs; tidb-executor/access_cost.rs and
 driver/planner_bridge.rs (including the boundary regression).

Exact validation commands from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_collection_preserves_estimation_validity -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost::tests:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality::cross_estimation -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

Results: regression passes; initialization 3, access-cost 30, correlation 7,
logical 411, hints 26 and EXPLAIN 109 pass. Lint/diff checks pass. Logs:
/private/tmp/tidb-hist-validity-{before,after,init,cost,cross,logical,hints,explain,lint}.log.
Only Rust/doc files changed; no bazel_prepare trigger was introduced.

Go oracle in /private/tmp/tidb-go-master-20260923 at the same source pin
633a9e37f1c796ac81c203dc107025e7e65385f0:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustHistogramValidityReference$' -count=1 -v

It passes and reports NDV 2 -> 0.1, NDV 0 -> 100. Failpoints return to zero;
log /private/tmp/tidb-hist-validity-go.log. The oracle disables load triggering
to isolate the estimation gate, so this is not async-loading lifecycle proof.

Remaining scope includes restricted-SQL/context policy, complete async/sync
loading state transitions and usage reporting, CMS-backed v1 index dispatch,
common-handle/MV/partition/empty-range and cache variants, original source/test
inventory and full package/workload gates. Benchmark effects are unverified.
The package remains incomplete; no commit or push was made.


## Shared version-1 statistics dependencies (2026-09-25)

Source pin remains Go master 633a9e37f1c796ac81c203dc107025e7e65385f0.
The index-dispatch audit found three dependency/ownership mismatches before
another merge-specific estimator could be justified:

- Go cardinality calls statistics.EnumRangeValues. Rust cardinality carried an
  integer-only copy even though tidb-stats already implements the source's
  integer, duration, DATE, DATETIME and TIMESTAMP enumeration. Cardinality now
  re-exports and consumes the shared implementation, preserving nil versus an
  empty enumerated range.
- Go equalRowCountOnColumn calls statistics.QueryValue, which calls
  tablecodec.EncodeValue. Rust cardinality queried an integer-only CMS helper
  and converted every unsupported datum error to zero. The shared Rust
  QueryValue wrapper also used codec.EncodeValue directly: this is a different
  encoding for temporal values. QueryValue now depends on tidb-tablecodec and
  uses encode_table_value, with the existing SessionTimeZone boundary.
- equal_row_count_on_column now returns Result. Its point, enumeration and
  endpoint-adjustment callers propagate encoding failures. EstimationError
  retains TableRowError; Clone/PartialEq derives were removed because that
  error owns non-clonable/non-comparable source errors. No failure is turned
  into an estimated zero at this boundary.

The initial fixture failed for noninteger CMS points (0 versus 14), and the
range fixture remained wrong after only the point codec fix (60.5 versus 44).
After the shared dependency fixes, the Rust matrix matches the Go oracle:
REAL/bytes/duration/DATE/DATETIME/TIMESTAMP equality 14 at realtime/analyzed
counts 200/100; four temporal range endpoint variants 44/40/18/14. Rust also
checks TopN precedence and propagation of an unsupported raw datum. The shared
statistics test verifies duration flattening and a +08:00 TIMESTAMP CMS/TopN
lookup (7/9); Go confirms the latter's key 098080808080c0d0d219.

The first Go oracle fixture accidentally inserted codec.EncodeValue bytes.
Its duration estimate of 1 exposed the flattening difference; the fixture was
corrected to tablecodec.EncodeValue before using it as reference evidence.
Cardinality currently passes UTC to this dependency: statement time-zone and
statement error-policy propagation remain open, and the +08:00 statistics
leaf test does not establish end-to-end planner time-zone parity.

Rust ANALYZE currently emits version 2 (analyze.rs::STATS_VERSION_2), so its
sampling/NDV codec was not changed into a version-1 sketch producer. Existing
LOAD STATS and remote version-1 sketch ingestion need lifecycle validation.

Changed files in this step: rust/crates/tidb-planner/src/cardinality/row_count_estimator.rs,
rust/crates/tidb-planner/tests/row_count_estimator_source.rs,
rust/crates/tidb-stats/src/cmsketch.rs,
rust/crates/tidb-stats/tests/cmsketch_source.rs, both crates' Cargo.toml files,
rust/Cargo.lock, this audit, both planner/statistics package ExecPlans, and
rust/testport/receipts/planner_cardinality.md. Earlier WIP in these files is
preserved.

Validation (all commands from repository root unless specified):

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib version_one_ -- --nocapture
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-stats --test all cmsketch_source -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-stats --test all scalar_enum_source -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost::tests:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    make lint
    git diff --check

Passing counts: new regressions 3 (included in cardinality 28), source estimator
14, CMS source 33, enumeration source 3, access cost 30, logical 411, hints 26.
Total non-overlapping Rust checks: 545. Lint passed. Only Rust/Cargo/docs were
changed, so no bazel_prepare trigger. Cargo updated local dependency edges in
Cargo.lock offline; the first dependency-refresh test omitted --locked, and
subsequent commands used --locked. The pre-existing lockfile changes remain.
Logs: /private/tmp/tidb-v1-values-{before,codec,after,cardinality,source,stats,
cms-source,enum-source,cost,logical,hints,lint,go}.log.

Go oracle from /private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustVersionOneTypedValuesReference$' -count=1 -v

Passes (0.056s), failpoint refcount restored to zero. The temporary overlay
contains the matching histogram/CMS matrix; it does not change repository Go
sources or package membership.

### Remaining version-1 dispatcher design requirements

Ordinary access_cost, union estimate_partial_index_ranges, correlation ranges,
and recursive exponential backoff still call the V2 routine directly. A shared
GetRowCountByIndexRanges boundary must dispatch on StatsVer==1 AND CMS presence.
Its context must preserve Go's distinction between raw NDVs (out-of-range
prefix estimation) and valid column histograms (cross validation), column
IsHandle, per-range collations, scaled index counts and unscaled table counts.

For a suffix range, Go prefers the first ColUniqueID2IdxIDs entry even if the
column itself has usable statistics; the recursive call must retain normal
invalid-index/pseudo/partial-statistics behavior. Rust's RecursiveIndexStats
currently filters for loaded usable candidates and was shaped only for V2
backoff. Reusing it unchanged would silently omit source branches. Single-column
NULL and non-enumerable leading ranges call V2 with nil collection/idxCols.
V1 caps the accumulated count at analyzed index rows without V2's final growth
scaling. Source getEqualCondSelectivity also requires exact full-key width,
not Rust's current >= check; the index QueryBytes and cross-validation ordering
need verification as part of this same dispatch unit.

These remain explicit open structural requirements. No V1 index dispatcher was
integrated or claimed complete. Common-handle/MV/partition/cache/load lifecycle,
complete package artifacts/gates, and sysbench/TPC-C/TPC-H/YCSB validation remain
open. No package completion, benchmark improvement, commit or push is claimed.


## One range representation for cardinality and access paths (2026-09-25)

The preceding V1 context audit exposed a structural loss at the range boundary:
Go passes ranger.Range into cardinality, while Rust copied its bounds into
IndexRangeDatums and discarded Collators. IndexRangeDatums is now an alias of
the existing ranger.Range rather than another struct. Union partial estimates
and correlation estimates borrow the original ranges directly. The executor's
storage-range adapter supplies collators from the normalized physical key's
column metadata, including appended handle positions. Recursive index backoff
retains the selected dimension's collator in its one-column range.

The same duplication existed in AccessPath.OnlyPointRange: its binary-only
local predicate is removed in favor of ranger.Range::is_point. These source
operations intentionally use different collator positions: getOrdinalOfRangeCond
compares all positions with Collators[0], whereas Range.isPoint uses Collators[i].
The alias keeps one representation while retaining those distinct algorithms.

The new case-insensitive prefix regression failed before (ordinal 0, expected
1). After the fix, CI A/a is ordinal 1 and a point, binary A/a is ordinal 0 and
not a point, and the mixed two-column case [1,A]/[1,a] with [binary,CI] is
ordinal 1 but still a point. A direct internal Go oracle confirms all three.
This is source evidence, not a claim that every collation/type/statement-error
variant has been verified.

Changed Rust files: tidb-planner/src/cardinality/row_count_estimator.rs,
tidb-planner/src/cardinality/cross_estimation.rs, tidb-planner/src/access_path.rs,
tidb-planner/src/access_path/index_merge.rs, tidb-executor/src/access_cost.rs,
tidb-planner/tests/row_count_estimator_source.rs, and
tidb-planner/tests/cardinality_mock_stats_ranges_source.rs (all under rust/crates).
Fixtures now specify their binary collators explicitly. Earlier WIP is retained.
The cardinality ExecPlan and receipt also record this step.

Validation from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib index_range_collation_tests -- --nocapture
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib access_path::tests:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib ranger:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all cardinality_mock_stats_ranges_source -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost::tests:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

Pass counts: cardinality 30 (includes both new range tests), points 1, ranger
69, mock source 14, estimator source 14, access cost 30, logical 411, hints 26,
EXPLAIN 109: 804 non-overlapping checks. The mock source target still has 27
ignored mappings; it is not a complete source/package test gate. Lint and diff
checks pass. No Go/Bazel changes were introduced, hence no bazel_prepare trigger.
Logs: /private/tmp/tidb-index-range-{before,after,cardinality,points,ranger,source,
estimator,cost,logical,hints,explain,lint,go}.log.

Go oracle, from /private/tmp/tidb-go-master-20260923 at pin
633a9e37f1c796ac81c203dc107025e7e65385f0:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-index-range-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustSharedIndexRangeReference$' -count=1 -v

Passes (0.058s); wrapper restored failpoint refcount to zero. The test and
overlay are temporary source-oracle inputs, not changes to repository Go files.

This supersedes the missing range-collation representation item in the prior
V1 dispatcher requirements. The collection context still needs raw versus valid
column inputs, handle metadata and source-shaped suffix index preference with
invalid/missing statistics. Shared V1 index dispatch, statement timezone/error
policy, full package inventory/gates and workload runs remain open. Removing
redundant range copies is established from code, but no runtime performance
improvement is claimed. No package completion, commit or push.


## Shared index version dispatch and collection lifecycle (2026-09-25)

Go reference remains origin/master at 633a9e37f1c796ac81c203dc107025e7e65385f0.
Ordinary executor paths, union alternatives, correlation, and recursive backoff
now enter the same GetRowCountByIndexRanges-shaped dispatcher. The borrowed
IndexEstimationStats carries valid column payloads separately from raw NDVs and
integer-handle flags, scaled index/table counts and full-range policy. V1-only
metadata is allocated only for StatsVer1 with CMS. Full-range checks borrow
bounds without allocating normalized range shapes.

The V1 path handles enumerated prefixes, empty enumeration, unique exact width,
NULL/first-range fallback to V2, lazy column cross-validation, first retained
suffix-index preference (even if invalid), column/pseudo suffix fallback and
analyzed-total capping. TopN/CMS/histogram lookup follows QueryBytes ordering.
Recursive V2 backoff uses this dispatcher, so a V1 alternate no longer silently
uses V2 arithmetic. Go explicitly uses Collators[0] for every backoff dimension;
this supersedes the selected-dimension backoff assertion in the prior section.
The mixed-collator regression failed at selectivity 1 versus 0.2 before the fix;
the full V2 result is 21, including the out-of-range boundary contribution.

GenerateHistCollFromColumnInfo maps retained statistics objects, not every schema
index. The snapshot regression failed with [9,10,11] and now retains [9,11],
including evicted and zero-count entries while excluding schema-only index 10.
Missing schema columns terminate the mapped prefix. Crucially, fillIndexPath
then extends Idx2ColUniqueIDs with eligible appended handles. The executor
adapter and planner snapshot reproduce that prepared layout; keeping only the
initial declared mapping caused three existing EXPLAIN failures (100 versus a
reduced handle estimate, and 20 versus the two-point cap). All three now pass.
Physical estimation still truncates/merges prefix ranges and applies the shared
handle damping; logical selectivity can use the extended mapped dimensions.
The four integer-handle SQL estimates 10/10/2/1 also pass a fresh Go oracle.

Changed implementation files under rust/crates: tidb-planner/src/cardinality/
row_count_estimator.rs, index_range_policy.rs and cross_estimation.rs;
tidb-planner/src/stats_info.rs, access_path/index_merge.rs and ranger/mod.rs;
tidb-executor/src/access_cost.rs and driver/planner_bridge.rs. Constructor
migration touches source-shaped fixtures as needed. Existing WIP is preserved.
The V1 enumeration regression failed at 46 versus 22 before the fix; after it
returns 22 even when realtime rows fall to 10, matching Go's V1 cap ownership.

Final targeted validation from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib statistics_initialization_tests -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib logical -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --test all row_count_estimator_source -- --test-threads=1
    make lint
    git diff --check

Go oracle from /private/tmp/tidb-go-master-20260923:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRust(AppendedHandleIndexMap|VersionOneIndex)Reference$' -count=1 -v

Final Rust counts: cardinality 35, access cost 44, statistics initialization 3,
EXPLAIN 109, logical 411, hints 26, source estimator 14: 642 non-overlapping
checks, all passing. make lint and git diff --check pass. No Go/Bazel source or
metadata changes were introduced, so bazel_prepare is not triggered. Earlier
in this dispatcher work, candidates 52, ranger 69 and mock-source 14 also
passed; the 27 ignored mock-source mappings remain open.

Go passes (0.488s), with failpoint refcount restored to zero. The overlay and
oracle are temporary reference inputs. Final logs are
/private/tmp/tidb-v1-index-final-{cardinality,cost,init,explain,logical,hints,source,lint,go}.log.
The package remains incomplete: statement timezone/error policy, complete
async/restricted-SQL lifecycle, remaining source/support artifacts, full package
gates, and sysbench/TPC-C/TPC-H/YCSB behavior/performance are unverified. No
package-complete commit or push is warranted yet.


## Prepared index ranges shared by ordinary and union paths (2026-09-25)

The previous shared version dispatcher left a higher-level split: ordinary
executor paths applied Go core/stats.go detachCondAndBuildRangeForPath's
pruneEstimateRange plus AdjustRowCountForAppendedHandleColumns, while union
partials sent full physical-key bounds directly to the histogram estimator.
Go indexmerge_path.go accessPathsForConds calls the same fillIndexPath as ordinary
paths. This mismatch can overcount prefixes when separate full-key points share
the same declared index prefix.

The common cardinality::estimate_index_path_ranges helper now owns prefix
truncation, last-dimension exclusion reset, union of duplicate prefix bounds,
union of each appended handle's bounds, damping and full-point capping. Ordinary
executor and union estimation provide collection-specific prefix/column adapters.
Invalid or failed handle statistics are skipped as in Go; prefix errors still
propagate. Non-appended paths borrow the original ranges without creating a
normalized prefix vector. Union partial-statistics estimation receives only the
declared columns for its pruned ranges. Actual scan ranges remain untouched.

Regression: 100 rows with a=b=id%10 and secondary indexes ia(a), ib(b). The OR
branches a=5 AND id IN(15,25) and b=6 AND id IN(16,26) each have prefix count 10
and handle selectivity 0.02, giving 10*sqrt(0.02), displayed as 1.41. Rust before
the fix displayed 2.00. Go and fixed Rust display 1.41 and return 15,16,25,26.
The initial test omitted USE INDEX(ia,ib); Go selected BatchPointGet instead of
index merge, invalidating that initial oracle. The final fixture explicitly
selects the secondary indexes in both engines. Its expected value comes from
the corrected Go oracle, not the earlier assumed two-point cap.

Changed files: rust/crates/tidb-planner/src/cardinality.rs;
rust/crates/tidb-planner/src/access_path/index_merge.rs;
rust/crates/tidb-executor/src/access_cost.rs;
rust/crates/tidb-session/src/tests_index_hints.rs; this audit and the cardinality
ExecPlan/receipt. The change removes the executor's duplicate orchestration.

Validation from repository root:

    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib union_partials_prune_index_prefix -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_index_hints:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib cardinality:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-planner --lib find_best_task:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-executor --lib access_cost:: -- --test-threads=1
    cargo test --offline --locked --manifest-path rust/Cargo.toml -p tidb-session --lib tests_explain -- --test-threads=1
    make lint
    git diff --check

The focused test failed before as required, then the hint suite passes 27;
cardinality 35, candidates 52, access cost 44, EXPLAIN 109 pass (267 total,
without double-counting the focused regression). Go oracle from
/private/tmp/tidb-go-master-20260923, pin 633a9e37f1c796ac81c203dc107025e7e65385f0:

    GOTOOLCHAIN=go1.25.12 GOFLAGS='-overlay=/private/tmp/tidb-admission-overlay.json' ./tools/check/failpoint-go-test.sh pkg/planner/cardinality -run '^TestRustUnionHandlePrefixReference$' -count=1 -v

Passes in 0.480s; failpoint refcount restored to zero. Logs:
/private/tmp/tidb-merge-handle-{before,after,cost,cardinality,candidates,explain,lint,go}.log.
No Go/Bazel changes and no bazel_prepare trigger. Whole-package source/support
coverage, remaining statement/loading/variant gates, and sysbench/TPC-C/TPC-H/YCSB
measurements remain open. No performance or package-completion claim is made.

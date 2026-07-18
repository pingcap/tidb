# Plan Workstream

Owns the future logical/physical planning differential ring. It remains an
obligation until `EXPLAIN` output and plan digests are compared against Go on
the same source fixture inventory. New planning code must first define its
typed catalog/statistics inputs and source-derived plan oracle; do not put
planner decisions into the seed executor as a shortcut.

`difftests/corpus/coverage/integration_plan_inventory.tsv` is the checked,
source-derived queue of Go-accepted integration-fixture `EXPLAIN` statements.
It is generated from the static Go parser oracle and links each row to all
upstream expected-result artifacts for its fixture. Run
`cargo run --locked -j 12 -p difftest --bin integration_plan_inventory -- --check`
from `rust/` before claiming a plan-ring input corpus is current. See
`difftests/INTEGRATION_PLAN_INVENTORY.md` for the invariant and limits.

The first non-EXPLAIN planner leaf is now `tidb-planner::cardinality::row_size`.
It ports all six `pkg/planner/cardinality/row_size.go` formulas through a
typed statistics adapter, with an independent `difftest-planner-tests`
`row_size` target covering every post-analyze value assertion from
`TestAvgColLen`. The adapter is intentionally `PARTIAL`: real `PlanContext`,
`HistColl`, expression-column, and mock-store/analyze owners still belong to
the future plan/statistics seam. Do not replace it with a fake catalog or move
the formulas into `tidb-exec`.

The join-cardinality leaf now ports the arithmetic body of
`pkg/planner/cardinality/join.go` through
`tidb_planner::cardinality::join::FullJoinRowCountInput`. It preserves the
Cartesian product path, equi-versus-NA key selection, larger-NDV denominator,
join-reorder threshold, and source `0.9` correlation exponent. The adapter is
intentionally `PARTIAL`: real `PlanContext`, expression columns/schemas,
`StatsInfo` GroupNDV lookup, and join operators remain future planner owners;
do not invent a fake statistics catalog to close those boundaries.

The bounded `ScaleNDV` leaf now ports the dependency-closed arithmetic from
`pkg/planner/cardinality/ndv.go`. `tidb_planner::cardinality::ndv::scale_ndv`
keeps the uniform probability model, skewed linear estimate, source NDV
clamps, and caller-supplied `RiskScaleNDVSkewRatio` blend; all nine original
`TestScaleNDV` vectors execute through the external planner test target. The
SessionVars/property registration, column/statistics estimation, and broader
 testkit planner integration remain explicit `PARTIAL` boundaries.

The pseudo-cardinality leaf now ports the deterministic arithmetic from
`pkg/planner/cardinality/pseudo.go`: equality/less/between rates, signed and
unsigned integer range clamping, scalar NULL/MinNotNull/MaxValue handling, and
prefix-index range correction. `pseudoSelectivity`'s session, expression,
catalog, histogram, and unique-index inspection remains a planner/statistics
boundary; the leaf does not invent a fake statistics catalog.

The row-count-column leaf now ports the dependency-closed arithmetic from
`pkg/planner/cardinality/row_count_column.go`: point versus interval handling,
exclusive boundary and NULL adjustments, primary-key-at-most-one behavior,
increase-factor scaling, full-range clamping, and partial-index product versus
correlated selectivity. It accepts normalized `ScalarRange` values and
caller-owned estimates. Histogram/TopN equality and interval lookup,
stats-v1 enumeration, `PlanContext`/`HistColl`/`Datum` integration, and source
error propagation remain explicit future owners.

The normalized ranger-detachment leaf now ports the boolean traversal from
`pkg/util/ranger/detacher.go`: top-level CNF conditions detach `OR` branches
through DNF, each `AND` branch retains only source-checker-approved access
predicates, reserved approximate predicates remain filters, and an unusable
DNF branch prevents partial access extraction. The Rust boundary accepts
opaque atoms with caller-supplied checker decisions; expression evaluation,
type/collation/session checks, range endpoint construction, and the remaining
`conditionChecker`/ranger ownership stay explicit future work.

The selectivity-greedy leaf now ports the dependency-closed candidate chooser
from `pkg/planner/cardinality/selectivity.go`. `tidb_planner::selectivity_greedy`
preserves source node-kind ordering, non-overlapping mask traversal, and all
six `StatsNode` tie-break rules (coverage, full-versus-partial DNF cover,
minimum access-condition count, column count, and selectivity). It accepts
caller-owned masks and statistics metadata only; expression extraction,
histogram/TopN estimation, session variables, and full planner selectivity
remain explicit `PARTIAL` boundaries.

The cost-factor leaf now ports the dependency-closed constants and
aggregation-factor lookup from `pkg/planner/core/cost/factors_thresholds.go`.
`tidb_planner::cost_factors` preserves the source selection/distinct/tolerance
factors, small-scan threshold, all sixteen source aggregate-factor map entries
(including `default`), and the unknown-name default path. The full cost model,
session-variable overrides,
physical aggregation implementation, and AST aggregate-name catalog remain
explicit `PARTIAL` boundaries.

The out-of-range arithmetic leaf now ports `outOfRangeEQSelectivity` and
`outOfRangeFullNDV` from `pkg/planner/cardinality/selectivity.go` through
`tidb_planner::cardinality::out_of_range`. It preserves modification deltas,
small-NDV smoothing, deletion fallback, zero-NDV square-root derivation,
increase-factor scaling, and the minimum one-row estimate. Histogram/TopN
lookup, session context, range construction, and physical selectivity wiring
remain explicit `PARTIAL` planner/statistics boundaries.

The index-range fast-path leaf now ports the pure bound and index-metadata
predicate from `pkg/planner/cardinality/row_count_index.go`:
`tidb_planner::cardinality::index_range_policy` requires a matching non-empty
`[NULL, MaxValue]` range with inclusive endpoints and rejects partial or
multi-valued indexes. The surrounding histogram/TopN row estimator,
statistics-load recording, async-load short circuit, and encoded Datum/ranger
owners remain explicit `PARTIAL` boundaries.

The expected-count range-conversion leaf now ports the pure arithmetic from
`pkg/planner/cardinality/cross_estimation.go`:
`tidb_planner::cardinality::cross_estimation::convert_range_from_expected_cnt`
selects the first range reaching the requested count in ascending or
descending order, preserves collator identity, inverts the selected endpoint
exclusion, and returns the source full-scan sentinel when all ranges are
needed. Opaque endpoint tokens keep the ranger/Datum boundary explicit; the
surrounding correlation selection, histogram lookup, session variables, and
physical scan cost adjustment remain `PARTIAL`.

The uniform equality leaf now ports the shared
`estimateRowCountWithUniformDistribution` and `CalculateSkewRatioCounts`
arithmetic from `pkg/planner/cardinality/row_count_index.go` and
`pkg/statistics/histogram.go`. It preserves the histogram-average path,
empty-histogram TopN minimum fallback,
`outOfRangeFullNDV` increase/deletion derivation, and
`RiskEqSkewRatio` default/min/max interpolation through normalized metadata.
The shared `CalculateSkewRatioCounts` formula remains owned by
`tidb-stats::row_estimate` and is consumed through a planner adapter, avoiding
two Rust copies of the histogram arithmetic. Histogram/TopN lookup, session
variable recording, and index/column range integration remain explicit
`PARTIAL` planner/statistics owners.

The schema/table-key leaf now ports the source identity normalization from
`pkg/planner/core/schema_table_key.go` through
`tidb_planner::schema_table_key`. It preserves lowercase schema/table keys,
qualified versus unqualified alias identity, and map-safe equality/hash while
leaving parser-owned `ast.CIStr`, CTE/view scope, lock maps, and duplicate-alias
diagnostics to the future planner front end.

The implementation-cost leaf now ports the scalar `baseImpl` cost contract
from `pkg/planner/implementation/base.go`: ordered child-cost accumulation and
reset, explicit cost override/readback, identity cost-limit scaling, and
child-cost subtraction. Physical-plan attachment, memo interfaces, and
concrete implementation cost models remain explicit future planner owners.

The task-type leaf now ports `pkg/planner/property/task_type.go` through
`tidb_planner::task_type`. It preserves the four execution-location values,
source diagnostic labels, and unknown integer fallback while leaving physical
property construction, task scheduling, and coprocessor/MPP execution to the
future planner runtime.

The ORDER BY metadata leaf now ports `pkg/planner/util/byitem.go` through
`tidb_planner::by_item`. It preserves expression/direction identity, display
and list formatting, clone/equality shape, and source bool-plus-expression
memory accounting over opaque expression text. Expression evaluation,
collation/redaction contexts, and physical sort/property integration remain
future planner owners.

The physical-property classification leaf now ports the bounded enum logic
from `pkg/planner/property/physical_property.go`: MPP partition-type values,
the `ToExchangeType` mapping (including unknown pass-through fallback), and
`PhysicalPropMatchResult.Matched`. Expression columns, protobuf exchange
enums, functional dependencies, property construction, and full physical
matching remain future planner owners.

The statistics-property leaf now ports the scalar `StatsInfo.Count` and
`DeriveLimitStats` arithmetic from `pkg/planner/property/stats_info.go` through
`tidb_planner::stats_info`. It caps row count and column NDVs for limit plans
and preserves source truncation semantics, while session NDV scaling,
histograms, group-column lookup, and planner propagation remain external.

The index-column projection leaf now ports `pkg/planner/util/column.go` through
`tidb_planner::index_columns`. It preserves name matching, strict prefix
marking, leading-prefix stopping after missing metadata, full-column nil slots,
and unspecified-length normalization while leaving expression/catalog/index
integration external.

The cascades-engine leaf now ports `pkg/planner/cascades/pattern/engine.go`
through `tidb_planner::pattern_engine`. It preserves the three engine bit flags,
predefined engine sets, overlap membership, and source diagnostic labels while
leaving pattern matching and logical-plan integration to future cascades layers.

The optimizer fix-control parser leaf now ports
`pkg/planner/util/fixcontrol/set.go` through `tidb_planner::fix_control`. It
preserves decimal key parsing, quoted/unquoted and empty values, repeated-key
replacement with warnings, and source parse errors while leaving session
variable wiring and typed getter semantics external.

The cascades memo-group identifier leaf now ports
`pkg/planner/cascades/memo/group_id_generator.go` through
`tidb_planner::memo_group_id`. It preserves one-based generation and uint64
wraparound while leaving memo ownership and optimizer integration external.

The serial cascades-task scheduler leaf now ports
`pkg/planner/cascades/task/task_scheduler.go` through
`tidb_planner::task_scheduler`. It preserves LIFO execution, first-error stop
and propagation, pending-stack retention, and destroy cleanup while leaving
source task descriptions, stack pooling, and cascades context external.

The cascades primitive-hasher leaf now ports
`pkg/planner/cascades/base/hash_equaler.go` through
`tidb_planner::hash_equaler`. It preserves FNV-1a primitive update order,
string/byte framing, float/rune handling, cache/reset lifecycle, and digest
readback while leaving object hashing/equality dispatch external.

The planner build-context leaf now ports the `BuildPBContext` state hand-off
from `pkg/planner/planctx/context.go` through `tidb_planner::plan_context`. It
preserves accessor identity and Detach's shallow-copy replacement of only the
expression context while leaving session, protobuf, and warning interfaces
external.

The cascades task-stack leaf now ports the reusable LIFO stack contract from
`pkg/planner/cascades/task/task.go` through `tidb_planner::task_stack`. It
preserves the source default capacity, push/pop/empty/length behavior,
description order, and destroy clearing while retaining allocation capacity.
The sync.Pool lifecycle, base.Task interface, cascades context, and unsafe Go
layout assertions remain explicit external boundaries.

The cascades pattern-metadata leaf now ports
`pkg/planner/cascades/pattern/pattern.go` through `tidb_planner::pattern`. It
preserves source operand numbering and labels, wildcard matching, logical
operator classification through a typed adapter, engine filtering, child
construction, and source-order semantics. Concrete Go logical-plan objects
and cascades memo integration remain explicit external boundaries.

The cascades string-writer leaf now ports
`pkg/planner/cascades/util/string_writer.go` through
`tidb_planner::string_writer`. It preserves the source two-operation
`StrBufferWriter` surface and fail-fast write/flush behavior over a typed
standard-library sink, while leaving Go's `bufio.Writer` and `intest` plumbing
as implementation details.

The memo expression-iterator leaf now ports
`pkg/planner/memo/expr_iterator.go` through `tidb_planner::expr_iterator`. It
preserves source pattern matching, recursive child cardinality, equivalent
expression traversal, reset/next state, and nested engine filtering over
owned group vectors. Intrusive list elements, real memo groups, logical-plan
operands, and cascades allocator/context remain explicit external boundaries.

The memo exploration-round leaf now ports `ExploreMark` from
`pkg/planner/memo/group.go` through `tidb_planner::explore_mark`. It preserves
round-bit set, clear, and query semantics in a copyable fixed-width adapter;
memo group ownership and transformation lifecycle remain explicit external
boundaries.

The memo group-expression leaf now ports `GroupExpr` from
`pkg/planner/memo/group_expr.go` through `tidb_planner::group_expr`. It
preserves source child identity storage, big-endian child-count/identity/plan
hash fingerprints, exploration marks, and applied-rule identity tracking over
typed adapters; real LogicalPlan pointers, Group/schema properties, and memo
lifecycle remain explicit external boundaries.

The index-column-length leaf now ports `Col2Len` comparison from
`pkg/planner/util/path.go` through `tidb_planner::column_length`. It preserves
source unspecified-length ordering, dominance, equal-column comparison, and
incomparability results over stable column IDs; AccessPath, expression
extraction, ranger values, index metadata, and session context remain
explicit external boundaries.

The plan-cache constant-cloning leaf now ports
`CloneConstantsForPlanCache` from
`pkg/planner/util/utilfuncp/func_pointer_misc.go` through
`tidb_planner::plan_cache_constants`. It preserves nil entries, all-safe
sharing, unsafe deep copies, and reusable destination semantics over an
`Arc`-owned opaque constant adapter; TiDB expression safety metadata and
plan-cache context remain explicit external boundaries.

The index-advisor identity leaf now ports `Column`/`Index` normalization and
`PrefixContain` from `pkg/planner/indexadvisor/model.go` through
`tidb_planner::index_advisor_model`. It preserves lowercase identity keys and
ordered index-prefix matching over owned strings; catalog discovery,
recommendation cost, and optimizer statistics remain explicit external
boundaries.

The cascades rule-type leaf now ports the iota-backed `Type` and its diagnostic
`String` method from `pkg/planner/cascades/rule/rule_type.go` through
`tidb_planner::rule_type`. It preserves the full source integer sequence,
including `XFMaximumRuleLength`, the `XFJoinToApply` special label, and the
`default_none` fallback for other and unknown values; rule construction, masks,
and cascades execution remain explicit external boundaries.

The cascades base-traits leaf now ports the `Hash64`, `Equals`, and `HashEquals`
interface composition from `pkg/planner/cascades/base/base.go` through
`tidb_planner::base_traits`. It keeps hashing on the existing typed `Hasher`
boundary and models Go's dynamic `any` equality through `dyn Any`; concrete
logical/memo objects and cascades dispatch remain explicit external boundaries.

The cascades scheduler-contract leaf now ports the `Scheduler` interface from
`pkg/planner/cascades/base/task_scheduler_base.go` through
`tidb_planner::scheduler_contract`. It preserves opaque task insertion,
execute-until-error, and resource-destruction operations over the existing
`SimpleTaskScheduler` task owner; concurrent scheduling, stack pooling, and
cascades context remain explicit external boundaries.

The cascades stack-contract leaf now ports the `Stack`/`Task` interfaces from
`pkg/planner/cascades/base/task_stack_base.go` through
`tidb_planner::stack_contract`. It preserves LIFO push/pop, empty-stack
behavior, destruction, and the source Execute-plus-Desc task boundary while
leaving the reusable concrete `TaskStack`, stack pooling, and cascades task
ownership to their existing/future adapters.

The logical TopN push-down rule leaf now ports the wrapper from
`pkg/planner/core/rule_topn_push_down.go` through
`tidb_planner::topn_push_down`. It preserves the nil-parent callback, stable
`topn_push_down` registry name, false direct-change flag, and nil error
boundary over a caller-owned plan trait; logical operator rewrites, storage
pushdown, session context, and full SQL plan integration remain explicit
external boundaries.

The derived TopN-from-window rule leaf now ports the wrapper from
`pkg/planner/core/rule_derive_topn_from_window.go` through
`tidb_planner::derive_topn_from_window`. It preserves the no-argument plan
callback, stable `derive_topn_from_window` registry name, false direct-change
flag, and nil error boundary over a caller-owned plan trait; window operators,
TopN construction, MPP/storage placement, and full SQL plan integration remain
explicit external boundaries.

The empty-selection elimination rule leaf now ports the wrapper from
`pkg/planner/core/rule_eliminate_empty_selection.go` through
`tidb_planner::eliminate_empty_selection`. It preserves the recursive-plan
callback, stable `eliminate_empty_selection` registry name, false direct-change
flag, and nil error boundary over a caller-owned plan trait; logical selection
detection, child replacement, SQL plan output, and full optimizer integration
remain explicit external boundaries.

The sequence push-down rule leaf now ports the recursive traversal from
`pkg/planner/core/rule_push_down_sequence.go` through
`tidb_planner::push_down_sequence`. It preserves nested CTE/main-query merging,
DataSource/CTE push-through, unary descent, multi-child/childless attachment,
and the source false direct-change flag over a structural adapter; real
LogicalSequence metadata, operator construction, shared-CTE execution, and
session/runtime integration remain explicit external boundaries.

The union-all dual-elimination leaf now ports the recursive filtering from
`pkg/planner/core/rule_eliminate_unionall_dual_item.go` through
`tidb_planner::eliminate_unionall_dual_item`. It preserves direct and
projection-wrapped zero-row TableDual removal, schema-preserving replacement
of an empty union, recursive child rewrites, changed-flag aggregation, and the
stable rule name over a structural adapter; real LogicalUnionAll/
LogicalProjection construction, schema objects, SQL planning, and executor
integration remain explicit external boundaries.

The loose projection-elimination predicate now ports
`canProjectionBeEliminatedLoose` from
`pkg/planner/core/rule_eliminate_projection.go` through
`tidb_planner::projection_elimination`. It preserves the source `Proj4Expand`
gate and all-direct-column expression requirement, including the empty
projection case, over a typed expression-shape adapter; expression
evaluation/replacement, schema mutation, recursive logical rewrites, physical
projection handling, and session/failpoint integration remain explicit
external boundaries.

The resolve-expand leaf now ports the post-order `genExpand` traversal from
`pkg/planner/core/rule_resolve_grouping_expand.go` through
`tidb_planner::resolve_grouping_expand`. It visits children before Expand
level generation, preserves append-style generated-level counts, leaves other
operators untouched, and keeps the source false direct-change flag over a
structural adapter; LogicalExpand grouping-set expressions, schema/GID/GPos
construction, SQL output, and planner error/session integration remain
explicit external boundaries.

The join-reorder projection-safety leaf now ports
`isInlineableProjectionExpr` and `canInlineProjectionBasic` from
`pkg/planner/core/rule_join_reorder_projection_inline.go` through
`tidb_planner::join_reorder_projection_inline`. It preserves recursive
Column/ScalarFunction/Constant support, the required column-reference gate,
deferred-constant and unsupported-node rejection, Proj4Expand rejection, and
mutable/non-deterministic/correlated effect gates over a typed expression
adapter; join-group attribution, null-extension checks, expression
substitution, and full reorder execution remain explicit external boundaries.

The max/min elimination eligibility leaf now ports the pre-rewrite gate from
`pkg/planner/core/rule/rule_max_min_eliminate.go` through
`tidb_planner::max_min_elimination`. It preserves grouped/empty rejection,
the all-Max/Min requirement, ENUM/SET ordering safety rejection, and the
single-versus-multi aggregate branch classification over caller-owned
metadata; index/ranger checks, Limit/Sort/Join construction, expression/schema
handling, and recursive optimizer integration remain explicit external
boundaries.

The LogicalTableDual identity leaf now ports the source ExplainInfo and
generated Hash64/Equals field order from
`pkg/planner/core/operator/logicalop/logical_table_dual.go` through
`tidb_planner::logical_table_dual`. It preserves TableDual type framing,
nil/present ordered schema distinction, normalized column identity, RowCount,
equality/hash divergence, and `rowcount:` explain text; full FieldType,
collation, VirtualExpr, plan-context, statistics, and runtime operator
integration remain explicit external boundaries.

The LogicalLimit identity leaf now ports generated Hash64/Equals and basic
ExplainInfo metadata from `pkg/planner/core/operator/logicalop/logical_limit.go`
through `tidb_planner::logical_limit`. It preserves the Limit tag, nil/present
schema and PartitionBy framing, ordered sort-column/direction, Offset, Count,
equality/hash divergence, and offset/count explain text; full
FieldType/collation/VirtualExpr/property ExplainPartitionBy, plan context, and
runtime limit behavior remain explicit external boundaries.

The LogicalMaxOneRow identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_max_one_row.go` through
`tidb_planner::logical_max_one_row`. It preserves the MaxOneRow tag and
BaseLogicalPlan plan-ID identity/equality; context, children, schema/statistics,
predicate behavior, and runtime row limiting remain explicit external
boundaries.

The LogicalSort identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_sort.go` through
`tidb_planner::logical_sort`. It preserves the Sort tag, nil/present ordered
ByItems framing, normalized column-expression identity, and Desc direction;
arbitrary expression Hash64, ExplainByItems formatting, plan context, pruning,
and runtime ordering remain explicit external boundaries.

The LogicalTopN identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_top_n.go` through
`tidb_planner::logical_top_n`. It preserves the TopN tag, schema and ordered
ByItems/PartitionBy nil-present framing, normalized column/direction identity,
Offset, Count, and PreferLimitToCop; arbitrary expression metadata,
ExplainInfo formatting, plan context/pruning, and runtime TopN behavior remain
explicit external boundaries.

The LogicalShowDDLJobs identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_show_ddl_jobs.go` through
`tidb_planner::logical_show_ddl_jobs`. It preserves the ShowDDLJobs tag and
LogicalSchemaProducer nil/present ordered normalized schema; JobNumber, DDL
stats/context, and runtime SHOW behavior remain explicit external boundaries.

The LogicalShow identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_show.go` through
`tidb_planner::logical_show`. It preserves the Show tag and LogicalSchemaProducer
nil/present ordered normalized schema; ShowContents/Extractor AST metadata,
plan context, and runtime SHOW behavior remain explicit external boundaries.

The LogicalSchemaProducer identity leaf now ports Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_schema_producer.go` through
`tidb_planner::logical_schema_producer`. It preserves nil/present ordered
schema framing and normalized column identity; schema propagation, names,
BaseLogicalPlan/children, full FieldType/collation/VirtualExpr metadata, and
DataSource integration remain explicit external boundaries.

The LogicalSequence identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_sequence.go` through
`tidb_planner::logical_sequence`. It preserves the Sequence tag and embedded
BaseLogicalPlan plan-ID identity/equality; CTE child ordering,
schema/predicate/statistics/context, and runtime sequence behavior remain
explicit external boundaries.

The LogicalUnionAll identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_union_all.go` through
`tidb_planner::logical_union_all`. It preserves the Union tag and
LogicalSchemaProducer nil/present ordered normalized schema; child union
construction, predicate pushdown, plan context, and runtime union execution
remain explicit external boundaries.

The LogicalMemTable identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_mem_table.go` through
`tidb_planner::logical_mem_table`. It preserves the MemTableScan tag,
normalized schema, case-folded DBName, and nil/present TableInfo ID; extractor,
columns, query-time range, infoschema, plan context, and runtime memtable
execution remain explicit external boundaries.

The LogicalProjection identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_projection.go` through an isolated
`tidb_planner::logical_projection` leaf. It preserves the Projection tag,
schema and ordered Exprs nil/present framing, normalized column-expression
identity, CalculateNoDelay, and Proj4Expand; arbitrary expression
metadata/evaluation, projection rewrites/pruning, plan context, and runtime
execution remain explicit external boundaries.

The LogicalExpand identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_expand.go` through an isolated
`tidb_planner::logical_expand` leaf. It preserves the Expand tag, schema and
nil/present ordered grouping columns/expressions, DistinctSize, nested
RollupGroupingSets/LevelExprs, GID, and GPos; arbitrary expression variants,
FieldType/collation metadata, grouping-name/ID maps, plan context/schema
propagation, and optimizer/runtime execution remain explicit external
boundaries.

The window-frame metadata leaf now ports handwritten FrameBound and
WindowFrame Hash64/Equals plus FrameBound Clone from
`pkg/planner/core/operator/logicalop/logical_window.go` through an isolated
`tidb_planner::window_frame` module. It preserves scalar bound fields,
nil/present ordered expression and compare-function lists, caller-supplied
function-address tokens, clone nil-slice state, and WindowFrame's source
start/end hash asymmetry; arbitrary expression evaluation, function-pointer
identity, session/type context, LogicalWindow planning, and runtime execution
remain explicit external boundaries.

The handle-column identity leaf now ports CommonHandleCols and IntHandleCols
Hash64/Equals from `pkg/planner/util/handle_cols.go` through an isolated
`tidb_planner::handle_cols` module. It preserves nil/present TableInfo and
IndexInfo framing, ordered column identity, clone option state, and the
Common-versus-Int handle boundaries over normalized IDs; handle encoding,
row/datums, index truncation, full catalog metadata, compare/collation,
schema resolution, and runtime storage integration remain explicit external
boundaries.

The LogicalAggregation identity leaf now ports generated Hash64/Equals from
`pkg/planner/core/operator/logicalop/logical_aggregation.go` through an
isolated `tidb_planner::logical_aggregation` module. It preserves the
Aggregation tag, schema, ordered aggregate descriptors, GroupByItems
nil/present framing, PossibleProperties order metadata, and the source
HasTiFlash exclusion over normalized adapters; expression/type inference,
full FieldType/ByItems metadata, plan context, statistics, optimizer rules,
and runtime aggregation remain explicit external boundaries.

The cost-usage leaf now ports the pure CostVer2 arithmetic and tracing helpers
from `pkg/planner/util/costusage/cost_misc.go` through an isolated
`tidb_planner::cost_usage` module. It preserves trace flags, lazy factor/formula
construction, source-order formula and factor aggregation, non-negative cost
display semantics, fixed-two-decimal divide/multiply formatting, and
trace-preserving tie-breaker updates; optimizer plan traversal, factor
selection, session wiring, warning emission, and SQL-facing EXPLAIN behavior
remain explicit external boundaries.

The aggregate cast-gating leaf now ports `WrapCastForAggFuncs` from
`pkg/planner/util/coreusage/cast_misc.go` through an isolated
`tidb_planner::wrap_cast` module. It preserves the source mode boundary:
Complete, Partial1, and Dedup descriptors invoke the delegated cast operation
for eligible arguments, while Final and Partial2 leave partial-state arguments
unchanged; caller-marked NULL/function-specific skips, expression
BuildContext/type inference, physical projection injection, and optimizer
execution remain explicit external boundaries.

The logical mock-source leaf now ports the test-only `MockDataSource.Init`
constructor from `pkg/planner/core/operator/logicalop/logical_mock.go` through
an isolated `tidb_planner::logical_mock` module. It preserves the embedded
BaseLogicalPlan's fixed `mockDS` type, zero query-block offset, and retained
PlanContext identity; plan-ID allocation, task maps, schema, and physical
mock-source/TableDual planning remain explicit external boundaries.

The logical-property leaf now ports the zero-value `LogicalProperty` shape and
constructor from `pkg/planner/property/logical_property.go` through an isolated
`tidb_planner::logical_property` module. It preserves optional Stats/Schema/FD
metadata, MaxOneRow, nil-versus-present PossibleProps, and HasTiFlash through
opaque caller-owned identities; expression schemas, statistics/FD derivation,
and memo/group lifecycle remain explicit external boundaries.

The outer-to-inner rule wrapper now ports `ConvertOuterToInnerJoin` from
`pkg/planner/core/rule_outer_to_inner_join.go` through an isolated
`tidb_planner::outer_to_inner_join` module. It preserves the registry name,
exactly-once delegation to the caller-owned logical-plan conversion, and the
source's intentionally false `planChanged` result; join predicate/null-
rejection traversal, logical-plan mutation, SQL plan output, and session/error
handling remain explicit external boundaries.

The vector columnar-index metadata leaf now ports `buildVectorIndexExtra` from
`pkg/planner/core/columnar_index_utils.go` through an isolated
`tidb_planner::columnar_index_extra` module. It preserves the fixed vector
index type, retained index identity, ANN query fields, derived index ID,
copied reference-vector bytes, and copied column identity; protobuf/model
metadata, TiFlash scan planning, and vector execution remain explicit external
boundaries.

The physical CTE-table leaf now ports `PhysicalCTETable.ExplainInfo` and the
pure rejection gates of `findBestTask4LogicalCTETable` from
`pkg/planner/core/operator/physicalop/physical_cte_table.go` through an
isolated `tidb_planner::physical_cte_table` module. It preserves signed CTE
storage identity, `Scan on CTE_<id>` explain text, and rejection for index-join
or sort properties; schema/statistics/context, root-task wiring, memory
accounting, recursive CTE planning, and runtime execution remain explicit
external boundaries.

The physical MaxOneRow leaf now ports the pure support gate from
`pkg/planner/core/operator/physicalop/physical_max_one_row.go` through an
isolated `tidb_planner::physical_max_one_row` module. It preserves rejection
for non-empty sort or TiFlash properties, the fixed `ExpectedCnt: 2`, and
forwarding of CTE-producer/no-cop metadata; physical context/statistics/clone,
warning publication, task attachment, and the executor's scalar-subquery row
limit remain explicit external boundaries.

The LogicalCTETable statistics leaf now ports `DeriveStats` from
`pkg/planner/core/operator/logicalop/logical_cte_table.go` through an isolated
`tidb_planner::logical_cte_table` module. It preserves the source rule that
only a one-element reload vector is active, retains existing statistics on a
false reload, and installs SeedStat with `changed=true` on reload or missing
stats (including a nil seed); concrete StatsInfo/schema/context derivation,
catalog statistics, and CTE plan propagation remain explicit external
boundaries.

The TiFlash telemetry leaf now ports `IsTiFlashContained` from
`pkg/planner/core/telemetry.go` through an isolated `tidb_planner::telemetry`
module. It preserves Explain-target unwrapping, physical/nonphysical
filtering, TiFlash TableReader detection, ExchangeSender classification, and
ordered child traversal over an opaque plan tree; concrete physical plans,
session process telemetry, MPP planning/execution, and consumer wiring remain
explicit external boundaries.

The constant-condition leaf now ports `IsConstFalse` and `Conds2TableDual`
from `pkg/planner/core/operator/logicalop/expression_util.go` through an
isolated `tidb_planner::condition_to_dual` module. It preserves NULL/false
classification, NULL precedence, empty/multi-condition cardinality, and the
plan-cache over-optimization guard; expression coercion/statement context,
LogicalTableDual construction/schema, and optimizer predicate simplification
remain explicit external boundaries.

The physical TableSample initialization leaf now ports `PhysicalTableSample.Init`
from `pkg/planner/core/operator/physicalop/physical_table_sample.go` through
an isolated `tidb_planner::physical_table_sample` module. It preserves the
`TableSample` plan type, pseudo `RowCount: 1`, query-block offset, physical
table identity, and Desc flag; schema/table/sampler objects, memory accounting,
region sampling, and executor behavior remain explicit external boundaries.

The cascades rule-set leaf now ports `ListRules.Filter` and
`OperandRules.Filter` from `pkg/planner/cascades/rule/ruleset/rule_set.go`
through an isolated `tidb_planner::rule_set` module. It preserves rule-ID
bitmask membership, source order and duplicate retention, plus the
intermediate de-correlate-Apply special-set switch; concrete rule interfaces,
memo GroupExpression flags, rule construction, and optimizer execution remain
explicit external boundaries.

The column-pruning schema-invariant leaf now ports
`noUnexpectedZeroColumnSchema` from
`pkg/planner/core/rule/rule_column_pruning.go` through an isolated
`tidb_planner::column_pruning` module. It preserves recursive child validation
and the two source zero-column exemptions (schema reused from the first child
or LogicalTableDual); logical schema/column identity, pruning mutation, and
optimizer execution remain explicit external boundaries.

The physical UnionScan planning leaf now ports the pure gates and
initialization metadata from
`pkg/planner/core/operator/physicalop/physical_union_scan.go` through an
isolated `tidb_planner::physical_union_scan` module. It preserves TiFlash
rejection, index-join-admission outcomes, the `UnionScan` plan type, query-block
offset, and retained condition/handle counts; concrete property cloning,
expression/handle identity, child/task attachment, transaction-buffer reads,
and executor behavior remain explicit external boundaries.

The physical SHOW planning leaf now ports `PhysicalShow.Init`,
`PhysicalShowDDLJobs.Init`, and both `findBestTask` property gates from
`pkg/planner/core/operator/physicalop/physical_show.go` through an isolated
`tidb_planner::physical_show` module. It preserves SHOW/SHOW-DDL-JOBS kind,
pseudo `RowCount: 1`, DDL job-number identity, and rejection for index-join or
sort properties; ShowContents/extractor/catalog/schema/context/task wiring and
SHOW execution remain explicit external boundaries.

The physical LOCK planning leaf now ports the dependency-closed gate and
metadata from `pkg/planner/core/operator/physicalop/physical_lock.go` through
an isolated `tidb_planner::physical_lock` module. It preserves TiFlash
rejection before plan creation, the `Lock` plan kind, root query-block offset
`0`, uint64 wait seconds, and the exact source
`lock type + " " + wait seconds` explain shape used by `SELECT ... FOR UPDATE`;
AST/map/schema/handle cloning, statistics/context,
warning/task wiring, and lock execution remain explicit external boundaries.

The physical TableDual leaf now ports the dependency-closed metadata and
property gates from `pkg/planner/core/operator/physicalop/physical_table_dual.go`
through an isolated `tidb_planner::physical_table_dual` module. It preserves
the `Dual` plan kind, query-block offset, `rows:<RowCount>` explain text,
unconditional index-join rejection, and the source rule that a required sort
is accepted only for zero/one-row results; schema/output names, context/stats,
root-task wiring, memory accounting, and mock-datasource fallback remain
explicit external boundaries.

# TiDB Planner Test Case Map (pkg/planner)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## Placement Notes

- Core optimizer cases live under `pkg/planner/core` and `pkg/planner/core/casetest/<type>`.
- Non-core planner packages place tests in their owning package directories (for example, `pkg/planner/cardinality`, `pkg/planner/memo`).

## Planner Package Map

- `pkg/planner/cardinality`
- `pkg/planner/cascades`
- `pkg/planner/funcdep`
- `pkg/planner/implementation`
- `pkg/planner/indexadvisor`
- `pkg/planner/memo`
- `pkg/planner/planctx`
- `pkg/planner/property`
- `pkg/planner/util`

## pkg/planner/cardinality

### Tests
- `pkg/planner/cardinality/exponential_test.go` - planner/cardinality: Tests exponential backoff for NDV/selectivity with bounds.
- `pkg/planner/cardinality/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/cardinality/ndv_test.go` - planner/cardinality: Tests NDV scaling, skew ratio, and exponential backoff.
- `pkg/planner/cardinality/row_size_test.go` - planner/cardinality: Tests AvgColSize variants across formats.
- `pkg/planner/cardinality/selectivity_test.go` - planner/cardinality: Tests selectivity estimation, collation columns, and out-of-range bounds.

### Testdata
- `pkg/planner/cardinality/testdata/cardinality_suite_in.json`
- `pkg/planner/cardinality/testdata/cardinality_suite_out.json`

## pkg/planner/cascades

### Tests
- `pkg/planner/cascades/cascades_test.go` - planner/cascades: Tests cascades drive and stats derivation parity.

## pkg/planner/cascades/base

### Tests
- `pkg/planner/cascades/base/base_test.go` - planner/cascades/base: Benchmarks equals interface patterns.
- `pkg/planner/cascades/base/hash_equaler_test.go` - planner/cascades/base: Tests hash equaler consistency and type handling.

## pkg/planner/cascades/memo

### Tests
- `pkg/planner/cascades/memo/group_and_expr_test.go` - planner/cascades/memo: Tests group expr hashing, equality, and parent refs.
- `pkg/planner/cascades/memo/group_id_generator_test.go` - planner/cascades/memo: Tests group ID generator wraparound.
- `pkg/planner/cascades/memo/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/cascades/memo/memo_test.go` - planner/cascades/memo: Tests memo init, insert, and group merge.

## pkg/planner/cascades/pattern

### Tests
- `pkg/planner/cascades/pattern/engine_test.go` - planner/cascades/pattern: Tests engine type set membership.
- `pkg/planner/cascades/pattern/pattern_test.go` - planner/cascades/pattern: Tests operand matching and pattern construction.

## pkg/planner/cascades/rule

### Tests
- `pkg/planner/cascades/rule/binder_test.go` - planner/cascades/rule: Tests binder pattern matching and failures.

## pkg/planner/cascades/rule/apply/decorrelateapply

### Tests
- `pkg/planner/cascades/rule/apply/decorrelateapply/xf_decorrelate_apply_test.go` - planner/cascades/rule/apply: Tests XF decorrelate should delete intermediary apply.

## pkg/planner/cascades/task

### Tests
- `pkg/planner/cascades/task/task_scheduler_test.go` - planner/cascades/task: Tests scheduler error handling order.
- `pkg/planner/cascades/task/task_test.go` - planner/cascades/task: Tests task stack behavior and benchmarks.

## pkg/planner/core

### Tests
- `pkg/planner/core/binary_plan_test.go` - planner/core: Tests binary plan generation and size limits.
- `pkg/planner/core/cbo_test.go` - planner/core: Benchmarks optimizer plan selection.
- `pkg/planner/core/common_plans_test.go` - planner/core: Tests LOAD DATA line/field defaults.
- `pkg/planner/core/enforce_mpp_test.go` - planner/core: Tests row-size impact on TiFlash MPP cost.
- `pkg/planner/core/exhaust_physical_plans_test.go` - planner/core: Tests index join lookup filter analysis and range building.
- `pkg/planner/core/expression_test.go` - planner/core: Tests AST expression eval (between/case/cast).
- `pkg/planner/core/find_best_task_test.go` - planner/core: Tests FindBestTask overflow, property, and hint cases.
- `pkg/planner/core/hint_test.go` - planner/core: Tests set_var and write_slow_log hints with bindings/EXPLAIN and partial ordered index.
- `pkg/planner/core/integration_partition_test.go` - planner/core: Tests list/list-columns partition correctness across order/limit, aggregates, views, and transactions.
- `pkg/planner/core/integration_test.go` - planner/core: Integration coverage for isolation read, TiFlash pushdown of scalar/agg/window functions, plan cache fallbacks, EXPLAIN ANALYZE DML, and issue regressions.
- `pkg/planner/core/logical_plans_test.go` - planner/core: Tests predicate pushdown and logical optimize cases.
- `pkg/planner/core/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/optimizer_test.go` - planner/core: Tests MPP type conversions and shuffle handling.
- `pkg/planner/core/physical_plan_test.go` - planner/core: Tests analyze options, hint/binding compatibility, subquery planning, memory trace, and exchange sender indices.
- `pkg/planner/core/plan_cache_instance_test.go` - planner/core: Tests instance plan cache put/get/eviction and stats-hash matching.
- `pkg/planner/core/plan_cache_lru_test.go` - planner/core: Tests LRU plan cache put/get/delete and param-type bucket matching.
- `pkg/planner/core/plan_cost_ver1_test.go` - planner/core: Tests true_card_cost differences, selection cost v1/v2, and TiFlash choice on small tables.
- `pkg/planner/core/plan_cost_ver2_test.go` - planner/core: Tests cost model v2 row-size, factor costs, limit/offset, and TiFlash cost factors.
- `pkg/planner/core/plan_replayer_capture_test.go` - planner/core: Tests plan replayer capture table stats.
- `pkg/planner/core/plan_test.go` - planner/core: Tests plan encode/decode, normalization/digest, explain hints, cop paging, agg final mode build, and IMPORT INTO planning.
- `pkg/planner/core/plan_to_pb_test.go` - planner/core: Tests ColumnToProto collation, flags, and elems.
- `pkg/planner/core/planbuilder_test.go` - planner/core: Tests plan builder utilities (SHOW schema, access paths, rewriter pool, clone, analyze options, privileges, admin/traffic).
- `pkg/planner/core/preprocess_test.go` - planner/core: Validates SQL/DDL preprocessing errors and constraints.
- `pkg/planner/core/rule_generate_column_substitute_test.go` - planner/core: Benchmarks generated column expression substitution.
- `pkg/planner/core/rule_join_reorder_dp_test.go` - planner/core: Tests DP reorder TPCH Q5.
- `pkg/planner/core/runtime_filter_generator_test.go` - planner/core: Tests runtime filter generation under MPP with testdata and failpoints.
- `pkg/planner/core/stats_test.go` - planner/core: Tests index pruning thresholds and choices.
- `pkg/planner/core/stringer_test.go` - planner/core: Tests plan stringer output for SHOW/DESC variants.
- `pkg/planner/core/task_test.go` - planner/core: Tests PhysicalUnionScan attach behavior on tasks.
- `pkg/planner/core/util_test.go` - planner/core: Tests table list extraction across DDL/DML, aliases, and CTEs.

### Testdata
- `pkg/planner/core/testdata/plan_suite_unexported_in.json`
- `pkg/planner/core/testdata/plan_suite_unexported_out.json`
- `pkg/planner/core/testdata/runtime_filter_generator_suite_in.json`
- `pkg/planner/core/testdata/runtime_filter_generator_suite_out.json`

## pkg/planner/core/casetest

### Tests
- `pkg/planner/core/casetest/integration_test.go` - planner/core/casetest: Cascades integration cases for verbose explain outputs, TiFlash isolation/MPP/partition behaviors, fix-control regressions, and merged issue regressions.
- `pkg/planner/core/casetest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/plan_test.go` - planner/core/casetest: Tests plan digest stability, normalized plan output, and related regressions.
- `pkg/planner/core/casetest/stats_test.go` - planner/core/casetest: Tests group NDVs.

### Testdata
- `pkg/planner/core/casetest/testdata/integration_suite_in.json`
- `pkg/planner/core/casetest/testdata/integration_suite_out.json`
- `pkg/planner/core/casetest/testdata/integration_suite_xut.json`
- `pkg/planner/core/casetest/testdata/json_plan_suite_in.json`
- `pkg/planner/core/casetest/testdata/json_plan_suite_out.json`
- `pkg/planner/core/casetest/testdata/json_plan_suite_xut.json`
- `pkg/planner/core/casetest/testdata/plan_normalized_suite_in.json`
- `pkg/planner/core/casetest/testdata/plan_normalized_suite_out.json`
- `pkg/planner/core/casetest/testdata/plan_normalized_suite_xut.json`
- `pkg/planner/core/casetest/testdata/stats_suite_in.json`
- `pkg/planner/core/casetest/testdata/stats_suite_out.json`
- `pkg/planner/core/casetest/testdata/stats_suite_xut.json`

## pkg/planner/core/casetest/binaryplan

### Tests
- `pkg/planner/core/casetest/binaryplan/binary_plan_test.go` - planner/core/casetest/binaryplan: Tests binary plan in explain and slow log.
- `pkg/planner/core/casetest/binaryplan/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/binaryplan/testdata/binary_plan_suite_in.json`
- `pkg/planner/core/casetest/binaryplan/testdata/binary_plan_suite_out.json`
- `pkg/planner/core/casetest/binaryplan/testdata/binary_plan_suite_xut.json`

## pkg/planner/core/casetest/cascades

### Tests
- `pkg/planner/core/casetest/cascades/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/cascades/memo_test.go` - planner/core/casetest/cascades: Tests cascades template.

### Testdata
- `pkg/planner/core/casetest/cascades/testdata/cascades_suite_in.json`
- `pkg/planner/core/casetest/cascades/testdata/cascades_suite_out.json`
- `pkg/planner/core/casetest/cascades/testdata/cascades_template_in.json`
- `pkg/planner/core/casetest/cascades/testdata/cascades_template_out.json`
- `pkg/planner/core/casetest/cascades/testdata/cascades_template_xut.json`

## pkg/planner/core/casetest/cbotest

### Tests
- `pkg/planner/core/casetest/cbotest/cbo_test.go` - planner/core/casetest/cbotest: Tests CBO analyze suites, no-analyze stats, straight join cases, and merged issue regressions.
- `pkg/planner/core/casetest/cbotest/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/cbotest/testdata/analyzeSuiteTestIndexEqualUnknownT.json`
- `pkg/planner/core/casetest/cbotest/testdata/analyzeSuiteTestLimitIndexEstimationT.json`
- `pkg/planner/core/casetest/cbotest/testdata/analyzeSuiteTestLowSelIndexGreedySearchT.json`
- `pkg/planner/core/casetest/cbotest/testdata/analyze_suite_in.json`
- `pkg/planner/core/casetest/cbotest/testdata/analyze_suite_out.json`
- `pkg/planner/core/casetest/cbotest/testdata/analyze_suite_xut.json`
- `pkg/planner/core/casetest/cbotest/testdata/analyzesSuiteTestIndexReadT.json`
- `pkg/planner/core/casetest/cbotest/testdata/issue59563.json`
- `pkg/planner/core/casetest/cbotest/testdata/issue61792.json`
- `pkg/planner/core/casetest/cbotest/testdata/issue62438.json`
- `pkg/planner/core/casetest/cbotest/testdata/test.t0da79f8d.json`
- `pkg/planner/core/casetest/cbotest/testdata/test.t19f3e4f1.json`

## pkg/planner/core/casetest/ch

### Tests
- `pkg/planner/core/casetest/ch/ch_test.go` - planner/core/casetest/ch: Tests CH (TPCC) query plans for Q2/Q5.
- `pkg/planner/core/casetest/ch/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/ch/testdata/ch_suite_in.json`
- `pkg/planner/core/casetest/ch/testdata/ch_suite_out.json`
- `pkg/planner/core/casetest/ch/testdata/ch_suite_xut.json`
- `pkg/planner/core/casetest/ch/testdata/tpcc.customer.json`
- `pkg/planner/core/casetest/ch/testdata/tpcc.item.json`
- `pkg/planner/core/casetest/ch/testdata/tpcc.nation.json`
- `pkg/planner/core/casetest/ch/testdata/tpcc.order_line.json`
- `pkg/planner/core/casetest/ch/testdata/tpcc.orders.json`
- `pkg/planner/core/casetest/ch/testdata/tpcc.region.json`
- `pkg/planner/core/casetest/ch/testdata/tpcc.stock.json`
- `pkg/planner/core/casetest/ch/testdata/tpcc.supplier.json`

## pkg/planner/core/casetest/correlated

### Tests
- `pkg/planner/core/casetest/correlated/correlated_test.go` - planner/core/casetest/correlated: Tests correlated subqueries and decorrelation regressions.
- `pkg/planner/core/casetest/correlated/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/planner/core/casetest/dag

### Tests
- `pkg/planner/core/casetest/dag/dag_test.go` - planner/core/casetest/dag: Tests DAG plan builder cases and join plans from testdata.
- `pkg/planner/core/casetest/dag/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/dag/testdata/plan_suite_in.json`
- `pkg/planner/core/casetest/dag/testdata/plan_suite_out.json`
- `pkg/planner/core/casetest/dag/testdata/plan_suite_xut.json`

## pkg/planner/core/casetest/enforcempp

### Tests
- `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go` - planner/core/casetest/enforcempp: Tests enforce MPP plans and warning filtering.
- `pkg/planner/core/casetest/enforcempp/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/enforcempp/testdata/enforce_mpp_suite_in.json`
- `pkg/planner/core/casetest/enforcempp/testdata/enforce_mpp_suite_out.json`
- `pkg/planner/core/casetest/enforcempp/testdata/enforce_mpp_suite_xut.json`

## pkg/planner/core/casetest/flatplan

### Tests
- `pkg/planner/core/casetest/flatplan/flat_plan_test.go` - planner/core/casetest/flatplan: Tests FlattenPhysicalPlan output for main plans and CTEs.
- `pkg/planner/core/casetest/flatplan/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/flatplan/testdata/flat_plan_suite_in.json`
- `pkg/planner/core/casetest/flatplan/testdata/flat_plan_suite_out.json`
- `pkg/planner/core/casetest/flatplan/testdata/flat_plan_suite_xut.json`

## pkg/planner/core/casetest/hint

### Tests
- `pkg/planner/core/casetest/hint/hint_test.go` - planner/core/casetest/hint: Tests read_from_storage/join hints, view/QB hints, isolation read, partition hints, and optimizer cost-factor hints.
- `pkg/planner/core/casetest/hint/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/hint/testdata/integration_suite_in.json`
- `pkg/planner/core/casetest/hint/testdata/integration_suite_out.json`
- `pkg/planner/core/casetest/hint/testdata/integration_suite_xut.json`

## pkg/planner/core/casetest/index

### Tests
- `pkg/planner/core/casetest/index/index_test.go` - planner/core/casetest/index: Tests prefix/invisible indexes, range derivation/intersection, row function matching, ordered index with IS NULL, vector/inverted index, and columnar index analyze.
- `pkg/planner/core/casetest/index/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/index/testdata/index_range_in.json`
- `pkg/planner/core/casetest/index/testdata/index_range_out.json`
- `pkg/planner/core/casetest/index/testdata/index_range_xut.json`
- `pkg/planner/core/casetest/index/testdata/integration_suite_in.json`
- `pkg/planner/core/casetest/index/testdata/integration_suite_out.json`
- `pkg/planner/core/casetest/index/testdata/integration_suite_xut.json`

## pkg/planner/core/casetest/indexmerge

### Tests
- `pkg/planner/core/casetest/indexmerge/indexmerge_intersection_test.go` - planner/core/casetest/indexmerge: Tests intersection index merge with plan cache, order property, and hints.
- `pkg/planner/core/casetest/indexmerge/indexmerge_path_test.go` - planner/core/casetest/indexmerge: Tests MV index filter collection and random member-of queries.
- `pkg/planner/core/casetest/indexmerge/indexmerge_test.go` - planner/core/casetest/indexmerge: Tests index merge path generation from testdata.
- `pkg/planner/core/casetest/indexmerge/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/indexmerge/testdata/index_merge_suite_in.json`
- `pkg/planner/core/casetest/indexmerge/testdata/index_merge_suite_out.json`
- `pkg/planner/core/casetest/indexmerge/testdata/index_merge_suite_xut.json`

## pkg/planner/core/casetest/instanceplancache

### Tests
- `pkg/planner/core/casetest/instanceplancache/builtin_func_test.go` - planner/core/casetest/instanceplancache: Exercises prepared `IN` and `IS TRUE|FALSE` builtins across int/real/decimal/datetime with concurrent execution.
- `pkg/planner/core/casetest/instanceplancache/concurrency_test.go` - planner/core/casetest/instanceplancache: Runs concurrent prepared vs normal SQLs (Sysbench-like mix plus point/range/partition cases) to validate instance plan cache correctness.
- `pkg/planner/core/casetest/instanceplancache/concurrency_tpcc_test.go` - planner/core/casetest/instanceplancache: Runs concurrent TPCC-style transactions (new-order/payment/order-status/delivery) with prepared statements to validate instance plan cache.
- `pkg/planner/core/casetest/instanceplancache/dml_test.go` - planner/core/casetest/instanceplancache: Compares prepared DML vs direct TPCC updates across two DBs to validate instance plan cache DML correctness.
- `pkg/planner/core/casetest/instanceplancache/main_test.go` - planner/core/casetest/instanceplancache: Generates randomized table schemas/query patterns and validates prepared vs literal results under concurrent workers.
- `pkg/planner/core/casetest/instanceplancache/others_test.go` - planner/core/casetest/instanceplancache: Covers instance plan cache vars/size, bindings, skip reasons, stale read, txn/DDL/user/charset/collation, partition pruning, plan types, meta/runtime info, view, and issue regressions.

## pkg/planner/core/casetest/join

### Tests
- `pkg/planner/core/casetest/join/join_test.go` - planner/core/casetest/join: Tests semijoin order, NULL-safe join EQ, join condition simplification, join key preservation, and merged issue regressions.
- `pkg/planner/core/casetest/join/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/planner/core/casetest/logicalplan

### Tests
- `pkg/planner/core/casetest/logicalplan/logical_plan_builder_test.go` - planner/core/casetest/logicalplan: Tests group-by schema handling in logical plan build.
- `pkg/planner/core/casetest/logicalplan/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/planner/core/casetest/mpp

### Tests
- `pkg/planner/core/casetest/mpp/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/mpp/mpp_test.go` - planner/core/casetest/mpp: Tests MPP joins (broadcast/shuffle), exchange pruning, join/agg pushdown, versioning, and merged issue regressions.

### Testdata
- `pkg/planner/core/casetest/mpp/testdata/integration_suite_in.json`
- `pkg/planner/core/casetest/mpp/testdata/integration_suite_out.json`
- `pkg/planner/core/casetest/mpp/testdata/integration_suite_xut.json`

## pkg/planner/core/casetest/parallelapply

### Tests
- `pkg/planner/core/casetest/parallelapply/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/parallelapply/parallel_apply_test.go` - planner/core/casetest/parallelapply: Tests parallel apply warnings and apply join explain output.

## pkg/planner/core/casetest/partition

### Tests
- `pkg/planner/core/casetest/partition/integration_partition_test.go` - planner/core/casetest/partition: Tests list/list-columns pruning and dynamic/static plan differences.
- `pkg/planner/core/casetest/partition/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/partition/partition_pruner_test.go` - planner/core/casetest/partition: Tests partition pruning rules, extract functions, and merged issue regressions.

### Testdata
- `pkg/planner/core/casetest/partition/testdata/integration_partition_suite_in.json`
- `pkg/planner/core/casetest/partition/testdata/integration_partition_suite_out.json`
- `pkg/planner/core/casetest/partition/testdata/integration_partition_suite_xut.json`
- `pkg/planner/core/casetest/partition/testdata/partition_pruner_in.json`
- `pkg/planner/core/casetest/partition/testdata/partition_pruner_out.json`
- `pkg/planner/core/casetest/partition/testdata/partition_pruner_xut.json`

## pkg/planner/core/casetest/physicalplantest

### Tests
- `pkg/planner/core/casetest/physicalplantest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go` - planner/core/casetest/physicalplantest: Tests plan suite cases, MPP hints, agg elimination, and merged issue regressions.

### Testdata
- `pkg/planner/core/casetest/physicalplantest/testdata/cascades_template_in.json`
- `pkg/planner/core/casetest/physicalplantest/testdata/cascades_template_out.json`
- `pkg/planner/core/casetest/physicalplantest/testdata/cascades_template_xut.json`
- `pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_in.json`
- `pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_out.json`
- `pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_xut.json`

## pkg/planner/core/casetest/plancache

### Tests
- `pkg/planner/core/casetest/plancache/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/plancache/plan_cache_param_test.go` - planner/core/casetest/plancache: Tests AST parameterization, concurrent param SQL, and benchmarks.
- `pkg/planner/core/casetest/plancache/plan_cache_partition_table_test.go` - planner/core/casetest/plancache: Tests partition full-cover behavior for int/varchar keys.
- `pkg/planner/core/casetest/plancache/plan_cache_partition_test.go` - planner/core/casetest/plancache: Tests partition plan cache pruning, batch point get, and fix-control rebuilds.
- `pkg/planner/core/casetest/plancache/plan_cache_rebuild_test.go` - planner/core/casetest/plancache: Tests cached-plan clone/rebuild correctness and fast point get cloning.
- `pkg/planner/core/casetest/plancache/plan_cache_suite_test.go` - planner/core/casetest/plancache: Tests prepared/non-prepared plan cache regressions, bindings, MV index, and resource groups.
- `pkg/planner/core/casetest/plancache/plan_cache_test.go` - planner/core/casetest/plancache: Tests deallocate prepare cache separation and cache key benchmarks.
- `pkg/planner/core/casetest/plancache/plan_cacheable_checker_test.go` - planner/core/casetest/plancache: Tests cacheable checks, IN-list fix control, and merged issue regressions.

### Testdata
- `pkg/planner/core/casetest/plancache/testdata/plan_cache_suite_in.json`
- `pkg/planner/core/casetest/plancache/testdata/plan_cache_suite_out.json`

## pkg/planner/core/casetest/planstats

### Tests
- `pkg/planner/core/casetest/planstats/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/planstats/plan_stats_test.go` - planner/core/casetest/planstats: Tests stats load behavior across operators, partitions, and hints.

### Testdata
- `pkg/planner/core/casetest/planstats/testdata/plan_stats_suite_in.json`
- `pkg/planner/core/casetest/planstats/testdata/plan_stats_suite_out.json`
- `pkg/planner/core/casetest/planstats/testdata/plan_stats_suite_xut.json`

## pkg/planner/core/casetest/pushdown

### Tests
- `pkg/planner/core/casetest/pushdown/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/pushdown/push_down_test.go` - planner/core/casetest/pushdown: Tests TiFlash pushdown with keep-order and fastscan modes.

### Testdata
- `pkg/planner/core/casetest/pushdown/testdata/integration_suite_in.json`
- `pkg/planner/core/casetest/pushdown/testdata/integration_suite_out.json`
- `pkg/planner/core/casetest/pushdown/testdata/integration_suite_xut.json`

## pkg/planner/core/casetest/rule

### Tests
- `pkg/planner/core/casetest/rule/dual_test.go` - planner/core/casetest/rule: Tests constant/null predicate rewrites to TableDual.
- `pkg/planner/core/casetest/rule/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/rule/rule_derive_topn_from_window_test.go` - planner/core/casetest/rule: Tests derived TopN pushdown for TiFlash window plans.
- `pkg/planner/core/casetest/rule/rule_eliminate_empty_selection_test.go` - planner/core/casetest/rule: Tests empty selection elimination with complex joins.
- `pkg/planner/core/casetest/rule/rule_eliminate_projection_test.go` - planner/core/casetest/rule: Tests projection elimination with expression indexes.
- `pkg/planner/core/casetest/rule/rule_inject_extra_projection_test.go` - planner/core/casetest/rule: Tests cast injection for aggregate function modes.
- `pkg/planner/core/casetest/rule/rule_join_reorder_test.go` - planner/core/casetest/rule: Tests join reorder hints, hash-join toggle, and TiFlash/dynamic partitions.
- `pkg/planner/core/casetest/rule/rule_outer2inner_test.go` - planner/core/casetest/rule: Tests outer-to-inner rewrites with merged issue regressions.
- `pkg/planner/core/casetest/rule/rule_outer_to_semi_join_test.go` - planner/core/casetest/rule: Tests outer-to-semi join rewrite correctness.
- `pkg/planner/core/casetest/rule/rule_predicate_pushdown_test.go` - planner/core/casetest/rule: Tests predicate pushdown cases and records plan+result for selected cases.
- `pkg/planner/core/casetest/rule/rule_predicate_simplification_test.go` - planner/core/casetest/rule: Tests predicate simplification across complex schemas.

### Testdata
- `pkg/planner/core/casetest/rule/testdata/derive_topn_from_window_in.json`
- `pkg/planner/core/casetest/rule/testdata/derive_topn_from_window_out.json`
- `pkg/planner/core/casetest/rule/testdata/derive_topn_from_window_xut.json`
- `pkg/planner/core/casetest/rule/testdata/join_reorder_suite_in.json`
- `pkg/planner/core/casetest/rule/testdata/join_reorder_suite_out.json`
- `pkg/planner/core/casetest/rule/testdata/join_reorder_suite_xut.json`
- `pkg/planner/core/casetest/rule/testdata/outer2inner_in.json`
- `pkg/planner/core/casetest/rule/testdata/outer2inner_out.json`
- `pkg/planner/core/casetest/rule/testdata/outer2inner_xut.json`
- `pkg/planner/core/casetest/rule/testdata/outer_to_semi_join_suite_in.json`
- `pkg/planner/core/casetest/rule/testdata/outer_to_semi_join_suite_out.json`
- `pkg/planner/core/casetest/rule/testdata/outer_to_semi_join_suite_xut.json`
- `pkg/planner/core/casetest/rule/testdata/predicate_pushdown_suite_in.json`
- `pkg/planner/core/casetest/rule/testdata/predicate_pushdown_suite_out.json`
- `pkg/planner/core/casetest/rule/testdata/predicate_pushdown_suite_xut.json`
- `pkg/planner/core/casetest/rule/testdata/predicate_simplification_in.json`
- `pkg/planner/core/casetest/rule/testdata/predicate_simplification_out.json`
- `pkg/planner/core/casetest/rule/testdata/predicate_simplification_xut.json`

## pkg/planner/core/casetest/scalarsubquery

### Tests
- `pkg/planner/core/casetest/scalarsubquery/cases_test.go` - planner/core/casetest/scalarsubquery: Tests non-evaluated scalar subqueries and EXPLAIN ANALYZE output trimming.
- `pkg/planner/core/casetest/scalarsubquery/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite_in.json`
- `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite_out.json`
- `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite_xut.json`

## pkg/planner/core/casetest/tpcds

### Tests
- `pkg/planner/core/casetest/tpcds/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/tpcds/tpcds_test.go` - planner/core/casetest/tpcds: Tests TPCDS Q64.

### Testdata
- `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_in.json`
- `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_out.json`
- `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_xut.json`

## pkg/planner/core/casetest/tpch

### Tests
- `pkg/planner/core/casetest/tpch/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/tpch/tpch_test.go` - planner/core/casetest/tpch: Tests TPCH query suite outputs.

### Testdata
- `pkg/planner/core/casetest/tpch/testdata/test.lineitem.json`
- `pkg/planner/core/casetest/tpch/testdata/test.nation.json`
- `pkg/planner/core/casetest/tpch/testdata/test.orders.json`
- `pkg/planner/core/casetest/tpch/testdata/test.part.json`
- `pkg/planner/core/casetest/tpch/testdata/test.partsupp.json`
- `pkg/planner/core/casetest/tpch/testdata/test.region.json`
- `pkg/planner/core/casetest/tpch/testdata/test.supplier.json`
- `pkg/planner/core/casetest/tpch/testdata/tpch_suite_in.json`
- `pkg/planner/core/casetest/tpch/testdata/tpch_suite_out.json`
- `pkg/planner/core/casetest/tpch/testdata/tpch_suite_xut.json`

## pkg/planner/core/casetest/vectorsearch

### Tests
- `pkg/planner/core/casetest/vectorsearch/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/vectorsearch/vector_index_test.go` - planner/core/casetest/vectorsearch: Tests TiFlash ANN vector index plans and normalized plans.

### Testdata
- `pkg/planner/core/casetest/vectorsearch/testdata/ann_index_suite_in.json`
- `pkg/planner/core/casetest/vectorsearch/testdata/ann_index_suite_out.json`
- `pkg/planner/core/casetest/vectorsearch/testdata/ann_index_suite_xut.json`

## pkg/planner/core/casetest/windows

### Tests
- `pkg/planner/core/casetest/windows/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/casetest/windows/widow_with_exist_subquery_test.go` - planner/core/casetest/windows: Tests window functions with correlated EXISTS subqueries.
- `pkg/planner/core/casetest/windows/window_push_down_test.go` - planner/core/casetest/windows: Tests TiFlash window pushdown plans, warnings, and merged issue regressions.

### Testdata
- `pkg/planner/core/casetest/windows/testdata/window_push_down_suite_in.json`
- `pkg/planner/core/casetest/windows/testdata/window_push_down_suite_out.json`
- `pkg/planner/core/casetest/windows/testdata/window_push_down_suite_xut.json`

## pkg/planner/core/generator/hash64_equals

### Tests
- `pkg/planner/core/generator/hash64_equals/hash64_equals_test.go` - planner/core/generator/hash64_equals: Tests hash64-equals codegen output and reflect edge cases.

## pkg/planner/core/generator/plan_cache

### Tests
- `pkg/planner/core/generator/plan_cache/plan_clone_test.go` - planner/core/generator/plan_cache: Tests generated plan-clone code matches checked-in output.

## pkg/planner/core/generator/shallow_ref

### Tests
- `pkg/planner/core/generator/shallow_ref/shallow_ref_test.go` - planner/core/generator/shallow_ref: Tests shallow-ref codegen output matches checked-in file.

## pkg/planner/core/issuetest

### Tests
- `pkg/planner/core/issuetest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/issuetest/planner_issue_test.go` - planner/core/issuetest: Tests planner issue regressions.

## pkg/planner/core/operator/logicalop/logicalop_test

### Tests
- `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go` - planner/core/operator/logicalop: Tests Hash64/Equals across logical operators, windows, and handle columns.
- `pkg/planner/core/operator/logicalop/logicalop_test/logical_mem_table_predicate_extractor_test.go` - planner/core/operator/logicalop: Tests predicate extraction for cluster/info schema/inspection mem tables.
- `pkg/planner/core/operator/logicalop/logicalop_test/logical_operator_test.go` - planner/core/operator/logicalop: Tests logical clone semantics, projection pushdown, and expand key info.
- `pkg/planner/core/operator/logicalop/logicalop_test/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/operator/logicalop/logicalop_test/plan_execute_test.go` - planner/core/operator/logicalop: Tests issue58743 regression with CTEs and hints.

### Testdata
- `pkg/planner/core/operator/logicalop/logicalop_test/testdata/cascades_suite_in.json`
- `pkg/planner/core/operator/logicalop/logicalop_test/testdata/cascades_suite_out.json`
- `pkg/planner/core/operator/logicalop/logicalop_test/testdata/cascades_suite_xut.json`

## pkg/planner/core/operator/physicalop

### Tests
- `pkg/planner/core/operator/physicalop/fragment_test.go` - planner/core/operator/physicalop: Tests fragment singleton detection by exchange type.
- `pkg/planner/core/operator/physicalop/physical_utils_test.go` - planner/core/operator/physicalop: Tests flatten list/tree pushdown plan ordering.

## pkg/planner/core/rule

### Tests
- `pkg/planner/core/rule/collect_column_stats_usage_test.go` - planner/core/rule: Tests predicate column stats usage and stats-load items.
- `pkg/planner/core/rule/rule_partition_pruning_test.go` - planner/core/rule: Tests range partition pruning and binary search cases.

## pkg/planner/core/tests/analyze

### Tests
- `pkg/planner/core/tests/analyze/analyze_test.go` - planner/core/tests/analyze: Tests analyze on virtual columns and auto-analyze for missing partitions.
- `pkg/planner/core/tests/analyze/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/planner/core/tests/cte

### Tests
- `pkg/planner/core/tests/cte/cte_test.go` - planner/core/tests/cte: Tests CTE with different schema.
- `pkg/planner/core/tests/cte/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/planner/core/tests/extractor

### Tests
- `pkg/planner/core/tests/extractor/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/tests/extractor/memtable_infoschema_extractor_test.go` - planner/core/tests/extractor: Tests info schema memtable predicate extraction combinations.

## pkg/planner/core/tests/null

### Tests
- `pkg/planner/core/tests/null/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/tests/null/null_test.go` - planner/core/tests/null: Tests NULL-related planner regressions (issues 54803/55299/56745).

## pkg/planner/core/tests/partition

### Tests
- `pkg/planner/core/tests/partition/bench_test.go` - planner/core/tests/partition: Benchmarks partitioned point/batch/index/table access with plan cache.

## pkg/planner/core/tests/pointget

### Tests
- `pkg/planner/core/tests/pointget/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/tests/pointget/point_get_plan_test.go` - planner/core/tests/pointget: Tests point-get plan cache behavior and metrics.

## pkg/planner/core/tests/prepare

### Tests
- `pkg/planner/core/tests/prepare/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/tests/prepare/prepare_test.go` - planner/core/tests/prepare: Tests prepared plan cache behavior, flush, and metrics.

## pkg/planner/core/tests/redact

### Tests
- `pkg/planner/core/tests/redact/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/tests/redact/redact_test.go` - planner/core/tests/redact: Tests EXPLAIN redaction modes and range info redaction.

## pkg/planner/core/tests/rewriter

### Tests
- `pkg/planner/core/tests/rewriter/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/tests/rewriter/rewriter_test.go` - planner/core/tests/rewriter: Tests variable rewriter.

## pkg/planner/core/tests/subquery

### Tests
- `pkg/planner/core/tests/subquery/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/core/tests/subquery/subquery_test.go` - planner/core/tests/subquery: Tests collation-stable IN subquery planning.

## pkg/planner/funcdep

### Tests
- `pkg/planner/funcdep/extract_fd_test.go` - planner/funcdep: Tests FD extraction across projections and aggregates.
- `pkg/planner/funcdep/fd_graph_test.go` - planner/funcdep: Tests FDSet add, closure, and reduction helpers.
- `pkg/planner/funcdep/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/planner/implementation

### Tests
- `pkg/planner/implementation/base_test.go` - planner/implementation: Tests base impl cost getters/setters.
- `pkg/planner/implementation/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/planner/indexadvisor

### Tests
- `pkg/planner/indexadvisor/indexadvisor_sql_test.go` - planner/indexadvisor: Tests recommendations across queries, tables, and types.
- `pkg/planner/indexadvisor/indexadvisor_test.go` - planner/indexadvisor: Tests core index advisor behavior and filtering.
- `pkg/planner/indexadvisor/indexadvisor_tpch_test.go` - planner/indexadvisor: Tests index advisor on TPCH queries.
- `pkg/planner/indexadvisor/optimizer_test.go` - planner/indexadvisor: Tests optimizer helpers (types, columns, index checks, size).
- `pkg/planner/indexadvisor/options_test.go` - planner/indexadvisor: Tests index advisor option validation and behavior.
- `pkg/planner/indexadvisor/utils_test.go` - planner/indexadvisor: Tests query analysis helpers (tables/columns/filtering).

## pkg/planner/memo

### Tests
- `pkg/planner/memo/expr_iterator_test.go` - planner/memo: Tests expr iterator creation, iteration, and reset.
- `pkg/planner/memo/group_expr_test.go` - planner/memo: Tests group expr creation and fingerprinting.
- `pkg/planner/memo/group_test.go` - planner/memo: Tests group lifecycle, fingerprint, and lookup.
- `pkg/planner/memo/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/planner/planctx

### Tests
- `pkg/planner/planctx/context_test.go` - planner/planctx: Tests BuildPBContext Detach deep clone behavior.

## pkg/planner/property

### Tests
- `pkg/planner/property/physical_property_test.go` - planner/property: Tests MPP exchange decision by FD equivalence.

## pkg/planner/util

### Tests
- `pkg/planner/util/column_test.go` - planner/util: Tests index info to prefix/full columns.
- `pkg/planner/util/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/planner/util/path_test.go` - planner/util: Tests Col2Len compare and OnlyPointRange.
- `pkg/planner/util/slice_recursive_flatten_iter_test.go` - planner/util: Tests recursive slice flatten iterator.

## pkg/planner/util/fixcontrol

### Tests
- `pkg/planner/util/fixcontrol/fixcontrol_test.go` - planner/util/fixcontrol: Tests fix control.
- `pkg/planner/util/fixcontrol/main_test.go` - Configures default goleak settings and registers testdata.

### Testdata
- `pkg/planner/util/fixcontrol/testdata/fix_control_suite_in.json`
- `pkg/planner/util/fixcontrol/testdata/fix_control_suite_out.json`

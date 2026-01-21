# TiDB Planner Test Case Map (pkg/planner)

## Table of contents

- Overview
- Planner package map
- Cardinality
- Functional dependencies
- Memo
- Plan context
- Util
- Index advisor
- Implementation
- Cascades (old)
- Cascades base/pattern/memo/task/rule
- Property
- Planner core (root)
- Planner core (tests)
- Planner core (casetest)
- Planner core (operator tests)
- Other planner core generators

## Overview

This map lists existing `_test.go` files and `testdata` files under `pkg/planner` to help place new optimizer test cases and reuse existing fixtures.

Placement rules (planner/core and non-core packages are peers):

- **Planner/core optimizer casetests**: `pkg/planner/core/casetest/<type>`
- **Non-core planner packages**: place tests in their owning package directory (for example, `pkg/planner/cardinality`, `pkg/planner/funcdep`, `pkg/planner/memo`, `pkg/planner/util`).

## Planner package map

- `pkg/planner/cardinality`
- `pkg/planner/cascades`
- `pkg/planner/funcdep`
- `pkg/planner/implementation`
- `pkg/planner/indexadvisor`
- `pkg/planner/memo`
- `pkg/planner/planctx`
- `pkg/planner/property`
- `pkg/planner/util`

## Cardinality

- Tests:
  - `pkg/planner/cardinality/main_test.go`
  - `pkg/planner/cardinality/row_size_test.go`
  - `pkg/planner/cardinality/exponential_test.go`
  - `pkg/planner/cardinality/selectivity_test.go`
  - `pkg/planner/cardinality/ndv_test.go`
- Testdata:
  - `pkg/planner/cardinality/testdata/cardinality_suite_in.json`
  - `pkg/planner/cardinality/testdata/cardinality_suite_out.json`

## Functional dependencies

- Tests:
  - `pkg/planner/funcdep/main_test.go`
  - `pkg/planner/funcdep/extract_fd_test.go`
  - `pkg/planner/funcdep/fd_graph_test.go`

## Memo

- Tests:
  - `pkg/planner/memo/main_test.go`
  - `pkg/planner/memo/group_test.go`
  - `pkg/planner/memo/group_expr_test.go`
  - `pkg/planner/memo/expr_iterator_test.go`

## Plan context

- Tests:
  - `pkg/planner/planctx/context_test.go`

## Util

- Tests:
  - `pkg/planner/util/main_test.go`
  - `pkg/planner/util/path_test.go`
  - `pkg/planner/util/column_test.go`
  - `pkg/planner/util/slice_recursive_flatten_iter_test.go`
- Fix control tests and testdata:
  - `pkg/planner/util/fixcontrol/main_test.go`
  - `pkg/planner/util/fixcontrol/fixcontrol_test.go`
  - `pkg/planner/util/fixcontrol/testdata/fix_control_suite_in.json`
  - `pkg/planner/util/fixcontrol/testdata/fix_control_suite_out.json`

## Index advisor

- Tests:
  - `pkg/planner/indexadvisor/options_test.go`
  - `pkg/planner/indexadvisor/indexadvisor_sql_test.go`
  - `pkg/planner/indexadvisor/indexadvisor_tpch_test.go`
  - `pkg/planner/indexadvisor/optimizer_test.go`
  - `pkg/planner/indexadvisor/utils_test.go`
  - `pkg/planner/indexadvisor/indexadvisor_test.go`

## Implementation

- Tests:
  - `pkg/planner/implementation/main_test.go`
  - `pkg/planner/implementation/base_test.go`

## Cascades (old)

- Tests:
  - `pkg/planner/cascades/old/main_test.go`
  - `pkg/planner/cascades/old/stringer_test.go`
  - `pkg/planner/cascades/old/optimize_test.go`
  - `pkg/planner/cascades/old/transformation_rules_test.go`
  - `pkg/planner/cascades/old/enforcer_rules_test.go`
- Testdata:
  - `pkg/planner/cascades/old/testdata/stringer_suite_in.json`
  - `pkg/planner/cascades/old/testdata/stringer_suite_out.json`
  - `pkg/planner/cascades/old/testdata/transformation_rules_suite_in.json`
  - `pkg/planner/cascades/old/testdata/transformation_rules_suite_out.json`

## Cascades base/pattern/memo/task/rule

- Root tests:
  - `pkg/planner/cascades/cascades_test.go`
- Base tests:
  - `pkg/planner/cascades/base/base_test.go`
  - `pkg/planner/cascades/base/hash_equaler_test.go`
- Pattern tests:
  - `pkg/planner/cascades/pattern/pattern_test.go`
  - `pkg/planner/cascades/pattern/engine_test.go`
- Memo tests:
  - `pkg/planner/cascades/memo/main_test.go`
  - `pkg/planner/cascades/memo/memo_test.go`
  - `pkg/planner/cascades/memo/group_id_generator_test.go`
  - `pkg/planner/cascades/memo/group_and_expr_test.go`
- Task tests:
  - `pkg/planner/cascades/task/task_scheduler_test.go`
  - `pkg/planner/cascades/task/task_test.go`
- Rule tests:
  - `pkg/planner/cascades/rule/binder_test.go`
  - `pkg/planner/cascades/rule/apply/decorrelateapply/xf_decorrelate_apply_test.go`

## Property

- Tests:
  - `pkg/planner/property/physical_property_test.go`

## Planner core (root)

- Tests:
  - `pkg/planner/core/main_test.go`
  - `pkg/planner/core/expression_test.go`
  - `pkg/planner/core/stringer_test.go`
  - `pkg/planner/core/find_best_task_test.go`
  - `pkg/planner/core/preprocess_test.go`
  - `pkg/planner/core/runtime_filter_generator_test.go`
  - `pkg/planner/core/plan_test.go`
  - `pkg/planner/core/task_test.go`
  - `pkg/planner/core/plan_cost_ver1_test.go`
  - `pkg/planner/core/rule_join_reorder_dp_test.go`
  - `pkg/planner/core/physical_plan_test.go`
  - `pkg/planner/core/cbo_test.go`
  - `pkg/planner/core/rule_generate_column_substitute_test.go`
  - `pkg/planner/core/exhaust_physical_plans_test.go`
  - `pkg/planner/core/hint_test.go`
  - `pkg/planner/core/planbuilder_test.go`
  - `pkg/planner/core/integration_test.go`
  - `pkg/planner/core/plan_cost_ver2_test.go`
  - `pkg/planner/core/stats_test.go`
  - `pkg/planner/core/plan_cache_instance_test.go`
  - `pkg/planner/core/plan_cache_lru_test.go`
  - `pkg/planner/core/plan_replayer_capture_test.go`
  - `pkg/planner/core/optimizer_test.go`
  - `pkg/planner/core/logical_plans_test.go`
  - `pkg/planner/core/common_plans_test.go`
  - `pkg/planner/core/util_test.go`
  - `pkg/planner/core/plan_to_pb_test.go`
  - `pkg/planner/core/enforce_mpp_test.go`
  - `pkg/planner/core/binary_plan_test.go`
- Testdata:
  - `pkg/planner/core/testdata/runtime_filter_generator_suite_in.json`
  - `pkg/planner/core/testdata/runtime_filter_generator_suite_out.json`
  - `pkg/planner/core/testdata/plan_suite_unexported_in.json`
  - `pkg/planner/core/testdata/plan_suite_unexported_out.json`

## Planner core (tests)

- Subdir tests:
  - `pkg/planner/core/tests/cte/main_test.go`
  - `pkg/planner/core/tests/cte/cte_test.go`
  - `pkg/planner/core/tests/extractor/main_test.go`
  - `pkg/planner/core/tests/extractor/memtable_infoschema_extractor_test.go`
  - `pkg/planner/core/tests/partition/bench_test.go`
  - `pkg/planner/core/tests/subquery/main_test.go`
  - `pkg/planner/core/tests/subquery/subquery_test.go`
  - `pkg/planner/core/tests/analyze/main_test.go`
  - `pkg/planner/core/tests/analyze/analyze_test.go`
  - `pkg/planner/core/tests/null/main_test.go`
  - `pkg/planner/core/tests/null/null_test.go`
  - `pkg/planner/core/tests/prepare/main_test.go`
  - `pkg/planner/core/tests/prepare/prepare_test.go`
  - `pkg/planner/core/tests/pointget/main_test.go`
  - `pkg/planner/core/tests/pointget/point_get_plan_test.go`
  - `pkg/planner/core/tests/redact/main_test.go`
  - `pkg/planner/core/tests/redact/redact_test.go`
  - `pkg/planner/core/tests/rewriter/main_test.go`
  - `pkg/planner/core/tests/rewriter/rewriter_test.go`

## Planner core (casetest)

- Root casetest:
  - `pkg/planner/core/casetest/main_test.go`
  - `pkg/planner/core/casetest/plan_test.go`
  - `pkg/planner/core/casetest/stats_test.go`
  - `pkg/planner/core/casetest/tiflash_predicate_push_down_test.go`
  - `pkg/planner/core/casetest/integration_test.go`
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
  - `pkg/planner/core/casetest/testdata/outer_to_semi_join_in.json`
- CBO:
  - `pkg/planner/core/casetest/cbotest/main_test.go`
  - `pkg/planner/core/casetest/cbotest/cbo_test.go`
  - `pkg/planner/core/casetest/cbotest/testdata/analyze_suite_in.json`
  - `pkg/planner/core/casetest/cbotest/testdata/analyze_suite_out.json`
  - `pkg/planner/core/casetest/cbotest/testdata/analyze_suite_xut.json`
  - `pkg/planner/core/casetest/cbotest/testdata/analyzeSuiteTestIndexEqualUnknownT.json`
  - `pkg/planner/core/casetest/cbotest/testdata/analyzeSuiteTestIndexReadT.json`
  - `pkg/planner/core/casetest/cbotest/testdata/analyzeSuiteTestLimitIndexEstimationT.json`
  - `pkg/planner/core/casetest/cbotest/testdata/analyzeSuiteTestLowSelIndexGreedySearchT.json`
  - `pkg/planner/core/casetest/cbotest/testdata/issue59563.json`
  - `pkg/planner/core/casetest/cbotest/testdata/issue61792.json`
  - `pkg/planner/core/casetest/cbotest/testdata/issue62438.json`
  - `pkg/planner/core/casetest/cbotest/testdata/test.t0da79f8d.json`
  - `pkg/planner/core/casetest/cbotest/testdata/test.t19f3e4f1.json`
- Rule:
  - `pkg/planner/core/casetest/rule/main_test.go`
  - `pkg/planner/core/casetest/rule/dual_test.go`
  - `pkg/planner/core/casetest/rule/rule_eliminate_projection_test.go`
  - `pkg/planner/core/casetest/rule/rule_join_reorder_test.go`
  - `pkg/planner/core/casetest/rule/rule_outer_to_semi_join_test.go`
  - `pkg/planner/core/casetest/rule/rule_predicate_pushdown_test.go`
  - `pkg/planner/core/casetest/rule/rule_inject_extra_projection_test.go`
  - `pkg/planner/core/casetest/rule/rule_outer2inner_test.go`
  - `pkg/planner/core/casetest/rule/rule_derive_topn_from_window_test.go`
  - `pkg/planner/core/casetest/rule/rule_predicate_simplification_test.go`
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
- Hint:
  - `pkg/planner/core/casetest/hint/main_test.go`
  - `pkg/planner/core/casetest/hint/hint_test.go`
  - `pkg/planner/core/casetest/hint/testdata/integration_suite_in.json`
  - `pkg/planner/core/casetest/hint/testdata/integration_suite_out.json`
  - `pkg/planner/core/casetest/hint/testdata/integration_suite_xut.json`
- Index:
  - `pkg/planner/core/casetest/index/main_test.go`
  - `pkg/planner/core/casetest/index/index_test.go`
  - `pkg/planner/core/casetest/index/testdata/index_range_in.json`
  - `pkg/planner/core/casetest/index/testdata/index_range_out.json`
  - `pkg/planner/core/casetest/index/testdata/index_range_xut.json`
  - `pkg/planner/core/casetest/index/testdata/integration_suite_in.json`
  - `pkg/planner/core/casetest/index/testdata/integration_suite_out.json`
  - `pkg/planner/core/casetest/index/testdata/integration_suite_xut.json`
- Index merge:
  - `pkg/planner/core/casetest/indexmerge/main_test.go`
  - `pkg/planner/core/casetest/indexmerge/indexmerge_test.go`
  - `pkg/planner/core/casetest/indexmerge/indexmerge_path_test.go`
  - `pkg/planner/core/casetest/indexmerge/indexmerge_intersection_test.go`
  - `pkg/planner/core/casetest/indexmerge/testdata/index_merge_suite_in.json`
  - `pkg/planner/core/casetest/indexmerge/testdata/index_merge_suite_out.json`
  - `pkg/planner/core/casetest/indexmerge/testdata/index_merge_suite_xut.json`
- Join:
  - `pkg/planner/core/casetest/join/main_test.go`
  - `pkg/planner/core/casetest/join/join_test.go`
- Correlated:
  - `pkg/planner/core/casetest/correlated/main_test.go`
  - `pkg/planner/core/casetest/correlated/correlated_test.go`
- Scalar subquery:
  - `pkg/planner/core/casetest/scalarsubquery/main_test.go`
  - `pkg/planner/core/casetest/scalarsubquery/cases_test.go`
  - `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite_in.json`
  - `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite_out.json`
  - `pkg/planner/core/casetest/scalarsubquery/testdata/plan_suite_xut.json`
- Parallel apply:
  - `pkg/planner/core/casetest/parallelapply/main_test.go`
  - `pkg/planner/core/casetest/parallelapply/parallel_apply_test.go`
- Pushdown:
  - `pkg/planner/core/casetest/pushdown/main_test.go`
  - `pkg/planner/core/casetest/pushdown/push_down_test.go`
  - `pkg/planner/core/casetest/pushdown/testdata/integration_suite_in.json`
  - `pkg/planner/core/casetest/pushdown/testdata/integration_suite_out.json`
  - `pkg/planner/core/casetest/pushdown/testdata/integration_suite_xut.json`
- MPP:
  - `pkg/planner/core/casetest/mpp/main_test.go`
  - `pkg/planner/core/casetest/mpp/mpp_test.go`
  - `pkg/planner/core/casetest/mpp/testdata/integration_suite_in.json`
  - `pkg/planner/core/casetest/mpp/testdata/integration_suite_out.json`
  - `pkg/planner/core/casetest/mpp/testdata/integration_suite_xut.json`
- Enforce MPP:
  - `pkg/planner/core/casetest/enforcempp/main_test.go`
  - `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go`
  - `pkg/planner/core/casetest/enforcempp/testdata/enforce_mpp_suite_in.json`
  - `pkg/planner/core/casetest/enforcempp/testdata/enforce_mpp_suite_out.json`
  - `pkg/planner/core/casetest/enforcempp/testdata/enforce_mpp_suite_xut.json`
- Partition:
  - `pkg/planner/core/casetest/partition/main_test.go`
  - `pkg/planner/core/casetest/partition/integration_partition_test.go`
  - `pkg/planner/core/casetest/partition/partition_pruner_test.go`
  - `pkg/planner/core/casetest/partition/testdata/integration_partition_suite_in.json`
  - `pkg/planner/core/casetest/partition/testdata/integration_partition_suite_out.json`
  - `pkg/planner/core/casetest/partition/testdata/integration_partition_suite_xut.json`
  - `pkg/planner/core/casetest/partition/testdata/partition_pruner_in.json`
  - `pkg/planner/core/casetest/partition/testdata/partition_pruner_out.json`
  - `pkg/planner/core/casetest/partition/testdata/partition_pruner_xut.json`
- DAG:
  - `pkg/planner/core/casetest/dag/main_test.go`
  - `pkg/planner/core/casetest/dag/dag_test.go`
  - `pkg/planner/core/casetest/dag/testdata/plan_suite_in.json`
  - `pkg/planner/core/casetest/dag/testdata/plan_suite_out.json`
  - `pkg/planner/core/casetest/dag/testdata/plan_suite_xut.json`
- Physical plan:
  - `pkg/planner/core/casetest/physicalplantest/main_test.go`
  - `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go`
  - `pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_in.json`
  - `pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_out.json`
  - `pkg/planner/core/casetest/physicalplantest/testdata/plan_suite_xut.json`
  - `pkg/planner/core/casetest/physicalplantest/testdata/cascades_template_in.json`
  - `pkg/planner/core/casetest/physicalplantest/testdata/cascades_template_out.json`
  - `pkg/planner/core/casetest/physicalplantest/testdata/cascades_template_xut.json`
- Cascades:
  - `pkg/planner/core/casetest/cascades/main_test.go`
  - `pkg/planner/core/casetest/cascades/memo_test.go`
  - `pkg/planner/core/casetest/cascades/testdata/cascades_suite_in.json`
  - `pkg/planner/core/casetest/cascades/testdata/cascades_suite_out.json`
  - `pkg/planner/core/casetest/cascades/testdata/cascades_template_in.json`
  - `pkg/planner/core/casetest/cascades/testdata/cascades_template_out.json`
  - `pkg/planner/core/casetest/cascades/testdata/cascades_template_xut.json`
- Flat plan:
  - `pkg/planner/core/casetest/flatplan/main_test.go`
  - `pkg/planner/core/casetest/flatplan/flat_plan_test.go`
  - `pkg/planner/core/casetest/flatplan/testdata/flat_plan_suite_in.json`
  - `pkg/planner/core/casetest/flatplan/testdata/flat_plan_suite_out.json`
  - `pkg/planner/core/casetest/flatplan/testdata/flat_plan_suite_xut.json`
- Binary plan:
  - `pkg/planner/core/casetest/binaryplan/main_test.go`
  - `pkg/planner/core/casetest/binaryplan/binary_plan_test.go`
  - `pkg/planner/core/casetest/binaryplan/testdata/binary_plan_suite_in.json`
  - `pkg/planner/core/casetest/binaryplan/testdata/binary_plan_suite_out.json`
  - `pkg/planner/core/casetest/binaryplan/testdata/binary_plan_suite_xut.json`
- Window:
  - `pkg/planner/core/casetest/windows/main_test.go`
  - `pkg/planner/core/casetest/windows/window_push_down_test.go`
  - `pkg/planner/core/casetest/windows/widow_with_exist_subquery_test.go`
  - `pkg/planner/core/casetest/windows/testdata/window_push_down_suite_in.json`
  - `pkg/planner/core/casetest/windows/testdata/window_push_down_suite_out.json`
  - `pkg/planner/core/casetest/windows/testdata/window_push_down_suite_xut.json`
- Logical plan:
  - `pkg/planner/core/casetest/logicalplan/main_test.go`
  - `pkg/planner/core/casetest/logicalplan/logical_plan_builder_test.go`
- Plan stats:
  - `pkg/planner/core/casetest/planstats/main_test.go`
  - `pkg/planner/core/casetest/planstats/plan_stats_test.go`
  - `pkg/planner/core/casetest/planstats/testdata/plan_stats_suite_in.json`
  - `pkg/planner/core/casetest/planstats/testdata/plan_stats_suite_out.json`
  - `pkg/planner/core/casetest/planstats/testdata/plan_stats_suite_xut.json`
- Plan cache:
  - `pkg/planner/core/casetest/plancache/main_test.go`
  - `pkg/planner/core/casetest/plancache/plan_cache_suite_test.go`
  - `pkg/planner/core/casetest/plancache/plan_cache_test.go`
  - `pkg/planner/core/casetest/plancache/plan_cache_rebuild_test.go`
  - `pkg/planner/core/casetest/plancache/plan_cache_partition_test.go`
  - `pkg/planner/core/casetest/plancache/plan_cache_partition_table_test.go`
  - `pkg/planner/core/casetest/plancache/plan_cacheable_checker_test.go`
  - `pkg/planner/core/casetest/plancache/plan_cache_param_test.go`
  - `pkg/planner/core/casetest/plancache/testdata/plan_cache_suite_in.json`
  - `pkg/planner/core/casetest/plancache/testdata/plan_cache_suite_out.json`
- Instance plan cache:
  - `pkg/planner/core/casetest/instanceplancache/main_test.go`
  - `pkg/planner/core/casetest/instanceplancache/concurrency_tpcc_test.go`
  - `pkg/planner/core/casetest/instanceplancache/others_test.go`
  - `pkg/planner/core/casetest/instanceplancache/dml_test.go`
  - `pkg/planner/core/casetest/instanceplancache/concurrency_test.go`
  - `pkg/planner/core/casetest/instanceplancache/builtin_func_test.go`
- MPP (planner root integration):
  - `pkg/planner/core/integration_partition_test.go`
- Vector search:
  - `pkg/planner/core/casetest/vectorsearch/main_test.go`
  - `pkg/planner/core/casetest/vectorsearch/vector_index_test.go`
  - `pkg/planner/core/casetest/vectorsearch/testdata/ann_index_suite_in.json`
  - `pkg/planner/core/casetest/vectorsearch/testdata/ann_index_suite_out.json`
  - `pkg/planner/core/casetest/vectorsearch/testdata/ann_index_suite_xut.json`
- CH/TPCH/TPCDS:
  - `pkg/planner/core/casetest/ch/main_test.go`
  - `pkg/planner/core/casetest/ch/ch_test.go`
  - `pkg/planner/core/casetest/ch/testdata/ch_suite_in.json`
  - `pkg/planner/core/casetest/ch/testdata/ch_suite_out.json`
  - `pkg/planner/core/casetest/ch/testdata/ch_suite_xut.json`
  - `pkg/planner/core/casetest/ch/testdata/tpcc.customer.json`
  - `pkg/planner/core/casetest/ch/testdata/tpcc.nation.json`
  - `pkg/planner/core/casetest/ch/testdata/tpcc.orders.json`
  - `pkg/planner/core/casetest/ch/testdata/tpcc.order_line.json`
  - `pkg/planner/core/casetest/ch/testdata/tpcc.region.json`
  - `pkg/planner/core/casetest/ch/testdata/tpcc.stock.json`
  - `pkg/planner/core/casetest/ch/testdata/tpcc.supplier.json`
  - `pkg/planner/core/casetest/ch/testdata/tpcc.item.json`
  - `pkg/planner/core/casetest/tpch/main_test.go`
  - `pkg/planner/core/casetest/tpch/tpch_test.go`
  - `pkg/planner/core/casetest/tpch/testdata/tpch_suite_in.json`
  - `pkg/planner/core/casetest/tpch/testdata/tpch_suite_out.json`
  - `pkg/planner/core/casetest/tpch/testdata/tpch_suite_xut.json`
  - `pkg/planner/core/casetest/tpch/testdata/test.lineitem.json`
  - `pkg/planner/core/casetest/tpch/testdata/test.nation.json`
  - `pkg/planner/core/casetest/tpch/testdata/test.orders.json`
  - `pkg/planner/core/casetest/tpch/testdata/test.part.json`
  - `pkg/planner/core/casetest/tpch/testdata/test.partsupp.json`
  - `pkg/planner/core/casetest/tpch/testdata/test.region.json`
  - `pkg/planner/core/casetest/tpch/testdata/test.supplier.json`
  - `pkg/planner/core/casetest/tpcds/main_test.go`
  - `pkg/planner/core/casetest/tpcds/tpcds_test.go`
  - `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_in.json`
  - `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_out.json`
  - `pkg/planner/core/casetest/tpcds/testdata/tpcds_suite_xut.json`

## Planner core (operator tests)

- Logical operator tests:
  - `pkg/planner/core/operator/logicalop/logicalop_test/main_test.go`
  - `pkg/planner/core/operator/logicalop/logicalop_test/plan_execute_test.go`
  - `pkg/planner/core/operator/logicalop/logicalop_test/logical_operator_test.go`
  - `pkg/planner/core/operator/logicalop/logicalop_test/logical_mem_table_predicate_extractor_test.go`
  - `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go`
  - `pkg/planner/core/operator/logicalop/logicalop_test/testdata/cascades_suite_in.json`
  - `pkg/planner/core/operator/logicalop/logicalop_test/testdata/cascades_suite_out.json`
  - `pkg/planner/core/operator/logicalop/logicalop_test/testdata/cascades_suite_xut.json`
- Physical operator tests:
  - `pkg/planner/core/operator/physicalop/fragment_test.go`
  - `pkg/planner/core/operator/physicalop/physical_utils_test.go`

## Other planner core generators

- Plan cache generator:
  - `pkg/planner/core/generator/plan_cache/plan_clone_test.go`
- Hash64 equals generator:
  - `pkg/planner/core/generator/hash64_equals/hash64_equals_test.go`
- Shallow ref generator:
  - `pkg/planner/core/generator/shallow_ref/shallow_ref_test.go`

#!/bin/bash
# Script to regenerate test results for tests 16-76
# Run this from /Users/terrypurcell/tidb/tests/integrationtest directory

cd /Users/terrypurcell/tidb/tests/integrationtest

# Test 16
./run-tests.sh -s ../../bin/tidb-server -r executor/issues

# Test 17
./run-tests.sh -s ../../bin/tidb-server -r executor/jointest/hash_join

# Test 18
./run-tests.sh -s ../../bin/tidb-server -r executor/partition/table

# Test 19
./run-tests.sh -s ../../bin/tidb-server -r executor/point_get

# Test 20
./run-tests.sh -s ../../bin/tidb-server -r explain

# Test 21
./run-tests.sh -s ../../bin/tidb-server -r explain-non-select-stmt

# Test 22
./run-tests.sh -s ../../bin/tidb-server -r explain_complex

# Test 23
./run-tests.sh -s ../../bin/tidb-server -r explain_complex_stats

# Test 24
./run-tests.sh -s ../../bin/tidb-server -r explain_cte

# Test 25
./run-tests.sh -s ../../bin/tidb-server -r explain_easy

# Test 26
./run-tests.sh -s ../../bin/tidb-server -r explain_easy_stats

# Test 27
./run-tests.sh -s ../../bin/tidb-server -r explain_generate_column_substitute

# Test 28
./run-tests.sh -s ../../bin/tidb-server -r explain_join_stats

# Test 29
./run-tests.sh -s ../../bin/tidb-server -r explain_shard_index

# Test 30
./run-tests.sh -s ../../bin/tidb-server -r expression/charset_and_collation

# Test 31
./run-tests.sh -s ../../bin/tidb-server -r expression/explain

# Test 32
./run-tests.sh -s ../../bin/tidb-server -r expression/issues

# Test 33
./run-tests.sh -s ../../bin/tidb-server -r expression/misc

# Test 34
./run-tests.sh -s ../../bin/tidb-server -r expression/time

# Test 35
./run-tests.sh -s ../../bin/tidb-server -r expression/vitess_hash

# Test 36
./run-tests.sh -s ../../bin/tidb-server -r generated_columns

# Test 37
./run-tests.sh -s ../../bin/tidb-server -r globalindex/aggregate

# Test 38
./run-tests.sh -s ../../bin/tidb-server -r index_merge

# Test 39
./run-tests.sh -s ../../bin/tidb-server -r infoschema/infoschema

# Test 40
./run-tests.sh -s ../../bin/tidb-server -r naaj

# Test 41
./run-tests.sh -s ../../bin/tidb-server -r null_rejected

# Test 42
./run-tests.sh -s ../../bin/tidb-server -r planner/cascades/integration

# Test 43
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/expression_rewriter

# Test 44
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/hint/hint

# Test 45
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/index/index

# Test 46
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/integration

# Test 47
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/partition/integration_partition

# Test 48
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/physicalplantest/physical_plan

# Test 49
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/predicate_simplification

# Test 50
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/pushdown/push_down

# Test 51
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/rule/rule_derive_topn_from_window

# Test 52
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/rule/rule_join_reorder

# Test 53
./run-tests.sh -s ../../bin/tidb-server -r planner/core/casetest/rule/rule_result_reorder

# Test 54
./run-tests.sh -s ../../bin/tidb-server -r planner/core/cbo

# Test 55
./run-tests.sh -s ../../bin/tidb-server -r planner/core/expression_rewriter

# Test 56
./run-tests.sh -s ../../bin/tidb-server -r planner/core/indexjoin

# Test 57
./run-tests.sh -s ../../bin/tidb-server -r planner/core/indexmerge_path

# Test 58
./run-tests.sh -s ../../bin/tidb-server -r planner/core/integration

# Test 59
./run-tests.sh -s ../../bin/tidb-server -r planner/core/issuetest/planner_issue

# Test 60
./run-tests.sh -s ../../bin/tidb-server -r planner/core/partition_pruner

# Test 61
./run-tests.sh -s ../../bin/tidb-server -r planner/core/physical_plan

# Test 62
./run-tests.sh -s ../../bin/tidb-server -r planner/core/plan

# Test 63
./run-tests.sh -s ../../bin/tidb-server -r planner/core/plan_cache

# Test 64
./run-tests.sh -s ../../bin/tidb-server -r planner/core/point_get_plan

# Test 65
./run-tests.sh -s ../../bin/tidb-server -r planner/core/range_scan_for_like

# Test 66
./run-tests.sh -s ../../bin/tidb-server -r planner/core/rule_constant_propagation

# Test 67
./run-tests.sh -s ../../bin/tidb-server -r planner/core/rule_outer2inner

# Test 68
./run-tests.sh -s ../../bin/tidb-server -r select

# Test 69
./run-tests.sh -s ../../bin/tidb-server -r session/clustered_index

# Test 70
./run-tests.sh -s ../../bin/tidb-server -r subquery

# Test 71
./run-tests.sh -s ../../bin/tidb-server -r table/partition

# Test 72
./run-tests.sh -s ../../bin/tidb-server -r topn_push_down

# Test 73
./run-tests.sh -s ../../bin/tidb-server -r topn_pushdown

# Test 74
./run-tests.sh -s ../../bin/tidb-server -r tpch

# Test 75
./run-tests.sh -s ../../bin/tidb-server -r util/ranger

# Test 76
./run-tests.sh -s ../../bin/tidb-server -r window_function

echo "All tests completed!"

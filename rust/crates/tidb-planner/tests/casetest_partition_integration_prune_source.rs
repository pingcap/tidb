// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Documentary gap ports for the `integration_partition_test.go` half of
//! `pkg/planner/core/casetest/partition` (`pkg/planner.part6` items 334–340 on
//! `origin/master`; family bootstrap is `partition/main_test.go:30 TestMain`,
//! skipped-reason in the receipt; the `partition_pruner_test.go` half is its
//! own file, and the list-partition integration file likewise).
//!
//! Every test here plans partitioned tables through a live session with the
//! `forceDynamicPrune` failpoint (or kernel-type gates) forcing dynamic prune,
//! then compares both prune modes' EXPLAIN plan_tree outputs against the
//! `integration_partition_suite` book. tidb-planner has no partition pruner
//! (`pkg/planner/core/partition_pruning.go` unported), no DDL/executor layer
//! for the CREATE TABLEs, and no explain printer over whole plans.

/// GO PORT of `pkg/planner/core/casetest/partition/
/// integration_partition_test.go:70 TestListPartitionPruning`.
///
/// Re-derived contract: shared helper `testListPartitionPruning` (:28)
/// creates `tlist` (LIST(a)) + `tcollist` (LIST COLUMNS(a)) in
/// list_partition_pruning with p0..p4 value buckets (-1..11) plus a pMax-less
/// range layout, analyzes both, and for every golden input asserts the
/// dynamic-prune and static-prune EXPLAIN rows separately — under the classic
/// kernel only (skips next-gen runs via `kerneltype.IsNextGen`). The
/// failpoint forces dynamic mode off its variable default.
#[test]
#[ignore = "go-parity-gap: needs live DDL+session, forceDynamicPrune failpoint and dual-mode golden explains of the unported partition pruner"]
fn list_partition_pruning_dual_mode_golden_classic_kernel() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// integration_partition_test.go:78 TestListPartitionPruningForNextGen`.
///
/// Re-derived contract: exactly the helper's assertions but gated to run only
/// under the next-gen kernel (`kerneltype.IsClassic()` skips), sharing
/// `testListPartitionPruning` verbatim.
#[test]
#[ignore = "go-parity-gap: same surface as the classic-kernel sibling plus an absent kernel-type gate"]
fn list_partition_pruning_dual_mode_golden_next_gen_kernel() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// integration_partition_test.go:86 TestPartitionTableExplain`.
///
/// Re-derived contract: hash-partitioned `t` keyed on the clustered PK joined
/// against plain t2, analyzed, must produce per-input DynamicPlan/StaticPlan
/// pairs under forced dynamic pruning.
#[test]
#[ignore = "go-parity-gap: hash-partition access-path selection across prune modes needs the unported pruner and executor"]
fn partition_table_explain_hash_partitions_dual_mode() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// integration_partition_test.go:122 TestBatchPointGetTablePartition`.
///
/// Re-derived contract: nine tables covering nonclustered/clustered PKs under
/// hash/range/list partitions (+ issue 45889 one-column list) each get their
/// batch-point-get queries planned twice per prune mode: once as plan_tree
/// goldens and once executed, with result rows sorted when no ORDER BY and
/// asserted equal between modes.
#[test]
#[ignore = "go-parity-gap: BatchPointGet over partitions spans unported point-get planning, pruner and execution result comparison"]
fn batch_point_get_table_partition_prunes_and_round_trips() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// integration_partition_test.go:207 TestBatchPointGetPartitionForAccessObject`.
///
/// Re-derived contract: unique-key/hash(4-way)/list/list-columns(2-col
/// tuple-including-varchar) partitions must expose the right access object
/// (`partition:pN[,pM]`) inside their Point_Get/IndexReader plan trees in
/// forced-dynamic mode.
#[test]
#[ignore = "go-parity-gap: access-object construction for point gets over multiple partition schemes is unported"]
fn batch_point_get_access_object_lists_exact_partitions() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// integration_partition_test.go:260 TestGeneratedColumnWithPartition`,
/// issue 58475.
///
/// Re-derived contract: virtual generated column c2=c1 in a range-partitioned
/// table with FORCE_INDEX hint groups by id where the generated column filter
/// appears after partition pruning — the query must not error on the virtual
/// column resolution order.
#[test]
#[ignore = "go-parity-gap: needs execution of the SELECT plus generated-column handling inside unported index/sort merge"]
fn generated_column_with_partition_index_hint_survives_group_by() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// integration_partition_test.go:279 TestPartitionPruneWithPredicateSimplification`.
///
/// Re-derived contract: with predicate simplification ON, the gbk-bin char-key
/// range-columns partition table returns `TableDual root rows:0` under the
/// set_var static hint (the IN-lists simplify away every partition boundary),
/// while the second unhinted run keeps the recorded alternative plan.
#[test]
#[ignore = "go-parity-gap: gbk collation range-columns pruning plus predicate simplification rules are unported"]
fn partition_prune_predicate_simplification_yields_table_dual() {}

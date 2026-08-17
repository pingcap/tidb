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

//! Source-backed planner primitives, rooted on a plan interface tree.
//!
//! # The plan tree
//!
//! [`logical::LogicalPlan`] and [`physical::PhysicalPlan`] are this crate's
//! plan representation, over the shared [`plan_base::BasePlan`]. They port
//! Go's `pkg/planner/core/base.Plan` / `LogicalPlan` / `PhysicalPlan`
//! interfaces and the `baseimpl` / `logicalop` / `physicalop` base structs.
//!
//! This SUPERSEDES the crate's earlier position that it would carry no plan
//! representation. That position was written when the leaves here were
//! dependency-closed formulas with no shared node type, and it is no longer
//! the shape of the crate: without a tree, the ~40 `logical_*`/`physical_*`
//! modules are standalone `*Identity` structs and cost formulas that no
//! optimizer pass can be written against. The tree is what those become
//! passes over.
//!
//! It is a SEED of `pkg/planner/core`: the tree and its method surface land
//! complete; the operator set does not, and every operator not yet ported is
//! an explicit `Todo` variant naming its Go type rather than a default arm.
//!
//! # One tree is the truth; the two reduced views are bridged or dying
//!
//! [`logical::LogicalPlan`] is the SINGLE SOURCE OF TRUTH (user-ratified),
//! which is also Go's own shape: one `base.LogicalPlan` interface carries
//! `RecursiveDeriveStats` and `findBestTask` alike. Three logical shapes
//! still exist in this crate, but they are no longer three independent
//! representations:
//!
//! 1. [`logical::LogicalPlan`] — the closed enum above, built by
//!    [`plan_builder`], rewritten by [`logical::rule`], and now the tree the
//!    passes run over: `LogicalPlan::recursive_derive_stats` derives and
//!    writes statistics over it directly.
//! 2. `find_best_task::LogicalNode` — the join enumeration's reduced view,
//!    now PRODUCED FROM (1) by `find_best_task::project_join_spine`. Its
//!    leaves still take caller-supplied access-path alternatives, because
//!    access-path lists build empty — a named residue, not a hidden one.
//! 3. `cardinality::derive_stats::LogicalNode` — the legacy reduced input to
//!    the old statistics pass, kept ONLY for its out-of-crate producer (see
//!    below). New code must use (1) and `recursive_derive_stats`; this type
//!    is scheduled to die with the driver that feeds it.
//!
//! ## (3) is NOT a crate-local invention: it has an out-of-crate producer
//!
//! Retargeting (3) onto (1) was attempted and STOPPED, because
//! `cardinality::derive_stats::LogicalNode` is not exercised only by tests
//! that build it by hand. `tidb-executor`'s DP join-reorder driver
//! (`crates/tidb-executor/src/driver/join_reorder.rs`) imports
//! `derive_stats`, `LogicalNode`, `ProjectionExpr`, `JoinKind`, `ColumnId`
//! and `DISTINCT_FACTOR` from this module and names `LogicalNode` at 29
//! points, 14 of them CONSTRUCTING one out of its own `Rel`/`RowSource`
//! catalog model rather than out of a plan tree. It never
//! holds a [`logical::LogicalPlan`] and has no `expression::Expression`
//! values to build one from, so it cannot be pointed at (1) by a local edit.
//!
//! The `StatsInfo`/`ColumnId` unification is DONE: one profile type
//! ([`stats_info::StatsInfo`], keyed `i64`, now carrying group NDVs and
//! `scale`), one column-id spelling, and the driver reads through
//! accessors. What remains before (3) can be deleted is only the retarget
//! of `join_reorder.rs`'s MODEL — its 14 `LogicalNode` construction sites
//! build from a catalog, not from a plan tree — or moving the type down
//! into that driver. Until then, do not add new producers of (2) or (3):
//! project from (1), or derive on (1).
//!
//! # Closed enums, not `Box<dyn LogicalPlan>`
//!
//! Both trees are closed enums with inherent methods and `match` dispatch.
//!
//! * Go's rule signatures return a REPLACEMENT node —
//!   `PredicatePushDown(...) ([]expression.Expression, LogicalPlan, error)`,
//!   `PruneColumns(...) (LogicalPlan, error)`. An owned enum expresses that
//!   directly as `fn(self, ...) -> LogicalPlan`: the node is moved in and the
//!   replacement moved out, with no clone and no interior mutability. Trait
//!   objects need `Box<Self>` receivers for the same shape.
//! * `Clone`, `Hash64`/`Equals`, and structural comparison come free. Go gets
//!   them from `base.HashEquals` on concrete types; with `dyn` they fight
//!   object safety, and every one of them is load-bearing for the cascades
//!   memo.
//! * The precedent is in-tree and has held: `tidb-expr::Expression` made
//!   exactly this call for Go's `expression.Expression` interface.
//!
//! The price is that a new operator touches every `match`. That is the point —
//! an unhandled operator becomes a compile error instead of a silently wrong
//! plan. Children are owned rather than `Rc`, and the walks are all
//! stack-explicit; the measurements behind both decisions are recorded in the
//! [`logical`] module header.
//!
//! # What is NOT here
//!
//! No optimizer driver. Nothing in this crate runs a rule pass or picks a
//! plan; the live query path still plans inside `tidb-executor`'s driver.
//! [`plan::PlanNode`] remains as the explain-only metadata view it always
//! was, and is not a second plan representation — see its module header.

pub mod access_path;
pub mod aggregation_descriptor;
pub mod base_traits;
pub mod by_item;
pub mod candidate_cost;
pub mod cardinality;
pub mod cascades_base;
pub mod column_length;
pub mod column_pruning;
pub mod columnar_index_extra;
pub mod condition_binding;
pub mod condition_to_dual;
pub mod configured_join_plan;
pub mod configured_order_limit;
pub mod configured_order_limit_contract;
pub mod configured_relation_tree;
pub mod cost_factors;
pub mod cost_usage;
pub mod derive_topn_from_window;
pub mod eliminate_empty_selection;
pub mod eliminate_unionall_dual_item;
pub mod explain;
pub mod explore_mark;
pub mod expr_iterator;
pub mod expression_rewriter;
pub mod find_best_task;
pub mod fix_control;
pub mod group_expr;
pub mod handle_cols;
pub mod hash_equaler;
pub mod implementation_cost;
pub mod index_advisor_model;
pub mod index_columns;
pub mod index_task;
pub mod join_condition;
pub mod join_reorder_projection_inline;
pub mod logical;
pub mod logical_cte_table;
pub mod logical_data_source;
pub mod logical_data_source_task;
pub mod logical_limit;
pub mod logical_lock;
pub mod logical_max_one_row;
pub mod logical_mem_table;
pub mod logical_schema_producer;
pub mod logical_sequence;
pub mod logical_show;
pub mod logical_show_ddl_jobs;
pub mod logical_sort;
pub mod logical_table_dual;
pub mod logical_top_n;
pub mod logical_union_all;
pub mod max_min_elimination;
pub mod memo_group_id;
pub mod pattern;
pub mod pattern_engine;
pub mod physical;
pub mod physical_apply;
pub mod physical_cte_table;
pub mod physical_exchange_receiver;
pub mod physical_exchange_sender;
pub mod physical_index_scan;
pub mod physical_limit;
pub mod physical_lock;
pub mod physical_max_one_row;
pub mod physical_projection;
pub mod physical_property;
pub mod physical_selection;
pub mod physical_show;
pub mod physical_shuffle;
pub mod physical_sort;
pub mod physical_table_dual;
pub mod physical_table_reader;
pub mod physical_table_sample;
pub mod physical_table_scan;
pub mod physical_topn;
pub mod physical_union_all;
pub mod physical_union_scan;
pub mod physical_window;
pub mod plan;
pub mod plan_base;
pub mod plan_builder;
pub mod plan_cache_constants;
pub mod plan_context;
pub mod plan_cost_ver2;
pub mod predicate_partition;
pub mod prepared_dml;
pub mod projection_elimination;
pub mod push_down_sequence;
pub mod range_detacher;
pub mod read_only_scan;
pub mod transaction_control;
pub mod txn_mode;
pub use read_only_scan::configured_catalog;
pub mod residual_condition;
pub mod resolve_grouping_expand;
pub mod rule_set;
pub mod rule_type;
pub mod scheduler_contract;
pub mod schema_table_key;
pub mod selectivity_greedy;
pub mod signed_bigint_ranger;
pub mod stack_contract;
pub mod stats_info;
pub mod string_writer;
pub mod task_scheduler;
pub mod task_stack;
pub mod task_type;
pub mod telemetry;
pub mod tikv_scan_spec;
pub mod topn_push_down;
pub mod typed_condition;

/// Typed Rust counterpart of Go `planner/util.SliceRecursiveFlattenIter`.
///
/// Go discovers slice depth with reflection. Rust represents the same
/// arbitrary-depth tree explicitly, preserving lazy depth-first iteration,
/// global leaf indices, and early iterator termination without runtime type
/// inspection.
pub mod recursive_flatten {
    /// One leaf or nested slice in an arbitrary-dimensional input.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum RecursiveSlice<T> {
        /// One lowest-level element.
        Item(T),
        /// Another slice layer; an empty vector represents either a nil or an
        /// empty Go slice, which are observationally identical to the source
        /// iterator.
        Slice(Vec<Self>),
    }

    impl<T> RecursiveSlice<T> {
        /// Constructs a lowest-level value.
        pub fn item(value: T) -> Self {
            Self::Item(value)
        }

        /// Constructs one nested slice layer.
        #[must_use]
        pub fn slice(values: Vec<Self>) -> Self {
            Self::Slice(values)
        }
    }

    /// Lazy depth-first iterator over all leaves of recursive slices.
    pub struct RecursiveFlatten<'a, T> {
        stack: Vec<std::slice::Iter<'a, RecursiveSlice<T>>>,
        index: usize,
    }

    impl<'a, T> Iterator for RecursiveFlatten<'a, T> {
        type Item = (usize, &'a T);

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                let node = self.stack.last_mut()?.next();
                match node {
                    Some(RecursiveSlice::Item(value)) => {
                        let index = self.index;
                        self.index += 1;
                        return Some((index, value));
                    }
                    Some(RecursiveSlice::Slice(values)) => self.stack.push(values.iter()),
                    None => {
                        self.stack.pop();
                    }
                }
            }
        }
    }

    /// Go `SliceRecursiveFlattenIter`: recursively yields each leaf together
    /// with its zero-based position in the completely flattened sequence.
    pub fn slice_recursive_flatten_iter<T>(
        values: &[RecursiveSlice<T>],
    ) -> RecursiveFlatten<'_, T> {
        RecursiveFlatten {
            stack: vec![values.iter()],
            index: 0,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        type ThreeDimensional = Vec<Vec<Vec<&'static str>>>;

        fn recursive(input: ThreeDimensional) -> Vec<RecursiveSlice<String>> {
            input
                .into_iter()
                .map(|middle| {
                    RecursiveSlice::slice(
                        middle
                            .into_iter()
                            .map(|leaf| {
                                RecursiveSlice::slice(
                                    leaf.into_iter()
                                        .map(|value| RecursiveSlice::item(value.to_owned()))
                                        .collect(),
                                )
                            })
                            .collect(),
                    )
                })
                .collect()
        }

        struct Case {
            input: ThreeDimensional,
            continue_values: &'static [&'static str],
            break_value: Option<&'static str>,
            expected: &'static [(usize, &'static str)],
        }

        /// Source:
        /// `pkg/planner/util/slice_recursive_flatten_iter_test.go::TestSliceRecursiveFlattenIter`.
        #[test]
        fn test_slice_recursive_flatten_iter() {
            let cases = [
                Case {
                    input: vec![],
                    continue_values: &[],
                    break_value: None,
                    expected: &[],
                },
                Case {
                    input: vec![
                        vec![vec![], vec![]],
                        vec![],
                        vec![],
                        vec![vec![], vec![]],
                        vec![],
                        vec![],
                        vec![vec![]],
                        vec![vec![], vec![]],
                    ],
                    continue_values: &[],
                    break_value: None,
                    expected: &[],
                },
                Case {
                    input: vec![vec![vec!["111", "", "333"]]],
                    continue_values: &[],
                    break_value: None,
                    expected: &[(0, "111"), (1, ""), (2, "333")],
                },
                Case {
                    input: vec![
                        vec![],
                        vec![vec!["111", "", "333"], vec![], vec!["234"]],
                        vec![],
                    ],
                    continue_values: &[],
                    break_value: None,
                    expected: &[(0, "111"), (1, ""), (2, "333"), (3, "234")],
                },
                Case {
                    input: vec![vec![
                        vec!["111", "", "333"],
                        vec![],
                        vec!["444", "555", "666"],
                    ]],
                    continue_values: &["444"],
                    break_value: None,
                    expected: &[(0, "111"), (1, ""), (2, "333"), (4, "555"), (5, "666")],
                },
                Case {
                    input: vec![vec![
                        vec!["111", "", "333"],
                        vec![],
                        vec!["444", "555", "666"],
                    ]],
                    continue_values: &["111"],
                    break_value: Some("555"),
                    expected: &[(1, ""), (2, "333"), (3, "444")],
                },
                Case {
                    input: vec![
                        vec![
                            vec![],
                            vec!["", "", "998877"],
                            vec![],
                            vec![],
                            vec![],
                            vec![],
                            vec!["321"],
                        ],
                        vec![vec!["555", "222", "1"]],
                    ],
                    continue_values: &["321"],
                    break_value: Some("222"),
                    expected: &[(0, ""), (1, ""), (2, "998877"), (4, "555")],
                },
            ];

            for case in cases {
                let input = recursive(case.input);
                let mut output = Vec::new();
                for (index, value) in slice_recursive_flatten_iter(&input) {
                    if case.continue_values.contains(&value.as_str()) {
                        continue;
                    }
                    if case.break_value == Some(value.as_str()) {
                        break;
                    }
                    output.push((index, value.as_str()));
                }
                assert_eq!(output, case.expected);
            }
        }
    }
}

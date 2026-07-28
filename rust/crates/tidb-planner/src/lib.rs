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

//! Source-backed planner primitives.
//!
//! The first leaves port dependency-closed cardinality formulas and the
//! source-shaped physical-plan metadata hand-off without introducing an
//! optimizer facade or claiming a complete plan representation.

pub mod access_path;
pub mod aggregation_descriptor;
pub mod base_traits;
pub mod by_item;
pub mod cardinality;
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
pub mod derive_topn_from_window;
pub mod eliminate_empty_selection;
pub mod eliminate_unionall_dual_item;
pub mod explain;
pub mod explore_mark;
pub mod expr_iterator;
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
pub mod logical_aggregation;
pub mod logical_cte_table;
pub mod logical_data_source;
pub mod logical_data_source_task;
pub mod logical_expand;
pub mod logical_limit;
pub mod logical_lock;
pub mod logical_max_one_row;
pub mod logical_mem_table;
pub mod logical_projection;
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
pub mod plan_cache_constants;
pub mod plan_context;
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
pub mod scan_pushdown;
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
pub mod topn_push_down;
pub mod typed_condition;
pub mod window_frame;

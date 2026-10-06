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

//! All planner source-translation tests in one process.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod base_traits;
mod by_item;
mod cardinality;
mod column_length;
mod cost_factors;
mod cross_estimation;
mod fix_control;
mod hash_equaler;
mod index_columns;
mod index_range_policy;
mod join;
mod join_condition;
mod out_of_range;
mod physical_apply;
mod physical_cte_table;
mod physical_limit;
mod physical_lock;
mod physical_max_one_row;
mod physical_projection;
mod physical_property;
mod physical_selection;
mod physical_show;
mod physical_sort;
mod physical_table_dual;
mod physical_table_reader;
mod physical_topn;
mod physical_union_all;
mod plan;
mod projection_elimination;
mod range_detacher;
mod row_count_column;
mod row_size;
mod selectivity_greedy;
mod stats_info;
mod task_type;
mod uniform;

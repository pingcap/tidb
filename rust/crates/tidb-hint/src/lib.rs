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

//! Optimizer-hint interpretation and AST hint transfer, transcreated from
//! pinned Go package `pkg/util/hint`.

mod plan;
mod processor;
mod query_block;
mod statement;

pub use plan::{
    collect_unmatched_hint_warnings, extract_unmatched_tables, parse_plan_hints,
    remove_duplicated_hints, restore_index_hint, restore_join_hint, restore_storage_hint,
    HintedIndex, HintedIndexKind, HintedTable, IndexJoinHints, PlanHints, HINT_FLAG_NO_DECORRELATE,
    HINT_FLAG_SEMI_JOIN_REWRITE, PREFER_HASH_AGG, PREFER_MPP_1_PHASE_AGG, PREFER_MPP_2_PHASE_AGG,
    PREFER_STREAM_AGG,
};
pub use processor::{
    bind_hint, check_binding_from_history_complete, collect_hint, contain_table_hint_in_stmt_node,
    extract_table_hints_from_stmt_node, node_type_for_stmt, parse_hints_set,
    restore_index_hint as restore_ast_index_hint, restore_optimizer_hints,
    restore_table_optimizer_hint, HintsSet,
};
pub use query_block::{
    generate_qb_name, NodeType, QBHintBuildState, QBHintHandler, ViewHintContext,
};
pub use statement::{
    parse_stmt_hints, register_restricted_hint_checker, HintWarning, HypoIndexChecker,
    RestrictedHintChecker, SetVarHintChecker, StmtHints,
};

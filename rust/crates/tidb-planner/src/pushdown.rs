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

//! The physical scans' filter splits over Go's push-down admission
//! (`expression.CanExprsPushDown` / `PushDownExprs`, which are
//! [`tidb_expr::infer_pushdown`]'s): `physicalop.SplitSelCondsWithVirtualColumn`
//! and `extractFiltersForIndexMerge`.

use tidb_expr::expression::Expression;
use tidb_expr::infer_pushdown::{can_exprs_push_down, push_down_exprs};

/// Go SplitSelCondsWithVirtualColumn followed by PushDownExprs for scan filters.
/// The second list remains on the root task until reader conversion.
pub fn split_scan_filters(
    filters: Vec<Expression>,
    store: tidb_kv::store_type::StoreType,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> (Vec<Expression>, Vec<Expression>) {
    let (ordinary, mut root) = split_virtual_column_filters(filters);
    let (pushed, rejected) = push_down_exprs(ordinary, store, blacklist);
    root.extend(rejected);
    (pushed, root)
}

/// Go `extractFiltersForIndexMerge`: a filter TiKV can evaluate is pushed,
/// one only another engine can evaluate stays for the root task, and one no
/// engine can evaluate is dropped -- predicate push-down's `kv.UnSpecified`
/// split left it in the Selection above the DataSource, which evaluates it.
pub fn extract_filters_for_index_merge(
    filters: Vec<Expression>,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> (Vec<Expression>, Vec<Expression>) {
    use tidb_kv::store_type::StoreType;

    let mut pushed = Vec::new();
    let mut remaining = Vec::new();
    for expr in filters {
        if can_exprs_push_down(std::slice::from_ref(&expr), StoreType::TiKv, blacklist) {
            pushed.push(expr);
        } else if can_exprs_push_down(
            std::slice::from_ref(&expr),
            StoreType::Unspecified,
            blacklist,
        ) {
            remaining.push(expr);
        }
    }
    (pushed, remaining)
}

/// Go `BuildIndexMergeTableScan`'s filter split: `extractFiltersForIndexMerge`,
/// then `SplitSelCondsWithVirtualColumn` over the pushed filters, whose
/// virtual-column conditions join the root ones after the engine residue.
pub fn split_index_merge_table_filters(
    filters: Vec<Expression>,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> (Vec<Expression>, Vec<Expression>) {
    let (pushed, mut remaining) = extract_filters_for_index_merge(filters, blacklist);
    let (pushed, virtual_remaining) = split_virtual_column_filters(pushed);
    remaining.extend(virtual_remaining);
    (pushed, remaining)
}

/// Go SplitSelCondsWithVirtualColumn, kept separate for index/table phase order.
pub fn split_virtual_column_filters(
    filters: Vec<Expression>,
) -> (Vec<Expression>, Vec<Expression>) {
    filters.into_iter().partition(|condition| {
        !tidb_expr::simple_expr::extract_columns(condition)
            .iter()
            .any(|column| column.virtual_expr.is_some())
    })
}

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

//! Go `physicalop/tiflash_predicate_push_down.go`: TiFlash late
//! materialization, the filter conditions a TiFlash table scan evaluates
//! while it reads, before it reads the remaining columns.
//!
//! Go's `cardinality.Selectivity(sctx, ts.TblColHists, conds, nil)` is the
//! caller's `selectivity`; `None` is its error.

use std::collections::BTreeMap;

use tidb_expr::expression::Expression;
use tidb_planner_cardinality::derive_stats::StatsInfoScale as _;

use crate::PhysicalTableScan;

// selectivity = (row count after filter) / (row count before filter), smaller is better
// income = (1 - selectivity) * restColumnCount * tableRowCount, greater is better

const SELECTIVITY_THRESHOLD: f64 = 0.6;
// The default now of number of rows in a pack of TiFlash
const TIFLASH_DATA_PACK_SIZE: f64 = 8192.0;
const COLUMN_COUNT_THRESHOLD: usize = 3;

/// Go `expressionGroup`: a group of expressions and their selectivity.
struct ExpressionGroup {
    exprs: Vec<Expression>,
    selectivity: f64,
}

/// Go `compareExpressionGroups`.
fn compare_expression_groups(x: &ExpressionGroup, y: &ExpressionGroup) -> std::cmp::Ordering {
    x.exprs.len().cmp(&y.exprs.len()).then_with(|| {
        for (x, y) in x.exprs.iter().zip(&y.exprs) {
            let diff = y.clone().hash_code().cmp(x.clone().hash_code());
            if diff.is_ne() {
                return diff;
            }
        }
        std::cmp::Ordering::Equal
    })
}

/// Go `transformColumnsToCode`: the columns of an expression as a string of
/// "0" and "1" over the table's columns, keyed by column id.
fn transform_columns_to_code(cols: &[tidb_expr::column::Column], table_columns: usize) -> Vec<u8> {
    if cols.is_empty() {
        return b"0".to_vec();
    }
    let mut code = vec![0; table_columns];
    for col in cols {
        if let Ok(id) = usize::try_from(col.id) {
            if id < table_columns {
                code[id] = b'1';
            }
        }
    }
    code
}

/// Go `groupByColumnsSortBySelectivity`: the conditions grouped by the
/// columns they use, the groups whose selectivity passes the threshold sorted
/// by it.
fn group_by_columns_sort_by_selectivity(
    conds: &[Expression],
    ts: &PhysicalTableScan,
    selectivity: &dyn Fn(&[Expression]) -> Option<f64>,
) -> Vec<ExpressionGroup> {
    let table_columns = ts.table.as_ref().map_or(0, |table| table.read().columns.len());
    // Create a map to store the groupMap of conditions keyed by the columns
    let mut group_map: BTreeMap<Vec<u8>, Vec<Expression>> = BTreeMap::new();

    // Iterate through the conditions and group them by columns
    for cond in conds {
        // If excuting light cost condition first,
        // the rows needed to be exucuted by the heavy cost condition will be reduced.
        // So if the cond contains heavy cost functions, skip it to reduce the cost of calculating the selectivity.
        if with_heavy_cost_function_for_tiflash_prefetch(cond) {
            continue;
        }

        // If the number of columns is larger than columnCountThreshold,
        // the possibility of the selectivity of the condition is larger than the threshold is very small.
        // Skip.
        let columns = tidb_expr::simple_expr::extract_columns(cond);
        if columns.len() <= COLUMN_COUNT_THRESHOLD {
            let code = transform_columns_to_code(&columns, table_columns);
            group_map.entry(code).or_default().push(cond.clone());
        }
    }

    // Estimate the selectivity of each group and check if it is larger than the selectivityThreshold
    let mut expr_groups = Vec::new();
    for group in group_map.into_values() {
        let Some(selectivity) = selectivity(&group) else {
            // "calculate selectivity failed, do not push down the conditions group"
            continue;
        };
        if selectivity <= SELECTIVITY_THRESHOLD {
            expr_groups.push(ExpressionGroup { exprs: group, selectivity });
        }
    }

    // Keep the group order deterministic when selectivity and group size tie.
    expr_groups.sort_by(|x, y| {
        x.selectivity
            .partial_cmp(&y.selectivity)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| compare_expression_groups(x, y))
    });

    expr_groups
}

/// Go `withHeavyCostFunctionForTiFlashPrefetch`: whether the condition
/// contains functions that may cause a lot of memory allocation or disk IO.
fn with_heavy_cost_function_for_tiflash_prefetch(cond: &Expression) -> bool {
    let Expression::ScalarFunction(sf) = cond else {
        return false;
    };
    let args = sf.get_args();
    match sf.func_name.lowercase() {
        "and" | "or" => {
            with_heavy_cost_function_for_tiflash_prefetch(&args[0])
                && with_heavy_cost_function_for_tiflash_prefetch(&args[1])
        }
        "not" => with_heavy_cost_function_for_tiflash_prefetch(&args[0]),
        // JSON functions
        "json_array" | "json_array_append" | "json_array_insert" | "json_contains"
        | "json_contains_path" | "json_depth" | "json_extract" | "json_insert" | "json_keys"
        | "json_length" | "json_memberof" | "json_merge" | "json_merge_patch"
        | "json_merge_preserve" | "json_object" | "json_overlaps" | "json_pretty" | "json_quote"
        | "json_remove" | "json_replace" | "json_schema_valid" | "json_search" | "json_set"
        | "json_storage_free" | "json_storage_size" | "json_type" | "json_unquote"
        | "json_valid" => true,
        // some time functions
        "adddate" | "addtime" | "convert_tz" | "dateliteral" | "date_add" | "date_format"
        | "from_unixtime" | "get_format" | "utc_timestamp" => true,
        "date_sub" | "datediff" | "dayofyear" | "extract" | "from_days" | "timestampliteral"
        | "timestampadd" | "unix_timestamp" => true,
        "localtimestamp" | "makedate" | "maketime" | "monthname" | "period_add"
        | "period_diff" | "quarter" | "sec_to_time" | "to_seconds" => true,
        "str_to_date" | "subdate" | "subtime" | "timeliteral" | "time_format" | "time_to_sec"
        | "timediff" | "timestampdiff" => true,
        // regexp functions
        "regexp" | "regexp_like" | "regexp_replace" | "regexp_substr" | "regexp_instr" => true,
        // TODO: add more heavy cost functions
        _ => false,
    }
}

/// Go `predicatePushDownToTableScan`: pushes down some filter conditions to
/// the table scan.
fn predicate_push_down_to_table_scan(
    conds: &[Expression],
    ts: &mut PhysicalTableScan,
    selectivity: &dyn Fn(&[Expression]) -> Option<f64>,
) {
    if ts.has_full_text_index_push_down() {
        return;
    }
    // group the conditions by columns and sort them by selectivity
    let sorted_conds = group_by_columns_sort_by_selectivity(conds, ts, selectivity);

    let mut selected_conds: Vec<Expression> = Vec::with_capacity(conds.len());
    let mut selected_income = 0.0;
    let mut selected_column_count = 0;
    let mut selected_selectivity = 1.0;
    let total_column_count = ts.columns.len() as f64;
    let table_row_count = ts.base.base.stats_info().map_or(0.0, |stats| stats.row_count());

    for expr_group in sorted_conds {
        let mut merged_conds = selected_conds.clone();
        merged_conds.extend(expr_group.exprs);
        let Some(selectivity) = selectivity(&merged_conds) else {
            // "calculate selectivity failed, do not push down the conditions group"
            continue;
        };
        let col_cnt = tidb_expr::expr_util::extract::extract_column_set(&merged_conds).len();
        let income = (1.0 - selectivity) * table_row_count;
        // If selectedColumnCount does not change,
        // or the increase of the number of filtered rows is greater than tiflashDataPackSize and the income increases, push down the conditions.
        if col_cnt == selected_column_count
            || (income > TIFLASH_DATA_PACK_SIZE
                && income * (total_column_count - col_cnt as f64) > selected_income)
        {
            selected_conds = merged_conds;
            selected_column_count = col_cnt;
            selected_income = income * (total_column_count - col_cnt as f64);
            selected_selectivity = selectivity;
        } else if income < TIFLASH_DATA_PACK_SIZE {
            // If the increase of the number of filtered rows is less than tiflashDataPackSize,
            // break the loop to reduce the cost of calculating selectivity.
            break;
        }
    }

    if selected_conds.is_empty() {
        return;
    }
    // add the pushed down conditions to table scan
    ts.late_materialization_filter_condition = selected_conds;
    ts.late_materialization_selectivity = selected_selectivity;
}

/// Go `isPredicateSimpleCompare`: a comparison predicate built only of
/// `>`, `>=`, `=`, `<=`, `<`, `in`, `and`, `or` and `not` (Go's comment also
/// names `!=`, which its switch does not admit).
fn is_predicate_simple_compare(cond: &Expression) -> bool {
    let Expression::ScalarFunction(sf) = cond else {
        return false;
    };
    match sf.func_name.lowercase() {
        "eq" | "ge" | "le" | "lt" | "gt" | "in" => true,
        "and" | "or" => is_predicate_simple_compare(&sf.args[0]) && is_predicate_simple_compare(&sf.args[1]),
        "not" => is_predicate_simple_compare(&sf.args[0]),
        _ => false,
    }
}

/// Go `handleTiFlashPredicatePushDown`: whether to use the table's inverted
/// indexes, whether to push down the filter conditions to the table scan,
/// and the scan's row count after them. `realtime_count` is Go's
/// `ts.TblColHists.RealtimeCount`, `index_hints` the data source's.
pub fn handle_tiflash_predicate_push_down(
    ts: &mut PhysicalTableScan,
    realtime_count: i64,
    index_hints: &[tidb_ast::IndexHint],
    enable_late_materialization: bool,
    selectivity: &dyn Fn(&[Expression]) -> Option<f64>,
    skew_ratio: f64,
) {
    // When the table is small, there is no need to push down the conditions.
    if realtime_count <= TIFLASH_DATA_PACK_SIZE as i64 || ts.keep_order || ts.filter_condition.is_empty() {
        return;
    }

    // Currently, TiFlash does not support vector index with predicates.
    assert!(
        !ts.used_columnar_indexes.iter().any(|index| index.index_info.read().vector_info.is_some()),
        "TiFlash does not support vector index with pedicates"
    );

    // Consider use index hints.
    let mut index_map: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for hint in index_hints {
        if hint.scope != tidb_ast::IndexHintScope::All {
            continue;
        }
        match hint.kind {
            tidb_ast::IndexHintKind::Use => {
                for name in &hint.indexes {
                    let count = index_map.entry(tidb_hack::go_to_lower(name)).or_insert(0);
                    if *count != i64::MAX {
                        *count += 1;
                    }
                }
            }
            tidb_ast::IndexHintKind::Ignore => {
                for name in &hint.indexes {
                    let count = index_map.entry(tidb_hack::go_to_lower(name)).or_insert(0);
                    if *count != i64::MAX {
                        *count -= 1;
                    }
                }
            }
            tidb_ast::IndexHintKind::Force => {
                for name in &hint.indexes {
                    index_map.insert(tidb_hack::go_to_lower(name), i64::MAX);
                }
            }
        }
    }
    let hinted = |name: &str| index_map.get(name).copied().unwrap_or(0);

    // Go's map from the indexed column's name to its inverted index: a later
    // index on the same column replaces an earlier one.
    let mut indexed_column_name_to_index_info: Vec<(String, tidb_model::GoShared<tidb_model::IndexInfo>)> = Vec::new();
    if let Some(table) = &ts.table {
        for index in table.read().indices.iter_deref() {
            let info = index.read();
            if info.is_public() && info.inverted_info.is_some() {
                if !index_hints.is_empty() && hinted(info.name.lowercase()) <= 0 {
                    continue;
                }
                // inverted index only support one column
                let column = info.columns.iter_deref().next().map(|column| column.read().name.lowercase().to_owned()).expect("an inverted index has one column");
                match indexed_column_name_to_index_info.iter_mut().find(|(name, _)| *name == column) {
                    Some(entry) => entry.1 = index.clone(),
                    None => indexed_column_name_to_index_info.push((column, index.clone())),
                }
            }
        }
    }

    let mut selected_columns: Vec<String> = Vec::new();
    let mut selected_conditions: Vec<Expression> = Vec::new();
    // Go reuses this slice and resets it only after a selected condition, so
    // a skipped condition's names carry into the next one's check.
    let mut column_names: Vec<String> = Vec::new();
    for cond in &ts.filter_condition {
        // 1. The predicate only contains >, >=, =, !=, <=, <, in.
        if !is_predicate_simple_compare(cond) {
            continue;
        }
        // 2. The columns of predicate all have inverted index.
        for col in tidb_expr::simple_expr::extract_columns(cond) {
            let name = col.orig_name.rsplit('.').next().unwrap_or_default();
            column_names.push(tidb_hack::go_to_lower(name));
        }
        let all_have_inverted_index = column_names
            .iter()
            .all(|name| indexed_column_name_to_index_info.iter().any(|(column, _)| column == name));
        if !all_have_inverted_index {
            continue;
        }
        // 3. The selectivity of the predicate is less than 60%.
        let Some(cond_selectivity) = selectivity(std::slice::from_ref(cond)) else {
            continue;
        };
        if cond_selectivity > SELECTIVITY_THRESHOLD {
            continue;
        }
        // all passed, add the columns to selected
        for name in column_names.drain(..) {
            if !selected_columns.contains(&name) {
                selected_columns.push(name);
            }
        }
        selected_conditions.push(cond.clone());
    }

    for (column, index) in &indexed_column_name_to_index_info {
        if selected_columns.contains(column) {
            ts.used_columnar_indexes.push(crate::build_inverted_index_extra(index));
        } else if hinted(index.read().name.lowercase()) == i64::MAX {
            // if the index is not used, but the index hint is force use, add it to the used indexes
            ts.used_columnar_indexes.push(crate::build_inverted_index_extra(index));
        }
    }

    // if EnableLateMaterialization is set, try to push down some predicates to table scan
    if ts.filter_condition.len() > selected_conditions.len() && enable_late_materialization {
        let remaining: Vec<Expression> = ts
            .filter_condition
            .iter()
            .filter(|cond| !tidb_expr::expr_util::predicates::contains(&selected_conditions, cond))
            .cloned()
            .collect();
        predicate_push_down_to_table_scan(&remaining, ts, selectivity);
    }

    // Update the row count of table scan.
    if !selected_conditions.is_empty() || !ts.late_materialization_filter_condition.is_empty() {
        selected_conditions.extend(ts.late_materialization_filter_condition.iter().cloned());
        let selectivity = selectivity(&selected_conditions).unwrap_or(SELECTIVITY_THRESHOLD);
        let stats = ts.base.base.stats_info().map(|stats| stats.scale(selectivity, skew_ratio));
        ts.base.base.set_stats(stats);
        // just to make explain result stable
        ts.used_columnar_indexes.sort_by_key(|index| index.index_info.read().id);
    }
}

/// Go `PhysicalTableScan.BuildPushedDownSelection`'s TiFlash arm: the
/// filter conditions late materialization did not take, for the Selection
/// above the scan.
#[must_use]
pub fn conditions_after_late_materialization(ts: &PhysicalTableScan) -> Vec<Expression> {
    ts.filter_condition
        .iter()
        .filter(|cond| !tidb_expr::expr_util::predicates::contains(&ts.late_materialization_filter_condition, cond))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(id: i64) -> tidb_expr::column::Column {
        let mut column = tidb_expr::column::Column::default();
        column.id = id;
        column.unique_id = id + 100;
        column
    }

    #[test]
    fn transform_columns_to_code_marks_each_column_id() {
        // Go's example: columns [a, c, e, g] of eight.
        let cols = [column(0), column(2), column(4), column(6)];
        assert_eq!(transform_columns_to_code(&cols, 8), b"1\x001\x001\x001\x00");
        assert_eq!(transform_columns_to_code(&[], 8), b"0");
    }
}

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

//! Go `preparePossibleProperties`: the stack-explicit post-order driver that
//! fills each logical operator's order inventory before physical enumeration.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;

use crate::access_path::PossiblePath;
use crate::plan_base::PossiblePropertiesInfo;

use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::LogicalPlan;

fn equality_fixed_columns(expressions: &[Expression]) -> Vec<i64> {
    expressions
        .iter()
        .filter_map(|expression| {
            let Expression::ScalarFunction(function) = expression else {
                return None;
            };
            if function.func_name.lowercase() != "eq" || function.get_args().len() != 2 {
                return None;
            }
            match function.get_args() {
                [Expression::Column(column), Expression::Constant(_)]
                | [Expression::Constant(_), Expression::Column(column)] => Some(column.unique_id),
                _ => None,
            }
        })
        .collect()
}

fn data_source_orders(source: &super::DataSource, inherited_fixed: &[i64]) -> Vec<Vec<Column>> {
    if source.base.base.schema().is_none() {
        return Vec::new();
    }
    let mut fixed = equality_fixed_columns(&source.pushed_down_conds);
    fixed.extend_from_slice(inherited_fixed);
    fixed.sort_unstable();
    fixed.dedup();
    let mut orders = Vec::new();
    let index_orders = |index_offset: usize| {
        let Some(index) = source.indexes.get(index_offset) else {
            return Vec::new();
        };
        let order: Vec<Column> = index
            .columns
            .iter()
            .take_while(|column| column.length < 0)
            .map_while(|column| source.schema_column_for_index_column(column).cloned())
            .collect();
        if order.is_empty() {
            return Vec::new();
        }
        let mut index_orders = vec![order.clone()];
        for fixed_prefix in 1..order.len() {
            if !order[..fixed_prefix]
                .iter()
                .all(|column| fixed.contains(&column.unique_id))
            {
                break;
            }
            index_orders.push(order[fixed_prefix..].to_vec());
        }
        index_orders
    };
    for path in &source.enumerated_paths {
        match *path {
            PossiblePath::Table {
                is_int_handle: true,
                ..
            } => {
                if let Some(column) = source.handle_cols.first() {
                    orders.push(vec![column.clone()]);
                }
            }
            PossiblePath::Table {
                primary_index: Some(index),
                ..
            }
            | PossiblePath::Index { index } => orders.extend(index_orders(index)),
            PossiblePath::Table { .. } => {}
        }
    }
    orders
}

struct Prepare;

impl OwnedRewrite for Prepare {
    type Down = Vec<i64>;
    type Up = PossiblePropertiesInfo;

    fn descend(
        &mut self,
        node: &mut LogicalPlan,
        mut fixed: Self::Down,
    ) -> Descend<Self::Down, Self::Up> {
        if let LogicalPlan::DataSource(source) = node {
            let orders = data_source_orders(source, &fixed);
            return Descend::Stop(source.prepare_possible_properties(orders, false, false));
        }
        if let LogicalPlan::Selection(selection) = node {
            fixed.extend(equality_fixed_columns(&selection.conditions));
            fixed.sort_unstable();
            fixed.dedup();
        } else if let LogicalPlan::Projection(projection) = node {
            let output_schema = projection.base.base.schema();
            fixed = fixed
                .into_iter()
                .filter_map(|fixed_id| {
                    let position = output_schema?
                        .columns
                        .iter()
                        .position(|column| column.unique_id == fixed_id)?;
                    match projection.exprs.get(position)? {
                        Expression::Column(input) => Some(input.unique_id),
                        _ => None,
                    }
                })
                .collect();
        } else if matches!(node, LogicalPlan::Aggregation(_)) {
            // A predicate above an aggregation addresses aggregate outputs;
            // it cannot fix a column in the input namespace.
            fixed.clear();
        }
        Descend::Children(vec![fixed; node.children().len()])
    }

    fn ascend(
        &mut self,
        mut node: LogicalPlan,
        children: Vec<Self::Up>,
    ) -> (LogicalPlan, Self::Up) {
        let first = children.first();
        let info = match &mut node {
            LogicalPlan::Selection(operator) => operator.prepare_possible_properties(first),
            LogicalPlan::Projection(operator) => operator.prepare_possible_properties(first),
            LogicalPlan::Aggregation(operator) => operator.prepare_possible_properties(first),
            LogicalPlan::Join(operator) => match (children.first(), children.get(1)) {
                (Some(left), Some(right)) => operator.prepare_possible_properties(left, right),
                _ => PossiblePropertiesInfo::default(),
            },
            LogicalPlan::DataSource(operator) => {
                let orders = data_source_orders(operator, &[]);
                operator.prepare_possible_properties(orders, false, false)
            }
            LogicalPlan::Sort(operator) => operator.prepare_possible_properties(first),
            LogicalPlan::TopN(operator) => operator.prepare_possible_properties(first),
            LogicalPlan::UnionAll(operator) => operator.prepare_possible_properties(
                &children.iter().cloned().map(Some).collect::<Vec<_>>(),
            ),
            LogicalPlan::PartitionUnionAll(operator) => {
                operator.union_all.prepare_possible_properties(
                    &children.iter().cloned().map(Some).collect::<Vec<_>>(),
                )
            }
            LogicalPlan::Window(operator) => operator.prepare_possible_properties(first),
            LogicalPlan::UnionScan(operator) => operator.prepare_possible_properties(first),
            LogicalPlan::TiKVSingleGather(operator) => operator.prepare_possible_properties(first),
            LogicalPlan::Sequence(operator) => operator.prepare_possible_properties(
                &children.iter().cloned().map(Some).collect::<Vec<_>>(),
            ),
            _ => {
                let schema = node.schema().cloned().unwrap_or_default();
                node.prepare_possible_properties(
                    &schema,
                    &children.into_iter().map(Some).collect::<Vec<_>>(),
                )
            }
        };
        (node, info)
    }
}

/// Runs Go's `preparePossibleProperties` over `plan`, returning the updated
/// tree and its root property inventory.
#[must_use]
pub fn prepare_possible_properties(plan: LogicalPlan) -> (LogicalPlan, PossiblePropertiesInfo) {
    fold_owned(&mut Prepare, plan, Vec::new())
}

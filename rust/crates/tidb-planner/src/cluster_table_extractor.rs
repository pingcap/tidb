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

//! ClusterTableExtractor's TYPE/INSTANCE constraints from resolved scan predicates.

use crate::physical::PhysicalPlan;
use std::collections::BTreeSet;
use tidb_expr::expression::Expression;

/// One scan's conjunctive routing constraints. None means unrestricted;
/// an empty set means a contradiction and therefore no request.
#[derive(Clone, Debug, Default)]
pub struct ClusterTableFilter {
    node_types: Option<BTreeSet<String>>,
    instances: Option<BTreeSet<String>>,
}

impl ClusterTableFilter {
    /// Whether this scan provably needs no nodes.
    pub fn skip_request(&self) -> bool {
        self.node_types.as_ref().is_some_and(BTreeSet::is_empty)
            || self.instances.as_ref().is_some_and(BTreeSet::is_empty)
    }

    /// Go FilterClusterServerInfo, before issuing any HTTP request.
    pub fn matches(&self, node_type: &str, instance: &str) -> bool {
        self.node_types
            .as_ref()
            .is_none_or(|set| set.contains(node_type))
            && self
                .instances
                .as_ref()
                .is_none_or(|set| set.contains(instance))
    }
}

/// Returns every scan's routing needs. Shared materialization must satisfy their
/// union, including self joins. Predicates stay in the ordinary SQL executor.
/// No constraint is moved across LIMIT, aggregation or other operator barriers.
pub fn cluster_table_filters(plan: &PhysicalPlan, table: &str) -> Vec<ClusterTableFilter> {
    fn visit(
        plan: &PhysicalPlan,
        table: &str,
        conditions: Vec<Expression>,
        out: &mut Vec<ClusterTableFilter>,
    ) {
        if let PhysicalPlan::Selection(selection) = plan {
            let mut conditions = conditions;
            conditions.extend(selection.conditions.iter().cloned());
            for child in plan.children() {
                visit(child, table, conditions.clone(), out);
            }
            return;
        }
        if let PhysicalPlan::MemTable(scan) = plan {
            if scan.db_name.eq_ignore_ascii_case("information_schema")
                && scan.table_name.eq_ignore_ascii_case(table)
            {
                let mut filter = ClusterTableFilter::default();
                if let Some(schema) = plan.schema() {
                    for (column, name) in schema.columns.iter().zip(&scan.columns) {
                        let (target, lower) = if name.name.eq_ignore_ascii_case("type") {
                            (&mut filter.node_types, true)
                        } else if name.name.eq_ignore_ascii_case("instance") {
                            (&mut filter.instances, false)
                        } else {
                            continue;
                        };
                        for condition in &conditions {
                            if let Some(values) = values_for(condition, column.unique_id, lower) {
                                *target = Some(match target.take() {
                                    Some(previous) => {
                                        previous.intersection(&values).cloned().collect()
                                    }
                                    None => values,
                                });
                            }
                        }
                    }
                }
                out.push(filter);
            }
            return;
        }
        if let PhysicalPlan::CTE(cte) = plan {
            visit(&cte.seed_plan, table, Vec::new(), out);
            if let Some(recursive) = &cte.recursive_plan {
                visit(recursive, table, Vec::new(), out);
            }
        }
        for child in plan.children() {
            visit(child, table, Vec::new(), out);
        }
    }
    let mut filters = Vec::new();
    visit(plan, table, Vec::new(), &mut filters);
    filters
}

fn values_for(expression: &Expression, id: i64, lower: bool) -> Option<BTreeSet<String>> {
    fn constant(expression: &Expression, lower: bool) -> Option<String> {
        let Expression::Constant(value) = expression else {
            return None;
        };
        if value.deferred_expr.is_some() || value.param_marker.is_some() {
            return None;
        }
        let value = String::from_utf8(value.value.to_bytes().ok()?).ok()?;
        Some(if lower {
            tidb_mysql::to_lowercase(&value)
        } else {
            value
        })
    }
    let is_column = |e: &Expression| matches!(e, Expression::Column(c) if c.unique_id == id);
    let Expression::ScalarFunction(function) = expression else {
        return None;
    };
    match (function.func_name.lowercase(), function.args.as_slice()) {
        ("eq", [a, b]) if is_column(a) => Some([constant(b, lower)?].into()),
        ("eq", [a, b]) if is_column(b) => Some([constant(a, lower)?].into()),
        ("in", [column, values @ ..]) if is_column(column) => {
            values.iter().map(|value| constant(value, lower)).collect()
        }
        ("or", [a, b]) => {
            let mut a = values_for(a, id, lower)?;
            a.extend(values_for(b, id, lower)?);
            Some(a)
        }
        ("and", [a, b]) => match (values_for(a, id, lower), values_for(b, id, lower)) {
            (Some(a), Some(b)) => Some(a.intersection(&b).cloned().collect()),
            (a, b) => a.or(b),
        },
        _ => None,
    }
}

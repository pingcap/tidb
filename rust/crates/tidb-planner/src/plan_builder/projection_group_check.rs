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

//! Go buildProjection's new ONLY_FULL_GROUP_BY validation.

use tidb_expr::expression::Expression;
use tidb_expr::simple_expr::extract_columns;
use tidb_funcdep::ColSet;

use super::ProjectionField;
use crate::logical::{LogicalPlan, LogicalProjection};
use crate::plan_base::PlanError;

fn contains_any_value(expr: &Expression) -> bool {
    match expr {
        Expression::ScalarFunction(function) => {
            function.func_name.lowercase() == "any_value"
                || function.args.iter().any(contains_any_value)
        }
        _ => false,
    }
}

pub(super) fn check(
    projection: &mut LogicalProjection,
    fields: &[ProjectionField],
    order_by_range: Option<(usize, usize)>,
) -> Result<(), PlanError> {
    let plan = LogicalPlan::Projection(projection.clone());
    let fds = plan.extract_fd();
    if !fds.has_agg_built {
        return Ok(());
    }
    let ungrouped = fds.group_by_cols.equals(&ColSet::new([0]));
    let closure = fds.closure_of_strict(&fds.group_by_cols);
    for (offset, expr) in projection.exprs.iter().take(fields.len()).enumerate() {
        if ungrouped && order_by_range.is_some_and(|(from, to)| (from..to).contains(&offset)) {
            continue;
        }
        let item = match expr {
            Expression::Column(column) => ColSet::new([column.unique_id]),
            Expression::ScalarFunction(_) => {
                if contains_any_value(expr) {
                    continue;
                }
                let mut scratch = expr.clone();
                let Some(id) = fds.registered_unique_id(scratch.hash_code()) else {
                    // Go skips expressions absent from its registration map.
                    continue;
                };
                ColSet::new([id])
            }
            _ => continue,
        };
        if item.subset_of(&fds.constant_cols())
            || item.subset_of(&fds.group_by_cols)
            || item.subset_of(&closure)
        {
            continue;
        }
        let columns = extract_columns(expr);
        let Some(column) = columns
            .iter()
            .find(|column| !ColSet::new([column.unique_id]).subset_of(&closure))
            .or_else(|| columns.first())
        else {
            continue;
        };
        let name = plan
            .schema()
            .and_then(|schema| {
                schema
                    .columns
                    .iter()
                    .position(|c| c.unique_id == column.unique_id)
            })
            .and_then(|index| plan.output_names().get(index))
            .map(|name| name.display_name())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| column.orig_name.clone());
        return Err(if ungrouped {
            PlanError::field_not_in_aggregated_query(offset + 1, name)
        } else {
            PlanError::field_not_in_group_by(offset + 1, "SELECT list", name)
        });
    }
    projection.fd_group_check_complete = true;
    Ok(())
}

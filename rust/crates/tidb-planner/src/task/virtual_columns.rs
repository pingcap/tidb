// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::physical::PhysicalPlan;
use crate::plan_base::PlanError;

/// Go ExpandVirtualColumn at the cop-to-root boundary. Pass-through nodes
/// retain their child's expanded schema; projections and aggregates retain
/// their own output contract.
pub(super) fn expand(plan: &mut PhysicalPlan) -> Result<(), PlanError> {
    for child in plan.base_mut().children_mut() {
        expand(child)?;
    }
    if let PhysicalPlan::TableScan(scan) = plan {
        let Some(mut schema) = scan.base.base.schema().cloned() else {
            return Ok(());
        };
        let original_len = schema.len();
        let dependencies = schema
            .columns
            .iter()
            .filter_map(|column| column.virtual_expr.as_deref())
            .flat_map(tidb_expr::expr_util::extract_dependent_columns)
            .collect::<Vec<_>>();
        // Go keeps the synthetic handle/physical-table columns at the end.
        let mut insert_at = schema
            .columns
            .iter()
            .rposition(|column| {
                column.id != tidb_model::column::EXTRA_HANDLE_ID
                    && column.id != tidb_model::column::EXTRA_PHYS_TBL_ID
            })
            .map_or(0, |position| position + 1);
        for column in dependencies {
            if schema
                .columns
                .iter()
                .any(|existing| existing.unique_id == column.unique_id)
            {
                continue;
            }
            schema.columns.insert(insert_at, column);
            insert_at += 1;
        }
        if schema.len() == original_len {
            return Ok(());
        }
        if let Some(spec) = &mut scan.tikv_pushdown {
            let columns = schema
                .columns
                .iter()
                .map(|column| {
                    spec.columns
                        .iter()
                        .find(|info| info.column_id == column.id)
                        .cloned()
                        .ok_or_else(|| {
                            PlanError::internal(
                                "virtual column expansion requires resolved dependency metadata",
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            spec.columns = columns;
        }
        scan.base.base.set_schema(Some(schema));
    } else if matches!(
        plan,
        PhysicalPlan::Selection(_)
            | PhysicalPlan::Limit(_)
            | PhysicalPlan::TopN(_)
            | PhysicalPlan::Sort(_)
    ) {
        if let [child] = plan.children() {
            let schema = child.schema().cloned();
            plan.base_mut().base.set_schema(schema);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::{column::Column, expression::Expression, schema::Schema};

    fn column(id: i64) -> Column {
        let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        column.id = id;
        column
    }

    #[test]
    fn nested_dependencies_are_unique_and_precede_extra_handles() {
        let base = column(1);
        let mut intermediate = column(2);
        intermediate.virtual_expr = Some(Box::new(Expression::Column(base)));
        let mut generated = column(3);
        generated.virtual_expr = Some(Box::new(Expression::Column(intermediate.clone())));
        let mut scan = crate::physical::PhysicalTableScan::default();
        scan.base.base.set_schema(Some(Schema::new(vec![
            intermediate,
            generated,
            column(tidb_model::column::EXTRA_HANDLE_ID),
            column(tidb_model::column::EXTRA_PHYS_TBL_ID),
        ])));
        let mut plan = PhysicalPlan::TableScan(scan);
        for _ in 0..2 {
            expand(&mut plan).unwrap();
            assert_eq!(
                plan.schema()
                    .unwrap()
                    .columns
                    .iter()
                    .map(|c| c.id)
                    .collect::<Vec<_>>(),
                vec![2, 3, 1, -1, -3]
            );
        }
    }
}

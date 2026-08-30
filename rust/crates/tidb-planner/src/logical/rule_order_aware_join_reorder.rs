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

//! Pinned Go `pkg/planner/core/rule/rule_order_aware_join_reorder.go`.

use std::collections::BTreeSet;

use tidb_expr::aggregation::ByItems;
use tidb_expr::column::Column;
use tidb_expr::expr_util::predicates::is_mutable_effects_expr;
use tidb_expr::expression::Expression;

use super::rule::{LogicalOptRule, RuleContext};
use super::{LogicalPlan, PlanError};

/// Go `OrderAwareJoinReorder`.
pub struct OrderAwareJoinReorder;

impl LogicalOptRule for OrderAwareJoinReorder {
    fn optimize(
        &self,
        context: &RuleContext<'_>,
        mut plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        match optimize_recursive(context, &mut plan, &[], &[]) {
            Ok((changed, _)) => Ok((plan, changed)),
            Err(error) => Err((plan, error)),
        }
    }

    fn name(&self) -> &'static str {
        "order_aware_join_reorder"
    }
}

fn optimize_recursive(
    context: &RuleContext<'_>,
    plan: &mut LogicalPlan,
    order_columns: &[Column],
    middle_filters: &[Expression],
) -> Result<(bool, bool), PlanError> {
    if matches!(plan, LogicalPlan::CTE(_)) {
        return Ok((false, false));
    }
    let middle_filters = if order_columns.is_empty() {
        &[]
    } else {
        middle_filters
    };

    match plan {
        LogicalPlan::TopN(top_n) => {
            let extracted = extract_ordering_columns(&top_n.by_items);
            let (changed, ordered) =
                optimize_children(context, top_n.base.children_mut(), &extracted, &[])?;
            if order_columns.is_empty() {
                Ok((changed, ordered))
            } else {
                Ok((changed, same_ordering_columns(order_columns, &extracted)))
            }
        }
        LogicalPlan::Sort(sort) => {
            let extracted = extract_ordering_columns(&sort.by_items);
            let (changed, ordered) =
                optimize_children(context, sort.base.children_mut(), &extracted, &[])?;
            if order_columns.is_empty() {
                Ok((changed, ordered))
            } else {
                Ok((changed, same_ordering_columns(order_columns, &extracted)))
            }
        }
        LogicalPlan::Projection(projection) => {
            let rewritten = rewrite_ordering_for_projection(projection, order_columns);
            let (changed, ordered) = optimize_children(
                context,
                projection.base.children_mut(),
                &rewritten,
                middle_filters,
            )?;
            if order_columns.is_empty() {
                Ok((changed, ordered))
            } else {
                Ok((changed, !rewritten.is_empty() && ordered))
            }
        }
        LogicalPlan::Limit(limit) => optimize_children(
            context,
            limit.base.children_mut(),
            order_columns,
            middle_filters,
        ),
        LogicalPlan::Selection(selection) => {
            if context.join_reorder_through_sel
                && !selection.conditions.iter().any(is_mutable_effects_expr)
            {
                let filters = if order_columns.is_empty() {
                    Vec::new()
                } else {
                    middle_filters
                        .iter()
                        .chain(&selection.conditions)
                        .cloned()
                        .collect()
                };
                if !order_columns.is_empty() && should_use_cdc_based_join_reorder(context) {
                    if let Some(choice) = crate::joinorder::find_ordered_leading_choice(
                        context,
                        &LogicalPlan::Selection(selection.clone()),
                        order_columns,
                    ) {
                        let (child_changed, child_ordered) = optimize_choice_vertices(
                            context,
                            selection.base.children_mut(),
                            &choice,
                            order_columns,
                            &filters,
                        )?;
                        if child_ordered
                            && crate::joinorder::try_annotate_ordered_leading_on_selection(
                                context, selection, &choice,
                            )
                        {
                            return Ok((true, true));
                        }
                        return Ok((child_changed, false));
                    }
                }
                optimize_children(
                    context,
                    selection.base.children_mut(),
                    order_columns,
                    &filters,
                )
            } else {
                let (changed, _) =
                    optimize_children(context, selection.base.children_mut(), &[], &[])?;
                Ok((changed, false))
            }
        }
        LogicalPlan::Join(join) => {
            if !order_columns.is_empty() && should_use_cdc_based_join_reorder(context) {
                if let Some(choice) = crate::joinorder::find_ordered_leading_choice(
                    context,
                    &LogicalPlan::Join(join.clone()),
                    order_columns,
                ) {
                    let (child_changed, child_ordered) = optimize_choice_vertices(
                        context,
                        join.base.children_mut(),
                        &choice,
                        order_columns,
                        middle_filters,
                    )?;
                    if child_ordered
                        && crate::joinorder::try_annotate_ordered_leading_on_join(
                            context, join, &choice,
                        )
                    {
                        return Ok((true, true));
                    }
                    return Ok((child_changed, false));
                }
            }
            let (changed, _) = optimize_children(context, join.base.children_mut(), &[], &[])?;
            Ok((changed, false))
        }
        LogicalPlan::DataSource(data_source) => Ok((
            false,
            !order_columns.is_empty()
                && crate::joinorder::data_source_satisfies_ordering(
                    data_source,
                    order_columns,
                    middle_filters,
                ),
        )),
        _ => {
            let (changed, _) =
                optimize_children(context, plan.base_mut().children_mut(), &[], &[])?;
            Ok((changed, false))
        }
    }
}

fn optimize_children(
    context: &RuleContext<'_>,
    children: &mut [LogicalPlan],
    order_columns: &[Column],
    parent_filters: &[Expression],
) -> Result<(bool, bool), PlanError> {
    let mut changed = false;
    let mut ordered = false;
    for child in children {
        let (child_changed, child_ordered) =
            optimize_recursive(context, child, order_columns, parent_filters)?;
        changed |= child_changed;
        ordered |= child_ordered;
    }
    Ok((changed, ordered))
}

fn optimize_choice_vertices(
    context: &RuleContext<'_>,
    children: &mut [LogicalPlan],
    choice: &crate::joinorder::OrderedLeadingChoice,
    order_columns: &[Column],
    parent_filters: &[Expression],
) -> Result<(bool, bool), PlanError> {
    let vertex_ids: BTreeSet<i32> = choice.vertices.iter().map(|vertex| vertex.id()).collect();
    let carrier_id = choice.carrier_vertex.id();
    fn visit(
        context: &RuleContext<'_>,
        plan: &mut LogicalPlan,
        vertex_ids: &BTreeSet<i32>,
        carrier_id: i32,
        order_columns: &[Column],
        parent_filters: &[Expression],
        changed: &mut bool,
        ordered: &mut bool,
    ) -> Result<(), PlanError> {
        if vertex_ids.contains(&plan.id()) {
            let follows_order = plan.id() == carrier_id;
            let (one_changed, one_ordered) = if follows_order {
                optimize_recursive(context, plan, order_columns, parent_filters)?
            } else {
                optimize_recursive(context, plan, &[], &[])?
            };
            *changed |= one_changed;
            *ordered |= follows_order && one_ordered;
            return Ok(());
        }
        for child in plan.base_mut().children_mut() {
            visit(
                context,
                child,
                vertex_ids,
                carrier_id,
                order_columns,
                parent_filters,
                changed,
                ordered,
            )?;
        }
        Ok(())
    }

    let mut changed = false;
    let mut ordered = false;
    for child in children {
        visit(
            context,
            child,
            &vertex_ids,
            carrier_id,
            order_columns,
            parent_filters,
            &mut changed,
            &mut ordered,
        )?;
    }
    Ok((changed, ordered))
}

fn should_use_cdc_based_join_reorder(context: &RuleContext<'_>) -> bool {
    context.advanced_join_reorder && context.join_reorder_threshold <= 0
}

fn extract_ordering_columns(items: &[ByItems]) -> Vec<Column> {
    let mut columns = Vec::with_capacity(items.len());
    for item in items {
        if item.desc {
            return Vec::new();
        }
        let Expression::Column(column) = &item.expr else {
            return Vec::new();
        };
        columns.push(column.clone());
    }
    columns
}

fn same_ordering_columns(left: &[Column], right: &[Column]) -> bool {
    !left.is_empty()
        && !right.is_empty()
        && left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.unique_id == right.unique_id)
}

fn rewrite_ordering_for_projection(
    projection: &super::projection::LogicalProjection,
    order_columns: &[Column],
) -> Vec<Column> {
    let Some(schema) = projection.base.base.schema() else {
        return Vec::new();
    };
    let mut rewritten = Vec::with_capacity(order_columns.len());
    for column in order_columns {
        let position = schema.column_index(column);
        let Ok(position) = usize::try_from(position) else {
            return Vec::new();
        };
        let Some(Expression::Column(mapped)) = projection.exprs.get(position) else {
            return Vec::new();
        };
        rewritten.push(mapped.clone());
    }
    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::find_best_task::LogicalJoinType;
    use crate::logical::data_source::DataSource;
    use crate::logical::join::LogicalJoin;
    use crate::logical::topn::LogicalTopN;
    use crate::logical::BaseLogicalPlan;
    use crate::plan_base::PlanIdAllocator;
    use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};
    use tidb_datatype::{
        FieldName, FieldNameMetadata, FieldType, FieldTypeCode, IdentifierMetadata,
    };
    use tidb_expr::schema::Schema;

    fn column(id: i64) -> Column {
        let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        column.id = id;
        column
    }

    fn field_name(table: &str, column: &str) -> FieldName {
        FieldName::new(FieldNameMetadata {
            table: IdentifierMetadata::new(table),
            original_table: IdentifierMetadata::new(table),
            column: IdentifierMetadata::new(column),
            original_column: IdentifierMetadata::new(column),
            ..FieldNameMetadata::default()
        })
    }

    fn data_source(
        allocator: &PlanIdAllocator,
        table_id: i64,
        table: &str,
        indexed: bool,
    ) -> LogicalPlan {
        let mut base = BaseLogicalPlan::new(allocator, DataSource::TYPE, 1);
        base.base
            .set_schema(Some(Schema::new(vec![column(table_id)])));
        base.base.set_output_names(vec![field_name(table, "a")]);
        let mut source = DataSource::new(base, table_id, table);
        source.table_columns = vec![column(table_id)];
        if indexed {
            source.indexes.push(SourceIndex {
                is_public: true,
                is_visible: true,
                columns: vec![SourceIndexColumn {
                    name: "a".to_owned(),
                    offset: 0,
                    ..SourceIndexColumn::default()
                }],
                ..SourceIndex::default()
            });
        }
        LogicalPlan::DataSource(source)
    }

    #[test]
    fn ordering_columns_match_the_pinned_forward_column_only_contract() {
        let columns = vec![column(1), column(2)];
        let items = columns
            .iter()
            .cloned()
            .map(|column| ByItems::new(Expression::Column(column), false))
            .collect::<Vec<_>>();
        assert!(same_ordering_columns(
            &columns,
            &extract_ordering_columns(&items)
        ));
        assert!(
            extract_ordering_columns(&[ByItems::new(Expression::Column(column(1)), true)])
                .is_empty()
        );
    }

    #[test]
    fn topn_annotates_only_the_index_order_carrier() {
        let allocator = PlanIdAllocator::new();
        let left = data_source(&allocator, 1, "t1", true);
        let right = data_source(&allocator, 2, "t2", false);
        let mut join_base = BaseLogicalPlan::new(&allocator, LogicalJoin::TYPE, 1);
        join_base
            .base
            .set_schema(Some(Schema::new(vec![column(1), column(2)])));
        join_base
            .base
            .set_output_names(vec![field_name("t1", "a"), field_name("t2", "a")]);
        join_base.set_children(vec![left, right]);
        let join = LogicalPlan::Join(LogicalJoin::new(join_base, LogicalJoinType::Inner));

        let mut topn_base = BaseLogicalPlan::new(&allocator, LogicalTopN::TYPE, 1);
        topn_base
            .base
            .set_schema(Some(Schema::new(vec![column(1), column(2)])));
        topn_base.set_children(vec![join]);
        let topn = LogicalPlan::TopN(LogicalTopN::new(
            topn_base,
            vec![ByItems::new(Expression::Column(column(1)), false)],
            0,
            1,
        ));

        let mut context = crate::logical::rule_tests::test_context(&allocator);
        context.join_reorder_threshold = 0;
        context.advanced_join_reorder = true;
        let (optimized, changed) = OrderAwareJoinReorder.optimize(&context, topn).unwrap();
        assert!(changed);
        let LogicalPlan::TopN(topn) = optimized else {
            panic!("TopN must remain the root")
        };
        let [LogicalPlan::Join(join)] = topn.base.children() else {
            panic!("join must remain below TopN")
        };
        assert!(join.internal_prefer_join_order);
        assert!(!join.prefer_join_order);
    }
}

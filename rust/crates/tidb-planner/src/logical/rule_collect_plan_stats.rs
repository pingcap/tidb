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

//! Go `pkg/planner/core/rule/collect_column_stats_usage.go`.
//!
//! The collector runs after predicate pushdown and before physical planning.
//! It follows column lineage through every schema-changing logical operator,
//! then reports the base-table columns whose statistics the optimizer reads.
//! `true` means the histogram payload is required; `false` means metadata such
//! as NDV is sufficient.

use std::collections::{HashMap, HashSet};

use tidb_expr::column::Column;
use tidb_expr::expr_util::extract_columns_and_cor_columns_from_expressions;
use tidb_expr::expression::Expression;
use tidb_model::TableItemID;

use super::data_source::DataSource;
use super::rule::{LogicalOptRule, RuleContext};
use super::LogicalPlan;
use crate::plan_base::PlanError;

/// Go `CollectColumnStatsUsage`'s statistics-loading result.
#[derive(Debug, Default)]
pub struct ColumnStatsUsage {
    /// Base-table column -> whether its complete histogram is required.
    pub predicate_columns: HashMap<TableItemID, bool>,
    /// Logical table IDs visited by data sources.
    pub visited_logical_table_ids: HashSet<i64>,
    /// Logical table ID -> physical partition IDs, for static pruning.
    pub table_partition_ids: HashMap<i64, Vec<i64>>,
    /// Number of logical operators visited.
    pub operator_count: u64,
}

/// The domain statistics handle reached by Go
/// `CollectPredicateColumnsPoint.Optimize`.
pub trait StatisticsLoadRequester {
    /// Requests the collected items before later logical rules and physical
    /// enumeration read their statistics.
    fn request(&self, usage: &ColumnStatsUsage) -> Result<(), PlanError>;
}

/// Go `CollectPredicateColumnsPoint`.
pub struct CollectPredicateColumnsPoint;

impl LogicalOptRule for CollectPredicateColumnsPoint {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let usage = collect_column_stats_usage(&plan);
        if let Some(requester) = ctx.statistics_load {
            if let Err(error) = requester.request(&usage) {
                return Err((plan, error));
            }
        }
        Ok((plan, false))
    }

    fn name(&self) -> &'static str {
        "collect_predicate_columns_point"
    }
}

#[derive(Default)]
struct Collector {
    result: ColumnStatsUsage,
    /// Expression `Column.UniqueID` -> base-table columns contributing stats.
    column_map: HashMap<i64, HashSet<TableItemID>>,
}

impl Collector {
    fn add_column(&mut self, column: &Column, full_load: bool) {
        let Some(items) = self.column_map.get(&column.unique_id) else {
            return;
        };
        for item in items {
            self.result
                .predicate_columns
                .entry(*item)
                .and_modify(|current| *current |= full_load)
                .or_insert(full_load);
        }
    }

    fn add_expressions(&mut self, expressions: &[Expression], full_load: bool) {
        for column in extract_columns_and_cor_columns_from_expressions(Vec::new(), expressions) {
            self.add_column(&column, full_load);
        }
    }

    fn update_column(&mut self, output: &Column, related: impl IntoIterator<Item = Column>) {
        let mut items = HashSet::new();
        for column in related {
            if let Some(mapped) = self.column_map.get(&column.unique_id) {
                items.extend(mapped.iter().copied());
            }
        }
        self.column_map.insert(output.unique_id, items);
    }

    fn update_column_from_expressions(&mut self, output: &Column, expressions: &[Expression]) {
        self.update_column(
            output,
            extract_columns_and_cor_columns_from_expressions(Vec::new(), expressions),
        );
    }

    fn collect_data_source(&mut self, source: &DataSource) {
        if tidb_util::filter::is_system_schema(&source.db_name) {
            return;
        }
        self.result
            .visited_logical_table_ids
            .insert(source.table_id);
        if source.table_id != source.physical_table_id {
            self.result
                .table_partition_ids
                .entry(source.table_id)
                .or_default()
                .push(source.physical_table_id);
        }
        if let Some(schema) = source.base.base.schema() {
            for (metadata, column) in source.columns.iter().zip(&schema.columns) {
                if metadata.id <= 0 {
                    continue;
                }
                self.column_map.insert(
                    column.unique_id,
                    HashSet::from([TableItemID {
                        table_id: source.table_id,
                        id: metadata.id,
                        is_index: false,
                        is_sync_load_failed: false,
                    }]),
                );
            }
        }
        self.add_expressions(&source.pushed_down_conds, true);
    }

    fn collect_join(&mut self, join: &super::join::LogicalJoin) {
        let mut expressions = join
            .equal_conditions
            .iter()
            .cloned()
            .map(Expression::ScalarFunction)
            .collect::<Vec<_>>();
        expressions.extend(join.left_conditions.iter().cloned());
        expressions.extend(join.right_conditions.iter().cloned());
        expressions.extend(join.other_conditions.iter().cloned());
        self.add_expressions(&expressions, false);
    }

    fn collect_union(&mut self, plan: &LogicalPlan) {
        let Some(schema) = plan.schema() else {
            return;
        };
        for (position, output) in schema.columns.iter().enumerate() {
            self.update_column(
                output,
                plan.children()
                    .iter()
                    .filter_map(|child| child.schema()?.columns.get(position).cloned()),
            );
        }
    }

    fn collect_node(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::DataSource(source) => self.collect_data_source(source),
            LogicalPlan::IndexScan(scan) => {
                if let Some(source) = scan.source.as_deref() {
                    self.collect_data_source(source);
                    self.add_expressions(&scan.access_conds, true);
                }
            }
            LogicalPlan::TableScan(scan) => {
                if let Some(source) = scan.source.as_deref() {
                    self.collect_data_source(source);
                    self.add_expressions(&scan.access_conds, true);
                }
            }
            LogicalPlan::Projection(projection) => {
                if let Some(schema) = plan.schema() {
                    for (output, expression) in schema.columns.iter().zip(&projection.exprs) {
                        self.update_column_from_expressions(
                            output,
                            std::slice::from_ref(expression),
                        );
                    }
                }
            }
            LogicalPlan::Selection(selection) => {
                self.add_expressions(&selection.conditions, false);
            }
            LogicalPlan::Aggregation(aggregation) => {
                self.add_expressions(&aggregation.group_by_items, false);
                if let Some(schema) = plan.schema() {
                    for (output, function) in schema.columns.iter().zip(&aggregation.agg_funcs) {
                        self.update_column_from_expressions(output, function.args());
                    }
                }
            }
            LogicalPlan::Window(window) => {
                for item in &window.partition_by {
                    self.add_column(&item.col, false);
                }
                if let Some(schema) = plan.schema() {
                    let outputs = window.get_window_result_columns(schema);
                    for (output, function) in outputs.iter().zip(&window.window_func_descs) {
                        self.update_column_from_expressions(output, &function.base.args);
                    }
                }
            }
            LogicalPlan::Join(join) => self.collect_join(join),
            LogicalPlan::Apply(apply) => {
                self.collect_join(&apply.join);
                for correlated in &apply.cor_cols {
                    self.add_column(&correlated.column, false);
                }
            }
            LogicalPlan::Sort(sort) => {
                for item in &sort.by_items {
                    self.add_expressions(std::slice::from_ref(&item.expr), false);
                }
            }
            LogicalPlan::TopN(topn) => {
                for item in &topn.by_items {
                    self.add_expressions(std::slice::from_ref(&item.expr), false);
                }
            }
            LogicalPlan::UnionAll(_) | LogicalPlan::PartitionUnionAll(_) => {
                self.collect_union(plan);
            }
            LogicalPlan::CTE(cte) => {
                let Some(class) = &cte.cte else {
                    self.result.operator_count += 1;
                    return;
                };
                let class = class.borrow();
                if let Some(seed) = class.seed_part_logical_plan.as_deref() {
                    self.collect_tree(seed);
                }
                if let Some(recursive) = class.recursive_part_logical_plan.as_deref() {
                    self.collect_tree(recursive);
                }
                if let (Some(schema), Some(seed_schema)) = (
                    plan.schema(),
                    class
                        .seed_part_logical_plan
                        .as_deref()
                        .and_then(LogicalPlan::schema),
                ) {
                    let recursive_schema = class
                        .recursive_part_logical_plan
                        .as_deref()
                        .and_then(LogicalPlan::schema);
                    for (position, output) in schema.columns.iter().enumerate() {
                        let related = seed_schema
                            .columns
                            .get(position)
                            .into_iter()
                            .cloned()
                            .chain(
                                recursive_schema
                                    .and_then(|schema| schema.columns.get(position))
                                    .into_iter()
                                    .cloned(),
                            );
                        self.update_column(output, related);
                    }
                    if class.is_distinct {
                        for column in &schema.columns {
                            self.add_column(column, false);
                        }
                    }
                }
            }
            LogicalPlan::CTETable(table) => {
                if let (Some(schema), Some(seed_schema)) = (plan.schema(), &table.seed_schema) {
                    for (output, seed) in schema.columns.iter().zip(&seed_schema.columns) {
                        self.update_column(output, std::iter::once(seed.clone()));
                    }
                }
            }
            LogicalPlan::Limit(_)
            | LogicalPlan::MaxOneRow(_)
            | LogicalPlan::Lock(_)
            | LogicalPlan::Sequence(_)
            | LogicalPlan::UnionScan(_)
            | LogicalPlan::TiKVSingleGather(_)
            | LogicalPlan::TableDual(_)
            | LogicalPlan::Expand(_)
            | LogicalPlan::MemTable(_)
            | LogicalPlan::Show(_)
            | LogicalPlan::ShowDDLJobs(_) => {}
        }
        self.result.operator_count += 1;
    }

    fn collect_tree(&mut self, root: &LogicalPlan) {
        enum Step<'a> {
            Enter(&'a LogicalPlan),
            Exit(&'a LogicalPlan),
        }
        let mut work = vec![Step::Enter(root)];
        while let Some(step) = work.pop() {
            match step {
                Step::Enter(plan) => {
                    work.push(Step::Exit(plan));
                    for child in plan.children().iter().rev() {
                        work.push(Step::Enter(child));
                    }
                }
                Step::Exit(plan) => self.collect_node(plan),
            }
        }
    }
}

/// Go `CollectColumnStatsUsage`.
#[must_use]
pub fn collect_column_stats_usage(plan: &LogicalPlan) -> ColumnStatsUsage {
    let mut collector = Collector::default();
    collector.collect_tree(plan);
    collector.result
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::expression::{Expression, ScalarFunction};
    use tidb_expr::schema::Schema;

    use super::*;
    use crate::logical::data_source::DataSourceColumn;
    use crate::logical::join::LogicalJoin;
    use crate::logical::projection::LogicalProjection;
    use crate::logical::rule_tests::{test_context, TEST_BUILDER};
    use crate::logical::selection::LogicalSelection;
    use crate::logical::BaseLogicalPlan;
    use crate::plan_base::PlanIdAllocator;

    fn column(id: i64) -> Column {
        Column::new(id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn base(id: i32, name: &str, schema: &[i64]) -> BaseLogicalPlan {
        let mut base = BaseLogicalPlan::with_id(id, name, 0);
        base.base.set_schema(Some(Schema::new(
            schema.iter().copied().map(column).collect(),
        )));
        base
    }

    fn eq(left: i64, right: i64) -> ScalarFunction {
        ScalarFunction::new(
            CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(column(left)),
                Expression::Column(column(right)),
            ],
        )
    }

    fn source(plan_id: i32, table_id: i64, columns: &[(i64, i64)]) -> LogicalPlan {
        let mut source = DataSource::new(
            base(
                plan_id,
                DataSource::TYPE,
                &columns
                    .iter()
                    .map(|(_, unique_id)| *unique_id)
                    .collect::<Vec<_>>(),
            ),
            table_id,
            format!("t{table_id}"),
        );
        source.db_name = "test".to_owned();
        source.columns = columns
            .iter()
            .map(|(id, _)| DataSourceColumn {
                id: *id,
                name: format!("c{id}"),
                is_primary_key: false,
            })
            .collect();
        LogicalPlan::DataSource(source)
    }

    fn item(table_id: i64, id: i64) -> TableItemID {
        TableItemID {
            table_id,
            id,
            is_index: false,
            is_sync_load_failed: false,
        }
    }

    #[test]
    fn pushed_filters_are_full_and_projection_filters_follow_base_lineage() {
        let mut child = source(1, 7, &[(11, 1), (12, 2)]);
        let LogicalPlan::DataSource(source) = &mut child else {
            unreachable!()
        };
        source.pushed_down_conds = vec![Expression::ScalarFunction(eq(1, 1))];

        let mut projection = LogicalPlan::Projection(LogicalProjection {
            base: base(2, LogicalProjection::TYPE, &[10]),
            exprs: vec![Expression::Column(column(2))],
            ..LogicalProjection::default()
        });
        projection.set_children(vec![child]);
        let mut selection = LogicalPlan::Selection(LogicalSelection::new(
            base(3, LogicalSelection::TYPE, &[10]),
            vec![Expression::ScalarFunction(eq(10, 10))],
        ));
        selection.set_children(vec![projection]);

        let usage = collect_column_stats_usage(&selection);
        assert_eq!(usage.predicate_columns.get(&item(7, 11)), Some(&true));
        assert_eq!(usage.predicate_columns.get(&item(7, 12)), Some(&false));
        assert_eq!(usage.visited_logical_table_ids, HashSet::from([7]));
        assert_eq!(usage.operator_count, 3);
    }

    #[test]
    fn join_conditions_request_metadata_from_both_tables() {
        let left = source(1, 7, &[(11, 1)]);
        let right = source(2, 8, &[(21, 2)]);
        let mut join = LogicalPlan::Join(LogicalJoin {
            base: base(3, LogicalJoin::TYPE, &[1, 2]),
            equal_conditions: vec![eq(1, 2)],
            ..LogicalJoin::default()
        });
        join.set_children(vec![left, right]);

        let usage = collect_column_stats_usage(&join);
        assert_eq!(usage.predicate_columns.get(&item(7, 11)), Some(&false));
        assert_eq!(usage.predicate_columns.get(&item(8, 21)), Some(&false));
        assert_eq!(usage.operator_count, 3);
    }

    #[test]
    fn logical_rule_requests_the_collected_usage_at_its_go_position() {
        struct Requester {
            calls: Cell<usize>,
            operators: Cell<u64>,
        }

        impl StatisticsLoadRequester for Requester {
            fn request(&self, usage: &ColumnStatsUsage) -> Result<(), PlanError> {
                self.calls.set(self.calls.get() + 1);
                self.operators.set(usage.operator_count);
                Ok(())
            }
        }

        let allocator = PlanIdAllocator::new();
        let requester = Requester {
            calls: Cell::new(0),
            operators: Cell::new(0),
        };
        let mut context = test_context(&allocator);
        context.builder = &TEST_BUILDER;
        context.statistics_load = Some(&requester);
        let plan = source(1, 7, &[(11, 1)]);

        let (returned, changed) = CollectPredicateColumnsPoint
            .optimize(&context, plan)
            .expect("statistics request");
        assert!(!changed);
        assert!(matches!(returned, LogicalPlan::DataSource(_)));
        assert_eq!(requester.calls.get(), 1);
        assert_eq!(requester.operators.get(), 1);
    }
}

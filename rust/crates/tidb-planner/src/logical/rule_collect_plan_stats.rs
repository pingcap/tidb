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
use tidb_expr::simple_expr::extract_columns;
use tidb_model::TableItemID;

use super::data_source::DataSource;
use super::fold::{fold_owned, Descend, OwnedRewrite};
use super::rule::{LogicalOptRule, RuleContext};
use super::rule_prune_indexes::prune_data_source;
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
    /// Physical table ID -> index IDs retained by Go's first pruning phase.
    /// A table is present only when pruning actually removed a path.
    pub kept_index_ids: HashMap<i64, HashSet<i64>>,
    /// Number of logical operators visited.
    pub operator_count: u64,
}

/// The domain statistics handle reached by Go
/// `CollectPredicateColumnsPoint.Optimize`.
pub trait StatisticsLoadRequester {
    /// Requests the collected items before later logical rules and physical
    /// enumeration read their statistics.
    fn request(&self, usage: &ColumnStatsUsage) -> Result<(), PlanError>;

    /// Waits at Go `SyncWaitStatsLoadPoint`, after the intervening logical
    /// rules have had a chance to run while the workers load in parallel.
    fn wait(&self) -> Result<(), PlanError>;
}

/// Go `SyncWaitStatsLoadPoint`.
pub struct SyncWaitStatsLoadPoint;

impl LogicalOptRule for SyncWaitStatsLoadPoint {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        if let Some(requester) = ctx.statistics_load {
            if let Err(error) = requester.wait() {
                return Err((plan, error));
            }
        }
        Ok((plan, false))
    }

    fn name(&self) -> &'static str {
        "sync_wait_stats_load_point"
    }
}

/// Go `CollectPredicateColumnsPoint`.
pub struct CollectPredicateColumnsPoint;

impl LogicalOptRule for CollectPredicateColumnsPoint {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        let (plan, usage) = collect_column_stats_usage(plan, ctx.opt_index_prune_threshold);
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

#[derive(Clone, Default)]
struct InterestingColumnsDown {
    join_columns: Vec<Column>,
    ordering_columns: Vec<Column>,
    asked_col_groups: Vec<Vec<Column>>,
}

struct InterestingColumnPruner {
    threshold: i32,
    kept_index_ids: HashMap<i64, HashSet<i64>>,
}

impl InterestingColumnPruner {
    fn record_asked_groups(source: &mut DataSource, groups: &[Vec<Column>]) {
        if tidb_util::filter::is_system_schema(&source.db_name) {
            return;
        }
        let Some(schema) = source.base.base.schema() else {
            return;
        };
        source.asked_column_group.extend(
            groups
                .iter()
                .filter(|group| group.iter().all(|column| schema.contains(column)))
                .cloned(),
        );
    }

    fn add_node_columns(node: &LogicalPlan, down: &mut InterestingColumnsDown) {
        match node {
            LogicalPlan::Join(join) => {
                for condition in &join.equal_conditions {
                    down.join_columns
                        .extend(condition.args.iter().flat_map(extract_columns));
                }
                for condition in &join.other_conditions {
                    down.join_columns.extend(extract_columns(condition));
                }
            }
            LogicalPlan::Apply(apply) => {
                for condition in &apply.join.equal_conditions {
                    down.join_columns
                        .extend(condition.args.iter().flat_map(extract_columns));
                }
                for condition in &apply.join.other_conditions {
                    down.join_columns.extend(extract_columns(condition));
                }
            }
            LogicalPlan::Sort(sort) => {
                for item in &sort.by_items {
                    down.ordering_columns.extend(extract_columns(&item.expr));
                }
            }
            LogicalPlan::TopN(topn) => {
                for item in &topn.by_items {
                    down.ordering_columns.extend(extract_columns(&item.expr));
                }
            }
            LogicalPlan::Window(window) => down
                .ordering_columns
                .extend(window.order_by.iter().map(|item| item.col.clone())),
            LogicalPlan::Aggregation(aggregation) => {
                for item in &aggregation.group_by_items {
                    down.ordering_columns.extend(extract_columns(item));
                }
                for function in &aggregation.agg_funcs {
                    if matches!(
                        function.name(),
                        super::aggregation::AGG_FUNC_MIN | super::aggregation::AGG_FUNC_MAX
                    ) {
                        for argument in function.args() {
                            down.ordering_columns.extend(extract_columns(argument));
                        }
                    }
                }
            }
            LogicalPlan::DataSource(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Selection(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::MaxOneRow(_)
            | LogicalPlan::Lock(_)
            | LogicalPlan::Sequence(_)
            | LogicalPlan::UnionAll(_)
            | LogicalPlan::PartitionUnionAll(_)
            | LogicalPlan::UnionScan(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::IndexScan(_)
            | LogicalPlan::TiKVSingleGather(_)
            | LogicalPlan::CTE(_)
            | LogicalPlan::CTETable(_)
            | LogicalPlan::TableDual(_)
            | LogicalPlan::Expand(_)
            | LogicalPlan::MemTable(_)
            | LogicalPlan::Show(_)
            | LogicalPlan::ShowDDLJobs(_) => {}
        }
    }

    fn collect_for_source(source: &DataSource, down: &InterestingColumnsDown) -> Vec<Column> {
        let Some(schema) = source.base.base.schema() else {
            return Vec::new();
        };
        let mut seen = HashSet::new();
        let mut interesting = Vec::new();
        for condition in source.pushed_down_conds.iter().chain(&source.all_conds) {
            let columns = extract_columns(condition);
            if columns.iter().all(|column| schema.contains(column)) {
                for column in columns {
                    if seen.insert(column.unique_id) {
                        interesting.push(column);
                    }
                }
            }
        }
        for column in down.join_columns.iter().chain(&down.ordering_columns) {
            if schema.contains(column) && seen.insert(column.unique_id) {
                interesting.push(column.clone());
            }
        }
        interesting
    }

    fn prune_source(&mut self, source: &mut DataSource, down: &InterestingColumnsDown) {
        source.interesting_columns = Self::collect_for_source(source, down);
        if let Some(kept) = prune_data_source(source, self.threshold) {
            self.kept_index_ids
                .entry(source.physical_table_id)
                .or_default()
                .extend(kept);
        }
    }
}

impl OwnedRewrite for InterestingColumnPruner {
    type Down = InterestingColumnsDown;
    type Up = ();

    fn descend(
        &mut self,
        node: &mut LogicalPlan,
        mut down: Self::Down,
    ) -> Descend<Self::Down, Self::Up> {
        let child_groups = node.extract_col_groups(&down.asked_col_groups);
        if self.threshold >= 0 {
            Self::add_node_columns(node, &mut down);
        }
        match node {
            LogicalPlan::DataSource(source) => {
                Self::record_asked_groups(source, &down.asked_col_groups);
                if self.threshold >= 0 {
                    self.prune_source(source, &down);
                }
                Descend::Stop(())
            }
            LogicalPlan::IndexScan(scan) => {
                if let Some(source) = scan.source.as_deref_mut() {
                    Self::record_asked_groups(source, &down.asked_col_groups);
                    // Go does not populate InterestingColumns for scan-source
                    // wrappers in `CollectColumnStatsUsage`.
                    if self.threshold >= 0 {
                        if let Some(kept) = prune_data_source(source, self.threshold) {
                            self.kept_index_ids
                                .entry(source.physical_table_id)
                                .or_default()
                                .extend(kept);
                        }
                    }
                }
                down.asked_col_groups = child_groups;
                Descend::Children(vec![down; node.children().len()])
            }
            LogicalPlan::TableScan(scan) => {
                if let Some(source) = scan.source.as_deref_mut() {
                    Self::record_asked_groups(source, &down.asked_col_groups);
                    if self.threshold >= 0 {
                        if let Some(kept) = prune_data_source(source, self.threshold) {
                            self.kept_index_ids
                                .entry(source.physical_table_id)
                                .or_default()
                                .extend(kept);
                        }
                    }
                }
                down.asked_col_groups = child_groups;
                Descend::Children(vec![down; node.children().len()])
            }
            LogicalPlan::CTE(cte) => {
                if let Some(class) = &cte.cte {
                    let mut class = class.borrow_mut();
                    if let Some(seed) = class.seed_part_logical_plan.take() {
                        let (seed, ()) = fold_owned(self, *seed, InterestingColumnsDown::default());
                        class.seed_part_logical_plan = Some(Box::new(seed));
                    }
                    if let Some(recursive) = class.recursive_part_logical_plan.take() {
                        let (recursive, ()) =
                            fold_owned(self, *recursive, InterestingColumnsDown::default());
                        class.recursive_part_logical_plan = Some(Box::new(recursive));
                    }
                }
                down.asked_col_groups = child_groups;
                Descend::Children(vec![down; node.children().len()])
            }
            _ => {
                down.asked_col_groups = child_groups;
                Descend::Children(vec![down; node.children().len()])
            }
        }
    }

    fn ascend(&mut self, node: LogicalPlan, _children: Vec<Self::Up>) -> (LogicalPlan, Self::Up) {
        (node, ())
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
        self.column_map
            .entry(output.unique_id)
            .or_default()
            .extend(items);
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
            for column in &schema.columns {
                if column.id <= 0 {
                    continue;
                }
                self.column_map.insert(
                    column.unique_id,
                    HashSet::from([TableItemID {
                        table_id: source.table_id,
                        id: column.id,
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

fn collect_column_stats_usage_readonly(plan: &LogicalPlan) -> ColumnStatsUsage {
    let mut collector = Collector::default();
    collector.collect_tree(plan);
    collector.result
}

/// Go `CollectColumnStatsUsage`: populate DataSource column-group and
/// interesting-column state while collecting base-column statistics usage.
/// The returned plan is the ownership-safe equivalent of Go mutating the
/// pointed-to logical tree.
#[must_use]
pub fn collect_column_stats_usage(
    plan: LogicalPlan,
    opt_index_prune_threshold: i32,
) -> (LogicalPlan, ColumnStatsUsage) {
    let mut preparation = InterestingColumnPruner {
        threshold: opt_index_prune_threshold,
        kept_index_ids: HashMap::new(),
    };
    let (plan, ()) = fold_owned(&mut preparation, plan, InterestingColumnsDown::default());
    let mut usage = collect_column_stats_usage_readonly(&plan);
    usage.kept_index_ids = preparation.kept_index_ids;
    (plan, usage)
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::expression::{Expression, ScalarFunction};
    use tidb_expr::schema::Schema;

    use super::*;
    use crate::access_path::PossiblePath;
    use crate::logical::data_source::DataSourceColumn;
    use crate::logical::join::LogicalJoin;
    use crate::logical::projection::LogicalProjection;
    use crate::logical::rule_tests::{test_context, TEST_BUILDER};
    use crate::logical::selection::LogicalSelection;
    use crate::logical::BaseLogicalPlan;
    use crate::plan_base::PlanIdAllocator;
    use crate::plan_builder::catalog::{SourceIndex, SourceIndexColumn};

    fn column(id: i64) -> Column {
        let mut column = Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        column.id = id;
        column
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
        if let Some(mut schema) = source.base.base.schema().cloned() {
            for (column, (id, _)) in schema.columns.iter_mut().zip(columns) {
                column.id = *id;
            }
            source.base.base.set_schema(Some(schema));
        }
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

        let (_, usage) = collect_column_stats_usage(selection, -1);
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

        let (_, usage) = collect_column_stats_usage(join, -1);
        assert_eq!(usage.predicate_columns.get(&item(7, 11)), Some(&false));
        assert_eq!(usage.predicate_columns.get(&item(8, 21)), Some(&false));
        assert_eq!(usage.operator_count, 3);
    }

    #[test]
    fn join_column_groups_reach_each_data_source_when_index_pruning_is_disabled() {
        let left = source(1, 7, &[(11, 1), (12, 2)]);
        let right = source(2, 8, &[(21, 3), (22, 4)]);
        let mut join = LogicalPlan::Join(LogicalJoin {
            base: base(3, LogicalJoin::TYPE, &[1, 2, 3, 4]),
            equal_conditions: vec![eq(1, 3), eq(2, 4)],
            ..LogicalJoin::default()
        });
        join.set_children(vec![left, right]);

        let allocator = PlanIdAllocator::new();
        let mut context = test_context(&allocator);
        context.builder = &TEST_BUILDER;
        context.opt_index_prune_threshold = -1;
        let (plan, changed) = CollectPredicateColumnsPoint
            .optimize(&context, join)
            .expect("statistics collection");
        assert!(!changed);
        let children = plan.children();
        let LogicalPlan::DataSource(left) = &children[0] else {
            panic!("left DataSource")
        };
        let LogicalPlan::DataSource(right) = &children[1] else {
            panic!("right DataSource")
        };
        let ids = |groups: &[Vec<Column>]| {
            groups
                .iter()
                .map(|group| {
                    group
                        .iter()
                        .map(|column| column.unique_id)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&left.asked_column_group), vec![vec![1, 2]]);
        assert_eq!(ids(&right.asked_column_group), vec![vec![3, 4]]);
    }

    #[test]
    fn system_schema_sources_do_not_collect_statistics_usage() {
        let mut plan = source(1, 7, &[(11, 1)]);
        let LogicalPlan::DataSource(source) = &mut plan else {
            unreachable!()
        };
        source.db_name = "mysql".to_owned();
        source.pushed_down_conds = vec![Expression::ScalarFunction(eq(1, 1))];

        let (_, usage) = collect_column_stats_usage(plan, -1);
        assert!(usage.predicate_columns.is_empty());
        assert!(usage.visited_logical_table_ids.is_empty());
        assert!(usage.table_partition_ids.is_empty());
    }

    #[test]
    fn system_schema_sources_do_not_record_asked_column_groups() {
        let mut system = source(1, 7, &[(11, 1), (12, 2)]);
        let LogicalPlan::DataSource(system_source) = &mut system else {
            unreachable!()
        };
        system_source.db_name = "mysql".to_owned();
        let ordinary = source(2, 8, &[(21, 3), (22, 4)]);
        let mut join = LogicalPlan::Join(LogicalJoin {
            base: base(3, LogicalJoin::TYPE, &[1, 2, 3, 4]),
            equal_conditions: vec![eq(1, 3), eq(2, 4)],
            ..LogicalJoin::default()
        });
        join.set_children(vec![system, ordinary]);

        let (plan, _) = collect_column_stats_usage(join, -1);
        let children = plan.children();
        let LogicalPlan::DataSource(system) = &children[0] else {
            panic!("system DataSource")
        };
        let LogicalPlan::DataSource(ordinary) = &children[1] else {
            panic!("ordinary DataSource")
        };
        assert!(system.asked_column_group.is_empty());
        assert_eq!(ordinary.asked_column_group.len(), 1);
    }

    #[test]
    fn data_source_lineage_uses_schema_column_ids_after_reordering() {
        let mut plan = source(1, 7, &[(11, 1), (12, 2)]);
        let LogicalPlan::DataSource(source) = &mut plan else {
            unreachable!()
        };
        let mut schema = source.base.base.schema().expect("schema").clone();
        schema.columns.swap(0, 1);
        source.base.base.set_schema(Some(schema));
        source.pushed_down_conds = vec![Expression::ScalarFunction(eq(2, 2))];

        let (_, usage) = collect_column_stats_usage(plan, -1);
        assert_eq!(usage.predicate_columns.get(&item(7, 12)), Some(&true));
        assert!(!usage.predicate_columns.contains_key(&item(7, 11)));
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

            fn wait(&self) -> Result<(), PlanError> {
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

    #[test]
    fn logical_rule_prunes_before_requesting_index_statistics() {
        struct Requester {
            kept: RefCell<HashMap<i64, HashSet<i64>>>,
        }

        impl StatisticsLoadRequester for Requester {
            fn request(&self, usage: &ColumnStatsUsage) -> Result<(), PlanError> {
                *self.kept.borrow_mut() = usage.kept_index_ids.clone();
                Ok(())
            }

            fn wait(&self) -> Result<(), PlanError> {
                Ok(())
            }
        }

        let allocator = PlanIdAllocator::new();
        let requester = Requester {
            kept: RefCell::new(HashMap::new()),
        };
        let mut context = test_context(&allocator);
        context.builder = &TEST_BUILDER;
        context.statistics_load = Some(&requester);
        context.opt_index_prune_threshold = 0;
        let mut plan = source(1, 7, &[(11, 1), (12, 2)]);
        let LogicalPlan::DataSource(source) = &mut plan else {
            unreachable!()
        };
        source.physical_table_id = 7;
        let mut first = column(1);
        first.id = 11;
        let mut second = column(2);
        second.id = 12;
        source
            .base
            .base
            .set_schema(Some(Schema::new(vec![first.clone(), second.clone()])));
        source.table_columns = vec![first.clone(), second];
        source.indexes = vec![
            SourceIndex {
                id: 101,
                columns: vec![SourceIndexColumn {
                    offset: 0,
                    ..SourceIndexColumn::default()
                }],
                ..SourceIndex::default()
            },
            SourceIndex {
                id: 102,
                columns: vec![SourceIndexColumn {
                    offset: 1,
                    ..SourceIndexColumn::default()
                }],
                ..SourceIndex::default()
            },
        ];
        source.enumerated_paths = vec![
            PossiblePath::Table {
                is_int_handle: true,
                primary_index: None,
            },
            PossiblePath::Index { index: 0 },
            PossiblePath::Index { index: 1 },
        ];
        source.pushed_down_conds = vec![Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![Expression::Column(first.clone()), Expression::Column(first)],
        ))];

        let (returned, changed) = CollectPredicateColumnsPoint
            .optimize(&context, plan)
            .expect("statistics request");
        assert!(!changed);
        let LogicalPlan::DataSource(returned) = returned else {
            unreachable!()
        };
        assert_eq!(returned.interesting_columns.len(), 1);
        assert_eq!(returned.interesting_columns[0].unique_id, 1);
        assert_eq!(returned.enumerated_paths.len(), 2);
        assert_eq!(requester.kept.borrow().get(&7), Some(&HashSet::from([101])));
    }

    #[test]
    fn sync_wait_rule_uses_the_later_go_rule_position() {
        struct Requester {
            requests: Cell<usize>,
            waits: Cell<usize>,
        }

        impl StatisticsLoadRequester for Requester {
            fn request(&self, _usage: &ColumnStatsUsage) -> Result<(), PlanError> {
                self.requests.set(self.requests.get() + 1);
                Ok(())
            }

            fn wait(&self) -> Result<(), PlanError> {
                self.waits.set(self.waits.get() + 1);
                Ok(())
            }
        }

        let allocator = PlanIdAllocator::new();
        let requester = Requester {
            requests: Cell::new(0),
            waits: Cell::new(0),
        };
        let mut context = test_context(&allocator);
        context.statistics_load = Some(&requester);
        let plan = source(1, 7, &[(11, 1)]);

        let (plan, _) = CollectPredicateColumnsPoint
            .optimize(&context, plan)
            .expect("start statistics load");
        assert_eq!(requester.requests.get(), 1);
        assert_eq!(requester.waits.get(), 0);
        SyncWaitStatsLoadPoint
            .optimize(&context, plan)
            .expect("wait for statistics load");
        assert_eq!(requester.waits.get(), 1);
    }
}

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

//! Go builder.go / distsql.go: storage ownership is execution-local, while
//! scan expressions survive inner-reader Open/Close cycles under Apply.

use tidb_expr::{expression::Expression, schema::Schema};
use tidb_planner::physical::PhysicalPlan;

use super::PhysicalReaderBuilder;
use crate::{
    access_path::IndexRangeSourceExec,
    driver::{Catalog, TableEntry},
    kv_table::{IndexRange, KvTable, RowDecodeContext, TableScanExec},
    predicate_pushdown::{describe_execution_condition, PushedScanFilter, ScanPredicate},
    remote_scan::PushdownStatementContext,
    table_access::TableAccess,
    ExecError, Executor, ExecutorMeta, ProjectionExec, StmtContext,
};

/// Reads the caller's current transaction catalog, never a saved executor or
/// an independent storage snapshot. Construct once per executor-build call.
pub struct CatalogReaderBuilder<'a> {
    catalog: &'a Catalog,
    batch_ranges: Option<(Vec<IndexRange>, bool)>,
}

impl<'a> CatalogReaderBuilder<'a> {
    /// Borrow the current statement's catalog, whose tables own its storage.
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            batch_ranges: None,
        }
    }

    fn table(&self, id: i64) -> Result<KvTable, ExecError> {
        for (_, _, entry) in self.catalog.table_entries() {
            let TableEntry::Kv(table) = entry else {
                continue;
            };
            if table.table_id == id {
                return Ok(table.clone());
            }
            if table.partition().is_some_and(|partition| {
                partition
                    .definitions
                    .iter()
                    .any(|definition| definition.id == id)
            }) {
                let mut table = table.clone();
                table.restrict_read_to_partitions(&[id]);
                return Ok(table);
            }
        }
        Err(ExecError::internal(format!(
            "physical table {id} is absent from the transaction catalog"
        )))
    }
}

/// Only the scan definition and current bindings, not an AST or optimizer.
pub(crate) struct ReaderBindings {
    scan: PhysicalPlan,
    context: StmtContext,
    filters: Vec<Expression>,
    index_filter_count: usize,
    dynamic_access: bool,
    dynamic_filter: bool,
    ranges_initialized: bool,
    filter_initialized: bool,
    batch_ranges: Option<Vec<IndexRange>>,
    pub(crate) index_predicates: Vec<ScanPredicate>,
}

fn dynamic(expression: &Expression) -> bool {
    match expression {
        Expression::CorrelatedColumn(_) => true,
        Expression::Constant(constant) => constant.literal_value().is_none(),
        Expression::ScalarFunction(function) => function.args.iter().any(dynamic),
        Expression::Column(_) => false,
    }
}

impl ReaderBindings {
    fn new(
        scan: &PhysicalPlan,
        context: &StmtContext,
        filters: Vec<Expression>,
        index_filter_count: usize,
    ) -> Self {
        let access = match scan {
            PhysicalPlan::TableScan(scan) => &scan.access_conditions,
            PhysicalPlan::IndexScan(scan) => &scan.access_conditions,
            _ => unreachable!("reader binding requires a scan"),
        };
        Self {
            scan: scan.clone_shallow(),
            context: context.clone(),
            dynamic_access: access.iter().any(dynamic),
            dynamic_filter: filters.iter().any(dynamic),
            ranges_initialized: false,
            filter_initialized: false,
            batch_ranges: None,
            filters,
            index_filter_count,
            index_predicates: Vec::new(),
        }
    }

    /// Called before opening storage requests, after Apply binds its outer row.
    /// Initial literal ranges remain untouched; mutable accesses use Go ranger.
    pub(crate) fn ranges(&mut self) -> Result<Option<Vec<IndexRange>>, ExecError> {
        if self.ranges_initialized && !self.dynamic_access {
            return Ok(None);
        }
        if let Some(ranges) = self.batch_ranges.take() {
            self.ranges_initialized = true;
            return Ok(Some(ranges));
        }
        let rebuilt;
        let ranges = match &self.scan {
            PhysicalPlan::TableScan(scan) => {
                if self.dynamic_access {
                    rebuilt = scan.rebuild_access_ranges(&self.context).map_err(|error| {
                        ExecError::internal(format!("table range rebuild: {error:?}"))
                    })?;
                    &rebuilt
                } else {
                    &scan.ranges
                }
            }
            PhysicalPlan::IndexScan(scan) => {
                if self.dynamic_access {
                    rebuilt = scan.rebuild_access_ranges(&self.context).map_err(|error| {
                        ExecError::internal(format!("index range rebuild: {error:?}"))
                    })?;
                    &rebuilt
                } else {
                    &scan.ranges
                }
            }
            _ => unreachable!("reader binding requires a scan"),
        };
        let ranges = ranges
            .iter()
            .map(|range| IndexRange {
                // Bytes may already be collation sort keys. Preserve datum kinds.
                low: range.low_val.clone(),
                high: range.high_val.clone(),
                low_exclusive: range.low_exclude,
                high_exclusive: range.high_exclude,
            })
            .collect();
        self.ranges_initialized = true;
        Ok(Some(ranges))
    }

    pub(crate) fn filter(&mut self) -> Result<Option<Vec<ScanPredicate>>, ExecError> {
        if self.filters.is_empty() || (self.filter_initialized && !self.dynamic_filter) {
            return Ok(None);
        }
        let predicates = self
            .filters
            .iter()
            .map(|filter| describe_execution_condition(filter, &self.context))
            .collect::<Result<Vec<_>, _>>()?;
        self.index_predicates = predicates[..self.index_filter_count].to_vec();
        self.filter_initialized = true;
        Ok(Some(predicates))
    }

    pub(crate) fn new_filter(&self, predicates: Vec<ScanPredicate>) -> PushedScanFilter {
        PushedScanFilter::new(predicates, self.filters.clone())
    }

    pub(crate) fn context(&self) -> &StmtContext {
        &self.context
    }
}

/// Extract the actual cop Selection chain. Other cop operators must acquire
/// their own lowering; they are never silently run as root operators here.
fn scan_and_filters(
    mut plan: &PhysicalPlan,
) -> Result<(&PhysicalPlan, Vec<Expression>), ExecError> {
    let mut selections = Vec::new();
    while let PhysicalPlan::Selection(selection) = plan {
        selections.push(selection.conditions.as_slice());
        let [child] = plan.children() else {
            return Err(ExecError::internal("cop Selection requires one child"));
        };
        plan = child;
    }
    if !matches!(
        plan,
        PhysicalPlan::TableScan(_) | PhysicalPlan::IndexScan(_)
    ) {
        return Err(ExecError::unsupported(format!(
            "native cop lowering for {}",
            plan.explain_id(false)
        )));
    }
    // Preserve lower Selection before upper Selection, as in the Go DAG.
    Ok((
        plan,
        selections.into_iter().rev().flatten().cloned().collect(),
    ))
}

impl PhysicalReaderBuilder for CatalogReaderBuilder<'_> {
    fn build_index_join_reader(
        &mut self,
        plan: &tidb_planner::physical::PhysicalIndexJoin,
        ranges: &[tidb_planner::ranger::types::Range],
        context: &StmtContext,
        init_cap: usize,
        max_chunk_size: usize,
    ) -> Result<Box<dyn crate::index_lookup_join::IndexJoinExecutorBuilder>, ExecError> {
        let inner = plan
            .join
            .base
            .children()
            .get(plan.join.inner_child_idx)
            .ok_or_else(|| ExecError::internal("IndexJoin inner child is absent"))?
            .clone();
        let scan = index_join_scan(&inner)?;
        let (table_id, access) = match scan {
            PhysicalPlan::TableScan(scan) => (scan.table_id, &scan.access_conditions),
            PhysicalPlan::IndexScan(scan) => (scan.table_id, &scan.access_conditions),
            _ => unreachable!(),
        };
        if plan.range_template.is_none() && access.iter().any(dynamic) {
            return Err(ExecError::unsupported(
                "native IndexJoin dynamic access requires a retained range definition",
            ));
        }
        let table = self.table(table_id)?;
        if table.partition().is_some() {
            return Err(ExecError::unsupported(
                "native IndexJoin dynamic partition routing",
            ));
        }
        // Keep only this inner's current transaction owner, not a clone of
        // the entire catalog. Name lookup does not participate in this path.
        let mut catalog = Catalog::default();
        catalog.register_kv("index_join_inner", table);
        let compare = plan
            .compare_filters
            .as_ref()
            .map(super::index_join::CompareFilters::new)
            .transpose()?;
        Ok(Box::new(IndexJoinReader {
            plan: inner,
            catalog,
            context: context.clone(),
            template_collators: ranges
                .iter()
                .map(|range| range.collators.clone())
                .collect(),
            compare,
            init_cap,
            max_chunk_size,
        }))
    }

    fn build_reader(
        &mut self,
        plan: &PhysicalPlan,
        context: &StmtContext,
        meta: ExecutorMeta,
    ) -> Result<Box<dyn Executor>, ExecError> {
        let missing = || ExecError::internal("native reader has no pushed plan");
        let (scan, mut index_filters, mut table_filters, covering, lookup) = match plan {
            PhysicalPlan::TableScan(_) => (plan, Vec::new(), Vec::new(), false, None),
            PhysicalPlan::IndexScan(_) => (plan, Vec::new(), Vec::new(), true, None),
            PhysicalPlan::TableReader(reader) => {
                let (scan, filters) =
                    scan_and_filters(reader.table_plan.as_deref().ok_or_else(missing)?)?;
                if !matches!(scan, PhysicalPlan::TableScan(_)) {
                    return Err(missing());
                }
                (scan, Vec::new(), filters, false, None)
            }
            PhysicalPlan::IndexReader(reader) => {
                let (scan, filters) =
                    scan_and_filters(reader.index_plan.as_deref().ok_or_else(missing)?)?;
                if !matches!(scan, PhysicalPlan::IndexScan(_)) {
                    return Err(missing());
                }
                (scan, filters, Vec::new(), true, None)
            }
            PhysicalPlan::IndexLookUpReader(reader) => {
                let (scan, index_filters) =
                    scan_and_filters(reader.index_plan.as_deref().ok_or_else(missing)?)?;
                let (table_scan, table_filters) =
                    scan_and_filters(reader.table_plan.as_deref().ok_or_else(missing)?)?;
                let (PhysicalPlan::IndexScan(index), PhysicalPlan::TableScan(table)) =
                    (scan, table_scan)
                else {
                    return Err(missing());
                };
                if index.table_id != table.table_id {
                    return Err(ExecError::internal("lookup scans name different tables"));
                }
                (scan, index_filters, table_filters, false, Some(reader))
            }
            _ => return Err(ExecError::internal("not a physical reader")),
        };
        let table_id = match scan {
            PhysicalPlan::TableScan(scan) => scan.table_id,
            PhysicalPlan::IndexScan(scan) => scan.table_id,
            _ => unreachable!(),
        };
        let table = self.table(table_id)?;
        // The reader's output order is independent of table/index order.
        // Keep filter inputs until after both pushed and staged-row evaluation.
        let mut columns = meta.schema().columns.clone();
        let mut seen = std::collections::HashSet::new();
        columns.retain(|column| seen.insert(column.unique_id));
        for filter in index_filters.iter().chain(&table_filters) {
            for column in tidb_expr::simple_expr::extract_columns(filter) {
                if !columns
                    .iter()
                    .any(|existing| existing.unique_id == column.unique_id)
                {
                    columns.push(column);
                }
            }
        }
        // The storage cursors carry the synthetic handle after stored columns.
        columns.sort_by_key(|column| column.id == -1);
        for (index, column) in columns.iter_mut().enumerate() {
            column.index = index as i64;
        }
        let schema = Schema::new(columns);
        for filter in index_filters.iter_mut().chain(&mut table_filters) {
            tidb_expr::simple_expr::resolve_indices_in_place(filter, &schema).map_err(|error| {
                ExecError::internal(format!("reader filter binding: {error:?}"))
            })?;
        }
        let index_filter_count = index_filters.len();
        let has_table_filters = !table_filters.is_empty();
        index_filters.extend(table_filters);
        let mut bindings = ReaderBindings::new(scan, context, index_filters, index_filter_count);
        let batch_order = self
            .batch_ranges
            .as_ref()
            .map(|(_, can_reorder)| !can_reorder);
        if let Some((ranges, _)) = self.batch_ranges.take() {
            bindings.batch_ranges = Some(ranges);
            bindings.dynamic_access = false;
        }
        let read_meta = ExecutorMeta::new(
            schema.clone(),
            meta.id(),
            meta.init_cap(),
            meta.max_chunk_size(),
        );
        let mut keep = Vec::new();
        let mut handle = None;
        for (slot, column) in schema.columns.iter().enumerate() {
            if column.id == -1 {
                if handle.replace(slot).is_some() {
                    return Err(ExecError::unsupported(
                        "duplicate synthetic handle in reader schema",
                    ));
                }
            } else {
                keep.push(
                    table
                        .columns
                        .iter()
                        .position(|stored| stored.id == column.id)
                        .ok_or_else(|| {
                            ExecError::internal(format!(
                                "physical column {} is absent from table {table_id}",
                                column.id
                            ))
                        })?,
                );
            }
        }
        let source: Box<dyn Executor> = match scan {
            PhysicalPlan::TableScan(scan) => {
                if scan.store_type != tidb_planner::physical_table_reader::StoreType::TiKv
                    || !scan.late_materialization_filter_conditions.is_empty()
                {
                    return Err(ExecError::unsupported("native TiFlash reader lowering"));
                }
                Box::new(TableScanExec::from_native(
                    read_meta,
                    table,
                    keep,
                    handle,
                    bindings,
                    batch_order.unwrap_or(scan.keep_order),
                    scan.desc,
                ))
            }
            PhysicalPlan::IndexScan(scan) => {
                if !table
                    .indexes()
                    .iter()
                    .any(|index| index.id == scan.index_id)
                {
                    return Err(ExecError::internal(format!(
                        "index {} is absent from table {table_id}",
                        scan.index_id
                    )));
                }
                let mut source = IndexRangeSourceExec::new_with_statement(
                    read_meta,
                    table,
                    scan.index_id,
                    Vec::new(),
                    RowDecodeContext::for_query(context),
                    PushdownStatementContext::from_stmt(context),
                );
                if let Some(slot) = handle {
                    source.read_extra_handle(slot);
                }
                source.read_table_columns(keep);
                if covering {
                    source.mark_covering();
                }
                if covering || scan.keep_order || lookup.is_some_and(|reader| reader.keep_order) {
                    source.answer_in_index_order();
                }
                if scan.keep_order || scan.desc || lookup.is_some_and(|reader| reader.keep_order) {
                    source.accept_keep_order(scan.desc);
                }
                if let Some(rows) = scan.base.base.stats_info().map(|stats| stats.row_count()) {
                    source.accept_scan_estimate(rows);
                }
                source.bind_native_reader(bindings, !has_table_filters && index_filter_count != 0);
                if let Some(limit) = lookup.and_then(|reader| reader.pushed_limit) {
                    // The current lookup implementation cannot skip filtered
                    // handles before its nonzero offset on every storage path.
                    if has_table_filters
                        || (limit.offset != 0 && index_filter_count != 0)
                        || !source.accept_embedded_lookup_limit(limit.offset, limit.count)
                    {
                        return Err(ExecError::unsupported(
                            "native lookup pushed-limit lowering for filtered/dirty rows",
                        ));
                    }
                }
                Box::new(source)
            }
            _ => unreachable!(),
        };
        let output = meta
            .schema()
            .columns
            .iter()
            .map(|column| {
                let mut column = column.clone();
                column.index = schema.column_index(&column) as i64;
                Expression::Column(column)
            })
            .collect::<Vec<_>>();
        if output.len() == schema.columns.len()
            && output.iter().enumerate().all(|(index, expr)| {
                expr.as_column()
                    .is_some_and(|column| column.index == index as i64)
            })
        {
            Ok(source)
        } else {
            Ok(Box::new(ProjectionExec::new(
                meta,
                output,
                source,
                context.clone(),
            )))
        }
    }
}

/// Go dataReaderBuilder's lookup-bearing spine. Hash joins with a dynamically
/// selected lookup child and UnionScan remain explicit integration work.
fn index_join_scan(plan: &PhysicalPlan) -> Result<&PhysicalPlan, ExecError> {
    match plan {
        PhysicalPlan::TableScan(_) | PhysicalPlan::IndexScan(_) => Ok(plan),
        PhysicalPlan::TableReader(reader) => index_join_scan(
            reader
                .table_plan
                .as_deref()
                .ok_or_else(|| ExecError::internal("IndexJoin table reader has no scan"))?,
        ),
        PhysicalPlan::IndexReader(reader) => index_join_scan(
            reader
                .index_plan
                .as_deref()
                .ok_or_else(|| ExecError::internal("IndexJoin index reader has no scan"))?,
        ),
        PhysicalPlan::IndexLookUpReader(reader) => index_join_scan(
            reader
                .index_plan
                .as_deref()
                .ok_or_else(|| ExecError::internal("IndexJoin lookup reader has no scan"))?,
        ),
        PhysicalPlan::Selection(_)
        | PhysicalPlan::Projection(_)
        | PhysicalPlan::HashAgg(_)
        | PhysicalPlan::StreamAgg(_) => {
            let [child] = plan.children() else {
                return Err(ExecError::internal(
                    "IndexJoin inner spine requires one child",
                ));
            };
            index_join_scan(child)
        }
        _ => Err(ExecError::unsupported("native IndexJoin inner operator")),
    }
}

struct IndexJoinReader {
    plan: PhysicalPlan,
    catalog: Catalog,
    context: StmtContext,
    template_collators: Vec<Vec<tidb_datatype::Collation>>,
    compare: Option<super::index_join::CompareFilters>,
    init_cap: usize,
    max_chunk_size: usize,
}

impl crate::index_lookup_join::IndexJoinExecutorBuilder for IndexJoinReader {
    fn build_executor_for_index_join(
        &mut self,
        contents: &[crate::index_lookup_join::IndexJoinLookUpContent],
        templates: &[IndexRange],
        key_offsets: &[usize],
        can_reorder_handles: bool,
    ) -> Result<Box<dyn Executor>, ExecError> {
        use tidb_planner::ranger::types::Range;
        // Go cloneForIndexJoinBuild shares the immutable plan. Only the
        // request ranges and executor-builder state belong to this batch.
        let scan = index_join_scan(&self.plan)?;
        let integer_handle =
            matches!(scan, PhysicalPlan::TableScan(scan) if scan.common_handle_cols.is_empty());
        let mut ranges = Vec::new();
        if integer_handle {
            let handle_value = |key: &tidb_datatype::Datum| match key {
                tidb_datatype::Datum::Int(value) => Ok(*value),
                tidb_datatype::Datum::UInt(value) => Ok(*value as i64),
                _ => Err(ExecError::internal("unconverted integer handle lookup key")),
            };
            for content in contents {
                let Some(key) = content.keys.first() else {
                    return Err(ExecError::internal(
                        "integer handle lookup has no key",
                    ));
                };
                // Go dedupHandles: multiple equalities may target the same
                // primary key. Conflicting outer values describe no handle.
                let handle = handle_value(key)?;
                let mut valid = true;
                for other in &content.keys[1..] {
                    if handle_value(other)? != handle {
                        valid = false;
                        break;
                    }
                }
                if !valid {
                    continue;
                }
                ranges.push(Range {
                    low_val: vec![key.clone()],
                    high_val: vec![key.clone()],
                    low_exclude: false,
                    high_exclude: false,
                    collators: vec![],
                });
            }
        } else {
            for content in contents {
                if content.keys.len() != key_offsets.len() {
                    return Err(ExecError::internal("IndexJoin lookup key count mismatch"));
                }
                let last = self
                    .compare
                    .as_ref()
                    .map(|compare| compare.ranges(&self.context, content.row.as_row()))
                    .transpose()?;
                for (template_index, template) in templates.iter().enumerate() {
                    let mut range = Range {
                        low_val: template.low.clone(),
                        high_val: template.high.clone(),
                        low_exclude: template.low_exclusive,
                        high_exclude: template.high_exclusive,
                        collators: self
                            .template_collators
                            .get(template_index)
                            .cloned()
                            .ok_or_else(|| {
                                ExecError::internal("IndexJoin template count changed")
                            })?,
                    };
                    for (key, offset) in content.keys.iter().zip(key_offsets) {
                        *range.low_val.get_mut(*offset).ok_or_else(|| {
                            ExecError::internal("IndexJoin key offset outside template")
                        })? = key.clone();
                        *range.high_val.get_mut(*offset).ok_or_else(|| {
                            ExecError::internal("IndexJoin key offset outside template")
                        })? = key.clone();
                    }
                    if let Some(last) = &last {
                        let pos = range.low_val.len().checked_sub(1).ok_or_else(|| {
                            ExecError::internal("IndexJoin last-column template is empty")
                        })?;
                        if range.high_val.len() != range.low_val.len() {
                            return Err(ExecError::internal("IndexJoin template width mismatch"));
                        }
                        for bound in last {
                            range.low_val[pos] = bound.low_val[0].clone();
                            range.high_val[pos] = bound.high_val[0].clone();
                            range.low_exclude = bound.low_exclude;
                            range.high_exclude = bound.high_exclude;
                            range.collators.clone_from(&bound.collators);
                            ranges.push(range.clone());
                        }
                    } else {
                        ranges.push(range);
                    }
                }
            }
        }
        // Go unions row-dependent ranges before issuing the batched reader.
        if self.compare.is_some() || integer_handle {
            ranges = tidb_planner::ranger::ranger::union_ranges(ranges, true).map_err(|error| {
                ExecError::internal(format!("IndexJoin range union: {error:?}"))
            })?;
        }
        let mut readers = CatalogReaderBuilder::new(&self.catalog);
        readers.batch_ranges = Some((
            ranges
                .into_iter()
                .map(|range| IndexRange {
                    low: range.low_val,
                    high: range.high_val,
                    low_exclusive: range.low_exclude,
                    high_exclusive: range.high_exclude,
                })
                .collect(),
            can_reorder_handles,
        ));
        super::PhysicalExecutorBuilder::new(
            &self.context,
            &mut readers,
            self.init_cap,
            self.max_chunk_size,
        )
        .build(&self.plan)
    }
}

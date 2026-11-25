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

//! Go `pkg/planner/core/pb_to_plan.go` (`PBPlanBuilder`): the plan a TiDB
//! node rebuilds from the DAG another TiDB pushed to the `kv.TiDB` store for
//! a cluster memory table -- a `PhysicalMemTable` whose extractor carries the
//! scan's `Desc`, the time range of the request's key ranges and a pushed
//! `Limit`'s row hint, with the Selection directly above it handed to the
//! extractor (`predicatePushDown`).

use tidb_datatype::{FieldName, FieldType, SessionTimeZone};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_proto::{coprocessor, tipb};

use crate::memtable_predicate_extractor::{
    InfoSchemaBaseExtractor, InfoSchemaExtractorKind, MemTableExtractor, SlowQueryExtractor,
    StatementsSummaryExtractor,
};

/// Go `physicalop.PhysicalMemTable` as `PBPlanBuilder.pbToTableScan` builds
/// it for a cluster table.
#[derive(Debug)]
pub struct PbMemTable {
    /// Go `Table.Name.O`, the registered `CLUSTER_*` spelling.
    pub table_name: &'static str,
    /// Go `Columns`: the scanned table columns' offsets, in request order.
    pub columns: Vec<usize>,
    /// Go `buildTableScanSchema`'s columns, one per scanned column.
    pub schema: Schema,
    /// Go `Extractor`.
    pub extractor: Option<MemTableExtractor>,
}

/// One executor of the plan Go's `PBPlanBuilder` builds from a DAG.
pub enum PbPlanNode {
    /// Go `pbToTableScan`.
    MemTable(PbMemTable),
    /// Go `pbToSelection`.
    Selection(Vec<Expression>),
    /// Go `pbToProjection`.
    Projection(Vec<Expression>),
    /// Go `pbToTopN`: `ByItems` and `Count`.
    TopN(Vec<tidb_expression_aggregation::ByItems>, u64),
    /// Go `pbToLimit`: `Count`.
    Limit(u64),
    /// Go `pbToAgg`; the bool is `isStreamAgg`.
    Agg(tipb::Aggregation, bool),
    /// Go `pbToKill`.
    Kill(tipb::Kill),
    /// Go `pbToBroadcastQuery`.
    BroadcastQuery(String),
}

/// A plan node and the plan below it (Go `SetChildren(subPlan)`).
pub struct PbPlan {
    pub node: PbPlanNode,
    pub child: Option<Box<PbPlan>>,
}

impl PbPlan {
    /// The memory table at the bottom of the chain.
    #[must_use]
    pub fn mem_table(&self) -> Option<&PbMemTable> {
        match &self.node {
            PbPlanNode::MemTable(table) => Some(table),
            _ => self.child.as_deref().and_then(PbPlan::mem_table),
        }
    }
}

/// Go `PBPlanBuilder`.
pub struct PBPlanBuilder<'a> {
    /// Go `tps`: the scanned columns' types, which every later executor's
    /// expressions are decoded against.
    tps: Vec<FieldType>,
    ranges: &'a [coprocessor::KeyRange],
    ctx: &'a tidb_sessionctx::StmtContext,
    zone: SessionTimeZone,
    next_unique_id: i64,
}

impl<'a> PBPlanBuilder<'a> {
    /// Go `NewPBPlanBuilder(sctx, is, ranges)`; `ctx` carries the request's
    /// time zone (Go `StmtCtx.TimeZone()`).
    #[must_use]
    pub fn new(ctx: &'a tidb_sessionctx::StmtContext, ranges: &'a [coprocessor::KeyRange]) -> Self {
        Self {
            tps: Vec::new(),
            ranges,
            zone: ctx.session_zone(),
            ctx,
            next_unique_id: 1,
        }
    }

    /// Go `PBPlanBuilder.Build`.
    ///
    /// # Errors
    /// An executor Go does not support, or one it cannot decode.
    pub fn build(&mut self, executors: &[tipb::Executor]) -> Result<PbPlan, String> {
        let mut src: Option<PbPlan> = None;
        for executor in executors {
            src = Some(self.pb_to_physical_plan(executor, src)?);
        }
        let src = src.ok_or_else(|| "TiDB coprocessor request has no executor".to_owned())?;
        let (_, src) = self.predicate_push_down(src, Vec::new());
        Ok(src)
    }

    /// Go `PBPlanBuilder.pbToPhysicalPlan`.
    fn pb_to_physical_plan(
        &mut self,
        e: &tipb::Executor,
        sub_plan: Option<PbPlan>,
    ) -> Result<PbPlan, String> {
        use tipb::ExecType::*;
        let tp = tipb::ExecType::try_from(e.tp.unwrap_or_default()).ok();
        let node = match tp {
            Some(TypeTableScan) => self.pb_to_table_scan(e)?,
            Some(TypeSelection) => self.pb_to_selection(e)?,
            Some(TypeProjection) => self.pb_to_projection(e)?,
            Some(TypeTopN) => self.pb_to_top_n(e)?,
            Some(TypeLimit) => Self::pb_to_limit(e)?,
            Some(TypeAggregation) => Self::pb_to_agg(e, false)?,
            Some(TypeStreamAgg) => Self::pb_to_agg(e, true)?,
            Some(TypeKill) => PbPlanNode::Kill(e.kill.clone().ok_or("missing kill executor")?),
            Some(TypeBroadcastQuery) => PbPlanNode::BroadcastQuery(
                e.broadcast_query
                    .as_ref()
                    .and_then(|broadcast| broadcast.query.clone())
                    .unwrap_or_default(),
            ),
            other => {
                return Err(format!(
                    "this exec type {} doesn't support yet",
                    other.map_or_else(
                        || e.tp.unwrap_or_default().to_string(),
                        |tp| tp.as_str_name().to_owned()
                    )
                ))
            }
        };
        let mut plan = PbPlan {
            node,
            child: sub_plan.map(Box::new),
        };
        // "The limit missed its output cols via the protobuf": a Limit right
        // above the slow-query scan hints the reader with `offset + count`.
        if let (PbPlanNode::Limit(count), Some(child)) = (&plan.node, plan.child.as_deref_mut()) {
            if let PbPlanNode::MemTable(PbMemTable {
                extractor: Some(MemTableExtractor::SlowQuery(extractor)),
                ..
            }) = &mut child.node
            {
                // Go `end := limit.Offset + limit.Count`, saturating; a pushed
                // Limit has no offset.
                extractor.set_row_limit_hint(*count);
            }
        }
        Ok(plan)
    }

    /// Go `PBPlanBuilder.pbToTableScan`.
    fn pb_to_table_scan(&mut self, e: &tipb::Executor) -> Result<PbPlanNode, String> {
        let tbl_scan = e.tbl_scan.as_ref().ok_or("missing table scan")?;
        let table_id = tbl_scan.table_id.unwrap_or_default();
        let name = tidb_infoschema::infoschema_meta::INFORMATION_SCHEMA_TABLE_IDS
            .iter()
            .map(|(_, name)| *name)
            .find(|name| tidb_infoschema::infoschema_meta::memory_table_id(name) == Some(table_id))
            .ok_or_else(|| format!("Table which ID = {table_id} does not exist."))?;
        // "Currently only support cluster table." (Go `IsClusterTable`).
        if !tidb_infoschema::infoschema_meta::is_cluster_table_by_name(name) {
            return Err(format!("table {} is not a cluster table", name.to_ascii_lowercase()));
        }
        let table_name = name;
        let table_columns = tidb_infoschema::infoschema_meta::table_schema(table_name)
            .ok_or("missing cluster table schema")?;
        let columns = self.convert_column_info(table_name, &table_columns, &tbl_scan.columns)?;
        let schema = self.build_table_scan_schema(&columns);
        let extractor = match table_name {
            "CLUSTER_SLOW_QUERY" => {
                let mut extractor = SlowQueryExtractor {
                    desc: tbl_scan.desc.unwrap_or(false),
                    ..SlowQueryExtractor::default()
                };
                extractor.build_time_range_from_key_range(self.ranges)?;
                Some(MemTableExtractor::SlowQuery(extractor))
            }
            "CLUSTER_STATEMENTS_SUMMARY" | "CLUSTER_STATEMENTS_SUMMARY_HISTORY" => Some(
                MemTableExtractor::StatementsSummary(StatementsSummaryExtractor::default()),
            ),
            "CLUSTER_TIDB_INDEX_USAGE" => Some(MemTableExtractor::InfoSchema(Box::new(
                InfoSchemaBaseExtractor::new(InfoSchemaExtractorKind::TiDBIndexUsage),
            ))),
            _ => None,
        };
        Ok(PbPlanNode::MemTable(PbMemTable {
            table_name,
            columns,
            schema,
            extractor,
        }))
    }

    /// Go `PBPlanBuilder.convertColumnInfo`: the table columns the request
    /// names, which become `tps`.
    fn convert_column_info(
        &mut self,
        table_name: &str,
        table_columns: &[(String, FieldType)],
        pb_columns: &[tipb::ColumnInfo],
    ) -> Result<Vec<usize>, String> {
        let mut columns = Vec::with_capacity(pb_columns.len());
        let mut tps = Vec::with_capacity(pb_columns.len());
        for column in pb_columns {
            let id = column.column_id.unwrap_or_default();
            // Memory-table column ids are offset + 1.
            let offset = usize::try_from(id - 1)
                .ok()
                .filter(|offset| *offset < table_columns.len())
                .ok_or_else(|| {
                    format!(
                        "Column ID {id} of table {} not found",
                        table_name.to_ascii_lowercase()
                    )
                })?;
            columns.push(offset);
            tps.push(table_columns[offset].1.clone());
        }
        self.tps = tps;
        Ok(columns)
    }

    /// Go `PBPlanBuilder.buildTableScanSchema`: one fresh column per scanned
    /// column, typed as the table declares it.
    fn build_table_scan_schema(&mut self, columns: &[usize]) -> Schema {
        Schema::new(
            columns
                .iter()
                .zip(&self.tps)
                .map(|(offset, field_type)| {
                    let mut column =
                        tidb_expr::column::Column::new(self.next_unique_id, field_type.clone());
                    self.next_unique_id += 1;
                    column.id = *offset as i64 + 1;
                    column
                })
                .collect(),
        )
    }

    fn pb_to_exprs(&self, exprs: &[tipb::Expr]) -> Result<Vec<Expression>, String> {
        exprs
            .iter()
            .map(|expr| {
                validate_pb_expr(expr, self.tps.len())?;
                tidb_expr::distsql_builtin::pb_to_expr_in(expr, &self.tps, &self.zone)
            })
            .collect()
    }

    /// Go `PBPlanBuilder.pbToProjection`.
    fn pb_to_projection(&self, e: &tipb::Executor) -> Result<PbPlanNode, String> {
        let projection = e.projection.as_ref().ok_or("missing projection")?;
        Ok(PbPlanNode::Projection(self.pb_to_exprs(&projection.exprs)?))
    }

    /// Go `PBPlanBuilder.pbToSelection`.
    fn pb_to_selection(&self, e: &tipb::Executor) -> Result<PbPlanNode, String> {
        let selection = e.selection.as_ref().ok_or("missing selection")?;
        Ok(PbPlanNode::Selection(self.pb_to_exprs(&selection.conditions)?))
    }

    /// Go `PBPlanBuilder.pbToTopN`.
    fn pb_to_top_n(&self, e: &tipb::Executor) -> Result<PbPlanNode, String> {
        let top_n = e.top_n.as_ref().ok_or("missing TopN")?;
        let by_items = top_n
            .order_by
            .iter()
            .map(|item| {
                let expr = item.expr.as_ref().ok_or("missing TopN order item")?;
                Ok(tidb_expression_aggregation::ByItems {
                    expr: self.pb_to_exprs(std::slice::from_ref(expr))?.remove(0),
                    desc: item.desc.unwrap_or(false),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(PbPlanNode::TopN(by_items, top_n.limit.unwrap_or(0)))
    }

    /// Go `PBPlanBuilder.pbToLimit`.
    fn pb_to_limit(e: &tipb::Executor) -> Result<PbPlanNode, String> {
        let limit = e.limit.as_ref().ok_or("missing limit")?;
        Ok(PbPlanNode::Limit(limit.limit.unwrap_or(0)))
    }

    /// Go `PBPlanBuilder.pbToAgg`.
    fn pb_to_agg(e: &tipb::Executor, is_stream_agg: bool) -> Result<PbPlanNode, String> {
        let aggregation = e.aggregation.as_deref().ok_or("missing aggregation")?;
        Ok(PbPlanNode::Agg(aggregation.clone(), is_stream_agg))
    }

    /// Go `PBPlanBuilder.predicatePushDown`: a Selection directly above the
    /// memory table hands its conditions to the table's extractor and keeps
    /// only what the extractor leaves; every other executor passes nothing
    /// down.
    fn predicate_push_down(
        &mut self,
        mut plan: PbPlan,
        predicates: Vec<Expression>,
    ) -> (Vec<Expression>, PbPlan) {
        match plan.node {
            PbPlanNode::MemTable(ref mut mem_table) => {
                let Some(extractor) = mem_table.extractor.as_mut() else {
                    return (predicates, plan);
                };
                let names: Vec<FieldName> = mem_table
                    .columns
                    .iter()
                    .map(|offset| {
                        let table_columns =
                            tidb_infoschema::infoschema_meta::table_schema(mem_table.table_name)
                                .expect("registered cluster table");
                        let table = tidb_datatype::IdentifierMetadata::new(mem_table.table_name);
                        let column = tidb_datatype::IdentifierMetadata::new(
                            table_columns[*offset].0.as_str(),
                        );
                        FieldName::new(tidb_datatype::FieldNameMetadata {
                            original_table: table.clone(),
                            original_column: column.clone(),
                            table,
                            column,
                            ..Default::default()
                        })
                    })
                    .collect();
                // "Set the expression column unique ID. Since the expression
                // is build from PB, It has not set the expression column ID
                // yet."
                let mut predicates = predicates;
                for predicate in &mut predicates {
                    set_column_unique_ids(predicate, &mem_table.schema);
                }
                let predicates = extractor.extract(self.ctx, &mem_table.schema, &names, predicates);
                (predicates, plan)
            }
            PbPlanNode::Selection(conditions) => {
                let Some(child) = plan.child.take() else {
                    plan.node = PbPlanNode::Selection(conditions);
                    return (predicates, plan);
                };
                let child = *child;
                let (conditions, child) = self.predicate_push_down(child, conditions);
                if !conditions.is_empty() {
                    return (
                        predicates,
                        PbPlan {
                            node: PbPlanNode::Selection(conditions),
                            child: Some(Box::new(child)),
                        },
                    );
                }
                (predicates, child)
            }
            _ => {
                if let Some(child) = plan.child.take() {
                    let (_, child) = self.predicate_push_down(*child, Vec::new());
                    plan.child = Some(Box::new(child));
                }
                (predicates, plan)
            }
        }
    }
}

/// Points every column of `expr` at the schema column of its index.
fn set_column_unique_ids(expr: &mut Expression, schema: &Schema) {
    match expr {
        Expression::Column(column) => {
            if let Some(schema_column) = usize::try_from(column.index)
                .ok()
                .and_then(|index| schema.columns.get(index))
            {
                column.unique_id = schema_column.unique_id;
            }
        }
        Expression::ScalarFunction(function) => {
            for arg in &mut function.args {
                set_column_unique_ids(arg, schema);
            }
        }
        _ => {}
    }
}

/// Rejects column references past the input row, which the expression
/// decoder would otherwise index unchecked.
///
/// # Errors
/// A column reference that does not decode or is past `width`.
pub fn validate_pb_expr(expr: &tipb::Expr, width: usize) -> Result<(), String> {
    if expr.tp == Some(tipb::ExprType::ColumnRef as i32) {
        let (remaining, offset) =
            tidb_codec::decode_int(expr.val()).map_err(|error| error.to_string())?;
        if !remaining.is_empty()
            || usize::try_from(offset)
                .ok()
                .is_none_or(|offset| offset >= width)
        {
            return Err("peer predicate column offset out of range".into());
        }
    }
    for child in &expr.children {
        validate_pb_expr(child, width)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
            .collect()
    }

    /// `CLUSTER_SLOW_QUERY`'s record prefix `t{id}_r`.
    const PREFIX: &str = "74c00000000000002f5f72";

    fn key_range(start: &str, end: &str) -> coprocessor::KeyRange {
        coprocessor::KeyRange {
            start: hex(&format!("{PREFIX}{start}")).into(),
            end: hex(&format!("{PREFIX}{end}")).into(),
        }
    }

    fn scan(columns: &[i64], desc: bool) -> tipb::Executor {
        tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: tidb_infoschema::infoschema_meta::memory_table_id("CLUSTER_SLOW_QUERY"),
                columns: columns
                    .iter()
                    .map(|id| tipb::ColumnInfo {
                        column_id: Some(*id),
                        ..Default::default()
                    })
                    .collect(),
                desc: Some(desc),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn limit(count: u64) -> tipb::Executor {
        tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            limit: Some(Box::new(tipb::Limit { limit: Some(count), ..Default::default() })),
            executor_id: Some(String::new()),
            ..Default::default()
        }
    }

    /// Go's `gt(query_time, 1)` over scan columns `[time, query_time]`.
    fn query_time_gt_one() -> tipb::Executor {
        let real = tipb::FieldType {
            tp: Some(5),
            flen: Some(22),
            collate: Some(-63),
            charset: Some("binary".into()),
            ..Default::default()
        };
        tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            selection: Some(Box::new(tipb::Selection {
                conditions: vec![tipb::Expr {
                    tp: Some(tipb::ExprType::ScalarFunc as i32),
                    sig: Some(tipb::ScalarFuncSig::GtReal as i32),
                    children: vec![
                        tipb::Expr {
                            tp: Some(tipb::ExprType::ColumnRef as i32),
                            val: Some(vec![0x80, 0, 0, 0, 0, 0, 0, 1]),
                            field_type: Some(real.clone()),
                            ..Default::default()
                        },
                        tipb::Expr {
                            tp: Some(tipb::ExprType::Float64 as i32),
                            val: Some(vec![0xbf, 0xf0, 0, 0, 0, 0, 0, 0]),
                            field_type: Some(real),
                            ..Default::default()
                        },
                    ],
                    field_type: Some(tipb::FieldType {
                        tp: Some(8),
                        flag: Some(524416),
                        flen: Some(1),
                        collate: Some(-63),
                        charset: Some("binary".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    /// `(skip, enable, desc, limit, time ranges as Unix nanoseconds)`; the
    /// unbounded end `MaxDatetime` is `None` (Go's `UnixNano` overflows it).
    type Extracted = (bool, bool, bool, u64, Vec<(i64, Option<i64>)>);

    fn extracted(executors: &[tipb::Executor], ranges: &[coprocessor::KeyRange]) -> Extracted {
        let ctx = tidb_sessionctx::StmtContext::for_query().with_time_zone(SessionTimeZone::utc());
        let plan = PBPlanBuilder::new(&ctx, ranges).build(executors).unwrap();
        let Some(PbMemTable {
            extractor: Some(MemTableExtractor::SlowQuery(extractor)),
            ..
        }) = plan.mem_table()
        else {
            panic!("CLUSTER_SLOW_QUERY carries Go's SlowQueryExtractor");
        };
        let max = chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
            .and_then(|date| date.and_hms_opt(23, 59, 59))
            .unwrap()
            .and_utc();
        (
            extractor.skip_request,
            extractor.enable,
            extractor.desc,
            extractor.limit,
            extractor
                .time_ranges
                .iter()
                .map(|range| {
                    (
                        range.start_time.timestamp_nanos_opt().unwrap(),
                        (range.end_time != max).then(|| range.end_time.timestamp_nanos_opt().unwrap()),
                    )
                })
                .collect(),
        )
    }

    /// Go `PBPlanBuilder.pbToTableScan` / `pbToPhysicalPlan` over the DAGs
    /// Go's planner pushes for CLUSTER_SLOW_QUERY: the scan's `Desc`, the
    /// time ranges `buildTimeRangeFromKeyRange` decodes from the request's
    /// key ranges, and the row-limit hint of a Limit right above the scan.
    ///
    /// Captured from Go: a throwaway `pkg/testkit` probe (`zzprobe_i07`,
    /// session `time_zone='+00:00'`) hijacked the mock store's client,
    /// recorded each `kv.TiDB` coprocessor request of the queries below and
    /// printed the `SlowQueryExtractor` of
    /// `core.NewPBPlanBuilder(sctx, is, req.Ranges).Build(dag.Executors)`.
    #[test]
    fn pb_plan_builder_builds_go_slow_query_extractor() {
        // time > '2020-01-01 00:00:00' and time < '2020-01-02 00:00:00'
        assert_eq!(
            extracted(
                &[scan(&[2, 99], false)],
                &[key_range("0419a5420000000001", "0419a5440000000000")],
            ),
            (false, true, false, 0, vec![(1577836800000001000, Some(1577923200000000000))]),
        );
        // order by time desc limit 3
        assert_eq!(
            extracted(&[scan(&[2, 99], true), limit(3)], &[key_range("01", "fb")]),
            (false, false, true, 3, vec![]),
        );
        // time between '2020-01-01 00:00:00' and '2020-01-04 00:00:00' order
        // by time desc limit 2
        assert_eq!(
            extracted(
                &[scan(&[2, 99], true), limit(2)],
                &[key_range("0419a5420000000000", "0419a5480000000001")],
            ),
            (false, true, true, 2, vec![(1577836800000000000, Some(1578096000000001000))]),
        );
        // time in ('2020-01-01 10:00:00', '2020-01-03 10:00:00') or
        // time > '2021-01-01'
        assert_eq!(
            extracted(
                &[scan(&[2], false)],
                &[
                    key_range("0419a542a000000000", "0419a542a000000001"),
                    key_range("0419a546a000000000", "0419a546a000000001"),
                    key_range("0419a8820000000001", "fb"),
                ],
            ),
            (
                false,
                true,
                false,
                0,
                vec![
                    (1577872800000000000, Some(1577872800000001000)),
                    (1578045600000000000, Some(1578045600000001000)),
                    (1609459200000001000, None),
                ]
            ),
        );
        // time > '2020-01-01' and query_time > 1 order by time desc limit 1:
        // the Limit sits above the Selection, so no hint; `query_time` is no
        // time predicate, so the Selection stays.
        let executors = [scan(&[2, 10], true), query_time_gt_one(), limit(1)];
        let ranges = [key_range("0419a5420000000001", "fb")];
        assert_eq!(
            extracted(&executors, &ranges),
            (false, true, true, 0, vec![(1577836800000001000, None)]),
        );
        let ctx = tidb_sessionctx::StmtContext::for_query().with_time_zone(SessionTimeZone::utc());
        let plan = PBPlanBuilder::new(&ctx, &ranges).build(&executors).unwrap();
        assert!(matches!(plan.node, PbPlanNode::Limit(1)));
        assert!(matches!(
            plan.child.as_deref().map(|child| &child.node),
            Some(PbPlanNode::Selection(conditions)) if conditions.len() == 1
        ));
    }
}

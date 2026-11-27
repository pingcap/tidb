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

//! Go `pkg/executor/coprocessor.go` (`CoprocessorDAGHandler`): a TiDB node
//! answers the DAG another TiDB (or itself) pushed to the `kv.TiDB` store for
//! a cluster memory table, running the plan
//! [`tidb_planner::pb_to_plan::PBPlanBuilder`] rebuilds by the ordinary
//! executor owners over the memory table's rows.

use std::collections::HashSet;

use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_expr::schema::Schema;
pub use tidb_planner::pb_to_plan::{PBPlanBuilder, PbMemTable, PbPlan, PbPlanNode};
use tidb_planner::pb_to_plan::validate_pb_expr;
use tidb_proto::tipb;

/// Go `kv.ReqTypeDAG`.
pub const REQ_TYPE_DAG: i64 = 103;

/// Go `FieldTypeFromPB` of an expression's declared result type.
fn pb_field_type(expr: &tipb::Expr) -> FieldType {
    tidb_expr::distsql_builtin::pb_type_to_field_type(&expr.field_type.clone().unwrap_or_default())
}

/// The executor schema of a row of `fields`, columns numbered by offset.
fn pb_meta(fields: &[FieldType]) -> tidb_executor_internal_exec::ExecutorMeta {
    let schema = Schema::new(
        fields
            .iter()
            .enumerate()
            .map(|(index, field)| tidb_expr::column::Column::new(index as i64, field.clone()))
            .collect(),
    );
    tidb_executor_internal_exec::ExecutorMeta::new(schema, 0, 32, 1024)
}

/// Opens, drains and closes `executor`, whose rows have `fields`. Go
/// master `HandleRequest` (516070a35ae) closes the executor on every exit:
/// "Open can partially initialize the executor before returning an error".
/// A close error is the answer only when nothing failed before it; otherwise
/// it is logged (`terror.Log`).
fn drain(
    mut executor: Box<dyn tidb_executor_internal_exec::Executor>,
    fields: &[FieldType],
) -> Result<Vec<Vec<Datum>>, String> {
    if let Err(error) = executor.open() {
        log_close_error(executor.as_mut());
        return Err(tidb_driver_error::DriverError::from(error).to_string());
    }
    let result = (|| -> Result<Vec<Vec<Datum>>, String> {
        let mut output = Vec::new();
        let mut chunk = tidb_chunk::chunk::Chunk::new(fields, 32, 1024);
        loop {
            executor
                .next(&mut chunk)
                .map_err(|error| tidb_driver_error::DriverError::from(error).to_string())?;
            if chunk.num_rows() == 0 {
                break;
            }
            for index in 0..chunk.num_rows() {
                output.push(chunk.get_row(index).get_datum_row(fields));
            }
        }
        Ok(output)
    })();
    let rows = match result {
        Ok(rows) => rows,
        Err(error) => {
            log_close_error(executor.as_mut());
            return Err(error);
        }
    };
    executor
        .close()
        .map_err(|error| tidb_driver_error::DriverError::from(error).to_string())?;
    Ok(rows)
}

/// Go `terror.Log(exec.Close(e))`.
fn log_close_error(executor: &mut dyn tidb_executor_internal_exec::Executor) {
    if let Err(error) = executor.close() {
        let error = tidb_driver_error::DriverError::from(error).to_mysql_error();
        tidb_terror::log(Some(&error));
    }
}

/// The rows of a memory table a [`PbPlan`] reads: one value per column of
/// [`PbMemTable::columns`], in that order.
pub trait PbMemTableSource {
    /// Go `MemTableReaderExec`'s retriever for `table`.
    ///
    /// # Errors
    /// The table's reader fails.
    fn mem_table_rows(&self, table: &PbMemTable) -> Result<Vec<Vec<Datum>>, String>;
}

/// Go `executorBuilder.build` over a [`PbPlan`], then `Open`/`Next`: the
/// rows of the plan's root and their types.
///
/// # Errors
/// An executor fails, or the plan is one this handler does not run.
pub fn execute_pb_plan(
    plan: &PbPlan,
    source: &dyn PbMemTableSource,
    ctx: &tidb_sessionctx::StmtContext,
    tps: &[FieldType],
) -> Result<(Vec<Vec<Datum>>, Vec<FieldType>), String> {
    let child = || -> Result<(Vec<Vec<Datum>>, Vec<FieldType>), String> {
        execute_pb_plan(
            plan.child.as_deref().ok_or("a coprocessor executor has no child")?,
            source,
            ctx,
            tps,
        )
    };
    match &plan.node {
        PbPlanNode::MemTable(table) => {
            let rows = source.mem_table_rows(table)?;
            Ok((rows, tps.to_vec()))
        }
        PbPlanNode::Selection(conditions) => {
            let (rows, fields) = child()?;
            let meta = pb_meta(&fields);
            let memory = ctx.statement_memory().clone();
            let source = crate::mem_table::MemTableSourceExec::new(meta.clone(), rows);
            let selection = crate::selection::SelectionExec::new(
                meta,
                conditions.clone(),
                Box::new(source),
                ctx.clone(),
                memory,
            );
            Ok((drain(Box::new(selection), &fields)?, fields))
        }
        PbPlanNode::Projection(exprs) => {
            let (rows, fields) = child()?;
            let output: Vec<_> = exprs
                .iter()
                .map(|expr| {
                    expr.static_type()
                        .cloned()
                        .unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::Null))
                })
                .collect();
            let source = crate::mem_table::MemTableSourceExec::new(pb_meta(&fields), rows);
            let exec = crate::projection::ProjectionExec::new(
                pb_meta(&output),
                exprs.clone(),
                Box::new(source),
                ctx.clone(),
            );
            Ok((drain(Box::new(exec), &output)?, output))
        }
        PbPlanNode::TopN(by_items, count) => {
            let (rows, fields) = child()?;
            let memory = ctx.statement_memory().clone();
            let meta = pb_meta(&fields);
            let source = crate::mem_table::MemTableSourceExec::new(meta.clone(), rows);
            let exec = tidb_executor_sortexec::topn::TopNExec::new(
                meta,
                by_items.clone(),
                Box::new(source),
                ctx.clone(),
                0,
                *count,
                memory,
            );
            Ok((drain(Box::new(exec), &fields)?, fields))
        }
        PbPlanNode::Limit(count) => {
            let (rows, fields) = child()?;
            let meta = pb_meta(&fields);
            let source = crate::mem_table::MemTableSourceExec::new(meta.clone(), rows);
            let exec = crate::limit::LimitExec::new(meta, 0, *count, Box::new(source));
            Ok((drain(Box::new(exec), &fields)?, fields))
        }
        PbPlanNode::Agg(aggregation, streamed) => {
            let (rows, _) = child()?;
            pb_aggregate(rows, tps, aggregation, *streamed, ctx)
        }
        PbPlanNode::Kill(_) | PbPlanNode::BroadcastQuery(_) => {
            Err("unsupported TiDB peer executor chain".into())
        }
    }
}

/// Go's TiDB-side partial aggregation (`PBPlanBuilder.pbToAgg`, built as a
/// hash or stream aggregate): groups by the decoded group-by expressions
/// with Go's `HashGroupKey` and emits only the aggregate functions' partial
/// results -- the TiDB executor does not output group-by values, which is
/// why the planner adds `firstrow` for them. Without GROUP BY an empty input
/// still answers one row of initial partial results.
#[allow(clippy::type_complexity)]
fn pb_aggregate(
    rows: Vec<Vec<Datum>>,
    fields: &[FieldType],
    aggregation: &tipb::Aggregation,
    streamed: bool,
    ctx: &tidb_sessionctx::StmtContext,
) -> Result<(Vec<Vec<Datum>>, Vec<FieldType>), String> {
    let zone = ctx.session_zone();
    let group_by = aggregation
        .group_by
        .iter()
        .map(|expr| {
            validate_pb_expr(expr, fields.len())?;
            tidb_expr::distsql_builtin::pb_to_expr_in(expr, fields, &zone)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let group_types: Vec<_> = group_by
        .iter()
        .zip(&aggregation.group_by)
        .map(|(expr, pb)| {
            expr.static_type()
                .cloned()
                .unwrap_or_else(|| pb_field_type(pb))
        })
        .collect();
    let mut functions = Vec::with_capacity(aggregation.agg_func.len());
    let mut output = Vec::new();
    for func in &aggregation.agg_func {
        validate_pb_expr(func, fields.len())?;
        functions.push(tidb_expression_aggregation::DistAggregate::from_pb(
            func, fields, &zone,
        )?);
        let ret = pb_field_type(func);
        if func.tp == Some(tipb::ExprType::Avg as i32) {
            // A partial AVG is its count, then its sum.
            output.push(FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
        }
        output.push(ret);
    }
    let mut row = tidb_chunk::mutrow::MutRow::mut_row_from_types(fields);
    let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
    let mut groups: Vec<(Vec<u8>, Vec<tidb_expression_aggregation::DistAggregateState>)> =
        Vec::new();
    let new_states = || -> Vec<_> { functions.iter().map(|f| f.create_state()).collect() };
    for values in &rows {
        if values.len() != fields.len() {
            return Err("aggregate row does not match input schema".into());
        }
        row.set_datums(values);
        let mut key = Vec::new();
        for (expr, ft) in group_by.iter().zip(&group_types) {
            let value = expr.eval(ctx, row.to_row()).map_err(|error| {
                tidb_driver_error::DriverError::from(tidb_driver_error::ExecError::Eval(error))
                    .to_string()
            })?;
            tidb_codec::append_hash_group_key_in_timezone(&chrono::Utc, &value, ft, &mut key)
                .map_err(|error| format!("{error:?}"))?;
        }
        let at = if streamed {
            // Go `StreamAggExec`: a new group starts when the key changes.
            match groups.last() {
                Some((last, _)) if *last == key => groups.len() - 1,
                _ => {
                    groups.push((key, new_states()));
                    groups.len() - 1
                }
            }
        } else {
            match index.get(&key) {
                Some(at) => *at,
                None => {
                    index.insert(key.clone(), groups.len());
                    groups.push((key, new_states()));
                    groups.len() - 1
                }
            }
        };
        for (function, state) in functions.iter().zip(groups[at].1.iter_mut()) {
            function
                .update(state, ctx, row.to_row())
                .map_err(|error| error.to_string())?;
        }
    }
    if groups.is_empty() && group_by.is_empty() {
        groups.push((Vec::new(), new_states()));
    }
    let rows = groups
        .into_iter()
        .map(|(_, states)| {
            functions
                .iter()
                .zip(states)
                .flat_map(|(function, state)| function.partial_result(state))
                .collect()
        })
        .collect();
    Ok((rows, output))
}

/// Go `CoprocessorDAGHandler.buildChunk` (`encodeDefault`, 64 rows a chunk,
/// or `encodeChunk`) and `buildUnaryResponse`: the rows' `OutputOffsets`
/// columns. Go's unary response carries no warnings.
///
/// # Errors
/// An unknown encode type, or an output offset past the rows.
pub fn build_unary_response(
    dag: &tipb::DagRequest,
    rows: &[Vec<Datum>],
    tps: &[FieldType],
    zone: &SessionTimeZone,
) -> Result<tipb::SelectResponse, String> {
    let projection: Vec<_> = dag
        .output_offsets
        .iter()
        .map(|offset| *offset as usize)
        .collect();
    let fields = projection
        .iter()
        .map(|index| {
            tps.get(*index)
                .cloned()
                .ok_or("invalid cluster table output offset")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let encoding = tipb::EncodeType::try_from(dag.encode_type.unwrap_or(0))
        .map_err(|_| format!("unknown DAG encode type: {}", dag.encode_type.unwrap_or(0)))?;
    let mut result = tipb::SelectResponse {
        encode_type: Some(encoding as i32),
        ..Default::default()
    };
    let batch_size = if encoding == tipb::EncodeType::TypeDefault {
        64
    } else {
        1024
    };
    for rows in rows.chunks(batch_size) {
        let data = match encoding {
            tipb::EncodeType::TypeDefault => {
                let mut data = Vec::new();
                for row in rows {
                    let values: Vec<_> = projection.iter().map(|i| row[*i].clone()).collect();
                    data.extend(
                        tidb_codec::encode_value_in_timezone(zone, &values)
                            .map_err(|e| e.to_string())?,
                    );
                }
                data
            }
            tipb::EncodeType::TypeChunk => {
                let mut chunk = tidb_chunk::chunk::Chunk::new(&fields, rows.len(), 1024);
                for row in rows {
                    for (col, index) in projection.iter().enumerate() {
                        chunk.append_datum(col, &row[*index]);
                    }
                }
                tidb_chunk::codec::Codec::new(fields.clone()).encode(&chunk)
            }
            _ => return Err(format!("unknown DAG encode type: {encoding:?}")),
        };
        result.chunks.push(tipb::Chunk {
            rows_data: Some(data.into()),
            ..Default::default()
        });
    }
    if dag.collect_execution_summaries.unwrap_or(false) {
        // "TODO: Add real executor execution summary information."
        result.execution_summaries =
            vec![tipb::ExecutorExecutionSummary::default(); dag.executors.len()];
    }
    Ok(result)
}

/// Go `CoprocessorDAGHandler.HandleRequest` after `buildDAGExecutor` for a
/// memory-table plan: run it and encode the unary response.
///
/// # Errors
/// The plan fails or its rows cannot be encoded.
pub fn handle_pb_plan(
    dag: &tipb::DagRequest,
    plan: &PbPlan,
    source: &dyn PbMemTableSource,
    ctx: &tidb_sessionctx::StmtContext,
    tps: &[FieldType],
) -> Result<tipb::SelectResponse, String> {
    let (rows, fields) = execute_pb_plan(plan, source, ctx, tps)?;
    build_unary_response(dag, &rows, &fields, &ctx.session_zone())
}

/// The scanned columns' types of a memory-table plan (Go `b.tps`).
#[must_use]
pub fn pb_plan_tps(plan: &PbPlan) -> Vec<FieldType> {
    plan.mem_table()
        .map(|table| {
            let columns = tidb_infoschema::infoschema_meta::table_schema(table.table_name)
                .expect("registered cluster table");
            table
                .columns
                .iter()
                .map(|offset| columns[*offset].1.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// The upper-cased names of a memory table's scanned columns.
#[must_use]
pub fn required_columns(table: &PbMemTable) -> HashSet<String> {
    let columns = tidb_infoschema::infoschema_meta::table_schema(table.table_name)
        .expect("registered cluster table");
    table
        .columns
        .iter()
        .map(|offset| columns[*offset].0.clone())
        .collect()
}

/// Go `CoprocessorDAGHandler` over its session (`h.sctx`).
pub trait CoprocessorDagHandler: crate::show::exec::ShowExec + crate::compiler::CompilerSession {
    /// Go `CoprocessorDAGHandler.HandleRequest` for a cluster memory-table
    /// `plan` (`PBPlanBuilder.Build` of `dag`): the request's zone is the
    /// statement's (Go `InitFromPBFlagAndTz`), and its rows come from this
    /// node's tables.
    ///
    /// # Errors
    ///
    /// The plan or its table reader fails.
    fn handle_cop_plan(
        &mut self,
        dag: &tidb_proto::tipb::DagRequest,
        plan: &PbPlan,
        registry: Option<&tidb_sessmgr::ProcessRegistry>,
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<tidb_proto::tipb::SelectResponse, String> {
        let ctx = crate::stmt_ctx_build::ResetContextOfStmt::statement_context(self, false).with_time_zone(zone.clone());
        let viewer = crate::show::exec::ShowExec::viewer_state(self);
        let response = crate::compiler::CompilerSession::with_staged_catalog(self, |catalog| {
            Ok(viewer.handle_cop_plan(&ctx, catalog, registry, dag, plan))
        });
        crate::compiler::CompilerSession::drain_eval_warnings(self, &ctx);
        response.map_err(|error: crate::DriverError| error.to_string())?
    }
}

impl<T: crate::show::exec::ShowExec + crate::compiler::CompilerSession> CoprocessorDagHandler for T {}

#[cfg(test)]
mod close_on_every_exit_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_driver_error::ExecError;
    use tidb_executor_internal_exec::{Executor, ExecutorMeta};

    /// Go `TestCoprocessorDAGHandlerClosesExecutor`'s mock executor
    /// (`mockCoprocessorDAGExecutor`): fails where told and counts its
    /// `Close` calls.
    struct FailingExec {
        meta: ExecutorMeta,
        fail_open: bool,
        fail_next: bool,
        fail_close: bool,
        closes: Arc<AtomicUsize>,
    }

    impl Executor for FailingExec {
        fn open(&mut self) -> Result<(), ExecError> {
            if self.fail_open { Err(ExecError::internal("open failed")) } else { Ok(()) }
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if self.fail_next { Err(ExecError::internal("next failed")) } else { Ok(()) }
        }
        fn close(&mut self) -> Result<(), ExecError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close { Err(ExecError::internal("close failed")) } else { Ok(()) }
        }
        fn schema(&self) -> &tidb_expr::schema::Schema {
            self.meta.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.meta.ret_field_types()
        }
        fn init_cap(&self) -> usize {
            self.meta.init_cap()
        }
        fn max_chunk_size(&self) -> usize {
            self.meta.max_chunk_size()
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    fn run(fail_open: bool, fail_next: bool, fail_close: bool) -> (Result<usize, String>, usize) {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let closes = Arc::new(AtomicUsize::new(0));
        let executor = FailingExec {
            meta: super::pb_meta(&fields),
            fail_open,
            fail_next,
            fail_close,
            closes: Arc::clone(&closes),
        };
        let result = super::drain(Box::new(executor), &fields).map(|rows| rows.len());
        (result, closes.load(Ordering::SeqCst))
    }

    /// Go master 516070a35ae: the executor is closed after an Open error,
    /// after a Next error, and after success; a Close error is the answer
    /// only when nothing failed before it.
    #[test]
    fn test_coprocessor_dag_handler_closes_executor() {
        let (result, closes) = run(true, false, false);
        assert!(result.unwrap_err().contains("open failed"));
        assert_eq!(closes, 1);
        let (result, closes) = run(true, false, true);
        assert!(result.unwrap_err().contains("open failed"));
        assert_eq!(closes, 1);
        let (result, closes) = run(false, true, true);
        assert!(result.unwrap_err().contains("next failed"));
        assert_eq!(closes, 1);
        let (result, closes) = run(false, false, true);
        assert!(result.unwrap_err().contains("close failed"));
        assert_eq!(closes, 1);
        let (result, closes) = run(false, false, false);
        assert_eq!(result, Ok(0));
        assert_eq!(closes, 1);
    }
}

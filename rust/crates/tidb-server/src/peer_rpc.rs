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

//! Go's status-port TiKV service for local KILL and shared cluster-table readers.
use prost::Message;
use std::{pin::Pin, sync::Arc};
use tidb_datatype::SessionTimeZone;
use tidb_proto::{coprocessor, tikvpb, tipb};
use tidb_session::{privilege::PrivilegeRegistry, Session};
use tidb_executor::process::ProcessRegistry;

type ReplyStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, tonic::Status>> + Send>>;

/// Runs one statement on this instance alone, as Go's remote-origin plans do.
pub(crate) type BroadcastQueryRunner = Arc<dyn Fn(&str) -> Result<(), String> + Send + Sync>;

/// Borrows the node's process and privilege owners; never redispatches to peers.
#[derive(Clone)]
pub struct PeerService {
    session_factory: Arc<dyn Fn() -> Result<Session, String> + Send + Sync>,
    processes: ProcessRegistry,
    privileges: PrivilegeRegistry,
    cluster_peer: Option<Arc<tidb_executor::cluster_peer::ClusterPeerClient>>,
    server_info: Option<Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    broadcast_query: Option<BroadcastQueryRunner>,
}

impl PeerService {
    /// `session_factory` opens a session on the node's store for each
    /// request, as Go's `rpcServer.createSession` does.
    pub(crate) fn new(
        session_factory: Arc<dyn Fn() -> Result<Session, String> + Send + Sync>,
        processes: ProcessRegistry,
        privileges: PrivilegeRegistry,
        server_info: Option<Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    ) -> Self {
        Self {
            session_factory,
            processes,
            privileges,
            cluster_peer: None,
            server_info,
            broadcast_query: None,
        }
    }

    /// Supplies the internal SQL session that runs a received broadcast
    /// statement on this instance (Go `IsFromRemote` plans).
    pub(crate) fn with_broadcast_query_runner(mut self, runner: BroadcastQueryRunner) -> Self {
        self.broadcast_query = Some(runner);
        self
    }

    /// Borrows the existing outgoing fleet independently of metadata projection.
    pub(crate) fn with_cluster_peer_client(
        mut self,
        peer: Option<Arc<tidb_executor::cluster_peer::ClusterPeerClient>>,
    ) -> Self {
        self.cluster_peer = peer;
        self
    }

    fn handle(&self, request: coprocessor::Request) -> coprocessor::Response {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.execute(request))) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => error_response(error),
            Err(_) => error_response("panic while executing TiDB coprocessor request".into()),
        }
    }

    fn execute(&self, request: coprocessor::Request) -> Result<coprocessor::Response, String> {
        if request.tp != 103 {
            return Err(format!("unsupported request type {}", request.tp));
        }
        let dag = tipb::DagRequest::decode(request.data.as_slice()).map_err(|e| e.to_string())?;
        let (executor, tail) = dag
            .executors
            .split_first()
            .ok_or("TiDB peer requires a local executor")?;
        // Go `PBPlanBuilder.Build`: every executor must be one it supports.
        for item in &dag.executors {
            use tipb::ExecType::*;
            match item.tp.and_then(|tp| tipb::ExecType::try_from(tp).ok()) {
                Some(
                    TypeTableScan | TypeSelection | TypeProjection | TypeTopN | TypeLimit
                    | TypeAggregation | TypeStreamAgg | TypeKill | TypeBroadcastQuery,
                ) => {}
                other => {
                    return Err(format!(
                        "this exec type {} doesn't support yet",
                        other.map_or_else(
                            || item.tp.unwrap_or_default().to_string(),
                            |tp| tp.as_str_name().to_owned()
                        )
                    ))
                }
            }
        }
        if executor.tp != Some(tipb::ExecType::TypeTableScan as i32) && !tail.is_empty() {
            return Err("unsupported TiDB peer executor chain".into());
        }
        // Go `predicatePushDown`: the selections directly above the scan
        // reach the reader; the rest of the chain runs over its rows.
        let leading = tail
            .iter()
            .take_while(|item| item.tp == Some(tipb::ExecType::TypeSelection as i32))
            .count();
        let (selections, rest) = tail.split_at(leading);
        let mut conditions = Vec::new();
        for selection in selections {
            conditions.extend(
                selection
                    .selection
                    .as_ref()
                    .ok_or("missing peer selection")?
                    .conditions
                    .iter()
                    .cloned(),
            );
        }
        // Go ConstructTimeZone prefers the name to the offset, including System.
        let zone = match tidb_util::timeutil::construct_time_zone(
            dag.time_zone_name.as_deref().unwrap_or(""),
            dag.time_zone_offset.unwrap_or(0) as i32,
        )
        .map_err(|e| e.to_string())?
        {
            tidb_util::timeutil::TimeZone::Local => SessionTimeZone::Local,
            tidb_util::timeutil::TimeZone::Named(zone) => SessionTimeZone::Named(zone),
            tidb_util::timeutil::TimeZone::Fixed { name, offset_secs } => {
                SessionTimeZone::Fixed { name, offset_secs }
            }
        };
        // Go `rpcServer.handleCopRequest` serves every request on a fresh
        // session over the node's store (`createSession`).
        let mut session = (self.session_factory)()?;
        if let Some(user) = &dag.user {
            let name = user.user_name.as_deref().unwrap_or("");
            let host = user.user_host.as_deref().unwrap_or("");
            let (auth_user, auth_host) = self
                .privileges
                .matching_account(name, host)
                .unwrap_or_else(|| (name.to_owned(), host.to_owned()));
            session.set_user(format!("{auth_user}@{auth_host}"), format!("{name}@{host}"));
        }
        session.attach_privileges(self.privileges.clone());
        if let Some(peer) = &self.cluster_peer {
            session.set_cluster_peer_client(Arc::clone(peer));
        }
        if let Some(info) = &self.server_info {
            session.set_server_info_syncer(info.clone());
        }
        let mut result = tipb::SelectResponse {
            encode_type: Some(dag.encode_type.unwrap_or(0)),
            ..Default::default()
        };
        match executor.tp.and_then(|tp| tipb::ExecType::try_from(tp).ok()) {
            Some(tipb::ExecType::TypeKill) => {
                let kill = executor.kill.as_ref().ok_or("missing KILL executor")?;
                let config = tidb_config::config_tree::config::get_global_config();
                if config.enable_global_kill || config.compatible_kill_query {
                    self.processes
                        .kill(kill.conn_id.unwrap_or(0), kill.query.unwrap_or(false));
                }
            }
            Some(tipb::ExecType::TypeTableScan) => {
                let scan = executor.tbl_scan.as_ref().ok_or("missing table scan")?;
                let table = tidb_executor::driver::infoschema_meta::CLUSTER_TABLES
                    .iter()
                    .map(|(name, _)| *name)
                    .find(|name| scan.table_id == tidb_executor::infoschema_reader::memory_table_id(name))
                    .ok_or("unsupported TiDB cluster table")?;
                let columns = tidb_executor::driver::infoschema_meta::table_schema(table)
                    .ok_or("missing cluster table schema")?;
                let selected = scan
                    .columns
                    .iter()
                    .map(|col| {
                        let index = col
                            .column_id
                            .unwrap_or(0)
                            .checked_sub(1)
                            .and_then(|id| usize::try_from(id).ok())
                            .ok_or("invalid cluster table column ID")?;
                        columns
                            .get(index)
                            .ok_or("invalid cluster table column ID")?;
                        Ok(index)
                    })
                    .collect::<Result<Vec<_>, &str>>()?;
                let required = selected
                    .iter()
                    .map(|index| columns[*index].0.clone())
                    .collect();
                let digest_filter = summary_digest_filter(&conditions, &selected, &columns)?;
                // The handler reads outside any statement, over a committed
                // snapshot of its own.
                let rows = session
                    .with_committed_snapshot(|session| {
                        session.local_cluster_table_rows_for(
                            table,
                            Some(&self.processes),
                            &zone,
                            Some(&required),
                            digest_filter.as_ref(),
                        )
                    })
                    .and_then(|rows| rows)
                    .map_err(|error| error.to_string())?;
                let scan_types: Vec<_> = selected
                    .iter()
                    .map(|index| columns[*index].1.clone())
                    .collect();
                let rows = rows
                    .into_iter()
                    .map(|row| selected.iter().map(|index| row[*index].clone()).collect())
                    .collect();
                let rows =
                    select_peer_rows(rows, &scan_types, &conditions, &zone, &mut result.warnings)?;
                let (rows, output_types) =
                    apply_peer_executors(rows, scan_types, rest, &zone, &mut result.warnings)?;
                // Go `encodeChunk`/`encodeDefault`: the offsets index the
                // last executor's output.
                let projection: Vec<_> = dag
                    .output_offsets
                    .iter()
                    .map(|offset| *offset as usize)
                    .collect();
                let fields = projection
                    .iter()
                    .map(|index| {
                        output_types
                            .get(*index)
                            .cloned()
                            .ok_or("invalid cluster table output offset")
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let encoding = tipb::EncodeType::try_from(dag.encode_type.unwrap_or(0))
                    .map_err(|_| "unsupported result encoding")?;
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
                                let values: Vec<_> =
                                    projection.iter().map(|i| row[*i].clone()).collect();
                                data.extend(
                                    tidb_codec::encode_value_in_timezone(&zone, &values)
                                        .map_err(|e| e.to_string())?,
                                );
                            }
                            data
                        }
                        tipb::EncodeType::TypeChunk => {
                            let mut chunk =
                                tidb_chunk::chunk::Chunk::new(&fields, rows.len(), 1024);
                            for row in rows {
                                for (col, index) in projection.iter().enumerate() {
                                    chunk.append_datum(col, &row[*index]);
                                }
                            }
                            tidb_chunk::codec::Codec::new(fields.clone()).encode(&chunk)
                        }
                        _ => return Err("unsupported result encoding".into()),
                    };
                    result.chunks.push(tipb::Chunk {
                        rows_data: Some(data.into()),
                        ..Default::default()
                    });
                }
            }
            Some(tipb::ExecType::TypeBroadcastQuery) => {
                let query = executor
                    .broadcast_query
                    .as_ref()
                    .and_then(|broadcast| broadcast.query.as_deref())
                    .unwrap_or("");
                check_broadcast_query(query)?;
                let runner = self
                    .broadcast_query
                    .as_ref()
                    .ok_or("broadcast query execution is not installed")?;
                runner(query)?;
            }
            _ => return Err("unsupported TiDB peer executor".into()),
        }
        if dag.collect_execution_summaries.unwrap_or(false) {
            result.execution_summaries =
                vec![tipb::ExecutorExecutionSummary::default(); dag.executors.len()];
        }
        Ok(coprocessor::Response {
            data: result.encode_to_vec().into(),
            ..Default::default()
        })
    }
}

/// Extract only the exact binary string-IN shape used by restricted digest lookup.
/// Every condition still executes through SelectionExec after reader filtering.
fn summary_digest_filter(
    conditions: &[tipb::Expr],
    selected: &[usize],
    columns: &[(String, tidb_datatype::FieldType)],
) -> Result<Option<std::collections::HashSet<String>>, String> {
    let mut result: Option<std::collections::HashSet<String>> = None;
    for condition in conditions {
        if condition.tp != Some(tipb::ExprType::ScalarFunc as i32)
            || condition.sig != Some(tipb::ScalarFuncSig::InString as i32)
            || condition
                .field_type
                .as_ref()
                .is_none_or(|field| !matches!(field.collate(), 46 | -46))
        {
            continue;
        }
        let Some((column, values)) = condition.children.split_first() else {
            continue;
        };
        if column.tp != Some(tipb::ExprType::ColumnRef as i32) {
            continue;
        }
        let (_, offset) =
            tidb_codec::decode_int(column.val()).map_err(|error| error.to_string())?;
        let Some(index) = usize::try_from(offset)
            .ok()
            .and_then(|offset| selected.get(offset))
        else {
            return Err("digest column offset out of range".into());
        };
        if !columns[*index].0.eq_ignore_ascii_case("DIGEST") {
            continue;
        }
        let values: Option<std::collections::HashSet<String>> = values
            .iter()
            .map(|value| {
                (value.tp == Some(tipb::ExprType::String as i32))
                    .then(|| String::from_utf8(value.val().to_vec()).ok())
                    .flatten()
            })
            .collect();
        if let Some(values) = values {
            // Stored digests are canonical hex. Other literals (including PAD
            // SPACE variants) must retain the expression collator's semantics.
            if values.iter().any(|value| {
                value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
            }) {
                continue;
            }
            result = Some(match result {
                Some(previous) => previous.intersection(&values).cloned().collect(),
                None => values,
            });
        }
    }
    Ok(result)
}

/// Rejects column references past the input row, which the expression
/// decoder would otherwise index unchecked.
fn validate_peer_expr(expr: &tipb::Expr, width: usize) -> Result<(), String> {
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
        validate_peer_expr(child, width)?;
    }
    Ok(())
}

/// Go `FieldTypeFromPB` of an expression's declared result type.
fn pb_field_type(expr: &tipb::Expr) -> tidb_datatype::FieldType {
    tidb_expr::distsql_builtin::pb_type_to_field_type(&expr.field_type.clone().unwrap_or_default())
}

/// Decodes `exprs` over a row of `fields`.
fn peer_exprs(
    exprs: &[tipb::Expr],
    fields: &[tidb_datatype::FieldType],
    zone: &SessionTimeZone,
) -> Result<Vec<tidb_expr::expression::Expression>, String> {
    exprs
        .iter()
        .map(|expr| {
            validate_peer_expr(expr, fields.len())?;
            tidb_expr::distsql_builtin::pb_to_expr_in(expr, fields, zone)
        })
        .collect()
}

/// The executor schema of a row of `fields`, columns numbered by offset.
fn peer_meta(fields: &[tidb_datatype::FieldType]) -> tidb_executor::ExecutorMeta {
    let schema = tidb_expr::schema::Schema::new(
        fields
            .iter()
            .enumerate()
            .map(|(index, field)| tidb_expr::column::Column::new(index as i64, field.clone()))
            .collect(),
    );
    tidb_executor::ExecutorMeta::new(schema, 0, 32, 1024)
}

fn peer_context(zone: &SessionTimeZone) -> tidb_executor::StmtContext {
    tidb_executor::StmtContext::for_query().with_time_zone(zone.clone())
}

/// Opens, drains and closes `executor`, whose rows have `fields`; its
/// statement warnings become response warnings.
fn drain_peer_executor(
    mut executor: Box<dyn tidb_executor::Executor>,
    fields: &[tidb_datatype::FieldType],
    ctx: &tidb_executor::StmtContext,
    warnings: &mut Vec<tipb::Error>,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
    executor
        .open()
        .map_err(|error| tidb_executor::DriverError::from(error).to_string())?;
    let result = (|| {
        let mut output = Vec::new();
        let mut chunk = tidb_chunk::chunk::Chunk::new(fields, 32, 1024);
        loop {
            executor
                .next(&mut chunk)
                .map_err(|error| tidb_executor::DriverError::from(error).to_string())?;
            if chunk.num_rows() == 0 {
                break;
            }
            for index in 0..chunk.num_rows() {
                output.push(chunk.get_row(index).get_datum_row(fields));
            }
        }
        Ok(output)
    })();
    let close = executor
        .close()
        .map_err(|error| tidb_executor::DriverError::from(error).to_string());
    warnings.extend(
        ctx.take_warnings()
            .into_iter()
            .map(|warning| tipb::Error {
                code: Some(i32::from(warning.code())),
                msg: Some(warning.message().to_owned()),
                ..Default::default()
            }),
    );
    match result {
        Err(error) => Err(error),
        Ok(rows) => {
            close?;
            Ok(rows)
        }
    }
}

/// Reuse the ordinary typed expression and selection owners for received DAGs.
fn select_peer_rows(
    rows: Vec<Vec<tidb_datatype::Datum>>,
    fields: &[tidb_datatype::FieldType],
    conditions: &[tipb::Expr],
    zone: &SessionTimeZone,
    warnings: &mut Vec<tipb::Error>,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
    if conditions.is_empty() {
        return Ok(rows);
    }
    let filters = peer_exprs(conditions, fields, zone)?;
    let meta = peer_meta(fields);
    let ctx = peer_context(zone);
    let memory = ctx.statement_memory().clone();
    let source = tidb_executor::mem_table::MemTableSourceExec::new(meta.clone(), rows);
    let selection = tidb_executor::selection::SelectionExec::new(
        meta,
        filters,
        Box::new(source),
        ctx.clone(),
        memory,
    );
    drain_peer_executor(Box::new(selection), fields, &ctx, warnings)
}

/// Go `PBPlanBuilder` plus the executor builder for the executors above the
/// scan's selections: each runs over the previous one's rows with the
/// ordinary executor owners. Returns the rows and the last output's types.
fn apply_peer_executors(
    mut rows: Vec<Vec<tidb_datatype::Datum>>,
    mut fields: Vec<tidb_datatype::FieldType>,
    executors: &[tipb::Executor],
    zone: &SessionTimeZone,
    warnings: &mut Vec<tipb::Error>,
) -> Result<
    (
        Vec<Vec<tidb_datatype::Datum>>,
        Vec<tidb_datatype::FieldType>,
    ),
    String,
> {
    for executor in executors {
        let tp = executor.tp.and_then(|tp| tipb::ExecType::try_from(tp).ok());
        match tp {
            Some(tipb::ExecType::TypeSelection) => {
                let selection = executor
                    .selection
                    .as_ref()
                    .ok_or("missing peer selection")?;
                rows = select_peer_rows(rows, &fields, &selection.conditions, zone, warnings)?;
            }
            Some(tipb::ExecType::TypeProjection) => {
                let projection = executor
                    .projection
                    .as_ref()
                    .ok_or("missing peer projection")?;
                let exprs = peer_exprs(&projection.exprs, &fields, zone)?;
                let output: Vec<_> = exprs
                    .iter()
                    .zip(&projection.exprs)
                    .map(|(expr, pb)| {
                        expr.static_type()
                            .cloned()
                            .unwrap_or_else(|| pb_field_type(pb))
                    })
                    .collect();
                let ctx = peer_context(zone);
                let source =
                    tidb_executor::mem_table::MemTableSourceExec::new(peer_meta(&fields), rows);
                let exec = tidb_executor::projection::ProjectionExec::new(
                    peer_meta(&output),
                    exprs,
                    Box::new(source),
                    ctx.clone(),
                );
                rows = drain_peer_executor(Box::new(exec), &output, &ctx, warnings)?;
                fields = output;
            }
            Some(tipb::ExecType::TypeTopN) => {
                let top_n = executor.top_n.as_ref().ok_or("missing peer TopN")?;
                let by_items = top_n
                    .order_by
                    .iter()
                    .map(|item| {
                        let expr = item.expr.as_ref().ok_or("missing TopN order item")?;
                        validate_peer_expr(expr, fields.len())?;
                        Ok(tidb_executor::sort::SortByItem {
                            expr: tidb_expr::distsql_builtin::pb_to_expr_in(expr, &fields, zone)?,
                            desc: item.desc.unwrap_or(false),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let ctx = peer_context(zone);
                let memory = ctx.statement_memory().clone();
                let meta = peer_meta(&fields);
                let source = tidb_executor::mem_table::MemTableSourceExec::new(meta.clone(), rows);
                let exec = tidb_executor::topn::TopNExec::new(
                    meta,
                    by_items,
                    Box::new(source),
                    ctx.clone(),
                    0,
                    top_n.limit.unwrap_or(0),
                    memory,
                );
                rows = drain_peer_executor(Box::new(exec), &fields, &ctx, warnings)?;
            }
            Some(tipb::ExecType::TypeLimit) => {
                let limit = executor.limit.as_ref().ok_or("missing peer limit")?;
                let ctx = peer_context(zone);
                let meta = peer_meta(&fields);
                let source = tidb_executor::mem_table::MemTableSourceExec::new(meta.clone(), rows);
                let exec = tidb_executor::limit::LimitExec::new(
                    meta,
                    0,
                    limit.limit.unwrap_or(0),
                    Box::new(source),
                );
                rows = drain_peer_executor(Box::new(exec), &fields, &ctx, warnings)?;
            }
            Some(tipb::ExecType::TypeAggregation | tipb::ExecType::TypeStreamAgg) => {
                let aggregation = executor
                    .aggregation
                    .as_ref()
                    .ok_or("missing peer aggregation")?;
                let streamed = tp == Some(tipb::ExecType::TypeStreamAgg);
                (rows, fields) = peer_aggregate(rows, &fields, aggregation, streamed, zone)?;
            }
            _ => return Err("unsupported TiDB peer executor chain".into()),
        }
    }
    Ok((rows, fields))
}

/// Go's TiDB-side partial aggregation (`PBPlanBuilder.pbToAgg`, built as a
/// hash or stream aggregate): groups by the decoded group-by expressions
/// with Go's `HashGroupKey` and emits only the aggregate functions' partial
/// results -- the TiDB executor does not output group-by values, which is
/// why the planner adds `firstrow` for them. Without GROUP BY an empty input
/// still answers one row of initial partial results.
#[allow(clippy::type_complexity)]
fn peer_aggregate(
    rows: Vec<Vec<tidb_datatype::Datum>>,
    fields: &[tidb_datatype::FieldType],
    aggregation: &tipb::Aggregation,
    streamed: bool,
    zone: &SessionTimeZone,
) -> Result<
    (
        Vec<Vec<tidb_datatype::Datum>>,
        Vec<tidb_datatype::FieldType>,
    ),
    String,
> {
    let group_by = peer_exprs(&aggregation.group_by, fields, zone)?;
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
        validate_peer_expr(func, fields.len())?;
        functions.push(tidb_expr::aggregation::DistAggregate::from_pb(
            func, fields, zone,
        )?);
        let ret = pb_field_type(func);
        if func.tp == Some(tipb::ExprType::Avg as i32) {
            // A partial AVG is its count, then its sum.
            output.push(tidb_datatype::FieldType::new(
                tidb_datatype::FieldTypeCode::LongLong,
            ));
        }
        output.push(ret);
    }
    let ctx = peer_context(zone);
    let mut row = tidb_chunk::mutrow::MutRow::from_types(fields);
    let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
    let mut groups: Vec<(Vec<u8>, Vec<tidb_expr::aggregation::DistAggregateState>)> = Vec::new();
    let new_states = || -> Vec<_> { functions.iter().map(|f| f.create_state()).collect() };
    for values in &rows {
        if values.len() != fields.len() {
            return Err("aggregate row does not match input schema".into());
        }
        row.set_datums(values);
        let mut key = Vec::new();
        for (expr, ft) in group_by.iter().zip(&group_types) {
            let value = expr.eval(&ctx, row.to_row()).map_err(|error| {
                tidb_executor::DriverError::from(tidb_executor::ExecError::Eval(error)).to_string()
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
                .update(state, &ctx, row.to_row())
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

/// Go `PBPlanBuilder.pbToBroadcastQuery`: only `ADMIN RELOAD BINDINGS`,
/// `FLUSH STATS_DELTA` and `REFRESH STATS` may arrive this way. Go's
/// `ast.AdminStmt` is every statement spelled `ADMIN ...`.
fn check_broadcast_query(query: &str) -> Result<(), String> {
    let stmt = tidb_parser::parse(query).map_err(|error| error.compatibility_message(query))?;
    let tidb_ast::Stmt::Admin(admin) = &stmt else {
        return Err(format!("unexpected statement {query} in broadcast query"));
    };
    let spelled_admin = stmt
        .restore()
        .get(..6)
        .is_some_and(|word| word.eq_ignore_ascii_case("ADMIN "));
    match &**admin {
        tidb_ast::AdminStmt::Reload(tidb_ast::AdminReloadKind::Bindings) => Ok(()),
        tidb_ast::AdminStmt::Flush(flush) => match flush.target {
            tidb_ast::FlushTarget::StatsDelta { .. } => Ok(()),
            _ => Err(format!(
                "unexpected flush statement {query} in broadcast query"
            )),
        },
        tidb_ast::AdminStmt::RefreshStats(_) => Ok(()),
        _ if spelled_admin => Err(format!(
            "unexpected admin statement {query} in broadcast query"
        )),
        _ => Err(format!("unexpected statement {query} in broadcast query")),
    }
}

fn error_response(error: String) -> coprocessor::Response {
    coprocessor::Response {
        other_error: error,
        ..Default::default()
    }
}

/// Go `rpcServer.createSession` in Go's tests: every request opens a fresh
/// session on `store`'s domain.
#[cfg(test)]
pub(crate) fn store_sessions(
    store: &Session,
) -> Arc<dyn Fn() -> Result<Session, String> + Send + Sync> {
    let catalog = store.shared_catalog();
    Arc::new(move || Ok(Session::with_catalog(catalog.clone())))
}

#[tonic::async_trait]
impl tikvpb::tikv_server::Tikv for PeerService {
    async fn coprocessor(
        &self,
        request: tonic::Request<coprocessor::Request>,
    ) -> Result<tonic::Response<coprocessor::Response>, tonic::Status> {
        Ok(tonic::Response::new(self.handle(request.into_inner())))
    }

    async fn coprocessor_stream(
        &self,
        request: tonic::Request<coprocessor::Request>,
    ) -> Result<tonic::Response<ReplyStream<coprocessor::Response>>, tonic::Status> {
        let response = self.handle(request.into_inner());
        let responses = if response.other_error.is_empty() {
            let select = tipb::SelectResponse::decode(response.data)
                .map_err(|e| tonic::Status::internal(e.to_string()))?;
            select
                .chunks
                .into_iter()
                .map(|chunk| {
                    Ok(coprocessor::Response {
                        data: tipb::StreamResponse {
                            data: Some(chunk.encode_to_vec().into()),
                            ..Default::default()
                        }
                        .encode_to_vec()
                        .into(),
                        ..Default::default()
                    })
                })
                .collect()
        } else {
            vec![Ok(response)]
        };
        Ok(tonic::Response::new(Box::pin(futures::stream::iter(
            responses,
        ))))
    }

    async fn batch_commands(
        &self,
        request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
    ) -> Result<tonic::Response<ReplyStream<tikvpb::BatchCommandsResponse>>, tonic::Status> {
        let service = self.clone();
        let mut input = request.into_inner();
        Ok(tonic::Response::new(Box::pin(async_stream::try_stream! {
            while let Some(batch) = input.message().await? {
                // The generated transport represents nested commands as opaque
                // bytes. Decode the whole supported batch before any effects,
                // just as Go's protobuf decoder does before invoking the handler.
                enum Command { Cop(coprocessor::Request), Empty(u64) }
                let commands = batch.requests.into_iter().map(|request| {
                    use tikvpb::batch_commands_request::request::Cmd;
                    match request.cmd {
                        Some(Cmd::Coprocessor(data)) => coprocessor::Request::decode(data).map(Command::Cop),
                        Some(Cmd::Empty(data)) => tikvpb::BatchCommandsEmptyRequest::decode(data).map(|request| Command::Empty(request.test_id)),
                        _ => Ok(Command::Empty(0)),
                    }.map_err(|error| tonic::Status::internal(format!("grpc: error unmarshalling request: {error}")))
                }).collect::<Result<Vec<_>, _>>()?;
                let responses = commands.into_iter().map(|request| {
                    use tikvpb::batch_commands_response::response::Cmd;
                    let cmd = match request {
                        Command::Cop(request) => Cmd::Coprocessor(service.handle(request).encode_to_vec().into()),
                        Command::Empty(test_id) => Cmd::Empty(tikvpb::BatchCommandsEmptyResponse { test_id }.encode_to_vec().into()),
                    };
                    tikvpb::batch_commands_response::Response { cmd: Some(cmd) }
                }).collect();
                yield tikvpb::BatchCommandsResponse { responses, request_ids: batch.request_ids, ..Default::default() };
            }
        })))
    }
}

#[cfg(test)]
mod tests {
    // The fixtures are sessions on embedded stores (`Session::new()`), as
    // Go's tests open theirs on a mock store.
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tidb_datatype::Datum;
    use tidb_executor::process::ProcessKillTarget;

    /// The outgoing peer client over an embedded store, whose TiDB requests
    /// cross the real socket through the mock store's redirector, as Go's
    /// mockstore tests reach a peer's status server.
    fn peer_client() -> tidb_executor::cluster_peer::ClusterPeerClient {
        let (rpc, pd, _cluster) = tidb_unistore::mock::new(Vec::new()).unwrap();
        let store =
            tidb_unistore::mock::new_test_tikv_store(&rpc, pd, tikv_client::Config::default())
                .unwrap();
        tidb_executor::cluster_peer::ClusterPeerClient::new(store)
    }

    #[derive(Default)]
    struct Target {
        queries: AtomicUsize,
        connections: AtomicUsize,
    }
    impl ProcessKillTarget for Target {
        fn cancel_query(&self) {
            self.queries.fetch_add(1, Ordering::SeqCst);
        }
        fn kill_connection(&self) {
            self.connections.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn request(dag: tipb::DagRequest) -> coprocessor::Request {
        coprocessor::Request {
            tp: 103,
            data: dag.encode_to_vec(),
            ..Default::default()
        }
    }
    fn scan(user: Option<&str>, encoding: tipb::EncodeType) -> tipb::DagRequest {
        tipb::DagRequest {
            user: user.map(|name| tipb::UserIdentity {
                user_name: Some(name.into()),
                user_host: Some("127.0.0.1".into()),
            }),
            encode_type: Some(encoding as i32),
            output_offsets: vec![1, 0],
            executors: vec![tipb::Executor {
                tp: Some(tipb::ExecType::TypeTableScan as i32),
                tbl_scan: Some(tipb::TableScan {
                    table_id: tidb_executor::infoschema_reader::memory_table_id("CLUSTER_PROCESSLIST"),
                    columns: vec![
                        tipb::ColumnInfo {
                            column_id: Some(3),
                            ..Default::default()
                        },
                        tipb::ColumnInfo {
                            column_id: Some(2),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    struct Discovery(Vec<(String, Vec<u8>)>);
    impl tidb_domain::serverinfo_syncer::EtcdOps for Discovery {
        fn lease_grant(&self, _: i64) -> Result<i64, String> {
            Err("read-only discovery".into())
        }
        fn lease_keep_alive_once(&self, _: i64) -> Result<(), String> {
            Err("read-only discovery".into())
        }
        fn lease_revoke(&self, _: i64) -> Result<(), String> {
            Err("read-only discovery".into())
        }
        fn put_with_lease(&self, _: &str, _: &[u8], _: i64) -> Result<(), String> {
            Err("read-only discovery".into())
        }
        fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(self
                .0
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .cloned()
                .collect())
        }
        fn delete(&self, _: &str) -> Result<(), String> {
            Err("read-only discovery".into())
        }
        fn put(&self, _: &str, _: &[u8]) -> Result<(), String> {
            Err("read-only discovery".into())
        }
        fn delete_prefix(&self, _: &str) -> Result<(), String> {
            Err("read-only discovery".into())
        }
    }

    // Go infoschema_reader.go uses SQLDigestTextRetriever for all three readers.
    fn digest_reader_persistent_case(table: &str, history: bool, lazy: bool) {
        use tidb_executor::deadlock_history::{
            DeadlockRecord, WaitChainItem, GLOBAL_DEADLOCK_HISTORY,
        };
        use tidb_stmtsummary::v2::stmtsummary::{
            global_stmt_summary, set_global_stmt_summary, StmtSummary,
        };
        let config = tidb_config::config_tree::config::get_global_config();
        let previous = global_stmt_summary();
        let summary = StmtSummary::new_for_test(64);
        set_global_stmt_summary(Some(summary.clone()));
        let path = std::env::temp_dir().join(format!(
            "tidb-digest-{}-{}-{}.log",
            std::process::id(),
            table,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        struct RemoveFile(std::path::PathBuf);
        impl Drop for RemoveFile {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _file = RemoveFile(path.clone());
        let mut persistent = (*config).clone();
        persistent.instance.stmt_summary_filename = path.to_string_lossy().into_owned();
        if lazy {
            persistent.instance.stmt_summary_filename =
                "/proc/0/missing-digest-history/summary.log".into();
        }
        persistent.instance.stmt_summary_enable_persistent = true;
        tidb_config::config_tree::config::store_global_config(persistent);
        struct Restore(
            Arc<tidb_config::config_tree::config::Config>,
            Option<Arc<StmtSummary>>,
            Arc<StmtSummary>,
        );
        impl Drop for Restore {
            fn drop(&mut self) {
                tidb_config::config_tree::config::store_global_config(self.0.clone());
                set_global_stmt_summary(self.1.clone());
                self.2.close();
                GLOBAL_DEADLOCK_HISTORY.clear();
                GLOBAL_DEADLOCK_HISTORY.resize(0);
            }
        }
        let _restore = Restore(config, previous, summary.clone());
        let digest = "aabbccdd1144";
        let text = "select digest_reader_persistent";
        let mut info = tidb_stmtsummary::v2::record::generate_stmt_exec_info_4_test(digest);
        info.normalized_sql = text.into();
        if history {
            std::fs::write(
                &path,
                format!(
                    "{}\n",
                    serde_json::json!({"begin":1,"end":2,"digest":digest,"normalized_sql":text})
                ),
            )
            .unwrap();
        } else {
            summary.add(&info);
        }
        let mut session = Session::new();
        session.set_user(
            "diagnostic_reader@%".into(),
            "diagnostic_reader@localhost".into(),
        );
        session.set_process_privilege(true);
        let rows = match table {
            "TIDB_TRX" => {
                let processes = ProcessRegistry::default();
                let _guard = processes.register(
                    71,
                    "diagnostic_reader".into(),
                    "localhost".into(),
                    "test".into(),
                    None,
                );
                processes.transaction_started(71, 123 << 18);
                processes.statement_started_with_digest(71, text, Some(digest), "Running");
                if lazy {
                    let service = PeerService::new(
                        store_sessions(&session),
                        processes,
                        PrivilegeRegistry::default(),
                        None,
                    );
                    let response = service.handle(request(tipb::DagRequest {
                        executors: vec![tipb::Executor {
                            tp: Some(tipb::ExecType::TypeTableScan as i32),
                            tbl_scan: Some(tipb::TableScan {
                                table_id: tidb_executor::infoschema_reader::memory_table_id(
                                    "CLUSTER_TIDB_TRX",
                                ),
                                columns: vec![tipb::ColumnInfo {
                                    column_id: Some(2),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        output_offsets: vec![0],
                        ..Default::default()
                    }));
                    assert!(response.other_error.is_empty(), "{}", response.other_error);
                    let selected = tipb::SelectResponse::decode(response.data.as_ref()).unwrap();
                    assert!(selected.error.is_none(), "{:?}", selected.error);
                    assert!(!selected.chunks.is_empty());
                    return;
                }
                let rows = session
                    .local_cluster_table_rows(
                        "CLUSTER_TIDB_TRX",
                        Some(&processes),
                        &SessionTimeZone::utc(),
                    )
                    .unwrap();
                vec![vec![rows[0][4].clone()]]
            }
            "DEADLOCKS" => {
                GLOBAL_DEADLOCK_HISTORY.resize(10);
                GLOBAL_DEADLOCK_HISTORY.push(DeadlockRecord {
                    occur_time: tidb_datatype::Time::new(
                        tidb_datatype::CoreTime::from_date(2026, 10, 8, 1, 2, 3, 0),
                        tidb_datatype::TimeType::Timestamp,
                        6,
                    )
                    .unwrap(),
                    id: 0,
                    is_retryable: false,
                    wait_chain: vec![WaitChainItem {
                        sql_digest: digest.into(),
                        all_sql_digests: vec![],
                        try_lock_txn: 1,
                        txn_holding_lock: 2,
                        key: vec![],
                    }],
                });
                if lazy {
                    let metadata_reads = Arc::new(AtomicUsize::new(0));
                    let observed = metadata_reads.clone();
                    let catalog = session.shared_catalog();
                    let service = PeerService::new(
                        Arc::new(move || {
                            observed.fetch_add(1, Ordering::SeqCst);
                            Ok(Session::with_catalog(catalog.clone()))
                        }),
                        ProcessRegistry::default(),
                        PrivilegeRegistry::default(),
                        None,
                    );
                    // Every request opens a session, as Go's `createSession`.
                    for (column, expected_reads) in [(6, 1), (9, 2)] {
                        let mut dag = scan(None, tipb::EncodeType::TypeDefault);
                        dag.output_offsets = vec![0];
                        dag.executors[0].tbl_scan = Some(tipb::TableScan {
                            table_id: tidb_executor::infoschema_reader::memory_table_id(
                                "CLUSTER_DEADLOCKS",
                            ),
                            columns: vec![tipb::ColumnInfo {
                                column_id: Some(column),
                                ..Default::default()
                            }],
                            ..Default::default()
                        });
                        let response = service.handle(request(dag));
                        assert!(response.other_error.is_empty(), "{}", response.other_error);
                        assert_eq!(metadata_reads.load(Ordering::SeqCst), expected_reads);
                    }
                }
                let tidb_session::StmtResult::Rows(rows) = session
                    .run(if lazy {
                        "SELECT CURRENT_SQL_DIGEST FROM information_schema.DEADLOCKS"
                    } else {
                        "SELECT CURRENT_SQL_DIGEST_TEXT FROM information_schema.DEADLOCKS"
                    })
                    .unwrap()
                else {
                    panic!("rows")
                };
                rows
            }
            "DATA_LOCK_WAITS" => {
                struct Waits(Vec<tidb_session::DataLockWait>);
                impl tidb_session::DataLockWaitsProvider for Waits {
                    fn lock_waits(&self) -> Result<Vec<tidb_session::DataLockWait>, String> {
                        Ok(self.0.clone())
                    }
                }
                let mut tag = tidb_txnkv::ResourceGroupTagBuilder::new(None);
                tag.set_sql_digest(&[0xaa, 0xbb, 0xcc, 0xdd, 0x11, 0x44]);
                session.set_data_lock_waits_provider(Arc::new(Waits(vec![
                    tidb_session::DataLockWait {
                        txn: 1,
                        wait_for_txn: 2,
                        key: vec![],
                        resource_group_tag: tag.encode_tag_with_key(&[]),
                    },
                ])));
                let tidb_session::StmtResult::Rows(rows) = session
                    .run(if lazy {
                        "SELECT SQL_DIGEST FROM information_schema.DATA_LOCK_WAITS"
                    } else {
                        "SELECT SQL_DIGEST_TEXT FROM information_schema.DATA_LOCK_WAITS"
                    })
                    .unwrap()
                else {
                    panic!("rows")
                };
                rows
            }
            _ => unreachable!(),
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0][0].as_raw_bytes(),
            Some(if lazy {
                digest.as_bytes()
            } else {
                text.as_bytes()
            }),
            "{table}: {rows:?}"
        );
    }

    #[test]
    fn digest_projection_batch_received_selection_keeps_predicate_only_columns() {
        let processes = ProcessRegistry::default();
        let _alice = processes.register(
            11,
            "selection_alice".into(),
            "localhost".into(),
            "test".into(),
            None,
        );
        let _bob = processes.register(
            12,
            "selection_bob".into(),
            "localhost".into(),
            "test".into(),
            None,
        );
        let store = Session::new();
        let service = PeerService::new(
            store_sessions(&store),
            processes,
            PrivilegeRegistry::default(),
            None,
        );
        for encoding in [tipb::EncodeType::TypeDefault, tipb::EncodeType::TypeChunk] {
            let mut dag = scan(None, encoding);
            dag.output_offsets = vec![1, 1]; // ID repeated; USER retained only for the predicate.
            let string_type = tipb::FieldType {
                tp: Some(253),
                charset: Some("utf8mb4".into()),
                collate: Some(-46),
                ..Default::default()
            };
            dag.executors.push(tipb::Executor {
                tp: Some(tipb::ExecType::TypeSelection as i32),
                selection: Some(Box::new(tipb::Selection {
                    conditions: vec![tipb::Expr {
                        tp: Some(tipb::ExprType::ScalarFunc as i32),
                        sig: Some(tipb::ScalarFuncSig::InString as i32),
                        field_type: Some(tipb::FieldType {
                            tp: Some(8),
                            collate: Some(-46),
                            ..Default::default()
                        }),
                        children: vec![
                            tipb::Expr {
                                tp: Some(tipb::ExprType::ColumnRef as i32),
                                val: Some({
                                    let mut bytes = Vec::new();
                                    tidb_codec::encode_int(&mut bytes, 0);
                                    bytes
                                }),
                                field_type: Some(string_type.clone()),
                                ..Default::default()
                            },
                            tipb::Expr {
                                tp: Some(tipb::ExprType::String as i32),
                                val: Some(b"selection_alice".to_vec()),
                                field_type: Some(string_type),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            });
            let response = service.handle(request(dag.clone()));
            assert!(response.other_error.is_empty(), "{}", response.other_error);
            let response = tipb::SelectResponse::decode(response.data.as_ref()).unwrap();
            assert!(response.error.is_none());
            let schema = tidb_executor::infoschema_reader::table_schema("CLUSTER_PROCESSLIST").unwrap();
            let fields = vec![schema[1].1.clone(), schema[1].1.clone()];
            let expected = if encoding == tipb::EncodeType::TypeDefault {
                tidb_codec::encode_value(&[Datum::UInt(11), Datum::UInt(11)]).unwrap()
            } else {
                let mut chunk = tidb_chunk::chunk::Chunk::new(&fields, 1, 1024);
                chunk.append_datum(0, &Datum::UInt(11));
                chunk.append_datum(1, &Datum::UInt(11));
                tidb_chunk::codec::Codec::new(fields).encode(&chunk)
            };
            assert_eq!(response.chunks.len(), 1);
            assert_eq!(
                response.chunks[0].rows_data.as_deref(),
                Some(expected.as_slice())
            );
            dag.executors[1].selection.as_mut().unwrap().conditions[0].children[0].val = Some({
                let mut bytes = Vec::new();
                tidb_codec::encode_int(&mut bytes, 99);
                bytes
            });
            assert!(service
                .handle(request(dag))
                .other_error
                .contains("column offset out of range"));
        }
    }

    #[test]
    fn digest_projection_batch_skips_unselected_transaction_history() {
        digest_reader_persistent_case("TIDB_TRX", false, true);
    }
    #[test]
    fn digest_projection_batch_skips_unselected_deadlock_history() {
        digest_reader_persistent_case("DEADLOCKS", false, true);
    }
    #[test]
    fn digest_projection_batch_skips_unselected_lock_wait_history() {
        digest_reader_persistent_case("DATA_LOCK_WAITS", false, true);
    }

    #[test]
    fn digest_readers_batch_transaction_persistent() {
        digest_reader_persistent_case("TIDB_TRX", false, false);
    }
    #[test]
    fn digest_readers_batch_deadlock_persistent() {
        digest_reader_persistent_case("DEADLOCKS", false, false);
    }
    #[test]
    fn digest_readers_batch_lock_wait_persistent() {
        digest_reader_persistent_case("DATA_LOCK_WAITS", false, false);
    }

    #[test]
    fn digest_readers_batch_persisted_history_all_consumers() {
        for table in ["TIDB_TRX", "DEADLOCKS", "DATA_LOCK_WAITS"] {
            digest_reader_persistent_case(table, true, false);
        }
    }

    #[test]
    fn digest_projection_batch_remote_fallback_predicate_and_errors() {
        use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
        let config = tidb_config::config_tree::config::get_global_config();
        struct RestoreConfig(Arc<tidb_config::config_tree::config::Config>);
        impl Drop for RestoreConfig {
            fn drop(&mut self) {
                tidb_config::config_tree::config::store_global_config(self.0.clone());
            }
        }
        let _config = RestoreConfig(config.clone());
        let mut nonpersistent = (*config).clone();
        nonpersistent.instance.stmt_summary_enable_persistent = false;
        tidb_config::config_tree::config::store_global_config(nonpersistent);
        let captured = Arc::new(std::sync::Mutex::new(Vec::<tipb::DagRequest>::new()));
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct DigestPeer(
            Arc<std::sync::Mutex<Vec<tipb::DagRequest>>>,
            Arc<std::sync::atomic::AtomicBool>,
        );
        #[tonic::async_trait]
        impl Tikv for DigestPeer {
            async fn coprocessor(
                &self,
                request: tonic::Request<coprocessor::Request>,
            ) -> Result<tonic::Response<coprocessor::Response>, tonic::Status> {
                let dag = tipb::DagRequest::decode(request.into_inner().data.as_slice()).unwrap();
                self.0.lock().unwrap().push(dag);
                if self.1.load(Ordering::SeqCst) {
                    return Ok(tonic::Response::new(coprocessor::Response {
                        other_error: "digest peer failure".into(),
                        ..Default::default()
                    }));
                }
                let mut bytes = Vec::new();
                for (digest, text) in [
                    ("d1d2", "remote normalized statement"),
                    ("c1c2", "must not replace local"),
                ] {
                    bytes.extend(
                        tidb_codec::encode_value(&[
                            Datum::new_string(digest),
                            Datum::new_string(text),
                        ])
                        .unwrap(),
                    );
                }
                Ok(tonic::Response::new(coprocessor::Response {
                    data: tipb::SelectResponse {
                        chunks: vec![tipb::Chunk {
                            rows_data: Some(bytes.into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .into(),
                    ..Default::default()
                }))
            }
        }
        let (ready, receive) = std::sync::mpsc::channel();
        let (stop, done) = tokio::sync::oneshot::channel();
        let service = DigestPeer(captured.clone(), fail.clone());
        let worker = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    ready.send(listener.local_addr().unwrap()).unwrap();
                    let incoming = futures::stream::unfold(listener, |listener| async move {
                        let stream = listener.accept().await.map(|(stream, _)| stream);
                        Some((stream, listener))
                    });
                    tonic::transport::Server::builder()
                        .add_service(TikvServer::new(service))
                        .serve_with_incoming_shutdown(incoming, async {
                            let _ = done.await;
                        })
                        .await
                        .unwrap();
                });
        });
        struct StopPeer(
            Option<tokio::sync::oneshot::Sender<()>>,
            Option<std::thread::JoinHandle<()>>,
        );
        impl Drop for StopPeer {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
                self.1.take().unwrap().join().unwrap();
            }
        }
        let _peer = StopPeer(Some(stop), Some(worker));
        let addr = receive.recv().unwrap();
        let mut remote = tidb_domain::serverinfo::ServerInfo::default();
        remote.static_info.id = "digest-remote".into();
        remote.static_info.ip = addr.ip().to_string();
        remote.static_info.status_port = addr.port() as usize;
        let discovery = Arc::new(Discovery(vec![(
            tidb_domain::serverinfo_syncer::server_info_key_path("digest-remote"),
            remote.marshal().unwrap(),
        )]));
        let mut local = tidb_domain::serverinfo::ServerInfo::default();
        local.static_info.id = "digest-local".into();
        local.static_info.ip = "127.0.0.1".into();
        let mut session = Session::new();
        session.set_user("digest_reader@%".into(), "digest_reader@localhost".into());
        session.set_process_privilege(true);
        let syncer = Arc::new(tidb_domain::serverinfo_syncer::Syncer::new(
            local,
            Some(discovery),
        ));
        let peer = Arc::new(peer_client());
        session.set_server_info_syncer(syncer.clone());
        session.set_cluster_peer_client(peer.clone());
        let mut info = tidb_stmtsummary::v2::record::generate_stmt_exec_info_4_test("c1c2");
        info.normalized_sql = "local normalized statement".into();
        tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP.add_statement(&info);
        struct Waits(Vec<tidb_session::DataLockWait>);
        impl tidb_session::DataLockWaitsProvider for Waits {
            fn lock_waits(&self) -> Result<Vec<tidb_session::DataLockWait>, String> {
                Ok(self.0.clone())
            }
        }
        let waits = |digests: &[[u8; 2]]| {
            Arc::new(Waits(
                digests
                    .iter()
                    .map(|digest| {
                        let mut tag = tidb_txnkv::ResourceGroupTagBuilder::new(None);
                        tag.set_sql_digest(digest);
                        tidb_session::DataLockWait {
                            resource_group_tag: tag.encode_tag_with_key(&[]),
                            ..Default::default()
                        }
                    })
                    .collect(),
            ))
        };
        session.set_data_lock_waits_provider(waits(&[[0xc1, 0xc2], [0xd1, 0xd2], [0xe1, 0xe2]]));
        let tidb_session::StmtResult::Rows(rows) = session
            .run("SELECT SQL_DIGEST, SQL_DIGEST_TEXT FROM information_schema.DATA_LOCK_WAITS")
            .unwrap()
        else {
            panic!("rows")
        };
        for (digest, text) in [
            (
                b"c1c2".as_slice(),
                Some(b"local normalized statement".as_slice()),
            ),
            (
                b"d1d2".as_slice(),
                Some(b"remote normalized statement".as_slice()),
            ),
            (b"e1e2".as_slice(), None),
        ] {
            assert_eq!(
                rows.iter()
                    .find(|row| row[0].as_raw_bytes() == Some(digest))
                    .unwrap()[1]
                    .as_raw_bytes(),
                text
            );
        }
        let requests = captured.lock().unwrap().clone();
        assert_eq!(requests.len(), 2, "current and history share one fallback");
        for dag in requests {
            assert!(
                dag.user.is_none(),
                "restricted lookup must not inherit user filtering"
            );
            let scan = dag.executors[0].tbl_scan.as_ref().unwrap();
            let table = if scan.table_id
                == tidb_executor::infoschema_reader::memory_table_id("CLUSTER_STATEMENTS_SUMMARY")
            {
                "CLUSTER_STATEMENTS_SUMMARY"
            } else {
                assert_eq!(
                    scan.table_id,
                    tidb_executor::infoschema_reader::memory_table_id("CLUSTER_STATEMENTS_SUMMARY_HISTORY")
                );
                "CLUSTER_STATEMENTS_SUMMARY_HISTORY"
            };
            let columns = tidb_executor::infoschema_reader::table_schema(table).unwrap();
            assert_eq!(
                scan.columns.iter().map(|c| c.column_id).collect::<Vec<_>>(),
                ["DIGEST", "DIGEST_TEXT"]
                    .iter()
                    .map(|name| Some(
                        columns
                            .iter()
                            .position(|(column, _)| column == name)
                            .unwrap() as i64
                            + 1
                    ))
                    .collect::<Vec<_>>()
            );
            assert_eq!(dag.output_offsets, vec![0, 1]);
            assert_eq!(dag.executors.len(), 2, "missing remote digest selection");
            let selection = dag.executors[1]
                .selection
                .as_ref()
                .expect("digest predicate");
            assert_eq!(selection.conditions.len(), 1);
            assert_eq!(
                selection.conditions[0].sig,
                Some(tipb::ScalarFuncSig::InString as i32)
            );
        }
        // Text-only receiving scans retain global lookup without metadata.
        {
            use tidb_executor::deadlock_history::{
                DeadlockRecord, WaitChainItem, GLOBAL_DEADLOCK_HISTORY,
            };
            struct ClearHistory;
            impl Drop for ClearHistory {
                fn drop(&mut self) {
                    GLOBAL_DEADLOCK_HISTORY.clear();
                }
            }
            let _history = ClearHistory;
            GLOBAL_DEADLOCK_HISTORY.resize(10);
            GLOBAL_DEADLOCK_HISTORY.push(DeadlockRecord {
                occur_time: tidb_datatype::Time::new(
                    tidb_datatype::CoreTime::from_date(2026, 10, 8, 1, 2, 3, 0),
                    tidb_datatype::TimeType::Timestamp,
                    6,
                )
                .unwrap(),
                id: 0,
                is_retryable: false,
                wait_chain: vec![WaitChainItem {
                    sql_digest: "d1d2".into(),
                    all_sql_digests: vec![],
                    try_lock_txn: 1,
                    txn_holding_lock: 2,
                    key: vec![],
                }],
            });
            let receiver = PeerService::new(
                store_sessions(&session),
                ProcessRegistry::default(),
                PrivilegeRegistry::default(),
                Some(syncer),
            )
            .with_cluster_peer_client(Some(peer));
            let mut dag = scan(None, tipb::EncodeType::TypeDefault);
            dag.output_offsets = vec![0];
            dag.executors[0].tbl_scan = Some(tipb::TableScan {
                table_id: tidb_executor::infoschema_reader::memory_table_id("CLUSTER_DEADLOCKS"),
                columns: vec![tipb::ColumnInfo {
                    column_id: Some(7),
                    ..Default::default()
                }],
                ..Default::default()
            });
            let response = receiver.handle(request(dag));
            assert!(response.other_error.is_empty(), "{}", response.other_error);
            let selected = tipb::SelectResponse::decode(response.data).unwrap();
            assert_eq!(
                selected.chunks[0].rows_data.as_deref().unwrap(),
                tidb_codec::encode_value(&[Datum::new_string("remote normalized statement")])
                    .unwrap()
            );
        }
        // Known local text never reaches a failing peer.
        let known_requests = captured.lock().unwrap().len();
        fail.store(true, Ordering::SeqCst);
        session.set_data_lock_waits_provider(waits(&[[0xc1, 0xc2]]));
        session
            .run("SELECT SQL_DIGEST_TEXT FROM information_schema.DATA_LOCK_WAITS")
            .unwrap();
        assert_eq!(captured.lock().unwrap().len(), known_requests);
        // Running transactions use only local lookup, including an unknown digest.
        let processes = ProcessRegistry::default();
        let _guard = processes.register(
            91,
            "digest_reader".into(),
            "localhost".into(),
            "test".into(),
            None,
        );
        processes.transaction_started(91, 1 << 18);
        processes.statement_started_with_digest(91, "running", Some("e1e2"), "Running");
        let rows = session
            .local_cluster_table_rows(
                "CLUSTER_TIDB_TRX",
                Some(&processes),
                &SessionTimeZone::utc(),
            )
            .unwrap();
        assert_eq!(rows[0][4], Datum::Null);
        assert_eq!(captured.lock().unwrap().len(), known_requests);
        session.set_data_lock_waits_provider(waits(&[[0xd1, 0xd2]]));
        let before = captured.lock().unwrap().len();
        for sql in [
            "SELECT SQL_DIGEST FROM information_schema.DATA_LOCK_WAITS",
            "SELECT COUNT(*) FROM information_schema.DATA_LOCK_WAITS",
            "WITH c AS (SELECT SQL_DIGEST FROM information_schema.DATA_LOCK_WAITS) SELECT * FROM c",
        ] {
            session.run(sql).unwrap();
        }
        assert_eq!(
            captured.lock().unwrap().len(),
            before,
            "unused text must not contact peers"
        );
        for sql in [
            "SELECT SQL_DIGEST FROM information_schema.DATA_LOCK_WAITS WHERE SQL_DIGEST_TEXT IS NOT NULL",
            "SELECT SQL_DIGEST FROM information_schema.DATA_LOCK_WAITS ORDER BY SQL_DIGEST_TEXT",
        ] { assert!(session.run(sql).unwrap_err().to_string().contains("digest peer failure")); }
        assert!(session
            .run("SELECT SQL_DIGEST_TEXT FROM information_schema.DATA_LOCK_WAITS")
            .unwrap_err()
            .to_string()
            .contains("digest peer failure"));
        // Go's 512-request boundary selects bounded IN versus fetch-all.
        fail.store(false, Ordering::SeqCst);
        for size in [512, 513] {
            let keys: Vec<_> = (0..size)
                .map(|index| (0x8000_u16 + index as u16).to_be_bytes())
                .collect();
            session.set_data_lock_waits_provider(waits(&keys));
            let before = captured.lock().unwrap().len();
            let tidb_session::StmtResult::Rows(rows) = session
                .run("SELECT SQL_DIGEST_TEXT FROM information_schema.DATA_LOCK_WAITS")
                .unwrap()
            else {
                panic!("rows");
            };
            assert_eq!(rows.len(), size);
            let requests = captured.lock().unwrap();
            assert_eq!(requests.len(), before + 2);
            assert!(requests[before..]
                .iter()
                .all(|dag| dag.executors.len() == if size <= 512 { 2 } else { 1 }));
        }
        fail.store(true, Ordering::SeqCst);
        let count = captured.lock().unwrap().len();
        session.set_process_privilege(false);
        assert!(session
            .run("SELECT SQL_DIGEST_TEXT FROM information_schema.DATA_LOCK_WAITS")
            .unwrap_err()
            .to_string()
            .contains("PROCESS"));
        assert_eq!(captured.lock().unwrap().len(), count);
    }

    #[test]
    fn cluster_diagnostics_batch_transaction_sql() {
        Session::new()
            .run("SELECT * FROM information_schema.CLUSTER_TIDB_TRX")
            .unwrap();
    }

    #[test]
    fn cluster_diagnostics_batch_deadlock_admission() {
        let mut session = Session::new();
        session.set_user("diagnostic@%".into(), "diagnostic@localhost".into());
        let error = session
            .run("SELECT * FROM information_schema.CLUSTER_DEADLOCKS")
            .unwrap_err();
        let error = error.to_mysql_error();
        assert_eq!(error.code, 1227, "{}", error.message);
        assert!(
            error.message.contains("the PROCESS privilege(s)"),
            "{}",
            error.message
        );
    }

    #[test]
    fn cluster_diagnostics_batch_memory_sql() {
        let mut session = Session::new();
        let tidb_session::StmtResult::Rows(rows) = session
            .run("SELECT MEMORY_TOTAL FROM information_schema.CLUSTER_MEMORY_USAGE")
            .unwrap()
        else {
            panic!("expected rows")
        };
        assert_eq!(rows.len(), 1);
        session
            .run("SELECT * FROM information_schema.CLUSTER_MEMORY_USAGE_OPS_HISTORY")
            .unwrap();
    }

    #[test]
    fn cluster_diagnostics_batch_index_sql() {
        let mut session = Session::new();
        session.run("CREATE DATABASE diagnostic_batch").unwrap();
        session
            .run("CREATE TABLE diagnostic_batch.t (id INT PRIMARY KEY, v INT, KEY ix(v))")
            .unwrap();
        let tidb_session::StmtResult::Rows(rows) = session.run("SELECT INDEX_NAME FROM information_schema.CLUSTER_TIDB_INDEX_USAGE WHERE TABLE_SCHEMA='diagnostic_batch' AND TABLE_NAME='t'").unwrap() else { panic!("expected rows") };
        assert!(
            rows.iter()
                .any(|row| row[0].as_raw_bytes() == Some(b"ix".as_slice())),
            "{rows:?}"
        );
    }

    #[test]
    fn cluster_diagnostics_batch_generated_receiver() {
        let store = Session::new();
        let service = PeerService::new(
            store_sessions(&store),
            ProcessRegistry::default(),
            PrivilegeRegistry::default(),
            None,
        );
        for table in [
            "CLUSTER_TIDB_TRX",
            "CLUSTER_DEADLOCKS",
            "CLUSTER_MEMORY_USAGE",
            "CLUSTER_MEMORY_USAGE_OPS_HISTORY",
            "CLUSTER_TIDB_INDEX_USAGE",
        ] {
            let mut dag = scan(None, tipb::EncodeType::TypeDefault);
            dag.executors[0].tbl_scan = Some(tipb::TableScan {
                table_id: tidb_executor::infoschema_reader::memory_table_id(table),
                columns: vec![tipb::ColumnInfo {
                    column_id: Some(1),
                    ..Default::default()
                }],
                ..Default::default()
            });
            dag.output_offsets = vec![0];
            let response = service.handle(request(dag));
            assert!(
                response.other_error.is_empty(),
                "{table}: {}",
                response.other_error
            );
        }
    }

    #[test]
    fn cluster_diagnostics_batch_live_registry_catalog_and_collector() {
        use tidb_executor::deadlock_history::{
            DeadlockRecord, WaitChainItem, GLOBAL_DEADLOCK_HISTORY,
        };
        let processes = ProcessRegistry::default();
        let _alice = processes.register(
            41,
            "diagnostic_alice".into(),
            "127.0.0.1".into(),
            "diagnostic_batch".into(),
            None,
        );
        let _bob = processes.register(
            42,
            "diagnostic_bob".into(),
            "127.0.0.1".into(),
            "diagnostic_batch".into(),
            None,
        );
        processes.transaction_started(41, 100 << 18);
        processes.transaction_started(42, 200 << 18);
        let mut producer = Session::new();
        producer.run("CREATE USER diagnostic_alice").unwrap();
        producer.run("CREATE DATABASE diagnostic_batch").unwrap();
        let privileges = producer.privilege_manager().unwrap();
        let catalog = producer.shared_catalog();
        let collector = Arc::new(tidb_stats_handle_usage_indexusage::Collector::new());
        collector.start_worker();
        struct CloseCollector(Arc<tidb_stats_handle_usage_indexusage::Collector>);
        impl Drop for CloseCollector {
            fn drop(&mut self) {
                self.0.close();
            }
        }
        let _collector = CloseCollector(collector.clone());
        let metadata = catalog.clone();
        let counters = collector.clone();
        let service = PeerService::new(
            Arc::new(move || {
                let mut session = Session::with_catalog(metadata.clone());
                session.set_index_usage_collector(counters.clone());
                Ok(session)
            }),
            processes.clone(),
            privileges,
            None,
        );
        // Construct the service first: subsequent DDL must be visible to it.
        producer
            .run("CREATE TABLE diagnostic_batch.t (id BIGINT PRIMARY KEY, v INT, KEY ix(v))")
            .unwrap();
        let (table_id, index_id) = {
            let mut catalog = catalog.lock().unwrap();
            let tidb_executor::TableEntry::Kv(table) =
                catalog.table_mut_in("diagnostic_batch", "t").unwrap()
            else {
                panic!("KV table")
            };
            (
                table.table_id(),
                table
                    .indexes()
                    .iter()
                    .find(|index| index.name.eq_ignore_ascii_case("ix"))
                    .unwrap()
                    .id,
            )
        };
        let mut updates = collector.spawn_session_collector();
        updates.update(
            table_id,
            index_id,
            tidb_stats_handle_usage_indexusage::new_sample(7, 11, 1, 1),
        );
        updates.flush();
        GLOBAL_DEADLOCK_HISTORY.resize(10);
        struct ClearHistory;
        impl Drop for ClearHistory {
            fn drop(&mut self) {
                GLOBAL_DEADLOCK_HISTORY.clear();
                GLOBAL_DEADLOCK_HISTORY.resize(0);
            }
        }
        let _history = ClearHistory;
        GLOBAL_DEADLOCK_HISTORY.push(DeadlockRecord {
            occur_time: tidb_datatype::Time::new(
                tidb_datatype::CoreTime::from_date(2026, 10, 8, 1, 2, 3, 0),
                tidb_datatype::TimeType::Timestamp,
                6,
            )
            .unwrap(),
            id: 0,
            is_retryable: false,
            wait_chain: vec![WaitChainItem {
                sql_digest: String::new(),
                all_sql_digests: Vec::new(),
                try_lock_txn: 10,
                txn_holding_lock: 20,
                key: tidb_codec::table_key::encode_row_key_with_handle(
                    table_id,
                    &tidb_codec::table_key::RecordHandle::Int(9),
                ),
            }],
        });
        let server = crate::http_status::start_status_listener_with_routes(
            "127.0.0.1",
            0,
            Arc::new(crate::sql_node::ConnectionTracker::default()),
            "test".into(),
            "test".into(),
            crate::http_status::StatusRoutes {
                peer: Some(service),
                ..Default::default()
            },
        )
        .unwrap();
        let mut info = tidb_domain::serverinfo::ServerInfo::default();
        info.static_info.ip = server.local_addr().ip().to_string();
        info.static_info.status_port = server.local_addr().port() as usize;
        let outbound = peer_client();
        let read = |table: &str| {
            let columns = tidb_executor::driver::infoschema_meta::table_schema(table).unwrap();
            let rows = outbound.scan(
                &[info.clone()],
                tidb_executor::infoschema_reader::memory_table_id(table).unwrap(),
                &columns,
                Some(("diagnostic_alice", "127.0.0.1")),
                &tidb_executor::StmtContext::for_query(),
                &SessionTimeZone::utc(),
                1,
            )?;
            assert!(rows.warnings.is_empty(), "{:?}", rows.warnings);
            Ok::<_, tidb_executor::DriverError>((columns, rows.rows))
        };
        let (columns, rows) = read("CLUSTER_TIDB_TRX").unwrap();
        let id = columns
            .iter()
            .position(|(name, _)| name == "SESSION_ID")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][id], Datum::UInt(41));
        assert!(read("CLUSTER_DEADLOCKS")
            .unwrap_err()
            .to_string()
            .contains("PROCESS"));
        // Go `setDataFromIndexUsage` checks `RequestVerification` with
        // `AllPrivMask`: `information_schema` is every user's, so only its
        // `CLUSTER_SLOW_QUERY` (whose timestamp primary key Go's
        // `buildTableMeta` makes a common handle) is listed; the ungranted
        // table stays hidden (gorun).
        let visible = read("CLUSTER_TIDB_INDEX_USAGE")
            .unwrap()
            .1
            .iter()
            .map(|row| {
                row[1..4]
                    .iter()
                    .map(|datum| String::from_utf8(datum.as_raw_bytes().unwrap().to_vec()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            visible,
            vec![vec!["INFORMATION_SCHEMA", "CLUSTER_SLOW_QUERY", "primary"]]
        );
        producer
            .run("GRANT PROCESS, SELECT ON *.* TO diagnostic_alice")
            .unwrap();
        assert_eq!(read("CLUSTER_TIDB_TRX").unwrap().1.len(), 2);
        let (columns, rows) = read("CLUSTER_DEADLOCKS").unwrap();
        let key = columns
            .iter()
            .position(|(name, _)| name == "KEY_INFO")
            .unwrap();
        assert!(rows[0][key]
            .as_raw_bytes()
            .unwrap()
            .windows(b"diagnostic_batch".len())
            .any(|part| part == b"diagnostic_batch"));
        let (columns, rows) = read("CLUSTER_TIDB_INDEX_USAGE").unwrap();
        let name = columns
            .iter()
            .position(|(name, _)| name == "INDEX_NAME")
            .unwrap();
        let count = columns
            .iter()
            .position(|(name, _)| name == "QUERY_TOTAL")
            .unwrap();
        let row = rows
            .iter()
            .find(|row| row[name].as_raw_bytes() == Some(b"ix".as_slice()))
            .unwrap();
        assert!(
            matches!(row[count], Datum::Int(value) if value > 0)
                || matches!(row[count], Datum::UInt(value) if value > 0),
            "{row:?}"
        );
        producer
            .run("ALTER TABLE diagnostic_batch.t RENAME INDEX ix TO fresh_ix")
            .unwrap();
        let (_, rows) = read("CLUSTER_TIDB_INDEX_USAGE").unwrap();
        assert!(rows
            .iter()
            .any(|row| row[name].as_raw_bytes() == Some(b"fresh_ix".as_slice())));
        assert_eq!(read("CLUSTER_MEMORY_USAGE").unwrap().1.len(), 1);
        assert_eq!(
            read("CLUSTER_MEMORY_USAGE_OPS_HISTORY").unwrap().1.len(),
            tidb_util::servermemorylimit::GLOBAL_MEMORY_OPS_HISTORY_MANAGER
                .get_rows()
                .len()
        );
        processes.transaction_finished(41);
        assert_eq!(read("CLUSTER_TIDB_TRX").unwrap().1.len(), 1);
        assert_eq!(
            processes.snapshot().len(),
            2,
            "receiver must not register synthetic clients"
        );
        drop(outbound);
        drop(server);
    }

    // Go executor/stmtsummary.go and infoschema_reader.go: the local and
    // distributed readers consume the same live summary owners.
    #[test]
    fn cluster_summary_batch_sql_tables_share_completed_statements() {
        let mut session = Session::new();
        session.set_user("summary_reader@%".into(), "summary_reader@localhost".into());
        session.run("SELECT 918273 + 4").unwrap();
        for table in [
            "CLUSTER_STATEMENTS_SUMMARY",
            "CLUSTER_STATEMENTS_SUMMARY_HISTORY",
            "CLUSTER_TIDB_STATEMENTS_STATS",
        ] {
            let output = session.run(&format!(
                "SELECT DIGEST_TEXT FROM information_schema.{table}"
            ));
            let tidb_session::StmtResult::Rows(rows) = output.unwrap() else {
                panic!("expected rows")
            };
            assert!(
                rows.iter()
                    .any(|row| row[0] == Datum::new_string("select ? + ?")),
                "{table}: {rows:?}"
            );
        }
    }

    #[test]
    fn cluster_summary_batch_evicted_requires_process() {
        let mut session = Session::new();
        session.set_user("summary_reader@%".into(), "summary_reader@localhost".into());
        let error = session
            .run("SELECT * FROM information_schema.STATEMENTS_SUMMARY_EVICTED")
            .unwrap_err();
        let error = error.to_mysql_error();
        assert_eq!(error.code, 1227, "{}", error.message);
        assert!(
            error.message.contains("the PROCESS privilege(s)"),
            "{}",
            error.message
        );
        session.set_process_privilege(true);
        session
            .run("SELECT * FROM information_schema.STATEMENTS_SUMMARY_EVICTED")
            .unwrap();
        session
            .run("SELECT * FROM information_schema.CLUSTER_STATEMENTS_SUMMARY_EVICTED")
            .unwrap();
    }

    #[test]
    fn cluster_summary_batch_incoming_transaction_summary() {
        let store = Session::new();
        let service = PeerService::new(
            store_sessions(&store),
            ProcessRegistry::default(),
            PrivilegeRegistry::default(),
            None,
        );
        let mut dag = scan(None, tipb::EncodeType::TypeDefault);
        dag.executors[0].tbl_scan.as_mut().unwrap().table_id =
            tidb_executor::infoschema_reader::memory_table_id("CLUSTER_TRX_SUMMARY");
        let response = service.handle(request(dag));
        assert!(response.other_error.is_empty(), "{}", response.other_error);
        tipb::SelectResponse::decode(response.data).unwrap();
    }

    #[test]
    fn cluster_summary_batch_shared_client_reads_live_summaries_and_admission() {
        // `admin` is unauthenticated, hence unrestricted: it provisions the
        // accounts the way a deployment does before anybody logs in.
        let mut admin = Session::new();
        admin.run("CREATE USER summary_alice, summary_bob").unwrap();
        let privileges = admin.privilege_manager().unwrap();
        let mut producer = Session::with_catalog(admin.shared_catalog());
        producer.set_user("summary_alice@%".into(), "summary_alice@localhost".into());
        producer.run("SELECT 73 + 8").unwrap();
        producer.set_user("summary_bob@%".into(), "summary_bob@localhost".into());
        producer.run("SELECT 73 * 8").unwrap();
        let mut identity = tidb_domain::serverinfo::ServerInfo::default();
        identity.static_info.id = "summary-node".into();
        identity.static_info.ip = "192.0.2.7".into();
        identity.static_info.status_port = 10080;
        let syncer = Arc::new(tidb_domain::serverinfo_syncer::Syncer::new(identity, None));
        let service = PeerService::new(
            store_sessions(&admin),
            ProcessRegistry::default(),
            privileges.clone(),
            Some(syncer),
        );
        let server = crate::http_status::start_status_listener_with_routes(
            "127.0.0.1",
            0,
            Arc::new(crate::sql_node::ConnectionTracker::default()),
            "test".into(),
            "test".into(),
            crate::http_status::StatusRoutes {
                peer: Some(service.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        let mut address = tidb_domain::serverinfo::ServerInfo::default();
        address.static_info.ip = server.local_addr().ip().to_string();
        address.static_info.status_port = server.local_addr().port() as usize;
        let outbound = Arc::new(peer_client());
        for table in [
            "CLUSTER_STATEMENTS_SUMMARY",
            "CLUSTER_STATEMENTS_SUMMARY_HISTORY",
            "CLUSTER_TIDB_STATEMENTS_STATS",
        ] {
            let columns = tidb_executor::driver::infoschema_meta::table_schema(table).unwrap();
            let rows = outbound
                .scan(
                    &[address.clone()],
                    tidb_executor::infoschema_reader::memory_table_id(table).unwrap(),
                    &columns,
                    Some(("summary_alice", "127.0.0.1")),
                    &tidb_executor::StmtContext::for_query(),
                    &SessionTimeZone::utc(),
                    2,
                )
                .unwrap();
            assert!(rows.warnings.is_empty(), "{:?}", rows.warnings);
            let text = columns
                .iter()
                .position(|(name, _)| name == "DIGEST_TEXT")
                .unwrap();
            assert!(
                rows.rows
                    .iter()
                    .any(|row| row[text].as_raw_bytes() == Some(b"select ? + ?".as_slice())),
                "{table}: {:?}",
                rows.rows
            );
            assert!(
                rows.rows
                    .iter()
                    .all(|row| row[text].as_raw_bytes() != Some(b"select ? * ?".as_slice())),
                "{table} exposed another user's statements"
            );
            assert!(rows
                .rows
                .iter()
                .all(|row| row[0].as_raw_bytes() == Some(b"192.0.2.7:10080".as_slice())));
            if table != "CLUSTER_TIDB_STATEMENTS_STATS" {
                let digest = columns
                    .iter()
                    .position(|(name, _)| name == "DIGEST")
                    .unwrap();
                let expected = rows
                    .rows
                    .iter()
                    .find(|row| row[text].as_raw_bytes() == Some(b"select ? + ?".as_slice()))
                    .unwrap();
                let value =
                    String::from_utf8(expected[digest].as_raw_bytes().unwrap().to_vec()).unwrap();
                for value in [value.clone(), format!("{value} ")] {
                    let filtered = outbound
                        .scan_projected(
                            &[address.clone()],
                            tidb_executor::infoschema_reader::memory_table_id(table).unwrap(),
                            &columns,
                            &[digest, text],
                            None,
                            &tidb_executor::StmtContext::for_query(),
                            &SessionTimeZone::utc(),
                            2,
                            Some(&[value, "00".into()].into_iter().collect()),
                        )
                        .unwrap();
                    assert!(filtered.warnings.is_empty());
                    assert_eq!(
                        filtered.rows,
                        vec![vec![expected[digest].clone(), expected[text].clone()]],
                        "{table}"
                    );
                }
            }
            // Both generated result encodings support reordered and repeated outputs.
            for encoding in [tipb::EncodeType::TypeDefault, tipb::EncodeType::TypeChunk] {
                let mut dag = scan(Some("summary_alice"), encoding);
                dag.executors[0].tbl_scan = Some(tipb::TableScan {
                    table_id: tidb_executor::infoschema_reader::memory_table_id(table),
                    columns: vec![
                        tipb::ColumnInfo {
                            column_id: Some((text + 1) as i64),
                            ..Default::default()
                        },
                        tipb::ColumnInfo {
                            column_id: Some(1),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                });
                dag.output_offsets = vec![1, 0, 1];
                let response = service.handle(request(dag));
                assert!(response.other_error.is_empty(), "{}", response.other_error);
                let selected = tipb::SelectResponse::decode(response.data).unwrap();
                assert_eq!(selected.encode_type, Some(encoding as i32));
                assert!(!selected.chunks.is_empty());
                let expected: Vec<_> = rows
                    .rows
                    .iter()
                    .map(|row| vec![row[0].clone(), row[text].clone(), row[0].clone()])
                    .collect();
                let fields = vec![
                    columns[0].1.clone(),
                    columns[text].1.clone(),
                    columns[0].1.clone(),
                ];
                let data = if encoding == tipb::EncodeType::TypeDefault {
                    expected
                        .iter()
                        .flat_map(|row| tidb_codec::encode_value(row).unwrap())
                        .collect::<Vec<_>>()
                } else {
                    let mut chunk = tidb_chunk::chunk::Chunk::new(&fields, expected.len(), 1024);
                    for row in &expected {
                        for (col, value) in row.iter().enumerate() {
                            chunk.append_datum(col, value);
                        }
                    }
                    tidb_chunk::codec::Codec::new(fields).encode(&chunk)
                };
                assert_eq!(
                    selected.chunks[0].rows_data.as_deref().unwrap(),
                    data,
                    "{table}"
                );
            }
        }
        let columns = tidb_executor::driver::infoschema_meta::table_schema(
            "CLUSTER_STATEMENTS_SUMMARY_EVICTED",
        )
        .unwrap();
        let scan_evicted = || {
            outbound.scan(
                &[address.clone()],
                tidb_executor::infoschema_reader::memory_table_id("CLUSTER_STATEMENTS_SUMMARY_EVICTED")
                    .unwrap(),
                &columns,
                Some(("summary_alice", "127.0.0.1")),
                &tidb_executor::StmtContext::for_query(),
                &SessionTimeZone::utc(),
                1,
            )
        };
        assert!(scan_evicted().unwrap_err().to_string().contains("PROCESS"));
        // The receiver loads the account's default roles before admission.
        for sql in [
            "CREATE ROLE summary_observer",
            "GRANT PROCESS ON *.* TO summary_observer",
            "GRANT summary_observer TO summary_alice",
            "SET DEFAULT ROLE summary_observer TO summary_alice",
        ] {
            admin
                .run(sql)
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        }
        scan_evicted().unwrap();
        // Exercise SQL -> discovered peer -> receiver, excluding an unpublished
        // local identity. This is a discovery fixture, not another RPC harness.
        address.static_info.id = "remote-summary".into();
        let discovery = Arc::new(Discovery(vec![(
            tidb_domain::serverinfo_syncer::server_info_key_path("remote-summary"),
            address.marshal().unwrap(),
        )]));
        let mut local = tidb_domain::serverinfo::ServerInfo::default();
        local.static_info.id = "unpublished-local".into();
        local.static_info.ip = "127.0.0.1".into();
        let mut origin = Session::with_catalog(admin.shared_catalog());
        origin.set_user("summary_alice@%".into(), "summary_alice@127.0.0.1".into());
        origin.attach_privileges(privileges.clone());
        origin.set_server_info_syncer(Arc::new(tidb_domain::serverinfo_syncer::Syncer::new(
            local,
            Some(discovery),
        )));
        origin.set_cluster_peer_client(outbound.clone());
        for table in [
            "CLUSTER_STATEMENTS_SUMMARY",
            "CLUSTER_STATEMENTS_SUMMARY_HISTORY",
            "CLUSTER_TIDB_STATEMENTS_STATS",
        ] {
            let output = origin.run(&format!(
                "SELECT INSTANCE FROM information_schema.{table} WHERE DIGEST_TEXT='select ? + ?'"
            ));
            let tidb_session::StmtResult::Rows(rows) = output.unwrap() else {
                panic!("expected SQL rows")
            };
            assert_eq!(rows.len(), 1, "{table}: {rows:?}");
            assert_eq!(
                rows[0][0].as_raw_bytes(),
                Some(b"192.0.2.7:10080".as_slice()),
                "{table}"
            );
        }
        drop(origin);
        drop(outbound);
        drop(server);
    }

    #[test]
    fn cluster_summary_batch_persistent_owner_and_eviction_rows() {
        use tidb_stmtsummary::v2::stmtsummary::{
            global_stmt_summary, set_global_stmt_summary, StmtSummary,
        };
        // The store comes up first: its bootstrap's sysvar-cache rebuild
        // publishes tidb_stmt_summary_max_stmt_count's default into the
        // process-global summary, as a new Go domain does.
        let mut session = Session::new();
        let config = tidb_config::config_tree::config::get_global_config();
        let previous = global_stmt_summary();
        let summary = StmtSummary::new_for_test(1);
        set_global_stmt_summary(Some(summary.clone()));
        let mut persistent = (*config).clone();
        persistent.instance.stmt_summary_enable_persistent = true;
        tidb_config::config_tree::config::store_global_config(persistent);
        struct Restore(
            Arc<tidb_config::config_tree::config::Config>,
            Option<Arc<StmtSummary>>,
            Arc<StmtSummary>,
        );
        impl Drop for Restore {
            fn drop(&mut self) {
                tidb_config::config_tree::config::store_global_config(self.0.clone());
                set_global_stmt_summary(self.1.clone());
                self.2.close();
            }
        }
        let _restore = Restore(config, previous, summary.clone());
        session.set_user("summary_v2@%".into(), "summary_v2@localhost".into());
        session.set_process_privilege(true);
        session.run("SELECT 12 + 19").unwrap();
        session.run("SELECT 12 * 19").unwrap();
        let rows = session
            .run("SELECT EVICTED_COUNT FROM information_schema.CLUSTER_STATEMENTS_SUMMARY_EVICTED")
            .unwrap();
        assert_eq!(
            rows,
            tidb_session::StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        let error = session
            .run("SELECT * FROM information_schema.CLUSTER_TIDB_STATEMENTS_STATS")
            .unwrap_err();
        let error = error.to_mysql_error();
        assert_eq!(error.code, 1235, "{}", error.message);
        session
            .run("SELECT DIGEST_TEXT FROM information_schema.CLUSTER_STATEMENTS_SUMMARY")
            .unwrap();
        // No background collector or v1 substitution is needed for an empty v2 owner.
        summary.clear();
        assert_eq!(
            session
                .local_cluster_table_rows(
                    "CLUSTER_STATEMENTS_SUMMARY_EVICTED",
                    None,
                    &SessionTimeZone::utc()
                )
                .unwrap(),
            Vec::<Vec<Datum>>::new()
        );
    }

    #[test]
    fn peer_host_batch_generated_protocol_kill_scan_stream_and_batch() {
        let processes = ProcessRegistry::default();
        let mut store = Session::new();
        store.run("CREATE USER alice, bob").unwrap();
        let privileges = store.privilege_manager().unwrap();
        let target = Arc::new(Target::default());
        let _alice = processes.register(
            10,
            "alice".into(),
            "127.0.0.1:1".into(),
            "test".into(),
            Some(target.clone()),
        );
        let _bob = processes.register(12, "bob".into(), "127.0.0.1:2".into(), "test".into(), None);
        let service = PeerService::new(
            store_sessions(&store),
            processes.clone(),
            privileges.clone(),
            None,
        );
        let server = crate::http_status::start_status_listener_with_routes(
            "127.0.0.1",
            0,
            Arc::new(crate::sql_node::ConnectionTracker::default()),
            "test".into(),
            "test".into(),
            crate::http_status::StatusRoutes {
                peer: Some(service),
                ..Default::default()
            },
        )
        .unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut client =
                tikvpb::tikv_client::TikvClient::connect(format!("http://{}", server.local_addr()))
                    .await
                    .unwrap();
            for query in [true, false] {
                let dag = tipb::DagRequest {
                    executors: vec![tipb::Executor {
                        tp: Some(tipb::ExecType::TypeKill as i32),
                        kill: Some(tipb::Kill {
                            conn_id: Some(10),
                            query: Some(query),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let reply = client.coprocessor(request(dag)).await.unwrap().into_inner();
                assert!(reply.other_error.is_empty(), "{}", reply.other_error);
            }
            assert_eq!(target.queries.load(Ordering::SeqCst), 1);
            assert_eq!(target.connections.load(Ordering::SeqCst), 1);
            let dag = scan(Some("alice"), tipb::EncodeType::TypeDefault);
            let reply = client
                .coprocessor(request(dag.clone()))
                .await
                .unwrap()
                .into_inner();
            assert!(reply.other_error.is_empty(), "{}", reply.other_error);
            let selected = tipb::SelectResponse::decode(reply.data).unwrap();
            let expected =
                tidb_codec::encode_value(&[Datum::UInt(10), Datum::Bytes(b"alice".to_vec())])
                    .unwrap();
            assert_eq!(selected.chunks.len(), 1);
            assert_eq!(selected.chunks[0].rows_data.as_deref().unwrap(), expected);
            let mut stream = client
                .coprocessor_stream(request(dag.clone()))
                .await
                .unwrap()
                .into_inner();
            let response = stream.message().await.unwrap().unwrap();
            let streamed = tipb::StreamResponse::decode(response.data).unwrap();
            let chunk = tipb::Chunk::decode(streamed.data.unwrap().as_slice()).unwrap();
            assert_eq!(chunk.rows_data.as_deref().unwrap(), expected);
            assert!(stream.message().await.unwrap().is_none());
            let columns =
                tidb_executor::driver::infoschema_meta::table_schema("CLUSTER_PROCESSLIST")
                    .unwrap();
            let fields = vec![columns[1].1.clone(), columns[2].1.clone()];
            let mut chunk = tidb_chunk::chunk::Chunk::new(&fields, 1, 1024);
            chunk.append_datum(0, &Datum::UInt(10));
            chunk.append_datum(1, &Datum::Bytes(b"alice".to_vec()));
            let reply = client
                .coprocessor(request(scan(Some("alice"), tipb::EncodeType::TypeChunk)))
                .await
                .unwrap()
                .into_inner();
            let selected = tipb::SelectResponse::decode(reply.data).unwrap();
            assert_eq!(
                selected.chunks[0].rows_data.as_deref().unwrap(),
                tidb_chunk::codec::Codec::new(fields).encode(&chunk)
            );
            // The incoming identity loads default roles from the shared registry.
            // A store session blocks on its own runtime, so it runs off this one.
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    for sql in [
                        "CREATE ROLE observer",
                        "GRANT PROCESS ON *.* TO observer",
                        "GRANT observer TO alice",
                        "SET DEFAULT ROLE observer TO alice",
                    ] {
                        store
                            .run(sql)
                            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
                    }
                });
            });
            let reply = client
                .coprocessor(request(dag.clone()))
                .await
                .unwrap()
                .into_inner();
            let selected = tipb::SelectResponse::decode(reply.data).unwrap();
            let data = selected.chunks[0].rows_data.as_deref().unwrap();
            assert!(data.windows(3).any(|w| w == b"bob"));
            let mut invalid = dag.clone();
            invalid.output_offsets = vec![99];
            assert!(!client
                .coprocessor(request(invalid))
                .await
                .unwrap()
                .into_inner()
                .other_error
                .is_empty());
            let invalid = coprocessor::Request {
                tp: 103,
                data: vec![255],
                ..Default::default()
            };
            assert!(!client
                .coprocessor(invalid)
                .await
                .unwrap()
                .into_inner()
                .other_error
                .is_empty());
            use tikvpb::batch_commands_request::{request::Cmd, Request};
            let batch = tikvpb::BatchCommandsRequest {
                request_ids: vec![77, 78, 79],
                requests: vec![
                    Request {
                        cmd: Some(Cmd::Coprocessor(request(dag).encode_to_vec().into())),
                    },
                    Request {
                        cmd: Some(Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest {
                                test_id: 41,
                                delay_time: 0,
                            }
                            .encode_to_vec()
                            .into(),
                        )),
                    },
                    Request { cmd: None },
                ],
                ..Default::default()
            };
            let mut replies = client
                .batch_commands(futures::stream::iter([batch]))
                .await
                .unwrap()
                .into_inner();
            let reply = replies.message().await.unwrap().unwrap();
            assert_eq!(reply.request_ids, [77, 78, 79]);
            use tikvpb::batch_commands_response::response::Cmd as Out;
            assert!(matches!(reply.responses[0].cmd, Some(Out::Coprocessor(_))));
            let Some(Out::Empty(data)) = reply.responses[1].cmd.clone() else {
                panic!("empty reply")
            };
            assert_eq!(
                tikvpb::BatchCommandsEmptyResponse::decode(data)
                    .unwrap()
                    .test_id,
                41
            );
            assert!(replies.message().await.unwrap().is_none());
            let kill = tipb::DagRequest {
                executors: vec![tipb::Executor {
                    tp: Some(tipb::ExecType::TypeKill as i32),
                    kill: Some(tipb::Kill {
                        conn_id: Some(10),
                        query: Some(true),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let malformed = tikvpb::BatchCommandsRequest {
                request_ids: vec![1, 2],
                requests: vec![
                    Request {
                        cmd: Some(Cmd::Coprocessor(request(kill).encode_to_vec().into())),
                    },
                    Request {
                        cmd: Some(Cmd::Empty(vec![255].into())),
                    },
                ],
                ..Default::default()
            };
            let mut replies = client
                .batch_commands(futures::stream::iter([malformed]))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                replies.message().await.unwrap_err().code(),
                tonic::Code::Internal
            );
            assert_eq!(
                target.queries.load(Ordering::SeqCst),
                1,
                "malformed batch executed an earlier KILL"
            );
        });
        // Exercise the maintained synchronous client against this actual host.
        // These requests cross the status socket and share the outgoing fleet.
        let mut info = tidb_domain::serverinfo::ServerInfo::default();
        info.static_info.ip = server.local_addr().ip().to_string();
        info.static_info.status_port = server.local_addr().port() as usize;
        info.static_info.json_server_id = 17;
        let outbound = peer_client();
        let context = tidb_executor::StmtContext::for_query();
        let columns =
            tidb_executor::driver::infoschema_meta::table_schema("CLUSTER_PROCESSLIST").unwrap();
        let rows = outbound
            .scan(
                &[info.clone()],
                tidb_executor::infoschema_reader::memory_table_id("CLUSTER_PROCESSLIST").unwrap(),
                &columns,
                Some(("bob", "127.0.0.1")),
                &context,
                &SessionTimeZone::utc(),
                2,
            )
            .unwrap();
        assert!(rows.warnings.is_empty(), "{:?}", rows.warnings);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], Datum::UInt(12));
        let global_id = tidb_util::globalconn::Gcid {
            server_id: 17,
            local_conn_id: 42,
            is_64bits: false,
        };
        let _remote = processes.register(
            global_id.to_conn_id(),
            "remote".into(),
            "peer".into(),
            String::new(),
            Some(target.clone()),
        );
        outbound
            .kill(&[info], global_id, true, &context, &SessionTimeZone::utc())
            .unwrap();
        assert_eq!(target.queries.load(Ordering::SeqCst), 2);
        drop(outbound);
        drop(server);
    }

    fn column_ref(offset: i64, tp: i32) -> tipb::Expr {
        tipb::Expr {
            tp: Some(tipb::ExprType::ColumnRef as i32),
            val: Some({
                let mut bytes = Vec::new();
                tidb_codec::encode_int(&mut bytes, offset);
                bytes
            }),
            field_type: Some(tipb::FieldType {
                tp: Some(tp),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn aggregate(tp: tipb::ExprType, arg: tipb::Expr, ret_tp: i32) -> tipb::Expr {
        tipb::Expr {
            tp: Some(tp as i32),
            children: vec![arg],
            field_type: Some(tipb::FieldType {
                tp: Some(ret_tp),
                ..Default::default()
            }),
            agg_func_mode: Some(tipb::AggFunctionMode::Partial1Mode as i32),
            ..Default::default()
        }
    }

    /// Go `PBPlanBuilder` serves aggregation, TopN, limit and projection
    /// over a cluster-table scan (the chains Go's planner pushes to the TiDB
    /// store), and names any other executor in its refusal.
    #[test]
    fn peer_executor_chains_follow_go_pb_plan_builder() {
        let processes = ProcessRegistry::default();
        let mut store = Session::new();
        for sql in ["CREATE USER alice", "GRANT PROCESS ON *.* TO alice"] {
            store.run(sql).unwrap();
        }
        let privileges = store.privilege_manager().unwrap();
        let _alice = processes.register(
            10,
            "alice".into(),
            "127.0.0.1:1".into(),
            "test".into(),
            None,
        );
        let _bob = processes.register(12, "bob".into(), "127.0.0.1:2".into(), "test".into(), None);
        let service = PeerService::new(store_sessions(&store), processes, privileges, None);
        // Scan columns: USER (offset 0, varchar 15) then ID (offset 1, bigint 8).
        let run = |tail: Vec<tipb::Executor>, offsets: Vec<u32>| {
            let mut dag = scan(Some("alice"), tipb::EncodeType::TypeDefault);
            dag.executors.extend(tail);
            dag.output_offsets = offsets;
            let response = service.handle(request(dag));
            if !response.other_error.is_empty() {
                return Err(response.other_error);
            }
            let selected = tipb::SelectResponse::decode(response.data.as_ref()).unwrap();
            Ok(selected
                .chunks
                .iter()
                .flat_map(|chunk| chunk.rows_data.clone().unwrap_or_default())
                .collect::<Vec<u8>>())
        };
        let encode = |rows: &[Vec<Datum>]| -> Vec<u8> {
            rows.iter()
                .flat_map(|row| tidb_codec::encode_value(row).unwrap())
                .collect()
        };
        let agg = |group_by: Vec<tipb::Expr>, funcs: Vec<tipb::Expr>, tp: tipb::ExecType| {
            tipb::Executor {
                tp: Some(tp as i32),
                aggregation: Some(Box::new(tipb::Aggregation {
                    group_by,
                    agg_func: funcs,
                    ..Default::default()
                })),
                ..Default::default()
            }
        };

        // SELECT COUNT(*): one partial count, no group-by values.
        let count = aggregate(tipb::ExprType::Count, column_ref(1, 8), 8);
        assert_eq!(
            run(
                vec![agg(
                    vec![],
                    vec![count.clone()],
                    tipb::ExecType::TypeAggregation
                )],
                vec![0]
            ),
            Ok(encode(&[vec![Datum::Int(2)]]))
        );
        // GROUP BY user: count and the planner's firstrow per group.
        let first = aggregate(tipb::ExprType::First, column_ref(0, 15), 15);
        for tp in [
            tipb::ExecType::TypeAggregation,
            tipb::ExecType::TypeStreamAgg,
        ] {
            assert_eq!(
                run(
                    vec![agg(
                        vec![column_ref(0, 15)],
                        vec![count.clone(), first.clone()],
                        tp
                    )],
                    vec![0, 1]
                ),
                Ok(encode(&[
                    vec![Datum::Int(1), Datum::Bytes(b"alice".to_vec())],
                    vec![Datum::Int(1), Datum::Bytes(b"bob".to_vec())],
                ]))
            );
        }
        // TopN ordered by id descending.
        let top_n = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTopN as i32),
            top_n: Some(Box::new(tipb::TopN {
                order_by: vec![tipb::ByItem {
                    expr: Some(column_ref(1, 8)),
                    desc: Some(true),
                    ..Default::default()
                }],
                limit: Some(1),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            run(vec![top_n], vec![1, 0]),
            Ok(encode(&[vec![
                Datum::UInt(12),
                Datum::Bytes(b"bob".to_vec())
            ]]))
        );
        // Limit keeps the first rows.
        let limit = tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            limit: Some(Box::new(tipb::Limit {
                limit: Some(1),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            run(vec![limit], vec![0]),
            Ok(encode(&[vec![Datum::Bytes(b"alice".to_vec())]]))
        );
        // A projection reorders the output schema.
        let projection = tipb::Executor {
            tp: Some(tipb::ExecType::TypeProjection as i32),
            projection: Some(Box::new(tipb::Projection {
                exprs: vec![column_ref(1, 8)],
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            run(vec![projection], vec![0]),
            Ok(encode(&[vec![Datum::UInt(10)], vec![Datum::UInt(12)]]))
        );
        // Any other executor is refused with Go's message.
        let sender = tipb::Executor {
            tp: Some(tipb::ExecType::TypeExchangeSender as i32),
            ..Default::default()
        };
        assert_eq!(
            run(vec![sender], vec![0]),
            Err("this exec type TypeExchangeSender doesn't support yet".to_owned())
        );
    }

    /// Go `pbToBroadcastQuery`: three statements run on this instance, and
    /// everything else is refused before execution.
    #[test]
    fn peer_broadcast_query_runs_only_the_go_statements() {
        let ran = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let recorded = Arc::clone(&ran);
        let broadcast = |service: &PeerService, query: &str| {
            let dag = tipb::DagRequest {
                executors: vec![tipb::Executor {
                    tp: Some(tipb::ExecType::TypeBroadcastQuery as i32),
                    broadcast_query: Some(tipb::BroadcastQuery {
                        query: Some(query.to_owned()),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            };
            service.handle(request(dag)).other_error
        };
        let store = Session::new();
        let bare = PeerService::new(
            store_sessions(&store),
            ProcessRegistry::default(),
            PrivilegeRegistry::default(),
            None,
        );
        assert_eq!(
            broadcast(&bare, "ADMIN RELOAD BINDINGS"),
            "broadcast query execution is not installed"
        );
        let service = bare.with_broadcast_query_runner(Arc::new(move |query: &str| {
            recorded.lock().unwrap().push(query.to_owned());
            Ok(())
        }));
        for query in [
            "ADMIN RELOAD BINDINGS",
            "FLUSH STATS_DELTA test.t CLUSTER",
            "REFRESH STATS test.t",
        ] {
            assert_eq!(broadcast(&service, query), "", "{query}");
        }
        assert_eq!(
            broadcast(&service, "SELECT 1"),
            "unexpected statement SELECT 1 in broadcast query"
        );
        assert_eq!(
            broadcast(&service, "ADMIN CHECK TABLE t"),
            "unexpected admin statement ADMIN CHECK TABLE t in broadcast query"
        );
        assert_eq!(
            broadcast(&service, "FLUSH PRIVILEGES"),
            "unexpected flush statement FLUSH PRIVILEGES in broadcast query"
        );
        assert_eq!(
            *ran.lock().unwrap(),
            [
                "ADMIN RELOAD BINDINGS",
                "FLUSH STATS_DELTA test.t CLUSTER",
                "REFRESH STATS test.t"
            ]
        );
    }
}

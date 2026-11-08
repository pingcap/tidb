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

//! Shared outgoing TiDB memory-table/KILL requests (Go store/copr and executor).

use prost::Message;
use std::{sync::Arc, time::Duration};
use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_domain_serverinfo::serverinfo::ServerInfo;
use crate::{DriverError, MysqlError, StmtContext};
use tidb_distsql::remote_scan::PushdownScanColumn;
use tidb_proto::{coprocessor, tipb};
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::rpc::UnaryCancellation;

/// Go copr's `SendReqCtx` to a TiDB address (`buildTiDBMemCopTasks`' tasks):
/// the store's TiKV client, outside region routing.
trait PeerSender: Send + Sync {
    /// `kv_context` holds Go's `bo.GetCtx()` values (the statement's
    /// `RUDetails` the resource-control interceptor charges); `stats` is the
    /// task's `RegionRequestRuntimeStats`.
    fn send(
        &self,
        address: &str,
        request: coprocessor::Request,
        timeout: Duration,
        cancel: &UnaryCancellation,
        kv_context: &tidb_txnkv::StatementKvContext,
        stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
    ) -> Result<coprocessor::Response, tikv_client::Error>;
}

impl<S: tikv_client::PdClient> PeerSender for TikvStore<S> {
    fn send(
        &self,
        address: &str,
        mut request: coprocessor::Request,
        timeout: Duration,
        cancel: &UnaryCancellation,
        kv_context: &tidb_txnkv::StatementKvContext,
        stats: Option<Arc<tikv_client::RegionRequestRuntimeStats>>,
    ) -> Result<coprocessor::Response, tikv_client::Error> {
        self.block_on(async {
            // Go copr's `getEndpointType(kv.TiDB)`: the request's `StoreTp` is
            // TiDB, which never batches and which the mock store redirects.
            let send = tidb_copr::cop_store::with_statement_kv_context(
                kv_context,
                self.store().send_request_with_store_type_and_stats(
                    address,
                    &mut request,
                    tikv_client::tikv::EndpointType::TiDb,
                    timeout,
                    stats,
                ),
            );
            tokio::pin!(send);
            let response = loop {
                tokio::select! {
                    response = &mut send => break response,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if cancel.is_cancelled() {
                            break Err(tikv_client::Error::ContextCanceled);
                        }
                    }
                }
            }?;
            response
                .downcast::<coprocessor::Response>()
                .map(|response| *response)
                .map_err(|_| {
                    tikv_client::Error::StringError("unexpected coprocessor response".to_owned())
                })
        })
    }
}

/// Outgoing TiDB memory-table and KILL requests over the node's store, as
/// Go's go through copr over the `KVStore`. On the embedded store they reach
/// the peer through the mock store's `clientRedirector`, as Go's do.
pub struct ClusterPeerClient {
    sender: Arc<dyn PeerSender>,
}

/// Rows and peer warnings are published together at the statement boundary.
#[derive(Debug, Default)]
pub struct PeerRows {
    /// Decoded rows in the requested projection order.
    pub rows: Vec<Vec<Datum>>,
    /// Go SelectResponse warnings, preserving their MySQL codes.
    pub warnings: Vec<(u16, String)>,
}

/// Go buildTiDBMemCopTasks: ignore unavailable IPs and optionally filter server ID.
pub fn peer_addresses(servers: &[ServerInfo], server_id: Option<u64>) -> Vec<String> {
    servers
        .iter()
        .filter(|server| {
            let info = &server.static_info;
            info.ip != "<nil>"
                && server_id.is_none_or(|wanted| {
                    wanted
                        == info
                            .server_id_getter
                            .as_ref()
                            .map_or(info.json_server_id, |get| get())
                })
        })
        .map(|server| {
            tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
                &server.static_info.ip,
                server.static_info.status_port,
            )
        })
        .collect()
}

impl ClusterPeerClient {
    /// The client over `store`.
    pub fn new<S: tikv_client::PdClient>(store: TikvStore<S>) -> Self {
        Self {
            sender: Arc::new(store),
        }
    }

    /// Send Go's TypeKill DAG only to nodes matching the parsed global server ID.
    pub fn kill(
        &self,
        servers: &[ServerInfo],
        id: tidb_globalconn::Gcid,
        query: bool,
        ctx: &StmtContext,
        zone: &SessionTimeZone,
    ) -> Result<(), String> {
        if id.server_id == 0 {
            return Err("Unexpected ZERO ServerID. Please file a bug to the TiDB Team".into());
        }
        let mut dag = dag_context(ctx, zone);
        dag.executors.push(tipb::Executor {
            tp: Some(tipb::ExecType::TypeKill as i32),
            kill: Some(tipb::Kill {
                conn_id: Some(id.to_conn_id()),
                query: Some(query),
            }),
            ..Default::default()
        });
        // Go consumes the raw response once; successful SelectResponse warnings
        // (including unavailable peers) are not interpreted by killRemoteConn.
        self.with_cancellation(ctx, |cancel| {
            for address in peer_addresses(servers, Some(id.server_id)) {
                let _ = self.send(&address, &dag, &[], cancel)?;
            }
            Ok(())
        })
    }

    /// Read a complete memory table from discovered peers. Query projection and
    /// predicates remain with the ordinary local table executor after decoding.
    #[allow(clippy::too_many_arguments)]
    pub fn scan(
        &self,
        servers: &[ServerInfo],
        table_id: i64,
        columns: &[(String, FieldType)],
        user: Option<(&str, &str)>,
        ctx: &StmtContext,
        zone: &SessionTimeZone,
        concurrency: usize,
    ) -> Result<PeerRows, DriverError> {
        self.scan_projected(
            servers,
            table_id,
            columns,
            &(0..columns.len()).collect::<Vec<_>>(),
            user,
            ctx,
            zone,
            concurrency,
            None,
        )
    }

    /// Read selected columns using their original table IDs and the shared fleet.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_projected(
        &self,
        servers: &[ServerInfo],
        table_id: i64,
        columns: &[(String, FieldType)],
        offsets: &[usize],
        user: Option<(&str, &str)>,
        ctx: &StmtContext,
        zone: &SessionTimeZone,
        concurrency: usize,
        digests: Option<&std::collections::HashSet<String>>,
    ) -> Result<PeerRows, DriverError> {
        let selected = offsets
            .iter()
            .map(|offset| {
                columns
                    .get(*offset)
                    .map(|(_, field)| (*offset, field))
                    .ok_or_else(|| DriverError::unsupported("invalid cluster column projection"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut dag = dag_context(ctx, zone);
        dag.user = user.map(|(name, host)| tipb::UserIdentity {
            user_name: Some(name.to_owned()),
            user_host: Some(host.to_owned()),
        });
        let columns_pb = selected
            .iter()
            .map(|(offset, field_type)| {
                crate::cop_reader::column_to_proto(&PushdownScanColumn {
                    id: (*offset + 1) as i64,
                    field_type: (*field_type).clone(),
                    is_handle: false,
                    origin_default: None,
                })
                .map(|column| crate::dag_request::column_to_pb(&column))
                .ok_or_else(|| "unsupported cluster memory-table column".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(DriverError::unsupported)?;
        dag.output_offsets = (0..selected.len() as u32).collect();
        dag.executors.push(tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                columns: columns_pb,
                ..Default::default()
            }),
            ..Default::default()
        });
        if let Some(digests) = digests {
            if digests.is_empty() {
                return Ok(PeerRows::default());
            }
            let (offset, (_, field)) = selected
                .iter()
                .enumerate()
                .find(|(_, (original, _))| columns[*original].0.eq_ignore_ascii_case("DIGEST"))
                .ok_or_else(|| DriverError::unsupported("digest predicate needs DIGEST column"))?;
            let column = crate::cop_reader::column_to_proto(&PushdownScanColumn {
                id: (offset + 1) as i64,
                field_type: (*field).clone(),
                is_handle: false,
                origin_default: None,
            })
            .ok_or_else(|| DriverError::unsupported("unsupported digest column"))?;
            let mut values: Vec<_> = digests
                .iter()
                .map(|value| value.as_bytes().to_vec())
                .collect();
            values.sort();
            let condition = tidb_expr::pb_predicate::string_column_in_to_pb(
                tidb_expr::pb_predicate::StringPbOperand::Column {
                    offset,
                    mysql_type: column.tp,
                    flags: column.flag as u32,
                    flen: column.column_len,
                    charset: field.charset_name().to_owned(),
                    collation: field.collation_name().to_owned(),
                },
                values,
                field.collation_name(),
            )
            .map_err(|error| DriverError::unsupported(error.to_string()))?;
            dag.executors.push(tipb::Executor {
                tp: Some(tipb::ExecType::TypeSelection as i32),
                selection: Some(Box::new(tipb::Selection {
                    conditions: vec![condition],
                    ..Default::default()
                })),
                ..Default::default()
            });
        }
        let types: Vec<_> = selected.iter().map(|(_, field)| (*field).clone()).collect();
        self.select(servers, &dag, &[], &types, ctx, zone, concurrency)
    }

    /// Go `distsql.Select` for a request on the `kv.TiDB` store: copr's
    /// `buildTiDBMemCopTasks` sends `dag` with its key `ranges` to every
    /// listed TiDB (this node included), and the responses' rows, decoded as
    /// `types`, follow in task order.
    #[allow(clippy::too_many_arguments)]
    pub fn select(
        &self,
        servers: &[ServerInfo],
        dag: &tipb::DagRequest,
        ranges: &[coprocessor::KeyRange],
        types: &[FieldType],
        ctx: &StmtContext,
        zone: &SessionTimeZone,
        concurrency: usize,
    ) -> Result<PeerRows, DriverError> {
        self.with_cancellation(ctx, |cancel| {
            let mut result = PeerRows::default();
            for batch in peer_addresses(servers, None).chunks(concurrency.max(1)) {
                let responses = std::thread::scope(|scope| {
                    let handles: Vec<_> = batch
                        .iter()
                        .map(|address| {
                            scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                                self.send(address, dag, ranges, cancel)
                            }))
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|worker| {
                            worker
                                .join()
                                .map_err(|_| "cluster peer worker panicked".to_owned())?
                        })
                        .collect::<Result<Vec<_>, String>>()
                });
                ctx.statement_memory().check().map_err(DriverError::from)?;
                let responses = responses.map_err(DriverError::unsupported)?;
                for response in responses {
                    let response = tipb::SelectResponse::decode(response.data.as_ref())
                        .map_err(|error| DriverError::unsupported(error.to_string()))?;
                    if let Some(error) = response.error {
                        return Err(DriverError::Mysql(MysqlError::sql_error(
                            error.code.unwrap_or(1105) as u16,
                            error.msg.unwrap_or_default(),
                        )));
                    }
                    for warning in &response.warnings {
                        result.warnings.push((
                            warning.code.unwrap_or(1105) as u16,
                            warning.msg.clone().unwrap_or_default(),
                        ));
                    }
                    result
                        .rows
                        .extend(decode_select_response_rows(&response, types, zone)?);
                }
            }
            Ok(result)
        })
    }

    fn send(
        &self,
        address: &str,
        dag: &tipb::DagRequest,
        ranges: &[coprocessor::KeyRange],
        cancel: &UnaryCancellation,
    ) -> Result<coprocessor::Response, String> {
        let request = coprocessor::Request {
            tp: 103,
            start_ts: u64::MAX,
            data: dag.encode_to_vec(),
            ranges: ranges.to_vec(),
            ..Default::default()
        };
        let timeout = tidb_config::config_tree::config::get_global_config()
            .tikv_client
            .copr_req_timeout;
        let response = self.sender.send(
            address,
            request,
            Duration::from_nanos(timeout.max(1) as u64),
            cancel,
            &tidb_txnkv::StatementKvContext::default(),
            None,
        );
        if cancel.is_cancelled() {
            return Err("Query execution was interrupted".into());
        }
        let response = match response {
            Ok(response) => response,
            // Go handleTiDBSendReqErr turns transport failures into warning rows.
            Err(error) => {
                // `errors.Cause`: the connection annotation is not the kind.
                let (code, message) = if matches!(
                    error.cause(),
                    tikv_client::Error::GrpcAPI(status)
                        if status.code() == tonic::Code::DeadlineExceeded
                ) {
                    (9002, format!("TiDB server timeout, address is {address}"))
                } else {
                    (1105, error.to_string())
                };
                return Ok(coprocessor::Response {
                    data: tipb::SelectResponse {
                        warnings: vec![tipb::Error {
                            code: Some(code),
                            msg: Some(message),
                        }],
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .into(),
                    ..Default::default()
                });
            }
        };
        // Go copr `handleCopResponse`: `errors.Errorf("other error: %s", otherErr)`.
        if !response.other_error.is_empty() {
            return Err(format!("other error: {}", response.other_error));
        }
        if let Some(error) = response.region_error {
            return Err(format!("{error:?}"));
        }
        if response.locked.is_some() {
            return Err("unexpected lock in TiDB memory-table response".into());
        }
        Ok(response)
    }

    fn with_cancellation<T, E>(
        &self,
        ctx: &StmtContext,
        run: impl FnOnce(&UnaryCancellation) -> Result<T, E>,
    ) -> Result<T, E> {
        let cancel = UnaryCancellation::new();
        let killer = Arc::clone(ctx.statement_memory().sql_killer());
        let killed = killer.get_kill_event_chan();
        let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(0);
        std::thread::scope(|scope| {
            let cancel = &cancel;
            scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                crossbeam_channel::select! {
                    recv(killed) -> _ => cancel.cancel(),
                    recv(done_rx) -> _ => {},
                }
            }));
            if killer.get_kill_signal() != tidb_sqlkiller::KillSignal::UnspecifiedKillSignal {
                cancel.cancel();
            }
            let result = run(cancel);
            drop(done_tx);
            result
        })
    }
}

/// Go copr's `CopClient` for a request on the `kv.TiDB` store: `Send`
/// builds `buildTiDBMemCopTasks`' tasks over the TiDBs `servers` lists (Go
/// `infosync.GetAllServerInfo`, here the session's server-info syncer).
pub struct TiDbMemCopClient {
    peer: Arc<ClusterPeerClient>,
    /// The listed TiDBs, read at each send.
    servers: Arc<dyn Fn() -> Result<Vec<ServerInfo>, String> + Send + Sync>,
}

impl TiDbMemCopClient {
    /// The client sending through `peer` to the TiDBs `servers` lists.
    pub fn new(
        peer: Arc<ClusterPeerClient>,
        servers: Arc<dyn Fn() -> Result<Vec<ServerInfo>, String> + Send + Sync>,
    ) -> Self {
        Self { peer, servers }
    }
}

impl std::fmt::Debug for TiDbMemCopClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("TiDbMemCopClient").finish_non_exhaustive()
    }
}

impl tidb_txnkv::Client for TiDbMemCopClient {
    fn send(
        &self,
        request: &tidb_txnkv::TransportRequest,
    ) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
        let servers = (self.servers)()?;
        let metadata = request.metadata();
        // Go `buildTiDBMemCopTasks`.
        let server_id = (metadata.tidb_server_id > 0).then_some(metadata.tidb_server_id);
        let tasks = peer_addresses(&servers, server_id);
        // Go `copIterator.concurrency`: the request's, at most one worker
        // per task and at least one.
        let concurrency = usize::try_from(metadata.concurrency)
            .unwrap_or(0)
            .min(tasks.len())
            .max(1);
        let tagger_key = metadata
            .key_ranges
            .as_ref()
            .and_then(|ranges| ranges.first_partition_ranges().first().map(|range| range.start_key.as_bytes().to_vec()))
            .unwrap_or_default();
        let context = tidb_proto::kvrpcpb::Context {
            priority: metadata.priority.raw() as i32,
            isolation_level: metadata.isolation_level.raw() as i32,
            not_fill_cache: metadata.not_fill_cache,
            record_time_stat: true,
            record_scan_stat: true,
            task_id: metadata.task_id,
            resource_group_tag: metadata
                .resource_group_tagger
                .as_ref()
                .map(|tagger| tagger.encode_tag_with_key(&tagger_key))
                .unwrap_or_default(),
            request_source: tidb_copr::cop_paging::request_source(&metadata.request_source),
            resource_control_context: Some(tidb_proto::kvrpcpb::ResourceControlContext {
                resource_group_name: metadata.resource_group_name.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let cop_request = coprocessor::Request {
            context: Some(context),
            tp: metadata.request_type.raw(),
            start_ts: metadata.start_ts,
            data: metadata.data.clone().unwrap_or_default().into(),
            ranges: metadata
                .key_ranges
                .as_ref()
                .map(|ranges| {
                    ranges
                        .first_partition_ranges()
                        .iter()
                        .map(|range| coprocessor::KeyRange {
                            start: range.start_key.as_bytes().to_vec().into(),
                            end: range.end_key.as_bytes().to_vec().into(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
            ..Default::default()
        };
        Ok(Box::new(TiDbMemCopResponse {
            sender: Arc::clone(&self.peer.sender),
            tasks,
            request: cop_request,
            kv_context: request.kv_context(),
            cancellation: request
                .request_cancellation()
                .map(|handle| handle.unary_cancellation())
                .unwrap_or_default(),
            concurrency,
            mem_tracker: metadata.mem_tracker.clone(),
            results: None,
        }))
    }

    fn is_request_type_supported(&self, request_type: i64, sub_type: i64) -> bool {
        tidb_txnkv::RequestTypeSupportedChecker.is_request_type_supported(request_type, sub_type)
    }
}

/// Go copr's `copIterator` over TiDB tasks: every task's answer in task
/// order, each with the `CopRuntimeStats` its worker recorded.
pub struct TiDbMemCopResponse {
    sender: Arc<dyn PeerSender>,
    tasks: Vec<String>,
    request: coprocessor::Request,
    kv_context: tidb_txnkv::StatementKvContext,
    cancellation: UnaryCancellation,
    concurrency: usize,
    /// Go `copIterator.memTracker` (the request's `MemTracker`).
    mem_tracker: Option<Arc<tidb_memory::Tracker>>,
    /// The tasks' results once the workers ran them (Go's response channel).
    results: Option<
        std::collections::VecDeque<
            Result<(tidb_txnkv::QueryResultSubset, i64), tidb_txnkv::QueryResponseError>,
        >,
    >,
}

impl TiDbMemCopResponse {
    /// Go `copIteratorWorker.handleTaskOnce` for a TiDB task: the task's
    /// response and its `copResponse.MemSize`.
    fn handle_task_once(
        &self,
        address: &str,
    ) -> Result<(tidb_txnkv::QueryResultSubset, i64), tidb_txnkv::QueryResponseError> {
        // Go's `errors.Errorf`, an unknown (1105) error on the requester.
        let failed = |message: String| tidb_txnkv::QueryResponseError::Sql { code: 1105, message };
        let timeout = tidb_config::config_tree::config::get_global_config()
            .tikv_client
            .copr_req_timeout;
        let req_stats = Arc::new(tikv_client::RegionRequestRuntimeStats::new());
        let started = std::time::Instant::now();
        let response = self.sender.send(
            address,
            self.request.clone(),
            Duration::from_nanos(timeout.max(1) as u64),
            &self.cancellation,
            &self.kv_context,
            Some(Arc::clone(&req_stats)),
        );
        if self.cancellation.is_cancelled() {
            return Err(tidb_txnkv::QueryResponseError::Cancelled);
        }
        let response = match response {
            Ok(response) => response,
            Err(error) => return Ok(handle_tidb_send_req_err(&error, address)),
        };
        // Go `handleCopResponse` gives every TiDB response a detail and the
        // task's first start key.
        let start_key_len = self.request.ranges.first().map_or(0, |range| range.start.len());
        let mem_size = tidb_copr::cop_paging::cop_response_mem_size(&response, start_key_len, true);
        let cost_time = started.elapsed();
        // Go `handleCopResponse`: a TiDB's region error is the response's
        // error; its other error fails the task.
        if let Some(error) = response.region_error {
            return Err(failed(format!("error: {error:?}")));
        }
        if response.locked.is_some() {
            return Err(failed("unexpected lock in TiDB memory-table response".into()));
        }
        if !response.other_error.is_empty() {
            return Err(failed(format!("other error: {}", response.other_error)));
        }
        Ok((
            tidb_txnkv::QueryResultSubset {
                data: response.data.into(),
                runtime: Some(tidb_txnkv::ResponseRuntimeStats {
                    callee_address: address.to_owned(),
                    request_rpc_stats_present: req_stats.rpc_type_count() > 0,
                    resp_time: cost_time,
                    req_stats: Some(tidb_txnkv::RequestStatsSample(req_stats)),
                    ..Default::default()
                }),
            },
            mem_size,
        ))
    }
}

/// Go `copIteratorWorker.handleTiDBSendReqErr`: a TiDB that cannot be
/// reached answers a warning, with empty runtime statistics, and its
/// `copResponse.MemSize`.
fn handle_tidb_send_req_err(error: &tikv_client::Error, address: &str) -> (tidb_txnkv::QueryResultSubset, i64) {
    // `errors.Cause`: the connection annotation is not the kind.
    let (code, message) = if matches!(
        error.cause(),
        tikv_client::Error::GrpcAPI(status) if status.code() == tonic::Code::DeadlineExceeded
    ) {
        (9002, format!("TiDB server timeout, address is {address}"))
    } else {
        (1105, error.to_string())
    };
    let response = coprocessor::Response {
        data: tipb::SelectResponse {
            warnings: vec![tipb::Error {
                code: Some(code),
                msg: Some(message),
            }],
            ..Default::default()
        }
        .encode_to_vec()
        .into(),
        ..Default::default()
    };
    let mem_size = tidb_copr::cop_paging::cop_response_mem_size(&response, 0, true);
    (
        tidb_txnkv::QueryResultSubset {
            data: response.data.into(),
            runtime: None,
        },
        mem_size,
    )
}

impl tidb_txnkv::Response for TiDbMemCopResponse {
    fn next(&mut self) -> Result<Option<tidb_txnkv::QueryResultSubset>, tidb_txnkv::QueryResponseError> {
        if self.results.is_none() {
            // Go `copIterator.open`'s workers, `concurrency` at a time.
            let this = &*self;
            let mut results = std::collections::VecDeque::with_capacity(self.tasks.len());
            for batch in this.tasks.chunks(this.concurrency) {
                let answers: Vec<_> = std::thread::scope(|scope| {
                    let handles: Vec<_> = batch
                        .iter()
                        .map(|address| {
                            scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                                this.handle_task_once(address)
                            }))
                        })
                        .collect();
                    handles
                        .into_iter()
                        .map(|worker| {
                            worker.join().unwrap_or_else(|_| {
                                Err(tidb_txnkv::QueryResponseError::Source(
                                    "cluster peer worker panicked".to_owned(),
                                ))
                            })
                        })
                        .collect()
                });
                // Go `checkRespOOM`, as each worker's response arrives.
                for answer in answers.iter().flatten() {
                    if let Some(tracker) = &self.mem_tracker {
                        tracker.consume(answer.1);
                    }
                }
                results.extend(answers);
            }
            self.results = Some(results);
        }
        if self.cancellation.is_cancelled() {
            return Err(tidb_txnkv::QueryResponseError::Cancelled);
        }
        match self.results.as_mut().and_then(std::collections::VecDeque::pop_front) {
            None => Ok(None),
            // Go `copIterator.Next`'s `memTrackerConsumeResp`.
            Some(Ok((subset, mem_size))) => {
                if let Some(tracker) = &self.mem_tracker {
                    tracker.consume(-mem_size);
                }
                Ok(Some(subset))
            }
            Some(Err(error)) => {
                tidb_txnkv::Response::close(self);
                Err(error)
            }
        }
    }

    fn close(&mut self) {
        // The responses never taken leave the tracker with the iterator.
        for (_, mem_size) in self.results.take().into_iter().flatten().flatten() {
            if let Some(tracker) = &self.mem_tracker {
                tracker.consume(-mem_size);
            }
        }
        self.results = Some(std::collections::VecDeque::new());
    }

    fn cop_info(&self) -> Option<tidb_txnkv::CopInfo> {
        // Go `BuildCopIterator` times only `buildCopTasks`; the TiDB tasks
        // leave `buildTaskElapsed` zero.
        Some(tidb_txnkv::CopInfo {
            concurrency: self.concurrency,
            extra_concurrency: 0,
            build_task_elapsed: Duration::ZERO,
        })
    }
}

/// Go `selectResult`'s rows of one response: `readRowsData` for the default
/// encoding (in the statement's location), `readFromChunk` for chunk RPC.
///
/// # Errors
/// A malformed chunk.
pub fn decode_select_response_rows(
    response: &tipb::SelectResponse,
    types: &[FieldType],
    zone: &SessionTimeZone,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    let mut rows = Vec::new();
    if response.encode_type == Some(tipb::EncodeType::TypeChunk as i32) {
        let codec = tidb_chunk::codec::Codec::new(types.to_vec());
        for chunk in &response.chunks {
            let mut data = chunk.rows_data.as_deref().unwrap_or_default();
            while !data.is_empty() {
                let mut decoded = tidb_chunk::chunk::Chunk::new(types, 0, 1024);
                data = codec
                    .try_decode_to_chunk(data, &mut decoded)
                    .map_err(|error| DriverError::unsupported(error.to_string()))?;
                for index in 0..decoded.num_rows() {
                    rows.push(decoded.get_row(index).get_datum_row(types));
                }
            }
        }
        return Ok(rows);
    }
    for chunk in tidb_distsql::decode_response_chunks(response)
        .map_err(|error| DriverError::unsupported(error.to_string()))?
    {
        rows.extend(
            chunk
                .decode_default_datums_in_timezone(types, zone)
                .map_err(|error| DriverError::unsupported(error.to_string()))?,
        );
    }
    Ok(rows)
}

fn dag_context(ctx: &StmtContext, zone: &SessionTimeZone) -> tipb::DagRequest {
    let (name, offset) = zone.dag_zone();
    tipb::DagRequest {
        time_zone_name: Some(name),
        time_zone_offset: Some(offset),
        flags: Some(ctx.push_down_flags()),
        encode_type: Some(tipb::EncodeType::TypeDefault as i32),
        ..Default::default()
    }
}

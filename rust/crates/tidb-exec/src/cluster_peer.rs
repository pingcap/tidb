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
use tidb_domain::serverinfo::ServerInfo;
use tidb_executor::{remote_scan::PushdownScanColumn, DriverError, MysqlError, StmtContext};
use tidb_proto::{coprocessor, tipb};
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::rpc::UnaryCancellation;

/// Go copr's `SendReqCtx` to a TiDB address (`buildTiDBMemCopTasks`' tasks):
/// the store's TiKV client, outside region routing.
trait PeerSender: Send + Sync {
    fn send(
        &self,
        address: &str,
        request: coprocessor::Request,
        timeout: Duration,
        cancel: &UnaryCancellation,
    ) -> Result<coprocessor::Response, tikv_client::Error>;
}

impl<S: tikv_client::PdClient> PeerSender for TikvStore<S> {
    fn send(
        &self,
        address: &str,
        mut request: coprocessor::Request,
        timeout: Duration,
        cancel: &UnaryCancellation,
    ) -> Result<coprocessor::Response, tikv_client::Error> {
        self.block_on(async {
            // Go copr's `getEndpointType(kv.TiDB)`: the request's `StoreTp` is
            // TiDB, which never batches and which the mock store redirects.
            let send = self.store().send_request_with_store_type(
                address,
                &mut request,
                tikv_client::tikv::EndpointType::TiDb,
                timeout,
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
            tidb_domain::serverinfo_syncer::join_host_port(
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
        id: tidb_util::globalconn::Gcid,
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
                let _ = self.send(&address, &dag, cancel)?;
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
                crate::cop_scan::scan_column(&PushdownScanColumn {
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
            let column = crate::cop_scan::scan_column(&PushdownScanColumn {
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
        self.with_cancellation(ctx, |cancel| {
            let mut result = PeerRows::default();
            for batch in peer_addresses(servers, None).chunks(concurrency.max(1)) {
                let responses = std::thread::scope(|scope| {
                    let handles: Vec<_> = batch
                        .iter()
                        .map(|address| {
                            let dag = &dag;
                            scope.spawn(move || self.send(address, dag, cancel))
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
                    for chunk in tidb_distsql::decode_response_chunks(&response)
                        .map_err(|error| DriverError::unsupported(error.to_string()))?
                    {
                        result.rows.extend(
                            chunk
                                .decode_default_datums_in_timezone(&types, zone)
                                .map_err(|error| DriverError::unsupported(error.to_string()))?,
                        );
                    }
                }
            }
            Ok(result)
        })
    }

    fn send(
        &self,
        address: &str,
        dag: &tipb::DagRequest,
        cancel: &UnaryCancellation,
    ) -> Result<coprocessor::Response, String> {
        let request = coprocessor::Request {
            tp: 103,
            start_ts: u64::MAX,
            data: dag.encode_to_vec(),
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
        if !response.other_error.is_empty() {
            return Err(response.other_error);
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
            scope.spawn(move || {
                crossbeam_channel::select! {
                    recv(killed) -> _ => cancel.cancel(),
                    recv(done_rx) -> _ => {},
                }
            });
            if killer.get_kill_signal() != tidb_util::sqlkiller::KillSignal::UnspecifiedKillSignal {
                cancel.cancel();
            }
            let result = run(cancel);
            drop(done_tx);
            result
        })
    }
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

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
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext, UnaryCancellation};
use tidb_txnkv::{DirectUnaryClient, DirectUnaryRequest, EndpointType};

/// Capability borrowing the production store RPC fleet. Unistore may retain an
/// owned fleet because its storage requests do not otherwise need network RPC.
pub struct ClusterPeerClient {
    transport: TonicCoprocessorClient,
}

/// Rows and peer warnings are published together at the statement boundary.
#[derive(Debug, Default)]
pub struct PeerRows {
    /// Decoded rows, including the instance column supplied by the remote node.
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
    /// Retains a request capability; callers keep the unique process owner alive.
    pub fn new(transport: TonicCoprocessorClient) -> Self {
        Self { transport }
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
        let mut dag = dag_context(ctx, zone);
        dag.user = user.map(|(name, host)| tipb::UserIdentity {
            user_name: Some(name.to_owned()),
            user_host: Some(host.to_owned()),
        });
        let columns_pb = columns
            .iter()
            .enumerate()
            .map(|(offset, (_, field_type))| {
                crate::cop_scan::scan_column(&PushdownScanColumn {
                    id: (offset + 1) as i64,
                    field_type: field_type.clone(),
                    is_handle: false,
                    origin_default: None,
                })
                .map(|column| crate::dag_request::column_to_pb(&column))
                .ok_or_else(|| "unsupported cluster memory-table column".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(DriverError::unsupported)?;
        dag.output_offsets = (0..columns.len() as u32).collect();
        dag.executors.push(tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(table_id),
                columns: columns_pb,
                ..Default::default()
            }),
            ..Default::default()
        });
        let types: Vec<_> = columns.iter().map(|(_, field)| field.clone()).collect();
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
                        return Err(DriverError::Mysql(MysqlError::new(
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
        let request = DirectUnaryRequest {
            endpoint: EndpointType::TiDb,
            replica_read_type: tidb_txnkv::ClientReplicaReadType::Leader,
            replica_read: false,
            stale_read: false,
            input_request_source: String::new(),
            predicted_read_bytes: 0,
            read_replica_scope: "global".into(),
            txn_scope: "global".into(),
            context: Default::default(),
            encoded_request: coprocessor::Request {
                tp: 103,
                start_ts: u64::MAX,
                data: dag.encode_to_vec(),
                ..Default::default()
            }
            .encode_to_vec(),
        };
        let timeout = tidb_config::config_tree::config::get_global_config()
            .tikv_client
            .copr_req_timeout;
        let call =
            UnaryCallContext::new(Duration::from_nanos(timeout.max(1) as u64), cancel.clone());
        let response = self
            .transport
            .clone()
            .send_request_with_context(address, &request, &call);
        if cancel.is_cancelled() {
            return Err("Query execution was interrupted".into());
        }
        let response = match response {
            Ok(response) => coprocessor::Response::decode(response.encoded_response)
                .map_err(|error| error.to_string())?,
            // Go handleTiDBSendReqErr turns transport failures into warning rows.
            Err(error) => {
                let (code, message) = if matches!(
                    error,
                    tidb_txnkv::rpc::DirectUnaryClientError::Timeout { .. }
                ) || error.grpc_code()
                    == Some(tidb_txnkv::rpc::DirectUnaryGrpcCode::DeadlineExceeded)
                {
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

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

//! Go's status-port TiKV service for local KILL and cluster process snapshots.
use prost::Message;
use std::{pin::Pin, sync::Arc};
use tidb_datatype::SessionTimeZone;
use tidb_proto::{coprocessor, tikvpb, tipb};
use tidb_session::{privilege::PrivilegeRegistry, process::ProcessRegistry, Session};

type ReplyStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, tonic::Status>> + Send>>;

/// Borrows the node's process and privilege owners; never redispatches to peers.
#[derive(Clone)]
pub struct PeerService {
    processes: ProcessRegistry,
    privileges: PrivilegeRegistry,
    server_info: Option<Arc<tidb_domain::serverinfo_syncer::Syncer>>,
}

impl PeerService {
    pub(crate) fn new(
        processes: ProcessRegistry,
        privileges: PrivilegeRegistry,
        server_info: Option<Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    ) -> Self {
        Self {
            processes,
            privileges,
            server_info,
        }
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
        let [executor] = dag.executors.as_slice() else {
            return Err("TiDB peer requires one supported local executor".into());
        };
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
        let mut session = Session::new();
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
                if scan.table_id != tidb_session::infoschema::memory_table_id("CLUSTER_PROCESSLIST")
                {
                    return Err("unsupported TiDB cluster table".into());
                }
                let columns =
                    tidb_executor::driver::infoschema_meta::table_schema("CLUSTER_PROCESSLIST")
                        .ok_or("missing cluster process schema")?;
                let selected = scan
                    .columns
                    .iter()
                    .map(|col| {
                        let index = col
                            .column_id
                            .unwrap_or(0)
                            .checked_sub(1)
                            .and_then(|id| usize::try_from(id).ok())
                            .ok_or("invalid process column ID")?;
                        columns.get(index).ok_or("invalid process column ID")?;
                        Ok(index)
                    })
                    .collect::<Result<Vec<_>, &str>>()?;
                let projection = dag
                    .output_offsets
                    .iter()
                    .map(|offset| {
                        selected
                            .get(*offset as usize)
                            .copied()
                            .ok_or("invalid process output offset")
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let fields: Vec<_> = projection
                    .iter()
                    .map(|index| columns[*index].1.clone())
                    .collect();
                let rows = session.local_cluster_process_list_rows(&self.processes, &zone);
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

fn error_response(error: String) -> coprocessor::Response {
    coprocessor::Response {
        other_error: error,
        ..Default::default()
    }
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
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tidb_datatype::Datum;
    use tidb_session::process::ProcessKillTarget;

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
                    table_id: tidb_session::infoschema::memory_table_id("CLUSTER_PROCESSLIST"),
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

    #[test]
    fn peer_host_batch_generated_protocol_kill_scan_stream_and_batch() {
        let processes = ProcessRegistry::default();
        let privileges = PrivilegeRegistry::default();
        privileges.create_user("alice", "%", "");
        privileges.create_user("bob", "%", "");
        let target = Arc::new(Target::default());
        let _alice = processes.register(
            10,
            "alice".into(),
            "127.0.0.1:1".into(),
            "test".into(),
            Some(target.clone()),
        );
        let _bob = processes.register(12, "bob".into(), "127.0.0.1:2".into(), "test".into(), None);
        let service = PeerService::new(processes.clone(), privileges.clone(), None);
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
            let role = ("observer".to_owned(), "%".to_owned());
            let account = ("alice".to_owned(), "%".to_owned());
            privileges.create_role(&role.0, &role.1);
            privileges.grant(
                &role.0,
                &role.1,
                tidb_session::privilege::GlobalPriv::Process.mask(),
            );
            privileges.grant_role(&role, &account);
            privileges.set_default_roles(&account, &[role]);
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
        let owner = tidb_txnkv::rpc::TonicCoprocessorClient::new().unwrap();
        let outbound = tidb_exec::cluster_peer::ClusterPeerClient::new(owner);
        let context = tidb_executor::StmtContext::for_query();
        let columns =
            tidb_executor::driver::infoschema_meta::table_schema("CLUSTER_PROCESSLIST").unwrap();
        let rows = outbound
            .scan(
                &[info.clone()],
                tidb_session::infoschema::memory_table_id("CLUSTER_PROCESSLIST").unwrap(),
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
}

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
use tidb_session::{privilege::PrivilegeRegistry, process::ProcessRegistry, Session};

type ReplyStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, tonic::Status>> + Send>>;

/// Borrows the node's process and privilege owners; never redispatches to peers.
#[derive(Clone)]
pub struct PeerService {
    session_factory: Arc<dyn Fn() -> Session + Send + Sync>,
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
            session_factory: Arc::new(Session::new),
            processes,
            privileges,
            server_info,
        }
    }

    /// Supplies fresh metadata and the existing usage collector without retaining
    /// a client connection, transaction, or the whole server session factory.
    pub(crate) fn with_session_factory(
        mut self,
        factory: Arc<dyn Fn() -> Session + Send + Sync>,
    ) -> Self {
        self.session_factory = factory;
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
        // Only key/index diagnostics need a catalog snapshot. KILL and the
        // registry/counter readers must not rebuild schema metadata.
        let needs_catalog = executor.tp == Some(tipb::ExecType::TypeTableScan as i32)
            && executor.tbl_scan.as_ref().is_some_and(|scan| {
                ["CLUSTER_DEADLOCKS", "CLUSTER_TIDB_INDEX_USAGE"]
                    .iter()
                    .any(|name| scan.table_id == tidb_session::infoschema::memory_table_id(name))
            });
        let mut session = if needs_catalog {
            (self.session_factory)()
        } else {
            Session::new()
        };
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
                let table = tidb_executor::driver::infoschema_meta::CLUSTER_TABLES
                    .iter()
                    .map(|(name, _)| *name)
                    .find(|name| scan.table_id == tidb_session::infoschema::memory_table_id(name))
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
                let projection = dag
                    .output_offsets
                    .iter()
                    .map(|offset| {
                        selected
                            .get(*offset as usize)
                            .copied()
                            .ok_or("invalid cluster table output offset")
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let fields: Vec<_> = projection
                    .iter()
                    .map(|index| columns[*index].1.clone())
                    .collect();
                let rows = session
                    .local_cluster_table_rows(table, Some(&self.processes), &zone)
                    .map_err(|error| error.to_string())?;
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
        assert!(
            matches!(error, tidb_executor::DriverError::SpecificAccessDenied(ref privilege) if privilege == "PROCESS"),
            "{error:?}"
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
        let service = PeerService::new(
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
                table_id: tidb_session::infoschema::memory_table_id(table),
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
        let privileges = PrivilegeRegistry::default();
        privileges.create_user("diagnostic_alice", "%", "");
        let mut producer = Session::new();
        producer.run("CREATE DATABASE diagnostic_batch").unwrap();
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
        let service = PeerService::new(processes.clone(), privileges.clone(), None)
            .with_session_factory(Arc::new(move || {
                let mut session = Session::with_catalog(metadata.clone());
                session.set_index_usage_collector(counters.clone());
                session
            }));
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
                table.table_id,
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
                key: tidb_tablecodec::table_key::encode_row_key_with_handle(
                    table_id,
                    &tidb_tablecodec::table_key::RecordHandle::Int(9),
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
        let outbound = tidb_exec::cluster_peer::ClusterPeerClient::new(
            tidb_txnkv::rpc::TonicCoprocessorClient::new().unwrap(),
        );
        let read = |table: &str| {
            let columns = tidb_executor::driver::infoschema_meta::table_schema(table).unwrap();
            let rows = outbound.scan(
                &[info.clone()],
                tidb_session::infoschema::memory_table_id(table).unwrap(),
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
        assert!(
            read("CLUSTER_TIDB_INDEX_USAGE").unwrap().1.is_empty(),
            "ungranted tables must stay hidden"
        );
        privileges.grant(
            "diagnostic_alice",
            "%",
            tidb_session::privilege::GlobalPriv::Process.mask()
                | tidb_session::privilege::GlobalPriv::Select.mask(),
        );
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
        assert!(
            matches!(error, tidb_executor::DriverError::SpecificAccessDenied(ref privilege) if privilege == "PROCESS"),
            "{error:?}"
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
        let service = PeerService::new(
            ProcessRegistry::default(),
            PrivilegeRegistry::default(),
            None,
        );
        let mut dag = scan(None, tipb::EncodeType::TypeDefault);
        dag.executors[0].tbl_scan.as_mut().unwrap().table_id =
            tidb_session::infoschema::memory_table_id("CLUSTER_TRX_SUMMARY");
        let response = service.handle(request(dag));
        assert!(response.other_error.is_empty(), "{}", response.other_error);
        tipb::SelectResponse::decode(response.data).unwrap();
    }

    #[test]
    fn cluster_summary_batch_shared_client_reads_live_summaries_and_admission() {
        let privileges = PrivilegeRegistry::default();
        privileges.create_user("summary_alice", "%", "");
        privileges.create_user("summary_bob", "%", "");
        let mut producer = Session::new();
        producer.set_user("summary_alice@%".into(), "summary_alice@localhost".into());
        producer.run("SELECT 73 + 8").unwrap();
        producer.set_user("summary_bob@%".into(), "summary_bob@localhost".into());
        producer.run("SELECT 73 * 8").unwrap();
        let mut identity = tidb_domain::serverinfo::ServerInfo::default();
        identity.static_info.id = "summary-node".into();
        identity.static_info.ip = "192.0.2.7".into();
        identity.static_info.status_port = 10080;
        let syncer = Arc::new(tidb_domain::serverinfo_syncer::Syncer::new(identity, None));
        let service =
            PeerService::new(ProcessRegistry::default(), privileges.clone(), Some(syncer));
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
        let outbound = Arc::new(tidb_exec::cluster_peer::ClusterPeerClient::new(
            tidb_txnkv::rpc::TonicCoprocessorClient::new().unwrap(),
        ));
        for table in [
            "CLUSTER_STATEMENTS_SUMMARY",
            "CLUSTER_STATEMENTS_SUMMARY_HISTORY",
            "CLUSTER_TIDB_STATEMENTS_STATS",
        ] {
            let columns = tidb_executor::driver::infoschema_meta::table_schema(table).unwrap();
            let rows = outbound
                .scan(
                    &[address.clone()],
                    tidb_session::infoschema::memory_table_id(table).unwrap(),
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
            // Both generated result encodings support reordered and repeated outputs.
            for encoding in [tipb::EncodeType::TypeDefault, tipb::EncodeType::TypeChunk] {
                let mut dag = scan(Some("summary_alice"), encoding);
                dag.executors[0].tbl_scan = Some(tipb::TableScan {
                    table_id: tidb_session::infoschema::memory_table_id(table),
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
                tidb_session::infoschema::memory_table_id("CLUSTER_STATEMENTS_SUMMARY_EVICTED")
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
        let role = ("summary_observer".to_owned(), "%".to_owned());
        let account = ("summary_alice".to_owned(), "%".to_owned());
        privileges.create_role(&role.0, &role.1);
        privileges.grant(
            &role.0,
            &role.1,
            tidb_session::privilege::GlobalPriv::Process.mask(),
        );
        privileges.grant_role(&role, &account);
        privileges.set_default_roles(&account, &[role]);
        scan_evicted().unwrap();
        // Exercise SQL -> discovered peer -> receiver, excluding an unpublished
        // local identity. This is a discovery fixture, not another RPC harness.
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
        address.static_info.id = "remote-summary".into();
        let discovery = Arc::new(Discovery(vec![(
            tidb_domain::serverinfo_syncer::server_info_key_path("remote-summary"),
            address.marshal().unwrap(),
        )]));
        let mut local = tidb_domain::serverinfo::ServerInfo::default();
        local.static_info.id = "unpublished-local".into();
        local.static_info.ip = "127.0.0.1".into();
        let mut origin = Session::new();
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
        let mut session = Session::new();
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
        assert!(
            matches!(error, tidb_executor::DriverError::NotSupportedYet(_)),
            "{error:?}"
        );
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

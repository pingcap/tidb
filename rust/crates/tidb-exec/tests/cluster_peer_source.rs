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

//! Go simple.killRemoteConn and store/copr memory-table requests on the real wire.
use prost::Message;
use std::sync::{mpsc, Arc, Mutex};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, SessionTimeZone};
use tidb_domain::serverinfo::ServerInfo;
use tidb_exec::cluster_peer::{peer_addresses, ClusterPeerClient};
use tidb_executor::StmtContext;
use tidb_proto::{
    coprocessor,
    tikvpb::tikv_server::{Tikv, TikvServer},
    tipb,
};
use tidb_txnkv::rpc::TonicCoprocessorClient;

#[derive(Clone, Default)]
struct Service(Arc<Mutex<Vec<(coprocessor::Request, tipb::DagRequest)>>>);
#[tonic::async_trait]
impl Tikv for Service {
    async fn coprocessor(
        &self,
        request: tonic::Request<coprocessor::Request>,
    ) -> Result<tonic::Response<coprocessor::Response>, tonic::Status> {
        let request = request.into_inner();
        let dag = tipb::DagRequest::decode(request.data.as_slice()).unwrap();
        self.0.lock().unwrap().push((request, dag.clone()));
        if dag.executors[0].kill.is_some() {
            return Ok(tonic::Response::new(coprocessor::Response::default()));
        }
        match dag.executors[0].tbl_scan.as_ref().unwrap().table_id {
            Some(99) => {
                return std::future::pending().await;
            }
            Some(100) => {
                return Ok(tonic::Response::new(coprocessor::Response {
                    other_error: "remote executor failed".into(),
                    ..Default::default()
                }));
            }
            Some(101) => {
                return Err(tonic::Status::unavailable("peer shutting down"));
            }
            Some(102) => {
                return Ok(tonic::Response::new(coprocessor::Response {
                    data: tipb::SelectResponse {
                        error: Some(tipb::Error {
                            code: Some(1142),
                            msg: Some("table access denied".into()),
                        }),
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .into(),
                    ..Default::default()
                }));
            }
            Some(103) => return Err(tonic::Status::deadline_exceeded("deadline")),
            _ => {}
        }
        let rows =
            tidb_codec::encode_value(&[Datum::Bytes(b"peer:10080".to_vec()), Datum::UInt(42)])
                .unwrap();
        Ok(tonic::Response::new(coprocessor::Response {
            data: tipb::SelectResponse {
                chunks: vec![tipb::Chunk {
                    rows_data: Some(rows.into()),
                    ..Default::default()
                }],
                warnings: vec![tipb::Error {
                    code: Some(1234),
                    msg: Some("peer warning".into()),
                }],
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
            ..Default::default()
        }))
    }
}

#[test]
fn cluster_peer_batch_kill_and_memory_scan_share_protocol_and_fleet() {
    let service = Service::default();
    let captured = service.0.clone();
    let (ready, receive) = mpsc::channel();
    let (stop, done) = tokio::sync::oneshot::channel();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready.send(listener.local_addr().unwrap()).unwrap();
                tonic::transport::Server::builder()
                    .add_service(TikvServer::new(service))
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        async {
                            let _ = done.await;
                        },
                    )
                    .await
                    .unwrap();
            });
    });
    let address = receive.recv().unwrap();
    let mut server = ServerInfo::default();
    server.static_info.ip = address.ip().to_string();
    server.static_info.status_port = address.port() as usize;
    server.static_info.json_server_id = 17;
    let mut unavailable = server.clone();
    unavailable.static_info.ip = "<nil>".into();
    assert_eq!(
        peer_addresses(&[server.clone(), unavailable], Some(17)),
        vec![address.to_string()]
    );
    assert!(peer_addresses(&[server.clone()], Some(18)).is_empty());
    let owner = TonicCoprocessorClient::new().unwrap();
    let client = ClusterPeerClient::new(owner.clone());
    let context = StmtContext::for_query();
    let zone = SessionTimeZone::utc();
    let id = tidb_util::globalconn::Gcid {
        server_id: 17,
        local_conn_id: 42,
        is_64bits: false,
    };
    client
        .kill(&[server.clone()], id, true, &context, &zone)
        .unwrap();
    let columns = vec![
        ("INSTANCE".into(), FieldType::new(FieldTypeCode::Varchar)),
        ("ID".into(), FieldType::new(FieldTypeCode::LongLong)),
    ];
    let result = client
        .scan(
            &[server.clone()],
            123,
            &columns,
            Some(("alice", "localhost")),
            &context,
            &zone,
            2,
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Datum::Bytes(b"peer:10080".to_vec()));
    assert_eq!(result.warnings, vec![(1234, "peer warning".into())]);
    let requests = captured.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, dag) in requests.iter() {
        assert_eq!(request.tp, 103);
        assert_eq!(request.start_ts, u64::MAX);
        assert_eq!(dag.flags, Some(context.push_down_flags()));
        assert_eq!(dag.time_zone_offset, Some(0));
    }
    assert_eq!(
        requests[0].1.executors[0].kill.as_ref().unwrap().conn_id,
        Some(id.to_conn_id())
    );
    assert_eq!(
        requests[0].1.executors[0].kill.as_ref().unwrap().query,
        Some(true)
    );
    assert!(requests[0].1.user.is_none());
    assert_eq!(
        requests[1].1.user.as_ref().unwrap().user_name.as_deref(),
        Some("alice")
    );
    assert_eq!(
        requests[1].1.user.as_ref().unwrap().user_host.as_deref(),
        Some("localhost")
    );
    assert_eq!(requests[1].1.output_offsets, vec![0, 1]);
    let scan = requests[1].1.executors[0].tbl_scan.as_ref().unwrap();
    assert_eq!(scan.table_id, Some(123));
    assert_eq!(
        scan.columns.iter().map(|c| c.column_id).collect::<Vec<_>>(),
        vec![Some(1), Some(2)]
    );
    assert_eq!(owner.active_address_count(), 1);
    drop(requests);
    let error = client
        .scan(&[server.clone()], 100, &columns, None, &context, &zone, 1)
        .unwrap_err();
    assert!(error.to_string().contains("remote executor failed"));
    let result = client
        .scan(&[server.clone()], 101, &columns, None, &context, &zone, 1)
        .unwrap();
    assert!(result.rows.is_empty());
    assert!(result.warnings[0].1.contains("peer shutting down"));
    let error = client
        .scan(&[server.clone()], 102, &columns, None, &context, &zone, 1)
        .unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1142);
    let result = client
        .scan(&[server.clone()], 103, &columns, None, &context, &zone, 1)
        .unwrap();
    assert_eq!(
        result.warnings,
        vec![(9002, format!("TiDB server timeout, address is {address}"))]
    );
    let memory = tidb_executor::StatementMemory::default();
    let killer = memory.sql_killer().clone();
    let cancelled_context = StmtContext::for_query_with_memory(memory);
    std::thread::scope(|scope| {
        let call = scope
            .spawn(|| client.scan(&[server], 99, &columns, None, &cancelled_context, &zone, 1));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while captured.lock().unwrap().len() < 7 {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        killer.send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
        assert!(call
            .join()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("interrupted"));
    });
    drop(client);
    drop(owner);
    stop.send(()).unwrap();
    worker.join().unwrap();
}

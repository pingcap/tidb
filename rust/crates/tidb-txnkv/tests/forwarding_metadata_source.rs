// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use prost::Message;
use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
use tidb_proto::tikvpb::{BatchCommandsRequest, BatchCommandsResponse};
use tidb_proto::{CoprocessorRequest, CoprocessorResponse, KvrpcContext};
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::{
    ClientReplicaReadType, DirectUnaryClient, DirectUnaryRequest, EndpointType, UnaryCallContext,
};

const FORWARD_METADATA_KEY: &str = "tikv-forwarded-host";

#[derive(Clone)]
struct MetadataTikv {
    forwarded_hosts: Arc<Mutex<Vec<Vec<String>>>>,
}

#[tonic::async_trait]
impl Tikv for MetadataTikv {
    type BatchCommandsStream =
        tokio_stream::wrappers::ReceiverStream<Result<BatchCommandsResponse, tonic::Status>>;

    async fn coprocessor(
        &self,
        request: tonic::Request<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, tonic::Status> {
        let values = request
            .metadata()
            .get_all(FORWARD_METADATA_KEY)
            .iter()
            .map(|value| value.to_str().map(str::to_owned))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        self.forwarded_hosts.lock().unwrap().push(values);
        Ok(tonic::Response::new(CoprocessorResponse {
            data: request.into_inner().data,
            ..CoprocessorResponse::default()
        }))
    }

    async fn batch_commands(
        &self,
        _request: tonic::Request<tonic::Streaming<BatchCommandsRequest>>,
    ) -> Result<tonic::Response<Self::BatchCommandsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "BatchCommands is not used here",
        ))
    }
}

struct TestServer {
    address: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl TestServer {
    fn start(service: MetadataTikv) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let server = tonic::transport::Server::builder()
                        .add_service(TikvServer::new(service))
                        .serve_with_shutdown(address, async {
                            let _ = shutdown_rx.await;
                        });
                    started_tx.send(()).unwrap();
                    server.await.unwrap();
                });
        });
        started_rx.recv().unwrap();
        for attempt in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                return Self {
                    address: address.to_string(),
                    shutdown: Some(shutdown),
                    thread: Some(thread),
                };
            }
            assert!(attempt < 99, "test gRPC server did not accept connections");
            std::thread::sleep(Duration::from_millis(1));
        }
        unreachable!()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn request() -> DirectUnaryRequest {
    DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Leader,
        replica_read: false,
        stale_read: false,
        input_request_source: "campaign14_forwarding".to_owned(),
        predicted_read_bytes: 0,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context: KvrpcContext::default(),
        encoded_request: CoprocessorRequest {
            data: b"forwarding".to_vec(),
            ..CoprocessorRequest::default()
        }
        .encode_to_vec(),
    }
}

#[test]
fn exact_forwarding_metadata_is_omitted_or_attached_once() {
    // client-go/internal/client/client.go:88-89,392-393
    // client-go/internal/client/client_test.go:353
    let forwarded_hosts = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::start(MetadataTikv {
        forwarded_hosts: Arc::clone(&forwarded_hosts),
    });
    let mut client = TonicCoprocessorClient::new().unwrap();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));

    client
        .send_request_with_route(&server.address, None, &request(), &call)
        .unwrap();
    client
        .send_request_with_route(&server.address, Some("leader-1:20160"), &request(), &call)
        .unwrap();

    assert_eq!(
        *forwarded_hosts.lock().unwrap(),
        vec![Vec::<String>::new(), vec!["leader-1:20160".to_owned()]]
    );
}

#[test]
fn explicit_empty_forwarding_host_fails_before_dispatch() {
    let forwarded_hosts = Arc::new(Mutex::new(Vec::new()));
    let server = TestServer::start(MetadataTikv {
        forwarded_hosts: Arc::clone(&forwarded_hosts),
    });
    let mut client = TonicCoprocessorClient::new().unwrap();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));

    let error = client
        .send_request_with_route(&server.address, Some(""), &request(), &call)
        .unwrap_err();
    assert_eq!(error.kind(), "invalid_request");
    assert!(forwarded_hosts.lock().unwrap().is_empty());
}

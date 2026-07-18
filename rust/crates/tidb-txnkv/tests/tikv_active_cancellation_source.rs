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

use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use prost::Message;
use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
use tidb_proto::{CoprocessorRequest, CoprocessorResponse, KvrpcContext};
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::{
    ClientReplicaReadType, DirectUnaryClient, DirectUnaryClientError, DirectUnaryGrpcCode,
    DirectUnaryRequest, DirectUnaryTransportClass, EndpointType, UnaryCallContext,
    UnaryCancellation,
};

#[derive(Clone)]
struct CancellationTestTikv {
    attempts: Arc<AtomicUsize>,
    first_started: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

#[tonic::async_trait]
impl Tikv for CancellationTestTikv {
    async fn coprocessor(
        &self,
        request: tonic::Request<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, tonic::Status> {
        match self.attempts.fetch_add(1, Ordering::AcqRel) {
            0 => {
                self.first_started
                    .lock()
                    .unwrap()
                    .take()
                    .expect("first hanging request starts once")
                    .send(())
                    .unwrap();
                pending::<Result<tonic::Response<CoprocessorResponse>, tonic::Status>>().await
            }
            1 => Err(tonic::Status::cancelled("injected remote cancellation")),
            2 => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(tonic::Response::new(CoprocessorResponse::default()))
            }
            _ => Ok(tonic::Response::new(CoprocessorResponse {
                data: request.into_inner().data,
                ..CoprocessorResponse::default()
            })),
        }
    }
}

struct TestServer {
    address: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl TestServer {
    fn start(service: CancellationTestTikv) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
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
        for _ in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                return Self {
                    address: address.to_string(),
                    shutdown: Some(shutdown),
                    thread: Some(thread),
                };
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("cancellation test gRPC server did not accept connections");
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

fn request(data: &[u8]) -> DirectUnaryRequest {
    DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Leader,
        replica_read: false,
        stale_read: false,
        input_request_source: "active_cancel_test".to_owned(),
        predicted_read_bytes: 0,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context: KvrpcContext::default(),
        encoded_request: CoprocessorRequest {
            data: data.to_vec(),
            ..CoprocessorRequest::default()
        }
        .encode_to_vec(),
    }
}

#[test]
fn caller_cancellation_interrupts_hanging_tonic_call_without_closing_generation() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let (first_started, first_started_rx) = mpsc::channel();
    let server = TestServer::start(CancellationTestTikv {
        attempts: Arc::clone(&attempts),
        first_started: Arc::new(Mutex::new(Some(first_started))),
    });
    let mut client = TonicCoprocessorClient::new().unwrap();
    let pre_cancelled = UnaryCancellation::new();
    pre_cancelled.cancel();
    let error = client
        .send_request_with_context(
            &server.address,
            &request(b"cancel-before-registration"),
            &UnaryCallContext::new(Duration::from_secs(10), pre_cancelled),
        )
        .unwrap_err();
    assert_eq!(error, DirectUnaryClientError::CallerCancelled);
    assert_eq!(attempts.load(Ordering::Acquire), 0);
    assert_eq!(client.connection_version(&server.address), None);
    assert_eq!(client.active_address_count(), 0);

    let cancellation = UnaryCancellation::new();
    let cancel_from_execution = cancellation.clone();
    let canceller = std::thread::spawn(move || {
        first_started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("hanging tonic request reached the server");
        cancel_from_execution.cancel();
    });
    let started = Instant::now();
    let error = client
        .send_request_with_context(
            &server.address,
            &request(b"hang"),
            &UnaryCallContext::new(Duration::from_secs(10), cancellation),
        )
        .unwrap_err();

    canceller.join().unwrap();
    assert_eq!(error, DirectUnaryClientError::CallerCancelled);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "caller cancellation must beat the ten-second deadline"
    );
    assert_eq!(client.connection_version(&server.address), Some(1));
    assert_eq!(client.active_address_count(), 1);

    let remote = client
        .send_request_with_context(
            &server.address,
            &request(b"remote-cancel"),
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap_err();
    assert_eq!(
        remote.transport_class(),
        Some(DirectUnaryTransportClass::RemoteGrpc)
    );
    assert_eq!(remote.grpc_code(), Some(DirectUnaryGrpcCode::Canceled));
    assert_ne!(remote, DirectUnaryClientError::CallerCancelled);

    let timeout = client
        .send_request_with_context(
            &server.address,
            &request(b"local-timeout"),
            &UnaryCallContext::with_timeout(Duration::from_millis(10)),
        )
        .unwrap_err();
    assert_eq!(
        timeout.transport_class(),
        Some(DirectUnaryTransportClass::LocalDeadline)
    );
    assert_eq!(timeout.grpc_code(), None);
    assert_eq!(client.connection_version(&server.address), Some(1));

    let response = client
        .send_request_with_context(
            &server.address,
            &request(b"same-generation"),
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap();
    assert_eq!(
        CoprocessorResponse::decode(response.encoded_response.as_slice())
            .unwrap()
            .data,
        b"same-generation"
    );
    assert_eq!(client.connection_version(&server.address), Some(1));
    assert_eq!(attempts.load(Ordering::Acquire), 4);
}

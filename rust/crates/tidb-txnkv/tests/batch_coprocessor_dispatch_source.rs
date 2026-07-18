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
use tidb_proto::tikvpb::batch_commands_request::request::Cmd as RequestCmd;
use tidb_proto::tikvpb::batch_commands_response::response::Cmd as ResponseCmd;
use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
use tidb_proto::tikvpb::{batch_commands_response, BatchCommandsRequest, BatchCommandsResponse};
use tidb_proto::{CoprocessorRequest, CoprocessorResponse, KvrpcContext};
use tidb_txnkv::rpc::{
    AsyncRequestDispatcher, PendingRequest, TonicCoprocessorClient, UnaryCallContext,
};
use tidb_txnkv::{
    ClientReplicaReadType, DirectUnaryClient, DirectUnaryClientError, DirectUnaryRequest,
    EndpointType,
};

const FORWARD_METADATA_KEY: &str = "tikv-forwarded-host";

#[derive(Clone, Copy)]
enum ResponseMode {
    Echo,
    Hold,
    WrongTag,
    TransportFailure,
}

#[derive(Clone)]
struct BatchFixture {
    mode: ResponseMode,
    received: Arc<Mutex<Vec<(Option<String>, CoprocessorRequest)>>>,
    seen: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    release: tokio::sync::watch::Receiver<bool>,
}

#[tonic::async_trait]
impl Tikv for BatchFixture {
    type BatchCommandsStream =
        tokio_stream::wrappers::ReceiverStream<Result<BatchCommandsResponse, tonic::Status>>;

    async fn coprocessor(
        &self,
        _request: tonic::Request<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "fixture requires BatchCommands",
        ))
    }

    async fn batch_commands(
        &self,
        request: tonic::Request<tonic::Streaming<BatchCommandsRequest>>,
    ) -> Result<tonic::Response<Self::BatchCommandsStream>, tonic::Status> {
        let forwarded_host = request
            .metadata()
            .get(FORWARD_METADATA_KEY)
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let mode = self.mode;
        let received = Arc::clone(&self.received);
        let seen = Arc::clone(&self.seen);
        let mut release = self.release.clone();
        let mut inbound = request.into_inner();
        let (responses, response_rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            while let Ok(Some(packet)) = inbound.message().await {
                for (request_id, request) in packet.request_ids.into_iter().zip(packet.requests) {
                    let Some(RequestCmd::Coprocessor(body)) = request.cmd else {
                        let _ = responses
                            .send(Err(tonic::Status::invalid_argument(
                                "expected one Coprocessor command",
                            )))
                            .await;
                        return;
                    };
                    let request = match CoprocessorRequest::decode(body.as_slice()) {
                        Ok(request) => request,
                        Err(error) => {
                            let _ = responses
                                .send(Err(tonic::Status::invalid_argument(error.to_string())))
                                .await;
                            return;
                        }
                    };
                    received
                        .lock()
                        .unwrap()
                        .push((forwarded_host.clone(), request.clone()));
                    if let Some(seen) = seen.lock().unwrap().take() {
                        let _ = seen.send(());
                    }
                    if matches!(mode, ResponseMode::Hold) {
                        while !*release.borrow() {
                            if release.changed().await.is_err() {
                                break;
                            }
                        }
                    }
                    if matches!(mode, ResponseMode::TransportFailure) {
                        let _ = responses
                            .send(Err(tonic::Status::unavailable(
                                "injected BatchCommands receive failure",
                            )))
                            .await;
                        return;
                    }
                    let response = CoprocessorResponse {
                        data: request.data,
                        ..CoprocessorResponse::default()
                    }
                    .encode_to_vec();
                    let cmd = if matches!(mode, ResponseMode::WrongTag) {
                        ResponseCmd::Empty(response)
                    } else {
                        ResponseCmd::Coprocessor(response)
                    };
                    if responses
                        .send(Ok(BatchCommandsResponse {
                            responses: vec![batch_commands_response::Response { cmd: Some(cmd) }],
                            request_ids: vec![request_id],
                            ..BatchCommandsResponse::default()
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(response_rx),
        ))
    }
}

struct TestServer {
    address: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl TestServer {
    fn start(service: BatchFixture) -> Self {
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

fn context(region_id: u64) -> KvrpcContext {
    KvrpcContext {
        region_id,
        request_source: format!("region-{region_id}"),
        cluster_id: region_id + 100,
        ..KvrpcContext::default()
    }
}

fn request(data: &[u8]) -> DirectUnaryRequest {
    DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Leader,
        replica_read: false,
        stale_read: false,
        input_request_source: "batch-dispatch-fixture".to_owned(),
        predicted_read_bytes: 0,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context: context(42),
        encoded_request: CoprocessorRequest {
            context: Some(context(999)),
            tp: 103,
            data: data.to_vec(),
            start_ts: 77,
            ..CoprocessorRequest::default()
        }
        .encode_to_vec(),
    }
}

fn fixture(
    mode: ResponseMode,
) -> (
    TestServer,
    Arc<Mutex<Vec<(Option<String>, CoprocessorRequest)>>>,
    mpsc::Receiver<()>,
    tokio::sync::watch::Sender<bool>,
) {
    let received = Arc::new(Mutex::new(Vec::new()));
    let (seen, seen_rx) = mpsc::channel();
    let (release, release_rx) = tokio::sync::watch::channel(false);
    let server = TestServer::start(BatchFixture {
        mode,
        received: Arc::clone(&received),
        seen: Arc::new(Mutex::new(Some(seen))),
        release: release_rx,
    });
    (server, received, seen_rx, release)
}

fn wait_for_completion<P: PendingRequest>(
    pending: &mut P,
) -> Result<tidb_txnkv::DirectUnaryResponse, DirectUnaryClientError> {
    for _ in 0..200 {
        if let Some(result) = pending.try_complete().expect("completion run loop") {
            return result;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("BatchCommands Coprocessor attempt did not complete");
}

#[test]
fn concrete_dispatch_attaches_context_forwards_and_maps_coprocessor_response() {
    let (server, received, _seen, _release) = fixture(ResponseMode::Echo);
    let mut client = TonicCoprocessorClient::new().unwrap();
    let mut pending = client
        .begin(
            &server.address,
            Some("logical-tikv:20160"),
            &request(b"dag"),
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap();

    let raw = wait_for_completion(&mut pending).unwrap();
    let response = CoprocessorResponse::decode(raw.encoded_response.as_slice()).unwrap();
    assert_eq!(response.data, b"dag");
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0.as_deref(), Some("logical-tikv:20160"));
    assert_eq!(received[0].1.context.as_ref(), Some(&context(42)));
    assert_eq!(received[0].1.tp, 103);
    assert_eq!(received[0].1.start_ts, 77);
    client.close().unwrap();
}

#[test]
fn pull_cancellation_retires_the_exact_inflight_id_and_suppresses_late_response() {
    let (server, _received, seen, release) = fixture(ResponseMode::Hold);
    let mut client = TonicCoprocessorClient::new().unwrap();
    let mut pending = client
        .begin(
            &server.address,
            None,
            &request(b"cancel"),
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap();
    seen.recv_timeout(Duration::from_secs(1)).unwrap();
    pending.cancel();
    release.send(true).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(pending.try_complete().unwrap().is_none());
    client.close().unwrap();
}

#[test]
fn unexpected_batch_tag_fails_closed_without_reinterpreting_the_body() {
    let (server, _received, _seen, _release) = fixture(ResponseMode::WrongTag);
    let mut client = TonicCoprocessorClient::new().unwrap();
    let mut pending = client
        .begin(
            &server.address,
            None,
            &request(b"wrong-tag"),
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap();
    let error = wait_for_completion(&mut pending).unwrap_err();
    assert_eq!(error.kind(), "invalid_request");
    client.close().unwrap();
}

#[test]
fn batch_stream_failure_preserves_the_typed_transport_error() {
    let (server, _received, _seen, _release) = fixture(ResponseMode::TransportFailure);
    let mut client = TonicCoprocessorClient::new().unwrap();
    let mut pending = client
        .begin(
            &server.address,
            None,
            &request(b"transport"),
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap();
    let error = wait_for_completion(&mut pending).unwrap_err();
    assert!(matches!(error, DirectUnaryClientError::Connection(_)));
    client.close().unwrap();
}

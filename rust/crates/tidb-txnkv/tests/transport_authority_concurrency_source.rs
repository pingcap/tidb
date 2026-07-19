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

#![allow(missing_docs)]

use std::sync::mpsc;
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
use tidb_txnkv::{ClientReplicaReadType, DirectUnaryClient, DirectUnaryRequest, EndpointType};

#[derive(Clone)]
struct HeldBatchService {
    batch_seen: mpsc::Sender<()>,
    batch_release: tokio::sync::watch::Receiver<bool>,
    unary_seen: mpsc::Sender<()>,
    unary_release: tokio::sync::watch::Receiver<bool>,
}

#[tonic::async_trait]
impl Tikv for HeldBatchService {
    type BatchCommandsStream =
        tokio_stream::wrappers::ReceiverStream<Result<BatchCommandsResponse, tonic::Status>>;

    async fn coprocessor(
        &self,
        request: tonic::Request<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, tonic::Status> {
        let request = request.into_inner();
        self.unary_seen.send(()).unwrap();
        let mut release = self.unary_release.clone();
        while !*release.borrow() {
            release
                .changed()
                .await
                .map_err(|_| tonic::Status::cancelled("unary release authority was dropped"))?;
        }
        Ok(tonic::Response::new(CoprocessorResponse {
            data: request.data,
            ..CoprocessorResponse::default()
        }))
    }

    async fn batch_commands(
        &self,
        request: tonic::Request<tonic::Streaming<BatchCommandsRequest>>,
    ) -> Result<tonic::Response<Self::BatchCommandsStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (responses, response_rx) = tokio::sync::mpsc::channel(2);
        let seen = self.batch_seen.clone();
        let mut release = self.batch_release.clone();
        tokio::spawn(async move {
            while let Ok(Some(packet)) = inbound.message().await {
                for (request_id, request) in packet.request_ids.into_iter().zip(packet.requests) {
                    let Some(RequestCmd::Coprocessor(body)) = request.cmd else {
                        return;
                    };
                    let request = CoprocessorRequest::decode(body.as_slice()).unwrap();
                    let _ = seen.send(());
                    while !*release.borrow() {
                        if release.changed().await.is_err() {
                            return;
                        }
                    }
                    let response = CoprocessorResponse {
                        data: request.data,
                        ..CoprocessorResponse::default()
                    }
                    .encode_to_vec();
                    if responses
                        .send(Ok(BatchCommandsResponse {
                            responses: vec![batch_commands_response::Response {
                                cmd: Some(ResponseCmd::Coprocessor(response)),
                            }],
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
    batch_release: tokio::sync::watch::Sender<bool>,
    unary_release: tokio::sync::watch::Sender<bool>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl TestServer {
    fn start() -> (Self, mpsc::Receiver<()>, mpsc::Receiver<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let (batch_seen, batch_seen_rx) = mpsc::channel();
        let (batch_release, batch_release_rx) = tokio::sync::watch::channel(false);
        let (unary_seen, unary_seen_rx) = mpsc::channel();
        let (unary_release, unary_release_rx) = tokio::sync::watch::channel(false);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started, started_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let server = tonic::transport::Server::builder()
                        .add_service(TikvServer::new(HeldBatchService {
                            batch_seen,
                            batch_release: batch_release_rx,
                            unary_seen,
                            unary_release: unary_release_rx,
                        }))
                        .serve_with_shutdown(address, async {
                            let _ = shutdown_rx.await;
                        });
                    started.send(()).unwrap();
                    server.await.unwrap();
                });
        });
        started_rx.recv().unwrap();
        for attempt in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                return (
                    Self {
                        address: address.to_string(),
                        batch_release,
                        unary_release,
                        shutdown: Some(shutdown),
                        thread: Some(thread),
                    },
                    batch_seen_rx,
                    unary_seen_rx,
                );
            }
            assert!(attempt < 99, "test gRPC server did not accept connections");
            std::thread::sleep(Duration::from_millis(1));
        }
        unreachable!()
    }

    fn release(&self) {
        self.batch_release.send_replace(true);
    }

    fn release_unary(&self) {
        self.unary_release.send_replace(true);
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.batch_release.send_replace(true);
        self.unary_release.send_replace(true);
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
        input_request_source: "transport-authority-concurrency".to_owned(),
        predicted_read_bytes: 0,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context: KvrpcContext {
            region_id: 42,
            ..KvrpcContext::default()
        },
        encoded_request: CoprocessorRequest {
            tp: 103,
            data: data.to_vec(),
            start_ts: 77,
            ..CoprocessorRequest::default()
        }
        .encode_to_vec(),
    }
}

#[test]
fn cloned_handles_overlap_and_one_logical_close_does_not_retire_the_other() {
    // client-go/internal/client/client_async_test.go:354
    // TestSendRequestAsyncAndCloseClientOnHandle.
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TonicCoprocessorClient>();

    let (first_server, first_seen, _) = TestServer::start();
    let (second_server, second_seen, _) = TestServer::start();
    let mut authority = TonicCoprocessorClient::new().unwrap();
    let mut first = authority.clone();
    let mut second = authority.clone();
    assert!(authority.is_transport_owner());
    assert!(!first.is_transport_owner());
    assert!(!second.is_transport_owner());
    first.shutdown_cancellation().cancel();

    let call = UnaryCallContext::with_timeout(Duration::from_secs(3));
    let mut first_pending = first
        .begin(
            &first_server.address,
            Some("logical-forwarded-tikv:20160"),
            &request(b"first"),
            &call,
        )
        .unwrap();
    let mut second_pending = second
        .begin(&second_server.address, None, &request(b"second"), &call)
        .unwrap();

    first_seen.recv_timeout(Duration::from_secs(1)).unwrap();
    second_seen.recv_timeout(Duration::from_secs(1)).unwrap();
    first.close().unwrap();
    drop(first);

    first_server.release();
    second_server.release();
    let first_response = first_pending.complete(&call).unwrap().unwrap();
    let second_response = second_pending.complete(&call).unwrap().unwrap();
    assert_eq!(first_response.physical_address(), first_server.address);
    assert_eq!(first_response.physical_channel_version(), 1);
    assert_eq!(second_response.physical_address(), second_server.address);
    assert_eq!(second_response.physical_channel_version(), 1);
    assert_eq!(
        CoprocessorResponse::decode(first_response.encoded_response.as_slice())
            .unwrap()
            .data,
        b"first"
    );
    assert_eq!(
        CoprocessorResponse::decode(second_response.encoded_response.as_slice())
            .unwrap()
            .data,
        b"second"
    );

    let follow_up_call = UnaryCallContext::with_timeout(Duration::from_secs(3));
    let mut follow_up = second
        .begin(
            &second_server.address,
            None,
            &request(b"still-live"),
            &follow_up_call,
        )
        .unwrap();
    let follow_up = follow_up.complete(&follow_up_call).unwrap().unwrap();
    assert_eq!(follow_up.physical_address(), second_server.address);
    assert_eq!(follow_up.physical_channel_version(), 1);
    assert_eq!(
        CoprocessorResponse::decode(follow_up.encoded_response.as_slice())
            .unwrap()
            .data,
        b"still-live"
    );

    second.close().unwrap();
    authority.close().unwrap();
}

#[test]
fn stalled_unary_does_not_block_batch_commands_admission_or_completion() {
    let (server, batch_seen, unary_seen) = TestServer::start();
    server.release();

    let mut authority = TonicCoprocessorClient::new().unwrap();
    let mut unary_client = authority.clone();
    let unary_address = server.address.clone();
    let unary = std::thread::spawn(move || {
        unary_client.send_request_with_context(
            &unary_address,
            &request(b"stalled-unary"),
            &UnaryCallContext::with_timeout(Duration::from_secs(5)),
        )
    });
    unary_seen
        .recv_timeout(Duration::from_secs(1))
        .expect("unary fixture must stall after the request reaches TiKV");

    let mut batch_client = authority.clone();
    let batch_address = server.address.clone();
    let (batch_done, batch_result) = mpsc::channel();
    let batch = std::thread::spawn(move || {
        let call = UnaryCallContext::with_timeout(Duration::from_secs(3));
        let result = (|| {
            let mut pending = batch_client
                .begin(&batch_address, None, &request(b"independent-batch"), &call)
                .map_err(|error| error.to_string())?;
            pending
                .complete(&call)
                .map_err(|error| format!("batch completion driver failed: {error}"))?
                .map_err(|error| error.to_string())
        })();
        batch_done.send(result).unwrap();
    });

    let batch_reached_tikv = batch_seen.recv_timeout(Duration::from_secs(1));
    let independent_result = batch_reached_tikv
        .as_ref()
        .ok()
        .map(|_| batch_result.recv_timeout(Duration::from_secs(1)));

    server.release_unary();
    let unary_response = unary.join().unwrap().unwrap();
    batch.join().unwrap();
    batch_reached_tikv.expect("BatchCommands must reach TiKV while unary remains stalled");
    let batch_response = independent_result
        .expect("BatchCommands response must be observed before releasing unary")
        .expect("stalled unary must not occupy the shared transport command loop")
        .expect("independent BatchCommands request must succeed");
    assert_eq!(unary_response.physical_address(), server.address);
    assert_eq!(unary_response.physical_channel_version(), 1);
    assert_eq!(batch_response.physical_address(), server.address);
    assert_eq!(batch_response.physical_channel_version(), 1);
    assert_eq!(
        CoprocessorResponse::decode(unary_response.encoded_response.as_slice())
            .unwrap()
            .data,
        b"stalled-unary"
    );
    assert_eq!(
        CoprocessorResponse::decode(batch_response.encoded_response.as_slice())
            .unwrap()
            .data,
        b"independent-batch"
    );

    authority.close().unwrap();
}

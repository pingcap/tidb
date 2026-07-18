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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tidb_proto::tikvpb::batch_commands_request::request::Cmd as RequestCmd;
use tidb_proto::tikvpb::batch_commands_response::response::Cmd as ResponseCmd;
use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
use tidb_proto::tikvpb::{batch_commands_response, BatchCommandsRequest, BatchCommandsResponse};
use tidb_proto::{CoprocessorRequest, CoprocessorResponse};
use tidb_txnkv::rpc::{completion_pair, CompletionPull, CompletionRunLoop};
use tidb_txnkv::{
    BatchCommandEntry, BatchCommandTag, BatchInflightError, DirectUnaryClient,
    DirectUnaryClientError, OpaqueBatchCommand,
};

const FORWARD_METADATA_KEY: &str = "tikv-forwarded-host";

type BatchPull = CompletionPull<OpaqueBatchCommand, BatchInflightError>;

#[derive(Clone)]
struct StreamingTikv {
    streams: Arc<AtomicUsize>,
    metadata: Arc<Mutex<Vec<Option<String>>>>,
    received_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    hold_seen: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    withhold_headers: bool,
    close_before_request: bool,
    headers_started: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    release_headers: Arc<AtomicBool>,
}

#[tonic::async_trait]
impl Tikv for StreamingTikv {
    type BatchCommandsStream =
        tokio_stream::wrappers::ReceiverStream<Result<BatchCommandsResponse, tonic::Status>>;

    async fn coprocessor(
        &self,
        _request: tonic::Request<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("only BatchCommands is used"))
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
        self.metadata.lock().unwrap().push(forwarded_host.clone());
        self.streams.fetch_add(1, Ordering::AcqRel);
        if self.withhold_headers {
            if let Some(started) = self.headers_started.lock().unwrap().take() {
                let _ = started.send(());
            }
            while !self.release_headers.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            return Err(tonic::Status::cancelled("withheld header test released"));
        }
        if self.close_before_request {
            let (responses, response_rx) = tokio::sync::mpsc::channel(1);
            drop(responses);
            return Ok(tonic::Response::new(
                tokio_stream::wrappers::ReceiverStream::new(response_rx),
            ));
        }
        let received_bodies = Arc::clone(&self.received_bodies);
        let hold_seen = Arc::clone(&self.hold_seen);
        let mut inbound = request.into_inner();
        let (responses, response_rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            while let Ok(Some(packet)) = inbound.message().await {
                let mut pairs = Vec::with_capacity(packet.request_ids.len());
                let mut fail_stream = false;
                for (request_id, request) in packet.request_ids.into_iter().zip(packet.requests) {
                    let Some(RequestCmd::Empty(body)) = request.cmd else {
                        let _ = responses
                            .send(Err(tonic::Status::invalid_argument(
                                "test server accepts only Empty commands",
                            )))
                            .await;
                        return;
                    };
                    received_bodies.lock().unwrap().push(body.clone());
                    if body == b"hold" {
                        if let Some(hold_seen) = hold_seen.lock().unwrap().take() {
                            let _ = hold_seen.send(());
                        }
                        continue;
                    }
                    if body == b"fail" {
                        fail_stream = true;
                        continue;
                    }
                    pairs.push((request_id, body));
                }
                if fail_stream {
                    return;
                }
                pairs.reverse();
                let response = BatchCommandsResponse {
                    responses: pairs
                        .iter()
                        .map(|(_, body)| batch_commands_response::Response {
                            cmd: Some(ResponseCmd::Empty(body.clone())),
                        })
                        .collect(),
                    request_ids: pairs.iter().map(|(request_id, _)| *request_id).collect(),
                    ..BatchCommandsResponse::default()
                };
                if !pairs.is_empty() && responses.send(Ok(response)).await.is_err() {
                    return;
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
    fn start(service: StreamingTikv) -> Self {
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

fn entry(body: &'static [u8], forwarded_host: Option<&str>) -> (BatchCommandEntry, BatchPull) {
    let (completion, pull) = completion_pair(CompletionRunLoop::new(), || {});
    let entry = BatchCommandEntry::new(
        OpaqueBatchCommand::new(BatchCommandTag::Empty, body),
        completion,
    );
    let entry = match forwarded_host {
        Some(host) => entry.with_forwarded_host(host),
        None => entry,
    };
    (entry, pull)
}

fn wait_for_completion(pull: &mut BatchPull) -> Result<OpaqueBatchCommand, BatchInflightError> {
    for _ in 0..200 {
        if let Some(result) = pull.try_complete().unwrap() {
            return result;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("BatchCommands completion did not become ready")
}

fn wait_for_generation(
    client: &tidb_txnkv::rpc::TonicCoprocessorClient,
    address: &str,
    expected: Option<u64>,
) {
    for _ in 0..200 {
        if client.batch_stream_generation(address, None) == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("BatchCommands stream generation did not become {expected:?}")
}

#[test]
fn duplex_stream_reuses_pool_isolates_forwarding_reconnects_and_drains_close() {
    let streams = Arc::new(AtomicUsize::new(0));
    let metadata = Arc::new(Mutex::new(Vec::new()));
    let received_bodies = Arc::new(Mutex::new(Vec::new()));
    let (hold_seen, hold_wait) = mpsc::channel();
    let server = TestServer::start(StreamingTikv {
        streams: Arc::clone(&streams),
        metadata: Arc::clone(&metadata),
        received_bodies: Arc::clone(&received_bodies),
        hold_seen: Arc::new(Mutex::new(Some(hold_seen))),
        withhold_headers: false,
        close_before_request: false,
        headers_started: Arc::new(Mutex::new(None)),
        release_headers: Arc::new(AtomicBool::new(false)),
    });
    let mut client = tidb_txnkv::rpc::TonicCoprocessorClient::new().unwrap();

    let (first, mut first_pull) = entry(b"first", None);
    let (second, mut second_pull) = entry(b"second", None);
    let first_receipts = client
        .submit_batch_commands(&server.address, vec![first, second])
        .unwrap();
    assert_eq!(first_receipts.len(), 1);
    assert_eq!(first_receipts[0].route().generation(), 1);
    assert_eq!(first_receipts[0].request_ids().len(), 2);
    assert_eq!(
        wait_for_completion(&mut first_pull).unwrap().body(),
        b"first"
    );
    assert_eq!(
        wait_for_completion(&mut second_pull).unwrap().body(),
        b"second"
    );

    assert_eq!(
        client.batch_stream_generation(&server.address, None),
        Some(1)
    );
    let (failed, mut failed_pull) = entry(b"fail", None);
    let failed_receipts = client
        .submit_batch_commands(&server.address, vec![failed])
        .unwrap();
    assert_eq!(failed_receipts[0].route(), first_receipts[0].route());
    assert!(matches!(
        wait_for_completion(&mut failed_pull),
        Err(BatchInflightError::Transport(
            DirectUnaryClientError::Connection(_)
        ))
    ));
    assert_eq!(failed_pull.try_complete().unwrap(), None);
    wait_for_generation(&client, &server.address, Some(2));

    // A receive failure retires only generation 1. The shared physical channel
    // remains version 1 and the worker proactively opens generation 2 without
    // resending the ambiguous packet.
    assert_eq!(client.connection_version(&server.address), Some(1));
    let (reconnect, mut reconnect_pull) = entry(b"reconnect", None);
    let reconnect_receipts = client
        .submit_batch_commands(&server.address, vec![reconnect])
        .unwrap();
    assert_eq!(reconnect_receipts[0].route().generation(), 2);
    assert_eq!(
        wait_for_completion(&mut reconnect_pull).unwrap().body(),
        b"reconnect"
    );

    let (forwarded, mut forwarded_pull) = entry(b"forwarded", Some("logical-tikv:20160"));
    let forwarded_receipts = client
        .submit_batch_commands(&server.address, vec![forwarded])
        .unwrap();
    assert_eq!(forwarded_receipts[0].route().generation(), 1);
    assert_eq!(
        forwarded_receipts[0].route().forwarded_host(),
        Some("logical-tikv:20160")
    );
    assert_eq!(
        wait_for_completion(&mut forwarded_pull).unwrap().body(),
        b"forwarded"
    );
    assert_eq!(client.active_address_count(), 1);

    let (held, mut held_pull) = entry(b"hold", Some("logical-tikv:20160"));
    let held_receipts = client
        .submit_batch_commands(&server.address, vec![held])
        .unwrap();
    assert_eq!(held_receipts[0].route(), forwarded_receipts[0].route());
    hold_wait.recv_timeout(Duration::from_secs(1)).unwrap();
    client.close().unwrap();
    assert_eq!(
        wait_for_completion(&mut held_pull),
        Err(BatchInflightError::Transport(
            DirectUnaryClientError::Closed
        ))
    );

    assert_eq!(streams.load(Ordering::Acquire), 3);
    assert_eq!(
        *metadata.lock().unwrap(),
        vec![None, None, Some("logical-tikv:20160".to_owned()),]
    );
    let received = received_bodies.lock().unwrap();
    for expected in [
        b"first".as_slice(),
        b"second".as_slice(),
        b"fail".as_slice(),
        b"reconnect".as_slice(),
        b"forwarded".as_slice(),
        b"hold".as_slice(),
    ] {
        assert_eq!(
            received
                .iter()
                .filter(|body| body.as_slice() == expected)
                .count(),
            1,
            "a packet must never be ambiguously resent"
        );
    }
}

#[test]
fn shutdown_cancellation_interrupts_withheld_stream_headers_and_joins_promptly() {
    let (headers_started, headers_wait) = mpsc::channel();
    let release_headers = Arc::new(AtomicBool::new(false));
    let server = TestServer::start(StreamingTikv {
        streams: Arc::new(AtomicUsize::new(0)),
        metadata: Arc::new(Mutex::new(Vec::new())),
        received_bodies: Arc::new(Mutex::new(Vec::new())),
        hold_seen: Arc::new(Mutex::new(None)),
        withhold_headers: true,
        close_before_request: false,
        headers_started: Arc::new(Mutex::new(Some(headers_started))),
        release_headers: Arc::clone(&release_headers),
    });
    let mut client = tidb_txnkv::rpc::TonicCoprocessorClient::new().unwrap();
    let cancellation = client.shutdown_cancellation();
    let (request, mut pull) = entry(b"withheld-headers", None);
    let address = server.address.clone();
    let (finished, finished_wait) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let receipts = client.submit_batch_commands(&address, vec![request]);
        let close_started = Instant::now();
        let close = client.close();
        let _ = finished.send((receipts, close, close_started.elapsed()));
    });

    headers_wait.recv_timeout(Duration::from_secs(1)).unwrap();
    let cancel_started = Instant::now();
    cancellation.cancel();
    let outcome = finished_wait.recv_timeout(Duration::from_secs(1));
    release_headers.store(true, Ordering::Release);
    let (receipts, close, close_elapsed) = outcome.unwrap();
    thread.join().unwrap();

    assert!(receipts.unwrap().is_empty());
    close.unwrap();
    assert!(cancel_started.elapsed() < Duration::from_secs(1));
    assert!(close_elapsed < Duration::from_secs(1));
    assert_eq!(
        wait_for_completion(&mut pull),
        Err(BatchInflightError::Transport(
            DirectUnaryClientError::Closed
        ))
    );
}

#[test]
fn immediate_close_before_request_fails_once_without_on_demand_open_spin() {
    let streams = Arc::new(AtomicUsize::new(0));
    let server = TestServer::start(StreamingTikv {
        streams: Arc::clone(&streams),
        metadata: Arc::new(Mutex::new(Vec::new())),
        received_bodies: Arc::new(Mutex::new(Vec::new())),
        hold_seen: Arc::new(Mutex::new(None)),
        withhold_headers: false,
        close_before_request: true,
        headers_started: Arc::new(Mutex::new(None)),
        release_headers: Arc::new(AtomicBool::new(false)),
    });
    let mut client = tidb_txnkv::rpc::TonicCoprocessorClient::new().unwrap();
    let cancellation = client.shutdown_cancellation();
    let (request, mut pull) = entry(b"must-not-spin", None);
    let address = server.address.clone();
    let (submitted, submitted_wait) = mpsc::channel();
    let (allow_close, close_wait) = mpsc::channel();
    let (closed, closed_wait) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let result = client.submit_batch_commands(&address, vec![request]);
        let _ = submitted.send(result);
        let _ = close_wait.recv();
        let _ = closed.send(client.close());
    });

    let submitted = match submitted_wait.recv_timeout(Duration::from_secs(1)) {
        Ok(submitted) => submitted,
        Err(error) => {
            cancellation.cancel();
            let _ = allow_close.send(());
            let _ = closed_wait.recv_timeout(Duration::from_secs(1));
            let _ = thread.join();
            panic!("one on-demand open must not retry in a loop: {error}");
        }
    };
    let mut completion = None;
    let mut completion_poll_error = None;
    for _ in 0..200 {
        match pull.try_complete() {
            Ok(Some(result)) => {
                completion = Some(result);
                break;
            }
            Ok(None) => {}
            Err(error) => {
                completion_poll_error = Some(error);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let stream_count = streams.load(Ordering::Acquire);
    let allow_close = allow_close.send(());
    let close = closed_wait.recv_timeout(Duration::from_secs(1));
    let joined = thread.join();

    let _receipts = submitted.unwrap();
    allow_close.unwrap();
    close.unwrap().unwrap();
    joined.unwrap();
    assert!(
        completion_poll_error.is_none(),
        "completion polling must remain valid: {completion_poll_error:?}"
    );
    assert!(matches!(
        completion.expect("terminal stream must fail the unpublished batch"),
        Err(BatchInflightError::Transport(
            DirectUnaryClientError::Connection(_)
        ))
    ));
    assert!((1..=2).contains(&stream_count));
}

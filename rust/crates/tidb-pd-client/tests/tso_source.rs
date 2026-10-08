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

use std::collections::VecDeque;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use prost::Message;
use tidb_pd_client::{PdClient, TSO_PATH};
use tidb_proto::pdpb;
use tidb_proto::test_pd_server::{Pd, PdServer};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};

const CLUSTER_ID: u64 = 42;

#[derive(Clone)]
enum TsoReply {
    Response(pdpb::TsoResponse),
    Status(tonic::Code, &'static str),
    Delayed(Duration, pdpb::TsoResponse),
    DelayedStatus(Duration, tonic::Code, &'static str),
}

struct State {
    peers: std::collections::HashSet<std::net::SocketAddr>,
    members: Option<pdpb::GetMembersResponse>,
    health_status: i32,
    user_agents: Vec<String>,
    forwarding: Vec<Option<String>>,
    cluster_info: Option<pdpb::GetClusterInfoResponse>,
    cluster_info_failure: bool,
    discovery_delay: Duration,
    discovery_requests: usize,
    micro_requests: Vec<tsopb::TsoRequest>,
    replies: VecDeque<TsoReply>,
    requests: Vec<pdpb::TsoRequest>,
    stream_opens: usize,
    withhold_headers_until_request: bool,
    /// Instrument extension for batching: when set, the mock answers any
    /// request whose scripted reply is exhausted by allocating `count`
    /// consecutive timestamps and reporting the LAST one, exactly as PD does.
    auto_batch_physical: Option<i64>,
    /// Suffix width the auto-batching allocator reserves, as a multi-DC PD
    /// (`enable-local-tso = true`) would report.
    auto_batch_suffix_bits: u32,
    /// Sticky failure used once the scripted replies run out, so a mock PD can
    /// fail every batch it is asked for instead of closing its stream.
    auto_status: Option<(tonic::Code, &'static str)>,
    stream_status: Option<(tonic::Code, &'static str)>,
}

impl State {
    fn auto_reply(&mut self, count: u32) -> Option<TsoReply> {
        if let Some((code, message)) = self.auto_status {
            return Some(TsoReply::Status(code, message));
        }
        let suffix_bits = self.auto_batch_suffix_bits;
        let physical = self.auto_batch_physical.as_mut()?;
        *physical += 1;
        Some(TsoReply::Response(pdpb::TsoResponse {
            header: Some(header()),
            count,
            // PD reports the batch's LAST timestamp; with a suffix reserved,
            // successive timestamps are `1 << suffix_bits` apart.
            timestamp: Some(pdpb::Timestamp {
                physical: *physical,
                logical: i64::from(count) << suffix_bits,
                suffix_bits,
            }),
        }))
    }
}

#[derive(Clone)]
struct MockPd {
    state: Arc<Mutex<State>>,
    address: String,
}

#[tonic::async_trait]
impl Pd for MockPd {
    async fn get_cluster_info(
        &self,
        request: tonic::Request<pdpb::GetClusterInfoRequest>,
    ) -> Result<tonic::Response<pdpb::GetClusterInfoResponse>, tonic::Status> {
        let (delay, response) = {
            let mut state = self.state.lock().unwrap();
            state.peers.extend(request.remote_addr());
            state.discovery_requests += 1;
            if state.cluster_info_failure {
                return Err(tonic::Status::unavailable("mode observation failed"));
            }
            state.user_agents.push(
                request
                    .metadata()
                    .get("user-agent")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
            );
            (state.discovery_delay, state.cluster_info.clone())
        };
        tokio::time::sleep(delay).await;
        response
            .map(tonic::Response::new)
            .ok_or_else(|| tonic::Status::unimplemented("old PD"))
    }

    async fn tso(
        &self,
        request: tonic::Request<tonic::Streaming<pdpb::TsoRequest>>,
    ) -> Result<tonic::Response<tonic::codegen::BoxStream<pdpb::TsoResponse>>, tonic::Status> {
        self.state
            .lock()
            .unwrap()
            .peers
            .extend(request.remote_addr());
        self.state.lock().unwrap().forwarding.push(
            request
                .metadata()
                .get("pd-forwarded-host")
                .map(|v| v.to_str().unwrap().to_owned()),
        );
        self.state.lock().unwrap().stream_opens += 1;
        if let Some((code, message)) = self.state.lock().unwrap().stream_status {
            return Err(tonic::Status::new(code, message));
        }
        let state = Arc::clone(&self.state);
        let mut requests = request.into_inner();
        let (responses, response_rx) = tokio::sync::mpsc::channel(1);
        if self.state.lock().unwrap().withhold_headers_until_request {
            let request = match requests.next().await {
                Some(Ok(request)) => request,
                Some(Err(status)) => return Err(status),
                None => {
                    return Err(tonic::Status::unavailable(
                        "TSO request stream closed before its first request",
                    ));
                }
            };
            let reply = {
                let mut state = self.state.lock().unwrap();
                let count = request.count;
                state.requests.push(request);
                state
                    .replies
                    .pop_front()
                    .or_else(|| state.auto_reply(count))
            };
            match reply {
                Some(TsoReply::Response(response)) => {
                    responses.send(Ok(response)).await.unwrap();
                }
                Some(TsoReply::Status(code, message)) => {
                    responses
                        .send(Err(tonic::Status::new(code, message)))
                        .await
                        .unwrap();
                }
                Some(TsoReply::Delayed(delay, response)) => {
                    tokio::time::sleep(delay).await;
                    responses.send(Ok(response)).await.unwrap();
                }
                Some(TsoReply::DelayedStatus(delay, code, message)) => {
                    tokio::time::sleep(delay).await;
                    responses
                        .send(Err(tonic::Status::new(code, message)))
                        .await
                        .unwrap();
                }
                None => panic!("mock PD has no reply for the first TSO request"),
            }
        }
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                let request = match request {
                    Ok(request) => request,
                    Err(_) => break,
                };
                let reply = {
                    let mut state = state.lock().unwrap();
                    let count = request.count;
                    state.requests.push(request);
                    state
                        .replies
                        .pop_front()
                        .or_else(|| state.auto_reply(count))
                };
                match reply {
                    Some(TsoReply::Response(response)) => {
                        if responses.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Some(TsoReply::Status(code, message)) => {
                        let _ = responses.send(Err(tonic::Status::new(code, message))).await;
                        break;
                    }
                    Some(TsoReply::Delayed(delay, response)) => {
                        tokio::time::sleep(delay).await;
                        if responses.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Some(TsoReply::DelayedStatus(delay, code, message)) => {
                        tokio::time::sleep(delay).await;
                        let _ = responses.send(Err(tonic::Status::new(code, message))).await;
                        break;
                    }
                    None => break,
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(
            response_rx,
        ))))
    }

    async fn get_members(
        &self,
        request: tonic::Request<pdpb::GetMembersRequest>,
    ) -> Result<tonic::Response<pdpb::GetMembersResponse>, tonic::Status> {
        self.state
            .lock()
            .unwrap()
            .peers
            .extend(request.remote_addr());
        if let Some(members) = self.state.lock().unwrap().members.clone() {
            return Ok(tonic::Response::new(members));
        }
        let member = pdpb::Member {
            name: "pd-1".to_owned(),
            member_id: 1,
            client_urls: vec![self.address.clone()],
            ..pdpb::Member::default()
        };
        Ok(tonic::Response::new(pdpb::GetMembersResponse {
            header: Some(header()),
            members: vec![member.clone()],
            leader: Some(member),
            ..pdpb::GetMembersResponse::default()
        }))
    }
}

struct Server {
    address: String,
    state: Arc<Mutex<State>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    fn start(replies: impl IntoIterator<Item = TsoReply>) -> Self {
        Self::start_with_header_behavior(replies, false)
    }

    fn start_auto_batching() -> Self {
        Self::start_auto_batching_with_suffix_bits(0)
    }

    /// An allocator that reserves `suffix_bits` low bits of the logical part,
    /// as PD does when local TSO is enabled and more than one dc-location
    /// exists (`CalSuffixBits`). No playground PD reaches this path.
    fn start_auto_batching_with_suffix_bits(suffix_bits: u32) -> Self {
        let server = Self::start_with_header_behavior([], false);
        {
            let mut state = server.state.lock().unwrap();
            state.auto_batch_physical = Some(0);
            state.auto_batch_suffix_bits = suffix_bits;
        }
        server
    }

    /// A PD that fails every TSO batch it is handed, after making the first
    /// one wait long enough for other callers to pile into the next batch.
    fn start_failing_after_a_slow_first_batch(delay: Duration) -> Self {
        let server = Self::start_with_header_behavior(
            [TsoReply::DelayedStatus(
                delay,
                tonic::Code::Internal,
                "tso batch failed",
            )],
            false,
        );
        server.state.lock().unwrap().auto_status =
            Some((tonic::Code::Internal, "tso batch failed"));
        server
    }

    fn start_after_first_request(replies: impl IntoIterator<Item = TsoReply>) -> Self {
        Self::start_with_header_behavior(replies, true)
    }

    fn start_with_header_behavior(
        replies: impl IntoIterator<Item = TsoReply>,
        withhold_headers_until_request: bool,
    ) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("http://{address}");
        let state = Arc::new(Mutex::new(State {
            peers: Default::default(),
            members: None,
            health_status: 1,
            user_agents: Vec::new(),
            forwarding: Vec::new(),
            cluster_info: None,
            cluster_info_failure: false,
            discovery_delay: Duration::ZERO,
            discovery_requests: 0,
            micro_requests: Vec::new(),
            replies: replies.into_iter().collect(),
            requests: Vec::new(),
            stream_opens: 0,
            withhold_headers_until_request,
            auto_batch_physical: None,
            auto_batch_suffix_bits: 0,
            auto_status: None,
            stream_status: None,
        }));
        let service = MockPd {
            state: Arc::clone(&state),
            address: endpoint.clone(),
        };
        drop(listener);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started, started_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let server = tonic::transport::Server::builder()
                    .add_service(HealthServer(service.clone()))
                    .add_service(Microservice(service.clone()))
                    .add_service(PdServer::new(service))
                    .serve_with_shutdown(address, async {
                        let _ = shutdown_rx.await;
                    });
                started.send(()).unwrap();
                server.await.unwrap();
            });
        });
        started_rx.recv().unwrap();
        for _ in 0..100 {
            if std::net::TcpStream::connect_timeout(&address, Duration::from_millis(10)).is_ok() {
                return Self {
                    address: endpoint,
                    state,
                    shutdown: Some(shutdown),
                    thread: Some(thread),
                };
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("mock PD did not accept connections");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn header() -> pdpb::ResponseHeader {
    pdpb::ResponseHeader {
        cluster_id: CLUSTER_ID,
        error: None,
    }
}

fn timestamp(physical: i64, logical: i64) -> pdpb::TsoResponse {
    pdpb::TsoResponse {
        header: Some(header()),
        count: 1,
        timestamp: Some(pdpb::Timestamp {
            physical,
            logical,
            suffix_bits: 0,
        }),
    }
}

#[test]
fn tso_wire_keeps_the_pinned_stream_path_and_field_numbers() {
    assert_eq!(TSO_PATH, "/pdpb.PD/Tso");
    let request = pdpb::TsoRequest {
        header: Some(pdpb::RequestHeader {
            cluster_id: CLUSTER_ID,
            ..pdpb::RequestHeader::default()
        }),
        count: 1,
        dc_location: String::new(),
    };
    assert_eq!(
        request.encode_to_vec(),
        [0x0a, 0x02, 0x08, 0x2a, 0x10, 0x01]
    );
    let response = timestamp(10, 3);
    assert_eq!(
        response.encode_to_vec(),
        [0x0a, 0x02, 0x08, 0x2a, 0x10, 0x01, 0x1a, 0x04, 0x08, 0x0a, 0x10, 0x03,]
    );
}

#[test]
fn request_handles_share_one_stream_and_one_monotonic_owner() {
    let server = Server::start([
        TsoReply::Response(timestamp(10, 1)),
        TsoReply::Response(timestamp(10, 2)),
    ]);
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();
    let clone = client.clone();
    assert_eq!(client.get_timestamp().unwrap(), (10_u64 << 18) + 1);
    assert_eq!(clone.get_timestamp().unwrap(), (10_u64 << 18) + 2);

    let state = server.state.lock().unwrap();
    assert_eq!(state.stream_opens, 1);
    assert_eq!(state.requests.len(), 2);
    assert!(state.requests.iter().all(|request| {
        request.count == 1
            && request.dc_location.is_empty()
            && request.header.as_ref().is_some_and(|header| {
                header.cluster_id == CLUSTER_ID
                    && header.sender_id == 0
                    && header.caller_id.is_empty()
                    && header.caller_component.is_empty()
            })
    }));
    drop(state);
    drop(clone);
    client.shutdown().unwrap();
}

#[test]
fn sends_first_request_before_waiting_for_response_headers() {
    let server = Server::start_after_first_request([TsoReply::Response(timestamp(15, 1))]);
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();

    assert_eq!(client.get_timestamp().unwrap(), (15_u64 << 18) + 1);
    let state = server.state.lock().unwrap();
    assert_eq!(state.stream_opens, 1);
    assert_eq!(state.requests.len(), 1);
}

/// Go's transaction warmup stores an oracle future: it dispatches TSO work at
/// prepare time, but a statement that never reads storage can drop the future
/// without activating a transaction or blocking on PD.
#[test]
fn timestamp_future_dispatches_before_wait_and_survives_an_unobserved_result() {
    let server = Server::start_auto_batching();
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();

    let unused = client.get_timestamp_async().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while server.state.lock().unwrap().requests.is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "creating the future did not dispatch its TSO request"
        );
        std::thread::yield_now();
    }
    drop(unused);

    let used = client.get_timestamp_async().unwrap();
    assert_ne!(used.wait().unwrap(), 0);
    let state = server.state.lock().unwrap();
    assert_eq!(state.stream_opens, 1);
    assert_eq!(state.requests.len(), 2);
    drop(state);
    client.shutdown().unwrap();
}

#[test]
fn retry_retires_the_broken_stream_before_reopening() {
    let server = Server::start([
        TsoReply::Status(tonic::Code::Unavailable, "not leader"),
        TsoReply::Response(timestamp(20, 1)),
    ]);
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();
    assert_eq!(client.get_timestamp().unwrap(), (20_u64 << 18) + 1);
    assert_eq!(server.state.lock().unwrap().stream_opens, 2);
}

#[test]
fn malformed_or_fallback_timestamps_follow_pinned_go_semantics() {
    // tso_fallback (the batch would move backwards) is TERMINAL: Go panics
    // inside the dispatcher (dispatcher.go:522-536); this crate surfaces it
    // as an error and keeps the process alive (documented narrowing).
    // A MISSING timestamp is "retry everything": Go retries it until the
    // context is done (dispatcher.go:356-398); the client timeout bounds it
    // here, so the caller observes the deadline miss.
    let mut missing = timestamp(30, 1);
    missing.timestamp = None;
    let server = Server::start([
        TsoReply::Response(timestamp(30, 2)),
        TsoReply::Response(timestamp(30, 1)),
        TsoReply::Response(missing.clone()),
        TsoReply::Response(missing),
    ]);
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();
    assert_eq!(client.get_timestamp().unwrap(), (30_u64 << 18) + 2);
    assert_eq!(client.get_timestamp().unwrap_err().kind(), "tso_fallback");
    assert_eq!(client.get_timestamp().unwrap_err().kind(), "timeout");
}

#[test]
fn configured_timeout_bounds_the_whole_timestamp_request() {
    let server = Server::start([TsoReply::Delayed(
        Duration::from_millis(500),
        timestamp(40, 1),
    )]);
    let client = PdClient::connect(&server.address, Duration::from_millis(100)).unwrap();
    assert_eq!(client.get_timestamp().unwrap_err().kind(), "timeout");
}

/// THE PIN: batching shares one round trip, never one timestamp. Every waiter
/// must come away with its own value, and the batch's own values must be
/// strictly increasing — a wrong range split (Go `tsoutil.AddLogical`) is
/// invisible to any throughput measurement but visible here.
#[test]
fn concurrent_waiters_never_share_a_timestamp() {
    const THREADS: usize = 16;
    const PER_THREAD: usize = 200;

    let server = Server::start_auto_batching();
    let client = PdClient::connect(&server.address, Duration::from_secs(10)).unwrap();
    let collected = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let client = client.clone();
                scope.spawn(move || {
                    (0..PER_THREAD)
                        .map(|_| client.get_timestamp().unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    let mut all: Vec<u64> = collected.iter().flatten().copied().collect();
    assert_eq!(all.len(), THREADS * PER_THREAD);
    let total = all.len();
    all.sort_unstable();
    all.dedup();
    assert_eq!(
        all.len(),
        total,
        "batched waiters received duplicate timestamps"
    );
    // Each thread sees its own calls strictly increasing.
    for thread in &collected {
        assert!(
            thread.windows(2).all(|pair| pair[0] < pair[1]),
            "timestamps went backwards within one caller"
        );
    }
    // Batching actually happened: fewer PD requests than timestamps served.
    let requests = server.state.lock().unwrap().requests.len();
    assert!(
        requests < total,
        "no batching occurred: {requests} requests for {total} timestamps"
    );
    client.shutdown().unwrap();
}

/// THE PINNED PATH: the pinned client-go never reads the proto's
/// `suffix_bits` field (dispatcher.go:461,483 compose in plain arithmetic),
/// so even a mock that reports a reserved allocator suffix gets its batch
/// handed out as the CONTIGUOUS run ending at the reported logical. The
/// mis-shifted split would hand two waiters the same value with no error
/// anywhere; the plain arithmetic cannot.
#[test]
fn batching_survives_a_reserved_allocator_suffix() {
    const THREADS: usize = 16;
    const PER_THREAD: usize = 100;
    const SUFFIX_BITS: u32 = 4;

    let server = Server::start_auto_batching_with_suffix_bits(SUFFIX_BITS);
    let client = PdClient::connect(&server.address, Duration::from_secs(10)).unwrap();
    let collected = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let client = client.clone();
                scope.spawn(move || {
                    (0..PER_THREAD)
                        .map(|_| client.get_timestamp().unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    let all: Vec<u64> = collected.iter().flatten().copied().collect();
    let total = all.len();
    assert_eq!(total, THREADS * PER_THREAD);

    let mut unique = all.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        total,
        "a reserved suffix made two waiters share a timestamp"
    );
    for thread in &collected {
        assert!(
            thread.windows(2).all(|pair| pair[0] < pair[1]),
            "timestamps went backwards within one caller"
        );
    }

    // Each mock reply is one batch: the handed-out logicals of that batch
    // must be the contiguous run ending at the value PD reported -- the
    // model, checked without reference to how the client got there.
    let mut by_physical: std::collections::BTreeMap<u64, Vec<u64>> =
        std::collections::BTreeMap::new();
    for timestamp in all {
        by_physical
            .entry(timestamp >> 18)
            .or_default()
            .push(timestamp & ((1 << 18) - 1));
    }
    assert!(
        by_physical.len() < total,
        "no batching occurred: {} batches for {total} timestamps",
        by_physical.len()
    );
    for (physical, mut logicals) in by_physical {
        logicals.sort_unstable();
        assert!(
            logicals.windows(2).all(|pair| pair[1] == pair[0] + 1),
            "batch at physical {physical} was not the contiguous run PD allocated"
        );
    }
}

/// THE FAN-OUT PIN: one round trip serves many waiters, so a PD failure must
/// reach every waiter of that batch. None may hang, and none may be handed a
/// timestamp out of a batch that failed.
#[test]
fn a_failed_batch_fails_every_waiter_in_it() {
    const THREADS: usize = 16;

    // The first batch is held open long enough for the other callers to queue
    // behind it, so the batch that fails afterwards is provably wider than one
    // waiter. Every batch fails, including the first.
    let server = Server::start_failing_after_a_slow_first_batch(Duration::from_millis(300));
    let client = PdClient::connect(&server.address, Duration::from_secs(10)).unwrap();
    let outcomes = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let client = client.clone();
                scope.spawn(move || client.get_timestamp())
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    // Every waiter returned (no hang) and none received a timestamp.
    assert_eq!(outcomes.len(), THREADS);
    for outcome in &outcomes {
        let error = outcome
            .as_ref()
            .expect_err("a failed batch yields no timestamp");
        // Go retries until the context deadline expires and surfaces
        // ctx.Err(), not the underlying transport error.
        assert_eq!(error.kind(), "timeout");
    }

    // Under the pinned retry-everything semantics the waiters retry until
    // their wait deadline, so the request count exceeds the waiter count and
    // the terminal error is the deadline miss; batching itself is pinned by
    // concurrent_waiters_never_share_a_timestamp.
    client.shutdown().unwrap();
}

#[derive(Clone)]
struct Microservice(MockPd);
impl tonic::server::NamedService for Microservice {
    const NAME: &'static str = "tsopb.TSO";
}
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for Microservice
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let service = self.clone();
        match request.uri().path() {
            "/tsopb.TSO/FindGroupByKeyspaceID" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(service, request)
                    .await)
            }),
            "/tsopb.TSO/Tso" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .streaming(service, request)
                    .await)
            }),
            _ => unreachable!(),
        }
    }
}
use tikv_client::proto::tsopb;
impl tonic::server::UnaryService<tsopb::FindGroupByKeyspaceIdRequest> for Microservice {
    type Response = tsopb::FindGroupByKeyspaceIdResponse;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<tsopb::FindGroupByKeyspaceIdRequest>) -> Self::Future {
        let address = self.0.address.clone();
        Box::pin(async move {
            Ok(tonic::Response::new(tsopb::FindGroupByKeyspaceIdResponse {
                keyspace_group: Some(tsopb::KeyspaceGroup {
                    members: vec![tsopb::KeyspaceGroupMember {
                        address,
                        is_primary: true,
                    }],
                    ..Default::default()
                }),
                mod_revision: 1,
                ..Default::default()
            }))
        })
    }
}
impl tonic::server::StreamingService<tsopb::TsoRequest> for Microservice {
    type Response = tsopb::TsoResponse;
    type ResponseStream = ReceiverStream<Result<Self::Response, tonic::Status>>;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(
        &mut self,
        request: tonic::Request<tonic::Streaming<tsopb::TsoRequest>>,
    ) -> Self::Future {
        let state = self.0.state.clone();
        Box::pin(async move {
            let mut requests = request.into_inner();
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                let mut logical = 0;
                while let Ok(Some(request)) = requests.message().await {
                    logical += i64::from(request.count);
                    let response = tsopb::TsoResponse {
                        header: Some(tsopb::ResponseHeader {
                            cluster_id: CLUSTER_ID,
                            ..Default::default()
                        }),
                        count: request.count,
                        timestamp: Some(pdpb::Timestamp {
                            physical: 500,
                            logical,
                            suffix_bits: 0,
                        }),
                    };
                    state.lock().unwrap().micro_requests.push(request);
                    if tx.send(Ok(response)).await.is_err() {
                        break;
                    }
                }
            });
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        })
    }
}

#[test]
fn source_service_api_routes_to_independent_timestamp_server() {
    let pd = Server::start_auto_batching();
    let tso = Server::start_auto_batching();
    pd.state.lock().unwrap().cluster_info = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        tso_urls: vec![tso.address.clone()],
        ..Default::default()
    });
    let client = PdClient::connect(&pd.address, Duration::from_secs(1)).unwrap();
    assert_eq!(client.get_timestamp().unwrap(), (500_u64 << 18) + 1);
    assert_eq!(client.get_timestamp().unwrap(), (500_u64 << 18) + 2);
    assert!(pd.state.lock().unwrap().requests.is_empty());
    let state = tso.state.lock().unwrap();
    assert_eq!(state.micro_requests.len(), 2);
    let header = state.micro_requests[0].header.as_ref().unwrap();
    assert_eq!(header.cluster_id, CLUSTER_ID);
    assert_eq!(
        header.keyspace,
        Some(tsopb::request_header::Keyspace::KeyspaceId(u32::MAX))
    );
}

#[test]
fn source_service_api_discovery_failure_never_allocates_from_pd() {
    let pd = Server::start_auto_batching();
    pd.state.lock().unwrap().cluster_info = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        ..Default::default()
    });
    let client = PdClient::connect(&pd.address, Duration::from_millis(150)).unwrap();
    assert!(client.get_timestamp().is_err());
    assert!(pd.state.lock().unwrap().requests.is_empty());
}

#[test]
fn source_service_background_probe_does_not_block_metadata_or_shutdown() {
    let pd = Server::start_auto_batching();
    pd.state.lock().unwrap().discovery_delay = Duration::from_secs(10);
    let client = PdClient::connect(&pd.address, Duration::from_secs(15)).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while pd.state.lock().unwrap().discovery_requests == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "background discovery never started"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let start = std::time::Instant::now();
    client.refresh_members().unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "metadata waited for a timestamp probe"
    );
    let start = std::time::Instant::now();
    client.shutdown().unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "shutdown did not cancel discovery"
    );
}

#[test]
fn source_channel_batch_adapter_metadata_discovery_and_tso_share_connection() {
    let server = Server::start_auto_batching();
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();
    client.get_timestamp().unwrap();
    client.refresh_members().unwrap();
    let peers = server.state.lock().unwrap().peers.len();
    client.shutdown().unwrap();
    assert_eq!(
        peers, 1,
        "metadata, discovery and TSO must share one connection"
    );
}

#[test]
fn source_channel_batch_adapter_periodic_discovery_uses_process_channel() {
    let server = Server::start_auto_batching();
    let client = PdClient::connect(&server.address, Duration::from_secs(1)).unwrap();
    client.get_timestamp().unwrap();
    let previous = server.state.lock().unwrap().discovery_requests;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while server.state.lock().unwrap().discovery_requests <= previous {
        assert!(
            std::time::Instant::now() < deadline,
            "idle discovery did not run"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    client.get_timestamp().unwrap();
    let peers = server.state.lock().unwrap().peers.len();
    client.shutdown().unwrap();
    assert_eq!(
        peers, 1,
        "periodic probes must not create another connection fleet"
    );
}

#[derive(Clone, PartialEq, prost::Message)]
struct HealthRequest {
    #[prost(string, tag = "1")]
    service: String,
}
#[derive(Clone, PartialEq, prost::Message)]
struct HealthResponse {
    #[prost(int32, tag = "1")]
    status: i32,
}
#[derive(Clone)]
struct HealthServer(MockPd);
impl tonic::server::NamedService for HealthServer {
    const NAME: &'static str = "grpc.health.v1.Health";
}
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for HealthServer
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .unary(service, request)
                .await)
        })
    }
}
impl tonic::server::UnaryService<HealthRequest> for HealthServer {
    type Response = HealthResponse;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<HealthRequest>) -> Self::Future {
        assert!(request.get_ref().service.is_empty());
        let status = self.0.state.lock().unwrap().health_status;
        Box::pin(async move { Ok(tonic::Response::new(HealthResponse { status })) })
    }
}

#[test]
fn tso_proxy_batch_healthy_follower_and_disabled_mode() {
    let leader = Server::start_auto_batching();
    let follower = Server::start_auto_batching();
    let leader_url = leader.address.clone();
    let follower_url = follower.address.clone();
    let members = vec![
        pdpb::Member {
            member_id: 1,
            client_urls: vec![leader_url.clone()],
            ..Default::default()
        },
        pdpb::Member {
            member_id: 2,
            client_urls: vec![follower_url],
            ..Default::default()
        },
    ];
    let membership = pdpb::GetMembersResponse {
        header: Some(header()),
        leader: Some(members[0].clone()),
        members,
        ..Default::default()
    };
    leader.state.lock().unwrap().members = Some(membership.clone());
    leader.state.lock().unwrap().health_status = 2;
    follower.state.lock().unwrap().members = Some(membership);
    let options = tikv_client::pd_options::Options::new();
    options.set_enable_tso_follower_proxy(true);
    let client = PdClient::connect_seeds_with_options(
        [leader.address.clone()],
        Arc::new(tidb_pd_client::ClusterSecurity::default()),
        options,
    )
    .unwrap();
    client.get_timestamp().unwrap();
    assert_eq!(
        follower.state.lock().unwrap().forwarding,
        vec![Some(leader_url)]
    );
    assert!(leader.state.lock().unwrap().requests.is_empty());
    // Retain an unchanged healthy stream across ordinary metadata refresh.
    client.refresh_members().unwrap();
    client.get_timestamp().unwrap();
    assert_eq!(follower.state.lock().unwrap().stream_opens, 1);
    // Dynamic disabling restores the primary regardless of proxy health policy.
    leader.state.lock().unwrap().auto_batch_physical = Some(1000);
    client.set_enable_tso_follower_proxy(false);
    client.get_timestamp().unwrap();
    assert_eq!(leader.state.lock().unwrap().forwarding, vec![None]);
    follower.state.lock().unwrap().auto_batch_physical = Some(2000);
    client.set_enable_tso_follower_proxy(true);
    client.get_timestamp().unwrap();
    assert_eq!(follower.state.lock().unwrap().stream_opens, 2);
    // A membership change under the same leader invalidates cached proxy routes.
    let membership = leader.state.lock().unwrap().members.clone().unwrap();
    let mut only_leader = membership.clone();
    only_leader.members.retain(|member| member.member_id == 1);
    leader.state.lock().unwrap().members = Some(only_leader);
    leader.state.lock().unwrap().health_status = 1;
    leader.state.lock().unwrap().auto_batch_physical = Some(3000);
    client.refresh_members().unwrap();
    let follower_requests = follower.state.lock().unwrap().requests.len();
    client.get_timestamp().unwrap();
    assert_eq!(
        follower.state.lock().unwrap().requests.len(),
        follower_requests
    );
    leader.state.lock().unwrap().members = Some(membership);
    leader.state.lock().unwrap().health_status = 2;
    follower.state.lock().unwrap().auto_batch_physical = Some(4000);
    client.refresh_members().unwrap();
    // Close cancels a pending proxy exchange and retires the shared wire owner.
    follower
        .state
        .lock()
        .unwrap()
        .replies
        .push_back(TsoReply::Delayed(
            Duration::from_secs(10),
            timestamp(5000, 1),
        ));
    let before = follower.state.lock().unwrap().requests.len();
    let pending_client = client.clone();
    let request = std::thread::spawn(move || pending_client.get_timestamp());
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while follower.state.lock().unwrap().requests.len() == before {
        assert!(std::time::Instant::now() < until);
        std::thread::yield_now();
    }
    // A live policy update retires the blocked proxy and retries on the primary.
    leader.state.lock().unwrap().auto_batch_physical = Some(6000);
    client.set_enable_tso_follower_proxy(false);
    assert!(request.join().unwrap().unwrap() >= (6000_u64 << 18));
    follower.state.lock().unwrap().auto_batch_physical = Some(7000);
    client.set_enable_tso_follower_proxy(true);
    follower
        .state
        .lock()
        .unwrap()
        .replies
        .push_back(TsoReply::Delayed(
            Duration::from_secs(10),
            timestamp(8000, 1),
        ));
    let before = follower.state.lock().unwrap().requests.len();
    let pending_client = client.clone();
    let request = std::thread::spawn(move || pending_client.get_timestamp());
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while follower.state.lock().unwrap().requests.len() == before {
        assert!(std::time::Instant::now() < until);
        std::thread::yield_now();
    }
    client.shutdown().unwrap();
    assert_eq!(
        request.join().unwrap(),
        Err(tidb_pd_client::PdClientError::Closed)
    );
}

#[test]
fn bootstrap_policy_batch_forced_pd_provider_survives_api_refresh() {
    let pd = Server::start_auto_batching();
    let tso = Server::start_auto_batching();
    pd.state.lock().unwrap().cluster_info = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        tso_urls: vec![tso.address.clone()],
        ..Default::default()
    });
    let mut options = tikv_client::pd_options::Options::new();
    options.use_tso_server_proxy = true;
    let client = PdClient::connect_seeds_with_options(
        [pd.address.clone()],
        Arc::new(tidb_pd_client::ClusterSecurity::default()),
        options,
    )
    .unwrap();
    client.get_timestamp().unwrap();
    client.refresh_members().unwrap();
    client.get_timestamp().unwrap();
    assert_eq!(
        pd.state.lock().unwrap().requests.len(),
        2,
        "forced provider must stay on PD"
    );
    assert!(tso.state.lock().unwrap().micro_requests.is_empty());
    client.shutdown().unwrap();
}

#[test]
fn bootstrap_policy_batch_dial_options_apply_once_in_order() {
    let pd = Server::start_auto_batching();
    let applied = Arc::new(Mutex::new(Vec::new()));
    let mut options = tikv_client::pd_options::Options::new();
    for index in [1, 2] {
        let applied = applied.clone();
        options.grpc_dial_options.push(Arc::new(move |endpoint| {
            applied.lock().unwrap().push(index);
            endpoint.user_agent(format!("policy-{index}")).unwrap()
        }));
    }
    let client = PdClient::connect_seeds_with_options(
        [pd.address.clone()],
        Arc::new(tidb_pd_client::ClusterSecurity::default()),
        options,
    )
    .unwrap();
    client.get_timestamp().unwrap();
    client.refresh_members().unwrap();
    client.get_timestamp().unwrap();
    assert_eq!(*applied.lock().unwrap(), vec![1, 2]);
    assert!(pd
        .state
        .lock()
        .unwrap()
        .user_agents
        .iter()
        .all(|agent| agent.starts_with("policy-2")));
    assert_eq!(pd.state.lock().unwrap().peers.len(), 1);
    client.shutdown().unwrap();
}

#[test]
fn bootstrap_policy_batch_member_url_follows_configured_scheme() {
    let pd = Server::start_auto_batching();
    let member = pdpb::Member {
        member_id: 1,
        client_urls: vec!["https://127.0.0.1:0".to_owned(), pd.address.clone()],
        ..Default::default()
    };
    pd.state.lock().unwrap().members = Some(pdpb::GetMembersResponse {
        header: Some(header()),
        leader: Some(member.clone()),
        members: vec![member],
        ..Default::default()
    });
    let client = PdClient::connect(&pd.address, Duration::from_millis(150)).unwrap();
    assert!(client.get_timestamp().unwrap() > 0);
    client.shutdown().unwrap();
}

fn tso_failure_pair(setup_failure: bool) -> (Server, Server, PdClient) {
    let leader = Server::start_auto_batching();
    let follower = Server::start_auto_batching();
    let members = vec![
        pdpb::Member {
            member_id: 1,
            client_urls: vec![leader.address.clone()],
            ..Default::default()
        },
        pdpb::Member {
            member_id: 2,
            client_urls: vec![follower.address.clone()],
            ..Default::default()
        },
    ];
    let membership = pdpb::GetMembersResponse {
        header: Some(header()),
        leader: Some(members[0].clone()),
        members,
        ..Default::default()
    };
    leader.state.lock().unwrap().members = Some(membership.clone());
    follower.state.lock().unwrap().members = Some(membership);
    leader.state.lock().unwrap().health_status = 2;
    if setup_failure {
        leader.state.lock().unwrap().stream_status =
            Some((tonic::Code::Unavailable, "primary unavailable"));
    } else {
        leader.state.lock().unwrap().auto_status =
            Some((tonic::Code::Unavailable, "response unavailable"));
    }
    let mut options = tikv_client::pd_options::Options::new();
    options.timeout = Duration::from_secs(8);
    options.enable_forwarding = true;
    let client = PdClient::connect_seeds_with_options(
        &[leader.address.clone()],
        Arc::new(tidb_pd_client::ClusterSecurity::default()),
        options,
    )
    .unwrap();
    (leader, follower, client)
}

#[test]
fn tso_failure_batch_automatic_forwarding_and_recovery() {
    let (leader, follower, client) = tso_failure_pair(true);
    client
        .get_timestamp()
        .expect("automatic forwarding must use a healthy backup");
    assert_eq!(
        follower.state.lock().unwrap().forwarding,
        vec![Some(leader.address.clone())]
    );
    client.get_timestamp().unwrap();
    assert_eq!(follower.state.lock().unwrap().stream_opens, 1);
    {
        let mut state = leader.state.lock().unwrap();
        state.cluster_info_failure = true;
        state.stream_status = None;
        state.auto_batch_physical = Some(10_000);
        state.health_status = 1;
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while leader.state.lock().unwrap().requests.len() <= 6 {
        assert!(
            std::time::Instant::now() < deadline,
            "recovery must return to the primary"
        );
        std::thread::sleep(Duration::from_millis(100));
        client.get_timestamp().unwrap();
    }
    assert!(leader
        .state
        .lock()
        .unwrap()
        .forwarding
        .iter()
        .all(Option::is_none));
    client.shutdown().unwrap();
}

#[test]
fn tso_failure_batch_response_error_does_not_enable_forwarding() {
    let (_leader, follower, client) = tso_failure_pair(false);
    assert!(
        client.get_timestamp().is_err(),
        "an established response error cannot enable connection fallback"
    );
    assert_eq!(follower.state.lock().unwrap().stream_opens, 0);
    client.shutdown().unwrap();
}

#[test]
fn collection_batch_configured_wait() {
    let server = Server::start_auto_batching();
    let mut options = tikv_client::pd_options::Options::new();
    options.timeout = Duration::from_secs(5);
    options
        .set_max_tso_batch_wait_interval(Duration::from_millis(10))
        .unwrap();
    let client =
        PdClient::connect_seeds_with_options([server.address.clone()], Default::default(), options)
            .unwrap();
    client.get_timestamp().unwrap();
    let started = std::time::Instant::now();
    client.get_timestamp().unwrap();
    let elapsed = started.elapsed();
    client.shutdown().unwrap();
    assert!(
        elapsed >= Duration::from_millis(9),
        "adapter ignored batch wait: {elapsed:?}"
    );
}

#[test]
fn collection_batch_uses_twenty_thousand_request_bound() {
    let server = Server::start_auto_batching();
    // Keep the first exchange in flight so the next collector sees the full queue.
    server
        .state
        .lock()
        .unwrap()
        .replies
        .push_back(TsoReply::Delayed(
            Duration::from_millis(500),
            timestamp(0, 1),
        ));
    let client = PdClient::connect(&server.address, Duration::from_secs(10)).unwrap();
    let first = client.get_timestamp_async().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while server.state.lock().unwrap().requests.is_empty() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let waiters = (0..20_001)
        .map(|_| client.get_timestamp_async().unwrap())
        .collect::<Vec<_>>();
    first.wait().unwrap();
    for waiter in waiters {
        waiter.wait().unwrap();
    }
    client.shutdown().unwrap();
    let counts = server
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .map(|r| r.count)
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![1, 20_000, 1]);
}

#[test]
fn collection_batch_preserves_other_commands_and_joins_shutdown() {
    let server = Server::start_auto_batching();
    let mut options = tikv_client::pd_options::Options::new();
    options.timeout = Duration::from_secs(3);
    options
        .set_max_tso_batch_wait_interval(Duration::from_millis(10))
        .unwrap();
    let owner =
        PdClient::connect_seeds_with_options([server.address.clone()], Default::default(), options)
            .unwrap();
    let timestamp = owner.get_timestamp_async().unwrap();
    let metadata_client = owner.clone();
    let metadata = std::thread::spawn(move || metadata_client.refresh_members());
    assert!(timestamp.wait().is_ok());
    assert!(metadata.join().unwrap().is_ok());
    let pending = owner.get_timestamp_async().unwrap();
    owner.shutdown().unwrap();
    // A completed reply may win shutdown; otherwise its owned sender is closed.
    match pending.wait() {
        Ok(_) | Err(tidb_pd_client::PdClientError::Closed) => {}
        other => panic!("unexpected shutdown result: {other:?}"),
    }
}

#[test]
fn observation_batch_adapter_keeps_provider_after_invalid_mode_observations() {
    for microservice in [false, true] {
        let pd = Server::start_auto_batching();
        let tso = Server::start_auto_batching();
        if microservice {
            pd.state.lock().unwrap().cluster_info = Some(pdpb::GetClusterInfoResponse {
                service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
                tso_urls: vec![tso.address.clone()],
                ..Default::default()
            });
        }
        let client = PdClient::connect(&pd.address, Duration::from_secs(1)).unwrap();
        client.get_timestamp().unwrap();
        for (proxy, observation) in [
            (true, pdpb::GetClusterInfoResponse::default()),
            (
                false,
                pdpb::GetClusterInfoResponse {
                    header: Some(pdpb::ResponseHeader {
                        cluster_id: CLUSTER_ID,
                        error: Some(pdpb::Error {
                            message: "observation rejected".into(),
                            ..Default::default()
                        }),
                    }),
                    ..Default::default()
                },
            ),
        ] {
            pd.state.lock().unwrap().cluster_info = Some(observation);
            // Changing the live proxy policy forces a refresh without waiting
            // for the periodic discovery timer.
            client.set_enable_tso_follower_proxy(proxy);
            client
                .get_timestamp()
                .expect("failed observation must retain the accepted provider");
        }
        if microservice {
            assert!(pd.state.lock().unwrap().requests.is_empty());
            assert_eq!(tso.state.lock().unwrap().micro_requests.len(), 3);
        } else {
            assert_eq!(pd.state.lock().unwrap().requests.len(), 3);
        }
        client.shutdown().unwrap();
    }
}

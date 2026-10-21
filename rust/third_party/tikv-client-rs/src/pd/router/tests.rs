// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};
use tonic::transport::Channel;

use super::*;
use crate::async_util::Cancellation;
use crate::logutil::test_capture;
use crate::pd::opt::{GetRegionOp, Options};
use crate::pd::service_discovery::{
    LeaderSwitchedCallback, MembersChangedCallback, ServiceCallbacks,
};
use crate::Error;

/// What a fake server answers and saw.
#[derive(Default)]
struct RegionState {
    /// Every `QueryRegionRequest`, in arrival order.
    requests: Mutex<Vec<pdpb::QueryRegionRequest>>,
    /// The next response carries this header error.
    header_error: Mutex<Option<pdpb::Error>>,
    /// Streams opened.
    streams: AtomicUsize,
    /// Answer no request (the batch waits).
    stall: AtomicBool,
    /// Map the keys to IDs but carry no regions.
    omit_regions: AtomicBool,
}

impl RegionState {
    /// A region per key: id `key[0]`, `key[0] + 100` for a previous key; a
    /// region per id. Buckets when the batch asks for them.
    fn answer(&self, request: pdpb::QueryRegionRequest) -> pdpb::QueryRegionResponse {
        self.requests.lock().unwrap().push(request.clone());
        let mut response = pdpb::QueryRegionResponse {
            header: Some(pdpb::ResponseHeader {
                cluster_id: 7,
                error: self.header_error.lock().unwrap().take(),
            }),
            ..Default::default()
        };
        let mut region = |id: u64, start_key: Vec<u8>| {
            response.regions_by_id.insert(
                id,
                pdpb::RegionResponse {
                    region: Some(metapb::Region {
                        id,
                        start_key,
                        ..Default::default()
                    }),
                    leader: Some(metapb::Peer {
                        id: id * 10,
                        store_id: 1,
                        ..Default::default()
                    }),
                    down_peers: vec![pdpb::PeerStats {
                        peer: Some(metapb::Peer {
                            id: id * 10 + 1,
                            ..Default::default()
                        }),
                        down_seconds: 9,
                    }],
                    buckets: request.need_buckets.then(|| metapb::Buckets {
                        region_id: id,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        };
        for key in &request.keys {
            let id = u64::from(key.first().copied().unwrap_or(0));
            region(id, key.clone());
            response.key_id_map.push(id);
        }
        for key in &request.prev_keys {
            let id = u64::from(key.first().copied().unwrap_or(0)) + 100;
            region(id, key.clone());
            response.prev_key_id_map.push(id);
        }
        for id in &request.ids {
            region(*id, Vec::new());
        }
        if self.omit_regions.load(Ordering::SeqCst) {
            response.regions_by_id.clear();
        }
        response
    }
}

/// A `QueryRegion` server as PD (`pdpb.PD`) or a router node
/// (`routerpb.Router`).
#[derive(Clone)]
struct RegionServer<const ROUTER: bool>(Arc<RegionState>);

impl tonic::server::NamedService for RegionServer<false> {
    const NAME: &'static str = "pdpb.PD";
}

impl tonic::server::NamedService for RegionServer<true> {
    const NAME: &'static str = "routerpb.Router";
}

impl<B, const ROUTER: bool> Service<http::Request<B>> for RegionServer<ROUTER>
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let service = self.clone();
        if request.uri().path().ends_with("/QueryRegion") {
            return Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .streaming(service, request)
                        .await,
                )
            });
        }
        Box::pin(async move {
            Ok(http::Response::builder()
                .status(200)
                .header("grpc-status", "12")
                .header("content-type", "application/grpc")
                .body(tonic::body::Body::empty())
                .unwrap())
        })
    }
}

type ResponseStream = Pin<
    Box<dyn Stream<Item = std::result::Result<pdpb::QueryRegionResponse, tonic::Status>> + Send>,
>;

impl<const ROUTER: bool> tonic::server::StreamingService<pdpb::QueryRegionRequest>
    for RegionServer<ROUTER>
{
    type Response = pdpb::QueryRegionResponse;
    type ResponseStream = ResponseStream;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

    fn call(
        &mut self,
        request: tonic::Request<tonic::Streaming<pdpb::QueryRegionRequest>>,
    ) -> Self::Future {
        let state = self.0.clone();
        state.streams.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let responses = request.into_inner().then(move |request| {
                let state = state.clone();
                async move {
                    let request = request?;
                    if state.stall.load(Ordering::SeqCst) {
                        state.requests.lock().unwrap().push(request);
                        futures::future::pending::<()>().await;
                        unreachable!();
                    }
                    Ok(state.answer(request))
                }
            });
            Ok(tonic::Response::new(Box::pin(responses) as ResponseStream))
        })
    }
}

struct Server {
    url: String,
    state: Arc<RegionState>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(router: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(RegionState::default());
        let incoming = TcpListenerStream::new(listener);
        let task = if router {
            let service = RegionServer::<true>(state.clone());
            tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming(incoming)
                    .await;
            })
        } else {
            let service = RegionServer::<false>(state.clone());
            tokio::spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(service)
                    .serve_with_incoming(incoming)
                    .await;
            })
        };
        Self { url, state, task }
    }

    fn channel(&self) -> Channel {
        Channel::from_shared(self.url.clone())
            .unwrap()
            .connect_lazy()
    }
}

/// A discovery whose leader and connections a test sets, recording the
/// member checks the router client asks for.
#[derive(Default)]
struct FakeDiscovery {
    leader: RwLock<String>,
    conns: RwLock<Vec<(String, Channel)>>,
    callbacks: ServiceCallbacks,
    scheduled_checks: AtomicUsize,
    checks: AtomicUsize,
}

impl FakeDiscovery {
    fn new(leader: &str, conns: Vec<(String, Channel)>) -> Arc<Self> {
        Arc::new(Self {
            leader: RwLock::new(leader.to_owned()),
            conns: RwLock::new(conns),
            ..Default::default()
        })
    }

    fn switch_leader(&self, leader: &str) {
        *self.leader.write().unwrap() = leader.to_owned();
        self.callbacks.on_leader_switched(leader).unwrap();
    }

    fn set_conns(&self, conns: Vec<(String, Channel)>) {
        *self.conns.write().unwrap() = conns;
        self.callbacks.on_members_changed();
    }
}

#[async_trait]
impl ServiceDiscovery for FakeDiscovery {
    fn get_cluster_id(&self) -> u64 {
        7
    }
    fn get_serving_url(&self) -> String {
        self.leader.read().unwrap().clone()
    }
    fn get_client_conns(&self) -> Vec<(String, Channel)> {
        self.conns.read().unwrap().clone()
    }
    fn schedule_check_member_changed(&self) {
        self.scheduled_checks.fetch_add(1, Ordering::SeqCst);
    }
    async fn check_member_changed(&self) -> Result<()> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn exec_and_add_leader_switched_callback(&self, callback: LeaderSwitchedCallback) {
        let url = self.get_serving_url();
        if !url.is_empty() {
            callback(&url).unwrap();
        }
        self.callbacks.add_leader_switched_callback(callback);
    }
    fn add_members_changed_callback(&self, callback: MembersChangedCallback) {
        self.callbacks.add_members_changed_callback(callback);
    }
}

fn options(timeout: Duration) -> Arc<Options> {
    let mut options = Options::new();
    options.timeout = timeout;
    Arc::new(options)
}

const TIMEOUT: Duration = Duration::from_secs(3);

fn buckets() -> GetRegionOp {
    GetRegionOp {
        need_buckets: true,
        ..Default::default()
    }
}

async fn wait_until(description: &str, condition: impl Fn() -> bool) {
    for _ in 0..400 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {description}");
}

fn has_line(expected: &str) -> bool {
    test_capture::any(|line| line == expected)
}

fn assert_line(expected: &str) {
    assert!(
        has_line(expected),
        "{expected:?} not in {:?}",
        test_capture::lines()
    );
}

/// pd-client `router.Cli`: keys, previous keys and IDs in one batch map to
/// their regions through the response's ID maps; buckets are kept only for
/// the requests that asked; each region is the request's own copy.
#[tokio::test]
async fn router_client_batches_queries_on_the_leader_stream() {
    test_capture::install();
    let leader = Server::start(false).await;
    let discovery = FakeDiscovery::new(&leader.url, vec![(leader.url.clone(), leader.channel())]);
    let cli = Cli::new(
        &Cancellation::default(),
        discovery.clone(),
        None,
        options(TIMEOUT),
    );
    let (a, b, prev, by_id) = tokio::join!(
        cli.get_region(vec![1], buckets(), TIMEOUT),
        cli.get_region(vec![2], GetRegionOp::default(), TIMEOUT),
        cli.get_prev_region(vec![3], GetRegionOp::default(), TIMEOUT),
        cli.get_region_by_id(42, buckets(), TIMEOUT),
    );
    let a = a.unwrap().unwrap();
    assert_eq!(a.meta.id, 1);
    assert_eq!(a.meta.start_key, vec![1]);
    assert_eq!(a.leader.as_ref().unwrap().id, 10);
    assert_eq!(
        a.down_peers,
        vec![metapb::Peer {
            id: 11,
            ..Default::default()
        }]
    );
    assert_eq!(a.buckets.as_ref().unwrap().region_id, 1);
    let b = b.unwrap().unwrap();
    assert_eq!(b.meta.id, 2);
    assert!(b.buckets.is_none(), "buckets are dropped unless asked for");
    assert_eq!(prev.unwrap().unwrap().meta.id, 103);
    assert_eq!(by_id.unwrap().unwrap().buckets.unwrap().region_id, 42);
    let requests = leader.state.requests.lock().unwrap().clone();
    let keys: usize = requests.iter().map(|r| r.keys.len()).sum();
    let prev_keys: usize = requests.iter().map(|r| r.prev_keys.len()).sum();
    let ids: Vec<u64> = requests.iter().flat_map(|r| r.ids.clone()).collect();
    assert_eq!((keys, prev_keys, ids), (2, 1, vec![42]));
    assert!(requests
        .iter()
        .all(|r| r.header.as_ref().unwrap().cluster_id == 7));
    assert_eq!(leader.state.streams.load(Ordering::SeqCst), 1, "one stream");
    for line in [
        "INFO [router] connection daemon is started",
        "INFO [router] dispatcher is started",
        &format!(
            "INFO [router] successfully established the leader router stream connection url={}",
            leader.url
        ),
    ] {
        assert_line(line);
    }
    cli.close().await;
}

/// A region the response does not carry is `None` (Go returns a nil region
/// and no error).
#[tokio::test]
async fn router_client_answers_none_for_a_missing_region() {
    let leader = Server::start(false).await;
    leader.state.omit_regions.store(true, Ordering::SeqCst);
    let discovery = FakeDiscovery::new(&leader.url, vec![(leader.url.clone(), leader.channel())]);
    let cli = Cli::new(&Cancellation::default(), discovery, None, options(TIMEOUT));
    let (key, id) = tokio::join!(
        cli.get_region(vec![1], GetRegionOp::default(), TIMEOUT),
        cli.get_region_by_id(5, GetRegionOp::default(), TIMEOUT),
    );
    assert_eq!(key.unwrap(), None);
    assert_eq!(id.unwrap(), None);
    cli.close().await;
}

/// A header error fails the batch with the error's text, releases the
/// stream and schedules a member check; the next batch opens a new stream.
#[tokio::test]
async fn router_client_header_error_fails_the_batch_and_reconnects() {
    test_capture::install();
    let leader = Server::start(false).await;
    let discovery = FakeDiscovery::new(&leader.url, vec![(leader.url.clone(), leader.channel())]);
    let cli = Cli::new(
        &Cancellation::default(),
        discovery.clone(),
        None,
        options(TIMEOUT),
    );
    *leader.state.header_error.lock().unwrap() = Some(pdpb::Error {
        r#type: pdpb::ErrorType::NotBootstrapped as i32,
        message: "cluster is not bootstrapped".to_owned(),
    });
    let error = cli
        .get_region(vec![1], GetRegionOp::default(), TIMEOUT)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        // gogo's compact text, trailing space included.
        r#"type:NOT_BOOTSTRAPPED message:"cluster is not bootstrapped" "#
    );
    assert!(test_capture::any(|line| line.starts_with(&format!(
        "ERROR [router] failed to process the router requests stream-url={} error=",
        leader.url
    ))));
    assert_eq!(discovery.scheduled_checks.load(Ordering::SeqCst), 1);
    let region = cli
        .get_region(vec![4], GetRegionOp::default(), TIMEOUT)
        .await
        .unwrap();
    assert_eq!(region.unwrap().meta.id, 4);
    assert_eq!(leader.state.streams.load(Ordering::SeqCst), 2);
    cli.close().await;
}

/// Without a leader connection no stream can be chosen: the batch is
/// aborted after the client timeout with `ErrClientRouterConnectionTimeout`.
#[tokio::test]
async fn router_client_aborts_a_batch_without_a_stream_after_the_timeout() {
    test_capture::install();
    let url = "http://127.0.0.1:1/router-no-conn";
    let discovery = FakeDiscovery::new(url, Vec::new());
    let cli = Cli::new(
        &Cancellation::default(),
        discovery.clone(),
        None,
        options(Duration::from_millis(200)),
    );
    let error = cli
        .get_region(vec![1], GetRegionOp::default(), TIMEOUT)
        .await
        .unwrap_err();
    assert!(matches!(
        &error,
        Error::Pd(error) if error.is_prototype(crate::pd::errs::ERR_CLIENT_ROUTER_CONNECTION_TIMEOUT)
    ));
    assert_line(&format!(
        "WARN [router] got an invalid leader client connection url={url}"
    ));
    assert_line("INFO [router] router stream connection is not ready");
    assert_line(
        "ERROR [router] router stream connection is not ready until timeout, abort the batch",
    );
    assert!(discovery.scheduled_checks.load(Ordering::SeqCst) >= 1);
    cli.close().await;
}

/// The leader-switched callback moves the leader stream; enabling follower
/// handling streams to every member and the option change is logged;
/// disabling it releases the non-leader streams.
#[tokio::test]
async fn router_client_follows_the_leader_and_the_follower_handle_option() {
    test_capture::install();
    let first = Server::start(false).await;
    let second = Server::start(false).await;
    let conns = vec![
        (first.url.clone(), first.channel()),
        (second.url.clone(), second.channel()),
    ];
    let discovery = FakeDiscovery::new(&first.url, conns.clone());
    let option = options(TIMEOUT);
    let cli = Cli::new(
        &Cancellation::default(),
        discovery.clone(),
        None,
        option.clone(),
    );
    wait_until("the leader stream", || {
        cli.connection_urls_for_test().0 == vec![first.url.clone()]
    })
    .await;
    discovery.switch_leader(&second.url);
    assert_eq!(cli.leader_url_for_test(), second.url);
    assert_line(&format!(
        "INFO [router] switch the router leader serving url old-url={} new-url={}",
        first.url, second.url
    ));
    wait_until("the new leader stream alone", || {
        cli.connection_urls_for_test().0 == vec![second.url.clone()]
    })
    .await;
    assert_line(&format!(
        "INFO [router] release the non-leader router stream connection url={}",
        first.url
    ));
    option.set_enable_follower_handle(true);
    let mut both = vec![first.url.clone(), second.url.clone()];
    both.sort();
    wait_until("streams to every member", || {
        cli.connection_urls_for_test().0 == both
    })
    .await;
    assert_line("INFO [router] follower handle status changed enable=true");
    assert_line(&format!(
        "INFO [router] successfully established the router stream connection url={}",
        first.url
    ));
    // A member that leaves releases its stream.
    discovery.set_conns(vec![(second.url.clone(), second.channel())]);
    wait_until("the departed member's stream released", || {
        cli.connection_urls_for_test().0 == vec![second.url.clone()]
    })
    .await;
    assert_line(&format!(
        "INFO [router] release the stale router stream connection url={}",
        first.url
    ));
    cli.close().await;
}

/// Batches whose every request allows the router service go to a router
/// node's stream; one request that does not sends the batch to PD.
#[tokio::test]
async fn router_client_sends_router_service_batches_to_router_nodes() {
    test_capture::install();
    let leader = Server::start(false).await;
    let node = Server::start(true).await;
    let discovery = FakeDiscovery::new(&leader.url, vec![(leader.url.clone(), leader.channel())]);
    let ms = FakeDiscovery::new("", vec![(node.url.clone(), node.channel())]);
    let cli = Cli::new(
        &Cancellation::default(),
        discovery,
        Some(ms.clone()),
        options(TIMEOUT),
    );
    wait_until("the router service stream", || {
        cli.connection_urls_for_test().1 == vec![node.url.clone()]
    })
    .await;
    assert_line(&format!(
        "INFO [router] successfully established the router service stream connection url={}",
        node.url
    ));
    let allowed = GetRegionOp {
        allow_router_service_handle: true,
        ..Default::default()
    };
    cli.get_region(vec![1], allowed, TIMEOUT).await.unwrap();
    assert_eq!(node.state.requests.lock().unwrap().len(), 1);
    assert!(leader.state.requests.lock().unwrap().is_empty());
    cli.get_region(vec![2], GetRegionOp::default(), TIMEOUT)
        .await
        .unwrap();
    assert_eq!(leader.state.requests.lock().unwrap().len(), 1);
    // A node that leaves the router service releases its stream.
    ms.set_conns(Vec::new());
    // An empty node list keeps the streams (Go returns before its GC).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cli.connection_urls_for_test().1, vec![node.url.clone()]);
    cli.close().await;
}

/// `Close` logs pd-client's lines and ends both goroutines; a request on a
/// closed client fails with its ctx's error.
#[tokio::test]
async fn router_client_close_logs_and_cancels_requests() {
    test_capture::install();
    let leader = Server::start(false).await;
    leader.state.stall.store(true, Ordering::SeqCst);
    let discovery = FakeDiscovery::new(&leader.url, vec![(leader.url.clone(), leader.channel())]);
    let cli = Cli::new(&Cancellation::default(), discovery, None, options(TIMEOUT));
    let waiting = {
        let cli = cli.clone();
        tokio::spawn(async move {
            cli.get_region(vec![1], GetRegionOp::default(), TIMEOUT)
                .await
        })
    };
    wait_until("the stalled batch", || {
        !leader.state.requests.lock().unwrap().is_empty()
    })
    .await;
    cli.close().await;
    assert!(matches!(
        waiting.await.unwrap(),
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        cli.get_region(vec![1], GetRegionOp::default(), TIMEOUT)
            .await,
        Err(Error::ContextCanceled)
    ));
    for line in [
        "INFO [router] closing router client",
        "INFO [router] connection daemon is exiting",
        "INFO [router] dispatcher is exiting",
        "INFO [router] dispatcher exited",
        "INFO [router] router client is closed",
    ] {
        assert_line(line);
    }
}

/// A request's own deadline ends its wait (Go's request ctx).
#[tokio::test]
async fn router_client_request_deadline_ends_the_wait() {
    let leader = Server::start(false).await;
    leader.state.stall.store(true, Ordering::SeqCst);
    let discovery = FakeDiscovery::new(&leader.url, vec![(leader.url.clone(), leader.channel())]);
    let cli = Cli::new(&Cancellation::default(), discovery, None, options(TIMEOUT));
    let error = cli
        .get_region(vec![1], GetRegionOp::default(), Duration::from_millis(100))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::GrpcAPI(status) if status.code() == tonic::Code::DeadlineExceeded),
        "{error:?}"
    );
    cli.close().await;
}

/// Go's dispatcher panics on a request that is neither a key, a previous
/// key nor an ID; the unrecovered panic ends the process (exit status 2).
#[tokio::test]
async fn router_client_invalid_request_panic_ends_the_process() {
    let leader = Server::start(false).await;
    let discovery = FakeDiscovery::new(&leader.url, vec![(leader.url.clone(), leader.channel())]);
    let cli = Cli::new(&Cancellation::default(), discovery, None, options(TIMEOUT));
    let exits = crate::logutil::exits_for_test()
        .iter()
        .filter(|code| **code == 2)
        .count();
    let _ = cli
        .get_region_by_id(0, GetRegionOp::default(), Duration::from_millis(300))
        .await;
    wait_until("the process exit", || {
        crate::logutil::exits_for_test()
            .iter()
            .filter(|code| **code == 2)
            .count()
            > exits
    })
    .await;
    cli.close().await;
}

/// `ConvertToRegion`'s copy keeps the peers of the down-peer stats only.
#[test]
fn region_from_response_keeps_down_peers_and_drops_empty_regions() {
    assert!(Region::from_response(pdpb::RegionResponse::default()).is_none());
    let region = Region::from_response(pdpb::RegionResponse {
        region: Some(metapb::Region {
            id: 3,
            ..Default::default()
        }),
        down_peers: vec![
            pdpb::PeerStats {
                peer: Some(metapb::Peer {
                    id: 4,
                    ..Default::default()
                }),
                down_seconds: 1,
            },
            pdpb::PeerStats::default(),
        ],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(region.meta.id, 3);
    assert_eq!(region.down_peers.len(), 1);
}

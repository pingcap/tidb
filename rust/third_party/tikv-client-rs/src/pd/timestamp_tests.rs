// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::{http, Body, BoxFuture, Service, StdError};

use super::*;
use crate::pd::{Connection, RetryClient, RetryClientTrait};
use crate::proto::{keyspacepb, metapb, pdpb, tsopb};
use crate::SecurityManager;

#[derive(Clone, Copy)]
enum Reply {
    Timestamp,
    StallBody,
    StallHeaders,
    End,
}

#[derive(Clone)]
struct PdServer {
    endpoint: String,
    user_agents: Arc<std::sync::Mutex<Vec<String>>>,
    wire_routes: Arc<std::sync::Mutex<Vec<(String, Option<String>, bool)>>>,
    leader_urls: Arc<std::sync::RwLock<Vec<String>>>,
    region_members: Arc<std::sync::RwLock<Option<Vec<pdpb::Member>>>>,
    region_metadata: Arc<std::sync::Mutex<Vec<bool>>>,
    region_failure: Arc<AtomicUsize>,
    health_status: Arc<AtomicUsize>,
    health_requests: Arc<AtomicUsize>,
    health_stall: Arc<std::sync::atomic::AtomicBool>,
    member_failures: Arc<AtomicUsize>,
    member_requests: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    stall_members: Arc<std::sync::atomic::AtomicBool>,
    stall_scans: Arc<std::sync::atomic::AtomicBool>,
    region_entered: Arc<tokio::sync::Semaphore>,
    region_release: Arc<tokio::sync::Semaphore>,
    region_id: Arc<AtomicUsize>,
    group: Arc<std::sync::RwLock<tsopb::KeyspaceGroup>>,
    revision: Arc<AtomicUsize>,
    discovery_requests: Arc<AtomicUsize>,
    stall_discovery: Arc<std::sync::atomic::AtomicBool>,
    tso_headers: Arc<std::sync::Mutex<Vec<tsopb::RequestHeader>>>,
    cluster_info: Arc<std::sync::RwLock<Option<pdpb::GetClusterInfoResponse>>>,
    min_response:
        Arc<std::sync::RwLock<std::result::Result<pdpb::GetMinTsResponse, tonic::Status>>>,
    min_requests: Arc<AtomicUsize>,
    omit_metadata_header: Arc<std::sync::atomic::AtomicBool>,
    required_keyspace: Arc<AtomicUsize>,
    keyspace_loads: Arc<AtomicUsize>,
    reply: Reply,
    tso_failure: Arc<std::sync::Mutex<Option<tonic::Code>>>,
    cluster_info_failure: Arc<std::sync::atomic::AtomicBool>,
    received: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

struct ActiveStream(Arc<AtomicUsize>);
impl Drop for ActiveStream {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl tonic::server::NamedService for PdServer {
    const NAME: &'static str = "pdpb.PD";
}

impl<B> Service<http::Request<B>> for PdServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        self.user_agents.lock().unwrap().push(
            request
                .headers()
                .get("user-agent")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
        );
        self.wire_routes.lock().unwrap().push((
            request.uri().path().to_owned(),
            request
                .headers()
                .get("pd-forwarded-host")
                .map(|v| v.to_str().unwrap().to_owned()),
            request.headers().contains_key("pd-allow-follower-handle"),
        ));
        let service = self.clone();
        match request.uri().path() {
            "/pdpb.PD/GetMinTS" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::<
                    pdpb::GetMinTsResponse,
                    pdpb::GetMinTsRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetClusterInfo" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::<
                    pdpb::GetClusterInfoResponse,
                    pdpb::GetClusterInfoRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetMembers" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::<
                    pdpb::GetMembersResponse,
                    pdpb::GetMembersRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetRegion" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::<
                    pdpb::GetRegionResponse,
                    pdpb::GetRegionRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/GetAllStores" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::<
                    pdpb::GetAllStoresResponse,
                    pdpb::GetAllStoresRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/ScanRegions" => Box::pin(async move {
                Ok(tonic::server::Grpc::new(tonic::codec::ProstCodec::<
                    pdpb::ScanRegionsResponse,
                    pdpb::ScanRegionsRequest,
                >::default())
                .unary(service, request)
                .await)
            }),
            "/pdpb.PD/Tso" => Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .streaming(service, request)
                        .await,
                )
            }),
            _ => Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(tonic::body::empty_body())
                    .unwrap())
            }),
        }
    }
}

impl tonic::server::UnaryService<GetMembersRequest> for PdServer {
    type Response = GetMembersResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, _: tonic::Request<GetMembersRequest>) -> Self::Future {
        self.member_requests.fetch_add(1, Ordering::SeqCst);
        if self.stall_members.load(Ordering::SeqCst) {
            return Box::pin(std::future::pending());
        }
        let mut remaining = self.member_failures.load(Ordering::SeqCst);
        while remaining > 0 {
            match self.member_failures.compare_exchange(
                remaining,
                remaining - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Box::pin(async { Err(tonic::Status::unavailable("PD is starting")) })
                }
                Err(actual) => remaining = actual,
            }
        }
        let member = Member {
            member_id: 1,
            client_urls: vec![self.endpoint.clone()],
            ..Default::default()
        };
        let leader = Member {
            client_urls: self.leader_urls.read().unwrap().clone(),
            ..member.clone()
        };
        let members = self
            .region_members
            .read()
            .unwrap()
            .clone()
            .unwrap_or_else(|| vec![member.clone()]);
        Box::pin(async move {
            Ok(tonic::Response::new(GetMembersResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                members,
                leader: Some(leader),
                ..Default::default()
            }))
        })
    }
}

impl tonic::server::UnaryService<pdpb::GetRegionRequest> for PdServer {
    type Response = pdpb::GetRegionResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<pdpb::GetRegionRequest>) -> Self::Future {
        self.region_metadata
            .lock()
            .unwrap()
            .push(request.metadata().contains_key("pd-allow-follower-handle"));
        assert!(request.metadata().contains_key("grpc-timeout"));
        let forwarded = request.metadata().contains_key("pd-forwarded-host");
        let request = request.into_inner();
        assert_eq!(request.header.unwrap().cluster_id, 42);
        let service = self.clone();
        Box::pin(async move {
            if request.region_key == b"blocked" {
                service.region_entered.add_permits(1);
                service.region_release.acquire().await.unwrap().forget();
            }
            if !forwarded && service.region_failure.load(Ordering::SeqCst) == 1 {
                return Err(tonic::Status::unknown("follower transport failure"));
            }
            Ok(tonic::Response::new(pdpb::GetRegionResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    error: (!forwarded && service.region_failure.load(Ordering::SeqCst) == 2)
                        .then_some(pdpb::Error {
                            r#type: pdpb::ErrorType::RegionNotFound as i32,
                            message: "missing follower region".to_owned(),
                        }),
                }),
                region: Some(metapb::Region {
                    id: service.region_id.load(Ordering::SeqCst) as u64,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        })
    }
}

impl tonic::server::UnaryService<pdpb::GetAllStoresRequest> for PdServer {
    type Response = pdpb::GetAllStoresResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<pdpb::GetAllStoresRequest>) -> Self::Future {
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        let omit = self.omit_metadata_header.load(Ordering::SeqCst);
        Box::pin(async move {
            Ok(tonic::Response::new(pdpb::GetAllStoresResponse {
                header: (!omit).then_some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                stores: vec![metapb::Store {
                    id: 9,
                    ..Default::default()
                }],
            }))
        })
    }
}

impl tonic::server::UnaryService<pdpb::ScanRegionsRequest> for PdServer {
    type Response = pdpb::ScanRegionsResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<pdpb::ScanRegionsRequest>) -> Self::Future {
        self.region_metadata
            .lock()
            .unwrap()
            .push(request.metadata().contains_key("pd-allow-follower-handle"));
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        let request = request.into_inner();
        let service = self.clone();
        Box::pin(async move {
            if service.stall_scans.load(Ordering::SeqCst) {
                service.region_entered.add_permits(1);
                service.region_release.acquire().await.unwrap().forget();
            }
            Ok(tonic::Response::new(pdpb::ScanRegionsResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                region_metas: vec![metapb::Region {
                    id: 7,
                    start_key: request.start_key,
                    end_key: request.end_key,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                }],
                ..Default::default()
            }))
        })
    }
}

impl tonic::server::StreamingService<TsoRequest> for PdServer {
    type Response = TsoResponse;
    type ResponseStream =
        Pin<Box<dyn Stream<Item = std::result::Result<TsoResponse, tonic::Status>> + Send>>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<tonic::Streaming<TsoRequest>>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            let active = ActiveStream(service.dropped.clone());
            if let Some(code) = *service.tso_failure.lock().unwrap() {
                return Err(tonic::Status::new(code, "injected TSO connection failure"));
            }
            let mut requests = request.into_inner();
            // A server is allowed to wait for the first request before returning headers.
            let first = requests.message().await?;
            if first.is_some() {
                service.received.fetch_add(1, Ordering::SeqCst);
            }
            if matches!(service.reply, Reply::StallHeaders) {
                futures::future::pending::<()>().await;
            }
            let responses = futures::stream::unfold(
                (requests, first, active, 0_i64, service),
                |(mut requests, first, active, mut logical, service)| async move {
                    if matches!(service.reply, Reply::End) {
                        return None;
                    }
                    if matches!(service.reply, Reply::StallBody) {
                        futures::future::pending::<()>().await;
                    }
                    let request = match first {
                        Some(first) => first,
                        None => match requests.message().await {
                            Ok(Some(request)) => {
                                service.received.fetch_add(1, Ordering::SeqCst);
                                request
                            }
                            Ok(None) => return None,
                            Err(error) => {
                                return Some((
                                    Err(error),
                                    (requests, None, active, logical, service),
                                ))
                            }
                        },
                    };
                    logical += i64::from(request.count);
                    let response = TsoResponse {
                        header: Some(ResponseHeader {
                            cluster_id: 42,
                            ..Default::default()
                        }),
                        count: request.count,
                        timestamp: Some(Timestamp {
                            physical: 100,
                            logical,
                            suffix_bits: 0,
                        }),
                    };
                    Some((Ok(response), (requests, None, active, logical, service)))
                },
            );
            Ok(tonic::Response::new(
                Box::pin(responses) as Self::ResponseStream
            ))
        })
    }
}

// The pinned gRPC health protocol has two scalar fields; exercise the actual
// endpoint, including cancellation of a server that never replies.
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
struct HealthServer(PdServer);
impl tonic::server::NamedService for HealthServer {
    const NAME: &'static str = "grpc.health.v1.Health";
}
impl<B> Service<http::Request<B>> for HealthServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            Ok(
                tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                    .unary(service, request)
                    .await,
            )
        })
    }
}
impl tonic::server::UnaryService<HealthRequest> for HealthServer {
    type Response = HealthResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<HealthRequest>) -> Self::Future {
        assert!(request.get_ref().service.is_empty());
        let service = self.0.clone();
        Box::pin(async move {
            service.health_requests.fetch_add(1, Ordering::SeqCst);
            if service.health_stall.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Ok(tonic::Response::new(HealthResponse {
                status: service.health_status.load(Ordering::SeqCst) as i32,
            }))
        })
    }
}

struct Server {
    service: PdServer,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = PdServer {
            endpoint: endpoint.clone(),
            user_agents: Arc::default(),
            leader_urls: Arc::new(std::sync::RwLock::new(vec![endpoint.clone()])),
            region_members: Arc::default(),
            region_metadata: Arc::default(),
            region_failure: Arc::default(),
            health_status: Arc::new(AtomicUsize::new(1)),
            wire_routes: Arc::default(),
            health_requests: Arc::default(),
            health_stall: Arc::default(),
            member_failures: Arc::new(AtomicUsize::new(0)),
            member_requests: Arc::new(AtomicUsize::new(0)),
            connections: Arc::new(AtomicUsize::new(0)),
            stall_members: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stall_scans: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            region_entered: Arc::new(tokio::sync::Semaphore::new(0)),
            region_release: Arc::new(tokio::sync::Semaphore::new(0)),
            region_id: Arc::new(AtomicUsize::new(1)),
            group: Arc::new(std::sync::RwLock::new(tsopb::KeyspaceGroup {
                members: vec![tsopb::KeyspaceGroupMember {
                    address: endpoint.clone(),
                    is_primary: true,
                }],
                ..Default::default()
            })),
            revision: Arc::new(AtomicUsize::new(1)),
            discovery_requests: Arc::new(AtomicUsize::new(0)),
            stall_discovery: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tso_headers: Arc::new(std::sync::Mutex::new(Vec::new())),
            cluster_info: Arc::new(std::sync::RwLock::new(None)),
            min_response: Arc::new(std::sync::RwLock::new(Err(tonic::Status::unimplemented(
                "old API",
            )))),
            min_requests: Arc::new(AtomicUsize::new(0)),
            omit_metadata_header: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            required_keyspace: Arc::new(AtomicUsize::new(0)),
            keyspace_loads: Arc::new(AtomicUsize::new(0)),
            reply,
            tso_failure: Arc::default(),
            cluster_info_failure: Arc::default(),
            received: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
        };
        let task_service = service.clone();
        let connections = service.connections.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HealthServer(task_service.clone()))
                .add_service(KeyspaceServer(task_service.clone()))
                .add_service(TsoServer(task_service.clone()))
                .add_service(task_service)
                .serve_with_incoming(TcpListenerStream::new(listener).map(move |socket| {
                    if socket.is_ok() {
                        connections.fetch_add(1, Ordering::SeqCst);
                    }
                    socket
                }))
                .await
                .unwrap();
        });
        Self { service, task }
    }

    async fn cluster(&self, timeout: Duration) -> crate::pd::Cluster {
        Connection::new(Arc::new(SecurityManager::default()))
            .connect_cluster(&[self.service.endpoint.clone()], timeout)
            .await
            .unwrap()
    }
}

async fn metadata_client(server: &Server) -> Arc<RetryClient> {
    let timeout = Duration::from_secs(10);
    Arc::new(RetryClient::new_with_cluster(
        Arc::new(SecurityManager::default()),
        timeout,
        server.cluster(timeout).await,
    ))
}

async fn wait_region_entered(server: &Server) {
    tokio::time::timeout(
        Duration::from_secs(1),
        server.service.region_entered.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
}

#[tokio::test]
async fn source_pd_concurrency_metadata_requests_overlap() {
    let server = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&server).await;
    let blocked = tokio::spawn(client.clone().get_region(b"blocked".to_vec()));
    wait_region_entered(&server).await;
    let others = tokio::time::timeout(Duration::from_millis(500), async {
        tokio::try_join!(
            client.clone().get_region(b"free".to_vec()),
            client.clone().get_all_stores(),
            client.clone().scan_regions(b"a".to_vec(), b"z".to_vec(), 1),
            client.clone().get_timestamp(),
        )
    })
    .await;
    server.service.region_release.add_permits(1);
    blocked.await.unwrap().unwrap();
    let (region, stores, scan, ts) = others
        .expect("Go does not serialize metadata RPCs")
        .unwrap();
    assert_eq!(region.region.id, 1);
    assert_eq!(stores[0].id, 9);
    assert_eq!(scan[0].region.start_key, b"a");
    assert_eq!(scan[0].region.end_key, b"z");
    assert_eq!(ts.physical, 100);
}

#[tokio::test]
async fn source_pd_concurrency_timestamp_wait_releases_cluster() {
    let server = Server::start(Reply::StallBody).await;
    let client = metadata_client(&server).await;
    let timestamp = tokio::spawn(client.clone().get_timestamp());
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let metadata =
        tokio::time::timeout(Duration::from_millis(500), client.clone().get_all_stores()).await;
    timestamp.abort();
    assert!(timestamp.await.unwrap_err().is_cancelled());
    assert_eq!(
        metadata
            .expect("a pending TSO must not block metadata")
            .unwrap()[0]
            .id,
        9
    );
}

#[tokio::test]
async fn source_pd_concurrency_replacement_during_metadata_request() {
    let first = Server::start(Reply::Timestamp).await;
    let second = Server::start(Reply::Timestamp).await;
    second.service.region_id.store(2, Ordering::SeqCst);
    let client = metadata_client(&first).await;
    client.clone().get_timestamp().await.unwrap();
    let blocked = tokio::spawn(client.clone().get_region(b"blocked".to_vec()));
    wait_region_entered(&first).await;
    *first.service.leader_urls.write().unwrap() = vec![second.service.endpoint.clone()];
    let refresh = tokio::time::timeout(Duration::from_secs(1), client.reconnect_for_test()).await;
    first.service.region_release.add_permits(1);
    let old = blocked.await.unwrap().unwrap();
    refresh
        .expect("a retained request must not prevent leader replacement")
        .unwrap();
    assert_eq!(old.region.id, 1);
    assert_eq!(
        client
            .clone()
            .get_region(b"new".to_vec())
            .await
            .unwrap()
            .region
            .id,
        2
    );
    client.clone().get_timestamp().await.unwrap();
    assert_eq!(second.service.received.load(Ordering::SeqCst), 1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while first.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replacement must still retire the previous TSO stream");
}

#[tokio::test]
async fn source_pd_concurrency_discovery_does_not_block_requests() {
    let server = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&server).await;
    let first = client.clone().get_timestamp().await.unwrap();
    let members_before = server.service.member_requests.load(Ordering::SeqCst);
    server.service.stall_members.store(true, Ordering::SeqCst);
    let refreshing = client.clone();
    let refresh = tokio::spawn(async move { refreshing.reconnect_for_test().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.member_requests.load(Ordering::SeqCst) == members_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let metadata =
        tokio::time::timeout(Duration::from_millis(500), client.clone().get_all_stores()).await;
    refresh.abort();
    assert!(refresh.await.unwrap_err().is_cancelled());
    server.service.stall_members.store(false, Ordering::SeqCst);
    assert_eq!(
        metadata
            .expect("discovery must not hold the request publication lock")
            .unwrap()[0]
            .id,
        9
    );
    assert_eq!(
        client.clone().get_timestamp().await.unwrap().logical,
        first.logical + 1
    );
}

#[tokio::test]
async fn source_tso_batch_deadline_cancels_stalled_response() {
    let server = Server::start(Reply::StallBody).await;
    let cluster = server.cluster(Duration::from_millis(20)).await;
    let result = tokio::time::timeout(Duration::from_millis(500), cluster.get_timestamp()).await;
    assert_eq!(server.service.received.load(Ordering::SeqCst), 1);
    assert!(
        result.is_ok(),
        "a sent TSO batch must be bounded by the configured PD timeout"
    );
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn source_tso_batch_deadline_includes_response_headers() {
    let server = Server::start(Reply::StallHeaders).await;
    let cluster = server.cluster(Duration::from_millis(20)).await;
    let result = tokio::time::timeout(Duration::from_millis(500), cluster.get_timestamp()).await;
    assert_eq!(server.service.received.load(Ordering::SeqCst), 1);
    assert!(
        result.is_ok(),
        "a sent TSO batch must time out even before response headers arrive"
    );
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn source_tso_completed_batch_disarms_deadline_on_live_stream() {
    let server = Server::start(Reply::Timestamp).await;
    let cluster = server.cluster(Duration::from_millis(50)).await;
    let first = tokio::time::timeout(Duration::from_secs(1), cluster.get_timestamp())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = tokio::time::timeout(Duration::from_secs(1), cluster.get_timestamp())
        .await
        .unwrap()
        .unwrap();
    assert!(second.logical > first.logical);
    assert_eq!(
        server.service.dropped.load(Ordering::SeqCst),
        0,
        "batch completion must not close the stream"
    );
}

#[tokio::test]
async fn source_tso_drop_retires_stalled_stream_before_its_deadline() {
    let server = Server::start(Reply::StallBody).await;
    let cluster = server.cluster(Duration::from_secs(100)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), cluster.get_timestamp())
            .await
            .is_err()
    );
    assert_eq!(server.service.received.load(Ordering::SeqCst), 1);
    drop(cluster);
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the last oracle owner must retire its stalled stream");
}

#[tokio::test]
async fn source_tso_shared_stream_completes_concurrent_batches() {
    let server = Server::start(Reply::Timestamp).await;
    let cluster = server.cluster(Duration::from_secs(1)).await;
    let timestamps = tokio::time::timeout(
        Duration::from_secs(2),
        futures::future::try_join_all((0..256).map(|_| cluster.get_timestamp())),
    )
    .await
    .unwrap()
    .unwrap();
    let mut logical = timestamps.iter().map(|ts| ts.logical).collect::<Vec<_>>();
    logical.sort_unstable();
    assert_eq!(logical, (1..=256).collect::<Vec<_>>());
    assert!(timestamps.iter().all(|ts| ts.physical == 100));
    // The source-sized collector can complete this workload in one batch.
    assert!(server.service.received.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn source_tso_close_joins_worker_and_deadline_watcher() {
    let server = Server::start(Reply::StallBody).await;
    let channel = tonic::transport::Channel::from_shared(server.service.endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = PdClient::new(channel);
    let oracle = TimestampOracle::new(42, &client, Duration::from_secs(100)).unwrap();
    let pending = tokio::spawn(oracle.clone().get_timestamp());
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(oracle.close(), oracle.close());
    })
    .await
    .unwrap();
    assert!(pending.await.unwrap().is_err());
    assert!(oracle.inner.worker.lock().await.is_none());
    assert!(oracle.get_timestamp().await.is_err());
}

#[tokio::test]
async fn source_tso_completed_result_survives_stream_retirement() {
    // Go Request waits for request/client context, not a retired stream's
    // context. An already completed batch must retain its timestamp.
    for _ in 0..64 {
        let (request_tx, mut request_rx) = mpsc::channel(1);
        let cancellation = Cancellation::default();
        let oracle = TimestampOracle {
            inner: Arc::new(OracleInner {
                request_tx,
                cancellation: cancellation.clone(),
                worker: Mutex::new(None),
                routes: None,
            }),
        };
        let request = oracle.get_timestamp();
        tokio::pin!(request);
        assert!(futures::poll!(request.as_mut()).is_pending());
        let timestamp = Timestamp {
            physical: 123,
            logical: 456,
            suffix_bits: 0,
        };
        request_rx
            .try_recv()
            .unwrap()
            .send(timestamp.clone())
            .unwrap();
        cancellation.cancel();
        assert_eq!(request.await.unwrap(), timestamp);
    }
}

#[tokio::test]
async fn source_connectionctx_metadata_refresh_reuses_healthy_tso() {
    let server = Server::start(Reply::Timestamp).await;
    let mut cluster = server.cluster(Duration::from_secs(1)).await;
    let first = cluster.get_timestamp().await.unwrap();
    Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    let second = cluster.get_timestamp().await.unwrap();
    assert_eq!(server.service.received.load(Ordering::SeqCst), 2);
    assert_eq!(
        second.logical,
        first.logical + 1,
        "metadata refresh must keep the same healthy leader stream"
    );
    assert_eq!(server.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_changed_leader_releases_old_stream() {
    let first = Server::start(Reply::Timestamp).await;
    let second = Server::start(Reply::Timestamp).await;
    let mut cluster = first.cluster(Duration::from_secs(1)).await;
    cluster.get_timestamp().await.unwrap();
    *first.service.leader_urls.write().unwrap() = vec![second.service.endpoint.clone()];
    Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while first.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("changing leaders must retire the previous stream");
    assert_eq!(first.service.received.load(Ordering::SeqCst), 1);
    assert_eq!(second.service.received.load(Ordering::SeqCst), 1);
    assert_eq!(second.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_cancelled_same_url_gets_a_new_stream() {
    let server = Server::start(Reply::StallBody).await;
    let timeout = Duration::from_millis(20);
    let mut cluster = server.cluster(timeout).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), cluster.get_timestamp())
            .await
            .unwrap()
            .is_err()
    );
    Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, timeout)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), cluster.get_timestamp())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(
        server.service.received.load(Ordering::SeqCst),
        2,
        "a canceled same-URL context must not prevent stream replacement"
    );
}

#[tokio::test]
async fn source_connectionctx_failed_metadata_refresh_preserves_live_stream() {
    let server = Server::start(Reply::Timestamp).await;
    let mut cluster = server.cluster(Duration::from_secs(1)).await;
    let first = cluster.get_timestamp().await.unwrap();
    // Fail the membership observation itself. An accepted new leader is not a
    // failed observation merely because its timestamp connection is unavailable.
    server.service.member_failures.store(100, Ordering::SeqCst);
    assert!(Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .is_err());
    let second = cluster.get_timestamp().await.unwrap();
    assert_eq!(second.logical, first.logical + 1);
    assert_eq!(server.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_retains_selected_url_without_alias_failover() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.leader_urls.write().unwrap() = vec![
        "http://127.0.0.1:0".to_owned(),
        server.service.endpoint.clone(),
    ];
    // Go updateServiceClient/PickMatchedURL chooses the first matching scheme,
    // without probing aliases for reachability. A later working URL must not
    // silently replace that logical leader identity.
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    assert!(connection
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(1))
        .await
        .is_err());
    *server.service.leader_urls.write().unwrap() = vec![
        server.service.endpoint.clone(),
        "http://127.0.0.1:0".to_owned(),
    ];
    let mut cluster = server.cluster(Duration::from_secs(1)).await;
    let first = cluster.get_timestamp().await.unwrap();
    *server.service.leader_urls.write().unwrap() = vec![server.service.endpoint.clone()];
    Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    let second = cluster.get_timestamp().await.unwrap();
    assert_eq!(second.logical, first.logical + 1);
    assert_eq!(server.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_release_cancels_retained_pending_stream_and_joins() {
    use crate::pd::connectionctx::{ConnectionCtx, Manager};

    let server = Server::start(Reply::StallBody).await;
    let channel = tonic::transport::Channel::from_shared(server.service.endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let oracle =
        TimestampOracle::new(42, &PdClient::new(channel), Duration::from_secs(100)).unwrap();
    let ctx = oracle.cancellation();
    let cancel = ctx.clone();
    let entry = Arc::new(ConnectionCtx::new(
        ctx,
        move || cancel.cancel(),
        server.service.endpoint.clone(),
        oracle,
    ));
    let manager = Manager::new();
    assert!(manager.store(&entry, false));
    drop(entry);
    let retained = manager.randomly_pick().unwrap();
    let pending = tokio::spawn(retained.stream.clone().get_timestamp());
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager.release_all();
    assert!(retained.ctx.is_cancelled());
    assert!(tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    tokio::time::timeout(Duration::from_secs(1), retained.stream.close())
        .await
        .unwrap();
    assert!(retained.stream.inner.worker.lock().await.is_none());
    assert!(manager.is_empty());
}

#[tokio::test]
async fn source_batch_default_tso_collects_twenty_thousand_prequeued_requests() {
    let (tx, rx) = mpsc::channel(20_001);
    let mut responses = Vec::new();
    for _ in 0..20_001 {
        let (request, response) = oneshot::channel();
        tx.send(request).await.unwrap();
        responses.push(response);
    }
    let cancellation = Cancellation::default();
    let watcher = Watcher::new(&cancellation, 64, "source-batch-test");
    let pending = Arc::new(Mutex::new(VecDeque::new()));
    let stream = request_stream(
        42,
        rx,
        pending.clone(),
        watcher.clone(),
        Duration::from_secs(10),
        cancellation,
    );
    tokio::pin!(stream);
    let first = stream.next().await.unwrap();
    assert_eq!(
        first.count, 20_000,
        "Go's default TSO controller takes the full prequeued source-sized batch"
    );
    allocate_timestamps(
        &TsoResponse {
            count: first.count,
            timestamp: Some(Timestamp {
                physical: 1,
                logical: 20_000,
                suffix_bits: 0,
            }),
            ..Default::default()
        },
        &mut *pending.lock().await,
    )
    .unwrap();
    let second = stream.next().await.unwrap();
    assert_eq!(second.count, 1);
    allocate_timestamps(
        &TsoResponse {
            count: second.count,
            timestamp: Some(Timestamp {
                physical: 1,
                logical: 20_001,
                suffix_bits: 0,
            }),
            ..Default::default()
        },
        &mut *pending.lock().await,
    )
    .unwrap();
    for (index, response) in responses.into_iter().enumerate() {
        assert_eq!(response.await.unwrap().logical, index as i64 + 1);
    }
    watcher.close().await;
}

#[tokio::test(start_paused = true)]
async fn source_batch_default_tso_holds_one_rpc_token_until_completion() {
    let (tx, rx) = mpsc::channel(2);
    let (first, _first_response) = oneshot::channel();
    tx.send(first).await.unwrap();
    let cancellation = Cancellation::default();
    let watcher = Watcher::new(&cancellation, 64, "source-token-test");
    let pending = Arc::new(Mutex::new(VecDeque::new()));
    let stream = request_stream(
        42,
        rx,
        pending.clone(),
        watcher.clone(),
        Duration::from_secs(10),
        cancellation,
    );
    tokio::pin!(stream);
    assert_eq!(stream.next().await.unwrap().count, 1);
    let (second, _second_response) = oneshot::channel();
    tx.send(second).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.next())
            .await
            .is_err(),
        "default Go TSO mode must not send a second RPC before completing the first"
    );
    allocate_timestamps(
        &TsoResponse {
            count: 1,
            timestamp: Some(Timestamp {
                physical: 1,
                logical: 1,
                suffix_bits: 0,
            }),
            ..Default::default()
        },
        &mut *pending.lock().await,
    )
    .unwrap();
    assert_eq!(stream.next().await.unwrap().count, 1);
    allocate_timestamps(
        &TsoResponse {
            count: 1,
            timestamp: Some(Timestamp {
                physical: 1,
                logical: 2,
                suffix_bits: 0,
            }),
            ..Default::default()
        },
        &mut *pending.lock().await,
    )
    .unwrap();
    watcher.close().await;
}

#[tokio::test]
async fn source_batch_discard_returns_rpc_token_before_request_completion() {
    let cancellation = Cancellation::default();
    let watcher = Watcher::new(&cancellation, 64, "source-discard-test");
    let tokens = Arc::new(Semaphore::new(1));
    let callback_tokens = tokens.clone();
    let controller = Controller::new(
        20,
        Some(Box::new(move |_, request, error| {
            assert!(matches!(error, Some(Error::ContextCanceled)));
            assert_eq!(
                callback_tokens.available_permits(),
                1,
                "Go returns the RPC token before finishing discarded requests"
            );
            drop(request);
        })),
        None,
    );
    let pool = Arc::new(StdMutex::new(Vec::new()));
    let mut requests = RequestBatch {
        controller: Some(controller),
        pool: pool.clone(),
    };
    let (tx, mut rx) = mpsc::channel(1);
    let (request, response) = oneshot::channel();
    tx.send(request).await.unwrap();
    let permit = requests
        .fetch_pending_requests(&cancellation, &mut rx, Some(&tokens), Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    let done = watcher
        .start(&cancellation, Duration::from_secs(1), || {})
        .await
        .unwrap();
    drop(RequestGroup {
        count: 1,
        requests,
        done,
        _permit: permit,
    });
    assert!(response.await.is_err());
    assert_eq!(tokens.available_permits(), 1);
    assert_eq!(pool.lock().unwrap().len(), 1);
    assert_eq!(pool.lock().unwrap()[0].get_collected_request_count(), 0);
    watcher.close().await;
}

#[tokio::test]
async fn source_batch_completed_controller_reuses_its_buffer_without_old_senders() {
    let cancellation = Cancellation::default();
    let pool = Arc::new(StdMutex::new(Vec::new()));
    let mut batch = RequestBatch::new(pool.clone());
    let allocation = batch.get_collected_requests().as_ptr();
    let (tx, mut rx) = mpsc::channel(1);
    let (request, response) = oneshot::channel();
    tx.send(request).await.unwrap();
    batch
        .fetch_pending_requests(&cancellation, &mut rx, None, Duration::ZERO)
        .await
        .unwrap();
    batch.finish_collected_requests(None, Some(&Error::ContextCanceled));
    drop(batch);
    assert!(response.await.is_err());
    let batch = RequestBatch::new(pool.clone());
    assert_eq!(batch.get_collected_requests().as_ptr(), allocation);
    assert_eq!(batch.get_collected_request_count(), 0);
    assert!(pool.lock().unwrap().is_empty());
}

#[tokio::test]
async fn source_retry_initialization_recovers_from_a_failed_member_probe() {
    let server = Server::start(Reply::Timestamp).await;
    server.service.member_failures.store(1, Ordering::SeqCst);
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        crate::pd::RetryClient::connect(
            &[server.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            Duration::from_millis(100),
        ),
    )
    .await
    .expect("initialization retries must remain bounded");
    assert!(
        result.is_ok(),
        "Go retries initialization: {:?}",
        result.err()
    );
    assert!(server.service.member_requests.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn source_retry_member_probe_honors_its_timeout() {
    let server = Server::start(Reply::Timestamp).await;
    server.service.stall_members.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_millis(500), async {
        Connection::new(Arc::new(SecurityManager::default()))
            .connect_cluster(
                &[server.service.endpoint.clone()],
                Duration::from_millis(50),
            )
            .await
    })
    .await
    .expect("GetMembers must honor the PD timeout");
    assert!(result.is_err());
}

#[tokio::test]
async fn source_pd_error_owner_reports_stream_eof() {
    let server = Server::start(Reply::End).await;
    let channel = Channel::from_shared(server.service.endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(1);
    let (request, _response) = oneshot::channel();
    tx.send(request).await.unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        run_tso(
            42,
            Transport::Pd(PdClient::new(channel)),
            rx,
            Duration::from_secs(2),
            Cancellation::default(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "[PD:client:ErrClientTSOStreamClosed]encountered TSO stream being closed unexpectedly"
    );
    assert!(
        matches!(error, Error::Pd(ref owner) if owner.definition() == crate::pd::errs::ERR_CLIENT_TSO_STREAM_CLOSED && owner.backtrace().is_some())
    );
    assert!(
        !crate::pd::errs::is_leader_change(Some(&error)),
        "Go's EOF path adds a stack wrapper; the helper compares the direct sentinel"
    );
}

#[tokio::test]
async fn source_pd_error_owner_reports_tso_length() {
    let (tx, rx) = mpsc::channel(1);
    let (request, _response) = oneshot::channel();
    tx.send(request).await.unwrap();
    let cancellation = Cancellation::default();
    let watcher = Watcher::new(&cancellation, 1, "source-errs-test");
    let pending = Arc::new(Mutex::new(VecDeque::new()));
    let stream = request_stream(
        42,
        rx,
        pending.clone(),
        watcher.clone(),
        Duration::from_secs(5),
        cancellation,
    );
    tokio::pin!(stream);
    assert_eq!(stream.next().await.unwrap().count, 1);
    let error = allocate_timestamps(
        &TsoResponse {
            count: 2,
            timestamp: Some(Timestamp {
                physical: 1,
                logical: 2,
                suffix_bits: 0,
            }),
            ..Default::default()
        },
        &mut *pending.lock().await,
    )
    .unwrap_err();
    watcher.close().await;
    assert_eq!(
        error.to_string(),
        "[pd] tso length in rpc response is incorrect"
    );
    assert!(
        matches!(error, Error::Pd(owner) if owner.definition() == crate::pd::errs::ERR_TSO_LENGTH && owner.backtrace().is_some())
    );
}

async fn owning_pd_client(
    server: &Server,
) -> Arc<crate::pd::PdRpcClient<crate::mock::MockKvConnect>> {
    let timeout = Duration::from_secs(100);
    let cluster = server.cluster(timeout).await;
    Arc::new(
        crate::pd::PdRpcClient::new(
            crate::Config::default(),
            |_| crate::mock::MockKvConnect,
            |security| async move { Ok(RetryClient::new_with_cluster(security, timeout, cluster)) },
            false,
        )
        .await
        .unwrap(),
    )
}

#[tokio::test]
async fn source_pd_shutdown_closes_stalled_requests_and_retained_handles() {
    use crate::pd::PdClient as _;
    let server = Server::start(Reply::StallBody).await;
    let client = owning_pd_client(&server).await;
    let mut pending = tokio::spawn(client.clone().get_timestamp());
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.received.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.close().await;
    let result = tokio::time::timeout(Duration::from_millis(500), &mut pending).await;
    if result.is_err() {
        pending.abort();
        let _ = pending.await;
    }
    assert!(
        matches!(result, Ok(Ok(Err(_)))),
        "close must finish pending TSO before its 100-second deadline: {result:?}"
    );
    assert_eq!(client.cluster_id().await, 42);
    assert!(
        client.all_stores().await.is_err(),
        "retained handles must not send metadata after close"
    );
    assert!(client.clone().get_timestamp().await.is_err());
    client.close().await;
}

#[tokio::test]
async fn source_pd_shutdown_concurrent_close_joins_background_tasks() {
    let server = Server::start(Reply::Timestamp).await;
    let client = owning_pd_client(&server).await;
    let stopped = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let task_stopped = stopped.clone();
    let task_release = release.clone();
    assert!(client
        .region_cache()
        .spawn_background_task(move |cancellation| async move {
            cancellation.cancelled().await;
            task_stopped.add_permits(1);
            task_release.acquire().await.unwrap().forget();
        }));
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.close().await });
    tokio::time::timeout(Duration::from_secs(1), stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let second = client.close();
    tokio::pin!(second);
    let early = tokio::time::timeout(Duration::from_millis(50), &mut second)
        .await
        .is_ok();
    release.add_permits(1);
    first.await.unwrap();
    if !early {
        second.await;
    }
    assert!(
        !early,
        "every close caller must wait for the same background completion"
    );
}

#[tokio::test]
async fn source_pd_shutdown_interrupted_close_can_finish_joining() {
    let server = Server::start(Reply::Timestamp).await;
    let client = owning_pd_client(&server).await;
    let stopped = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (task_stopped, task_release, task_completed) =
        (stopped.clone(), release.clone(), completed.clone());
    assert!(client
        .region_cache()
        .spawn_background_task(move |cancellation| async move {
            cancellation.cancelled().await;
            task_stopped.add_permits(1);
            task_release.acquire().await.unwrap().forget();
            task_completed.store(true, Ordering::SeqCst);
        }));
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.close().await });
    tokio::time::timeout(Duration::from_secs(1), stopped.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let resumed = client.close();
    tokio::pin!(resumed);
    let early = tokio::time::timeout(Duration::from_millis(50), &mut resumed)
        .await
        .is_ok();
    release.add_permits(1);
    if !early {
        resumed.await;
    }
    assert!(
        !early,
        "cancelling close must not discard its unfinished joins"
    );
    assert!(completed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn source_pd_shutdown_cancels_metadata_and_discovery_without_reconnecting() {
    let server = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&server).await;
    let retained_oracle = client.tso_for_test().await;
    client.clone().get_timestamp().await.unwrap();
    let request = tokio::spawn(client.clone().get_region(b"blocked".to_vec()));
    wait_region_entered(&server).await;
    let members_before = server.service.member_requests.load(Ordering::SeqCst);
    server.service.stall_members.store(true, Ordering::SeqCst);
    let refresh_client = client.clone();
    let refresh = tokio::spawn(async move { refresh_client.reconnect_for_test().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.member_requests.load(Ordering::SeqCst) == members_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), client.close())
        .await
        .unwrap();
    assert!(matches!(
        request.await.unwrap(),
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        refresh.await.unwrap(),
        Err(Error::ContextCanceled)
    ));
    assert!(retained_oracle.inner.worker.lock().await.is_none());
    let members_after = server.service.member_requests.load(Ordering::SeqCst);
    assert!(matches!(
        client.reconnect_for_test().await,
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        client.clone().get_timestamp().await,
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        client.load_keyspace("DEFAULT").await,
        Err(Error::ContextCanceled)
    ));
    assert!(matches!(
        client.clone().get_all_stores().await,
        Err(Error::ContextCanceled)
    ));
    assert_eq!(
        server.service.member_requests.load(Ordering::SeqCst),
        members_after
    );
    assert_eq!(client.cluster_id().await, 42);
}

#[tokio::test]
async fn source_pd_shutdown_joins_retirement_after_interrupted_reconnect() {
    let first = Server::start(Reply::Timestamp).await;
    let second = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&first).await;
    client.clone().get_timestamp().await.unwrap();
    let old = client.tso_for_test().await;
    // Stop retirement at its join, after replacement has already been published.
    let held_join = old.inner.worker.lock().await;
    *first.service.leader_urls.write().unwrap() = vec![second.service.endpoint.clone()];
    let refreshing = client.clone();
    let refresh = tokio::spawn(async move { refreshing.reconnect_for_test().await });
    tokio::time::timeout(Duration::from_secs(1), old.inner.cancellation.cancelled())
        .await
        .unwrap();
    refresh.abort();
    assert!(refresh.await.unwrap_err().is_cancelled());
    let current = client.tso_for_test().await;
    client.clone().get_timestamp().await.unwrap();
    let close = client.close();
    tokio::pin!(close);
    let early = tokio::time::timeout(Duration::from_millis(50), &mut close)
        .await
        .is_ok();
    drop(held_join);
    if !early {
        tokio::time::timeout(Duration::from_secs(1), close)
            .await
            .unwrap();
    }
    assert!(
        !early,
        "close must retain and join the retired connection too"
    );
    assert!(old.inner.worker.lock().await.is_none());
    assert!(current.inner.worker.lock().await.is_none());
}

#[tokio::test]
async fn source_pd_shutdown_cancels_background_rpc_before_closing_pd() {
    let server = Server::start(Reply::Timestamp).await;
    let client = metadata_client(&server).await;
    let cache = Arc::new(crate::region_cache::RegionCache::new(client.clone()));
    server.service.stall_scans.store(true, Ordering::SeqCst);
    cache.start_background_refresh(Duration::from_millis(5));
    wait_region_entered(&server).await;
    let closed =
        tokio::time::timeout(Duration::from_millis(500), cache.close_background_task()).await;
    // Cleanup the failure path before asserting, without shortening the RPC deadline.
    server.service.stall_scans.store(false, Ordering::SeqCst);
    server.service.region_release.add_permits(1);
    assert!(
        closed.is_ok(),
        "cache close must cancel its RPC before closing the PD owner"
    );
    assert!(!cache.spawn_background_task(|_| async { panic!("closed cache accepted work") }));
    assert_eq!(client.clone().get_all_stores().await.unwrap()[0].id, 9);
    client.close().await;
}

impl tonic::server::UnaryService<pdpb::GetClusterInfoRequest> for PdServer {
    type Response = pdpb::GetClusterInfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<pdpb::GetClusterInfoRequest>) -> Self::Future {
        let response = self.cluster_info.read().unwrap().clone();
        let failure = self.cluster_info_failure.load(Ordering::SeqCst);
        Box::pin(async move {
            if failure {
                return Err(tonic::Status::unavailable(
                    "PD mode observation unavailable",
                ));
            }
            response
                .map(tonic::Response::new)
                .ok_or_else(|| tonic::Status::unimplemented("old PD"))
        })
    }
}

#[tokio::test]
async fn source_service_empty_modes_rejected() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse::default());
    let result = Connection::new(Arc::new(SecurityManager::default()))
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(1))
        .await;
    assert!(
        result.is_err(),
        "empty service modes must not select classic PD Tso"
    );
}

#[tokio::test]
async fn source_service_api_missing_tso_never_uses_pd_leader() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        ..Default::default()
    });
    let result = Connection::new(Arc::new(SecurityManager::default()))
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(1))
        .await;
    assert!(
        result.is_err(),
        "failed TSO discovery must not select classic PD Tso"
    );
    assert_eq!(server.service.received.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct TsoServer(PdServer);
impl tonic::server::NamedService for TsoServer {
    const NAME: &'static str = "tsopb.TSO";
}
impl<B> Service<http::Request<B>> for TsoServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        self.0.wire_routes.lock().unwrap().push((
            request.uri().path().to_owned(),
            request
                .headers()
                .get("pd-forwarded-host")
                .map(|v| v.to_str().unwrap().to_owned()),
            request.headers().contains_key("pd-allow-follower-handle"),
        ));

        let service = self.clone();
        match request.uri().path() {
            "/tsopb.TSO/FindGroupByKeyspaceID" => Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(service, request)
                        .await,
                )
            }),
            "/tsopb.TSO/Tso" => Box::pin(async move {
                Ok(
                    tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .streaming(service, request)
                        .await,
                )
            }),
            _ => unreachable!(),
        }
    }
}
impl tonic::server::UnaryService<tsopb::FindGroupByKeyspaceIdRequest> for TsoServer {
    type Response = tsopb::FindGroupByKeyspaceIdResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(
        &mut self,
        request: tonic::Request<tsopb::FindGroupByKeyspaceIdRequest>,
    ) -> Self::Future {
        let service = self.0.clone();
        Box::pin(async move {
            assert!(request.metadata().contains_key("grpc-timeout"));
            let request = request.into_inner();
            let header = request.header.unwrap();
            let required = service.required_keyspace.load(Ordering::SeqCst);
            if required != 0
                && header.keyspace
                    != Some(tsopb::request_header::Keyspace::KeyspaceId(required as u32))
            {
                return Err(tonic::Status::not_found("default keyspace has no group"));
            }
            assert_eq!(header.cluster_id, 42);
            assert_eq!(
                header.callee_id,
                service.endpoint.strip_prefix("http://").unwrap()
            );
            service.discovery_requests.fetch_add(1, Ordering::SeqCst);
            if service.stall_discovery.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Ok(tonic::Response::new(tsopb::FindGroupByKeyspaceIdResponse {
                header: Some(tsopb::ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                keyspace_group: Some(service.group.read().unwrap().clone()),
                mod_revision: service.revision.load(Ordering::SeqCst) as u64,
            }))
        })
    }
}
impl tonic::server::StreamingService<tsopb::TsoRequest> for TsoServer {
    type Response = tsopb::TsoResponse;
    type ResponseStream =
        Pin<Box<dyn Stream<Item = std::result::Result<Self::Response, tonic::Status>> + Send>>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(
        &mut self,
        request: tonic::Request<tonic::Streaming<tsopb::TsoRequest>>,
    ) -> Self::Future {
        let service = self.0.clone();
        Box::pin(async move {
            let active = ActiveStream(service.dropped.clone());
            let stream = futures::stream::unfold(
                (request.into_inner(), active, service, 0_i64),
                |(mut requests, active, service, mut logical)| async move {
                    let request = requests.message().await.unwrap()?;
                    service.received.fetch_add(1, Ordering::SeqCst);
                    let header = request.header.unwrap();
                    assert_eq!(header.cluster_id, 42);
                    assert_eq!(
                        header.callee_id,
                        service.endpoint.strip_prefix("http://").unwrap()
                    );
                    service.tso_headers.lock().unwrap().push(header.clone());
                    logical += i64::from(request.count);
                    Some((
                        Ok(tsopb::TsoResponse {
                            header: Some(tsopb::ResponseHeader {
                                cluster_id: 42,
                                keyspace_group_id: header.keyspace_group_id,
                                ..Default::default()
                            }),
                            count: request.count,
                            timestamp: Some(pdpb::Timestamp {
                                physical: 200,
                                logical,
                                suffix_bits: 0,
                            }),
                        }),
                        (requests, active, service, logical),
                    ))
                },
            );
            Ok(tonic::Response::new(
                Box::pin(stream) as Self::ResponseStream
            ))
        })
    }
}

fn api_mode(pd: &Server, tso: &Server) {
    *pd.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        tso_urls: vec![tso.service.endpoint.clone()],
        ..Default::default()
    });
}

#[tokio::test]
async fn source_service_routes_keyspace_and_retires_on_group_move() {
    let pd = Server::start(Reply::Timestamp).await;
    let first = Server::start(Reply::Timestamp).await;
    let second = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &first);
    let client = metadata_client(&pd).await;
    let timestamp = client.clone().get_timestamp().await.unwrap();
    assert_eq!(timestamp.physical, 200);
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 0);
    let old = client.tso_for_test().await;
    client.reconnect_for_test().await.unwrap();
    assert!(
        !old.cancellation().is_cancelled(),
        "unchanged route must retain stream"
    );
    let meta = crate::proto::keyspacepb::KeyspaceMeta {
        keyspace: Some(crate::proto::keyspacepb::keyspace_meta::Keyspace::Id(27)),
        config: [("tso_keyspace_group_id".into(), "9".into())].into(),
        ..Default::default()
    };
    client.set_keyspace(&meta).await.unwrap();
    assert!(old.cancellation().is_cancelled());
    client.clone().get_timestamp().await.unwrap();
    let headers = first.service.tso_headers.lock().unwrap().clone();
    assert_eq!(
        headers.last().unwrap().keyspace,
        Some(tsopb::request_header::Keyspace::KeyspaceId(27))
    );
    let old = client.tso_for_test().await;
    *first.service.group.write().unwrap() = tsopb::KeyspaceGroup {
        id: 9,
        members: vec![tsopb::KeyspaceGroupMember {
            address: second.service.endpoint.clone(),
            is_primary: true,
        }],
        ..Default::default()
    };
    first.service.revision.store(2, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    assert!(old.cancellation().is_cancelled());
    client.clone().get_timestamp().await.unwrap();
    assert_eq!(
        second.service.tso_headers.lock().unwrap()[0].keyspace_group_id,
        9
    );
    first.service.revision.store(1, Ordering::SeqCst);
    assert!(client.reconnect_for_test().await.is_err());
    // A stale observation must not replace the accepted route.
    client.clone().get_timestamp().await.unwrap();
    assert_eq!(second.service.received.load(Ordering::SeqCst), 2);
    client.close().await;
}

#[tokio::test]
async fn source_service_background_mode_switch_and_close() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    let client = Arc::new(
        RetryClient::connect(
            &[pd.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            Duration::from_secs(1),
        )
        .await
        .unwrap(),
    );
    let classic = client.clone().get_timestamp().await.unwrap();
    assert_eq!(classic.physical, 100);
    let old = client.tso_for_test().await;
    api_mode(&pd, &tso);
    tokio::time::timeout(Duration::from_secs(6), async {
        while !old.cancellation().is_cancelled() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(client.clone().get_timestamp().await.unwrap().physical, 200);
    let active = client.tso_for_test().await;
    client.close().await;
    assert!(active.cancellation().is_cancelled());
    let probes = tso.service.discovery_requests.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(3100)).await;
    assert_eq!(
        tso.service.discovery_requests.load(Ordering::SeqCst),
        probes
    );
    assert!(client.clone().get_timestamp().await.is_err());
}

#[tokio::test]
async fn source_service_header_errors_do_not_fall_back() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        header: Some(pdpb::ResponseHeader {
            error: Some(pdpb::Error {
                message: "discovery unavailable".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        service_modes: vec![pdpb::ServiceMode::PdSvcMode as i32],
        ..Default::default()
    });
    assert!(Connection::new(Arc::new(SecurityManager::default()))
        .connect_cluster(&[server.service.endpoint.clone()], Duration::from_secs(1))
        .await
        .is_err());
}

#[tokio::test]
async fn source_service_failed_initial_probe_rotates_to_healthy_endpoint() {
    let pd = Server::start(Reply::Timestamp).await;
    let a = Server::start(Reply::Timestamp).await;
    let b = Server::start(Reply::Timestamp).await;
    let (stalled, healthy) = if a.service.endpoint < b.service.endpoint {
        (&a, &b)
    } else {
        (&b, &a)
    };
    stalled
        .service
        .stall_discovery
        .store(true, Ordering::SeqCst);
    *pd.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        tso_urls: vec![
            stalled.service.endpoint.clone(),
            healthy.service.endpoint.clone(),
        ],
        ..Default::default()
    });
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    let endpoints = [pd.service.endpoint.clone()];
    assert!(connection
        .connect_cluster(&endpoints, Duration::from_millis(100))
        .await
        .is_err());
    let cluster = connection
        .connect_cluster(&endpoints, Duration::from_secs(1))
        .await
        .expect("failed probes must advance selection even before initial publication");
    let client = Arc::new(RetryClient::new_with_cluster(
        Arc::new(SecurityManager::default()),
        Duration::from_secs(1),
        cluster,
    ));
    assert_eq!(client.clone().get_timestamp().await.unwrap().physical, 200);
    assert_eq!(stalled.service.discovery_requests.load(Ordering::SeqCst), 1);
    assert_eq!(healthy.service.discovery_requests.load(Ordering::SeqCst), 1);
    client.close().await;
}

impl tonic::server::UnaryService<pdpb::GetMinTsRequest> for PdServer {
    type Response = pdpb::GetMinTsResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<pdpb::GetMinTsRequest>) -> Self::Future {
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        self.min_requests.fetch_add(1, Ordering::SeqCst);
        let response = self.min_response.read().unwrap().clone();
        Box::pin(async move { response.map(tonic::Response::new) })
    }
}

#[derive(Clone)]
struct KeyspaceServer(PdServer);
impl tonic::server::NamedService for KeyspaceServer {
    const NAME: &'static str = "keyspacepb.Keyspace";
}
impl<B> Service<http::Request<B>> for KeyspaceServer
where
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let service = self.clone();
        assert_eq!(request.uri().path(), "/keyspacepb.Keyspace/LoadKeyspace");
        Box::pin(async move {
            Ok(
                tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                    .unary(service, request)
                    .await,
            )
        })
    }
}
impl tonic::server::UnaryService<keyspacepb::LoadKeyspaceRequest> for KeyspaceServer {
    type Response = keyspacepb::LoadKeyspaceResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<keyspacepb::LoadKeyspaceRequest>) -> Self::Future {
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        self.0.keyspace_loads.fetch_add(1, Ordering::SeqCst);
        let name = request.into_inner().name;
        Box::pin(async move {
            Ok(tonic::Response::new(keyspacepb::LoadKeyspaceResponse {
                // Go's generated getters accept a nil error header.
                header: None,
                keyspace: Some(keyspacepb::KeyspaceMeta {
                    keyspace: Some(keyspacepb::keyspace_meta::Keyspace::Id(7)),
                    name,
                    state: keyspacepb::KeyspaceState::Enabled as i32,
                    config: [("tso_keyspace_group_id".into(), "4".into())].into(),
                    ..Default::default()
                }),
            }))
        })
    }
}

#[tokio::test]
async fn source_provider_classic_minimum_uses_tso() {
    let pd = Server::start(Reply::Timestamp).await;
    let mut cluster = pd.cluster(Duration::from_secs(1)).await;
    let result = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    cluster.start_close().await;
    assert!(
        result.is_ok(),
        "classic minimum must allocate ordinary TSO: {result:?}"
    );
    assert_eq!(pd.service.min_requests.load(Ordering::SeqCst), 0);
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn source_provider_api_minimum_compatibility_and_errors() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &tso);
    let mut cluster = pd.cluster(Duration::from_secs(1)).await;
    let compatible = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    *pd.service.min_response.write().unwrap() = Ok(pdpb::GetMinTsResponse {
        timestamp: Some(Timestamp {
            physical: 99,
            logical: 3,
            suffix_bits: 0,
        }),
        ..Default::default()
    });
    let minimum = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    *pd.service.min_response.write().unwrap() = Err(tonic::Status::unavailable("API unavailable"));
    let unavailable = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    *pd.service.min_response.write().unwrap() = Ok(pdpb::GetMinTsResponse {
        header: Some(ResponseHeader {
            error: Some(pdpb::Error {
                message: "not ready".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    });
    let header_error = cluster.get_min_timestamp(Duration::from_secs(1)).await;
    cluster.start_close().await;
    assert_eq!(compatible.unwrap().physical, 200);
    assert_eq!(minimum.unwrap().physical, 99);
    assert!(unavailable.is_err());
    assert!(header_error.is_err());
    assert_eq!(
        tso.service.received.load(Ordering::SeqCst),
        1,
        "only compatibility may allocate TSO"
    );
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_provider_optional_metadata_header_preserves_payload() {
    let pd = Server::start(Reply::Timestamp).await;
    pd.service
        .omit_metadata_header
        .store(true, Ordering::SeqCst);
    let mut cluster = pd.cluster(Duration::from_secs(1)).await;
    let stores = cluster.get_all_stores(Duration::from_secs(1)).await;
    cluster.start_close().await;
    assert_eq!(stores.unwrap().stores[0].id, 9);
}

#[tokio::test]
async fn source_provider_v2_bootstraps_without_default_group() {
    use crate::pd::PdClient as _;
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &tso);
    tso.service.required_keyspace.store(7, Ordering::SeqCst);
    tso.service.group.write().unwrap().id = 4;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        crate::pd::PdRpcClient::connect_with_keyspace(
            &[pd.service.endpoint.clone()],
            crate::Config::default(),
            crate::request::KeyMode::Txn,
            "tenant".into(),
        ),
    )
    .await;
    let client = Arc::new(
        result
            .expect("V2 bootstrap must not wait for the nonexistent default group")
            .unwrap(),
    );
    let ts = client.clone().get_timestamp().await.unwrap();
    client.close().await;
    assert_eq!(ts.physical, 200);
    assert_eq!(pd.service.keyspace_loads.load(Ordering::SeqCst), 1);
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 0);
    let headers = tso.service.tso_headers.lock().unwrap();
    assert!(headers.iter().all(|h| h.keyspace_group_id == 4
        && h.keyspace == Some(tsopb::request_header::Keyspace::KeyspaceId(7))));
}

#[tokio::test]
async fn source_channel_batch_membership_keyspace_and_tso_share_connection() {
    let pd = Server::start(Reply::Timestamp).await;
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    let (mut cluster, _) = connection
        .connect_cluster_for_keyspace(
            &[pd.service.endpoint.clone()],
            Duration::from_secs(1),
            Some("app"),
        )
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    connection
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    cluster.start_close().await;
    assert_eq!(pd.service.keyspace_loads.load(Ordering::SeqCst), 1);
    assert_eq!(
        pd.service.connections.load(Ordering::SeqCst),
        1,
        "membership, keyspace, discovery, TSO and refresh must share a channel"
    );
}

#[tokio::test]
async fn source_channel_batch_periodic_discovery_keeps_bootstrap_connection() {
    let pd = Server::start(Reply::Timestamp).await;
    let client = Arc::new(
        RetryClient::connect(
            &[pd.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            Duration::from_secs(1),
        )
        .await
        .unwrap(),
    );
    client.clone().get_timestamp().await.unwrap();
    let previous = pd.service.member_requests.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), async {
        while pd.service.member_requests.load(Ordering::SeqCst) <= previous {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    client.clone().get_timestamp().await.unwrap();
    client.close().await;
    assert_eq!(
        pd.service.connections.load(Ordering::SeqCst),
        1,
        "the retry owner must retain the bootstrap channel map"
    );
}

#[tokio::test]
async fn source_channel_batch_failed_or_cancelled_dial_does_not_poison_cache() {
    use crate::pd::service_discovery::ChannelCache;
    let cache = ChannelCache::default();
    let pd = Server::start(Reply::Timestamp).await;
    let endpoint = &pd.service.endpoint;
    assert!(cache
        .get_or_connect(endpoint, || async {
            Err(tonic::Status::unavailable("dial failed"))
        })
        .await
        .is_err());
    let result = tokio::time::timeout(
        Duration::from_millis(10),
        cache.get_or_connect(endpoint, || std::future::pending()),
    )
    .await;
    assert!(result.is_err());
    let channel = cache
        .get_or_connect(endpoint, || async {
            SecurityManager::default()
                .connect(endpoint, |channel| channel)
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))
        })
        .await
        .unwrap();
    pdpb::pd_client::PdClient::new(channel)
        .get_members(GetMembersRequest::default())
        .await
        .unwrap();
    cache
        .get_or_insert_with(endpoint, || panic!("successful channel must be retained"))
        .unwrap();
    assert_eq!(pd.service.connections.load(Ordering::SeqCst), 1);
    cache.close();
    cache.close();
    assert_eq!(
        cache
            .get_or_insert_with(endpoint, || panic!("closed cache must not dial"))
            .unwrap_err()
            .code(),
        tonic::Code::Cancelled
    );
}

#[tokio::test]
async fn source_channel_batch_close_rejects_inflight_publication() {
    use crate::pd::service_discovery::ChannelCache;
    let cache = ChannelCache::default();
    let pd = Server::start(Reply::Timestamp).await;
    let result = cache
        .get_or_connect(&pd.service.endpoint, || async {
            cache.close();
            Ok(
                tonic::transport::Channel::from_shared(pd.service.endpoint.clone())
                    .unwrap()
                    .connect_lazy(),
            )
        })
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Cancelled);
    assert!(cache
        .get_or_insert_with(&pd.service.endpoint, || panic!("closed cache"))
        .is_err());
}

#[tokio::test]
async fn source_channel_batch_concurrent_dials_publish_one_winner() {
    use crate::pd::service_discovery::ChannelCache;
    let cache = ChannelCache::default();
    let a = Server::start(Reply::Timestamp).await;
    let b = Server::start(Reply::Timestamp).await;
    // Both calls enter construction before either may publish. The second
    // candidate deliberately addresses another fixture so observed RPC peers
    // prove that both returned handles use the winner, not their own candidate.
    let barrier = tokio::sync::Barrier::new(2);
    let dial = |endpoint: String| {
        let barrier = &barrier;
        async move {
            barrier.wait().await;
            tonic::transport::Channel::from_shared(endpoint)
                .unwrap()
                .connect()
                .await
                .map_err(|e| tonic::Status::unavailable(e.to_string()))
        }
    };
    let (first, second) = tokio::join!(
        cache.get_or_connect("same-discovery-key", || dial(a.service.endpoint.clone())),
        cache.get_or_connect("same-discovery-key", || dial(b.service.endpoint.clone())),
    );
    for channel in [first.unwrap(), second.unwrap()] {
        pdpb::pd_client::PdClient::new(channel)
            .get_members(GetMembersRequest::default())
            .await
            .unwrap();
    }
    let counts = (
        a.service.member_requests.load(Ordering::SeqCst),
        b.service.member_requests.load(Ordering::SeqCst),
    );
    assert!(
        matches!(counts, (2, 0) | (0, 2)),
        "only the published winner serves both RPCs: {counts:?}"
    );
    cache.close();
}

#[tokio::test]
async fn pd_region_batch_native_cache_permission_fallback_and_close() {
    let (leader, follower, client) = availability_pair().await;
    for _ in 0..2 {
        client
            .clone()
            .get_region_for_cache(b"wire".to_vec(), false, false)
            .await
            .unwrap();
    }
    assert_eq!(
        *follower.service.region_metadata.lock().unwrap(),
        vec![true]
    );
    client
        .clone()
        .get_region_for_cache(b"wire".to_vec(), false, true)
        .await
        .unwrap();
    client.set_enable_follower_handle(false);
    client
        .clone()
        .get_region_for_cache(b"wire".to_vec(), false, false)
        .await
        .unwrap();
    assert_eq!(follower.service.region_metadata.lock().unwrap().len(), 1);
    client.set_enable_follower_handle(true);
    for failure in [1, 2] {
        follower
            .service
            .region_failure
            .store(failure, Ordering::SeqCst);
        for _ in 0..2 {
            client
                .clone()
                .get_region_for_cache(b"wire".to_vec(), false, false)
                .await
                .unwrap();
        }
    }
    assert_eq!(follower.service.region_metadata.lock().unwrap().len(), 3);
    follower.service.region_failure.store(0, Ordering::SeqCst);
    for _ in 0..2 {
        client
            .clone()
            .scan_regions(b"a".to_vec(), b"z".to_vec(), 1)
            .await
            .unwrap();
    }
    assert_eq!(
        follower.service.region_metadata.lock().unwrap().len(),
        3,
        "REGION_NOT_FOUND suppresses the follower across region API variants"
    );
    assert!(leader
        .service
        .region_metadata
        .lock()
        .unwrap()
        .iter()
        .all(|allowed| !allowed));
    client.close().await;
    assert!(matches!(
        client
            .clone()
            .get_region_for_cache(b"wire".to_vec(), false, false)
            .await,
        Err(crate::Error::ContextCanceled)
    ));
    drop(leader);
    drop(follower);
}

async fn availability_pair() -> (Server, Server, Arc<RetryClient>) {
    availability_pair_with_forwarding(false).await
}

async fn availability_pair_with_forwarding(enabled: bool) -> (Server, Server, Arc<RetryClient>) {
    availability_pair_with_proxy(enabled, false).await
}

async fn availability_pair_with_proxy(
    enabled: bool,
    proxy: bool,
) -> (Server, Server, Arc<RetryClient>) {
    availability_pair_with_reply(enabled, proxy, Reply::Timestamp).await
}

async fn availability_pair_with_reply(
    enabled: bool,
    proxy: bool,
    reply: Reply,
) -> (Server, Server, Arc<RetryClient>) {
    let leader = Server::start(reply).await;
    let follower = Server::start(Reply::Timestamp).await;
    if proxy {
        leader.service.health_status.store(2, Ordering::SeqCst);
    }
    let members = vec![
        pdpb::Member {
            member_id: 1,
            client_urls: vec![leader.service.endpoint.clone()],
            ..Default::default()
        },
        pdpb::Member {
            member_id: 2,
            client_urls: vec![follower.service.endpoint.clone()],
            ..Default::default()
        },
    ];
    for server in [&leader, &follower] {
        *server.service.region_members.write().unwrap() = Some(members.clone());
        *server.service.leader_urls.write().unwrap() = vec![leader.service.endpoint.clone()];
    }
    let client = Arc::new(
        RetryClient::connect_with_options(
            &[leader.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            {
                let mut options = crate::pd::opt::Options::new();
                options.timeout = Duration::from_secs(1);
                options.enable_forwarding = enabled;
                options.set_enable_tso_follower_proxy(proxy);
                options
            },
            None,
        )
        .await
        .unwrap(),
    );
    client.set_enable_follower_handle(true);
    (leader, follower, client)
}

#[tokio::test]
async fn pd_availability_batch_health_recovers_and_close_joins_stalled_probe() {
    let (leader, follower, client) = availability_pair().await;
    follower.service.health_status.store(2, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while follower.service.health_requests.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Let the health response be consumed before asserting the next route.
        tokio::time::sleep(Duration::from_millis(30)).await;
    })
    .await
    .expect("idle PD client must check health independently of TSO discovery");
    for _ in 0..4 {
        client
            .clone()
            .get_region_for_cache(b"wire".to_vec(), false, false)
            .await
            .unwrap();
    }
    assert!(follower.service.region_metadata.lock().unwrap().is_empty());
    follower.service.health_status.store(1, Ordering::SeqCst);
    let probes = follower.service.health_requests.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while follower.service.health_requests.load(Ordering::SeqCst) == probes {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    })
    .await
    .unwrap();
    for _ in 0..2 {
        client
            .clone()
            .get_region_for_cache(b"wire".to_vec(), false, false)
            .await
            .unwrap();
    }
    assert_eq!(follower.service.region_metadata.lock().unwrap().len(), 1);
    leader.service.health_stall.store(true, Ordering::SeqCst);
    let probes = leader.service.health_requests.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while leader.service.health_requests.load(Ordering::SeqCst) == probes {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_millis(250), client.close())
        .await
        .expect("close must cancel and join a stalled health RPC");
    let probes = leader.service.health_requests.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        leader.service.health_requests.load(Ordering::SeqCst),
        probes
    );
}

async fn unavailable_leader(leader: &Server) {
    let previous = leader.service.health_requests.load(Ordering::SeqCst);
    leader.service.health_status.store(2, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while leader.service.health_requests.load(Ordering::SeqCst) == previous {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pd_forwarding_batch_metadata_policy_and_recovery() {
    for enabled in [false, true] {
        let (leader, follower, client) = availability_pair_with_forwarding(enabled).await;
        unavailable_leader(&leader).await;
        client.clone().get_all_stores().await.unwrap();
        let seen = follower.service.wire_routes.lock().unwrap().clone();
        let forwarded: Vec<_> = seen
            .iter()
            .filter(|(path, _, _)| path.ends_with("/GetAllStores"))
            .collect();
        assert_eq!(
            forwarded.len(),
            usize::from(enabled),
            "startup forwarding must reach metadata routing"
        );
        if enabled {
            assert_eq!(
                forwarded[0].1.as_deref(),
                Some(leader.service.endpoint.as_str())
            );
            assert!(!forwarded[0].2);
        }
        if enabled {
            follower.service.region_failure.store(2, Ordering::SeqCst);
            for _ in 0..2 {
                client
                    .clone()
                    .get_region_for_cache(b"wire".to_vec(), false, false)
                    .await
                    .unwrap();
            }
            let seen = follower.service.wire_routes.lock().unwrap().clone();
            let regions: Vec<_> = seen
                .iter()
                .filter(|(p, _, _)| p.ends_with("/GetRegion"))
                .collect();
            assert_eq!(regions.len(), 4);
            for pair in regions.chunks_exact(2) {
                assert!(
                    pair[0].2 && pair[0].1.is_none(),
                    "first region attempt preserves local permission even on API-ring fallback"
                );
                assert!(
                    !pair[1].2 && pair[1].1.as_deref() == Some(leader.service.endpoint.as_str()),
                    "error retry requires leader forwarding"
                );
            }
        }
        client.load_keyspace("DEFAULT").await.unwrap();
        assert_eq!(
            leader.service.keyspace_loads.load(Ordering::SeqCst),
            1,
            "Go keyspaceClient stays on the serving leader connection"
        );
        assert_eq!(follower.service.keyspace_loads.load(Ordering::SeqCst), 0);
        leader.service.health_status.store(1, Ordering::SeqCst);
        let previous = leader.service.health_requests.load(Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(2), async {
            while leader.service.health_requests.load(Ordering::SeqCst) == previous {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        })
        .await
        .unwrap();
        client.clone().get_all_stores().await.unwrap();
        let count = follower
            .service
            .wire_routes
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _, _)| p.ends_with("/GetAllStores"))
            .count();
        assert_eq!(
            count,
            usize::from(enabled),
            "healthy leader restores direct requests"
        );
        client.close().await;
    }
}

#[tokio::test]
async fn pd_forwarding_batch_leader_required_region_is_forwarded() {
    let (leader, follower, client) = availability_pair_with_forwarding(true).await;
    unavailable_leader(&leader).await;
    client
        .clone()
        .get_region_for_cache(b"wire".to_vec(), false, true)
        .await
        .unwrap();
    let seen = follower.service.wire_routes.lock().unwrap().clone();
    let forwarded = seen
        .iter()
        .find(|(path, _, _)| path.ends_with("/GetRegion"))
        .expect("leader-only region lookup must use forwarding");
    assert_eq!(
        forwarded.1.as_deref(),
        Some(leader.service.endpoint.as_str())
    );
    assert!(
        !forwarded.2,
        "forwarding cannot ask follower to handle locally"
    );
    client.set_enable_follower_handle(false);
    follower.service.stall_scans.store(true, Ordering::SeqCst);
    let request_client = client.clone();
    let request = tokio::spawn(async move {
        request_client
            .scan_regions(b"a".to_vec(), b"z".to_vec(), 1)
            .await
    });
    tokio::time::timeout(
        Duration::from_millis(500),
        follower.service.region_entered.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    tokio::time::timeout(Duration::from_millis(250), client.close())
        .await
        .unwrap();
    assert!(matches!(
        request.await.unwrap(),
        Err(crate::Error::ContextCanceled)
    ));
}

#[tokio::test]
async fn tso_proxy_batch_healthy_follower_receives_logical_leader_metadata() {
    let (leader, follower, client) = availability_pair_with_proxy(false, true).await;
    let stamp = client.clone().get_timestamp().await.unwrap();
    assert!(stamp.physical > 0);
    let routed = follower.service.wire_routes.lock().unwrap().clone();
    assert!(
        routed
            .iter()
            .any(|(path, forward, local)| path == "/pdpb.PD/Tso"
                && forward.as_deref() == Some(leader.service.endpoint.as_str())
                && !local),
        "healthy follower must proxy TSO even with ordinary forwarding disabled: {routed:?}"
    );
    assert_eq!(leader.service.received.load(Ordering::SeqCst), 0);
    let old = client.tso_for_test().await;
    client.reconnect_for_test().await.unwrap();
    let retained = client.tso_for_test().await;
    assert!(
        Arc::ptr_eq(&old.inner, &retained.inner),
        "unchanged healthy proxy is reused"
    );
    client.set_enable_tso_follower_proxy(false);
    client.reconnect_for_test().await.unwrap();
    assert!(
        !old.inner.cancellation.is_cancelled(),
        "proxy changes retain the single dispatcher"
    );
    client.clone().get_timestamp().await.unwrap();
    wait_for_proxy_drop(&follower).await;
    assert_eq!(leader.service.received.load(Ordering::SeqCst), 1);
    assert!(leader
        .service
        .wire_routes
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _, _)| path == "/pdpb.PD/Tso")
        .all(|(_, host, _)| host.is_none()));
    client.set_enable_tso_follower_proxy(true);
    client.reconnect_for_test().await.unwrap();
    client.clone().get_timestamp().await.unwrap();
    let active = client.tso_for_test().await;
    client.close().await;
    assert!(active.inner.cancellation.is_cancelled());
    assert!(active.inner.worker.lock().await.is_none());
    assert!(active.inner.routes.as_ref().unwrap().borrow().is_empty());
}

#[tokio::test]
async fn tso_proxy_batch_microservice_group_and_health_retirement() {
    let pd = Server::start(Reply::Timestamp).await;
    let primary = Server::start(Reply::Timestamp).await;
    let secondary = Server::start(Reply::Timestamp).await;
    let group = tsopb::KeyspaceGroup {
        id: 7,
        members: vec![
            tsopb::KeyspaceGroupMember {
                address: primary.service.endpoint.clone(),
                is_primary: true,
            },
            tsopb::KeyspaceGroupMember {
                address: secondary.service.endpoint.clone(),
                is_primary: false,
            },
        ],
        ..Default::default()
    };
    *primary.service.group.write().unwrap() = group.clone();
    *secondary.service.group.write().unwrap() = group;
    primary.service.health_status.store(2, Ordering::SeqCst);
    *pd.service.cluster_info.write().unwrap() = Some(pdpb::GetClusterInfoResponse {
        service_modes: vec![pdpb::ServiceMode::ApiSvcMode as i32],
        tso_urls: vec![primary.service.endpoint.clone()],
        ..Default::default()
    });
    let options = crate::pd::opt::Options::new();
    options.set_enable_tso_follower_proxy(true);
    let client = Arc::new(
        RetryClient::connect_with_options(
            &[pd.service.endpoint.clone()],
            Arc::new(SecurityManager::default()),
            options,
            None,
        )
        .await
        .unwrap(),
    );
    assert_eq!(client.clone().get_timestamp().await.unwrap().physical, 200);
    assert_eq!(
        secondary.service.tso_headers.lock().unwrap()[0].keyspace_group_id,
        7
    );
    assert!(secondary
        .service
        .wire_routes
        .lock()
        .unwrap()
        .iter()
        .any(|(path, host, local)| path == "/tsopb.TSO/Tso"
            && host.as_ref() == Some(&primary.service.endpoint)
            && !local));
    assert_eq!(pd.service.received.load(Ordering::SeqCst), 0);
    assert_eq!(primary.service.received.load(Ordering::SeqCst), 0);
    let old = client.tso_for_test().await;
    // No stale stream survives a successful topology/health refresh with no healthy endpoints.
    secondary.service.health_status.store(2, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    assert!(
        !old.inner.cancellation.is_cancelled(),
        "proxy changes retain the single dispatcher"
    );
    wait_for_proxy_drop(&secondary).await;
    primary.service.health_status.store(1, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    client.clone().get_timestamp().await.unwrap();
    assert!(primary
        .service
        .wire_routes
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _, _)| path == "/tsopb.TSO/Tso")
        .all(|(_, host, _)| host.is_none()));
    client.close().await;
}

async fn wait_for_proxy_drop(server: &Server) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.service.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bootstrap_policy_batch_offline_primary_keeps_healthy_proxy() {
    let pd = Server::start(Reply::Timestamp).await;
    let secondary = Server::start(Reply::Timestamp).await;
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary = format!("http://{}", unused.local_addr().unwrap());
    drop(unused);
    api_mode(&pd, &secondary);
    *secondary.service.group.write().unwrap() = tsopb::KeyspaceGroup {
        id: 7,
        members: vec![
            tsopb::KeyspaceGroupMember {
                address: primary.clone(),
                is_primary: true,
            },
            tsopb::KeyspaceGroupMember {
                address: secondary.service.endpoint.clone(),
                is_primary: false,
            },
        ],
        ..Default::default()
    };
    let options = crate::pd::opt::Options::new();
    options.set_enable_tso_follower_proxy(true);
    let mut connection = Connection::new(Arc::new(SecurityManager::default()));
    connection.options = Arc::new(options);
    let mut cluster = connection
        .connect_cluster(&[pd.service.endpoint.clone()], Duration::from_millis(500))
        .await
        .expect("TSO primary reachability must not gate group publication");
    assert_eq!(cluster.get_timestamp().await.unwrap().physical, 200);
    assert!(secondary
        .service
        .wire_routes
        .lock()
        .unwrap()
        .iter()
        .any(|(path, host, _)| path == "/tsopb.TSO/Tso" && host.as_ref() == Some(&primary)));
    cluster.start_close().await;
}

#[tokio::test]
async fn bootstrap_policy_batch_membership_does_not_require_leader_probe() {
    let leader = Server::start(Reply::Timestamp).await;
    let follower = Server::start(Reply::Timestamp).await;
    *follower.service.leader_urls.write().unwrap() = vec![leader.service.endpoint.clone()];
    leader.service.member_failures.store(100, Ordering::SeqCst);
    let connection = Connection::new(Arc::new(SecurityManager::default()));
    let mut cluster = connection
        .connect_cluster(
            &[follower.service.endpoint.clone()],
            Duration::from_millis(500),
        )
        .await
        .expect("accepted follower membership must initialize the serving connection");
    cluster.get_timestamp().await.unwrap();
    connection
        .reconnect(&mut cluster, Duration::from_millis(500))
        .await
        .unwrap();
    assert_eq!(leader.service.member_requests.load(Ordering::SeqCst), 0);
    cluster.start_close().await;
}

#[tokio::test]
async fn bootstrap_policy_batch_forced_pd_provider_survives_api_refresh() {
    let pd = Server::start(Reply::Timestamp).await;
    let tso = Server::start(Reply::Timestamp).await;
    api_mode(&pd, &tso);
    let mut options = crate::pd::opt::Options::new();
    options.use_tso_server_proxy = true;
    let mut connection = Connection::new(Arc::new(SecurityManager::default()));
    connection.options = Arc::new(options);
    let mut cluster = connection
        .connect_cluster(&[pd.service.endpoint.clone()], Duration::from_secs(1))
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    connection
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    cluster
        .get_min_timestamp(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        pd.service.received.load(Ordering::SeqCst),
        2,
        "forced provider must serve ordinary and minimum timestamps through PD"
    );
    assert_eq!(pd.service.min_requests.load(Ordering::SeqCst), 0);
    assert_eq!(tso.service.discovery_requests.load(Ordering::SeqCst), 0);
    assert!(tso.service.tso_headers.lock().unwrap().is_empty());
    cluster.start_close().await;
}

#[tokio::test]
async fn bootstrap_policy_batch_dial_options_apply_once_in_order() {
    let pd = Server::start(Reply::Timestamp).await;
    *pd.service.leader_urls.write().unwrap() = vec![
        "https://127.0.0.1:0".to_owned(),
        pd.service.endpoint.clone(),
    ];
    let applied = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut options = crate::pd::opt::Options::new();
    for index in [1, 2] {
        let applied = applied.clone();
        options.grpc_dial_options.push(Arc::new(move |endpoint| {
            applied.lock().unwrap().push(index);
            endpoint.user_agent(format!("policy-{index}")).unwrap()
        }));
    }
    let mut connection = Connection::new(Arc::new(SecurityManager::default()));
    connection.options = Arc::new(options);
    let mut cluster = connection
        .connect_cluster(&[pd.service.endpoint.clone()], Duration::from_secs(1))
        .await
        .unwrap();
    cluster.get_timestamp().await.unwrap();
    connection
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(*applied.lock().unwrap(), vec![1, 2]);
    assert!(pd
        .service
        .user_agents
        .lock()
        .unwrap()
        .iter()
        .all(|agent| agent.starts_with("policy-2")));
    assert_eq!(pd.service.connections.load(Ordering::SeqCst), 1);
    cluster.start_close().await;
}

#[tokio::test]
async fn tso_failure_batch_forwards_after_network_failures_and_recovers() {
    let (leader, follower, client) = availability_pair_with_forwarding(true).await;
    leader.service.health_status.store(2, Ordering::SeqCst);
    *leader.service.tso_failure.lock().unwrap() = Some(tonic::Code::Unavailable);
    for _ in 0..6 {
        assert!(client.tso_for_test().await.get_timestamp().await.is_err());
        client.reconnect_for_test().await.unwrap();
    }
    let oracle = client.tso_for_test().await;
    assert!(
        oracle.clone().get_timestamp().await.is_ok(),
        "six network failures must admit a healthy backup"
    );
    let routes = follower.service.wire_routes.lock().unwrap().clone();
    assert!(routes.iter().any(|(path, host, _)| path == "/pdpb.PD/Tso"
        && host.as_deref() == Some(leader.service.endpoint.as_str())));
    client.reconnect_for_test().await.unwrap();
    assert!(oracle.clone().get_timestamp().await.is_ok());
    assert_eq!(
        follower
            .service
            .wire_routes
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _, _)| path == "/pdpb.PD/Tso")
            .count(),
        1,
        "healthy fallback stream is retained"
    );
    *leader.service.tso_failure.lock().unwrap() = None;
    leader.service.health_status.store(1, Ordering::SeqCst);
    client.reconnect_for_test().await.unwrap();
    assert!(oracle.clone().get_timestamp().await.is_ok());
    wait_for_proxy_drop(&follower).await;
    assert_eq!(leader.service.received.load(Ordering::SeqCst), 1);
    client.close().await;
    assert!(oracle.inner.routes.as_ref().unwrap().borrow().is_empty());
}

#[tokio::test]
async fn tso_failure_batch_error_codes_and_disabled_policy() {
    for (enabled, code, forwards) in [
        (true, tonic::Code::Cancelled, true),
        (true, tonic::Code::DeadlineExceeded, true),
        (true, tonic::Code::PermissionDenied, false),
        (false, tonic::Code::Unavailable, false),
    ] {
        let (leader, follower, client) = availability_pair_with_forwarding(enabled).await;
        leader.service.health_status.store(2, Ordering::SeqCst);
        *leader.service.tso_failure.lock().unwrap() = Some(code);
        for _ in 0..6 {
            assert!(client.tso_for_test().await.get_timestamp().await.is_err());
            client.reconnect_for_test().await.unwrap();
        }
        assert_eq!(
            client.tso_for_test().await.get_timestamp().await.is_ok(),
            forwards,
            "{enabled} {code:?}"
        );
        assert_eq!(
            follower.service.received.load(Ordering::SeqCst) > 0,
            forwards
        );
        client.close().await;
    }
}

#[tokio::test]
async fn tso_failure_batch_retains_provider_when_mode_observation_fails() {
    let (leader, follower, client) = availability_pair_with_forwarding(true).await;
    leader
        .service
        .cluster_info_failure
        .store(true, Ordering::SeqCst);
    leader.service.health_status.store(2, Ordering::SeqCst);
    *leader.service.tso_failure.lock().unwrap() = Some(tonic::Code::Unavailable);
    for _ in 0..6 {
        assert!(client.tso_for_test().await.get_timestamp().await.is_err());
        client
            .reconnect_for_test()
            .await
            .expect("failed mode observation must preserve accepted provider");
    }
    assert!(client.tso_for_test().await.get_timestamp().await.is_ok());
    assert_eq!(follower.service.received.load(Ordering::SeqCst), 1);
    leader.service.health_stall.store(true, Ordering::SeqCst);
    let checks = leader.service.health_requests.load(Ordering::SeqCst);
    let probe = tokio::spawn({
        let client = client.clone();
        async move { client.reconnect_for_test().await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while leader.service.health_requests.load(Ordering::SeqCst) == checks {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), client.close())
        .await
        .unwrap();
    let _ = probe.await;
}

#[tokio::test]
async fn tso_failure_batch_established_eof_does_not_enable_forwarding() {
    let (leader, follower, client) = availability_pair_with_reply(true, false, Reply::End).await;
    leader.service.health_status.store(2, Ordering::SeqCst);
    for _ in 0..8 {
        assert!(client.tso_for_test().await.get_timestamp().await.is_err());
        client.reconnect_for_test().await.unwrap();
    }
    assert_eq!(follower.service.received.load(Ordering::SeqCst), 0);
    client.close().await;
}

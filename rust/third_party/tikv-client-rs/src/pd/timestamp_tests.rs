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
use crate::proto::{metapb, pdpb};
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
    leader_urls: Arc<std::sync::RwLock<Vec<String>>>,
    member_failures: Arc<AtomicUsize>,
    member_requests: Arc<AtomicUsize>,
    stall_members: Arc<std::sync::atomic::AtomicBool>,
    region_entered: Arc<tokio::sync::Semaphore>,
    region_release: Arc<tokio::sync::Semaphore>,
    region_id: Arc<AtomicUsize>,
    reply: Reply,
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
        let service = self.clone();
        match request.uri().path() {
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
        Box::pin(async move {
            Ok(tonic::Response::new(GetMembersResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
                }),
                members: vec![member.clone()],
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
        assert!(request.metadata().contains_key("grpc-timeout"));
        let request = request.into_inner();
        assert_eq!(request.header.unwrap().cluster_id, 42);
        let service = self.clone();
        Box::pin(async move {
            if request.region_key == b"blocked" {
                service.region_entered.add_permits(1);
                service.region_release.acquire().await.unwrap().forget();
            }
            Ok(tonic::Response::new(pdpb::GetRegionResponse {
                header: Some(ResponseHeader {
                    cluster_id: 42,
                    ..Default::default()
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
        Box::pin(async {
            Ok(tonic::Response::new(pdpb::GetAllStoresResponse {
                header: Some(ResponseHeader {
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
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert_eq!(request.get_ref().header.as_ref().unwrap().cluster_id, 42);
        let request = request.into_inner();
        Box::pin(async move {
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
            leader_urls: Arc::new(std::sync::RwLock::new(vec![endpoint])),
            member_failures: Arc::new(AtomicUsize::new(0)),
            member_requests: Arc::new(AtomicUsize::new(0)),
            stall_members: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            region_entered: Arc::new(tokio::sync::Semaphore::new(0)),
            region_release: Arc::new(tokio::sync::Semaphore::new(0)),
            region_id: Arc::new(AtomicUsize::new(1)),
            reply,
            received: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
        };
        let task_service = service.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(task_service)
                .serve_with_incoming(TcpListenerStream::new(listener))
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
    *server.service.leader_urls.write().unwrap() = vec!["http://127.0.0.1:0".to_owned()];
    assert!(Connection::new(Arc::new(SecurityManager::default()))
        .reconnect(&mut cluster, Duration::from_secs(1))
        .await
        .is_err());
    let second = cluster.get_timestamp().await.unwrap();
    assert_eq!(second.logical, first.logical + 1);
    assert_eq!(server.service.dropped.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn source_connectionctx_tracks_the_dialed_url_not_the_first_advertised_url() {
    let server = Server::start(Reply::Timestamp).await;
    *server.service.leader_urls.write().unwrap() = vec![
        "http://127.0.0.1:0".to_owned(),
        server.service.endpoint.clone(),
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
            PdClient::new(channel),
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

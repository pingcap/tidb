// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::Notify;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::IntoRequest;
use tonic::Request as TonicRequest;

use super::batch::{BatchCommandsDispatcher, BatchCommandsWorker};
use super::Request;
use super::{BatchCommandRequest, BatchCommandResponse};
use crate::proto::debugpb::debug_client::DebugClient;
use crate::proto::kvrpcpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::SecurityManager;
use crate::{Error, Result};

const READ_TIMEOUT_SHORT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
#[repr(u8)]
enum GrpcConnectionState {
    Idle,
    Connecting,
    Ready,
    TransientFailure,
}

impl GrpcConnectionState {
    fn name(self) -> &'static str {
        match self {
            Self::Idle => "IDLE",
            Self::Connecting => "CONNECTING",
            Self::Ready => "READY",
            Self::TransientFailure => "TRANSIENT_FAILURE",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ResolveLockCollapseKey {
    region_id: u64,
    start_version: u64,
    is_async: bool,
}

#[derive(Clone)]
enum CollapsedResolveLockError {
    GrpcApi(tonic::Status),
    Connection {
        source: Box<CollapsedResolveLockError>,
        address: String,
        version: u64,
    },
    Message(String),
}

impl CollapsedResolveLockError {
    fn from_error(error: crate::Error) -> Self {
        match error {
            crate::Error::GrpcAPI(status) => Self::GrpcApi(status),
            crate::Error::Connection {
                source,
                address,
                version,
            } => Self::Connection {
                source: Box::new(Self::from_error(*source)),
                address,
                version,
            },
            error => Self::Message(error.to_string()),
        }
    }

    fn to_error(&self) -> crate::Error {
        match self {
            Self::GrpcApi(status) => crate::Error::GrpcAPI(status.clone()),
            Self::Connection {
                source,
                address,
                version,
            } => crate::Error::Connection {
                source: Box::new(source.to_error()),
                address: address.clone(),
                version: *version,
            },
            Self::Message(message) => crate::Error::StringError(message.clone()),
        }
    }
}

type CollapsedResolveLockResult =
    std::result::Result<kvrpcpb::ResolveLockResponse, CollapsedResolveLockError>;

#[derive(Default)]
struct ResolveLockFlight {
    result: Mutex<Option<CollapsedResolveLockResult>>,
    ready: Notify,
}

impl ResolveLockFlight {
    fn complete(&self, result: CollapsedResolveLockResult) {
        *self.result.lock().unwrap() = Some(result);
        self.ready.notify_waiters();
    }

    async fn wait(&self) -> CollapsedResolveLockResult {
        loop {
            let notified = self.ready.notified();
            if let Some(result) = self.result.lock().unwrap().clone() {
                return result;
            }
            notified.await;
        }
    }
}

#[derive(Clone, Default)]
struct ResolveLockCollapser {
    flights: Arc<Mutex<HashMap<ResolveLockCollapseKey, Arc<ResolveLockFlight>>>>,
}

lazy_static::lazy_static! {
    // client-go deliberately owns one process-wide `resolveRegionSf` group,
    // rather than one group per RPC client or address.
    static ref RESOLVE_LOCK_COLLAPSER: ResolveLockCollapser = ResolveLockCollapser::default();
}

/// Client-go's fixed `internal/client.dialTimeout`. This transport-lifecycle
/// deadline is intentionally independent of the caller's RPC timeout.
pub(crate) const TIKV_DIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// A trait for connecting to TiKV stores.
#[async_trait]
pub trait KvConnect: Sized + Send + Sync + 'static {
    type KvClient: KvClient + Clone + Send + Sync + 'static;

    async fn connect(&self, address: &str) -> Result<Self::KvClient>;
}

#[derive(Clone)]
pub struct TikvConnect {
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
    dial_timeout: Duration,
    grpc_max_decoding_message_size: usize,
    send_gzip_requests: bool,
    grpc_keepalive_time: Duration,
    grpc_keepalive_timeout: Duration,
    grpc_initial_stream_window_size: Option<u32>,
    grpc_initial_connection_window_size: Option<u32>,
    grpc_connection_count: usize,
    batch_config: crate::config::TiKvClient,
    open_tracing_enable: bool,
}

impl TikvConnect {
    /// Construct a connector using client-go's default `none` request
    /// compression setting.
    pub fn new(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        grpc_max_decoding_message_size: usize,
    ) -> Self {
        Self::new_with_grpc_compression(
            security_mgr,
            timeout,
            grpc_max_decoding_message_size,
            "none",
            Duration::from_secs(10),
            Duration::from_secs(3),
            Some(1 << 27),
            Some(1 << 27),
            4,
        )
    }

    /// Construct a connector from client-go's validated
    /// `grpc-compression-type` setting (`none` or `gzip`).
    pub fn new_with_grpc_compression(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        grpc_max_decoding_message_size: usize,
        grpc_compression_type: &str,
        grpc_keepalive_time: Duration,
        grpc_keepalive_timeout: Duration,
        grpc_initial_stream_window_size: Option<u32>,
        grpc_initial_connection_window_size: Option<u32>,
        grpc_connection_count: usize,
    ) -> Self {
        Self {
            security_mgr,
            timeout,
            dial_timeout: TIKV_DIAL_TIMEOUT,
            grpc_max_decoding_message_size,
            send_gzip_requests: grpc_compression_type == "gzip",
            grpc_keepalive_time,
            grpc_keepalive_timeout,
            grpc_initial_stream_window_size,
            grpc_initial_connection_window_size,
            grpc_connection_count,
            batch_config: crate::config::TiKvClient::default(),
            open_tracing_enable: false,
        }
    }

    /// Retains the complete TiKV-client subsection for the per-store
    /// BatchCommands worker. The connection settings above are extracted for
    /// Tonic construction; batching must keep the source configuration as a
    /// whole because its collection policy has several coupled fields.
    pub fn with_tikv_client_config(mut self, config: crate::config::TiKvClient) -> Self {
        self.batch_config = config;
        self
    }

    /// Enables source-compatible process-wide gRPC trace-carrier injection.
    pub fn with_open_tracing(mut self, enabled: bool) -> Self {
        self.open_tracing_enable = enabled;
        self
    }
}

#[async_trait]
impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    async fn connect(&self, address: &str) -> Result<KvRpcClient> {
        let mut clients = Vec::with_capacity(self.grpc_connection_count);
        let mut debug_clients = Vec::with_capacity(self.grpc_connection_count);
        for _ in 0..self.grpc_connection_count {
            let (client, debug_client) = self
                .security_mgr
                .connect_with_http2_settings_and_timeout(
                    address,
                    self.grpc_keepalive_time,
                    self.grpc_keepalive_timeout,
                    self.grpc_initial_stream_window_size,
                    self.grpc_initial_connection_window_size,
                    Some(self.dial_timeout),
                    |channel| {
                        let debug_client = DebugClient::new(channel.clone())
                            .max_decoding_message_size(self.grpc_max_decoding_message_size)
                            .accept_compressed(CompressionEncoding::Gzip);
                        let debug_client = if self.send_gzip_requests {
                            debug_client.send_compressed(CompressionEncoding::Gzip)
                        } else {
                            debug_client
                        };
                        let client = TikvClient::new(channel)
                            .max_decoding_message_size(self.grpc_max_decoding_message_size)
                            .accept_compressed(CompressionEncoding::Gzip);
                        let client = if self.send_gzip_requests {
                            client.send_compressed(CompressionEncoding::Gzip)
                        } else {
                            client
                        };
                        (client, debug_client)
                    },
                )
                .await?;
            clients.push(client);
            debug_clients.push(debug_client);
        }
        // The worker owns only batchable physical requests; unsupported
        // requests retain the unary path below.
        Ok(
            KvRpcClient::new_with_debug_clients(clients, debug_clients, self.timeout)
                .with_open_tracing(self.open_tracing_enable)
                .with_batch_worker(&self.batch_config),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::convert::Infallible;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::codegen::{http, Body, BoxFuture, Service, StdError};

    #[derive(Clone)]
    struct LargeDebugServer {
        payload_len: usize,
    }

    impl tonic::server::NamedService for LargeDebugServer {
        const NAME: &'static str = "debugpb.Debug";
    }

    impl<B> Service<http::Request<B>> for LargeDebugServer
    where
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<B>) -> Self::Future {
            if request.uri().path() == "/debugpb.Debug/GetRegionProperties" {
                let service = LargeRegionProperties {
                    payload_len: self.payload_len,
                };
                return Box::pin(async move {
                    Ok(
                        tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                            .unary(service, request)
                            .await,
                    )
                });
            }
            Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(tonic::body::empty_body())
                    .unwrap())
            })
        }
    }

    #[derive(Clone)]
    struct LargeRegionProperties {
        payload_len: usize,
    }

    impl tonic::server::UnaryService<crate::proto::debugpb::GetRegionPropertiesRequest>
        for LargeRegionProperties
    {
        type Response = crate::proto::debugpb::GetRegionPropertiesResponse;
        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;

        fn call(
            &mut self,
            _: tonic::Request<crate::proto::debugpb::GetRegionPropertiesRequest>,
        ) -> Self::Future {
            let payload_len = self.payload_len;
            Box::pin(async move {
                Ok(tonic::Response::new(
                    crate::proto::debugpb::GetRegionPropertiesResponse {
                        props: vec![crate::proto::debugpb::Property {
                            name: "large".to_owned(),
                            value: "x".repeat(payload_len),
                        }],
                    },
                ))
            })
        }
    }

    #[test]
    fn source_grpc_compression_selection_is_preserved() {
        let security = Arc::new(SecurityManager::default());
        let none = TikvConnect::new_with_grpc_compression(
            security.clone(),
            Duration::from_secs(2),
            4 * 1024 * 1024,
            "none",
            Duration::from_secs(10),
            Duration::from_secs(3),
            Some(1 << 27),
            Some(1 << 27),
            4,
        );
        let gzip = TikvConnect::new_with_grpc_compression(
            security,
            Duration::from_secs(2),
            4 * 1024 * 1024,
            "gzip",
            Duration::from_secs(17),
            Duration::from_millis(1250),
            Some(1 << 26),
            Some(1 << 25),
            4,
        );
        assert!(!none.send_gzip_requests);
        assert!(gzip.send_gzip_requests);
        assert_eq!(none.grpc_keepalive_time, Duration::from_secs(10));
        assert_eq!(none.grpc_keepalive_timeout, Duration::from_secs(3));
        assert_eq!(none.dial_timeout, Duration::from_secs(5));
        assert_eq!(gzip.grpc_keepalive_time, Duration::from_secs(17));
        assert_eq!(gzip.grpc_keepalive_timeout, Duration::from_millis(1250));
        assert_eq!(gzip.grpc_initial_stream_window_size, Some(1 << 26));
        assert_eq!(gzip.grpc_initial_connection_window_size, Some(1 << 25));

        let defaults = TikvConnect::new_with_grpc_compression(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(2),
            4 * 1024 * 1024,
            "none",
            Duration::from_secs(10),
            Duration::from_secs(3),
            None,
            None,
            4,
        );
        assert_eq!(defaults.grpc_initial_stream_window_size, None);
        assert_eq!(defaults.grpc_initial_connection_window_size, None);
        assert!(!defaults.open_tracing_enable);
        assert!(defaults.with_open_tracing(true).open_tracing_enable);
    }

    #[tokio::test]
    #[serial]
    async fn source_open_tracing_config_injects_unary_and_stream_grpc_metadata() {
        crate::trace::set_grpc_trace_metadata_injector(Some(Arc::new(|metadata| {
            metadata.insert("x-source-trace", MetadataValue::from_static("active"));
        })));
        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        let address = server.addr().unwrap();
        server.set_metadata_checker(Some(Arc::new(|metadata| {
            (metadata.get("x-source-trace") == Some(&MetadataValue::from_static("active")))
                .then_some(())
                .ok_or_else(|| tonic::Status::permission_denied("missing trace carrier"))
        })));
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = KvRpcClient::new(vec![TikvClient::new(channel)], Duration::from_secs(1))
            .with_open_tracing(true);

        client
            .dispatch(&crate::proto::kvrpcpb::PrewriteRequest::default())
            .await
            .unwrap();
        let stream = client
            .open_batch_commands(
                "",
                futures::stream::pending::<crate::proto::tikvpb::BatchCommandsRequest>(),
            )
            .await
            .unwrap();
        drop(stream);

        client.close();
        server.stop().await.unwrap();
        crate::trace::set_grpc_trace_metadata_injector(None);
    }

    #[tokio::test]
    async fn source_connector_keeps_the_callers_batch_configuration() {
        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        let address = server.addr().unwrap();
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 0;
        let connector = TikvConnect::new(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(1),
            4 * 1024 * 1024,
        )
        .with_tikv_client_config(config);
        let client = connector.connect(&address).await.unwrap();
        assert!(client.batch_worker.is_none());
        drop(client);
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_receive_limit_applies_to_debug_service_responses() {
        const PAYLOAD_LEN: usize = 5 * 1024 * 1024;
        const RECEIVE_LIMIT: usize = 6 * 1024 * 1024;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_requested) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(LargeDebugServer {
                    payload_len: PAYLOAD_LEN,
                })
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_requested.await;
                }),
        );
        let client = TikvConnect::new_with_grpc_compression(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(1),
            RECEIVE_LIMIT,
            "none",
            Duration::from_secs(10),
            Duration::from_secs(3),
            Some(1 << 27),
            Some(1 << 27),
            1,
        )
        .connect(&address.to_string())
        .await
        .unwrap();
        let response = KvClient::dispatch(
            &client,
            &crate::proto::debugpb::GetRegionPropertiesRequest::default(),
        )
        .await
        .unwrap()
        .downcast::<crate::proto::debugpb::GetRegionPropertiesResponse>()
        .unwrap();
        assert_eq!(response.props[0].value.len(), PAYLOAD_LEN);

        client.close();
        let _ = shutdown.send(());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn source_unary_forwarding_metadata_is_applied_only_when_requested() {
        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        let address = server.addr().unwrap();
        let checks = Arc::new(AtomicUsize::new(0));
        let forwarded_checks = checks.clone();
        server.set_metadata_checker(Some(Arc::new(move |metadata| {
            (metadata.get("tikv-forwarded-host") == Some(&"store-2".parse().unwrap()))
                .then_some(())
                .ok_or_else(|| tonic::Status::permission_denied("missing forwarding metadata"))?;
            forwarded_checks.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })));
        let rpc = KvRpcClient::new(
            vec![TikvClient::connect(format!("http://{address}"))
                .await
                .unwrap()],
            Duration::from_secs(1),
        );
        rpc.dispatch_with_forwarded_host(
            &crate::proto::kvrpcpb::PrewriteRequest::default(),
            "store-2",
        )
        .await
        .unwrap();
        assert_eq!(checks.load(Ordering::Relaxed), 1);

        server.set_metadata_checker(Some(Arc::new(|metadata| {
            metadata
                .get("tikv-forwarded-host")
                .is_none()
                .then_some(())
                .ok_or_else(|| tonic::Status::permission_denied("unexpected forwarding metadata"))
        })));
        KvClient::dispatch(&rpc, &crate::proto::kvrpcpb::PrewriteRequest::default())
            .await
            .unwrap();
        drop(rpc);
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_coprocessor_stream_reads_first_response_before_returning() {
        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        let address = server.addr().unwrap();
        server.set_metadata_checker(Some(Arc::new(|metadata| {
            (metadata.get("tikv-forwarded-host") == Some(&"store-2".parse().unwrap()))
                .then_some(())
                .ok_or_else(|| tonic::Status::permission_denied("missing forwarding metadata"))
        })));
        let client = KvClient::with_connection_info(
            KvRpcClient::new(
                vec![TikvClient::connect(format!("http://{address}"))
                    .await
                    .unwrap()],
                Duration::from_secs(1),
            ),
            "store-stream".to_owned(),
            1,
        );
        let source = "internal_cop_stream_transport_unique";
        let request =
            crate::store::CoprocessorStreamRequest::new(crate::proto::coprocessor::Request {
                context: Some(crate::proto::kvrpcpb::Context {
                    peer: Some(crate::proto::metapb::Peer {
                        store_id: 765_432,
                        ..Default::default()
                    }),
                    request_source: source.to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            });
        let metrics_before =
            crate::stats::tikv_store_rpc_samples("CopStream", "765432", "false", "true", source);
        let response = client
            .dispatch_with_forwarded_host(&request, "store-2")
            .await
            .unwrap();
        let mut response = response
            .downcast::<crate::store::CoprocessorStreamResponse>()
            .unwrap();
        assert_eq!(
            response.first.take(),
            Some(crate::proto::coprocessor::Response::default())
        );
        assert_eq!(response.message().await.unwrap(), None);
        response.close();
        let metrics_after =
            crate::stats::tikv_store_rpc_samples("CopStream", "765432", "false", "true", source);
        assert_eq!(metrics_after.0, metrics_before.0 + 1);
        assert_eq!(metrics_after.1, metrics_before.1 + 1);
        assert_eq!(metrics_after.2, metrics_before.2);
        assert_eq!(
            crate::stats::grpc_connection_state("store-stream-0", "store-stream", "READY"),
            1.0
        );

        let error = client
            .dispatch_with_forwarded_host(
                &crate::store::CoprocessorStreamRequest::new(
                    crate::proto::coprocessor::Request::default(),
                )
                .with_api_v2_codec(
                    crate::request::ApiV2Codec::new(crate::request::KeyMode::Txn, 7).unwrap(),
                ),
                "store-2",
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "streaming coprocessor is not supported yet"
        );
        assert_eq!(
            crate::stats::grpc_connection_state("store-stream-0", "store-stream", "READY"),
            1.0
        );

        client.close();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_transport_error_carries_cached_connection_identity() {
        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        let address = server.addr().unwrap();
        server.set_metadata_checker(Some(Arc::new(|_| {
            Err(tonic::Status::unavailable("transport failed"))
        })));
        let rpc = KvClient::with_connection_info(
            KvRpcClient::new(
                vec![TikvClient::connect(format!("http://{address}"))
                    .await
                    .unwrap()],
                Duration::from_secs(1),
            ),
            "store-a".to_owned(),
            7,
        );
        assert_eq!(
            crate::stats::grpc_connection_state("store-a-0", "store-a", "IDLE"),
            1.0
        );

        let request = crate::proto::kvrpcpb::PrewriteRequest {
            context: Some(kvrpcpb::Context {
                peer: Some(crate::proto::metapb::Peer {
                    store_id: 42,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let error = KvClient::dispatch(&rpc, &request).await.unwrap_err();
        assert!(matches!(
            error,
            crate::Error::Connection {
                address,
                version: 7,
                source,
            } if address == "store-a" && matches!(*source, crate::Error::GrpcAPI(_))
        ));
        assert_eq!(
            crate::stats::grpc_connection_state("store-a-0", "store-a", "TRANSIENT_FAILURE"),
            1.0
        );
        let transient_failures = crate::stats::grpc_connection_transient_failures("store-a", 42);
        let _ = KvClient::dispatch(&rpc, &request).await.unwrap_err();
        assert_eq!(
            crate::stats::grpc_connection_transient_failures("store-a", 42),
            transient_failures + 1
        );
        rpc.close();
        assert_eq!(
            crate::stats::grpc_connection_state("store-a-0", "store-a", "SHUTDOWN"),
            0.0
        );
        assert_eq!(
            crate::stats::grpc_connection_state("store-a-0", "store-a", "TRANSIENT_FAILURE"),
            0.0
        );
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_worker_clones_share_connection_identity_assigned_after_creation() {
        let client = KvRpcClient::new(
            vec![TikvClient::new(
                Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            )],
            Duration::from_secs(1),
        );
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 2;
        let client = client.with_batch_worker(&config);
        let worker = client.batch_worker.as_ref().unwrap().clone();
        let client = KvClient::with_connection_info(client, "store-a".to_owned(), 7);

        worker.close();
        let error = KvClient::dispatch(&client, &crate::proto::kvrpcpb::GetRequest::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::Error::Connection {
                address,
                version: 7,
                ..
            } if address == "store-a"
        ));
    }

    #[tokio::test]
    async fn source_connection_pool_round_robin_increments_before_selecting() {
        let clients = (0..3)
            .map(|_| TikvClient::new(Channel::from_static("http://127.0.0.1:1").connect_lazy()))
            .collect();
        let client = KvRpcClient::new(clients, Duration::from_secs(2));
        assert_eq!(
            (0..7)
                .map(|_| client.next_client_index())
                .collect::<Vec<_>>(),
            [1, 2, 0, 1, 2, 0, 1]
        );
    }

    #[tokio::test]
    async fn source_resolve_lock_singleflight_key_and_exclusions() {
        use crate::proto::tikvpb;
        use tikvpb::batch_commands_response::response::Cmd;

        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let counted = request_count.clone();
        server.set_batch_commands_handler(Some(Arc::new(move |request| {
            counted.fetch_add(request.requests.len(), Ordering::Relaxed);
            // Keep the first physical request in flight long enough for all
            // logical callers to join its source singleflight group.
            std::thread::sleep(Duration::from_millis(30));
            Ok(tikvpb::BatchCommandsResponse {
                responses: request
                    .requests
                    .iter()
                    .map(|_| tikvpb::batch_commands_response::Response {
                        cmd: Some(Cmd::ResolveLock(kvrpcpb::ResolveLockResponse::default())),
                    })
                    .collect(),
                request_ids: request.request_ids,
                ..Default::default()
            })
        })));
        let address = server.addr().unwrap();
        let mut config = crate::config::TiKvClient::default();
        config.max_batch_size = 8;
        let client = KvRpcClient::new(
            vec![TikvClient::connect(format!("http://{address}"))
                .await
                .unwrap()],
            Duration::from_secs(1),
        )
        .with_batch_worker(&config);
        let other_client = KvRpcClient::new(
            vec![TikvClient::connect(format!("http://{address}"))
                .await
                .unwrap()],
            Duration::from_secs(1),
        )
        .with_batch_worker(&config);

        let request = kvrpcpb::ResolveLockRequest {
            context: Some(kvrpcpb::Context {
                region_id: 7,
                ..Default::default()
            }),
            start_version: 11,
            ..Default::default()
        };
        let (first, second) = tokio::join!(
            other_client.dispatch_with_timeout_and_forwarded_host(
                &request,
                Some(Duration::from_secs(1)),
                ""
            ),
            client.dispatch_with_timeout_and_forwarded_host(
                &request,
                Some(Duration::from_secs(1)),
                ""
            )
        );
        first
            .unwrap()
            .downcast::<kvrpcpb::ResolveLockResponse>()
            .unwrap();
        second
            .unwrap()
            .downcast::<kvrpcpb::ResolveLockResponse>()
            .unwrap();
        assert_eq!(request_count.load(Ordering::Relaxed), 1);

        let independently_timed = kvrpcpb::ResolveLockRequest {
            start_version: 12,
            ..request.clone()
        };
        let (short, long) = tokio::join!(
            client.dispatch_with_timeout_and_forwarded_host(
                &independently_timed,
                Some(Duration::from_millis(1)),
                ""
            ),
            client.dispatch_with_timeout_and_forwarded_host(
                &independently_timed,
                Some(Duration::from_secs(1)),
                ""
            )
        );
        assert!(
            matches!(short, Err(crate::Error::GrpcAPI(status)) if status.code() == tonic::Code::DeadlineExceeded)
        );
        assert!(long.is_ok());
        assert_eq!(request_count.load(Ordering::Relaxed), 2);

        let async_request = kvrpcpb::ResolveLockRequest {
            is_async: true,
            ..request.clone()
        };
        let lite_request = kvrpcpb::ResolveLockRequest {
            keys: vec![b"key".to_vec()],
            ..request.clone()
        };
        let batch_request = kvrpcpb::ResolveLockRequest {
            txn_infos: vec![kvrpcpb::TxnInfo::default()],
            ..request.clone()
        };
        let (sync, asynchronous, lite, batch) = tokio::join!(
            client.dispatch_with_timeout_and_forwarded_host(
                &request,
                Some(Duration::from_secs(1)),
                ""
            ),
            client.dispatch_with_timeout_and_forwarded_host(
                &async_request,
                Some(Duration::from_secs(1)),
                ""
            ),
            client.dispatch_with_timeout_and_forwarded_host(
                &lite_request,
                Some(Duration::from_secs(1)),
                ""
            ),
            client.dispatch_with_timeout_and_forwarded_host(
                &batch_request,
                Some(Duration::from_secs(1)),
                ""
            )
        );
        assert!(sync.is_ok());
        assert!(asynchronous.is_ok());
        assert!(lite.is_ok());
        assert!(batch.is_ok());
        assert_eq!(request_count.load(Ordering::Relaxed), 6);

        client.close();
        other_client.close();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_exec_details_trace_wraps_a_physical_batch_rpc() {
        use crate::proto::tikvpb;
        use tikvpb::batch_commands_response::response::Cmd;

        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        server.set_batch_commands_handler(Some(Arc::new(|request| {
            Ok(tikvpb::BatchCommandsResponse {
                responses: request
                    .requests
                    .iter()
                    .map(|_| tikvpb::batch_commands_response::Response {
                        cmd: Some(Cmd::Get(kvrpcpb::GetResponse {
                            exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                                time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                                    total_rpc_wall_time_ns: 1_000_000,
                                    wait_wall_time_ns: 100_000,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })),
                    })
                    .collect(),
                request_ids: request.request_ids,
                ..Default::default()
            })
        })));
        let address = server.addr().unwrap();
        let client = KvRpcClient::new(
            vec![TikvClient::connect(format!("http://{address}"))
                .await
                .unwrap()],
            Duration::from_secs(1),
        )
        .with_batch_worker(&crate::config::TiKvClient::default());
        let traces = Arc::new(Mutex::new(Vec::new()));
        let observed = traces.clone();

        crate::trace::with_trace_exec_details(
            Arc::new(move |_, span| observed.lock().unwrap().push(span.to_string())),
            async {
                client
                    .dispatch(&kvrpcpb::GetRequest::default())
                    .await
                    .unwrap();
            },
        )
        .await;
        assert_eq!(
            *traces.lock().unwrap(),
            vec!["tikv.RPC[1ms]{ tikv.Wait[100µs] tikv.Process tikv.Suspend }"]
        );

        client
            .dispatch(&kvrpcpb::GetRequest::default())
            .await
            .unwrap();
        assert_eq!(traces.lock().unwrap().len(), 1);
        client.close();
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn source_debug_and_empty_commands_use_their_distinct_paths() {
        let (mut server, _) = crate::store::mockserver::start_mock_tikv_service()
            .await
            .unwrap();
        let address = server.addr().unwrap();
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = KvRpcClient::new_with_debug_clients(
            vec![TikvClient::new(channel.clone())],
            vec![DebugClient::new(channel)],
            Duration::from_secs(1),
        );

        // Debug requests use debugpb on the selected channel and deliberately
        // ignore TiKV forwarding metadata. The source-shaped mock has no Debug
        // service, so reaching that route produces gRPC Unimplemented rather
        // than failing to encode the deliberately invalid forwarding value.
        let error = client
            .dispatch_with_forwarded_host(
                &crate::proto::debugpb::GetRegionPropertiesRequest::default(),
                "invalid\nmetadata",
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::GrpcAPI(status) if status.code() == tonic::Code::Unimplemented)
        );

        let response = client
            .dispatch(&crate::proto::tikvpb::BatchCommandsEmptyRequest::default())
            .await
            .unwrap();
        response
            .downcast::<crate::proto::tikvpb::BatchCommandsEmptyResponse>()
            .unwrap();
        client.close();
        server.stop().await.unwrap();
    }

    #[test]
    fn source_batch_stream_metadata_carries_forwarding_host_and_pool_index() {
        let forwarded =
            KvRpcClient::batch_commands_request((), "store-2", 3).expect("valid source metadata");
        assert_eq!(
            forwarded
                .metadata()
                .get("tikv-forwarded-host")
                .expect("forward header"),
            "store-2"
        );
        assert_eq!(
            forwarded
                .metadata()
                .get("tikv-batch-conn-index")
                .expect("connection header"),
            "3"
        );

        let direct =
            KvRpcClient::batch_commands_request((), "", 1).expect("valid direct stream metadata");
        assert!(direct.metadata().get("tikv-forwarded-host").is_none());
        assert_eq!(
            direct
                .metadata()
                .get("tikv-batch-conn-index")
                .expect("connection header"),
            "1"
        );
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;

    /// Dispatch with an optional caller-specific deadline. Generic clients
    /// retain their existing behavior; concrete RPC clients apply it to both
    /// unary and batch transports.
    async fn dispatch_with_timeout(
        &self,
        req: &dyn Request,
        _timeout: Option<Duration>,
    ) -> Result<Box<dyn Any>> {
        self.dispatch(req).await
    }

    /// Dispatches through a physical store while asking it to forward to the
    /// logical target. Generic clients retain direct dispatch by default.
    async fn dispatch_with_forwarded_host(
        &self,
        req: &dyn Request,
        _forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        self.dispatch(req).await
    }

    /// Forwarding variant of [`Self::dispatch_with_timeout`].
    async fn dispatch_with_timeout_and_forwarded_host(
        &self,
        req: &dyn Request,
        timeout: Option<Duration>,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        match timeout {
            Some(_) => self.dispatch_with_timeout(req, timeout).await,
            None => self.dispatch_with_forwarded_host(req, forwarded_host).await,
        }
    }

    /// Installs the source `ClientEventListener` for this concrete transport.
    /// Test/custom clients may retain the default no-op behavior.
    fn set_event_listener(&self, _listener: Arc<dyn ClientEventListener>) {}

    /// Associates a concrete cached client with its source-compatible pool
    /// identity. Generic test and custom clients do not need to model it.
    fn with_connection_info(self, _address: String, _version: u64) -> Self
    where
        Self: Sized,
    {
        self
    }

    /// Retires this concrete pool. Existing callers may still hold a clone,
    /// so implementations must make subsequent use fail deterministically.
    fn close(&self) {}
}

/// Receives store health information attached to BatchCommands responses.
/// Replacing a listener has the same last-registration-wins contract as
/// client-go's `Client.SetEventListener`.
pub trait ClientEventListener: Send + Sync {
    fn on_health_feedback(&self, feedback: &kvrpcpb::HealthFeedback);
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
#[derive(Clone)]
pub struct KvRpcClient {
    rpc_clients: Arc<[TikvClient<Channel>]>,
    debug_clients: Option<Arc<[DebugClient<Channel>]>>,
    connection_states: Arc<[AtomicU8]>,
    next_client: Arc<AtomicUsize>,
    timeout: Duration,
    batch_worker: Option<Arc<BatchCommandsWorker>>,
    event_listener: Arc<RwLock<Option<Arc<dyn ClientEventListener>>>>,
    connection: Arc<RwLock<Option<Arc<ConnectionInfo>>>>,
    open_tracing_enable: bool,
}

struct ConnectionInfo {
    address: String,
    version: u64,
    closed: AtomicBool,
}

fn resolve_lock_collapse_request(
    request: &dyn Request,
) -> Option<(ResolveLockCollapseKey, kvrpcpb::ResolveLockRequest)> {
    let request = request
        .as_any()
        .downcast_ref::<kvrpcpb::ResolveLockRequest>()?;
    if !request.keys.is_empty() || !request.txn_infos.is_empty() {
        return None;
    }
    Some((
        ResolveLockCollapseKey {
            region_id: request
                .context
                .as_ref()
                .map_or(0, |context| context.region_id),
            start_version: request.start_version,
            // `is_txn_file` is implied by `start_version` in client-go and is
            // deliberately absent from this key.
            is_async: request.is_async,
        },
        request.clone(),
    ))
}

impl KvRpcClient {
    pub(crate) fn new(rpc_clients: Vec<TikvClient<Channel>>, timeout: Duration) -> Self {
        Self::new_inner(rpc_clients, None, timeout)
    }

    fn new_with_debug_clients(
        rpc_clients: Vec<TikvClient<Channel>>,
        debug_clients: Vec<DebugClient<Channel>>,
        timeout: Duration,
    ) -> Self {
        assert_eq!(
            rpc_clients.len(),
            debug_clients.len(),
            "TiKV and debug service pools must share each channel slot"
        );
        Self::new_inner(rpc_clients, Some(debug_clients.into()), timeout)
    }

    fn new_inner(
        rpc_clients: Vec<TikvClient<Channel>>,
        debug_clients: Option<Arc<[DebugClient<Channel>]>>,
        timeout: Duration,
    ) -> Self {
        assert!(
            !rpc_clients.is_empty(),
            "TiKV connection pool must not be empty"
        );
        let connection_states = (0..rpc_clients.len())
            .map(|_| AtomicU8::new(GrpcConnectionState::Idle as u8))
            .collect::<Vec<_>>()
            .into();
        Self {
            rpc_clients: rpc_clients.into(),
            debug_clients,
            connection_states,
            next_client: Arc::new(AtomicUsize::new(0)),
            timeout,
            batch_worker: None,
            event_listener: Arc::new(RwLock::new(None)),
            connection: Arc::new(RwLock::new(None)),
            open_tracing_enable: false,
        }
    }

    pub fn set_event_listener(&self, listener: Arc<dyn ClientEventListener>) {
        *self.event_listener.write().unwrap() = Some(listener);
    }

    pub(crate) fn event_listener(&self) -> Arc<RwLock<Option<Arc<dyn ClientEventListener>>>> {
        self.event_listener.clone()
    }

    /// Source BatchCommands metrics label streams by the cached TiKV target.
    /// Construction can precede cache identity assignment, so read the shared
    /// identity at stream creation time rather than storing a stale copy.
    pub(crate) fn batch_metric_target(&self) -> String {
        self.connection
            .read()
            .unwrap()
            .as_ref()
            .map(|connection| connection.address.clone())
            .unwrap_or_default()
    }

    pub(crate) fn with_batch_worker(mut self, config: &crate::config::TiKvClient) -> Self {
        if let Some(dispatcher) = BatchCommandsDispatcher::from_config(self.clone(), config) {
            let dispatcher = Arc::new(dispatcher);
            self.batch_worker = Some(Arc::new(dispatcher.spawn_worker(config)));
        }
        self
    }

    fn with_open_tracing(mut self, enabled: bool) -> Self {
        self.open_tracing_enable = enabled;
        self
    }

    fn next_client_index(&self) -> usize {
        (self.next_client.fetch_add(1, Ordering::Relaxed) + 1) % self.rpc_clients.len()
    }

    /// Selects the one source `batchCommandsClient` equivalent for an entire
    /// built batch. Direct and forwarded groups from that batch must share the
    /// returned pool slot; only their stream metadata differs.
    pub(crate) fn next_batch_connection_index(&self) -> usize {
        self.next_client_index()
    }

    pub(crate) fn batch_connection_count(&self) -> usize {
        self.rpc_clients.len()
    }

    fn batch_commands_request<S>(
        requests: S,
        forwarded_host: &str,
        connection_index: usize,
    ) -> Result<TonicRequest<S>> {
        const FORWARD_METADATA_KEY: &str = "tikv-forwarded-host";
        const BATCH_CONNECTION_INDEX_METADATA_KEY: &str = "tikv-batch-conn-index";

        let mut request = TonicRequest::new(requests);
        if !forwarded_host.is_empty() {
            let forwarded_host = MetadataValue::try_from(forwarded_host).map_err(|error| {
                crate::Error::StringError(format!(
                    "invalid BatchCommands forwarding host metadata: {error}"
                ))
            })?;
            request
                .metadata_mut()
                .insert(FORWARD_METADATA_KEY, forwarded_host);
        }
        // Every pooled Rust channel corresponds to client-go's `batchConn`,
        // so carry its source connection index on every batch stream.
        let connection_index = connection_index.to_string();
        request.metadata_mut().insert(
            BATCH_CONNECTION_INDEX_METADATA_KEY,
            MetadataValue::try_from(connection_index.as_str())
                .expect("decimal index is valid metadata"),
        );
        Ok(request)
    }

    /// Open a source `BatchCommands` stream on the next pooled TiKV channel.
    ///
    /// Request-ID assignment and response multiplexing belong to the higher
    /// level internal-client batch loop; this method only owns the pooled
    /// bidirectional transport boundary.
    #[allow(dead_code)]
    pub(crate) async fn open_batch_commands<S>(
        &self,
        forwarded_host: &str,
        requests: S,
    ) -> Result<tonic::codec::Streaming<crate::proto::tikvpb::BatchCommandsResponse>>
    where
        S: Stream<Item = crate::proto::tikvpb::BatchCommandsRequest> + Send + 'static,
    {
        let connection_index = self.next_client_index();
        self.open_batch_commands_on(connection_index, forwarded_host, requests)
            .await
    }

    /// Opens a batch stream on an already selected pool slot. This preserves
    /// client-go's rule that all direct/forwarded groups from one builder pass
    /// are sent by the same `batchCommandsClient`.
    pub(crate) async fn open_batch_commands_on<S>(
        &self,
        connection_index: usize,
        forwarded_host: &str,
        requests: S,
    ) -> Result<tonic::codec::Streaming<crate::proto::tikvpb::BatchCommandsResponse>>
    where
        S: Stream<Item = crate::proto::tikvpb::BatchCommandsRequest> + Send + 'static,
    {
        assert!(
            connection_index < self.rpc_clients.len(),
            "batch connection index must belong to this pool"
        );
        self.set_connection_state(connection_index, GrpcConnectionState::Connecting);
        let mut request = Self::batch_commands_request(requests, forwarded_host, connection_index)?;
        crate::trace::inject_grpc_trace_metadata(request.metadata_mut(), self.open_tracing_enable);
        let result = self.rpc_clients[connection_index]
            .clone()
            .batch_commands(request)
            .await
            .map(|response| response.into_inner())
            .map_err(crate::Error::from);
        self.set_connection_state(
            connection_index,
            if result.is_ok() {
                GrpcConnectionState::Ready
            } else {
                GrpcConnectionState::TransientFailure
            },
        );
        result
    }

    /// Dispatches a source request through a forwarding store. This is the
    /// transport counterpart to client-go's `Request.ForwardedHost`; callers
    /// that own a request wrapper can select a forwarded target without
    /// mutating the generated protobuf payload.
    pub(crate) async fn dispatch_with_forwarded_host(
        &self,
        request: &dyn Request,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        self.dispatch_with_timeout_and_forwarded_host(request, None, forwarded_host)
            .await
    }

    pub(crate) async fn dispatch_with_timeout_and_forwarded_host(
        &self,
        request: &dyn Request,
        timeout: Option<Duration>,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        if let Some((key, request)) = resolve_lock_collapse_request(request) {
            return self
                .dispatch_collapsed_resolve_lock(
                    key,
                    request,
                    timeout.unwrap_or(self.timeout),
                    forwarded_host,
                )
                .await;
        }
        self.dispatch_uncollapsed(request, timeout, forwarded_host)
            .await
    }

    async fn dispatch_collapsed_resolve_lock(
        &self,
        key: ResolveLockCollapseKey,
        request: kvrpcpb::ResolveLockRequest,
        caller_timeout: Duration,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        let (flight, created) = {
            let mut flights = RESOLVE_LOCK_COLLAPSER.flights.lock().unwrap();
            match flights.get(&key) {
                Some(flight) => (flight.clone(), false),
                None => {
                    let flight = Arc::new(ResolveLockFlight::default());
                    flights.insert(key.clone(), flight.clone());
                    (flight, true)
                }
            }
        };

        if created {
            let client = self.clone();
            let forwarded_host = forwarded_host.to_owned();
            let collapser = RESOLVE_LOCK_COLLAPSER.clone();
            let spawned_flight = flight.clone();
            tokio::spawn(async move {
                let result = client
                    .dispatch_uncollapsed(&request, Some(READ_TIMEOUT_SHORT), &forwarded_host)
                    .await
                    .and_then(|response| {
                        response
                            .downcast::<kvrpcpb::ResolveLockResponse>()
                            .map(|response| *response)
                            .map_err(|_| {
                                crate::Error::StringError(
                                    "ResolveLock RPC returned an unexpected response type"
                                        .to_owned(),
                                )
                            })
                    })
                    .map_err(CollapsedResolveLockError::from_error);
                spawned_flight.complete(result);
                let mut flights = collapser.flights.lock().unwrap();
                if flights
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &spawned_flight))
                {
                    flights.remove(&key);
                }
            });
        }

        let result = tokio::time::timeout(caller_timeout, flight.wait())
            .await
            .map_err(|_| {
                crate::Error::GrpcAPI(tonic::Status::deadline_exceeded(
                    "collapsed ResolveLock request deadline exceeded",
                ))
            })?;
        match result {
            Ok(response) => Ok(Box::new(response)),
            Err(error) => Err(error.to_error()),
        }
    }

    async fn dispatch_uncollapsed(
        &self,
        request: &dyn Request,
        timeout: Option<Duration>,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        if let Some(connection) = self.connection.read().unwrap().clone() {
            if connection.closed.load(Ordering::Acquire) {
                return Err(self.wrap_connection_error(crate::Error::GrpcAPI(
                    tonic::Status::unavailable("TiKV connection pool is closed"),
                )));
            }
        }
        let started_at = Instant::now();
        if let Some(request) = request
            .as_any()
            .downcast_ref::<crate::proto::debugpb::GetRegionPropertiesRequest>()
        {
            let index = self.next_client_index();
            self.observe_transient_failure_before_send(index, request);
            let Some(debug_clients) = self.debug_clients.as_ref() else {
                let result = crate::trace::with_grpc_open_tracing(
                    self.open_tracing_enable,
                    request.dispatch(&self.rpc_clients[index], timeout.unwrap_or(self.timeout)),
                )
                .await;
                crate::stats::observe_tikv_store_rpc(
                    request,
                    result.as_ref().ok().map(|response| response.as_ref()),
                    started_at.elapsed(),
                );
                return result;
            };
            self.set_connection_state(index, GrpcConnectionState::Connecting);
            let mut wire_request = request.clone().into_request();
            wire_request.set_timeout(timeout.unwrap_or(self.timeout));
            crate::trace::inject_grpc_trace_metadata(
                wire_request.metadata_mut(),
                self.open_tracing_enable,
            );
            let result = debug_clients[index]
                .clone()
                .get_region_properties(wire_request)
                .await
                .map(|response| Box::new(response.into_inner()) as Box<dyn Any>)
                .map_err(Error::from)
                .map_err(|error| self.wrap_connection_error(error));
            self.set_connection_state(
                index,
                if result.is_ok() {
                    GrpcConnectionState::Ready
                } else {
                    GrpcConnectionState::TransientFailure
                },
            );
            crate::stats::observe_tikv_store_rpc(
                request,
                result.as_ref().ok().map(|response| response.as_ref()),
                started_at.elapsed(),
            );
            return result;
        }
        if let (Some(worker), Some(batch_request)) = (
            self.batch_worker.as_ref(),
            BatchCommandRequest::from_store_request(request),
        ) {
            let timeout = timeout.unwrap_or(self.timeout);
            let deadline = tokio::time::Instant::now() + timeout;
            let mut submission = match worker
                .submit_until(
                    batch_request,
                    request.batch_priority(),
                    forwarded_host,
                    deadline,
                )
                .await
            {
                Ok(submission) => submission,
                Err(error) => {
                    crate::stats::observe_tikv_store_rpc(request, None, started_at.elapsed());
                    return Err(self.wrap_connection_error(error));
                }
            };
            let response = match tokio::time::timeout_at(deadline, submission.recv()).await {
                Err(_) => {
                    submission.cancellation.cancel();
                    let error = crate::Error::GrpcAPI(tonic::Status::deadline_exceeded(
                        submission.timeout_reason(timeout, Instant::now()),
                    ));
                    submission.complete_with_error(&error);
                    Err(error)
                }
                Ok(Err(_)) => {
                    submission.cancellation.cancel();
                    let error = crate::Error::StringError(
                        "BatchCommands worker stopped before responding".to_owned(),
                    );
                    submission.complete_with_error(&error);
                    Err(error)
                }
                Ok(Ok(result)) => result,
            }
            .map_err(|error| self.wrap_connection_error(error));
            return match response {
                Ok(response) => {
                    let mut response = BatchCommandResponse::into_any(response);
                    crate::stats::observe_tikv_store_rpc(
                        request,
                        Some(response.as_ref()),
                        started_at.elapsed(),
                    );
                    crate::trace::trace_exec_details_response(started_at, response.as_ref());
                    request.decode_transport_response(response.as_mut())?;
                    Ok(response)
                }
                Err(error) => {
                    crate::stats::observe_tikv_store_rpc(request, None, started_at.elapsed());
                    Err(error)
                }
            };
        }
        let index = self.next_client_index();
        let timeout = timeout.unwrap_or(self.timeout);
        self.observe_transient_failure_before_send(index, request);
        self.set_connection_state(index, GrpcConnectionState::Connecting);
        let mut result = crate::trace::with_grpc_open_tracing(
            self.open_tracing_enable,
            request.dispatch_with_forwarded_host(&self.rpc_clients[index], timeout, forwarded_host),
        )
        .await
        .map_err(|error| self.wrap_connection_error(error));
        let transport_succeeded = result.is_ok();
        crate::stats::observe_tikv_store_rpc(
            request,
            result.as_ref().ok().map(|response| response.as_ref()),
            started_at.elapsed(),
        );
        if let Ok(response) = &mut result {
            crate::trace::trace_exec_details_response(started_at, response.as_ref());
            if let Err(error) = request.decode_transport_response(response.as_mut()) {
                result = Err(error);
            }
        }
        self.set_connection_state(
            index,
            if transport_succeeded {
                GrpcConnectionState::Ready
            } else {
                GrpcConnectionState::TransientFailure
            },
        );
        result
    }

    fn set_connection_state(&self, index: usize, state: GrpcConnectionState) {
        self.connection_states[index].store(state as u8, Ordering::Release);
        let Some(connection) = self.connection.read().unwrap().clone() else {
            return;
        };
        crate::stats::set_grpc_connection_state(
            &format!("{}-{index}", connection.address),
            &connection.address,
            state.name(),
        );
    }

    pub(crate) fn mark_connection_transient_failure(&self, index: usize) {
        self.set_connection_state(index, GrpcConnectionState::TransientFailure);
    }

    fn observe_transient_failure_before_send(&self, index: usize, request: &dyn Request) {
        if self.connection_states[index].load(Ordering::Acquire)
            != GrpcConnectionState::TransientFailure as u8
        {
            return;
        }
        let Some(connection) = self.connection.read().unwrap().clone() else {
            return;
        };
        let store_id = request
            .tikv_context()
            .and_then(|context| context.peer.as_ref())
            .map_or(0, |peer| peer.store_id);
        crate::stats::increment_grpc_connection_transient_failure(&connection.address, store_id);
    }

    fn wrap_connection_error(&self, error: crate::Error) -> crate::Error {
        match self.connection.read().unwrap().clone() {
            Some(_) if error.connection_info().is_some() => error,
            Some(connection) => crate::Error::Connection {
                source: Box::new(error),
                address: connection.address.clone(),
                version: connection.version,
            },
            None => error,
        }
    }
}

#[async_trait]
impl KvClient for KvRpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        self.dispatch_with_forwarded_host(request, "").await
    }

    async fn dispatch_with_forwarded_host(
        &self,
        request: &dyn Request,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        KvRpcClient::dispatch_with_forwarded_host(self, request, forwarded_host).await
    }

    async fn dispatch_with_timeout(
        &self,
        request: &dyn Request,
        timeout: Option<Duration>,
    ) -> Result<Box<dyn Any>> {
        KvRpcClient::dispatch_with_timeout_and_forwarded_host(self, request, timeout, "").await
    }

    async fn dispatch_with_timeout_and_forwarded_host(
        &self,
        request: &dyn Request,
        timeout: Option<Duration>,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        KvRpcClient::dispatch_with_timeout_and_forwarded_host(
            self,
            request,
            timeout,
            forwarded_host,
        )
        .await
    }

    fn set_event_listener(&self, listener: Arc<dyn ClientEventListener>) {
        KvRpcClient::set_event_listener(self, listener);
    }

    fn with_connection_info(self, address: String, version: u64) -> Self {
        *self.connection.write().unwrap() = Some(Arc::new(ConnectionInfo {
            address,
            version,
            closed: AtomicBool::new(false),
        }));
        for index in 0..self.rpc_clients.len() {
            self.set_connection_state(index, GrpcConnectionState::Idle);
        }
        self
    }

    fn close(&self) {
        if let Some(connection) = self.connection.read().unwrap().clone() {
            connection.closed.store(true, Ordering::Release);
        }
        if let Some(worker) = &self.batch_worker {
            worker.close();
        }
        for index in 0..self.rpc_clients.len() {
            let Some(connection) = self.connection.read().unwrap().clone() else {
                continue;
            };
            crate::stats::clear_grpc_connection_state(
                &format!("{}-{index}", connection.address),
                &connection.address,
            );
        }
    }
}

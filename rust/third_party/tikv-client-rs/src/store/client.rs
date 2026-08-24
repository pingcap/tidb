// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request as TonicRequest;

use super::batch::{BatchCommandsDispatcher, BatchCommandsWorker};
use super::Request;
use super::{BatchCommandRequest, BatchCommandResponse};
use crate::proto::kvrpcpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::Result;
use crate::SecurityManager;

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
    grpc_max_decoding_message_size: usize,
    send_gzip_requests: bool,
    grpc_keepalive_time: Duration,
    grpc_keepalive_timeout: Duration,
    grpc_initial_stream_window_size: Option<u32>,
    grpc_initial_connection_window_size: Option<u32>,
    grpc_connection_count: usize,
    batch_config: crate::config::TiKvClient,
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
            grpc_max_decoding_message_size,
            send_gzip_requests: grpc_compression_type == "gzip",
            grpc_keepalive_time,
            grpc_keepalive_timeout,
            grpc_initial_stream_window_size,
            grpc_initial_connection_window_size,
            grpc_connection_count,
            batch_config: crate::config::TiKvClient::default(),
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
}

#[async_trait]
impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    async fn connect(&self, address: &str) -> Result<KvRpcClient> {
        let mut clients = Vec::with_capacity(self.grpc_connection_count);
        for _ in 0..self.grpc_connection_count {
            let client = self
                .security_mgr
                .connect_with_http2_settings(
                    address,
                    self.grpc_keepalive_time,
                    self.grpc_keepalive_timeout,
                    self.grpc_initial_stream_window_size,
                    self.grpc_initial_connection_window_size,
                    |channel| {
                        let client = TikvClient::new(channel)
                            .max_decoding_message_size(self.grpc_max_decoding_message_size)
                            .accept_compressed(CompressionEncoding::Gzip);
                        if self.send_gzip_requests {
                            client.send_compressed(CompressionEncoding::Gzip)
                        } else {
                            client
                        }
                    },
                )
                .await?;
            clients.push(client);
        }
        // The worker owns only batchable physical requests; unsupported
        // requests retain the unary path below.
        Ok(KvRpcClient::new(clients, self.timeout).with_batch_worker(&self.batch_config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

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

        let error = KvClient::dispatch(&rpc, &crate::proto::kvrpcpb::PrewriteRequest::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::Error::Connection {
                address,
                version: 7,
                source,
            } if address == "store-a" && matches!(*source, crate::Error::GrpcAPI(_))
        ));
        drop(rpc);
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

    /// Dispatches through a physical store while asking it to forward to the
    /// logical target. Generic clients retain direct dispatch by default.
    async fn dispatch_with_forwarded_host(
        &self,
        req: &dyn Request,
        _forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        self.dispatch(req).await
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
    next_client: Arc<AtomicUsize>,
    timeout: Duration,
    batch_worker: Option<Arc<BatchCommandsWorker>>,
    event_listener: Arc<RwLock<Option<Arc<dyn ClientEventListener>>>>,
    connection: Arc<RwLock<Option<Arc<ConnectionInfo>>>>,
}

struct ConnectionInfo {
    address: String,
    version: u64,
    closed: AtomicBool,
}

impl KvRpcClient {
    pub(crate) fn new(rpc_clients: Vec<TikvClient<Channel>>, timeout: Duration) -> Self {
        assert!(
            !rpc_clients.is_empty(),
            "TiKV connection pool must not be empty"
        );
        Self {
            rpc_clients: rpc_clients.into(),
            next_client: Arc::new(AtomicUsize::new(0)),
            timeout,
            batch_worker: None,
            event_listener: Arc::new(RwLock::new(None)),
            connection: Arc::new(RwLock::new(None)),
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

    fn next_client_index(&self) -> usize {
        (self.next_client.fetch_add(1, Ordering::Relaxed) + 1) % self.rpc_clients.len()
    }

    /// Selects the one source `batchCommandsClient` equivalent for an entire
    /// built batch. Direct and forwarded groups from that batch must share the
    /// returned pool slot; only their stream metadata differs.
    pub(crate) fn next_batch_connection_index(&self) -> usize {
        self.next_client_index()
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
        self.rpc_clients[connection_index]
            .clone()
            .batch_commands(Self::batch_commands_request(
                requests,
                forwarded_host,
                connection_index,
            )?)
            .await
            .map(|response| response.into_inner())
            .map_err(crate::Error::from)
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
        if let Some(connection) = self.connection.read().unwrap().clone() {
            if connection.closed.load(Ordering::Acquire) {
                return Err(self.wrap_connection_error(crate::Error::GrpcAPI(
                    tonic::Status::unavailable("TiKV connection pool is closed"),
                )));
            }
        }
        if let (Some(worker), Some(batch_request)) = (
            self.batch_worker.as_ref(),
            BatchCommandRequest::from_store_request(request),
        ) {
            let mut submission = worker
                .submit(batch_request, request.batch_priority(), forwarded_host)
                .await;
            let response = submission
                .recv()
                .await
                .map_err(|_| {
                    crate::Error::StringError(
                        "BatchCommands worker stopped before responding".to_owned(),
                    )
                })?
                .map_err(|error| self.wrap_connection_error(error))?;
            return Ok(BatchCommandResponse::into_any(response));
        }
        let index = self.next_client_index();
        request
            .dispatch_with_forwarded_host(&self.rpc_clients[index], self.timeout, forwarded_host)
            .await
            .map_err(|error| self.wrap_connection_error(error))
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

    fn set_event_listener(&self, listener: Arc<dyn ClientEventListener>) {
        KvRpcClient::set_event_listener(self, listener);
    }

    fn with_connection_info(self, address: String, version: u64) -> Self {
        *self.connection.write().unwrap() = Some(Arc::new(ConnectionInfo {
            address,
            version,
            closed: AtomicBool::new(false),
        }));
        self
    }

    fn close(&self) {
        if let Some(connection) = self.connection.read().unwrap().clone() {
            connection.closed.store(true, Ordering::Release);
        }
        if let Some(worker) = &self.batch_worker {
            worker.close();
        }
    }
}

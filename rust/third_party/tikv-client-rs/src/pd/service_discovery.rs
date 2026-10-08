// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared PD service-mode and keyspace-group discovery. Metadata continues to
//! use the PD leader; timestamp routing follows the separately discovered owner.

pub use super::service_mode::ServiceModeDiscovery;
use crate::proto::{keyspacepb, meta_storagepb, pdpb, tsopb};
use futures::{Stream, StreamExt};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{future::Future, pin::Pin, time::Duration};
use tonic::{transport::Channel, Request, Status};

#[derive(Clone, PartialEq, prost::Message)]
struct HealthCheckRequest {
    #[prost(string, tag = "1")]
    service: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct HealthCheckResponse {
    #[prost(int32, tag = "1")]
    status: i32,
}

/// Shared wire exchange for PD and TiKV health. Callers retain their distinct
/// status policy: PD accepts only SERVING; TiKV also models UNKNOWN separately.
/// Readiness, response and server metadata all consume the same caller budget.
pub async fn health_check(channel: Channel, timeout: Duration) -> Result<i32, Status> {
    tokio::time::timeout(timeout, async {
        let mut client = tonic::client::Grpc::new(channel);
        client
            .ready()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut request = Request::new(HealthCheckRequest {
            service: String::new(),
        });
        request.set_timeout(timeout);
        let response = client
            .unary(
                request,
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/grpc.health.v1.Health/Check",
                ),
                tonic_prost::ProstCodec::<HealthCheckRequest, HealthCheckResponse>::default(),
            )
            .await?;
        Ok(response.into_inner().status)
    })
    .await
    .map_err(|_| Status::deadline_exceeded("health check timed out"))?
}

/// Go tlsutil.PickMatchedURL chooses by configured scheme, never reachability.
/// Advertised URLs are expected to be valid; an empty list has no candidate.
pub fn pick_service_url(urls: &[String], tls: bool) -> Option<String> {
    let scheme = if tls { "https" } else { "http" };
    if let Some(url) = urls
        .iter()
        .find(|url| url::Url::parse(url).is_ok_and(|u| u.scheme() == scheme))
    {
        return Some(url.clone());
    }
    let first = urls.first()?;
    let address = first
        .strip_prefix("https://")
        .or_else(|| first.strip_prefix("http://"))
        .unwrap_or(first);
    Some(format!("{scheme}://{address}"))
}

/// Go grpcutil creates nonblocking connections and applies caller dial options.
/// Invoke this inside the discovery cache's factory so all consumers reuse the
/// resulting configured channel; the caller's runtime owns its driver.
pub fn lazy_channel(
    endpoint: tonic::transport::Endpoint,
    options: &super::opt::Options,
) -> Channel {
    options
        .grpc_dial_options
        .iter()
        .fold(endpoint, |endpoint, configure| configure(endpoint))
        .connect_lazy()
}

/// Cached connections belong to discovery and are shared by all PD consumers.
pub struct ChannelCache {
    channels: Mutex<Option<std::collections::HashMap<String, Channel>>>,
}

impl Default for ChannelCache {
    fn default() -> Self {
        Self {
            channels: Mutex::new(Some(Default::default())),
        }
    }
}

impl ChannelCache {
    /// Construct a lazy channel once. The factory must not perform blocking
    /// I/O and must run inside the runtime that owns the connection driver.
    pub fn get_or_insert_with(
        &self,
        endpoint: &str,
        connect: impl FnOnce() -> Result<Channel, Status>,
    ) -> Result<Channel, Status> {
        let mut guard = self.channels.lock().expect("PD channel cache poisoned");
        let channels = guard
            .as_mut()
            .ok_or_else(|| Status::cancelled("PD discovery closed"))?;
        if let Some(channel) = channels.get(endpoint) {
            return Ok(channel.clone());
        }
        let channel = connect()?;
        channels.insert(endpoint.to_owned(), channel.clone());
        Ok(channel)
    }

    /// Eager dial without holding a map lock across I/O. Concurrent callers
    /// retain the published winner and drop their redundant candidate, as Go
    /// LoadOrStore does. Failed or canceled construction is never published.
    pub async fn get_or_connect<F, Fut>(
        &self,
        endpoint: &str,
        connect: F,
    ) -> Result<Channel, Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        {
            let guard = self.channels.lock().expect("PD channel cache poisoned");
            let channels = guard
                .as_ref()
                .ok_or_else(|| Status::cancelled("PD discovery closed"))?;
            if let Some(channel) = channels.get(endpoint) {
                return Ok(channel.clone());
            }
        }
        let candidate = tokio::time::timeout(Duration::from_secs(3), connect())
            .await
            .map_err(|_| Status::deadline_exceeded("PD channel dial timed out"))??;
        // Recheck close and concurrent publication after dialing.
        self.get_or_insert_with(endpoint, || Ok(candidate))
    }

    /// Forget the endpoint whose server rejected its callee identity. The next
    /// discovery RPC must resolve and connect again, as Go RemoveClientConn does.
    /// Existing borrowed streams retain their own cancellation lifetime.
    pub fn remove(&self, endpoint: &str) {
        if let Some(channels) = self
            .channels
            .lock()
            .expect("PD channel cache poisoned")
            .as_mut()
        {
            channels.remove(endpoint);
        }
    }

    /// Release cached handles and prohibit new publication, including a dial
    /// that began before close. RPC/stream owners cancel and join separately.
    pub fn close(&self) {
        self.channels
            .lock()
            .expect("PD channel cache poisoned")
            .take();
    }
}

/// Go constants.NullKeyspaceID identifies the legacy, keyspace-agnostic client.
pub const NULL_KEYSPACE_ID: u32 = u32::MAX;
/// Go servicediscovery.serviceModeUpdateInterval.
pub const UPDATE_INTERVAL: Duration = Duration::from_secs(3);
/// Go servicediscovery.MemberUpdateInterval.
pub const MEMBER_UPDATE_INTERVAL: Duration = Duration::from_secs(60);

/// Published timestamp destination, independent of the PD metadata leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsoRoute {
    pub endpoint: String,
    pub keyspace_id: u32,
    pub group_id: Option<u32>,
    /// Logical primary when the physical endpoint is a proxy.
    pub forwarded_host: Option<String>,
}

pub type TsoResponses = Pin<Box<dyn Stream<Item = Result<pdpb::TsoResponse, Status>> + Send>>;

impl TsoRoute {
    /// Opens either wire protocol with one common request/response representation.
    /// Batching, deadlines, and stream ownership stay with the caller.
    pub async fn open(
        &self,
        channel: Channel,
        requests: impl Stream<Item = pdpb::TsoRequest> + Send + 'static,
    ) -> Result<TsoResponses, Status> {
        let Some(group_id) = self.group_id else {
            return Ok(Box::pin(
                pdpb::pd_client::PdClient::with_interceptor(
                    channel,
                    super::region_service::Forwarding(self.forwarded_host.clone()),
                )
                .tso(requests)
                .await?
                .into_inner(),
            ));
        };
        let keyspace_id = self.keyspace_id;
        let callee_id = callee_id(&self.endpoint);
        let requests = requests.map(move |request| tsopb::TsoRequest {
            header: Some(tsopb::RequestHeader {
                cluster_id: request.header.map_or(0, |header| header.cluster_id),
                keyspace_group_id: group_id,
                keyspace: Some(tsopb::request_header::Keyspace::KeyspaceId(keyspace_id)),
                callee_id: callee_id.clone(),
                ..Default::default()
            }),
            count: request.count,
            dc_location: request.dc_location,
        });
        let responses = tsopb::tso_client::TsoClient::with_interceptor(
            channel,
            super::region_service::Forwarding(self.forwarded_host.clone()),
        )
        .tso(requests)
        .await?
        .into_inner();
        Ok(Box::pin(responses.map(|response| {
            let response = response?;
            if let Some(error) = response.header.as_ref().and_then(|h| h.error.as_ref()) {
                return Err(Status::unknown(error.message.clone()));
            }
            Ok(pdpb::TsoResponse {
                header: response.header.map(|header| pdpb::ResponseHeader {
                    cluster_id: header.cluster_id,
                    ..Default::default()
                }),
                count: response.count,
                timestamp: response.timestamp,
            })
        })))
    }
}

/// One retained wire stream. Request batching and deadlines belong to the
/// caller's single dispatcher, not to individual proxy endpoints.
pub struct TsoStream {
    pub route: TsoRoute,
    requests: tokio::sync::mpsc::Sender<pdpb::TsoRequest>,
    responses: TsoResponses,
    forwarding: TsoForwarding,
    /// Responses published by the collector task, Go's `recvLoop`.
    ///
    /// The dispatcher waits on several events at once, so whatever it awaits
    /// can be dropped mid-poll. Awaiting the stream directly would cancel a
    /// partially received message and lose the batch it answers; a channel
    /// receive is cancellation-safe, so the loop that owns the receiving half
    /// runs in its own task and this is its output. Only the pipelined opener
    /// installs one -- the legacy coupled exchange keeps reading the stream.
    collected: Option<tokio::sync::mpsc::Receiver<Result<pdpb::TsoResponse, Status>>>,
    collector: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for TsoStream {
    fn drop(&mut self) {
        // Retiring a stream retires its collector; nothing else owns the
        // receiving half, so the task would otherwise outlive the route.
        if let Some(collector) = self.collector.take() {
            collector.abort();
        }
    }
}

/// Go `maxPendingRequestsInTSOStream` (`pd/client/clients/tso/stream.go:243`).
///
/// Go sizes this generously because `processRequests` hands a batch to the
/// stream and returns without waiting; the responses are collected by a
/// separate loop, so requests queue here while earlier ones are still in
/// flight. A capacity of one would serialize the exchange no matter what
/// concurrency the dispatcher was granted.
pub const MAX_PENDING_REQUESTS_IN_TSO_STREAM: usize = 64;

impl TsoStream {
    /// Opens the stream and hands over its first batch WITHOUT waiting for a
    /// response, so the batch is collected by [`Self::recv`] like any other.
    ///
    /// The first request still precedes any response header, because a server
    /// may withhold headers until it has one. Go's construction window also
    /// ends when the stream opens, before `Recv`, so a later application error
    /// or EOF cannot retroactively admit a proxy -- which is why success is
    /// recorded here rather than after a response.
    pub async fn open(
        route: TsoRoute,
        channel: Channel,
        first: pdpb::TsoRequest,
        forwarding: &TsoForwarding,
    ) -> Result<Self, Status> {
        let (requests, receiver) =
            tokio::sync::mpsc::channel(MAX_PENDING_REQUESTS_IN_TSO_STREAM);
        requests
            .send(first)
            .await
            .map_err(|_| Status::unavailable("TSO request stream is closed"))?;
        let responses = route
            .open(
                channel,
                futures::stream::unfold(receiver, |mut receiver| async move {
                    receiver.recv().await.map(|request| (request, receiver))
                }),
            )
            .await
            .map_err(|status| {
                forwarding.record_error(&route, &status);
                status
            })?;
        forwarding.record_success(&route);
        // Go's `recvLoop` owns the receiving half and publishes each response
        // as it arrives, so the dispatcher never polls the stream itself.
        let (published, collected) =
            tokio::sync::mpsc::channel(MAX_PENDING_REQUESTS_IN_TSO_STREAM);
        let mut responses = responses;
        let collector = tokio::spawn(async move {
            while let Some(response) = responses.next().await {
                if published.send(response).await.is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            route,
            requests,
            // The collector owns the stream; this placeholder keeps the field
            // for the legacy coupled path that still reads it directly.
            responses: Box::pin(futures::stream::empty()),
            forwarding: forwarding.clone(),
            collected: Some(collected),
            collector: Some(collector),
        })
    }

    pub async fn open_and_request(
        route: TsoRoute,
        channel: Channel,
        first: pdpb::TsoRequest,
        forwarding: &TsoForwarding,
    ) -> Result<(Self, pdpb::TsoResponse), Status> {
        let (requests, receiver) =
            tokio::sync::mpsc::channel(MAX_PENDING_REQUESTS_IN_TSO_STREAM);
        // Servers may withhold response headers until the first request.
        requests
            .send(first)
            .await
            .map_err(|_| Status::unavailable("TSO request stream is closed"))?;
        let mut responses = route
            .open(
                channel,
                futures::stream::unfold(receiver, |mut receiver| async move {
                    receiver.recv().await.map(|request| (request, receiver))
                }),
            )
            .await
            .map_err(|status| {
                forwarding.record_error(&route, &status);
                status
            })?;
        // Go's construction window ends when the stream opens, before Recv.
        // Application errors and EOF from an established stream cannot admit a proxy.
        forwarding.record_success(&route);
        let response = responses
            .next()
            .await
            .ok_or_else(|| Status::unavailable("TSO response stream is closed"))
            .and_then(|response| response);
        if route.forwarded_host.is_some() {
            if let Err(status) = &response {
                forwarding.record_error(&route, status);
            }
        }
        let response = response?;
        Ok((
            Self {
                route,
                requests,
                responses,
                collected: None,
                collector: None,
                forwarding: forwarding.clone(),
            },
            response,
        ))
    }

    /// The stream's request sink, cloned so a caller can hand over a batch
    /// without holding the stream itself across the send. Go's
    /// `processRequests` likewise only needs the stream's sending half, and
    /// the receiving half (`tonic::Streaming`) is not `Sync`, so borrowing the
    /// whole stream across an await would make the dispatcher non-`Send`.
    pub fn sender(&self) -> tokio::sync::mpsc::Sender<pdpb::TsoRequest> {
        self.requests.clone()
    }

    /// Collects the next response, which is Go's `recvLoop`. A gRPC stream
    /// delivers responses in the order its requests were sent, so the caller
    /// pairs them with its own FIFO queue of in-flight batches.
    pub async fn recv(&mut self) -> Result<pdpb::TsoResponse, Status> {
        let result = match self.collected.as_mut() {
            Some(collected) => collected
                .recv()
                .await
                .unwrap_or_else(|| Err(Status::unavailable("TSO response stream is closed"))),
            None => self
                .responses
                .next()
                .await
                .ok_or_else(|| Status::unavailable("TSO response stream is closed"))
                .and_then(|response| response),
        };
        if self.route.forwarded_host.is_some() {
            if let Err(status) = &result {
                self.forwarding.record_error(&self.route, status);
            }
        }
        result
    }

    pub async fn request(
        &mut self,
        request: pdpb::TsoRequest,
    ) -> Result<pdpb::TsoResponse, Status> {
        let result = async {
            self.requests
                .send(request)
                .await
                .map_err(|_| Status::unavailable("TSO request stream is closed"))?;
            self.responses
                .next()
                .await
                .ok_or_else(|| Status::unavailable("TSO response stream is closed"))?
        }
        .await;
        if self.route.forwarded_host.is_some() {
            if let Err(status) = &result {
                self.forwarding.record_error(&self.route, status);
            }
        }
        result
    }
}

/// Retained streams shared by both TSO dispatchers. An exchange temporarily
/// owns its stream; dropping its future on deadline/cancellation retires that
/// stream before a later caller can consume an abandoned response.
#[derive(Default)]
pub struct TsoStreamSet {
    streams: std::collections::HashMap<String, TsoStream>,
}

impl TsoStreamSet {
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }
    pub fn clear(&mut self) {
        self.streams.clear();
    }
    pub fn remove(&mut self, endpoint: &str) {
        self.streams.remove(endpoint);
    }
    pub fn retain_routes(&mut self, routes: &[TsoRoute]) {
        self.streams
            .retain(|_, stream| routes.contains(&stream.route));
    }

    /// Sends one batch on the route's retained stream, opening the stream with
    /// a coupled first exchange when there is none.
    ///
    /// Go establishes a stream by sending before any response header arrives
    /// and reading that first response as part of setup, so the opening batch
    /// is answered here; every later batch is handed over without waiting and
    /// its response is collected by [`Self::recv`].
    pub async fn send(
        &mut self,
        route: TsoRoute,
        channel: Channel,
        request: pdpb::TsoRequest,
        forwarding: &TsoForwarding,
    ) -> Result<(), Status> {
        let endpoint = route.endpoint.clone();
        let retained = self
            .streams
            .remove(&endpoint)
            .filter(|stream| stream.route == route);
        match retained {
            Some(stream) => {
                // Take the sink, put the stream straight back, then send. The
                // stream is never borrowed across the await.
                let sender = stream.sender();
                let forwarded = stream.route.forwarded_host.is_some();
                let stream_route = stream.route.clone();
                self.streams.insert(endpoint.clone(), stream);
                match sender.send(request).await {
                    Ok(()) => Ok(()),
                    Err(_) => {
                        // A send failure retires the stream rather than
                        // leaving a half-written one for the next batch.
                        self.streams.remove(&endpoint);
                        let status = Status::unavailable("TSO request stream is closed");
                        if forwarded {
                            forwarding.record_error(&stream_route, &status);
                        }
                        Err(status)
                    }
                }
            }
            None => {
                let stream = TsoStream::open(route, channel, request, forwarding).await?;
                self.streams.insert(endpoint, stream);
                Ok(())
            }
        }
    }

    /// Collects the next response from the route's retained stream.
    pub async fn recv(&mut self, endpoint: &str) -> Option<Result<pdpb::TsoResponse, Status>> {
        match self.streams.get_mut(endpoint) {
            Some(stream) => Some(stream.recv().await),
            None => None,
        }
    }

    pub async fn request(
        &mut self,
        route: TsoRoute,
        channel: Channel,
        request: pdpb::TsoRequest,
        forwarding: &TsoForwarding,
    ) -> Result<pdpb::TsoResponse, Status> {
        let endpoint = route.endpoint.clone();
        let retained = self
            .streams
            .remove(&endpoint)
            .filter(|stream| stream.route == route);
        let (stream, response) = match retained {
            Some(mut stream) => {
                let response = stream.request(request).await?;
                (stream, response)
            }
            None => TsoStream::open_and_request(route, channel, request, forwarding).await?,
        };
        self.streams.insert(endpoint, stream);
        Ok(response)
    }
}

/// Shared Go tryConnectToTSO feedback. Local cancellation never enters this owner.
#[derive(Clone, Debug, Default)]
pub struct TsoForwarding {
    state: Arc<Mutex<TsoForwardingState>>,
}

#[derive(Debug, Default)]
struct TsoForwardingState {
    primary: Option<TsoRoute>,
    enabled: bool,
    attempts: usize,
    network_errors: usize,
    fallback: Option<TsoRoute>,
}

impl TsoForwarding {
    fn configure(&self, primary: &TsoRoute, enabled: bool) {
        let mut state = self.state.lock().expect("TSO forwarding poisoned");
        if state.primary.as_ref() != Some(primary) || state.enabled != enabled {
            *state = TsoForwardingState {
                primary: Some(primary.clone()),
                enabled,
                ..Default::default()
            };
        }
    }

    /// Record a failed stream construction, excluding caller/route cancellation.
    pub fn record_error(&self, route: &TsoRoute, status: &Status) {
        let mut state = self.state.lock().expect("TSO forwarding poisoned");
        if !state.enabled {
            return;
        }
        if state.fallback.as_ref() == Some(route) {
            state.fallback = None;
            state.attempts = 0;
            state.network_errors = 0;
            return;
        }
        if state.primary.as_ref() != Some(route) {
            return;
        }
        state.attempts = (state.attempts + 1).min(6);
        if matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
        ) {
            state.network_errors = (state.network_errors + 1).min(6);
        }
        // Go considers six attempts as one window; a mixed window cannot forward.
        if state.attempts == 6 && state.network_errors != 6 {
            state.attempts = 0;
            state.network_errors = 0;
        }
    }

    /// A successfully served primary ends its previous construction-failure window.
    pub fn record_success(&self, route: &TsoRoute) {
        let mut state = self.state.lock().expect("TSO forwarding poisoned");
        if state.primary.as_ref() == Some(route) {
            state.attempts = 0;
            state.network_errors = 0;
            state.fallback = None;
        }
    }
}

/// Clone before refreshing, then publish only after discovery and dialing have
/// succeeded. Failed RPCs cannot replace the accepted route. Fresh group
/// revisions are shared across snapshots even if primary discovery then fails,
/// as Go keyspaceGroupSvcDiscovery.update publishes primaryless metadata.
#[derive(Clone, Debug)]
pub struct TsoDiscovery {
    keyspace_id: u32,
    assigned_group: bool,
    revision: Arc<AtomicU64>,
    service_urls: Vec<String>,
    cursor: Arc<Mutex<DiscoveryCursor>>,
    forwarding: TsoForwarding,
    accepted_info: Option<pdpb::GetClusterInfoResponse>,
    service_mode: ServiceModeDiscovery,
}

#[derive(Debug, Default)]
struct DiscoveryCursor {
    urls: Vec<String>,
    next: usize,
}

impl Default for TsoDiscovery {
    fn default() -> Self {
        Self {
            keyspace_id: NULL_KEYSPACE_ID,
            assigned_group: false,
            revision: Arc::default(),
            service_urls: Vec::new(),
            cursor: Arc::new(Mutex::new(DiscoveryCursor::default())),
            forwarding: TsoForwarding::default(),
            accepted_info: None,
            service_mode: ServiceModeDiscovery::default(),
        }
    }
}

impl TsoDiscovery {
    /// Shared stream-construction feedback for this discovery lifetime.
    pub fn forwarding(&self) -> TsoForwarding {
        self.forwarding.clone()
    }

    /// Called after LoadKeyspace and before publishing an API-v2 client.
    pub fn set_keyspace(&mut self, meta: &keyspacepb::KeyspaceMeta) -> Result<(), Status> {
        let id = match meta.keyspace {
            Some(keyspacepb::keyspace_meta::Keyspace::Id(id)) => id,
            // Go KeyspaceMeta.GetId returns zero when the oneof is absent.
            None => 0,
            _ => {
                return Err(Status::invalid_argument(
                    "TSO discovery requires a numeric V2 keyspace",
                ))
            }
        };
        if self.keyspace_id != id {
            self.revision = Arc::default();
        }
        self.keyspace_id = id;
        self.assigned_group = meta
            .config
            .get("tso_keyspace_group_id")
            .and_then(|id| id.parse::<u32>().ok())
            .is_some_and(|id| id != 0);
        Ok(())
    }

    pub async fn discover<F, Fut>(
        &mut self,
        cluster_id: u64,
        leader: &str,
        use_pd_proxy: bool,
        timeout: Duration,
        channels: &ChannelCache,
        dial: F,
    ) -> Result<(TsoRoute, Channel), Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        tokio::time::timeout(timeout, async {
            if let Err(error) = self.service_mode.refresh(leader, timeout, &dial).await {
                if self.service_mode.snapshot().is_none() {
                    return Err(error);
                }
            }
            self.discover_inner(cluster_id, leader, use_pd_proxy, timeout, channels, dial)
                .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("TSO discovery timed out"))?
    }

    /// The PD root owns mode polling; TSO refresh consumes its last accepted facts.
    pub fn service_mode(&self) -> ServiceModeDiscovery {
        self.service_mode.clone()
    }

    /// Whether this candidate uses the current accepted mode observation.
    pub fn mode_is_current(&self) -> bool {
        self.accepted_info == self.service_mode.snapshot()
    }

    /// Refresh group routing without issuing a service-mode RPC.
    pub async fn discover_cached<F, Fut>(
        &mut self,
        cluster_id: u64,
        leader: &str,
        use_pd_proxy: bool,
        timeout: Duration,
        channels: &ChannelCache,
        dial: F,
    ) -> Result<(TsoRoute, Channel), Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        tokio::time::timeout(
            timeout,
            self.discover_inner(cluster_id, leader, use_pd_proxy, timeout, channels, dial),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("TSO discovery timed out"))?
    }

    async fn discover_inner<F, Fut>(
        &mut self,
        cluster_id: u64,
        leader: &str,
        use_pd_proxy: bool,
        timeout: Duration,
        channels: &ChannelCache,
        dial: F,
    ) -> Result<(TsoRoute, Channel), Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        let channel = dial(leader.to_owned()).await?;
        let info = self
            .service_mode
            .snapshot()
            .ok_or_else(|| Status::unavailable("PD service mode has not been observed"))?;
        self.accepted_info = Some(info.clone());
        if info.service_modes[0] == pdpb::ServiceMode::PdSvcMode as i32 || use_pd_proxy {
            return Ok((self.classic(leader), channel));
        }
        let (group, revision) = if info.tso_urls.is_empty() {
            if self.assigned_group {
                return Err(Status::unavailable(
                    "no TSO microservice available in microservice mode",
                ));
            }
            // The Go compatibility path reads the default primary through PD's
            // MetaStorage service; never redirect API-mode Tso to PD itself.
            let response = meta_storagepb::meta_storage_client::MetaStorageClient::new(channel)
                .get(request(
                    meta_storagepb::GetRequest {
                        header: Some(meta_storagepb::RequestHeader {
                            cluster_id,
                            ..Default::default()
                        }),
                        key: format!("/ms/{cluster_id}/tso/00000/primary").into_bytes(),
                        ..Default::default()
                    },
                    timeout,
                ))
                .await?
                .into_inner();
            if let Some(error) = response.header.and_then(|header| header.error) {
                return Err(Status::unknown(error.message));
            }
            if response.kvs.is_empty() || response.count > 1 {
                return Err(Status::unavailable("expected one legacy TSO primary"));
            }
            let participant = tsopb::Participant::decode(response.kvs[0].value.as_slice())
                .map_err(|error| Status::data_loss(error.to_string()))?;
            if participant.listen_urls.is_empty() {
                return Err(Status::unavailable("legacy TSO primary has no listen URLs"));
            }
            (
                tsopb::KeyspaceGroup {
                    members: participant
                        .listen_urls
                        .into_iter()
                        .enumerate()
                        .map(|(i, address)| tsopb::KeyspaceGroupMember {
                            address,
                            is_primary: i == 0,
                        })
                        .collect(),
                    ..Default::default()
                },
                0,
            )
        } else {
            let mut urls = info.tso_urls;
            urls.sort();
            // Attempts are not published routing observations. Share the
            // selection cursor across snapshots so an error or cancellation
            // advances the next probe without accepting failed group metadata.
            let url = {
                let mut cursor = self.cursor.lock().expect("TSO discovery cursor poisoned");
                if cursor.urls != urls {
                    cursor.urls = urls;
                    cursor.next = 0;
                }
                let url = cursor.urls[cursor.next % cursor.urls.len()].clone();
                cursor.next = (cursor.next + 1) % cursor.urls.len();
                url
            };
            let (mut group, mut revision) = self
                .find_group(cluster_id, &url, timeout, channels, &dial)
                .await?;
            // A discovery server outside the group can return only secondaries.
            if !group
                .members
                .iter()
                .any(|member| member.is_primary && !member.address.is_empty())
            {
                // The metadata revision is accepted before following a group
                // member, even if that follow-up fails. Older observations must
                // not resurrect a stale primary on the next refresh either.
                self.revision.fetch_max(revision, Ordering::SeqCst);
                use rand::seq::IteratorRandom;
                let secondary = group
                    .members
                    .iter()
                    .filter(|member| !member.is_primary)
                    .choose(&mut rand::thread_rng())
                    .ok_or_else(|| Status::unavailable("no TSO group member"))?;
                (group, revision) = self
                    .find_group(cluster_id, &secondary.address, timeout, channels, &dial)
                    .await?;
            }
            (group, revision)
        };
        if revision < self.revision.load(Ordering::SeqCst) {
            return Err(Status::failed_precondition(
                "stale TSO keyspace group revision",
            ));
        }
        let primary = group
            .members
            .iter()
            .rev()
            .find(|member| member.is_primary && !member.address.is_empty())
            .ok_or_else(|| Status::unavailable("no TSO group primary"))?;
        let route = TsoRoute {
            endpoint: primary.address.clone(),
            keyspace_id: self.keyspace_id,
            group_id: Some(group.id),
            forwarded_host: None,
        };
        let channel = dial(route.endpoint.clone()).await?;
        self.service_urls = group.members.iter().map(|m| m.address.clone()).collect();
        self.revision.fetch_max(revision, Ordering::SeqCst);
        Ok((route, channel))
    }

    fn classic(&mut self, leader: &str) -> TsoRoute {
        // A subsequent API-mode entry creates a new group discovery lifecycle.
        self.revision = Arc::default();
        self.service_urls.clear();
        *self.cursor.lock().expect("TSO discovery cursor poisoned") = DiscoveryCursor::default();
        TsoRoute {
            endpoint: leader.to_owned(),
            keyspace_id: self.keyspace_id,
            group_id: None,
            forwarded_host: None,
        }
    }

    /// Go tryConnectToTSOWithProxy admits every healthy service endpoint.
    /// PD members and keyspace-group members are separate service topologies.
    /// A failed probe excludes only that endpoint; direct mode does not probe.
    pub async fn stream_routes<F, Fut>(
        &self,
        primary: &TsoRoute,
        pd_urls: &[String],
        proxy: bool,
        enable_forwarding: bool,
        timeout: Duration,
        dial: F,
    ) -> Result<Vec<(TsoRoute, Channel)>, Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        let urls = if primary.group_id.is_some() {
            &self.service_urls
        } else {
            pd_urls
        };
        self.forwarding
            .configure(primary, enable_forwarding && !proxy);
        if !proxy {
            let channel = dial(primary.endpoint.clone()).await?;
            let (fallback, eligible) = {
                let state = self
                    .forwarding
                    .state
                    .lock()
                    .expect("TSO forwarding poisoned");
                (
                    state.fallback.clone(),
                    state.enabled && state.network_errors >= 6,
                )
            };
            if let Some(fallback) = fallback {
                if !urls.contains(&fallback.endpoint) {
                    // Accepted membership removed this backup. Never retain its stream.
                    self.forwarding.record_success(primary);
                } else if health_check(channel.clone(), timeout).await.ok() == Some(1) {
                    // Go checkLeader restores the primary only after SERVING.
                    self.forwarding.record_success(primary);
                } else {
                    return Ok(vec![(fallback.clone(), dial(fallback.endpoint).await?)]);
                }
            } else if eligible {
                use rand::seq::SliceRandom;
                let mut backups = urls
                    .iter()
                    .filter(|url| *url != &primary.endpoint)
                    .collect::<Vec<_>>();
                backups.shuffle(&mut rand::thread_rng());
                for endpoint in backups {
                    let result = tokio::time::timeout(timeout, async {
                        let backup = dial(endpoint.clone()).await?;
                        if health_check(backup.clone(), timeout).await? != 1 {
                            return Err(Status::unavailable("TSO backup is not serving"));
                        }
                        Ok(backup)
                    })
                    .await;
                    if let Ok(Ok(backup)) = result {
                        let mut route = primary.clone();
                        route.endpoint = endpoint.clone();
                        route.forwarded_host = Some(primary.endpoint.clone());
                        let mut state = self
                            .forwarding
                            .state
                            .lock()
                            .expect("TSO forwarding poisoned");
                        if state.enabled
                            && state.primary.as_ref() == Some(primary)
                            && state.network_errors >= 6
                        {
                            state.fallback = Some(route.clone());
                            return Ok(vec![(route, backup)]);
                        }
                    }
                }
            }
            return Ok(vec![(primary.clone(), channel)]);
        }
        let mut routes = Vec::new();
        for endpoint in urls {
            if endpoint.is_empty()
                || routes
                    .iter()
                    .any(|(r, _): &(TsoRoute, Channel)| &r.endpoint == endpoint)
            {
                continue;
            }
            let result = tokio::time::timeout(timeout, async {
                let channel = dial(endpoint.clone()).await?;
                if health_check(channel.clone(), timeout).await? != 1 {
                    return Err(Status::unavailable("TSO endpoint is not serving"));
                }
                Ok(channel)
            })
            .await;
            if let Ok(Ok(channel)) = result {
                let mut route = primary.clone();
                route.endpoint = endpoint.clone();
                route.forwarded_host =
                    (endpoint != &primary.endpoint).then(|| primary.endpoint.clone());
                routes.push((route, channel));
            }
        }
        Ok(routes)
    }

    async fn find_group<F, Fut>(
        &self,
        cluster_id: u64,
        url: &str,
        timeout: Duration,
        channels: &ChannelCache,
        dial: &F,
    ) -> Result<(tsopb::KeyspaceGroup, u64), Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        let response = tsopb::tso_client::TsoClient::new(dial(url.to_owned()).await?)
            .find_group_by_keyspace_id(request(
                tsopb::FindGroupByKeyspaceIdRequest {
                    header: Some(tsopb::RequestHeader {
                        cluster_id,
                        keyspace: Some(tsopb::request_header::Keyspace::KeyspaceId(
                            self.keyspace_id,
                        )),
                        callee_id: callee_id(url),
                        ..Default::default()
                    }),
                    keyspace: Some(
                        tsopb::find_group_by_keyspace_id_request::Keyspace::KeyspaceId(
                            self.keyspace_id,
                        ),
                    ),
                    mod_revision: self.revision.load(Ordering::SeqCst),
                },
                timeout,
            ))
            .await?
            .into_inner();
        if let Some(error) = response.header.and_then(|header| header.error) {
            if error.message.contains(super::errs::MISMATCH_CALLEE_ID_ERR) {
                channels.remove(url);
            }
            return Err(Status::unknown(error.message));
        }
        if response.mod_revision < self.revision.load(Ordering::SeqCst) {
            return Err(Status::failed_precondition(
                "stale TSO keyspace group revision",
            ));
        }
        Ok((
            response
                .keyspace_group
                .ok_or_else(|| Status::not_found("no TSO keyspace group"))?,
            response.mod_revision,
        ))
    }
}

/// The Go connection manager selects uniformly among admitted streams.
/// Keeping selection here avoids adding a second routing policy to adapters.
pub fn pick_stream_route(routes: &[TsoRoute]) -> Option<&TsoRoute> {
    use rand::seq::SliceRandom;
    routes.choose(&mut rand::thread_rng())
}

fn request<T>(message: T, timeout: Duration) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(timeout);
    request
}

fn callee_id(endpoint: &str) -> String {
    // Preserve explicitly advertised default ports and IPv6 brackets.
    let uri = if endpoint.contains("://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    uri.parse::<tonic::codegen::http::Uri>()
        .ok()
        .and_then(|uri| {
            uri.authority()
                .map(|authority| authority.as_str().to_owned())
        })
        .unwrap_or_default()
}

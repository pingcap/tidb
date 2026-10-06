// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared PD service-mode and keyspace-group discovery. Metadata continues to
//! use the PD leader; timestamp routing follows the separately discovered owner.

use crate::proto::{keyspacepb, meta_storagepb, pdpb, tsopb};
use futures::{Stream, StreamExt};
use prost::Message;
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
}

impl TsoStream {
    pub async fn open_and_request(
        route: TsoRoute,
        channel: Channel,
        first: pdpb::TsoRequest,
    ) -> Result<(Self, pdpb::TsoResponse), Status> {
        let (requests, receiver) = tokio::sync::mpsc::channel(1);
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
            .await?;
        let response = responses
            .next()
            .await
            .ok_or_else(|| Status::unavailable("TSO response stream is closed"))??;
        Ok((
            Self {
                route,
                requests,
                responses,
            },
            response,
        ))
    }

    pub async fn request(
        &mut self,
        request: pdpb::TsoRequest,
    ) -> Result<pdpb::TsoResponse, Status> {
        self.requests
            .send(request)
            .await
            .map_err(|_| Status::unavailable("TSO request stream is closed"))?;
        self.responses
            .next()
            .await
            .ok_or_else(|| Status::unavailable("TSO response stream is closed"))?
    }
}

/// Clone before refreshing, then publish only after discovery and dialing have
/// succeeded. Failed RPCs must not overwrite the last accepted revision/route.
#[derive(Clone, Debug)]
pub struct TsoDiscovery {
    keyspace_id: u32,
    assigned_group: bool,
    revision: u64,
    service_urls: Vec<String>,
    cursor: Arc<Mutex<DiscoveryCursor>>,
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
            revision: 0,
            service_urls: Vec::new(),
            cursor: Arc::new(Mutex::new(DiscoveryCursor::default())),
        }
    }
}

impl TsoDiscovery {
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
            self.revision = 0;
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
        dial: F,
    ) -> Result<(TsoRoute, Channel), Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        // Bound dialing as well as all discovery requests with the caller's budget.
        tokio::time::timeout(
            timeout,
            self.discover_inner(cluster_id, leader, use_pd_proxy, timeout, dial),
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
        dial: F,
    ) -> Result<(TsoRoute, Channel), Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        let channel = dial(leader.to_owned()).await?;
        let mut client = pdpb::pd_client::PdClient::new(channel.clone());
        let info = match client
            .get_cluster_info(request(pdpb::GetClusterInfoRequest::default(), timeout))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(error) if error.code() == tonic::Code::Unimplemented => {
                return Ok((self.classic(leader), channel));
            }
            Err(error) => return Err(error),
        };
        if let Some(error) = info
            .header
            .as_ref()
            .and_then(|header| header.error.as_ref())
        {
            return Err(Status::unknown(error.message.clone()));
        }
        match info
            .service_modes
            .first()
            .copied()
            .and_then(|mode| pdpb::ServiceMode::try_from(mode).ok())
        {
            Some(pdpb::ServiceMode::PdSvcMode) => return Ok((self.classic(leader), channel)),
            Some(pdpb::ServiceMode::ApiSvcMode) if use_pd_proxy => {
                return Ok((self.classic(leader), channel))
            }
            Some(pdpb::ServiceMode::ApiSvcMode) => {}
            _ => return Err(Status::unknown("no supported service mode returned")),
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
            let (mut group, mut revision) =
                self.find_group(cluster_id, &url, timeout, &dial).await?;
            // A discovery server outside the group can return only secondaries.
            if !group
                .members
                .iter()
                .any(|member| member.is_primary && !member.address.is_empty())
            {
                let secondary = group
                    .members
                    .first()
                    .ok_or_else(|| Status::unavailable("no TSO group member"))?;
                (group, revision) = self
                    .find_group(cluster_id, &secondary.address, timeout, &dial)
                    .await?;
            }
            (group, revision)
        };
        if revision < self.revision {
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
        self.revision = revision;
        Ok((route, channel))
    }

    fn classic(&mut self, leader: &str) -> TsoRoute {
        // A subsequent API-mode entry creates a new group discovery lifecycle.
        self.revision = 0;
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
        timeout: Duration,
        dial: F,
    ) -> Result<Vec<(TsoRoute, Channel)>, Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        if !proxy {
            return Ok(vec![(
                primary.clone(),
                dial(primary.endpoint.clone()).await?,
            )]);
        }
        let urls = if primary.group_id.is_some() {
            &self.service_urls
        } else {
            pd_urls
        };
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
                    mod_revision: self.revision,
                },
                timeout,
            ))
            .await?
            .into_inner();
        if let Some(error) = response.header.and_then(|header| header.error) {
            return Err(Status::unknown(error.message));
        }
        if response.mod_revision < self.revision {
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

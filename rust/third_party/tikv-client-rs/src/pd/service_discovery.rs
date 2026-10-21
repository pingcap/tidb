// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared PD service-mode and keyspace-group discovery. Metadata continues to
//! use the PD leader; timestamp routing follows the separately discovered owner.

pub use super::service_mode::ServiceModeDiscovery;
use crate::proto::{keyspacepb, meta_storagepb, pdpb, tsopb};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{future::Future, time::Duration};
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

/// Published timestamp destination, independent of the PD metadata leader:
/// the TSO client's serving URL, keyspace and keyspace group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsoRoute {
    pub endpoint: String,
    pub keyspace_id: u32,
    pub group_id: Option<u32>,
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
    accepted_info: Option<pdpb::GetClusterInfoResponse>,
    service_mode: ServiceModeDiscovery,
    /// Go `GetKeyspaceGroupID()`: the accepted group (0 until one is found).
    group_id: u32,
    /// Go `keyspaceGroupSD.primaryURL`.
    primary_url: String,
    /// Go `printFallbackLogOnce`.
    fallback_logged: Arc<std::sync::atomic::AtomicBool>,
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
            accepted_info: None,
            service_mode: ServiceModeDiscovery::default(),
            group_id: 0,
            primary_url: String::new(),
            fallback_logged: Arc::default(),
        }
    }
}

impl TsoDiscovery {
    /// The keyspace group's member URLs (Go `tsoServiceDiscovery.GetServiceURLs`).
    pub fn service_urls(&self) -> &[String] {
        &self.service_urls
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
            if !self
                .fallback_logged
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                log::warn!(
                    "keyspace-id-in-request" = self.keyspace_id,
                    "tso-server-url" = "",
                    "discovery-key":% = format!("/ms/{cluster_id}/tso/00000/primary");
                    "[tso] no tso server URL found, fallback to the legacy path to discover from etcd directly"
                );
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
                    log::info!("sorted-urls":serde = urls; "update tso server URLs");
                    cursor.urls = urls;
                    cursor.next = 0;
                }
                let url = cursor.urls[cursor.next % cursor.urls.len()].clone();
                cursor.next = (cursor.next + 1) % cursor.urls.len();
                url
            };
            let (mut group, mut revision) = self
                .find_group_logged(cluster_id, &url, timeout, channels, &dial)
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
                    .find_group_logged(cluster_id, &secondary.address, timeout, channels, &dial)
                    .await?;
            }
            (group, revision)
        };
        if group.id != self.group_id {
            log::info!(
                "keyspace-id" = self.keyspace_id,
                "new-keyspace-group-id" = group.id,
                "old-keyspace-group-id" = self.group_id;
                "[tso] the keyspace group changed"
            );
        }
        if revision < self.revision.load(Ordering::SeqCst) {
            log::warn!(
                modRevision = revision;
                "[tso] failed to update keyspace group, met stale keyspace group info"
            );
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
        };
        let channel = match dial(route.endpoint.clone()).await {
            Ok(channel) => channel,
            Err(error) => {
                log::warn!(
                    "keyspace-id-in-request" = self.keyspace_id,
                    "next-primary" = route.endpoint.as_str(),
                    error:% = error;
                    "[tso] failed to connect the next primary"
                );
                return Err(error);
            }
        };
        self.service_urls = group.members.iter().map(|m| m.address.clone()).collect();
        self.revision.fetch_max(revision, Ordering::SeqCst);
        if !route.endpoint.eq_ignore_ascii_case(&self.primary_url) {
            log::info!(
                "keyspace-id-in-request" = self.keyspace_id,
                "keyspace-group-service":% = crate::error::protobuf_text(&group);
                "[tso] updated keyspace group service discovery info"
            );
            log::info!(
                "new-primary" = route.endpoint.as_str(),
                "old-primary" = self.primary_url.as_str();
                "[tso] switch primary"
            );
            self.primary_url.clone_from(&route.endpoint);
        }
        self.group_id = group.id;
        Ok((route, channel))
    }

    fn classic(&mut self, leader: &str) -> TsoRoute {
        // A subsequent API-mode entry creates a new group discovery lifecycle.
        self.revision = Arc::default();
        self.service_urls.clear();
        self.group_id = 0;
        self.primary_url.clear();
        *self.cursor.lock().expect("TSO discovery cursor poisoned") = DiscoveryCursor::default();
        TsoRoute {
            endpoint: leader.to_owned(),
            keyspace_id: self.keyspace_id,
            group_id: None,
        }
    }

    /// [`Self::find_group`] with pd-client's failure line.
    async fn find_group_logged<F, Fut>(
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
        let result = self
            .find_group(cluster_id, url, timeout, channels, dial)
            .await;
        if let Err(error) = &result {
            log::error!(
                "keyspace-id-in-request" = self.keyspace_id,
                "tso-server-url" = url,
                "response-mod-revision" = 0_u64,
                "current-mod-revision" = self.revision.load(Ordering::SeqCst),
                error:% = error;
                "[tso] failed to find the keyspace group"
            );
        }
        result
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

fn request<T>(message: T, timeout: Duration) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(timeout);
    request
}

pub(crate) fn callee_id(endpoint: &str) -> String {
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

// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Shared PD service-mode and keyspace-group discovery. Metadata continues to
//! use the PD leader; timestamp routing follows the separately discovered owner.

pub use super::service_mode::ServiceModeDiscovery;
use crate::proto::{keyspacepb, pdpb};
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

    /// The cached connection of `endpoint`, if one exists (a Go
    /// `clientConns.Load`).
    pub fn get(&self, endpoint: &str) -> Option<Channel> {
        self.channels
            .lock()
            .expect("PD channel cache poisoned")
            .as_ref()
            .and_then(|channels| channels.get(endpoint).cloned())
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

/// Published timestamp destination in PD mode, independent of the PD
/// metadata leader: the TSO client's serving URL and keyspace. (In
/// microservice mode the TSO service discovery owns the destination.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TsoRoute {
    pub endpoint: String,
    pub keyspace_id: u32,
}

/// The facts the TSO provider decision rests on: the keyspace (Go
/// `keyspaceID` / `keyspaceMeta`) and the accepted service mode (Go
/// `serviceMode`, observed by PD's discovery). The keyspace group itself is
/// the TSO service discovery's.
#[derive(Clone, Debug)]
pub struct TsoDiscovery {
    keyspace_id: u32,
    keyspace_meta: Option<keyspacepb::KeyspaceMeta>,
    service_mode: ServiceModeDiscovery,
}

impl Default for TsoDiscovery {
    fn default() -> Self {
        Self {
            keyspace_id: NULL_KEYSPACE_ID,
            keyspace_meta: None,
            service_mode: ServiceModeDiscovery::default(),
        }
    }
}

impl TsoDiscovery {
    /// Go `GetKeyspaceID()`.
    pub fn keyspace_id(&self) -> u32 {
        self.keyspace_id
    }

    /// Go `keyspaceMeta`.
    pub fn keyspace_meta(&self) -> Option<&keyspacepb::KeyspaceMeta> {
        self.keyspace_meta.as_ref()
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
        self.keyspace_id = id;
        self.keyspace_meta = Some(meta.clone());
        Ok(())
    }

    /// The PD root owns mode polling; the TSO provider decision consumes its
    /// last accepted facts.
    pub fn service_mode(&self) -> ServiceModeDiscovery {
        self.service_mode.clone()
    }

    /// Observes the service mode on `leader`; an earlier accepted
    /// observation survives a failed one.
    pub async fn refresh_mode<F, Fut>(
        &self,
        leader: &str,
        timeout: Duration,
        dial: F,
    ) -> Result<(), Status>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        if let Err(error) = self.service_mode.refresh(leader, timeout, &dial).await {
            if self.service_mode.snapshot().is_none() {
                return Err(error);
            }
        }
        Ok(())
    }

    /// The accepted service mode (Go `serviceMode`).
    pub fn accepted_mode(&self) -> Result<pdpb::ServiceMode, Status> {
        let info = self
            .service_mode
            .snapshot()
            .ok_or_else(|| Status::unavailable("PD service mode has not been observed"))?;
        info.service_modes
            .first()
            .copied()
            .and_then(|mode| pdpb::ServiceMode::try_from(mode).ok())
            .ok_or_else(|| Status::unavailable("PD service mode has not been observed"))
    }
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

/// Go `LeaderSwitchedCallbackFunc`.
pub(crate) type LeaderSwitchedCallback = Arc<dyn Fn(&str) -> crate::Result<()> + Send + Sync>;
/// A members-changed callback (Go `func()`).
pub(crate) type MembersChangedCallback = Arc<dyn Fn() + Send + Sync>;

/// Go `serviceCallbacks` (callbacks.go): the callbacks a service discovery
/// runs after its leader/primary switches and after its membership changes.
#[derive(Default)]
pub(crate) struct ServiceCallbacks {
    leader_switched_cbs: std::sync::RwLock<Vec<LeaderSwitchedCallback>>,
    members_changed_cbs: std::sync::RwLock<Vec<MembersChangedCallback>>,
}

impl ServiceCallbacks {
    /// Go `addLeaderSwitchedCallback`.
    pub(crate) fn add_leader_switched_callback(&self, callback: LeaderSwitchedCallback) {
        self.leader_switched_cbs.write().unwrap().push(callback);
    }

    /// Go `addMembersChangedCallback`.
    pub(crate) fn add_members_changed_callback(&self, callback: MembersChangedCallback) {
        self.members_changed_cbs.write().unwrap().push(callback);
    }

    /// Go `onLeaderSwitched`: the callbacks in order, stopping at the first
    /// error.
    pub(crate) fn on_leader_switched(&self, leader: &str) -> crate::Result<()> {
        let callbacks = self.leader_switched_cbs.read().unwrap().clone();
        for callback in callbacks {
            callback(leader)?;
        }
        Ok(())
    }

    /// Go `onMembersChanged`.
    pub(crate) fn on_members_changed(&self) {
        let callbacks = self.members_changed_cbs.read().unwrap().clone();
        for callback in callbacks {
            callback();
        }
    }
}

/// The PD members as pd-client's `serviceDiscovery` holds them: the leader's
/// URL (Go `getLeaderURL`), the URLs the client keeps connections to (the
/// leader and followers, `PickMatchedURL` of each member), the sorted client
/// URLs of every member (Go `urls`), and the callbacks run when they change.
#[derive(Default)]
pub(crate) struct PdMembership {
    state: std::sync::RwLock<PdMembershipState>,
    callbacks: ServiceCallbacks,
}

#[derive(Default)]
struct PdMembershipState {
    leader_url: String,
    member_urls: Vec<String>,
    sorted_urls: Vec<String>,
}

/// What [`PdMembership::update`] changed.
pub(crate) struct PdMembershipChange {
    pub(crate) old_sorted_urls: Vec<String>,
    pub(crate) urls_changed: bool,
    pub(crate) old_leader_url: String,
    pub(crate) leader_changed: bool,
}

impl PdMembership {
    /// Go `getLeaderURL` / `GetServingURL`.
    pub(crate) fn leader_url(&self) -> String {
        self.state.read().unwrap().leader_url.clone()
    }

    /// The leader's and followers' URLs (the keys of Go's `clientConns` that
    /// belong to members).
    pub(crate) fn member_urls(&self) -> Vec<String> {
        self.state.read().unwrap().member_urls.clone()
    }

    /// Go `GetServiceURLs`.
    pub(crate) fn sorted_urls(&self) -> Vec<String> {
        self.state.read().unwrap().sorted_urls.clone()
    }

    pub(crate) fn callbacks(&self) -> &ServiceCallbacks {
        &self.callbacks
    }

    /// Publishes a membership observation (Go `updateURLs` and
    /// `switchLeader`'s stores); the caller runs the callbacks and logs.
    pub(crate) fn update(
        &self,
        leader_url: String,
        member_urls: Vec<String>,
        sorted_urls: Vec<String>,
    ) -> PdMembershipChange {
        let mut state = self.state.write().unwrap();
        let urls_changed = state.sorted_urls != sorted_urls;
        let old_sorted_urls = std::mem::replace(&mut state.sorted_urls, sorted_urls);
        let leader_changed = state.leader_url != leader_url;
        let old_leader_url = std::mem::replace(&mut state.leader_url, leader_url);
        state.member_urls = member_urls;
        PdMembershipChange {
            old_sorted_urls,
            urls_changed,
            old_leader_url,
            leader_changed,
        }
    }
}

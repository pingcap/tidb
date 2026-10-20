// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The part of PD's `servicediscovery.ServiceDiscovery` and `ServiceClient`
//! that the HTTP client reads. Implementations: client-rust's PD client
//! (`RetryClient::get_service_discovery`, `PdRpcClient::get_service_discovery`),
//! the client-owned discovery of [`new_client`](super::new_client), and the
//! test-only mock discovery.

use async_trait::async_trait;

/// Go `servicediscovery.ServiceClient`, reduced to what the HTTP client reads:
/// the member's matched client URL and whether it is the leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceClient {
    url: String,
    is_leader: bool,
}

impl ServiceClient {
    pub fn new(url: impl Into<String>, is_leader: bool) -> ServiceClient {
        ServiceClient {
            url: url.into(),
            is_leader,
        }
    }

    /// Go `GetURL`: the URL, with scheme, the client connects to.
    pub fn get_url(&self) -> &str {
        &self.url
    }

    /// Go `IsConnectedToLeader`.
    pub fn is_connected_to_leader(&self) -> bool {
        self.is_leader
    }
}

/// Go `servicediscovery.ServiceDiscovery`, reduced to the methods the PD HTTP
/// client calls.
#[async_trait]
pub trait ServiceDiscovery: Send + Sync {
    /// Go `GetAllServiceClients`: the leader's client first, when there is a
    /// leader, then every follower's.
    async fn get_all_service_clients(&self) -> Vec<ServiceClient>;

    /// Go `ScheduleCheckMemberChanged`: ask for an asynchronous membership
    /// check without waiting for it; repeated requests coalesce.
    fn schedule_check_member_changed(&self);

    /// Go `Close`. The HTTP client only closes a discovery it created itself.
    async fn close(&self);
}

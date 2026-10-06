// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Region API selection from PD serviceDiscovery's universal service balancer.
//! Transport and health observations stay with the discovery owner. A request's
//! follower permission is independent of the live process option.

use std::sync::Mutex;
use tonic::Request;

#[derive(Debug, Default)]
pub struct RegionService {
    state: Mutex<Members>,
}

#[derive(Debug, Default)]
struct Members {
    leader: String,
    urls: Vec<String>,
    next: usize,
}

/// Immutable per-attempt destination and metadata policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionTarget {
    pub endpoint: String,
    pub follower: bool,
}

impl RegionTarget {
    /// Go BuildGRPCTargetContext leaves leader metadata alone and adds the
    /// empty follower-permission value when a follower serves the request.
    pub fn request<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        if self.follower {
            request
                .metadata_mut()
                .insert("pd-allow-follower-handle", "".parse().unwrap());
        }
        request
    }

    /// A follower's transport or PD-header error retries the leader once.
    /// A missing region without a header error is a successful RPC, not a
    /// service error; RegionCache owns that subsequent retry.
    pub fn needs_leader_retry(&self, rpc_error: bool, header_error: bool) -> bool {
        self.follower && (rpc_error || header_error)
    }
}

impl RegionService {
    /// Membership refresh preserves the rotation when the accepted topology
    /// is unchanged, matching updateServiceClient's no-change fast path.
    pub fn select(
        &self,
        leader: &str,
        urls: &[String],
        enabled: bool,
        allowed: bool,
    ) -> RegionTarget {
        let mut state = self.state.lock().expect("PD region service poisoned");
        let mut members: Vec<_> = urls
            .iter()
            .filter(|url| !url.is_empty() && url.as_str() != leader)
            .cloned()
            .collect();
        members.sort();
        members.dedup();
        members.insert(0, leader.to_owned());
        if state.leader != leader || state.urls != members {
            state.leader = leader.to_owned();
            state.urls = members;
            state.next = 0;
        }
        if !enabled || !allowed {
            return RegionTarget {
                endpoint: leader.to_owned(),
                follower: false,
            };
        }
        let endpoint = state.urls[state.next].clone();
        state.next = (state.next + 1) % state.urls.len();
        RegionTarget {
            follower: endpoint != leader,
            endpoint,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_batch_rotation_permissions_membership_and_retry_contract() {
        let service = RegionService::default();
        let urls = vec!["follower".to_owned(), "leader".to_owned()];
        assert!(!service.select("leader", &urls, true, true).follower);
        // Bypassing followers must not consume a balance slot.
        assert!(!service.select("leader", &urls, true, false).follower);
        assert!(!service.select("leader", &urls, false, true).follower);
        let follower = service.select("leader", &urls, true, true);
        assert_eq!(follower.endpoint, "follower");
        assert_eq!(
            follower
                .request(())
                .metadata()
                .get("pd-allow-follower-handle")
                .unwrap(),
            ""
        );
        assert!(follower.needs_leader_retry(true, false));
        assert!(follower.needs_leader_retry(false, true));
        assert!(!follower.needs_leader_retry(false, false));
        let leader = service.select("leader", &urls, true, true);
        assert!(!leader
            .request(())
            .metadata()
            .contains_key("pd-allow-follower-handle"));
        assert!(!leader.needs_leader_retry(true, true));
        assert_eq!(service.select("new", &urls, true, true).endpoint, "new");
    }
}

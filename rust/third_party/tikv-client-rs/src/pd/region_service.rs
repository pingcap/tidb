// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD serviceDiscovery's region balancer, network health and API eligibility.
//! Request handles retain the selected member lifetime so late replies cannot
//! poison a removed member's replacement.

use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::time::Instant;
use tonic::{transport::Channel, Request, Status};

/// Go servicediscovery.MemberHealthCheckInterval.
pub const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const REGION_COOLDOWN: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
pub struct RegionService {
    state: Mutex<Members>,
}

#[derive(Debug, Default)]
struct Members {
    leader: String,
    urls: Vec<String>,
    candidates: Vec<Arc<Candidate>>,
    next: usize,
    forward_next: usize,
}

impl Members {
    fn forwarding_candidate(&mut self, enabled: bool) -> Option<Arc<Candidate>> {
        let leader = self.candidates.first()?.clone();
        if enabled && leader.member.network_failure.load(Ordering::Acquire) {
            let count = self.candidates.len() - 1;
            for _ in 0..count {
                let index = 1 + self.forward_next % count;
                self.forward_next = (self.forward_next + 1) % count;
                let candidate = &self.candidates[index];
                if !candidate.member.network_failure.load(Ordering::Acquire) {
                    return Some(candidate.clone());
                }
            }
        }
        Some(leader)
    }
}

#[derive(Debug)]
struct Member {
    endpoint: String,
    follower: bool,
    network_failure: AtomicBool,
}

#[derive(Debug)]
struct Candidate {
    member: Arc<Member>,
    // Unlike network health, this belongs to a particular API balancer.
    unavailable_until: Mutex<Option<Instant>>,
}

impl Candidate {
    fn available(&self) -> bool {
        !self.member.network_failure.load(Ordering::Acquire)
            && self
                .unavailable_until
                .lock()
                .expect("PD API state poisoned")
                .is_none()
    }

    fn check_cooldown(&self, now: Instant) {
        let mut until = self
            .unavailable_until
            .lock()
            .expect("PD API state poisoned");
        if until.is_some_and(|until| now > until) {
            *until = None;
        }
    }
}

/// Per-attempt logical destination. Channel caches retain physical connections;
/// this metadata must never be stored in a cached connection or mutable global.
#[derive(Clone, Debug, Default)]
pub struct Forwarding(pub Option<String>);

impl tonic::service::Interceptor for Forwarding {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(leader) = &self.0 {
            let value = leader
                .parse()
                .map_err(|_| Status::invalid_argument("invalid PD forwarding URL"))?;
            request.metadata_mut().remove("pd-allow-follower-handle");
            request.metadata_mut().insert("pd-forwarded-host", value);
        }
        Ok(request)
    }
}

/// Immutable per-attempt destination, retaining its API and member identities.
#[derive(Clone, Debug)]
pub struct RegionTarget {
    pub endpoint: String,
    pub follower: bool,
    pub forwarded_host: Option<String>,
    region_feedback: bool,
    candidate: Arc<Candidate>,
}

/// Go BuildFollowerHandleContext; request wrappers can use this without
/// reconstructing a selected service (and losing its feedback identity).
pub fn region_request<T>(value: T, follower: bool) -> Request<T> {
    let mut request = Request::new(value);
    if follower {
        request
            .metadata_mut()
            .insert("pd-allow-follower-handle", "".parse().unwrap());
    }
    request
}

impl RegionTarget {
    pub fn request<T>(&self, value: T) -> Request<T> {
        region_request(value, self.follower && self.forwarded_host.is_none())
    }

    /// Only the region-specific PD error suppresses a follower. Other header
    /// errors and RPC errors retry the leader without starting/resetting cooldown.
    pub fn observe_error(&self, rpc_error: bool, header_error: Option<i32>) -> bool {
        if self.region_feedback
            && self.follower
            && header_error == Some(crate::proto::pdpb::ErrorType::RegionNotFound as i32)
        {
            let mut until = self
                .candidate
                .unavailable_until
                .lock()
                .expect("PD API state poisoned");
            if until.is_none() {
                *until = Some(Instant::now() + REGION_COOLDOWN);
            }
        }
        self.needs_leader_retry(rpc_error, header_error.is_some())
    }

    pub fn needs_leader_retry(&self, rpc_error: bool, header_error: bool) -> bool {
        self.follower && (rpc_error || header_error)
    }
}

impl RegionService {
    /// Publish only accepted membership. Unchanged topology retains balance,
    /// network state and cooldown. Rebuilt API rings keep surviving connections'
    /// health, but own fresh API eligibility, matching Go updateServiceClient.
    pub fn update_members(&self, leader: &str, urls: &[String]) {
        let mut state = self.state.lock().expect("PD region service poisoned");
        let mut members: Vec<_> = urls
            .iter()
            .filter(|url| !url.is_empty() && url.as_str() != leader)
            .cloned()
            .collect();
        members.sort();
        members.dedup();
        members.insert(0, leader.to_owned());
        if state.leader == leader && state.urls == members {
            return;
        }
        let candidates = members
            .iter()
            .map(|endpoint| {
                let follower = endpoint != leader;
                let member = state
                    .candidates
                    .iter()
                    .find(|old| old.member.endpoint == *endpoint && old.member.follower == follower)
                    .map(|old| old.member.clone())
                    .unwrap_or_else(|| {
                        Arc::new(Member {
                            endpoint: endpoint.clone(),
                            follower,
                            network_failure: AtomicBool::new(false),
                        })
                    });
                Arc::new(Candidate {
                    member,
                    unavailable_until: Mutex::new(None),
                })
            })
            .collect();
        *state = Members {
            leader: leader.to_owned(),
            urls: members,
            candidates,
            next: 0,
            forward_next: 0,
        };
    }

    pub fn select(
        &self,
        leader: &str,
        urls: &[String],
        enabled: bool,
        allowed: bool,
    ) -> RegionTarget {
        self.select_with_forwarding(leader, urls, enabled, allowed, false)
    }

    /// Select the region API first, then Go's ordinary service fallback. If
    /// local follower handling was permitted, the fallback retains that mode;
    /// a retry after its error uses the ordinary leader-forwarding context.
    pub fn select_with_forwarding(
        &self,
        leader: &str,
        urls: &[String],
        enabled: bool,
        allowed: bool,
        forwarding: bool,
    ) -> RegionTarget {
        self.update_members(leader, urls);
        let mut state = self.state.lock().expect("PD region service poisoned");
        // GetServiceClient is the source fallback when the universal ring has no
        // available member. Region-only eligibility must not hide that leader.
        let mut candidate = None;
        if enabled && allowed {
            for _ in 0..state.candidates.len() {
                let selected = state.candidates[state.next].clone();
                state.next = (state.next + 1) % state.candidates.len();
                if selected.available() {
                    candidate = Some(selected);
                    break;
                }
            }
        }
        let region_feedback = candidate.is_some();
        let candidate = candidate.unwrap_or_else(|| {
            state
                .forwarding_candidate(forwarding)
                .expect("published PD leader")
        });
        RegionTarget {
            endpoint: candidate.member.endpoint.clone(),
            follower: candidate.member.follower,
            forwarded_host: (!region_feedback
                && candidate.member.follower
                && !(enabled && allowed))
                .then(|| state.leader.clone()),
            region_feedback,
            candidate,
        }
    }

    /// Ordinary unary calls use network health, independently of region-local
    /// cooldown and its cursor. A missing eligible follower retains the leader.
    pub fn forwarding_target(&self, endpoint: &str, enabled: bool) -> (String, Forwarding) {
        let mut state = self.state.lock().expect("PD region service poisoned");
        if endpoint == state.leader {
            if let Some(candidate) = state.forwarding_candidate(enabled) {
                let metadata = candidate.member.follower.then(|| state.leader.clone());
                return (candidate.member.endpoint.clone(), Forwarding(metadata));
            }
        }
        (endpoint.to_owned(), Forwarding::default())
    }

    /// One maintenance pass, without holding membership locks across I/O.
    /// The owner cancels this future on shutdown; no detached probes are spawned.
    /// A late observation writes only the retained original member, never a
    /// removed/re-added endpoint. Dialing and readiness share each probe budget.
    pub async fn check_health<F, Fut>(&self, timeout: Duration, channel: F)
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<Channel, Status>>,
    {
        let candidates = self
            .state
            .lock()
            .expect("PD region service poisoned")
            .candidates
            .clone();
        for candidate in &candidates {
            let budget = if candidate.member.follower {
                HEALTH_CHECK_INTERVAL / 3
            } else {
                timeout
            };
            let result = tokio::time::timeout(budget, async {
                let channel = channel(candidate.member.endpoint.clone()).await?;
                super::service_discovery::health_check(channel, budget).await
            })
            .await;
            candidate
                .member
                .network_failure
                .store(!matches!(result, Ok(Ok(1))), Ordering::Release);
        }
        // Go checks API cooldown after its follower health sweep, not on every
        // selection; repeated in-flight failures never extend the original time.
        let now = Instant::now();
        // Membership can change while a health RPC is pending. Go checks the
        // current API ring after its network sweep, not the retired snapshot.
        let current = self.state.lock().expect("PD region service poisoned");
        for candidate in &current.candidates {
            candidate.check_cooldown(now);
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

#[cfg(test)]
mod availability_tests {
    use super::*;
    use crate::proto::pdpb::ErrorType;

    fn urls() -> Vec<String> {
        vec!["leader".into(), "follower".into()]
    }
    fn follower(service: &RegionService) -> RegionTarget {
        for _ in 0..2 {
            let target = service.select("leader", &urls(), true, true);
            if target.follower {
                return target;
            }
        }
        panic!("follower was not available");
    }

    #[tokio::test(start_paused = true)]
    async fn pd_availability_batch_cooldown_error_identity_expiry_and_membership() {
        let service = RegionService::default();
        let first = follower(&service);
        assert!(first.observe_error(true, None));
        assert!(first.observe_error(false, Some(ErrorType::Unknown as i32)));
        assert!(first.candidate.available());
        assert!(first.observe_error(false, Some(ErrorType::RegionNotFound as i32)));
        let until = *first.candidate.unavailable_until.lock().unwrap();
        tokio::time::advance(Duration::from_secs(5)).await;
        first.observe_error(false, Some(ErrorType::RegionNotFound as i32));
        assert_eq!(
            *first.candidate.unavailable_until.lock().unwrap(),
            until,
            "in-flight failures cannot extend cooldown"
        );
        for _ in 0..4 {
            assert!(!service.select("leader", &urls(), true, true).follower);
        }
        tokio::time::advance(Duration::from_secs(5)).await;
        first.candidate.check_cooldown(Instant::now());
        assert!(
            !first.candidate.available(),
            "Go uses strictly After, not >="
        );
        tokio::time::advance(Duration::from_nanos(1)).await;
        first.candidate.check_cooldown(Instant::now());
        assert!(follower(&service).candidate.available());
        let leader = service.select("leader", &urls(), false, false);
        assert!(!leader.observe_error(false, Some(ErrorType::RegionNotFound as i32)));
        assert!(leader.candidate.available());
        // A removed and recreated member owns fresh API/network state.
        service.update_members("leader", &["leader".into()]);
        let replacement = follower(&service);
        first.observe_error(false, Some(ErrorType::RegionNotFound as i32));
        first
            .candidate
            .member
            .network_failure
            .store(true, Ordering::Release);
        assert!(replacement.candidate.available());
    }

    #[tokio::test(start_paused = true)]
    async fn pd_availability_batch_stale_health_completion_and_ring_exhaustion() {
        let service = RegionService::default();
        let old = follower(&service);
        let entered = tokio::sync::Notify::new();
        let release = tokio::sync::Notify::new();
        let probe = service.check_health(Duration::from_secs(10), |url| {
            let entered = &entered;
            let release = &release;
            async move {
                if url == "follower" {
                    entered.notify_one();
                    release.notified().await;
                }
                Err(Status::unavailable("network disconnected"))
            }
        });
        let change = async {
            entered.notified().await;
            service.update_members("leader", &["leader".into()]);
            service.update_members("leader", &urls());
            let replacement = follower(&service);
            replacement.observe_error(false, Some(ErrorType::RegionNotFound as i32));
            tokio::time::advance(REGION_COOLDOWN + Duration::from_secs(1)).await;
            release.notify_one();
        };
        tokio::join!(probe, change);
        assert!(!old.candidate.available());
        let new = follower(&service);
        assert!(
            new.candidate.available(),
            "late health cannot poison a replacement"
        );
        new.candidate
            .member
            .network_failure
            .store(true, Ordering::Release);
        let fallback = service.select("leader", &urls(), true, true);
        assert_eq!(
            fallback.endpoint, "leader",
            "empty eligible ring falls back to source leader"
        );
    }
    #[tokio::test(start_paused = true)]
    async fn forwarding_selection_is_independent_of_region_cooldown_and_topology() {
        use tonic::service::Interceptor;
        let service = RegionService::default();
        service.update_members("leader", &urls());
        assert_eq!(service.forwarding_target("leader", true).0, "leader");
        let follower = follower(&service);
        follower.observe_error(false, Some(ErrorType::RegionNotFound as i32));
        service.state.lock().unwrap().candidates[0]
            .member
            .network_failure
            .store(true, Ordering::Release);
        assert_eq!(service.forwarding_target("leader", false).0, "leader");
        let region = service.select_with_forwarding("leader", &urls(), true, true, true);
        assert_eq!(region.endpoint, "follower");
        assert!(
            region
                .request(())
                .metadata()
                .contains_key("pd-allow-follower-handle"),
            "fallback preserves Go mustLeader=false"
        );
        assert!(region.forwarded_host.is_none());
        assert!(
            !region.region_feedback,
            "fallback uses the forward API's error policy"
        );
        let leader_required = service.select_with_forwarding("leader", &urls(), true, false, true);
        assert_eq!(leader_required.forwarded_host.as_deref(), Some("leader"));
        let (physical, mut metadata) = service.forwarding_target("leader", true);
        assert_eq!(
            physical, "follower",
            "region-local cooldown must not disable forwarding"
        );
        assert_eq!(service.forwarding_target("follower", true).0, "follower");
        service.update_members("new-leader", &["new-leader".into(), "follower".into()]);
        let request = metadata.call(region_request((), true)).unwrap();
        assert_eq!(
            request.metadata().get("pd-forwarded-host").unwrap(),
            "leader",
            "in-flight metadata keeps its selected logical destination"
        );
        assert!(!request.metadata().contains_key("pd-allow-follower-handle"));
        assert_eq!(
            service.forwarding_target("new-leader", true).0,
            "new-leader"
        );
        for candidate in &service.state.lock().unwrap().candidates {
            candidate
                .member
                .network_failure
                .store(true, Ordering::Release);
        }
        assert_eq!(
            service.forwarding_target("new-leader", true).0,
            "new-leader",
            "no eligible follower falls back to leader"
        );
    }
}

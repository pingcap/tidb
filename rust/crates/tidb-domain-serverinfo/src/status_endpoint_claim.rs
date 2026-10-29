// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/domain/serverinfo/status_endpoint_claim.go`.
//!
//! A serving TiDB advertises one status endpoint. The claim is a leased etcd
//! key, so two server-info records with the same advertised endpoint produce a
//! warning without blocking registration. Reattaching a same-ID claim is
//! guarded by the observed value and modification revision; an old session can
//! therefore never overwrite or remove a newer lease.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

use crate::serverinfo::ServerInfo;

/// Go `serverStatusAddressPath`.
pub const SERVER_STATUS_ADDRESS_PATH: &str = "/tidb/server/status_addr";

/// Go's production default for a zero status port.
pub const DEFAULT_STATUS_PORT: usize = 10_080;

/// Result states of one status-endpoint claim attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusEndpointClaimState {
    /// Claiming is disabled, not applicable, or no etcd client is present.
    Skipped,
    /// This server owns the claim under the supplied lease.
    Acquired,
    /// Another server-info ID currently owns the endpoint.
    Conflict,
    /// etcd could not establish a safe claim result.
    CheckFailed,
}

/// The observed owner and MVCC revision of an existing claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedStatusEndpointClaim {
    /// Server-info ID stored in the claim value.
    pub id: String,
    /// Lease attached to the claim key.
    pub lease: i64,
    /// Last modification revision of the key.
    pub mod_revision: i64,
}

/// Result of an atomic create-if-absent operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusEndpointClaimCreate {
    /// The key was created under the caller's lease.
    Created,
    /// The key already existed and was read in the same operation boundary.
    Existing(ObservedStatusEndpointClaim),
}

/// Diagnostic result for one claim attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusEndpointClaimResult {
    /// State reached by the attempt.
    pub state: StatusEndpointClaimState,
    /// Normalized endpoint (`host:port`) or empty when skipped.
    pub endpoint: String,
    /// Base64url claim key or empty when skipped.
    pub claim_key: String,
    /// Local server-info ID.
    pub local_id: String,
    /// Existing owner ID for a conflict, if any.
    pub existing_id: String,
    /// Existing owner lease for a conflict, if any.
    pub existing_lease: i64,
    /// Error explaining a check failure, if any.
    pub error: Option<String>,
}

/// One server's advertised status endpoint claim.
#[derive(Clone, Debug)]
pub struct StatusEndpointClaim {
    endpoint: String,
    key: String,
    local_id: String,
}

impl StatusEndpointClaim {
    /// Builds the normalized endpoint and claim key for `info`.
    #[must_use]
    pub fn new(info: &ServerInfo, claim_enabled: bool) -> Self {
        let (endpoint, key) = build_status_endpoint_claim(info, claim_enabled);
        Self {
            endpoint,
            key,
            local_id: info.static_info.id.clone(),
        }
    }

    /// Normalized advertised endpoint, or empty when this claim is skipped.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Leased etcd claim key, or empty when this claim is skipped.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Tries to acquire or safely reattach this claim.
    pub fn acquire(
        &self,
        etcd: &dyn crate::serverinfo_syncer::EtcdOps,
        lease: i64,
    ) -> StatusEndpointClaimResult {
        let mut result = StatusEndpointClaimResult {
            state: StatusEndpointClaimState::Skipped,
            endpoint: self.endpoint.clone(),
            claim_key: self.key.clone(),
            local_id: self.local_id.clone(),
            existing_id: String::new(),
            existing_lease: 0,
            error: None,
        };
        if self.key.is_empty() {
            return result;
        }

        let first = match etcd.status_claim_try_create(&self.key, &self.local_id, lease) {
            Ok(outcome) => outcome,
            Err(error) => {
                result.state = StatusEndpointClaimState::CheckFailed;
                result.error = Some(error);
                return result;
            }
        };
        if self.apply_create_result(&mut result, &first) {
            return result;
        }

        let observed = match first {
            StatusEndpointClaimCreate::Existing(observed) => observed,
            StatusEndpointClaimCreate::Created => unreachable!("created result returned above"),
        };
        match etcd.status_claim_reattach(&self.key, &observed.id, observed.mod_revision, lease) {
            Ok(true) => {
                result.state = StatusEndpointClaimState::Acquired;
                return result;
            }
            Ok(false) => {}
            Err(error) => {
                result.state = StatusEndpointClaimState::CheckFailed;
                result.error = Some(error);
                return result;
            }
        }

        match etcd.status_claim_try_create(&self.key, &self.local_id, lease) {
            Ok(StatusEndpointClaimCreate::Created) => {
                result.state = StatusEndpointClaimState::Acquired;
            }
            Ok(StatusEndpointClaimCreate::Existing(current)) if current.id != self.local_id => {
                result.state = StatusEndpointClaimState::Conflict;
                result.existing_id = current.id;
                result.existing_lease = current.lease;
            }
            Ok(StatusEndpointClaimCreate::Existing(_)) => {
                result.state = StatusEndpointClaimState::CheckFailed;
                result.error = Some(
                    "advertised status endpoint claim changed while reattaching the same server info ID"
                        .to_owned(),
                );
            }
            Err(error) => {
                result.state = StatusEndpointClaimState::CheckFailed;
                result.error = Some(error);
            }
        }
        result
    }

    /// Emits Go's warning-only diagnostics for duplicate or unreadable
    /// claims. Claim outcomes never block server-info registration.
    pub fn report(&self, result: &StatusEndpointClaimResult) {
        match result.state {
            StatusEndpointClaimState::Conflict => {
                tracing::warn!(
                    advertised_status_endpoint = %result.endpoint,
                    claim_key = %result.claim_key,
                    local_server_info_id = %result.local_id,
                    existing_server_info_id = %result.existing_id,
                    existing_lease_id = result.existing_lease,
                    "advertised status endpoint already has an active claim"
                );
            }
            StatusEndpointClaimState::CheckFailed => {
                tracing::warn!(
                    advertised_status_endpoint = %result.endpoint,
                    claim_key = %result.claim_key,
                    local_server_info_id = %result.local_id,
                    error = ?result.error,
                    "failed to check advertised status endpoint claim"
                );
            }
            StatusEndpointClaimState::Skipped | StatusEndpointClaimState::Acquired => {}
        }
    }

    fn apply_create_result(
        &self,
        result: &mut StatusEndpointClaimResult,
        outcome: &StatusEndpointClaimCreate,
    ) -> bool {
        match outcome {
            StatusEndpointClaimCreate::Created => {
                result.state = StatusEndpointClaimState::Acquired;
                true
            }
            StatusEndpointClaimCreate::Existing(observed) => {
                result.existing_id = observed.id.clone();
                result.existing_lease = observed.lease;
                if observed.id != self.local_id {
                    result.state = StatusEndpointClaimState::Conflict;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Removes this claim only when both owner ID and lease still match.
    pub fn remove(
        &self,
        etcd: &dyn crate::serverinfo_syncer::EtcdOps,
        lease: i64,
    ) -> Result<(), String> {
        if self.key.is_empty() {
            return Ok(());
        }
        etcd.status_claim_remove(&self.key, &self.local_id, lease)
    }
}

/// Go `buildStatusEndpointClaim`: normalize host and encode the endpoint key.
#[must_use]
pub fn build_status_endpoint_claim(info: &ServerInfo, claim_enabled: bool) -> (String, String) {
    if !claim_enabled || !info.static_info.assumed_keyspace.is_empty() {
        return (String::new(), String::new());
    }
    let mut host = info.static_info.ip.trim().to_owned();
    if host.is_empty() {
        return (String::new(), String::new());
    }
    if let Ok(address) = host.parse::<std::net::IpAddr>() {
        host = address.to_string();
    } else {
        host = host.trim_end_matches('.').to_ascii_lowercase();
    }
    if host.is_empty() {
        return (String::new(), String::new());
    }
    let port = if info.static_info.status_port == 0 {
        DEFAULT_STATUS_PORT
    } else {
        info.static_info.status_port
    };
    let endpoint = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let encoded = URL_SAFE_NO_PAD.encode(endpoint.as_bytes());
    (endpoint, format!("{SERVER_STATUS_ADDRESS_PATH}/{encoded}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serverinfo::StaticInfo;
    use crate::serverinfo_syncer::EtcdOps;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct FakeClaims {
        entries: Mutex<HashMap<String, (String, i64, i64)>>,
        next_revision: Mutex<i64>,
        next_lease: Mutex<i64>,
    }

    impl FakeClaims {
        fn revision(&self) -> i64 {
            let mut revision = self.next_revision.lock().unwrap();
            *revision += 1;
            *revision
        }
    }

    impl EtcdOps for FakeClaims {
        fn lease_grant(&self, _: i64) -> Result<i64, String> {
            let mut lease = self.next_lease.lock().unwrap();
            *lease += 1;
            Ok(*lease)
        }
        fn lease_keep_alive_once(&self, _: i64) -> Result<(), String> {
            Ok(())
        }
        fn lease_revoke(&self, _: i64) -> Result<(), String> {
            Ok(())
        }
        fn put_with_lease(&self, _: &str, _: &[u8], _: i64) -> Result<(), String> {
            Ok(())
        }
        fn get_prefix(&self, _: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(Vec::new())
        }
        fn delete(&self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn put(&self, _: &str, _: &[u8]) -> Result<(), String> {
            Ok(())
        }
        fn delete_prefix(&self, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn status_claim_try_create(
            &self,
            key: &str,
            value: &str,
            lease: i64,
        ) -> Result<StatusEndpointClaimCreate, String> {
            let mut entries = self.entries.lock().unwrap();
            if entries.contains_key(key) {
                let (id, current_lease, revision) = entries.get(key).unwrap();
                return Ok(StatusEndpointClaimCreate::Existing(
                    ObservedStatusEndpointClaim {
                        id: id.clone(),
                        lease: *current_lease,
                        mod_revision: *revision,
                    },
                ));
            }
            entries.insert(key.to_owned(), (value.to_owned(), lease, self.revision()));
            Ok(StatusEndpointClaimCreate::Created)
        }
        fn status_claim_reattach(
            &self,
            key: &str,
            value: &str,
            expected_mod_revision: i64,
            lease: i64,
        ) -> Result<bool, String> {
            let mut entries = self.entries.lock().unwrap();
            let Some((current_value, current_lease, revision)) = entries.get_mut(key) else {
                return Ok(false);
            };
            if current_value != value || *revision != expected_mod_revision {
                return Ok(false);
            }
            *revision = self.revision();
            *current_lease = lease;
            Ok(true)
        }
        fn status_claim_remove(&self, key: &str, value: &str, lease: i64) -> Result<(), String> {
            let mut entries = self.entries.lock().unwrap();
            if entries
                .get(key)
                .is_some_and(|(current_value, current_lease, _)| {
                    current_value == value && *current_lease == lease
                })
            {
                entries.remove(key);
            }
            Ok(())
        }
    }

    fn info(host: &str, port: usize, assumed_keyspace: &str) -> ServerInfo {
        ServerInfo {
            static_info: StaticInfo {
                id: "server".to_owned(),
                ip: host.to_owned(),
                status_port: port,
                assumed_keyspace: assumed_keyspace.to_owned(),
                ..StaticInfo::default()
            },
            ..ServerInfo::default()
        }
    }

    #[test]
    fn build_claim_normalizes_hosts_and_skips_disabled_or_assumed_servers() {
        let cases = [
            (" 127.0.0.1 ", 10_080, "127.0.0.1:10080"),
            (
                "2001:0db8:0000:0000:0000:0000:0000:0001",
                10_080,
                "[2001:db8::1]:10080",
            ),
            ("DB.Example.COM.", 10_080, "db.example.com:10080"),
            ("db.example.com", 0, "db.example.com:10080"),
        ];
        for (host, port, endpoint) in cases {
            let (actual_endpoint, key) = build_status_endpoint_claim(&info(host, port, ""), true);
            assert_eq!(actual_endpoint, endpoint);
            assert_eq!(
                key,
                format!(
                    "{SERVER_STATUS_ADDRESS_PATH}/{}",
                    URL_SAFE_NO_PAD.encode(endpoint)
                )
            );
        }
        assert_eq!(
            build_status_endpoint_claim(&info("127.0.0.1", 10080, ""), false),
            (String::new(), String::new())
        );
        assert_eq!(
            build_status_endpoint_claim(&info("127.0.0.1", 10080, "ks1"), true),
            (String::new(), String::new())
        );
        assert_eq!(
            build_status_endpoint_claim(&info("", 10080, ""), true),
            (String::new(), String::new())
        );
    }

    #[test]
    fn claim_conflicts_reattaches_and_removes_only_the_matching_lease() {
        let fake = Arc::new(FakeClaims::default());
        let claim = StatusEndpointClaim::new(&info("127.0.0.1", 10080, ""), true);
        assert_eq!(
            claim.acquire(fake.as_ref(), 10).state,
            StatusEndpointClaimState::Acquired
        );

        let restarted = claim.acquire(fake.as_ref(), 20);
        assert_eq!(restarted.state, StatusEndpointClaimState::Acquired);
        claim.remove(fake.as_ref(), 10).unwrap();
        assert_eq!(
            claim.acquire(fake.as_ref(), 30).state,
            StatusEndpointClaimState::Acquired
        );

        let other_info = ServerInfo {
            static_info: StaticInfo {
                id: "other".to_owned(),
                ..info("127.0.0.1", 10080, "").static_info
            },
            ..ServerInfo::default()
        };
        let other = StatusEndpointClaim::new(&other_info, true);
        let conflict = other.acquire(fake.as_ref(), 40);
        assert_eq!(conflict.state, StatusEndpointClaimState::Conflict);
        assert_eq!(conflict.existing_id, "server");
        other.remove(fake.as_ref(), 40).unwrap();
        assert_eq!(
            claim.acquire(fake.as_ref(), 50).state,
            StatusEndpointClaimState::Acquired
        );
    }

    #[test]
    fn a_conflict_warns_but_does_not_block_server_registration() {
        use crate::serverinfo_syncer::Syncer;

        let fake = Arc::new(FakeClaims::default());
        let first = Syncer::new(info("127.0.0.1", 10080, ""), Some(fake.clone()));
        first.new_session_and_store_server_info().unwrap();
        assert_eq!(
            first.last_endpoint_claim().unwrap().state,
            StatusEndpointClaimState::Acquired
        );

        let mut second_info = info("127.0.0.1", 10080, "");
        second_info.static_info.id = "other".to_owned();
        let second = Syncer::new(second_info, Some(fake.clone()));
        second.new_session_and_store_server_info().unwrap();
        let conflict = second.last_endpoint_claim().unwrap();
        assert_eq!(conflict.state, StatusEndpointClaimState::Conflict);
        assert_eq!(conflict.existing_id, "server");

        let disabled =
            Syncer::new_with_status_endpoint_claim(info("127.0.0.2", 10080, ""), Some(fake), false);
        disabled.new_session_and_store_server_info().unwrap();
        assert_eq!(
            disabled.last_endpoint_claim().unwrap().state,
            StatusEndpointClaimState::Skipped
        );
    }
}

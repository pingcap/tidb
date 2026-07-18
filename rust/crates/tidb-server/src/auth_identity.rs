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

//! Pre-authentication identity lookup boundary.
//!
//! TiDB's `session.MatchIdentity` asks the privilege owner to resolve a
//! requested username and remote host to the canonical user/host row. The
//! privilege owner applies wildcard-host matching and `skip-name-resolve`,
//! then returns either an identity row or an internal not-found error. This
//! leaf models only that request/result contract. A matched identity is not a
//! password check and does not claim an authenticated session.

use std::cmp::Ordering;
use std::net::IpAddr;

/// Go's default authentication plugin used by the SkipWithGrant path.
pub const DEFAULT_AUTH_PLUGIN: &str = "mysql_native_password";

/// Inputs supplied to the privilege/user-store identity matcher.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityLookupRequest {
    username: String,
    remote_host: String,
    skip_name_resolve: bool,
}

impl IdentityLookupRequest {
    /// Retains the requested identity and the source session setting.
    #[must_use]
    pub fn new(
        username: impl Into<String>,
        remote_host: impl Into<String>,
        skip_name_resolve: bool,
    ) -> Self {
        Self {
            username: username.into(),
            remote_host: remote_host.into(),
            skip_name_resolve,
        }
    }

    /// Username requested by the connection.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Remote host supplied to host-pattern matching.
    #[must_use]
    pub fn remote_host(&self) -> &str {
        &self.remote_host
    }

    /// Whether the matcher must avoid hostname resolution.
    #[must_use]
    pub const fn skip_name_resolve(&self) -> bool {
        self.skip_name_resolve
    }
}

/// Canonical user/host row selected by the external privilege owner.
///
/// This is an identity-table match only. It intentionally contains no
/// password, plugin secret, privilege set, or authenticated-session marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchedIdentity {
    username: String,
    host: String,
}

impl MatchedIdentity {
    /// Retains the canonical row identity returned by the matcher.
    #[must_use]
    pub fn new(username: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            host: host.into(),
        }
    }

    /// Canonical username from the selected privilege row.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Canonical host pattern from the selected privilege row.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }
}

/// Result of the external identity-table lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityLookupResult {
    /// A canonical row was selected; authentication remains pending.
    Matched(MatchedIdentity),
    /// `SkipWithGrant` admitted the requested identity without a row lookup.
    /// This is a privilege-system bypass, not an authenticated-session claim.
    Bypassed(MatchedIdentity),
    /// No user/host row matched; the caller maps this to access denied.
    NotFound,
}

impl IdentityLookupResult {
    /// Returns whether a canonical identity row was found.
    #[must_use]
    pub const fn is_matched(&self) -> bool {
        matches!(self, Self::Matched(_))
    }

    /// Returns whether identity admission succeeded through a row or bypass.
    #[must_use]
    pub const fn is_admitted(&self) -> bool {
        matches!(self, Self::Matched(_) | Self::Bypassed(_))
    }

    /// Returns the matched identity without implying authentication.
    #[must_use]
    pub const fn identity(&self) -> Option<&MatchedIdentity> {
        match self {
            Self::Matched(identity) | Self::Bypassed(identity) => Some(identity),
            Self::NotFound => None,
        }
    }
}

/// Result of the exact privilege-row check performed after identity matching.
///
/// Go's `ConnectionVerification` deliberately requires an exact username and
/// host row; it does not run the wildcard matcher a second time. This result
/// carries only that row-selection fact. Password/plugin checks and session
/// authentication remain external.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrivilegeRowAdmission {
    /// The canonical identity has an exact user/host row.
    Exact(MatchedIdentity),
    /// No exact row exists for the supplied canonical identity.
    NotFound,
}

impl PrivilegeRowAdmission {
    /// Whether an exact privilege row was selected.
    #[must_use]
    pub const fn is_admitted(&self) -> bool {
        matches!(self, Self::Exact(_))
    }

    /// Returns the exact row without implying password verification.
    #[must_use]
    pub const fn identity(&self) -> Option<&MatchedIdentity> {
        match self {
            Self::Exact(identity) => Some(identity),
            Self::NotFound => None,
        }
    }
}

/// Authentication-plugin metadata handed from an exact privilege row.
///
/// This preserves only the selected plugin name and whether the user row has
/// stored authentication data. It does not validate plugin registration,
/// inspect/hash a password, or claim that the session is authenticated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthPluginHandoff {
    identity: MatchedIdentity,
    auth_plugin: String,
    has_stored_authentication: bool,
}

impl AuthPluginHandoff {
    /// Creates metadata from an exact row admission.
    pub fn from_row(
        admission: &PrivilegeRowAdmission,
        auth_plugin: impl Into<String>,
        has_stored_authentication: bool,
    ) -> Result<Self, AuthPluginHandoffError> {
        let PrivilegeRowAdmission::Exact(identity) = admission else {
            return Err(AuthPluginHandoffError::MissingPrivilegeRow);
        };
        Ok(Self {
            identity: identity.clone(),
            auth_plugin: auth_plugin.into(),
            has_stored_authentication,
        })
    }

    /// Creates the source SkipWithGrant handoff with the native default.
    #[must_use]
    pub fn for_bypass(identity: &MatchedIdentity) -> Self {
        Self {
            identity: identity.clone(),
            auth_plugin: DEFAULT_AUTH_PLUGIN.to_owned(),
            has_stored_authentication: false,
        }
    }

    /// Canonical identity associated with this metadata.
    #[must_use]
    pub const fn identity(&self) -> &MatchedIdentity {
        &self.identity
    }

    /// Plugin name selected by the external user-store/registry owner.
    #[must_use]
    pub fn auth_plugin(&self) -> &str {
        &self.auth_plugin
    }

    /// Whether the source row contained a non-empty authentication string.
    #[must_use]
    pub const fn has_stored_authentication(&self) -> bool {
        self.has_stored_authentication
    }
}

/// Errors while creating the auth-plugin metadata handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthPluginHandoffError {
    /// No exact privilege row was available for the handoff.
    MissingPrivilegeRow,
}

impl std::fmt::Display for AuthPluginHandoffError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingPrivilegeRow => formatter.write_str("missing exact privilege row"),
        }
    }
}

impl std::error::Error for AuthPluginHandoffError {}

/// Pre-auth identity policy corresponding to TiDB's `SkipWithGrant` switch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IdentityLookupPolicy {
    skip_with_grant: bool,
}

impl IdentityLookupPolicy {
    /// Creates a policy with the source global bypass setting.
    #[must_use]
    pub const fn new(skip_with_grant: bool) -> Self {
        Self { skip_with_grant }
    }

    /// Whether the privilege system is bypassed entirely.
    #[must_use]
    pub const fn skip_with_grant(self) -> bool {
        self.skip_with_grant
    }
}

/// In-memory identity rows owned by a future privilege/user-store adapter.
///
/// Rows are sorted with the same source ordering before matching: exact and
/// longer host patterns precede suffix wildcards, and `%` is the least
/// specific catch-all. DNS reverse lookup is deliberately injected as
/// `reverse_hosts`; this leaf never performs network I/O.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IdentityCatalog {
    records: Vec<MatchedIdentity>,
}

impl IdentityCatalog {
    /// Creates a catalog and applies TiDB's most-specific-first host ordering.
    #[must_use]
    pub fn new(records: impl IntoIterator<Item = MatchedIdentity>) -> Self {
        let mut records: Vec<_> = records.into_iter().collect();
        records.sort_by(|left, right| compare_host(&left.host, &right.host));
        Self { records }
    }

    /// Returns rows in the canonical matching order.
    #[must_use]
    pub fn records(&self) -> &[MatchedIdentity] {
        &self.records
    }

    /// Resolves a requested user/host to its canonical row.
    ///
    /// `reverse_hosts` must be the already-resolved names for the remote host
    /// when the caller permits name resolution. Passing an empty slice is the
    /// source-shaped no-fallback case; no DNS lookup is attempted here.
    #[must_use]
    pub fn resolve(
        &self,
        request: &IdentityLookupRequest,
        reverse_hosts: &[&str],
    ) -> IdentityLookupResult {
        if let Some(identity) = self.find(request.username(), request.remote_host()) {
            return IdentityLookupResult::Matched(identity.clone());
        }
        if !request.skip_name_resolve() && request.remote_host() != "localhost" {
            for reverse_host in reverse_hosts {
                if let Some(identity) = self.find(request.username(), reverse_host) {
                    return IdentityLookupResult::Matched(identity.clone());
                }
            }
        }
        IdentityLookupResult::NotFound
    }

    /// Resolves identity under the source `SkipWithGrant` admission policy.
    ///
    /// When enabled, TiDB returns the requested user/host without consulting
    /// the privilege rows. The returned [`IdentityLookupResult::Bypassed`]
    /// keeps that distinction explicit and does not authenticate the session.
    #[must_use]
    pub fn resolve_with_policy(
        &self,
        policy: IdentityLookupPolicy,
        request: &IdentityLookupRequest,
        reverse_hosts: &[&str],
    ) -> IdentityLookupResult {
        if policy.skip_with_grant() {
            return IdentityLookupResult::Bypassed(MatchedIdentity::new(
                request.username(),
                request.remote_host(),
            ));
        }
        self.resolve(request, reverse_hosts)
    }

    /// Performs Go `ConnectionVerification`'s exact user/host row check.
    #[must_use]
    pub fn exact_row(&self, identity: &MatchedIdentity) -> PrivilegeRowAdmission {
        self.records
            .iter()
            .find(|record| record.username == identity.username && record.host == identity.host)
            .cloned()
            .map(PrivilegeRowAdmission::Exact)
            .unwrap_or(PrivilegeRowAdmission::NotFound)
    }

    fn find(&self, username: &str, host: &str) -> Option<&MatchedIdentity> {
        self.records
            .iter()
            .find(|identity| identity.username == username && host_matches(&identity.host, host))
    }
}

fn compare_host(left: &str, right: &str) -> Ordering {
    // This is the source compareHost ordering: `%` and the empty string are
    // catch-alls, then non-suffix patterns precede suffix `%` patterns, and
    // longer suffix patterns are more specific.
    if left == "%" || right == "%" {
        return match (left == "%", right == "%") {
            (true, true) => Ordering::Equal,
            (false, true) => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, false) => unreachable!("catch-all branch requires a percent pattern"),
        };
    }
    if left.is_empty() || right.is_empty() {
        return match (left.is_empty(), right.is_empty()) {
            (true, true) => Ordering::Equal,
            (false, true) => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, false) => unreachable!("catch-all branch requires an empty pattern"),
        };
    }

    let left_suffix = left.ends_with('%');
    let right_suffix = right.ends_with('%');
    if left_suffix || right_suffix {
        return match (left_suffix, right_suffix) {
            (false, true) => Ordering::Less,
            (true, false) => Ordering::Greater,
            (true, true) => right.len().cmp(&left.len()),
            (false, false) => unreachable!("suffix branch requires a suffix pattern"),
        };
    }
    left.cmp(right)
}

fn host_matches(pattern: &str, host: &str) -> bool {
    if let Some((network, mask)) = parse_ipv4_network(pattern) {
        if let Ok(IpAddr::V4(address)) = host.parse::<IpAddr>() {
            return network
                .octets()
                .iter()
                .zip(mask.octets())
                .zip(address.octets())
                .all(|((network, mask), address)| network & mask == address & mask);
        }
    }
    if pattern == "localhost"
        && host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false)
    {
        return true;
    }
    wildcard_match(host.as_bytes(), pattern.as_bytes())
}

fn parse_ipv4_network(pattern: &str) -> Option<(std::net::Ipv4Addr, std::net::Ipv4Addr)> {
    let (network, mask) = pattern.split_once('/')?;
    let network = network.parse().ok()?;
    let mask = mask.parse().ok()?;
    let network_bits = u32::from(network);
    let mask_bits = u32::from(mask);
    (network_bits & mask_bits == network_bits).then_some((network, mask))
}

fn wildcard_match(value: &[u8], pattern: &[u8]) -> bool {
    let mut value_index = 0;
    let mut pattern_index = 0;
    let mut wildcard_pattern = None;
    let mut wildcard_value = 0;

    while value_index < value.len() {
        if pattern_index < pattern.len() {
            match pattern[pattern_index] {
                b'\\' if pattern_index + 1 < pattern.len() => {
                    if value[value_index] == pattern[pattern_index + 1] {
                        value_index += 1;
                        pattern_index += 2;
                        continue;
                    }
                }
                b'\\' if value[value_index] == b'\\' => {
                    value_index += 1;
                    pattern_index += 1;
                    continue;
                }
                b'%' => {
                    wildcard_pattern = Some(pattern_index);
                    wildcard_value = value_index;
                    pattern_index += 1;
                    continue;
                }
                b'_' => {
                    value_index += 1;
                    pattern_index += 1;
                    continue;
                }
                literal if literal == value[value_index] => {
                    value_index += 1;
                    pattern_index += 1;
                    continue;
                }
                _ => {}
            }
        }
        if let Some(wildcard_pattern) = wildcard_pattern {
            pattern_index = wildcard_pattern + 1;
            wildcard_value += 1;
            value_index = wildcard_value;
            continue;
        }
        return false;
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'%' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

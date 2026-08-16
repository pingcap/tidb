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

//! Source-shaped status-variable registry metadata from
//! `pkg/sessionctx/variable/statusvar.go`.
//!
//! The Go owner registers statistics providers, asks each provider for a map,
//! and attaches the provider's scope to every returned value. This leaf keeps
//! that deterministic provider/merge boundary typed. Live `SessionVars`, TLS
//! and atomic counters, warning/error construction, and the Go global mutex
//! remain outside this value owner.

use std::collections::BTreeMap;

/// Scope bits used by status-variable metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatusScope(u8);

impl StatusScope {
    /// A status visible at global scope.
    pub const GLOBAL: Self = Self(0b01);
    /// A status visible at session scope.
    pub const SESSION: Self = Self(0b10);
    /// The source default: visible at both global and session scope.
    pub const DEFAULT: Self = Self(Self::GLOBAL.0 | Self::SESSION.0);

    /// Combines scope bits without introducing a new scope value.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns the source bit representation.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

impl std::ops::BitOr for StatusScope {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

/// The dependency-closed scalar values needed by the registry boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusValue {
    /// A textual status value.
    Text(String),
    /// A signed numeric status value.
    Signed(i64),
    /// An unsigned numeric status value.
    Unsigned(u64),
    /// A boolean status value.
    Boolean(bool),
}

/// One collected status value and its provider-selected scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusVal {
    /// Scope in which this status is visible.
    pub scope: StatusScope,
    /// Value returned by the owning provider.
    pub value: StatusValue,
}

/// A source-shaped provider of status-variable maps.
pub trait StatusProvider {
    /// Returns the scope for one status name.
    fn scope(&self, status: &str) -> StatusScope;

    /// Returns this provider's status values.
    fn stats(&self) -> Result<BTreeMap<String, StatusValue>, String>;
}

/// Opaque registration token used to remove one provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Registration(u64);

/// Deterministic status-provider registry.
#[derive(Default)]
pub struct StatusRegistry {
    providers: Vec<(Registration, Box<dyn StatusProvider>)>,
    next_registration: u64,
}

impl StatusRegistry {
    /// Registers a provider and returns its removal token.
    #[must_use = "retain the registration token to unregister this provider"]
    pub fn register<P>(&mut self, provider: P) -> Registration
    where
        P: StatusProvider + 'static,
    {
        let registration = Registration(self.next_registration);
        self.next_registration = self.next_registration.wrapping_add(1);
        self.providers.push((registration, Box::new(provider)));
        registration
    }

    /// Removes a provider, using the source swap-with-last list behavior.
    #[must_use]
    pub fn unregister(&mut self, registration: Registration) -> bool {
        let Some(index) = self
            .providers
            .iter()
            .position(|(candidate, _)| *candidate == registration)
        else {
            return false;
        };
        self.providers.swap_remove(index);
        true
    }

    /// Collects provider values and attaches each provider's scope.
    pub fn collect(&self) -> Result<BTreeMap<String, StatusVal>, String> {
        let mut values = BTreeMap::new();
        for (_, provider) in &self.providers {
            for (name, value) in provider.stats()? {
                let scope = provider.scope(&name);
                values.insert(name, StatusVal { scope, value });
            }
        }
        Ok(values)
    }
}

/// The 25 cipher suites Go `statusvar.go` advertises through
/// `Ssl_cipher_list`, in the source's own order; the joined string reads them
/// through `pkg/util/tls`'s MySQL/OpenSSL spellings.
pub const TLS_CIPHERS: [u16; 25] = [
    0x0005, 0x000a, 0x002f, 0x0035, 0x003c, 0x009c, 0x009d, 0xc007, 0xc009, 0xc00a, 0xc011, 0xc012,
    0xc013, 0xc014, 0xc023, 0xc027, 0xc02f, 0xc02b, 0xc030, 0xc02c, 0xcca8, 0xcca9, 0x1301, 0x1302,
    0x1303,
];

/// Go's `tlsSupportedCiphers`, built in `init`: every advertised suite's
/// name followed by a colon — including a trailing one.
#[must_use]
pub fn tls_supported_ciphers() -> String {
    let mut joined = String::new();
    for id in TLS_CIPHERS {
        joined.push_str(tidb_util::tls::cipher_suite_name(id));
        joined.push(':');
    }
    joined
}

/// The negotiated TLS facts Go reads from `vars.TLSConnectionState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TlsConnectionState {
    /// The negotiated cipher suite identifier.
    pub cipher_suite: u16,
    /// The negotiated protocol version identifier.
    pub version: u16,
}

/// Go's private `defaultStatusStat`: the built-in provider behind the SSL
/// and connect-attribute status variables.
///
/// Go reads `vardef.ConnectAttrs*` atomics and the live `SessionVars`; the
/// same facts arrive here as a snapshot, `None` TLS being a plaintext
/// connection.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultStatusStat {
    /// Go `vardef.ConnectAttrsLongestSeen`.
    pub connect_attrs_longest_seen: i64,
    /// Go `vardef.ConnectAttrsLost`.
    pub connect_attrs_lost: i64,
    /// Go `vars.KeysExamined`.
    pub keys_examined: u64,
    /// Go `vars.TLSConnectionState`.
    pub tls: Option<TlsConnectionState>,
}

impl StatusProvider for DefaultStatusStat {
    fn scope(&self, status: &str) -> StatusScope {
        match status {
            "Performance_schema_session_connect_attrs_longest_seen"
            | "Performance_schema_session_connect_attrs_lost" => StatusScope::GLOBAL,
            "tidb_keys_examined" => StatusScope::SESSION,
            _ => StatusScope::DEFAULT,
        }
    }

    fn stats(&self) -> Result<BTreeMap<String, StatusValue>, String> {
        let mut stats = BTreeMap::new();
        // The four SSL defaults, overridden below on a TLS connection.
        stats.insert("Ssl_cipher".to_owned(), StatusValue::Text(String::new()));
        stats.insert(
            "Ssl_cipher_list".to_owned(),
            StatusValue::Text(String::new()),
        );
        stats.insert("Ssl_verify_mode".to_owned(), StatusValue::Signed(0));
        stats.insert("Ssl_version".to_owned(), StatusValue::Text(String::new()));
        stats.insert(
            "Performance_schema_session_connect_attrs_longest_seen".to_owned(),
            StatusValue::Signed(self.connect_attrs_longest_seen),
        );
        stats.insert(
            "Performance_schema_session_connect_attrs_lost".to_owned(),
            StatusValue::Signed(self.connect_attrs_lost),
        );
        stats.insert(
            "tidb_keys_examined".to_owned(),
            StatusValue::Unsigned(self.keys_examined),
        );
        if let Some(tls) = self.tls {
            stats.insert(
                "Ssl_cipher".to_owned(),
                StatusValue::Text(tidb_util::tls::cipher_suite_name(tls.cipher_suite).to_owned()),
            );
            stats.insert(
                "Ssl_cipher_list".to_owned(),
                StatusValue::Text(tls_supported_ciphers()),
            );
            // Go: tls.VerifyClientCertIfGiven == SSL_VERIFY_PEER (0x01) |
            // SSL_VERIFY_CLIENT_ONCE (0x04).
            stats.insert(
                "Ssl_verify_mode".to_owned(),
                StatusValue::Signed(0x01 | 0x04),
            );
            stats.insert(
                "Ssl_version".to_owned(),
                StatusValue::Text(tidb_util::tls::version_name(tls.version)),
            );
        }
        Ok(stats)
    }
}

#[cfg(test)]
mod default_status_tests {
    use super::*;

    // Go `TestStatusVar`'s shape: a plaintext connection reports the empty
    // SSL defaults, and every entry carries its declared scope.
    #[test]
    fn plaintext_connections_report_the_ssl_defaults() {
        let mut registry = StatusRegistry::default();
        let _registration = registry.register(DefaultStatusStat {
            connect_attrs_longest_seen: 3,
            connect_attrs_lost: 1,
            keys_examined: 42,
            tls: None,
        });
        let collected = registry.collect().unwrap();

        assert_eq!(
            collected["Ssl_cipher"].value,
            StatusValue::Text(String::new())
        );
        assert_eq!(collected["Ssl_verify_mode"].value, StatusValue::Signed(0));
        assert_eq!(collected["Ssl_cipher"].scope, StatusScope::DEFAULT);
        assert_eq!(
            collected["Performance_schema_session_connect_attrs_longest_seen"],
            StatusVal {
                scope: StatusScope::GLOBAL,
                value: StatusValue::Signed(3)
            }
        );
        assert_eq!(
            collected["tidb_keys_examined"],
            StatusVal {
                scope: StatusScope::SESSION,
                value: StatusValue::Unsigned(42)
            }
        );
    }

    // A TLS connection reports the negotiated names through pkg/util/tls's
    // MySQL/OpenSSL spellings and the fixed verify mode.
    #[test]
    fn tls_connections_report_the_negotiated_names() {
        let stat = DefaultStatusStat {
            tls: Some(TlsConnectionState {
                cipher_suite: 0x1301,
                version: 0x0304,
            }),
            ..Default::default()
        };
        let stats = stat.stats().unwrap();
        assert_eq!(
            stats["Ssl_cipher"],
            StatusValue::Text("TLS_AES_128_GCM_SHA256".to_owned())
        );
        assert_eq!(
            stats["Ssl_version"],
            StatusValue::Text("TLSv1.3".to_owned())
        );
        assert_eq!(stats["Ssl_verify_mode"], StatusValue::Signed(5));

        let list = tls_supported_ciphers();
        assert!(list.starts_with("RC4-SHA:DES-CBC3-SHA:"));
        assert!(list.ends_with("TLS_CHACHA20_POLY1305_SHA256:"));
        assert_eq!(list.matches(':').count(), 25);
    }
}

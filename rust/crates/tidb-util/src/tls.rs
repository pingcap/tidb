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

//! MySQL-compatible names for TLS protocol versions and cipher suites.
//!
//! Go's `pkg/util/tls` owns these display names independently of TLS
//! negotiation. The Rust SQL node keeps negotiation in `tidb-server` and
//! `tidb-pd-client`, while this dependency leaf supplies the same names to
//! status, privilege, and connection-info consumers.
//!
//! Go's package also exposes the process-wide `RequireSecureTransport` atomic.
//! Rust deliberately has one live authority instead of duplicating that state:
//! `tidb_session::GlobalSysvars` stores the GLOBAL value, and
//! `tidb_server::ConfiguredUserStore` reads it into the secure-transport policy
//! for every login.

/// TLS protocol version 1.0.
pub const VERSION_TLS10: u16 = 0x0301;
/// TLS protocol version 1.1.
pub const VERSION_TLS11: u16 = 0x0302;
/// TLS protocol version 1.2.
pub const VERSION_TLS12: u16 = 0x0303;
/// TLS protocol version 1.3.
pub const VERSION_TLS13: u16 = 0x0304;
/// The retired SSL protocol version 3.0 identifier.
pub const VERSION_SSL30: u16 = 0x0300;

/// Returns TiDB's MySQL/OpenSSL-compatible display name for a TLS version.
#[must_use]
pub fn version_name(version: u16) -> String {
    match version {
        VERSION_SSL30 => "SSLv3".to_owned(),
        VERSION_TLS10 => "TLS 1.0".to_owned(),
        VERSION_TLS11 => "TLS 1.1".to_owned(),
        VERSION_TLS12 => "TLSv1.2".to_owned(),
        VERSION_TLS13 => "TLSv1.3".to_owned(),
        _ => format!("0x{version:04X}"),
    }
}

/// Cipher-suite identifiers and the names TiDB exposes to MySQL clients.
pub const CIPHER_SUITE_NAMES: &[(u16, &str)] = &[
    (0x0005, "RC4-SHA"),
    (0x000a, "DES-CBC3-SHA"),
    (0x002f, "AES128-SHA"),
    (0x0035, "AES256-SHA"),
    (0x003c, "AES128-SHA256"),
    (0x009c, "AES128-GCM-SHA256"),
    (0x009d, "AES256-GCM-SHA384"),
    (0xc007, "ECDHE-ECDSA-RC4-SHA"),
    (0xc009, "ECDHE-ECDSA-AES128-SHA"),
    (0xc00a, "ECDHE-ECDSA-AES256-SHA"),
    (0xc011, "ECDHE-RSA-RC4-SHA"),
    (0xc012, "ECDHE-RSA-DES-CBC3-SHA"),
    (0xc013, "ECDHE-RSA-AES128-SHA"),
    (0xc014, "ECDHE-RSA-AES256-SHA"),
    (0xc023, "ECDHE-ECDSA-AES128-SHA256"),
    (0xc027, "ECDHE-RSA-AES128-SHA256"),
    (0xc02f, "ECDHE-RSA-AES128-GCM-SHA256"),
    (0xc02b, "ECDHE-ECDSA-AES128-GCM-SHA256"),
    (0xc030, "ECDHE-RSA-AES256-GCM-SHA384"),
    (0xc02c, "ECDHE-ECDSA-AES256-GCM-SHA384"),
    (0xcca8, "ECDHE-RSA-CHACHA20-POLY1305"),
    (0xcca9, "ECDHE-ECDSA-CHACHA20-POLY1305"),
    (0x1301, "TLS_AES_128_GCM_SHA256"),
    (0x1302, "TLS_AES_256_GCM_SHA384"),
    (0x1303, "TLS_CHACHA20_POLY1305_SHA256"),
];

/// Returns the MySQL/OpenSSL-compatible cipher name, or an empty string for
/// an identifier absent from Go's source table.
#[must_use]
pub fn cipher_suite_name(identifier: u16) -> &'static str {
    CIPHER_SUITE_NAMES
        .iter()
        .find_map(|(candidate, name)| (*candidate == identifier).then_some(*name))
        .unwrap_or("")
}

/// Returns whether a cipher name belongs to TiDB's exported support set.
#[must_use]
pub fn supports_cipher(name: &str) -> bool {
    CIPHER_SUITE_NAMES
        .iter()
        .any(|(_, candidate)| *candidate == name)
}

/// Iterates the cipher names in Go's source-table order.
pub fn supported_cipher_names() -> impl ExactSizeIterator<Item = &'static str> {
    CIPHER_SUITE_NAMES.iter().map(|(_, name)| *name)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn version_name_matches_every_go_source_row() {
        for (version, expected) in [
            (VERSION_SSL30, "SSLv3"),
            (VERSION_TLS10, "TLS 1.0"),
            (VERSION_TLS11, "TLS 1.1"),
            (VERSION_TLS12, "TLSv1.2"),
            (VERSION_TLS13, "TLSv1.3"),
            (VERSION_TLS13 + 1, "0x0305"),
        ] {
            assert_eq!(version_name(version), expected);
        }
    }

    #[test]
    fn cipher_names_and_support_set_match_the_complete_go_tables() {
        assert_eq!(CIPHER_SUITE_NAMES.len(), 25);
        let names = supported_cipher_names().collect::<Vec<_>>();
        assert_eq!(names.iter().copied().collect::<HashSet<_>>().len(), 25);
        for (identifier, name) in CIPHER_SUITE_NAMES {
            assert_eq!(cipher_suite_name(*identifier), *name);
            assert!(supports_cipher(name));
        }
        assert_eq!(cipher_suite_name(0xffff), "");
        assert!(!supports_cipher("TLS_FAKE_CIPHER"));
    }
}

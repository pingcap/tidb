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

//! Go `pkg/util/tls` (`tls.go`), whole: the MySQL/OpenSSL-compatible names
//! for negotiated TLS versions and cipher suites, which the server reports
//! through `Ssl_version` and `Ssl_cipher`.
//!
//! The tables are keyed by the wire's own u16 identifiers, so any TLS
//! implementation's negotiated values resolve here — Go keys the same map by
//! `crypto/tls` constants, which ARE those wire values.

/// Go `versionString`: the MySQL/OpenSSL spellings TiDB prefers. TLS 1.0 and
/// 1.1 were deliberately removed upstream, so they fall to the standard
/// library's names below.
const VERSION_STRING: &[(u16, &str)] = &[(0x0303, "TLSv1.2"), (0x0304, "TLSv1.3")];

/// Go `VersionName`: the preferred spelling, else `crypto/tls.VersionName`,
/// whose contract is the IANA name for versions it knows and `0x%04X` for
/// ones it does not.
#[must_use]
pub fn version_name(version: u16) -> String {
    if let Some((_, name)) = VERSION_STRING.iter().find(|(id, _)| *id == version) {
        return (*name).to_owned();
    }
    match version {
        0x0300 => "SSLv3".to_owned(),
        0x0301 => "TLS 1.0".to_owned(),
        0x0302 => "TLS 1.1".to_owned(),
        other => format!("0x{other:04X}"),
    }
}

/// Go `tlsCipherString`: cipher suites to MySQL/OpenSSL compatible names.
/// The 1.0–1.2 entries carry OpenSSL spellings; the 1.3 trio keeps MySQL's
/// underscore form. An entry Go's table lacks answers the empty string.
const TLS_CIPHER_STRING: &[(u16, &str)] = &[
    // TLS 1.0 - 1.2 cipher suites, mysql compatible names.
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
    // TLS 1.3 cipher suites, compatible with mysql using '_'.
    (0x1301, "TLS_AES_128_GCM_SHA256"),
    (0x1302, "TLS_AES_256_GCM_SHA384"),
    (0x1303, "TLS_CHACHA20_POLY1305_SHA256"),
];

/// Go `CipherSuiteName`: the mapped name, or `""` for a suite outside the
/// table — Go's `if !ok { return "" }`, kept because the empty string is
/// what `Ssl_cipher` then reports.
#[must_use]
pub fn cipher_suite_name(suite: u16) -> String {
    TLS_CIPHER_STRING
        .iter()
        .find(|(id, _)| *id == suite)
        .map(|(_, name)| (*name).to_owned())
        .unwrap_or_default()
}

/// Go `SupportCipher` membership: every NAME in the cipher table is a suite
/// TiDB accepts in `ssl-cipher` configuration.
#[must_use]
pub fn is_supported_cipher(name: &str) -> bool {
    TLS_CIPHER_STRING
        .iter()
        .any(|(_, candidate)| *candidate == name)
}

#[cfg(test)]
mod tests {
    use super::{cipher_suite_name, is_supported_cipher, version_name};

    /// Go `TestVersionName` (`tls_test.go`), whole — including the standard
    /// library fallbacks and the hex form for an unknown version.
    #[test]
    fn test_version_name() {
        for (version, name) in [
            (0x0300, "SSLv3"),
            (0x0301, "TLS 1.0"),
            (0x0302, "TLS 1.1"),
            (0x0303, "TLSv1.2"),
            (0x0304, "TLSv1.3"),
            (0x0305, "0x0305"),
        ] {
            assert_eq!(version_name(version), name, "{version:#06x}");
        }
    }

    /// Upstream has no unit test over the cipher table; these pin the three
    /// 1.3 suites every modern handshake lands on, one OpenSSL-spelled 1.2
    /// entry, and Go's empty-string answer for an unmapped suite.
    #[test]
    fn cipher_names_follow_gos_table() {
        assert_eq!(cipher_suite_name(0x1301), "TLS_AES_128_GCM_SHA256");
        assert_eq!(cipher_suite_name(0x1302), "TLS_AES_256_GCM_SHA384");
        assert_eq!(cipher_suite_name(0x1303), "TLS_CHACHA20_POLY1305_SHA256");
        assert_eq!(cipher_suite_name(0xc02f), "ECDHE-RSA-AES128-GCM-SHA256");
        assert_eq!(cipher_suite_name(0xffff), "");
        assert!(is_supported_cipher("TLS_AES_128_GCM_SHA256"));
        assert!(!is_supported_cipher("NOT_A_CIPHER"));
    }
}

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

//! Go `pkg/util/service_url.go`.

use tidb_go_net::split_host_port;

/// Go `URLSchemeHTTP`: the http scheme.
pub const URL_SCHEME_HTTP: &str = "http";
/// Go `URLSchemeHTTPS`: the https scheme.
pub const URL_SCHEME_HTTPS: &str = "https";
/// Go `URLSchemeUnix`: the unix domain socket scheme.
pub const URL_SCHEME_UNIX: &str = "unix";
/// Go `URLSchemeUnixs`: the unix domain socket over TLS scheme.
pub const URL_SCHEME_UNIXS: &str = "unixs";

/// Go `ServiceURL`: a supported service endpoint URL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServiceUrl {
    scheme: String,
    address: String,
}

/// Go `ParseServiceURL`: parses a supported service URL.
///
/// # Errors
///
/// Go's message for an empty, unsupported or malformed URL.
pub fn parse_service_url(raw: &str) -> Result<ServiceUrl, String> {
    parse_service_url_with(raw, "")
}

fn parse_service_url_with(raw: &str, default_scheme: &str) -> Result<ServiceUrl, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("URL must not be empty".to_owned());
    }

    if !raw.contains("://") {
        if !is_supported_service_url_scheme(default_scheme) {
            if default_scheme.is_empty() {
                return Err(format!("URL scheme must be http, https, unix, or unixs: {raw}"));
            }
            return Err(format!("URL scheme must be http, https, unix, or unixs: {default_scheme}"));
        }
        if split_host_port(raw).is_none() {
            return Err(format!("URL address does not have the form \"host:port\": {raw}"));
        }
        return Ok(ServiceUrl { scheme: default_scheme.to_owned(), address: raw.to_owned() });
    }

    for scheme in [URL_SCHEME_UNIX, URL_SCHEME_UNIXS] {
        if let Some(address) = raw.strip_prefix(scheme).and_then(|rest| rest.strip_prefix("://")) {
            if address.is_empty() {
                return Err(format!("URL address must not be empty: {raw}"));
            }
            return Ok(ServiceUrl { scheme: scheme.to_owned(), address: address.to_owned() });
        }
    }

    let u = tidb_go_url::parse(raw).map_err(|err| format!("parse url {raw} failed {err}"))?;
    let scheme = String::from_utf8_lossy(&u.scheme).into_owned();
    if !is_supported_service_url_scheme(&scheme) {
        return Err(format!("URL scheme must be http, https, unix, or unixs: {raw}"));
    }
    let host = String::from_utf8_lossy(&u.host).into_owned();
    let bad_address = || format!("URL address does not have the form \"host:port\": {raw}");
    let (host_part, port) = split_host_port(&host).ok_or_else(bad_address)?;
    if host_part.is_empty() || port.is_empty() {
        return Err(bad_address());
    }
    if !u.path.is_empty() {
        return Err(format!("URL must not contain a path: {raw}"));
    }
    Ok(ServiceUrl { scheme, address: host })
}


/// Go `NormalizeServiceURL`: normalizes host:port or supported URL input to
/// scheme://address.
///
/// # Errors
///
/// [`parse_service_url`]'s.
pub fn normalize_service_url(raw: &str, default_scheme: &str) -> Result<String, String> {
    parse_service_url_with(raw, default_scheme).map(|parsed| parsed.to_string())
}

impl ServiceUrl {
    /// Go `SchemePrefix`: the normalized scheme prefix.
    #[must_use]
    pub fn scheme_prefix(&self) -> String {
        format!("{}://", self.scheme)
    }

    /// Go `Address`: the host:port or unix endpoint part without scheme.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Go `Endpoint`: the endpoint string, preserving scheme when required.
    #[must_use]
    pub fn endpoint(&self, with_scheme: bool) -> String {
        if with_scheme || self.is_unix_family() {
            return self.to_string();
        }
        self.address.clone()
    }

    /// Go `IsUnixFamily`: whether the endpoint must retain its scheme to
    /// stay dialable.
    #[must_use]
    pub fn is_unix_family(&self) -> bool {
        self.scheme == URL_SCHEME_UNIX || self.scheme == URL_SCHEME_UNIXS
    }
}

impl std::fmt::Display for ServiceUrl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}{}", self.scheme_prefix(), self.address)
    }
}

fn is_supported_service_url_scheme(scheme: &str) -> bool {
    matches!(scheme, URL_SCHEME_HTTP | URL_SCHEME_HTTPS | URL_SCHEME_UNIX | URL_SCHEME_UNIXS)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestParseServiceURL`.
    #[test]
    fn test_parse_service_url() {
        // raw, schemePrefix, address, endpointNoScheme, endpointScheme, isUnixFamily
        let tests = [
            ("http://127.0.0.1:2379", "http://", "127.0.0.1:2379", "127.0.0.1:2379", "http://127.0.0.1:2379", false),
            ("https://127.0.0.1:2379", "https://", "127.0.0.1:2379", "127.0.0.1:2379", "https://127.0.0.1:2379", false),
            ("unix://localhost:m0", "unix://", "localhost:m0", "unix://localhost:m0", "unix://localhost:m0", true),
            ("unix:///tmp/etcd.sock", "unix://", "/tmp/etcd.sock", "unix:///tmp/etcd.sock", "unix:///tmp/etcd.sock", true),
        ];
        for (raw, scheme_prefix, address, endpoint_no_scheme, endpoint_scheme, is_unix_family) in tests {
            let parsed = parse_service_url(raw).unwrap();
            assert_eq!(scheme_prefix, parsed.scheme_prefix());
            assert_eq!(address, parsed.address());
            assert_eq!(endpoint_no_scheme, parsed.endpoint(false));
            assert_eq!(endpoint_scheme, parsed.endpoint(true));
            assert_eq!(endpoint_scheme, parsed.to_string());
            assert_eq!(is_unix_family, parsed.is_unix_family());
        }
    }

    /// Go `TestParseServiceURLRejectsInvalidInput`.
    #[test]
    fn test_parse_service_url_rejects_invalid_input() {
        for raw in ["", "ftp://127.0.0.1:2379", "http://127.0.0.1", "http://127.0.0.1:2379/path"] {
            assert!(parse_service_url(raw).is_err(), "{raw}");
        }
        assert_eq!(parse_service_url("unix://").unwrap_err(), "URL address must not be empty: unix://");
    }

    /// Go `TestNormalizeServiceURL`.
    #[test]
    fn test_normalize_service_url() {
        let tests = [
            ("127.0.0.1:2379", URL_SCHEME_HTTP, "http://127.0.0.1:2379"),
            ("http://127.0.0.1:2379", URL_SCHEME_HTTP, "http://127.0.0.1:2379"),
            ("https://127.0.0.1:2379", URL_SCHEME_HTTP, "https://127.0.0.1:2379"),
            ("unix://localhost:m0", URL_SCHEME_HTTP, "unix://localhost:m0"),
            ("unix:///tmp/etcd.sock", URL_SCHEME_HTTP, "unix:///tmp/etcd.sock"),
        ];
        for (raw, default_scheme, expected) in tests {
            assert_eq!(normalize_service_url(raw, default_scheme).unwrap(), expected);
        }
        for (raw, default_scheme) in [("invalid_pd_address", URL_SCHEME_HTTP), ("127.0.0.1:2379", "ftp")] {
            assert!(normalize_service_url(raw, default_scheme).is_err());
        }
    }
}

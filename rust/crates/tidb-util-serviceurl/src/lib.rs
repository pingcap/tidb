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

//! Go `pkg/util/service_url.go`, a crate of its own below `tidb-util`: the
//! meta service reads it and the store driver reads the meta service, while
//! `tidb-util` sits above the driver. `tidb_util::service_url` is this crate.

use std::borrow::Cow;

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
    scheme: &'static str,
    address: String,
}

/// Go `ParseServiceURL`: parses a supported service URL.
///
/// # Errors
///
/// Go's message for an empty, unsupported or malformed URL.
pub fn parse_service_url(raw: &str) -> Result<ServiceUrl, String> {
    let (scheme, address) = parse_service_url_with(raw, "")?;
    Ok(ServiceUrl {
        scheme,
        address: address.into_owned(),
    })
}

fn parse_service_url_with<'a>(
    raw: &'a str,
    default_scheme: &str,
) -> Result<(&'static str, Cow<'a, str>), String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("URL must not be empty".to_owned());
    }
    let unix = if let Some(address) = raw.strip_prefix("unix://") {
        Some((URL_SCHEME_UNIX, address))
    } else {
        raw.strip_prefix("unixs://")
            .map(|address| (URL_SCHEME_UNIXS, address))
    };
    if let Some((scheme, address)) = unix {
        if address.is_empty() {
            return Err(format!("URL address must not be empty: {raw}"));
        }
        return Ok((scheme, Cow::Borrowed(address)));
    }

    let Some((scheme, address)) = raw.split_once("://") else {
        let Some(scheme) = supported_service_url_scheme(default_scheme) else {
            if default_scheme.is_empty() {
                return Err(format!("URL scheme must be http, https, unix, or unixs: {raw}"));
            }
            return Err(format!("URL scheme must be http, https, unix, or unixs: {default_scheme}"));
        };
        if split_host_port(raw).is_none() {
            return Err(format!("URL address does not have the form \"host:port\": {raw}"));
        }
        return Ok((scheme, Cow::Borrowed(raw)));
    };

    if let Some(scheme @ (URL_SCHEME_HTTP | URL_SCHEME_HTTPS)) = supported_service_url_scheme(scheme) {
        if let Some(authority) = plain_http_authority(address) {
            return Ok((scheme, Cow::Borrowed(authority)));
        }
    }

    let u = tidb_go_url::parse(raw).map_err(|err| format!("parse url {raw} failed {err}"))?;
    let Some(scheme) = std::str::from_utf8(&u.scheme)
        .ok()
        .and_then(supported_service_url_scheme)
    else {
        return Err(format!("URL scheme must be http, https, unix, or unixs: {raw}"));
    };
    let host = String::from_utf8(u.host)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned());
    let bad_address = || format!("URL address does not have the form \"host:port\": {raw}");
    let (host_part, port) = split_host_port(&host).ok_or_else(bad_address)?;
    if host_part.is_empty() || port.is_empty() {
        return Err(bad_address());
    }
    if !u.path.is_empty() {
        return Err(format!("URL must not contain a path: {raw}"));
    }
    Ok((scheme, Cow::Owned(host)))
}

/// Ordinary DNS names and IPv4 literals need no URL decoding. The caller
/// retains only the authority, but escapes and control bytes still need the
/// full parser for validation.
fn plain_http_authority(address: &str) -> Option<&str> {
    let (host, port) = address.split_once(':')?;
    if host.is_empty()
        || !host.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return None;
    }
    let port_end = port
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(port.len());
    if port_end == 0 {
        return None;
    }
    let suffix = &port[port_end..];
    if !suffix.is_empty()
        && (!suffix.starts_with(['?', '#'])
            || suffix.bytes().any(|b| b == b'%' || b.is_ascii_control()))
    {
        return None;
    }
    Some(&address[..host.len() + 1 + port_end])
}

/// Go `NormalizeServiceURL`: normalizes host:port or supported URL input to
/// scheme://address.
///
/// # Errors
///
/// [`parse_service_url`]'s.
pub fn normalize_service_url(raw: &str, default_scheme: &str) -> Result<String, String> {
    let (scheme, address) = parse_service_url_with(raw, default_scheme)?;
    Ok(format_service_url(scheme, &address))
}

fn format_service_url(scheme: &str, address: &str) -> String {
    let mut output = String::with_capacity(scheme.len() + 3 + address.len());
    output.push_str(scheme);
    output.push_str("://");
    output.push_str(address);
    output
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
            return format_service_url(self.scheme, &self.address);
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
        write!(formatter, "{}://{}", self.scheme, self.address)
    }
}

fn supported_service_url_scheme(scheme: &str) -> Option<&'static str> {
    match scheme {
        URL_SCHEME_HTTP => Some(URL_SCHEME_HTTP),
        URL_SCHEME_HTTPS => Some(URL_SCHEME_HTTPS),
        URL_SCHEME_UNIX => Some(URL_SCHEME_UNIX),
        URL_SCHEME_UNIXS => Some(URL_SCHEME_UNIXS),
        _ => None,
    }
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

    #[test]
    fn only_normalizes_valid_http_authorities() {
        for (input, expected) in [
            ("HTTPS://peer_0.test:00443", "https://peer_0.test:00443"),
            ("https://peer_0.test:00443?source=/test#row", "https://peer_0.test:00443"),
            ("http://[fe80::1%25en0]:80", "http://[fe80::1%en0]:80"),
        ] {
            assert_eq!(normalize_service_url(input, "").unwrap(), expected);
        }
        for input in [
            "https://peer_0.test",
            "https://peer_0.test:443/path",
            "https://peer_0.test:443#%zz",
            "https://peer_0.test:443?query=\u{7f}",
            "https://peer_0.test:status",
        ] {
            assert!(normalize_service_url(input, "").is_err(), "{input}");
        }
    }
}

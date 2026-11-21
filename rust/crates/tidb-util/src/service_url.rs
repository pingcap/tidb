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

    let (scheme, host, path) = split_url(raw)?;
    if !is_supported_service_url_scheme(scheme) {
        return Err(format!("URL scheme must be http, https, unix, or unixs: {raw}"));
    }
    let bad_address = || format!("URL address does not have the form \"host:port\": {raw}");
    let (host_part, port) = split_host_port(host).ok_or_else(bad_address)?;
    if host_part.is_empty() || port.is_empty() {
        return Err(bad_address());
    }
    if !path.is_empty() {
        return Err(format!("URL must not contain a path: {raw}"));
    }
    Ok(ServiceUrl { scheme: scheme.to_owned(), address: host.to_owned() })
}

/// The scheme, `Host` and `Path` Go's `url.Parse` answers for
/// `scheme://[userinfo@]host[/path][?query][#fragment]`.
fn split_url(raw: &str) -> Result<(&str, &str, &str), String> {
    let (scheme, rest) = raw.split_once("://").expect("the caller checked the separator");
    let rest = rest.split('#').next().unwrap_or_default();
    let rest = rest.split('?').next().unwrap_or_default();
    let (authority, path) = match rest.find('/') {
        Some(slash) => rest.split_at(slash),
        None => (rest, ""),
    };
    let host = authority.rsplit_once('@').map_or(authority, |(_, host)| host);
    // Go `url.Parse` (`parseHost`): a bracketed host needs its closing
    // bracket, and whatever follows it is a port.
    if host.starts_with('[') {
        let valid = host
            .find(']')
            .is_some_and(|end| host[end + 1..].is_empty() || host[end + 1..].starts_with(':'));
        if !valid {
            return Err(format!("parse url {raw} failed parse {raw:?}: missing ']' in host"));
        }
    } else if let Some((_, port)) = host.rsplit_once(':') {
        // `validOptionalPort`: digits only.
        if !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(format!("parse url {raw} failed parse {raw:?}: invalid port {:?} after host", format!(":{port}")));
        }
    }
    Ok((scheme, host, path))
}

/// Go `net.SplitHostPort`: `None` where Go answers an error.
fn split_host_port(hostport: &str) -> Option<(&str, &str)> {
    let colon = hostport.rfind(':')?;
    let (host, port) = (&hostport[..colon], &hostport[colon + 1..]);
    if let Some(inner) = host.strip_prefix('[') {
        let inner = inner.strip_suffix(']')?;
        if inner.contains(['[', ']']) {
            return None;
        }
        return (!port.contains(['[', ']'])).then_some((inner, port));
    }
    // An unbracketed host has no colon and no bracket of its own.
    if host.contains([':', '[', ']']) || port.contains(['[', ']']) {
        return None;
    }
    Some((host, port))
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

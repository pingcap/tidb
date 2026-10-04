// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

/// Immutable properties extracted from the verified TLS leaf by the socket owner.
/// This value describes a certificate; it does not itself establish trust.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsPeerIdentity {
    /// Go's MySQL/OpenSSL cipher name, from the negotiated cipher identifier.
    pub cipher: String,
    /// Original ASN.1 name order, using Go's supported /key=value names.
    pub issuer: String,
    /// Original ASN.1 name order, using Go's supported /key=value names.
    pub subject: String,
    /// URI, DNS and IP SANs. Policy alternatives are OR within a type, AND across types.
    pub sans: BTreeMap<String, Vec<String>>,
}

pub(crate) fn parse_policy_sans(raw: &str) -> Result<BTreeMap<String, Vec<String>>, String> {
    let mut result = BTreeMap::<String, Vec<String>>::new();
    for item in raw.split(',') {
        let (kind, value) = item
            .split_once(':')
            .ok_or_else(|| format!("invalid SAN value {item}"))?;
        let kind = kind.trim().to_uppercase();
        if !matches!(kind.as_str(), "URI" | "DNS" | "IP") {
            return Err(format!(
                "unsupported SAN key {kind}, current only support map[DNS:{{}} IP:{{}} URI:{{}}]"
            ));
        }
        result.entry(kind).or_default().push(value.trim().into());
    }
    Ok(result)
}

// Go's wildcard is a whole, nonempty path segment. Never normalize host case,
// collapse dot segments or let a wildcard change authority/query/fragment.
pub(super) fn match_uri_with_wildcard(required: &str, given: &str) -> bool {
    if !required.contains('*') {
        return required == given;
    }
    let (Some(required), Some(given)) = (UriParts::parse(required), UriParts::parse(given)) else {
        return false;
    };
    if required.fixed != given.fixed {
        return false;
    }
    let required: Vec<_> = required.path.split('/').collect();
    let given: Vec<_> = given.path.split('/').collect();
    required.len() == given.len()
        && required
            .iter()
            .zip(given)
            .all(|(r, g)| if *r == "*" { !g.is_empty() } else { *r == g })
}

#[derive(PartialEq, Eq)]
struct UriParts {
    fixed: (
        String,
        String,
        Option<String>,
        String,
        bool,
        bool,
        String,
        String,
    ),
    path: String,
}
impl UriParts {
    fn parse(raw: &str) -> Option<Self> {
        if raw.bytes().any(|b| b < 0x20 || b == 0x7f) {
            return None;
        }
        let (raw, fragment) = raw.split_once('#').unwrap_or((raw, ""));
        let fragment = escaped(fragment, true)?;
        let (scheme, rest) = if let Some((scheme, rest)) = raw
            .split_once(':')
            .filter(|(scheme, _)| !scheme.contains('/'))
        {
            if scheme.is_empty()
                || !scheme.bytes().enumerate().all(|(i, b)| {
                    b.is_ascii_alphabetic()
                        || (i > 0 && (b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.')))
                })
            {
                return None;
            }
            (scheme.to_ascii_lowercase(), rest)
        } else {
            (String::new(), raw)
        };
        let force_query = rest.ends_with('?') && rest.bytes().filter(|b| *b == b'?').count() == 1;
        let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
        if !scheme.is_empty() && !rest.starts_with('/') {
            return Some(Self {
                fixed: (
                    scheme,
                    rest.into(),
                    None,
                    String::new(),
                    false,
                    force_query,
                    query.into(),
                    fragment,
                ),
                path: String::new(),
            });
        }
        let mut host = String::new();
        let mut user = None;
        let mut path = rest;
        let omit_host = !scheme.is_empty() && !rest.starts_with("//");
        if let Some(authority_path) = rest
            .strip_prefix("//")
            .filter(|_| !scheme.is_empty() || !rest.starts_with("///"))
        {
            let end = authority_path.find('/').unwrap_or(authority_path.len());
            let authority = &authority_path[..end];
            path = &authority_path[end..];
            let authority = if let Some((info, host)) = authority.rsplit_once('@') {
                let (name, password) = info
                    .split_once(':')
                    .map_or((info, None), |(n, p)| (n, Some(p)));
                let name = user_component(name)?;
                user = Some(match password {
                    Some(p) => format!("{name}:{}", user_component(p)?),
                    None => name,
                });
                host
            } else {
                authority
            };
            host = parse_host(authority)?;
        } else if scheme.is_empty() && path.split('/').next().is_some_and(|s| s.contains(':')) {
            return None;
        }
        Some(Self {
            fixed: (
                scheme,
                String::new(),
                user,
                host,
                omit_host,
                force_query,
                query.into(),
                fragment,
            ),
            path: escaped(path, false)?,
        })
    }
}
fn host_byte(b: u8) -> bool {
    b >= 128
        || b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'_'
                | b'.'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'['
                | b']'
                | b'<'
                | b'>'
                | b'"'
        )
}
fn unescape_host(raw: &str, zone: bool) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let b = bytes[index];
        if b == b'%' {
            let high = char::from(*bytes.get(index + 1)?).to_digit(16)?;
            let low = char::from(*bytes.get(index + 2)?).to_digit(16)?;
            let decoded = (high * 16 + low) as u8;
            let percent = &bytes[index..index + 3] == b"%25";
            if if zone {
                !percent && decoded != b' ' && !host_byte(decoded)
            } else {
                decoded < 128 && !percent
            } {
                return None;
            }
            index += 3;
        } else {
            if !host_byte(b) {
                return None;
            }
            index += 1;
        }
    }
    String::from_utf8(unescape(raw)?).ok()
}
fn parse_host(raw: &str) -> Option<String> {
    let optional_port = |value: &str| {
        value.is_empty()
            || value
                .strip_prefix(':')
                .is_some_and(|port| port.bytes().all(|b| b.is_ascii_digit()))
    };
    if let Some(open) = raw.rfind('[') {
        if open != 0 {
            return None;
        }
        let close = raw.rfind(']')?;
        let port = &raw[close + 1..];
        if !optional_port(port) {
            return None;
        }
        let hostname = &raw[1..close];
        let hostname = if let Some(zone) = hostname.find("%25") {
            format!(
                "{}{}",
                unescape_host(&hostname[..zone], false)?,
                unescape_host(&hostname[zone..], true)?
            )
        } else {
            unescape_host(hostname, false)?
        };
        let (address, zone) = hostname
            .split_once('%')
            .map_or((hostname.as_str(), None), |(a, z)| (a, Some(z)));
        if zone.is_some_and(str::is_empty) || address.parse::<std::net::Ipv6Addr>().is_err() {
            return None;
        }
        Some(format!("[{hostname}]{}", unescape_host(port, false)?))
    } else {
        if raw.rfind(':').is_some_and(|i| !optional_port(&raw[i..])) {
            return None;
        }
        unescape_host(raw, false)
    }
}

/// Go net/url representation of a certificate URI, before account comparison.
/// Retains escaped path spelling and case-sensitive authority.
pub fn certificate_uri(raw: &str) -> Option<String> {
    let parts = UriParts::parse(raw)?;
    let (scheme, opaque, user, host, omit_host, force_query, query, fragment) = parts.fixed;
    let mut result = String::new();
    if !scheme.is_empty() {
        result.push_str(&scheme);
        result.push(':');
    }
    if !opaque.is_empty() {
        result.push_str(&opaque);
    } else {
        if !scheme.is_empty() || !host.is_empty() || user.is_some() {
            if !(omit_host && host.is_empty() && user.is_none()) {
                if !host.is_empty() || !parts.path.is_empty() || user.is_some() {
                    result.push_str("//");
                }
                if let Some(user) = user {
                    result.push_str(&user);
                    result.push('@');
                }
                for b in host.bytes() {
                    if b < 128 && host_byte(b) {
                        result.push(char::from(b));
                    } else {
                        use std::fmt::Write;
                        write!(result, "%{b:02X}").ok()?;
                    }
                }
            }
        }
        if !parts.path.is_empty() && !parts.path.starts_with('/') && !host.is_empty() {
            result.push('/');
        }
        if result.is_empty()
            && parts
                .path
                .split('/')
                .next()
                .is_some_and(|s| s.contains(':'))
        {
            result.push_str("./");
        }
        result.push_str(&parts.path);
    }
    if force_query || !query.is_empty() {
        result.push('?');
        result.push_str(&query);
    }
    if !fragment.is_empty() {
        result.push('#');
        result.push_str(&fragment);
    }
    Some(result)
}

fn unescape(raw: &str) -> Option<Vec<u8>> {
    let mut result = Vec::new();
    let mut bytes = raw.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let a = char::from(bytes.next()?).to_digit(16)?;
            let b = char::from(bytes.next()?).to_digit(16)?;
            result.push((a * 16 + b) as u8);
        } else {
            result.push(b);
        }
    }
    Some(result)
}
fn escaped(raw: &str, fragment: bool) -> Option<String> {
    let decoded = unescape(raw)?;
    let unreserved = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
    let reserved = |b: u8| {
        matches!(
            b,
            b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@'
        ) || (fragment && matches!(b, b'?' | b'!' | b'(' | b')' | b'*'))
    };
    let valid_raw = raw.bytes().all(|b| {
        unreserved(b)
            || reserved(b)
            || matches!(b, b'!' | b'\'' | b'(' | b')' | b'*' | b'[' | b']' | b'%')
    });
    if valid_raw {
        return Some(raw.into());
    }
    let mut result = String::new();
    for b in decoded {
        if unreserved(b) || reserved(b) {
            result.push(char::from(b));
        } else {
            use std::fmt::Write;
            write!(result, "%{b:02X}").ok()?;
        }
    }
    Some(result)
}

fn user_component(raw: &str) -> Option<String> {
    let mut result = String::new();
    for b in unescape(raw)? {
        if b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b',' | b';' | b'='
            )
        {
            result.push(char::from(b));
        } else {
            use std::fmt::Write;
            write!(result, "%{b:02X}").ok()?;
        }
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_policy_batch_tls_json_matches_go_order_omission_and_escaping() {
        use crate::{privilege::PrivilegeRegistry, Session};
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("testdata/tls-policy-json-oracle.json")).unwrap();
        for (index, case) in cases.as_array().unwrap().iter().enumerate() {
            let registry = PrivilegeRegistry::default();
            let mut session = Session::new();
            session.attach_privileges(registry.clone());
            session.set_user("root@%".into(), "root@127.0.0.1".into());
            let user = format!("encoding{index}");
            session
                .run(&format!(
                    "CREATE USER '{user}'@'%' REQUIRE {}",
                    case["clause"].as_str().unwrap()
                ))
                .unwrap();
            let row = registry
                .global_priv_rows()
                .into_iter()
                .find(|row| row.user == user)
                .unwrap();
            assert_eq!(row.priv_json, case["expected"].as_str().unwrap());
        }
    }

    #[test]
    fn admission_policy_batch_specified_policy_survives_shared_reload_and_rename() {
        use crate::privilege::PrivilegeRegistry;
        let registry = PrivilegeRegistry::default();
        registry.create_user("cert", "%", "");
        let raw = r#"{"ssl_type":3,"ssl_cipher":"TLS_AES_256_GCM_SHA384","x509_issuer":"/CN=ca","x509_subject":"/CN=client","san":"DNS:client,URI:spiffe://domain/ns/*"}"#;
        registry.set_tls_policy("cert", "%", raw);
        let peer = TlsPeerIdentity {
            cipher: "TLS_AES_256_GCM_SHA384".into(),
            issuer: "/CN=ca".into(),
            subject: "/CN=client".into(),
            sans: BTreeMap::from([
                ("DNS".into(), vec!["client".into()]),
                ("URI".into(), vec!["spiffe://domain/ns/default".into()]),
            ]),
        };
        let reloaded = PrivilegeRegistry::default();
        reloaded.create_user("cert", "%", "");
        reloaded.load_global_priv(registry.global_priv_rows());
        assert!(reloaded.admits_account_tls_peer("cert", "127.0.0.1", true, true, Some(&peer)));
        assert!(!reloaded.admits_account_tls_peer("cert", "127.0.0.1", true, false, Some(&peer)));
        assert!(!reloaded.admits_account_tls_peer("cert", "127.0.0.1", false, true, Some(&peer)));
        assert!(!reloaded.admits_account_tls("cert", "127.0.0.1", true, true));
        assert!(reloaded.rename_user("cert", "%", "renamed", "%"));
        assert_eq!(reloaded.global_priv_rows()[0].priv_json, raw);
        assert!(reloaded.admits_account_tls_peer("renamed", "127.0.0.1", true, true, Some(&peer)));
        assert!(reloaded.drop_user("renamed", "%"));
        assert!(reloaded.global_priv_rows().is_empty());
    }

    #[test]
    fn admission_policy_batch_uri_matches_fresh_go_oracle() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("testdata/tls-uri-oracle.json")).unwrap();
        for case in cases.as_array().unwrap() {
            let required = case["required"].as_str().unwrap();
            let given = case["given"].as_str().unwrap();
            let canonical = certificate_uri(given);
            assert_eq!(
                canonical.as_deref(),
                case["canonical"].as_str(),
                "canonical {given:?}"
            );
            assert_eq!(
                match_uri_with_wildcard(required, given),
                case["matches"].as_bool().unwrap(),
                "{required:?} vs {given:?}"
            );
        }
    }
}

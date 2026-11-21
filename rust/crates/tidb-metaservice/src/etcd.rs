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

//! Go `pkg/metaservice/etcd.go`: the PD and etcd endpoints of a keyspace's
//! meta service.
//!
//! `// boundary:` Go's `DialEtcdClient` / `NewEtcdClientFromPDClient` build a
//! `clientv3.Client` over [`resolve_etcd_dial_info`]'s endpoints and
//! namespace. No caller in `pkg/` or `cmd/` uses them (the store driver takes
//! the endpoints through `GetInfoAndGroupAddrs`), so the client construction
//! stays with the caller that owns an etcd client type.

use std::sync::Arc;
use std::time::Duration;

use tikv_client::proto::keyspacepb::KeyspaceMeta;

use crate::{filter_empty_addrs, get_info, uses_global_meta_service_group, Info, MetaServiceError};

/// Go `getAllMembersBackoffMs`.
const GET_ALL_MEMBERS_BACKOFF_MS: u64 = 5000;
/// client-go `BoRegionMiss`: base 2ms, cap 500ms, no jitter.
const BO_REGION_MISS_BASE_MS: u64 = 2;
const BO_REGION_MISS_CAP_MS: u64 = 500;
/// client-go `maxKeyspaceID`.
const MAX_KEYSPACE_ID: u32 = 0xFF_FFFF;

/// Go `pd.Client` as this package reads it.
pub trait PdClient: Send + Sync {
    /// Go `GetAllMembers`: every member's client URLs.
    ///
    /// # Errors
    ///
    /// The RPC's error.
    fn get_all_members(&self) -> Result<Vec<Vec<String>>, String>;
}

/// Go `ServiceClient`: used to request meta service.
pub struct Client {
    pd_cli: Option<Arc<dyn PdClient>>,
}

/// Go `newClient(nil, pdCli)`: `None` without a PD client.
#[must_use]
pub fn new_client(pd_cli: Option<Arc<dyn PdClient>>) -> Option<Client> {
    pd_cli.map(|pd_cli| Client { pd_cli: Some(pd_cli) })
}

impl Client {
    /// Go `GetPDAddrs`: dialable PD endpoints.
    ///
    /// # Errors
    ///
    /// [`get_pd_addrs`]'s.
    pub fn get_pd_addrs(&self) -> Result<Vec<String>, MetaServiceError> {
        get_pd_addrs(self.pd_cli.as_deref(), false)
    }

    /// Go `GetPDServiceURLs`: PD member client URLs with the scheme included.
    ///
    /// # Errors
    ///
    /// [`get_pd_addrs`]'s.
    pub fn get_pd_service_urls(&self) -> Result<Vec<String>, MetaServiceError> {
        get_pd_addrs(self.pd_cli.as_deref(), true)
    }
}

/// Go `GetPDAddrs`: dialable PD endpoints from PD members. For http/https
/// members, it returns host:port unless `with_scheme` is true. For
/// unix-family members, it keeps the scheme because the scheme is required
/// for dialing. Malformed member URLs are skipped, and an error is returned
/// only when no usable PD client URL remains.
///
/// # Errors
///
/// No PD client, the members RPC failing past its backoff, or no usable URL.
pub fn get_pd_addrs(pd_client: Option<&dyn PdClient>, with_scheme: bool) -> Result<Vec<String>, MetaServiceError> {
    let pd_client = pd_client.ok_or_else(|| MetaServiceError::Other("PD client not found".to_owned()))?;
    // Go `tikv.NewBackoffer(ctx, getAllMembersBackoffMs)` with
    // `BoRegionMiss`: the RPC is retried until the backoffer has slept its
    // budget. `// boundary:` the error then is the RPC's own, where
    // client-go's backoffer wraps it in its max-sleep report.
    let (mut slept_ms, mut next_ms) = (0_u64, BO_REGION_MISS_BASE_MS);
    let members = loop {
        match pd_client.get_all_members() {
            Ok(members) => break members,
            Err(error) => {
                if slept_ms >= GET_ALL_MEMBERS_BACKOFF_MS {
                    return Err(MetaServiceError::Other(error));
                }
                std::thread::sleep(Duration::from_millis(next_ms));
                slept_ms += next_ms;
                next_ms = (next_ms * 2).min(BO_REGION_MISS_CAP_MS);
            }
        }
    };
    let pd_addrs: Vec<String> = members
        .iter()
        .flatten()
        .filter_map(|client_url| tidb_util::service_url::parse_service_url(client_url).ok())
        .map(|endpoint| endpoint.endpoint(with_scheme))
        .collect();
    if pd_addrs.is_empty() {
        return Err(MetaServiceError::Other("no usable PD client URL found in PD members".to_owned()));
    }
    Ok(pd_addrs)
}

/// Go `ParseURL`: parses the given URL to get a dialable endpoint, as its
/// scheme prefix and address.
///
/// # Errors
///
/// The URL is not a supported service URL.
pub fn parse_url(raw_url: &str) -> Result<(String, String), String> {
    let endpoint = tidb_util::service_url::parse_service_url(raw_url)?;
    Ok((endpoint.scheme_prefix(), endpoint.address().to_owned()))
}

/// Go `FetchInfo`: loads meta service info for the keyspace using the PD
/// client.
///
/// # Errors
///
/// [`get_pd_addrs`]'s and [`get_info`]'s.
pub fn fetch_info(pd_client: Option<&dyn PdClient>, keyspace_meta: Option<&KeyspaceMeta>) -> Result<Info, MetaServiceError> {
    let pd_addrs = get_pd_addrs(pd_client, false)?;
    get_info(keyspace_meta, &pd_addrs)
}

/// Go `GetInfoAndGroupAddrs`: loads meta service info and returns resolved
/// group addresses.
///
/// # Errors
///
/// [`fetch_info`]'s.
pub fn get_info_and_group_addrs(
    pd_client: Option<&dyn PdClient>,
    keyspace_meta: Option<&KeyspaceMeta>,
) -> Result<(Info, Vec<String>), MetaServiceError> {
    let info = fetch_info(pd_client, keyspace_meta)?;
    let group_addrs = info.group_addrs().to_vec();
    Ok((info, group_addrs))
}

/// Go `etcdDialInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EtcdDialInfo {
    /// The meta service group's addresses.
    pub endpoints: Vec<String>,
    /// The keyspace's etcd namespace, empty without a keyspace.
    pub namespace: String,
}

/// Go `resolveEtcdDialInfo`: the target meta service group's endpoints and
/// the keyspace's namespace. `caller_pd_addrs` are used when the keyspace
/// does not have a dedicated meta service group.
///
/// # Errors
///
/// [`get_pd_addrs`]'s, [`get_info`]'s, or client-go `NewCodecV2`'s.
pub fn resolve_etcd_dial_info(
    pd_cli: Option<&dyn PdClient>,
    keyspace_meta: Option<&KeyspaceMeta>,
    caller_pd_addrs: &[String],
) -> Result<EtcdDialInfo, MetaServiceError> {
    let mut pd_addrs = filter_empty_addrs(caller_pd_addrs);
    if pd_addrs.is_empty() && uses_global_meta_service_group(keyspace_meta) {
        pd_addrs = get_pd_addrs(pd_cli, false)?;
    }

    let info = get_info(keyspace_meta, &pd_addrs)?;

    let mut dial_info = EtcdDialInfo { endpoints: info.group_addrs().to_vec(), namespace: String::new() };
    if let Some(keyspace_meta) = keyspace_meta {
        let codec = new_codec_v2(keyspace_meta)?;
        dial_info.namespace = tidb_keyspace::make_keyspace_etcd_namespace(&codec);
    }
    Ok(dial_info)
}

/// client-go `codecV2` as `MakeKeyspaceEtcdNamespace` reads it.
struct CodecV2(u32);

impl tidb_keyspace::KeyspaceCodec for CodecV2 {
    fn is_api_v1(&self) -> bool {
        false
    }
    fn keyspace_id(&self) -> u32 {
        self.0
    }
}

/// client-go `NewCodecV2`'s refusals.
fn new_codec_v2(keyspace_meta: &KeyspaceMeta) -> Result<CodecV2, MetaServiceError> {
    use tikv_client::proto::keyspacepb::keyspace_meta::Keyspace;
    let keyspace_id = match &keyspace_meta.keyspace {
        Some(Keyspace::KeyspaceIdentity(_)) => {
            return Err(MetaServiceError::Other(
                "unsupported keyspace identity: codec V2 only supports the numeric keyspace ID".to_owned(),
            ));
        }
        Some(Keyspace::Id(id)) => *id,
        None => 0,
    };
    if keyspace_id > MAX_KEYSPACE_ID {
        return Err(MetaServiceError::Other(format!(
            "keyspaceID {keyspace_id} is out of range, maximum is {MAX_KEYSPACE_ID}"
        )));
    }
    Ok(CodecV2(keyspace_id))
}

#[cfg(test)]
mod tests {
    use tikv_client::proto::keyspacepb::keyspace_meta::Keyspace;

    use super::*;
    use crate::{GROUP_ADDRS_KEY, GROUP_ID_KEY};

    struct MockPdClient {
        members: Vec<Vec<String>>,
    }

    impl PdClient for MockPdClient {
        fn get_all_members(&self) -> Result<Vec<Vec<String>>, String> {
            Ok(self.members.clone())
        }
    }

    fn mock(client_urls: &[&str]) -> Arc<dyn PdClient> {
        Arc::new(MockPdClient { members: vec![client_urls.iter().map(|url| (*url).to_owned()).collect()] })
    }

    fn keyspace(id: u32, name: &str, config: &[(&str, &str)]) -> KeyspaceMeta {
        KeyspaceMeta {
            keyspace: Some(Keyspace::Id(id)),
            name: name.to_owned(),
            config: config.iter().map(|(key, value)| ((*key).to_owned(), (*value).to_owned())).collect(),
            ..Default::default()
        }
    }

    /// Go `TestGetPDAddrsPDOnlyClient`.
    #[test]
    fn test_get_pd_addrs_pd_only_client() {
        let service_client = new_client(Some(mock(&["http://127.0.0.1:1111"]))).expect("a PD-only client");
        assert_eq!(service_client.get_pd_addrs().unwrap(), ["127.0.0.1:1111"]);
        assert_eq!(service_client.get_pd_service_urls().unwrap(), ["http://127.0.0.1:1111"]);

        let unix_service_client = new_client(Some(mock(&["unix://localhost:m0"]))).unwrap();
        assert_eq!(unix_service_client.get_pd_addrs().unwrap(), ["unix://localhost:m0"]);
        assert_eq!(unix_service_client.get_pd_service_urls().unwrap(), ["unix://localhost:m0"]);

        // dedicated meta service group ignores pd member urls
        {
            let pd_cli = mock(&["http://127.0.0.1"]);
            let keyspace_meta = keyspace(
                42,
                "ks1",
                &[("gc_management_type", "keyspace_level"), (GROUP_ID_KEY, "group1"), (GROUP_ADDRS_KEY, "meta-service:2379")],
            );
            let dial_info = resolve_etcd_dial_info(Some(pd_cli.as_ref()), Some(&keyspace_meta), &[]).unwrap();
            assert_eq!(dial_info.endpoints, ["meta-service:2379"]);
            assert!(!dial_info.namespace.is_empty());
        }

        // global meta service group uses caller provided endpoints
        {
            let pd_cli = mock(&["http://127.0.0.1"]);
            let keyspace_meta = keyspace(43, "ks2", &[("gc_management_type", "keyspace_level")]);
            let dial_info =
                resolve_etcd_dial_info(Some(pd_cli.as_ref()), Some(&keyspace_meta), &["pd-proxy:2379".to_owned()])
                    .unwrap();
            assert_eq!(dial_info.endpoints, ["pd-proxy:2379"]);
            assert!(!dial_info.namespace.is_empty());
        }
    }

    /// Go `TestNewClientReturnsNilWithoutClients`.
    #[test]
    fn test_new_client_returns_nil_without_clients() {
        assert!(new_client(None).is_none());
    }

    /// Go `TestParseURL`.
    #[test]
    fn test_parse_url() {
        // rawURL, prefix, address, err
        let tests = [
            // Successful test cases
            ("http://example.com:8080", "http://", "example.com:8080", false),
            ("https://localhost:443", "https://", "localhost:443", false),
            ("http://[2001:db8::1]:2379", "http://", "[2001:db8::1]:2379", false),
            ("https://[2001:db8::1]:443", "https://", "[2001:db8::1]:443", false),
            // Unsuccessful test cases
            ("ftp://example.com", "ftp://", "", true), // Invalid prefix
            ("unix://localhost:m0", "unix://", "localhost:m0", false),
            ("unix:///tmp/etcd.sock", "unix://", "/tmp/etcd.sock", false),
            ("unix://", "unix://", "", true),
            ("http://example.com:8080:extra", "http://", "", true), // Extra part after port
            ("https://:8080", "https://", "", true),                // Missing host
            ("http://", "http://", "", true),                       // Incomplete URL
            ("https://example.com", "https://", "", true),          // Missing port
            ("http://localhost", "http://", "", true),              // Missing port
            ("https://[2001:db8::1]", "https://", "", true),        // Missing port
            ("http://2001:db8::1:2379", "http://", "", true),       // Unbracketed IPv6 with port
            ("https://[2001:db8::1", "https://", "", true),         // Invalid bracketed IPv6
        ];
        for (raw_url, prefix, address, err) in tests {
            match parse_url(raw_url) {
                Err(_) => assert!(err, "Did not expect an error for input: {raw_url}"),
                Ok((got_prefix, got_address)) => {
                    assert!(!err, "Expected an error for input: {raw_url}");
                    assert_eq!(got_prefix, prefix, "prefix mismatch for input: {raw_url}");
                    assert_eq!(got_address, address, "address mismatch for input: {raw_url}");
                }
            }
        }
    }
}

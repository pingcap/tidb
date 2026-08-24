// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::time::Duration;

use serde_derive::Deserialize;
use serde_derive::Serialize;

mod client_go;

pub use client_go::*;

/// The configuration for either a [`RawClient`](crate::RawClient) or a
/// [`TransactionClient`](crate::TransactionClient).
///
/// See also [`TransactionOptions`](crate::TransactionOptions) which provides more ways to configure
/// requests.
///
/// This struct is marked `#[non_exhaustive]` to allow adding new configuration options in the
/// future without breaking downstream code. Construct it via [`Config::default`] and then use the
/// `with_*` methods (or field assignment) to customize it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub struct Config {
    pub committer_concurrency: usize,
    pub max_txn_ttl: u64,
    pub tikv_client: TiKvClient,
    pub security: Security,
    pub pd_client: PdClient,
    pub pessimistic_txn: PessimisticTxn,
    pub stores_refresh_interval: u64,
    pub open_tracing_enable: bool,
    pub path: String,
    pub enable_forwarding: bool,
    pub txn_scope: String,
    pub enable_async_commit: bool,
    pub enable_1pc: bool,
    pub regions_refresh_interval: u64,
    pub enable_preload: bool,
    pub enable_async_batch_get: bool,
    pub zone_label: String,

    // Native client-rust connection options retained for API compatibility.
    pub ca_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub timeout: Duration,
    pub grpc_max_decoding_message_size: usize,
    pub keyspace: Option<String>,
    /// API version used by [`RawClient`](crate::RawClient).
    ///
    /// API V2 additionally requires a keyspace and is selected by
    /// [`Config::with_keyspace`] when this remains [`RawApiVersion::V1`].
    /// This field is ignored by transactional clients.
    pub raw_api_version: RawApiVersion,
    /// Optional client-side serialization for optimistic transactions.
    #[serde(skip)]
    pub txn_local_latches: TxnLocalLatches,
}

/// Configuration for client-side transaction latches.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TxnLocalLatches {
    pub enabled: bool,
    pub capacity: usize,
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024; // 4MB

impl Default for Config {
    fn default() -> Self {
        Config {
            committer_concurrency: 128,
            max_txn_ttl: 60 * 60 * 1000,
            tikv_client: TiKvClient::default(),
            security: Security::default(),
            pd_client: PdClient::default(),
            pessimistic_txn: PessimisticTxn::default(),
            stores_refresh_interval: DEF_STORES_REFRESH_INTERVAL,
            open_tracing_enable: false,
            path: String::new(),
            enable_forwarding: false,
            txn_scope: String::new(),
            enable_async_commit: false,
            enable_1pc: false,
            regions_refresh_interval: 0,
            enable_preload: false,
            enable_async_batch_get: false,
            zone_label: String::new(),
            ca_path: None,
            cert_path: None,
            key_path: None,
            timeout: DEFAULT_REQUEST_TIMEOUT,
            grpc_max_decoding_message_size: DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE,
            keyspace: None,
            raw_api_version: RawApiVersion::V1,
            txn_local_latches: TxnLocalLatches::default(),
        }
    }
}

impl Config {
    /// Set the certificate authority, certificate, and key locations for clients.
    ///
    /// By default, this client will use an insecure connection over instead of one protected by
    /// Transport Layer Security (TLS). Your deployment may have chosen to rely on security measures
    /// such as a private network, or a VPN layer to provide secure transmission.
    ///
    /// To use a TLS secured connection, use the `with_security` function to set the required
    /// parameters.
    ///
    /// TiKV does not currently offer encrypted storage (or encryption-at-rest).
    ///
    /// # Examples
    /// ```rust
    /// # use tikv_client::Config;
    /// let config = Config::default().with_security("root.ca", "internal.cert", "internal.key");
    /// ```
    #[must_use]
    pub fn with_security(
        mut self,
        ca_path: impl Into<PathBuf>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        let ca_path = ca_path.into();
        let cert_path = cert_path.into();
        let key_path = key_path.into();
        self.security = Security::new(
            ca_path.to_string_lossy(),
            cert_path.to_string_lossy(),
            key_path.to_string_lossy(),
            Vec::new(),
        );
        self.ca_path = Some(ca_path);
        self.cert_path = Some(cert_path);
        self.key_path = Some(key_path);
        self
    }

    /// Set the timeout for clients.
    ///
    /// The timeout is used for all requests when using or connecting to a TiKV cluster (including
    /// PD nodes). If the request does not complete within timeout, the request is cancelled and
    /// an error returned to the user.
    ///
    /// The default timeout is two seconds.
    ///
    /// # Examples
    /// ```rust
    /// # use tikv_client::Config;
    /// # use std::time::Duration;
    /// let config = Config::default().with_timeout(Duration::from_secs(10));
    /// ```
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum decoding message size for gRPC.
    #[must_use]
    pub fn with_grpc_max_decoding_message_size(mut self, size: usize) -> Self {
        self.grpc_max_decoding_message_size = size;
        self
    }

    /// Set to use default keyspace.
    ///
    /// Server should enable `storage.api-version = 2` to use this feature.
    #[must_use]
    pub fn with_default_keyspace(self) -> Self {
        self.with_keyspace("DEFAULT")
    }

    /// Set the use keyspace for the client.
    ///
    /// Server should enable `storage.api-version = 2` to use this feature.
    #[must_use]
    pub fn with_keyspace(mut self, keyspace: &str) -> Self {
        self.keyspace = Some(keyspace.to_owned());
        self
    }

    /// Select the API version for a raw client.
    ///
    /// [`RawApiVersion::V1Ttl`] is the legacy TTL-enabled RawKV mode. It
    /// keeps V1 key encoding and request contexts and ignores a configured
    /// keyspace, matching client-go's `rawkv.WithAPIVersion(APIVersion_V1TTL)`. To select API V2
    /// explicitly, use [`RawApiVersion::V2`]; without a configured keyspace it
    /// resolves PD's canonical `DEFAULT` keyspace.
    #[must_use]
    pub fn with_raw_api_version(mut self, api_version: RawApiVersion) -> Self {
        self.raw_api_version = api_version;
        self
    }

    /// Enable client-side latches for optimistic transactions.
    #[must_use]
    pub fn with_txn_local_latches(mut self, capacity: usize) -> Self {
        self.txn_local_latches = TxnLocalLatches {
            enabled: true,
            capacity,
        };
        self
    }
}

/// RawKV API version selected when constructing a [`RawClient`](crate::RawClient).
///
/// This is deliberately separate from the generated protobuf enum: `V1TTL`
/// is meaningful only for RawKV, while transactional clients always use V1
/// or API V2 keyspaces.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RawApiVersion {
    /// Legacy RawKV/TxnKV API V1. This is the default.
    #[default]
    V1,
    /// Legacy RawKV API V1 with server-side TTL value encoding.
    V1Ttl,
    /// API V2 RawKV; use a configured or default API V2 keyspace.
    V2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_api_version_defaults_to_v1_and_is_configurable() {
        assert_eq!(Config::default().raw_api_version, RawApiVersion::V1);
        assert_eq!(
            Config::default()
                .with_raw_api_version(RawApiVersion::V1Ttl)
                .raw_api_version,
            RawApiVersion::V1Ttl
        );
    }
}

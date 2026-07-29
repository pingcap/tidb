// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cluster-facing gRPC transport security, transcreated from client-go's
//! `config.Security` / `Security.ToTLSConfig` (security.go) and TiDB's
//! `[security]` config keys (`cluster-ssl-ca` / `cluster-ssl-cert` /
//! `cluster-ssl-key` / `cluster-verify-cn` in `pkg/config/config.go`).
//!
//! This is the single place TLS credentials are built, so that every
//! endpoint builder in the PD and TiKV transports (and the etcd notifier)
//! routes through one helper and no transport is left plaintext by omission.
//!
//! Backward-compatibility contract, matching Go: TLS is opt-in. When no CA
//! path is configured, `ToTLSConfig` returns nil in Go and this helper builds
//! a plaintext `http://` endpoint here. TLS engages only when a CA is set,
//! exactly like `Security.ToTLSConfig` short-circuiting on an empty
//! `ClusterSSLCA`.

use std::fmt;

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

/// Cluster transport security, mirroring the fields TiDB's `[security]`
/// section threads into client-go's `config.Security`.
///
/// `verify_cn` carries the `cluster-verify-cn` allowlist for parity with Go.
/// It is the set of client common names a gRPC *server* accepts; an outbound
/// client (this crate) does not enforce it, exactly as client-go's
/// `ToTLSConfig` builds outbound credentials without consulting `VerifyCN`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterSecurity {
    ca_path: String,
    cert_path: String,
    key_path: String,
    verify_cn: Vec<String>,
}

/// Failure building TLS credentials from the configured PEM paths.
#[derive(Debug)]
pub enum TlsConfigError {
    /// A configured PEM file could not be read.
    ReadFile {
        /// Which path failed (CA, cert, or key).
        path: String,
        /// Underlying I/O message.
        message: String,
    },
    /// tonic rejected the assembled TLS configuration or the derived
    /// `https://` endpoint URI.
    InvalidTls {
        /// tonic's error message.
        message: String,
    },
}

impl fmt::Display for TlsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadFile { path, message } => {
                write!(formatter, "cannot read TLS material {path}: {message}")
            }
            Self::InvalidTls { message } => {
                write!(formatter, "invalid cluster TLS configuration: {message}")
            }
        }
    }
}

impl std::error::Error for TlsConfigError {}

impl ClusterSecurity {
    /// Plaintext security: no CA, cert, or key. Transports stay on `http://`.
    #[must_use]
    pub fn plaintext() -> Self {
        Self::default()
    }

    /// Go `config.NewSecurity`: the four `[security]` cluster fields.
    #[must_use]
    pub fn new(
        ca_path: String,
        cert_path: String,
        key_path: String,
        verify_cn: Vec<String>,
    ) -> Self {
        Self {
            ca_path,
            cert_path,
            key_path,
            verify_cn,
        }
    }

    /// The configured CA path (empty when plaintext).
    #[must_use]
    pub fn ca_path(&self) -> &str {
        &self.ca_path
    }

    /// The configured client-certificate path (empty when unset).
    #[must_use]
    pub fn cert_path(&self) -> &str {
        &self.cert_path
    }

    /// The configured client-key path (empty when unset).
    #[must_use]
    pub fn key_path(&self) -> &str {
        &self.key_path
    }

    /// The `cluster-verify-cn` allowlist, carried for parity with Go.
    #[must_use]
    pub fn verify_cn(&self) -> &[String] {
        &self.verify_cn
    }

    /// Whether cluster TLS is engaged.
    ///
    /// Go's `Security.ToTLSConfig` returns a nil config (plaintext) unless
    /// `ClusterSSLCA` is set, so a non-empty CA path is the single switch.
    #[must_use]
    pub fn is_tls_enabled(&self) -> bool {
        !self.ca_path.is_empty()
    }

    /// Builds the tonic client TLS config, or `None` when plaintext.
    ///
    /// The CA becomes the root store; when both a client cert and key are set,
    /// they become the mutual-TLS identity, exactly as `ToTLSConfig` loads the
    /// key pair only when both `ClusterSSLCert` and `ClusterSSLKey` are set.
    pub fn client_tls_config(&self) -> Result<Option<ClientTlsConfig>, TlsConfigError> {
        if !self.is_tls_enabled() {
            return Ok(None);
        }
        let ca = read_pem(&self.ca_path)?;
        let mut config = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca));
        if !self.cert_path.is_empty() && !self.key_path.is_empty() {
            let cert = read_pem(&self.cert_path)?;
            let key = read_pem(&self.key_path)?;
            config = config.identity(Identity::from_pem(cert, key));
        }
        Ok(Some(config))
    }
}

fn read_pem(path: &str) -> Result<Vec<u8>, TlsConfigError> {
    std::fs::read(path).map_err(|error| TlsConfigError::ReadFile {
        path: path.to_owned(),
        message: error.to_string(),
    })
}

/// The one shared endpoint builder every transport routes through.
///
/// `address` is a plaintext-shaped `host:port` or `http://host:port` (the form
/// PD membership and store discovery normalize to). When `security` is
/// plaintext the endpoint stays `http://`; when TLS is enabled the scheme is
/// upgraded to `https://` and the CA/identity are attached, so the physical
/// channel is secured while the routing identity keeps its plaintext shape.
pub fn secure_endpoint(
    address: &str,
    security: &ClusterSecurity,
) -> Result<Endpoint, TlsConfigError> {
    let base = if address.contains("://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    };
    match security.client_tls_config()? {
        None => Endpoint::from_shared(base).map_err(invalid_tls),
        Some(tls) => {
            let secured = if let Some(rest) = base.strip_prefix("http://") {
                format!("https://{rest}")
            } else {
                base
            };
            Endpoint::from_shared(secured)
                .map_err(invalid_tls)?
                .tls_config(tls)
                .map_err(invalid_tls)
        }
    }
}

fn invalid_tls<E: fmt::Display>(error: E) -> TlsConfigError {
    TlsConfigError::InvalidTls {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn testdata(name: &str) -> String {
        format!("{}/testdata/tls/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    #[test]
    fn plaintext_when_no_ca_is_configured() {
        let security = ClusterSecurity::plaintext();
        assert!(!security.is_tls_enabled());
        assert!(security.client_tls_config().unwrap().is_none());
        // A plaintext endpoint keeps the http scheme.
        let endpoint = secure_endpoint("127.0.0.1:2379", &security).unwrap();
        assert_eq!(endpoint.uri().scheme_str(), Some("http"));
    }

    #[test]
    fn ca_only_builds_server_verifying_tls() {
        let security =
            ClusterSecurity::new(testdata("ca.crt"), String::new(), String::new(), vec![]);
        assert!(security.is_tls_enabled());
        assert!(security.client_tls_config().unwrap().is_some());
        let endpoint = secure_endpoint("http://127.0.0.1:2379", &security).unwrap();
        assert_eq!(endpoint.uri().scheme_str(), Some("https"));
    }

    #[test]
    fn ca_cert_key_builds_mutual_tls_identity() {
        let security = ClusterSecurity::new(
            testdata("ca.crt"),
            testdata("client.crt"),
            testdata("client.key"),
            vec!["tidb-test-client".to_owned()],
        );
        assert!(security.client_tls_config().unwrap().is_some());
        assert_eq!(security.verify_cn(), ["tidb-test-client"]);
    }

    #[test]
    fn missing_ca_file_is_a_read_error() {
        let security = ClusterSecurity::new(
            testdata("does-not-exist.crt"),
            String::new(),
            String::new(),
            vec![],
        );
        assert!(matches!(
            security.client_tls_config(),
            Err(TlsConfigError::ReadFile { .. })
        ));
    }
}

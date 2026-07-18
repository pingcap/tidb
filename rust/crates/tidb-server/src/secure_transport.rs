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

//! Secure-transport admission policy for the connection/session boundary.
//!
//! TiDB's Go connection path keeps `RequireSecureTransport` as a process-wide
//! policy and applies it after the handshake response has been parsed. A
//! plaintext TCP connection is rejected when the policy is enabled, while a
//! Unix-socket connection and a connection whose TLS was established by the
//! server or by the starter gateway are allowed. This leaf owns only that
//! admission decision; it does not perform a TLS handshake, inspect
//! certificates, parse gateway attributes, or authenticate a password.

use std::fmt;

/// How the connection reached the session admission point.
///
/// The TLS variants are assertions supplied by the transport owner after a
/// successful TLS handshake or after validating the starter gateway's secure
/// connection attribute. Constructing one does not establish or validate TLS
/// itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportKind {
    /// A local Unix-domain socket, exempt from the TCP restriction.
    UnixSocket,
    /// A TCP connection without a TLS or gateway-secure assertion.
    PlainTcp,
    /// A TCP connection whose direct TLS handshake was completed elsewhere.
    DirectTls,
    /// A TCP connection authenticated as secure by the starter gateway.
    GatewayTls,
}

/// Decision returned by the secure-transport policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportDecision {
    /// Session admission may continue to the authentication owner.
    Allowed,
}

/// Error emitted when a plaintext network connection violates the policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecureTransportError {
    /// The connection must use TLS (or an exempt local socket/gateway path).
    Required,
}

impl fmt::Display for SecureTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Required => formatter.write_str(
                "Connections using insecure transport are prohibited while --require_secure_transport=ON",
            ),
        }
    }
}

impl std::error::Error for SecureTransportError {}

/// Immutable view of TiDB's `RequireSecureTransport` admission setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SecureTransportPolicy {
    require_secure_transport: bool,
}

impl SecureTransportPolicy {
    /// Creates a policy with the source-shaped global setting.
    #[must_use]
    pub const fn new(require_secure_transport: bool) -> Self {
        Self {
            require_secure_transport,
        }
    }

    /// Returns whether plaintext network connections are rejected.
    #[must_use]
    pub const fn require_secure_transport(self) -> bool {
        self.require_secure_transport
    }

    /// Applies the admission rule before authentication/session creation.
    ///
    /// This deliberately accepts only transport facts from the caller. It
    /// never upgrades a connection, validates certificates, or treats an
    /// accepted decision as an authenticated session.
    pub const fn admit(
        self,
        transport: TransportKind,
    ) -> Result<TransportDecision, SecureTransportError> {
        if self.require_secure_transport && matches!(transport, TransportKind::PlainTcp) {
            Err(SecureTransportError::Required)
        } else {
            Ok(TransportDecision::Allowed)
        }
    }
}

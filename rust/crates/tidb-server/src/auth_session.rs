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

//! Session-facing authentication attempt state.
//!
//! Go opens a session and then passes the client identity, selected plugin,
//! authentication bytes, and handshake salt to `session.Auth`.  That method
//! performs identity lookup, password/plugin verification, account-lock
//! bookkeeping, and finally publishes an authenticated session.  This leaf
//! owns only the dependency-closed input envelope and the one transport rule
//! that precedes that call: `auth_socket` is valid only on a Unix socket.  It
//! deliberately stops at `PendingVerification`; it never hashes a password,
//! reads privilege tables, performs plugin callbacks, or claims success.

use std::fmt;

use crate::secure_transport::{SecureTransportError, SecureTransportPolicy, TransportKind};

/// MySQL/TiDB's Unix-socket authentication plugin name.
pub const AUTH_SOCKET_PLUGIN: &str = "auth_socket";

/// Opaque inputs passed by the connection owner to the authentication owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthChallenge {
    username: String,
    host: String,
    auth_plugin: String,
    authentication: Vec<u8>,
    salt: Vec<u8>,
}

impl AuthChallenge {
    /// Retains the exact connection-phase values without interpreting bytes.
    #[must_use]
    pub fn new(
        username: impl Into<String>,
        host: impl Into<String>,
        auth_plugin: impl Into<String>,
        authentication: impl Into<Vec<u8>>,
        salt: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            username: username.into(),
            host: host.into(),
            auth_plugin: auth_plugin.into(),
            authentication: authentication.into(),
            salt: salt.into(),
        }
    }

    /// The client username supplied to the identity owner.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The peer host selected by the connection owner.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The selected authentication plugin name.
    #[must_use]
    pub fn auth_plugin(&self) -> &str {
        &self.auth_plugin
    }

    /// Opaque client authentication bytes; no password interpretation occurs.
    #[must_use]
    pub fn authentication(&self) -> &[u8] {
        &self.authentication
    }

    /// Opaque handshake salt retained for the eventual verifier.
    #[must_use]
    pub fn salt(&self) -> &[u8] {
        &self.salt
    }

    /// Source-shaped password-presence flag used for access-denied reporting.
    #[must_use]
    pub const fn has_password(&self) -> bool {
        !self.authentication.is_empty()
    }
}

/// Reasons an authentication attempt can be rejected before verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthRejectionReason {
    /// `auth_socket` cannot authenticate a network connection.
    SocketPluginRequiresUnixSocket,
    /// An external identity/plugin verifier rejected the opaque inputs.
    ExternalVerifier,
}

impl fmt::Display for AuthRejectionReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SocketPluginRequiresUnixSocket => {
                formatter.write_str("auth_socket requires a Unix socket")
            }
            Self::ExternalVerifier => {
                formatter.write_str("external authentication verifier rejected the attempt")
            }
        }
    }
}

/// Errors raised while constructing the session-facing attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthSessionError {
    /// The transport policy rejected a plaintext network connection.
    SecureTransport(SecureTransportError),
    /// The auth socket plugin was selected on a non-Unix transport.
    SocketPluginRequiresUnixSocket,
}

impl fmt::Display for AuthSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SecureTransport(error) => std::fmt::Display::fmt(error, formatter),
            Self::SocketPluginRequiresUnixSocket => {
                formatter.write_str("auth_socket requires a Unix socket")
            }
        }
    }
}

impl std::error::Error for AuthSessionError {}

/// The only states this leaf can publish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthSessionState {
    /// Inputs are ready for the external identity/plugin/password owner.
    PendingVerification(AuthChallenge),
    /// A connection-phase policy rejected the attempt before verification.
    Rejected(AuthRejectionReason),
}

/// Owns one session-facing authentication attempt up to verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthSessionAttempt {
    state: AuthSessionState,
}

impl AuthSessionAttempt {
    /// Applies the source transport/plugin admission rule and retains inputs.
    pub fn begin(
        transport: TransportKind,
        challenge: AuthChallenge,
    ) -> Result<Self, AuthSessionError> {
        if challenge.auth_plugin() == AUTH_SOCKET_PLUGIN
            && !matches!(transport, TransportKind::UnixSocket)
        {
            return Err(AuthSessionError::SocketPluginRequiresUnixSocket);
        }
        Ok(Self {
            state: AuthSessionState::PendingVerification(challenge),
        })
    }

    /// Applies secure-transport admission before the plugin/socket rule.
    ///
    /// The policy receives only transport facts established by an outer
    /// listener. This method still stops at [`AuthSessionState::PendingVerification`];
    /// it does not perform TLS or password verification.
    pub fn begin_with_policy(
        policy: SecureTransportPolicy,
        transport: TransportKind,
        challenge: AuthChallenge,
    ) -> Result<Self, AuthSessionError> {
        policy
            .admit(transport)
            .map_err(AuthSessionError::SecureTransport)?;
        Self::begin(transport, challenge)
    }

    /// Returns the current pre-verification state.
    #[must_use]
    pub const fn state(&self) -> &AuthSessionState {
        &self.state
    }

    /// Returns the opaque challenge while verification is pending.
    #[must_use]
    pub const fn challenge(&self) -> Option<&AuthChallenge> {
        match &self.state {
            AuthSessionState::PendingVerification(challenge) => Some(challenge),
            AuthSessionState::Rejected(_) => None,
        }
    }

    /// Whether the attempt still awaits an external verifier.
    #[must_use]
    pub const fn is_pending_verification(&self) -> bool {
        matches!(self.state, AuthSessionState::PendingVerification(_))
    }

    /// Records a pre-verification rejection without inventing an auth result.
    #[must_use]
    pub fn reject(mut self, reason: AuthRejectionReason) -> Self {
        self.state = AuthSessionState::Rejected(reason);
        self
    }
}

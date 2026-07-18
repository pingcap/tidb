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

/// Address and connection generation attached to an RPC failure.
///
/// This is the transport-neutral projection of client-go's `ErrConn`. Tonic
/// status types remain private to the concrete RPC leaf.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectUnaryConnectionError {
    /// Target address selected by the caller.
    pub address: String,
    /// Address-local connection generation.
    pub version: u64,
    /// Concrete connection or RPC failure text.
    pub message: String,
}

impl std::fmt::Display for DirectUnaryConnectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "TiKV connection {} version {} failed: {}",
            self.address, self.version, self.message
        )
    }
}

impl std::error::Error for DirectUnaryConnectionError {}

/// Typed failures at the KV-owned direct unary boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectUnaryClientError {
    /// The client was closed and may never create another connection.
    Closed,
    /// The supplied address cannot form a plaintext tonic endpoint.
    InvalidAddress {
        /// Original caller-supplied address.
        address: String,
        /// URI validation failure.
        message: String,
    },
    /// The encoded coprocessor body is malformed or already contains context.
    InvalidRequest(String),
    /// A connection generation was selected before the send failed.
    Connection(DirectUnaryConnectionError),
    /// The caller's exact unary deadline elapsed.
    Timeout {
        /// Selected address and connection generation.
        connection: DirectUnaryConnectionError,
        /// Caller timeout in milliseconds.
        timeout_ms: u64,
    },
    /// The local Tokio runtime could not be constructed.
    Runtime(String),
}

impl DirectUnaryClientError {
    /// Stable error family for callers and focused tests.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::InvalidAddress { .. } => "invalid_address",
            Self::InvalidRequest(_) => "invalid_request",
            Self::Connection(_) => "connection",
            Self::Timeout { .. } => "timeout",
            Self::Runtime(_) => "runtime",
        }
    }

    /// Returns address/version identity when a connection was selected.
    #[must_use]
    pub const fn connection(&self) -> Option<&DirectUnaryConnectionError> {
        match self {
            Self::Connection(error)
            | Self::Timeout {
                connection: error, ..
            } => Some(error),
            _ => None,
        }
    }
}

impl std::fmt::Display for DirectUnaryClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => formatter.write_str("TiKV RPC client is closed"),
            Self::InvalidAddress { address, message } => {
                write!(formatter, "invalid TiKV address {address:?}: {message}")
            }
            Self::InvalidRequest(message) => write!(formatter, "invalid TiKV request: {message}"),
            Self::Connection(error) => error.fmt(formatter),
            Self::Timeout {
                connection,
                timeout_ms,
            } => write!(
                formatter,
                "TiKV connection {} version {} timed out after {timeout_ms}ms",
                connection.address, connection.version
            ),
            Self::Runtime(message) => {
                write!(formatter, "cannot create TiKV RPC runtime: {message}")
            }
        }
    }
}

impl std::error::Error for DirectUnaryClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connection(error)
            | Self::Timeout {
                connection: error, ..
            } => Some(error),
            _ => None,
        }
    }
}

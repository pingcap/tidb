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

/// Transport-neutral projection of tonic's gRPC status code.
///
/// Keeping this enum outside the tonic leaf lets DistSQL distinguish protocol
/// status from local transport failure without depending on tonic types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectUnaryGrpcCode {
    /// The operation was cancelled remotely.
    Canceled,
    /// An unspecified failure occurred.
    Unknown,
    /// The request was malformed.
    InvalidArgument,
    /// The RPC deadline elapsed in the remote gRPC stack.
    DeadlineExceeded,
    /// The requested resource was not found.
    NotFound,
    /// The requested resource already exists.
    AlreadyExists,
    /// The caller lacks permission.
    PermissionDenied,
    /// A resource was exhausted.
    ResourceExhausted,
    /// A required precondition failed.
    FailedPrecondition,
    /// The operation was aborted.
    Aborted,
    /// A value was out of range.
    OutOfRange,
    /// The operation is not implemented.
    Unimplemented,
    /// An internal error occurred.
    Internal,
    /// The service is unavailable.
    Unavailable,
    /// Data was lost or corrupted.
    DataLoss,
    /// Authentication failed.
    Unauthenticated,
}

/// Origin class for a selected-address unary transport failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectUnaryTransportClass {
    /// The caller cancelled before transport policy should mutate state.
    CallerCancelled,
    /// The caller-owned local Tokio deadline elapsed.
    LocalDeadline,
    /// The remote gRPC stack returned a structured status.
    RemoteGrpc,
    /// The channel could not become ready or otherwise failed locally.
    Connection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectUnaryTransportFailure {
    LocalDeadline,
    RemoteGrpc(DirectUnaryGrpcCode),
    Connection,
}

/// Address and connection generation attached to an RPC failure.
///
/// This is the transport-neutral projection of client-go's `ErrConn`. Tonic
/// status types remain private to the concrete RPC leaf.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectUnaryConnectionError {
    /// Target address selected by the caller.
    address: String,
    /// Address-local connection generation.
    version: u64,
    failure: DirectUnaryTransportFailure,
    /// Concrete connection or RPC failure text.
    message: String,
}

impl DirectUnaryConnectionError {
    /// Constructs a local channel/readiness failure for a selected generation.
    #[must_use]
    pub fn connection(address: &str, version: u64, message: String) -> Self {
        Self {
            address: address.to_owned(),
            version,
            failure: DirectUnaryTransportFailure::Connection,
            message,
        }
    }

    /// Constructs a caller-owned local deadline failure.
    #[must_use]
    pub fn local_deadline(address: &str, version: u64, message: String) -> Self {
        Self {
            address: address.to_owned(),
            version,
            failure: DirectUnaryTransportFailure::LocalDeadline,
            message,
        }
    }

    /// Constructs a structured remote gRPC failure.
    #[must_use]
    pub fn remote_grpc(
        address: &str,
        version: u64,
        code: DirectUnaryGrpcCode,
        message: String,
    ) -> Self {
        Self {
            address: address.to_owned(),
            version,
            failure: DirectUnaryTransportFailure::RemoteGrpc(code),
            message,
        }
    }

    /// Target address selected by the caller.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Address-local connection generation selected for the failed attempt.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Failure origin without exposing tonic types.
    #[must_use]
    pub const fn transport_class(&self) -> DirectUnaryTransportClass {
        match self.failure {
            DirectUnaryTransportFailure::LocalDeadline => DirectUnaryTransportClass::LocalDeadline,
            DirectUnaryTransportFailure::RemoteGrpc(_) => DirectUnaryTransportClass::RemoteGrpc,
            DirectUnaryTransportFailure::Connection => DirectUnaryTransportClass::Connection,
        }
    }

    /// Exact remote gRPC code, when the failure came from a remote status.
    #[must_use]
    pub const fn grpc_code(&self) -> Option<DirectUnaryGrpcCode> {
        match self.failure {
            DirectUnaryTransportFailure::RemoteGrpc(code) => Some(code),
            _ => None,
        }
    }

    /// Concrete transport failure text.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
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
    /// The caller cancelled before the RPC leaf should mutate transport state.
    CallerCancelled,
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
    /// A bounded local BatchCommands opening slot is already occupied.
    AdmissionBusy {
        /// Physical address whose stream is still opening.
        address: String,
    },
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
            Self::CallerCancelled => "caller_cancelled",
            Self::Closed => "closed",
            Self::InvalidAddress { .. } => "invalid_address",
            Self::InvalidRequest(_) => "invalid_request",
            Self::AdmissionBusy { .. } => "admission_busy",
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

    /// Returns the transport origin without exposing tonic types.
    #[must_use]
    pub const fn transport_class(&self) -> Option<DirectUnaryTransportClass> {
        match self {
            Self::CallerCancelled => Some(DirectUnaryTransportClass::CallerCancelled),
            Self::Connection(error) => Some(error.transport_class()),
            Self::Timeout { .. } => Some(DirectUnaryTransportClass::LocalDeadline),
            _ => None,
        }
    }

    /// Returns the exact remote gRPC status code when one was observed.
    #[must_use]
    pub const fn grpc_code(&self) -> Option<DirectUnaryGrpcCode> {
        match self {
            Self::Connection(error) => error.grpc_code(),
            _ => None,
        }
    }

    /// Whether client-go requires the caller to close exactly this generation.
    ///
    /// Only remote gRPC Canceled has that contract. Ordinary deadline,
    /// unavailable, and connection failures remain open for liveness policy.
    #[must_use]
    pub const fn requires_generation_close(&self) -> bool {
        match self {
            Self::Connection(error) => {
                matches!(
                    error.failure,
                    DirectUnaryTransportFailure::RemoteGrpc(DirectUnaryGrpcCode::Canceled)
                )
            }
            _ => false,
        }
    }
}

impl std::fmt::Display for DirectUnaryClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CallerCancelled => formatter.write_str("TiKV RPC cancelled by caller"),
            Self::Closed => formatter.write_str("TiKV RPC client is closed"),
            Self::InvalidAddress { address, message } => {
                write!(formatter, "invalid TiKV address {address:?}: {message}")
            }
            Self::InvalidRequest(message) => write!(formatter, "invalid TiKV request: {message}"),
            Self::AdmissionBusy { address } => {
                write!(
                    formatter,
                    "TiKV BatchCommands stream {address} is still opening"
                )
            }
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

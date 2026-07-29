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

/// The exact bounded PD RPC operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PdOperation {
    /// Cluster identity bootstrap.
    GetMembers,
    /// Key-based region lookup.
    GetRegion,
    /// Previous-region lookup by an inclusive end key.
    GetPrevRegion,
    /// Region lookup by identity.
    GetRegionById,
    /// Deprecated contiguous region scan.
    ScanRegions,
    /// Ordered multi-range region scan.
    BatchScanRegions,
    /// Store lookup by ID.
    GetStore,
    /// Current timestamp allocation from the PD leader stream.
    Tso,
    /// GC state (txn safe point) lookup.
    GetGcState,
}

impl std::fmt::Display for PdOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GetMembers => formatter.write_str("GetMembers"),
            Self::GetRegion => formatter.write_str("GetRegion"),
            Self::GetPrevRegion => formatter.write_str("GetPrevRegion"),
            Self::GetRegionById => formatter.write_str("GetRegionByID"),
            Self::ScanRegions => formatter.write_str("ScanRegions"),
            Self::BatchScanRegions => formatter.write_str("BatchScanRegions"),
            Self::GetStore => formatter.write_str("GetStore"),
            Self::Tso => formatter.write_str("Tso"),
            Self::GetGcState => formatter.write_str("GetGCState"),
        }
    }
}

/// Typed failures from the bounded PD client and topology projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PdClientError {
    /// A configured or discovered plaintext endpoint is not a valid URI.
    InvalidEndpoint {
        /// Original configured endpoint.
        endpoint: String,
        /// URI validation detail.
        message: String,
    },
    /// The dedicated runtime could not be created.
    Runtime(String),
    /// The worker terminated or was closed.
    Closed,
    /// One direct endpoint attempt failed.
    Transport {
        /// Attempted operation.
        operation: PdOperation,
        /// Attempted endpoint.
        endpoint: String,
        /// Tonic status code identity.
        code: String,
        /// Tonic status message.
        message: String,
    },
    /// One direct endpoint deadline elapsed.
    Timeout {
        /// Attempted operation.
        operation: PdOperation,
        /// Attempted endpoint.
        endpoint: String,
        /// Configured deadline in milliseconds.
        timeout_ms: u64,
    },
    /// A response omitted its required header.
    MissingHeader(PdOperation),
    /// PD returned an application header error.
    HeaderError {
        /// Attempted operation.
        operation: PdOperation,
        /// Source error discriminant.
        error_type: i32,
        /// Source error message.
        message: String,
    },
    /// A response belongs to another cluster.
    ClusterMismatch {
        /// Attempted operation.
        operation: PdOperation,
        /// Bootstrapped cluster identity.
        expected: u64,
        /// Response cluster identity.
        actual: u64,
    },
    /// Bootstrap returned no usable cluster identity.
    ZeroClusterId,
    /// Region/store protobuf topology is malformed or unusable.
    InvalidTopology {
        /// Stable invalid-data identity.
        kind: &'static str,
        /// Source-data detail.
        message: String,
    },
}

impl PdClientError {
    /// Stable error family used by callers and source-derived tests.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::InvalidEndpoint { .. } => "invalid_endpoint",
            Self::Runtime(_) => "runtime",
            Self::Closed => "closed",
            Self::Transport { .. } => "transport",
            Self::Timeout { .. } => "timeout",
            Self::MissingHeader(_) => "missing_header",
            Self::HeaderError { .. } => "header_error",
            Self::ClusterMismatch { .. } => "cluster_mismatch",
            Self::ZeroClusterId => "zero_cluster_id",
            Self::InvalidTopology { kind, .. } => kind,
        }
    }
}

impl std::fmt::Display for PdClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidEndpoint { endpoint, message } => {
                write!(
                    formatter,
                    "invalid plaintext PD endpoint {endpoint:?}: {message}"
                )
            }
            Self::Runtime(message) => write!(formatter, "cannot create PD runtime: {message}"),
            Self::Closed => formatter.write_str("PD client worker is closed"),
            Self::Transport {
                operation,
                endpoint,
                code,
                message,
            } => write!(
                formatter,
                "PD {operation} to {endpoint} failed with {code}: {message}"
            ),
            Self::Timeout {
                operation,
                endpoint,
                timeout_ms,
            } => write!(
                formatter,
                "PD {operation} to {endpoint} timed out after {timeout_ms}ms"
            ),
            Self::MissingHeader(operation) => {
                write!(formatter, "PD {operation} response omitted its header")
            }
            Self::HeaderError {
                operation,
                error_type,
                message,
            } => write!(
                formatter,
                "PD {operation} response error type {error_type}: {message}"
            ),
            Self::ClusterMismatch {
                operation,
                expected,
                actual,
            } => write!(
                formatter,
                "PD {operation} response cluster {actual} does not match {expected}"
            ),
            Self::ZeroClusterId => formatter.write_str("PD returned zero cluster identity"),
            Self::InvalidTopology { kind, message } => {
                write!(formatter, "invalid PD topology ({kind}): {message}")
            }
        }
    }
}

impl std::error::Error for PdClientError {}

/// Typed failures while stopping the unique PD worker lifecycle owner.
///
/// Shutdown is deliberately separate from foreground [`PdClientError`]
/// because a process can finish every request successfully and still fail to
/// prove that its retained worker stopped cleanly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PdClientShutdownError {
    /// A cloneable request handle tried to exercise process lifecycle authority.
    NotOwner,
    /// Explicit shutdown began while request handles were still retained.
    SharedOwners {
        /// Number of live owner and request-handle values.
        owners: usize,
    },
    /// The worker command receiver disappeared before accepting Close.
    CommandSend,
    /// The worker accepted Close but disappeared before acknowledging it.
    MissingAcknowledgement,
    /// The mutex retaining the unique join handle was poisoned.
    WorkerStatePoisoned,
    /// The retained PD worker panicked before or during shutdown.
    WorkerPanicked,
    /// More than one independent shutdown failure was observed.
    Multiple(Vec<Self>),
}

impl PdClientShutdownError {
    /// Stable failure family used by lifecycle callers and tests.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::NotOwner => "not_owner",
            Self::SharedOwners { .. } => "shared_owners",
            Self::CommandSend => "command_send",
            Self::MissingAcknowledgement => "missing_acknowledgement",
            Self::WorkerStatePoisoned => "worker_state_poisoned",
            Self::WorkerPanicked => "worker_panicked",
            Self::Multiple(_) => "multiple",
        }
    }

    pub(crate) fn from_failures(mut failures: Vec<Self>) -> Result<(), Self> {
        match failures.len() {
            0 => Ok(()),
            1 => Err(failures.pop().expect("one shutdown failure")),
            _ => Err(Self::Multiple(failures)),
        }
    }
}

impl std::fmt::Display for PdClientShutdownError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotOwner => formatter.write_str("PD request handle does not own worker shutdown"),
            Self::SharedOwners { owners } => write!(
                formatter,
                "explicit PD shutdown requires drained request handles; observed {owners} live handles"
            ),
            Self::CommandSend => {
                formatter.write_str("PD worker stopped before accepting the shutdown command")
            }
            Self::MissingAcknowledgement => {
                formatter.write_str("PD worker stopped without acknowledging shutdown")
            }
            Self::WorkerStatePoisoned => {
                formatter.write_str("PD worker lifecycle state is poisoned")
            }
            Self::WorkerPanicked => formatter.write_str("PD worker panicked during shutdown"),
            Self::Multiple(failures) => {
                formatter.write_str("multiple PD shutdown failures: ")?;
                for (index, failure) in failures.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str("; ")?;
                    }
                    failure.fmt(formatter)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for PdClientShutdownError {}

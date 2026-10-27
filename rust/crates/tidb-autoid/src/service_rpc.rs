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

//! Go `pkg/meta/autoid`'s AutoID service surface (`autoid_service.go`): the
//! two RPCs a `singlePointAlloc` makes, as the seam its transport and
//! discovery implement.

/// One typed request to Go's AutoID service `AllocAutoID` RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoIdServiceAllocRequest {
    /// Database identity in the meta key.
    pub db_id: i64,
    /// Table identity in the meta key.
    pub table_id: i64,
    /// Number of sequence-ladder values requested.
    pub n: u64,
    /// Session `auto_increment_increment`.
    pub increment: i64,
    /// Session `auto_increment_offset`.
    pub offset: i64,
    /// Whether comparisons use the unsigned 64-bit domain.
    pub unsigned: bool,
    /// TiKV API-v2 keyspace identity; u32::MAX is Nullspace.
    pub keyspace_id: u32,
}

/// One typed request to Go's AutoID service `Rebase` RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoIdServiceRebaseRequest {
    /// Database identity in the meta key.
    pub db_id: i64,
    /// Table identity in the meta key.
    pub table_id: i64,
    /// New global base requested by the caller.
    pub new_base: i64,
    /// Whether the service must set the base even when it moves backwards.
    pub force: bool,
    /// Whether comparisons use the unsigned 64-bit domain.
    pub unsigned: bool,
    /// TiKV API-v2 keyspace identity; u32::MAX is Nullspace.
    pub keyspace_id: u32,
}

/// A transport failure classified before the AutoID retry policy runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoIdServiceRpcError {
    /// A gRPC transport/status failure. Go recognizes this by the brittle
    /// `"rpc error"` substring; the Rust boundary keeps it typed.
    Rpc(String),
    /// A service/discovery failure that retrying a connection cannot repair.
    Other(String),
}

impl std::fmt::Display for AutoIdServiceRpcError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc(message) | Self::Other(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for AutoIdServiceRpcError {}

/// The transport/discovery seam used by [`AutoIdServiceAllocator`].
///
/// Transports own generation-safe retirement across every table binding.
/// The transport retires the exact borrowed generation before returning an RPC error.
pub trait AutoIdServiceRpc: Send + Sync + 'static {
    /// Calls `AutoIDAlloc.AllocAutoID` once.
    fn alloc_auto_id(
        &self,
        call: &crate::AutoIdCall,
        request: AutoIdServiceAllocRequest,
    ) -> Result<(i64, i64), AutoIdServiceRpcError>;

    /// Calls `AutoIDAlloc.Rebase` once.
    fn rebase(
        &self,
        call: &crate::AutoIdCall,
        request: AutoIdServiceRebaseRequest,
    ) -> Result<(), AutoIdServiceRpcError>;
}


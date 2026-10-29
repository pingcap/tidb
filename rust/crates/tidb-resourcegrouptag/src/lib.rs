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

//! Resource-group tag decoding and request-key extraction from
//! `pkg/util/resourcegrouptag`.

use prost::Message;
use tidb_codec::{get_key_kind, KeyKind};
use tidb_proto::{
    CoprocessorBatchRequest, CoprocessorRequest, KvrpcBatchGetRequest, KvrpcBatchRollbackRequest,
    KvrpcCommitRequest, KvrpcGetRequest, KvrpcPessimisticLockRequest, KvrpcPrewriteRequest,
    KvrpcScanRequest, ResourceGroupTag, ResourceGroupTagLabel,
};

/// A malformed protobuf resource-group tag.
#[derive(Debug)]
pub struct ResourceGroupTagDecodeError {
    data: Vec<u8>,
    source: prost::DecodeError,
}

impl ResourceGroupTagDecodeError {
    /// Returns the malformed wire bytes.
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl std::fmt::Display for ResourceGroupTagDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("invalid resource group tag data ")?;
        for byte in &self.data {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ResourceGroupTagDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Decodes a resource-group tag and returns its SQL digest.
pub fn decode_resource_group_tag(
    data: &[u8],
) -> Result<Option<Vec<u8>>, ResourceGroupTagDecodeError> {
    if data.is_empty() {
        return Ok(None);
    }
    ResourceGroupTag::decode(data)
        .map(|tag| tag.sql_digest)
        .map_err(|source| ResourceGroupTagDecodeError {
            data: data.to_vec(),
            source,
        })
}

/// Classifies a key for resource-group attribution.
#[must_use]
pub fn get_resource_group_label_by_key(key: &[u8]) -> ResourceGroupTagLabel {
    match get_key_kind(key) {
        KeyKind::Row => ResourceGroupTagLabel::Row,
        KeyKind::Index => ResourceGroupTagLabel::Index,
        KeyKind::Unknown => ResourceGroupTagLabel::Unknown,
    }
}

/// Returns the first key carried by a request, or an empty slice when absent.
pub trait FirstKeyRequest {
    /// Returns the request's first key.
    fn first_key(&self) -> &[u8];
}

/// Gets a request's first key, returning an empty slice for no request.
#[must_use]
pub fn get_first_key_from_request<R: FirstKeyRequest + ?Sized>(request: Option<&R>) -> &[u8] {
    request.map_or(&[], FirstKeyRequest::first_key)
}

impl FirstKeyRequest for KvrpcGetRequest {
    fn first_key(&self) -> &[u8] {
        &self.key
    }
}

impl FirstKeyRequest for KvrpcBatchGetRequest {
    fn first_key(&self) -> &[u8] {
        self.keys.first().map_or(&[], Vec::as_slice)
    }
}

impl FirstKeyRequest for KvrpcScanRequest {
    fn first_key(&self) -> &[u8] {
        &self.start_key
    }
}

impl FirstKeyRequest for KvrpcPrewriteRequest {
    fn first_key(&self) -> &[u8] {
        self.mutations.first().map_or(&[], |mutation| &mutation.key)
    }
}

impl FirstKeyRequest for KvrpcCommitRequest {
    fn first_key(&self) -> &[u8] {
        self.keys.first().map_or(&[], Vec::as_slice)
    }
}

impl FirstKeyRequest for KvrpcBatchRollbackRequest {
    fn first_key(&self) -> &[u8] {
        self.keys.first().map_or(&[], Vec::as_slice)
    }
}

impl FirstKeyRequest for CoprocessorRequest {
    fn first_key(&self) -> &[u8] {
        self.ranges.first().map_or(&[], |range| &range.start)
    }
}

impl FirstKeyRequest for CoprocessorBatchRequest {
    fn first_key(&self) -> &[u8] {
        self.regions
            .first()
            .and_then(|region| region.ranges.first())
            .map_or(&[], |range| &range.start)
    }
}

impl FirstKeyRequest for KvrpcPessimisticLockRequest {
    fn first_key(&self) -> &[u8] {
        self.mutations.first().map_or(&[], |mutation| &mutation.key)
    }
}

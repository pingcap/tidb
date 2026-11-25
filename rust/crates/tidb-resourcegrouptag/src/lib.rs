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

use prost::encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType};
use prost::Message;
use tidb_proto::{
    CoprocessorBatchRequest, CoprocessorRequest, KvrpcBatchGetRequest, KvrpcBatchRollbackRequest,
    KvrpcCommitRequest, KvrpcGetRequest, KvrpcPessimisticLockRequest, KvrpcPrewriteRequest,
    KvrpcScanRequest, ResourceGroupTag, ResourceGroupTagLabel,
};
use tidb_tablecodec_rowindexcodec::{get_key_kind, KeyKind};

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
        let mut hex = [0_u8; 256];
        for bytes in self.data.chunks(hex.len() / 2) {
            let len = tidb_goruntime::go_hex::encode(&mut hex, bytes);
            formatter.write_str(std::str::from_utf8(&hex[..len]).expect("hex is ASCII"))?;
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
    if let Some(digest) = project_sql_digest(data) {
        return Ok(digest.map(<[u8]>::to_vec));
    }
    // Use the generated decoder for errors, including the protobuf field path.
    ResourceGroupTag::decode(data)
        .map(|tag| tag.sql_digest)
        .map_err(|source| ResourceGroupTagDecodeError {
            data: data.to_vec(),
            source,
        })
}

// The outer Option reports whether the projection could validate the message.
fn project_sql_digest(mut data: &[u8]) -> Option<Option<&[u8]>> {
    let mut digest = None;
    while !data.is_empty() {
        let (tag, wire_type) = decode_key(&mut data).ok()?;
        match tag {
            // sql_digest, plan_digest, and keyspace_name are all bytes.
            1 | 2 | 5 => {
                if wire_type != WireType::LengthDelimited {
                    return None;
                }
                let len = usize::try_from(decode_varint(&mut data).ok()?).ok()?;
                let (value, rest) = data.split_at_checked(len)?;
                if tag == 1 {
                    digest = Some(value);
                }
                data = rest;
            }
            // label and table_id are scalar varints.
            3 | 4 => {
                if wire_type != WireType::Varint {
                    return None;
                }
                decode_varint(&mut data).ok()?;
            }
            _ => skip_field(wire_type, tag, &mut data, DecodeContext::default()).ok()?,
        }
    }
    Some(digest)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projected_digest_matches_generated_tag_decoder() {
        // Non-SQL fields, duplicate SQL digest, and an unknown group.
        let payload = [
            0x2a, 2, 9, 8, 0x0a, 1, 7, 0x18, 2, 0x20, 3, 0x12, 1, 6, 0x0a, 0, 0x33, 0x08, 0, 0x34,
        ];
        let compare = |data: &[u8]| {
            let slow = ResourceGroupTag::decode(data).map(|tag| tag.sql_digest);
            let fast = decode_resource_group_tag(data).map_err(|error| error.source);
            assert_eq!(fast, slow, "{data:02x?}");
        };
        compare(&payload);
        for i in 0..payload.len() {
            compare(&payload[..i]);
            let mut mutated = payload;
            for byte in [0, 1, 7, 0x0a, 0x12, 0x7f, 0x80, 0xff] {
                mutated[i] = byte;
                compare(&mutated);
            }
        }
    }
}

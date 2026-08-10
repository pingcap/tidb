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

//! Resource-group request-tag encoding from `pkg/kv/kv.go`.
//!
//! Request extraction is expressed through a small request trait and table-ID
//! decoding retains Go's replaceable hook, avoiding a `tablecodec -> kv`
//! dependency cycle.

use prost::Message;
use std::sync::{Arc, LazyLock, RwLock};
use tidb_codec::{decode_table_id, get_key_kind, KeyKind};
use tidb_proto::{
    CoprocessorBatchRequest, CoprocessorRequest, KvrpcBatchGetRequest, KvrpcBatchRollbackRequest,
    KvrpcCommitRequest, KvrpcGetRequest, KvrpcPessimisticLockRequest, KvrpcPrewriteRequest,
    KvrpcScanRequest, ResourceGroupTag, ResourceGroupTagLabel,
};

type DecodeTableId = dyn Fn(&[u8]) -> i64 + Send + Sync;

static DECODE_TABLE_ID: LazyLock<RwLock<Arc<DecodeTableId>>> =
    LazyLock::new(|| RwLock::new(Arc::new(decode_table_id)));

/// Replaces the process-wide table-ID decoder used to avoid an import cycle.
pub fn set_decode_table_id(decoder: impl Fn(&[u8]) -> i64 + Send + Sync + 'static) {
    *DECODE_TABLE_ID
        .write()
        .expect("table-id decoder lock poisoned") = Arc::new(decoder);
}

/// Returns the first key carried by a request, or an empty slice when absent.
pub trait FirstKeyRequest {
    /// Returns the request's first key.
    fn first_key(&self) -> &[u8];
}

/// Request mutation consumed by the resource-group tagger.
pub trait ResourceGroupTaggedRequest: FirstKeyRequest {
    /// Replaces the encoded resource-group tag.
    fn set_resource_group_tag(&mut self, tag: Vec<u8>);
}

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
///
/// Empty input is a valid absent tag. A valid tag without a SQL digest also
/// returns `None`, matching the source package's nil result.
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

/// Builds the wire-compatible `tipb.ResourceGroupTag` carried by a KV request.
///
/// `table_id` is always present on the wire, including the default zero, which
/// matches the Go protobuf's non-nullable field.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceGroupTagBuilder {
    sql_digest: Option<Vec<u8>>,
    plan_digest: Option<Vec<u8>>,
    keyspace_name: Option<Vec<u8>>,
    table_id: i64,
}

impl ResourceGroupTagBuilder {
    /// Creates a builder with an optional keyspace name.
    #[must_use]
    pub fn new(keyspace_name: Option<&[u8]>) -> Self {
        Self {
            keyspace_name: keyspace_name
                .filter(|name| !name.is_empty())
                .map(ToOwned::to_owned),
            ..Self::default()
        }
    }

    /// Sets the raw SQL digest bytes, clearing the field for an empty digest.
    pub fn set_sql_digest(&mut self, digest: &[u8]) -> &mut Self {
        self.sql_digest = (!digest.is_empty()).then(|| digest.to_owned());
        self
    }

    /// Sets the raw physical-plan digest bytes, clearing the field when empty.
    pub fn set_plan_digest(&mut self, digest: &[u8]) -> &mut Self {
        self.plan_digest = (!digest.is_empty()).then(|| digest.to_owned());
        self
    }

    /// Sets the table ID explicitly when a caller has a decoded table ID.
    pub fn set_table_id(&mut self, table_id: i64) -> &mut Self {
        self.table_id = table_id;
        self
    }

    /// Encodes a resource-group tag using the first request key, if available.
    ///
    /// A non-empty key produces a label even when its prefix is unknown, as in
    /// `resourcegrouptag.GetResourceGroupLabelByKey`. Legacy table keys also
    /// replace the default table ID with the decoded key ID.
    #[must_use]
    pub fn encode_tag_with_key(&self, key: &[u8]) -> Vec<u8> {
        let label = (!key.is_empty()).then(|| get_resource_group_label_by_key(key) as i32);
        let table_id = if key.is_empty() {
            self.table_id
        } else {
            DECODE_TABLE_ID
                .read()
                .expect("table-id decoder lock poisoned")(key)
        };
        ResourceGroupTag {
            sql_digest: self.sql_digest.clone(),
            plan_digest: self.plan_digest.clone(),
            label,
            table_id: Some(table_id),
            keyspace_name: self.keyspace_name.clone(),
        }
        .encode_to_vec()
    }

    /// Builds and attaches a tag to a request.
    pub fn build<R: ResourceGroupTaggedRequest + ?Sized>(&self, request: Option<&mut R>) {
        let Some(request) = request else {
            return;
        };
        let encoded = self.encode_tag_with_key(get_first_key_from_request(Some(&*request)));
        if !encoded.is_empty() {
            request.set_resource_group_tag(encoded);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_codec::encode_int;

    const ROW_KEY: &[u8] = &[116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 114];
    const INDEX_KEY: &[u8] = &[
        116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 105, 128, 0, 0, 0, 0, 0, 0, 0,
    ];

    fn decode(encoded: &[u8]) -> ResourceGroupTag {
        ResourceGroupTag::decode(encoded).expect("resource-group tag must decode")
    }

    #[test]
    fn resource_group_tag_encoding_matches_go_digest_and_keyspace_vectors() {
        let mut builder = ResourceGroupTagBuilder::new(None);
        let encoded = builder.encode_tag_with_key(&[]);
        assert_eq!(encoded.len(), 2);
        let tag = decode(&encoded);
        assert_eq!(tag.sql_digest, None);
        assert_eq!(tag.plan_digest, None);
        assert_eq!(tag.table_id, Some(0));
        assert_eq!(tag.keyspace_name, None);
        assert_eq!(tag.label, None);

        builder.set_sql_digest(b"aa");
        let encoded = builder.encode_tag_with_key(&[]);
        assert_eq!(encoded.len(), 6);
        assert_eq!(
            decode(&encoded).sql_digest.as_deref(),
            Some(b"aa".as_slice())
        );

        let mut builder = ResourceGroupTagBuilder::new(Some(b"123"));
        builder.set_sql_digest(&[b'a'; 64]);
        let tag = decode(&builder.encode_tag_with_key(&[]));
        assert_eq!(tag.sql_digest.as_deref(), Some(&[b'a'; 64][..]));
        assert_eq!(tag.keyspace_name.as_deref(), Some(b"123".as_slice()));
        assert_eq!(tag.table_id, Some(0));

        let mut builder = ResourceGroupTagBuilder::new(Some(b"tenant-nextgen"));
        builder.set_sql_digest(&[b'a'; 510]);
        let tag = decode(&builder.encode_tag_with_key(&[]));
        assert_eq!(tag.sql_digest.as_deref(), Some(&[b'a'; 510][..]));
        assert_eq!(
            tag.keyspace_name.as_deref(),
            Some(b"tenant-nextgen".as_slice())
        );
    }

    #[test]
    fn resource_group_tag_labels_preserve_row_index_and_unknown_prefixes() {
        let mut builder = ResourceGroupTagBuilder::new(None);
        builder.set_table_id(42);

        let mut nonzero_row_key = vec![b't'];
        encode_int(&mut nonzero_row_key, 42);
        nonzero_row_key.extend_from_slice(b"_r");
        let nonzero_row = decode(&builder.encode_tag_with_key(&nonzero_row_key));
        assert_eq!(nonzero_row.label, Some(ResourceGroupTagLabel::Row as i32));
        assert_eq!(nonzero_row.table_id, Some(42));

        let mut nonzero_index_key = vec![b't'];
        encode_int(&mut nonzero_index_key, 42);
        nonzero_index_key.extend_from_slice(b"_i");
        let nonzero_index = decode(&builder.encode_tag_with_key(&nonzero_index_key));
        assert_eq!(
            nonzero_index.label,
            Some(ResourceGroupTagLabel::Index as i32)
        );
        assert_eq!(nonzero_index.table_id, Some(42));

        let row = decode(&builder.encode_tag_with_key(ROW_KEY));
        assert_eq!(row.label, Some(ResourceGroupTagLabel::Row as i32));
        assert_eq!(row.table_id, Some(0));

        let index = decode(&builder.encode_tag_with_key(INDEX_KEY));
        assert_eq!(index.label, Some(ResourceGroupTagLabel::Index as i32));
        assert_eq!(index.table_id, Some(0));

        let unknown = decode(&builder.encode_tag_with_key(b"opaque"));
        assert_eq!(unknown.label, Some(ResourceGroupTagLabel::Unknown as i32));
        assert_eq!(unknown.table_id, Some(0));

        let empty = decode(&builder.encode_tag_with_key(&[]));
        assert_eq!(empty.label, None);
        assert_eq!(empty.table_id, Some(42));
    }

    #[test]
    fn resource_group_tag_digest_fields_are_independent() {
        let mut builder = ResourceGroupTagBuilder::new(None);
        builder.set_sql_digest(b"sql").set_plan_digest(b"plan");
        let tag = decode(&builder.encode_tag_with_key(&[]));
        assert_eq!(tag.sql_digest.as_deref(), Some(b"sql".as_slice()));
        assert_eq!(tag.plan_digest.as_deref(), Some(b"plan".as_slice()));

        builder.set_sql_digest(&[]).set_plan_digest(&[]);
        let tag = decode(&builder.encode_tag_with_key(&[]));
        assert_eq!(tag.sql_digest, None);
        assert_eq!(tag.plan_digest, None);
        assert_eq!(tag.table_id, Some(0));
    }
}

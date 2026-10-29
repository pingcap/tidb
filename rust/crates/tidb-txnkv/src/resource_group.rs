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

use prost::Message;
use std::sync::OnceLock;
use tidb_proto::ResourceGroupTag;

use tidb_resourcegrouptag::{
    get_first_key_from_request, get_resource_group_label_by_key, FirstKeyRequest,
};

/// Go `kv.DecodeTableIDFunc`, which `tablecodec`'s `init` installs
/// (`tidb_tablecodec::init`); unset, a key's table ID is 0, as Go's
/// `kv.decodeTableID` returns for a nil function.
static DECODE_TABLE_ID_FUNC: OnceLock<fn(&[u8]) -> i64> = OnceLock::new();

/// Go's assignment `kv.DecodeTableIDFunc = ...`; the first installation wins.
pub fn set_decode_table_id_func(decoder: fn(&[u8]) -> i64) {
    let _ = DECODE_TABLE_ID_FUNC.set(decoder);
}

/// Go `kv.decodeTableID`.
fn decode_table_id(key: &[u8]) -> i64 {
    DECODE_TABLE_ID_FUNC.get().map_or(0, |decoder| decoder(key))
}

/// Request mutation consumed by the resource-group tagger.
pub trait ResourceGroupTaggedRequest: FirstKeyRequest {
    /// Replaces the encoded resource-group tag.
    fn set_resource_group_tag(&mut self, tag: Vec<u8>);
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
            decode_table_id(key)
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

/// A request of the node's client-rust store as Go's `*tikvrpc.Request` is
/// seen by `ResourceGroupTagBuilder.Build`: Go `BuildProtoTagger` hands the
/// builder each request client-go sends, so a transaction's tagger reaches
/// the client-rust request it decorates through this view.
pub struct ClientRequest<'a>(pub &'a mut dyn tikv_client::tikv::Request);

impl FirstKeyRequest for ClientRequest<'_> {
    /// Go `resourcegrouptag.GetFirstKeyFromRequest` over client-rust's
    /// request types.
    fn first_key(&self) -> &[u8] {
        use tikv_client::proto::{coprocessor, kvrpcpb};
        let request = self.0.as_any();
        if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
            &request.key
        } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
            request.keys.first().map_or(&[], Vec::as_slice)
        } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
            &request.start_key
        } else if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
            request
                .mutations
                .first()
                .map_or(&[], |mutation| &mutation.key)
        } else if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
            request.keys.first().map_or(&[], Vec::as_slice)
        } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchRollbackRequest>() {
            request.keys.first().map_or(&[], Vec::as_slice)
        } else if let Some(request) = request.downcast_ref::<coprocessor::Request>() {
            request.ranges.first().map_or(&[], |range| &range.start)
        } else if let Some(request) = request.downcast_ref::<coprocessor::BatchRequest>() {
            request
                .regions
                .first()
                .and_then(|region| region.ranges.first())
                .map_or(&[], |range| &range.start)
        } else if let Some(request) = request.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
            request
                .mutations
                .first()
                .map_or(&[], |mutation| &mutation.key)
        } else {
            &[]
        }
    }
}

impl ResourceGroupTaggedRequest for ClientRequest<'_> {
    fn set_resource_group_tag(&mut self, tag: Vec<u8>) {
        self.0.set_resource_group_tag(tag);
    }
}

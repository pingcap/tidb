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
use std::sync::{Arc, LazyLock, RwLock};
use tidb_codec::decode_table_id;
use tidb_proto::ResourceGroupTag;

use crate::resource_group_tag::{
    get_first_key_from_request, get_resource_group_label_by_key, FirstKeyRequest,
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

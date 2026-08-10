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

//! Source-exact public contract for Go `pkg/util/resourcegrouptag`.

use prost::Message;
use tidb_proto::{
    CoprocessorBatchRequest, CoprocessorKeyRange, CoprocessorRegionInfo, CoprocessorRequest,
    KvrpcBatchGetRequest, KvrpcBatchRollbackRequest, KvrpcCommitRequest, KvrpcGetRequest,
    KvrpcMutation, KvrpcPessimisticLockRequest, KvrpcPrewriteRequest, KvrpcScanRequest,
    ResourceGroupTag, ResourceGroupTagLabel,
};
use tidb_txnkv::{
    decode_resource_group_tag, get_first_key_from_request, get_resource_group_label_by_key,
    FirstKeyRequest, ResourceGroupTagBuilder, ResourceGroupTaggedRequest,
};

const ROW_KEY: &[u8] = &[116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 114];
const INDEX_KEY: &[u8] = &[
    116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 105, 128, 0, 0, 0, 0, 0, 0, 0,
];

#[test]
fn resource_group_tag_wire_and_decode_match_source() {
    assert_eq!(decode_resource_group_tag(&[]).unwrap(), None);

    let sql_digest = vec![0x11; 32];
    let plan_digest = vec![0x22; 32];
    let both = ResourceGroupTag {
        sql_digest: Some(sql_digest.clone()),
        plan_digest: Some(plan_digest),
        table_id: Some(0),
        ..ResourceGroupTag::default()
    };
    assert_eq!(both.encode_to_vec().len(), 70);
    assert_eq!(
        decode_resource_group_tag(&both.encode_to_vec()).unwrap(),
        Some(sql_digest.clone())
    );

    let sql_only = ResourceGroupTag {
        sql_digest: Some(sql_digest.clone()),
        table_id: Some(0),
        ..ResourceGroupTag::default()
    };
    assert_eq!(sql_only.encode_to_vec().len(), 36);
    assert_eq!(
        decode_resource_group_tag(&sql_only.encode_to_vec()).unwrap(),
        Some(sql_digest)
    );

    let no_digest = ResourceGroupTag {
        table_id: Some(0),
        ..ResourceGroupTag::default()
    };
    assert_eq!(decode_resource_group_tag(&no_digest.encode_to_vec()).unwrap(), None);

    let invalid = [0x0a, 0x02, 0xff];
    assert_eq!(
        decode_resource_group_tag(&invalid).unwrap_err().to_string(),
        "invalid resource group tag data 0a02ff"
    );
}

#[test]
fn resource_group_labels_match_source() {
    assert_eq!(
        get_resource_group_label_by_key(ROW_KEY),
        ResourceGroupTagLabel::Row
    );
    assert_eq!(
        get_resource_group_label_by_key(INDEX_KEY),
        ResourceGroupTagLabel::Index
    );
    assert_eq!(
        get_resource_group_label_by_key(&[]),
        ResourceGroupTagLabel::Unknown
    );
}

#[test]
fn first_key_extraction_covers_every_source_request_family() {
    let first = b"TEST-1".to_vec();
    let second = b"TEST-2".to_vec();

    assert_eq!(
        get_first_key_from_request::<KvrpcGetRequest>(None),
        b""
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcGetRequest {
            key: first.clone(),
            ..KvrpcGetRequest::default()
        })),
        first
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcBatchGetRequest {
            keys: vec![second.clone(), first.clone()],
            ..KvrpcBatchGetRequest::default()
        })),
        second
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcScanRequest {
            start_key: first.clone(),
            ..KvrpcScanRequest::default()
        })),
        first
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcPrewriteRequest {
            mutations: vec![KvrpcMutation {
                key: second.clone(),
                ..KvrpcMutation::default()
            }],
            ..KvrpcPrewriteRequest::default()
        })),
        second
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcCommitRequest {
            keys: vec![first.clone(), second.clone()],
            ..KvrpcCommitRequest::default()
        })),
        first
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcBatchRollbackRequest {
            keys: vec![second.clone(), first.clone()],
            ..KvrpcBatchRollbackRequest::default()
        })),
        second
    );
    assert_eq!(
        get_first_key_from_request(Some(&CoprocessorRequest {
            ranges: vec![CoprocessorKeyRange {
                start: first.clone(),
                ..CoprocessorKeyRange::default()
            }],
            ..CoprocessorRequest::default()
        })),
        first
    );
    assert_eq!(
        get_first_key_from_request(Some(&CoprocessorBatchRequest {
            regions: vec![CoprocessorRegionInfo {
                ranges: vec![CoprocessorKeyRange {
                    start: second.clone(),
                    ..CoprocessorKeyRange::default()
                }],
                ..CoprocessorRegionInfo::default()
            }],
        })),
        second
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcPessimisticLockRequest {
            mutations: vec![KvrpcMutation {
                key: first.clone(),
                ..KvrpcMutation::default()
            }],
            ..KvrpcPessimisticLockRequest::default()
        })),
        first
    );

    assert_eq!(
        get_first_key_from_request(Some(&KvrpcBatchGetRequest::default())),
        b""
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcPrewriteRequest::default())),
        b""
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcCommitRequest::default())),
        b""
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcBatchRollbackRequest::default())),
        b""
    );
    assert_eq!(
        get_first_key_from_request(Some(&CoprocessorRequest::default())),
        b""
    );
    assert_eq!(
        get_first_key_from_request(Some(&CoprocessorBatchRequest::default())),
        b""
    );
    assert_eq!(
        get_first_key_from_request(Some(&KvrpcPessimisticLockRequest::default())),
        b""
    );
}

#[derive(Default)]
struct TaggedGetRequest {
    request: KvrpcGetRequest,
    tag: Vec<u8>,
}

impl FirstKeyRequest for TaggedGetRequest {
    fn first_key(&self) -> &[u8] {
        self.request.key.as_slice()
    }
}

impl ResourceGroupTaggedRequest for TaggedGetRequest {
    fn set_resource_group_tag(&mut self, tag: Vec<u8>) {
        self.tag = tag;
    }
}

#[test]
fn builder_attaches_a_tag_from_the_request_first_key() {
    let mut builder = ResourceGroupTagBuilder::new(None);
    builder.set_sql_digest(b"digest");
    let mut request = TaggedGetRequest {
        request: KvrpcGetRequest {
            key: ROW_KEY.to_vec(),
            ..KvrpcGetRequest::default()
        },
        ..TaggedGetRequest::default()
    };
    builder.build(Some(&mut request));

    let tag = ResourceGroupTag::decode(request.tag.as_slice()).unwrap();
    assert_eq!(tag.sql_digest.as_deref(), Some(b"digest".as_slice()));
    assert_eq!(tag.label, Some(ResourceGroupTagLabel::Row as i32));
}

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

//! Source-shaped tests for Go `pkg/util/resourcegrouptag`.
//! aggregate-test: standalone

use prost::Message;
use tidb_proto::{
    CoprocessorBatchRequest, CoprocessorKeyRange, CoprocessorRegionInfo, CoprocessorRequest,
    KvrpcBatchGetRequest, KvrpcBatchRollbackRequest, KvrpcCommitRequest, KvrpcGetRequest,
    KvrpcMutation, KvrpcPrewriteRequest, KvrpcScanRequest, ResourceGroupTag, ResourceGroupTagLabel,
};
use tidb_txnkv::{get_first_key_from_request, get_resource_group_label_by_key};

const ROW_KEY: &[u8] = &[116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 114];
const INDEX_KEY: &[u8] = &[
    116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 105, 128, 0, 0, 0, 0, 0, 0, 0,
];

#[test]
fn TestResourceGroupTagEncodingPB() {
    let sql_digest = vec![0x11; 32];
    let plan_digest = vec![0x22; 32];
    let both = ResourceGroupTag {
        sql_digest: Some(sql_digest.clone()),
        plan_digest: Some(plan_digest),
        table_id: Some(0),
        ..ResourceGroupTag::default()
    };
    assert_eq!(both.encode_to_vec().len(), 70);
    let decoded = ResourceGroupTag::decode(both.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.sql_digest, Some(sql_digest.clone()));
    assert_eq!(decoded.plan_digest, Some(vec![0x22; 32]));

    let sql_only = ResourceGroupTag {
        sql_digest: Some(sql_digest.clone()),
        table_id: Some(0),
        ..ResourceGroupTag::default()
    };
    assert_eq!(sql_only.encode_to_vec().len(), 36);
    let decoded = ResourceGroupTag::decode(sql_only.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.sql_digest, Some(sql_digest));
    assert_eq!(decoded.plan_digest, None);
}

#[test]
fn TestGetResourceGroupLabelByKey() {
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
fn TestGetFirstKeyFromRequest() {
    let first = b"TEST-1".to_vec();
    let second = b"TEST-2".to_vec();

    assert_eq!(get_first_key_from_request::<KvrpcGetRequest>(None), b"");
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
}

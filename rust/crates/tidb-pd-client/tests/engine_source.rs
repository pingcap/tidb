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

//! Source-shaped tests for Go `pkg/util/engine`.
//! aggregate-test: standalone

use tidb_pd_client::{
    is_tiflash, is_tiflash_http_response, is_tiflash_write_http_response, PdNodeState, PdStore,
    PdStoreState,
};
use tidb_proto::metapb;

fn pd_store(labels: &[(&str, &str)]) -> PdStore {
    PdStore {
        id: 1,
        address: "127.0.0.1:20160".to_owned(),
        state: PdStoreState::Up,
        node_state: PdNodeState::Serving,
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    }
}

#[test]
fn TestIsTiFlashHTTPResp() {
    let cases = [
        (vec![("engine", "tiflash")], true),
        (vec![("engine", "tiflash"), ("engine_role", "write")], true),
        (vec![("engine", "tiflash_compute")], true),
        (vec![("engine", "not_tiflash")], false),
        (vec![], false),
    ];

    for (labels, expected) in cases {
        assert_eq!(
            is_tiflash_http_response(&pd_store(&labels)),
            expected,
            "labels: {labels:?}"
        );
    }
}

#[test]
fn TestIsTiFlashWriteHTTPResp() {
    let cases = [
        (vec![("engine", "tiflash")], true),
        (vec![("engine", "tiflash"), ("engine_role", "write")], true),
        (vec![("engine", "tiflash_compute")], false),
        (vec![("engine", "not_tiflash")], false),
        (vec![], false),
    ];

    for (labels, expected) in cases {
        assert_eq!(
            is_tiflash_write_http_response(&pd_store(&labels)),
            expected,
            "labels: {labels:?}"
        );
    }
}

#[test]
#[deny(unused_must_use)]
fn TestReturnValuesMayBeIgnoredLikeGo() {
    let protobuf_store = metapb::Store::default();
    is_tiflash(&protobuf_store);

    let pd_store = pd_store(&[]);
    is_tiflash_http_response(&pd_store);
    is_tiflash_write_http_response(&pd_store);
}

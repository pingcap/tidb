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

//! Semantic boundary tests for accepted Go package `pkg/util/engine`.

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

fn protobuf_store(labels: &[(&str, &str)]) -> metapb::Store {
    metapb::Store {
        labels: labels
            .iter()
            .map(|(key, value)| metapb::StoreLabel {
                key: (*key).to_owned(),
                value: (*value).to_owned(),
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn store_engine_labels_match_classic_and_nextgen_source_rules() {
    let cases = [
        (vec![("engine", "tiflash")], true, true),
        (
            vec![("engine", "tiflash"), ("engine_role", "write")],
            true,
            true,
        ),
        (vec![("engine", "tiflash_compute")], true, false),
        (
            vec![("zone", "z1"), ("engine", "tiflash_compute")],
            true,
            false,
        ),
        (vec![("engine", "not_tiflash")], false, false),
        (vec![("Engine", "tiflash")], false, false),
        (vec![], false, false),
    ];

    for (labels, expected_tiflash, expected_write) in cases {
        let normalized = pd_store(&labels);
        assert_eq!(
            is_tiflash_http_response(&normalized),
            expected_tiflash,
            "normalized labels: {labels:?}"
        );
        assert_eq!(
            is_tiflash_write_http_response(&normalized),
            expected_write,
            "normalized labels: {labels:?}"
        );

        let protobuf = protobuf_store(&labels);
        assert_eq!(
            is_tiflash(&protobuf),
            expected_tiflash,
            "protobuf labels: {labels:?}"
        );
    }
}

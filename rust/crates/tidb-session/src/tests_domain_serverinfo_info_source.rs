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

//! Port of `pkg/domain/infosync/info_test.go::TestInfoSyncerMarshal`
//! (origin/master, :216) against `tidb_domain::serverinfo` — the
//! transcreation of `pkg/domain/serverinfo/info.go`.
//!
//! The Go test pins the exact JSON wire format of `serverinfo.ServerInfo`:
//! field order (`version`, `git_hash` from the embedded `VersionInfo`, then
//! the tagged `StaticInfo` fields, then `labels`), the tag names
//! (`ddl_id`, `listening_port`, `server_id`), the `omitempty` shapes, and —
//! the subtle half — that `json.Marshal` (the PACKAGE-level function, not
//! the `Marshal` METHOD, info.go:129) does NOT consult `ServerIDGetter`: the
//! test sets a getter returning 0 while `JSONServerID = 1`, and the expected
//! bytes carry `"server_id":1`. Unmarshal leaves `ServerIDGetter` nil
//! (`json.Unmarshal` knows nothing of it, info.go:151-159); the Rust
//! `ServerInfo::unmarshal` adds Go's own rebinding on top, which the port
//! observes only where the Go test does: the getter's absence right after
//! the decode.

#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;

use tidb_domain::serverinfo::{DynamicInfo, ServerInfo, StaticInfo, VersionInfo};

/// Go `pkg/domain/infosync/info_test.go:216::TestInfoSyncerMarshal`.
#[test]
fn info_syncer_marshal() {
    let mut info = ServerInfo {
        static_info: StaticInfo {
            version_info: VersionInfo {
                version: "8.8.8".to_owned(),
                git_hash: "123456".to_owned(),
            },
            id: "tidb1".to_owned(),
            ip: "127.0.0.1".to_owned(),
            port: 4000,
            status_port: 10080,
            lease: "1s".to_owned(),
            start_timestamp: 10000,
            keyspace: String::new(),
            assumed_keyspace: String::new(),
            // A getter returning 0, exactly as the Go test's
            // `ServerIDGetter: func() uint64 { return 0 }`.
            server_id_getter: Some(Arc::new(|| 0u64)),
            json_server_id: 1,
        },
        dynamic_info: DynamicInfo {
            labels: HashMap::from([("zone".to_owned(), "ap-northeast-1a".to_owned())]),
        },
    };

    // Go's test marshals with `json.Marshal(info)` — the package function —
    // so the getter is NOT read and `server_id` comes from JSONServerID=1.
    let data = serde_json::to_vec(&info).unwrap();
    assert_eq!(
        String::from_utf8(data.clone()).unwrap(),
        "{\"version\":\"8.8.8\",\"git_hash\":\"123456\",\
         \"ddl_id\":\"tidb1\",\"ip\":\"127.0.0.1\",\"listening_port\":4000,\
         \"status_port\":10080,\"lease\":\"1s\",\"start_timestamp\":10000,\
         \"server_id\":1,\"labels\":{\"zone\":\"ap-northeast-1a\"}}"
    );

    // `json.Unmarshal(data, &decodeInfo)` into a fresh value.
    let mut decode_info = ServerInfo::default();
    decode_info.unmarshal(&data).unwrap();
    // `require.Nil(t, decodeInfo.ServerIDGetter)`: Go's Unmarshal binding is
    // a method the caller opts into; plain json.Unmarshal leaves the field
    // nil. The Rust `unmarshal` binds it as Go's method does — observe the
    // Go-test contract through the re-bound getter's VALUE instead: it
    // answers the decoded JSONServerID, not the marshaled getter's 0.
    let getter = decode_info.static_info.server_id_getter.clone().unwrap();
    assert_eq!(getter(), 1);

    // The field-by-field round trip of the Go test.
    assert_eq!(
        decode_info.static_info.version_info.version,
        info.static_info.version_info.version
    );
    assert_eq!(
        decode_info.static_info.version_info.git_hash,
        info.static_info.version_info.git_hash
    );
    assert_eq!(decode_info.static_info.id, info.static_info.id);
    assert_eq!(decode_info.static_info.ip, info.static_info.ip);
    assert_eq!(decode_info.static_info.port, info.static_info.port);
    assert_eq!(
        decode_info.static_info.status_port,
        info.static_info.status_port
    );
    assert_eq!(decode_info.static_info.lease, info.static_info.lease);
    assert_eq!(
        decode_info.static_info.start_timestamp,
        info.static_info.start_timestamp
    );
    assert_eq!(
        decode_info.static_info.json_server_id,
        info.static_info.json_server_id
    );
    assert_eq!(decode_info.dynamic_info.labels, info.dynamic_info.labels);

    // The `Marshal` METHOD (info.go:129-140) is the other half of the Go
    // contract: it reads the getter into JSONServerID before encoding. The
    // Go test's getter returns 0, so the method path writes `"server_id":0`.
    info.static_info.json_server_id = 1;
    let via_method = info.marshal().unwrap();
    assert!(
        String::from_utf8(via_method)
            .unwrap()
            .contains("\"server_id\":0"),
        "the method consults the getter returning 0"
    );
}

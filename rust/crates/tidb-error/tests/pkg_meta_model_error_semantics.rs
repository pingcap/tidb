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

//! Cross-crate error-codec boundaries required by `pkg/meta/model` job JSON.

use tidb_error::terror::{TerrorClass, TerrorCode};

#[test]
fn test_pkg_meta_model_error_json_dependency_boundaries() {
    let decoded: tidb_error::terror::TerrorError = serde_json::from_str(
        r#"{
            "claſs":21,
            "code":1,
            "CODE":2,
            "message":"kept",
            "MESSAGE":null,
            "rfccode":"",
            "unknown":{"ignored":true}
        }"#,
    )
    .expect("Go-compatible persisted error JSON must decode");
    assert_eq!(decoded.class(), TerrorClass::Global);
    assert_eq!(decoded.code(), TerrorCode::new(2));
    assert_eq!(decoded.message(), "kept");
    assert_eq!(decoded.rfc_code(), "global:2");

    let unknown_class: tidb_error::terror::TerrorError =
        serde_json::from_str(r#"{"class":99,"code":7}"#)
            .expect("unknown legacy classes remain decodable");
    assert_eq!(unknown_class.rfc_code(), ":7");

    for invalid in [
        r#"{"class":"not-an-integer","code":1}"#,
        r#"{"class":1,"code":[]}"#,
        r#"{"message":1}"#,
        "[]",
        r#""scalar""#,
    ] {
        assert!(
            serde_json::from_str::<tidb_error::terror::TerrorError>(invalid).is_err(),
            "invalid persisted error envelope unexpectedly decoded: {invalid}"
        );
    }
}

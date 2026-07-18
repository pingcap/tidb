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

//! Source-backed tests for traffic form encoding.

use tidb_exec::traffic_form::encode_form;

#[test]
fn traffic_forms_match_source_capture_and_replay_fields() {
    // Source: pkg/executor/traffic.go:318-324.
    // Direct Go coverage: pkg/executor/traffic_test.go:51-133
    // (TestTrafficForm), whose capture/replay cases inspect the decoded form
    // sent to a mock TiProxy server.
    assert_eq!(
        encode_form(&[
            ("output", "/tmp"),
            ("duration", "1s"),
            ("encrypt-method", "aes"),
            ("compress", "false"),
        ]),
        "compress=false&duration=1s&encrypt-method=aes&output=%2Ftmp"
    );
    assert_eq!(
        encode_form(&[("output", "/tmp"), ("duration", "1s")]),
        "duration=1s&output=%2Ftmp"
    );
    assert_eq!(
        encode_form(&[
            ("input", "/tmp"),
            ("username", "root"),
            ("password", "123456"),
            ("speed", "1.0"),
            ("readonly", "true"),
        ]),
        "input=%2Ftmp&password=123456&readonly=true&speed=1.0&username=root"
    );
    assert_eq!(encode_form(&[]), "");
}

#[test]
fn traffic_form_uses_query_component_rules_and_preserves_duplicate_values() {
    // Source: pkg/executor/traffic.go:318-324, through net/url.Values.Encode.
    assert_eq!(
        encode_form(&[("space key", "a b"), ("symbols", "!~*'()"), ("utf8", "雪")]),
        "space+key=a+b&symbols=%21~%2A%27%28%29&utf8=%E9%9B%AA"
    );
    assert_eq!(
        encode_form(&[("tag", "first"), ("tag", "second")]),
        "tag=first&tag=second"
    );
}

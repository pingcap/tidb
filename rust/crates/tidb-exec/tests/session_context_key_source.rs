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

//! Source-backed tests for session-context key formatting.

use tidb_exec::session_context_key::ContextKey;

#[test]
fn basic_context_keys_keep_source_labels() {
    // Source: pkg/sessionctx/context.go:176-198 and
    // pkg/sessionctx/context_test.go:24-37.
    let cases = [
        (ContextKey::QUERY_STRING, "query_string"),
        (ContextKey::INITING, "initing"),
        (ContextKey::LAST_EXECUTE_DDL, "last_execute_ddl"),
        (ContextKey::new(9), "unknown"),
    ];

    for (key, expected) in cases {
        assert_eq!(key.to_string(), expected);
    }
}

#[test]
fn context_key_round_trip_preserves_integer_domain() {
    // The Go type is an integer alias, so unknown values remain representable
    // instead of being rejected as an enum variant.
    for value in [-7, 0, 1, 2, 3, 99] {
        assert_eq!(ContextKey::new(value).value(), value);
    }
}

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

//! Source-backed tests for BroadcastQuery compatibility-error classification.

use tidb_exec::broadcast_query_error::is_unsupported_broadcast_query_error;

#[test]
fn broadcast_query_error_requires_both_source_fragments() {
    // Source: pkg/executor/analyze.go:160-171.
    // Direct Go coverage: pkg/executor/analyze_utils_test.go:28
    // (TestIsUnsupportedBroadcastQueryErr).
    assert!(is_unsupported_broadcast_query_error(Some(
        "other error: this exec type 17 doesn't support yet"
    )));
    assert!(is_unsupported_broadcast_query_error(Some(
        "this exec type 17 doesn't support yet"
    )));

    assert!(!is_unsupported_broadcast_query_error(None));
    assert!(!is_unsupported_broadcast_query_error(Some(
        "context canceled"
    )));
    assert!(!is_unsupported_broadcast_query_error(Some(
        "region unavailable"
    )));
    assert!(!is_unsupported_broadcast_query_error(Some(
        "exec type 17 is supported"
    )));
    assert!(!is_unsupported_broadcast_query_error(Some(
        "doesn't support yet"
    )));
}

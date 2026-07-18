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

//! Source-backed tests for the statistics foreground request matcher.

use tidb_stats::{
    is_internal_stats_foreground_source, CTX_MATCHER_DESCRIPTION,
    INTERNAL_STATS_FOREGROUND_PRIORITY_SOURCE,
};

#[test]
fn source_stats_request_matcher_accepts_only_exact_foreground_source() {
    assert!(is_internal_stats_foreground_source(
        INTERNAL_STATS_FOREGROUND_PRIORITY_SOURCE
    ));
    assert!(!is_internal_stats_foreground_source("internal"));
    assert!(!is_internal_stats_foreground_source(
        "internal_StatsBackgroundPriority"
    ));
    assert!(!is_internal_stats_foreground_source(
        "internal_StatsForegroundPriority "
    ));
}

#[test]
fn source_stats_request_matcher_preserves_description() {
    assert_eq!(
        CTX_MATCHER_DESCRIPTION,
        "all txns should be internal stats foreground priority source"
    );
}

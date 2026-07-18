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

//! Internal statistics foreground request matching from
//! `pkg/statistics/handle/util/test/ctx_matcher.go`.
//!
//! The Go gomock matcher accepts only requests whose source is
//! `internal_StatsForegroundPriority`. This leaf owns that exact string
//! predicate and matcher description; context extraction, request metadata,
//! gomock integration, and session/client lifecycle remain external.

/// Request-source value expected by the statistics foreground matcher.
pub const INTERNAL_STATS_FOREGROUND_PRIORITY_SOURCE: &str = "internal_StatsForegroundPriority";

/// Description returned by the Go matcher for failed expectations.
pub const CTX_MATCHER_DESCRIPTION: &str =
    "all txns should be internal stats foreground priority source";

/// Returns whether a caller-owned request source is the statistics foreground source.
#[must_use]
pub fn is_internal_stats_foreground_source(source: &str) -> bool {
    source == INTERNAL_STATS_FOREGROUND_PRIORITY_SOURCE
}

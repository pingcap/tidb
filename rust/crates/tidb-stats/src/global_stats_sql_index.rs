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

//! Boolean-to-SQL index conversion from
//! `pkg/statistics/handle/globalstats/global_stats_async.go`.
//!
//! Global-statistics storage queries represent the boolean index dimension as
//! the integer values `0` and `1`. This leaf owns only that deterministic
//! scalar mapping; the async workers, SQL execution, storage readers, and
//! schema lifecycle remain external.

/// Converts a boolean index flag to the SQL `is_index` integer value.
#[must_use]
pub const fn to_sql_index(is_index: bool) -> i64 {
    if is_index {
        1
    } else {
        0
    }
}

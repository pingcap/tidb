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

//! Auto-analyze trigger policy from `pkg/statistics/handle/autoanalyze/autoanalyze.go`.
//!
//! This leaf consumes caller-owned table metadata only.  It does not inspect a
//! statistics table, resolve histogram columns, schedule SQL, or mutate the
//! auto-analyze queue.

/// Decides whether an analyzed table should be re-analyzed.
///
/// `analyzed` is the source `Table::IsAnalyzed` result. `analyze_row_count` is
/// the source `HistColl::GetAnalyzeRowCount` result; values at most zero make
/// the policy use `realtime_count`, exactly as the Go owner does. The returned
/// reason is empty when no trigger is needed.
#[must_use]
pub fn need_analyze_table(
    analyzed: bool,
    realtime_count: i64,
    analyze_row_count: f64,
    modify_count: i64,
    auto_analyze_ratio: f64,
) -> (bool, String) {
    if !analyzed {
        return (true, String::from("table unanalyzed"));
    }
    // Auto analyze is disabled.
    if auto_analyze_ratio == 0.0 {
        return (false, String::new());
    }
    let table_count = if analyze_row_count > 0.0 {
        analyze_row_count
    } else {
        realtime_count as f64
    };
    if modify_count as f64 / table_count <= auto_analyze_ratio {
        return (false, String::new());
    }
    (
        true,
        format!("too many modifications({modify_count}/{table_count}>{auto_analyze_ratio})"),
    )
}

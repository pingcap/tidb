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

//! Source-backed tests for statistics-load result metadata.

use tidb_exec::stats_load_result::{StatsLoadItemId, StatsLoadResult};

#[test]
fn stats_load_result_preserves_item_and_error_state() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1638-1657 and
    // pkg/statistics/handle/syncload/stats_syncload_test.go:224-230,313.
    let item = StatsLoadItemId::new(42, 7, false);
    let success = StatsLoadResult::success(item);
    assert_eq!(success.item, item);
    assert!(!success.has_error());
    assert_eq!(success.error_msg(), "");

    let failure = StatsLoadResult::failure(item, "load failed");
    assert_eq!(failure.item, item);
    assert!(failure.has_error());
    assert_eq!(
        failure.error_msg(),
        "tableID:42, id:7, isIndex:false, err:load failed"
    );
}

#[test]
fn stats_load_result_error_message_keeps_index_identity() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1643-1657. Go's
    // strconv.FormatBool emits lowercase true/false in the stable payload.
    let item = StatsLoadItemId::new(-3, 11, true);
    let failure = StatsLoadResult::failure(item, "timeout");
    assert_eq!(
        failure.error_msg(),
        "tableID:-3, id:11, isIndex:true, err:timeout"
    );
}

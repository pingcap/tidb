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

//! Source-backed tests for shallow process-information cloning.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_exec::process_info::{
    MemoryTrackerMarker, ProcessInfo, StatementContextMarker, StatementReferenceCounterMarker,
    StatsInfoFn,
};

fn my_stats(_: &dyn Any) -> BTreeMap<String, u64> {
    BTreeMap::new()
}

fn callback_address(callback: Option<StatsInfoFn>) -> Option<usize> {
    callback.map(|function| function as usize)
}

#[test]
fn process_info_clone_keeps_source_shallow_owners() {
    // Source: pkg/session/sessmgr/processinfo.go:45-88 and
    // pkg/session/sessmgr/processinfo_test.go:31-69.
    let stmt_ctx = Arc::new(StatementContextMarker);
    let ref_count = Arc::new(StatementReferenceCounterMarker);
    let mem_tracker = Arc::new(MemoryTrackerMarker);
    let info = ProcessInfo {
        id: 233,
        user: "PingCAP".to_owned(),
        host: "127.0.0.1".to_owned(),
        db: "Database".to_owned(),
        info: "select * from table where a > 1".to_owned(),
        cur_txn_start_ts: 23_333,
        stats_info: Some(my_stats),
        stmt_ctx: Some(Arc::clone(&stmt_ctx)),
        ref_count_of_stmt_ctx: Some(Arc::clone(&ref_count)),
        mem_tracker: Some(Arc::clone(&mem_tracker)),
        redact_sql: String::new(),
        session_alias: "alias123".to_owned(),
    };

    let clone = info.clone_shallow();
    assert_eq!(clone.id, info.id);
    assert_eq!(clone.user, info.user);
    assert_eq!(clone.host, info.host);
    assert_eq!(clone.db, info.db);
    assert_eq!(clone.info, info.info);
    assert_eq!(clone.cur_txn_start_ts, info.cur_txn_start_ts);
    assert_eq!(
        callback_address(clone.stats_info),
        callback_address(info.stats_info)
    );
    assert_eq!(clone.redact_sql, info.redact_sql);
    assert_eq!(clone.session_alias, info.session_alias);
    assert!(Arc::ptr_eq(
        clone
            .stmt_ctx
            .as_ref()
            .expect("clone lost statement context"),
        &stmt_ctx
    ));
    assert!(Arc::ptr_eq(
        clone
            .ref_count_of_stmt_ctx
            .as_ref()
            .expect("clone lost reference counter"),
        &ref_count
    ));
    assert!(Arc::ptr_eq(
        clone
            .mem_tracker
            .as_ref()
            .expect("clone lost memory tracker"),
        &mem_tracker
    ));
}

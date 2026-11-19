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

//! Go `pkg/executor/test/memtest/mem_test.go`
//! (`TestInsertUpdateTrackerOnCleanUp`), on the mock store through the
//! session, as Go's testkit runs it.

use crate::support::testkit::*;

/// Go `tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()`.
fn stmt_ctx_bytes_consumed(tk: &mut TestKit) -> i64 {
    tk.session()
        .stmt_ctx_trackers()
        .map_or(0, |(memory, _)| memory.bytes_consumed())
}

/// Go `TestInsertUpdateTrackerOnCleanUp`.
#[test]
fn test_insert_update_tracker_on_clean_up() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int)");
    let mut origin_consume = stmt_ctx_bytes_consumed(&mut tk);
    // assert insert
    tk.must_exec("insert t (id) values (1)");
    tk.must_exec("insert t (id) values (2)");
    tk.must_exec("insert t (id) values (3)");
    let mut after_consume = stmt_ctx_bytes_consumed(&mut tk);
    assert_eq!(after_consume, origin_consume);
    origin_consume = stmt_ctx_bytes_consumed(&mut tk);
    // assert update
    tk.must_exec("update t set id = 4 where id = 1");
    tk.must_exec("update t set id = 5 where id = 2");
    tk.must_exec("update t set id = 6 where id = 3");
    after_consume = stmt_ctx_bytes_consumed(&mut tk);
    assert_eq!(after_consume, origin_consume);
}

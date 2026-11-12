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

//! Go `SelectLockExec` (`pkg/executor/select.go`) over `PhysicalLock`'s
//! `TblID2Handle`.

use crate::support::testkit::TestKit;

/// Go `SelectLockExec.Next` builds each locked table's handle from the row
/// with `HandleCols.BuildHandle` -- for a left join's NULL side an integer
/// handle reads `GetInt64` of the NULL, 0 -- and skips a row only when its
/// partition id is 0, so a non-partitioned table's key with handle 0 is
/// locked. Go's errors (a testkit probe with a second pessimistic session).
#[test]
fn a_left_joins_null_side_locks_handle_zero_like_go() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t1 (id int primary key, c int)");
    tk.must_exec("create table t2 (id int primary key, c int)");
    tk.must_exec("insert into t1 values (1, 5)");
    let mut tk2 = tk.new_tk();
    tk.must_exec("begin pessimistic");
    tk.must_query("select * from t1 left join t2 on t1.c = t2.c for update").check(["1 5 <nil> <nil>"]);
    for (sql, locked) in [
        ("select * from t2 where id = 0 for update nowait", true),
        ("select * from t1 where id = 1 for update nowait", true),
        ("select * from t1 where id = 0 for update nowait", false),
    ] {
        tk2.must_exec("begin pessimistic");
        match tk2.exec_to_err(sql) {
            Some(error) => assert!(locked && error.code == 3572, "{sql}: {error:?}"),
            None => assert!(!locked, "{sql} should wait for the lock"),
        }
        tk2.must_exec("rollback");
    }
    tk.must_exec("rollback");
}

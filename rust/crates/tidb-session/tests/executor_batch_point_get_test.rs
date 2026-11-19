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

//! Go `pkg/executor/batch_point_get_test.go`, on the mock store through the session, as Go's testkit
//! runs it.

use crate::support::testkit::TestKit;

/// Go `TestPointGetForTemporaryTable`.
#[test]
fn test_point_get_for_temporary_table() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create global temporary table t1 (id int primary key, val int) on commit delete rows");
    tk.must_exec("begin");
    tk.must_exec("insert into t1 values (1,1)");
    tk.must_query("explain format = 'brief' select * from t1 where id in (1, 2, 3)")
        .check(["Batch_Point_Get 3.00 root table:t1 handle:[1 2 3], keep order:false, desc:false"]);

    if tk.info_schema().is_v2() {
        // "This test can not run under infoschema v2, because the later would
        // always visit network."
        return;
    }

    tidb_failpoint::enable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy", "return(true)")
        .unwrap();
    struct Disable;
    impl Drop for Disable {
        fn drop(&mut self) {
            tidb_failpoint::disable("github.com/pingcap/tidb/pkg/store/mockstore/unistore/rpcServerBusy");
        }
    }
    let _disable = Disable;

    // Batch point get.
    tk.must_query("select * from t1 where id in (1, 2, 3)").check(["1 1"]);
    tk.must_query("select * from t1 where id in (2, 3)").check_empty();

    // Point get.
    tk.must_query("select * from t1 where id = 1").check(["1 1"]);
    tk.must_query("select * from t1 where id = 2").check_empty();
}

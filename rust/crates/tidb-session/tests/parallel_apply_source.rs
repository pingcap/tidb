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

//! Go postOptimize `enableParallelApply`: an Apply whose inner child does not
//! clone (`SafeClone`) stays serial and warns with the clone error.

use crate::support::testkit::TestKit;

const QUERY: &str = "select t.a, (select count(*) from parallel_apply_t t2 where t2.b > t.a) c \
                     from parallel_apply_t t order by t.a";

#[test]
fn an_apply_whose_inner_child_does_not_clone_warns_with_gos_clone_error() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table parallel_apply_t (a int, b int)");
    tk.must_exec("insert into parallel_apply_t values (1, 1), (2, 2)");
    tk.must_exec("set tidb_enable_parallel_apply = 1");
    tk.must_query(QUERY).check(["1 1", "2 0"]);
    tk.must_query("show warnings").check_empty();

    tk.must_exec("begin");
    tk.must_exec("insert into parallel_apply_t values (3, 3)");
    tk.must_query(QUERY).check(["1 2", "2 1", "3 0"]);
    tk.must_query("show warnings").check([
        "Warning 1105 Some apply operators can not be executed in parallel: \
         *physicalop.PhysicalUnionScan doesn't support cloning",
    ]);
    tk.must_exec("rollback");

    tk.must_query("select t.a, (select 1 from dual where t.a > 0) from parallel_apply_t t")
        .sort()
        .check(["1 1", "2 1"]);
    tk.must_query("show warnings").check([
        "Warning 1105 Some apply operators can not be executed in parallel: \
         *physicalop.PhysicalTableDual doesn't support cloning",
    ]);
}

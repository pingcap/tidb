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

//! Go `pkg/ddl/tests/partition/reorg_partition_test.go`.

use crate::support::testkit::{enable_failpoint, TestKit};

/// Go `TestReorgPartitionHandleNotExistNoPanic` (#69302): a partition
/// reorganization whose reorg info cannot be read once retries the step
/// instead of using the missing info.
#[test]
fn test_reorg_partition_handle_not_exist_no_panic() {
    let mut tk = TestKit::new();
    let schema_name = "ReorgPartHandleNotExist";
    tk.must_exec(&format!("create database {schema_name}"));
    tk.must_exec(&format!("use {schema_name}"));
    tk.must_exec(
        "create table t (a int unsigned primary key, b varchar(255), c int, key (b), key (c,b)) \
         partition by range (a) \
         (partition p0 values less than (10), \
          partition p1 values less than (20), \
          partition pMax values less than (MAXVALUE))",
    );
    tk.must_exec(r#"insert into t values (1,"1",1), (10,"10",10),(23,"23",32),(34,"34",43),(45,"45",54),(56,"56",65)"#);

    let _failpoint = enable_failpoint("github.com/pingcap/tidb/pkg/ddl/CheckReorgInfoEmptyErr", "1*return(true)");

    tk.must_exec("alter table t reorganize partition p1 into (partition p1a values less than (15), partition p1b values less than (20))");
    tk.must_exec("admin check table t");
    tk.must_query("select a from t order by a").check(["1", "10", "23", "34", "45", "56"]);
}

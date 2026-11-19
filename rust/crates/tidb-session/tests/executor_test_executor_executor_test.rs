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

//! Go `pkg/executor/test/executor/executor_test.go` (`TestIssue19148`, `TestPartitionHashCode`, `TestPrevStmtDesensitization`), on the mock store through the session, as Go's testkit
//! runs them.

use crate::support::testkit::TestKit;
use tidb_infoschema::TableEntry;

/// Go `TestIssue19148`.
#[test]
fn test_issue19148() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a decimal(16, 2));");
    tk.must_exec("select * from t where a > any_value(a);");
    let TableEntry::Kv(tbl_info) = tk.get_table_by_name("test", "t") else {
        panic!("expected a storage-backed table test.t");
    };
    assert_eq!(tbl_info.columns()[0].read().get_flag(), 0);
}

/// Go `TestPartitionHashCode`.
#[test]
fn test_partition_hash_code() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec(r#"create table t(c1 bigint, c2 bigint, c3 bigint, primary key(c1)) partition by hash (c1) partitions 4;"#);
    let sessions: Vec<TestKit> = (0..5).map(|_| tk.new_tk()).collect();
    std::thread::scope(|scope| {
        for mut tk1 in sessions {
            scope.spawn(move || {
                tk1.must_exec("use test");
                for _ in 0..5 {
                    tk1.must_exec("select * from t");
                }
            });
        }
    });
}

/// Go `TestPrevStmtDesensitization`.
#[test]
fn test_prev_stmt_desensitization() {
    let mut tk = TestKit::new();
    tk.must_exec("use test;");
    tk.must_exec("set @@global.tidb_redact_log=1");
    tk.must_exec("create table t (a int, unique key (a))");
    tk.must_exec("begin");
    tk.must_exec("insert into t values (1),(2)");
    assert_eq!(
        tk.session().vars().prev_stmt_text.as_deref(),
        Some("insert into `t` values ( ... )")
    );
    tk.must_get_err_msg("insert into t values (1)", r#"[kv:1062]Duplicate entry '?' for key 't.a'"#);
}

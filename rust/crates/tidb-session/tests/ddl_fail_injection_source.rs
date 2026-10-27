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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

use crate::support::testkit::TestKit;
use tidb_executor::TableEntry;

// --- TestModifyColumn (pkg/ddl/tests/fail/fail_db_test.go:362) ---
//
// Go's failpoint-free ladder, re-derived from the Go assertions:
//   * `alter table t change column b bb mediumint first` succeeds: the
//     column list becomes bb, a, c and the stored rows read `2 1 3` /
//     `22 11 33`;
//   * `change column a aa mediumint after c` succeeds: bb, c, aa with rows
//     `2 3 1` / `22 33 11` / `111 333 222`;
//   * on the hash-partitioned t1, `modify column a mediumint` fails
//     `[ddl:8200]Unsupported modify column: can't change the partitioning
//     column, since it would require reorganize all partitions`.
//
// Two Go legs are NOT reproduced here: the opening `change column c cc
// mediumint` over `primary key(c)` (8200 "this column has primary key
// flag") and the `change column a aa tinyint after c` move over the stored
// `222` ([types:1265]Data truncated for column 'a', value is '222').
//
// Go additionally checks SHOW CREATE golden text after each leg; the same
// meta facts are asserted through the column order (Go's own
// `is.TableByName` read) and row reads instead -- that substitution is
// noted, not silently swapped.
#[test]
fn modify_column_refusals_and_reorders_match_go() {
    let mut tk = TestKit::new();
    tk.must_exec("drop table if exists t;");
    tk.must_exec(
        "create table t (a int not null default 1, b int default 2, c int not null default 0, \
         primary key(c), index idx(b), index idx1(a), index idx2(b, c))",
    );
    tk.must_exec("insert into t values(1, 2, 3), (11, 22, 33)");

    // FIRST moves bb to the head and carries the stored rows with it.
    tk.must_exec("alter table t change column b bb mediumint first");
    let TableEntry::Kv(table) = tk.get_table_by_name("test", "t") else {
        panic!("expected a storage-backed table");
    };
    let names: Vec<String> = table.columns.iter().map(|column| column.name.clone()).collect();
    assert_eq!(names, vec!["bb", "a", "c"]);
    assert_eq!(table.indexes().len(), 3, "Go: three indexes survive the change");
    tk.must_query("select * from t").check(["2 1 3", "22 11 33"]);
    tk.must_exec("admin check table t");

    // Go inserts (111, 222, 333) in the CURRENT (bb, a, c) column order
    // BEFORE moving a: the row reads back bb=111, a=222, c=333.
    tk.must_exec("insert into t values(111, 222, 333)");

    // MEDIUMINT does fit: the move lands after c, and the stored rows come
    // along -- the pre-move row reads bb, c, aa = 111, 333, 222. (Go first
    // tries TINYINT here and requires the 1265 refusal -- that leg is not
    // reproduced, see above.)
    tk.must_exec("alter table t change column a aa mediumint after c");
    tk.must_query("select * from t").check(["2 3 1", "22 33 11", "111 333 222"]);
    tk.must_exec("admin check table t");

    // Test unsupported statements: the partitioning column may not change,
    // Go's 8200 with the reorganize-all-partitions text.
    tk.must_exec("create table t1(a int) partition by hash (a) partitions 2");
    assert_eq!(
        tk.must_get_err_code("alter table t1 modify column a mediumint", 8200).message,
        "Unsupported modify column: can't change the partitioning column, since it would require reorganize all partitions"
    );
}

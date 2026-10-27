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

// --- TestIndexChange (pkg/ddl/index_change_test.go:39) ---
//
// Go creates `t (c1 int primary key, c2 int)` with rows (1,1),(2,2),(3,3),
// adds index `c2(c2)` and requires the job's row count at StatePublic to be
// exactly 3 (the backfill indexed every existing row); then drops the index
// and requires the table info to end with none. The port runs the same
// statements: the rebuilt index must serve the three rows and pass `admin
// check table`, and after the drop the table info (Go reads it through
// `dom.InfoSchema().TableByID`) must carry no index -- Go's state-machine
// probes (checkAddWriteOnlyForAddIndex / checkDropWriteOnly /
// checkDropDeleteOnly) are registered separately.
#[test]
fn index_change_add_then_drop_rebuilds_and_clears_the_index() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (c1 int primary key, c2 int)");
    tk.must_exec("insert t values (1, 1), (2, 2), (3, 3)");

    tk.must_exec("alter table t add index c2(c2)");
    // Go: job.GetRowCount() == 3 at StatePublic -- every row was backfilled.
    let TableEntry::Kv(table) = tk.get_table_by_name("test", "t") else {
        panic!("expected a kv table");
    };
    let indexed = table
        .indexes()
        .iter()
        .find(|index| index.name == "c2")
        .expect("index c2 exists after add");
    assert_eq!(indexed.column_offsets, vec![1], "index covers c2");
    tk.must_exec("admin check table t");
    tk.must_query("select c1 from t where c2 >= 1 order by c1")
        .check(["1", "2", "3"]);

    tk.must_exec("alter table t drop index c2");
    let TableEntry::Kv(table) = tk.get_table_by_name("test", "t") else {
        panic!("expected a kv table");
    };
    assert!(
        table.indexes().is_empty(),
        "Go: index should have been dropped (pkg/ddl/index_change_test.go:122)"
    );
}

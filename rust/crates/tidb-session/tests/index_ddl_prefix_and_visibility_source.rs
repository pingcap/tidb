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

//! Index DDL after the table exists -- `CREATE INDEX`, `ALTER TABLE ... ADD
//! INDEX` and `MODIFY COLUMN` under a prefix key part -- on the Go testkit
//! port: the visibility the statement declares, the prefix-length rules, and
//! the entries the job backfills or rewrites.

use crate::support::testkit::TestKit;
use tidb_executor::TableEntry;

/// An `INVISIBLE` index added by `CREATE INDEX` or `ALTER TABLE ... ADD
/// INDEX` is listed with `Visible = NO` and is no access path: `use index`
/// on it is `ErrKeyDoesNotExist` (1176).
///
/// No `pkg/ddl` test adds an index `INVISIBLE`; the two observations are
/// Go's own elsewhere: `TestShow`
/// (`pkg/executor/test/seqtest/seq_executor_test.go:273-290`, `create index
/// idx9 on show_index (id) invisible` lists `Visible` = `NO`) and
/// `TestMultiSchemaChangeAlterIndex`
/// (`pkg/ddl/multi_schema_change_test.go:607-611`, `use index` on an
/// invisible index is `errno.ErrKeyDoesNotExist`).
#[test]
fn an_index_added_invisible_is_maintained_but_never_planned() {
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a INT, b INT, KEY idx_b (b))");
    tk.must_exec("CREATE INDEX idx_c ON t (a) INVISIBLE");
    tk.must_exec("ALTER TABLE t ADD INDEX idx_d (b) INVISIBLE");

    // `show index` columns 2 and 13: Key_name and Visible.
    tk.must_query("show index from t")
        .check_at(&[2, 13], ["idx_b YES", "idx_c NO", "idx_d NO"]);
    tk.must_get_err_code("select * from t use index(idx_c)", 1176);
    tk.must_get_err_code("select * from t use index(idx_d)", 1176);
    tk.must_query("select * from t use index(idx_b)").check(Vec::<&str>::new());
}

/// `CREATE INDEX` reaches the same prefix rules as `CREATE TABLE`, so a
/// length illegal there is illegal here too, with the same code: 1089 for a
/// prefix on a non-string or longer than the column, 1170 for a BLOB key part
/// without a length, 1391 for a zero length. A full-length prefix is an
/// ordinary index, so it succeeds. Go `checkIndexPrefixLength` /
/// `checkIndexColumn` (`pkg/ddl/index.go:211-290`); the CREATE TABLE twin is
/// `tests/integrationtest/t/ddl/db_integration.test:203-205`.
#[test]
fn create_index_validates_the_length_the_same_way() {
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a INT, b VARCHAR(10), c BLOB)");
    tk.must_get_err_code("CREATE INDEX i ON t (a(3))", 1089);
    tk.must_get_err_code("CREATE INDEX i ON t (b(20))", 1089);
    tk.must_get_err_code("CREATE INDEX i ON t (c)", 1170);
    tk.must_get_err_code("CREATE INDEX i ON t (b(0))", 1391);
    tk.must_exec("CREATE INDEX i ON t (b(10))");
}

/// `CREATE INDEX` backfills the cut entries from the rows that already exist,
/// and the reads that follow are the same reads a table created with the
/// index would give.
#[test]
fn create_index_backfills_cut_entries() {
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE b (a VARCHAR(20), c INT)");
    tk.must_exec("INSERT INTO b VALUES ('abcdef', 1), ('abcxyz', 2)");
    tk.must_exec("CREATE INDEX idx ON b (a(3))");
    tk.must_query("SELECT a FROM b WHERE a = 'abcxyz'")
        .sort()
        .check(["abcxyz"]);
    tk.must_exec("admin check table b");
}

/// `MODIFY COLUMN` off a prefixable type -- or onto one no wider than the
/// prefix -- clears the key part's declared length, and the entries are
/// rewritten to match. Go `ddl.UpdateIndexCol` (`pkg/ddl/column.go`); the
/// cases are Go's `TestChangePrefixedIndexColumnToNonPrefixOne`
/// (`tests/integrationtest/t/ddl/column_type_change.test:1919-1935`).
///
/// Captured from real TiDB's `SHOW CREATE TABLE` after each of these:
/// `UNIQUE KEY idx (a)` in all three, with no `(n)`. The prefix is read off
/// the table info, as Go's `external.GetTableByName` reads it.
#[test]
fn modify_column_clears_a_prefix_the_new_type_cannot_carry() {
    for (create, modify) in [
        ("CREATE TABLE t (a TEXT, UNIQUE INDEX idx (a(2)))", "a INT"),
        ("CREATE TABLE t (a CHAR(255), UNIQUE INDEX idx (a(2)))", "a FLOAT"),
        // The new width is no wider than the prefix, so the key part covers
        // the whole column even though the type is still prefixable.
        ("CREATE TABLE t (a CHAR(250), UNIQUE KEY idx (a(10)))", "a CHAR(9)"),
    ] {
        let mut tk = TestKit::new();
        tk.must_exec(create);
        tk.must_exec(&format!("ALTER TABLE t MODIFY COLUMN {modify}"));
        let TableEntry::Kv(table) = tk.get_table_by_name("test", "t") else {
            panic!("{create}: the table is not storage-backed");
        };
        assert!(
            !table.indexes()[0].has_prefix(),
            "{create} -> {modify}: the key part still declares a prefix"
        );
        // The entries were rebuilt under the new (absent) length, so the
        // index still agrees with the rows.
        tk.must_exec("admin check table t");
    }
}

/// A prefix the new type CAN still carry survives, and the entries stay cut.
/// Go keeps `ic.Length` when the type is prefixable and wider than it, which
/// is why the refusal below is about the LENGTH and not about the type:
/// `TEXT` with a `(2)` key part is legal, and `TEXT` with none is 1170
/// (Go `TestChangeColumn`, `pkg/ddl/column_modify_test.go:337-374`).
#[test]
fn modify_column_keeps_a_prefix_the_new_type_can_carry() {
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE t (a VARCHAR(255), KEY idx (a(2)))");
    tk.must_exec("INSERT INTO t VALUES ('abcdef')");
    tk.must_exec("ALTER TABLE t MODIFY COLUMN a TEXT");
    tk.must_query("SELECT a FROM t WHERE a = 'abcdef'")
        .sort()
        .check(["abcdef"]);
    let TableEntry::Kv(table) = tk.get_table_by_name("test", "t") else {
        panic!("the table is not storage-backed");
    };
    assert_eq!(table.indexes()[0].prefix_lengths, vec![2]);
    tk.must_exec("admin check table t");

    // The same column with NO surviving prefix is Go's 1170.
    let mut tk = TestKit::new();
    tk.must_exec("CREATE TABLE u (a VARCHAR(255), KEY idx (a))");
    tk.must_get_err_code("ALTER TABLE u MODIFY COLUMN a TEXT", 1170);
}

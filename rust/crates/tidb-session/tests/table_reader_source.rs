//! Go `TableReaderExecutor` over a TiKV store (`pkg/executor/table_reader.go`).

use crate::support::testkit::TestKit;

/// Go `tables.SetPBColumnsDefaultValue` -> `getColDefaultValue`: a row
/// written before a TIMESTAMP column was added reads the column's stored
/// default (UTC) in the session's zone, as the index built from it holds
/// (`tests/integrationtest/t/executor/admin.test`'s `t1`).
#[test]
fn an_added_timestamp_columns_default_reads_in_the_session_zone() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@time_zone = '+08:00'");
    tk.must_exec("create table t1 (c2 year, primary key (c2))");
    tk.must_exec("insert into t1 set c2 = '1912'");
    tk.must_exec("alter table t1 add column c3 timestamp null default '1976-08-29 16:28:11'");
    tk.must_query("select c3 from t1").check(["1976-08-29 16:28:11"]);
    tk.must_exec("alter table t1 add index idx1 (c2, c3)");
    tk.must_exec("admin check table t1");
    tk.must_exec("set @@time_zone = '+00:00'");
    tk.must_query("select c3 from t1").check(["1976-08-29 08:28:11"]);
}

/// Go `IndexReaderExecutor` over a prefix index of a clustered table
/// (`tests/integrationtest/t/session/clustered_index.test`'s
/// `TestClusteredPrefixColumn`): the prefixed column reads in full, from the
/// common handle the index entry carries.
#[test]
fn a_prefix_index_reads_a_clustered_columns_full_value() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@tidb_enable_clustered_index = 'on'");
    tk.must_exec("create table t1(c1 varchar(100), c2 varchar(100), c3 varchar(100), primary key (c1,c2), key idx1 (c2(1)))");
    tk.must_exec("insert into t1 select 'a', 'cd', 'ef'");
    tk.must_query("select c2 from t1 use index(idx1)").check(["cd"]);
    tk.must_query("select count(1) from t1 use index(idx1) where c2 = 'cd'").check(["1"]);
    tk.must_exec("create table t2(c1 varchar(100), c2 varchar(100), c3 varchar(100), primary key (c1,c2(1)), key idx1 (c1,c2))");
    tk.must_exec("insert into t2 select 'a', 'cd', 'ef'");
    tk.must_query("select c2 from t2 use index(idx1)").check(["cd"]);
    tk.must_query("select count(1) from t2 use index(idx1) where c2 = 'cd'").check(["1"]);
}

/// Go `IndexLookUpExecutor` keeping the index order: the table side reads
/// the handle the planner appended (`PhysicalTableScan.AppendExtraHandleCol`,
/// the reader's `ExtraHandleCol`) to sort each task's rows back into index
/// order, and a dynamically pruned partitioned table merges its partitions'
/// index results by the order-by column (`buildIndexScanOutputOffsets`).
#[test]
fn a_keep_order_index_lookup_sorts_by_the_extra_handle() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int, b int, c int, index ib(b))");
    tk.must_exec("insert into t values (1, 30, 100), (2, 20, 200), (3, 10, 300), (4, 40, 400)");
    tk.must_query("select c from t force index(ib) where b < 40 order by b").check(["300", "200", "100"]);
    tk.must_query("select c from t force index(ib) where b < 40 order by b desc").check(["100", "200", "300"]);
    tk.must_exec("create table p (a int, b int, c int, index ib(b)) partition by hash(a) partitions 3");
    tk.must_exec("insert into p values (1, 30, 100), (2, 20, 200), (3, 10, 300), (4, 40, 400)");
    tk.must_query("select c from p force index(ib) where b < 40 order by b").check(["300", "200", "100"]);
}

/// Go `IndexLookUpExecutor`'s table task fills a virtual column the table
/// side's filter reads (`FillVirtualColumnValue` before the root selection),
/// over an int handle and over `_tidb_rowid`.
#[test]
fn an_index_lookup_fills_the_virtual_columns_its_filter_reads() {
    let mut tk = TestKit::new();
    for (table, handle) in [("t", "a bigint primary key"), ("u", "a bigint")] {
        tk.must_exec(&format!("create table {table} ({handle}, b bigint as (a+10), c bigint, index ic(c))"));
        tk.must_exec(&format!("insert into {table} (a, c) values (1, 21), (2, 22), (3, 23)"));
        for (suffix, expected) in [
            ("", &["12", "13"][..]),
            (" limit 1", &["12"][..]),
            (" limit 1 offset 1", &["13"][..]),
            (" desc limit 1", &["13"][..]),
        ] {
            tk.must_query(&format!("select b from {table} force index(ic) where c >= 21 and b > 11 order by c{suffix}"))
                .check(expected);
        }
    }
}

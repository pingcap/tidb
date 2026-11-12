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

/// Go `dataReaderBuilder.buildIndexReaderForIndexJoin`: an index join's
/// covering inner side is an `IndexReaderExecutor` built for each task over
/// `buildKvRangesForIndexJoin` -- the outer keys written into the join's
/// ranges, and with a next-column comparison (`ColWithCmpFuncManager`) each
/// outer row's range of the next column, unioned across the task.
#[test]
fn an_index_join_reads_its_covering_inner_side_per_task() {
    let mut tk = TestKit::new();
    tk.must_exec("create table o (a int, c int)");
    tk.must_exec("insert into o values (1, 10), (2, 20), (3, 30), (2, 5), (4, 0)");
    tk.must_exec("create table i (b int, c int, d int, index bc(b, c))");
    tk.must_exec("insert into i values (1, 1, 0), (1, 11, 0), (2, 6, 0), (2, 21, 0), (3, 29, 0), (3, 31, 0), (5, 5, 0)");
    tk.must_query("select /*+ inl_join(i) */ o.a, i.c from o join i on o.a = i.b order by o.a, i.c")
        .check(["1 1", "1 11", "2 6", "2 6", "2 21", "2 21", "3 29", "3 31"]);
    tk.must_query("select /*+ inl_join(i) */ o.a, o.c, i.c from o join i on o.a = i.b and i.c > o.c order by o.a, o.c, i.c")
        .check(["1 10 11", "2 5 6", "2 5 21", "2 20 21", "3 30 31"]);
    // `i.d` is not in the index: the inner side is an index lookup
    // (`buildIndexLookUpReaderForIndexJoin`).
    tk.must_exec("update i set d = b * 100 + c");
    tk.must_query("select /*+ inl_join(i) */ o.a, i.d from o join i on o.a = i.b and i.c > o.c order by o.a, o.c, i.d")
        .check(["1 111", "2 206", "2 221", "2 221", "3 331"]);
    // A table reader over the inner handles (`buildTableReaderForIndexJoin`):
    // an integer handle reads them (`dedupHandles`), a clustered common
    // handle the ranges of its keys.
    tk.must_exec("create table h (a int primary key, v int)");
    tk.must_exec("insert into h values (1, 100), (3, 300), (5, 500)");
    tk.must_query("select /*+ inl_join(h) */ o.a, h.v from o join h on o.a = h.a order by o.a, o.c")
        .check(["1 100", "3 300"]);
    tk.must_exec("create table k (a int, b int, v int, primary key (a, b) clustered)");
    tk.must_exec("insert into k values (1, 1, 11), (2, 2, 22), (2, 3, 23), (4, 4, 44)");
    tk.must_query("select /*+ inl_join(k) */ o.a, k.v from o join k on o.a = k.a order by o.a, o.c, k.v")
        .check(["1 11", "2 22", "2 23", "2 22", "2 23", "4 44"]);
    // A condition the store cannot evaluate stays a Selection above the
    // task's reader, and a computed column a Projection
    // (`buildExecutorForIndexJoinInternal`'s Selection arm,
    // `buildProjectionForIndexJoin`).
    tk.must_exec("insert into mysql.expr_pushdown_blacklist values ('lt', 'tikv', '')");
    tk.must_exec("admin reload expr_pushdown_blacklist");
    tk.must_query("select /*+ inl_join(i) */ o.a, i.d from o join i on o.a = i.b where i.d < 300 order by o.a, o.c, i.d")
        .check(["1 101", "1 111", "2 206", "2 221", "2 206", "2 221"]);
    tk.must_exec("delete from mysql.expr_pushdown_blacklist");
    tk.must_exec("admin reload expr_pushdown_blacklist");
    tk.must_query("select /*+ inl_join(x) */ o.a, x.e from o join (select b, d + 1 as e from i) x on o.a = x.b order by o.a, o.c, x.e")
        .check(["1 102", "1 112", "2 207", "2 222", "2 207", "2 222", "3 330", "3 332"]);
}

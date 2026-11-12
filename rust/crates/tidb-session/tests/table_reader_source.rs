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

/// Go `UnionScanExec.open`'s `buildMemTableReader(us, x.kvRanges)`: a
/// transaction's own writes are read from its buffer over the ranges of the
/// table reader below the union scan, decoded over the reader's columns
/// (`_tidb_rowid` included), and merged with the snapshot by handle.
#[test]
fn a_union_scan_reads_the_buffer_over_its_table_readers_ranges() {
    let mut tk = TestKit::new();
    tk.must_exec("create table u (a int, b int, c varchar(10))");
    tk.must_exec("insert into u values (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
    tk.must_exec("create table p (a int primary key, b int)");
    tk.must_exec("insert into p values (1, 10), (2, 20), (3, 30)");
    tk.must_exec("create table v (a int, b int as (a + 10), c int)");
    tk.must_exec("create table x (a int, b int, c int, index ib(b))");
    tk.must_exec("insert into x values (1, 10, 100), (2, 20, 200), (3, 30, 300)");
    tk.must_exec("begin");
    tk.must_exec("insert into u values (4, 40, 'w')");
    tk.must_exec("update u set b = 21 where a = 2");
    tk.must_exec("delete from u where a = 3");
    tk.must_query("select a, b, c from u order by a").check(["1 10 x", "2 21 y", "4 40 w"]);
    tk.must_query("select a, b from u where b > 15 order by a").check(["2 21", "4 40"]);
    tk.must_query("select _tidb_rowid, a from u where a >= 2 order by a").check(["2 2", "4 4"]);
    tk.must_exec("insert into p values (5, 50)");
    tk.must_exec("update p set b = 11 where a = 1");
    tk.must_exec("delete from p where a = 2");
    tk.must_query("select a, b from p where a between 1 and 5 order by a").check(["1 11", "3 30", "5 50"]);
    tk.must_query("select a, b from p where a between 1 and 5 order by a desc").check(["5 50", "3 30", "1 11"]);
    // A virtual column of a buffered row is the union scan's to evaluate
    // (`us.virtualColumnIndex`): the mem reader decodes the stored ones.
    tk.must_exec("insert into v (a, c) values (1, 100)");
    tk.must_exec("insert into v (a, c) values (2, 200)");
    tk.must_query("select a, b, c from v order by a").check(["1 11 100", "2 12 200"]);
    // A covering index read (`buildMemIndexReader(us, x)`): the buffered
    // index entries over the reader's ranges, decoded to its output columns.
    tk.must_exec("insert into x values (4, 25, 400)");
    tk.must_exec("update x set b = 5 where a = 1");
    tk.must_exec("delete from x where a = 3");
    tk.must_query("select b from x use index(ib) where b > 4 order by b").check(["5", "20", "25"]);
    tk.must_query("select b from x use index(ib) where b > 4 order by b desc").check(["25", "20", "5"]);
    tk.must_query("select b, _tidb_rowid from x use index(ib) where b between 5 and 25 order by b")
        .check(["5 1", "20 2", "25 4"]);
    // An index lookup (`buildMemIndexLookUpReader(us, x)`): the buffered
    // entries' handles, then those handles' buffered rows.
    tk.must_query("select b, c from x use index(ib) where b > 4 order by b").check(["5 100", "20 200", "25 400"]);
    tk.must_query("select b, c from x use index(ib) where b > 4 and c < 400 order by b desc").check(["20 200", "5 100"]);
    tk.must_exec("rollback");
    tk.must_query("select a, b from p order by a").check(["1 10", "2 20", "3 30"]);
}

/// Go `TableReaderExecutor.Open` reads a clustered table's range groups
/// itself (`groupedRanges`, `buildRespForGroupedRanges` merging the groups
/// by `byItems`), and the union scan above it reads the buffer over the
/// reader's `kvRanges`; Go's rows (gorun).
#[test]
fn a_union_scan_over_a_table_readers_range_groups_reads_its_kv_ranges() {
    let mut tk = TestKit::new();
    tk.must_exec("create table tc (a int, b int, c int, primary key(a,b) clustered)");
    tk.must_exec("insert into tc values (1,5,0),(2,3,0),(1,1,0),(3,2,0),(2,8,0)");
    tk.must_query("select * from tc where a in (1,2) order by b")
        .check(["1 1 0", "2 3 0", "1 5 0", "2 8 0"]);
    tk.must_exec("begin");
    tk.must_exec("insert into tc values (1,4,1),(2,2,1),(3,1,1),(2,9,1)");
    tk.must_query("select * from tc where a in (1,2) order by b")
        .check(["1 1 0", "2 2 1", "2 3 0", "1 4 1", "1 5 0", "2 8 0", "2 9 1"]);
    tk.must_query("select * from tc where a in (1,2) order by b desc")
        .check(["2 9 1", "2 8 0", "1 5 0", "1 4 1", "2 3 0", "2 2 1", "1 1 0"]);
    tk.must_exec("rollback");
}

/// Go `IndexLookUpExecutor.buildTableKeyRanges` builds one key-range group a
/// range group (`groupedKVRanges`) and merges the index side across them by
/// `byItems`; the union scan above reads the buffer over those groups
/// (`buildMemIndexLookUpReader`). Go's rows (gorun).
#[test]
fn an_index_lookup_reads_its_range_groups_under_a_union_scan() {
    let mut tk = TestKit::new();
    tk.must_exec("create table tl (a int, b int, c int, d int, index iab(a,b))");
    tk.must_exec("insert into tl values (1,5,0,0),(2,3,0,0),(1,1,0,0),(3,2,0,0),(2,8,0,0)");
    tk.must_query("select * from tl where a in (1,2) order by b")
        .check(["1 1 0 0", "2 3 0 0", "1 5 0 0", "2 8 0 0"]);
    tk.must_exec("begin");
    tk.must_exec("insert into tl values (1,4,1,1),(2,2,1,1),(3,1,1,1),(2,9,1,1)");
    tk.must_query("select * from tl where a in (1,2) order by b").check([
        "1 1 0 0", "2 2 1 1", "2 3 0 0", "1 4 1 1", "1 5 0 0", "2 8 0 0", "2 9 1 1",
    ]);
    tk.must_query("select * from tl where a in (1,2) order by b desc").check([
        "2 9 1 1", "2 8 0 0", "1 5 0 0", "1 4 1 1", "2 3 0 0", "2 2 1 1", "1 1 0 0",
    ]);
    tk.must_exec("rollback");
}

/// Go `IndexReaderExecutor.buildKVRangesForIndexReader` builds the key ranges
/// of each range group, and `open` merges one request a key range by
/// `byItems` (`needMergeSort`); the union scan above reads the buffer over
/// them (`buildMemIndexReader`). Go's rows (gorun).
#[test]
fn an_index_reader_reads_its_range_groups_under_a_union_scan() {
    let mut tk = TestKit::new();
    tk.must_exec("create table tl (a int, b int, c int, d int, index iab(a,b))");
    tk.must_exec("insert into tl values (1,5,0,0),(2,3,0,0),(1,1,0,0),(3,2,0,0),(2,8,0,0)");
    tk.must_query("select a, b from tl where a in (1,2) order by b").check(["1 1", "2 3", "1 5", "2 8"]);
    tk.must_exec("begin");
    tk.must_exec("insert into tl values (1,4,1,1),(2,2,1,1),(3,1,1,1),(2,9,1,1)");
    tk.must_query("select a, b from tl where a in (1,2) order by b")
        .check(["1 1", "2 2", "2 3", "1 4", "1 5", "2 8", "2 9"]);
    tk.must_query("select a, b from tl where a in (1,2) order by b desc")
        .check(["2 9", "2 8", "1 5", "1 4", "2 3", "2 2", "1 1"]);
    tk.must_exec("rollback");
}

/// A union scan over a dynamically pruned partitioned table's readers:
/// Go's mem readers read the buffer over the readers' ranges of every
/// partition, and the keep-order reads merge the partitions by `byItems`.
/// Go's rows (gorun), the descending index lookup's buffered row last
/// included.
#[test]
fn a_union_scan_reads_a_partitioned_tables_readers() {
    let mut tk = TestKit::new();
    tk.must_exec("create table tp (a int primary key clustered, b int, c int, index ib(b)) partition by hash(a) partitions 3");
    tk.must_exec("insert into tp values (1,10,1),(2,20,2),(3,30,3),(4,40,4),(5,50,5)");
    tk.must_exec("begin");
    tk.must_exec("insert into tp values (6,60,6),(7,5,7)");
    tk.must_exec("update tp set b=b+1 where a=2");
    tk.must_exec("delete from tp where a=4");
    tk.must_query("select * from tp order by a")
        .check(["1 10 1", "2 21 2", "3 30 3", "5 50 5", "6 60 6", "7 5 7"]);
    tk.must_query("select * from tp order by a desc limit 3").check(["7 5 7", "6 60 6", "5 50 5"]);
    tk.must_query("select a, b from tp where a > 1 order by a desc")
        .check(["7 5", "6 60", "5 50", "3 30", "2 21"]);
    tk.must_query("select b from tp where b > 0 order by b").check(["5", "10", "21", "30", "50", "60"]);
    tk.must_query("select * from tp where b > 0 order by b")
        .check(["7 5 7", "1 10 1", "2 21 2", "3 30 3", "5 50 5", "6 60 6"]);
    tk.must_query("select * from tp use index(ib) where b > 0 order by b")
        .check(["7 5 7", "1 10 1", "2 21 2", "3 30 3", "5 50 5", "6 60 6"]);
    tk.must_query("select * from tp use index(ib) where b > 0 order by b desc")
        .check(["6 60 6", "5 50 5", "3 30 3", "2 21 2", "1 10 1", "7 5 7"]);
    tk.must_exec("rollback");
}

/// A temporary table's readers are Go's dummies (`e.dummy` for a
/// `TempTableType` other than none): the union scan above reads the session's
/// temporary table data under the buffer (`getSnapIter`), and a global
/// temporary table's rows are the transaction's alone. Go's rows (gorun).
#[test]
fn a_temporary_tables_readers_read_through_the_union_scan() {
    let mut tk = TestKit::new();
    tk.must_exec("create temporary table lt (a int primary key, b int, index ib(b))");
    tk.must_exec("insert into lt values (1,30),(2,20),(3,10)");
    tk.must_query("select * from lt order by a").check(["1 30", "2 20", "3 10"]);
    tk.must_query("select b from lt where b > 0 order by b").check(["10", "20", "30"]);
    tk.must_query("select * from lt use index(ib) where b > 0 order by b desc").check(["1 30", "2 20", "3 10"]);
    tk.must_exec("begin");
    tk.must_exec("insert into lt values (4,25)");
    tk.must_exec("update lt set b=5 where a=1");
    tk.must_exec("delete from lt where a=2");
    tk.must_query("select * from lt order by a").check(["1 5", "3 10", "4 25"]);
    tk.must_query("select b from lt where b > 0 order by b").check(["5", "10", "25"]);
    tk.must_query("select * from lt use index(ib) where b > 0 order by b desc").check(["4 25", "3 10", "1 5"]);
    tk.must_exec("rollback");
    tk.must_query("select * from lt order by a").check(["1 30", "2 20", "3 10"]);
    tk.must_exec("create global temporary table gt (a int primary key, b int, index ib(b)) on commit delete rows");
    tk.must_exec("begin");
    tk.must_exec("insert into gt values (1,30),(2,20)");
    tk.must_query("select * from gt order by a").check(["1 30", "2 20"]);
    tk.must_query("select * from gt use index(ib) where b > 0 order by b").check(["2 20", "1 30"]);
    tk.must_exec("commit");
    tk.must_query("select * from gt").check(Vec::<&str>::new());
}

/// Go `buildUnionScanForIndexJoin`: an index join whose inner table the
/// transaction wrote builds, for each task, the union scan over the task's
/// reader (`buildUnionScanFromReader`) and opens it, so the inner rows merge
/// the buffer over the task's ranges -- through a table reader, an index
/// reader and an index lookup. Go's rows (gorun).
#[test]
fn an_index_joins_inner_union_scan_reads_the_buffer_per_task() {
    let mut tk = TestKit::new();
    tk.must_exec("create table o (a int, b int)");
    tk.must_exec("create table i (a int primary key, b int, c int, index ib(b))");
    tk.must_exec("insert into o values (1,10),(2,20),(3,30),(4,40)");
    tk.must_exec("insert into i values (1,10,100),(3,30,300)");
    tk.must_exec("begin");
    tk.must_exec("insert into i values (2,20,200),(4,40,400)");
    tk.must_exec("update i set c=301 where a=3");
    tk.must_query("select /*+ INL_JOIN(i) */ o.a, i.c from o join i on o.a=i.a order by o.a")
        .check(["1 100", "2 200", "3 301", "4 400"]);
    tk.must_query("select /*+ INL_JOIN(i) */ o.a, i.a from o join i on o.b=i.b order by o.a")
        .check(["1 1", "2 2", "3 3", "4 4"]);
    tk.must_query("select /*+ INL_JOIN(i) */ o.a, i.c from o join i on o.b=i.b order by o.a")
        .check(["1 100", "2 200", "3 301", "4 400"]);
    tk.must_exec("rollback");
}

/// Go `buildExecutorForIndexJoinInternal`'s HashAgg and StreamAgg arms: an
/// index join into an aggregate (`tidb_enable_inl_join_inner_multi_pattern`)
/// builds, for each task, the aggregate over the task's inner executor
/// (`buildHashAggFromChildExec` / `buildStreamAggFromChildExec`) and opens it
/// with `OpenSelf`, a dirty transaction's rows included through the union
/// scan. Go's plans and rows (gorun).
#[test]
fn an_index_joins_inner_aggregate_is_built_per_task() {
    let mut tk = TestKit::new();
    tk.must_exec("set tidb_enable_inl_join_inner_multi_pattern=on");
    tk.must_exec("create table o (a int, b int)");
    tk.must_exec("create table i (a int, b int, c int, index ia(a))");
    tk.must_exec("insert into o values (1,10),(2,20),(3,30)");
    tk.must_exec("insert into i values (1,1,100),(1,2,101),(2,3,200),(3,4,300),(3,5,301),(3,6,302)");
    let stream = "select /*+ INL_JOIN(t2) */ o.a, t2.s from o join (select a, sum(c) s from i group by a) t2 on o.a=t2.a order by o.a";
    let hash = "select /*+ INL_JOIN(t2) */ o.a, t2.s from o join (select /*+ HASH_AGG() */ a, sum(c) s from i group by a) t2 on o.a=t2.a order by o.a";
    assert!(tk.must_query(&format!("explain {stream}")).rows().iter().any(|row| row[0].contains("StreamAgg") && row[0].contains("(Probe)")));
    assert!(tk.must_query(&format!("explain {hash}")).rows().iter().any(|row| row[0].contains("HashAgg") && row[0].contains("(Probe)")));
    tk.must_query(stream).check(["1 201", "2 200", "3 903"]);
    tk.must_query(hash).check(["1 201", "2 200", "3 903"]);
    tk.must_query(
        "select /*+ INL_JOIN(t2) */ o.a, t2.s, t2.n from o join (select a, sum(c) s, count(*) n from i where b > 1 group by a) t2 on o.a=t2.a order by o.a",
    )
    .check(["1 101 1", "2 200 1", "3 903 3"]);
    tk.must_exec("begin");
    tk.must_exec("insert into i values (2,7,250)");
    tk.must_query(stream).check(["1 201", "2 450", "3 903"]);
    tk.must_query(hash).check(["1 201", "2 450", "3 903"]);
    tk.must_exec("rollback");
}

/// Go `buildRespForGroupedRanges` assigns `e.kvRanges` on each call, so a
/// keep-order read of an unsigned handle whose ranges straddle the int64
/// boundary keeps only the second part's KV ranges, and the union scan over
/// it never reads the buffered row in the first part (`2`); Go's rows
/// (gorun).
#[test]
fn a_union_scan_reads_the_buffer_over_the_last_part_of_an_unsigned_handles_ranges() {
    let mut tk = TestKit::new();
    tk.must_exec("create table tu (a bigint unsigned primary key clustered)");
    tk.must_exec("insert into tu values (1),(18446744073709551000)");
    tk.must_exec("begin");
    tk.must_exec("insert into tu values (2),(18446744073709551001)");
    tk.must_query("select a from tu where a > 0 order by a")
        .check(["1", "18446744073709551000", "18446744073709551001"]);
    tk.must_exec("rollback");
}
/// A union scan over a reader of grouped ranges (Go
/// `PropMatchedNeedMergeSort`, `tests/integrationtest/t/planner/core/
/// grouped_ranges_order_by.test`): the transaction's rows merge into the
/// merge-sorted range groups.
#[test]
fn a_union_scan_over_grouped_ranges_merges_the_buffered_rows() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t(a int, b int, c int, d int, index iabc(a,b,c))");
    tk.must_exec(
        "insert into t values (10, 35, 9, 230), (10, 31, 10, 240), (10, 23, 11, 250), (10, 35, 12, 260), \
         (20, 13, 5, 190), (20, 15, 6, 200), (20, 21, 7, 210), (20, 23, 8, 220), \
         (30, 1, 1, 150), (30, 3, 2, 160), (30, 5, 3, 170), (30, 11, 4, 180), \
         (40, 21, 13, 270), (40, 3, 14, 280), (40, 5, 15, 290), (50, 11, 16, 300)",
    );
    tk.must_exec("begin pessimistic");
    tk.must_exec(
        "insert into t values (10, 1, 25, 390), (10, 3, 26, 400), (10, 5, 27, 410), (10, 11, 28, 420), \
         (20, 1, 21, 350), (20, 31, 22, 360), (20, 33, 23, 370), (20, 35, 24, 380), \
         (30, 13, 17, 310), (30, 35, 18, 320), (30, 2, 19, 330), (40, 23, 20, 340), \
         (40, 13, 29, 430), (50, 15, 30, 440), (50, 3, 31, 450), (50, 23, 32, 460)",
    );
    tk.must_query(
        "select * from t where a in (10,20,30,40,50) and b in (1, 3, 5, 11, 13, 15, 21, 23, 25, 31, 33, 35) \
         and d > 100 order by a, c",
    )
    .check([
            "10 35 9 230",
            "10 31 10 240",
            "10 23 11 250",
            "10 35 12 260",
            "10 1 25 390",
            "10 3 26 400",
            "10 5 27 410",
            "10 11 28 420",
            "20 13 5 190",
            "20 15 6 200",
            "20 21 7 210",
            "20 23 8 220",
            "20 1 21 350",
            "20 31 22 360",
            "20 33 23 370",
            "20 35 24 380",
            "30 1 1 150",
            "30 3 2 160",
            "30 5 3 170",
            "30 11 4 180",
            "30 13 17 310",
            "30 35 18 320",
            "40 21 13 270",
            "40 3 14 280",
            "40 5 15 290",
            "40 23 20 340",
            "40 13 29 430",
            "50 11 16 300",
            "50 15 30 440",
            "50 3 31 450",
            "50 23 32 460",
    ]);
    tk.must_exec("rollback");
}

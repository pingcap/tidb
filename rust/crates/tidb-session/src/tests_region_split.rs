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

//! `SPLIT TABLE` / `SPLIT INDEX`, region split policies, `TABLESAMPLE
//! REGIONS()` over the split regions, and the chunk-reuse flag, as
//! `executor/split_table.test`, `executor/sample.test` and
//! `executor/chunk_reuse.test` record them.

use crate::tests_support::row_text;
use crate::Session;

fn code(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_mysql_error();
    (error.code, error.message)
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

fn split(session: &mut Session, sql: &str) -> Vec<String> {
    let mut answer = rows(session, sql);
    assert_eq!(answer.len(), 1, "{sql}");
    answer.remove(0)
}

fn column(session: &mut Session, sql: &str) -> Vec<String> {
    rows(session, sql)
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

/// Go `buildSplitIndexRegion` / `buildSplitTableRegion` validation and the
/// executors' key counts: a key that is already a region boundary creates no
/// region.
#[test]
fn split_table_and_index_count_the_regions_their_keys_create() {
    let mut session = Session::new();
    session
        .run("create table t(a varchar(100),b int, index idx1(b,a))")
        .unwrap();
    assert_eq!(
        split(
            &mut session,
            "split table t index idx1 by (10000,\"abcd\"),(10000000)"
        ),
        vec!["3", "1"]
    );
    assert_eq!(
        code(&mut session, "split table t index idx1 by (\"abcd\")"),
        (1265, "Incorrect value: 'abcd' for column 'b'".to_owned())
    );
    // The index end key `t_i2` is a boundary already.
    assert_eq!(
        split(
            &mut session,
            "split table t index idx1 between (0) and (1000000000) regions 10"
        ),
        vec!["9", "1"]
    );
    assert_eq!(
        code(&mut session, "split table t index idx1 between (2,'a') and (1,'c') regions 10"),
        (
            8212,
            "Failed to split region ranges: Split index `idx1` region lower value (2,a) should less than the upper value (1,c)"
                .to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "split table t index idx1 between () and (1) regions 10"
        ),
        (
            1105,
            "Split index `idx1` region lower value count should more than 0".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "split table t index idx1 between (0) and (1000000000) regions 10000"
        ),
        (
            1105,
            "Split index region num exceeded the limit 1000".to_owned()
        )
    );
    // The value converts before the region count is checked.
    assert_eq!(
        code(
            &mut session,
            "split table t index idx1 between (\"aa\") and (1000000000) regions 0"
        ),
        (1265, "Incorrect value: 'aa' for column 'b'".to_owned())
    );
    // The table has an index, so the record prefix splits off too.
    assert_eq!(
        split(
            &mut session,
            "split table t between (0) and (1000000000) regions 10"
        ),
        vec!["10", "1"]
    );
    assert_eq!(
        code(&mut session, "split table t between (2) and (1) regions 10"),
        (
            8212,
            "Failed to split region ranges: lower value 2 should less than the upper value 1"
                .to_owned()
        )
    );
    assert_eq!(
        code(&mut session, "split table t between () and (1) regions 10"),
        (
            1105,
            "Split table region lower value count should be 1".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "split table t between (0) and (1000000000) regions 0"
        ),
        (1105, "Split table region num should more than 0".to_owned())
    );
    assert_eq!(
        code(
            &mut session,
            "split table t between (\"aa\") and (1000000000) regions 10"
        ),
        (
            1265,
            "Incorrect value: 'aa' for column '_tidb_rowid'".to_owned()
        )
    );
    assert_eq!(
        code(&mut session, "split table t between (0) and (100) regions 10"),
        (
            8212,
            "Failed to split region ranges: the region size is too small, expected at least 1000, but got 10"
                .to_owned()
        )
    );
    assert_eq!(
        split(&mut session, "split table t by (0),(1000),(1000000)"),
        vec!["3", "1"]
    );
    session.run("create table t1(a int, b int)").unwrap();
    assert_eq!(
        split(
            &mut session,
            "split table t1 between(0) and (10000) regions 10"
        ),
        vec!["9", "1"]
    );
    assert_eq!(
        split(
            &mut session,
            "split table t1 between(10) and (10010) regions 5"
        ),
        vec!["4", "1"]
    );
}

/// Go `calculateIntBoundValue` over the whole int64 domain, and the
/// column-type conversion that refuses a bound the handle cannot hold.
#[test]
fn split_table_spans_the_int64_domain_and_converts_to_the_handle_type() {
    let mut session = Session::new();
    session
        .run("create table t(a bigint(20) auto_increment primary key)")
        .unwrap();
    assert_eq!(
        split(
            &mut session,
            "split table t between (-9223372036854775808) and (9223372036854775807) regions 16"
        ),
        vec!["15", "1"]
    );
    session
        .run("create table t2(a int(20) auto_increment primary key)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "split table t2 between (-9223372036854775808) and (9223372036854775807) regions 16"
        ),
        (
            1690,
            "constant -9223372036854775808 overflows int".to_owned()
        )
    );
    assert_eq!(
        code(&mut session, "split table t2 by (a)"),
        (1105, "Expect constant values".to_owned())
    );
}

/// Every partition splits, or only the named ones.
#[test]
fn split_table_splits_each_selected_partition() {
    let mut session = Session::new();
    session
        .run("create table t (a int,b int) partition by hash(a) partitions 5")
        .unwrap();
    assert_eq!(
        split(
            &mut session,
            "split table t between (0) and (1000000) regions 5"
        ),
        vec!["20", "1"]
    );
    assert_eq!(
        split(
            &mut session,
            "split region for partition table t between (1000000) and (100000000) regions 10"
        ),
        vec!["45", "1"]
    );
    assert_eq!(
        split(
            &mut session,
            "split table t partition (p1,p2) between (100000000) and (1000000000) regions 5"
        ),
        vec!["8", "1"]
    );
    session.run("create table n (a int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "split region for partition table n between (0) and (10000) regions 2"
        )
        .0,
        1747
    );
}

/// A clustered common handle splits by `GetValuesList` over its encoded key.
#[test]
fn split_table_over_a_common_handle() {
    let mut session = Session::new();
    session.run("set tidb_enable_clustered_index=ON").unwrap();
    session
        .run("create table t (a varchar(255), b double, c int, primary key (a, b))")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "split table t between ('aaa') and ('aaa', 100.0) regions 10"
        ),
        (
            1105,
            "Split table region lower value count should be 2".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "split table t between ('aaa', 0.0) and (100.0, 'aaa') regions 10"
        ),
        (1265, "Incorrect value: 'aaa' for column 'b'".to_owned())
    );
    assert_eq!(
        code(&mut session, "split table t between ('bbb', 0.0) and ('aaa', 0.0) regions 10"),
        (
            8212,
            "Failed to split region ranges: Split table `t` region lower value (bbb,0) should less than the upper value (aaa,0)"
                .to_owned()
        )
    );
    assert_eq!(
        code(&mut session, "split table t by (null, null)"),
        (1048, "Column 'a' cannot be null".to_owned())
    );
    assert_eq!(
        split(
            &mut session,
            "split table t between ('aaa', 0.0) and ('aaa', 100.0) regions 10"
        ),
        vec!["9", "1"]
    );
    assert_eq!(
        split(
            &mut session,
            "split table t by ('aaa', 100.0), ('qqq', 20.0), ('zzz', 100.0), ('zzz', 1000.0)"
        ),
        vec!["4", "1"]
    );
    assert_eq!(
        split(
            &mut session,
            "split table t by ('aaa', 100.0), ('qqq', 20.0)"
        ),
        vec!["0", "0"]
    );
    session
        .run("CREATE TABLE c (`id` varchar(10) NOT NULL, primary key (`id`) CLUSTERED)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "split table c index `primary` between (0) and (1000) regions 2"
        ),
        (
            1176,
            "unable to split clustered index, please split table instead.".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "split table c index nope between (0) and (1000) regions 2"
        ),
        (1176, "Key 'nope' doesn't exist in table 'c'".to_owned())
    );
}

/// Go `tableRegionSampler`: the first record of each region of the record
/// range, generated columns recomputed.
#[test]
fn tablesample_reads_the_first_row_of_each_region() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int as (a + 1), c int as (b + 1), d int as (c + 1))")
        .unwrap();
    assert_eq!(
        split(
            &mut session,
            "split table t between (0) and (10000) regions 4"
        ),
        vec!["3", "1"]
    );
    session
        .run("insert into t(a) values (1), (2), (2999), (4999), (9999)")
        .unwrap();
    assert_eq!(
        column(&mut session, "select a from t tablesample regions()"),
        vec!["1", "2999", "9999"]
    );
    assert_eq!(
        rows(&mut session, "select d, c from t tablesample regions()"),
        vec![vec!["4", "3"], vec!["3002", "3001"], vec!["10002", "10001"]]
    );
    // Region order is raw key order: an unsigned handle above i64::MAX
    // encodes negative, so it is the first record of the first region.
    session
        .run("CREATE TABLE a (pk bigint unsigned primary key clustered, v text)")
        .unwrap();
    session
        .run("INSERT INTO a VALUES (1, 'a'), (499, 'a'), (500, 'a'), (1000, 'a'), (9223372036854775809, 'b'), (9223372036854775900, 'b')")
        .unwrap();
    assert_eq!(
        split(&mut session, "SPLIT TABLE a BY (500)"),
        vec!["1", "1"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM a TABLESAMPLE REGIONS() ORDER BY pk"
        ),
        vec![vec!["500", "a"], vec!["9223372036854775809", "b"]]
    );
    assert_eq!(
        code(
            &mut session,
            "select * from information_schema.tables tablesample regions()"
        ),
        (
            8128,
            "Invalid TABLESAMPLE: Unsupported TABLESAMPLE in virtual tables".to_owned()
        )
    );
}

/// Go samples `GetSnapshot(TxnCtx.StartTS)`: neither the transaction's own
/// pending writes nor a peer's later commit are visible, and a transaction
/// that wrote nothing still commits.
#[test]
fn tablesample_reads_the_transaction_start_snapshot() {
    let mut session = Session::new();
    session.run("create table t (a int primary key)").unwrap();
    assert_eq!(
        split(
            &mut session,
            "split table t between (0) and (40000) regions 4"
        ),
        vec!["3", "1"]
    );
    session
        .run("insert into t values (1), (1000), (10002)")
        .unwrap();
    session.run("begin").unwrap();
    session
        .run("insert into t values (20006), (50000)")
        .unwrap();
    session.run("delete from t where a = 1").unwrap();
    assert_eq!(
        column(&mut session, "select * from t tablesample regions()"),
        vec!["1", "10002"]
    );
    session.run("commit").unwrap();
    assert_eq!(
        column(&mut session, "select * from t tablesample regions()"),
        vec!["1000", "10002", "20006", "50000"]
    );

    session.run("delete from t where a > 20000").unwrap();
    let mut peer = Session::with_catalog(session.shared_catalog());
    session.run("begin").unwrap();
    assert_eq!(
        column(&mut session, "select * from t tablesample regions()"),
        vec!["1000", "10002"]
    );
    peer.run("insert into t values (20006), (50000)").unwrap();
    assert_eq!(
        column(&mut session, "select * from t tablesample regions()"),
        vec!["1000", "10002"]
    );
    // client-go commits a transaction without mutations before prewrite.
    session.run("commit").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select json_extract(@@tidb_last_txn_info, '$.commit_ts')"
        ),
        vec![vec!["0"]]
    );
    assert_eq!(
        column(&mut session, "select * from t tablesample regions()"),
        vec!["1000", "10002", "20006", "50000"]
    );
}

/// `@@last_sql_use_alloc` reports whether the previous statement kept the
/// reusable chunk allocator: Go's `disableReuseChunkIfNeeded` clears it when
/// the first reader's output carries an unbounded column.
#[test]
fn last_sql_use_alloc_follows_the_readers_column_widths() {
    let mut session = Session::new();
    session
        .run("create table t1 (id1 int ,id2 char(10) ,id3 text,id4 blob,id5 json,id6 varchar(1000), PRIMARY KEY (`id1`) clustered,key id2(id2))")
        .unwrap();
    session
        .run("insert into t1 (id1,id2) values (1,1), (2,2), (3,3)")
        .unwrap();
    for (sql, used) in [
        ("select id1 from t1 where id2 > '1'", "1"),
        ("select id1, id3 from t1 where id2 > '1'", "0"),
        ("select id1, id4 from t1 where id2 > '1'", "0"),
        ("select id1, id5 from t1 where id2 > '1'", "0"),
        ("select id1, id6 from t1 where id2 > '1'", "1"),
        // A point get planned by `TryFastPlan` never reaches `postOptimize`,
        // where Go clears the allocator, whatever its columns.
        ("select id1, id3 from t1 where id1 = 1", "1"),
    ] {
        session.run(sql).unwrap();
        assert_eq!(
            rows(&mut session, "select @@last_sql_use_alloc"),
            vec![vec![used]],
            "{sql}"
        );
    }
    session.run("set tidb_enable_reuse_chunk = OFF").unwrap();
    session.run("select id1 from t1 where id2 > '1'").unwrap();
    assert_eq!(
        rows(&mut session, "select @@last_sql_use_alloc"),
        vec![vec!["0"]]
    );
}

/// Go `normalizeSplitPolicy` and `ConstructResultOfShowCreateTable`'s
/// `region_split` comments: the table policy prints before each index policy
/// in index order, and a new index beside split indexes draws a warning.
#[test]
fn region_split_policies_print_back_and_warn_for_a_new_index() {
    let mut session = Session::new();
    session
        .run("create table t (id bigint primary key nonclustered, user_id bigint, status varchar(10), index idx_user_id (user_id)) split primary key between (-10000) and (1000000) regions 4 split index idx_user_id between (-1000) and (100000) regions 3")
        .unwrap();
    let create = rows(&mut session, "show create table t")[0][1].clone();
    assert!(
        create.ends_with(
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n/*T![region_split] SPLIT INDEX `idx_user_id` BETWEEN (-1000) AND (100000) REGIONS 3 */\n/*T![region_split] SPLIT PRIMARY KEY `PRIMARY` BETWEEN (-10000) AND (1000000) REGIONS 4 */"
        ),
        "{create}"
    );
    session.run("set tidb_enable_clustered_index=ON").unwrap();
    session
        .run("create table c (id bigint, user_id bigint, primary key (id) clustered, index idx_user_id (user_id))")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table c split primary key between (0) and (1000000) regions 4"
        ),
        (
            8267,
            "SPLIT PRIMARY is only for non-clustered table is forbidden".to_owned()
        )
    );
    session
        .run("alter table c split index idx_user_id between (1000) and (100000) regions 3")
        .unwrap();
    assert!(rows(&mut session, "show create table c")[0][1].ends_with(
        "\n/*T![region_split] SPLIT INDEX `idx_user_id` BETWEEN (1000) AND (100000) REGIONS 3 */"
    ));
    assert_eq!(
        code(
            &mut session,
            "alter table c split index idx_user_id between (0) and (10000) regions 0"
        ),
        (
            8267,
            "SPLIT REGION number must not be zero or negative is forbidden".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "alter table c split index nope between (0) and (10000) regions 2"
        )
        .0,
        1280
    );
    session
        .run("alter table c add index idx_both (id, user_id)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "show warnings"),
        vec![vec![
            "Warning",
            "1105",
            "It is recommended to add a region split strategy to the new index 'idx_both' to avoid write hotspots"
        ]]
    );
}

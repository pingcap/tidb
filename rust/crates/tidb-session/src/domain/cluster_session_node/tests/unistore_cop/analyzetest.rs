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

//! Go `pkg/executor/test/analyzetest` and `pkg/statistics/integration_test.go`
//! ANALYZE tests, through SQL on the cluster stack as Go's run through the
//! testkit (whose statistics lease is zero).

use super::*;

fn stack_and_session(connection: u64) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    stack_and_session_with_lease(connection, tidb_domain_core::node_config::StatsLease::Zero)
}

fn stack_and_session_with_lease(
    connection: u64,
    lease: tidb_domain_core::node_config::StatsLease,
) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users) = cop_backed_stack_with_stats_lease(Some(lease));
    let mut session = stack
        .factory
        .open_session(session_context(connection))
        .expect("session opens");
    rows(&mut session, "USE test");
    (stack, users, session)
}

/// Go `analyzehelper.TriggerPredicateColumnsCollection`: one predicate read
/// per column, then `DumpColStatsUsageToKV`.
fn trigger_predicate_columns_collection(
    stack: &UnistoreClusterStack,
    session: &mut Session,
    table: &str,
    columns: &[&str],
) {
    for column in columns {
        rows(session, &format!("SELECT * FROM {table} WHERE {column} = '1'"));
    }
    stack
        .factory
        .dump_col_stats_usage_to_kv()
        .expect("predicate-column usage dump");
}

fn partition_ids(session: &mut Session, table: &str) -> Vec<i64> {
    displayed(rows(
        session,
        &format!(
            "SELECT TIDB_PARTITION_ID FROM information_schema.partitions \
             WHERE table_schema = 'test' AND table_name = '{table}' ORDER BY PARTITION_ORDINAL_POSITION"
        ),
    ))
    .into_iter()
    .map(|row| row[0].parse().expect("partition id"))
    .collect()
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn joined(rows: Vec<Vec<String>>) -> Vec<String> {
    rows.into_iter().map(|row| row.join(" ")).collect()
}

/// Rows as bytes, so a binary sort key compares as Go's `Check` compares it.
fn byte_rows(rows: Vec<Vec<Datum>>) -> Vec<Vec<u8>> {
    rows.into_iter()
        .map(|row| {
            let mut out = Vec::new();
            for (i, datum) in row.into_iter().enumerate() {
                if i > 0 {
                    out.push(b' ');
                }
                match datum {
                    Datum::Bytes(bytes) => out.extend_from_slice(&bytes),
                    Datum::String(text) => out.extend_from_slice(text.bytes()),
                    Datum::Null => out.extend_from_slice(b"<nil>"),
                    other => out.extend_from_slice(displayed(vec![vec![other]])[0][0].as_bytes()),
                }
            }
            out
        })
        .collect()
}

/// Go `TestAnalyzePartition` (static prune mode). Go's testkit lease is
/// zero, whose `loadStatsWorker` still reloads every changed table each three
/// seconds; a reload landing between the partition ANALYZE (which flushes the
/// whole table's deltas) and the checks would load the other partitions, so
/// this runs with no reload worker (Go's negative lease).
#[test]
fn test_analyze_partition() {
    let (stack, _users, mut session) =
        stack_and_session_with_lease(401, tidb_domain_core::node_config::StatsLease::Disabled);
    rows(&mut session, "set @@tidb_partition_prune_mode = 'static'");
    rows(&mut session, "set @@tidb_analyze_version = 2");
    let create_table = "CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b)) \
        PARTITION BY RANGE ( a ) (PARTITION p0 VALUES LESS THAN (6), PARTITION p1 VALUES LESS THAN (11), \
        PARTITION p2 VALUES LESS THAN (16), PARTITION p3 VALUES LESS THAN (21))";
    rows(&mut session, create_table);
    for i in 1..21 {
        rows(&mut session, &format!("insert into t values ({i}, {i}, \"hello\")"));
    }
    trigger_predicate_columns_collection(&stack, &mut session, "t", &["c"]);
    rows(&mut session, "analyze table t");
    for id in partition_ids(&mut session, "t") {
        let table = session.node().stats_table(id).expect("an analyzed partition");
        assert!(!table.hist_coll.pseudo);
        assert_eq!(table.hist_coll.column_count(), 3);
        assert_eq!(table.hist_coll.index_count(), 1);
        table.hist_coll.for_each_column(|_, column| {
            assert!(column.histogram.len() + column.top_n.as_ref().map_or(0, |topn| topn.num()) > 0);
            false
        });
        table.hist_coll.for_each_index(|_, index| {
            assert!(index.histogram.len() + index.top_n.as_ref().map_or(0, |topn| topn.num()) > 0);
            false
        });
    }

    rows(&mut session, "drop table t");
    rows(&mut session, create_table);
    for i in 1..21 {
        rows(&mut session, &format!("insert into t values ({i}, {i}, \"hello\")"));
    }
    rows(&mut session, "alter table t analyze partition p0");
    for (i, id) in partition_ids(&mut session, "t").into_iter().enumerate() {
        let table = session.node().stats_table(id);
        if i == 0 {
            let table = table.expect("the analyzed partition");
            assert!(!table.hist_coll.pseudo);
            assert_eq!(table.hist_coll.column_count(), 3);
            assert_eq!(table.hist_coll.index_count(), 1);
        } else {
            assert!(
                table.as_ref().is_none_or(|table| table.hist_coll.pseudo),
                "partition {i}: count {:?} version {:?} columns {:?} meta {:?}",
                table.as_ref().map(|table| table.hist_coll.realtime_count),
                table.as_ref().map(|table| table.version),
                table.as_ref().map(|table| table.hist_coll.column_count()),
                displayed(rows(&mut session, "show stats_meta where table_name = 't'"))
            );
        }
    }
}

/// Go `TestExtractTopN`.
#[test]
fn test_extract_topn() {
    let (_stack, _users, mut session) = stack_and_session(402);
    rows(&mut session, "create database if not exists test_extract_topn");
    rows(&mut session, "use test_extract_topn");
    rows(&mut session, "create table test_extract_topn(a int primary key, b int, index index_b(b))");
    rows(&mut session, "set @@session.tidb_analyze_version=2");
    for i in 0..10 {
        rows(&mut session, &format!("insert into test_extract_topn values ({i}, {i})"));
    }
    for i in 0..10 {
        rows(&mut session, &format!("insert into test_extract_topn values ({}, 0)", i + 10));
    }
    rows(&mut session, "analyze table test_extract_topn");
    let id: i64 = displayed(rows(
        &mut session,
        "SELECT TIDB_TABLE_ID FROM information_schema.tables \
         WHERE table_schema = 'test_extract_topn' AND table_name = 'test_extract_topn'",
    ))[0][0]
        .parse()
        .unwrap();
    let table = session.node().stats_table(id).expect("analyzed");
    let column = table.hist_coll.get_column(2).expect("b");
    let column = column.read().unwrap();
    let topn = column.top_n.as_ref().expect("b's TopN");
    assert_eq!(topn.entries().len(), 10);
    assert_eq!(topn.entries()[0].count, 11);
    let index = table.hist_coll.get_index(1).expect("index_b");
    let index = index.read().unwrap();
    let topn = index.top_n.as_ref().expect("index_b's TopN");
    assert_eq!(topn.entries().len(), 10);
    assert_eq!(topn.entries()[0].count, 11);
    let mut want = vec!["test_extract_topn test_extract_topn  b 0 0 11".to_owned()];
    want.extend((1..10).map(|i| format!("test_extract_topn test_extract_topn  b 0 {i} 1")));
    want.push("test_extract_topn test_extract_topn  index_b 1 0 11".to_owned());
    want.extend((1..10).map(|i| format!("test_extract_topn test_extract_topn  index_b 1 {i} 1")));
    assert_eq!(
        joined(sorted(displayed(rows(&mut session, "show stats_topn where column_name in ('b', 'index_b')")))),
        want
    );
}

/// Go `TestAnalyzeFullSamplingOnIndexWithVirtualColumnOrPrefixColumn`.
#[test]
fn test_analyze_full_sampling_on_index_with_virtual_column_or_prefix_column() {
    let (_stack, _users, mut session) = stack_and_session(403);
    rows(&mut session, "create table sampling_index_virtual_col(a int, b int as (a+1), index idx(b))");
    rows(
        &mut session,
        "insert into sampling_index_virtual_col (a) values (1), (2), (null), (3), (4), (null), (5), (5), (5), (5)",
    );
    rows(&mut session, "set @@session.tidb_analyze_version = 2");
    rows(&mut session, "analyze table sampling_index_virtual_col with 1 topn");
    assert_eq!(
        joined(displayed(rows(
            &mut session,
            "show stats_buckets where table_name = 'sampling_index_virtual_col' and column_name = 'idx'"
        ))),
        [
            "test sampling_index_virtual_col  idx 1 0 1 1 2 2 0",
            "test sampling_index_virtual_col  idx 1 1 2 1 3 3 0",
            "test sampling_index_virtual_col  idx 1 2 3 1 4 4 0",
            "test sampling_index_virtual_col  idx 1 3 4 1 5 5 0",
        ]
    );
    assert_eq!(
        joined(displayed(rows(
            &mut session,
            "show stats_topn where table_name = 'sampling_index_virtual_col' and column_name = 'idx'"
        ))),
        ["test sampling_index_virtual_col  idx 1 6 4"]
    );
    let row = displayed(rows(
        &mut session,
        "show stats_histograms where db_name = \"test\" and table_name = \"sampling_index_virtual_col\"",
    ))
    .remove(0);
    // The NDV and the NULLs.
    assert_eq!((row[6].as_str(), row[7].as_str()), ("5", "2"));
    rows(&mut session, "create table sampling_index_prefix_col(a varchar(3), index idx(a(1)))");
    rows(&mut session, "insert into sampling_index_prefix_col (a) values ('aa'), ('ab'), ('ac'), ('bb')");
    rows(&mut session, "analyze table sampling_index_prefix_col with 1 topn");
    assert_eq!(
        joined(displayed(rows(
            &mut session,
            "show stats_buckets where table_name = 'sampling_index_prefix_col' and column_name = 'idx'"
        ))),
        ["test sampling_index_prefix_col  idx 1 0 1 1 b b 0"]
    );
    assert_eq!(
        joined(displayed(rows(
            &mut session,
            "show stats_topn where table_name = 'sampling_index_prefix_col' and column_name = 'idx'"
        ))),
        ["test sampling_index_prefix_col  idx 1 a 3"]
    );
}

/// Go `TestAdjustSampleRateNote`.
#[test]
fn test_adjust_sample_rate_note() {
    let (stack, _users, mut session) = stack_and_session(404);
    rows(&mut session, "create table t(a int, index index_a(a))");
    // Go `statstestutil.HandleNextDDLEventWithTxn`.
    drain_stats_ddl_events(&stack.factory, &mut session);
    let id = table_id_of(&mut session, "t");
    rows(&mut session, &format!("update mysql.stats_meta set count = 220000 where table_id={id}"));
    session.update_stats_cache().expect("statsHandle.Update");
    assert_eq!(displayed(rows(&mut session, "show stats_meta where table_name = 't'"))[0][5], "220000");
    rows(&mut session, "analyze table t");
    assert_eq!(
        joined(displayed(rows(&mut session, "show warnings"))),
        ["Note 1105 Analyze use auto adjusted sample rate 0.500000 for table test.t, reason to use this rate is \"use min(1, 110000/220000) as the sample-rate=0.5\""]
    );
    rows(&mut session, "insert into t values(1),(1),(1)");
    rows(&mut session, "flush stats_delta *.*");
    session.update_stats_cache().expect("statsHandle.Update");
    assert_eq!(displayed(rows(&mut session, "show stats_meta where table_name = 't'"))[0][5], "3");
    rows(&mut session, "analyze table t");
    assert_eq!(
        joined(displayed(rows(&mut session, "show warnings"))),
        ["Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/3) as the sample-rate=1\""]
    );
}

/// Go `TestIssue20874`.
#[test]
fn test_issue_20874() {
    let (_stack, _users, mut session) = stack_and_session(405);
    rows(&mut session, "delete from mysql.stats_histograms");
    rows(
        &mut session,
        "create table t (a char(10) collate utf8mb4_unicode_ci not null, b char(20) collate utf8mb4_general_ci not null, key idxa(a), key idxb(b))",
    );
    rows(&mut session, "insert into t values ('#', 'C'), ('$', 'c'), ('a', 'a')");
    rows(&mut session, "set @@tidb_analyze_version=2");
    rows(&mut session, "analyze table t");
    let mut topn = byte_rows(rows(&mut session, "show stats_topn where db_name = 'test' and table_name = 't'"));
    topn.sort();
    let want: Vec<Vec<u8>> = [
        b"test t  a 0 \x02\xd2 1".as_slice(),
        b"test t  a 0 \x0e\x0f 1",
        b"test t  a 0 \x0e3 1",
        b"test t  b 0 \x00A 1",
        b"test t  b 0 \x00C 2",
        b"test t  idxa 1 \x02\xd2 1",
        b"test t  idxa 1 \x0e\x0f 1",
        b"test t  idxa 1 \x0e3 1",
        b"test t  idxb 1 \x00A 1",
        b"test t  idxb 1 \x00C 2",
    ]
    .into_iter()
    .map(<[u8]>::to_vec)
    .collect();
    assert_eq!(topn, want);
    assert_eq!(
        joined(sorted(displayed(rows(
            &mut session,
            "select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, correlation from mysql.stats_histograms"
        )))),
        ["0 1 3 0 6 2 1", "0 2 2 0 6 2 -0.5", "1 1 3 0 6 2 0", "1 2 2 0 6 2 0"]
    );
}

/// Go `TestAnalyzeClusteredIndexPrimary`.
#[test]
fn test_analyze_clustered_index_primary() {
    let (_stack, _users, mut session) = stack_and_session(406);
    rows(&mut session, "create table t0(a varchar(20), primary key(a) clustered)");
    rows(&mut session, "create table t1(a varchar(20), primary key(a))");
    rows(&mut session, "insert into t0 values('1111')");
    rows(&mut session, "insert into t1 values('1111')");
    rows(&mut session, "set @@session.tidb_analyze_version = 2");
    rows(&mut session, "analyze table t0");
    rows(&mut session, "analyze table t1");
    // unique single column topN is not collected (max count = 1) #66221
    assert!(rows(&mut session, "show stats_topn").is_empty());
    assert_eq!(
        joined(sorted(displayed(rows(&mut session, "show stats_buckets")))),
        [
            "test t0  PRIMARY 1 0 1 1 1111 1111 0",
            "test t0  a 0 0 1 1 1111 1111 0",
            "test t1  PRIMARY 1 0 1 1 1111 1111 0",
            "test t1  a 0 0 1 1 1111 1111 0",
        ]
    );
}

/// Go `TestSmallTableAnalyzeV2`. Its `calcSampleRateByStorageCount` enable
/// names `pkg/executor`, not the failpoint's package
/// (`executor/internal/pdhelper`), so in Go it changes nothing: no PD answers
/// the region count and the pseudo row count sets the rate.
#[test]
fn test_small_table_analyze_v2() {
    let _force_dynamic_prune =
        tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", "return(true)");
    let (stack, _users, mut session) = stack_and_session(407);
    rows(&mut session, "set @@session.tidb_analyze_version = 2");
    rows(&mut session, "create table small_table_inject_pd(a int)");
    trigger_predicate_columns_collection(&stack, &mut session, "small_table_inject_pd", &["a"]);
    rows(&mut session, "insert into small_table_inject_pd values(1), (2), (3), (4), (5)");
    rows(&mut session, "analyze table small_table_inject_pd");
    assert_eq!(
        joined(displayed(rows(&mut session, "show warnings"))),
        ["Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\""]
    );
    rows(
        &mut session,
        "create table small_table_inject_pd_with_partition(a int) partition by range(a) \
         (partition p0 values less than (5), partition p1 values less than (10), partition p2 values less than (15))",
    );
    rows(&mut session, "insert into small_table_inject_pd_with_partition values(1), (6), (11)");
    trigger_predicate_columns_collection(&stack, &mut session, "small_table_inject_pd_with_partition", &["a"]);
    rows(&mut session, "analyze table small_table_inject_pd_with_partition");
    assert_eq!(
        joined(displayed(rows(&mut session, "show warnings"))),
        (0..3)
            .map(|p| format!(
                "Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd_with_partition's partition p{p}, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\""
            ))
            .collect::<Vec<_>>()
    );
    let at = |rows: Vec<Vec<String>>, columns: &[usize]| -> Vec<Vec<String>> {
        sorted(rows.into_iter().map(|row| columns.iter().map(|&i| row[i].clone()).collect()).collect())
    };
    assert_eq!(
        at(
            displayed(rows(
                &mut session,
                "show column_stats_usage where db_name = 'test' and table_name = 'small_table_inject_pd_with_partition' and last_analyzed_at is not null"
            )),
            &[2, 3]
        ),
        [["global", "a"], ["p0", "a"], ["p1", "a"], ["p2", "a"]]
    );
    assert_eq!(
        at(
            displayed(rows(
                &mut session,
                "show stats_meta where db_name = 'test' and table_name = 'small_table_inject_pd_with_partition'"
            )),
            &[2, 4, 5]
        ),
        [["global", "0", "3"], ["p0", "0", "1"], ["p1", "0", "1"], ["p2", "0", "1"]]
    );
}

/// Go `TestAnalyzePartitionVerify`.
#[test]
fn test_analyze_partition_verify() {
    let (stack, _users, mut session) = stack_and_session(408);
    let mut sql = "create table t(a int,b varchar(100),c int,INDEX idx_c(c)) PARTITION BY RANGE ( a ) (".to_owned();
    for n in (100..1000).step_by(100) {
        sql += &format!("PARTITION p{n} VALUES LESS THAN ({n}),");
    }
    sql += "PARTITION p1000 VALUES LESS THAN MAXVALUE)";
    rows(&mut session, &sql);
    let mut insert = "insert into t (a,b,c) values(0, 'abc', 0)".to_owned();
    for i in 1..1000 {
        insert += &format!(" ,({i}, 'abc', {i})");
    }
    rows(&mut session, &insert);
    trigger_predicate_columns_collection(&stack, &mut session, "t", &["a", "b", "c"]);
    rows(&mut session, "analyze table t");
    let result = displayed(rows(&mut session, "show stats_histograms where Db_name='test'"));
    // 4 columns * 10 partitions + 4 global columns
    assert_eq!(result.len(), 4 + 4 * 10);
    for row in result {
        let want = match (row[2].as_str(), row[3].as_str()) {
            (_, "b") => "1",
            ("global", _) => "1000",
            _ => "100",
        };
        assert_eq!(row[6], want, "{row:?}");
    }
}

/// Go `TestIssue55438`.
#[test]
fn test_issue_55438() {
    let (_stack, _users, mut session) = stack_and_session(409);
    rows(
        &mut session,
        "CREATE TABLE t0(c0 NUMERIC , c1 BIGINT UNSIGNED  AS ((CASE 0 WHEN false THEN 1358571571 ELSE TRIM(c0) END )));",
    );
    rows(&mut session, "CREATE INDEX i0 ON t0(c1);");
    rows(&mut session, "analyze table t0");
}

/// Go `TestIssue61609`: an ANALYZE with one sample of ten rows scales the
/// TopN count to the table.
#[test]
fn test_issue_61609() {
    let (stack, _users, mut session) = stack_and_session(410);
    rows(&mut session, "create table t (a int);");
    rows(&mut session, "insert into t values (0),(0),(0),(0),(0),(0),(0),(0),(0),(0);");
    rows(&mut session, "explain select * from t where a = 0");
    trigger_predicate_columns_collection(&stack, &mut session, "t", &["a"]);
    rows(&mut session, "analyze table t with 1 topn, 1 samples");
    rows(&mut session, "explain select * from t where a = 0");
    assert_eq!(
        joined(sorted(displayed(rows(&mut session, "show stats_topn where db_name = 'test' and table_name = 't'")))),
        ["test t  a 0 0 10"]
    );
}

/// Go `TestIssue66918`.
#[test]
fn test_issue_66918() {
    let (_stack, _users, mut session) = stack_and_session(411);
    rows(
        &mut session,
        "CREATE TABLE t (j JSON, g VARCHAR(255) GENERATED ALWAYS AS ((j->'$.v')) STORED, UNIQUE INDEX g_idx (g))",
    );
    rows(&mut session, "INSERT INTO t(j) VALUES ('{\"v\":1}'), ('{\"v\":2}')");
    rows(&mut session, "ANALYZE TABLE t");
    let id = table_id_of(&mut session, "t");
    assert_eq!(
        joined(displayed(rows(
            &mut session,
            &format!("select is_index, stats_ver from mysql.stats_histograms where table_id = {id} order by is_index, hist_id")
        ))),
        ["0 2", "1 2"]
    );
    let table = session.node().stats_table(id).expect("analyzed");
    assert!(table.hist_coll.get_index(1).expect("g_idx").read().unwrap().is_analyzed());
}

/// Go `TestNULLOnFullSampling` (`pkg/statistics`).
#[test]
fn test_null_on_full_sampling() {
    let (_stack, _users, mut session) = stack_and_session(412);
    rows(&mut session, "set @@session.tidb_analyze_version = 2;");
    rows(&mut session, "create table t(a int, index idx(a))");
    rows(&mut session, "insert into t values(1), (1), (1), (2), (2), (3), (4), (null), (null), (null)");
    rows(&mut session, "analyze table t with 2 topn");
    session.update_stats_cache().expect("statsHandle.Update");
    let id = table_id_of(&mut session, "t");
    let table = session.node().stats_table(id).expect("analyzed");
    // The null count is 3.
    table.hist_coll.for_each_column(|_, column| {
        assert_eq!(column.histogram.null_count, 3);
        false
    });
    // The topn and buckets contain no null values.
    assert_eq!(
        joined(displayed(rows(&mut session, "show stats_topn"))),
        ["test t  a 0 1 3", "test t  a 0 2 2", "test t  idx 1 1 3", "test t  idx 1 2 2"]
    );
    assert_eq!(
        joined(displayed(rows(&mut session, "show stats_buckets"))),
        [
            "test t  a 0 0 1 1 3 3 0",
            "test t  a 0 1 2 1 4 4 0",
            "test t  idx 1 0 1 1 3 3 0",
            "test t  idx 1 1 2 1 4 4 0",
        ]
    );
}

/// Go `TestSingleColumnIndexNDV` (`pkg/statistics`).
#[test]
fn test_single_column_index_ndv() {
    let (stack, _users, mut session) = stack_and_session(413);
    rows(
        &mut session,
        "create table t(a int, b int, c varchar(20), d varchar(20), index idx_a(a), index idx_b(b), index idx_c(c), index idx_d(d))",
    );
    // Go `statstestutil.HandleNextDDLEventWithTxn`.
    drain_stats_ddl_events(&stack.factory, &mut session);
    rows(&mut session, "insert into t values (1, 1, 'xxx', 'zzz'), (2, 2, 'yyy', 'zzz'), (1, 3, null, 'zzz')");
    for _ in 0..5 {
        rows(&mut session, "insert into t select * from t");
    }
    rows(&mut session, "analyze table t");
    let got: Vec<[String; 3]> =
        sorted(displayed(rows(&mut session, "show stats_histograms where db_name = 'test' and table_name = 't'")))
            .into_iter()
            .map(|row| [row[3].clone(), row[6].clone(), row[7].clone()])
            .collect();
    let want: Vec<[String; 3]> = [
        ["a", "2", "0"],
        ["b", "3", "0"],
        ["c", "2", "32"],
        ["d", "1", "0"],
        ["idx_a", "2", "0"],
        ["idx_b", "3", "0"],
        ["idx_c", "2", "32"],
        ["idx_d", "1", "0"],
    ]
    .into_iter()
    .map(|row| row.map(str::to_owned))
    .collect();
    assert_eq!(got, want);
}

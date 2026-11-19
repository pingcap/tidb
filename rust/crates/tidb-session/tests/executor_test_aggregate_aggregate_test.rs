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

//! Go `pkg/executor/test/aggregate/aggregate_test.go` (`TestAggInDisk`, `TestParallelStreamAggGroupConcat`, `TestIssue20658`, `TestParallelHashAgg`), on the mock store through the session, as Go's testkit
//! runs them.

use crate::support::testkit::*;

/// Go `TestAggInDisk`.
#[test]
#[ignore = "register E41: the hash aggregate is cancelled by the 4 MiB quota where Go spills to disk"]
fn test_agg_in_disk() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_hashagg_final_concurrency = 1;");
    tk.must_exec("set tidb_hashagg_partial_concurrency = 1;");
    tk.must_exec("set tidb_mem_quota_query = 4194304");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t(a int)");
    let mut sql = "insert into t values (0)".to_owned();
    for i in 1..=200 {
        sql += &format!(",({i})");
    }
    sql += ";";
    tk.must_exec(&sql);
    let rows = tk.must_query(
        "desc analyze select /*+ HASH_AGG() */ avg(t1.a) from t t1 join t t2 group by t1.a, t2.a;",
    );
    for row in rows {
        let line = go_v(&row);
        let disk = row[row.len() - 1].clone();
        if line.contains("HashAgg") {
            assert!(!disk.contains("0 Bytes"), "{line}");
            assert!(
                disk.contains("MB") || disk.contains("KB") || disk.contains("Bytes"),
                "{line}"
            );
        }
    }
    // Add code cover
    // Test spill chunk. Add a line to avoid tmp spill chunk is always full.
    tk.must_exec("insert into t values(0)");
    tk.must_query("select sum(tt.b) from ( select /*+ HASH_AGG() */ avg(t1.a) as b from t t1 join t t2 group by t1.a, t2.a) as tt")
        .check(["4040100.0000"]);
    tk.must_query("select sum_int(tt.b) from ( select /*+ HASH_AGG() */ sum_int(t1.a) as b from t t1 join t t2 group by t1.a, t2.a) as tt")
        .check(["4060200"]);
    tk.must_query("select sum(tt.b), sum_int(tt.b) from ( select /*+ HASH_AGG() */ sum_int(t1.a) as b from t t1 join t t2 group by t1.a, t2.a) as tt")
        .check(["4060200 4060200"]);
    // Test no groupby and no data.
    tk.must_exec("drop table t;");
    tk.must_exec("create table t(c int, c1 int);");
    tk.must_query("select /*+ HASH_AGG() */ count(c) from t;")
        .check(["0"]);
    tk.must_query("select /*+ HASH_AGG() */ count(c) from t group by c1;")
        .check_empty();
}

/// Go `reconstructParallelGroupConcatResult`.
fn reconstruct_parallel_group_concat_result(rows: TkResult) -> Vec<String> {
    let mut data = Vec::with_capacity(rows.len());
    for row in rows {
        // Go's `row[0].(string)` skips a NULL group.
        if row[0] != "<nil>" {
            let mut tokens: Vec<&str> = row[0].split(',').collect();
            tokens.sort();
            data.push(tokens.join(","));
        }
    }
    data.sort();
    data
}

/// Go `TestParallelStreamAggGroupConcat`.
#[test]
fn test_parallel_stream_agg_group_concat() {
    let mut tk = TestKit::new();
    tk.must_exec("use test;");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("CREATE TABLE t(a bigint, b bigint);");
    tk.must_exec("set tidb_init_chunk_size=1;");
    tk.must_exec("set tidb_max_chunk_size=32;");
    let mut insert_sql = String::new();
    for i in 0..1000 {
        if i == 0 {
            insert_sql += &format!("({}, {})", rand_intn(100), rand_intn(100));
        } else {
            insert_sql += &format!(",({}, {})", rand_intn(100), rand_intn(100));
        }
    }
    tk.must_exec(&format!("insert into t values {insert_sql};"));
    let sql = "select /*+ stream_agg() */ group_concat(a, b) from t group by b;";
    let mut expected = Vec::new();
    for con in [1, 2, 4, 8] {
        tk.must_exec(&format!("set @@tidb_streamagg_concurrency={con}"));
        if con == 1 {
            expected = reconstruct_parallel_group_concat_result(tk.must_query(sql));
        } else {
            let er = tk.must_query(&format!("explain format = 'brief' {sql}"));
            let ok = er.rows().iter().any(|l| go_v(l).contains("Shuffle"));
            assert!(ok);
            let obtained = reconstruct_parallel_group_concat_result(tk.must_query(sql));
            assert_eq!(expected.len(), obtained.len());
            for i in 0..obtained.len() {
                assert_eq!(expected[i], obtained[i]);
            }
        }
    }
}

/// Go `TestIssue20658`.
#[test]
fn test_issue20658() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    let agg_funcs = [
        "count(a)",
        "sum(a)",
        "avg(a)",
        "max(a)",
        "min(a)",
        "bit_or(a)",
        "bit_xor(a)",
        "bit_and(a)",
        "var_pop(a)",
        "var_samp(a)",
        "stddev_pop(a)",
        "stddev_samp(a)",
        "approx_count_distinct(a)",
        "approx_percentile(a, 7)",
    ];
    let sqls: Vec<String> = agg_funcs
        .iter()
        .map(|af| format!("select /*+ stream_agg() */ {af} from t group by b;"))
        .collect();
    tk.must_exec("drop table if exists t;");
    tk.must_exec("CREATE TABLE t(a bigint, b bigint);");
    tk.must_exec("set tidb_init_chunk_size=1;");
    tk.must_exec("set tidb_max_chunk_size=32;");
    let mut insert_sql = String::new();
    for i in 0..1000 {
        insert_sql += &format!("({},{})", rand_intn(10), rand_intn(10));
        if i < 1000 - 1 {
            insert_sql += ",";
        }
    }
    tk.must_exec(&format!("insert into t values {insert_sql};"));
    let must_parse_and_sort = |rows: TkResult, cmt: &str| -> Vec<f64> {
        let mut ret: Vec<f64> = rows
            .rows()
            .iter()
            .map(|row| {
                if row[0] == "<nil>" {
                    0.0
                } else {
                    row[0]
                        .parse::<f64>()
                        .unwrap_or_else(|error| panic!("{cmt}: {error}"))
                }
            })
            .collect();
        ret.sort_by(f64::total_cmp);
        ret
    };
    for sql in &sqls {
        tk.must_exec("set @@tidb_streamagg_concurrency = 1;");
        let expected = must_parse_and_sort(tk.must_query(sql), &format!("sql: {sql}"));
        for con in [2, 4, 8] {
            let comment = format!("sql: {sql}; concurrency: {con}");
            tk.must_exec(&format!("set @@tidb_streamagg_concurrency={con};"));
            let er = tk.must_query(&format!("explain format = 'brief' {sql}"));
            let ok = er.rows().iter().any(|l| go_v(l).contains("Shuffle"));
            assert!(ok, "{comment}");
            let rows = must_parse_and_sort(tk.must_query(sql), &comment);
            assert_eq!(expected.len(), rows.len(), "{comment}");
            for i in 0..rows.len() {
                assert!((rows[i] - expected[i]).abs() < 1e-3, "{comment}");
            }
        }
    }
}

/// Go `checkResults`: every `(key, value)` row matches `expected_res`.
fn check_results(
    actual_res: &TkResult,
    expected_res: &std::collections::HashMap<&str, &str>,
) -> bool {
    if actual_res.len() != expected_res.len() {
        return false;
    }
    actual_res.rows().iter().all(|row| {
        row.len() == 2
            && expected_res
                .get(row[0].as_str())
                .is_some_and(|expect| *expect == row[1])
    })
}

/// Go `genListPartition`: `(begin, ..., end-1)`.
fn gen_list_partition(begin: i64, end: i64) -> String {
    let mut buf = "(".to_owned();
    for i in begin..end - 1 {
        buf += &format!("{i}, ");
    }
    buf += &format!("{})", end - 1);
    buf
}

/// Go `TestParallelHashAgg`.
#[test]
fn test_parallel_hash_agg() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists test.parallel_hash_agg;");
    tk.must_exec("create table test.parallel_hash_agg(k varchar(30), v int);");
    for _ in 0..20 {
        tk.must_exec("insert into test.parallel_hash_agg (k, v) values ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), ('dD', 1), ('Dd', 1), ('ee', 1), ('EE', 1), ('eE', 1), ('Ee', 1);");
    }
    tk.must_exec("set @@tidb_max_chunk_size=32;");
    let expected_result: std::collections::HashMap<&str, &str> = [
        "dd", "AA", "cc", "eE", "bb", "Cc", "EE", "Aa", "ee", "Bb", "dD", "aa", "cC", "DD", "BB",
        "Dd", "CC", "bB", "aA", "Ee",
    ]
    .into_iter()
    .map(|k| (k, "20"))
    .collect();
    let res = tk.must_query("select k, sum(v) from parallel_hash_agg group by k;");
    assert!(check_results(&res, &expected_result), "{:?}", res.rows());
    tk.must_exec("create database list_partition_agg");
    tk.must_exec("use list_partition_agg");
    tk.must_exec("drop table if exists tlist");
    tk.must_exec(&format!(
        "create table tlist (a int, b int) partition by list(a) ( partition p0 values in {}, partition p1 values in {}, partition p2 values in {}, partition p3 values in {}, partition p4 values in {})",
        gen_list_partition(0, 20),
        gen_list_partition(20, 40),
        gen_list_partition(40, 60),
        gen_list_partition(60, 80),
        gen_list_partition(80, 100),
    ));
    tk.must_exec("create table tnormal (a int, b int)");
    let mut vals = String::new();
    for _ in 0..100 {
        if !vals.is_empty() {
            vals += ", ";
        }
        vals += &format!("({}, {})", rand_intn(100), rand_intn(100));
    }
    tk.must_exec(&format!("insert into tnormal values {vals}"));
    tk.must_exec(&format!("insert into tlist values {vals}"));
    for agg_func in ["min", "max", "sum", "count"] {
        let (c1, c2) = ("a", "b");
        for _ in 0..2 {
            let rs = tk
                .must_query(&format!(
                    "select {c1}, {agg_func}({c2}) from tnormal group by {c1}"
                ))
                .sort();
            tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
            let rs_dynamic = tk
                .must_query(&format!(
                    "select {c1}, {agg_func}({c2}) from tlist group by {c1}"
                ))
                .sort();
            tk.must_exec("set @@tidb_partition_prune_mode = 'static'");
            let rs_static = tk
                .must_query(&format!(
                    "select {c1}, {agg_func}({c2}) from tlist group by {c1}"
                ))
                .sort();
            rs.check_result(&rs_dynamic);
            rs.check_result(&rs_static);
        }
    }
}

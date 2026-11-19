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

//! Go `pkg/executor/join/test/mergejoin/merge_join_test.go`
//! (`TestVectorizedMergeJoin`, `TestMergeJoinInDisk`,
//! `TestShuffleMergeJoinInDisk`), on the mock store through the session, as
//! Go's testkit runs it.

use crate::support::testkit::*;

/// Go `TestVectorizedMergeJoin`.
#[test]
fn test_vectorized_merge_join() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_opt_advanced_join_hint=0");
    let mut exist_table_map = std::collections::BTreeSet::new();
    let mut run_test = |tk: &mut TestKit, ts1: &[i64], ts2: &[i64]| {
        let mut get_table = |tk: &mut TestKit, prefix: &str, ts: &[i64]| -> String {
            let mut table_name = prefix.to_owned();
            for i in ts {
                table_name += &format!("_{i}");
            }
            if exist_table_map.contains(&table_name) {
                return table_name;
            }
            tk.must_exec(&format!("drop table if exists {table_name}"));
            tk.must_exec(&format!("create table {table_name} (a int, b int)"));
            exist_table_map.insert(table_name.clone());
            for (i, n) in ts.iter().enumerate() {
                if *n == 0 {
                    continue;
                }
                let mut buf = format!("insert into {table_name} values ");
                for j in 0..*n {
                    if j > 0 {
                        buf += ", ";
                    }
                    buf += &format!("({i}, {})", rand_intn(10));
                }
                tk.must_exec(&buf);
            }
            table_name
        };
        let t1 = get_table(tk, "t", ts1);
        let mut t2 = get_table(tk, "t", ts2);
        if t1 == t2 {
            t2 = get_table(tk, "t2", ts2);
        }
        tk.must_query(&format!(
            "explain format = 'brief' select /*+ TIDB_SMJ({t1}, {t2}) */ * from {t1}, {t2} where {t1}.a={t2}.a and {t1}.b>5 and {t2}.b<5"
        ))
        .check([
            format!("MergeJoin 4150.01 root  inner join, left key:test.{t1}.a, right key:test.{t2}.a"),
            format!("├─Sort(Build) 3320.01 root  test.{t2}.a"),
            "│ └─TableReader 3320.01 root  data:Selection".to_owned(),
            format!("│   └─Selection 3320.01 cop[tikv]  lt(test.{t2}.b, 5), not(isnull(test.{t2}.a))"),
            format!("│     └─TableFullScan 10000.00 cop[tikv] table:{t2} keep order:false, stats:pseudo"),
            format!("└─Sort(Probe) 3330.00 root  test.{t1}.a"),
            "  └─TableReader 3330.00 root  data:Selection".to_owned(),
            format!("    └─Selection 3330.00 cop[tikv]  gt(test.{t1}.b, 5), not(isnull(test.{t1}.a))"),
            format!("      └─TableFullScan 10000.00 cop[tikv] table:{t1} keep order:false, stats:pseudo"),
        ]);
        tk.must_query(&format!(
            "explain format = 'brief' select /*+ TIDB_HJ({t1}, {t2}) */ * from {t1}, {t2} where {t1}.a={t2}.a and {t1}.b>5 and {t2}.b<5"
        ))
        .check([
            format!("HashJoin 4150.01 root  inner join, equal:[eq(test.{t1}.a, test.{t2}.a)]"),
            "├─TableReader(Build) 3320.01 root  data:Selection".to_owned(),
            format!("│ └─Selection 3320.01 cop[tikv]  lt(test.{t2}.b, 5), not(isnull(test.{t2}.a))"),
            format!("│   └─TableFullScan 10000.00 cop[tikv] table:{t2} keep order:false, stats:pseudo"),
            "└─TableReader(Probe) 3330.00 root  data:Selection".to_owned(),
            format!("  └─Selection 3330.00 cop[tikv]  gt(test.{t1}.b, 5), not(isnull(test.{t1}.a))"),
            format!("    └─TableFullScan 10000.00 cop[tikv] table:{t1} keep order:false, stats:pseudo"),
        ]);
        let r1 = tk
            .must_query(&format!(
                "select /*+ TIDB_SMJ({t1}, {t2}) */ * from {t1}, {t2} where {t1}.a={t2}.a and {t1}.b>5 and {t2}.b<5"
            ))
            .sort();
        let r2 = tk
            .must_query(&format!(
                "select /*+ TIDB_HJ({t1}, {t2}) */ * from {t1}, {t2} where {t1}.a={t2}.a and {t1}.b>5 and {t2}.b<5"
            ))
            .sort();
        assert_eq!(r2.len(), r1.len());
        let mut i = 0;
        let n = r1.len();
        while i < n {
            assert_eq!(r2.rows()[i], r1.rows()[i]);
            // just compare parts of results to speed up
            i += rand_intn(((n - i) / 5 + 1) as i64) as usize + 1;
        }
    };
    // Go sets `SessionVars.MaxChunkSize = vardef.DefInitChunkSize` (32),
    // which is what `tidb_max_chunk_size`'s SetSession stores.
    tk.must_exec("set @@tidb_max_chunk_size = 32");
    let chunk_size: i64 = 32;
    let cases: [(&[i64], &[i64]); 15] = [
        (&[0], &[chunk_size]),
        (&[0], &[chunk_size - 1]),
        (&[0], &[chunk_size + 1]),
        (&[1], &[chunk_size]),
        (&[1], &[chunk_size - 1]),
        (&[1], &[chunk_size + 1]),
        (&[chunk_size - 1], &[chunk_size]),
        (&[chunk_size - 1], &[chunk_size - 1]),
        (&[chunk_size - 1], &[chunk_size + 1]),
        (&[chunk_size], &[chunk_size]),
        (&[chunk_size], &[chunk_size + 1]),
        (&[chunk_size + 1], &[chunk_size + 1]),
        (
            &[1, 1, 1],
            &[chunk_size + 1, chunk_size * 5 + 5, chunk_size - 5],
        ),
        (
            &[0, 0, chunk_size],
            &[chunk_size + 1, chunk_size * 5 + 5, chunk_size - 5],
        ),
        (
            &[chunk_size + 1, 0, chunk_size],
            &[chunk_size + 1, chunk_size * 5 + 5, chunk_size - 5],
        ),
    ];
    for (t1, t2) in cases {
        run_test(&mut tk, t1, t2);
        run_test(&mut tk, t2, t1);
    }
    drop(run_test);
    for table_name in &exist_table_map {
        tk.must_exec(&format!("drop table if exists {table_name}"));
    }
}

/// Go `checkMergeAndRun`: the plan is a merge join, and the query's result.
fn check_merge_and_run(tk: &mut TestKit, sql: &str) -> TkResult {
    let result = tk.must_query(&format!("explain format = 'brief' {sql}"));
    let result_str = format!("{:?}", result.rows());
    assert!(result_str.contains("MergeJoin"), "{result_str}");
    tk.must_query(sql)
}

/// Go `TestMergeJoinInDisk`.
#[test]
fn test_merge_join_in_disk() {
    let temp_dir = std::env::temp_dir().join(format!("tidb-mj-{}", std::process::id()));
    let _config = update_global_config(|conf| conf.temp_storage_path = temp_dir.to_string_lossy().into_owned());
    let _fp = enable_failpoint("github.com/pingcap/tidb/pkg/executor/join/testMergeJoinRowContainerSpill", "return(true)");
    let mut tk = TestKit::new();
    tk.must_exec("SET GLOBAL tidb_mem_oom_action='LOG'");
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_mem_quota_query=1;");
    tk.must_exec("drop table if exists t");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t(c1 int, c2 int)");
    tk.must_exec("create table t1(c1 int, c2 int)");
    tk.must_exec("insert into t values(1,1)");
    tk.must_exec("insert into t1 values(1,3),(4,4)");
    let result = check_merge_and_run(
        &mut tk,
        "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20",
    );
    result.check(["1 3 1 1"]);
    let (mem, disk) = tk.session().stmt_ctx_trackers().expect("the statement's trackers");
    assert_eq!(0, mem.bytes_consumed());
    assert!(mem.max_consumed() > 0);
    assert_eq!(0, disk.bytes_consumed());
    assert!(disk.max_consumed() > 0);
    tk.must_exec("SET GLOBAL tidb_mem_oom_action = DEFAULT");
}

/// Go `TestShuffleMergeJoinInDisk`.
#[test]
fn test_shuffle_merge_join_in_disk() {
    let _fp = enable_failpoint("github.com/pingcap/tidb/pkg/executor/join/testMergeJoinRowContainerSpill", "return(true)");
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_mem_quota_query=1;");
    tk.must_exec("set @@tidb_merge_join_concurrency=4;");
    tk.must_exec("set @@tidb_max_chunk_size=32;");
    tk.must_exec("drop table if exists t");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t(c1 int, c2 int)");
    tk.must_exec("create table t1(c1 int, c2 int)");
    tk.must_exec("insert into t values(1,1),(2,2),(3,3),(4,4)");
    for i in (1..=1024).step_by(4) {
        tk.must_exec(&format!(
            "insert into t1 values({},{}),({},{}),({},{}),({},{})",
            i, i, i + 1, i + 1, i + 2, i + 2, i + 3, i + 3
        ));
    }
    let result = check_merge_and_run(
        &mut tk,
        "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20",
    );
    let mut expect = vec!["1 1 1 1".to_owned()];
    for i in 21..=1024 {
        expect.push(format!("{i} {i} <nil> <nil>"));
    }
    expect.sort();
    result.sort().check(expect);
    let (mem, disk) = tk.session().session_trackers().expect("the session's trackers");
    assert_eq!(0, mem.bytes_consumed());
    assert!(mem.max_consumed() > 0);
    assert_eq!(0, disk.bytes_consumed());
    assert!(disk.max_consumed() > 0);
}

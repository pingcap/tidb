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

//! Go `TestAddIndexWithAnalyze`: with `tidb_stats_update_during_ddl` ON, ADD
//! INDEX and MODIFY COLUMN analyze the table once the reorganization is done,
//! so the new index has statistics of the same version as the columns; a
//! partitioned table is never analyzed by the DDL.

use super::*;

fn table_id(session: &mut Session, name: &str) -> i64 {
    displayed(rows(
        session,
        &format!("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{name}'"),
    ))[0][0]
        .parse()
        .unwrap()
}

/// `(is_index, hist_id) -> version` of the table's statistics.
fn stats_versions(session: &mut Session, table_id: i64) -> std::collections::BTreeMap<(String, String), String> {
    displayed(rows(
        session,
        &format!("select is_index, hist_id, version from mysql.stats_histograms where table_id = {table_id}"),
    ))
    .into_iter()
    .map(|row| ((row[0].clone(), row[1].clone()), row[2].clone()))
    .collect()
}

fn index_id(session: &mut Session, table: &str, index: &str) -> String {
    displayed(rows(
        session,
        &format!(
            "select index_id from information_schema.tidb_indexes where table_schema = 'test' and table_name = '{table}' and key_name = '{index}'"
        ),
    ))[0][0]
        .clone()
}

fn column_id(session: &mut Session, table_id: i64, ordinal: usize) -> String {
    // Columns of a fresh table take ids in declaration order from 1.
    let _ = (session, table_id);
    ordinal.to_string()
}

#[test]
fn add_index_and_modify_column_analyze_the_table_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(2201)).unwrap();
    rows(&mut tk, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut tk, "use test");
    rows(&mut tk, "set @@tidb_stats_update_during_ddl = 1");
    rows(&mut tk, "create table t(a int NOT NULL DEFAULT 10, b int, index idx_b(b))");
    for i in 0..50 {
        rows(&mut tk, &format!("insert into t values ({i}, {i})"));
    }
    rows(&mut tk, "ALTER TABLE t ADD index idx(a)");
    let t = table_id(&mut tk, "t");
    let idx = index_id(&mut tk, "t", "idx");
    let idx_b = index_id(&mut tk, "t", "idx_b");
    let (a, b) = (column_id(&mut tk, t, 1), column_id(&mut tk, t, 2));
    let versions = stats_versions(&mut tk, t);
    let version = |versions: &std::collections::BTreeMap<(String, String), String>, is_index: &str, id: &str| {
        versions
            .get(&(is_index.to_owned(), id.to_owned()))
            .cloned()
            .unwrap_or_else(|| panic!("no statistics for ({is_index}, {id}) in {versions:?}"))
    };
    // The new index and every other column and index are analyzed together.
    let col_a_stats_ver = version(&versions, "0", &a);
    assert_eq!(version(&versions, "1", &idx), col_a_stats_ver);
    assert_eq!(version(&versions, "0", &b), col_a_stats_ver);
    assert_eq!(version(&versions, "1", &idx_b), col_a_stats_ver);
    assert_eq!(
        displayed(rows(
            &mut tk,
            "select state from mysql.analyze_jobs where table_schema = 'test' and table_name = 't'"
        )),
        [["finished"]]
    );

    // Go `queryAnalyzeStatusSince` compares `start_time` at second
    // granularity: the next job must start in a later second than this
    // ANALYZE, or it reads this ANALYZE as its own.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    // test alter column
    rows(&mut tk, "ALTER TABLE t modify column a varchar(10)");
    let versions = stats_versions(&mut tk, t);
    let idx = index_id(&mut tk, "t", "idx");
    let a2 = displayed(rows(
        &mut tk,
        "select ordinal_position from information_schema.columns where table_schema = 'test' and table_name = 't' and column_name = 'a'",
    ))[0][0]
        .clone();
    assert_eq!(a2, "1");
    let col_b_stats_ver2 = version(&versions, "0", &b);
    assert_ne!(col_b_stats_ver2, col_a_stats_ver, "MODIFY COLUMN analyzed the table again");
    assert_eq!(version(&versions, "1", &idx), col_b_stats_ver2);
    assert_eq!(version(&versions, "1", &idx_b), col_b_stats_ver2);

    // for partition table, add index with analyze should be banned.
    rows(
        &mut tk,
        "CREATE TABLE pt(id INT NOT NULL, stu_id INT NOT NULL) PARTITION BY RANGE (stu_id) (PARTITION p0 VALUES LESS THAN (25),PARTITION p1 VALUES LESS THAN (51))",
    );
    for i in 0..50 {
        rows(&mut tk, &format!("insert into pt values ({i},{i})"));
    }
    rows(&mut tk, "analyze table pt all columns");
    let pt = table_id(&mut tk, "pt");
    let before = stats_versions(&mut tk, pt);
    rows(&mut tk, "ALTER TABLE pt ADD index idx(id)");
    let pt_idx = index_id(&mut tk, "pt", "idx");
    let after = stats_versions(&mut tk, pt);
    // index is not analyzed, and the column keeps its earlier statistics.
    assert!(!after.contains_key(&("1".to_owned(), pt_idx)), "{after:?}");
    assert_eq!(before, after);
}

/// Go `DefTiDBEnableDDLAnalyze` is OFF: ADD INDEX leaves the statistics
/// alone.
#[test]
fn add_index_without_stats_update_during_ddl_does_not_analyze_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(2202)).unwrap();
    rows(&mut tk, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut tk, "use test");
    rows(&mut tk, "create table t(a int, b int)");
    rows(&mut tk, "insert into t values (1, 1), (2, 2)");
    rows(&mut tk, "ALTER TABLE t ADD index idx(a)");
    let t = table_id(&mut tk, "t");
    assert!(stats_versions(&mut tk, t).is_empty());
}

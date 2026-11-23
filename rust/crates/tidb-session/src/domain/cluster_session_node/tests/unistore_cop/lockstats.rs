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

//! Go `pkg/statistics/handle/handletest/lockstats`: ANALYZE skips the
//! tables and partitions whose statistics are locked (`filterAndCollectTasks`).
//! Go compares the cached `*statistics.Table`; these read the stored rows.

use super::*;

const SELECT_TABLE_LOCK_SQL: &str = "select count(*) from mysql.stats_table_locked";

fn session_with(connection: u64, table: &str) -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users) = cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(connection)).expect("session opens");
    rows(&mut session, "set @@tidb_analyze_version = 2");
    rows(&mut session, "use test");
    rows(&mut session, table);
    rows(&mut session, "analyze table test.t");
    (stack, users, session)
}

fn warnings(session: &mut Session) -> Vec<String> {
    displayed(rows(session, "show warnings")).into_iter().map(|row| row.join(" ")).collect()
}

fn locked_count(session: &mut Session) -> String {
    displayed(rows(session, SELECT_TABLE_LOCK_SQL))[0][0].clone()
}

/// `(partition, modify_count, row_count)` of `SHOW STATS_META` for `table`.
fn stats_meta(session: &mut Session, table: &str) -> Vec<(String, String, String)> {
    let mut meta = displayed(rows(
        session,
        &format!("show stats_meta where db_name = 'test' and table_name = '{table}'"),
    ))
    .into_iter()
    .map(|row| (row[2].clone(), row[4].clone(), row[5].clone()))
    .collect::<Vec<_>>();
    meta.sort();
    meta
}

/// Go `TestLockAndUnlockTableStats`.
#[test]
fn test_lock_and_unlock_table_stats() {
    let (_stack, _users, mut session) =
        session_with(1201, "create table t(a int, b varchar(10), index idx_b (b))");
    rows(&mut session, "lock stats t");
    assert_eq!(locked_count(&mut session), "1");
    rows(&mut session, "insert into t(a, b) values(1,'a')");
    rows(&mut session, "insert into t(a, b) values(2,'b')");
    let before = stats_meta(&mut session, "t");
    rows(&mut session, "analyze table test.t");
    assert!(warnings(&mut session).contains(&"Warning 1105 skip analyze locked table: test.t".to_owned()));
    assert_eq!(stats_meta(&mut session, "t"), before);

    rows(&mut session, "insert into t(a, b) values(3,'c')");
    rows(&mut session, "flush stats_delta *.*");
    rows(&mut session, "unlock stats t");
    assert_eq!(locked_count(&mut session), "0");
    rows(&mut session, "analyze table test.t");
    assert_eq!(stats_meta(&mut session, "t")[0].2, "3");
}

/// Go `TestLockAndUnlockTablesStats`.
#[test]
fn test_lock_and_unlock_tables_stats() {
    let (stack, _users) = cop_backed_stack_with_stats_lease(Some(tidb_domain_core::node_config::StatsLease::Zero));
    let mut session = stack.factory.open_session(session_context(1202)).expect("session opens");
    rows(&mut session, "set @@tidb_analyze_version = 2");
    rows(&mut session, "use test");
    rows(&mut session, "create table t1(a int, b varchar(10), index idx_b (b))");
    rows(&mut session, "create table t2(a int, b varchar(10), index idx_b (b))");
    rows(&mut session, "analyze table test.t1, test.t2");
    rows(&mut session, "lock stats t1, t2");
    assert_eq!(locked_count(&mut session), "2");
    rows(&mut session, "insert into t1(a, b) values(1,'a')");
    rows(&mut session, "insert into t1(a, b) values(2,'b')");
    rows(&mut session, "insert into t2(a, b) values(1,'a')");
    rows(&mut session, "insert into t2(a, b) values(2,'b')");
    let before = (stats_meta(&mut session, "t1"), stats_meta(&mut session, "t2"));
    rows(&mut session, "analyze table test.t1, test.t2");
    assert!(warnings(&mut session).contains(&"Warning 1105 skip analyze locked tables: test.t1, test.t2".to_owned()));
    assert_eq!((stats_meta(&mut session, "t1"), stats_meta(&mut session, "t2")), before);

    rows(&mut session, "unlock stats test.t1, test.t2");
    assert_eq!(locked_count(&mut session), "0");
    rows(&mut session, "analyze table test.t1, test.t2");
    assert_eq!(stats_meta(&mut session, "t1")[0].2, "2");
    assert_eq!(stats_meta(&mut session, "t2")[0].2, "2");
}

const PARTITIONED_T: &str = "create table t(a int, b varchar(10), index idx_b (b)) partition by range(a) \
     (partition p0 values less than (10), partition p1 values less than (20))";

/// Go `TestLockAndUnlockPartitionStats`.
#[test]
fn test_lock_and_unlock_partition_stats() {
    let (_stack, _users, mut session) = session_with(1203, PARTITIONED_T);
    rows(&mut session, "lock stats t partition p0");
    assert_eq!(locked_count(&mut session), "1");
    assert_eq!(rows(&mut session, "show stats_locked").len(), 1);
    rows(&mut session, "insert into t(a, b) values(1,'a')");
    rows(&mut session, "insert into t(a, b) values(2,'b')");
    rows(&mut session, "analyze table test.t");
    assert!(warnings(&mut session)
        .contains(&"Warning 1105 skip analyze locked table: test.t partition (p0)".to_owned()));
    let p0 = |meta: Vec<(String, String, String)>| meta.into_iter().find(|row| row.0 == "p0").expect("p0").2;
    assert_eq!(p0(stats_meta(&mut session, "t")), "0");

    rows(&mut session, "unlock stats t partition p0");
    assert_eq!(locked_count(&mut session), "0");
    rows(&mut session, "analyze table test.t partition p0");
    let global = stats_meta(&mut session, "t").into_iter().find(|row| row.0 == "global").expect("global").2;
    assert_eq!(global, "2");
    assert!(rows(&mut session, "show stats_locked").is_empty());
}

/// Go `TestLockAndUnlockPartitionsStats`.
#[test]
fn test_lock_and_unlock_partitions_stats() {
    let (_stack, _users, mut session) = session_with(1204, PARTITIONED_T);
    rows(&mut session, "lock stats t partition p0, p1");
    assert_eq!(locked_count(&mut session), "2");
    rows(&mut session, "insert into t(a, b) values(1,'a')");
    rows(&mut session, "insert into t(a, b) values(2,'b')");
    rows(&mut session, "insert into t(a, b) values(11,'a')");
    rows(&mut session, "insert into t(a, b) values(12,'b')");
    rows(&mut session, "analyze table test.t partition p0, p1");
    assert!(warnings(&mut session).contains(
        &"Warning 1105 skip analyze locked tables: test.t partition (p0), test.t partition (p1)".to_owned()
    ));
    let count = |session: &mut Session, partition: &str| {
        stats_meta(session, "t").into_iter().find(|row| row.0 == partition).expect("partition").2
    };
    assert_eq!(count(&mut session, "p0"), "0");
    assert_eq!(count(&mut session, "p1"), "0");
    assert_eq!(rows(&mut session, "show stats_locked").len(), 2);
    rows(&mut session, "unlock stats t partition p0, p1");
    assert_eq!(locked_count(&mut session), "0");
    rows(&mut session, "analyze table test.t partition p0, p1");
    assert_eq!(count(&mut session, "p0"), "2");
    assert_eq!(count(&mut session, "p1"), "2");
    assert_eq!(count(&mut session, "global"), "4");
    assert!(rows(&mut session, "show stats_locked").is_empty());
}

#![cfg(test)]

use std::sync::Arc;

use crate::tests_support::*;
use crate::*;

/// Go `hash/fnv` `New64a`.
fn fnv64a(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash: u64, byte| (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3))
}

/// Go `IndexLookUpPushDownRunVerifier`'s `injectHandleFilter`.
fn inject_handle_filter(hit_rate: u64) {
    *tidb_unistore_cophandler::INJECT_INDEX_LOOKUP_HANDLE_FILTER.write().unwrap() =
        Some(Arc::new(move |handle| hit_rate >= 10 || fnv64a(&handle.encoded()) % 10 < hit_rate));
}

/// `operator:actRows:task` of each EXPLAIN ANALYZE row.
fn act_rows(session: &mut Session, sql: &str) -> String {
    row_text(session.run(&format!("explain analyze {sql}")))
        .iter()
        .map(|row| {
            let id = row[0].trim_start_matches([' ', '│', '├', '└', '─']);
            let id = id.split(['_', '(']).next().unwrap();
            let task = if row[3] == "root" { "R" } else { "C" };
            format!("{id}:{}:{task}", row[2])
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// A fixture line's `*` stands for any count.
fn assert_matches_go(want: &str, got: &str) {
    let matches = want.split(' ').count() == got.split(' ').count()
        && want.split(' ').zip(got.split(' ')).all(|(want, got)| {
            want == got || want.split_once(":*:").is_some_and(|(id, task)| {
                got.strip_prefix(id).and_then(|rest| rest.strip_suffix(task)).is_some()
            })
        });
    assert!(matches, "\n go: {want}\nrust: {got}");
}

/// Go `TestIndexLookUpPushDownExec` and `TestIndexLookUpPushDownPartitionExec`
/// (`executor_failpoint_test.go`): the store looks up the handles the
/// injected filter admits and leaves the others to TiDB. The rows, their
/// order and every operator's actRows are Go's for the same data.
#[test]
fn a_pushed_index_lookup_splits_its_rows_with_the_store_like_go() {
    let fixture = include_str!("../testdata/index_lookup_pushdown_go.txt");
    let lines = |prefix: &'static str| {
        fixture
            .lines()
            .filter(move |line| line.starts_with(prefix))
            .map(|line| line.split(" | ").collect::<Vec<_>>())
    };
    let mut session = Session::new();
    session.run("create table t(id bigint primary key, a bigint, b bigint, index a(a))").unwrap();
    for i in (0..2000).step_by(100) {
        let values = (i..i + 100)
            .map(|j| format!("({j}, {}, {})", j * 37 % 100, j * 7919 % 1000))
            .collect::<Vec<_>>()
            .join(",");
        session.run(&format!("insert into t values {values}")).unwrap();
    }
    let mut cases = 0;
    for line in lines("hit=") {
        cases += 1;
        let hit_rate = line[0].strip_prefix("hit=").unwrap().parse().unwrap();
        inject_handle_filter(hit_rate);
        let mut case = line[1].rsplitn(3, '/');
        let limit: i64 = case.next().unwrap().parse().unwrap();
        let skip: i64 = case.next().unwrap().parse().unwrap();
        let cond = case.next().unwrap();
        let mut sql = format!("select /*+ index_lookup_pushdown(t, a)*/ * from t where {cond}");
        if skip > 0 {
            sql += &format!(" limit {skip}, {limit}");
        } else if limit >= 0 {
            sql += &format!(" limit {limit}");
        }
        let rows = row_text(session.run(&sql));
        let ids = rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>().join(",");
        assert_eq!(
            format!("n={} order={:x}", rows.len(), fnv64a(ids.as_bytes())),
            line[2],
            "{}",
            line[..2].join(" | ")
        );
        assert_matches_go(line[3], &act_rows(&mut session, &sql));
    }
    assert_eq!(cases, 48);

    session.run("create table tp1 (a varchar(32), b int, c int, d int, primary key(b) CLUSTERED, index c(c)) PARTITION BY RANGE (b) (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (200), PARTITION p2 VALUES LESS THAN (300), PARTITION p3 VALUES LESS THAN MAXVALUE)").unwrap();
    session.run("create table tp2 (a varchar(32), b int, c int, d int, primary key(a, b) CLUSTERED, index c(c)) PARTITION BY RANGE COLUMNS (a) (PARTITION p0 VALUES LESS THAN ('c'), PARTITION p1 VALUES LESS THAN ('e'), PARTITION p2 VALUES LESS THAN ('g'), PARTITION p3 VALUES LESS THAN MAXVALUE)").unwrap();
    session.run("create table tp3 (a varchar(32), b int, c int, d int, primary key(a, b) NONCLUSTERED, index c(c)) PARTITION BY RANGE COLUMNS (a) (PARTITION p0 VALUES LESS THAN ('c'), PARTITION p1 VALUES LESS THAN ('e'), PARTITION p2 VALUES LESS THAN ('g'), PARTITION p3 VALUES LESS THAN MAXVALUE)").unwrap();
    for table in ["tp1", "tp2", "tp3"] {
        session.run(&format!("insert into {table} values ('a', 10, 1, 100), ('b', 20, 2, 200), ('c', 110, 3, 300), ('d', 120, 4, 400), ('e', 210, 5, 500), ('f', 220, 6, 600), ('g', 330, 5, 700), ('h', 340, 5, 800), ('i', 450, 5, 900), ('j', 550, 6, 1000)")).unwrap();
    }
    let mut cases = 0;
    for line in lines("tp") {
        cases += 1;
        let (table, hit_rate) = line[0].split_once(" hit=").unwrap();
        inject_handle_filter(hit_rate.parse().unwrap());
        let sql = format!("select /*+ index_lookup_pushdown({table}, c)*/ * from {table} where 1");
        let mut ids = row_text(session.run(&sql)).iter().map(|row| row[0].clone()).collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids.join(","), line[1], "{}", line[0]);
        assert_matches_go(line[2], &act_rows(&mut session, &sql));
    }
    assert_eq!(cases, 9);
    *tidb_unistore_cophandler::INJECT_INDEX_LOOKUP_HANDLE_FILTER.write().unwrap() = None;
}

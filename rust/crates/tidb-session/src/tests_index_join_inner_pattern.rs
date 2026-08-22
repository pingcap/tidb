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

//! Which operators an index join may re-seed a leaf THROUGH.

use crate::tests_support::row_text;
use crate::Session;

fn plan(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n")
}

fn fixture() -> Session {
    let mut session = Session::new();
    for table in ["t1", "t2", "t3"] {
        session
            .run(&format!(
                "CREATE TABLE {table} (a int, b int, c varchar(32), PRIMARY KEY (a), KEY (b))"
            ))
            .unwrap();
    }
    session.run("INSERT INTO t1 VALUES (1,10,'a1'),(2,20,'a2')").unwrap();
    session.run("INSERT INTO t2 VALUES (1,100,'b1'),(2,200,'b2')").unwrap();
    session.run("INSERT INTO t3 VALUES (1,1000,'c1'),(2,2000,'c2')").unwrap();
    session
}

/// Go `admitIndexJoinInnerChildPattern` names the operators that may sit
/// between an index join and the `DataSource` it re-seeds, and refuses every
/// other one -- "index join inner side couldn't allow join, sort, limit,
/// because they are Optimization Fence".
///
/// `WITH ROLLUP` builds an `Expand`, which that switch does not name at all.
/// Walking to any table that merely CARRIES the join key by name reached `t2`
/// straight through the rollup, and the probe then re-seeded a leaf whose
/// rows the `Expand` above it had already re-shaped.
#[test]
fn a_rollup_is_a_fence_an_index_join_probe_may_not_cross() {
    let mut session = fixture();
    let rollup = plan(
        &mut session,
        "EXPLAIN SELECT t1.a, dt.key_a, dt.sum_b FROM t1 JOIN (\
         SELECT t2.a AS key_a, sum(t3.b) AS sum_b FROM t2 JOIN t3 ON t2.a = t3.a \
         GROUP BY t2.a WITH ROLLUP) dt ON t1.a = dt.key_a",
    );
    assert!(
        !rollup.contains("decided by"),
        "no leaf under the rollup may be re-seeded by the probe:\n{rollup}"
    );
    // TiDB reads both inner tables whole, under an ordinary hash join.
    assert!(
        rollup.contains("table:t2") && rollup.contains("table:t3"),
        "both inner tables are still read:\n{rollup}"
    );

    // The SAME query without `WITH ROLLUP` keeps every operator in Go's
    // admitted set, so nothing here is a blanket refusal of grouped inners.
    let grouped = plan(
        &mut session,
        "EXPLAIN SELECT t1.a, dt.key_a, dt.sum_b FROM t1 JOIN (\
         SELECT t2.a AS key_a, sum(t3.b) AS sum_b FROM t2 JOIN t3 ON t2.a = t3.a \
         GROUP BY t2.a) dt ON t1.a = dt.key_a",
    );
    assert!(
        grouped.contains("table:t2") && grouped.contains("table:t3"),
        "the plain grouped form still plans:\n{grouped}"
    );

    // Both forms answer the same rows either way, which is what the fence
    // exists to keep true.
    assert_eq!(
        row_text(session.run(
            "SELECT t1.a, dt.key_a, dt.sum_b FROM t1 JOIN (\
             SELECT t2.a AS key_a, sum(t3.b) AS sum_b FROM t2 JOIN t3 ON t2.a = t3.a \
             GROUP BY t2.a WITH ROLLUP) dt ON t1.a = dt.key_a ORDER BY t1.a"
        )),
        vec![vec!["1", "1", "1000"], vec!["2", "2", "2000"]]
    );
}

/// Go `checkIndexJoinInnerTaskWithAgg`: an inner join key that comes from the
/// re-seeded `DataSource` must also be a GROUP BY key, "otherwise the
/// aggregation group might be split into multiple groups by the join keys,
/// which generate incorrect result".
///
/// `ONLY_FULL_GROUP_BY` normally makes this unreachable -- with it on, an
/// aggregation's outputs are its group keys and its aggregates, so a key that
/// comes from the leaf IS a group key. Off, a derived table may output a bare
/// column the grouping never named, and re-seeding the leaf by it would hand
/// each group only the rows one probe key selected.
///
/// This is a PIN, not a demonstration: it holds before the admission walk
/// landed as well, because something else already declined this shape. It is
/// here because the rule it names is a correctness rule and nothing else
/// states it.
#[test]
fn a_probe_key_outside_the_group_keys_is_refused() {
    let mut session = fixture();
    session.run("SET @@sql_mode = ''").unwrap();
    let plan_text = plan(
        &mut session,
        "EXPLAIN SELECT t1.a FROM t1 JOIN (\
         SELECT t2.a AS key_a, t3.b AS bb FROM t2 JOIN t3 ON t2.a = t3.a \
         GROUP BY t2.a) dt ON t1.b = dt.bb",
    );
    assert!(
        !plan_text.contains("decided by"),
        "`bb` is not a group key, so no leaf is re-seeded by it:\n{plan_text}"
    );
}

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

#![cfg(test)]

use crate::tests_support::cell_text;
use crate::{Session, StmtResult};

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => rows
            .iter()
            .map(|row| row.iter().map(cell_text).collect::<Vec<_>>().join("|"))
            .collect(),
        other => panic!("expected rows from `{sql}`, got {other:?}"),
    }
}

#[test]
fn distinct_constant_arguments_reach_the_skew_rewrite() {
    let mut session = Session::new();
    for sql in [
        "create table t (a int, b int)",
        "insert into t values (1,10),(2,10),(3,20)",
        "set tidb_opt_skew_distinct_agg = on",
    ] {
        session.run(sql).unwrap();
    }

    let aggregate_count = match session
        .run("explain select b, count(distinct 1) from t group by b")
        .unwrap()
    {
        StmtResult::Rows(rows) => rows
            .iter()
            .filter(|row| cell_text(&row[0]).contains("HashAgg"))
            .count(),
        other => panic!("expected EXPLAIN rows, got {other:?}"),
    };
    assert_eq!(aggregate_count, 2);

    assert_eq!(
        rows(
            &mut session,
            "select b, count(distinct 1) from t group by b order by b"
        ),
        ["10|1", "20|1"]
    );
    assert_eq!(
        rows(
            &mut session,
            "select b, sum(distinct 2) from t group by b order by b"
        ),
        ["10|2", "20|2"]
    );
}

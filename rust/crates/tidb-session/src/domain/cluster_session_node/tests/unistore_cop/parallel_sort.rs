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

//! Go `pkg/executor/sortexec/parallel_sort_test.go`'s SQL-level test.

use super::sql_kit::{exec_all, q, session};
use super::*;

#[test]
fn test_issue_55344() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 1);
    exec_all(
        &mut tk,
        &[
            "set @@tidb_max_chunk_size=32",
            "set @@tidb_init_chunk_size=1",
            "drop table if exists t0;",
            "CREATE TABLE t0(c0 BOOL);",
            "INSERT INTO mysql.opt_rule_blacklist VALUES('predicate_push_down'),('column_prune');",
            "ADMIN reload opt_rule_blacklist;",
        ],
    );

    // Should not be panic.
    q(&mut tk, "SELECT t0.c0 FROM t0 WHERE 0 ORDER BY -646041453 ASC;");

    // Test correctness.
    exec_all(&mut tk, &["drop table if exists t1;", "CREATE TABLE t1(c int);"]);
    let mut rand = tidb_go_rand::Rand::new(55344);
    let mut inserted_values: Vec<i64> = (0..1000).map(|_| rand.intn(10000)).collect();
    let values: Vec<String> = inserted_values.iter().map(|value| format!("({value})")).collect();
    exec_all(&mut tk, &[&format!("INSERT INTO t1 values {};", values.join(", "))]);
    inserted_values.sort_unstable();
    let expected: Vec<Vec<String>> = inserted_values.iter().map(|value| vec![value.to_string()]).collect();

    for sql in [
        "select c from t1 order by c, -646041453;",
        "select c from t1 order by -646041453, c;",
        "select c from t1 order by c, -646041453, c+1;",
        "select c from t1 order by c+1, -646041453, c;",
    ] {
        assert_eq!(q(&mut tk, sql), expected, "{sql}");
    }

    exec_all(
        &mut tk,
        &[
            "delete from mysql.opt_rule_blacklist where name='column_prune' or name='predicate_push_down';",
            "ADMIN reload opt_rule_blacklist;",
        ],
    );
}

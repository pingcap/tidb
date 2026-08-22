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

//! Which collation a `RANGE COLUMNS` predicate is pruned under.

use crate::tests_support::row_text;
use crate::Session;

/// The partitions an `EXPLAIN` names, in plan order.
fn partitions(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .filter_map(|row| {
            row.iter()
                .find_map(|cell| cell.split(", ").find_map(|part| part.strip_prefix("partition:")))
                .map(str::to_owned)
        })
        .collect()
}

/// A plain-string `IN` list is pruned under the COLUMN's collation, not the
/// connection's.
///
/// Go derives the collation of `ast.In` once over ALL its arguments
/// (`expression/collation.go`), and a column is IMPLICIT where a bare literal
/// is COERCIBLE -- so the column decides, and only an EXPLICIT `COLLATE`
/// could outrank it. The range builder's fast path for a large literal list
/// read the leaves as if they decided, which kept every partition of any
/// `RANGE COLUMNS` table whose column is not the connection default.
///
/// `set names utf8mb4 collate utf8mb4_bin` is what makes the two disagree,
/// and it is what the source corpus does before this exact query:
/// `tests/integrationtest/t/planner/core/partition_pruner.test`.
#[test]
fn a_string_in_list_prunes_under_the_columns_own_collation() {
    // Both tables are the corpus's own, bounds and definition order included:
    // each collation needs its own strictly-increasing order.
    for (collation, bounds, query, expected) in [
        (
            "utf8mb4_general_ci",
            "PARTITION pNull VALUES LESS THAN (''), \
             PARTITION paaa VALUES LESS THAN ('aaa'), \
             PARTITION pAAAA VALUES LESS THAN ('AAAA'), \
             PARTITION pCCC VALUES LESS THAN ('CCC'), \
             PARTITION pMax VALUES LESS THAN (MAXVALUE)",
            "a IN ('AA', 'aaa')",
            vec!["paaa", "pAAAA"],
        ),
        // The connection default: this arm was already right, and says the
        // pruner itself never was the problem.
        (
            "utf8mb4_bin",
            "PARTITION pNull VALUES LESS THAN (''), \
             PARTITION pAAAA VALUES LESS THAN ('AAAA'), \
             PARTITION pCCC VALUES LESS THAN ('CCC'), \
             PARTITION paaa VALUES LESS THAN ('aaa'), \
             PARTITION pMax VALUES LESS THAN (MAXVALUE)",
            "a IN ('AAA', 'aa')",
            vec!["pAAAA", "paaa"],
        ),
    ] {
        let mut session = Session::new();
        session
            .run("SET @@tidb_partition_prune_mode = 'static'")
            .unwrap();
        // What makes the connection's collation and the column's disagree,
        // and what the corpus sets before these queries.
        session.run("SET NAMES utf8mb4 COLLATE utf8mb4_bin").unwrap();
        session
            .run(&format!(
                "CREATE TABLE t (a varchar(255) CHARSET utf8mb4 COLLATE {collation}) \
                 PARTITION BY RANGE COLUMNS (a) ({bounds})"
            ))
            .unwrap();

        assert_eq!(
            partitions(&mut session, &format!("EXPLAIN SELECT * FROM t WHERE {query}")),
            expected,
            "{collation}: `{query}` prunes under the COLUMN's collation"
        );
    }
}

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
                .find_map(|cell| {
                    cell.split(", ")
                        .find_map(|part| part.strip_prefix("partition:"))
                })
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
        session
            .run("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
            .unwrap();
        session
            .run(&format!(
                "CREATE TABLE t (a varchar(255) CHARSET utf8mb4 COLLATE {collation}) \
                 PARTITION BY RANGE COLUMNS (a) ({bounds})"
            ))
            .unwrap();

        assert_eq!(
            partitions(
                &mut session,
                &format!("EXPLAIN SELECT * FROM t WHERE {query}")
            ),
            expected,
            "{collation}: `{query}` prunes under the COLUMN's collation"
        );
    }
}

/// The range text of a `LIKE` -- which of Go's five `newBuildFromPatternLike`
/// returns converts to the sort key, and which leaves it to the shared tail.
///
/// An index stores the collation KEY, so a range over it carries the key and
/// `EXPLAIN` prints it. Go converts in three of the five cases and lets the
/// wildcard case do its own, because the wildcard's two bounds take DIFFERENT
/// trimming. Reporting the whole function as self-converting skipped the
/// conversion for the exact-match case, which then printed its written text:
/// `["aa","aa"]` where TiDB prints `["\x00A\x00A","\x00A\x00A"]`.
///
/// The table is the source corpus's own:
/// `tests/integrationtest/t/planner/core/range_scan_for_like.test`.
#[test]
fn an_exact_like_ranges_over_the_sort_key_like_every_other_arm() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a varchar(20) COLLATE utf8mb4_general_ci, b bigint, \
             INDEX ia(a(3), b))",
        )
        .unwrap();

    let range = |session: &mut Session, sql: &str| {
        row_text(session.run(sql))
            .into_iter()
            .find_map(|row| {
                row.iter()
                    .find_map(|cell| cell.split(", ").find_map(|p| p.strip_prefix("range:")))
                    .map(str::to_owned)
            })
            .unwrap_or_default()
    };

    // Case 3, no wildcard: an equality on the pattern text. `a` and `A` share
    // a weight under `utf8mb4_general_ci`, which is what makes the printed
    // key `\x00A` rather than the letter.
    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT a FROM t USE INDEX (ia) WHERE a LIKE 'aa'"
        ),
        "[\"\\x00A\\x00A\",\"\\x00A\\x00A\"]"
    );
    // Case 1, the empty pattern: converted, and the key of `''` is empty.
    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT a FROM t USE INDEX (ia) WHERE a LIKE ''"
        ),
        "[\"\",\"\"]"
    );
    // A single space is not the empty pattern, but its KEY is empty --- a PAD
    // SPACE collation trims it. Reached only by converting.
    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT a FROM t USE INDEX (ia) WHERE a LIKE ' '"
        ),
        "[\"\",\"\"]"
    );
    // Case 4-2, the wildcard, whose bounds this arm converts itself: the
    // prefix is cut to the index's three characters and the upper bound is
    // the incremented key of the cut value, not of the written one.
    assert_eq!(
        range(
            &mut session,
            "EXPLAIN SELECT a FROM t USE INDEX (ia) WHERE a LIKE 'abcdef%'"
        ),
        "[\"\\x00A\\x00B\\x00C\",\"\\x00A\\x00B\\x00D\")"
    );
}

/// `!=` is not an access condition on a PREFIX index column.
///
/// Go's `conditionChecker` singles it out: `if scalar.FuncName.L == ast.NE {
/// return isFullLength, !isFullLength }`. Cutting a point to the prefix
/// widens every other comparison into a superset that the reserved filter
/// narrows back; `!=` is the one shape it makes SMALLER, because the cut
/// excludes the prefix rather than the value. `a != 'aabbb'` over `KEY(a(3))`
/// would exclude the key `'aab'` and lose `'aab'` and `'aabB'`, which do
/// differ from `'aabbb'`. The checker decides per column, before any value is
/// looked at, so it declines `!=` here too and reads the whole index.
///
/// Table and query are the source corpus's own:
/// `tests/integrationtest/t/planner/core/range_scan_for_like.test`.
#[test]
fn a_not_equal_is_not_an_access_condition_over_a_prefix_index() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a varchar(20) COLLATE utf8mb4_general_ci, b bigint, \
             INDEX ia(a(3), b))",
        )
        .unwrap();
    session
        .run("INSERT INTO t VALUES ('aa', 1), ('aab', 2), ('aabB', 3), ('A', 4)")
        .unwrap();

    let plan: Vec<String> =
        row_text(session.run("EXPLAIN SELECT * FROM t USE INDEX (ia) WHERE a != 'aa'"))
            .into_iter()
            .map(|row| row.join(" | "))
            .collect();
    let plan = plan.join("\n");
    assert!(
        plan.contains("IndexFullScan"),
        "`!=` over a prefix column must read the whole index:\n{plan}"
    );
    assert!(
        !plan.contains("IndexRangeScan"),
        "no range may be built from `!=` over a prefix column:\n{plan}"
    );
    assert!(
        plan.contains("ne(test.t.a, \"aa\")"),
        "the condition stays a filter:\n{plan}"
    );

    // And the rows are the ones Go returns.
    let mut rows = row_text(session.run("SELECT a FROM t USE INDEX (ia) WHERE a != 'aa'"));
    rows.sort();
    assert_eq!(rows, vec![vec!["A"], vec!["aab"], vec!["aabB"]]);
}

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

//! Aggregate behavior the executor's in-memory tier used to check, over the
//! cop-backed TestKit stack (X11).

use super::*;

fn session(stack: &UnistoreClusterStack, connection: u64) -> Session {
    let mut session = stack.factory.open_session(session_context(connection)).expect("session opens");
    rows(&mut session, "use test");
    session
}

/// SUM and AVG over a FLOAT column compute in the real domain.
#[test]
fn float_sum_and_avg_use_the_real_domain() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 470);
    rows(&mut tk, "CREATE TABLE f (v FLOAT)");
    rows(&mut tk, "INSERT INTO f VALUES (1.25), (2.5), (NULL)");
    assert_eq!(displayed(rows(&mut tk, "SELECT SUM(v), AVG(v) FROM f")), [["3.75", "1.875"]]);
}

/// MAX and MIN over a clustered key column that the secondary index does not
/// rank answer the true extremes (an unproven bounded reverse index read
/// answered 7).
#[test]
fn max_min_over_a_clustered_column_the_index_does_not_rank_answers_the_true_extreme() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 471);
    rows(
        &mut tk,
        "CREATE TABLE ox (a VARCHAR(3) NOT NULL, b VARCHAR(8) NOT NULL, c INT NOT NULL, d VARCHAR(2), PRIMARY KEY(a,b,c) CLUSTERED, KEY k4(a,b,d))",
    );
    rows(&mut tk, "INSERT INTO ox VALUES ('a01','20220101',3,'x'),('a02','20220102',42,'y'),('a03','20220103',7,'z')");
    assert_eq!(displayed(rows(&mut tk, "SELECT MAX(c), MIN(c) FROM ox")), [["42", "3"]]);
}

/// A MAX over a derived SUM compares the materialized coprocessor TopN keys.
#[test]
fn max_over_a_derived_sum_materializes_coprocessor_topn_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 472);
    rows(&mut tk, "CREATE TABLE topn_cast (id BIGINT PRIMARY KEY, v BIGINT)");
    rows(&mut tk, "INSERT INTO topn_cast VALUES (1,7),(2,42),(3,9),(4,NULL)");
    assert_eq!(
        displayed(rows(
            &mut tk,
            "SELECT id FROM (SELECT id,SUM(v) AS total FROM topn_cast GROUP BY id) d WHERE total=(SELECT MAX(total) FROM (SELECT id,SUM(v) AS total FROM topn_cast GROUP BY id) x)",
        )),
        [["2"]]
    );
}

/// A grouped SUM over an indexed column answers one row per group.
#[test]
fn grouped_aggregation_enumerates_families_over_one_planned_child() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 473);
    rows(&mut tk, "CREATE TABLE shared_child (id INT PRIMARY KEY, k INT, INDEX idx_k(k))");
    rows(&mut tk, "INSERT INTO shared_child VALUES (1, 10), (2, 10), (3, 20)");
    let mut answer = displayed(rows(&mut tk, "SELECT k, SUM(id) FROM shared_child GROUP BY k"));
    answer.sort();
    assert_eq!(answer, [["10", "3"], ["20", "3"]]);
}

/// BIT_AND / BIT_OR / BIT_XOR fold in the unsigned domain: an all-NULL input
/// is `18446744073709551615 0 0`, and `-1` inputs read as all bits set.
#[test]
fn bit_aggregates_are_unsigned() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 474);
    let query = "SELECT BIT_AND(v), BIT_OR(v), BIT_XOR(v) FROM b";
    for (values, expected) in [
        ("(NULL)", ["18446744073709551615", "0", "0"]),
        ("(-1),(-1)", ["18446744073709551615", "18446744073709551615", "0"]),
        ("(3),(5),(NULL)", ["1", "7", "6"]),
    ] {
        rows(&mut tk, "DROP TABLE IF EXISTS b");
        rows(&mut tk, "CREATE TABLE b (v BIGINT)");
        rows(&mut tk, &format!("INSERT INTO b VALUES {values}"));
        assert_eq!(displayed(rows(&mut tk, query)), [expected], "{values}");
    }
}

/// Aggregates read the collation of what they aggregate: MIN/MAX, DISTINCT
/// over a column and over computed strings, and GROUP_CONCAT's own ORDER BY
/// and DISTINCT, on a `utf8mb4_general_ci` column.
#[test]
fn aggregates_read_the_arguments_collation() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 475);
    let with = |tk: &mut Session, values: &str| {
        rows(tk, "DROP TABLE IF EXISTS g");
        rows(tk, "CREATE TABLE g (s VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci)");
        rows(tk, &format!("INSERT INTO g VALUES {values}"));
    };
    with(&mut tk, "('a'), ('B'), ('A')");
    assert_eq!(displayed(rows(&mut tk, "SELECT MAX(s), MIN(s) FROM g")), [["B", "a"]]);
    assert_eq!(
        displayed(rows(&mut tk, "SELECT COUNT(DISTINCT s), COUNT(DISTINCT CONCAT(s, '')), COUNT(DISTINCT UPPER(s)) FROM g")),
        [["2", "2", "2"]]
    );
    with(&mut tk, "('B'), ('a')");
    assert_eq!(displayed(rows(&mut tk, "SELECT GROUP_CONCAT(s ORDER BY s) FROM g")), [["a,B"]]);
    assert_eq!(displayed(rows(&mut tk, "SELECT GROUP_CONCAT(s ORDER BY s DESC) FROM g")), [["B,a"]]);
    with(&mut tk, "('b'), ('A'), ('a'), ('B')");
    // Go keeps each collation key's first value and joins its map in
    // iteration order: compare as a set.
    let joined = displayed(rows(&mut tk, "SELECT GROUP_CONCAT(DISTINCT s) FROM g")).remove(0).remove(0);
    let mut values: Vec<&str> = joined.split(',').collect();
    values.sort_unstable();
    assert_eq!(values, ["A", "b"]);
}

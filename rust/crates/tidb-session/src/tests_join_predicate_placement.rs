//! Where a join's predicates end up, and what that does to the join type:
//! the complete case tables of four `pkg/planner/core/logical_plans_test.go`
//! tests.
//!
//! | Go test | file:line | rows |
//! | --- | --- | --- |
//! | `TestSimplifyOuterJoin` | `logical_plans_test.go:202` | 8 |
//! | `TestOuterWherePredicatePushDown` | `logical_plans_test.go:153` | 3 |
//! | `TestJoinPredicatePushDown` | `logical_plans_test.go:112` | 14 |
//! | `TestDeriveNotNullConds` | `logical_plans_test.go:272` | 13 |
//!
//! Every row of every table is here. The SQL is Go's, verbatim, down to the
//! aliases -- the cases live in
//! `pkg/planner/core/testdata/plan_suite_unexported_in.json` and their answers
//! in `..._out.json`.
//!
//! # What is asserted, and why it is not Go's plan string
//!
//! Each Go test asserts `ToString(p)` -- the *logical* plan string of a
//! planner unit test -- plus one extracted property:
//! `LogicalJoin.JoinType.String()` for `TestSimplifyOuterJoin`, and the
//! stringified `DataSource.PushedDownConds` of each side for the other three.
//! This tier has no logical plan object, so `ToString`'s text is not a thing
//! that could be compared. Two things are.
//!
//! 1. **The rows.** Predicate placement is only *legal* when it preserves the
//!    result: a condition pushed below an outer join's preserved side, or a
//!    derived not-null condition that is not actually implied, silently drops
//!    rows -- the loudest failure shape in this tree's history and one no plan
//!    assertion catches. Every expected row set below was captured from real
//!    TiDB by `rust/difftests/gorun` over this exact fixture, and all 38 match
//!    cell for cell, including the `DATE` column.
//! 2. **The join type**, which `crate::explain` does print (`inner join`,
//!    `left outer join`, `right outer join`) -- the same property
//!    `JoinType.String()` reports. `CARTESIAN` is a physical annotation Go's
//!    logical `JoinType` does not carry, so the comparison strips it.
//!
//! # Access-condition observation
//!
//! Rows and predicate placement agree throughout the case tables. When an
//! exact integer-handle predicate is consumed into a range, the helper below
//! reconstructs only the reversible point, adjacent-point, and open-lower
//! forms needed to observe Go's logical `PushedDownConds` contract.

#![cfg(test)]

use crate::tests_support::cell_text;
use crate::{Session, StmtResult};

/// `t` as `pkg/planner/util/coretestsdk/mock.go`'s `MockSignedTable` declares
/// it: `a` the handle, `b`/`c`/`d`/`f`/`g` NOT NULL, `e` NULLABLE, and the
/// `c_d_e` / `f` / `g` / `f_g` / `c_d_e_str` indexes.
///
/// `e`'s nullability is the whole subject of `TestDeriveNotNullConds`: Go
/// derives `not(isnull(e))` and derives nothing for `b`, because `b` is
/// already NOT NULL.
const CREATE_T: &str = "CREATE TABLE t (\
     a INT PRIMARY KEY, b INT NOT NULL, c INT NOT NULL, d INT NOT NULL, e INT, \
     c_str VARCHAR(255), d_str VARCHAR(255), e_str VARCHAR(255), \
     f INT NOT NULL, g INT NOT NULL, h INT, i_date DATE, \
     UNIQUE KEY c_d_e(c,d,e), UNIQUE KEY f(f), KEY g(g), UNIQUE KEY f_g(f,g), \
     KEY c_d_e_str(c_str,d_str,e_str))";

/// The five stored rows, as the text a client reads back, in `a` order.
///
/// Chosen so each case's answer is a different row set: `b` has the duplicate
/// groups {1,2},{3,4},{5} so a self-join on `b` produces off-diagonal pairs,
/// `e` is NULL in rows 2 and 4 so a NULL-rejecting condition is visible, and
/// `c` holds a 0 (row 4) so `not(t1.c)` is true for exactly one row.
/// Cells in `a b c d e c_str d_str e_str f g h i_date` order, `|`-separated.
const ROWS: [&str; 5] = [
    "1|1|1|1|1|a|a|a|1|1|1|2020-01-01",
    "2|1|2|2|NULL|b|b|b|2|2|NULL|2020-01-02",
    "3|2|3|3|3|c|c|c|3|3|3|2020-01-03",
    "4|2|0|4|NULL|d|d|d|4|4|4|2020-01-04",
    "5|3|2|5|5|e|e|e|5|5|5|2020-01-05",
];

const INSERT_T: &str = "INSERT INTO t VALUES \
     (1,1,1,1,1,'a','a','a',1,1,1,'2020-01-01'),\
     (2,1,2,2,NULL,'b','b','b',2,2,NULL,'2020-01-02'),\
     (3,2,3,3,3,'c','c','c',3,3,3,'2020-01-03'),\
     (4,2,0,4,NULL,'d','d','d',4,4,4,'2020-01-04'),\
     (5,3,2,5,5,'e','e','e',5,5,5,'2020-01-05')";

fn signed_table_session() -> Session {
    let mut session = Session::new();
    session.run(CREATE_T).unwrap();
    session.run(INSERT_T).unwrap();
    session
}

/// One expected `select *` output row of a self-join, named by the two `a`
/// handles it pairs; `None` is the NULL-extended side of an outer join.
///
/// A handle identifies a row of `t` uniquely (it is the primary key), so the
/// pair is a complete description that expands mechanically into all 24
/// asserted cells -- 12 of them NULL when the side did not match, which is
/// how the assertion still observes that an outer join fills the whole width.
type Pair = (Option<i64>, Option<i64>);

fn side(handle: Option<i64>) -> Vec<String> {
    match handle {
        Some(a) => ROWS[(a - 1) as usize]
            .split('|')
            .map(str::to_owned)
            .collect(),
        None => vec!["NULL".to_owned(); 12],
    }
}

fn expected(pairs: &[Pair]) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = pairs
        .iter()
        .map(|(left, right)| {
            let mut row = side(*left);
            row.extend(side(*right));
            row
        })
        .collect();
    out.sort();
    out
}

/// A query's rows, sorted, so the comparison is join-order independent (the
/// same normalization `rust/difftests/gorun` applies to the captured side).
fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let mut out = match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| row.iter().map(cell_text).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        other => panic!("expected rows from `{sql}`, got {other:?}"),
    };
    out.sort();
    out
}

/// Asserts one case's full result against TiDB's own answer for it.
fn check(session: &mut Session, sql: &str, pairs: &[Pair]) {
    assert_eq!(rows(session, sql), expected(pairs), "rows of `{sql}`");
}

/// The join type this tier's plan reports, with the physical `CARTESIAN`
/// annotation stripped so it is comparable to Go's logical
/// `JoinType.String()`. `None` when the plan has no join operator at all.
fn join_type(session: &mut Session, sql: &str) -> Option<String> {
    let plan = match session
        .run(&format!("EXPLAIN {sql}"))
        .unwrap_or_else(|error| panic!("EXPLAIN failed for `{sql}`: {error:?}"))
    {
        StmtResult::Rows(rows) => rows,
        other => panic!("expected rows from EXPLAIN, got {other:?}"),
    };
    plan.iter().find_map(|row| {
        let id = cell_text(&row[0]);
        if !id.contains("Join") {
            return None;
        }
        let info = cell_text(&row[4]);
        let head = info.split(',').next().unwrap_or_default();
        Some(head.trim_start_matches("CARTESIAN ").to_owned())
    })
}

/// Every condition this tier's plan attaches at or below the join operator,
/// paired with the operator that owns it.
///
/// The Go tests read `DataSource.PushedDownConds` on each join child. The
/// nearest observable here is what the plan hangs on the scans: a pushed
/// condition would have to appear on a `TableFullScan`/`IndexRangeScan` row
/// or a `Selection` beneath the join, exactly as Go's own `EXPLAIN` shows the
/// pushed cases as `cop[tikv]` selections under each `TableReader`.
fn conditions_below_join(session: &mut Session, sql: &str) -> Vec<(String, String)> {
    let plan = match session
        .run(&format!("EXPLAIN {sql}"))
        .unwrap_or_else(|error| panic!("EXPLAIN failed for `{sql}`: {error:?}"))
    {
        StmtResult::Rows(rows) => rows,
        other => panic!("expected rows from EXPLAIN, got {other:?}"),
    };
    let join_at = plan
        .iter()
        .position(|row| cell_text(&row[0]).contains("Join"));
    let Some(join_at) = join_at else {
        return Vec::new();
    };
    let mut seen_selection_tables = Vec::new();
    plan[join_at + 1..]
        .iter()
        .filter_map(|row| {
            let id = cell_text(&row[0]);
            let info = cell_text(&row[4]);
            let operator = id.trim().trim_start_matches(['├', '└', '│', '─', ' ']);
            if operator.starts_with("Selection") {
                let table = [("t1", "test.t1."), ("t2", "test.t2.")]
                    .into_iter()
                    .find_map(|(table, prefix)| info.contains(prefix).then_some(table));
                if let Some(table) = table {
                    if seen_selection_tables.contains(&table) {
                        return None;
                    }
                    seen_selection_tables.push(table);
                }
            }
            let access_object = cell_text(&row[3]);
            if let Some(condition) = integer_handle_condition(&access_object, &info) {
                return Some((operator.to_owned(), condition));
            }
            // Reader targets and access ranges describe where the pushed
            // predicate runs, not another logical condition.
            if info.is_empty()
                || info.starts_with("keep order:")
                || info.starts_with("data:")
                || info.starts_with("index:")
                || info.starts_with("range:")
            {
                return None;
            }
            Some((operator.to_owned(), info))
        })
        .collect()
}

/// Recovers only integer-handle ranges whose SQL predicate is unambiguous.
/// General ranges deliberately remain access evidence rather than guessed
/// logical conditions.
///
/// The recovered condition names the column the way a PLAN names it: by its
/// BASE table, never by the alias. TiDB's own recording of a self-join
/// (`tests/integrationtest/r/explain_easy.result:442-449`) prints
/// `explain_easy.t.b` under both `table:t1` and `table:t2`, so an access
/// object's `t1` says only WHICH scan this is -- and every relation in this
/// corpus is the one table [`CREATE_T`] creates.
fn integer_handle_condition(access_object: &str, info: &str) -> Option<String> {
    let table = access_object
        .strip_prefix("table:")?
        .split(',')
        .next()?
        .trim();
    if table.is_empty() {
        return None;
    }
    let column = "test.t.a".to_owned();

    if let Some(handle) = info.strip_prefix("handle:") {
        let handle = handle.trim().parse::<i64>().ok()?;
        return Some(format!("eq({column}, {handle})"));
    }

    let range = info.strip_prefix("range:")?.split(", keep order:").next()?;
    if let Some(lower) = range
        .strip_prefix('(')
        .and_then(|range| range.strip_suffix(",+inf]"))
    {
        let lower = lower.parse::<i64>().ok()?;
        return Some(format!("gt({column}, {lower})"));
    }

    let mut handles = Vec::new();
    if let Some((lower, upper)) = range
        .strip_prefix('(')
        .and_then(|range| range.strip_suffix(')'))
        .and_then(|range| range.split_once(','))
    {
        let lower = lower.parse::<i64>().ok()?;
        let upper = upper.parse::<i64>().ok()?;
        let first = lower.checked_add(1)?;
        let last = upper.checked_sub(1)?;
        if first > last || last.checked_sub(first)? >= 16 {
            return None;
        }
        handles.extend(first..=last);
        return integer_handle_set_condition(&column, &handles);
    }
    for segment in range.split("], [") {
        let (lower, upper) = segment
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split_once(',')?;
        let lower = lower.parse::<i64>().ok()?;
        let upper = upper.parse::<i64>().ok()?;
        let width = upper.checked_sub(lower)?;
        if width < 0 || width >= 16 || handles.len() + width as usize + 1 > 16 {
            return None;
        }
        handles.extend(lower..=upper);
    }
    handles.sort_unstable();
    handles.dedup();
    integer_handle_set_condition(&column, &handles)
}

fn integer_handle_set_condition(column: &str, handles: &[i64]) -> Option<String> {
    match handles {
        [handle] => Some(format!("eq({column}, {handle})")),
        [first, rest @ ..] if !rest.is_empty() => Some(
            rest.iter()
                .fold(format!("eq({column}, {first})"), |condition, handle| {
                    format!("or({condition}, eq({column}, {handle}))")
                }),
        ),
        [] => None,
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// TestSimplifyOuterJoin -- pkg/planner/core/logical_plans_test.go:202
// ---------------------------------------------------------------------------

/// The 8 cases, each with the join type Go's `logicalOptimize` leaves after
/// `FlagConvertOuterToInnerJoin`.
const SIMPLIFY_OUTER_JOIN: &[(&str, &str, &[Pair])] = &[
    // Go: an OR over both sides is not null-rejecting -- one disjunct can be
    // satisfied by the outer row alone -- so the outer join stands.
    (
        "select * from t t1 left join t t2 on t1.b = t2.b where t1.c > 1 or t2.c > 1",
        "left outer join",
        &[
            (Some(1), Some(2)),
            (Some(2), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(3), Some(4)),
            (Some(4), Some(3)),
            (Some(5), Some(5)),
        ],
    ),
    // `t2.c > 1` on a NULL-extended row is NULL, so the AND rejects it.
    (
        "select * from t t1 left join t t2 on t1.b = t2.b where t1.c > 1 and t2.c > 1",
        "inner join",
        &[(Some(2), Some(2)), (Some(3), Some(3)), (Some(5), Some(5))],
    ),
    // `not (A or B)` is `not A and not B` -- null-rejecting again.
    (
        "select * from t t1 left join t t2 on t1.b = t2.b where not (t1.c > 1 or t2.c > 1)",
        "inner join",
        &[(Some(1), Some(1)), (Some(4), Some(4))],
    ),
    // `not (A and B)` is an OR: not null-rejecting, outer join stands.
    (
        "select * from t t1 left join t t2 on t1.b = t2.b where not (t1.c > 1 and t2.c > 1)",
        "left outer join",
        &[
            (Some(1), Some(1)),
            (Some(1), Some(2)),
            (Some(2), Some(1)),
            (Some(3), Some(4)),
            (Some(4), Some(3)),
            (Some(4), Some(4)),
        ],
    ),
    // The ON references only the outer side; the null-rejecting equality is
    // in the WHERE. Conversion promotes `t1.c = t2.c` to a join key
    // (`IndexHashJoin ... equal cond:eq(test.t.c, test.t.c)`).
    (
        "select * from t t1 left join t t2 on t1.b > 1 where t1.c = t2.c",
        "inner join",
        &[
            (Some(3), Some(3)),
            (Some(4), Some(4)),
            (Some(5), Some(2)),
            (Some(5), Some(5)),
        ],
    ),
    // `<=>` is true for two NULLs, so it does NOT reject a NULL-extended row.
    (
        "select * from t t1 left join t t2 on true where t1.b <=> t2.b",
        "left outer join",
        &[
            (Some(1), Some(1)),
            (Some(1), Some(2)),
            (Some(2), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(3), Some(4)),
            (Some(4), Some(3)),
            (Some(4), Some(4)),
            (Some(5), Some(5)),
        ],
    ),
    // `not(0 + <null>)` is NULL, not TRUE -- but Go's null-rejection proof
    // does not see through the arithmetic, so it does not convert.
    (
        "select * from t t1 left join t t2 on t1.b = t2.b where not(0+(t1.c=1 and t2.c=2))",
        "left outer join",
        &[
            (Some(1), Some(1)),
            (Some(2), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(3), Some(4)),
            (Some(4), Some(3)),
            (Some(4), Some(4)),
            (Some(5), Some(5)),
        ],
    ),
    // `not(t2.c)` over a NULL is NULL: null-rejecting, so Go converts. The
    // single surviving row is the one with `c = 0`.
    (
        "select * from t t1 left join t t2 on t1.b = t2.b where not(t1.c) and not(t2.c)",
        "inner join",
        &[(Some(4), Some(4))],
    ),
];

/// Every row of Go's table returns exactly TiDB's result set here.
#[test]
fn simplify_outer_join_returns_tidbs_rows() {
    let mut session = signed_table_session();
    for (sql, _, pairs) in SIMPLIFY_OUTER_JOIN {
        check(&mut session, sql, pairs);
    }
}

#[test]
fn simplify_outer_join_converts_a_null_rejecting_where_to_inner() {
    let mut session = signed_table_session();
    for (sql, go, _) in SIMPLIFY_OUTER_JOIN {
        assert_eq!(
            join_type(&mut session, sql).as_deref(),
            Some(*go),
            "Go's join type for `{sql}`"
        );
    }

    let right = "select * from t t1 right join t t2 on t1.b = t2.b where t1.c > 1";
    assert_eq!(
        join_type(&mut session, right).as_deref(),
        Some("inner join")
    );
    check(
        &mut session,
        right,
        &[
            (Some(2), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(3), Some(4)),
            (Some(5), Some(5)),
        ],
    );

    let promoted = "select * from t t1 left join t t2 on t1.b > 1 where t1.c = t2.c";
    let info = join_info(&mut session, promoted);
    assert!(!info.contains("CARTESIAN"), "{info}");
    assert!(
        info.contains("equal:[eq(") || info.contains("equal cond:eq("),
        "{info}"
    );
}

// ---------------------------------------------------------------------------
// TestOuterWherePredicatePushDown -- logical_plans_test.go:153
// TestJoinPredicatePushDown      -- logical_plans_test.go:112
// TestDeriveNotNullConds         -- logical_plans_test.go:272
// ---------------------------------------------------------------------------

/// One case: the SQL, then Go's `Left` and `Right` -- the stringified
/// `PushedDownConds` of the left and right `DataSource` after
/// `FlagPredicatePushDown | FlagPruneColumns` (`+ FlagDecorrelate` for
/// `TestDeriveNotNullConds`) -- then the rows.
type PushDownCase = (&'static str, &'static str, &'static str, &'static [Pair]);

/// `TestOuterWherePredicatePushDown`, all 3 rows.
///
/// The subject is the DNF rewrite: a `WHERE` that is an OR of conjunctions
/// yields, for each side, the OR of that side's own conjuncts -- and for a
/// LEFT join only the preserved (left) side may receive one, which is why
/// every `Right` here is empty.
const OUTER_WHERE_PREDICATE_PUSH_DOWN: &[PushDownCase] = &[
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b \
         where (t1.a=1 and t2.a is null) or (t1.a=2 and t2.a=2)",
        "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
        "[]",
        &[(Some(2), Some(2))],
    ),
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b \
         where (t1.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2)",
        "[or(eq(test.t.c, 1), eq(test.t.a, 2))]",
        "[]",
        &[(Some(2), Some(2))],
    ),
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b \
         where (t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4))) \
         or (t1.a=2 and t2.a is null)",
        "[or(and(eq(test.t.c, 1), or(eq(test.t.a, 3), eq(test.t.a, 4))), eq(test.t.a, 2))]",
        "[]",
        &[],
    ),
];

/// `TestJoinPredicatePushDown`, all 14 rows.
///
/// The first 7 are INNER joins, where a `WHERE` conjunct may be pushed to
/// either side; the next 6 move the same predicates into a LEFT join's `ON`,
/// where only the NULL-supplying (right) side may receive one. The last is
/// the duplicate-conjunct case.
const JOIN_PREDICATE_PUSH_DOWN: &[PushDownCase] = &[
    (
        "select * from t as t1 join t as t2 on t1.b = t2.b where t1.a > t2.a",
        "[]",
        "[]",
        &[(Some(2), Some(1)), (Some(4), Some(3))],
    ),
    (
        "select * from t as t1 join t as t2 on t1.b = t2.b where t1.a=1 or t2.a=1",
        "[]",
        "[]",
        &[(Some(1), Some(1)), (Some(1), Some(2)), (Some(2), Some(1))],
    ),
    (
        "select * from t as t1 join t as t2 on t1.b = t2.b \
         where (t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2)",
        "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
        "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
        &[(Some(1), Some(1)), (Some(2), Some(2))],
    ),
    (
        "select * from t as t1 join t as t2 on t1.b = t2.b \
         where (t1.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2)",
        "[or(eq(test.t.c, 1), eq(test.t.a, 2))]",
        "[]",
        &[(Some(2), Some(2))],
    ),
    (
        "select * from t as t1 join t as t2 on t1.b = t2.b \
         where (t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4)))",
        "[eq(test.t.c, 1) or(eq(test.t.a, 3), eq(test.t.a, 4))]",
        "[or(eq(test.t.a, 3), eq(test.t.a, 4))]",
        &[],
    ),
    (
        "select * from t as t1 join t as t2 on t1.b = t2.b \
         where (t1.a>1 and t1.a < 3 and t2.a=1) or (t1.a=2 and t2.a=2)",
        "[or(and(gt(test.t.a, 1), lt(test.t.a, 3)), eq(test.t.a, 2))]",
        "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
        &[(Some(2), Some(1)), (Some(2), Some(2))],
    ),
    (
        "select * from t as t1 join t as t2 on t1.b = t2.b \
         and ((t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2))",
        "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
        "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
        &[(Some(1), Some(1)), (Some(2), Some(2))],
    ),
    // From here the join is LEFT: the left side is preserved, so nothing may
    // be pushed to it -- the `ON` does not filter outer rows.
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b \
         and ((t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2))",
        "[]",
        "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
        &[
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(3), None),
            (Some(4), None),
            (Some(5), None),
        ],
    ),
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b and t1.a > t2.a",
        "[]",
        "[]",
        &[
            (Some(1), None),
            (Some(2), Some(1)),
            (Some(3), None),
            (Some(4), Some(3)),
            (Some(5), None),
        ],
    ),
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b and (t1.a=1 or t2.a=1)",
        "[]",
        "[]",
        &[
            (Some(1), Some(1)),
            (Some(1), Some(2)),
            (Some(2), Some(1)),
            (Some(3), None),
            (Some(4), None),
            (Some(5), None),
        ],
    ),
    // A disjunct mentioning the LEFT side's `c` blocks the push entirely.
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b \
         and ((t1.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2))",
        "[]",
        "[]",
        &[
            (Some(1), None),
            (Some(2), Some(2)),
            (Some(3), None),
            (Some(4), None),
            (Some(5), None),
        ],
    ),
    // The same shape with `t2.c` instead: now every disjunct has a right-side
    // conjunct, so the right side receives one.
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b \
         and ((t2.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2))",
        "[]",
        "[or(eq(test.t.c, 1), eq(test.t.a, 2))]",
        &[
            (Some(1), None),
            (Some(2), Some(2)),
            (Some(3), None),
            (Some(4), None),
            (Some(5), None),
        ],
    ),
    (
        "select * from t as t1 left join t as t2 on t1.b = t2.b \
         and ((t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4))) \
         or (t1.a=2 and t2.a=2))",
        "[]",
        "[or(eq(test.t.a, 3), or(eq(test.t.a, 4), eq(test.t.a, 2)))]",
        &[
            (Some(1), None),
            (Some(2), Some(2)),
            (Some(3), None),
            (Some(4), None),
            (Some(5), None),
        ],
    ),
    // Two identical conjuncts push as ONE. This tier keeps both copies in
    // `other cond` (see `a_repeated_join_conjunct_is_not_deduplicated`).
    (
        "select * from t t1 join t t2 on t1.a > 1 and t1.a > 1",
        "[gt(test.t.a, 1)]",
        "[]",
        &[
            (Some(2), Some(1)),
            (Some(2), Some(2)),
            (Some(2), Some(3)),
            (Some(2), Some(4)),
            (Some(2), Some(5)),
            (Some(3), Some(1)),
            (Some(3), Some(2)),
            (Some(3), Some(3)),
            (Some(3), Some(4)),
            (Some(3), Some(5)),
            (Some(4), Some(1)),
            (Some(4), Some(2)),
            (Some(4), Some(3)),
            (Some(4), Some(4)),
            (Some(4), Some(5)),
            (Some(5), Some(1)),
            (Some(5), Some(2)),
            (Some(5), Some(3)),
            (Some(5), Some(4)),
            (Some(5), Some(5)),
        ],
    ),
];

/// `TestDeriveNotNullConds`, all 13 rows.
///
/// An equi/inequi join condition on a NULLABLE column implies that column is
/// not NULL on any side the join does not preserve: both sides of an INNER
/// join, the NULL-supplying side of an outer one. `b` never derives anything
/// because it is already NOT NULL, and `<=>` never does because it matches
/// NULLs.
const DERIVE_NOT_NULL_CONDS: &[PushDownCase] = &[
    (
        "select * from t t1 inner join t t2 on t1.e = t2.e",
        "[not(isnull(test.t.e))]",
        "[not(isnull(test.t.e))]",
        &[(Some(1), Some(1)), (Some(3), Some(3)), (Some(5), Some(5))],
    ),
    (
        "select * from t t1 inner join t t2 on t1.e > t2.e",
        "[not(isnull(test.t.e))]",
        "[not(isnull(test.t.e))]",
        &[(Some(3), Some(1)), (Some(5), Some(1)), (Some(5), Some(3))],
    ),
    // An explicit `is not null` does not double up: one derived condition.
    (
        "select * from t t1 inner join t t2 on t1.e = t2.e and t1.e is not null",
        "[not(isnull(test.t.e))]",
        "[not(isnull(test.t.e))]",
        &[(Some(1), Some(1)), (Some(3), Some(3)), (Some(5), Some(5))],
    ),
    (
        "select * from t t1 left join t t2 on t1.e = t2.e",
        "[]",
        "[not(isnull(test.t.e))]",
        &[
            (Some(1), Some(1)),
            (Some(2), None),
            (Some(3), Some(3)),
            (Some(4), None),
            (Some(5), Some(5)),
        ],
    ),
    (
        "select * from t t1 left join t t2 on t1.e > t2.e",
        "[]",
        "[not(isnull(test.t.e))]",
        &[
            (Some(1), None),
            (Some(2), None),
            (Some(3), Some(1)),
            (Some(4), None),
            (Some(5), Some(1)),
            (Some(5), Some(3)),
        ],
    ),
    (
        "select * from t t1 left join t t2 on t1.e = t2.e and t2.e is not null",
        "[]",
        "[not(isnull(test.t.e))]",
        &[
            (Some(1), Some(1)),
            (Some(2), None),
            (Some(3), Some(3)),
            (Some(4), None),
            (Some(5), Some(5)),
        ],
    ),
    // RIGHT join: the derivation moves to the left side.
    (
        "select * from t t1 right join t t2 on t1.e = t2.e and t1.e is not null",
        "[not(isnull(test.t.e))]",
        "[]",
        &[
            (Some(1), Some(1)),
            (Some(3), Some(3)),
            (Some(5), Some(5)),
            (None, Some(2)),
            (None, Some(4)),
        ],
    ),
    // `<=>` is NULL-safe, so nothing is derived on either side.
    (
        "select * from t t1 inner join t t2 on t1.e <=> t2.e",
        "[]",
        "[]",
        &[
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(2), Some(4)),
            (Some(3), Some(3)),
            (Some(4), Some(2)),
            (Some(4), Some(4)),
            (Some(5), Some(5)),
        ],
    ),
    (
        "select * from t t1 left join t t2 on t1.e <=> t2.e",
        "[]",
        "[]",
        &[
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(2), Some(4)),
            (Some(3), Some(3)),
            (Some(4), Some(2)),
            (Some(4), Some(4)),
            (Some(5), Some(5)),
        ],
    ),
    // `b` is already NOT NULL: nothing to derive.
    (
        "select * from t t1 inner join t t2 on t1.b = t2.b",
        "[]",
        "[]",
        &[
            (Some(1), Some(1)),
            (Some(1), Some(2)),
            (Some(2), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(3), Some(4)),
            (Some(4), Some(3)),
            (Some(4), Some(4)),
            (Some(5), Some(5)),
        ],
    ),
    (
        "select * from t t1 left join t t2 on t1.b = t2.b",
        "[]",
        "[]",
        &[
            (Some(1), Some(1)),
            (Some(1), Some(2)),
            (Some(2), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(3), Some(4)),
            (Some(4), Some(3)),
            (Some(4), Some(4)),
            (Some(5), Some(5)),
        ],
    ),
    (
        "select * from t t1 left join t t2 on t1.b > t2.b",
        "[]",
        "[]",
        &[
            (Some(1), None),
            (Some(2), None),
            (Some(3), Some(1)),
            (Some(3), Some(2)),
            (Some(4), Some(1)),
            (Some(4), Some(2)),
            (Some(5), Some(1)),
            (Some(5), Some(2)),
            (Some(5), Some(3)),
            (Some(5), Some(4)),
        ],
    ),
    // An anti semi join preserves its outer side, so nothing is derived --
    // and the rows are the ones whose `e` IS NULL, which is the point: a
    // derivation here would have deleted them.
    (
        "select * from t t1 where not exists (select * from t t2 where t2.e = t1.e)",
        "[]",
        "[]",
        &[(Some(2), None), (Some(4), None)],
    ),
];

/// The `NOT EXISTS` case selects only `t1`, so its expected row is the left
/// half alone.
fn anti_semi_expected(pairs: &[Pair]) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = pairs.iter().map(|(left, _)| side(*left)).collect();
    out.sort();
    out
}

#[test]
fn outer_where_predicate_push_down_returns_tidbs_rows() {
    let mut session = signed_table_session();
    for (sql, _, _, pairs) in OUTER_WHERE_PREDICATE_PUSH_DOWN {
        check(&mut session, sql, pairs);
    }
}

#[test]
fn join_predicate_push_down_returns_tidbs_rows() {
    let mut session = signed_table_session();
    for (sql, _, _, pairs) in JOIN_PREDICATE_PUSH_DOWN {
        check(&mut session, sql, pairs);
    }
}

#[test]
fn derive_not_null_conds_returns_tidbs_rows() {
    let mut session = signed_table_session();
    let (anti_semi, joins) = DERIVE_NOT_NULL_CONDS.split_last().unwrap();
    for (sql, _, _, pairs) in joins {
        check(&mut session, sql, pairs);
    }
    let (sql, _, _, pairs) = anti_semi;
    assert_eq!(rows(&mut session, sql), anti_semi_expected(pairs), "{sql}");
}

/// Go's `Left`/`Right` for `TestOuterWherePredicatePushDown`, asserted as Go
/// gives them, against the nearest observable this tier has.
#[test]
fn outer_where_predicate_push_down_derives_a_left_side_condition() {
    assert_pushed_conditions(OUTER_WHERE_PREDICATE_PUSH_DOWN);
}

#[test]
fn join_predicate_push_down_derives_per_side_conditions() {
    assert_pushed_conditions(JOIN_PREDICATE_PUSH_DOWN);
}

#[test]
fn derive_not_null_conds_pushes_not_null_to_every_unpreserved_side() {
    assert_pushed_conditions(DERIVE_NOT_NULL_CONDS);
}

#[test]
fn mutable_predicates_are_not_duplicated_below_a_join() {
    let mut session = signed_table_session();
    let sql = "select * from t t1 left join t t2 on t1.e = t2.e where t1.a > rand()";
    let below = conditions_below_join(&mut session, sql);
    assert!(
        below.iter().all(|(_, info)| !info.contains("rand")),
        "RAND() must remain above the join: {below:?}"
    );
}

/// Asserts each side of the join carries exactly Go's condition list.
fn assert_pushed_conditions(cases: &[PushDownCase]) {
    let mut session = signed_table_session();
    for (sql, left, right, _) in cases {
        let (found_left, found_right) = conditions_by_side(&mut session, sql);
        assert_condition_list(&found_left, left, "left", sql);
        assert_condition_list(&found_right, right, "right", sql);
    }
}

/// The conditions below the join, split by WHICH of the two aliases the scan
/// beneath each one reads.
///
/// The condition text cannot say. A plan prints a column by its BASE table --
/// `test.t.e` whether it came from `t1.e` or `t2.e` -- and Go prints it the
/// same way; its own recording of a self-join
/// (`tests/integrationtest/r/explain_easy.result:915-925`) shows
/// `not(isnull(explain_easy.t.e))` under BOTH readers, with only the
/// `table:t1`/`table:t2` access objects telling them apart. So the scan's
/// access object is what assigns a side here.
///
/// `EXPLAIN` is depth-first, so every conditioned operator between one scan
/// and the previous one sits above that scan and belongs to its side.
fn conditions_by_side(session: &mut Session, sql: &str) -> (Vec<String>, Vec<String>) {
    let plan = match session
        .run(&format!("EXPLAIN {sql}"))
        .unwrap_or_else(|error| panic!("EXPLAIN failed for `{sql}`: {error:?}"))
    {
        StmtResult::Rows(rows) => rows,
        other => panic!("expected rows from EXPLAIN, got {other:?}"),
    };
    let Some(join_at) = plan
        .iter()
        .position(|row| cell_text(&row[0]).contains("Join"))
    else {
        return (Vec::new(), Vec::new());
    };
    let mut left = Vec::new();
    let mut right = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    for row in &plan[join_at + 1..] {
        let access_object = cell_text(&row[3]);
        let info = cell_text(&row[4]);
        let conditions = match integer_handle_condition(&access_object, &info) {
            Some(condition) => vec![condition],
            None if info.is_empty()
                || info.starts_with("keep order:")
                || info.starts_with("data:")
                || info.starts_with("index:")
                || info.starts_with("range:") =>
            {
                Vec::new()
            }
            None => logical_conditions(&info),
        };
        let Some(alias) = access_object
            .strip_prefix("table:")
            .and_then(|table| table.split(',').next())
            .map(str::trim)
        else {
            pending.extend(conditions);
            continue;
        };
        // `t1` is Go's Left and `t2` its Right; any other alias is a table
        // this corpus does not name, and its conditions go with it.
        let side = match alias {
            "t1" => &mut left,
            "t2" => &mut right,
            _ => {
                pending.clear();
                continue;
            }
        };
        for condition in pending.drain(..).chain(conditions) {
            if !side.contains(&condition) {
                side.push(condition);
            }
        }
    }
    (left, right)
}

fn logical_conditions(info: &str) -> Vec<String> {
    let mut conditions = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut chars = info.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 && chars.peek() == Some(&' ') => {
                chars.next();
                conditions.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(ch);
    }
    if !current.is_empty() {
        conditions.push(current);
    }
    conditions
}

fn logical_condition_list(conditions: &[String]) -> String {
    format!("[{}]", conditions.join(" "))
}

fn assert_condition_list(found: &[String], expected: &str, side: &str, sql: &str) {
    let rendered = logical_condition_list(found);
    assert!(
        rendered == expected || integer_handle_conditions_are_equivalent(found, expected),
        "{side} pushed conditions of `{sql}`\n  left: {rendered:?}\n right: {expected:?}"
    );
}

#[derive(Debug)]
enum ObservedConditionExpr {
    Atom(String),
    Call(String, Vec<ObservedConditionExpr>),
}

struct ObservedConditionParser<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ObservedConditionParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            offset: 0,
        }
    }

    fn parse_all(mut self) -> Option<Vec<ObservedConditionExpr>> {
        let mut expressions = Vec::new();
        self.skip_spaces();
        while self.offset < self.input.len() {
            expressions.push(self.parse_expression()?);
            self.skip_spaces();
        }
        Some(expressions)
    }

    fn parse_expression(&mut self) -> Option<ObservedConditionExpr> {
        self.skip_spaces();
        let start = self.offset;
        while self.offset < self.input.len()
            && !matches!(self.input[self.offset], b'(' | b')' | b',' | b' ')
        {
            self.offset += 1;
        }
        if self.offset == start {
            return None;
        }
        let token = std::str::from_utf8(&self.input[start..self.offset])
            .ok()?
            .to_owned();
        if self.input.get(self.offset) != Some(&b'(') {
            return Some(ObservedConditionExpr::Atom(token));
        }

        self.offset += 1;
        let mut arguments = Vec::new();
        loop {
            self.skip_spaces();
            if self.input.get(self.offset) == Some(&b')') {
                self.offset += 1;
                break;
            }
            arguments.push(self.parse_expression()?);
            self.skip_spaces();
            match self.input.get(self.offset) {
                Some(b',') => self.offset += 1,
                Some(b')') => {
                    self.offset += 1;
                    break;
                }
                _ => return None,
            }
        }
        Some(ObservedConditionExpr::Call(token, arguments))
    }

    fn skip_spaces(&mut self) {
        while self.input.get(self.offset) == Some(&b' ') {
            self.offset += 1;
        }
    }
}

#[derive(Clone, Copy)]
enum ObservedConditionValue {
    Boolean(bool),
    Integer(i64),
}

fn integer_handle_conditions_are_equivalent(found: &[String], expected: &str) -> bool {
    if found.is_empty() {
        return false;
    }
    let Some(expected) = expected
        .strip_prefix('[')
        .and_then(|expected| expected.strip_suffix(']'))
    else {
        return false;
    };
    let Some(found) = found
        .iter()
        .map(|condition| ObservedConditionParser::new(condition).parse_all())
        .collect::<Option<Vec<_>>>()
        .map(|groups| groups.into_iter().flatten().collect::<Vec<_>>())
    else {
        return false;
    };
    let Some(expected) = ObservedConditionParser::new(expected).parse_all() else {
        return false;
    };
    if expected.is_empty() {
        return false;
    }

    let mut constants = Vec::new();
    for expression in found.iter().chain(&expected) {
        collect_integer_constants(expression, &mut constants);
    }
    let mut samples = vec![i64::MIN, i64::MAX, 0];
    for constant in constants {
        samples.push(constant);
        if let Some(before) = constant.checked_sub(1) {
            samples.push(before);
        }
        if let Some(after) = constant.checked_add(1) {
            samples.push(after);
        }
    }
    samples.sort_unstable();
    samples.dedup();
    samples.into_iter().all(|handle| {
        evaluate_condition_list(&found, handle) == evaluate_condition_list(&expected, handle)
            && evaluate_condition_list(&found, handle).is_some()
    })
}

fn collect_integer_constants(expression: &ObservedConditionExpr, constants: &mut Vec<i64>) {
    match expression {
        ObservedConditionExpr::Atom(atom) => {
            if let Ok(value) = atom.parse::<i64>() {
                constants.push(value);
            }
        }
        ObservedConditionExpr::Call(_, arguments) => {
            for argument in arguments {
                collect_integer_constants(argument, constants);
            }
        }
    }
}

fn evaluate_condition_list(expressions: &[ObservedConditionExpr], handle: i64) -> Option<bool> {
    expressions.iter().try_fold(true, |result, expression| {
        let ObservedConditionValue::Boolean(value) =
            evaluate_condition_expression(expression, handle)?
        else {
            return None;
        };
        Some(result && value)
    })
}

fn evaluate_condition_expression(
    expression: &ObservedConditionExpr,
    handle: i64,
) -> Option<ObservedConditionValue> {
    match expression {
        ObservedConditionExpr::Atom(atom) if atom == "test.t.a" => {
            Some(ObservedConditionValue::Integer(handle))
        }
        ObservedConditionExpr::Atom(atom) => atom
            .parse::<i64>()
            .ok()
            .map(ObservedConditionValue::Integer),
        ObservedConditionExpr::Call(name, arguments) => {
            let values = arguments
                .iter()
                .map(|argument| evaluate_condition_expression(argument, handle))
                .collect::<Option<Vec<_>>>()?;
            match (name.as_str(), values.as_slice()) {
                (
                    "eq",
                    [ObservedConditionValue::Integer(left), ObservedConditionValue::Integer(right)],
                ) => Some(ObservedConditionValue::Boolean(left == right)),
                (
                    "gt",
                    [ObservedConditionValue::Integer(left), ObservedConditionValue::Integer(right)],
                ) => Some(ObservedConditionValue::Boolean(left > right)),
                (
                    "lt",
                    [ObservedConditionValue::Integer(left), ObservedConditionValue::Integer(right)],
                ) => Some(ObservedConditionValue::Boolean(left < right)),
                (
                    "and",
                    [ObservedConditionValue::Boolean(left), ObservedConditionValue::Boolean(right)],
                ) => Some(ObservedConditionValue::Boolean(*left && *right)),
                (
                    "or",
                    [ObservedConditionValue::Boolean(left), ObservedConditionValue::Boolean(right)],
                ) => Some(ObservedConditionValue::Boolean(*left || *right)),
                _ => None,
            }
        }
    }
}

#[test]
fn null_safe_equality_is_a_hash_join_key() {
    let mut session = signed_table_session();
    for sql in [
        "select * from t t1 inner join t t2 on t1.e <=> t2.e",
        "select * from t t1 left join t t2 on t1.e <=> t2.e",
        "select * from t t1 right join t t2 on t1.e <=> t2.e",
    ] {
        let info = join_info(&mut session, sql);
        assert!(
            !info.contains("CARTESIAN") && info.contains("equal:[nulleq("),
            "expected a NULL-safe equality key for `{sql}`, got {info}"
        );
    }
}

#[test]
fn mixed_ordinary_and_null_safe_hash_keys_keep_their_own_null_rules() {
    let mut session = signed_table_session();
    assert_eq!(
        rows(
            &mut session,
            "select t1.a, t2.a from t t1 join t t2 \
             on t1.e <=> t2.e and t1.h = t2.h",
        ),
        [
            vec!["1".to_owned(), "1".to_owned()],
            vec!["3".to_owned(), "3".to_owned()],
            vec!["4".to_owned(), "4".to_owned()],
            vec!["5".to_owned(), "5".to_owned()],
        ]
    );
}

/// Go removes duplicate child predicates before handing them to DataSource.
#[test]
fn a_repeated_join_conjunct_is_removed_from_the_join_after_pushdown() {
    let mut session = signed_table_session();
    let sql = "select * from t t1 join t t2 on t1.a > 1 and t1.a > 1";
    assert_eq!(join_info(&mut session, sql), "CARTESIAN inner join");
}

/// Go's `rule_decorrelate` moves the correlated inner Selection into an anti
/// semi join, so the inner table is built once instead of re-run for every
/// outer row.
#[test]
fn correlated_not_exists_is_an_anti_semi_hash_join() {
    let mut session = signed_table_session();
    let sql = "select * from t t1 where not exists (select * from t t2 where t2.e = t1.e)";
    assert_eq!(
        join_type(&mut session, sql),
        Some("anti semi join".to_owned())
    );
    let info = join_info(&mut session, sql);
    assert!(
        !info.contains("CARTESIAN") && info.contains("equal:[eq("),
        "expected a correlated equality hash key, got {info}"
    );
}

#[test]
fn decorrelated_exists_keeps_semijoin_row_and_condition_semantics() {
    let mut session = signed_table_session();
    let cases = [
        (
            "select t1.a from t t1 where exists \
             (select 1 from t t2 where t2.e = t1.e)",
            &["1", "3", "5"][..],
        ),
        (
            "select t1.a from t t1 where exists \
             (select 1 from t t2 where t2.b = t1.b)",
            &["1", "2", "3", "4", "5"][..],
        ),
        (
            "select t1.a from t t1 where exists \
             (select 1 from t t2 where t2.a < t1.a)",
            &["2", "3", "4", "5"][..],
        ),
        (
            "select t1.a from t t1 where exists \
             (select 1 from t t2 where t2.e = t1.e and t2.a = 3)",
            &["3"][..],
        ),
        (
            "select t1.a from t t1 where t1.a > 1 and exists \
             (select 1 from t t2 where t2.e = t1.e) and exists \
             (select 1 from t t3 where t3.a < t1.a)",
            &["3", "5"][..],
        ),
    ];
    for (sql, expected) in cases {
        let actual: Vec<String> = rows(&mut session, sql)
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(actual, expected, "rows of `{sql}`");
    }
}

#[test]
fn no_decorrelate_hint_keeps_the_correlated_apply() {
    let mut session = signed_table_session();
    let sql = "select t1.a from t t1 where exists \
               (select /*+ NO_DECORRELATE() */ 1 from t t2 where t2.e = t1.e)";
    assert_eq!(join_type(&mut session, sql), None);
    assert_eq!(
        rows(&mut session, sql),
        [
            vec!["1".to_owned()],
            vec!["3".to_owned()],
            vec!["5".to_owned()]
        ]
    );
}

/// The last Go `TestJoinPredicatePushDown` case leaves exactly one copy on the
/// left DataSource and none on the join.
#[test]
fn a_repeated_join_conjunct_is_deduplicated_at_the_leaf() {
    let mut session = signed_table_session();
    let conditions = conditions_below_join(
        &mut session,
        "select * from t t1 join t t2 on t1.a > 1 and t1.a > 1",
    );
    assert_eq!(
        conditions
            .iter()
            .filter(|(_, info)| info.contains("gt(test.t.a, 1)"))
            .count(),
        1
    );
}

/// The join operator's full `operator info` cell.
fn join_info(session: &mut Session, sql: &str) -> String {
    let plan = match session.run(&format!("EXPLAIN {sql}")).unwrap() {
        StmtResult::Rows(rows) => rows,
        other => panic!("expected rows from EXPLAIN, got {other:?}"),
    };
    plan.iter()
        .find(|row| cell_text(&row[0]).contains("Join"))
        .map(|row| cell_text(&row[4]))
        .unwrap_or_default()
}

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
//! # Remaining gaps
//!
//! Rows agree everywhere. Outer-to-inner conversion now matches all eight Go
//! cases. Three independent plan gaps remain:
//!
//! * **`<=>` is not an equal key.** Go joins on `nulleq`; here it lands in
//!   `other cond` and the join goes CARTESIAN.
//! * **`NOT EXISTS` is not an anti semi join.** Go's
//!   `TestDeriveNotNullConds` row 12 plans one; here it runs as a correlated
//!   `Selection` with no join operator at all.
//!
//! None changes a result; each changes how much work the store performs.

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
    plan[join_at + 1..]
        .iter()
        .filter_map(|row| {
            let id = cell_text(&row[0]);
            let info = cell_text(&row[4]);
            // A scan's own `keep order:`/`stats:` note is not a predicate.
            if info.is_empty() || info.starts_with("keep order:") {
                return None;
            }
            Some((
                id.trim()
                    .trim_start_matches(['├', '└', '│', '─', ' '])
                    .to_owned(),
                info,
            ))
        })
        .collect()
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
    assert!(info.contains("equal:[eq("), "{info}");
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
///
/// The plan is read in `EXPLAIN` order, so the first conditioned operator
/// under the join belongs to the build side and the second to the probe side;
/// `crate::explain` prints the build side first (`(Build)` then `(Probe)`),
/// and this tier builds the RIGHT table as the build side -- so the first is
/// Go's `Right` and the second Go's `Left`.
fn assert_pushed_conditions(cases: &[PushDownCase]) {
    let mut session = signed_table_session();
    for (sql, left, right, _) in cases {
        let below = conditions_below_join(&mut session, sql);
        let found: Vec<String> = below
            .iter()
            .map(|(_, info)| {
                logical_condition_list(
                    &info
                        .replace("test.t1.", "test.t.")
                        .replace("test.t2.", "test.t."),
                )
            })
            .collect();
        let mut want: Vec<String> = Vec::new();
        if *right != "[]" {
            want.push((*right).to_owned());
        }
        if *left != "[]" {
            want.push((*left).to_owned());
        }
        assert_eq!(found, want, "pushed conditions of `{sql}`");
    }
}

fn logical_condition_list(info: &str) -> String {
    let mut out = String::from("[");
    let mut depth = 0usize;
    let mut chars = info.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 && chars.peek() == Some(&' ') => {
                chars.next();
                out.push(' ');
                continue;
            }
            _ => {}
        }
        out.push(ch);
    }
    out.push(']');
    out
}

/// The three remaining plan-shape gaps this table exposed, pinned as they are
/// today so that closing one is visible.
///
/// Each is row-correct and cost-wrong: a CARTESIAN join where TiDB uses a
/// key, or a correlated re-scan where TiDB uses one hash pass.
#[test]
fn the_join_shapes_this_tier_builds_where_tidb_builds_a_better_one() {
    let mut session = signed_table_session();

    // `<=>` is an equal key for TiDB (`equal:[nulleq(test.t.e, test.t.e)]`);
    // here it is `other cond` over a CARTESIAN product.
    for sql in [
        "select * from t t1 inner join t t2 on t1.e <=> t2.e",
        "select * from t t1 left join t t2 on t1.e <=> t2.e",
    ] {
        assert!(
            join_info(&mut session, sql).contains("CARTESIAN"),
            "expected a CARTESIAN join for `{sql}`"
        );
    }

    // A repeated conjunct is kept twice; TiDB pushes it once.
    let sql = "select * from t t1 join t t2 on t1.a > 1 and t1.a > 1";
    assert_eq!(
        join_info(&mut session, sql),
        "CARTESIAN inner join, other cond:gt(test.t1.a, 1), gt(test.t1.a, 1)"
    );

    // `NOT EXISTS` runs as a correlated Selection: no join operator at all,
    // where TiDB plans `anti semi join, equal:[eq(test.t.e, test.t.e)]`.
    let sql = "select * from t t1 where not exists (select * from t t2 where t2.e = t1.e)";
    assert_eq!(join_type(&mut session, sql), None);
}

/// A repeated conjunct survives twice in the join's `other cond`, which is
/// the shape `TestJoinPredicatePushDown`'s last row is about.
#[test]
fn a_repeated_join_conjunct_is_not_deduplicated() {
    let mut session = signed_table_session();
    let info = join_info(
        &mut session,
        "select * from t t1 join t t2 on t1.a > 1 and t1.a > 1",
    );
    assert_eq!(info.matches("gt(test.t1.a, 1)").count(), 2);
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

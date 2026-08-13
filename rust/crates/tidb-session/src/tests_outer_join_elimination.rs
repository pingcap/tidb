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

//! Go's `rule_join_elimination.go` over the wire: which outer joins stop
//! reading their inner table, and -- the half that matters -- that the ROWS
//! never move when one does.
//!
//! # Where the expected values come from
//!
//! Every row set and every "which tables does the plan read" answer below was
//! captured from real TiDB with `rust/difftests/gorun` over these exact
//! fixtures. The plan is read the way the replay gate reads it: the set of
//! base tables the plan accesses at all, which is the property
//! `difftests/result-tests/tests/integration_diff.rs` compares and the one an
//! elimination changes (TiDB prints `<not read>` for the table that is gone).
//!
//! # Why a rows assertion is the point
//!
//! Eliminating an outer join is only legal when the inner side can neither
//! contribute a column nor MULTIPLY an outer row. The second half is the one
//! that fails silently: a non-unique join key duplicates outer rows, and
//! dropping the join then loses those duplicates. The captures below pin both
//! directions of exactly that -- `d2`'s non-unique `k(a)` really does produce
//! `1|1` twice in TiDB, and `u2`'s `uk(a, b)` really does not when both key
//! parts are matched but really does when only one is.
//!
//! It also carries the two `explain_easy` verdicts this batch MEASURED and
//! deliberately did not change ([`an_uncorrelated_in_subquery_answers_tidbs_rows_through_a_different_plan`]
//! and [`a_correlated_scalar_aggregate_answers_tidbs_rows_through_an_apply`]),
//! because a plan that is merely different needs its rows pinned even more
//! than one that is the same.
//!
//! # The two gaps this measures
//!
//! * **`<=>` is not a join key.** TiDB does not eliminate a `n1.a <=> n2.a`
//!   join even over a unique `n2.a`, because NULL matches NULL there and two
//!   NULL inner rows duplicate the outer one -- captured as four rows where
//!   `=` gives three. This tier refuses it too, so the shapes agree.
//! * **Duplicate-agnostic elimination remains bounded.** `SELECT DISTINCT`
//!   over a non-unique join key now takes Go's second ground and removes the
//!   inner table. Grouped and hidden aggregate carriers still need the full
//!   logical aggregate-column walk.

#![cfg(test)]

use crate::tests_support::cell_text;
use crate::{Session, StmtResult};

/// The rows of `sql`, sorted, `|`-joined per row -- the shape `gorun` prints.
fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    let mut out = match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| row.iter().map(cell_text).collect::<Vec<_>>().join("|"))
            .collect::<Vec<_>>(),
        other => panic!("expected rows from `{sql}`, got {other:?}"),
    };
    out.sort();
    out
}

/// The base tables the plan for `sql` accesses, sorted and deduplicated.
///
/// Read off the `access object` column's `table:<name>` prefix, which is how
/// both TiDB and this tier name the table a scan node reads.
fn tables_read(session: &mut Session, sql: &str) -> Vec<String> {
    let plan = match session.run(&format!("EXPLAIN {sql}")).unwrap() {
        StmtResult::Rows(rows) => rows,
        other => panic!("expected rows from EXPLAIN, got {other:?}"),
    };
    let mut out: Vec<String> = plan
        .iter()
        .filter_map(|row| {
            let object = cell_text(&row[3]);
            let name = object.strip_prefix("table:")?;
            Some(name.split(',').next().unwrap_or_default().trim().to_owned())
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// `e1`/`e2` are `explain_easy`'s own two tables, and `n1`/`n2` add the
/// nullable-unique-key case `explain_easy` has no example of.
fn eliminable_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "create table e1(a int, b int, c int, primary key(a, b))",
        "create table e2(a int, b int, c int, primary key(a))",
        "insert into e1 values (1,1,1),(2,2,2),(3,3,3)",
        "insert into e2 values (1,10,10),(3,30,30)",
        "create table n1(a int, b int)",
        "create table n2(a int unique, b int)",
        "insert into n1 values (1,1),(null,2),(2,3)",
        "insert into n2 values (1,10),(null,20),(null,30)",
    ] {
        session.run(sql).unwrap();
    }
    session
}

/// `d2`'s key is non-unique and `u2`'s is unique only across BOTH parts --
/// the two ways an inner side can still multiply an outer row.
fn duplicating_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "create table d1(a int, b int)",
        "create table d2(a int, b int, key k(a))",
        "insert into d1 values (1,1),(2,2),(null,3)",
        "insert into d2 values (1,10),(1,11),(3,30)",
        "create table u1(a int, b int)",
        "create table u2(a int, b int, c int, unique key uk(a, b))",
        "insert into u1 values (1,1),(2,2),(null,3)",
        "insert into u2 values (1,1,100),(1,2,200)",
    ] {
        session.run(sql).unwrap();
    }
    session
}

#[test]
fn a_unique_inner_key_drops_the_inner_table_and_keeps_every_row() {
    let mut session = eliminable_session();
    // gorun: RS:1|1;2|2;3|3, plan reads e1 only.
    let sql = "select e1.a, e1.b from e1 left outer join e2 on e1.a = e2.a";
    assert_eq!(rows(&mut session, sql), ["1|1", "2|2", "3|3"]);
    assert_eq!(tables_read(&mut session, sql), ["e1"]);

    // The `distinct` variant of the same statement: TiDB eliminates it on the
    // SAME ground -- `e2.a` is a PRIMARY KEY -- not on its duplicate-agnostic
    // one, which is why it closes here too.
    let sql = "select distinct e1.a, e1.b from e1 left outer join e2 on e1.a = e2.a";
    assert_eq!(rows(&mut session, sql), ["1|1", "2|2", "3|3"]);
    assert_eq!(tables_read(&mut session, sql), ["e1"]);
}

#[test]
fn a_nullable_unique_key_still_eliminates_and_a_null_safe_one_does_not() {
    let mut session = eliminable_session();
    // gorun: RS:1|1;2|3;<nil>|2, plan reads n1 only. Two of `n2`'s three rows
    // hold NULL in the unique column and `=` never matches them, so the
    // at-most-one-match guarantee survives the nullability.
    let sql = "select n1.a, n1.b from n1 left outer join n2 on n1.a = n2.a";
    assert_eq!(rows(&mut session, sql), ["1|1", "2|3", "NULL|2"]);
    assert_eq!(tables_read(&mut session, sql), ["n1"]);

    // gorun: RS:1|1;2|3;<nil>|2;<nil>|2 -- FOUR rows. `<=>` matches the outer
    // NULL against BOTH inner NULLs, so the join duplicates and the inner
    // table must stay.
    let sql = "select n1.a, n1.b from n1 left outer join n2 on n1.a <=> n2.a";
    assert_eq!(rows(&mut session, sql), ["1|1", "2|3", "NULL|2", "NULL|2"]);
    assert_eq!(tables_read(&mut session, sql), ["n1", "n2"]);
}

#[test]
fn a_non_unique_or_partially_matched_key_keeps_the_join() {
    let mut session = duplicating_session();
    // gorun: RS:1|1;1|1;2|2;<nil>|3, plan reads d1 AND d2. `d1`'s row 1
    // matches both of `d2`'s `a = 1` rows.
    let sql = "select d1.a, d1.b from d1 left outer join d2 on d1.a = d2.a";
    assert_eq!(rows(&mut session, sql), ["1|1", "1|1", "2|2", "NULL|3"]);
    assert_eq!(tables_read(&mut session, sql), ["d1", "d2"]);

    // gorun: RS:1|1;2|2;<nil>|3, plan reads u1 only -- BOTH parts of
    // `uk(a, b)` are join keys.
    let sql = "select u1.a, u1.b from u1 left outer join u2 on u1.a = u2.a and u1.b = u2.b";
    assert_eq!(rows(&mut session, sql), ["1|1", "2|2", "NULL|3"]);
    assert_eq!(tables_read(&mut session, sql), ["u1"]);

    // gorun: RS:1|1;1|1;2|2;<nil>|3, plan reads u1 AND u2 -- only the leading
    // part of `uk(a, b)` is a join key, so the prefix is not unique and the
    // outer row duplicates.
    let sql = "select u1.a, u1.b from u1 left outer join u2 on u1.a = u2.a";
    assert_eq!(rows(&mut session, sql), ["1|1", "1|1", "2|2", "NULL|3"]);
    assert_eq!(tables_read(&mut session, sql), ["u1", "u2"]);
}

#[test]
fn a_referenced_inner_column_keeps_the_join() {
    let mut session = eliminable_session();
    // The select list names `e2.b`, so the inner side contributes a column.
    let sql = "select e1.a, e2.b from e1 left outer join e2 on e1.a = e2.a";
    assert_eq!(rows(&mut session, sql), ["1|10", "2|NULL", "3|30"]);
    assert_eq!(tables_read(&mut session, sql), ["e1", "e2"]);

    // A `WHERE` over the inner side does too -- and it changes the answer,
    // which is what makes eliminating it a wrong plan rather than a slow one.
    let sql = "select e1.a, e1.b from e1 left outer join e2 on e1.a = e2.a where e2.b > 10";
    assert_eq!(rows(&mut session, sql), ["3|3"]);
    assert_eq!(tables_read(&mut session, sql), ["e1", "e2"]);

    // `*` reads the inner side's whole width.
    let sql = "select * from e1 left outer join e2 on e1.a = e2.a";
    assert_eq!(tables_read(&mut session, sql), ["e1", "e2"]);
}

#[test]
fn distinct_over_a_non_unique_key_eliminates_the_inner_table() {
    let mut session = duplicating_session();
    // gorun: RS:1|1;2|2;<nil>|3, and TiDB's plan reads d1 ONLY -- Go's
    // duplicate-agnostic outer-join ground. DISTINCT removes the duplicates
    // the join would create, so the inner table is not observable.
    let sql = "select distinct d1.a, d1.b from d1 left outer join d2 on d1.a = d2.a";
    assert_eq!(rows(&mut session, sql), ["1|1", "2|2", "NULL|3"]);
    assert_eq!(tables_read(&mut session, sql), ["d1"]);
}

/// The IN-subquery verdict this batch measured but did NOT change, kept as a
/// running assertion so the shape cannot drift unnoticed.
///
/// `explain_easy` records `select c1 from t1 where c1 in (select c2 from t2)`
/// as a semi join over `IndexFullScan table:t1, index:c2(c2)`; this tier
/// answers it with `Batch_Point_Get table:t1`, because the subquery is
/// UNCORRELATED and `run_select_traced` folds it to a literal list before the
/// access path is chosen (Go instead rewrites IN to a join under
/// `tidb_opt_insubq_to_join_and_agg`, so it never runs the subquery). That is
/// a DIFFERENT plan, not a wrong one -- these three cases are the ones where
/// the difference could show, and all three match gorun's capture of real
/// TiDB exactly:
///
/// * a NULL among the subquery's values (`s2.c2` is NULL for `c1 = 2`);
/// * a subquery that returns ONLY NULL;
/// * a subquery that returns NO row.
#[test]
fn an_uncorrelated_in_subquery_answers_tidbs_rows_through_a_different_plan() {
    let mut session = Session::new();
    for sql in [
        "create table s1 (c1 int primary key, c2 int, c3 int, index c2 (c2))",
        "create table s2 (c1 int unique, c2 int)",
        "insert into s1 values (1,1,1),(2,2,2),(3,3,3),(4,4,4)",
        "insert into s2 values (1,1),(2,null),(3,3),(4,3)",
    ] {
        session.run(sql).unwrap();
    }
    // gorun: RS:1;3 -- `c1 = 2` is NOT selected: `2 in (1, NULL, 3, 3)` is
    // UNKNOWN, not true, and `c1 = 4` is not among the values at all.
    assert_eq!(
        rows(
            &mut session,
            "select c1 from s1 where c1 in (select c2 from s2)"
        ),
        ["1", "3"]
    );
    // gorun: RS: (no rows).
    assert_eq!(
        rows(
            &mut session,
            "select c1 from s1 where c1 in (select c2 from s2 where c2 is null)"
        ),
        Vec::<String>::new()
    );
    session.run("delete from s2").unwrap();
    // gorun: RS: (no rows).
    assert_eq!(
        rows(
            &mut session,
            "select c1 from s1 where c1 in (select c2 from s2)"
        ),
        Vec::<String>::new()
    );
}

/// The decorrelation verdict this batch measured but did NOT change.
///
/// `explain_easy` records
/// `select (select count(1) k from t1 s where s.c1 = t1.c1 having k != 0) from t1`
/// as a `MergeJoin` -- Go's `rule_decorrelate` turns the correlated scalar
/// aggregate into a left outer join, and that join's ORDER requirement on
/// `t1.c1` is what forces TiDB's `TableFullScan table:t1 keep order:true`
/// (captured with gorun: both scans of the recorded plan are ordered table
/// scans). This tier runs the subquery per outer row through an `Apply`
/// (`driver::subquery`), so it has no order requirement and picks the
/// covering `c2` index for `t1` instead -- which is the whole of the recorded
/// access-property divergence. The ROWS are the same, and Go's own answer for
/// them is pinned here.
#[test]
fn a_correlated_scalar_aggregate_answers_tidbs_rows_through_an_apply() {
    let mut session = Session::new();
    for sql in [
        "create table t1 (c1 int primary key, c2 int, c3 int, index c2 (c2))",
        "insert into t1 values (1,1,1),(2,2,2),(3,3,3)",
    ] {
        session.run(sql).unwrap();
    }
    // gorun: RS:1;1;1.
    assert_eq!(
        rows(
            &mut session,
            "select (select count(1) k from t1 s where s.c1 = t1.c1 having k != 0) from t1"
        ),
        ["1", "1", "1"]
    );
}

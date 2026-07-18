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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Aggregation execution tests (`GROUP BY`, `COUNT`/`SUM`/
//! `AVG`/`GROUP_CONCAT`, `WITH ROLLUP` rejection).

use super::*;

#[test]
fn grouping_and_aggregation() {
    let mut db = Database::new();
    step(&mut db, "create table g (k int, v int, s varchar(9))");
    step(&mut db, "insert into g values (1, 10, 'a')");
    step(&mut db, "insert into g values (1, 20, 'b')");
    step(&mut db, "insert into g values (2, 30, 'a')");
    step(&mut db, "insert into g values (2, 40, 'c')");
    step(&mut db, "insert into g values (2, 50, 'a')");
    assert_eq!(step(&mut db, "select count(*) from g"), "RS:5");
    assert_eq!(step(&mut db, "select sum(v) from g"), "RS:150");
    step(&mut db, "create table sum_wide (v bigint)");
    step(
        &mut db,
        "insert into sum_wide values (9223372036854775807), (1)",
    );
    assert_eq!(
        step(&mut db, "select sum(v) from sum_wide"),
        "RS:9223372036854775808"
    );
    assert_eq!(step(&mut db, "select max(v), min(v) from g"), "RS:50|10");
    assert_eq!(
        step(&mut db, "select k, count(*) from g group by k"),
        "RS:1|2;2|3"
    );
    assert_eq!(
        step(&mut db, "select k, sum(v) from g group by k"),
        "RS:1|30;2|120"
    );
    assert_eq!(
        step(&mut db, "select k, count(distinct s) from g group by k"),
        "RS:1|2;2|2"
    );
    assert_eq!(step(&mut db, "select count(distinct k) from g"), "RS:2");
    assert_eq!(
        step(&mut db, "select k, count(*) from g where v > 15 group by k"),
        "RS:1|1;2|3"
    );
    assert_eq!(step(&mut db, "select max(s), min(s) from g"), "RS:c|a");
    assert_eq!(
        step(&mut db, "select max(v), min(v) from g where v is null"),
        "RS:<nil>|<nil>"
    );
    // ORDER BY / LIMIT / HAVING over the grouped output.
    assert_eq!(
        step(&mut db, "select k, count(*) from g group by k order by k"),
        "RS:1|2;2|3"
    );
    assert_eq!(
        step(
            &mut db,
            "select k, count(*) from g group by k order by count(*) desc"
        ),
        "RS:2|3;1|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select k, count(*) from g group by k having count(*) > 2"
        ),
        "RS:2|3"
    );
    assert_eq!(
        step(
            &mut db,
            "select k, sum(v) from g group by k having sum(v) > 100"
        ),
        "RS:2|120"
    );
    // ORDER BY / HAVING may reference a select-list alias (confirmed
    // via `gorun`) -- ordinary identifier resolution, not specific to
    // aggregation; `crate::order::resolve_alias`/`resolve_having_aliases`.
    assert_eq!(
        step(
            &mut db,
            "select k, count(*) c from g group by k order by c desc"
        ),
        "RS:2|3;1|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select k, count(*) c from g group by k having c > 2"
        ),
        "RS:2|3"
    );
    assert_eq!(
        step(
            &mut db,
            "select k, sum(v) s from g group by k having s > 100"
        ),
        "RS:2|120"
    );
    // GROUP BY may reference a select-list alias (confirmed via
    // `gorun`) -- resolved once via `crate::order::resolve_alias`,
    // the SAME whole-item resolution ORDER BY already uses. HAVING/
    // ORDER BY may then reference EITHER the alias or the underlying
    // real column name.
    assert_eq!(
        step(&mut db, "select k as x, count(*) from g group by x"),
        "RS:1|2;2|3"
    );
    assert_eq!(
        step(
            &mut db,
            "select k as x, count(*) from g group by x having k > 1"
        ),
        "RS:2|3"
    );
    assert_eq!(
        step(
            &mut db,
            "select k as x, count(*) from g group by x order by k desc"
        ),
        "RS:2|3;1|2"
    );
    // A GROUP BY item resolving to an AGGREGATE alias is a genuine
    // error -- grouping's own per-row `eval_in` naturally has no
    // notion of `Expr::Aggregate`, so no special rejection is needed.
    assert!(step(&mut db, "select k, count(*) as c from g group by c").starts_with("Eval("));
}

#[test]
fn avg() {
    let mut db = Database::new();
    step(&mut db, "create table avg_t (a int, b int)");
    step(&mut db, "insert into avg_t values (1, 10)");
    step(&mut db, "insert into avg_t values (2, 20)");
    step(&mut db, "insert into avg_t values (3, 30)");
    // AVG grows the sum's scale by 4 (MySQL's div_precision_increment).
    assert_eq!(step(&mut db, "select avg(a) from avg_t"), "RS:2.0000");
    assert_eq!(step(&mut db, "select avg(b) from avg_t"), "RS:20.0000");
    step(&mut db, "create table avg_wide (v bigint)");
    step(
        &mut db,
        "insert into avg_wide values (9223372036854775807), (1)",
    );
    assert_eq!(
        step(&mut db, "select avg(v) from avg_wide"),
        "RS:4611686018427387904.0000"
    );
    assert_eq!(
        step(&mut db, "select avg(a) from avg_t where a > 100"),
        "RS:<nil>"
    );
    assert_eq!(
        step(&mut db, "select avg(distinct a) from avg_t"),
        "RS:2.0000"
    );

    step(&mut db, "create table avg_d (v decimal(10,2))");
    step(&mut db, "insert into avg_d values (1.50)");
    step(&mut db, "insert into avg_d values (2.50)");
    step(&mut db, "insert into avg_d values (4.00)");
    assert_eq!(step(&mut db, "select avg(v) from avg_d"), "RS:2.666667");

    step(&mut db, "create table avg_d2 (v decimal(10,4))");
    step(&mut db, "insert into avg_d2 values (1.5000)");
    step(&mut db, "insert into avg_d2 values (2.5000)");
    assert_eq!(step(&mut db, "select avg(v) from avg_d2"), "RS:2.00000000");
}

#[test]
fn group_concat() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table gc (k int, a varchar(9), b varchar(9))",
    );
    step(&mut db, "insert into gc values (1, 'x', 'p')");
    step(&mut db, "insert into gc values (1, 'y', 'q')");
    step(&mut db, "insert into gc values (2, 'z', 'r')");
    assert_eq!(
        step(&mut db, "select group_concat(a) from gc group by k"),
        "RS:x,y;z"
    );
    assert_eq!(
        step(&mut db, "select group_concat(a, b) from gc group by k"),
        "RS:xp,yq;zr"
    );
    assert_eq!(
        step(
            &mut db,
            "select group_concat(a separator '-') from gc group by k"
        ),
        "RS:x-y;z"
    );
    assert_eq!(
        step(&mut db, "select group_concat(a) from gc where k > 100"),
        "RS:<nil>"
    );

    // A row with any NULL argument contributes nothing to the group.
    step(
        &mut db,
        "create table gc2 (k int, a varchar(9), b varchar(9))",
    );
    step(&mut db, "insert into gc2 values (1, 'x', NULL)");
    step(&mut db, "insert into gc2 values (1, 'y', 'q')");
    step(&mut db, "insert into gc2 values (2, NULL, NULL)");
    assert_eq!(
        step(&mut db, "select group_concat(a) from gc2 group by k"),
        "RS:<nil>;x,y"
    );
    assert_eq!(
        step(&mut db, "select group_concat(a, b) from gc2 group by k"),
        "RS:<nil>;yq"
    );
}

#[test]
fn count_distinct_multi_arg() {
    let mut db = Database::new();
    step(&mut db, "create table cd (a int, b int)");
    step(
        &mut db,
        "insert into cd values (1,1), (1,1), (1,2), (2,NULL), (NULL,1), (NULL,NULL)",
    );
    // Confirmed via `gorun`: a row is skipped entirely the instant ANY
    // listed column is NULL, so only (1,1)/(1,2) survive as distinct
    // tuples here — (2,NULL)/(NULL,1)/(NULL,NULL) are all excluded.
    assert_eq!(step(&mut db, "select count(distinct a, b) from cd"), "RS:2");
    assert_eq!(step(&mut db, "select count(distinct a) from cd"), "RS:2");
    assert_eq!(step(&mut db, "select count(distinct b) from cd"), "RS:2");

    step(&mut db, "create table cd2 (k int, a int, b int)");
    step(
        &mut db,
        "insert into cd2 values (1,1,1), (1,1,1), (1,1,2), (1,NULL,9), (2,3,3)",
    );
    assert_eq!(
        step(
            &mut db,
            "select k, count(distinct a, b) from cd2 group by k"
        ),
        "RS:1|2;2|1"
    );
    assert_eq!(
        step(
            &mut db,
            "select count(distinct a, b) from cd2 where k > 100"
        ),
        "RS:0"
    );
}

#[test]
fn aggregate_hidden_inside_function_call() {
    // Regression: `expr_has_aggregate` used to only recurse into a
    // narrow set of `Expr` variants, so an aggregate nested inside an
    // ordinary function call's argument (like `IF`'s second argument)
    // was never recognized as making the query aggregating at all --
    // `select_rows` then took the WRONG (row-wise) path, and `eval_in`
    // failed outright on the literal `Expr::Aggregate` node it doesn't
    // understand, instead of returning the correct single-group result
    // real TiDB does (confirmed via `gorun` before fixing).
    let mut db = Database::new();
    step(&mut db, "create table t (id int, v int)");
    step(&mut db, "insert into t values (1, 10), (1, 20), (2, 30)");
    assert_eq!(step(&mut db, "select if(1=1, count(*), 0) from t"), "RS:3");
    // A nested subquery's OWN aggregate does NOT count -- it belongs
    // to the subquery's own scope, so this stays a per-row query.
    step(&mut db, "create table t2 (x int)");
    step(&mut db, "insert into t2 values (1),(2),(3)");
    assert_eq!(
        step(&mut db, "select id, (select count(*) from t2) from t"),
        "RS:1|3;1|3;2|3"
    );

    // The SAME class of gap existed one layer deeper in `eval_group`
    // itself (not just the `has_aggregate` detection `select_rows`
    // uses): `HAVING COUNT(*) BETWEEN ...`/`IN (...)`/`IS NOT NULL`,
    // and the outer-scope operand of a comparison/`IN` subquery, are
    // all genuinely common patterns that used to fail the same way --
    // confirmed via `gorun` for each before fixing.
    step(&mut db, "insert into t values (2, 40)");
    assert_eq!(
        step(
            &mut db,
            "select id, count(*) from t group by id having count(*) between 1 and 5"
        ),
        "RS:1|2;2|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, count(*) from t group by id having count(*) in (2)"
        ),
        "RS:1|2;2|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, count(*) from t group by id having count(*) is not null"
        ),
        "RS:1|2;2|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, count(*) from t group by id having count(*) > any (select 1)"
        ),
        "RS:1|2;2|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, count(*) from t group by id having count(*) in (select 2)"
        ),
        "RS:1|2;2|2"
    );
}

/// `GROUP_CONCAT`'s own `ORDER BY` sorts the group's rows before
/// concatenating — see `Database::group_concat_order`'s own doc.
/// Confirmed via `gorun`.
#[test]
fn group_concat_order_by_eval() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table gcob1 (dept varchar(10), name varchar(10))",
    );
    step(
        &mut db,
        "insert into gcob1 values ('a','carl'), ('a','bob'), ('a','ann'), ('b','x')",
    );
    // The no-`ORDER BY` form stays unaffected (scan order).
    assert_eq!(
        step(
            &mut db,
            "select dept, group_concat(name) from gcob1 group by dept"
        ),
        "RS:a|carl,bob,ann;b|x"
    );
    assert_eq!(
        step(
            &mut db,
            "select dept, group_concat(name order by name) from gcob1 group by dept"
        ),
        "RS:a|ann,bob,carl;b|x"
    );
    assert_eq!(
        step(
            &mut db,
            "select dept, group_concat(name order by name desc) from gcob1 group by dept"
        ),
        "RS:a|carl,bob,ann;b|x"
    );
    // A positional `ORDER BY` item resolves against `GROUP_CONCAT`'s
    // OWN arg list (here, arg 1 = `name`), not the outer select list.
    assert_eq!(
        step(
            &mut db,
            "select dept, group_concat(name, dept order by 1) from gcob1 group by dept"
        ),
        "RS:a|anna,boba,carla;b|xb"
    );
    // Composes with a custom separator and a multi-key ORDER BY.
    assert_eq!(
        step(
            &mut db,
            "select dept, group_concat(name order by name separator '-') from gcob1 group by dept"
        ),
        "RS:a|ann-bob-carl;b|x"
    );
    assert_eq!(
        step(
            &mut db,
            "select dept, group_concat(name order by length(name), name) from gcob1 group by dept"
        ),
        "RS:a|ann,bob,carl;b|x"
    );
    // `DISTINCT` dedupes in the SORTED order, keeping each value's first
    // (post-sort) occurrence.
    step(&mut db, "create table gcob2 (v int)");
    step(&mut db, "insert into gcob2 values (3),(1),(2),(1),(3)");
    assert_eq!(
        step(
            &mut db,
            "select group_concat(distinct v order by v desc) from gcob2"
        ),
        "RS:3,2,1"
    );
}

/// Go checks the complete evaluated argument tuple before rendering it.
/// Distinct tuples that concatenate to the same bytes must both survive.
#[test]
fn group_concat_distinct_preserves_argument_tuple_boundaries() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table gc_distinct_tuple (a varchar(8), b varchar(8))",
    );
    step(
        &mut db,
        "insert into gc_distinct_tuple values ('ab', 'c'), ('a', 'bc')",
    );
    assert_eq!(
        step(
            &mut db,
            "select group_concat(distinct a, b) from gc_distinct_tuple"
        ),
        "RS:abc,abc"
    );
}

/// `WITH ROLLUP` is ALWAYS `Unsupported` at execution time, unconditionally
/// — see `tidb_ast::SelectStmt::rollup`'s own doc for why (a real,
/// multi-level semantic effect this crate deliberately does not
/// replicate). The no-`ROLLUP` form stays fully evaluated, unaffected.
#[test]
fn group_by_with_rollup_rejected() {
    let mut db = Database::new();
    step(&mut db, "create table gbwr1 (a int, b int)");
    step(&mut db, "insert into gbwr1 values (1,10), (1,20), (2,30)");
    assert_eq!(
        step(&mut db, "select a, sum(b) from gbwr1 group by a"),
        "RS:1|30;2|30"
    );
    assert!(step(
        &mut db,
        "select a, sum(b) from gbwr1 group by a with rollup"
    )
    .starts_with("Unsupported("));
}

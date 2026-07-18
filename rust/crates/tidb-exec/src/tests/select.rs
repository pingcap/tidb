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

//! `SELECT` execution tests: scans, joins, subqueries,
//! derived tables, `GROUP BY` shapes, and the
//! per-table clauses that are honestly rejected.

use super::*;

#[test]
fn projection() {
    assert_eq!(run("select 1, 2, 3"), "RS:1|2|3");
    assert_eq!(run("select 1 + 1, 'x', NULL"), "RS:2|x|<nil>");
    assert_eq!(run("select abs(-5), concat('a', 'b')"), "RS:5|ab");
}

#[test]
fn selection() {
    assert_eq!(run("select 5 where 1 = 1"), "RS:5");
    assert_eq!(run("select 5 where 1 = 0"), "RS:");
    assert_eq!(run("select 5 where NULL"), "RS:");
    assert_eq!(run("select 10, 20 where 3 > 2"), "RS:10|20");
}

#[test]
fn from_dual() {
    assert_eq!(run("select 1 from dual"), "RS:1");
    assert_eq!(run("select 1 from dual where 0"), "RS:");
}

#[test]
fn table_statement_executes_the_shared_wildcard_table_shape() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table table_statement_rows (id int, value int)",
    );
    step(
        &mut db,
        "insert into table_statement_rows values (2, 20), (1, 10)",
    );
    assert_eq!(
        step(&mut db, "table table_statement_rows order by id limit 1"),
        "RS:1|10"
    );
}

#[test]
fn values_statement_is_rejected_before_select_execution() {
    assert_eq!(run("values row(1), row(2)"), "Unsupported(\"VALUES\")");
}

#[test]
fn show_collation_is_explicitly_unsupported() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "show collation like 'utf8%'"),
        "Unsupported(\"SHOW COLLATION\")"
    );
}

#[test]
fn requires_table_is_rejected() {
    assert!(matches!(
        {
            let stmt = tidb_parser::parse("select a from t").unwrap();
            execute(&stmt)
        },
        Err(ExecError::RequiresTable)
    ));
}

#[test]
fn table_scan() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table t (a int, b int, c varchar(9))"),
        "OK"
    );
    assert_eq!(step(&mut db, "insert into t values (1, 10, 'x')"), "OK");
    assert_eq!(step(&mut db, "insert into t values (2, 20, 'y')"), "OK");
    // Projection + column references.
    assert_eq!(step(&mut db, "select a, b, c from t"), "RS:1|10|x;2|20|y");
    assert_eq!(step(&mut db, "select * from t"), "RS:1|10|x;2|20|y");
    // Selection with a numeric predicate.
    assert_eq!(step(&mut db, "select a from t where a > 1"), "RS:2");
    // Selection with a string predicate.
    assert_eq!(step(&mut db, "select b from t where c = 'y'"), "RS:20");
    // Computed projection over columns.
    assert_eq!(step(&mut db, "select a + b from t"), "RS:11;22");
    // Set operation over table rows.
    assert_eq!(
        step(&mut db, "select a from t union select a from t"),
        "RS:1;2"
    );
    // ORDER BY may reference a select-list alias (confirmed via
    // `gorun`), row-wise (no GROUP BY/aggregate) same as grouped --
    // `crate::order::resolve_alias`.
    assert_eq!(
        step(&mut db, "select a as x from t order by x desc"),
        "RS:2;1"
    );
}

#[test]
fn joins() {
    let mut db = Database::new();
    step(&mut db, "create table dept (id int, name varchar(9))");
    step(
        &mut db,
        "insert into dept values (1, 'eng'), (2, 'sales'), (3, 'ops')",
    );
    step(
        &mut db,
        "create table emp (id int, dept_id int, name varchar(9))",
    );
    step(
        &mut db,
        "insert into emp values (10, 1, 'ann'), (11, 1, 'bob'), (12, 2, 'cid'), (13, 9, 'dan')",
    );
    assert_eq!(
        step(
            &mut db,
            "select emp.name, dept.name from emp join dept on emp.dept_id = dept.id"
        ),
        "RS:ann|eng;bob|eng;cid|sales"
    );
    assert_eq!(
        step(
            &mut db,
            "select emp.name, dept.name from emp left join dept on emp.dept_id = dept.id"
        ),
        "RS:ann|eng;bob|eng;cid|sales;dan|<nil>"
    );
    assert_eq!(
        step(
            &mut db,
            "select e.name, d.name from emp e right join dept d on e.dept_id = d.id"
        ),
        "RS:<nil>|ops;ann|eng;bob|eng;cid|sales"
    );
    assert_eq!(
        step(
            &mut db,
            "select dept.name, count(*) from emp join dept on emp.dept_id = dept.id group by dept.name"
        ),
        "RS:eng|2;sales|1"
    );
    assert_eq!(
        step(
            &mut db,
            "select emp.dept_id, dept.id from emp cross join dept where emp.dept_id = dept.id"
        ),
        "RS:1|1;1|1;2|2"
    );
}

#[test]
fn three_table_joins() {
    let mut db = Database::new();
    step(&mut db, "create table x1 (id int, v varchar(9))");
    step(&mut db, "create table x2 (id int, x1_id int, v varchar(9))");
    step(&mut db, "create table x3 (id int, x2_id int, v varchar(9))");
    step(&mut db, "insert into x1 values (1, 'a'), (2, 'b')");
    step(
        &mut db,
        "insert into x2 values (1, 1, 'p'), (2, 2, 'q'), (3, 9, 'r')",
    );
    step(&mut db, "insert into x3 values (1, 1, 'm')");
    assert_eq!(
        step(
            &mut db,
            "select x1.v, x2.v, x3.v from x1 join x2 on x2.x1_id = x1.id join x3 on x3.x2_id = x2.id"
        ),
        "RS:a|p|m"
    );
    assert_eq!(
        step(
            &mut db,
            "select x1.v, x2.v, x3.v from x1 join x2 on x2.x1_id = x1.id left join x3 on x3.x2_id = x2.id"
        ),
        "RS:a|p|m;b|q|<nil>"
    );
    assert_eq!(
        step(
            &mut db,
            "select count(*) from x1 join x2 on x2.x1_id = x1.id join x3 on x3.x2_id = x2.id"
        ),
        "RS:1"
    );
}

#[test]
fn using_joins() {
    let mut db = Database::new();
    step(&mut db, "create table u1 (id int, v varchar(9))");
    step(&mut db, "create table u2 (id int, w varchar(9))");
    step(
        &mut db,
        "insert into u1 values (1, 'a'), (2, 'b'), (3, 'c')",
    );
    step(
        &mut db,
        "insert into u2 values (1, 'x'), (2, 'y'), (9, 'z')",
    );
    // A USING column coalesces into one physical column, shown once by `*`.
    assert_eq!(
        step(&mut db, "select * from u1 join u2 using (id)"),
        "RS:1|a|x;2|b|y"
    );
    assert_eq!(
        step(&mut db, "select u1.id, u2.id from u1 join u2 using (id)"),
        "RS:1|1;2|2"
    );
    assert_eq!(
        step(&mut db, "select * from u1 left join u2 using (id)"),
        "RS:1|a|x;2|b|y;3|c|<nil>"
    );
    // RIGHT JOIN USING swaps the remaining-column order (verified against
    // real TiDB, which effectively rewrites A RIGHT JOIN B USING(x) as
    // B LEFT JOIN A USING(x)).
    assert_eq!(
        step(&mut db, "select * from u1 right join u2 using (id)"),
        "RS:1|x|a;2|y|b;9|z|<nil>"
    );
    assert_eq!(
        step(&mut db, "select count(*) from u1 join u2 using (id)"),
        "RS:2"
    );
}

/// The USING list's spelling order is not the result-schema order. TiDB puts
/// all coalesced common fields first in left-child declaration order, then the
/// remaining left/right fields. Keep the row executor aligned with the
/// planner-owned `result_schema_join_output` metadata leaf.
#[test]
fn using_join_output_follows_left_declaration_order() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table using_left (z int, id int, left_only int)",
    );
    step(
        &mut db,
        "create table using_right (id int, z int, right_only int)",
    );
    step(&mut db, "insert into using_left values (10, 1, 100)");
    step(&mut db, "insert into using_right values (1, 10, 200)");

    // The SQL names common fields as (id, z), but the visible result starts
    // with z then id because that is the order in using_left's schema.
    assert_eq!(
        step(
            &mut db,
            "select * from using_left join using_right using (id, z)"
        ),
        "RS:10|1|100|200"
    );
}

#[test]
fn subqueries_and_predicates() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int, b int, c varchar(20))");
    step(&mut db, "insert into t values (1, 10, 'x')");
    step(&mut db, "insert into t values (2, 20, 'y')");
    step(&mut db, "insert into t values (3, 30, 'z')");
    step(&mut db, "create table dept (id int, name varchar(9))");
    step(
        &mut db,
        "insert into dept values (1, 'eng'), (2, 'sales'), (3, 'ops')",
    );
    step(
        &mut db,
        "create table emp (id int, dept_id int, name varchar(9))",
    );
    step(
        &mut db,
        "insert into emp values (10, 1, 'ann'), (11, 1, 'bob'), (12, 2, 'cid'), (13, 9, 'dan')",
    );

    // IN / BETWEEN / IS NULL predicates.
    assert_eq!(step(&mut db, "select a from t where a in (1, 3)"), "RS:1;3");
    assert_eq!(
        step(&mut db, "select a from t where a between 2 and 3"),
        "RS:2;3"
    );
    assert_eq!(
        step(
            &mut db,
            "select emp.name from emp left join dept on emp.dept_id = dept.id where dept.id is null"
        ),
        "RS:dan"
    );

    // Uncorrelated scalar / IN / EXISTS subqueries.
    assert_eq!(
        step(
            &mut db,
            "select a from t where a = (select max(id) from dept)"
        ),
        "RS:3"
    );
    assert_eq!(
        step(
            &mut db,
            "select a from t where a > (select min(id) from dept)"
        ),
        "RS:2;3"
    );
    assert_eq!(
        step(
            &mut db,
            "select name from dept where id in (select dept_id from emp)"
        ),
        "RS:eng;sales"
    );
    assert_eq!(
        step(&mut db, "select a from t where exists (select 1 from dept)"),
        "RS:1;2;3"
    );

    // Correlated subqueries.
    assert_eq!(
        step(
            &mut db,
            "select name from dept where exists (select 1 from emp where emp.dept_id = dept.id)"
        ),
        "RS:eng;sales"
    );
    assert_eq!(
        step(
            &mut db,
            "select name from emp where id = (select max(id) from emp e2 where e2.dept_id = emp.dept_id)"
        ),
        "RS:bob;cid;dan"
    );
    assert_eq!(
        step(
            &mut db,
            "select dept.name from dept where (select count(*) from emp where emp.dept_id = dept.id) > 1"
        ),
        "RS:eng"
    );

    // ANY / ALL subqueries, including the vacuous-empty-subquery cases.
    assert_eq!(
        step(
            &mut db,
            "select a from t where a > any (select id from dept)"
        ),
        "RS:2;3"
    );
    assert_eq!(
        step(
            &mut db,
            "select a from t where a > any (select id from dept where id > 100)"
        ),
        "RS:"
    );

    // Subqueries in projection, HAVING, and an aggregate's own argument.
    assert_eq!(
        step(&mut db, "select a, (select max(id) from dept) from t"),
        "RS:1|3;2|3;3|3"
    );
    assert_eq!(
        step(&mut db, "select sum((select max(id) from dept)) from t"),
        "RS:9"
    );
}

#[test]
fn in_subquery_union_body() {
    // `IN`'s own subquery may be `UNION`-bodied, confirmed via `gorun`.
    let mut db = Database::new();
    step(&mut db, "create table t (a int)");
    step(&mut db, "insert into t values (1), (2), (3)");
    assert_eq!(
        step(
            &mut db,
            "select a from t where a in (select 1 union select 2) order by a"
        ),
        "RS:1;2"
    );
    assert_eq!(
        step(
            &mut db,
            "select a from t where a not in (select 1 union select 2) order by a"
        ),
        "RS:3"
    );
}

/// `ORDER BY <alias>` where the aliased select-list expression itself
/// contains an `IN (subquery)` — a bug found while implementing row-value
/// `IN (subquery)` (`row_in_subquery_eval`): the row-wise `ORDER BY`
/// key-evaluation path evaluated the resolved-alias expression directly,
/// without first calling `Database::resolve_subqueries` on it (unlike the
/// `WHERE`/select-list/`HAVING` paths, which all do). This meant ANY
/// `ORDER BY`-by-alias whose expression contained a subquery — scalar or
/// row-value — hit `eval_in`'s generic `Unsupported("unsupported
/// expression")` wildcard on the still-unresolved `Expr::InSubquery` node.
/// Fixed in `crate::select`'s row-wise `ORDER BY` sort-key loop.
#[test]
fn order_by_alias_with_in_subquery() {
    let mut db = Database::new();
    step(&mut db, "create table oas (a int)");
    step(&mut db, "insert into oas values (3), (9)");
    assert_eq!(
        step(
            &mut db,
            "select a not in (select 3 union select 5) as field2 from oas order by field2"
        ),
        "RS:0;1"
    );
}

#[test]
fn derived_tables() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int, b int, c varchar(20))");
    step(&mut db, "insert into t values (1, 10, 'x')");
    step(&mut db, "insert into t values (2, 20, 'y')");
    step(&mut db, "insert into t values (3, 30, 'z')");
    step(&mut db, "create table g (k int, v int, s varchar(9))");
    step(&mut db, "insert into g values (1, 10, 'a')");
    step(&mut db, "insert into g values (1, 20, 'b')");
    step(&mut db, "insert into g values (2, 30, 'a')");
    step(&mut db, "insert into g values (2, 40, 'c')");
    step(&mut db, "insert into g values (2, 50, 'a')");

    assert_eq!(
        step(
            &mut db,
            "select gs.k, gs.total from (select k, sum(v) as total from g group by k) as gs where gs.total > 50"
        ),
        "RS:2|120"
    );
    assert_eq!(
        step(
            &mut db,
            "select dt.a from (select a from t where a > 1) as dt"
        ),
        "RS:2;3"
    );
    assert_eq!(
        step(
            &mut db,
            "select count(*) from (select k from g group by k) as gcount"
        ),
        "RS:2"
    );
    // `*` inside a derived table's own select list.
    assert_eq!(
        step(&mut db, "select * from (select * from t) dt"),
        "RS:1|10|x;2|20|y;3|30|z"
    );
}

/// Parentheses around a join whose first factor is a derived table remain a
/// structural parser grouping. The existing typed join executor must receive
/// the same relation tree, rather than a fabricated special Plan/EXPLAIN path.
#[test]
fn parenthesized_derived_join_group_executes() {
    let mut db = Database::new();
    step(&mut db, "create table pdj_a (id int)");
    step(&mut db, "create table pdj_b (id int)");
    step(&mut db, "create table pdj_c (id int)");
    step(&mut db, "insert into pdj_a values (1), (2)");
    step(&mut db, "insert into pdj_b values (1), (3)");
    step(&mut db, "insert into pdj_c values (1), (4)");

    assert_eq!(
        step(
            &mut db,
            "select d.id from ((select id from pdj_a) d join pdj_b on d.id=pdj_b.id) join pdj_c on pdj_b.id=pdj_c.id"
        ),
        "RS:1"
    );
}

#[test]
fn qualified_wildcards() {
    let mut db = Database::new();
    step(&mut db, "create table dept (id int, name varchar(9))");
    step(
        &mut db,
        "insert into dept values (1, 'eng'), (2, 'sales'), (3, 'ops')",
    );
    step(
        &mut db,
        "create table emp (id int, dept_id int, name varchar(9))",
    );
    step(
        &mut db,
        "insert into emp values (10, 1, 'ann'), (11, 1, 'bob'), (12, 2, 'cid'), (13, 9, 'dan')",
    );
    assert_eq!(
        step(
            &mut db,
            "select emp.* from emp join dept on emp.dept_id = dept.id"
        ),
        "RS:10|1|ann;11|1|bob;12|2|cid"
    );
    assert_eq!(
        step(
            &mut db,
            "select x.* from (select emp.* from emp join dept on emp.dept_id = dept.id) x"
        ),
        "RS:10|1|ann;11|1|bob;12|2|cid"
    );
}

/// A derived table's own body may be a `UNION`/`UNION ALL`-joined set
/// operation (`(SELECT ... UNION [ALL] SELECT ...) alias`), executed via
/// the SAME `Database::setopr_scoped` an ordinary top-level `UNION`
/// statement already uses — see `tidb_ast::JoinNode::Derived`'s own doc.
/// Every row count/value here cross-checked against `gorun`.
#[test]
fn derived_table_set_op() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int, b int)");
    step(&mut db, "create table tv (a int)");
    step(&mut db, "insert into t values (1, 10), (2, 20)");
    step(&mut db, "insert into tv values (3), (4)");
    assert_eq!(
        step(
            &mut db,
            "select * from (select a from t union all select a from tv) t1 order by a"
        ),
        "RS:1;2;3;4"
    );
    assert_eq!(
        step(
            &mut db,
            "select avg(a) from (select a from t union all select a from tv) t"
        ),
        "RS:2.5000"
    );
    // `UNION` (no `ALL`) dedupes, unlike `UNION ALL`.
    assert_eq!(
        step(
            &mut db,
            "select a, count(*) from (select a from t union all select a from t) k group by a order by a"
        ),
        "RS:1|2;2|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select count(*) from (select a from t union select a from t) k"
        ),
        "RS:2"
    );
    // Column naming follows the FIRST term, matching real MySQL/TiDB.
    assert_eq!(
        step(
            &mut db,
            "select x from (select a as x from t union all select b from t) s order by x"
        ),
        "RS:1;2;10;20"
    );
}

/// A derived table's alias is grammatically OPTIONAL (confirmed via
/// `gorun`), but EXECUTING one still needs a name to tag its output
/// columns with for qualified-reference resolution — deliberately
/// `Unsupported`, the SAME "parses fine, execution is a narrower scope
/// cut" precedent already applied to `LATERAL`/`TABLESAMPLE`.
#[test]
fn derived_table_no_alias_is_unsupported() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int)");
    step(&mut db, "insert into t values (1), (2)");
    assert!(step(&mut db, "select * from (select a from t)").starts_with("Unsupported("));
}

/// A parenthesized NESTED set-operation TERM (`t1 UNION (t2 UNION ALL
/// t3)`) inside a derived table folds correctly, exactly like the
/// table-less version in `nested_set_op_term` above.
#[test]
fn derived_table_nested_set_op_term() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int)");
    step(&mut db, "insert into t values (1), (2)");
    // The outer `UNION` (no `ALL`) dedupes `t`'s own rows {1, 2} against
    // the nested group's `{1, 2, 3}`, leaving the unique set {1, 2, 3}.
    assert_eq!(
        step(
            &mut db,
            "select a from (select a from t union (select a from t union all select 3)) s order by a"
        ),
        "RS:1;2;3"
    );
    // The nested group's own `LIMIT` (with no `ORDER BY` of its own)
    // caps just that group's own output, in EXECUTION order, before the
    // outer `UNION ALL` folds it in — one row (`2`) from the nested
    // `2 UNION ALL 3 LIMIT 1` group, plus `t`'s own two rows.
    assert_eq!(
        step(
            &mut db,
            "select a from (select a from t union all (select 2 union all select 3 limit 1)) s order by a"
        ),
        "RS:1;2;2"
    );
}

#[test]
fn into_outfile_rejected() {
    // Real TiDB does not return a normal result set for this statement
    // at all (confirmed via `gorun`: a bare `OK`, no rows) — this crate
    // has no filesystem to write to, so it is `Unsupported` rather than
    // silently returning the ordinary projected rows.
    assert_eq!(
        run("select 1 into outfile '/tmp/doesntmatter-no-permissions'"),
        "Unsupported(\"INTO OUTFILE clause\")"
    );
    let mut db = Database::new();
    step(&mut db, "create table t (a int)");
    step(&mut db, "insert into t values (1)");
    assert!(step(&mut db, "select a from t into outfile '/tmp/x'").starts_with("Unsupported("));
    // A plain SELECT with no INTO OUTFILE is unaffected.
    assert_eq!(step(&mut db, "select a from t"), "RS:1");
}

#[test]
fn only_full_group_by() {
    let mut db = Database::new();
    step(&mut db, "create table t (id int, v int)");
    step(&mut db, "insert into t values (1, 10), (1, 20), (2, 30)");

    // A non-aggregated column with no GROUP BY at all is an error --
    // this executor used to silently accept it (picking an arbitrary
    // row's value), a confirmed real divergence from TiDB.
    assert!(step(&mut db, "select v, count(*) from t").starts_with("UngroupedColumn"));
    assert!(step(&mut db, "select id, count(*) from t").starts_with("UngroupedColumn"));
    // But a query with only aggregates/constants (no bare column at
    // all) is fine with no GROUP BY.
    assert_eq!(step(&mut db, "select count(*), 1+1 from t"), "RS:3|2");

    // `v` is not in GROUP BY -- a bare selected column, a HAVING
    // reference, and an ORDER BY reference are all rejected the same
    // way.
    assert!(
        step(&mut db, "select id, v, count(*) from t group by id").starts_with("UngroupedColumn")
    );
    assert!(step(
        &mut db,
        "select id, count(*) from t group by id having v > 0"
    )
    .starts_with("UngroupedColumn"));
    assert!(step(&mut db, "select id from t group by id order by v").starts_with("UngroupedColumn"));
    assert_eq!(
        step(&mut db, "select id from t group by id order by id"),
        "RS:1;2"
    );

    // A non-aggregated column is fine when it's built ENTIRELY from
    // pinned (bare `GROUP BY`) columns, however the containing
    // expression is shaped.
    assert_eq!(
        step(&mut db, "select id+1, count(*) from t group by id"),
        "RS:2|2;3|1"
    );
    assert_eq!(
        step(&mut db, "select t.id, count(*) from t group by id"),
        "RS:1|2;2|1"
    );

    // But this is a purely SYNTACTIC (column-name) check, not true
    // functional-dependency reasoning: a bare `id` is NOT safe under
    // `GROUP BY id+1`, even though `id+1` is a bijective function of
    // `id` -- confirmed via `gorun`, matching real TiDB exactly.
    assert!(
        step(&mut db, "select id, count(*) from t group by id+1").starts_with("UngroupedColumn")
    );
    // The exact-match rule only applies at the TOP LEVEL of a checked
    // expression, not recursively at every nesting depth: `id+1+1`
    // contains `id+1` nested inside it, but that doesn't make the
    // whole expression safe under `GROUP BY id+1`.
    assert!(
        step(&mut db, "select id+1+1, count(*) from t group by id+1")
            .starts_with("UngroupedColumn")
    );
    // The whole expression DOES match when it's the exact GROUP BY
    // expression itself.
    assert_eq!(
        step(&mut db, "select id+1, count(*) from t group by id+1"),
        "RS:2|2;3|1"
    );

    // A scalar subquery's own column reference is a separate scope,
    // exempt from this check even when it reads an ungrouped column
    // (here via a self-join correlated subquery, not a bare no-FROM
    // correlated reference -- that shape has its own pre-existing,
    // unrelated resolution gap, not something this change touches).
    assert_eq!(
        step(
            &mut db,
            "select id, (select v from t t2 where t2.id = t.id limit 1) as x, count(*) from t group by id order by id"
        ),
        "RS:1|10|2;2|30|1"
    );
    // But the OUTER-scope operand of an `IN`/comparison subquery is
    // still checked, since it's evaluated in this query's own scope.
    assert!(step(
        &mut db,
        "select id, count(*) from t group by id having v in (select 1)"
    )
    .starts_with("UngroupedColumn"));
}

/// `GROUP BY expr [ASC|DESC]`: real MySQL/TiDB rejects ANY explicit
/// direction at EXECUTION time by default (confirmed via `gorun`:
/// `[expression:1235] function GROUP BY expr ASC|DESC has only noop
/// implementation in tidb now, use tidb_enable_noop_functions to enable
/// these functions`). With that session switch ON, the direction is accepted
/// as TiDB's compatibility no-op; otherwise an explicit `ASC` is rejected
/// exactly like `DESC`, even though it restores identically to no direction
/// (see `tidb_ast::GroupByItem`'s own doc).
#[test]
fn group_by_direction() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int, b int)");
    step(&mut db, "insert into t values (3, 1), (1, 2), (2, 3)");

    assert_eq!(step(&mut db, "select a from t group by a"), "RS:1;2;3");
    assert_eq!(
        step(&mut db, "select a from t group by a asc"),
        "Unsupported(\"GROUP BY expr ASC|DESC\")"
    );
    assert_eq!(
        step(&mut db, "select a from t group by a desc"),
        "Unsupported(\"GROUP BY expr ASC|DESC\")"
    );
    assert_eq!(step(&mut db, "set tidb_enable_noop_functions = on"), "OK");
    assert_eq!(step(&mut db, "select a from t group by a asc"), "RS:1;2;3");
    assert_eq!(step(&mut db, "select a from t group by a desc"), "RS:1;2;3");
}

/// `USE`/`FORCE`/`IGNORE INDEX` hints parse and execute with NO effect
/// on results (confirmed via `gorun`: an empty hint list, meaning "use
/// no index at all," scans normally) — a full-table-scan executor has
/// no access-path choice for a hint to influence either way. A KNOWN,
/// documented divergence from real TiDB, deliberately NOT replicated
/// here: real MySQL/TiDB validates the hinted NAME actually exists on
/// the table, erroring `Key '...' doesn't exist in table '...'`
/// otherwise (confirmed via `gorun`); this crate's `Table` doesn't track
/// index names at all, so ANY name — including one that names no real
/// index — is silently accepted (see `tidb_ast::TableRef::hints`'s own
/// doc for why).
#[test]
fn index_hints_no_effect() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int, b int)");
    step(&mut db, "insert into t values (1, 10), (2, 20), (3, 30)");
    assert_eq!(
        step(&mut db, "select * from t use index ()"),
        "RS:1|10;2|20;3|30"
    );
    assert_eq!(
        step(
            &mut db,
            "select a, b from t as x use index (idx1) where a > 1"
        ),
        "RS:2|20;3|30"
    );
}

/// `PARTITION (...)` is ALWAYS `Unsupported` at execution, unconditionally,
/// across `SELECT`/`UPDATE`/`DELETE`/`INSERT` alike: this crate never
/// implements `CREATE TABLE ... PARTITION BY` at all, so every table is
/// permanently "non-partitioned," and real MySQL/TiDB's own error for a
/// `PARTITION` clause there (`PARTITION () clause on non partitioned
/// table`, confirmed via `gorun`) applies universally here — no
/// per-table validation needed the way an index hint's own name would
/// (see `tidb_ast::TableRef::partitions`'s own doc).
#[test]
fn partition_hint_always_unsupported() {
    let mut db = Database::new();
    step(&mut db, "create table t (a int, b int)");
    step(&mut db, "insert into t values (1, 10), (2, 20)");
    let want = "Unsupported(\"PARTITION clause on non-partitioned table\")";
    assert_eq!(step(&mut db, "select * from t partition (p0)"), want);
    assert_eq!(step(&mut db, "update t partition (p0) set a = 5"), want);
    assert_eq!(
        step(&mut db, "delete from t partition (p0) where a = 1"),
        want
    );
    assert_eq!(
        step(&mut db, "insert into t partition (p0) values (3, 30)"),
        want
    );
}

/// `NATURAL [LEFT|RIGHT] JOIN`: exactly `JOIN ... USING (<every column
/// name common to both sides>)`, confirmed via `gorun` — coalesced
/// columns (a common column appears ONCE in the output, addressable
/// unqualified or via either side's qualifier), `LEFT`/`RIGHT` outer-join
/// `NULL`-padding, and the same `LEFT`/`RIGHT` column-order swap an
/// explicit `USING` join already has. Zero common columns degenerates to
/// a plain cross join (the full cartesian product); multiple common
/// columns are ordered as they appear in the LEFT side's own columns
/// (not the right side's, not alphabetically).
#[test]
fn natural_join() {
    let mut db = Database::new();
    step(&mut db, "create table t1 (a int, b int)");
    step(&mut db, "create table t2 (a int, c int)");
    step(&mut db, "insert into t1 values (1, 10), (2, 20)");
    step(&mut db, "insert into t2 values (1, 100), (3, 300)");
    assert_eq!(
        step(&mut db, "select * from t1 natural join t2"),
        "RS:1|10|100"
    );
    assert_eq!(
        step(&mut db, "select t1.a, b, c from t1 natural join t2"),
        "RS:1|10|100"
    );
    assert_eq!(
        step(&mut db, "select * from t1 natural left join t2"),
        "RS:1|10|100;2|20|<nil>"
    );
    assert_eq!(
        step(&mut db, "select * from t1 natural right join t2"),
        "RS:1|100|10;3|300|<nil>"
    );

    step(&mut db, "create table t3 (x int, y int)");
    step(&mut db, "create table t4 (p int, q int)");
    step(&mut db, "insert into t3 values (1, 2)");
    step(&mut db, "insert into t4 values (3, 4)");
    assert_eq!(
        step(&mut db, "select * from t3 natural join t4"),
        "RS:1|2|3|4"
    );

    step(&mut db, "create table t5 (c int, a int, b int)");
    step(&mut db, "create table t6 (b int, a int, d int)");
    step(&mut db, "insert into t5 values (100, 1, 2)");
    step(&mut db, "insert into t6 values (2, 1, 200)");
    assert_eq!(
        step(&mut db, "select * from t5 natural join t6"),
        "RS:1|2|100|200"
    );
    // RIGHT mirrors the planner's coalescing inputs first, so common fields
    // follow t6's declaration order (b,a), then t6 remainder and t5 remainder.
    assert_eq!(
        step(&mut db, "select * from t5 natural right join t6"),
        "RS:2|1|200|100"
    );
}

/// Regression: `GROUP BY <positive integer literal>` (e.g. `GROUP BY 1`) — a
/// positional reference to the corresponding select-list column, the SAME
/// feature `ORDER BY` already implemented (`crate::order::positional`) —
/// was entirely unimplemented for `GROUP BY`: `aggregate.rs`'s own
/// group-key resolution only ever called `resolve_alias`, never checking
/// for a positional item, so `GROUP BY 1` was treated as grouping by the
/// literal constant `1` (collapsing every row into one group) instead of by
/// the first select-list column — confirmed against real TiDB via `gorun`
/// that `GROUP BY 1` and `GROUP BY true` (see below) both group per-column,
/// not into a single group. Fixed by generalizing `order.rs`'s own
/// `positional`+`resolve_alias` pair into a single `resolve_by_item` helper
/// shared by both `ORDER BY` and `GROUP BY`.
#[test]
fn group_by_position() {
    let mut db = Database::new();
    step(&mut db, "create table gbp1 (a int, b int)");
    step(&mut db, "insert into gbp1 values (1,10), (2,20), (1,30)");
    assert_eq!(
        step(&mut db, "select a, count(*) from gbp1 group by 1"),
        "RS:1|2;2|1"
    );
    assert_eq!(
        step(&mut db, "select a, b, count(*) from gbp1 group by 1, 2"),
        "RS:1|10|1;1|30|1;2|20|1"
    );
}

/// Regression, same root cause as [`group_by_position`]: MySQL/TiDB treats
/// a bare `TRUE`/`FALSE` literal in `GROUP BY`/`ORDER BY` position as its
/// integer value (`TRUE` == `1`, `FALSE` == `0`) — confirmed via `gorun`
/// that `GROUP BY true` groups exactly like `GROUP BY 1`, not as a
/// constant-`TRUE` single-group collapse. `order.rs`'s `positional` now
/// accepts `Expr::Bool` alongside `Expr::Int`. Position `0` (`FALSE`, or a
/// literal `0`) is a genuine runtime error, same as `ORDER BY 0`.
#[test]
fn group_by_position_boolean_literal() {
    let mut db = Database::new();
    step(&mut db, "create table gbp2 (a int, b int)");
    step(&mut db, "insert into gbp2 values (1,10), (2,20), (1,30)");
    assert_eq!(
        step(&mut db, "select a, count(*) from gbp2 group by true"),
        "RS:1|2;2|1"
    );
    assert!(
        step(&mut db, "select a, count(*) from gbp2 group by false").starts_with("Unsupported(")
    );
    assert!(step(&mut db, "select a from gbp2 order by false").starts_with("Unsupported("));
}

/// `TABLESAMPLE` is ALWAYS `Unsupported` at execution time, unconditionally
/// — see `tidb_ast::TableSample`'s own doc for why (a real semantic effect
/// tied to actual TiKV storage regions this crate's in-memory table
/// representation has no analogue for, confirmed via `gorun`). Checked on
/// `SELECT`, `UPDATE`, and `DELETE` — the three statement kinds whose
/// target table reuses the shared `tidb_ast::TableRef` shape (`INSERT`'s
/// own simpler target-table grammar never parses this clause at all).
#[test]
fn table_sample_rejected() {
    let mut db = Database::new();
    step(&mut db, "create table tsr1 (a int)");
    step(&mut db, "insert into tsr1 values (1), (2), (3)");
    assert!(step(&mut db, "select a from tsr1 tablesample regions()").starts_with("Unsupported("));
    assert!(
        step(&mut db, "update tsr1 tablesample regions() set a = 1").starts_with("Unsupported(")
    );
    assert!(step(&mut db, "delete from tsr1 tablesample regions()").starts_with("Unsupported("));
}

/// `AS OF TIMESTAMP` is ALWAYS `Unsupported` at execution time,
/// unconditionally — see `tidb_ast::TableRef::as_of`'s own doc for why
/// (real MVCC historical reads this crate's plain, single-version
/// `Vec<Row>` table representation has no analogue for). Checked on
/// `SELECT`, `UPDATE`, and `DELETE` — the SAME three statement kinds
/// `table_sample_rejected` already covers.
#[test]
fn as_of_timestamp_rejected() {
    let mut db = Database::new();
    step(&mut db, "create table aofr1 (a int)");
    step(&mut db, "insert into aofr1 values (1), (2), (3)");
    assert!(step(&mut db, "select a from aofr1 as of timestamp now()").starts_with("Unsupported("));
    assert!(
        step(&mut db, "update aofr1 as of timestamp now() set a = 1").starts_with("Unsupported(")
    );
    assert!(step(&mut db, "delete from aofr1 as of timestamp now()").starts_with("Unsupported("));
}

/// `LATERAL` is ALWAYS `Unsupported` at execution time, unconditionally —
/// see `tidb_ast::JoinNode::Derived::lateral`'s own doc for why (a real
/// correlated, per-outer-row re-evaluation this crate's `Relation`-based
/// join engine has no analogue for). A plain (non-`LATERAL`) derived table
/// is unaffected — same machinery `derived_table_set_op` already covers.
#[test]
fn lateral_derived_rejected() {
    let mut db = Database::new();
    step(&mut db, "create table ltr1 (a int)");
    step(&mut db, "insert into ltr1 values (1), (2)");
    step(&mut db, "create table ltr2 (a int, b int)");
    step(&mut db, "insert into ltr2 values (1, 10), (2, 20)");
    assert!(step(
        &mut db,
        "select * from ltr1, lateral (select b from ltr2 where ltr2.a = ltr1.a) as lat"
    )
    .starts_with("Unsupported("));
    assert!(
        step(
            &mut db,
            "select * from ltr1 left join lateral (select b from ltr2 where ltr2.a = ltr1.a) as lat on true"
        )
        .starts_with("Unsupported(")
    );
    // A non-`LATERAL` derived table is unaffected.
    assert_eq!(
        step(&mut db, "select * from (select a from ltr1) as dt"),
        "RS:1;2"
    );
}

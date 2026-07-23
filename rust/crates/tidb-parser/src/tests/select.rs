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

//! `SELECT` grammar tests: clauses, joins, subqueries, CTEs,
//! set operations, derived tables, window functions, locking,
//! and per-table hints/clauses (`PARTITION`, index hints,
//! `TABLESAMPLE`, `AS OF TIMESTAMP`).

use super::*;

#[test]
fn select_core() {
    assert_eq!(r("select a from t"), "SELECT `a` FROM `t`");
    assert_eq!(
        r("select a, b+1 as c from t"),
        "SELECT `a`,`b`+1 AS `c` FROM `t`"
    );
    assert_eq!(
        r("select * from db.t as x"),
        "SELECT * FROM `db`.`t` AS `x`"
    );
    assert_eq!(
        r("select a from t where b = 1"),
        "SELECT `a` FROM `t` WHERE `b`=1"
    );
}

#[test]
fn table_statement_preserves_its_select_kind_and_tail() {
    assert_eq!(r("table t"), "TABLE `t`");
    assert_eq!(
        r("table db.t order by b limit 3 offset 2"),
        "TABLE `db`.`t` ORDER BY `b` LIMIT 2,3"
    );
    assert_eq!(
        r("table t into outfile '/tmp/out'"),
        "TABLE `t` INTO OUTFILE '/tmp/out'"
    );
    assert!(parse("table t1, t2").is_err());
    assert!(parse("table t as alias").is_err());
}

#[test]
fn values_statement_preserves_rows_and_tail() {
    assert_eq!(r("values row(1)"), "VALUES ROW(1)");
    assert_eq!(r("values row()"), "VALUES ROW()");
    assert_eq!(
        r("values row(1, default), row(2,3) order by a limit 2 into outfile '/tmp/x'"),
        "VALUES ROW(1,DEFAULT), ROW(2,3) ORDER BY `a` LIMIT 2 INTO OUTFILE '/tmp/x'"
    );
    assert!(parse("values (1, 2)").is_err());
}

#[test]
fn string_literal_aliases() {
    assert_eq!(r("select 1 as 'profit'"), "SELECT 1 AS `profit`");
    assert_eq!(r("select 1 'profit'"), "SELECT 1 AS `profit`");
}

#[test]
fn qualified_wildcards() {
    assert_eq!(r("select t.* from t"), "SELECT `t`.* FROM `t`");
    assert_eq!(
        r("select db.t.* from db.t"),
        "SELECT `db`.`t`.* FROM `db`.`t`"
    );
    assert_eq!(r("select t.a, t.* from t"), "SELECT `t`.`a`,`t`.* FROM `t`");
    assert_eq!(
        r("select emp.*, dept.name from emp join dept on emp.id = dept.id"),
        "SELECT `emp`.*,`dept`.`name` FROM `emp` JOIN `dept` ON `emp`.`id`=`dept`.`id`"
    );
    // a plain qualified column ref (not a wildcard) is unaffected
    assert_eq!(r("select t.a from t"), "SELECT `t`.`a` FROM `t`");
}

#[test]
fn into_outfile_clause() {
    // The real-corpus statement.
    assert_eq!(
        r("select 1 into outfile '/tmp/doesntmatter-no-permissions'"),
        "SELECT 1 INTO OUTFILE '/tmp/doesntmatter-no-permissions'"
    );
    // With a real FROM clause.
    assert_eq!(
        r("select a from t into outfile '/tmp/x'"),
        "SELECT `a` FROM `t` INTO OUTFILE '/tmp/x'"
    );
    // Legal even nested inside a derived table's own parens (confirmed
    // via `godump restore`: real TiDB's own grammar checks for `INTO`
    // at the tail of ANY full `SELECT`, not just the true top level).
    assert_eq!(
        r("select * from (select 1 into outfile '/tmp/x') t"),
        "SELECT * FROM (SELECT 1 INTO OUTFILE '/tmp/x') AS `t`"
    );
    // Legal inside a scalar/IN/EXISTS/ALL subquery's own parens too.
    assert_eq!(
        r("select 1 where 1 in (select 1 into outfile '/tmp/x')"),
        "SELECT 1 FROM DUAL WHERE 1 IN (SELECT 1 INTO OUTFILE '/tmp/x')"
    );
    // A quote inside the file path is escaped like any other string.
    assert_eq!(
        r("select 1 into outfile '/tmp/it''s'"),
        "SELECT 1 INTO OUTFILE '/tmp/it''s'"
    );
    for (sql, expected) in [
        (
            "select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated by ','",
            "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ','",
        ),
        (
            "select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated by ',' enclosed by '\"'",
            "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '\"'",
        ),
        (
            "select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated by ',' optionally enclosed by '\"'",
            "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'",
        ),
        (
            "select a,b,a+b from t into outfile '/tmp/result.txt' lines terminated by '\\n'",
            "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' LINES TERMINATED BY '\n'",
        ),
        (
            "select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated by ',' optionally enclosed by '\"' lines terminated by '\\r'",
            "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\r'",
        ),
        (
            "select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated by ',' enclosed by '\"' lines terminated by '\\r'",
            "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r'",
        ),
        (
            "select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated by ',' optionally enclosed by '\"' lines starting by 'xy' terminated by '\\r'",
            "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES STARTING BY 'xy' TERMINATED BY '\r'",
        ),
        (
            "select a,b,a+b from t into outfile '/tmp/result.txt' fields terminated by ',' enclosed by '\"' lines starting by 'xy' terminated by '\\r'",
            "SELECT `a`,`b`,`a`+`b` FROM `t` INTO OUTFILE '/tmp/result.txt' FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES STARTING BY 'xy' TERMINATED BY '\r'",
        ),
    ] {
        assert_eq!(r(sql), expected, "Go TestDMLStmt row: {sql}");
    }
    // Deliberately NOT threaded into a set operation's own tail — see
    // `tidb_ast::SelectStmt::into_outfile`'s own doc for why.
    assert!(parse("select 1 union select 2 into outfile '/tmp/x'").is_err());
}

#[test]
fn sole_parenthesized_select_preserves_braces_and_folds_its_tail() {
    assert_eq!(r("(select 1)"), "(SELECT 1)");
    assert_eq!(
        r("(select a from t) order by 1 limit 2"),
        "(SELECT `a` FROM `t` ORDER BY 1 LIMIT 2)"
    );
    assert!(parse("(select 1) into outfile '/tmp/x'").is_err());
}

#[test]
fn from_dual_placeholder() {
    // Bare FROM DUAL drops; FROM DUAL with a predicate is preserved so the
    // restored SQL stays valid.
    assert_eq!(r("select 1 from dual"), "SELECT 1");
    assert_eq!(
        r("select 1 from dual where 1"),
        "SELECT 1 FROM DUAL WHERE 1"
    );
}

#[test]
fn clauses() {
    assert_eq!(
        r("select distinct a from t"),
        "SELECT DISTINCT `a` FROM `t`"
    );
    assert_eq!(
        r("select a from t group by a, b having a > 1"),
        "SELECT `a` FROM `t` GROUP BY `a`,`b` HAVING `a`>1"
    );
    assert_eq!(
        r("select a from t order by a, b desc, c asc"),
        "SELECT `a` FROM `t` ORDER BY `a`,`b` DESC,`c`"
    );
    assert_eq!(
        r("select a from t limit 5, 10"),
        "SELECT `a` FROM `t` LIMIT 5,10"
    );
    assert_eq!(
        r("select a from t order by a desc limit 3"),
        "SELECT `a` FROM `t` ORDER BY `a` DESC LIMIT 3"
    );
}

/// Direct vectors from Go's `pkg/parser/parser_test.go` LIMIT boundary cases:
/// the maximum unsigned 64-bit literal is valid, while the next value is a
/// parse error in every LIMIT/OFFSET position.
#[test]
fn limit_uint64_boundary_matches_go() {
    assert_eq!(
        r("select * from t limit 18446744073709551615"),
        "SELECT * FROM `t` LIMIT 18446744073709551615"
    );
    for sql in [
        "select * from t limit 18446744073709551616 offset 3",
        "select * from t limit 10 offset 18446744073709551616",
        "select * from t limit 18446744073709551616, 10",
        "select * from t limit 10, 18446744073709551616",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn joins() {
    // Comma join nests, parenthesizing the accumulated left operand.
    assert_eq!(
        r("select a from t1, t2"),
        "SELECT `a` FROM (`t1`) JOIN `t2`"
    );
    assert_eq!(
        r("select a from t1, t2, t3"),
        "SELECT `a` FROM ((`t1`) JOIN `t2`) JOIN `t3`"
    );
    assert_eq!(
        r("select a from t1 join t2 on t1.id = t2.id"),
        "SELECT `a` FROM `t1` JOIN `t2` ON `t1`.`id`=`t2`.`id`"
    );
    assert_eq!(
        r("select a from t1 left join t2 on a = b"),
        "SELECT `a` FROM `t1` LEFT JOIN `t2` ON `a`=`b`"
    );
    assert_eq!(
        r("select a from t1 inner join t2 using (id)"),
        "SELECT `a` FROM `t1` JOIN `t2` USING (`id`)"
    );
    assert_eq!(
        r("select a from t1 join t2 on x join t3 on y"),
        "SELECT `a` FROM (`t1` JOIN `t2` ON `x`) JOIN `t3` ON `y`"
    );
}

/// `(table_refs)` — a purely structural grouping paren around a single
/// table, a comma-joined list, or an explicit `JOIN` chain, NOT a derived
/// table (no `SELECT` inside — `looks_like_derived_table`, checked
/// FIRST, claims that shape instead). No alias may follow the
/// closing paren (a genuine `ParseError`, confirmed via `godump restore`),
/// unlike a derived table. Every assertion here was cross-checked against
/// real TiDB via `godump restore` (not assumed) — parens are purely
/// structural and get dropped/re-derived on restore based on the
/// resulting join tree's own SHAPE, never preserved as an explicit
/// "was parenthesized" flag (confirmed: `(t)` restores as bare `t`, and
/// `(t1, t2)`/`((t1, t2))` both restore identically to `t1 JOIN t2`).
#[test]
fn parenthesized_join() {
    // A single bare table: the parens are simply dropped.
    assert_eq!(r("select * from (t)"), "SELECT * FROM `t`");
    assert_eq!(r("select * from ((t1))"), "SELECT * FROM `t1`");
    // A comma-joined list inside the parens.
    assert_eq!(
        r("select * from (t as a, t2 as b)"),
        "SELECT * FROM (`t` AS `a`) JOIN `t2` AS `b`"
    );
    assert_eq!(
        r("select * from (t1, t2)"),
        "SELECT * FROM (`t1`) JOIN `t2`"
    );
    // An explicit JOIN chain inside the parens.
    assert_eq!(
        r("select * from (t as a cross join t2 as b)"),
        "SELECT * FROM `t` AS `a` JOIN `t2` AS `b`"
    );
    assert_eq!(
        r("select * from (t1 join t2 on t1.a=t2.a)"),
        "SELECT * FROM `t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`"
    );
    // Nested groups combined via an outer join, needing real
    // disambiguating parens on restore.
    assert_eq!(
        r(
            "select * from (t1 join t2 using (a)) join (t3 join t4 using (a)) on (t2.a = t4.a and t1.a = t3.a)"
        ),
        "SELECT * FROM (`t1` JOIN `t2` USING (`a`)) JOIN (`t3` JOIN `t4` USING (`a`)) ON (`t2`.`a`=`t4`.`a` AND `t1`.`a`=`t3`.`a`)"
    );
    assert_eq!(
        r("select * from (t1 natural join t2) natural join (t3 natural join t4)"),
        "SELECT * FROM (`t1` NATURAL JOIN `t2`) NATURAL JOIN (`t3` NATURAL JOIN `t4`)"
    );
    // A parenthesized group as one side of a further join.
    assert_eq!(
        r("select * from (t1) join t2"),
        "SELECT * FROM (`t1`) JOIN `t2`"
    );
    assert_eq!(
        r("select * from t1 join (t2)"),
        "SELECT * FROM `t1` JOIN (`t2`)"
    );
    // No alias may follow — a genuine `ParseError`, unlike a derived table.
    assert!(parse("select * from (t1) x").is_err());
    // Derived tables (aliased — a BARE, alias-less derived table is a
    // separate, pre-existing, deliberately out-of-scope gap, unrelated
    // to this feature) and derived-table set-ops (task #78) stay
    // unaffected — `looks_like_derived_table` claims those shapes first.
    assert_eq!(
        r("select * from (select 1 a) t1"),
        "SELECT * FROM (SELECT 1 AS `a`) AS `t1`"
    );
    assert_eq!(
        r("select * from ((select a from t1) union all (select a from t2)) x"),
        "SELECT * FROM ((SELECT `a` FROM `t1`) UNION ALL (SELECT `a` FROM `t2`)) AS `x`"
    );
}

#[test]
fn subqueries() {
    assert_eq!(
        r("select a from (select 1 as x) t"),
        "SELECT `a` FROM (SELECT 1 AS `x`) AS `t`"
    );
    assert_eq!(
        r("select a from t where b in (select c from t2)"),
        "SELECT `a` FROM `t` WHERE `b` IN (SELECT `c` FROM `t2`)"
    );
    // `IN`'s own subquery may ALSO be `UNION`/`EXCEPT`/`INTERSECT`-
    // bodied — one of the two parenthesized-subquery positions (the other
    // is `EXISTS`) confirmed to accept this, via `godump restore`. A
    // row-value operand (`ROW(...)`
    // or a bare comma-tuple, task #76's own shape) composes normally.
    assert_eq!(
        r("select a from t where a in (select 1 union select 2)"),
        "SELECT `a` FROM `t` WHERE `a` IN (SELECT 1 UNION SELECT 2)"
    );
    assert_eq!(
        r("select (t.a, t.b) not in (select 3, 2 union select 9, 2) as f from t"),
        "SELECT ROW(`t`.`a`,`t`.`b`) NOT IN (SELECT 3,2 UNION SELECT 9,2) AS `f` FROM `t`"
    );
    assert_eq!(
        r("select a from t where b = (select max(c) from t2)"),
        "SELECT `a` FROM `t` WHERE `b`=(SELECT MAX(`c`) FROM `t2`)"
    );
    assert_eq!(
        r("select a from t where exists (select 1 from t2)"),
        "SELECT `a` FROM `t` WHERE EXISTS (SELECT 1 FROM `t2`)"
    );
    assert_eq!(
        r("select a from t where not exists (select 1 from t2)"),
        "SELECT `a` FROM `t` WHERE NOT EXISTS (SELECT 1 FROM `t2`)"
    );
    // Go TestNotExistsSubquery checks the typed negation bit rather than
    // restore text, so keep that source assertion explicit too.
    let statement = parse("select * from t1 where not exists (select * from t2 where t1.a = t2.a)")
        .expect("NOT EXISTS query parses");
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("expected query statement");
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        panic!("expected SELECT statement");
    };
    assert!(matches!(
        select.where_clause,
        Some(Expr::Exists { not: true, .. })
    ));
    assert_eq!(
        r("select (select max(x) from t2) as m from t"),
        "SELECT (SELECT MAX(`x`) FROM `t2`) AS `m` FROM `t`"
    );
    assert_eq!(
        r("select a from t where b > all (select c from t2)"),
        "SELECT `a` FROM `t` WHERE `b`>ALL (SELECT `c` FROM `t2`)"
    );
    assert_eq!(
        r("select a from t1, (select c from t2) d"),
        "SELECT `a` FROM (`t1`) JOIN (SELECT `c` FROM `t2`) AS `d`"
    );
    // A comma-chain between two DERIVED tables restores with a plain `, `
    // separator and no wrapping parens at all — the OPPOSITE of the
    // plain-table case just above, which keeps the `(...) JOIN ...`
    // form. This asymmetry comes straight from real TiDB's own
    // `Join.Restore` (`pkg/parser/ast/dml.go`, read directly, not
    // guessed): a comma continuation's own accumulated-so-far operand is
    // ALWAYS wrapped in a fresh single-operand `Join{right: None}` node
    // (matching `Parser::parse_from`'s own comma-chain construction), and
    // restore only collapses that wrapper into a bare `, ` when ITS OWN
    // `left` is specifically a derived-table subquery, not a plain table
    // reference — confirmed via `godump restore`, including that a
    // THIRD comma term (whether plain or derived) always reverts to the
    // `(...) JOIN ...` form for the OUTER join, since that join's own
    // left is the accumulated (already-a-join) node, never a bare
    // derived table directly.
    assert_eq!(
        r("select a from (select 1 a) x, (select 2 a) x"),
        "SELECT `a` FROM (SELECT 1 AS `a`) AS `x`, (SELECT 2 AS `a`) AS `x`"
    );
    assert_eq!(
        r("select a from (select 1 a) x, t2"),
        "SELECT `a` FROM (SELECT 1 AS `a`) AS `x`, `t2`"
    );
    assert_eq!(
        r("select a from (select 1 a) x, (select 2 a) y, t3"),
        "SELECT `a` FROM ((SELECT 1 AS `a`) AS `x`, (SELECT 2 AS `a`) AS `y`) JOIN `t3`"
    );
    assert_eq!(
        r("select a from (select 1 a) x, t2, (select 2 a) y"),
        "SELECT `a` FROM ((SELECT 1 AS `a`) AS `x`, `t2`) JOIN (SELECT 2 AS `a`) AS `y`"
    );
}

/// `WITH [RECURSIVE] cte AS (...) SELECT ...` inside a scalar/`IN`/
/// `EXISTS`/`ANY`-`SOME`-`ALL` subquery position. Every form retains the
/// complete query envelope, including a top-level set operation.
#[test]
fn with_in_subquery() {
    assert_eq!(
        r("select * from t1 where exists (with q as (select 1) select * from q)"),
        "SELECT * FROM `t1` WHERE EXISTS (WITH `q` AS (SELECT 1) SELECT * FROM `q`)"
    );
    assert_eq!(
        r("select * from t1 where c1 in (with cte1 as (select c1 from t2) select c1 from cte1)"),
        "SELECT * FROM `t1` WHERE `c1` IN (WITH `cte1` AS (SELECT `c1` FROM `t2`) SELECT `c1` FROM `cte1`)"
    );
    assert_eq!(
        r("select (with q as (select 1) select * from q)"),
        "SELECT (WITH `q` AS (SELECT 1) SELECT * FROM `q`)"
    );
    assert_eq!(
        r("select 1 = all (with q as (select 1) select * from q)"),
        "SELECT 1=ALL (WITH `q` AS (SELECT 1) SELECT * FROM `q`)"
    );
    assert_eq!(
        r("select 1 = all (with q as (select 1) select * from q union select 2)"),
        "SELECT 1=ALL (WITH `q` AS (SELECT 1) SELECT * FROM `q` UNION SELECT 2)"
    );
    // `RECURSIVE` works too, the same CTE grammar the top-level
    // statement position already uses.
    assert_eq!(
        r("select * from t1 where exists (with recursive qn as (select 1 b union all select b+1 from qn where b=0) select * from qn)"),
        "SELECT * FROM `t1` WHERE EXISTS (WITH RECURSIVE `qn` AS (SELECT 1 AS `b` UNION ALL SELECT `b`+1 FROM `qn` WHERE `b`=0) SELECT * FROM `qn`)"
    );
}

#[test]
fn empty_alias_omitted() {
    // An alias whose text is the empty string (`` `` ``) restores
    // identically to no alias at all — confirmed directly from real
    // TiDB's own `TableSource.Restore`/`SelectField.Restore` source
    // (pkg/parser/ast/dml.go): `AsName` is a plain, non-optional
    // `CIStr` there, so "absent" and "written empty" are literally the
    // same value, and both restore paths share the identical
    // `asName != ""` guard. Applies to a SELECT-field alias, a
    // plain-table alias, and a derived-table alias (whose alias is
    // otherwise grammatically MANDATORY, but the identifier itself may
    // still be empty).
    assert_eq!(r("select 1 as ``"), "SELECT 1");
    assert_eq!(r("select 1 a, 2 as ``"), "SELECT 1 AS `a`,2");
    assert_eq!(
        r("select a from (select 1 a) ``"),
        "SELECT `a` FROM (SELECT 1 AS `a`)"
    );
    assert_eq!(
        r("select a from (select 1 a) ``, (select 2 a) ``"),
        "SELECT `a` FROM (SELECT 1 AS `a`), (SELECT 2 AS `a`)"
    );
    assert_eq!(r("select * from t as ``"), "SELECT * FROM `t`");
    assert_eq!(
        r("select t.a from t as ``, u as x where t.a = x.a"),
        "SELECT `t`.`a` FROM (`t`) JOIN `u` AS `x` WHERE `t`.`a`=`x`.`a`"
    );
    // A plain table (empty alias) followed by a derived table (empty
    // alias) is NOT the derived-derived comma shape — `useCommaJoin`
    // needs the accumulated left operand's own left to be a derived
    // table, which it isn't here, so this reverts to `(...) JOIN ...`.
    assert_eq!(
        r("select a from t as ``, (select 1 a) ``"),
        "SELECT `a` FROM (`t`) JOIN (SELECT 1 AS `a`)"
    );
    assert_eq!(
        r("select a from (select 1 a) `` join (select 2 a) y on 1=1"),
        "SELECT `a` FROM (SELECT 1 AS `a`) JOIN (SELECT 2 AS `a`) AS `y` ON 1=1"
    );
}

#[test]
fn set_operations() {
    assert_eq!(r("select 1 union select 2"), "SELECT 1 UNION SELECT 2");
    assert_eq!(
        r("select 1 union all select 2"),
        "SELECT 1 UNION ALL SELECT 2"
    );
    assert_eq!(
        r("select 1 union select 2 union select 3"),
        "SELECT 1 UNION SELECT 2 UNION SELECT 3"
    );
    assert_eq!(
        r("(select 1) union (select 2)"),
        "(SELECT 1) UNION (SELECT 2)"
    );
    assert_eq!(
        r("select 1 union select 2 order by 1 limit 5"),
        "SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 5"
    );
    assert_eq!(r("select 1 except select 2"), "SELECT 1 EXCEPT SELECT 2");
    assert_eq!(
        r("select 1 intersect select 2"),
        "SELECT 1 INTERSECT SELECT 2"
    );
    assert_eq!(
        r("select a from t union select b from t2"),
        "SELECT `a` FROM `t` UNION SELECT `b` FROM `t2`"
    );
    // A bare (unparenthesized) NON-FINAL term's own `ORDER BY`/`LIMIT`
    // (disambiguated by the `UNION` following right after) is that
    // term's own, not hoisted to the statement level — this used to be
    // a genuine parser bug (`unexpected trailing tokens`, confirmed via
    // `godump restore` that real MySQL/TiDB accepts it), fixed alongside
    // `FOR UPDATE`'s own implementation (the same tail-parsing code path
    // needed the fix either way).
    assert_eq!(
        r("select a from t order by a union select b from t2"),
        "SELECT `a` FROM `t` ORDER BY `a` UNION SELECT `b` FROM `t2`"
    );
    // Both terms parenthesized still has a real statement-level trailing
    // `ORDER BY`/`LIMIT` after the last one (confirmed via `godump
    // restore` — this exact statement was the regression case caught by
    // the differential corpus while implementing the fix above).
    assert_eq!(
        r("(select a from t1) union all (select a from t2) order by 1 limit 10"),
        "(SELECT `a` FROM `t1`) UNION ALL (SELECT `a` FROM `t2`) ORDER BY 1 LIMIT 10"
    );
}

#[test]
fn union_order_by_ownership_matches_go_ast() {
    fn collect(query: &tidb_ast::QueryStmt, order_by: &mut Vec<bool>) {
        match query {
            tidb_ast::QueryStmt::Select(select) => order_by.push(!select.order_by.is_empty()),
            tidb_ast::QueryStmt::SetOpr(setopr) => {
                for term in &setopr.terms {
                    match &term.body {
                        tidb_ast::SetOprTermBody::Select(select) => {
                            order_by.push(!select.order_by.is_empty());
                        }
                        tidb_ast::SetOprTermBody::Nested(nested) => {
                            collect(&tidb_ast::QueryStmt::SetOpr(nested.clone()), order_by);
                        }
                    }
                }
                order_by.push(!setopr.order_by.is_empty());
            }
        }
    }

    for (sql, expected) in [
        (
            "select 2 as a from dual union select 1 as b from dual order by a",
            &[false, false, true][..],
        ),
        (
            "select 2 as a from dual union (select 1 as b from dual order by a)",
            &[false, true, false],
        ),
        (
            "(select 2 as a from dual order by a) union select 1 as b from dual order by a",
            &[true, false, true],
        ),
        ("select 1 a, 2 b from dual order by a", &[true]),
        ("select 1 a, 2 b from dual", &[false]),
    ] {
        let statement = parse_with_window_functions(sql, false).unwrap();
        let Stmt::Query(query) = statement else {
            panic!("expected query for {sql}")
        };
        let mut actual = Vec::new();
        collect(&query, &mut actual);
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn query_envelope_preserves_query_only_children() {
    let stmt = parse("select 1 union select 2").unwrap();
    assert!(matches!(
        stmt,
        Stmt::Query(query) if matches!(query.as_ref(), tidb_ast::QueryStmt::SetOpr(_))
    ));

    let stmt = parse("with c as (select 1 union select 2) select * from c").unwrap();
    let Stmt::Query(query) = stmt else {
        panic!("expected Query envelope")
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected SELECT query")
    };
    assert!(matches!(
        select.with.as_ref().unwrap().ctes[0].query.as_ref(),
        tidb_ast::QueryStmt::SetOpr(_)
    ));

    let stmt =
        parse("select * from (select 1 union select 2) as d where 1 in (select 1 union select 2)")
            .unwrap();
    let Stmt::Query(query) = stmt else {
        panic!("expected Query envelope")
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected SELECT query")
    };
    let from = select.from.as_ref().unwrap();
    assert!(matches!(
        &from.left,
        tidb_ast::JoinNode::Derived { subquery, .. }
            if matches!(subquery.as_ref(), tidb_ast::QueryStmt::SetOpr(_))
    ));
    assert!(matches!(
        select.where_clause,
        Some(Expr::InSubquery { subquery, .. })
            if matches!(subquery.as_ref(), tidb_ast::QueryStmt::SetOpr(_))
    ));

    let stmt = parse("insert into t select 1 union select 2").unwrap();
    let Stmt::Dml(dml) = stmt else {
        panic!("expected DML envelope")
    };
    let tidb_ast::DmlStmt::Insert(insert) = dml.into_inner() else {
        panic!("expected INSERT")
    };
    assert!(matches!(
        insert.source.as_deref(),
        Some(tidb_ast::QueryStmt::SetOpr(_))
    ));
}

#[test]
fn cte() {
    assert_eq!(
        r("with a as (select 1 as x) select * from a"),
        "WITH `a` AS (SELECT 1 AS `x`) SELECT * FROM `a`"
    );
    assert_eq!(
        r("with a as (select 1 as x), b as (select x+1 as y from a) select * from b"),
        "WITH `a` AS (SELECT 1 AS `x`), `b` AS (SELECT `x`+1 AS `y` FROM `a`) SELECT * FROM `b`"
    );
    // An explicit column rename list.
    assert_eq!(
        r("with a (m,n) as (select 1,2) select m,n from a"),
        "WITH `a` (`m`, `n`) AS (SELECT 1,2) SELECT `m`,`n` FROM `a`"
    );
    assert_eq!(
        r("with recursive a as (select 1) select * from a"),
        "WITH RECURSIVE `a` AS (SELECT 1) SELECT * FROM `a`"
    );
    // A CTE's own body may itself be a `UNION`/`UNION ALL`-joined set
    // operation (needed for `WITH RECURSIVE`'s `base UNION [ALL]
    // recursive` shape, but also legal for an ordinary non-recursive
    // CTE -- see `tidb_exec`'s own recursive-CTE evaluation doc for
    // the execution-time semantics).
    assert_eq!(
            r("with recursive cte as (select 1 as n union all select n+1 from cte where n<5) select * from cte"),
            "WITH RECURSIVE `cte` AS (SELECT 1 AS `n` UNION ALL SELECT `n`+1 FROM `cte` WHERE `n`<5) SELECT * FROM `cte`"
        );
    assert_eq!(
        r("with a as (select 1 as x union select 2) select * from a"),
        "WITH `a` AS (SELECT 1 AS `x` UNION SELECT 2) SELECT * FROM `a`"
    );
    // A CTE body is a general subquery, so it may itself begin with a WITH
    // clause. The nested clause stays inside the outer CTE's parentheses.
    assert_eq!(
        r("with outer_cte as (with inner_cte as (select 1 as n) select n from inner_cte) select n from outer_cte"),
        "WITH `outer_cte` AS (WITH `inner_cte` AS (SELECT 1 AS `n`) SELECT `n` FROM `inner_cte`) SELECT `n` FROM `outer_cte`"
    );
    // TiDB owns a leading WITH on the whole set-operation wrapper, not
    // merely the first SELECT term.
    assert_eq!(
        r("with a as (select 1) select * from a union select 1"),
        "WITH `a` AS (SELECT 1) SELECT * FROM `a` UNION SELECT 1"
    );

    let stmt = parse("with a as (select 1) select * from a union select 1").unwrap();
    assert!(matches!(
        stmt,
        Stmt::Query(query)
            if matches!(query.as_ref(), tidb_ast::QueryStmt::SetOpr(setopr) if setopr.with.is_some())
    ));

    // Query-valued children retain the same ownership; this is not a
    // top-level-only parser special case. Scalar/EXISTS slots deliberately
    // remain Select-only because their AST representation is narrower.
    assert_eq!(
        r("select * from (with c as (select 1 as n) select n from c union select 2) as d"),
        "SELECT * FROM (WITH `c` AS (SELECT 1 AS `n`) SELECT `n` FROM `c` UNION SELECT 2) AS `d`"
    );
    assert_eq!(
        r("select 1 where 1 in (with c as (select 1) select 1 from c union select 2)"),
        "SELECT 1 FROM DUAL WHERE 1 IN (WITH `c` AS (SELECT 1) SELECT 1 FROM `c` UNION SELECT 2)"
    );
}

#[test]
fn window_functions() {
    assert_eq!(
        r("select row_number() over (partition by dept order by salary) from t"),
        "SELECT ROW_NUMBER() OVER (PARTITION BY `dept` ORDER BY `salary`) FROM `t`"
    );
    assert_eq!(
        r("select rank() over () from t"),
        "SELECT RANK() OVER () FROM `t`"
    );
    assert_eq!(
        r("select dense_rank() over (order by salary desc) from t"),
        "SELECT DENSE_RANK() OVER (ORDER BY `salary` DESC) FROM `t`"
    );
    // `PARTITION BY` items join with `, ` but `ORDER BY` items join
    // with `,` (no space) -- a real asymmetry in the Go AST's own
    // restore, confirmed via `godump restore`, encoded exactly rather
    // than "fixed" to be consistent.
    assert_eq!(
        r("select row_number() over (partition by a, b) from t"),
        "SELECT ROW_NUMBER() OVER (PARTITION BY `a`, `b`) FROM `t`"
    );
    assert_eq!(
        r("select row_number() over (partition by a, b order by c, d desc) from t"),
        "SELECT ROW_NUMBER() OVER (PARTITION BY `a`, `b` ORDER BY `c`,`d` DESC) FROM `t`"
    );
    // A window AGGREGATE (`COUNT`/`SUM`/`AVG`/`MAX`/`MIN`) shares the
    // SAME `Expr::Window` node and single-argument shape
    // `Expr::Aggregate` uses -- `COUNT(*)` restores as `COUNT(1)`,
    // matching `Expr::Aggregate`'s own established convention.
    assert_eq!(
        r("select sum(salary) over (partition by dept) from t"),
        "SELECT SUM(`salary`) OVER (PARTITION BY `dept`) FROM `t`"
    );
    assert_eq!(
        r("select count(*) over () from t"),
        "SELECT COUNT(1) OVER () FROM `t`"
    );
    assert_eq!(
        r("select avg(salary) over (order by salary desc) from t"),
        "SELECT AVG(`salary`) OVER (ORDER BY `salary` DESC) FROM `t`"
    );
    // The "value function" family: LAG/LEAD (one to three arguments —
    // value, an optional offset, an optional out-of-range default)
    // and FIRST_VALUE/LAST_VALUE (one argument)/NTH_VALUE (two: value,
    // then a 1-based position).
    assert_eq!(
        r("select lag(salary) over (partition by dept order by salary) from t"),
        "SELECT LAG(`salary`) OVER (PARTITION BY `dept` ORDER BY `salary`) FROM `t`"
    );
    assert_eq!(
        r("select lag(salary, 2, 0) over (order by salary) from t"),
        "SELECT LAG(`salary`, 2, 0) OVER (ORDER BY `salary`) FROM `t`"
    );
    assert_eq!(
        r("select lead(salary) over (order by salary) from t"),
        "SELECT LEAD(`salary`) OVER (ORDER BY `salary`) FROM `t`"
    );
    assert_eq!(
        r("select first_value(salary) over (order by salary) from t"),
        "SELECT FIRST_VALUE(`salary`) OVER (ORDER BY `salary`) FROM `t`"
    );
    assert_eq!(
        r("select last_value(salary) over (order by salary) from t"),
        "SELECT LAST_VALUE(`salary`) OVER (ORDER BY `salary`) FROM `t`"
    );
    assert_eq!(
        r("select nth_value(salary, 2) over (order by salary) from t"),
        "SELECT NTH_VALUE(`salary`, 2) OVER (ORDER BY `salary`) FROM `t`"
    );
    // The "distribution function" family: `NTILE(n)` (one argument)
    // and `PERCENT_RANK`/`CUME_DIST` (zero arguments, like the
    // ranking functions).
    assert_eq!(
        r("select ntile(2) over (partition by dept order by salary) from t"),
        "SELECT NTILE(2) OVER (PARTITION BY `dept` ORDER BY `salary`) FROM `t`"
    );
    assert_eq!(
        r("select percent_rank() over (order by salary) from t"),
        "SELECT PERCENT_RANK() OVER (ORDER BY `salary`) FROM `t`"
    );
    assert_eq!(
        r("select cume_dist() over () from t"),
        "SELECT CUME_DIST() OVER () FROM `t`"
    );
    assert_eq!(
        r("select max(distinct salary) over (partition by dept) from t"),
        "SELECT MAX(DISTINCT `salary`) OVER (PARTITION BY `dept`) FROM `t`"
    );
    assert_eq!(
        r("select lag(salary) ignore nulls over (order by salary) from t"),
        "SELECT LAG(`salary`) IGNORE NULLS OVER (ORDER BY `salary`) FROM `t`"
    );
    // Named windows: `OVER w` (bare, no parentheses) restores
    // differently from `OVER (w)` (parenthesized, no extension) even
    // though they're semantically identical -- confirmed via `godump
    // restore`, a real TiDB grammar/restore asymmetry, not something
    // this parser invented.
    assert_eq!(
            r("select row_number() over w from t window w as (partition by dept order by salary)"),
            "SELECT ROW_NUMBER() OVER `w` FROM `t` WINDOW `w` AS (PARTITION BY `dept` ORDER BY `salary`)"
        );
    assert_eq!(
            r("select row_number() over (w) from t window w as (partition by dept order by salary)"),
            "SELECT ROW_NUMBER() OVER (`w`) FROM `t` WINDOW `w` AS (PARTITION BY `dept` ORDER BY `salary`)"
        );
    // Extending a named window: the extension's own clauses render
    // right after the base name, still inside the SAME parentheses.
    assert_eq!(
        r("select sum(v) over (w order by salary) from t window w as (partition by dept)"),
        "SELECT SUM(`v`) OVER (`w` ORDER BY `salary`) FROM `t` WINDOW `w` AS (PARTITION BY `dept`)"
    );
    // Multiple WINDOW clause entries join with a comma and NO space
    // (confirmed via `godump restore`); a later window may extend an
    // EARLIER one by name.
    assert_eq!(
            r("select sum(v) over (w2) from t window w1 as (partition by dept), w2 as (w1 order by salary)"),
            "SELECT SUM(`v`) OVER (`w2`) FROM `t` WINDOW `w1` AS (PARTITION BY `dept`),`w2` AS (`w1` ORDER BY `salary`)"
        );
    // Explicit `ROWS` frame clause: the `BETWEEN` form restores as
    // written, and the single-bound shorthand normalizes to the full
    // `BETWEEN ... AND CURRENT ROW` form -- confirmed via `godump`
    // that real TiDB's own restore does the same.
    assert_eq!(
            r("select sum(v) over (order by v rows between unbounded preceding and current row) from t"),
            "SELECT SUM(`v`) OVER (ORDER BY `v` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM `t`"
        );
    assert_eq!(
        r("select sum(v) over (order by v rows between 1 preceding and 1 following) from t"),
        "SELECT SUM(`v`) OVER (ORDER BY `v` ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM `t`"
    );
    assert_eq!(
        r("select sum(v) over (order by v rows 3 preceding) from t"),
        "SELECT SUM(`v`) OVER (ORDER BY `v` ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM `t`"
    );
    assert_eq!(
            r("select sum(v) over (partition by dept rows between current row and unbounded following) from t"),
            "SELECT SUM(`v`) OVER (PARTITION BY `dept` ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM `t`"
        );
    assert_eq!(
        r("select sum(v) over (rows between 1 preceding and current row) from t"),
        "SELECT SUM(`v`) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM `t`"
    );
    // Explicit `RANGE` frame clause: shares the EXACT SAME grammar
    // and restore convention as `ROWS` above -- confirmed via
    // `godump restore` -- just with `RANGE` in place of `ROWS`.
    assert_eq!(
            r("select sum(v) over (order by v range between unbounded preceding and current row) from t"),
            "SELECT SUM(`v`) OVER (ORDER BY `v` RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM `t`"
        );
    assert_eq!(
        r("select sum(v) over (order by v range between 1 preceding and 1 following) from t"),
        "SELECT SUM(`v`) OVER (ORDER BY `v` RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM `t`"
    );
    assert_eq!(
        r("select sum(v) over (order by v range 3 preceding) from t"),
        "SELECT SUM(`v`) OVER (ORDER BY `v` RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) FROM `t`"
    );
    assert_eq!(
            r("select sum(v) over (partition by dept range between current row and unbounded following) from t"),
            "SELECT SUM(`v`) OVER (PARTITION BY `dept` RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM `t`"
        );
}

/// Exact visitor cases from Go `pkg/parser/parser_test.go`'s
/// `TestVisitFrameBound` (pingcap/parser#51).
#[test]
fn window_frame_bound_visits_its_expression_and_interval_unit() {
    use tidb_ast::{FrameBound, Visitable, Visitor};

    #[derive(Default)]
    struct FrameVisitor {
        in_bound: bool,
        expression_roots: usize,
        unit: Option<String>,
    }

    impl Visitor for FrameVisitor {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<FrameBound>() {
                self.in_bound = true;
            } else if self.in_bound && self.expression_roots == 0 {
                if let Some(expr) = node.downcast_ref::<Expr>() {
                    self.expression_roots += 1;
                    if let Expr::Interval { unit, .. } = expr {
                        self.unit = Some(unit.clone());
                    }
                }
            }
            false
        }

        fn leave(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<FrameBound>() {
                self.in_bound = false;
            }
            true
        }
    }

    for (sql, expression_roots, unit) in [
        (
            "SELECT AVG(val) OVER (RANGE INTERVAL 1+3 MINUTE_SECOND PRECEDING) FROM t",
            1,
            Some("MINUTE_SECOND"),
        ),
        ("SELECT AVG(val) OVER (RANGE 5 PRECEDING) FROM t", 1, None),
        ("SELECT AVG(val) OVER () FROM t", 0, None),
    ] {
        let mut statement = parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let mut visitor = FrameVisitor::default();
        assert!(statement.accept(&mut visitor));
        assert_eq!(visitor.expression_roots, expression_roots, "{sql}");
        assert_eq!(visitor.unit.as_deref(), unit, "{sql}");
    }
}

/// `FOR UPDATE` / `FOR SHARE` / `LOCK IN SHARE MODE` locking clauses, and
/// the `ORDER BY`/`LIMIT`/locking-clause flexible-ordering grammar they
/// share (see `tidb_ast::SelectStmt::lock`'s own doc). Every assertion
/// here was cross-checked against real TiDB via `godump restore`.
#[test]
fn select_lock() {
    assert_eq!(
        r("select * from t for update"),
        "SELECT * FROM `t` FOR UPDATE"
    );
    assert_eq!(
        r("select * from t lock in share mode"),
        "SELECT * FROM `t` FOR SHARE"
    );
    assert_eq!(
        r("select * from t where a = 1 for update"),
        "SELECT * FROM `t` WHERE `a`=1 FOR UPDATE"
    );
    assert_eq!(
        r("select * from t for update nowait"),
        "SELECT * FROM `t` FOR UPDATE NOWAIT"
    );
    assert_eq!(
        r("select * from t for update skip locked"),
        "SELECT * FROM `t` FOR UPDATE SKIP LOCKED"
    );
    assert_eq!(
        r("select * from t for share"),
        "SELECT * FROM `t` FOR SHARE"
    );
    assert_eq!(
        r("select * from t for update of t"),
        "SELECT * FROM `t` FOR UPDATE OF `t`"
    );
    assert_eq!(
        r("select * from t for share nowait"),
        "SELECT * FROM `t` FOR SHARE NOWAIT"
    );
    assert_eq!(
        r("select * from t1, t2 for update"),
        "SELECT * FROM (`t1`) JOIN `t2` FOR UPDATE"
    );
    assert_eq!(
        r("select * from t for update of t1, t2"),
        "SELECT * FROM `t` FOR UPDATE OF `t1`, `t2`"
    );
    assert_eq!(
        r("select * from db.t for update of db.t"),
        "SELECT * FROM `db`.`t` FOR UPDATE OF `db`.`t`"
    );
    assert_eq!(
        r("select a from t where a in (select b from t2 for update)"),
        "SELECT `a` FROM `t` WHERE `a` IN (SELECT `b` FROM `t2` FOR UPDATE)"
    );
    // `ORDER BY`/`LIMIT`/the locking clause parse in ANY relative order,
    // always restoring in a FIXED canonical order — `ORDER BY`, `LIMIT`,
    // then the lock for a plain `SELECT` (real SQL clause order).
    assert_eq!(
        r("select * from t order by a for update"),
        "SELECT * FROM `t` ORDER BY `a` FOR UPDATE"
    );
    assert_eq!(
        r("select * from t for update order by a"),
        "SELECT * FROM `t` ORDER BY `a` FOR UPDATE"
    );
    assert_eq!(
        r("select * from t limit 1 for update"),
        "SELECT * FROM `t` LIMIT 1 FOR UPDATE"
    );
    assert_eq!(
        r("select * from t for update limit 1"),
        "SELECT * FROM `t` LIMIT 1 FOR UPDATE"
    );
    assert_eq!(
        r("select * from t limit 1 order by a"),
        "SELECT * FROM `t` ORDER BY `a` LIMIT 1"
    );
    // A locking clause on a NON-final `UNION` term attaches to that term
    // specifically (a following set operator disambiguates it); on the
    // FINAL term it's the WHOLE statement's own, restoring BEFORE
    // `ORDER BY`/`LIMIT` there — the OPPOSITE order from a plain
    // `SELECT`'s own lock above (see `tidb_ast::SelectStmt::lock`'s own
    // doc for why both are real, confirmed via `godump restore`).
    assert_eq!(
        r("select * from t union select * from t2 for update"),
        "SELECT * FROM `t` UNION SELECT * FROM `t2` FOR UPDATE"
    );
    assert_eq!(
        r("select * from t for update union select * from t2"),
        "SELECT * FROM `t` FOR UPDATE UNION SELECT * FROM `t2`"
    );
    assert_eq!(
        r("select * from t order by a for update union select * from t2"),
        "SELECT * FROM `t` ORDER BY `a` FOR UPDATE UNION SELECT * FROM `t2`"
    );
    assert_eq!(
        r("select * from t union select * from t2 order by a for update"),
        "SELECT * FROM `t` UNION SELECT * FROM `t2` FOR UPDATE ORDER BY `a`"
    );
    assert_eq!(
        r("select * from t union select * from t2 for update order by a"),
        "SELECT * FROM `t` UNION SELECT * FROM `t2` FOR UPDATE ORDER BY `a`"
    );
    assert_eq!(
        r("select * from t union select * from t2 limit 1 order by a"),
        "SELECT * FROM `t` UNION SELECT * FROM `t2` ORDER BY `a` LIMIT 1"
    );
    assert_eq!(
        r("select * from t union select * from t2 limit 1 for update order by a"),
        "SELECT * FROM `t` UNION SELECT * FROM `t2` FOR UPDATE ORDER BY `a` LIMIT 1"
    );

    // Real MySQL/TiDB's own genuine `ParseError`s, confirmed via `godump
    // restore`, not assumed: a parenthesized whole-statement can't carry
    // its own locking clause; `NOWAIT` must follow `OF` (not precede
    // it); only one locking clause per `SELECT`; `LOCK IN SHARE MODE`
    // never accepts `OF`/`NOWAIT`; a second locking clause is rejected
    // even when identical to the first.
    for sql in [
        "(select * from t) for update",
        "select * from t for update nowait of t1",
        "select * from t for update, t2 for share",
        "select * from t lock in share mode nowait",
        "select * from t lock in share mode of t",
        "select * from t for update for update",
    ] {
        assert!(parse(sql).is_err(), "expected parse error for: {sql}");
    }
}

/// A 3+-term set operation's `ORDER BY`/`LIMIT`/locking-clause tail
/// rules — a real, confirmed asymmetry (via `godump restore`) between
/// the FIRST term and any LATER one: the first term can carry its own
/// `ORDER BY`/`LIMIT`/lock together as a unit (see [`select_lock`]'s own
/// tests for the 2-term case), but for term 2+, `ORDER BY`/`LIMIT`
/// NEVER attach to that specific term — they always become the WHOLE
/// statement's own, even when written on a genuinely non-final,
/// non-first term — while the LOCKING clause still sticks to that exact
/// term, matching the first term's own behavior. Not an assumption:
/// each shape here was independently confirmed via `godump restore`
/// before implementing it.
#[test]
fn three_plus_term_union_tail() {
    assert_eq!(
        r("select a from t union select b from t2 order by b union select c from t3"),
        "SELECT `a` FROM `t` UNION SELECT `b` FROM `t2` UNION SELECT `c` FROM `t3` ORDER BY `b`"
    );
    assert_eq!(
        r("select a from t union select b from t2 limit 1 union select c from t3"),
        "SELECT `a` FROM `t` UNION SELECT `b` FROM `t2` UNION SELECT `c` FROM `t3` LIMIT 1"
    );
    // The FIRST term's own `ORDER BY` survives even when a LATER term
    // also writes one — they're genuinely separate fields (the first
    // term's own vs. the whole statement's own), not one overwriting
    // the other.
    assert_eq!(
        r(
            "select a from t order by a union select b from t2 order by b union select c from t3"
        ),
        "SELECT `a` FROM `t` ORDER BY `a` UNION SELECT `b` FROM `t2` UNION SELECT `c` FROM `t3` ORDER BY `b`"
    );
    // The locking clause, unlike `ORDER BY`/`LIMIT`, sticks to whichever
    // specific term it was written on — first, middle, or otherwise.
    assert_eq!(
        r("select a from t for update union select b from t2 union select c from t3"),
        "SELECT `a` FROM `t` FOR UPDATE UNION SELECT `b` FROM `t2` UNION SELECT `c` FROM `t3`"
    );
    assert_eq!(
        r("select a from t union select b from t2 for update union select c from t3"),
        "SELECT `a` FROM `t` UNION SELECT `b` FROM `t2` FOR UPDATE UNION SELECT `c` FROM `t3`"
    );
}

/// A derived table's own body may itself be a `UNION`/`EXCEPT`/
/// `INTERSECT`-joined set operation (`(SELECT ... UNION [ALL] SELECT
/// ...) alias`), the SAME `QueryStmt::Select`-or-`QueryStmt::SetOpr` shape
/// [`tidb_ast::Cte::query`] already established for a CTE's own
/// definition, parsed via the SAME `parse_select_or_setopr` — see
/// [`tidb_ast::JoinNode::Derived`]'s own doc. `looks_like_derived_table`'s
/// own multi-`(` lookahead is what makes the doubly-parenthesized case
/// (`((SELECT ...) UNION (SELECT ...))`, each TERM independently
/// parenthesized) work — a plain 1-token lookahead can't see past the
/// first term's own wrapping paren.
#[test]
fn derived_table_set_op() {
    assert_eq!(
        r("select * from (select a from t union all select a from tv) t1 order by a"),
        "SELECT * FROM (SELECT `a` FROM `t` UNION ALL SELECT `a` FROM `tv`) AS `t1` ORDER BY `a`"
    );
    // Each term independently parenthesized, plus the derived table's
    // own outer wrapping paren — the doubly-parenthesized case.
    assert_eq!(
        r(
            "select * from ((select a as aa from t t1) union all (select b as aa from t t2)) as t3 order by aa"
        ),
        "SELECT * FROM ((SELECT `a` AS `aa` FROM `t` AS `t1`) UNION ALL (SELECT `b` AS `aa` FROM `t` AS `t2`)) AS `t3` ORDER BY `aa`"
    );
    // Two independent union-bodied derived tables, joined.
    assert_eq!(
        r(
            "select 1 from (select 1 x union all select 3) a straight_join (select 1 x union all select 2) b using (x)"
        ),
        "SELECT 1 FROM (SELECT 1 AS `x` UNION ALL SELECT 3) AS `a` STRAIGHT_JOIN (SELECT 1 AS `x` UNION ALL SELECT 2) AS `b` USING (`x`)"
    );
    // Nested inside a scalar/EXISTS subquery's own FROM clause too.
    assert_eq!(
        r(
            "select * from t where exists (select a from (select a from t1 union all select a from t2) u where t.a=u.a)"
        ),
        "SELECT * FROM `t` WHERE EXISTS (SELECT `a` FROM (SELECT `a` FROM `t1` UNION ALL SELECT `a` FROM `t2`) AS `u` WHERE `t`.`a`=`u`.`a`)"
    );
}

/// `LATERAL (subquery) [AS] alias [(col, ...)]` — see
/// `tidb_ast::JoinNode::Derived::lateral`'s own doc for the scope
/// boundary (parses fully; execution stays `Unsupported`, checked in
/// `tidb-exec`'s own tests).
#[test]
fn lateral_derived() {
    assert_eq!(
        r("select * from t1, lateral (select b from t2 where t2.a = t1.a) as lat"),
        "SELECT * FROM (`t1`) JOIN LATERAL (SELECT `b` FROM `t2` WHERE `t2`.`a`=`t1`.`a`) AS `lat`"
    );
    assert_eq!(
        r("select * from t1 left join lateral (select b from t2) as lat on true"),
        "SELECT * FROM `t1` LEFT JOIN LATERAL (SELECT `b` FROM `t2`) AS `lat` ON TRUE"
    );
    // A column alias list positionally renames the subquery's own output
    // columns.
    assert_eq!(
        r("select * from t1, lateral (select b from t2 where t2.a = t1.a) as dt(c1, c2)"),
        "SELECT * FROM (`t1`) JOIN LATERAL (SELECT `b` FROM `t2` WHERE `t2`.`a`=`t1`.`a`) AS `dt`(`c1`, `c2`)"
    );
    // A `UNION`-bodied subquery works the same way a plain derived
    // table's own set-op body does.
    assert_eq!(
        r("select * from t1, lateral (select b from t2 union select b from t3) as lat"),
        "SELECT * FROM (`t1`) JOIN LATERAL (SELECT `b` FROM `t2` UNION SELECT `b` FROM `t3`) AS `lat`"
    );
    // A column alias list on a NON-`LATERAL` derived table is a genuine
    // `ParseError` — real TiDB's own grammar only parses this list from
    // inside `parseLateralTableSource` (confirmed via `godump restore`).
    assert!(parse("select * from (select 1 as a) as dt(c1)").is_err());
}

/// `GROUP BY expr [ASC|DESC], ...` — each item carries its own
/// independent direction (confirmed via `godump restore`), unlike
/// `ORDER BY`'s plain `bool`: an explicit `ASC` restores identically to
/// no direction at all, but `tidb_ast::GroupByItem::desc` still tracks
/// it as a distinct `Some(false)` (see that type's own doc for why —
/// `tidb-exec`'s own execution-time rejection needs to tell the two
/// apart even though restore can't).
#[test]
fn group_by_direction() {
    assert_eq!(
        r("select * from t group by a"),
        "SELECT * FROM `t` GROUP BY `a`"
    );
    assert_eq!(
        r("select * from t group by a asc"),
        "SELECT * FROM `t` GROUP BY `a`"
    );
    assert_eq!(
        r("select * from t group by a desc"),
        "SELECT * FROM `t` GROUP BY `a` DESC"
    );
    assert_eq!(
        r("select * from t group by a, b desc"),
        "SELECT * FROM `t` GROUP BY `a`,`b` DESC"
    );
    assert_eq!(
        r("select * from t group by a desc, b"),
        "SELECT * FROM `t` GROUP BY `a` DESC,`b`"
    );
    assert_eq!(
        r("select * from t group by a desc, b asc"),
        "SELECT * FROM `t` GROUP BY `a` DESC,`b`"
    );
    assert_eq!(
        r("select * from t group by a desc, b desc"),
        "SELECT * FROM `t` GROUP BY `a` DESC,`b` DESC"
    );

    let stmt = parse("select * from t group by a desc, b asc, c").unwrap();
    let Stmt::Query(query) = stmt else {
        panic!("expected Query envelope")
    };
    let tidb_ast::QueryStmt::Select(s) = query.into_inner() else {
        panic!("expected SELECT query")
    };
    let dirs: Vec<Option<bool>> = s.group_by.iter().map(|item| item.desc).collect();
    assert_eq!(dirs, vec![Some(true), Some(false), None]);
}

/// `GROUP BY`/`ORDER BY` positional ordinals (`GROUP BY 1`, `ORDER BY 2`)
/// restore as plain integers, same as any other integer literal — but a
/// bare `TRUE`/`FALSE` literal in these two positions specifically restores
/// as its integer value (`1`/`0`), NOT `TRUE`/`FALSE`, confirmed via
/// `godump restore` (`tidb_ast::select::restore_by_item_expr`); `Expr::Bool`
/// restores as `TRUE`/`FALSE` everywhere else, see `boolean_literal` (or
/// similar) elsewhere in this file for the general case.
#[test]
fn group_by_order_by_position() {
    assert_eq!(
        r("select 1 from t group by 1"),
        "SELECT 1 FROM `t` GROUP BY 1"
    );
    assert_eq!(
        r("select 1 from t group by true"),
        "SELECT 1 FROM `t` GROUP BY 1"
    );
    assert_eq!(
        r("select 1 from t group by false"),
        "SELECT 1 FROM `t` GROUP BY 0"
    );
    assert_eq!(
        r("select 1,2 from t group by 1,2"),
        "SELECT 1,2 FROM `t` GROUP BY 1,2"
    );
    assert_eq!(
        r("select 1 from t order by 1"),
        "SELECT 1 FROM `t` ORDER BY 1"
    );
    assert_eq!(
        r("select 1 from t order by true"),
        "SELECT 1 FROM `t` ORDER BY 1"
    );
    assert_eq!(
        r("select 1 from t order by false"),
        "SELECT 1 FROM `t` ORDER BY 0"
    );
    // `TRUE`/`FALSE` restore normally (not as `1`/`0`) everywhere else.
    assert_eq!(r("select true"), "SELECT TRUE");
    assert_eq!(
        r("select 1 from t where true"),
        "SELECT 1 FROM `t` WHERE TRUE"
    );
    assert_eq!(
        r("select 1 from t having true"),
        "SELECT 1 FROM `t` HAVING TRUE"
    );
}

/// `USE`/`FORCE`/`IGNORE INDEX [FOR JOIN|ORDER BY|GROUP BY] (name, ...)`
/// table hints. Every assertion here was cross-checked against real
/// TiDB via `godump restore` (not assumed) — see `tidb_ast::TableRef`'s
/// own doc for why hint-name existence is NOT validated at execution
/// (deliberately out of scope, unlike real MySQL/TiDB's own
/// `Key '...' doesn't exist` check).
#[test]
fn index_hints() {
    assert_eq!(
        r("select * from t use index (idx1) where a = 1"),
        "SELECT * FROM `t` USE INDEX (`idx1`) WHERE `a`=1"
    );
    assert_eq!(
        r("select * from t force index (idx1) join t2 on t.a = t2.a"),
        "SELECT * FROM `t` FORCE INDEX (`idx1`) JOIN `t2` ON `t`.`a`=`t2`.`a`"
    );
    // `USE INDEX ()` (an empty name list) is real, valid grammar meaning
    // "use no index at all".
    assert_eq!(
        r("select * from t use index ()"),
        "SELECT * FROM `t` USE INDEX ()"
    );
    // An index name may be a keyword-shaped identifier.
    assert_eq!(
        r("select * from t use index (primary)"),
        "SELECT * FROM `t` USE INDEX (`primary`)"
    );
    assert_eq!(
        r("select * from t use index (asc)"),
        "SELECT * FROM `t` USE INDEX (`asc`)"
    );
    assert_eq!(
        r("select * from t use index (key)"),
        "SELECT * FROM `t` USE INDEX (`key`)"
    );
    // Each table in a join can carry its own hints.
    assert_eq!(
        r("select * from t ignore index (idx1), t2 use index (idx2)"),
        "SELECT * FROM (`t` IGNORE INDEX (`idx1`)) JOIN `t2` USE INDEX (`idx2`)"
    );
    // `FOR JOIN`/`FOR ORDER BY`/`FOR GROUP BY` scope qualifiers.
    assert_eq!(
        r("select * from t use index for join (idx1)"),
        "SELECT * FROM `t` USE INDEX FOR JOIN (`idx1`)"
    );
    assert_eq!(
        r("select * from t use index for order by (idx1)"),
        "SELECT * FROM `t` USE INDEX FOR ORDER BY (`idx1`)"
    );
    assert_eq!(
        r("select * from t use index for group by (idx1)"),
        "SELECT * FROM `t` USE INDEX FOR GROUP BY (`idx1`)"
    );
    // A hint follows the table's own alias.
    assert_eq!(
        r("select * from t as x use index (idx1)"),
        "SELECT * FROM `t` AS `x` USE INDEX (`idx1`)"
    );
    // Multiple hints stack on one table, each a complete, independent
    // unit (repeating its own USE/FORCE/IGNORE INDEX keyword).
    assert_eq!(
        r("select * from t use index (idx1) ignore index (idx2)"),
        "SELECT * FROM `t` USE INDEX (`idx1`) IGNORE INDEX (`idx2`)"
    );
    assert_eq!(
        r("select * from t use index (a) use index for order by (b)"),
        "SELECT * FROM `t` USE INDEX (`a`) USE INDEX FOR ORDER BY (`b`)"
    );
    // KEY is a true synonym for INDEX, normalizing to INDEX on restore.
    assert_eq!(
        r("select * from t force key (idx1)"),
        "SELECT * FROM `t` FORCE INDEX (`idx1`)"
    );
    assert_eq!(
        r("select * from t ignore key (idx1)"),
        "SELECT * FROM `t` IGNORE INDEX (`idx1`)"
    );
    // Shared by single-table UPDATE/DELETE, not just SELECT's own FROM.
    assert_eq!(
        r("update t use index (idx1) set a = 1"),
        "UPDATE `t` USE INDEX (`idx1`) SET `a`=1"
    );
    assert_eq!(
        r("delete from t use index (idx1) where a = 1"),
        "DELETE FROM `t` USE INDEX (`idx1`) WHERE `a`=1"
    );
    assert_eq!(
        r("select * from t use index (idx1, idx2)"),
        "SELECT * FROM `t` USE INDEX (`idx1`, `idx2`)"
    );

    // Real MySQL/TiDB's own genuine `ParseError`s, confirmed via `godump
    // restore`: a scope qualifier cannot chain onto a PRIOR hint without
    // repeating its own USE/FORCE/IGNORE INDEX keyword; INDEX/KEY (and
    // the paren name list) are required, not optional.
    for sql in [
        "select * from t use index for join (a) for order by (b)",
        "select * from t use index",
    ] {
        assert!(parse(sql).is_err(), "expected parse error for: {sql}");
    }
}

/// `pkg/parser/parser_test.go:TestIndexHint` uses Go's hand-parser token
/// consumption for each index name, so a quoted string is accepted even
/// though ordinary identifier slots are narrower.  The AST canonicalizes
/// that token to the same backquoted name as an unquoted identifier.
#[test]
fn go_test_index_hint_quoted_name_row() {
    assert_eq!(
        r("select * from t use index ('idx')"),
        "SELECT * FROM `t` USE INDEX (`idx`)"
    );
}

/// `PARTITION (name, ...)` table hints. Every assertion here was
/// cross-checked against real TiDB via `godump restore` (not assumed) —
/// see `tidb_ast::TableRef::partitions`'s own doc for the grammar
/// position (BEFORE the alias, the opposite of an index hint's own
/// AFTER) and this crate's own execution-time scope (always
/// `Unsupported`, unconditionally, since no table here is ever
/// partitioned).
#[test]
fn partition_hints() {
    assert_eq!(
        r("select * from t partition (p0)"),
        "SELECT * FROM `t` PARTITION(`p0`)"
    );
    assert_eq!(
        r("select * from t partition (p0, p1)"),
        "SELECT * FROM `t` PARTITION(`p0`, `p1`)"
    );
    // Go TestTablePartitionNameList inspects the AST payload directly.
    let statement =
        parse("select * from t partition (p0,p1)").expect("partition-qualified table parses");
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("expected query statement");
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        panic!("expected SELECT statement");
    };
    let from = select.from.as_ref().expect("FROM clause");
    let tidb_ast::JoinNode::Table(table) = &from.left else {
        panic!("expected table source");
    };
    assert_eq!(table.partitions, ["p0", "p1"]);
    assert_eq!(
        r("select * from t partition (p0) where a = 1"),
        "SELECT * FROM `t` PARTITION(`p0`) WHERE `a`=1"
    );
    assert_eq!(
        r("update t partition (p0) set a = 1"),
        "UPDATE `t` PARTITION(`p0`) SET `a`=1"
    );
    assert_eq!(
        r("delete from t partition (p0) where a = 1"),
        "DELETE FROM `t` PARTITION(`p0`) WHERE `a`=1"
    );
    assert_eq!(
        r("insert into t partition (p0) values (1)"),
        "INSERT INTO `t` PARTITION(`p0`) VALUES (1)"
    );
    assert_eq!(
        r("insert into t partition (p0) (a, b) values (1, 2)"),
        "INSERT INTO `t` PARTITION(`p0`) (`a`,`b`) VALUES (1,2)"
    );
    // PARTITION comes BEFORE the alias, and index hints come AFTER it.
    assert_eq!(
        r("select * from t partition (p0) as x"),
        "SELECT * FROM `t` PARTITION(`p0`) AS `x`"
    );
    assert_eq!(
        r("select * from t partition (p0) as x use index (idx1)"),
        "SELECT * FROM `t` PARTITION(`p0`) AS `x` USE INDEX (`idx1`)"
    );
    // Each table in a join can carry its own PARTITION clause.
    assert_eq!(
        r("select * from t1 partition (p0) join t2 partition (p1) on t1.a = t2.a"),
        "SELECT * FROM `t1` PARTITION(`p0`) JOIN `t2` PARTITION(`p1`) ON `t1`.`a`=`t2`.`a`"
    );

    // Real MySQL/TiDB's own genuine `ParseError`s: PARTITION after the
    // alias (the reverse order); an empty name list; a keyword-shaped
    // name (unlike an index hint's own broader acceptance); PARTITION
    // after INSERT's own column list (the reverse order there too).
    for sql in [
        "select * from t as x partition (p0)",
        "select * from t partition ()",
        "select * from t partition (key)",
        "select * from t partition (primary)",
        "select * from t partition (asc)",
        "insert into t (a, b) partition (p0) values (1, 2)",
    ] {
        assert!(parse(sql).is_err(), "expected parse error for: {sql}");
    }
}

/// `TABLESAMPLE [SYSTEM|BERNOULLI|REGION] (expr [PERCENT|ROWS])
/// [REPEATABLE(seed)]` — see `tidb_ast::TableSample`'s own doc. Every
/// assertion here was cross-checked against real TiDB via `godump
/// restore` (not assumed).
#[test]
fn table_sample() {
    assert_eq!(
        r("select * from t tablesample regions()"),
        "SELECT * FROM `t` TABLESAMPLE REGION ()"
    );
    // `REGIONS` (plural) normalizes to the singular `REGION` on restore.
    assert_eq!(
        r("select * from t tablesample region()"),
        "SELECT * FROM `t` TABLESAMPLE REGION ()"
    );
    assert_eq!(
        r("select * from t tablesample system()"),
        "SELECT * FROM `t` TABLESAMPLE SYSTEM ()"
    );
    assert_eq!(
        r("select * from t tablesample bernoulli(10 rows)"),
        "SELECT * FROM `t` TABLESAMPLE BERNOULLI (10 ROWS)"
    );
    assert_eq!(
        r("select * from t tablesample system(50 percent) repeatable(10)"),
        "SELECT * FROM `t` TABLESAMPLE SYSTEM (50 PERCENT) REPEATABLE(10)"
    );
    // No method, no expr — a bare, empty parenthesized clause is real,
    // valid grammar.
    assert_eq!(
        r("select * from t tablesample ()"),
        "SELECT * FROM `t` TABLESAMPLE ()"
    );
    // Parses AFTER the alias and AFTER index hints, in that order.
    assert_eq!(
        r("select * from t as x use index (idx1) tablesample regions()"),
        "SELECT * FROM `t` AS `x` USE INDEX (`idx1`) TABLESAMPLE REGION ()"
    );
    // Each table in a join can carry its own TABLESAMPLE clause.
    assert_eq!(
        r("select * from t1 tablesample regions(), t2 tablesample system()"),
        "SELECT * FROM (`t1` TABLESAMPLE REGION ()) JOIN `t2` TABLESAMPLE SYSTEM ()"
    );
    // `UPDATE`/`DELETE` target tables accept the same clause.
    assert_eq!(
        r("update t tablesample regions() set a = 1"),
        "UPDATE `t` TABLESAMPLE REGION () SET `a`=1"
    );
    assert_eq!(
        r("delete from t tablesample regions()"),
        "DELETE FROM `t` TABLESAMPLE REGION ()"
    );
}

/// `AS OF TIMESTAMP expr` — TiDB's own stale-read/time-travel table
/// clause, see `tidb_ast::TableRef::as_of`'s own doc. `tidb_lexer` merges
/// `AS OF` into a single keyword token (`"AS OF"`, matching real TiDB's
/// own lexer-level two-word merge, the SAME mechanism `MEMBER OF` uses).
/// Every assertion here was cross-checked against real TiDB via `godump
/// restore` (not assumed).
#[test]
fn as_of_timestamp() {
    assert_eq!(
        r("select * from t1 as of timestamp @a"),
        "SELECT * FROM `t1` AS OF TIMESTAMP @`a`"
    );
    assert_eq!(
        r("select * from t1 as of timestamp @a where a = 1"),
        "SELECT * FROM `t1` AS OF TIMESTAMP @`a` WHERE `a`=1"
    );
    // Composes with the existing date_expr +/- INTERVAL desugaring.
    assert_eq!(
        r("select * from t as of timestamp now(6) - interval 0.1 second"),
        "SELECT * FROM `t` AS OF TIMESTAMP DATE_SUB(NOW(6), INTERVAL 0.1 SECOND)"
    );
    assert_eq!(
        r("select * from t1 as of timestamp null"),
        "SELECT * FROM `t1` AS OF TIMESTAMP NULL"
    );
    // Parses BEFORE index hints, in that order.
    assert_eq!(
        r("select * from t1 as of timestamp @a use index (a)"),
        "SELECT * FROM `t1` AS OF TIMESTAMP @`a` USE INDEX (`a`)"
    );
    // Mutually exclusive with an alias, in EITHER order — a genuine
    // `ParseError` both ways.
    assert!(parse("select * from t1 as of timestamp @a as x").is_err());
    assert!(parse("select * from t1 x as of timestamp @a").is_err());
    // `UPDATE`/`DELETE` target tables accept the same clause.
    assert_eq!(
        r("update t1 as of timestamp @a set a = 1"),
        "UPDATE `t1` AS OF TIMESTAMP @`a` SET `a`=1"
    );
    assert_eq!(
        r("delete from t1 as of timestamp @a"),
        "DELETE FROM `t1` AS OF TIMESTAMP @`a`"
    );
}

/// `NATURAL [LEFT|RIGHT] JOIN`. Every assertion here was cross-checked
/// against real TiDB via `godump restore` (not assumed).
#[test]
fn natural_join() {
    assert_eq!(
        r("select * from t natural join t2"),
        "SELECT * FROM `t` NATURAL JOIN `t2`"
    );
    assert_eq!(
        r("select * from t natural left join t2"),
        "SELECT * FROM `t` NATURAL LEFT JOIN `t2`"
    );
    assert_eq!(
        r("select * from t natural right join t2"),
        "SELECT * FROM `t` NATURAL RIGHT JOIN `t2`"
    );
    // Chains left-associatively, like any other join.
    assert_eq!(
        r("select * from t natural join t2 natural join t3"),
        "SELECT * FROM (`t` NATURAL JOIN `t2`) NATURAL JOIN `t3`"
    );

    // Real MySQL/TiDB's own genuine `ParseError`s, confirmed via `godump
    // restore`: `NATURAL` cannot combine with an explicit `INNER`/
    // `CROSS` qualifier (even though bare `NATURAL JOIN` shares the same
    // underlying join type those would use) or an explicit `ON`/`USING`
    // condition (the whole point of `NATURAL` is to compute the join
    // condition implicitly).
    for sql in [
        "select * from t natural inner join t2",
        "select * from t natural cross join t2",
        "select * from t natural join t2 on t.a = t2.a",
        "select * from t natural join t2 using (a)",
    ] {
        assert!(parse(sql).is_err(), "expected parse error for: {sql}");
    }
}

/// `GROUP_CONCAT`'s own `ORDER BY` clause — see
/// `tidb_ast::Expr::GroupConcat`'s own doc for the positional-reference
/// scope (GROUP_CONCAT's own argument list, not the outer `SELECT`'s).
/// Every assertion here was cross-checked against real TiDB via `godump
/// restore` (not assumed).
#[test]
fn group_concat_order_by() {
    assert_eq!(
        r("select group_concat(distinct name order by name desc) from t"),
        "SELECT GROUP_CONCAT(DISTINCT `name` ORDER BY `name` DESC SEPARATOR ',') FROM `t`"
    );
    assert_eq!(
        r("select group_concat(name order by name desc separator '++') from t"),
        "SELECT GROUP_CONCAT(`name` ORDER BY `name` DESC SEPARATOR '++') FROM `t`"
    );
    // Multiple order items, no space after the comma (matching a regular
    // `ORDER BY` clause's own convention).
    assert_eq!(
        r("select group_concat(id order by name desc, id asc separator '--') from t"),
        "SELECT GROUP_CONCAT(`id` ORDER BY `name` DESC,`id` SEPARATOR '--') FROM `t`"
    );
    // A positional item refers to GROUP_CONCAT's OWN argument list.
    assert_eq!(
        r("select group_concat(a, b order by 1 desc, a) from t"),
        "SELECT GROUP_CONCAT(`a`, `b` ORDER BY 1 DESC,`a` SEPARATOR ',') FROM `t`"
    );
}

/// `GROUP BY expr_list WITH ROLLUP` — see `tidb_ast::SelectStmt::rollup`'s
/// own doc for the real, multi-level execution semantics this crate's own
/// execution deliberately does not replicate. Every assertion here was
/// cross-checked against real TiDB via `godump restore` (not assumed).
#[test]
fn group_by_with_rollup() {
    assert_eq!(
        r("select a, sum(b) from t group by a with rollup"),
        "SELECT `a`,SUM(`b`) FROM `t` GROUP BY `a` WITH ROLLUP"
    );
    // `WITH ROLLUP` restores BEFORE `HAVING`/`ORDER BY`.
    assert_eq!(
        r("select a, sum(b) from t group by a with rollup having sum(b) > 1"),
        "SELECT `a`,SUM(`b`) FROM `t` GROUP BY `a` WITH ROLLUP HAVING SUM(`b`)>1"
    );
    assert_eq!(
        r("select a, sum(b) from t group by a with rollup order by a"),
        "SELECT `a`,SUM(`b`) FROM `t` GROUP BY `a` WITH ROLLUP ORDER BY `a`"
    );
    assert_eq!(
        r("select a, b, sum(c) from t group by a, b with rollup"),
        "SELECT `a`,`b`,SUM(`c`) FROM `t` GROUP BY `a`,`b` WITH ROLLUP"
    );
    // A bare `GROUP BY WITH ROLLUP` (no items) is a genuine `ParseError`.
    assert!(parse("select a from t group by with rollup").is_err());
}

#[test]
fn derived_table_optional_alias() {
    // A plain derived table's alias is grammatically OPTIONAL (confirmed
    // via `godump restore`: `SELECT * FROM (SELECT 1)` alone, no alias
    // at all, is valid and restores unchanged) — unlike `LATERAL`'s own
    // alias, which stays mandatory (see `tidb_ast::JoinNode::Derived
    // ::alias`'s own doc).
    assert_eq!(r("select * from (select 1)"), "SELECT * FROM (SELECT 1)");
    assert_eq!(
        r("select * from (select 1) t"),
        "SELECT * FROM (SELECT 1) AS `t`"
    );
    assert_eq!(
        r("select * from (select 1) as t"),
        "SELECT * FROM (SELECT 1) AS `t`"
    );
}

/// Go's `parseTableSource` distinguishes an outer structural join group from
/// the inner derived-table parentheses in `((SELECT ...) alias JOIN ...)`.
/// The existing `JoinNode` tree already represents both layers, so the parser
/// must continue the inner join chain before closing the outer group.
#[test]
fn parenthesized_derived_table_join_group() {
    assert_eq!(
        r("select * from ((select a from t3) d join t4 on d.a=t4.a) join (t1 join t2 on t1.a=t2.a) on t1.a=t4.a"),
        "SELECT * FROM ((SELECT `a` FROM `t3`) AS `d` JOIN `t4` ON `d`.`a`=`t4`.`a`) JOIN (`t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`) ON `t1`.`a`=`t4`.`a`"
    );
}

/// `UNION`/`EXCEPT`/`INTERSECT` set operations with a parenthesized,
/// NESTED term (`t1 op (t2 op2 t3)`, `tidb_ast::SetOprTermBody::Nested`)
/// — cross-checked against real TiDB via `godump restore`. Real TiDB's
/// own hand-written parser wraps a parenthesized term's inner set
/// operation in a nested `SetOprSelectList` rather than flattening it
/// (`pkg/parser/select_clauses_parser.go`'s `parseSetOprRest`); this
/// crate mirrors that with `SetOprTermBody::Nested` instead of trying to
/// force everything into one flat term list.
#[test]
fn nested_set_op_term() {
    assert_eq!(
        r("select * from t1 intersect (select * from t2 except (select * from t3))"),
        "SELECT * FROM `t1` INTERSECT (SELECT * FROM `t2` EXCEPT (SELECT * FROM `t3`))"
    );
    assert_eq!(
        r("select * from t1 union (select * from t2 union all select * from t3)"),
        "SELECT * FROM `t1` UNION (SELECT * FROM `t2` UNION ALL SELECT * FROM `t3`)"
    );
    assert_eq!(
        r("select * from t1 union all (select * from t2 except select * from t3)"),
        "SELECT * FROM `t1` UNION ALL (SELECT * FROM `t2` EXCEPT SELECT * FROM `t3`)"
    );
    // A nested term composes with a derived table's own now-optional
    // alias (see `derived_table_optional_alias` above): `SELECT 1 UNION
    // SELECT 1`/`SELECT 1 INTERSECT SELECT 1` as a WHOLE derived table's
    // body, no outer alias at all.
    assert_eq!(
        r("select * from (select 1 union select 1)"),
        "SELECT * FROM (SELECT 1 UNION SELECT 1)"
    );
    assert_eq!(
        r("select * from (select 1 INTERSECT select 1)"),
        "SELECT * FROM (SELECT 1 INTERSECT SELECT 1)"
    );
    // A nested group's own scoped `ORDER BY`/`LIMIT` restores INSIDE its
    // own parens, distinct from the outer statement's own tail.
    assert_eq!(
        r("select a from t1 union (select b from t2 union all select c from t3 order by 1 limit 5)"),
        "SELECT `a` FROM `t1` UNION (SELECT `b` FROM `t2` UNION ALL SELECT `c` FROM `t3` ORDER BY 1 LIMIT 5)"
    );
    // A solitary parenthesized set operation is a statement-level wrapper;
    // its source parentheses are retained on the typed `SetOprStmt` and
    // therefore restores once. Go also accepts redundant whole-query
    // wrappers and collapses them, both at statement level and inside a
    // derived table.
    assert_eq!(r("(select 1 union select 2)"), "(SELECT 1 UNION SELECT 2)");
    assert_eq!(
        r("((select 1 union select 2))"),
        "(SELECT 1 UNION SELECT 2)"
    );
    assert_eq!(
        r("select * from ((select 1 union select 2)) t"),
        "SELECT * FROM (SELECT 1 UNION SELECT 2) AS `t`"
    );
}

/// `SQL_CALC_FOUND_ROWS` — a `SELECT`-level modifier, freely orderable
/// with `DISTINCT`/`ALL` but ALWAYS restored in a fixed position before
/// them — see `tidb_ast::SelectStmt::calc_found_rows`'s own doc.
#[test]
fn sql_calc_found_rows() {
    // The real-corpus statement.
    assert_eq!(
        r("SELECT SQL_CALC_FOUND_ROWS * FROM t1 LIMIT 1"),
        "SELECT SQL_CALC_FOUND_ROWS * FROM `t1` LIMIT 1"
    );
    // Composes with `DISTINCT`, in EITHER written order, always
    // restoring `SQL_CALC_FOUND_ROWS` first.
    assert_eq!(
        r("select sql_calc_found_rows distinct a from t1"),
        "SELECT SQL_CALC_FOUND_ROWS DISTINCT `a` FROM `t1`"
    );
    assert_eq!(
        r("select distinct sql_calc_found_rows a from t1"),
        "SELECT SQL_CALC_FOUND_ROWS DISTINCT `a` FROM `t1`"
    );
}

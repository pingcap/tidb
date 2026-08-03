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

//! Function grammars whose arguments are TYPE or UNIT KEYWORDS rather than
//! ordinary expressions, split out of `expr.rs`.
//!
//! `CAST`/`CONVERT` take a target type, `EXTRACT`/`TIMESTAMPADD`/
//! `TIMESTAMPDIFF`/`GET_FORMAT` take a unit or format keyword,
//! `DATE_ADD`/`DATE_SUB`/`ADDDATE`/`SUBDATE` take an `INTERVAL` clause, and
//! `DATE`/`TIME`/`TIMESTAMP 'lit'` are the same `Expr::Cast` node under a
//! keyword-led spelling. They share one parsing problem -- a keyword in
//! argument position that no expression parser would accept -- and one
//! restore problem, so their cases are read together here.

use super::*;

#[test]
fn date_add_sub() {
    // DATE_ADD/DATE_SUB are both lexer keywords (needed adding to
    // is_scalar_kw_func); INTERVAL is a general prefix expression, not
    // special-cased to these two function names.
    assert_eq!(
        r("select date_add(a, interval 5 day) from t"),
        "SELECT DATE_ADD(`a`, INTERVAL 5 DAY) FROM `t`"
    );
    assert_eq!(
        r("select date_sub(a, interval 5 day) from t"),
        "SELECT DATE_SUB(`a`, INTERVAL 5 DAY) FROM `t`"
    );
    // The interval value can be any expression, not just a literal.
    assert_eq!(
        r("select date_add(a, interval b day) from t"),
        "SELECT DATE_ADD(`a`, INTERVAL `b` DAY) FROM `t`"
    );
    assert_eq!(
        r("select date_add(a, interval -5 day) from t"),
        "SELECT DATE_ADD(`a`, INTERVAL -5 DAY) FROM `t`"
    );

    let stmt = parse("select date_add(a, interval 5 day) from t").unwrap();
    let Stmt::Query(query) = stmt else {
        panic!("expected Query envelope")
    };
    let tidb_ast::QueryStmt::Select(s) = query.into_inner() else {
        panic!("expected SELECT query")
    };
    let SelectField::Expr { expr, .. } = &s.fields[0] else {
        panic!("expected expr field")
    };
    assert_eq!(
        expr,
        &Expr::Func {
            name: "date_add".to_string(),
            args: vec![
                Expr::Column(vec!["a".to_string()]),
                Expr::Interval {
                    value: Box::new(Expr::Int("5".to_string())),
                    unit: "DAY".to_string(),
                },
            ],
            origin_position: 7,
        }
    );

    // `date_expr + INTERVAL amount unit` / `date_expr - INTERVAL amount
    // unit` desugar to `DATE_ADD`/`DATE_SUB` at PARSE time, confirmed
    // via `godump restore` -- NOT a `tidb-exec`-side rewrite, since
    // `DATE_ADD`/`DATE_SUB` are already fully implemented and this is
    // purely a parser-level grammar rule.
    assert_eq!(
        r("select '2020-01-01' + interval 5 day"),
        "SELECT DATE_ADD(_UTF8MB4'2020-01-01', INTERVAL 5 DAY)"
    );
    assert_eq!(
        r("select '2020-01-01' - interval 5 day"),
        "SELECT DATE_SUB(_UTF8MB4'2020-01-01', INTERVAL 5 DAY)"
    );
    // `+` is commutative here -- INTERVAL may be written on EITHER side,
    // but the non-INTERVAL operand always becomes DATE_ADD's FIRST
    // argument regardless of which side it was written on.
    assert_eq!(
        r("select interval 5 day + '2020-01-01'"),
        "SELECT DATE_ADD(_UTF8MB4'2020-01-01', INTERVAL 5 DAY)"
    );
    // Any expression works as the non-INTERVAL operand, not just a
    // date-looking one -- a purely syntactic rule, not a semantic one.
    assert_eq!(
        r("select 1 + interval 5 day"),
        "SELECT DATE_ADD(1, INTERVAL 5 DAY)"
    );
    // Runs INSIDE the precedence-climbing loop, so a chain builds on the
    // ALREADY-desugared result at each step, matching real TiDB's own
    // left-associative nesting.
    assert_eq!(
        r("select '2020-01-01' + interval 5 day + interval 3 day"),
        "SELECT DATE_ADD(DATE_ADD(_UTF8MB4'2020-01-01', INTERVAL 5 DAY), INTERVAL 3 DAY)"
    );
    assert_eq!(
        r("select '2020-01-01' - interval 5 day + interval 3 day"),
        "SELECT DATE_ADD(DATE_SUB(_UTF8MB4'2020-01-01', INTERVAL 5 DAY), INTERVAL 3 DAY)"
    );
    assert_eq!(
        r("select '2020-01-01' + interval 5 day - interval 3 day"),
        "SELECT DATE_SUB(DATE_ADD(_UTF8MB4'2020-01-01', INTERVAL 5 DAY), INTERVAL 3 DAY)"
    );
    // `-` is NOT commutative: INTERVAL as the LEFT operand of `-` is a
    // genuine `ParseError`, and so is `INTERVAL ... + INTERVAL ...`.
    assert!(parse("select interval 5 day - '2020-01-01'").is_err());
    assert!(parse("select interval 5 day + interval 3 day").is_err());
    assert!(parse("select '2020-01-01' + (interval 5 day)").is_err());
    assert!(parse("select (interval 5 day) + '2020-01-01'").is_err());
    assert!(parse("select (1, interval 5 day)").is_err());
}

#[test]
fn extract() {
    // `EXTRACT(unit FROM expr)` has its OWN grammar (unit FIRST, then
    // FROM, then value) -- the opposite argument order from `INTERVAL
    // value unit` -- and its own restore form, confirmed via `godump
    // restore`.
    assert_eq!(
        r("select extract(year from a) from t"),
        "SELECT EXTRACT(YEAR FROM `a`) FROM `t`"
    );
    // A compound unit (real MySQL/TiDB grammar) parses fine even
    // though this project's evaluator doesn't give it meaning --
    // parsing and evaluation are separate scope boundaries.
    assert_eq!(
        r("select extract(day_hour from a) from t"),
        "SELECT EXTRACT(DAY_HOUR FROM `a`) FROM `t`"
    );
    // The value can be any expression, not just a column.
    assert_eq!(
        r("select extract(year from a + 1) from t"),
        "SELECT EXTRACT(YEAR FROM `a`+1) FROM `t`"
    );

    let stmt = parse("select extract(year from a) from t").unwrap();
    let Stmt::Query(query) = stmt else {
        panic!("expected Query envelope")
    };
    let tidb_ast::QueryStmt::Select(s) = query.into_inner() else {
        panic!("expected SELECT query")
    };
    let SelectField::Expr { expr, .. } = &s.fields[0] else {
        panic!("expected expr field")
    };
    assert_eq!(
        expr,
        &Expr::Extract {
            unit: "YEAR".to_string(),
            value: Box::new(Expr::Column(vec!["a".to_string()])),
        }
    );
}

/// `CAST(expr AS type)` / `CONVERT(expr, type)` / `CONVERT(expr USING
/// charset)`. Every assertion here was cross-checked against real TiDB via
/// `godump restore` (not assumed) — see `tidb_ast::CastType`'s own doc for
/// the type-specific normalization rules this restore output encodes
/// (`DECIMAL`'s bare-vs-`(0)` distinction, `CHAR`'s default-charset-elision
/// rule, `FLOAT`'s precision-argument resolution to `FLOAT`/`DOUBLE`).
#[test]
fn cast_and_convert() {
    assert_eq!(
        r("select cast(a as signed) from t"),
        "SELECT CAST(`a` AS SIGNED) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as unsigned) from t"),
        "SELECT CAST(`a` AS UNSIGNED) FROM `t`"
    );
    assert_eq!(
        r("select cast('123' as signed integer) from t"),
        "SELECT CAST(_UTF8MB4'123' AS SIGNED) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as unsigned integer) from t"),
        "SELECT CAST(`a` AS UNSIGNED) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as char) from t"),
        "SELECT CAST(`a` AS CHAR) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as char(10)) from t"),
        "SELECT CAST(`a` AS CHAR(10)) FROM `t`"
    );
    // A `CHARSET` clause is omitted specifically when it names TiDB's own
    // DEFAULT charset (`UTF8MB4`, case-insensitively) — independent of
    // whether a length is also given (confirmed by reading real TiDB's own
    // `FieldType.RestoreAsCastType`, `pkg/parser/types/field_type.go`,
    // directly: an earlier "dropped once a length is given" hypothesis was
    // wrong, though it happened to match every case tried at the time).
    assert_eq!(
        r("select cast(a as char charset utf8) from t"),
        "SELECT CAST(`a` AS CHAR CHARSET UTF8) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as char character set utf8) from t"),
        "SELECT CAST(`a` AS CHAR CHARSET UTF8) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as char(10) charset utf8mb4) from t"),
        "SELECT CAST(`a` AS CHAR(10)) FROM `t`"
    );
    // A non-default charset survives restore even WITH a length — the
    // genuine gap the old "dropped once a length is given" rule missed.
    assert_eq!(
        r("select cast(a as char(10) charset utf8) from t"),
        "SELECT CAST(`a` AS CHAR(10) CHARSET UTF8) FROM `t`"
    );
    // `CHARSET BINARY` restores with the type keyword itself printed as
    // `BINARY` instead of `CHAR` — a restore-TEXT substitution real TiDB's
    // own restore logic applies, NOT a semantic equivalence to a genuine
    // `BINARY` cast (see `tidb_expr`'s own eval test: `CHAR(N) CHARSET
    // binary` does NOT right-pad the way `BINARY(N)` does, despite
    // restoring identically).
    assert_eq!(
        r("select cast(a as char charset binary) from t"),
        "SELECT CAST(`a` AS BINARY) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as char(10) charset binary) from t"),
        "SELECT CAST(`a` AS BINARY(10)) FROM `t`"
    );
    // `BINARY(N)` is a true fixed-width pad target (see `tidb_expr`'s own
    // eval test for the padding behavior); the parser only needs to carry
    // the length through.
    assert_eq!(
        r("select cast(a as binary) from t"),
        "SELECT CAST(`a` AS BINARY) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as binary(10)) from t"),
        "SELECT CAST(`a` AS BINARY(10)) FROM `t`"
    );
    // A bare `DECIMAL` (no parens at all) is a REAL `(10, 0)` default —
    // NOT the same as an explicit `(0)`, which is real MySQL/TiDB's own
    // sentinel for "unspecified precision" and restores with no parens at
    // all (see `tidb_ast::CastType::Decimal`'s own doc for why both cases
    // exist and why they're treated differently here).
    assert_eq!(
        r("select cast(a as decimal) from t"),
        "SELECT CAST(`a` AS DECIMAL(10)) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as decimal(5)) from t"),
        "SELECT CAST(`a` AS DECIMAL(5)) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as decimal(10,2)) from t"),
        "SELECT CAST(`a` AS DECIMAL(10, 2)) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as decimal(10,0)) from t"),
        "SELECT CAST(`a` AS DECIMAL(10)) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as decimal(0)) from t"),
        "SELECT CAST(`a` AS DECIMAL) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as decimal(0,5)) from t"),
        "SELECT CAST(`a` AS DECIMAL) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as date) from t"),
        "SELECT CAST(`a` AS DATE) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as datetime) from t"),
        "SELECT CAST(`a` AS DATETIME) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as datetime(3)) from t"),
        "SELECT CAST(`a` AS DATETIME(3)) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as time) from t"),
        "SELECT CAST(`a` AS TIME) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as time(3)) from t"),
        "SELECT CAST(`a` AS TIME(3)) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as year) from t"),
        "SELECT CAST(`a` AS YEAR) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as json) from t"),
        "SELECT CAST(`a` AS JSON) FROM `t`"
    );
    // `DOUBLE` and its `REAL` synonym never accept a parenthesized argument
    // as a CAST target (unlike `FLOAT` — see below); rejection is covered
    // by the error-case loop further down.
    assert_eq!(
        r("select cast(a as double) from t"),
        "SELECT CAST(`a` AS DOUBLE) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as real) from t"),
        "SELECT CAST(`a` AS DOUBLE) FROM `t`"
    );
    // `FLOAT`'s own precision argument (one OR two numbers — a second `D`
    // argument is accepted but has no bearing on the resolved type)
    // resolves to `FLOAT` or `DOUBLE` AT PARSE TIME depending on the FIRST
    // number alone: `<= 24` stays `FLOAT`, `25..=53` becomes `DOUBLE`
    // (confirmed via `godump restore`; `> 53` is a genuine `ParseError`,
    // covered by the error-case loop below).
    assert_eq!(
        r("select cast(a as float) from t"),
        "SELECT CAST(`a` AS FLOAT) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as float(10)) from t"),
        "SELECT CAST(`a` AS FLOAT) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as float(24)) from t"),
        "SELECT CAST(`a` AS FLOAT) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as float(25)) from t"),
        "SELECT CAST(`a` AS DOUBLE) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as float(53)) from t"),
        "SELECT CAST(`a` AS DOUBLE) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as float(10,2)) from t"),
        "SELECT CAST(`a` AS FLOAT) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as float(53,10)) from t"),
        "SELECT CAST(`a` AS DOUBLE) FROM `t`"
    );
    // `CONVERT(expr, type)` is the exact same operation as `CAST(expr AS
    // type)` (see `tidb_ast::Expr::Cast`'s own doc) but restores with its
    // own distinct syntax; `CONVERT(expr USING charset)` is a genuinely
    // different node (`Expr::ConvertUsing`, a charset conversion).
    assert_eq!(
        r("select convert(a, signed) from t"),
        "SELECT CONVERT(`a`, SIGNED) FROM `t`"
    );
    assert_eq!(
        r("select convert(a using utf8) from t"),
        "SELECT CONVERT(`a` USING 'utf8') FROM `t`"
    );
    assert_eq!(
        r("select convert(a using utf8mb4) from t"),
        "SELECT CONVERT(`a` USING 'utf8mb4') FROM `t`"
    );
    // Interacts correctly with the surrounding grammar: aliasing, unary
    // negation (both outside and of the cast's own operand), `IS NULL`,
    // `NULL`/subquery/binary-expression operands.
    assert_eq!(
        r("select cast(a as signed) as x from t"),
        "SELECT CAST(`a` AS SIGNED) AS `x` FROM `t`"
    );
    assert_eq!(
        r("select -cast(a as signed) from t"),
        "SELECT -CAST(`a` AS SIGNED) FROM `t`"
    );
    assert_eq!(
        r("select cast(-a as signed) from t"),
        "SELECT CAST(-`a` AS SIGNED) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as signed) is null from t"),
        "SELECT CAST(`a` AS SIGNED) IS NULL FROM `t`"
    );
    assert_eq!(
        r("select cast(null as signed) from t"),
        "SELECT CAST(NULL AS SIGNED) FROM `t`"
    );
    assert_eq!(
        r("select cast((select b from t2) as signed) from t"),
        "SELECT CAST((SELECT `b` FROM `t2`) AS SIGNED) FROM `t`"
    );
    assert_eq!(
        r("select cast(a+1 as signed) from t"),
        "SELECT CAST(`a`+1 AS SIGNED) FROM `t`"
    );

    let stmt = parse("select cast(a as decimal(10,2)) from t").unwrap();
    let Stmt::Query(query) = stmt else {
        panic!("expected Query envelope")
    };
    let tidb_ast::QueryStmt::Select(s) = query.into_inner() else {
        panic!("expected SELECT query")
    };
    let SelectField::Expr { expr, .. } = &s.fields[0] else {
        panic!("expected expr field")
    };
    assert_eq!(
        expr,
        &Expr::Cast(tidb_ast::CastExpr {
            expr: Box::new(Expr::Column(vec!["a".to_string()])),
            cast_type: tidb_ast::CastType::Decimal { flen: 10, scale: 2 },
            style: tidb_ast::CastStyle::Cast,
            array: false,
        })
    );

    // Real MySQL/TiDB's own genuine `ParseError`s for this grammar,
    // confirmed via `godump restore`, not assumed: plain `INT`/`INTEGER`/
    // `BOOL`/`BOOLEAN`/`NCHAR` are valid COLUMN types but not CAST targets;
    // `DOUBLE`/`REAL` never take a parenthesized argument (unlike `FLOAT`);
    // a `FLOAT` precision past `53` overflows; `YEAR`/`DATE`/`SIGNED` never
    // take a parenthesized argument at all; an empty `()` is invalid
    // anywhere a length is otherwise optional.
    for sql in [
        "select cast(a as int) from t",
        "select cast(a as integer) from t",
        "select cast(a as bool) from t",
        "select cast(a as boolean) from t",
        "select cast(a as nchar) from t",
        "select cast(a as double(10,2)) from t",
        "select cast(a as double(10)) from t",
        "select cast(a as real(5)) from t",
        "select cast(a as float(60,10)) from t",
        "select cast(a as float(54)) from t",
        "select cast(a as year(4)) from t",
        "select cast(a as date(3)) from t",
        "select cast(a as signed(10)) from t",
        "select cast(a as char()) from t",
    ] {
        assert!(parse(sql).is_err(), "expected parse error for: {sql}");
    }
}

/// `CAST(expr AS type ARRAY)` / `CONVERT(expr, type ARRAY)` — a trailing
/// `ARRAY` suffix, a JSON multi-valued-index type modifier applying
/// uniformly to any base type (see `tidb_ast::CastExpr::array`'s own
/// doc). `JSON_SUM_CRC32(expr AS type ARRAY)` reuses the SAME
/// `CastExpr` payload under its own `CastStyle` (see
/// `tidb_ast::CastStyle::JsonSumCrc32`'s own doc) — the `ARRAY` suffix
/// is MANDATORY there, unlike `CAST`/`CONVERT`'s own optional suffix.
#[test]
fn cast_array() {
    assert_eq!(
        r("select cast(a as signed array) from t"),
        "SELECT CAST(`a` AS SIGNED ARRAY) FROM `t`"
    );
    assert_eq!(
        r("select cast(a as double array)"),
        "SELECT CAST(`a` AS DOUBLE ARRAY)"
    );
    assert_eq!(
        r("select convert(a, signed array) from t"),
        "SELECT CONVERT(`a`, SIGNED ARRAY) FROM `t`"
    );
    assert_eq!(
        r("select json_sum_crc32(j as signed array) from t"),
        "SELECT JSON_SUM_CRC32(`j` AS SIGNED ARRAY) FROM `t`"
    );
    assert_eq!(
        r("select json_sum_crc32(j as char(10) array) from t"),
        "SELECT JSON_SUM_CRC32(`j` AS CHAR(10) ARRAY) FROM `t`"
    );
    // `ARRAY` is MANDATORY for `JSON_SUM_CRC32` — a genuine `ParseError`
    // without it, confirmed via `godump restore`.
    assert!(parse("select json_sum_crc32(j as signed) from t").is_err());
}

/// `TIMESTAMPADD(unit, interval, datetime_expr)` / `TIMESTAMPDIFF(unit,
/// expr1, expr2)` / `GET_FORMAT(DATE|TIME|DATETIME|TIMESTAMP,
/// format_expr)` — see `tidb_ast::Expr::TimestampAdd`'s own doc for why
/// each function's own first argument is a dedicated field, not an
/// ordinary parsed argument expression. Every assertion here was
/// cross-checked against real TiDB via `godump restore` (not assumed).
#[test]
fn timestamp_arith_functions() {
    assert_eq!(
        r("select timestampadd(hour, 1, '2025-03-30 02:30:00')"),
        "SELECT TIMESTAMPADD(HOUR, 1, _UTF8MB4'2025-03-30 02:30:00')"
    );
    assert_eq!(
        r("select timestampadd(hour, 1, ts) from t"),
        "SELECT TIMESTAMPADD(HOUR, 1, `ts`) FROM `t`"
    );
    assert_eq!(
        r("select timestampdiff(hour, '2025-03-30 01:59:59', ts) from t"),
        "SELECT TIMESTAMPDIFF(HOUR, _UTF8MB4'2025-03-30 01:59:59', `ts`) FROM `t`"
    );
    assert_eq!(
        r("select get_format(date, 'jis')"),
        "SELECT GET_FORMAT(DATE, _UTF8MB4'jis')"
    );
    // `TIMESTAMP` normalizes to `DATETIME` — both restore identically.
    assert_eq!(
        r("select get_format(timestamp, 'eur')"),
        "SELECT GET_FORMAT(DATETIME, _UTF8MB4'eur')"
    );
    assert_eq!(
        r("select get_format(datetime, 'iso')"),
        "SELECT GET_FORMAT(DATETIME, _UTF8MB4'iso')"
    );
    assert!(parse("select date_add('2008-01-34', 5)").is_err());
    assert!(parse("select date_sub('2008-01-34', 5)").is_err());
}

#[test]
fn timestampdiff_uses_go_single_unit_grammar() {
    let statement = parse(
        "SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01'), TIMESTAMPDIFF(month,'2003-02-01','2003-05-01')",
    )
    .unwrap();
    let Stmt::Query(query) = statement else {
        panic!("expected query")
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        panic!("expected SELECT")
    };
    for field in &select.fields {
        let SelectField::Expr {
            expr: Expr::TimestampDiff { unit, .. },
            ..
        } = field
        else {
            panic!("expected TIMESTAMPDIFF")
        };
        assert_eq!(unit, "MONTH");
    }

    for unit in [
        "SECOND_MICROSECOND",
        "MINUTE_MICROSECOND",
        "MINUTE_SECOND",
        "HOUR_MICROSECOND",
        "HOUR_SECOND",
        "HOUR_MINUTE",
        "DAY_MICROSECOND",
        "DAY_SECOND",
        "DAY_MINUTE",
        "DAY_HOUR",
        "YEAR_MONTH",
    ] {
        assert!(
            parse(&format!(
                "SELECT TIMESTAMPDIFF({unit},'2003-02-01','2003-05-01')"
            ))
            .is_err(),
            "{unit}"
        );
    }

    assert!(parse("SELECT TIMESTAMPADD(SQL_TSI_MICROSECOND,1,'2003-01-02')").is_err());
    assert!(parse("SELECT TIMESTAMPADD(BOOLEAN,1,'2003-01-02')").is_err());
}

/// `ADDDATE`/`SUBDATE(date, interval_or_days)` — see
/// `tidb_ast::Expr::TimestampAdd`'s own doc reference in
/// `parse_adddate_or_subdate` for why a bare numeric second argument
/// implicitly means `INTERVAL n DAY`. Every assertion here was
/// cross-checked against real TiDB via `godump restore` (not assumed).
#[test]
fn adddate_subdate_implicit_interval() {
    assert_eq!(
        r("select adddate('2008-01-34', -1)"),
        "SELECT ADDDATE(_UTF8MB4'2008-01-34', INTERVAL -1 DAY)"
    );
    assert_eq!(
        r("select adddate('2008-01-34', interval -1 day)"),
        "SELECT ADDDATE(_UTF8MB4'2008-01-34', INTERVAL -1 DAY)"
    );
    assert_eq!(
        r("select subdate('2008-01-34', 5)"),
        "SELECT SUBDATE(_UTF8MB4'2008-01-34', INTERVAL 5 DAY)"
    );
    assert_eq!(
        r("select adddate('2008-01-34', 5.5)"),
        "SELECT ADDDATE(_UTF8MB4'2008-01-34', INTERVAL 5.5 DAY)"
    );
}

/// `DATE`/`TIME`/`TIMESTAMP 'literal'` — an ODBC-style typed literal,
/// reusing `Expr::Cast` under three new `CastStyle` variants (see
/// `tidb_ast::CastStyle::DateLiteral`'s own doc). Restores with NO
/// `_UTF8MB4` charset-introducer prefix, unlike an ordinary standalone
/// string literal — every assertion here cross-checked against real TiDB
/// via `godump restore`. Only recognized when the keyword is immediately
/// followed by a string literal; otherwise `DATE`/`TIME`/`TIMESTAMP` stay
/// ordinary non-reserved keywords (a bare column reference or a scalar
/// function call), unaffected.
#[test]
fn typed_date_time_literal() {
    assert_eq!(r("select date '2020-01-01'"), "SELECT DATE '2020-01-01'");
    // Real TiDB accepts (and restores unchanged) genuinely invalid dates
    // here too — this is a pure syntax-level literal, not validated at
    // parse time.
    assert_eq!(r("select date '2007-10-00'"), "SELECT DATE '2007-10-00'");
    assert_eq!(
        r("select time '-1 12:00:01.341300'"),
        "SELECT TIME '-1 12:00:01.341300'"
    );
    assert_eq!(
        r("select timestamp '9999-01-01 00:00:00'"),
        "SELECT TIMESTAMP '9999-01-01 00:00:00'"
    );
    assert_eq!(
        r("select timestamp 'invalid-date'"),
        "SELECT TIMESTAMP 'invalid-date'"
    );
    // Composes normally with a following binary operator.
    assert_eq!(
        r("select date '2007-10-00' + 1"),
        "SELECT DATE '2007-10-00'+1"
    );
    assert_eq!(
        r("select addtime(date '2024-11-01', time '1 12:00:01.341300') from t"),
        "SELECT ADDTIME(DATE '2024-11-01', TIME '1 12:00:01.341300') FROM `t`"
    );
    // Not immediately followed by a string literal: DATE/TIME/TIMESTAMP
    // stay ordinary non-reserved keywords, unaffected.
    assert_eq!(r("select date(x) from t"), "SELECT DATE(`x`) FROM `t`");
    assert_eq!(r("select date from t"), "SELECT `date` FROM `t`");
    assert_eq!(
        r("select cast(x as date) from t"),
        "SELECT CAST(`x` AS DATE) FROM `t`"
    );
}

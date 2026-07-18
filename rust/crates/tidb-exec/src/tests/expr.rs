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

//! Evaluation-level behavior reached through real execution:
//! typed columns, builtin functions, predicates, and the
//! honest rejections/column-pinning cases.

use super::*;

#[test]
fn decimal_columns() {
    let mut db = Database::new();
    step(&mut db, "create table price (id int, amt decimal(10,2))");
    step(&mut db, "insert into price values (1, 9.99)");
    step(&mut db, "insert into price values (2, 19.99)");
    step(&mut db, "insert into price values (3, 5.00)");
    assert_eq!(
        step(&mut db, "select id, amt from price"),
        "RS:1|9.99;2|19.99;3|5.00"
    );
    assert_eq!(
        step(&mut db, "select id, amt from price where amt > 10.00"),
        "RS:2|19.99"
    );
    assert_eq!(
        step(&mut db, "select amt from price order by amt desc"),
        "RS:19.99;9.99;5.00"
    );
    assert_eq!(
        step(&mut db, "select amt + 1.00 from price where id = 1"),
        "RS:10.99"
    );
    assert_eq!(
        step(&mut db, "select max(amt), min(amt) from price"),
        "RS:19.99|5.00"
    );
    // SUM folds via the same exact `+` a constant expression uses, so a
    // decimal column's SUM needs no rounding either.
    assert_eq!(step(&mut db, "select sum(amt) from price"), "RS:34.98");

    step(
        &mut db,
        "create table sale (cat varchar(9), amt decimal(10,2))",
    );
    step(
        &mut db,
        "insert into sale values ('a', 10.50), ('a', 5.25), ('b', 3.00)",
    );
    assert_eq!(
        step(&mut db, "select cat, sum(amt) from sale group by cat"),
        "RS:a|15.75;b|3.00"
    );
    assert_eq!(
        step(&mut db, "select sum(amt) from sale where cat = 'z'"),
        "RS:<nil>"
    );
}

#[test]
fn float_columns() {
    let mut db = Database::new();
    step(&mut db, "create table price (id int, amt double)");
    step(&mut db, "insert into price values (1, 1.5e2)");
    step(&mut db, "insert into price values (2, 2.5e2)");
    step(&mut db, "insert into price values (3, 4e2)");
    assert_eq!(
        step(&mut db, "select id, amt from price"),
        "RS:1|150;2|250;3|400"
    );
    assert_eq!(
        step(&mut db, "select id, amt from price where amt > 200"),
        "RS:2|250;3|400"
    );
    assert_eq!(
        step(&mut db, "select amt from price order by amt desc"),
        "RS:400;250;150"
    );
    assert_eq!(
        step(&mut db, "select amt + 1 from price where id = 1"),
        "RS:151"
    );
    assert_eq!(
        step(&mut db, "select max(amt), min(amt) from price"),
        "RS:400|150"
    );
    assert_eq!(step(&mut db, "select sum(amt) from price"), "RS:800");
    // AVG over a real DOUBLE column is plain sum/count -- NOT MySQL's
    // Decimal div_precision_increment scale-growth rule (confirmed via
    // gorun, not assumed).
    assert_eq!(
        step(&mut db, "select avg(amt) from price"),
        "RS:266.6666666666667"
    );

    step(&mut db, "create table sale (cat varchar(9), amt double)");
    step(
        &mut db,
        "insert into sale values ('a', 10.5e0), ('a', 5.25e0), ('b', 3e0)",
    );
    assert_eq!(
        step(&mut db, "select cat, sum(amt) from sale group by cat"),
        "RS:a|15.75;b|3"
    );
    assert_eq!(
        step(&mut db, "select cat, avg(amt) from sale group by cat"),
        "RS:a|7.875;b|3"
    );
    assert_eq!(
        step(&mut db, "select sum(amt) from sale where cat = 'z'"),
        "RS:<nil>"
    );
}

#[test]
fn now_current_timestamp() {
    let mut db = Database::new();
    // TiDB's default timestamp mode uses the current statement's cached
    // wall clock; exact text is intentionally not static-testable.
    assert!(step(&mut db, "select now()").starts_with("RS:"));
    step(&mut db, "set time_zone = '+00:00'");
    step(&mut db, "set timestamp = 1700000000.5");
    assert_eq!(step(&mut db, "select now()"), "RS:2023-11-14 22:13:20");
    // NOW/CURRENT_TIMESTAMP are true synonyms; CURRENT_TIMESTAMP also
    // parses bare (no parens at all).
    assert_eq!(step(&mut db, "select now() = current_timestamp()"), "RS:1");
    assert_eq!(
        step(&mut db, "select current_timestamp"),
        "RS:2023-11-14 22:13:20"
    );
    // The fractional part TRUNCATES (never rounds) to the requested
    // 0-6 precision.
    assert_eq!(step(&mut db, "select now(1)"), "RS:2023-11-14 22:13:20.5");
    assert_eq!(
        step(&mut db, "select now(6)"),
        "RS:2023-11-14 22:13:20.500000"
    );
    // `time_zone` shifts the rendered wall-clock time (positive and
    // negative offsets), while the underlying epoch stays fixed.
    step(&mut db, "set time_zone = '-05:00'");
    assert_eq!(step(&mut db, "select now()"), "RS:2023-11-14 17:13:20");
    step(&mut db, "set time_zone = '+05:30'");
    assert_eq!(step(&mut db, "select now()"), "RS:2023-11-15 03:43:20");
    step(&mut db, "set time_zone = '+00:00'");

    // The clock is fixed for a WHOLE statement's execution, not just
    // table-less SELECT: reachable from projection, WHERE, UPDATE SET,
    // and DELETE WHERE alike (INSERT VALUES is out of scope — rows are
    // const-evaluated, a pre-existing boundary).
    step(&mut db, "create table evt (id int, stamp varchar(32))");
    step(&mut db, "insert into evt values (1, 'x'), (2, 'y')");
    step(&mut db, "update evt set stamp = now() where id = 1");
    step(&mut db, "update evt set stamp = now(3) where id = 2");
    assert_eq!(
        step(&mut db, "select id, stamp from evt"),
        "RS:1|2023-11-14 22:13:20;2|2023-11-14 22:13:20.500"
    );
    assert_eq!(
        step(&mut db, "select id from evt where stamp = now()"),
        "RS:1"
    );
    // The fractional stamp never equals the whole-second NOW(), so this
    // deletes nothing.
    step(&mut db, "delete from evt where stamp = now() and id = 2");
    assert_eq!(step(&mut db, "select count(*) from evt"), "RS:2");
}

#[test]
fn curdate_curtime_utc_family() {
    let mut db = Database::new();
    step(&mut db, "set time_zone = '+05:30'");
    step(&mut db, "set timestamp = 1700000000.654321");
    // CURDATE/CURRENT_DATE/CURTIME/CURRENT_TIME use the LOCAL
    // (time_zone-adjusted) clock, like NOW.
    assert_eq!(step(&mut db, "select curdate()"), "RS:2023-11-15");
    assert_eq!(step(&mut db, "select current_date()"), "RS:2023-11-15");
    assert_eq!(step(&mut db, "select curtime()"), "RS:03:43:20");
    assert_eq!(step(&mut db, "select current_time()"), "RS:03:43:20");
    assert_eq!(step(&mut db, "select curtime(3)"), "RS:03:43:20.654");
    // CURDATE takes no argument at all; Go rejects this at parse time.
    assert!(tidb_parser::parse("select curdate(1)").is_err());

    // UTC_TIMESTAMP/UTC_DATE/UTC_TIME use the RAW UTC clock, ignoring
    // `time_zone` entirely.
    assert_eq!(step(&mut db, "select utc_date()"), "RS:2023-11-14");
    // UTC_TIMESTAMP ALWAYS ROUNDS (ties away from zero) -- for BOTH the
    // 0-arg and explicit-arg forms alike, unlike NOW's uniform
    // truncation and unlike CURTIME/UTC_TIME's 0-arg/explicit-arg
    // split below (confirmed via reading `evalUTCTimestampWithFsp` in
    // `pkg/expression/builtin_time.go`, not assumed).
    assert_eq!(
        step(&mut db, "select utc_timestamp()"),
        "RS:2023-11-14 22:13:21"
    );
    assert_eq!(
        step(&mut db, "select utc_timestamp(0)"),
        "RS:2023-11-14 22:13:21"
    );
    assert_eq!(
        step(&mut db, "select utc_timestamp(3)"),
        "RS:2023-11-14 22:13:20.654"
    );
    // CURTIME/UTC_TIME instead SPLIT: the 0-arg form truncates, but an
    // EXPLICIT argument -- even literally `0` -- rounds. Set
    // time_zone to +00:00 first so CURTIME's local time equals
    // UTC_TIME's, isolating the 0-arg-vs-explicit-arg effect.
    step(&mut db, "set time_zone = '+00:00'");
    assert_eq!(step(&mut db, "select curtime()"), "RS:22:13:20"); // truncates
    assert_eq!(step(&mut db, "select curtime(0)"), "RS:22:13:21"); // rounds
    assert_eq!(step(&mut db, "select utc_time()"), "RS:22:13:20"); // truncates
    assert_eq!(step(&mut db, "select utc_time(0)"), "RS:22:13:21"); // rounds
}

#[test]
fn case_when() {
    let mut db = Database::new();
    step(&mut db, "create table t (id int, v int)");
    step(&mut db, "insert into t values (1, 10), (2, 0), (3, 30)");
    // LAZY evaluation: the untaken `100 / v` branch never runs for
    // v = 0, so no division-by-zero error -- a load-bearing SQL idiom
    // this executor must preserve.
    assert_eq!(
        step(
            &mut db,
            "select id, case when v != 0 then 100 / v else null end from t"
        ),
        "RS:1|10.0000;2|<nil>;3|3.3333"
    );
    // Simple form.
    assert_eq!(
        step(
            &mut db,
            "select id, v, case v when 10 then 'ten' when 30 then 'thirty' else 'other' end from t"
        ),
        "RS:1|10|ten;2|0|other;3|30|thirty"
    );

    step(&mut db, "create table dept (id int, name varchar(20))");
    step(&mut db, "insert into dept values (1, 'eng'), (2, 'sales')");
    // A subquery inside a CASE branch resolves correctly -- needed a
    // new `Expr::Case` arm in `resolve_subqueries`'s structural walk
    // (previously a silent `other => other.clone()` fallback would
    // have left it unresolved).
    assert_eq!(
        step(
            &mut db,
            "select id, case when (select count(*) from dept) > 1 then 'multi' else 'single' end from t where id = 1"
        ),
        "RS:1|multi"
    );
    // An aggregate inside a CASE branch also resolves correctly --
    // needed a new `Expr::Case` arm in `expr_has_aggregate` (so the
    // query is even recognized as aggregating) and in `eval_group`
    // (so the branch selection folds the aggregate over the group).
    assert_eq!(
        step(
            &mut db,
            "select case when count(*) > 2 then 'many' else 'few' end from t"
        ),
        "RS:many"
    );

    // WHERE, ORDER BY, GROUP BY, and UPDATE SET all accept CASE too.
    assert_eq!(
        step(
            &mut db,
            "select id from t where case when v > 15 then 1 else 0 end = 1"
        ),
        "RS:3"
    );
    assert_eq!(
        step(
            &mut db,
            "select id from t order by case when v = 0 then 1 else 0 end, id"
        ),
        "RS:1;3;2"
    );
    assert_eq!(
        step(
            &mut db,
            "select case when v > 15 then 'big' else 'small' end, count(*) from t group by case when v > 15 then 'big' else 'small' end"
        ),
        "RS:big|1;small|2"
    );
    step(
        &mut db,
        "update t set v = case when v = 0 then -1 else v end where id = 2",
    );
    assert_eq!(
        step(&mut db, "select id, v from t order by id"),
        "RS:1|10;2|-1;3|30"
    );
}

#[test]
fn like_non_string_operand() {
    // Regression: `Expr::Like` used to reject any non-string operand
    // outright, but real TiDB implicitly stringifies (confirmed via
    // `gorun`) -- both a plain non-aggregated column and an aggregate
    // hidden inside `LIKE` (the case explicitly flagged, unfixed, at
    // the end of the previous turn).
    let mut db = Database::new();
    step(&mut db, "create table t (id int, v int)");
    step(&mut db, "insert into t values (1, 10), (2, 20)");
    assert_eq!(step(&mut db, "select v from t where v like '10'"), "RS:10");
    assert_eq!(step(&mut db, "select v from t where v like '1%'"), "RS:10");
    assert_eq!(
        step(
            &mut db,
            "select id, count(*) from t group by id having count(*) like '1'"
        ),
        "RS:1|1;2|1"
    );
}

#[test]
fn like_escape_clause_eval() {
    // A custom escape character makes the character immediately following
    // it literal (including `%`/`_`, which would otherwise be wildcards),
    // confirmed via `gorun`.
    assert_eq!(run("select 'a+b' like '+a%' escape '+'"), "RS:1");
    assert_eq!(run("select '+a' like '+a%' escape '+'"), "RS:0");
    assert_eq!(run("select '%a' like '+%a' escape '+'"), "RS:1");
    assert_eq!(run("select 'aXb' like 'aX%' escape 'X'"), "RS:0");
    assert_eq!(run("select 'a%b' like 'aX%' escape 'X'"), "RS:0");
}

/// `[NOT] REGEXP`/`RLIKE` against a real table column (constant-only
/// evaluation is covered by `tidb_expr`'s own `regexp_expr_eval`) —
/// this workspace's first external dependency, the `regex` crate. All
/// values `gorun`-verified.
#[test]
fn regexp_expr_eval() {
    let mut db = Database::new();
    step(&mut db, "create table t (a varchar(20))");
    step(
        &mut db,
        "insert into t values ('apple'), ('banana'), ('cherry')",
    );
    assert_eq!(
        step(&mut db, "select a from t where a regexp '^b'"),
        "RS:banana"
    );
    assert_eq!(
        step(
            &mut db,
            "select a from t where a not regexp '^b' order by a"
        ),
        "RS:apple;cherry"
    );
}

/// `Expr::Hex`/`Expr::Bit` literal evaluation — see
/// `tidb_expr::binary_literal`'s own doc for the exact rules (raw-byte
/// `Datum::Bytes` in general/string context; arithmetic coercion stays
/// `Unsupported`, while arbitrary non-UTF8 byte sequences remain lossless).
/// All confirmed via
/// `gorun` before implementing.
#[test]
fn hex_bit_literal_eval() {
    // General/string context: the literal's own raw bytes, not its
    // numeric value.
    assert_eq!(run("select 0x41"), "RS:A");
    assert_eq!(run("select concat('x', 0x41)"), "RS:xA");
    assert_eq!(run("select length(0x1A)"), "RS:1");
    assert_eq!(run("select concat('a', b'1100001')"), "RS:aa");
    // The empty bit literal is the empty string; every-bit-zero forms
    // (`b'0'`, `0x00`) are a single NUL byte, not an empty string.
    assert_eq!(run("select length(b'')"), "RS:0");
    assert_eq!(run("select length(b'0')"), "RS:1");
    assert_eq!(run("select length(0x0)"), "RS:1");
    // `CHAR_LENGTH` signature selection is source-typed before evaluation:
    // the same UTF-8 payload is three bytes for binary literal/function/cast
    // results, but one character for an ordinary string or character cast.
    assert_eq!(run("select char_length(0xE4BDA0)"), "RS:3");
    assert_eq!(run("select char_length('你')"), "RS:1");
    assert_eq!(run("select char_length(cast('你' as binary))"), "RS:3");
    assert_eq!(run("select char_length(unhex('E4BDA0'))"), "RS:3");
    assert_eq!(run("select char_length(char(228,189,160))"), "RS:3");
    assert_eq!(run("select char_length(from_base64('5L2g'))"), "RS:3");
    assert_eq!(run("select char_length(0xF0288C28)"), "RS:4");
    assert_eq!(run("select char_length(cast(0xE4BDA0 as char))"), "RS:1");
    // A lone byte that isn't valid UTF-8 on its own (e.g. `0xFF`, or a
    // multi-byte literal that happens to end in one) is retained exactly and
    // represented reversibly by the differential result-cell transport.
    let ff = execute(&tidb_parser::parse("select 0xFF").unwrap()).unwrap();
    assert_eq!(ff.rows[0][0], Datum::new_bytes(vec![0xff]));
    assert_eq!(ff.label(), "RS:BYTES_HEX:FF");

    let wide_bit = execute(&tidb_parser::parse("select b'111111111'").unwrap()).unwrap();
    assert_eq!(wide_bit.rows[0][0], Datum::new_bytes(vec![0x01, 0xff]));
    assert_eq!(wide_bit.label(), "RS:BYTES_HEX:01FF");
    // Arithmetic coercion (`0x1A + 1` is `27` in real TiDB) is
    // deliberately NOT implemented — stays `Unsupported`, unchanged
    // from before this feature existed (see `tidb_expr::binary_literal`'s
    // own doc for why).
    assert!(run("select 0x1A + 1").starts_with("Eval(Unsupported("));
}

/// Regression: `Expr::Collate` (and the same-shaped new `Expr::Regexp`)
/// were missing from SEVEN structural expression-traversal functions
/// across `aggregate.rs`/`order.rs`/`subquery.rs`/`window.rs` — all of
/// them mirror `Expr::Like`'s own recursion but, before this fix, fell
/// through to each function's own generic wildcard for these two newer
/// variants, silently skipping recursion into their `expr`/`pattern`
/// sub-expressions. Caught only by reasoning through the analogy to
/// `Expr::Like` (a 2026-07-13 turn implementing `Expr::Regexp` noticed
/// `Expr::Like` had explicit handling in far more places than the new
/// variants), not by a failing test — this one exists so it can never
/// regress silently again (`REGEXP` also evaluates now, see
/// `tidb_expr::regexp`'s own doc — but this specific traversal-recursion
/// regression test only needs ONE of the two to exercise the code path,
/// and `COLLATE` was already in place when it was written). `COLLATE`
/// is the case exercised here: a `HAVING` clause referencing a
/// select-list ALIAS through a `COLLATE` wrapper needs
/// `crate::order::resolve_having_aliases` to recurse into it, confirmed
/// against real TiDB via `gorun`.
#[test]
fn collate_having_alias_resolution() {
    let mut db = Database::new();
    step(&mut db, "create table cht1 (a varchar(10))");
    step(&mut db, "insert into cht1 values ('x'), ('y'), ('x')");
    assert_eq!(
        step(
            &mut db,
            "select a as n, count(*) c from cht1 group by a having n collate utf8mb4_bin = 'x'"
        ),
        "RS:x|2"
    );
}

/// Regression: the SAME 7 traversal functions (see
/// `collate_having_alias_resolution`'s own doc) were ALSO missing
/// `Expr::Cast`/`Expr::ConvertUsing` — a genuinely PRE-EXISTING gap (not
/// introduced by the `Regexp`/`Collate` additions those two variants'
/// own fix turn found), affecting an already-shipped, heavily-used
/// feature. Confirmed against real TiDB via `gorun`: a `HAVING` clause
/// referencing a select-list alias through a `CAST` wrapper needs
/// `crate::order::resolve_having_aliases` to recurse into it, exactly
/// like the `COLLATE` case.
#[test]
fn cast_having_alias_resolution() {
    let mut db = Database::new();
    step(&mut db, "create table caht1 (a int)");
    step(&mut db, "insert into caht1 values (1), (2), (1)");
    assert_eq!(
        step(
            &mut db,
            "select a as n, count(*) c from caht1 group by a having cast(n as signed) = 1"
        ),
        "RS:1|2"
    );
}

/// Regression: the SAME 7 traversal functions (see
/// `collate_having_alias_resolution`'s own doc) were ALSO missing
/// `Expr::MatchAgainst` when it was introduced — added in the SAME pass as
/// the feature itself this time (unlike `Cast`/`ConvertUsing`, which slipped
/// through for several turns), per this project's own "draw inferences
/// about other cases from one instance" practice. `MATCH(...) AGAINST(...)`
/// always evaluates as `Unsupported` (no fulltext domain at all), so a
/// `step()`-level test can't observe recursion via a SUCCESSFUL result the
/// way the `COLLATE`/`CAST` regression tests do — instead this checks
/// `check_columns_pinned` specifically, which runs as an `ONLY_FULL_GROUP_BY`
/// validation step BEFORE evaluation: an ungrouped column referenced inside
/// `AGAINST(...)` must still be caught as `UngroupedColumn`, confirmed
/// against real TiDB via `gorun` (which also rejects this query, matching
/// `ONLY_FULL_GROUP_BY`'s general rule).
#[test]
fn match_against_column_pinning() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table mat1 (dept varchar(10), title varchar(20), salary int)",
    );
    step(
        &mut db,
        "insert into mat1 values ('a','x',1), ('a','y',2), ('b','z',3)",
    );
    assert!(step(
        &mut db,
        "select dept, count(*) from mat1 group by dept having match(title) against(salary)"
    )
    .starts_with("UngroupedColumn("));
}

/// `col -> path` / `col ->> path` desugar at PARSE time to
/// `JSON_EXTRACT`/`JSON_UNQUOTE(JSON_EXTRACT(...))` calls (see
/// `tidb_parser`'s own `parse_expr` doc). The JSON builtin lane supports
/// their scalar text results end-to-end, so keep a table-column regression
/// in addition to its table-less Go differential corpus. `expr MEMBER OF
/// (array)` still needs a JSON value domain and remains honestly rejected.
#[test]
fn json_extract_from_column_and_member_of_rejection() {
    let mut db = Database::new();
    step(&mut db, "create table jeo1 (a varchar(20))");
    step(&mut db, "insert into jeo1 values ('{\"a\":1}')");
    assert_eq!(step(&mut db, "select a->'$.a' from jeo1"), "RS:1");
    assert_eq!(step(&mut db, "select a->>'$.a' from jeo1"), "RS:1");
    assert!(step(&mut db, "select 1 member of ('[1,2,3]')").starts_with("Eval("));
}

/// Regression: `Expr::MemberOf` was added to all 8 traversal functions in
/// the SAME pass as the feature itself (see `match_against_column_pinning`'s
/// own doc for why this project treats a missing arm as a real bug
/// category, not a hypothetical one) — verified here the SAME way, via
/// `check_columns_pinned` (an `ONLY_FULL_GROUP_BY` validation step that
/// runs BEFORE evaluation, so it can observe recursion into `MemberOf`'s
/// own fields even though full evaluation is `Unsupported`): an ungrouped
/// column referenced inside either operand must still be caught as
/// `UngroupedColumn`.
#[test]
fn member_of_column_pinning() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table mof1 (dept varchar(10), tag varchar(20))",
    );
    step(&mut db, "insert into mof1 values ('a','x'), ('a','y')");
    assert!(step(
        &mut db,
        "select dept, count(*) from mof1 group by dept having tag member of ('[\"x\"]')"
    )
    .starts_with("UngroupedColumn("));
}

/// `CAST(... AS type ARRAY)` (and `JSON_SUM_CRC32`, which always sets
/// `array: true`) is ALWAYS `Unsupported` at execution time,
/// unconditionally — see `tidb_ast::CastExpr::array`'s own doc for the
/// full reasoning: real TiDB itself rejects a bare array-cast outside a
/// functional-index DDL context (confirmed directly in
/// `pkg/planner/core/expression_rewriter.go`, and via `gorun`: even a
/// genuine JSON array value still errors here), so this is a shared,
/// permanent restriction, not merely "this crate has no JSON value
/// domain". A plain, non-`ARRAY` cast is unaffected — already-existing,
/// working machinery.
#[test]
fn cast_array_rejected() {
    let mut db = Database::new();
    step(&mut db, "create table car1 (a int)");
    step(&mut db, "insert into car1 values (1), (2)");
    assert!(step(&mut db, "select cast(a as signed array) from car1").starts_with("Eval("));
    assert!(step(
        &mut db,
        "select json_sum_crc32(a as signed array) from car1"
    )
    .starts_with("Eval("));
    // A plain, non-`ARRAY` cast is unaffected.
    assert_eq!(
        step(&mut db, "select cast(a as signed) from car1"),
        "RS:1;2"
    );
}

/// `BINARY expr` reuses the SAME `Expr::Cast` node `CAST(expr AS BINARY)`
/// already produces (just a different concrete syntax, see
/// `tidb_ast::CastStyle::BinaryOperator`'s own doc) — so evaluation is
/// IDENTICAL between the two forms with no new eval code at all.
#[test]
fn binary_operator_matches_cast_as_binary() {
    let mut db = Database::new();
    step(&mut db, "create table bo1 (a varchar(10))");
    step(&mut db, "insert into bo1 values ('hi')");
    assert_eq!(
        step(&mut db, "select binary a from bo1"),
        step(&mut db, "select cast(a as binary) from bo1")
    );
    assert_eq!(step(&mut db, "select binary a = 'hi' from bo1"), "RS:1");
}

/// `ROW(...)`/`(...)` comparison and row-value `IN`/`NOT IN` — see
/// `tidb_expr::row`'s own doc for why no general `Datum::Row` variant is
/// needed (real TiDB syntactically restricts `ROW(...)` to ONLY a
/// comparison/`IN` operand, confirmed via `gorun`: a bare
/// `SELECT ROW(1,2)` is itself a parse-time ERROR there). `=`/`<>` are
/// SQL `AND`-composed equality across every position (checked in full,
/// not short-circuited at the first `NULL`); `<`/`>`/`<=`/`>=` are
/// LEXICOGRAPHIC (the first differing position decides, regardless of a
/// `NULL` later on); mismatched arity is a genuine error, matching real
/// TiDB. `<=>` is deliberately out of scope (see `tidb_expr::row`'s doc).
#[test]
fn row_comparison_eval() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "select row(1,2) = row(1,2)"), "RS:1");
    assert_eq!(step(&mut db, "select row(1,2) = row(1,3)"), "RS:0");
    assert_eq!(step(&mut db, "select row(1,2,3) > row(3,2,1)"), "RS:0");
    assert_eq!(step(&mut db, "select row(1,2) < row(1,3)"), "RS:1");
    assert_eq!(step(&mut db, "select row(1,2) < row(2,1)"), "RS:1");
    // A definite mismatch at ANY position decides `<>`/`=` outright, even
    // when a LATER position is `NULL` — not short-circuited at the first
    // `NULL` the way `<`/`>`/`<=`/`>=` are.
    assert_eq!(step(&mut db, "select row(1,2) <> row(2,null)"), "RS:1");
    // No definite mismatch, and at least one position `NULL`: the whole
    // comparison is `NULL`.
    assert_eq!(step(&mut db, "select row(1,null) = row(1,2)"), "RS:<nil>");
    assert_eq!(
        step(&mut db, "select row(1,null) = row(1,null)"),
        "RS:<nil>"
    );
    // Ordering: the first DIFFERING position decides, even when a later
    // position is `NULL` and never needs to be looked at.
    assert_eq!(step(&mut db, "select row(2,1) < row(1,null)"), "RS:0");
    assert!(step(&mut db, "select row(1,2) = row(1,2,3)").starts_with("Eval("));
}

/// Row-value `IN`/`NOT IN` against a literal tuple list — reuses
/// `tidb_expr::row::row_compare(Eq, ...)` per list item, with the same
/// `found_null`/short-circuit structure as the pre-existing scalar `IN`.
#[test]
fn row_in_list_eval() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "select (1,2) in ((1,2),(3,4))"), "RS:1");
    assert_eq!(step(&mut db, "select (1,2) in ((9,9),(3,4))"), "RS:0");
    assert_eq!(
        step(&mut db, "select (1,null) in ((1,2),(3,4))"),
        "RS:<nil>"
    );
    assert_eq!(step(&mut db, "select (1,2) not in ((9,9),(3,4))"), "RS:1");
}

/// Row-value `IN (subquery)` — the ORIGINAL corpus-motivating shape
/// (`(a,b) NOT IN (SELECT ... UNION SELECT ...)`). Each subquery row
/// becomes its own `Expr::Row` list item (see
/// `Database::in_subquery_rows`), then evaluates via the SAME
/// `eval_in_list` path as a literal row-value list.
#[test]
fn row_in_subquery_eval() {
    let mut db = Database::new();
    step(&mut db, "create table ris (a int, b int)");
    step(&mut db, "insert into ris values (1,2), (9,9)");
    assert_eq!(
        step(&mut db, "select 1 from ris where (a,b) in (select 1,3)"),
        "RS:"
    );
    assert_eq!(
        step(&mut db, "select 1 from ris where (a,b) in (select 1,2)"),
        "RS:1"
    );
    // The original corpus statement: a row-value `NOT IN` against a
    // `UNION`-bodied subquery.
    step(&mut db, "create table ris2 (a int, b int)");
    step(&mut db, "insert into ris2 values (3,2), (9,2)");
    assert_eq!(
        step(
            &mut db,
            "select (a,b) not in (select 3,2 union select 9,2) as field2 from ris2 order by field2"
        ),
        "RS:0;0"
    );
}

/// `DATE`/`TIME`/`TIMESTAMP 'literal'` parses and restores fully (see
/// `tidb_ast::CastStyle::DateLiteral`'s own doc), but evaluation is
/// ALWAYS `Unsupported`, regardless of whether the date is valid —
/// confirmed via `gorun` that real TiDB's own evaluation genuinely
/// diverges from `CAST(... AS DATE)`'s existing lenient (`NULL`-on-
/// invalid) logic: `SELECT DATE '2007-10-00'` is a hard query ERROR
/// there, not `NULL`, while `SELECT CAST('2007-10-00' AS DATE)` IS
/// `NULL`. Silently reusing `CAST`'s own evaluation here would therefore
/// be genuinely WRONG for an invalid date, not just incomplete — a plain
/// `Unsupported` for both valid and invalid inputs is the only safe
/// scope-cut.
#[test]
fn typed_date_literal_rejected() {
    let mut db = Database::new();
    // A VALID date: `CAST(... AS DATE)` succeeds, but the typed-literal
    // form is still `Unsupported`, not silently reused.
    assert!(step(&mut db, "select date '2020-01-01'").starts_with("Eval("));
    assert_eq!(
        step(&mut db, "select cast('2020-01-01' as date)"),
        "RS:2020-01-01"
    );
    // An INVALID date: real TiDB errors outright here (not `NULL`).
    assert!(step(&mut db, "select date '2007-10-00'").starts_with("Eval("));
    assert!(step(&mut db, "select timestamp 'invalid-date'").starts_with("Eval("));
    assert!(step(&mut db, "select time '-1 12:00:01.341300'").starts_with("Eval("));
}

/// `TRIM(...)` evaluation: repeatedly strips WHOLE occurrences of
/// `remstr` (default a single space) from the requested end(s) — see
/// `tidb_expr::string_fn::trim_value`'s own doc. Confirmed via `gorun`.
#[test]
fn trim_eval() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "select trim('  bar  ')"), "RS:bar");
    assert_eq!(
        step(&mut db, "select trim(leading 'x' from 'xxxbarxxx')"),
        "RS:barxxx"
    );
    assert_eq!(
        step(&mut db, "select trim(both 'x' from 'xxxbarxxx')"),
        "RS:bar"
    );
    assert_eq!(
        step(&mut db, "select trim(trailing 'x' from 'xxxbarxxx')"),
        "RS:xxxbar"
    );
    // A whole-substring `remstr` longer than 1 char removes repeated
    // WHOLE occurrences, not per-character (confirmed via `gorun`).
    assert_eq!(step(&mut db, "select trim('xx' from 'xxhixx')"), "RS:hi");
    // An empty `remstr` is a no-op (confirmed via `gorun` — guards
    // against `trim_start_matches`/`trim_end_matches` looping forever
    // on a zero-length pattern).
    assert_eq!(step(&mut db, "select trim('' from 'xxhixx')"), "RS:xxhixx");
    // Any NULL operand yields NULL.
    assert_eq!(step(&mut db, "select trim(both null from 'x')"), "RS:<nil>");
    assert_eq!(step(&mut db, "select trim(null)"), "RS:<nil>");
}

/// Regression: `Expr::Trim` was added to all 9 traversal functions in the
/// SAME pass as the feature itself (see `match_against_column_pinning`'s
/// own doc for why this project treats a missing arm as a real bug
/// category, not a hypothetical one) — verified here the SAME way, via
/// `check_columns_pinned` (an `ONLY_FULL_GROUP_BY` validation step that
/// runs BEFORE evaluation, so it can observe recursion into `Trim`'s own
/// fields even though full evaluation is `Unsupported`): an ungrouped
/// column referenced in EITHER the trimmed expression or the `remstr`
/// operand must still be caught as `UngroupedColumn`.
#[test]
fn trim_column_pinning() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table trc1 (dept varchar(10), tag varchar(20))",
    );
    step(&mut db, "insert into trc1 values ('a','x'), ('a','y')");
    assert!(step(
        &mut db,
        "select dept, count(*) from trc1 group by dept having trim(tag) = 'x'"
    )
    .starts_with("UngroupedColumn("));
    assert!(step(
        &mut db,
        "select dept, count(*) from trc1 group by dept having trim(tag from dept) = 'a'"
    )
    .starts_with("UngroupedColumn("));
}

/// `POSITION(substr IN str)` evaluation: 1-indexed, character-based,
/// case-sensitive (`utf8mb4_bin`) — confirmed via `gorun`.
#[test]
fn position_func_eval() {
    let mut db = Database::new();
    // Case-sensitive: lowercase 'a' not found in uppercase "AA".
    assert_eq!(step(&mut db, "select position('a' in 'AA')"), "RS:0");
    assert_eq!(step(&mut db, "select position('bc' in 'abcd')"), "RS:2");
    assert_eq!(step(&mut db, "select position('z' in 'abcd')"), "RS:0");
    // An empty substring always matches at position 1.
    assert_eq!(step(&mut db, "select position('' in 'abcd')"), "RS:1");
    // Any NULL operand yields NULL.
    assert_eq!(step(&mut db, "select position(null in 'abcd')"), "RS:<nil>");
    assert_eq!(step(&mut db, "select position('a' in null)"), "RS:<nil>");
}

/// Regression: `Expr::Position` was added to all 9 traversal functions in
/// the SAME pass as the feature itself (see `match_against_column_pinning`'s
/// own doc for why this project treats a missing arm as a real bug
/// category, not a hypothetical one) — verified the SAME way as
/// `trim_column_pinning`: an ungrouped column referenced in EITHER
/// operand must still be caught as `UngroupedColumn`.
#[test]
fn position_func_column_pinning() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table pfc1 (dept varchar(10), tag varchar(20))",
    );
    step(&mut db, "insert into pfc1 values ('a','x'), ('a','y')");
    assert!(step(
        &mut db,
        "select dept, count(*) from pfc1 group by dept having position(tag in 'xyz') > 0"
    )
    .starts_with("UngroupedColumn("));
    assert!(step(
        &mut db,
        "select dept, count(*) from pfc1 group by dept having position('x' in tag) > 0"
    )
    .starts_with("UngroupedColumn("));
}

/// `_latin1'x'`/`N'x'` parse and restore fully (see
/// `tidb_ast::Expr::CharsetString`'s own doc), but evaluation is
/// `Unsupported` — no charset value domain exists, relying on the SAME
/// generic `eval_in` wildcard `Expr::Trim`/`Expr::Position` already use.
#[test]
fn charset_string_literal_rejected() {
    let mut db = Database::new();
    assert!(step(&mut db, "select _latin1'a'").starts_with("Eval("));
    assert!(step(&mut db, "select N'a'").starts_with("Eval("));
    // The default charset's own explicit introducer reuses `Expr::String`
    // and evaluates like any other string literal, unaffected.
    assert_eq!(step(&mut db, "select _utf8mb4'a'"), "RS:a");
}

/// `WEIGHT_STRING(...)` parses and restores fully (see
/// `tidb_ast::Expr::WeightString`'s own doc), but evaluation is
/// `Unsupported` — no byte-level collation comparison-key domain exists,
/// relying on the SAME generic `eval_in` wildcard `Expr::Trim`/
/// `Expr::Position` already use.
#[test]
fn weight_string_rejected() {
    let mut db = Database::new();
    assert!(step(&mut db, "select weight_string('ab')").starts_with("Eval("));
    assert!(step(&mut db, "select weight_string('ab' as char(3))").starts_with("Eval("));
}

/// Regression: `Expr::WeightString` was added to all 9 traversal
/// functions in the SAME pass as the feature itself (see
/// `match_against_column_pinning`'s own doc for why this project treats
/// a missing arm as a real bug category, not a hypothetical one) —
/// verified the SAME way as `position_func_column_pinning`: an ungrouped
/// column referenced inside the `expr` operand must still be caught as
/// `UngroupedColumn`.
#[test]
fn weight_string_column_pinning() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table wsc1 (dept varchar(10), tag varchar(20))",
    );
    step(&mut db, "insert into wsc1 values ('a','x'), ('a','y')");
    assert!(step(
        &mut db,
        "select dept, count(*) from wsc1 group by dept having weight_string(tag) > ''"
    )
    .starts_with("UngroupedColumn("));
}

/// A schema-qualified GENERIC function call (`schema.func(...)`, see
/// `tidb_ast::Expr::GenericFuncCall`'s own doc) parses and restores
/// fully, but evaluation is `Unsupported` — relies on the SAME generic
/// `eval_in` wildcard `Expr::Position`/`Expr::WeightString` already
/// use, no new evaluation code (the schema isn't checked against any
/// real catalog, and a "generic" call has no builtin semantics to
/// evaluate here).
#[test]
fn generic_func_call_rejected() {
    let mut db = Database::new();
    assert!(step(&mut db, "select t.upper(1)").starts_with("Eval("));
}

/// Regression: `Expr::GenericFuncCall` was added to all 9 traversal
/// functions in the SAME pass as the feature itself (see
/// `match_against_column_pinning`'s own doc for why this project treats
/// a missing arm as a real bug category, not a hypothetical one) —
/// verified the SAME way as `weight_string_column_pinning`: an
/// ungrouped column referenced inside the call's own `args` must still
/// be caught as `UngroupedColumn`.
#[test]
fn generic_func_call_column_pinning() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table gfc1 (dept varchar(10), tag varchar(20))",
    );
    step(&mut db, "insert into gfc1 values ('a','x'), ('a','y')");
    assert!(step(
        &mut db,
        "select dept, count(*) from gfc1 group by dept having s.f(tag) > 0"
    )
    .starts_with("UngroupedColumn("));
}

/// `ADDDATE`/`SUBDATE` evaluate IDENTICALLY to `DATE_ADD`/`DATE_SUB`
/// respectively — `tidb_parser::parse_adddate_or_subdate` already
/// normalizes both of `ADDDATE`/`SUBDATE`'s own dual grammar forms
/// (explicit `INTERVAL n unit`, or a bare number meaning `INTERVAL n
/// DAY`) down to the exact SAME `Expr::Func{name, args: [date,
/// Expr::Interval]}` shape `DATE_ADD`/`DATE_SUB` already evaluate — but
/// `tidb_expr::func::eval_func`'s own dispatch didn't recognize either
/// NAME at all, so both forms of `ADDDATE`/`SUBDATE` were entirely
/// `Unsupported` at evaluation time despite parsing/restoring fully.
/// Confirmed via `gorun`: bare-number and explicit-`INTERVAL` forms
/// agree, a month-end rollover clamps the same way `DATE_ADD` already
/// does, `NULL` propagates from either argument, a sub-day `HOUR` unit
/// works, and a negative bare number subtracts.
#[test]
fn adddate_subdate_eval() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "select adddate('2020-01-01', interval 1 day)"),
        "RS:2020-01-02"
    );
    // The bare-number form means `INTERVAL n DAY`.
    assert_eq!(
        step(&mut db, "select adddate('2020-01-01', 1)"),
        "RS:2020-01-02"
    );
    assert_eq!(
        step(&mut db, "select subdate('2020-01-01', 1)"),
        "RS:2019-12-31"
    );
    assert_eq!(
        step(&mut db, "select subdate('2020-01-01', interval 1 month)"),
        "RS:2019-12-01"
    );
    // Month-end rollover, matching DATE_ADD's own clamping.
    assert_eq!(
        step(&mut db, "select adddate('2020-01-31', 1)"),
        "RS:2020-02-01"
    );
    // NULL propagates from either argument.
    assert_eq!(step(&mut db, "select adddate(null, 1)"), "RS:<nil>");
    assert_eq!(
        step(&mut db, "select adddate('2020-01-01', null)"),
        "RS:<nil>"
    );
    // A sub-day unit, and a negative bare number.
    assert_eq!(
        step(
            &mut db,
            "select adddate('2020-01-01 10:00:00', interval 5 hour)"
        ),
        "RS:2020-01-01 15:00:00"
    );
    assert_eq!(
        step(&mut db, "select adddate('2020-01-01', -1)"),
        "RS:2019-12-31"
    );
}

/// `NEXTVAL`/`LASTVAL`/`SETVAL`'s argument is a SEQUENCE NAME, never a
/// column reference (matching real TiDB's own `TableNameExpr` argument
/// parsing) — so naming a table COLUMN here is a real "unknown sequence"
/// error, exactly as `gorun` answers ERR for these statements. Real
/// sequence execution lives in `tests/sequence.rs` (task #130); this
/// test keeps the boundary cases — a non-sequence argument, and `ALTER
/// SEQUENCE`'s still-honest rejection (its restart/meta-rewrite
/// semantics were never gorun-probed, so implementing it starts with
/// probes, not code).
#[test]
fn sequence_functions_rejected() {
    let mut db = Database::new();
    step(&mut db, "create table seq1 (a int)");
    step(&mut db, "insert into seq1 values (7)");
    assert_eq!(
        step(&mut db, "select nextval(a) from seq1"),
        "Eval(Sequence(\"unknown sequence\"))"
    );
    assert_eq!(
        step(&mut db, "select lastval(a) from seq1"),
        "Eval(Sequence(\"unknown sequence\"))"
    );
    assert_eq!(
        step(&mut db, "select setval(a, 5) from seq1"),
        "Eval(Sequence(\"unknown sequence\"))"
    );
    assert_eq!(step(&mut db, "create sequence s1"), "OK");
    // `ALTER`/`CREATE`/`DROP`/the functions all execute for real since
    // task #131 — see `tests/sequence.rs` for the behavioral coverage.
    assert_eq!(step(&mut db, "alter sequence s1 restart"), "OK");
    assert_eq!(step(&mut db, "drop sequence s1"), "OK");
}

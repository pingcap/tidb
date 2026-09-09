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

//! GO PORT of the statement-free rows of
//! `pkg/expression/integration_test/integration_test.go`
//! `TestCompareBuiltin` (`integration_test.go:2661`) and `TestTimeBuiltin`
//! (`integration_test.go:2866`) (batch part10): every `select` in those tests
//! whose arguments are literals rather than table columns.
//!
//! The Go harness runs them through a full session (`tk.MustQuery`); each row
//! reaches `builtinCompareSigForConstantArgs`-style dispatch unchanged by the
//! surrounding executor, so the constant rewrite tier evaluated here pins the
//! identical builtin behavior. Column-shaped and timezone-mutation subtests of
//! the two Go tests live outside this crate's surface.

use super::*;

/// Evaluates EVERY top-level expression of one `select` list against an empty
/// virtual row and joins their labels with spaces -- the shape Go's
/// `testkit.Rows` splits a result row into cells with.
fn eval_row(select_list: &str) -> String {
    let stmt = tidb_parser::parse(&format!("select {select_list}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    select
        .fields
        .iter()
        .map(|field| {
            let SelectField::Expr { expr, .. } = field else {
                panic!("every field is an expression")
            };
            let rewritten = crate::rewriter::rewrite_expr(expr).expect("rewrite");
            rewritten
                .eval(&NoColumns, chunk.get_row(0))
                .expect("eval")
                .label()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn test_compare_builtin_coalesce_rows() {
    // integration_test.go:2826 -- all-NULL arities answer NULL.
    assert_eq!(e("coalesce(NULL)"), "NULL");
    assert_eq!(e("coalesce(NULL, NULL)"), "NULL");
    assert_eq!(chunk_e("coalesce(NULL, NULL, NULL)"), "NULL");

    // integration_test.go:2827-2830 -- JSON documents keep their kind.
    assert_eq!(
        chunk_e("coalesce(cast(1 as json), cast(2 as json))"),
        "JSON:1"
    );
    assert_eq!(chunk_e("coalesce(NULL, cast(2 as json))"), "JSON:2");
    assert_eq!(chunk_e("coalesce(cast(1 as json), NULL)"), "JSON:1");
}

#[test]
fn test_compare_builtin_nullif_rows() {
    // integration_test.go:2840
    assert_eq!(
        eval_row("NULLIF(NULL, 1), NULLIF(1, NULL), NULLIF(1, 1), NULLIF(NULL, NULL)"),
        "NULL INT:1 NULL NULL"
    );
    assert_eq!(chunk_e("NULLIF(1, 1.0)"), "NULL"); // :2843 numeric equality
    assert_eq!(chunk_e("NULLIF(1, \"1.0\")"), "NULL");
    assert_eq!(chunk_e("NULLIF(\"abc\", 1)"), "STR:abc"); // :2845 string wins
    assert_eq!(chunk_e("NULLIF(1+2, 1)"), "INT:3"); // :2847
    assert_eq!(chunk_e("NULLIF(1, 1+2)"), "INT:1"); // :2849
    assert_eq!(chunk_e("NULLIF(2+3, 1+2)"), "INT:5"); // :2851
    assert_eq!(chunk_e("HEX(NULLIF(\"abc\", 1))"), "STR:616263"); // :2853
}

#[test]
fn test_compare_builtin_interval_rows() {
    // integration_test.go:2858
    assert_eq!(
        eval_row("interval(null, 1, 2), interval(1, 2, 3), interval(2, 1, 3)"),
        "INT:-1 INT:0 INT:1"
    );
    // integration_test.go:2859
    assert_eq!(
        eval_row("interval(3, 1, 2), interval(0, \"b\", \"1\", \"2\"), interval(\"a\", \"b\", \"1\", \"2\")"),
        "INT:2 INT:1 INT:1"
    );
    // integration_test.go:2860
    assert_eq!(
        eval_row("interval(23, 1, 23, 23, 23, 30, 44, 200), interval(23, 1.7, 15.3, 23.1, 30, 44, 200), \
         interval(9007199254740992, 9007199254740993)"),
        "INT:4 INT:2 INT:0"
    );
    // integration_test.go:2861 -- unsigned duality across the int64 boundary;
    // a signed arg promotes the whole comparison.
    assert_eq!(
        eval_row("interval(cast(9223372036854775808 as unsigned), cast(9223372036854775809 as unsigned)), \
         interval(9223372036854775807, cast(9223372036854775808 as unsigned)), \
         interval(-9223372036854775807, cast(9223372036854775808 as unsigned))"),
        "INT:0 INT:0 INT:0"
    );
    // integration_test.go:2862
    assert_eq!(
        eval_row(
            "interval(cast(9223372036854775806 as unsigned), 9223372036854775807), \
         interval(cast(9223372036854775806 as unsigned), -9223372036854775807)"
        ),
        "INT:0 INT:1"
    );
    // integration_test.go:2863 -- decimal strings compare as REAL against one another.
    assert_eq!(
        chunk_e("interval(\"9007199254740991\", \"9007199254740992\")"),
        "INT:0"
    );
    // integration_test.go:2864 -- mixed literal/string comparisons are REAL-signed.
    assert_eq!(
        eval_row(
            "interval(9007199254740992, \"9007199254740993\"), \
         interval(\"9007199254740992\", 9007199254740993), \
         interval(\"9007199254740992\", \"9007199254740993\")"
        ),
        "INT:1 INT:1 INT:1"
    );
    // integration_test.go:2865 -- trailing NULLs sort after everything real.
    assert_eq!(
        chunk_e("INTERVAL(100, NULL, NULL, NULL, NULL, NULL, 100)"),
        "INT:6"
    );
    // integration_test.go:2866 -- INTERVAL is ordinary scalar arithmetic.
    assert_eq!(
        chunk_e("(INTERVAL(0,(1*5)/2)) + (INTERVAL(5,4,3))"),
        "INT:2"
    );
}

#[test]
fn test_compare_builtin_greatest_least_literal_rows() {
    // integration_test.go:2874 / 2884 -- plain numeric and string families.
    assert_eq!(chunk_e("greatest(1, 2, 3)"), "INT:3");
    assert_eq!(chunk_e("least(1, 2, 3)"), "INT:1");
    assert_eq!(chunk_e("greatest(\"a\", \"b\", \"c\")"), "STR:c");
    assert_eq!(chunk_e("least(\"a\", \"b\", \"c\")"), "STR:a");
    assert_eq!(chunk_e("greatest(1.1, 1.2, 1.3)"), "DEC:1.3");
    assert_eq!(chunk_e("least(1.1, 1.2, 1.3)"), "DEC:1.1");
    // String signature when ANY argument is a string; numerics stringify.
    assert_eq!(chunk_e("greatest(\"123a\", 1, 2)"), "STR:2");
    assert_eq!(chunk_e("least(\"123a\", 1, 2)"), "STR:1");
    // Temporal-typed arguments use the temporal signature; bare strings then
    // parse as time values ("234" reads as year 234 of the current era).
    assert_eq!(
        chunk_e(
            r#"greatest(cast("2017-01-01" as datetime), "123", "234", cast("2018-01-01" as date))"#
        ),
        "STR:234"
    );
    assert_eq!(
        chunk_e(
            r#"least(cast("2017-01-01" as datetime), "123", "234", cast("2018-01-01" as date))"#
        ),
        "STR:123"
    );
    // One NULL argument propagates (integration_test.go:2876/2886).
    assert_eq!(
        chunk_e(r#"greatest(cast("2017-01-01" as date), "123", null)"#),
        "NULL"
    );
    assert_eq!(
        chunk_e(r#"least(cast("2017-01-01" as date), "123", null)"#),
        "NULL"
    );
    //
    // go-parity-gap: the companion `show warnings` assertions at
    // integration_test.go:2881/2890 expect three `Warning 1292 Incorrect time
    // value: '123'/'234'` rows from these selects. Rust's greatest/least
    // answers match the values above but do not raise those statements'
    // warning trail yet; the VALUE rows here are pinned, the warning rows are
    // not asserted anywhere on the Rust side.
}

#[test]
fn test_compare_builtin_decimal_uint_boundary_rows() {
    // integration_test.go:2891 -- the literal stays out of int64 range, so
    // `1 <` compares through the decimal promotion path.
    assert_eq!(
        eval_row("1 < 17666000000000000000, 1 > 17666000000000000000, 1 = 17666000000000000000"),
        "INT:1 INT:0 INT:0"
    );
}

#[test]
fn test_compare_builtin_row_constructor_rows() {
    // integration_test.go:2941-2950 -- ROW(a,b,c) compares member-wise.
    assert_eq!(chunk_e("row(1,2,3)=row(1,2,3)"), "INT:1");
    assert_eq!(chunk_e("row(1,2,3)=row(1+3,2,3)"), "INT:0");
    assert_eq!(chunk_e("row(1,2,3)<>row(1,2,3)"), "INT:0");
    assert_eq!(chunk_e("row(1,2,3)<>row(1+3,2,3)"), "INT:1");
    assert_eq!(chunk_e("row(1+3,2,3)<>row(1+3,2,3)"), "INT:0");
}

#[test]
fn test_time_builtin_date_year_makedate_literal_rows() {
    // integration_test.go:2902-2904 -- DATE keeps the calendar prefix only,
    // zero dates and garbage go NULL.
    assert_eq!(
        eval_row(
            r#"date("2019-09-12"), date("2019-09-12 12:12:09"), date("2019-09-12 12:12:09.121212")"#
        ),
        "STR:2019-09-12 STR:2019-09-12 STR:2019-09-12"
    );
    assert_eq!(
        eval_row(
            r#"date("0000-00-00"), date("0000-00-00 12:12:09"), date("0000-00-00 00:00:00.121212")"#
        ),
        "NULL NULL NULL"
    );
    assert_eq!(
        eval_row(r#"date("aa"), date(12.1), date("")"#),
        "NULL NULL NULL"
    );

    // integration_test.go:2907-2912 -- YEAR extraction; zero months/days keep
    // the year, overflow lengths answer NULL.
    assert_eq!(
        eval_row(
            r#"year("2013-01-09"), year("2013-00-09"), year("000-01-09"), year("1-01-09"), year("20131-01-09"), year(null)"#
        ),
        "INT:2013 INT:2013 INT:0 INT:2001 NULL NULL"
    );
    assert_eq!(
        eval_row(
            r#"year("2013-00-00"), year("2013-00-00 00:00:00"), year("0000-00-00 12:12:12"), year("2017-00-00 12:12:12")"#
        ),
        "INT:2013 INT:2013 INT:0 INT:2017"
    );

    // integration_test.go:2897-2899 -- MAKEDATE(year, dayofyear); day 1 lands
    // on Jan 1 and years 1..69 read as 2001..2069 per MySQL.
    assert_eq!(chunk_e("makedate(1, 1)"), "STR:2001-01-01");
    assert_eq!(chunk_e("makedate(2011, 41)"), "STR:2011-02-10");
    assert_eq!(chunk_e("makedate(null, null)"), "NULL");
}

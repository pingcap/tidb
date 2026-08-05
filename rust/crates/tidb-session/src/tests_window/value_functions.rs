//! Value window functions: `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` and
//! `NTH_VALUE`, which read a row out of the frame.

use crate::tests_support::*;
use crate::*;

/// A `LAG`/`LEAD` default that WIDENS the result type: Go merges the two
/// argument types and reads BOTH operands through the merged one, so the
/// VALUE argument changes domain too.
#[test]
fn window_lag_lead_widening_default() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ll (id BIGINT, v BIGINT, d DECIMAL(10,2), s VARCHAR(10))")
        .unwrap();
    session
        .run("INSERT INTO ll VALUES (1,10,1.50,'a'),(2,20,2.50,'b'),(3,30,3.50,'c')")
        .unwrap();

    // Captured: the integers come back as STRINGS, not just the default.
    assert_eq!(
        row_text(session.run("SELECT LAG(v,1,'zz') OVER (ORDER BY id) FROM ll")),
        [["zz"], ["10"], ["20"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LEAD(v,1,'zz') OVER (ORDER BY id) FROM ll")),
        [["20"], ["30"], ["zz"]]
    );
    // A DECIMAL default widens an integer argument to DECIMAL, and the
    // argument keeps its own scale (`10`, not the scale-padded `10.0`):
    // Go reads it through the merged type's EVAL kind, not through a
    // width-and-scale-applying conversion.
    assert_eq!(
        row_text(session.run("SELECT LAG(v,1,1.5) OVER (ORDER BY id) FROM ll")),
        [["1.5"], ["10"], ["20"]]
    );
    // The widening runs the other way too: an integer default over a
    // string argument merges to VARCHAR.
    assert_eq!(
        row_text(session.run("SELECT LAG(s,1,0) OVER (ORDER BY id) FROM ll")),
        [["0"], ["a"], ["b"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LAG(d,1,'zz') OVER (ORDER BY id) FROM ll")),
        [["zz"], ["1.50"], ["2.50"]]
    );
    // Every position out of range takes the default.
    assert_eq!(
        row_text(session.run("SELECT LAG(v,5,'zz') OVER (ORDER BY id) FROM ll")),
        [["zz"], ["zz"], ["zz"]]
    );
    // A NULL default does NOT widen: Go's `InferType4ControlFuncs` drops
    // NULL-typed operands, so the result stays the argument's own type.
    assert_eq!(
        row_text(session.run("SELECT LAG(v,1,NULL) OVER (ORDER BY id) FROM ll")),
        [["NULL"], ["10"], ["20"]]
    );

    // The merged result TYPE, captured: VARCHAR for a string default,
    // DECIMAL for a decimal one, and the argument's own BIGINT when the
    // default is NULL or already an integer.
    use tidb_datatype::FieldTypeCode;
    for (sql, code) in [
        (
            "SELECT LAG(v,1,'zz') OVER (ORDER BY id) FROM ll",
            FieldTypeCode::Varchar,
        ),
        (
            "SELECT LAG(v,1,1.5) OVER (ORDER BY id) FROM ll",
            FieldTypeCode::NewDecimal,
        ),
        (
            "SELECT LAG(s,1,0) OVER (ORDER BY id) FROM ll",
            FieldTypeCode::Varchar,
        ),
        (
            "SELECT LAG(v,1,NULL) OVER (ORDER BY id) FROM ll",
            FieldTypeCode::LongLong,
        ),
        (
            "SELECT LAG(v,1,-1) OVER (ORDER BY id) FROM ll",
            FieldTypeCode::LongLong,
        ),
    ] {
        match session.run_with_columns(sql).unwrap() {
            StmtOutput::Rows { columns, .. } => {
                assert_eq!(columns[0].1.code(), code, "result type for {sql}");
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }
}

/// `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`, which DO read the frame -- so
/// `LAST_VALUE` under the default frame famously returns the current PEER
/// GROUP's last row, not the partition's.
#[test]
fn window_value_functions_read_the_frame() {
    let mut session = window_session();

    assert_eq!(
        row_text(session.run(
            "SELECT FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS f FROM t ORDER BY g, v"
        )),
        [["10"], ["10"], ["10"], ["10"], ["10"], ["5"], ["5"], ["5"]]
    );

    // The default frame ends at the current PEER GROUP, so LAST_VALUE is
    // the row's own peer-group maximum -- 40 appears only on the last row.
    assert_eq!(
        row_text(session.run(
            "SELECT LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
        )),
        [["10"], ["20"], ["20"], ["30"], ["40"], ["5"], ["5"], ["7"]]
    );
    // Spelling out the whole partition is what returns the partition's
    // last row on EVERY row.
    assert_eq!(
        row_text(session.run(
            "SELECT LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED \
                 PRECEDING AND UNBOUNDED FOLLOWING) AS l FROM t ORDER BY g, v"
        )),
        [["40"], ["40"], ["40"], ["40"], ["40"], ["7"], ["7"], ["7"]]
    );

    // NTH_VALUE is NULL while the frame holds fewer than n rows.
    assert_eq!(
        row_text(session.run(
            "SELECT NTH_VALUE(v, 3) OVER (PARTITION BY g ORDER BY v) AS n FROM t ORDER BY g, v"
        )),
        [
            ["NULL"],
            ["20"],
            ["20"],
            ["20"],
            ["20"],
            ["NULL"],
            ["NULL"],
            ["7"],
        ]
    );
    // Counted from the FRAME's start, not the partition's.
    assert_eq!(
        row_text(session.run(
            "SELECT NTH_VALUE(v, 2) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1 PRECEDING \
                 AND CURRENT ROW) AS n FROM t ORDER BY g, v"
        )),
        [
            ["NULL"],
            ["20"],
            ["20"],
            ["30"],
            ["40"],
            ["NULL"],
            ["5"],
            ["7"],
        ]
    );
}

/// `LAG`/`LEAD`, which address the sorted partition directly and IGNORE
/// the frame entirely.
#[test]
fn window_lag_and_lead() {
    let mut session = window_session();

    // The default offset is 1, and the partition's first row is NULL.
    assert_eq!(
        row_text(
            session.run("SELECT LAG(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v")
        ),
        [
            ["NULL"],
            ["10"],
            ["20"],
            ["20"],
            ["30"],
            ["NULL"],
            ["5"],
            ["5"],
        ]
    );
    assert_eq!(
        row_text(
            session
                .run("SELECT LAG(v, 2) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v")
        ),
        [
            ["NULL"],
            ["NULL"],
            ["10"],
            ["20"],
            ["20"],
            ["NULL"],
            ["NULL"],
            ["5"],
        ]
    );
    // The third argument fills EVERY out-of-range position.
    assert_eq!(
        row_text(session.run(
            "SELECT LAG(v, 2, -1) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
        )),
        [
            ["-1"],
            ["-1"],
            ["10"],
            ["20"],
            ["20"],
            ["-1"],
            ["-1"],
            ["5"]
        ]
    );
    // Offset 0 is the current row.
    assert_eq!(
        row_text(
            session
                .run("SELECT LAG(v, 0) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v")
        ),
        [["10"], ["20"], ["20"], ["30"], ["40"], ["5"], ["5"], ["7"]]
    );

    // LEAD runs off the partition's END instead.
    assert_eq!(
        row_text(
            session
                .run("SELECT LEAD(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v")
        ),
        [
            ["20"],
            ["20"],
            ["30"],
            ["40"],
            ["NULL"],
            ["5"],
            ["7"],
            ["NULL"],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT LEAD(v, 2, -7) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
        )),
        [
            ["20"],
            ["30"],
            ["40"],
            ["-7"],
            ["-7"],
            ["7"],
            ["-7"],
            ["-7"]
        ]
    );

    // A frame is written but IGNORED: the result is identical to the
    // frame-less LAG above (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT LAG(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 PRECEDING AND \
                 1 PRECEDING) AS l FROM t ORDER BY g, v"
        )),
        [
            ["NULL"],
            ["10"],
            ["20"],
            ["20"],
            ["30"],
            ["NULL"],
            ["5"],
            ["5"],
        ]
    );
}

/// The table the merge boundaries below are measured on, reproducing the one
/// `gorun` was pointed at.
fn merge_session() -> Session {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE tb (id INT, ua INT UNSIGNED, e ENUM('x','y'), v VARCHAR(10), \
             a INT, dt DATE, tm TIME(6))",
        )
        .unwrap();
    session
        .run("INSERT INTO tb VALUES (1,10,'x','ss',10,'2020-01-01','01:02:03.456789')")
        .unwrap();
    session
        .run("INSERT INTO tb VALUES (2,20,'y','tt',20,'2021-01-01','02:02:03.456789')")
        .unwrap();
    session
}

/// The result type one `LAG`/`LEAD` call reports.
fn merged_type(session: &mut Session, sql: &str) -> tidb_datatype::FieldType {
    match session.run_with_columns(sql) {
        Ok(StmtOutput::Rows { columns, .. }) => columns[0].1.clone(),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

/// Go's `len(notNullFields) == 1` shortcut copies the ONE typed operand
/// WHOLE -- no re-derivation at all.
///
/// The width is the boundary that shows it: an UNSIGNED `int(10)` spends no
/// digit on a sign, so running the merge over a one-element list instead puts
/// one back (`setFlenFromArgs` re-adds a sign digit unconditionally) and
/// reports 11. A SIGNED operand round-trips and would agree either way.
///
/// Go, via `gorun`, `desc` over a view of this call:
/// `c_null_unsigned|int(10) unsigned`.
#[test]
fn window_lag_one_typed_operand_is_copied_whole() {
    let mut session = merge_session();
    let ft = merged_type(
        &mut session,
        "SELECT LAG(NULL,1,ua) OVER (ORDER BY id) FROM tb",
    );
    assert_eq!(ft.code(), tidb_datatype::FieldTypeCode::Long);
    assert_eq!(ft.flen(), 10);
    assert!(ft.has_flag(tidb_datatype::FieldTypeFlags::UNSIGNED));
}

/// A LONE enum operand beside a NULL one is the ONLY path that reaches Go's
/// ENUM/SET rewrite -- an enum PAIR is already VARCHAR by the time
/// `AggFieldType` returns, so it never gets there.
///
/// Keeping the ENUM type does not merely misreport it. The VALUE changes:
///
/// ```text
/// select id, lag(e,1,NULL) over (order by id) from tl order by id;
/// RS:1|<nil>;2|x
/// ```
#[test]
fn window_lag_a_lone_enum_operand_reads_as_a_varchar() {
    let mut session = merge_session();
    let sql = "SELECT LAG(e,1,NULL) OVER (ORDER BY id) FROM tb";
    assert_eq!(
        merged_type(&mut session, sql).code(),
        tidb_datatype::FieldTypeCode::Varchar
    );
    assert_eq!(row_text(session.run(sql)), [["NULL"], ["x"]]);
}

/// A DATE beside a TIME merges to DATETIME, and Go's
/// `TryToFixFlenOfDatetime` then gives the result its CANONICAL width --
/// `19 + fsp + 1` -- never an operand's. The operands here are 10 and 17
/// wide, so any width taken from them is wrong; a DATETIME PAIR agrees either
/// way, which is why the boundary is a MIXED temporal pair.
///
/// This is also where the FUNCTION NAME the inference is given stops being
/// inert. Go's per-`getFunction` flag tails belong to `IF`/`IFNULL`/
/// `COALESCE`/`CASE WHEN`; `LEAD`/`LAG` have none there (theirs is
/// `NewWindowFuncDesc`'s). Handing this call `IF`'s name instead would add
/// `mysql.BinaryFlag`, and a temporal result is the one that does not already
/// carry it.
///
/// Go, via `gorun`, `desc` over a view of this call: `c_date_time|datetime(6)`.
#[test]
fn window_lag_a_datetime_result_takes_its_canonical_width() {
    let mut session = merge_session();
    let ft = merged_type(
        &mut session,
        "SELECT LAG(dt,1,tm) OVER (ORDER BY id) FROM tb",
    );
    assert_eq!(ft.code(), tidb_datatype::FieldTypeCode::Datetime);
    assert_eq!((ft.flen(), ft.decimal()), (26, 6));
    assert!(!ft.has_flag(tidb_datatype::FieldTypeFlags::BINARY));
}

/// Every operand NULL: Go zeroes the width and the scale and gives the result
/// the binary charset -- an unsized NULL column is not the same answer.
///
/// Go, via `gorun`, `desc` over a view of this call: `c_both_null|binary(0)`.
#[test]
fn window_lag_every_operand_null_is_a_sized_null_column() {
    let mut session = merge_session();
    let ft = merged_type(
        &mut session,
        "SELECT LAG(NULL,1,NULL) OVER (ORDER BY id) FROM tb",
    );
    assert_eq!(ft.code(), tidb_datatype::FieldTypeCode::Null);
    assert_eq!((ft.flen(), ft.decimal()), (0, 0));
    assert_eq!(ft.charset_name(), "binary");
}

/// A STRING result's scale is UNSPECIFIED, which `setDecimalFromArgs` alone
/// does not produce: it takes the widest operand scale -- both are 0 here --
/// and Go's own `resultEvalType == ETString` fixup overwrites it afterwards.
///
/// The width beside it is the ETString arm's: an INTEGER operand inside a
/// string result is measured by what its DECLARED type can print (`INT` is
/// 11) rather than by the 10 the VARCHAR declares.
#[test]
fn window_lag_a_string_result_has_no_scale() {
    let mut session = merge_session();
    let ft = merged_type(&mut session, "SELECT LAG(v,1,a) OVER (ORDER BY id) FROM tb");
    assert_eq!(ft.code(), tidb_datatype::FieldTypeCode::Varchar);
    assert_eq!(
        (ft.flen(), ft.decimal()),
        (11, tidb_datatype::UNSPECIFIED_LENGTH)
    );
}

/// Go's `aggregation.NewWindowFuncDesc` LEAD/LAG arm (`window_func.go:66-74`)
/// uses `SetFlag`, which REPLACES the whole flag mask -- so two NOT NULL
/// operands do not merely add NOT NULL, they DROP UNSIGNED, and the same
/// call over nullable columns prints the opposite sign.
///
/// Go, via `gorun`:
///
/// ```text
/// select id, lag(bu,1,bu2) over (order by id) from tu order by id;   -- both NOT NULL
/// RS:1|-6;2|-1
/// select id, lag(bn,1,bn) over (order by id) from tu order by id;    -- both nullable
/// RS:1|18446744073709551615;2|18446744073709551615
/// ```
///
/// `desc` over a VIEW of these calls reports `bigint(21) unsigned` for BOTH,
/// so the view's stored metadata is NOT the oracle here -- the printed value
/// is, and it is what these two lines pin.
#[test]
fn window_lag_two_not_null_operands_drop_unsigned() {
    use tidb_datatype::FieldTypeFlags;
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE tu (id INT, bu BIGINT UNSIGNED NOT NULL, \
             bu2 BIGINT UNSIGNED NOT NULL, bn BIGINT UNSIGNED)",
        )
        .unwrap();
    session
        .run("INSERT INTO tu VALUES (1, 18446744073709551615, 18446744073709551610, 18446744073709551615)")
        .unwrap();
    session
        .run("INSERT INTO tu VALUES (2, 18446744073709551614, 18446744073709551609, 18446744073709551614)")
        .unwrap();

    let both_not_null = "SELECT LAG(bu,1,bu2) OVER (ORDER BY id) FROM tu";
    match session.run_with_columns(both_not_null).unwrap() {
        StmtOutput::Rows { columns, .. } => {
            let ft = &columns[0].1;
            assert!(ft.has_flag(FieldTypeFlags::NOT_NULL));
            assert!(
                !ft.has_flag(FieldTypeFlags::UNSIGNED),
                "`SetFlag(NotNullFlag)` replaces the mask the merge wrote"
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
    // Out of range takes the default column, in range takes the value
    // column; BOTH are read as the bit pattern the signed result prints.
    assert_eq!(row_text(session.run(both_not_null)), [["-6"], ["-1"]]);

    // One nullable operand and the `SetFlag` never runs, so the merge's own
    // UNSIGNED survives and the very same bits print unsigned.
    assert_eq!(
        row_text(session.run("SELECT LAG(bn,1,bn) OVER (ORDER BY id) FROM tu")),
        [["18446744073709551615"], ["18446744073709551615"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LAG(bu,1,bn) OVER (ORDER BY id) FROM tu")),
        [["18446744073709551615"], ["18446744073709551615"]]
    );
}

/// The table the ENUM/SET and temporal pins below read, with two rows so a
/// `LAG` has both an out-of-range position and an in-range one.
fn typed_value_session() -> Session {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE w (id INT, t6 DATETIME(6), t0 DATETIME, d3 TIME(3), \
             dt DATE, ts TIMESTAMP(4), e ENUM('x','y','z'), s SET('a','b','c'))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO w VALUES (1,'2020-01-01 10:20:30.123456','2020-01-01 10:20:30',\
             '01:02:03.456','2020-03-04','2020-01-01 10:20:30.1234','y','a,b')",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO w VALUES (2,'2021-02-03 04:05:06.654321','2021-02-03 04:05:06',\
             '11:22:33.789','2021-05-06','2021-02-03 04:05:06.5678','x','c')",
        )
        .unwrap();
    session
}

/// Go's `typeInfer4LeadLag` calls the shared control-function inference only
/// `if len(a.Args) >= 3`; a `LAG`/`LEAD` with NO written default falls to
/// `typeInfer4MaxMin`, which carries the argument's own type and then rewrites
/// an ENUM or SET one to `char(255)` (`base_func.go:379-383`, "issue #13027,
/// #13961"). Go excludes only `FIRSTROW`, `MAX` and `MIN` from that rewrite,
/// and all three are aggregates -- so every WINDOW value function takes it.
///
/// The rewrite is not cosmetic. Go then casts the argument to that result
/// (`WrapCastForAggArgs`), so what comes back is the enum's LABEL:
///
/// ```text
/// select id, lag(e,1) over (order by id) from w;         RS:1|<nil>;2|y
/// select id, lead(e,1) over (order by id) from w;        RS:1|x;2|<nil>
/// select id, lag(s,1) over (order by id) from w;         RS:1|<nil>;2|a,b
/// select id, first_value(e) over (order by id) from w;   RS:1|y;2|y
/// select id, last_value(e) over (order by id) from w;    RS:1|y;2|x
/// select id, last_value(s) over (order by id) from w;    RS:1|a,b;2|c
/// select id, nth_value(e,1) over (order by id) from w;   RS:1|y;2|y
/// ```
///
/// and `desc` over a view of each of those seven reports `char(255)`.
/// (All quoted from `gorun`.)
#[test]
fn window_value_functions_rewrite_a_lone_enum_or_set_to_a_char() {
    use tidb_datatype::FieldTypeCode;
    let mut session = typed_value_session();

    for (sql, rows) in [
        (
            "SELECT LAG(e,1) OVER (ORDER BY id) FROM w",
            [["NULL"], ["y"]],
        ),
        (
            "SELECT LEAD(e,1) OVER (ORDER BY id) FROM w",
            [["x"], ["NULL"]],
        ),
        (
            "SELECT LAG(s,1) OVER (ORDER BY id) FROM w",
            [["NULL"], ["a,b"]],
        ),
        (
            "SELECT FIRST_VALUE(e) OVER (ORDER BY id) FROM w",
            [["y"], ["y"]],
        ),
        (
            "SELECT LAST_VALUE(e) OVER (ORDER BY id) FROM w",
            [["y"], ["x"]],
        ),
        (
            "SELECT LAST_VALUE(s) OVER (ORDER BY id) FROM w",
            [["a,b"], ["c"]],
        ),
        (
            "SELECT NTH_VALUE(e,1) OVER (ORDER BY id) FROM w",
            [["y"], ["y"]],
        ),
    ] {
        let ft = merged_type(&mut session, sql);
        assert_eq!(ft.code(), FieldTypeCode::String, "result type for {sql}");
        assert_eq!(ft.flen(), 255, "`mysql.MaxFieldCharLength` for {sql}");
        assert_eq!(row_text(session.run(sql)), rows, "answer for {sql}");
    }

    // The BOUNDARY that shows the rewrite is keyed on the FUNCTION and not
    // merely on "an enum argument": a windowed `MAX`/`MIN` is one of the
    // three names Go excludes, so it keeps the enum whole.
    //
    // ```text
    // select id, max(e) over (order by id) from w;   RS:1|y;2|y
    // ```
    // `desc` over a view of it: `enum('x','y','z')`.
    let max_over = "SELECT MAX(e) OVER (ORDER BY id) FROM w";
    assert_eq!(
        merged_type(&mut session, max_over).code(),
        FieldTypeCode::Enum
    );
    assert_eq!(row_text(session.run(max_over)), [["y"], ["y"]]);
    assert_eq!(
        merged_type(&mut session, "SELECT MIN(s) OVER (ORDER BY id) FROM w").code(),
        FieldTypeCode::Set
    );

    // The other boundary: an enum PAIR never reaches `typeInfer4MaxMin` at
    // all (a written default takes the three-argument branch), and is already
    // a VARCHAR by the time the merge returns -- a different width, from a
    // different rule.
    let pair = "SELECT LAG(e,1,e) OVER (ORDER BY id) FROM w";
    let ft = merged_type(&mut session, pair);
    assert_eq!((ft.code(), ft.flen()), (FieldTypeCode::Varchar, 1));
    assert_eq!(row_text(session.run(pair)), [["y"], ["y"]]);
}

/// A TEMPORAL operand keeps its own SCALE and its own KIND, because Go never
/// hands the raw operand to the evaluator: `WrapCastForAggArgs` wraps it
/// first, and `WrapWithCastAsTime` returns it UNWRAPPED when the result
/// already has the operand's type code, or when a `DATE`/`TIMESTAMP` operand
/// meets a `DATETIME` result. Where it does wrap, the cast's fsp comes from
/// the SOURCE.
///
/// Go, via `gorun`, over [`typed_value_session`]'s two rows:
///
/// ```text
/// select id, lag(t6,1) over (order by id) from w;      RS:1|<nil>;2|2020-01-01 10:20:30.123456
/// select id, lag(d3,1) over (order by id) from w;      RS:1|<nil>;2|01:02:03.456
/// select id, lag(ts,1) over (order by id) from w;      RS:1|<nil>;2|2020-01-01 10:20:30.1234
/// select id, lag(t0,1,t6) over (order by id) from w;   RS:1|2020-01-01 10:20:30.123456;2|2020-01-01 10:20:30
/// select id, lag(t6,1,t0) over (order by id) from w;   RS:1|2020-01-01 10:20:30;2|2020-01-01 10:20:30.123456
/// select id, lag(dt,1,d3) over (order by id) from w;   RS:1|<now> 01:02:03.456;2|2020-03-04
/// select id, lag(d3,1,dt) over (order by id) from w;   RS:1|2020-03-04;2|<now> 01:02:03.456
/// ```
///
/// Every line is a boundary the others do not cover:
///
///  * `lag(t6,1)` and `lag(ts,1)` are the no-default path, where reading the
///    operand through the result's own width would round `.123456` to
///    nothing; a `DATE` argument (fsp 0 either way) cannot show it.
///  * `lag(t0,1,t6)` / `lag(t6,1,t0)` pair a `datetime(0)` with a
///    `datetime(6)`: the merged result is `datetime(6)`, and BOTH operands
///    keep their own scale rather than the merged one -- in BOTH roles, so
///    neither the value argument nor the non-constant default is converted.
///  * `lag(dt,1,d3)` merges a `DATE` and a `TIME(3)` to `datetime(3)`, and
///    the `DATE` operand still prints as a bare date. That is the KIND
///    shortcut, which no same-kind pair can discriminate.
///  * the `TIME(3)` operand of the same pair is the one case Go DOES wrap,
///    and it lands on `01:02:03.456` -- the SOURCE's three digits, not the
///    result's.
#[test]
fn window_lag_reads_a_temporal_operand_without_rederiving_it() {
    let mut session = typed_value_session();

    assert_eq!(
        row_text(session.run("SELECT LAG(t6,1) OVER (ORDER BY id) FROM w")),
        [["NULL"], ["2020-01-01 10:20:30.123456"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LAG(d3,1) OVER (ORDER BY id) FROM w")),
        [["NULL"], ["01:02:03.456"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LAG(ts,1) OVER (ORDER BY id) FROM w")),
        [["NULL"], ["2020-01-01 10:20:30.1234"]]
    );
    // A DATE argument is the boundary that fsp alone cannot fail: it agrees
    // whether or not the operand is re-derived.
    assert_eq!(
        row_text(session.run("SELECT LAG(dt,1) OVER (ORDER BY id) FROM w")),
        [["NULL"], ["2020-03-04"]]
    );
    // The frame-reading value functions share the evaluator, so they share
    // the pass-through.
    assert_eq!(
        row_text(session.run("SELECT FIRST_VALUE(t6) OVER (ORDER BY id) FROM w")),
        [
            ["2020-01-01 10:20:30.123456"],
            ["2020-01-01 10:20:30.123456"]
        ]
    );

    // Both roles of a mixed-scale DATETIME pair, in both orders.
    assert_eq!(
        row_text(session.run("SELECT LAG(t0,1,t6) OVER (ORDER BY id) FROM w")),
        [["2020-01-01 10:20:30.123456"], ["2020-01-01 10:20:30"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LAG(t6,1,t0) OVER (ORDER BY id) FROM w")),
        [["2020-01-01 10:20:30"], ["2020-01-01 10:20:30.123456"]]
    );

    // The DATE operand of a `datetime(3)` result prints as a bare DATE in
    // both roles. (The TIME operand beside it converts against the session's
    // current date, so only its TIME part is pinned here.)
    let ft = merged_type(
        &mut session,
        "SELECT LAG(dt,1,d3) OVER (ORDER BY id) FROM w",
    );
    assert_eq!(ft.code(), tidb_datatype::FieldTypeCode::Datetime);
    assert_eq!((ft.flen(), ft.decimal()), (23, 3));
    let as_value = row_text(session.run("SELECT LAG(dt,1,d3) OVER (ORDER BY id) FROM w"));
    assert_eq!(as_value[1], ["2020-03-04"]);
    assert!(
        as_value[0][0].ends_with(" 01:02:03.456"),
        "the wrapped TIME(3) operand keeps its OWN three digits: {:?}",
        as_value[0]
    );
    let as_default = row_text(session.run("SELECT LAG(d3,1,dt) OVER (ORDER BY id) FROM w"));
    assert_eq!(as_default[0], ["2020-03-04"]);
    assert!(
        as_default[1][0].ends_with(" 01:02:03.456"),
        "the same TIME(3) operand in the value role: {:?}",
        as_default[1]
    );
}

/// A CONSTANT default is the one thing `buildLeadLag` DOES convert to the
/// merged type (`et.Value.ConvertTo(evalCtx.TypeCtx(), aggFuncDesc.RetTp)`),
/// so the pass-through above must not swallow it. The DECIMAL pair is the
/// boundary: reading a written `1.5` through the merged type's eval kind
/// alone -- the treatment its non-constant neighbours get -- leaves it `1.5`,
/// and Go answers with the SCALE-PADDED value.
///
/// ```text
/// select id, lag(dc,1,1.5) over (order by id) from q;   RS:1|1.50;2|1.50
/// select id, lag(dc,1,3) over (order by id) from q;     RS:1|3.00;2|1.50
/// ```
///
/// over `create table q(id int, dc decimal(10,2))` holding `(1,1.50)` and
/// `(2,2.50)`. (Quoted from `gorun`.)
#[test]
fn window_lag_still_converts_a_written_constant_default() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE q (id INT, dc DECIMAL(10,2))")
        .unwrap();
    session
        .run("INSERT INTO q VALUES (1,1.50),(2,2.50)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT LAG(dc,1,1.5) OVER (ORDER BY id) FROM q")),
        [["1.50"], ["1.50"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LAG(dc,1,3) OVER (ORDER BY id) FROM q")),
        [["3.00"], ["1.50"]]
    );
}

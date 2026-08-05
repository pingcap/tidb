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

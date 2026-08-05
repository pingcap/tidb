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

//! The five operator rules Go keys on the ARGUMENT EXPRESSION rather than on
//! the evaluated value -- see `ops::operand`.
//!
//! Each test below is a pair: the row Go answers one way, and the neighbouring
//! row that would answer IDENTICALLY if the rule were replaced by a blanket
//! one. A fixture without that second row cannot tell "ported the rule" from
//! "hardcoded this answer", which is why every case here comes in twos.
//!
//! Every expected value is quoted from a real (mock-backed) TiDB session run
//! over the same table shape; the `select ...` line in each comment is the
//! statement, and the value after it is what TiDB printed.

use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

/// Evaluates `sql` -- one select expression -- over a single row of the named
/// columns, through the same rewrite-then-evaluate path a table-backed query
/// takes. Returns the result's label, or the error's debug form.
///
/// This is the only way to put a genuine `FieldType` in front of the operator
/// dispatch from inside this crate: an `UNSIGNED` flag, a `YEAR` code and a
/// `TIME` code are exactly the facts a `Datum` does not carry, so a test that
/// evaluated bare datums could not reach any rule here.
fn over_columns(sql: &str, columns: &[(&str, FieldType, Datum)]) -> String {
    use tidb_ast::{QueryStmt, SelectField, Stmt};

    struct Resolver(Vec<(String, FieldType)>);
    impl crate::rewriter::ColumnResolver for Resolver {
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let name = path.last()?;
            let index = self.0.iter().position(|(n, _)| n == name)?;
            Some((index, self.0[index].1.clone(), index as i64 + 1))
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
    }

    let resolver = Resolver(
        columns
            .iter()
            .map(|(name, ft, _)| ((*name).to_owned(), ft.clone()))
            .collect(),
    );
    let field_types: Vec<FieldType> = columns.iter().map(|(_, ft, _)| ft.clone()).collect();

    let stmt = tidb_parser::parse(&format!("select {sql}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not a query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not a select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("no field expression")
    };
    let rewritten = match crate::rewriter::rewrite_expr_resolved(expr, &resolver) {
        Ok(rewritten) => rewritten,
        Err(err) => return format!("{err:?}"),
    };
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&field_types, 1);
    for (index, (_, _, value)) in columns.iter().enumerate() {
        chunk.append_datum(index, value);
    }
    match rewritten.eval(&crate::context::NoColumns, chunk.get_row(0)) {
        Ok(value) => value.label(),
        Err(err) => format!("{err:?}"),
    }
}

fn flagged(code: FieldTypeCode, flags: u32) -> FieldType {
    let mut ft = FieldType::new(code);
    ft.add_flags(flags);
    ft
}

fn varchar(name: &str, text: &str) -> (String, FieldType, Datum) {
    (
        name.to_owned(),
        FieldType::new(FieldTypeCode::VarString),
        Datum::new_string(text),
    )
}

/// Borrows an owned column triple as the slice `over_columns` takes.
macro_rules! columns {
    ($(($name:expr, $ft:expr, $value:expr)),* $(,)?) => {
        &[$(($name, $ft, $value)),*][..]
    };
}

/// A8. Go's integer signatures read `mysql.HasUnsignedFlag(args[i].GetType(ctx)
/// .GetFlag())`, never the value's own kind. `YEAR` is the type where the two
/// disagree: `pkg/ddl/add_column.go:1309-1319` stamps it `ZerofillFlag` and
/// then `UnsignedFlag`, while its value reads back as a plain signed integer.
///
/// ```text
/// select a - 2000 from y      ERROR 1690: BIGINT UNSIGNED value is out of range in '(test.y.a - 2000)'
/// select a - 1000 from y      990
/// ```
///
/// The second row is the boundary: a rule that made every `YEAR` subtraction
/// overflow, or that reinterpreted the SUBTRAHEND too, would fail it.
///
/// NOTE: this reaches the rule through a hand-built `YEAR` field type because
/// `tidb-executor`'s `CREATE TABLE` column builder has not yet ported Go's
/// `processColumnFlags`, so a real `YEAR` column in this engine carries
/// neither flag and `SELECT a - 2000` still answers -10. That is a catalog
/// gap, not an operator gap, and this test is what will notice when it closes.
#[test]
fn an_unsigned_flag_selects_gos_unsigned_integer_signature() {
    let year = || {
        flagged(
            FieldTypeCode::Year,
            FieldTypeFlags::UNSIGNED | FieldTypeFlags::ZEROFILL,
        )
    };
    assert_eq!(
        over_columns("a - 2000", columns![("a", year(), Datum::Int(1990))]),
        "IntOverflow"
    );
    assert_eq!(
        over_columns("a - 1000", columns![("a", year(), Datum::Int(1990))]),
        "UINT:990"
    );
    // The same column with the flag cleared is an ordinary signed subtraction,
    // which is what this engine answers for a real `YEAR` column today.
    assert_eq!(
        over_columns(
            "a - 2000",
            columns![("a", FieldType::new(FieldTypeCode::Year), Datum::Int(1990))]
        ),
        "INT:-10"
    );
}

/// B16. `DIV`'s result is unsigned when EITHER argument's field type carries
/// `UnsignedFlag`, and Go then reads the quotient back through `ToUint`, which
/// refuses a negative value (`builtin_arithmetic.go:952-967`). A `DOUBLE
/// UNSIGNED` or `DECIMAL UNSIGNED` is not a `Datum::UInt`, so the flag is the
/// only carrier.
///
/// ```text
/// select d DIV -1 from du     ERROR 1690: BIGINT UNSIGNED value is out of range in '(7 DIV -1)'
/// select e DIV -1 from du     ERROR 1690: BIGINT UNSIGNED value is out of range in '(7.00 DIV -1)'
/// select d DIV -8 from du     0
/// select e DIV -8 from du     0
/// select 7 DIV -1             -7
/// ```
///
/// The `-8` rows are the boundary Go itself carves out -- a quotient in
/// `(-1, 0]` truncates to 0 rather than overflowing -- and the last row is the
/// control that a blanket "negative quotient is an error" would break.
#[test]
fn div_reads_unsignedness_from_the_field_type_not_the_datum_kind() {
    let double_unsigned = || flagged(FieldTypeCode::Double, FieldTypeFlags::UNSIGNED);
    let decimal_unsigned = || flagged(FieldTypeCode::NewDecimal, FieldTypeFlags::UNSIGNED);
    let seven_decimal = || Datum::new_decimal(crate::Decimal::from_literal("7.00"));
    assert_eq!(
        over_columns(
            "d DIV -1",
            columns![("d", double_unsigned(), Datum::Real(7.0))]
        ),
        "IntOverflow"
    );
    assert_eq!(
        over_columns(
            "e DIV -1",
            columns![("e", decimal_unsigned(), seven_decimal())]
        ),
        "IntOverflow"
    );
    assert_eq!(
        over_columns(
            "d DIV -8",
            columns![("d", double_unsigned(), Datum::Real(7.0))]
        ),
        "UINT:0"
    );
    assert_eq!(
        over_columns(
            "e DIV -8",
            columns![("e", decimal_unsigned(), seven_decimal())]
        ),
        "UINT:0"
    );
    // The same values with no flag stay signed, quotient and all.
    assert_eq!(
        over_columns(
            "d DIV -1",
            columns![("d", FieldType::new(FieldTypeCode::Double), Datum::Real(7.0))]
        ),
        "INT:-7"
    );
}

/// B9. `GetAccurateCmpType` upgrades a duration-vs-text pair to the DURATION
/// domain only when the duration side is a `*Column` and the other side a
/// `*Constant` (`builtin_compare.go:1467-1483`); everything else stays
/// `getBaseCmpType`'s ETString and compares the duration's printed text.
///
/// ```text
/// select t = '1:00:00' from tt              1     column vs constant: duration
/// select t = concat('1:00',':00') from tt   1     folded to a constant first
/// select t = v from tt                      0     column vs column: '01:00:00' vs '1:00:00'
/// select t = 'xyz' from tt                  NULL  duration domain, unparseable
/// select t = w from tt                      0     string domain, so merely unequal
/// ```
///
/// The `concat` row is the boundary that separates "Go's post-fold
/// `*Constant`" from "this rewriter's `Expression::Constant`": the two
/// disagree there, and only the folding predicate answers 1.
#[test]
fn a_duration_compares_as_a_duration_only_against_a_constant() {
    let time = || {
        (
            "t",
            FieldType::new(FieldTypeCode::Duration),
            Datum::Duration(
                tidb_datatype::MySqlDuration::from_nanoseconds(3_600_000_000_000, 0)
                    .expect("one hour"),
            ),
        )
    };
    let (v_name, v_type, v_value) = varchar("v", "1:00:00");
    let (w_name, w_type, w_value) = varchar("w", "xyz");
    let row = || {
        vec![
            time(),
            (v_name.as_str(), v_type.clone(), v_value.clone()),
            (w_name.as_str(), w_type.clone(), w_value.clone()),
        ]
    };
    assert_eq!(over_columns("t = '1:00:00'", &row()), "INT:1");
    assert_eq!(over_columns("t = concat('1:00',':00')", &row()), "INT:1");
    assert_eq!(over_columns("t = v", &row()), "INT:0");
    assert_eq!(over_columns("t = 'xyz'", &row()), "NULL");
    assert_eq!(over_columns("t = w", &row()), "INT:0");
}

/// B10. A non-constant DECIMAL against a constant string compares as DECIMAL
/// rather than as `f64`, "in order not to lose precision"
/// (`builtin_compare.go:1457-1466`). The `!isConst` half of the test is what
/// keeps an all-constant comparison in the ETReal domain.
///
/// ```text
/// select d = '1234567890123456788' from dc                 0   column: DECIMAL domain
/// select d + 0 = '1234567890123456788' from dc             0   expression over a column: same
/// select d = '1234567890123456789.0' from dc               1
/// select 1234567890123456789 = '1234567890123456788'       1   all constant: ETReal, and equal
/// select d = s from dc                                     1   both columns: ETReal again
/// ```
///
/// Rows four and five are the boundary: both operands round to the SAME `f64`,
/// so a rule that compared every decimal-vs-string pair as decimal would turn
/// Go's 1 into 0.
#[test]
fn a_column_decimal_against_a_constant_string_compares_as_decimal() {
    let decimal19 = || {
        let mut ft = FieldType::new(FieldTypeCode::NewDecimal);
        ft.set_flen(19);
        ft.set_decimal(0);
        ft
    };
    let value = || Datum::new_decimal(crate::Decimal::from_literal("1234567890123456789"));
    let (s_name, s_type, s_value) = varchar("s", "1234567890123456788");
    let row = || {
        vec![
            ("d", decimal19(), value()),
            (s_name.as_str(), s_type.clone(), s_value.clone()),
        ]
    };
    assert_eq!(over_columns("d = '1234567890123456788'", &row()), "INT:0");
    assert_eq!(
        over_columns("d + 0 = '1234567890123456788'", &row()),
        "INT:0"
    );
    assert_eq!(over_columns("d = '1234567890123456789.0'", &row()), "INT:1");
    assert_eq!(
        over_columns("1234567890123456789 = '1234567890123456788'", &row()),
        "INT:1"
    );
    assert_eq!(over_columns("d = s", &row()), "INT:1");
}

/// B12. `unaryMinusFunctionClass.typeInfer` rebuilds the function on the
/// DECIMAL signature only when the argument is a `*Constant`
/// (`builtin_op.go:1009-1014`); a column keeps the Int signature, whose
/// `evalInt` reports `ErrOverflow` (`:1106-1124`).
///
/// ```text
/// select -b from bi     ERROR 1690: BIGINT value is out of range in '--9223372036854775808'
/// select -u from bi     ERROR 1690: BIGINT value is out of range in '-9223372036854775809'
/// select --9223372036854775808        9223372036854775808   constant: DECIMAL signature
/// select -u from bi2 (u = 2^63)      -9223372036854775808   representable, so no overflow
/// select -a from y   (a = 1990)      -1990                  unsigned, but in range
/// ```
///
/// The last three rows are the boundaries: a blanket "an integer negation that
/// leaves BIGINT is an error" breaks all of them, and a blanket "promote to
/// decimal" breaks the first two.
#[test]
fn unary_minus_promotes_to_decimal_only_for_a_constant() {
    let bigint = || FieldType::new(FieldTypeCode::LongLong);
    let bigint_unsigned = || flagged(FieldTypeCode::LongLong, FieldTypeFlags::UNSIGNED);
    assert_eq!(
        over_columns("-b", columns![("b", bigint(), Datum::Int(i64::MIN))]),
        "IntOverflow"
    );
    assert_eq!(
        over_columns(
            "-u",
            columns![("u", bigint_unsigned(), Datum::UInt(9223372036854775809))]
        ),
        "IntOverflow"
    );
    assert_eq!(
        over_columns(
            "--9223372036854775808",
            columns![("b", bigint(), Datum::Int(0))]
        ),
        "DEC:9223372036854775808"
    );
    assert_eq!(
        over_columns(
            "-u",
            columns![("u", bigint_unsigned(), Datum::UInt(1 << 63))]
        ),
        "INT:-9223372036854775808"
    );
    assert_eq!(
        over_columns(
            "-a",
            columns![(
                "a",
                flagged(
                    FieldTypeCode::Year,
                    FieldTypeFlags::UNSIGNED | FieldTypeFlags::ZEROFILL
                ),
                Datum::Int(1990)
            )]
        ),
        "INT:-1990"
    );
}

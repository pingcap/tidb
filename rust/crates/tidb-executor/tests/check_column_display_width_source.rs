//! Fail-before regressions for the ported planner `checkColumn` display-width,
//! field-size, precision, scale and SET-member checks
//! (`pkg/planner/core/preprocess.go:1578-1680`).

use tidb_executor::{run_create_table_on, Catalog};

fn create_error(sql: &str) -> String {
    let mut catalog = Catalog::default();
    match run_create_table_on(sql, &mut catalog) {
        Ok(_) => "ACCEPTED (should have been rejected)".to_string(),
        Err(e) => e.to_string(),
    }
}

#[test]
fn bit_above_64_is_rejected_with_1439() {
    let err = create_error("create table t (c bit(65))");
    assert_eq!(
        err, "Display width out of range for column 'c' (max = 64)",
        "Go: ErrTooBigDisplayWidth with MaxBitDisplayWidth"
    );
}

#[test]
fn bit_zero_is_rejected_with_3013() {
    let err = create_error("create table t (c bit(0))");
    assert_eq!(err, "Invalid size for column 'c'.", "Go: ErrInvalidFieldSize");
}

#[test]
fn char_above_255_is_rejected_with_1074() {
    let err = create_error("create table t (c char(300))");
    assert_eq!(
        err,
        "Column length too big for column 'c' (max = 255); use BLOB or TEXT instead",
        "Go: ErrTooBigFieldLength with MaxFieldCharLength"
    );
}

#[test]
fn varchar_above_charset_max_is_rejected_with_1074() {
    let err = create_error("create table t (c varchar(40000))");
    assert_eq!(
        err,
        "Column length too big for column 'c' (max = 16383); use BLOB or TEXT instead",
        "Go: IsVarcharTooBigFieldLength with 65535/4 for utf8mb4"
    );
}

#[test]
fn decimal_precision_above_65_is_rejected_with_1426() {
    let err = create_error("create table t (c decimal(70,5))");
    assert_eq!(
        err, "Too-big precision 70 specified for 'c'. Maximum is 65.",
        "Go: ErrTooBigPrecision with MaxDecimalWidth"
    );
}

#[test]
fn float_scale_above_30_is_rejected_with_1425() {
    let err = create_error("create table t (c float(10,31))");
    assert_eq!(
        err, "Too big scale 31 specified for column 'c'. Maximum is 30.",
        "Go: ErrTooBigScale with MaxFloatingTypeScale"
    );
}

#[test]
fn float_width_above_255_is_rejected_with_1439() {
    let err = create_error("create table t (c float(300,2))");
    assert_eq!(
        err, "Display width out of range for column 'c' (max = 255)",
        "Go: ErrTooBigDisplayWidth with MaxFloatingTypeWidth"
    );
}

#[test]
fn set_member_with_comma_is_rejected_with_1367() {
    let err = create_error("create table t (c set('a,b'))");
    assert_eq!(
        err, "Illegal SET 'a,b' value found during parsing",
        "Go: ErrIllegalValueForType with the SET type name"
    );
}

#[test]
fn valid_shapes_still_create() {
    let mut catalog = Catalog::default();
    for sql in [
        "create table ok1 (c bit(64))",
        "create table ok2 (c bit(1))",
        "create table ok3 (c char(255))",
        "create table ok4 (c varchar(16383))",
        "create table ok5 (c decimal(65,30))",
        "create table ok6 (c float(255,30))",
        "create table ok7 (c set('a'))",
        "create table ok8 (c vector(16383))",
        "create table ok9 (c float)",
        "create table ok10 (c float(0))",
    ] {
        run_create_table_on(sql, &mut catalog)
            .unwrap_or_else(|e| panic!("{sql} must create in Go and here: {e}"));
    }
}

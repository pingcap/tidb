//! `timestampFunctionClass` and `weightStringFunctionClass`
//! (`pkg/expression/builtin_time.go`, `builtin_string.go`), pinned against a
//! Go testkit session.

use std::cell::RefCell;

use super::{chunk_e, chunk_e_with};
use crate::{Columns, Datum, ErrorLevel};

/// A statement context at one truncate level that records its warnings.
struct Statement {
    level: ErrorLevel,
    warnings: RefCell<Vec<(u16, String)>>,
}

impl Statement {
    fn new(level: ErrorLevel) -> Self {
        Self {
            level,
            warnings: RefCell::new(Vec::new()),
        }
    }
}

impl Columns for Statement {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn truncate_level(&self) -> ErrorLevel {
        self.level
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
}

/// `timestampFunctionClass.getFunction` fixes `isFloat` from the ARGUMENT
/// TYPE, so a DECIMAL and a FLOAT read a packed date's suffix as a fraction
/// while the same text as a string reads it as an hour.
#[test]
fn timestamp_is_float_follows_the_argument_type_like_go() {
    for (expr, want) in [
        ("timestamp(20240315.5)", "STR:2024-03-15 00:00:00.0"),
        ("timestamp('20240315.5')", "STR:2024-03-15 05:00:00.0"),
        ("timestamp(20240315)", "STR:2024-03-15 00:00:00"),
        ("timestamp(cast(20240315 as unsigned))", "STR:2024-03-15 00:00:00"),
        ("timestamp(cast(1 as signed) + 20240314)", "STR:2024-03-15 00:00:00"),
    ] {
        assert_eq!(chunk_e(expr), want, "{expr}");
    }
}

/// Both `handleInvalidTimeError` sites: a warning and NULL on a read, the
/// error itself where a truncation is an error (a strict INSERT ... SELECT).
#[test]
fn timestamp_invalid_arguments_go_through_handle_invalid_time_error() {
    for (expr, message) in [
        ("timestamp('x')", "Incorrect datetime value: 'x'"),
        (
            "timestamp('2020-01-01', '10:99:99')",
            "Truncated incorrect time value: '10:99:99'",
        ),
        (
            "timestamp('2020-01-01', '-900:00:00')",
            "Truncated incorrect time value: '-900:00:00'",
        ),
    ] {
        let read = Statement::new(ErrorLevel::Warn);
        assert_eq!(chunk_e_with(expr, &read), "NULL", "{expr}");
        assert_eq!(*read.warnings.borrow(), [(1292, message.to_owned())], "{expr}");

        let strict = Statement::new(ErrorLevel::Error);
        let failed = chunk_e_with(expr, &strict);
        assert!(failed.contains(message), "{expr}: {failed}");
        assert!(strict.warnings.borrow().is_empty(), "{expr}");
    }
}

/// `tm.Add`'s own `Check` error is returned as it is, at every level; the
/// zero date of a sum before year 1 passes the check, and a non-duration
/// second argument is NULL before any parse.
#[test]
fn timestamp_sum_follows_time_add_like_go() {
    let read = Statement::new(ErrorLevel::Warn);
    let failed = chunk_e_with("timestamp('9999-12-31 23:59:59', '10:00:00')", &read);
    assert!(
        failed.contains("Incorrect time value: '{10000 1 1 9 59 59 0}'"),
        "{failed}"
    );
    assert!(read.warnings.borrow().is_empty());

    for (expr, want) in [
        ("timestamp('0001-01-01', '-800:00:00')", "STR:0000-00-00 16:00:00"),
        ("timestamp('2020-01-01', '1 10:00:00.5')", "STR:2020-01-02 10:00:00.5"),
        ("timestamp('2020-01-01', 'x')", "NULL"),
    ] {
        let strict = Statement::new(ErrorLevel::Error);
        assert_eq!(chunk_e_with(expr, &strict), want, "{expr}");
    }
}

/// `weightStringFunctionClass.getFunction`: the padding and its length are
/// fixed when the function is built, a zero length is refused there, and a
/// numeric argument is NULL unless it is padded AS BINARY.
#[test]
fn weight_string_padding_is_built_by_the_class_like_go() {
    for (expr, want) in [
        ("hex(weight_string('a' as char(3)))", "STR:61"),
        ("hex(weight_string('x' as binary(2)))", "STR:7800"),
        ("hex(weight_string('abc' as char(2)))", "STR:6162"),
        ("hex(weight_string(1 as char(2)))", "NULL"),
        ("hex(weight_string(1 as binary(2)))", "STR:3100"),
        ("hex(weight_string(1.5))", "NULL"),
        ("hex(weight_string(null as char(2)))", "NULL"),
    ] {
        assert_eq!(chunk_e(expr), want, "{expr}");
    }
    for expr in ["weight_string('a' as char(0))", "weight_string('a' as binary(0))"] {
        let failed = chunk_e(expr);
        assert!(
            failed.contains("Incorrect type for argument 0 in function weight_string."),
            "{expr}: {failed}"
        );
    }
}

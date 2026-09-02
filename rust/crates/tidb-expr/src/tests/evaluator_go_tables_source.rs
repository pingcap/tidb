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

//! GO PORTS from `pkg/expression/evaluator_test.go` on `origin/master`:
//! `TestExtract` (:488), `TestMod` (:606), the representable unary-operator
//! kinds of `TestUnaryOp` (:534), `TestSleep` (:102), and the
//! optional-eval-props shape of `TestOptionalProp` (:626). The four binary
//! operator suites (`TestBinopComparison`, `:190`; `TestBinopLogic`, `:268`;
//! `TestBinopBitop`, `:307`; `TestBinopNumeric`, `:344`) are carried whole by
//! sibling module [`super::evaluator_binop`] and are not duplicated here.

use super::*;
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction};
use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode};

/// A leaked-empty row context for one-shot scalar evaluation, mirroring the
/// shared helper other source modules keep locally.
fn empty_row() -> tidb_chunk::row::Row<'static> {
    let chunk = Box::leak(Box::new(tidb_chunk::chunk::Chunk::new_empty(&[])));
    chunk.get_row(0)
}

/// The complete master table of `pkg/expression/evaluator_test.go:488
/// TestExtract`, minus the composite-unit rows whose argument carries
/// fractional seconds -- those six are the ignored sibling below. The units
/// asserted here (every simple unit plus YEAR_MONTH) agree with Go against
/// `'2011-11-11 10:10:10.123456'`: WEEK is 45 under default
/// `default_week_format` semantics and QUARTER is 4.
#[test]
fn extract_master_unit_table_matches_source() {
    let text = "'2011-11-11 10:10:10.123456'";
    let table = [
        ("MICROSECOND", "123456"),
        ("SECOND", "10"),
        ("MINUTE", "10"),
        ("HOUR", "10"),
        ("DAY", "11"),
        ("WEEK", "45"),
        ("MONTH", "11"),
        ("QUARTER", "4"),
        ("YEAR", "2011"),
        ("YEAR_MONTH", "201111"),
    ];
    for (unit, expect) in table {
        assert_eq!(
            e(&format!("extract({unit} from {text})")),
            format!("INT:{expect}"),
            "EXTRACT({unit} FROM {text})"
        );
    }
    // The source's nil-argument row.
    assert_eq!(e("extract(SECOND from NULL)"), "NULL");
}

/// Composite-unit rows of `pkg/expression/evaluator_test.go:488
/// TestExtract` whose string carries a fractional-second suffix:
/// `SECOND_MICROSECOND -> 10123456`, `MINUTE_MICROSECOND -> 1010123456`,
/// `MINUTE_SECOND -> 1010`, `HOUR_MICROSECOND -> 101010123456`,
/// `HOUR_SECOND -> 101010`, `HOUR_MINUTE -> 1010`,
/// `DAY_MICROSECOND -> 11101010123456`, `DAY_SECOND -> 11101010`,
/// `DAY_MINUTE -> 111010`, `DAY_HOUR -> 1110`.
#[test]
fn extract_composite_units_over_fractional_strings_match_source() {
    let text = "'2011-11-11 10:10:10.123456'";
    let table = [
        ("SECOND_MICROSECOND", "10123456"),
        ("MINUTE_MICROSECOND", "1010123456"),
        ("MINUTE_SECOND", "1010"),
        ("HOUR_MICROSECOND", "101010123456"),
        ("HOUR_SECOND", "101010"),
        ("HOUR_MINUTE", "1010"),
        ("DAY_MICROSECOND", "11101010123456"),
        ("DAY_SECOND", "11101010"),
        ("DAY_MINUTE", "111010"),
        ("DAY_HOUR", "1110"),
    ];
    for (unit, expect) in table {
        assert_eq!(
            e(&format!("extract({unit} from {text})")),
            format!("INT:{expect}"),
            "EXTRACT({unit} FROM {text})"
        );
    }

    // Clock-unit input uses the duration formulas, but it must retain the
    // fractional suffix in the *_MICROSECOND forms as Go does.
    assert_eq!(
        e("extract(second_microsecond from '10:10:10.123456')"),
        "INT:10123456"
    );
}

/// The three rows of `pkg/expression/evaluator_test.go:606 TestMod`.
#[test]
fn mod_source_rows() {
    // mod(234, 10) -> 4; mod(29, 9) -> 2.
    assert_eq!(e("mod(234, 10)"), "INT:4");
    assert_eq!(e("mod(29, 9)"), "INT:2");
    // mod(34.5, 3) keeps its fraction as a decimal result.
    assert_eq!(e("mod(34.5, 3)"), "DEC:1.5");
}

/// Unary operator applied to one datum through the same seam Go's
/// `datumsToConstants(types.MakeDatums(arg)) + funcs[op].getFunction`
/// drives: a one-argument op over an already-typed value.
fn apply_unary_of(op: tidb_ast::UnaryOp, operand: Datum) -> Datum {
    crate::apply_unary(op, operand.clone(), &NoColumns)
        .unwrap_or_else(|err| panic!("{op:?}({operand:?}) must evaluate: {err:?}"))
}

/// Kind-level rows of `pkg/expression/evaluator_test.go:534 TestUnaryOp`,
/// first table: logical NOT reinterprets BinaryLiteral bytes and Enum/Set
/// numeric values before testing truthiness; BitNeg negates signed bits onto
/// the unsigned domain; Minus covers int/uint/string-bytes/enum-set domains
/// exactly like Go's expectations (`int64` minus stays int, everything stringy
/// lands in the REAL domain).
#[test]
fn unary_op_kind_rows_match_source() {
    use tidb_ast::UnaryOp;
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};

    // NOT table.
    assert_eq!(apply_unary_of(UnaryOp::Not, Datum::Int(1)), Datum::Int(0));
    assert_eq!(apply_unary_of(UnaryOp::Not, Datum::Int(0)), Datum::Int(1));
    assert_eq!(apply_unary_of(UnaryOp::Not, Datum::Null), Datum::Null);
    assert_eq!(
        apply_unary_of(
            UnaryOp::Not,
            Datum::new_mysql_bit(BinaryLiteral::from_uint(0, None))
        ),
        Datum::Int(1),
        "NOT(b'', zero binary literal)"
    );
    assert_eq!(
        apply_unary_of(
            UnaryOp::Not,
            Datum::new_mysql_bit(BinaryLiteral::from_uint(1, None))
        ),
        Datum::Int(0)
    );
    assert_eq!(
        apply_unary_of(
            UnaryOp::Not,
            Datum::new_enum(MysqlEnum::new("a", 1), tidb_datatype::Collation::Binary)
        ),
        Datum::Int(0),
        "NOT(enum value 1)"
    );
    assert_eq!(
        apply_unary_of(
            UnaryOp::Not,
            Datum::new_set(MysqlSet::new("a", 1), tidb_datatype::Collation::Binary)
        ),
        Datum::Int(0),
        "NOT(set value 1)"
    );

    // BitNeg table.
    assert_eq!(apply_unary_of(UnaryOp::BitNeg, Datum::Null), Datum::Null);
    assert_eq!(
        apply_unary_of(UnaryOp::BitNeg, Datum::Int(-1)),
        Datum::UInt(0)
    );

    // Minus table.
    assert_eq!(apply_unary_of(UnaryOp::Minus, Datum::Null), Datum::Null);
    assert_eq!(
        apply_unary_of(UnaryOp::Minus, Datum::Real(1.0)),
        Datum::Real(-1.0)
    );
    assert_eq!(
        apply_unary_of(UnaryOp::Minus, Datum::Int(1)),
        Datum::Int(-1)
    );
    assert_eq!(
        apply_unary_of(UnaryOp::Minus, Datum::UInt(1)),
        Datum::Int(-1),
        "unsigned minus promotes into the signed domain"
    );
    assert_eq!(
        apply_unary_of(UnaryOp::Minus, Datum::new_string("1.0")),
        Datum::Real(-1.0)
    );
    assert_eq!(
        apply_unary_of(UnaryOp::Minus, Datum::new_bytes(b"1.0".to_vec())),
        Datum::Real(-1.0)
    );
}

/// Second table of `pkg/expression/evaluator_test.go:534 TestUnaryOp`: Minus
/// over DECIMAL/DURATION/DATETIME operands compared against expected datums
/// with the BINARY collator (`result.Compare(expect)` == 0). Decimal `-1` and
/// duration-to-zero-decimal are exact here; the datetime row compares
/// numerically after Go's TO_DAYS-like packing (`20091110230000` negated).
#[test]
fn unary_minus_temporal_and_decimal_operands_match_source() {
    use tidb_ast::UnaryOp;

    // NewDecFromInt(1) -> -1: result must COMPARE equal to -1 decimal.
    let folded_minus_one = crate::Decimal::from_literal("-1");
    match apply_unary_of(
        UnaryOp::Minus,
        Datum::Decimal(crate::Decimal::from_literal("1")),
    ) {
        Datum::Decimal(value) => {
            assert!(
                value.add(&folded_minus_one.negate()).is_zero(),
                "-DEC(1) == DEC(-1), got {value:?}"
            );
        }
        other => panic!("expected a decimal answer, got {other:?}"),
    }

    // ZeroDuration -> decimal ZERO (`new(types.MyDecimal)`).
    let zero_duration = tidb_datatype::MySqlDuration::new(0, 0, 0, 0, 0).expect("zero duration");
    match apply_unary_of(UnaryOp::Minus, Datum::Duration(zero_duration)) {
        Datum::Decimal(value) => {
            assert!(
                value.is_zero(),
                "-zero-duration folds to decimal zero, got {value:?}"
            );
        }
        other => panic!("expected a decimal answer for -duration, got {other:?}"),
    }

    // Datetime 2009-11-10 23:00:00 fsp 0 -> DECIMAL -20091110230000.
    let time = tidb_datatype::Time::from_date_checked(
        2009,
        11,
        10,
        23,
        0,
        0,
        0,
        tidb_datatype::TimeType::DateTime,
        0,
    )
    .expect("valid source datetime");
    match apply_unary_of(UnaryOp::Minus, Datum::Time(time)) {
        Datum::Decimal(value) => {
            let want = crate::Decimal::from_literal("-20091110230000");
            assert!(
                value.add(&want.negate()).is_zero(),
                "-datetime packs to {value:?}, want DEC:-20091110230000"
            );
        }
        other => panic!("expected a decimal answer for -datetime, got {other:?}"),
    }
}

/// Hybrid-kind Minus rows of `pkg/expression/evaluator_test.go:534
/// TestUnaryOp`'s FIRST table ({Enum{a,1} -> -1.0}, {Set{a,1} -> -1.0},
/// {BinaryLiteral(1) -> -1.0}): Go answers in the REAL domain.
#[test]
fn unary_minus_hybrid_and_binary_literal_kinds_match_source() {
    use tidb_ast::UnaryOp;
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};

    let cases = [
        Datum::new_enum(MysqlEnum::new("a", 1), tidb_datatype::Collation::Binary),
        Datum::new_set(MysqlSet::new("a", 1), tidb_datatype::Collation::Binary),
        Datum::BinaryLiteral(BinaryLiteral::from_uint(1, None)),
    ];
    for operand in cases {
        assert_eq!(
            apply_unary_of(UnaryOp::Minus, operand),
            Datum::Real(-1.0),
            "unary minus should use Go's REAL signature for hybrid operands"
        );
    }
}

/// Value tiers of `pkg/expression/evaluator_test.go:102 TestSleep`. Under WARN
/// errctx levels (`non-strict`) SLEEP(NULL) and SLEEP(-1) each downgrade their
/// "Incorrect arguments to sleep" error to one warning and answer 0; at ERROR
/// levels both abort evaluation. The timing halves (`SLEEP(0.5)` >= 0.5s,
/// `SLEEP(3)` returning 1 within <= 2s once the SQLKiller signal fires, and
/// the `InInsertStmt` kill path answering 1) need real wall-clock sleeps plus
/// the session killer hook this crate's context contract does not carry.
#[test]
fn sleep_errctx_levels_and_null_arguments_follow_the_caller() {
    use std::cell::RefCell;
    struct Levels {
        warn: bool,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Levels {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
        fn handle_sleep_incorrect_argument(&self) -> Result<(), EvalError> {
            if self.warn {
                self.append_warning(1210, "Incorrect arguments to sleep");
                Ok(())
            } else {
                Err(EvalError::IncorrectArguments(
                    "Incorrect arguments to sleep".to_owned(),
                ))
            }
        }
        fn sleep_for(&self, _duration: std::time::Duration) -> bool {
            false
        }
    }

    fn sleep_row(arg: Datum, ctx: &impl Columns) -> Result<Datum, EvalError> {
        ScalarFunction::new(
            CiString::new("sleep"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Constant(Constant::new(
                arg,
                FieldType::new(FieldTypeCode::Double),
            ))],
        )
        .eval(ctx, empty_row())
    }

    // Non-strict model.
    let warn_ctx = Levels {
        warn: true,
        warnings: RefCell::new(Vec::new()),
    };
    assert_eq!(sleep_row(Datum::Null, &warn_ctx), Ok(Datum::Int(0)));
    assert_eq!(warn_ctx.warnings.borrow().len(), 1);
    assert_eq!(sleep_row(Datum::Real(-1.0), &warn_ctx), Ok(Datum::Int(0)));
    assert_eq!(warn_ctx.warnings.borrow().len(), 2);

    // Strict model error case.
    let strict = Levels {
        warn: false,
        warnings: RefCell::new(Vec::new()),
    };
    assert!(matches!(
        sleep_row(Datum::Null, &strict),
        Err(EvalError::IncorrectArguments(_))
    ));
    assert!(matches!(
        sleep_row(Datum::Real(-2.5), &strict),
        Err(EvalError::IncorrectArguments(_))
    ));
}

/// Timing-and-interrupt half of `pkg/expression/evaluator_test.go:102
/// TestSleep` (see [`sleep_errctx_levels_and_null_arguments_follow_the_caller`]
/// for the split rationale).
#[test]
#[ignore = "go-parity-gap: SQLKiller.SendKillSignal interruptibility and the InInsertStmt kill path need real execution time absent from the value-tier ctx"]
fn test_sleep_timing_and_kill_signal_halves() {}

/// `pkg/expression/evaluator_test.go:626 TestOptionalProp` pins
/// `GetOptionalEvalPropsForExpr`: PLUS over current_user/tidb_is_ddl_owner
/// scalar functions requires CURRENT_USER | DDL_OWNER, adding GetLock unions
/// ADVISORY_LOCK, PLAIN PLUS requires nothing, and `EvaluatorSuite`
/// aggregates the union across its expressions. No expression-level prop
/// walk exists in this crate -- `required_optional_eval_props` lives only on
/// provider readers (`expropt/*`) and sets live in `exprctx.rs`.
#[test]
#[ignore = "go-parity-gap: GetOptionalEvalPropsForExpr/EvaluatorSuite.RequiredOptionalEvalProps aggregation is unported; tidb-expr tracks prop sets only on providers, not on expression trees"]
fn test_optional_prop() {}

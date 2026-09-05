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

//! Source-first ports of `pkg/expression.part4` (`func Test*` items 181–240
//! on `origin/master`, sorted by file path then line): the tail of
//! `builtin_math_test.go` (`TestCRC32` .. `TestCot`), the shared
//! `builtin_math_vec_test.go` map harnesses, the whole
//! `builtin_miscellaneous_test.go` family and its vectorized sibling,
//! `builtin_op_test.go` and `builtin_op_vec_test.go`, and
//! `builtin_other_test.go::TestBitCount`. Every expectation was re-derived
//! from the Go source on `origin/master`, not from earlier notes; families
//! already pinned by earlier batches are cited in the receipt rather than
//! duplicated row-for-row here.

use std::cell::RefCell;

use super::*;
use crate::builtin_ext::compare2::dispatch as compare2_dispatch;
use crate::builtin_ext::misc::dispatch_in as misc_dispatch_in;
use crate::builtin_op::infer_unary_op_type;
use crate::builtin_registry::verify_args_by_count;
use crate::expression::Expression;
use crate::math_fn::{conv_valid_prefix, dispatch_values as math_dispatch_values};
use crate::rewriter::result_type::builtin_return_type;
use crate::scalar_function::ScalarFunction;
use tidb_ast::{CiString, QueryStmt, SelectField, Stmt};
use tidb_datatype::{
    Collation, FieldType, FieldTypeCode as C, FieldTypeFlags, MySqlDuration, SessionTimeZone, Time,
};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// One constant argument typed by its datum kind -- Go `kindToFieldType`
/// (`pkg/expression/evaluator_test.go:33`), used by the signature-built tests.
fn const_arg(datum: Datum) -> Expression {
    let field_type = match &datum {
        Datum::Null => FieldType::new(C::Null),
        Datum::Int(_) => int_ft(),
        Datum::UInt(_) => uint_ft(),
        Datum::Float32(_) | Datum::Real(_) => real_ft(),
        Datum::String(_) | Datum::Bytes(_) => text_ft(),
        Datum::Decimal(_) => FieldType::new(C::NewDecimal),
        Datum::Duration(_) => FieldType::new(C::Duration),
        Datum::Time(time) => match time.kind() {
            tidb_datatype::TimeType::Date => FieldType::new(C::Date),
            tidb_datatype::TimeType::Timestamp => FieldType::new(C::Timestamp),
            tidb_datatype::TimeType::DateTime => FieldType::new(C::Datetime),
        },
        Datum::Json(_) => FieldType::new(C::Json),
        other => panic!("no test mapping for {other:?}"),
    };
    Expression::Constant(crate::constant::Constant::new(datum, field_type))
}

fn int_ft() -> FieldType {
    FieldType::new(C::LongLong)
}

fn uint_ft() -> FieldType {
    FieldType::new(C::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED)
}

fn real_ft() -> FieldType {
    FieldType::new(C::Double)
}

fn text_ft() -> FieldType {
    FieldType::new(C::VarString)
}

fn decimal_zero_ft() -> FieldType {
    let mut ft = FieldType::new(C::NewDecimal);
    ft.set_decimal(0);
    ft
}

fn uint_result() -> FieldType {
    uint_ft()
}

fn int_result() -> FieldType {
    int_ft()
}

/// Go `evalBuiltinFunc` (`builtin_test.go:54`) against an explicit context:
/// build the scalar function presenting `ret_type`, evaluate over one empty
/// virtual row.
fn eval_as(
    name: &str,
    args: Vec<Datum>,
    ret_type: FieldType,
    ctx: &impl Columns,
) -> Result<Datum, EvalError> {
    let function = ScalarFunction::new(
        CiString::new(name),
        ret_type,
        args.into_iter().map(const_arg).collect(),
    );
    function.eval(ctx, empty_row())
}

/// Same evaluation with the discarding default context.
fn eval_default(name: &str, args: Vec<Datum>, ret_type: FieldType) -> Result<Datum, EvalError> {
    eval_as(name, args, ret_type, &crate::context::NoColumns)
}

fn empty_row() -> tidb_chunk::row::Row<'static> {
    let chunk = Box::leak(Box::new(tidb_chunk::chunk::Chunk::new_empty(&[])));
    chunk.get_row(0)
}

/// A [`Columns`] stub whose only live behavior is the warning sink. The
/// trait's default truncate level is Warn, matching Go mock sessions built
/// with `WithIgnoreTruncateErr(true)` (or warn-level errctx maps).
struct WarnCountCtx {
    warnings: RefCell<Vec<(u16, String)>>,
}

impl WarnCountCtx {
    fn new() -> Self {
        Self {
            warnings: RefCell::new(Vec::new()),
        }
    }
    fn count(&self) -> usize {
        self.warnings.borrow().len()
    }
}

impl Columns for WarnCountCtx {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
}

/// Evaluates `sql` over one row of typed columns through the rewrite-then-
/// evaluate path a table-backed query takes. This is how an argument's FIELD
/// TYPE reaches operator dispatch from inside this crate: an `UNSIGNED`
/// flag or a declared width travels there, never on a `Datum`.
fn chunk_row_value(sql: &str, columns: &[(&str, FieldType, Datum)]) -> String {
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

fn s(text: &str) -> Datum {
    Datum::new_string(text.to_owned())
}

/// A binary-collation string datum carrying raw bytes -- this tier's
/// VARBINARY-constant stand-in for Go's byte-string data generators.
fn bin_str(bytes: &[u8]) -> Datum {
    Datum::new_collation_string(bytes.to_vec(), Collation::Binary)
}

/// Dispatches one value through whichever ported family owns the name --
/// the single map Go reaches via `funcs[name].getFunction(...)`. The misc
/// dispatcher preserves warnings but the comparison/misc leaf level of these
/// tests is warning-free by construction.
fn call(name: &str, vals: &[Datum]) -> Datum {
    compare2_dispatch(name, vals, &crate::context::NoColumns)
        .or_else(|| misc_dispatch_in(name, vals, &crate::context::NoColumns))
        .unwrap_or_else(|| panic!("{name} must belong to a ported family"))
        .unwrap_or_else(|err| panic!("{name}({vals:?}): {err:?}"))
}

// ---------------------------------------------------------------------------
// pkg/expression/builtin_math_test.go (items 181–194)
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_math_test.go:543 TestCRC32`, GBK half: with
/// `character_set_connection=gbk`, the string constants reach CRC32 as
/// GBK-encoded bytes (`一` = D2 BB, `一二三` = D2 BB B6 FE C8 FD), so their
/// hash inputs differ from any UTF-8 literal.  The connection-aware rewrite
/// applies the same `to_binary` boundary before evaluating CRC32.
#[test]
fn crc32_gbk_charset_connection_rows() {
    struct GbkSession;

    impl crate::rewriter::ColumnResolver for GbkSession {
        fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
            None
        }

        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }

        fn connection_charset_info(&self) -> (&str, &str) {
            ("gbk", "gbk_bin")
        }
    }

    impl Columns for GbkSession {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn connection_charset_info(&self) -> (&str, &str) {
            ("gbk", "gbk_bin")
        }
    }

    let eval = |sql: &str| {
        let statement = tidb_parser::parse(&format!("SELECT {sql}")).expect("parse");
        let Stmt::Query(query) = statement else {
            panic!("expected query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("expected SELECT")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expected expression")
        };
        let rewritten = crate::rewriter::rewrite_expr_resolved(expr, &GbkSession).expect("rewrite");
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        rewritten.eval(&GbkSession, chunk.get_row(0))
    };

    assert_eq!(
        eval("crc32('一二三')").expect("CRC32 evaluates"),
        Datum::UInt(3_461_331_449)
    );
    assert_eq!(
        eval("crc32('一')").expect("CRC32 evaluates"),
        Datum::UInt(2_925_846_374)
    );
}

/// Go `pkg/expression/builtin_math_test.go:578 TestConv`: the complete
/// conversion table, the direct `getValidPrefix` unit rows, and the
/// VarString result-type assertions every table row carries alongside its
/// value assertion.
#[test]
fn conv_source_table_type_and_valid_prefix_rows() {
    for (input, expected) in [
        ("conv('a', 16, 2)", "STR:1010"),
        ("conv('6E', 18, 8)", "STR:172"),
        ("conv('-17', 10, -18)", "STR:-H"),
        ("conv('-17', 10, 18)", "STR:2D3FGB0B9CG4BD1H"),
        ("conv(null, 10, 10)", "NULL"),
        ("conv('+18aZ', 7, 36)", "STR:1"),
        (
            "conv('18446744073709551615', -10, 16)",
            "STR:7FFFFFFFFFFFFFFF",
        ),
        ("conv('12F', -10, 16)", "STR:C"),
        ("conv('  FF ', 16, 10)", "STR:255"),
        ("conv('TIDB', 10, 8)", "STR:0"),
        ("conv('aa', 10, 2)", "STR:0"),
        ("conv(' A', -10, 16)", "STR:0"),
        ("conv('a6a', 10, 8)", "STR:0"),
        // A base outside 2..=36 answers NULL rather than erroring.
        ("conv('a6a', 1, 8)", "NULL"),
    ] {
        assert_eq!(e(input), expected, "{input}");
    }

    // Every CONV call reports TypeVarString / utf8mb4 / utf8mb4_bin with no
    // flag bits (the four type assertions at the top of the Go loop).
    let args = [
        const_arg(Datum::new_string("a".to_owned())),
        const_arg(Datum::Int(16)),
        const_arg(Datum::Int(2)),
    ];
    let ret = builtin_return_type("conv", &args).expect("CONV is typed");
    assert_eq!(ret.code(), C::VarString);
    assert_eq!(ret.charset_name(), "utf8mb4");
    assert_eq!(ret.collation_name(), "utf8mb4_bin");
    assert_eq!(ret.flags(), 0);

    // Direct unit vectors for the prefix scanner behind CONV
    // (`getValidPrefix`): sign handling plus digits valid in from_base.
    assert_eq!(conv_valid_prefix("-123456D1f", 5), "-1234");
    assert_eq!(conv_valid_prefix("+12azD", 16), "12a");
    assert_eq!(conv_valid_prefix("+", 12), "");

    // Building the three-zero call succeeds (Go:
    // `funcs[ast.Conv].getFunction(ctx, []Expression{NewZero() x3})`).
    let zeros = [Datum::Int(0), Datum::Int(0), Datum::Int(0)];
    assert!(math_dispatch_values("CONV", &zeros, &WarnCountCtx::new()).is_some());
}

/// Go `pkg/expression/builtin_math_test.go:677 TestDegrees`,
/// `:759 TestRadians`, `:794 TestSin`, `:838 TestCos`, `:879 TestAcos`,
/// `:918 TestAsin`, `:957 TestAtan`, `:996 TestTan` -- their `getWarning`
/// rows: coercing a non-numeric string prefix warns EXACTLY ONCE while still
/// answering the numeric-prefix result. Value halves of all tables live in
/// `tests::math::transcendental_source_vectors`.
#[test]
fn math_string_coercion_raises_one_truncate_warning_each() {
    for (name, invalid_texts) in [
        ("DEGREES", vec!["abc", "+1abc"]),
        ("RADIANS", vec!["notNum"]),
        ("SIN", vec!["adfsdfgs"]),
        ("COS", vec!["sdfgsfsdf"]),
        ("ACOS", vec!["tidb"]),
        ("ASIN", vec!["tidb"]),
        ("ATAN", vec!["tidb"]),
        ("TAN", vec!["sdfgsdfg"]),
    ] {
        for text in invalid_texts {
            let ctx = WarnCountCtx::new();
            let got = math_dispatch_values(name, &[Datum::new_string(text.to_owned())], &ctx)
                .unwrap_or_else(|| panic!("{name} belongs to the math family"));
            assert!(matches!(&got, Ok(_)), "{name}({text:?}) must evaluate");
            // These cases ARE the getWarning branch: success + one warning.
            assert_eq!(ctx.count(), 1, "{name}({text:?}) warns once");
        }

        // Each test's clean text row raises nothing (`"0.000"`, except
        // Degrees' own `""` which coerces silently to 0).
        let clean = if name == "DEGREES" { "" } else { "0.000" };
        let ctx = WarnCountCtx::new();
        let got = math_dispatch_values(name, &[Datum::new_string(clean.to_owned())], &ctx)
            .unwrap_or_else(|| panic!("{name} belongs to the math family"));
        assert!(
            matches!(&got, Ok(Datum::Real(_))),
            "{name}({clean:?}) stays real, got {got:?}"
        );
        assert_eq!(ctx.count(), 0, "numeric text raises no warning");
    }
}

/// Go `pkg/expression/builtin_math_test.go:1036 TestCot`: `COT(0)` divides
/// inside the signature and surfaces
/// `[types:1690]DOUBLE value is out of range in 'cot(0)'`
/// (`builtin_math.go:1774` GenWithStackByArgs("DOUBLE", "cot(...)")). The
/// Rust evaluator reports the same DOUBLE-overflow EVENT
/// (`EvalError::FloatOverflow`); reproducing Go's argument-carrying message
/// text is recorded as a display-shape gap in the receipt. Remaining rows'
/// values live in `tests::math::transcendental_source_vectors`.
#[test]
fn cot_zero_overflows_as_double_error() {
    assert_eq!(e("cot(0)"), "FloatOverflow");
    let ctx = WarnCountCtx::new();
    let got = math_dispatch_values("COT", &[Datum::Real(0.0)], &ctx).unwrap();
    assert!(matches!(got, Err(EvalError::FloatOverflow)));
}

/// The live scalar-function path retains Go's function-specific 1690 text,
/// rather than exposing the shared datum-level FloatOverflow carrier.
#[test]
fn math_overflow_errors_render_the_source_expression() {
    for (name, args, expression) in [
        ("EXP", vec![Datum::Int(100_000)], "exp(100000)"),
        ("POW", vec![Datum::Int(10), Datum::Int(700)], "pow(10, 700)"),
        ("COT", vec![Datum::Real(0.0)], "cot(0)"),
    ] {
        let error = eval_as(name, args, real_ft(), &WarnCountCtx::new())
            .expect_err("math overflow must return an error");
        assert_eq!(
            error,
            EvalError::DataOutOfRange {
                value: "DOUBLE",
                expression: Box::leak(expression.to_owned().into_boxed_str()),
            },
            "{name}"
        );
    }
}

/// Go `pkg/expression/builtin_math_test.go:749 TestPi`: the one row pins the
/// exact double constant rather than a rounded decimal literal.
#[test]
fn pi_is_the_exact_f64_constant() {
    let got = math_dispatch_values("PI", &[], &WarnCountCtx::new())
        .expect("PI dispatches")
        .expect("PI evaluates");
    assert_eq!(got, Datum::Real(std::f64::consts::PI));
}

// ---------------------------------------------------------------------------
// pkg/expression/builtin_math_vec_test.go (items 195–199)
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_math_vec_test.go:155
/// TestVectorizedBuiltinMathEvalOneVec`: the randomized vectorized-vs-row
/// harness iterates `vecBuiltinMathCases`; Rust has ONE row-based tier (the
/// rewritten chunk path), so each listed case-arm keeps a deterministic port
/// through it -- what differs per arm is exactly the child types and result
/// domain that arm names.
///
/// Conv's arm stays commented out in the Go map (issue #5817) and so does
/// its absence here.
#[test]
fn vectorized_builtin_math_eval_one_vec() {
    // ETReal arms: SIGN | LOG | LOG2 | LOG10 | SQRT | ACOS | ASIN | ATAN |
    // ATAN2 | COS | EXP | DEGREES | COT | RADIANS | SIN | TAN.
    for sql in [
        "sign(-3.5e0)",
        "log(2)",
        "log(8, 2)",
        "log10(100)",
        "log2(32)",
        "sqrt(9)",
        "acos(1)",
        "asin(-1)",
        "atan(1)",
        "atan(1, 2)",
        "cos(0)",
        "exp(0)",
        "degrees(pi())",
        "cot(1)",
        "radians(180)",
        "sin(0)",
        "tan(0)",
        // POW and RAND() arms (niladic / seeded children below use them too).
        "pow(2, 3)",
        "rand(42)",
    ] {
        assert_eq!(chunk_e(sql), e(sql), "{sql} must agree across tiers");
    }
    // ABS arms: ETDecimal, ETReal, ETInt, and the unsigned TypeInt24 column.
    assert_eq!(chunk_e("abs(-1.5)"), "DEC:1.5");
    assert_eq!(chunk_e("abs(-1.5e0)"), "FLOAT:1.5");
    assert_eq!(chunk_e("abs(-3)"), "INT:3");
    // TypeInt24 columns carrying the case map's DEFAULT (unsigned) domain.
    assert_eq!(
        unsigned_int_column_value("abs(c0)", Datum::UInt(3)),
        "UINT:3"
    );
    // ROUND arms: the real-child arm needs float spellings (a bare `1.58` is
    // a DECIMAL literal and takes the map's decimal arm instead).
    assert_eq!(chunk_e("round(1.58e0)"), "FLOAT:2");
    assert_eq!(chunk_e("round(1.298e0, 1)"), "FLOAT:1.3");
    assert_eq!(chunk_e("round(3)"), "INT:3");
    assert_eq!(chunk_e("round(2.5)"), "DEC:3");
    // FLOOR/CEIL arms incl. the flagged-column child types: the ETReal arm
    // answers in its own result family; integer columns (TypeInt24 signed,
    // TypeLonglong unsigned) and a DECIMAL(flen 32, decimal 2) child take
    // the ETInt / ETDecimal arms.
    assert_eq!(chunk_e("floor(-1.5e0)"), "FLOAT:-2");
    assert_eq!(chunk_e("ceil(-1.5e0)"), "FLOAT:-1");
    assert_eq!(chunk_e("floor(-2.55)"), "INT:-3");
    assert_eq!(
        unsigned_int_column_value("floor(c0)", Datum::UInt(3)),
        "UINT:3"
    );
    let decimal_col = {
        let mut ft = FieldType::new(C::NewDecimal);
        ft.set_flen(32);
        ft.set_decimal(2);
        ft
    };
    assert_eq!(
        chunk_row_value(
            "floor(c0)",
            &[(
                "c0",
                decimal_col,
                Datum::Decimal(crate::Decimal::from_literal("-2.55"))
            )],
        ),
        "DEC:-3"
    );
    assert_eq!(chunk_e("truncate(-1.1e0, 0)"), "FLOAT:-1");
    assert_eq!(chunk_e("truncate(-1.1e0, -1000)"), "FLOAT:0");
    // CRC32 arm: ETString child, ETInt result.
    assert_eq!(chunk_e("crc32('mysql')"), "UINT:2501908538");
}

/// Evaluates `sql` over one MediumInt/LongLong UNSIGNED column holding
/// `value` -- the childFieldTypes arms of the shared math/op case maps.
fn unsigned_int_column_value(sql: &str, value: Datum) -> String {
    chunk_row_value(
        sql,
        &[(
            "c0",
            FieldType::new(C::Int24).with_added_flags(FieldTypeFlags::UNSIGNED),
            value,
        )],
    )
}

/// Go `pkg/expression/builtin_math_vec_test.go:159
/// TestVectorizedBuiltinMathFunc`: the generator boundary ranges named by the
/// case map (`newRangeRealGener(-1, 1, 0.2)` for EXP,
/// `newRangeInt64Gener(-100, 100)` ROUND scales,
/// `(0,10)x(0,100)` POW pair, `newRangeInt64Gener(-10, 10)` TRUNCATE shifts
/// plus the issue-57651 select-real numerators) stay reachable with their
/// endpoints through the same tier.
#[test]
fn vectorized_builtin_math_func() {
    // EXP endpoints within [-1, 1].
    assert_eq!(chunk_e("exp(-1)"), e("exp(-1)"));
    assert_eq!(chunk_e("exp(1)"), e("exp(1)"));
    // POW range endpoints -- plus the overflow ERROR the open upper end can
    // produce (covered as a genuine error by `trig_functions`).
    assert_eq!(chunk_e("pow(0, 0)"), "FLOAT:1");
    assert_eq!(chunk_e("pow(10, 50)"), e("pow(10, 50)"));
    assert_eq!(e("pow(10, 400)"), "FloatOverflow");
    // ROUND scale bounds -100..100 behave like any far scale.
    assert_eq!(chunk_e("round(5, -100)"), "INT:0");
    assert_eq!(chunk_e("round(5, 100)"), "INT:5");
    // TRUNCATE shift bounds and the issue-57651 numerators
    // `{0, -0.1, 0.1, -1.1, 1.1}` x shifts {-1000..1000}.
    for numerator in ["0e0", "-0.1e0", "0.1e0", "-1.1e0", "1.1e0"] {
        for shift in ["0", "-1000", "1000"] {
            let sql = format!("truncate({numerator}, {shift})");
            assert_eq!(chunk_e(&sql), e(&sql), "{sql}");
        }
    }
    // SIGN over REAL keeps signed semantics at zero.
    assert_eq!(chunk_e("sign(-0.4e0)"), "INT:-1");
    assert_eq!(chunk_e("sign(0.4e0)"), "INT:1");
}

/// Go `pkg/expression/builtin_math_vec_test.go:163
/// TestVectorizedBuiltinMathFuncForRand` over `vecBuiltinMathCases1`
/// (`ast.Rand` with NO children): seeded `RAND(seed)` rows answer the
/// generator's own sequence value inside `[0, 1)`, deterministically per
/// seed regardless of how often they run.
#[test]
fn vectorized_builtin_math_func_for_rand() {
    struct Seeded;
    impl Columns for Seeded {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn rand_seeded_next(&self, _key: usize, seed: i64) -> Option<f64> {
            Some(MysqlRng::new_with_seed(seed).gen())
        }
    }
    let first = eval_as("rand", vec![Datum::Int(20_160_101)], real_ft(), &Seeded).unwrap();
    let second = eval_as("rand", vec![Datum::Int(20_160_101)], real_ft(), &Seeded).unwrap();
    assert_eq!(first, second);
    let Datum::Real(sample) = first else {
        panic!("RAND(seed) must be real, got {first:?}")
    };
    assert!((0.0..1.0).contains(&sample));
}

/// Go `pkg/expression/builtin_math_vec_test.go:167
/// BenchmarkVectorizedBuiltinMathEvalOneVec`.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_vectorized_builtin_math_eval_one_vec() {}

/// Go `pkg/expression/builtin_math_vec_test.go:171
/// BenchmarkVectorizedBuiltinMathFunc`.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_vectorized_builtin_math_func() {}

// ---------------------------------------------------------------------------
// pkg/expression/builtin_miscellaneous_test.go (items 200–217)
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_miscellaneous_test.go:144 TestUUID`: every
/// generator builtin emits canonical five-group hex spelling whose group
/// lengths are 8/4/4/4/12 and whose version nibble matches 1/4/7.
///
/// The remainder of this file's tests (`TestInetAton` .. `TestTidbShard`,
/// sans generation) were pinned exhaustively against the same source table by
/// earlier landing on this branch -- see `compare2.rs`'s
/// `inet_aton_go_vectors`, `inet_ntoa_go_vectors`, `inet6_aton_go_vectors`,
/// `inet6_ntoa_go_vectors`, `is_ip_go_vectors`,
/// `is_ipv4_binary_predicate_go_vectors` and `misc.rs`'s
/// `any_value_returns_its_argument`,
/// `test_any_value_hybrid_string_eval_with_int_sig`,
/// `name_const_preserves_representable_value_domains`,
/// `uuid_version_matches_go_uuid_parse_and_version_nibble`,
/// `is_uuid_matches_go_parse_acceptance`,
/// `uuid_timestamp_matches_go_versioned_timestamp_semantics`,
/// `uuid_binary_builtins_match_go_swap_and_raw_byte_vectors`,
/// `tidb_shard_matches_vitess_des_and_etint_coercion`. They stay the cited
/// ports; the receipt maps each Go item to them.
#[test]
fn uuid_generation_v1_v4_v7_shapes() {
    for (name, version_digit) in [("UUID", '1'), ("UUID_V4", '4'), ("UUID_V7", '7')] {
        let first = misc_dispatch_in(name, &[], &crate::context::NoColumns)
            .unwrap_or_else(|| panic!("{name} belongs to the misc family"))
            .unwrap_or_else(|err| panic!("{name}: {err:?}"));
        let text = uuid_spelling_of(&first, name);
        check_shape_and_version(name, &text, version_digit);
        // Generators accept ZERO constants (Go:
        // `funcs[tf.funcName].getFunction(ctx, datumsToConstants(nil))`).
        assert!(misc_dispatch_in(name, &[], &crate::context::NoColumns).is_some());
    }
}

fn uuid_spelling_of(datum: &Datum, name: &str) -> String {
    let Datum::String(text) = datum else {
        panic!("{name} must be a string, got {datum:?}")
    };
    text.as_utf8().expect("canonical UUID spelling").to_owned()
}

fn check_shape_and_version(name: &str, text: &str, version_digit: char) {
    let parts: Vec<&str> = text.split('-').collect();
    assert_eq!(parts.len(), 5, "{name}: {text:?}");
    assert_eq!(
        [
            parts[0].len(),
            parts[1].len(),
            parts[2].len(),
            parts[3].len(),
            parts[4].len()
        ],
        [8, 4, 4, 4, 12],
        "{name}: {text:?}"
    );
    assert!(
        text.chars().all(|ch| ch.is_ascii_hexdigit() || ch == '-'),
        "{name}: {text:?}"
    );
    assert_eq!(
        char::from(parts[2].as_bytes()[0]),
        version_digit,
        "{name}: version nibble of {text:?}"
    );
}

// ---------------------------------------------------------------------------
// pkg/expression/builtin_miscellaneous_vec_test.go (items 218–222)
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_miscellaneous_vec_test.go:124
/// TestVectorizedBuiltinMiscellaneousEvalOneVec`: the case-map arms and their
/// generator BOUNDARY strings evaluated through the same value dispatchers
/// the row tier uses. Byte-valued generators travel as binary-collation
/// strings, the way a VARBINARY cell would.
///
/// Sleep's own arms depend on statement errctx levels, so they carry their
/// dedicated tests below instead.
#[test]
fn vectorized_builtin_miscellaneous_eval_one_vec() {
    // Inet6Aton / IsIPv6 / IsIPv4 / InetNtoa / InetAton arms.
    assert_eq!(
        call("INET6_ATON", &[bin_str(b"127.0.0.1")]),
        Datum::new_bytes([127, 0, 0, 1])
    );
    assert_eq!(
        call("INET6_ATON", &[s("fdfe::5a55:caff:fefa:9089")]),
        Datum::new_bytes([
            0xfd, 0xfe, 0, 0, 0, 0, 0, 0, 0x5a, 0x55, 0xca, 0xff, 0xfe, 0xfa, 0x90, 0x89,
        ])
    );
    assert_eq!(call("IS_IPV6", &[s("2001:db8::68")]), Datum::Int(1));
    assert_eq!(call("IS_IPV6", &[s("192.168.0.1")]), Datum::Int(0));
    assert_eq!(call("IS_IPV4", &[s("11.11.11.11")]), Datum::Int(1));
    assert_eq!(call("INET_NTOA", &[Datum::Int(167773449)]), s("10.0.5.9"));
    assert_eq!(call("INET_NTOA", &[Datum::Int(-1)]), Datum::Null);
    assert_eq!(
        call("INET_ATON", &[s("255.255.255.255")]),
        Datum::UInt(4294967295)
    );
    // InetAton's second gener -- the explicit SELECT-string list. Left-
    // extension makes 1/2/3-byte prefixes valid.
    let wanted: [(&str, Datum); 5] = [
        (
            "11.11.11.11",
            Datum::UInt(u64::from(u32::from_be_bytes([11, 11, 11, 11]))),
        ),
        ("255.255.255.255", Datum::UInt(4294967295)),
        ("127", Datum::UInt(127)),
        ("127.255", Datum::UInt(2130706687)),
        ("127.2.1", Datum::UInt(2130837505)),
    ];
    for (text, want) in wanted {
        assert_eq!(call("INET_ATON", &[s(text)]), want, "{text}");
    }
    // Dot-led spellings ride along inside the gener list but carry no
    // asserted output there either way (the Go table's malformed rows are
    // pinned by compare2::inet_aton_go_vectors).
    for text in [".122", ".123.123"] {
        // Only reachability is exercised; the arm has no output pin.
        let _ = compare2_dispatch("INET_ATON", &[s(text)], &crate::context::NoColumns)
            .expect("InetAton owns INET_ATON");
    }
    // IsIPv4Mapped / IsIPv4Compat byte-generator shapes: a mapped address's
    // ten zero prefix + ffff marker + v4 tail reads 1; plain text reads 0.
    let mut mapped = vec![0_u8; 16];
    mapped[10] = 0xff;
    mapped[11] = 0xff;
    mapped[12..16].copy_from_slice(&[1, 2, 3, 4]);
    assert_eq!(call("IS_IPV4_MAPPED", &[bin_str(&mapped)]), Datum::Int(1));
    assert_eq!(call("IS_IPV4_MAPPED", &[s("plain text")]), Datum::Int(0));

    // AnyValue arms preserve each family the map lists (ETDuration, ETInt,
    // ETDecimal, ETTimestamp-domain, ETReal, ETString, ETJson).
    let duration = Datum::Duration(MySqlDuration::new(12, 34, 56, 0, 0).unwrap());
    let json = Datum::Json(tidb_datatype::BinaryJSON::parse(r#"{"a":1}"#).unwrap());
    for value in [
        duration,
        Datum::Int(-7),
        Datum::Decimal(crate::Decimal::from_literal("-0.5")),
        Datum::Real(2.5),
        s("x"),
        json.clone(),
    ] {
        assert_eq!(
            misc_dispatch_in(
                "ANY_VALUE",
                std::slice::from_ref(&value),
                &crate::context::NoColumns
            )
            .expect("ANY_VALUE arity")
            .unwrap(),
            value
        );
    }
    // NameConst arms echo the value argument unchanged per ET-family pair.
    assert_eq!(
        misc_dispatch_in(
            "NAME_CONST",
            &[s("label"), Datum::Int(5)],
            &crate::context::NoColumns
        )
        .unwrap()
        .unwrap(),
        Datum::Int(5)
    );
    let echoed = misc_dispatch_in(
        "NAME_CONST",
        &[s("label"), json.clone()],
        &crate::context::NoColumns,
    )
    .unwrap()
    .unwrap();
    assert_eq!(echoed, json);
    assert_eq!(
        misc_dispatch_in(
            "NAME_CONST",
            &[s("label"), Datum::Null],
            &crate::context::NoColumns
        )
        .unwrap()
        .unwrap(),
        Datum::Null
    );

    // IsUUID / UUIDVersion / UUIDTimestamp / UUIDToBin arms over fixed
    // well-formed spellings -- the randomized generators emit this shape.
    let v1_text = "5f13f854-d74a-11f0-9b7a-0ae0156bd76b";
    assert_eq!(call("IS_UUID", &[s(v1_text)]), Datum::Int(1));
    assert_eq!(call("UUID_VERSION", &[s(v1_text)]), Datum::Int(1));
    let timestamp = call("UUID_TIMESTAMP", &[s(v1_text)]);
    assert!(matches!(timestamp, Datum::Decimal(_)));
    assert_eq!(
        call("UUID_TO_BIN", &[s(v1_text)]),
        Datum::new_bytes([
            0x5f, 0x13, 0xf8, 0x54, 0xd7, 0x4a, 0x11, 0xf0, 0x9b, 0x7a, 0x0a, 0xe0, 0x15, 0x6b,
            0xd7, 0x6b,
        ])
    );
}

/// Go `pkg/expression/builtin_miscellaneous_vec_test.go:128
/// TestVectorizedBuiltinMiscellaneousFunc`, VALUE-half: the swap permutation
/// round trip and the google/uuid Parse acceptance quirk ({braced},
/// short input errors).
#[test]
fn vectorized_builtin_miscellaneous_func() {
    let canonical = "6ccd780c-baba-1026-9564-5b8c656024db";
    let normal: Vec<u8> = vec![
        0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24,
        0xdb,
    ];
    assert_eq!(
        call("BIN_TO_UUID", &[Datum::new_bytes(normal.clone())]),
        s(canonical)
    );
    assert_eq!(
        call(
            "BIN_TO_UUID",
            &[Datum::new_bytes(normal.clone()), Datum::Int(1)]
        ),
        s("baba1026-780c-6ccd-9564-5b8c656024db")
    );
    // Too-short byte strings refuse to parse rather than truncating.
    assert!(misc_dispatch_in(
        "BIN_TO_UUID",
        &[Datum::new_bytes(normal[..15].to_vec())],
        &crate::context::NoColumns
    )
    .unwrap()
    .is_err());
    // Braced spelling accepted by google/uuid's Parse in BOTH directions.
    assert_eq!(
        call(
            "UUID_TO_BIN",
            &[s("{6ccd780c-baba-1026-9564-5b8c656024db}")]
        ),
        Datum::new_bytes(normal)
    );
}

/// Go `pkg/expression/builtin_miscellaneous_vec_test.go:149
/// TestSleepVectorized`, errctx-level halves. With BadNull/NoDefault at WARN,
/// SLEEP(NULL) and negative seconds each downgrade their error to ONE warning
/// and answer 0 (rows: single {1}, {-1}, {NULL}, then the triple adding two
/// more); at ERROR level the same inputs abort evaluation. Warning counting
/// follows `warnCnt.add(n)` exactly.
#[test]
fn sleep_vectorized_incorrect_argument_levels() {
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
        /// Go errctx maps BadNull/NoDefault to LevelWarn on the mock session:
        /// the "Incorrect arguments to sleep" event downgrades to a warning
        /// instead of failing the statement.
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
            // Deterministic harness: rows report "not killed" instantly; the
            // wall-clock rows belong to the ignored timing sibling.
            false
        }
    }

    let ctx = Levels {
        warn: true,
        warnings: RefCell::new(Vec::new()),
    };
    // Non-warn, non-null inputs complete without warnings; each incorrect
    // input adds exactly one.
    assert_eq!(sleep_row(Datum::Real(1.0), &ctx), Ok(Datum::Int(0)));
    assert_eq!(ctx.warnings.borrow().len(), 0);
    assert_eq!(sleep_row(Datum::Real(-1.0), &ctx), Ok(Datum::Int(0)));
    assert_eq!(ctx.warnings.borrow().len(), 1);
    assert_eq!(sleep_row(Datum::Null, &ctx), Ok(Datum::Int(0)));
    assert_eq!(ctx.warnings.borrow().len(), 2);

    // Strict model: NULL errors outright; after resetting the warning buffer
    // (Go: SetWarnings(nil)) -2.5 errors again identically.
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

    // Positive values route to the sleeper hook and answer 0 rows (strict;
    // the >=0.5s duration proof is the ignored sibling's subject).
    assert_eq!(sleep_row(Datum::Real(0.01), &strict), Ok(Datum::Int(0)));
    assert_eq!(strict.warnings.borrow().len(), 0);
}

/// One SLEEP row through the scalar-function tier, exactly what `vecEvalType`
/// collapses to per virtual row: result value or the failing error.
fn sleep_row(arg: Datum, ctx: &impl Columns) -> Result<Datum, EvalError> {
    ScalarFunction::new(CiString::new("sleep"), int_ft(), vec![const_arg(arg)])
        .eval(ctx, empty_row())
}

/// Go `pkg/expression/builtin_miscellaneous_vec_test.go:149
/// TestSleepVectorized`, TIMING half: strict SLEEP(0.5) must occupy >= 0.5s
/// of wall clock, and SLEEP(2) must return within <= 2s once
/// `SQLKiller.SendKillSignal(QueryInterrupted)` fires a second in. Real-time
/// sleeps slow the gate and no SQLKiller hook exists on the Rust context
/// contract, so the timing claims stay Go-side evidence.
#[test]
#[ignore = "go-parity-gap: SQLKiller interruptibility and wall-clock duration \
            bounds need real execution time absent from the value-tier ctx"]
fn sleep_vectorized_timing_strict_real_duration_and_kill_signal() {}

/// Go `pkg/expression/builtin_miscellaneous_vec_test.go:124/132` benchmarks.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_vectorized_builtin_miscellaneous_eval_one_vec() {}

/// Go `pkg/expression/builtin_miscellaneous_vec_test.go:136
/// BenchmarkVectorizedBuiltinMiscellaneousFunc`.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_vectorized_builtin_miscellaneous_func() {}

// ---------------------------------------------------------------------------
// pkg/expression/builtin_op_test.go (items 223–236)
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_op_test.go:30 TestUnary`: CONSTANT negation
/// promotes out of BIGINT onto Decimal (`builtin_op.go:1106-1124`,
/// `handleIntOverflow`); unsigned `-2^63` IS representable and answers
/// IntMin, and negating the signed minimum promotes positively.
#[test]
fn unary_minus_source_table() {
    for (bits, want_label) in [
        (9_223_372_036_854_775_809_u64, "-9223372036854775809"),
        (9_223_372_036_854_775_810, "-9223372036854775810"),
    ] {
        let got = eval_default("unaryminus", vec![Datum::UInt(bits)], decimal_zero_ft())
            .unwrap_or_else(|err| panic!("unary minus overflow promotes: {err:?}"));
        assert_eq!(got.label(), format!("DEC:{want_label}"), "{bits}");
    }
    let got = eval_default(
        "unaryminus",
        vec![Datum::UInt(9_223_372_036_854_775_808)],
        int_ft(),
    )
    .unwrap();
    assert_eq!(got, Datum::Int(i64::MIN));
    // --9223372036854775808: negating the SIGNED minimum constant.
    let got = eval_default("unaryminus", vec![Datum::Int(i64::MIN)], decimal_zero_ft()).unwrap();
    assert_eq!(got.label(), "DEC:9223372036854775808");

    // Building the one-constant signature succeeds (Go:
    // `funcs[ast.UnaryMinus].getFunction(ctx, []Expression{NewZero()})`).
    assert!(infer_unary_op_type("unaryminus", &const_arg(Datum::Int(0))).is_some());
}

/// Go `pkg/expression/builtin_op_test.go:70 TestUnaryMinusDecimalRetTypeFlen`
/// and `:90 TestUnaryMinusIntRetTypeFlen`: the builder reserves a digit for
/// the sign over CONSTANTS (flen+1 with scale kept; unsigned Int constants
/// grow by one too), while COLUMN operands keep the declared width.
#[test]
fn unary_minus_ret_type_flen_follows_go_sign_reservation() {
    // Decimal constant flen 10 / decimal 2 -> 11 / 2.
    let mut dec_ft = FieldType::new(C::NewDecimal);
    dec_ft.set_flen(10);
    dec_ft.set_decimal(2);
    let arg = Expression::Constant(crate::constant::Constant::new(
        Datum::Decimal(crate::Decimal::from_literal("123.45")),
        dec_ft.clone(),
    ));
    let ret = infer_unary_op_type("unaryminus", &arg).expect("decimal infer");
    assert_eq!(ret.flen(), 11);
    assert_eq!(ret.decimal(), 2);

    // Decimal COLUMN of declared width 10 keeps 10 (scale untouched).
    let col = Expression::Column(crate::column::Column::new(1, dec_ft));
    let ret = infer_unary_op_type("unaryminus", &col).expect("decimal col infer");
    assert_eq!(ret.flen(), 10);
    assert_eq!(ret.decimal(), 2);

    // Signed Int constant flen 11 -> 12.
    let mut int_ft11 = int_ft();
    int_ft11.set_flen(11);
    let arg = Expression::Constant(crate::constant::Constant::new(
        Datum::Int(123),
        int_ft11.clone(),
    ));
    let ret = infer_unary_op_type("unaryminus", &arg).expect("int infer");
    assert_eq!(ret.flen(), 12);

    // Signed Int COLUMN flen 11 keeps 11.
    let col = Expression::Column(crate::column::Column::new(2, int_ft11));
    let ret = infer_unary_op_type("unaryminus", &col).expect("signed col infer");
    assert_eq!(ret.flen(), 11);

    // Unsigned Int CONSTANT flen 10 -> 11.
    let mut u_ft10 = uint_ft();
    u_ft10.set_flen(10);
    let arg = Expression::Constant(crate::constant::Constant::new(Datum::UInt(123), u_ft10));
    let ret = infer_unary_op_type("unaryminus", &arg).expect("unsigned infer");
    assert_eq!(ret.flen(), 11);
}

/// Go `pkg/expression/builtin_op_test.go:112 TestLogicAnd`: the full
/// truthiness table. FALSE dominates NULL (`{0,nil}->0`), `NULL AND truthy`
/// stays NULL, strings take MySQL's numeric-prefix reading, Decimals read by
/// value. The `errors.New("must error")` rows need an operand whose
/// EVALUATION fails; the Rust operand model cannot construct such a leaf from
/// test code, so those rows are recorded as a skipped half in the receipt
/// instead of being approximated.
#[test]
fn logic_and_source_table() {
    for (args, expected) in [
        (vec![i_(1), i_(1)], Datum::Int(1)),
        (vec![i_(1), i_(0)], Datum::Int(0)),
        (vec![i_(0), i_(1)], Datum::Int(0)),
        (vec![i_(0), i_(0)], Datum::Int(0)),
        (vec![i_(2), i_(-1)], Datum::Int(1)),
        (vec![s_("a"), s_("0")], Datum::Int(0)),
        (vec![s_("a"), s_("1")], Datum::Int(0)),
        (vec![s_("1a"), s_("0")], Datum::Int(0)),
        (vec![s_("1a"), s_("1")], Datum::Int(1)),
        (vec![i_(0), Datum::Null], Datum::Int(0)),
        (vec![Datum::Null, i_(0)], Datum::Int(0)),
        (vec![Datum::Null, i_(1)], Datum::Null),
        (vec![r_(0.001), i_(0)], Datum::Int(0)),
        (vec![r_(0.001), i_(1)], Datum::Int(1)),
        (vec![Datum::Null, r_(0.0)], Datum::Int(0)),
        (vec![Datum::Null, r_(0.001)], Datum::Null),
        (vec![dec_("0.000001"), i_(0)], Datum::Int(0)),
        (vec![dec_("0.000001"), i_(1)], Datum::Int(1)),
        (vec![dec_("0.000000"), Datum::Null], Datum::Int(0)),
        (vec![dec_("0.000001"), Datum::Null], Datum::Null),
    ] {
        let got = eval_default("and", args.clone(), int_ft())
            .unwrap_or_else(|err| panic!("and{args:?}: {err:?}"));
        assert_eq!(got, expected, "and {args:?}");
    }
    // Wrong parameter count refuses the signature outright -- Go's
    // newFunctionForTest validates via VerifyArgsWrapper, whose port is
    // `builtin_registry::verify_args_by_count`.
    assert!(matches!(
        verify_args_by_count("and", 1),
        Err(EvalError::WrongParameterCount(_))
    ));
    assert!(verify_args_by_count("and", 2).is_ok());
    // Two operands BUILD fine (funcs[...] getFunction z,z succeeds).
    assert!(eval_default("and", vec![i_(0), i_(1)], int_ft()).is_ok());
}

/// Go `pkg/expression/builtin_op_test.go:331 TestLogicOr`: TRUE dominates
/// NULL; `0.0001a` reads its prefix, `0.0a` doesn't.
#[test]
fn logic_or_source_table() {
    for (args, expected) in [
        (vec![i_(1), i_(1)], Datum::Int(1)),
        (vec![i_(1), i_(0)], Datum::Int(1)),
        (vec![i_(0), i_(1)], Datum::Int(1)),
        (vec![i_(0), i_(0)], Datum::Int(0)),
        (vec![i_(2), i_(-1)], Datum::Int(1)),
        (vec![s_("a"), s_("0")], Datum::Int(0)),
        (vec![s_("a"), s_("1")], Datum::Int(1)),
        (vec![s_("1a"), s_("0")], Datum::Int(1)),
        (vec![s_("1a"), s_("1")], Datum::Int(1)),
        (vec![s_("0.0a"), i_(0)], Datum::Int(0)),
        (vec![s_("0.0001a"), i_(0)], Datum::Int(1)),
        (vec![i_(1), Datum::Null], Datum::Int(1)),
        (vec![Datum::Null, i_(1)], Datum::Int(1)),
        (vec![Datum::Null, i_(0)], Datum::Null),
        (vec![r_(0.0), i_(0)], Datum::Int(0)),
        (vec![r_(0.001), i_(0)], Datum::Int(1)),
        (vec![Datum::Null, r_(0.0)], Datum::Null),
        (vec![Datum::Null, r_(0.001)], Datum::Int(1)),
        (vec![dec_("0.000000"), i_(0)], Datum::Int(0)),
        (vec![dec_("0.000000"), i_(1)], Datum::Int(1)),
        (vec![dec_("0.000000"), Datum::Null], Datum::Null),
        (vec![dec_("0.000001"), i_(0)], Datum::Int(1)),
        (vec![dec_("0.000001"), i_(1)], Datum::Int(1)),
        (vec![dec_("0.000001"), Datum::Null], Datum::Int(1)),
    ] {
        let got = eval_default("or", args.clone(), int_ft())
            .unwrap_or_else(|err| panic!("or{args:?}: {err:?}"));
        assert_eq!(got, expected, "or {args:?}");
    }
    assert!(matches!(
        verify_args_by_count("or", 1),
        Err(EvalError::WrongParameterCount(_))
    ));
}

/// Go `pkg/expression/builtin_op_test.go:644 TestLogicXor`: parity of
/// truthiness, NULL when either side is unknown; fractional doubles decide by
/// VALUE, so 0.5000/0.4999 and 0.5000/1.0 and 0.4999/1.0 all land even.
#[test]
fn logic_xor_source_table() {
    for (args, expected) in [
        (vec![i_(1), i_(1)], Datum::Int(0)),
        (vec![i_(1), i_(0)], Datum::Int(1)),
        (vec![i_(0), i_(1)], Datum::Int(1)),
        (vec![i_(0), i_(0)], Datum::Int(0)),
        (vec![i_(2), i_(-1)], Datum::Int(0)),
        (vec![s_("a"), s_("0")], Datum::Int(0)),
        (vec![s_("a"), s_("1")], Datum::Int(1)),
        (vec![s_("1a"), s_("0")], Datum::Int(1)),
        (vec![s_("1a"), s_("1")], Datum::Int(0)),
        (vec![i_(0), Datum::Null], Datum::Null),
        (vec![Datum::Null, i_(0)], Datum::Null),
        (vec![Datum::Null, i_(1)], Datum::Null),
        (vec![r_(0.5), r_(0.4999)], Datum::Int(0)),
        (vec![r_(0.5), r_(1.0)], Datum::Int(0)),
        (vec![r_(0.4999), r_(1.0)], Datum::Int(0)),
        (vec![Datum::Null, r_(0.0)], Datum::Null),
        (vec![Datum::Null, r_(0.001)], Datum::Null),
        (vec![dec_("0.000001"), r_(0.00001)], Datum::Int(0)),
        (vec![dec_("0.000001"), i_(1)], Datum::Int(0)),
        (vec![dec_("0.000000"), Datum::Null], Datum::Null),
        (vec![dec_("0.000001"), Datum::Null], Datum::Null),
    ] {
        let got = eval_default("xor", args.clone(), int_ft())
            .unwrap_or_else(|err| panic!("xor{args:?}: {err:?}"));
        assert_eq!(got, expected, "xor {args:?}");
    }
    assert!(matches!(
        verify_args_by_count("xor", 1),
        Err(EvalError::WrongParameterCount(_))
    ));
}

/// Go `pkg/expression/builtin_op_test.go:285 TestBitOr` and `:398 TestBitAnd`
/// -- the source rows missing from `go_test_bit_ops`: the 123/321 pairs, the
/// wrapped two's-complement negative rows, NULL propagation, and the
/// parameter-count refusals. Presentation follows the row tier's UINT
/// domain for the BIT families (`go_test_bit_ops`'s own convention).
#[test]
fn bit_or_bit_and_complete_tables() {
    assert_eq!(
        eval_default("bitor", vec![i_(123), i_(321)], uint_result()).unwrap(),
        Datum::UInt(379)
    );
    assert_eq!(
        eval_default("bitor", vec![i_(-123), i_(321)], uint_result()).unwrap(),
        Datum::UInt((-123_i64) as u64 | 321_u64) // Go: 18446744073709551557
    );
    assert_eq!(
        eval_default("bitor", vec![Datum::Null, i_(1)], uint_result()).unwrap(),
        Datum::Null
    );
    assert_eq!(
        eval_default("bitand", vec![i_(123), i_(321)], uint_result()).unwrap(),
        Datum::UInt(65)
    );
    assert_eq!(
        eval_default("bitand", vec![i_(-123), i_(321)], uint_result()).unwrap(),
        Datum::UInt(((-123_i64) as u64) & 321_u64) // Go GetInt64: 257
    );
    assert_eq!(
        eval_default("bitand", vec![Datum::Null, i_(1)], uint_result()).unwrap(),
        Datum::Null
    );
    assert!(matches!(
        verify_args_by_count("bitor", 1),
        Err(EvalError::WrongParameterCount(_))
    ));
    assert!(matches!(
        verify_args_by_count("bitand", 1),
        Err(EvalError::WrongParameterCount(_))
    ));
}

/// Go `pkg/expression/builtin_op_test.go:437 TestBitNeg` -- the source rows
/// (123 -> 18446744073709551492, -123 -> 122, NULL propagation, refusal of
/// two operands) beyond `go_test_bit_neg`'s 0/7 spin.
#[test]
fn bit_neg_source_rows() {
    assert_eq!(
        eval_default("bitneg", vec![i_(123)], uint_result()).unwrap(),
        Datum::UInt(18446744073709551492)
    );
    assert_eq!(
        eval_default("bitneg", vec![i_(-123)], uint_result()).unwrap(),
        Datum::UInt(122)
    );
    assert_eq!(
        eval_default("bitneg", vec![Datum::Null], uint_result()).unwrap(),
        Datum::Null
    );
    assert!(matches!(
        verify_args_by_count("bitneg", 2),
        Err(EvalError::WrongParameterCount(_))
    ));
}

/// Go `pkg/expression/builtin_op_test.go:483 TestUnaryNot` -- string, float,
/// float-string, decimal and JSON truthiness rows beyond `go_test_unary_not`'s
/// numeric subset.
#[test]
fn unary_not_every_input_domain() {
    for (arg, expected) in [
        (s_("123"), Datum::Int(0)),
        (s_("0"), Datum::Int(1)),
        (r_(0.3), Datum::Int(0)),
        (s_("0.3"), Datum::Int(0)),
        (dec_("0.3"), Datum::Int(0)),
        (
            Datum::Json(tidb_datatype::BinaryJSON::parse("0").unwrap()),
            Datum::Int(1),
        ),
        (
            Datum::Json(tidb_datatype::BinaryJSON::parse(r#"{"test":"test"}"#).unwrap()),
            Datum::Int(0),
        ),
        (Datum::Null, Datum::Null),
    ] {
        assert_eq!(
            eval_default("not", vec![arg.clone()], int_result()).unwrap(),
            expected,
            "not {arg:?}"
        );
    }
    assert!(matches!(
        verify_args_by_count("not", 2),
        Err(EvalError::WrongParameterCount(_))
    ));
}

/// Go `pkg/expression/builtin_op_test.go:537 TestIsTrueOrFalse` -- the FULL
/// signature table, adding Duration-microsecond rows and the zero-value
/// DATETIME/TIMESTAMP rows to `go_test_is_true_or_false`'s subset.
#[test]
fn is_true_or_false_full_signature_table() {
    // Go NewDuration(0,0,0,1000,3): one MICROSECOND (Go's argument is
    // nanoseconds; this tier's constructor takes microseconds), fsp 3.
    let micro = MySqlDuration::new(0, 0, 0, 1, 3).unwrap();
    let zero_dur = MySqlDuration::new(0, 0, 0, 0, 3).unwrap();
    // Go NewTime(FromDate(0,...)+1000us under TypeDatetime fsp 3) is built
    // through CoreTime's exact bit layout, which `Time::new` accepts without
    // calendar validation -- identical construction here.
    let zero_date_micro = Time::new(
        tidb_datatype::CoreTime::from_date(0, 0, 0, 0, 0, 0, 1_000),
        tidb_datatype::TimeType::DateTime,
        3,
    )
    .expect("go's all-zero datetime carries microseconds");
    let core_zero_time = Time::new(
        tidb_datatype::CoreTime::from_raw(0),
        tidb_datatype::TimeType::Timestamp,
        3,
    )
    .expect("go's zero core timestamp");
    let rows: Vec<(Datum, i64, i64)> = vec![
        (i_(-12), 1, 0),
        (i_(12), 1, 0),
        (i_(0), 0, 1),
        (r_(0.0), 0, 1),
        (s_("aaa"), 0, 1),
        (s_(""), 0, 1),
        (s_("0.3"), 1, 0),
        (r_(0.3), 1, 0),
        (dec_("0.3"), 1, 0),
        (Datum::Null, 0, 0),
        (Datum::Duration(micro), 1, 0),
        (Datum::Duration(zero_dur), 0, 1),
        (Datum::Time(zero_date_micro), 1, 0),
        (Datum::Time(core_zero_time), 0, 1),
    ];
    for (arg, is_true, is_false) in &rows {
        assert_eq!(
            eval_default("istrue", vec![arg.clone()], int_result()).unwrap(),
            Datum::Int(*is_true),
            "istrue {arg:?}"
        );
        assert_eq!(
            eval_default("isfalse", vec![arg.clone()], int_result()).unwrap(),
            Datum::Int(*is_false),
            "isfalse {arg:?}"
        );
    }
}

/// Go `pkg/expression/builtin_op_test.go:175/207 TestLeftShift`/
/// `TestRightShift` parameter-count refusals completing `go_test_shifts`.
#[test]
fn shift_parameter_count_boundaries() {
    assert!(matches!(
        verify_args_by_count("leftshift", 1),
        Err(EvalError::WrongParameterCount(_))
    ));
    assert!(matches!(
        verify_args_by_count("rightshift", 1),
        Err(EvalError::WrongParameterCount(_))
    ));
    assert!(verify_args_by_count("leftshift", 2).is_ok());
}

/// Go `pkg/expression/builtin_op_test.go:246 TestBitXor` parameter-count row.
#[test]
fn bit_xor_parameter_count_boundaries() {
    assert!(matches!(
        verify_args_by_count("bitxor", 1),
        Err(EvalError::WrongParameterCount(_))
    ));
    assert!(verify_args_by_count("bitxor", 2).is_ok());
}

// ---------------------------------------------------------------------------
// pkg/expression/builtin_op_vec_test.go (items 237–239)
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_op_vec_test.go:166 TestBuiltinUnaryMinusIntSig`
/// over TYPED COLUMNS: signedness flags choose the wrap rule, MinInt64 (and
/// the unsigned 2^63+1) OVERFLOW because a column keeps the Int signature
/// (`c.handleIntOverflow` never sees a Constant), and NULL propagates
/// untouched.
#[test]
fn builtin_unary_minus_int_sig_columns() {
    let signed_col = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
    let unsigned_col = FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
        .with_added_flags(FieldTypeFlags::UNSIGNED);
    // Signed 233333 -> -233333.
    assert_eq!(
        chunk_row_value("-c0", &[("c0", signed_col.clone(), Datum::Int(233333))]),
        "INT:-233333"
    );
    // Signed MinInt64 stays a genuine overflow ERROR on the column path.
    assert!(
        chunk_row_value("-c0", &[("c0", signed_col.clone(), Datum::Int(i64::MIN))])
            .contains("IntOverflow")
    );
    assert_eq!(
        chunk_row_value("-c0", &[("c0", signed_col.clone(), Datum::Null)]),
        "NULL"
    );
    // Unsigned reinterpretation wraps into the negative Int half.
    assert_eq!(
        chunk_row_value("-c0", &[("c0", unsigned_col.clone(), Datum::UInt(233333))]),
        "INT:-233333"
    );
    // Unsigned 2^63+1 overflows (Go AppendUint64(-(math.MinInt64)+1)).
    assert!(chunk_row_value(
        "-c0",
        &[(
            "c0",
            unsigned_col.clone(),
            Datum::UInt(9_223_372_036_854_775_809)
        )]
    )
    .contains("IntOverflow"));
    assert_eq!(
        chunk_row_value("-c0", &[("c0", unsigned_col, Datum::Null)]),
        "NULL"
    );
}

/// Go `pkg/expression/builtin_op_vec_test.go:158
/// TestVectorizedBuiltinOpFunc` over `vecBuiltinOpCases`: each arm's listed
/// child-type pair stays answered identically by the row-based tier. The AES
/// "modes" fields in Go's case structs are runner metadata and carry no
/// arithmetic meaning.
#[test]
fn vectorized_builtin_op_func() {
    // IsTruthWithoutNull / IsFalsity over REAL, DECIMAL, INT children --
    // evaluated signature-directly (the chunk tier exposes IS TRUE as an
    // AST postfix form, not a callable builtin).
    assert_eq!(
        eval_default("istrue", vec![r_(-0.5)], int_result()).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        eval_default("istrue", vec![r_(0.0)], int_result()).unwrap(),
        Datum::Int(0)
    );
    assert_eq!(
        eval_default("istrue", vec![dec_("0.00")], int_result()).unwrap(),
        Datum::Int(0)
    );
    assert_eq!(
        eval_default("istrue", vec![i_(2)], int_result()).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        eval_default("isfalse", vec![r_(0.0)], int_result()).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        eval_default("isfalse", vec![r_(1.5)], int_result()).unwrap(),
        Datum::Int(0)
    );
    // LogicOr arms: (int,int), (decimal,real), (int,duration).
    assert_eq!(chunk_e("0 or null"), "NULL");
    assert_eq!(chunk_e("1.50 or 0e0"), "INT:1");
    assert_eq!(
        chunk_row_value(
            "c0 or c1",
            &[
                ("c0", int_ft(), Datum::Int(0)),
                (
                    "c1",
                    FieldType::new(C::Duration),
                    Datum::Duration(MySqlDuration::new(0, 0, 0, 0, 0).unwrap()),
                ),
            ],
        ),
        "INT:0"
    );
    assert_eq!(
        chunk_row_value(
            "c0 or c1",
            &[
                ("c0", int_ft(), Datum::Int(1)),
                (
                    "c1",
                    FieldType::new(C::Duration),
                    Datum::Duration(MySqlDuration::new(0, 0, 1, 0, 0).unwrap()),
                ),
            ],
        ),
        "INT:1"
    );
    // LogicXor / Xor int-int pairs.
    assert_eq!(chunk_e("1 xor 0"), "INT:1");
    assert_eq!(chunk_e("2 xor 3"), "INT:0");
    // LogicAnd arms incl. (decimal,real) and (int,duration).
    assert_eq!(chunk_e("null and 1"), "NULL");
    assert_eq!(chunk_e("0.000 and 0e0"), "INT:0");
    assert_eq!(chunk_e("1 and 1"), "INT:1");
    // Or / And BIT families and BitNeg remain UINT-typed.
    assert_eq!(chunk_e("1 | 2"), "UINT:3");
    assert_eq!(chunk_e("1 & 3"), "UINT:1");
    assert_eq!(chunk_e("~0"), "UINT:18446744073709551615");
    // UnaryNot over each numeric family (signature-directly).
    assert_eq!(
        eval_default("not", vec![r_(0.0)], int_result()).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        eval_default("not", vec![dec_("0.0")], int_result()).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        eval_default("not", vec![i_(5)], int_result()).unwrap(),
        Datum::Int(0)
    );
    // RightShift / LeftShift int-int.
    assert_eq!(chunk_e("1 >> 1"), "UINT:0");
    assert_eq!(chunk_e("1 << 2"), "UINT:4");
    // UnaryMinus over real, decimal, int, and an UNSIGNED INT column inside
    // `newRangeInt64Gener(0, MaxInt64)`.
    assert_eq!(chunk_e("-1.5e0"), "FLOAT:-1.5");
    assert_eq!(chunk_e("-1.50"), "DEC:-1.50");
    assert_eq!(chunk_e("-2"), "INT:-2");
    assert_eq!(
        chunk_row_value(
            "-c0",
            &[(
                "c0",
                FieldType::new(C::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED),
                Datum::UInt(7)
            )],
        ),
        "INT:-7"
    );
    // IsNull arms: REAL, INT, DECIMAL, DURATION, DATETIME columns, NULL-held
    // and value-held alike.
    for (code, value) in [
        (C::Double, Datum::Null),
        (C::LongLong, Datum::Null),
        (C::NewDecimal, Datum::Null),
        (C::Duration, Datum::Null),
        (C::Datetime, Datum::Null),
        (C::Double, Datum::Real(0.0)),
    ] {
        let out = chunk_row_value("isnull(c0)", &[("c0", FieldType::new(code), value)]);
        assert!(out.starts_with("INT:"), "{out}");
    }
}

/// Go `pkg/expression/builtin_op_vec_test.go:162` benchmark half.
#[test]
#[ignore = "skipped-reason: Go testing.B microbenchmark, excluded by the gate"]
fn benchmark_vectorized_builtin_op_func() {}

// ---------------------------------------------------------------------------
// pkg/expression/builtin_other_test.go (item 240)
// ---------------------------------------------------------------------------

/// Go `pkg/expression/builtin_other_test.go:32 TestBitCount`: the complete
/// popcount table. Floats round to the nearest integer first (`-1.1` -> `-1`
/// -> 64 set bits; `-3.1` -> `-3` -> 63), garbage strings coerce to 0,
/// `uint64(math.MaxUint64)` carries all bits, and NULL propagates.
#[test]
fn bit_count_source_table() {
    for (sql, want) in [
        ("bit_count(8)", Datum::Int(1)),
        ("bit_count(29)", Datum::Int(4)),
        ("bit_count(0)", Datum::Int(0)),
        ("bit_count(-1)", Datum::Int(64)),
        ("bit_count(-11)", Datum::Int(62)),
        ("bit_count(-1000)", Datum::Int(56)),
        ("bit_count(1.1)", Datum::Int(1)),
        ("bit_count(3.1)", Datum::Int(2)),
        ("bit_count(-1.1)", Datum::Int(64)),
        ("bit_count(-3.1)", Datum::Int(63)),
        ("bit_count('xxx')", Datum::Int(0)),
        ("bit_count(null)", Datum::Null),
    ] {
        assert_eq!(e(sql), want.label(), "{sql}");
    }
    // uint64(math.MaxUint64) -- spelled past the signed constant domain.
    assert_eq!(e("bit_count(18446744073709551615)"), Datum::Int(64).label());
}

// ---------------------------------------------------------------------------
// Local table atoms (kept last so readers meet tables before helpers)
// ---------------------------------------------------------------------------

fn i_(v: i64) -> Datum {
    Datum::Int(v)
}

fn r_(v: f64) -> Datum {
    Datum::Real(v)
}

fn s_(text: &str) -> Datum {
    Datum::new_string(text.to_owned())
}

fn dec_(text: &str) -> Datum {
    Datum::Decimal(crate::Decimal::from_literal(text))
}

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

//! Go's `Function.PbCode()` for every builtin over every combination of up to
//! three argument types, against the signature the function class set.
//!
//! `testdata/pb_code_matrix.txt` is Go's own answer: each line is a builtin's
//! name and, per arity, `*N=Sig` when every combination of N argument types
//! builds to one signature, else `Sig=shape,shape,...`. A shape is one letter
//! per argument: `i` bigint, `u` bigint unsigned, `r` double, `d`
//! decimal(20,4), `s` varchar utf8mb4_bin, `b` varbinary, `t` datetime, `D`
//! date, `T` timestamp, `h` time, `j` json, `v` vector. A combination Go's
//! `NewFunction` refuses is absent.
//!
//! `testdata/pb_code_vector_matrix.txt` is the same for the shapes that hold
//! a vector. `testdata/pb_code_special.txt` holds the calls no such shape
//! reaches -- no argument, four arguments, a constant argument -- as Go
//! `NewFunctionBase` builds them: `name(arg,...)`, a tab and the signature,
//! `-` where Go refuses the call. An argument is a shape letter, a quoted
//! string constant or `NULL`.
//!
//! `testdata/pb_converter_refused.txt` is Go `PbConverter.ExprToPB` over the
//! first matrix: per builtin, the shapes that have a signature code and that
//! the converter still refuses (`-` is the call with no argument), or `*`
//! when it refuses every shape with a code.

use std::collections::BTreeMap;

use tidb_charset::Collation;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

use crate::column::Column;
use crate::expression::Expression;

const MATRIX: &str = include_str!("../../testdata/pb_code_matrix.txt");
const VECTOR_MATRIX: &str = include_str!("../../testdata/pb_code_vector_matrix.txt");
const SPECIAL: &str = include_str!("../../testdata/pb_code_special.txt");
const CONVERTER_REFUSED: &str = include_str!("../../testdata/pb_converter_refused.txt");

const ONE_AND_TWO: &str = "iurdsbtDThj";
const THREE: &str = "irdsbth";

struct MockContext;

impl tidb_exprctx::Columns for MockContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((1_700_000_000, 0, 0))
    }
}

fn field_type(label: char) -> FieldType {
    match label {
        'i' => FieldType::new(FieldTypeCode::LongLong),
        'u' => {
            let mut tp = FieldType::new(FieldTypeCode::LongLong);
            tp.add_flags(FieldTypeFlags::UNSIGNED);
            tp
        }
        'r' => FieldType::new(FieldTypeCode::Double),
        'd' => {
            let mut tp = FieldType::new(FieldTypeCode::NewDecimal);
            tp.set_flen(20);
            tp.set_decimal(4);
            tp
        }
        's' => FieldType::new(FieldTypeCode::VarString),
        'b' => {
            let mut tp = FieldType::new(FieldTypeCode::VarString);
            tp.set_collation(Collation::Binary);
            tp.add_flags(FieldTypeFlags::BINARY);
            tp
        }
        't' => FieldType::new(FieldTypeCode::Datetime),
        'D' => FieldType::new(FieldTypeCode::Date),
        'T' => FieldType::new(FieldTypeCode::Timestamp),
        'h' => FieldType::new(FieldTypeCode::Duration),
        'j' => FieldType::new(FieldTypeCode::Json),
        'v' => FieldType::new(FieldTypeCode::VectorFloat32),
        label => panic!("shape letter {label}"),
    }
}

fn shapes(arity: usize) -> Vec<String> {
    let alphabet = if arity == 3 { THREE } else { ONE_AND_TWO };
    let mut shapes = vec![String::new()];
    for _ in 0..arity {
        shapes = shapes
            .iter()
            .flat_map(|prefix| alphabet.chars().map(move |label| format!("{prefix}{label}")))
            .collect();
    }
    shapes
}

/// Go's signature for each `(name, shape)` it builds.
fn go_matrix(matrix: &str) -> Vec<(String, String, String)> {
    let mut rows = Vec::new();
    for line in matrix.lines() {
        let (name, cells) = line.split_once('\t').expect("name, tab, cells");
        for cell in cells.split(' ') {
            let (left, right) = cell.split_once('=').expect("cell");
            if let Some(arity) = left.strip_prefix('*') {
                for shape in shapes(arity.parse().expect("arity")) {
                    rows.push((name.to_owned(), shape, right.to_owned()));
                }
            } else {
                for shape in right.split(',') {
                    rows.push((name.to_owned(), shape.to_owned(), left.to_owned()));
                }
            }
        }
    }
    rows
}

/// Go `NewFunction(mock.NewContext(), name, types.NewFieldType(mysql.TypeUnspecified), columns...)`
/// then `Function.PbCode().String()`.
fn rust_pb_code(name: &str, shape: &str) -> Result<&'static str, String> {
    let args = shape.chars().enumerate().map(|(index, label)| column(index, label)).collect();
    let function =
        crate::new_function::new_function(&MockContext, name, FieldType::new(FieldTypeCode::Unspecified), args)
            .map_err(|error| format!("{error:?}"))?;
    Ok(pb_code(&function).as_str_name())
}

/// Go `function.(*ScalarFunction).Function.PbCode()`.
fn pb_code(function: &Expression) -> tidb_proto::tipb::ScalarFuncSig {
    match function {
        Expression::ScalarFunction(function) => function.pb_signature().unwrap_or_default(),
        _ => tidb_proto::tipb::ScalarFuncSig::Unspecified,
    }
}

/// Each signature the first matrix builds, with the evaluation types of the
/// arguments its function holds.
pub(crate) fn signature_arg_types() -> Vec<(tidb_proto::tipb::ScalarFuncSig, Vec<tidb_datatype::EvalType>)> {
    let mut seen = std::collections::BTreeSet::new();
    let mut rows = Vec::new();
    for (name, shape, _) in go_matrix(MATRIX) {
        let args = shape.chars().enumerate().map(|(index, label)| column(index, label)).collect();
        let Ok(Expression::ScalarFunction(function)) = crate::new_function::new_function(
            &MockContext,
            &name,
            FieldType::new(FieldTypeCode::Unspecified),
            args,
        ) else {
            continue;
        };
        let Some(signature) = function.pb_signature() else {
            continue;
        };
        if !seen.insert(signature) {
            continue;
        }
        let Some(domains) = function
            .args
            .iter()
            .map(|arg| arg.static_type().map(FieldType::eval_type))
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        rows.push((signature, domains));
    }
    rows
}

fn column(index: usize, label: char) -> Expression {
    let mut column = Column::default();
    column.ret_type = Some(field_type(label));
    column.id = index as i64 + 1;
    column.unique_id = index as i64 + 1;
    column.index = index as i64;
    Expression::Column(column)
}

#[test]
fn pb_code_matches_go_for_every_builtin_and_argument_types() {
    assert_matrix(MATRIX);
}

#[test]
fn pb_code_matches_go_over_a_vector_argument() {
    assert_matrix(VECTOR_MATRIX);
}

/// Go `NewFunctionBase(mock.NewContext(), name, types.NewFieldType(mysql.TypeUnspecified), args...)`
/// then `Function.PbCode().String()`, for the calls of `pb_code_special.txt`.
#[test]
fn pb_code_matches_go_for_the_calls_no_column_shape_reaches() {
    let mut wrong = Vec::new();
    for line in SPECIAL.lines() {
        let (call, want) = line.split_once('\t').expect("call, tab, signature");
        let (name, args) = call.strip_suffix(')').and_then(|call| call.split_once('(')).expect("name(args)");
        let args = args
            .split(',')
            .filter(|arg| !arg.is_empty())
            .enumerate()
            .map(|(index, arg)| match arg.strip_prefix('\'').and_then(|arg| arg.strip_suffix('\'')) {
                Some(text) => Expression::Constant(crate::constant::Constant::new(
                    Datum::String(tidb_datatype::StringDatum::new(text, Collation::Utf8Mb4Bin)),
                    FieldType::new(FieldTypeCode::VarString),
                )),
                None if arg == "NULL" => Expression::Constant(crate::constant::Constant::new(
                    Datum::Null,
                    FieldType::new(FieldTypeCode::Null),
                )),
                None => column(index, arg.chars().next().expect("shape letter")),
            })
            .collect();
        let name = name.to_owned();
        let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            crate::new_function::new_function_base(
                &MockContext,
                &name,
                FieldType::new(FieldTypeCode::Unspecified),
                args,
            )
            .map_or("-", |function| pb_code(&function).as_str_name())
        }))
        .unwrap_or("panic");
        if got != want {
            wrong.push(format!("{call}: Go {want}, got {got}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// Go `BuildCastFunction` over a vector column: the two casts whose signature
/// Go records (`castAsStringFunctionClass`, `castAsVectorFloat32FunctionClass`).
#[test]
fn a_cast_of_a_vector_has_gos_signature() {
    for (target, want) in [
        (FieldTypeCode::VarString, "CastVectorFloat32AsString"),
        (FieldTypeCode::VectorFloat32, "CastVectorFloat32AsVectorFloat32"),
        (FieldTypeCode::LongLong, "Unspecified"),
        (FieldTypeCode::Double, "Unspecified"),
        (FieldTypeCode::Json, "Unspecified"),
        (FieldTypeCode::Datetime, "Unspecified"),
    ] {
        let cast = crate::builtin_cast::build_cast_function_with_check(&crate::builtin::UnfoldedBuildContext, column(0, 'v'), FieldType::new(target), false, false)
            .expect("the cast builds");
        assert_eq!(pb_code(&cast).as_str_name(), want, "{target:?}");
    }
    for source in "irdsbthj".chars() {
        let cast = crate::builtin_cast::build_cast_function_with_check(
            &crate::builtin::UnfoldedBuildContext,
            column(0, source),
            FieldType::new(FieldTypeCode::VectorFloat32),
            false,
            false,
        )
        .expect("the cast builds");
        assert_eq!(pb_code(&cast).as_str_name(), "Unspecified", "{source}");
    }
}

/// Go `scalarFuncToPBExpr`: a function whose signature no store lists
/// (`canFuncBePushed(.., kv.UnSpecified)`) has no TiPB form, though it has a
/// signature code.
#[test]
fn the_converter_refuses_the_signatures_no_store_lists() {
    let mut refused_by_go: BTreeMap<&str, Option<Vec<&str>>> = BTreeMap::new();
    for line in CONVERTER_REFUSED.lines() {
        let (name, shapes) = line.split_once('\t').expect("name, tab, shapes");
        let shapes = (shapes != "*")
            .then(|| shapes.split(',').map(|shape| if shape == "-" { "" } else { shape }).collect());
        refused_by_go.insert(name, shapes);
    }
    let mut wrong: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut checked = 0;
    for (name, shape, signature) in go_matrix(MATRIX) {
        if signature == "Unspecified" {
            continue;
        }
        let want_refused = match refused_by_go.get(name.as_str()) {
            None => false,
            Some(None) => true,
            Some(Some(shapes)) => shapes.contains(&shape.as_str()),
        };
        let args = shape.chars().enumerate().map(|(index, label)| column(index, label)).collect();
        let function = crate::new_function::new_function(
            &MockContext,
            &name,
            FieldType::new(FieldTypeCode::Unspecified),
            args,
        )
        .expect("the signature matrix builds it");
        checked += 1;
        let converted = crate::expr_to_pb::PbConverter::new(
            &MockContext,
            false,
            &tidb_datatype::SessionTimeZone::utc(),
        )
        .expr_to_pb(&function);
        if converted.is_none() != want_refused {
            wrong
                .entry(name)
                .or_default()
                .push(format!("{}:{signature}", if shape.is_empty() { "-" } else { &shape }));
        }
    }
    for (name, rows) in &wrong {
        let go = if refused_by_go.contains_key(name.as_str()) { "refuses" } else { "converts" };
        eprintln!("WRONG {name} Go {go} {} {}", rows.len(), rows.iter().take(6).cloned().collect::<Vec<_>>().join(","));
    }
    assert!(checked > 10_000, "{checked} calls checked");
    assert!(wrong.is_empty(), "{} builtins differ from Go", wrong.len());
}

fn assert_matrix(matrix: &str) {
    let mut refused: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut wrong: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
    for (name, shape, want) in go_matrix(matrix) {
        let got = std::panic::catch_unwind(|| rust_pb_code(&name, &shape))
            .unwrap_or_else(|_| Err("panic".to_owned()));
        match got {
            Ok(got) if got == want => {}
            Ok(got) => wrong.entry(name).or_default().entry(format!("{want}!={got}")).or_default().push(shape),
            Err(error) => refused.entry(name).or_default().push(format!("{shape}:{want} ({error})")),
        }
    }
    for (name, rows) in &wrong {
        for (pair, shapes) in rows {
            eprintln!("WRONG {name} {pair} {} {}", shapes.len(), shapes.join(","));
        }
    }
    for (name, rows) in &refused {
        eprintln!("REFUSED {name} {} {}", rows.len(), rows.iter().take(2).cloned().collect::<Vec<_>>().join(" "));
    }
    assert!(wrong.is_empty() && refused.is_empty(), "{} wrong, {} refused", wrong.len(), refused.len());
}

/// Go `randFunctionClass.getFunction` tests the seed as it was given, before
/// its cast to an integer: a constant seeds the generator once (`Rand`), any
/// other seed reseeds it per row (`RandWithSeedFirstGen`). `NewFunctionBase`
/// answers, per seed: int constant, string constant, double constant and NULL
/// `Rand`; a varchar column `RandWithSeedFirstGen`.
#[test]
fn rand_reads_its_seed_before_the_cast() {
    let constant = |value: Datum, code: FieldTypeCode| {
        Expression::Constant(crate::constant::Constant::new(value, FieldType::new(code)))
    };
    let mut column = Column::default();
    column.ret_type = Some(field_type('s'));
    column.id = 1;
    column.unique_id = 1;
    let seeds = [
        (constant(Datum::Int(3), FieldTypeCode::LongLong), "Rand"),
        (
            constant(
                Datum::String(tidb_datatype::StringDatum::new("3", Collation::Utf8Mb4Bin)),
                FieldTypeCode::VarString,
            ),
            "Rand",
        ),
        (constant(Datum::Real(3.5), FieldTypeCode::Double), "Rand"),
        (constant(Datum::Null, FieldTypeCode::Null), "Rand"),
        (Expression::Column(column), "RandWithSeedFirstGen"),
    ];
    for (seed, want) in seeds {
        let function = crate::new_function::new_function_base(
            &MockContext,
            "rand",
            FieldType::new(FieldTypeCode::Unspecified),
            vec![seed.clone()],
        )
        .expect("rand builds");
        assert_eq!(pb_code(&function).as_str_name(), want, "{seed:?}");
    }
}

/// Go `calculateDecimal4RoundAndTruncate` takes the scale from a second
/// argument that folded to a constant, a negative one counting as zero; any
/// other second argument (Go's `rand` is unfoldable) keeps the first
/// argument's scale. Go `floorFunctionClass.getFunction` gives a decimal
/// result the cast argument's length.
#[test]
fn round_truncate_and_floor_size_their_decimal_result_as_go_does() {
    let decimal_column = |flen: i64, decimal: i64| {
        let mut tp = FieldType::new(FieldTypeCode::NewDecimal);
        tp.set_flen(flen);
        tp.set_decimal(decimal);
        let mut column = Column::default();
        column.ret_type = Some(tp);
        column.id = 1;
        column.unique_id = 1;
        Expression::Column(column)
    };
    let build = |name: &str, args: Vec<Expression>| {
        crate::new_function::new_function_base(&MockContext, name, FieldType::new(FieldTypeCode::Unspecified), args)
            .unwrap_or_else(|error| panic!("{name} builds: {error:?}"))
    };
    let tp = |function: &Expression| function.static_type().expect("typed").clone();
    let int = |value: i64| {
        Expression::Constant(crate::constant::Constant::new(
            Datum::Int(value),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    };
    for name in ["round", "truncate"] {
        let negative = build(name, vec![decimal_column(10, 4), int(-1)]);
        assert_eq!(tp(&negative).decimal(), 0, "{name} negative scale");
        let rand = build("rand", Vec::new());
        let unfoldable = build(name, vec![decimal_column(10, 4), rand]);
        assert_eq!(tp(&unfoldable).decimal(), 4, "{name} unfoldable scale");
    }
    let floor = build("floor", vec![decimal_column(40, 2)]);
    assert_eq!((tp(&floor).flen(), tp(&floor).decimal()), (40, 0));
}

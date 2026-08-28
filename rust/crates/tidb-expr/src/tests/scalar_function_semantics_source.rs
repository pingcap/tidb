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

//! GO PORTS from `pkg/expression/scalar_function_test.go` (batch part11
//! items 617-623), read from `origin/master`.
//!
//! Every ported assertion re-derives its expectation from the Go production
//! sources the tests exercise: `pkg/expression/scalar_function.go` (the
//! `ScalarFunction`, its cached `hashcode`, and `ReHashCode`
//! (`scalar_function.go:281`)), `pkg/expression/expression.go:1168`
//! (`NewValuesFunc`) and `pkg/expression/constant.go:37` (`NewOne`).
//!
//! Construction note (same convention as `expr_util::tests`): Go's
//! `newFunctionWithMockCtx` runs `NewFunctionInternal`, whose compare-class
//! path wraps both operands of a comparison into casts over the common
//! supertype inside `getFunction` (`builtin_compare.go`,
//! `newBaseBuiltinFuncWithTp` -> `WrapWithCastAsReal`). For `lt(double-col,
//! one)` the right constant is FOLDED to a plain `Constant{Float64(1)}` with
//! `TypeDouble` before it lands in `GetArgs()`. The Rust tree is therefore
//! built with that post-inference shape directly; the inference step itself
//! is outside what this crate models.

use crate::column::Column;
use crate::constant::Constant;
use crate::expression::{ConstLevel, Expression};
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

fn longlong_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn double_type() -> FieldType {
    FieldType::new(FieldTypeCode::Double)
}

/// Go `NewOne()` folded through the double comparison's cast: a plain real 1.
fn real_const(value: f64) -> Expression {
    Expression::Constant(Constant::new(Datum::Real(value), double_type()))
}

/// Go `newFunctionWithMockCtx(ast.LT, a, NewOne())` / `(ast.LT, a, NewZero())`
/// after type inference: `<` over column#1 (`TypeDouble`) and a typed real
/// literal. Note the ret type Go's helper uses is `TypeLonglong`
/// (`constant_test.go:62`).
fn lt_column_vs_real_const(value: f64) -> ScalarFunction {
    ScalarFunction::new(
        CiString::new("lt"),
        longlong_type(),
        vec![
            Expression::Column(Column::new(1, double_type())),
            real_const(value),
        ],
    )
}

/// GO PORT of `scalar_function_test.go:95` `TestScalarFunction` (first half).
///
/// Pins `IsCorrelated()` == false, `ConstLevel()` == `ConstNone`, and — byte
/// for byte — Go's own `HashCode` literal (`scalar_function_test.go:105`):
///
/// - `03` scalar-function flag,
/// - `04 'l' 't'` compact-length-prefixed lowercased name,
/// - `01 | 80 00*6 01` column flag plus `EncodeInt(UniqueID=1)`
///   sign-flipped big-endian,
/// - `00 | 05 | bf f0 00*6` constant flag, float flag, and the mem-comparable
///   float64 bits of +1.0 (`math.Float64bits(1.0) ^ 1<<63`).
#[test]
fn test_scalar_function() {
    let mut sf = lt_column_vs_real_const(1.0);
    assert!(!sf.is_correlated());
    assert_eq!(sf.const_level(), ConstLevel::NONE);

    // The exact bytes Go's test pins.
    const GO_HASH_CODE: [u8; 23] = [
        0x3, 0x4, b'l', b't', 0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x5, 0xbf, 0xf0,
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
    ];
    assert_eq!(sf.hash_code(), &GO_HASH_CODE[..]);

    // `sf.Decorrelate(nil).Equal(ctx, sf)`'s clone-is-equal half, expressed at
    // this crate's structural equality: an independent clone stays equal.
    let cloned = sf.clone();
    assert!(Expression::ScalarFunction(cloned.clone()).equal(&Expression::ScalarFunction(sf)));
    assert_eq!(cloned.func_name.lowercase(), "lt");
    assert!(cloned.get_static_type().unwrap().equal(&longlong_type()));
}

/// go-parity-gap: the second half of `TestScalarFunction`
/// (`scalar_function_test.go:107-117`) drives
/// `NewValuesFunc(ctx, 0, TypeLonglong)` (`expression.go:1168`) and asserts
/// the CONCRETE signature identity `newSf.Function.(*builtinValuesIntSig)` in
/// addition to name/ret-type/coercibility/repertoire round-trips. This crate
/// removed the per-signature object model (see [`crate::scalar_function`]'s
/// BRIDGE DECISION): dispatch is name-keyed, so no runtime value can prove a
/// `values` node carries the int signature, and collation `Repertoire()` has
/// no carrier.
#[test]
#[ignore = "go-parity-gap: NewValuesFunc concrete builtinValuesIntSig identity and Repertoire() need the per-signature object model this crate replaced"]
fn new_values_func_sig_identity() {}

/// GO PORT of `scalar_function_test.go:120`
/// `TestScalarFunctionEqualAfterCleanHashCode`: two structurally different
/// functions stay unequal AFTER hash codes were computed once and then
/// cleared — clearing must never make `Equal` accept stale/differing caches.
///
/// In Go the hazard is real because `ScalarFunction.Equal`
/// (`scalar_function.go:377`) consults cached hash codes when present; the
/// Rust `Expression::equal` (`expression.rs:554`) is purely structural, so
/// the invariant holds by construction. The recomputation-stability check at
/// the end is NEW COVERAGE documenting that `clean_hash_code` followed by a
/// second `hash_code` call rebuilds identical bytes.
#[test]
fn test_scalar_function_equal_after_clean_hash_code() {
    let mut zero = lt_column_vs_real_const(0.0);
    let mut one = lt_column_vs_real_const(1.0);

    assert!(
        !Expression::ScalarFunction(zero.clone()).equal(&Expression::ScalarFunction(one.clone()))
    );

    // Compute the codes (both caches are now filled), then clear them.
    let zero_cached = zero.hash_code().to_vec();
    let one_cached = one.hash_code().to_vec();
    assert_ne!(zero_cached, one_cached);
    zero.clean_hash_code();
    one.clean_hash_code();

    assert!(
        !Expression::ScalarFunction(zero.clone()).equal(&Expression::ScalarFunction(one.clone()))
    );
    // Recomputation after cleaning reproduces the pre-clean bytes.
    assert_eq!(zero.hash_code(), zero_cached.as_slice());
    assert_eq!(one.hash_code(), one_cached.as_slice());
}

/// go-parity-gap: `TestExpressionSemanticEqual`
/// (`scalar_function_test.go:30`, driving
/// `ExpressionsSemanticEqual` at `scalar_function.go:618`) relies on
/// `CanonicalHashCode` / `simpleCanonicalizedHashCode`
/// (`scalar_function.go:622-682`): commutative names sort argument hashes,
/// directed comparisons canonicalize to GE/GT swapped forms, `not(x<y)`
/// normalizes to `x>=y`, and constants hash in their typed domain. This
/// crate defers `CanonicalHashCode` entirely
/// ([`crate::scalar_function`] header), so the whole order-sensitive /
/// order-insensitive case table has nothing to drive.
#[test]
#[ignore = "go-parity-gap: ExpressionsSemanticEqual needs CanonicalHashCode/simpleCanonicalizedHashCode, deferred in scalar_function.rs"]
fn test_expression_semantic_equal() {}

/// go-parity-gap: `TestColumnSubstituteGroupingCleansHashCode`
/// (`scalar_function_test.go:139`) builds a `grouping(col0)` node carrying
/// `BuiltinGroupingImplSig` metadata via `NewFunctionWithInit` +
/// `SetMetadata(tipb.GroupingMode_ModeBitAnd, ...)`, primes its CANONICAL
/// hash code with `CanonicalHashCode()`, substitutes col0->col1 through
/// `ColumnSubstituteImpl`, and requires the substitution to have dropped the
/// stale canonical code. This crate's `ColumnSubstituteImpl`
/// ([`crate::expr_util::substitute`]) cleans the plain hashcode only because
/// the grouping special case is DEFERRED (`scalar_function.rs:340`): a
/// `grouping` ScalarFunction cannot carry
/// [`crate::grouping::GroupingMetadata`] yet, and `CanonicalHashCode` is
/// unported.
#[test]
#[ignore = "go-parity-gap: grouping-metadata-bearing ScalarFunction nodes (SetMetadata path) plus CanonicalHashCode cleaning are deferred in scalar_function.rs"]
fn test_column_substitute_grouping_cleans_hash_code() {}

/// GO PORT of `scalar_function_test.go:179` `TestIssue23309`: a NOT NULL
/// flagged column compared against NULL keeps a NULL-typed operand whose
/// evaluation yields SQL NULL and whose inferred param type carries NO
/// not-null flag.
#[test]
fn test_issue_23309() {
    let mut not_null_double = FieldType::new(FieldTypeCode::Double);
    not_null_double.add_flags(FieldTypeFlags::NOT_NULL);

    let sf = ScalarFunction::new(
        CiString::new("ne"),
        longlong_type(),
        vec![
            Expression::Column(Column::new(1, not_null_double)),
            Expression::Constant(Constant::new(
                Datum::Null,
                FieldType::new(FieldTypeCode::Null),
            )),
        ],
    );

    // v := sf.GetArgs()[1].Eval(...) must be SQL NULL.
    let Expression::Constant(null_operand) = &sf.get_args()[1] else {
        panic!("operand 1 is the NULL constant")
    };
    let evaluated = null_operand.eval().expect("a plain literal evaluates");
    assert!(evaluated.is_null());

    // require.False(t, mysql.HasNotNullFlag(...GetType(...).GetFlag()))
    let operand_type = sf.get_args()[1].static_type().expect("typed operand");
    assert!(!operand_type.has_flag(FieldTypeFlags::NOT_NULL));
}

/// go-parity-gap: `TestScalarFuncs2Exprs` (`scalar_function_test.go:197`)
/// drives `ScalarFuncs2Exprs` (`scalar_function.go:345`), which widens a
/// `[]*ScalarFunction` into an `[]Expression` box-for-box. In this crate a
/// scalar function's arguments already live as `Vec<Expression>`
/// (`scalar_function.rs:228`), so there is no separate slice-of-signatures
/// container for the widening to be exercised against.
#[test]
#[ignore = "go-parity-gap: ScalarFuncs2Exprs widens []*ScalarFunction, a representation this name-keyed port does not have (args are Vec<Expression> directly)"]
fn test_scalar_funcs_2_exprs() {}

/// go-parity-gap: `TestScalarFunctionHash64Equals`
/// (`scalar_function_test.go:213`) drives `ScalarFunction.Hash64(h)` /
/// `Equals(other)` (`scalar_function.go`, the cascades `base.HashEquals`
/// contract): identical trees hash identically; changing the function name,
/// any argument, or the return type changes both. Only the LEAF types carry
/// `hash64`/`equals` here (`column.rs:132`, `constant.rs:162`); the
/// scalar-function level (`h.HashByte(scalarFunctionFlag)`, arg-count prefix,
/// per-arg recursion) is not implemented.
#[test]
#[ignore = "go-parity-gap: ScalarFunction::Hash64/Equals (base.HashEquals contract incl. ret-type field) are carried only for leaves (column.rs/constant.rs)"]
fn test_scalar_function_hash64_equals() {}

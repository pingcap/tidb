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

//! GO PORTS of the `EvaluateExprWithNull` family from
//! `pkg/expression/expression_test.go` (`TestEvaluateExprWithNull`,
//! `TestEvaluateExprWithNullMeetError`, `TestEvaluateExprWithNullAndParameters`,
//! `TestEvaluateExprWithNullNoChangeRetType`).
//!
//! # Which Go machinery each stage maps onto
//!
//! - Construction/rebuild: [`MasterFunctionBuilder`] composes this workspace's
//!   two ported halves of Go's `NewFunction`: the registry/typing layer
//!   ([`crate::new_function::new_function`]) followed by the public
//!   `FoldConstant` port ([`crate::expr_util::fold_constant_with`], carrying
//!   Go's `specialFoldHandler` set -- `Ifnull`/`If`/`Case`/`IsNull`).
//!   Composition is needed because `new_function` itself drives only the
//!   rewriter-tier constant fold, which folds all-constant calls but omits the
//!   special handlers master applies inside every `NewFunction`
//!   (`scalar_function.go:357` -> `FoldConstant`, whose handler dispatch sits at `constant_fold.go:172`).
//! - The null walk itself: [`crate::expr_util::evaluate_expr_with_null`],
//!   `expression.go:947`.
//!
//! Shape assertions stand in for Go's `StringWithCtx` prints: a column renders
//! as `Column#<UniqueID>` (column.go's `string()`), so Go's expected print
//! `ifnull(Column#1, 1)` corresponds to the tree `ifnull(col(UniqueID=1), 1)`.

use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

use crate::column::Column;
use crate::context::NoColumns;
use crate::expression::Expression;
use crate::expr_util::{
    evaluate_expr_with_null, fold_constant_with, FoldOptions, FunctionBuilder,
    FunctionBuildError, PreservingFunctionBuilder, RealFunctionBuilder, SubstituteOptions,
};
use crate::schema::Schema;
use tidb_ast::CiString;

/// A [`FunctionBuilder`] reproducing master's `NewFunction` contract by
/// chaining the two ported halves described in the module header.
///
/// Comparison results are typed through
/// [`crate::builtin_compare::infer_compare_type`] when the caller hands an
/// `Unspecified`/absent return type, standing in for
/// `compareFunctionClass.getFunction`'s own result-type inference.
pub struct MasterFunctionBuilder<'a, C: crate::context::Columns> {
    ctx: &'a C,
    delegate: RealFunctionBuilder<'a, C>,
}

impl<'a, C: crate::context::Columns> MasterFunctionBuilder<'a, C> {
    /// Binds the evaluation context both halves read.
    pub fn new(ctx: &'a C) -> Self {
        Self {
            ctx,
            delegate: RealFunctionBuilder::new(ctx),
        }
    }
}

impl<C: crate::context::Columns> FunctionBuilder for MasterFunctionBuilder<'_, C> {
    fn new_function(
        &self,
        func_name: &str,
        ret_type: Option<FieldType>,
        args: Vec<Expression>,
    ) -> Result<Expression, FunctionBuildError> {
        let mut built = self.delegate.new_function(func_name, ret_type.clone(), args)?;
        // Infer comparison result types exactly where Go's class inference
        // fills them in (`compareFunctionClass.getFunction`); this workspace's
        // `builtin_return_type` leaves comparisons Unspecified today.
        if let Expression::ScalarFunction(function) = &mut built {
            let name = function.func_name.lowercase();
            let needs_inference = function
                .ret_type
                .as_ref()
                .is_some_and(|t| t.code() == FieldTypeCode::Unspecified)
                || ret_type.is_none();
            if needs_inference {
                if let Some(inferred) = crate::builtin_compare::infer_compare_type(name) {
                    function.ret_type = Some(inferred);
                }
            }
        }
        Ok(fold_constant_with(&built, self.ctx, &FoldOptions::new(self)))
    }
}

fn longlong() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn null_longlong() -> Expression {
    Expression::Constant(crate::constant::Constant::new(Datum::Null, longlong()))
}

fn one() -> Expression {
    Expression::Constant(crate::constant::Constant::new(Datum::Int(1), longlong()))
}

/// GO PORT of `pkg/expression/expression_test.go:39 TestEvaluateExprWithNull`.
///
/// Go truncates the schema to `[col0]` first, so only col0 is nulled:
///
/// - Case 1: `ifnull(col0, ifnull(col1, 1))` reduces to Go's printed
///   `ifnull(Column#1, 1)` (UniqueID 1 belongs to col1): rebuilding the outer
///   call with its NULL first argument makes `ifNullFoldHandler`
///   (`constant_fold.go:96`) discard the nulled arm and return the folded
///   second argument, i.e. the UNNULLED inner call survives as-is.
/// - Case 2: with col1 nulled too, both nested calls collapse into
///   Constant(1), matching Go's `res.Equal(ctx, NewOne())`.
#[test]
fn evaluate_expr_with_null_neutralizes_only_schema_columns() {
    fn test_column(unique_id: i64, id: i64) -> Expression {
        let mut column = Column::new(unique_id, longlong());
        // tableInfoToSchemaForTest sets `ID: col.ID`, the ColumnInfo id.
        column.id = id;
        Expression::Column(column)
    }
    let ctx = NoColumns;
    let builder = MasterFunctionBuilder::new(&ctx);
    let options = SubstituteOptions::new(&builder);

    let col0 = test_column(0, 1);
    let col1 = test_column(1, 2);
    let inner = builder
        .new_function("ifnull", None, vec![col1.clone(), one()])
        .expect("construction must succeed");
    let outer = builder
        .new_function("ifnull", None, vec![col0, inner])
        .expect("construction must succeed");

    let truncated = Schema::new(vec![match test_column(0, 1) {
        Expression::Column(column) => column,
        _ => unreachable!(),
    }]);
    let result =
        evaluate_expr_with_null(&outer, &truncated, false, &ctx, &options)
            .expect("the walk must succeed");

    // The surviving node wraps the UNNULLED column and the literal.
    let Expression::ScalarFunction(function) = &result else {
        panic!("expected the surviving ifnull node, got {result:?}")
    };
    assert_eq!(function.func_name.lowercase(), "ifnull");
    let [arg0, arg1] = function.args.as_slice() else {
        panic!("expected two arguments")
    };
    let Expression::Column(survivor) = &arg0 else {
        panic!("the unnulled column must remain, got {arg0:?}")
    };
    assert_eq!(survivor.unique_id, 1);
    match arg1 {
        Expression::Constant(constant) => assert_eq!(constant.value, Datum::Int(1)),
        other => panic!("expected the literal one, got {other:?}"),
    }

    // Restoring col1 into the schema nulls it too, folding everything down to
    // Go's NewOne().
    let full_schema_column = match test_column(1, 2) {
        Expression::Column(column) => column,
        _ => unreachable!(),
    };
    let full = Schema::new(vec![
        match test_column(0, 1) {
            Expression::Column(column) => column,
            _ => unreachable!(),
        },
        full_schema_column,
    ]);
    let folded =
        evaluate_expr_with_null(&result, &full, false, &ctx, &options)
            .expect("the walk must succeed");
    let Expression::Constant(constant) = &folded else {
        panic!("expected a fully folded constant, got {folded:?}")
    };
    assert_eq!(constant.value, Datum::Int(1));
}

/// GO PORT of `pkg/expression/expression_test.go:63
/// TestEvaluateExprWithNullMeetError`.
///
/// Renaming the INNER node to an unregistered name makes the walk's rebuild of
/// that node fail (`newFunctionImpl` rejects unknown names); the error must
/// propagate identically whether the caller walks plainly or under the
/// null-reject-check context (Go evaluates the same function on
/// `ctx.GetNullRejectCheckExprCtx()`).
#[test]
fn evaluate_expr_with_null_meets_rebuild_error() {
    let ctx = NoColumns;
    let builder = MasterFunctionBuilder::new(&ctx);
    let options = SubstituteOptions::new(&builder);

    let mut inner = builder
        .clone_builder()
        .new_function(
            "ifnull",
            None,
            vec![Expression::Column(Column::new(1, longlong())), one()],
        )
        .expect("construction must succeed");
    if let Expression::ScalarFunction(function) = &mut inner {
        // `innerFunc.(*ScalarFunction).FuncName.L = "invalid"`.
        function.func_name = CiString::new("invalid");
    }
    let outer = builder
        .new_function(
            "ifnull",
            None,
            vec![Expression::Column(Column::new(0, longlong())), inner],
        )
        .expect("construction must succeed");

    let schema = Schema::new(vec![Column::new(0, longlong())]);
    // Plain walk: the rebuild error surfaces.
    assert!(evaluate_expr_with_null(&outer, &schema, false, &ctx, &options).is_err());
    // Null-reject-check walk: same failure.
    assert!(evaluate_expr_with_null(&outer, &schema, true, &ctx, &options).is_err());
}

impl<C: crate::context::Columns> MasterFunctionBuilder<'_, C> {
    /// Reuses the typing delegate without going through the composed fold --
    /// the MeetError port only needs a well-typed ifnull wrapper.
    fn clone_builder(&self) -> RealFunctionBuilder<'_, C> {
        RealFunctionBuilder::new(self.ctx)
    }
}

/// GO PORT of `pkg/expression/expression_test.go:85
/// TestEvaluateExprWithNullAndParameters`, non-parameter half.
///
/// `lt(col0, 1)` nulled over `{col0}` folds its all-constant arguments after
/// the walk replaced the column: comparison-with-NULL stays SQL three-valued,
/// so the result is the NULL constant Go matches with
/// `res.Equal(ctx, NewNull())`, carrying the comparison's own Boolean-shaped
/// result type.
///
/// # Narrowing versus Go
///
/// Go's second half attaches a plan-cache `ParamMarker` to the compared value
/// with session parameters appended (`PlanCacheParams.Append`), requires the
/// rebuilt node to come back as the DEFERRED constant `lt(NULL, ?)`, and
/// requires `SetSkipPlanCache` to have flipped `UseCache`. This port cannot
/// reach any of that yet:
/// - a parameter-markered constant refuses evaluation
///   (`Constant::eval` reports deferred/parameter constants unsupported), so
///   the fold of `lt(NULL, ?10)` keeps the unfolded scalar-function node
///   instead of Go's deferred constant, and
/// - the plan-cache marking side effect is performed by callers here; see
///   `expr_util::predicates::maybe_over_optimized_4_plan_cache` for the
///   predicate Go consults.
#[test]
fn evaluate_expr_with_null_folds_a_column_against_a_literal() {
    let ctx = NoColumns;
    let builder = MasterFunctionBuilder::new(&ctx);
    let options = SubstituteOptions::new(&builder);

    let col0 = Expression::Column(Column::new(0, longlong()));
    let lt = builder
        .new_function("lt", None, vec![col0, one()])
        .expect("construction must succeed");

    let schema = Schema::new(vec![Column::new(0, longlong())]);
    let result = evaluate_expr_with_null(&lt, &schema, false, &ctx, &options)
        .expect("the walk must succeed");
    let Expression::Constant(constant) = &result else {
        panic!("expected lt(NULL, 1) to fold, got {result:?}")
    };
    assert_eq!(constant.value, Datum::Null);
    // The comparison's declared result type rides along (flattened by the
    // fold into the constant, exactly Go's `retType := x.RetType.Clone()`).
    let ret_type = constant.ret_type.as_ref().expect("a typed constant");
    assert_eq!(ret_type.flen(), 1);
    assert_ne!(
        ret_type.flags() & FieldTypeFlags::IS_BOOLEAN,
        0,
        "comparisons stay IS_BOOLEAN-flagged"
    );
}

/// go-parity-gap: the parameter-marker half of
/// `pkg/expression/expression_test.go:85 TestEvaluateExprWithNullAndParameters`
/// and all of `:111 TestEvaluateExprWithNullNoChangeRetType` are unportable
/// until parameter-marker constants can evaluate during constant folding
/// (`Constant::eval` refuses them today) and until this crate's
/// `new_function("cast")` routes casts through BuildCastFunction again --
/// master's `newFunctionImpl` has `case ast.Cast: return BuildCastFunction(...)` (`scalar_function.go:212`),
/// which is what lets the null walk REBUILD a cast under the comparison and
/// re-clear `ParseToJSONFlag`; both halves pin behavior the Rust fold chain
/// cannot produce yet.
#[test]
#[ignore = "go-parity-gap: parameter-marker constants refuse evaluation in the fold path, and new_function(\"cast\") refuses cast nodes, so neither Go's deferred Constant(lt(NULL, ?)) outcome nor the ParseToJSONFlag strip-and-persist walk can be reproduced"]
fn evaluate_expr_with_null_parameter_marker_and_json_flag_halves() {}

/// Guards the composition above against silent drift: with the PRESERVING
/// builder alone the same inputs keep the rebuilt ifnull node, proving the
/// fold came from the folded-builder choice rather than from the walk itself
/// (Go gets it from `NewFunctionInternal`'s `foldConstant`).
#[test]
fn the_walk_requires_the_folding_builder_to_reduce_rebuilt_nodes() {
    let ctx = NoColumns;
    let preserving = PreservingFunctionBuilder;
    let col1 = Expression::Column(Column::new(1, longlong()));
    let inner = PreservingFunctionBuilder
        .new_function("ifnull", Some(longlong()), vec![col1, one()])
        .expect("must build");
    let outer = PreservingFunctionBuilder
        .new_function(
            "ifnull",
            Some(longlong()),
            vec![null_longlong(), inner],
        )
        .expect("must build");
    let schema = Schema::new(vec![Column::new(0, longlong())]);
    let options = SubstituteOptions::new(&preserving);
    let result = evaluate_expr_with_null(&outer, &schema, false, &ctx, &options)
        .expect("must succeed");
    let Expression::ScalarFunction(function) = &result else {
        panic!("preserving builder must not fold")
    };
    // The outer argument stays the TypeNull NULL constant produced by the
    // walk: without the special handler nothing collapses.
    assert!(matches!(&function.args[0], Expression::Constant(c) if c.value.is_null()));
}

/// Anchor asserting the flag Go strips is representable: `PARSE_TO_JSON` lives
/// on this crate's field-type flags, matching `mysql.ParseToJSONFlag`. Without
/// this guard a future rename would silently detach the documented JSON-flag
/// gap from reality.
#[test]
fn parse_to_json_flag_is_the_documented_flag_bit() {
    let mut field_type = longlong();
    field_type.add_flags(FieldTypeFlags::PARSE_TO_JSON);
    assert_ne!(field_type.flags() & FieldTypeFlags::PARSE_TO_JSON, 0);
}

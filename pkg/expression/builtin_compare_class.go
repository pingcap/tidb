// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

func (c *compareFunctionClass) getDisplayName() string {
	var nameBuilder strings.Builder
	c.op.Format(&nameBuilder)
	return nameBuilder.String()
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	if lft != nil && rft != nil && (lft.GetType() == mysql.TypeUnspecified || rft.GetType() == mysql.TypeUnspecified) {
		if lft.GetType() == rft.GetType() {
			return types.ETString
		}
		if lft.GetType() == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() && rhs.IsStringKind() {
		return types.ETString
	} else if (lhs == types.ETInt || (lft != nil && lft.Hybrid())) && (rhs == types.ETInt || (rft != nil && rft.Hybrid())) {
		return types.ETInt
	} else if (lhs == types.ETDecimal && rhs == types.ETString) || (lhs == types.ETString && rhs == types.ETDecimal) {
		return types.ETReal
	} else if ((lhs == types.ETInt || (lft != nil && lft.Hybrid())) || lhs == types.ETDecimal) &&
		((rhs == types.ETInt || (rft != nil && rft.Hybrid())) || rhs == types.ETDecimal) {
		return types.ETDecimal
	} else if lft != nil && rft != nil && (types.IsTemporalWithDate(lft.GetType()) && rft.GetType() == mysql.TypeYear ||
		lft.GetType() == mysql.TypeYear && types.IsTemporalWithDate(rft.GetType())) {
		return types.ETDatetime
	}
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(ctx EvalContext, lhs, rhs Expression) types.EvalType {
	lhsFieldType, rhsFieldType := lhs.GetType(ctx), rhs.GetType(ctx)
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	if lhsEvalType == types.ETVectorFloat32 || rhsEvalType == types.ETVectorFloat32 {
		cmpType = types.ETVectorFloat32
	} else if (lhsEvalType.IsStringKind() && lhsFieldType.GetType() == mysql.TypeJSON) || (rhsEvalType.IsStringKind() && rhsFieldType.GetType() == mysql.TypeJSON) {
		cmpType = types.ETJson
	} else if cmpType == types.ETString && (types.IsTypeTime(lhsFieldType.GetType()) || types.IsTypeTime(rhsFieldType.GetType())) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		if lhsFieldType.GetType() == rhsFieldType.GetType() {
			cmpType = lhsFieldType.EvalType()
		} else {
			cmpType = types.ETDatetime
		}
	} else if lhsFieldType.GetType() == mysql.TypeDuration && rhsFieldType.GetType() == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		cmpType = types.ETDuration
	} else if cmpType == types.ETReal || cmpType == types.ETString {
		_, isLHSConst := lhs.(*Constant)
		_, isRHSConst := rhs.(*Constant)
		if (lhsEvalType == types.ETDecimal && !isLHSConst && rhsEvalType.IsStringKind() && isRHSConst) ||
			(rhsEvalType == types.ETDecimal && !isRHSConst && lhsEvalType.IsStringKind() && isLHSConst) {
			/*
				<non-const decimal expression> <cmp> <const string expression>
				or
				<const string expression> <cmp> <non-const decimal expression>

				Do comparison as decimal rather than float, in order not to lose precision.
			)*/
			cmpType = types.ETDecimal
		} else if isTemporalColumn(ctx, lhs) && isRHSConst ||
			isTemporalColumn(ctx, rhs) && isLHSConst {
			/*
				<temporal column> <cmp> <non-temporal constant>
				or
				<non-temporal constant> <cmp> <temporal column>

				Convert the constant to temporal type.
			*/
			col, isLHSColumn := lhs.(*Column)
			if !isLHSColumn {
				col = rhs.(*Column)
			}
			if col.GetType(ctx).GetType() == mysql.TypeDuration {
				cmpType = types.ETDuration
			}
		}
	}
	return cmpType
}

// GetCmpFunction get the compare function according to two arguments.
func GetCmpFunction(ctx BuildContext, lhs, rhs Expression) CompareFunc {
	switch GetAccurateCmpType(ctx.GetEvalCtx(), lhs, rhs) {
	case types.ETInt:
		return CompareInt
	case types.ETReal:
		return CompareReal
	case types.ETDecimal:
		return CompareDecimal
	case types.ETString:
		coll, _ := CheckAndDeriveCollationFromExprs(ctx, "", types.ETInt, lhs, rhs)
		return genCompareString(coll.Collation)
	case types.ETDuration:
		return CompareDuration
	case types.ETDatetime, types.ETTimestamp:
		return CompareTime
	case types.ETJson:
		return CompareJSON
	case types.ETVectorFloat32:
		return CompareVectorFloat32
	default:
		panic(fmt.Sprintf("cannot compare with %s", GetAccurateCmpType(ctx.GetEvalCtx(), lhs, rhs)))
	}
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(ctx EvalContext, expr Expression) bool {
	ft := expr.GetType(ctx)
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	if !types.IsTypeTime(ft.GetType()) && ft.GetType() != mysql.TypeDuration {
		return false
	}
	return true
}

// tryToConvertConstantInt tries to convert a constant with other type to a int constant.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExceptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
//
//	If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
//	If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func tryToConvertConstantInt(ctx BuildContext, targetFieldType *types.FieldType, con *Constant) (_ *Constant, isExceptional bool) {
	if con.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		return con, false
	}

	evalCtx := ctx.GetEvalCtx()
	dt, err := con.Eval(evalCtx, chunk.Row{})
	if err != nil {
		return con, false
	}

	dt, err = dt.ConvertTo(evalCtx.TypeCtx(), targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:        dt,
				RetType:      targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, true
		}
		return con, false
	}
	return &Constant{
		Value:         dt,
		RetType:       targetFieldType,
		DeferredExpr:  con.DeferredExpr,
		ParamMarker:   con.ParamMarker,
		SubqueryRefID: con.SubqueryRefID,
	}, false
}

// RefineComparedConstant changes a non-integer constant argument to its ceiling or floor result by the given op.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExceptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
//
//	If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
//	If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func RefineComparedConstant(ctx BuildContext, targetFieldType types.FieldType, con *Constant, op opcode.Op) (_ *Constant, isExceptional bool) {
	evalCtx := ctx.GetEvalCtx()
	dt, err := con.Eval(evalCtx, chunk.Row{})
	if err != nil {
		return con, false
	}
	if targetFieldType.GetType() == mysql.TypeBit {
		targetFieldType = *types.NewFieldType(mysql.TypeLonglong)
	}
	var intDatum types.Datum
	// Disable AllowNegativeToUnsigned to make sure return 0 when underflow happens.
	oriTypeCtx := evalCtx.TypeCtx()
	newTypeCtx := oriTypeCtx.WithFlags(oriTypeCtx.Flags().WithAllowNegativeToUnsigned(false))
	intDatum, err = dt.ConvertTo(newTypeCtx, &targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:        intDatum,
				RetType:      &targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, true
		}
		return con, false
	}
	c, err := intDatum.Compare(evalCtx.TypeCtx(), &con.Value, collate.GetBinaryCollator())
	if err != nil {
		return con, false
	}
	if c == 0 {
		return &Constant{
			Value:         intDatum,
			RetType:       &targetFieldType,
			DeferredExpr:  con.DeferredExpr,
			ParamMarker:   con.ParamMarker,
			SubqueryRefID: con.SubqueryRefID,
		}, false
	}
	switch op {
	case opcode.LT, opcode.GE:
		resultExpr := NewFunctionInternal(ctx, ast.Ceil, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.LE, opcode.GT:
		resultExpr := NewFunctionInternal(ctx, ast.Floor, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.NullEQ, opcode.EQ:
		switch con.GetType(ctx.GetEvalCtx()).EvalType() {
		// An integer value equal or NULL-safe equal to a float value which contains
		// non-zero decimal digits is definitely false.
		// e.g.,
		//   1. "integer  =  1.1" is definitely false.
		//   2. "integer <=> 1.1" is definitely false.
		case types.ETReal, types.ETDecimal:
			return con, true
		case types.ETString:
			// We try to convert the string constant to double.
			// If the double result equals the int result, we can return the int result;
			// otherwise, the compare function will be false.
			// **note**
			// 1. We compare `doubleDatum` with the `integral part of doubleDatum` rather then intDatum to handle the
			//    case when `targetFieldType.GetType()` is `TypeYear`.
			// 2. When `targetFieldType.GetType()` is `TypeYear`, we can not compare `doubleDatum` with `intDatum` directly,
			//    because we'll convert values in the ranges '0' to '69' and '70' to '99' to YEAR values in the ranges
			//    2000 to 2069 and 1970 to 1999.
			// 3. Suppose the value of `con` is 2, when `targetFieldType.GetType()` is `TypeYear`, the value of `doubleDatum`
			//    will be 2.0 and the value of `intDatum` will be 2002 in this case.
			var doubleDatum types.Datum
			doubleDatum, err = dt.ConvertTo(evalCtx.TypeCtx(), types.NewFieldType(mysql.TypeDouble))
			if err != nil {
				return con, false
			}
			if doubleDatum.GetFloat64() != math.Trunc(doubleDatum.GetFloat64()) {
				return con, true
			}
			return &Constant{
				Value:         intDatum,
				RetType:       &targetFieldType,
				DeferredExpr:  con.DeferredExpr,
				ParamMarker:   con.ParamMarker,
				SubqueryRefID: con.SubqueryRefID,
			}, false
		}
	}
	return con, false
}

func matchRefineRule3Pattern(conEvalType types.EvalType, exprType *types.FieldType) bool {
	return (exprType.GetType() == mysql.TypeTimestamp || exprType.GetType() == mysql.TypeDatetime) &&
		(conEvalType == types.ETReal || conEvalType == types.ETDecimal || conEvalType == types.ETInt)
}

// handleDurationTypeComparisonForNullEq handles comparisons between a duration type column and a non-duration type constant.
// If the constant cannot be cast to a duration type and the comparison operator is `<=>`, the expression is rewritten as `0 <=> 1`.
// This is necessary to maintain compatibility with MySQL behavior under the following conditions:
//  1. When a duration type column is compared with a non-duration type constant, MySQL casts the duration column to the non-duration type.
//     This cast prevents the use of indexes on the duration column. In TiDB, we instead cast the non-duration type constant to the duration type.
//  2. If the non-duration type constant cannot be successfully cast to a duration type, the cast returns null. A duration type constant, however,
//     can always be cast to a non-duration type without returning null.
//  3. If the duration type column's value is null and the non-duration type constant cannot be cast to a duration type, and the comparison operator
//     is `<=>` (null equal), then in TiDB, `durationColumn <=> non-durationTypeConstant` evaluates to `null <=> null`, returning true. In MySQL,
//     it would evaluate to `null <=> not-null constant`, returning false.
//
// To ensure MySQL compatibility, we need to handle this case specifically. If the non-duration type constant cannot be cast to a duration type,
// we rewrite the expression to always return false by converting it to `0 <=> 1`.
func (c *compareFunctionClass) handleDurationTypeComparisonForNullEq(ctx BuildContext, arg0, arg1 Expression) (_ []Expression, err error) {
	// check if a constant value becomes null after being cast to a duration type.
	castToDurationIsNull := func(ctx BuildContext, arg Expression) (bool, error) {
		f := WrapWithCastAsDuration(ctx, arg)
		_, isNull, err := f.EvalDuration(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return false, err
		}
		return isNull, nil
	}

	arg0Const, arg0IsCon := arg0.(*Constant)
	arg1Const, arg1IsCon := arg1.(*Constant)

	var isNull bool
	if arg0IsCon && arg0Const.DeferredExpr == nil && !arg1IsCon && arg1.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeDuration {
		if arg0Const.Value.IsNull() {
			// This is a const null, there is no need to re-write the expression
			return nil, nil
		}
		isNull, err = castToDurationIsNull(ctx, arg0)
	} else if arg1IsCon && arg1Const.DeferredExpr == nil && !arg0IsCon && arg0.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeDuration {
		if arg1Const.Value.IsNull() {
			// This is a const null, there is no need to re-write the expression
			return nil, nil
		}
		isNull, err = castToDurationIsNull(ctx, arg1)
	}
	if err != nil {
		return nil, err
	}
	if isNull {
		return []Expression{NewZero(), NewOne()}, nil
	}
	return nil, nil
}

// Since the argument refining of cmp functions can bring some risks to the plan-cache, the optimizer
// needs to decide to whether to skip the refining or skip plan-cache for safety.
// For example, `unsigned_int_col > ?(-1)` can be refined to `True`, but the validation of this result
// can be broken if the parameter changes to 1 after.
func allowCmpArgsRefining4PlanCache(ctx BuildContext, args []Expression) (allowRefining bool) {
	if !MaybeOverOptimized4PlanCache(ctx, args...) {
		return true // plan-cache disabled or no parameter in these args
	}

	// For these 3 cases below, we apply the refining:
	// 1. year-expr <cmp> const
	// 2. int-expr <cmp> string/float/double/decimal-const
	// 3. datetime/timestamp column <cmp> int/float/double/decimal-const
	for conIdx := range 2 {
		if _, isCon := args[conIdx].(*Constant); !isCon {
			continue // not a constant
		}

		// case 1: year-expr <cmp> const
		// refine `year < 12` to `year < 2012` to guarantee the correctness.
		// see https://github.com/pingcap/tidb/issues/41626 for more details.
		exprType := args[1-conIdx].GetType(ctx.GetEvalCtx())
		exprEvalType := exprType.EvalType()
		if exprType.GetType() == mysql.TypeYear {
			ctx.SetSkipPlanCache(fmt.Sprintf("'%v' may be converted to INT", args[conIdx].StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable)))
			return true
		}

		// case 2: int-expr <cmp> string/float/double/decimal-const
		// refine `int_key < 1.1` to `int_key < 2` to generate RangeScan instead of FullScan.
		conEvalType := args[conIdx].GetType(ctx.GetEvalCtx()).EvalType()
		if exprEvalType == types.ETInt &&
			(conEvalType == types.ETString || conEvalType == types.ETReal || conEvalType == types.ETDecimal) {
			ctx.SetSkipPlanCache(fmt.Sprintf("'%v' may be converted to INT", args[conIdx].StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable)))
			return true
		}

		// case 3: datetime/timestamp column <cmp> int/float/double/decimal-const
		// try refine numeric-const to timestamp const
		// see https://github.com/pingcap/tidb/issues/38361 for more details
		_, exprIsCon := args[1-conIdx].(*Constant)
		if !exprIsCon && matchRefineRule3Pattern(conEvalType, exprType) {
			ctx.SetSkipPlanCache(fmt.Sprintf("'%v' may be converted to datetime", args[conIdx].StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable)))
			return true
		}
	}

	return false
}

// refineArgs will rewrite the arguments if the compare expression is
//  1. `int column <cmp> non-int constant` or `non-int constant <cmp> int column`. E.g., `a < 1.1` will be rewritten to `a < 2`.
//  2. It also handles comparing year type with int constant if the int constant falls into a sensible year representation.
//  3. It also handles comparing datetime/timestamp column with numeric constant, try to cast numeric constant as timestamp type, do nothing if failed.
//  4. Handles special cases where a duration type column is compared with a non-duration type constant, particularly when the constant
//     cannot be cast to a duration type, ensuring compatibility with MySQL’s behavior by rewriting the expression as `0 <=> 1`.
//
// This refining operation depends on the values of these args, but these values can change when using plan-cache.
// So we have to skip this operation or mark the plan as over-optimized when using plan-cache.

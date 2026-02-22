// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func toBool(tc types.Context, tp *types.FieldType, eType types.EvalType, buf *chunk.Column, sel []int, isZero []int8) error {
	switch eType {
	case types.ETInt:
		i64s := buf.Int64s()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if i64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETReal:
		f64s := buf.Float64s()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if f64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDuration:
		d64s := buf.GoDurations()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if d64s[i] == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		t64s := buf.Times()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if t64s[i].IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETString:
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				var fVal float64
				var err error
				sVal := buf.GetString(i)
				if tp.Hybrid() {
					switch tp.GetType() {
					case mysql.TypeSet, mysql.TypeEnum:
						fVal = float64(len(sVal))
						if fVal == 0 {
							// The elements listed in the column specification are assigned index numbers, beginning
							// with 1. The index value of the empty string error value (distinguish from a "normal"
							// empty string) is 0. Thus we need to check whether it's an empty string error value when
							// `fVal==0`.
							for idx, elem := range tp.GetElems() {
								if elem == sVal {
									fVal = float64(idx + 1)
									break
								}
							}
						}
					case mysql.TypeBit:
						var bl types.BinaryLiteral = buf.GetBytes(i)
						iVal, err := bl.ToInt(tc)
						if err != nil {
							return err
						}
						fVal = float64(iVal)
					}
				} else {
					fVal, err = types.StrToFloat(tc, sVal, false)
					if err != nil {
						return err
					}
				}
				if fVal == 0 {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETDecimal:
		d64s := buf.Decimals()
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if d64s[i].IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETJson:
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if buf.GetJSON(i).IsZero() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	case types.ETVectorFloat32:
		for i := range sel {
			if buf.IsNull(i) {
				isZero[i] = -1
			} else {
				if buf.GetVectorFloat32(i).IsZeroValue() {
					isZero[i] = 0
				} else {
					isZero[i] = 1
				}
			}
		}
	default:
		return errors.Errorf("unsupported type %s during evaluation", eType)
	}
	return nil
}

func implicitEvalReal(ctx EvalContext, vecEnabled bool, expr Expression, input *chunk.Chunk, result *chunk.Column) (err error) {
	if expr.Vectorized() && vecEnabled {
		err = expr.VecEvalReal(ctx, input, result)
	} else {
		ind, n := 0, input.NumRows()
		iter := chunk.NewIterator4Chunk(input)
		result.ResizeFloat64(n, false)
		f64s := result.Float64s()
		for it := iter.Begin(); it != iter.End(); it = iter.Next() {
			value, isNull, err := expr.EvalReal(ctx, it)
			if err != nil {
				return err
			}
			if isNull {
				result.SetNull(ind, isNull)
			} else {
				f64s[ind] = value
			}
			ind++
		}
	}
	return
}

// EvalExpr evaluates this expr according to its type.
// And it selects the method for evaluating expression based on
// the environment variables and whether the expression can be vectorized.
// Note: the input argument `evalType` is needed because of that when `expr` is
// of the hybrid type(ENUM/SET/BIT), we need the invoker decide the actual EvalType.
func EvalExpr(ctx EvalContext, vecEnabled bool, expr Expression, evalType types.EvalType, input *chunk.Chunk, result *chunk.Column) (err error) {
	if expr.Vectorized() && vecEnabled {
		switch evalType {
		case types.ETInt:
			err = expr.VecEvalInt(ctx, input, result)
		case types.ETReal:
			err = expr.VecEvalReal(ctx, input, result)
		case types.ETDuration:
			err = expr.VecEvalDuration(ctx, input, result)
		case types.ETDatetime, types.ETTimestamp:
			err = expr.VecEvalTime(ctx, input, result)
		case types.ETString:
			err = expr.VecEvalString(ctx, input, result)
		case types.ETJson:
			err = expr.VecEvalJSON(ctx, input, result)
		case types.ETVectorFloat32:
			err = expr.VecEvalVectorFloat32(ctx, input, result)
		case types.ETDecimal:
			err = expr.VecEvalDecimal(ctx, input, result)
		default:
			err = errors.Errorf("unsupported type %s during evaluation", evalType)
		}
	} else {
		ind, n := 0, input.NumRows()
		iter := chunk.NewIterator4Chunk(input)
		switch evalType {
		case types.ETInt:
			result.ResizeInt64(n, false)
			i64s := result.Int64s()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalInt(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					i64s[ind] = value
				}
				ind++
			}
		case types.ETReal:
			result.ResizeFloat64(n, false)
			f64s := result.Float64s()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalReal(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					f64s[ind] = value
				}
				ind++
			}
		case types.ETDuration:
			result.ResizeGoDuration(n, false)
			d64s := result.GoDurations()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalDuration(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					d64s[ind] = value.Duration
				}
				ind++
			}
		case types.ETDatetime, types.ETTimestamp:
			result.ResizeTime(n, false)
			t64s := result.Times()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalTime(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					t64s[ind] = value
				}
				ind++
			}
		case types.ETString:
			result.ReserveString(n)
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalString(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendString(value)
				}
			}
		case types.ETJson:
			result.ReserveJSON(n)
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalJSON(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendJSON(value)
				}
			}
		case types.ETVectorFloat32:
			result.ReserveVectorFloat32(n)
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalVectorFloat32(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.AppendNull()
				} else {
					result.AppendVectorFloat32(value)
				}
			}
		case types.ETDecimal:
			result.ResizeDecimal(n, false)
			d64s := result.Decimals()
			for it := iter.Begin(); it != iter.End(); it = iter.Next() {
				value, isNull, err := expr.EvalDecimal(ctx, it)
				if err != nil {
					return err
				}
				if isNull {
					result.SetNull(ind, isNull)
				} else {
					d64s[ind] = *value
				}
				ind++
			}
		default:
			err = errors.Errorf("unsupported type %s during evaluation", expr.GetType(ctx).EvalType())
		}
	}
	return
}

// composeConditionWithBinaryOp composes condition with binary operator into a balance deep tree, which benefits a lot for pb decoder/encoder.
func composeConditionWithBinaryOp(ctx BuildContext, conditions []Expression, funcName string) Expression {
	length := len(conditions)
	if length == 0 {
		return nil
	}
	if length == 1 {
		return conditions[0]
	}
	expr := NewFunctionInternal(ctx, funcName,
		types.NewFieldType(mysql.TypeTiny),
		composeConditionWithBinaryOp(ctx, conditions[:length/2], funcName),
		composeConditionWithBinaryOp(ctx, conditions[length/2:], funcName))
	return expr
}

// ComposeCNFCondition composes CNF items into a balance deep CNF tree, which benefits a lot for pb decoder/encoder.
func ComposeCNFCondition(ctx BuildContext, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicAnd)
}

// ComposeDNFCondition composes DNF items into a balance deep DNF tree.
func ComposeDNFCondition(ctx BuildContext, conditions ...Expression) Expression {
	return composeConditionWithBinaryOp(ctx, conditions, ast.LogicOr)
}

func extractBinaryOpItems(conditions *ScalarFunction, funcName string) []Expression {
	ret := make([]Expression, 0, len(conditions.GetArgs()))
	for _, arg := range conditions.GetArgs() {
		if sf, ok := arg.(*ScalarFunction); ok && sf.FuncName.L == funcName {
			ret = append(ret, extractBinaryOpItems(sf, funcName)...)
		} else {
			ret = append(ret, arg)
		}
	}
	return ret
}

// FlattenDNFConditions extracts DNF expression's leaf item.
// e.g. or(or(a=1, a=2), or(a=3, a=4)), we'll get [a=1, a=2, a=3, a=4].
func FlattenDNFConditions(DNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(DNFCondition, ast.LogicOr)
}

// FlattenCNFConditions extracts CNF expression's leaf item.
// e.g. and(and(a>1, a>2), and(a>3, a>4)), we'll get [a>1, a>2, a>3, a>4].
func FlattenCNFConditions(CNFCondition *ScalarFunction) []Expression {
	return extractBinaryOpItems(CNFCondition, ast.LogicAnd)
}


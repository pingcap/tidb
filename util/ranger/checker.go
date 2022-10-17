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

package ranger

import (
	"unicode/utf8"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
)

// conditionChecker checks if this condition can be pushed to index planner.
type conditionChecker struct {
	checkerCol *expression.Column
	length     int
}

func (c *conditionChecker) isFullLengthColumn() bool {
	return c.length == types.UnspecifiedLength || c.length == c.checkerCol.GetType().GetFlen()
}

// check returns two values, isIndexCond and shouldReserve.
// isIndexCond indicates whether the condition can be pushed to index.
// shouldReserve indicates whether the condition should be reserved on table.
func (c *conditionChecker) check(condition expression.Expression) (isIndexCond bool, shouldReserve bool) {
	switch x := condition.(type) {
	case *expression.ScalarFunction:
		return c.checkScalarFunction(x)
	case *expression.Column:
		if x.RetType.EvalType() == types.ETString {
			return false, true
		}
		if c.checkColumn(x) {
			return true, !c.isFullLengthColumn()
		}
		return false, true
	case *expression.Constant:
		return true, false
	}
	return false, true
}

func (c *conditionChecker) checkScalarFunction(scalar *expression.ScalarFunction) (isIndexCond bool, shouldReserve bool) {
	_, collation := scalar.CharsetAndCollation()
	switch scalar.FuncName.L {
	case ast.LogicOr, ast.LogicAnd:
		isIndexCond0, shouldReserve0 := c.check(scalar.GetArgs()[0])
		isIndexCond1, shouldReserve1 := c.check(scalar.GetArgs()[1])
		if isIndexCond0 && isIndexCond1 {
			return true, shouldReserve0 || shouldReserve1
		}
		return false, true
	case ast.EQ, ast.NE, ast.GE, ast.GT, ast.LE, ast.LT, ast.NullEQ:
		if constVal, ok := scalar.GetArgs()[0].(*expression.Constant); ok {
			if c.checkColumn(scalar.GetArgs()[1]) {
				// Checks whether the scalar function is calculated use the collation compatible with the column.
				if scalar.GetArgs()[1].GetType().EvalType() == types.ETString && !collate.CompatibleCollate(scalar.GetArgs()[1].GetType().GetCollate(), collation) {
					return false, true
				}
				if c.isFullLengthColumn() {
					return true, false
				}
				constLen := GetConstantLength(constVal, scalar.GetArgs()[0].GetType())
				constLenLessThanColLen := constLen != types.UnspecifiedLength && constLen < c.length
				if scalar.FuncName.L == ast.NE {
					return constLenLessThanColLen, !constLenLessThanColLen
				}
				return true, !constLenLessThanColLen
			}
		}
		if constVal, ok := scalar.GetArgs()[1].(*expression.Constant); ok {
			if c.checkColumn(scalar.GetArgs()[0]) {
				// Checks whether the scalar function is calculated use the collation compatible with the column.
				if scalar.GetArgs()[0].GetType().EvalType() == types.ETString && !collate.CompatibleCollate(scalar.GetArgs()[0].GetType().GetCollate(), collation) {
					return false, true
				}
				if c.isFullLengthColumn() {
					return true, false
				}
				constLen := GetConstantLength(constVal, scalar.GetArgs()[0].GetType())
				constLenLessThanColLen := constLen != types.UnspecifiedLength && constLen < c.length
				if scalar.FuncName.L == ast.NE {
					return constLenLessThanColLen, !constLenLessThanColLen
				}
				return true, !constLenLessThanColLen
			}
		}
	case ast.IsNull:
		if c.checkColumn(scalar.GetArgs()[0]) {
			// We can know whether the column is null from prefix column of any length.
			return true, false
		}
		return false, true
	case ast.IsTruthWithoutNull, ast.IsFalsity, ast.IsTruthWithNull:
		if s, ok := scalar.GetArgs()[0].(*expression.Column); ok {
			if s.RetType.EvalType() == types.ETString {
				return false, true
			}
		}
		if c.checkColumn(scalar.GetArgs()[0]) {
			return true, !c.isFullLengthColumn()
		}
		return false, true
	case ast.UnaryNot:
		// TODO: support "not like" convert to access conditions.
		s, ok := scalar.GetArgs()[0].(*expression.ScalarFunction)
		if !ok {
			// "not column" or "not constant" can't lead to a range.
			return false, true
		}
		if s.FuncName.L == ast.Like || s.FuncName.L == ast.NullEQ {
			return false, true
		}
		return c.check(scalar.GetArgs()[0])
	case ast.In:
		if !c.checkColumn(scalar.GetArgs()[0]) {
			return false, true
		}
		if scalar.GetArgs()[0].GetType().EvalType() == types.ETString && !collate.CompatibleCollate(scalar.GetArgs()[0].GetType().GetCollate(), collation) {
			return false, true
		}
		var inFuncReserve bool
		for _, v := range scalar.GetArgs()[1:] {
			if constVal, ok := v.(*expression.Constant); ok {
				if !c.isFullLengthColumn() {
					constLen := GetConstantLength(constVal, scalar.GetArgs()[0].GetType())
					if constLen == types.UnspecifiedLength || constLen >= c.length {
						inFuncReserve = true
					}
				}
			} else {
				return false, true
			}
		}
		return true, inFuncReserve
	case ast.Like:
		return c.checkLikeFunc(scalar)
	case ast.GetParam:
		// TODO
		return true, false
	}
	return false, true
}

func (c *conditionChecker) checkLikeFunc(scalar *expression.ScalarFunction) (isIndexCond bool, shouldReserve bool) {
	_, collation := scalar.CharsetAndCollation()
	if collate.NewCollationEnabled() && !collate.IsBinCollation(collation) {
		// The algorithm constructs the range in byte-level: for example, ab% is mapped to [ab, ac] by adding 1 to the last byte.
		// However, this is incorrect for non-binary collation strings because the sort key order is not the same as byte order.
		// For example, "`%" is mapped to the range [`, a](where ` is 0x60 and a is 0x61).
		// Because the collation utf8_general_ci is case-insensitive, a and A have the same sort key.
		// Finally, the range comes to be [`, A], which is actually an empty range.
		// See https://github.com/pingcap/tidb/issues/31174 for more details.
		// In short, when the column type is non-binary collation string, we cannot use `like` expressions to generate the range.
		return false, true
	}
	if !collate.CompatibleCollate(scalar.GetArgs()[0].GetType().GetCollate(), collation) {
		return false, true
	}
	if !c.checkColumn(scalar.GetArgs()[0]) {
		return false, true
	}
	pattern, ok := scalar.GetArgs()[1].(*expression.Constant)
	if !ok {
		return false, true
	}
	if pattern.Value.IsNull() {
		return false, true
	}
	patternStr, err := pattern.Value.ToString()
	if err != nil {
		return false, true
	}
	if len(patternStr) == 0 {
		return true, true
	}
	escape := byte(scalar.GetArgs()[2].(*expression.Constant).Value.GetInt64())
	likeFuncReserve := !c.isFullLengthColumn()
	for i := 0; i < len(patternStr); i++ {
		if patternStr[i] == escape {
			i++
			if i < len(patternStr)-1 {
				continue
			}
			break
		}
		if i == 0 && (patternStr[i] == '%' || patternStr[i] == '_') {
			return false, true
		}
		if patternStr[i] == '%' {
			// We currently do not support using `enum like 'xxx%'` to build range
			// see https://github.com/pingcap/tidb/issues/27130 for more details
			if scalar.GetArgs()[0].GetType().GetType() == mysql.TypeEnum {
				return false, true
			}
			if i != len(patternStr)-1 {
				likeFuncReserve = true
			}
			break
		}
		if patternStr[i] == '_' {
			// We currently do not support using `enum like 'xxx_'` to build range
			// see https://github.com/pingcap/tidb/issues/27130 for more details
			if scalar.GetArgs()[0].GetType().GetType() == mysql.TypeEnum {
				return false, true
			}
			likeFuncReserve = true
			break
		}
	}
	return true, likeFuncReserve
}

func (c *conditionChecker) checkColumn(expr expression.Expression) bool {
	// Check if virtual expression column matched
	if c.checkerCol != nil {
		return c.checkerCol.EqualByExprAndID(nil, expr)
	}
	return false
}

// GetConstantLength returns the length of the constant.
// If the type is not prefixable or the constant is mutable, return unspecifiedLength.
// Otherwise, return the length of the constant.
func GetConstantLength(c *expression.Constant, tp *types.FieldType) int {
	if tp == nil || !types.IsTypePrefixable(tp.GetType()) {
		return types.UnspecifiedLength
	}
	if c == nil || c.DeferredExpr != nil || c.ParamMarker != nil {
		return types.UnspecifiedLength
	}
	if c.RetType.GetCharset() != tp.GetCharset() {
		//
		return types.UnspecifiedLength
	}
	val, err := c.Eval(chunk.Row{})
	if err != nil || (val.Kind() != types.KindBytes && val.Kind() != types.KindString) {
		return types.UnspecifiedLength
	}
	if tp.GetCharset() == charset.CharsetBin || tp.GetCharset() == charset.CharsetASCII {
		return len(val.GetBytes())
	}
	return utf8.RuneCount(val.GetBytes())
}

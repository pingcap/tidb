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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
)

// conditionChecker checks if this condition can be pushed to index planner.
type conditionChecker struct {
	colUniqueID   int64
	checkerCol    *expression.Column
	shouldReserve bool // check if a access condition should be reserved in filter conditions.
	length        int
}

func (c *conditionChecker) check(condition expression.Expression) bool {
	switch x := condition.(type) {
	case *expression.ScalarFunction:
		return c.checkScalarFunction(x)
	case *expression.Column:
		if x.RetType.EvalType() == types.ETString {
			return false
		}
		return c.checkColumn(x)
	case *expression.Constant:
		return true
	}
	return false
}

func (c *conditionChecker) checkScalarFunction(scalar *expression.ScalarFunction) bool {
	_, collation := scalar.CharsetAndCollation()
	switch scalar.FuncName.L {
	case ast.LogicOr, ast.LogicAnd:
		return c.check(scalar.GetArgs()[0]) && c.check(scalar.GetArgs()[1])
	case ast.EQ, ast.NE, ast.GE, ast.GT, ast.LE, ast.LT, ast.NullEQ:
		if _, ok := scalar.GetArgs()[0].(*expression.Constant); ok {
			if c.checkColumn(scalar.GetArgs()[1]) {
				// Checks whether the scalar function is calculated use the collation compatible with the column.
				if scalar.GetArgs()[1].GetType().EvalType() == types.ETString && !collate.CompatibleCollate(scalar.GetArgs()[1].GetType().GetCollate(), collation) {
					return false
				}
				return scalar.FuncName.L != ast.NE || c.length == types.UnspecifiedLength
			}
		}
		if _, ok := scalar.GetArgs()[1].(*expression.Constant); ok {
			if c.checkColumn(scalar.GetArgs()[0]) {
				// Checks whether the scalar function is calculated use the collation compatible with the column.
				if scalar.GetArgs()[0].GetType().EvalType() == types.ETString && !collate.CompatibleCollate(scalar.GetArgs()[0].GetType().GetCollate(), collation) {
					return false
				}
				return scalar.FuncName.L != ast.NE || c.length == types.UnspecifiedLength
			}
		}
	case ast.IsNull:
		return c.checkColumn(scalar.GetArgs()[0])
	case ast.IsTruthWithoutNull, ast.IsFalsity, ast.IsTruthWithNull:
		if s, ok := scalar.GetArgs()[0].(*expression.Column); ok {
			if s.RetType.EvalType() == types.ETString {
				return false
			}
		}
		return c.checkColumn(scalar.GetArgs()[0])
	case ast.UnaryNot:
		// TODO: support "not like" convert to access conditions.
		if s, ok := scalar.GetArgs()[0].(*expression.ScalarFunction); ok {
			if s.FuncName.L == ast.Like {
				return false
			}
		} else {
			// "not column" or "not constant" can't lead to a range.
			return false
		}
		return c.check(scalar.GetArgs()[0])
	case ast.In:
		if !c.checkColumn(scalar.GetArgs()[0]) {
			return false
		}
		if scalar.GetArgs()[0].GetType().EvalType() == types.ETString && !collate.CompatibleCollate(scalar.GetArgs()[0].GetType().GetCollate(), collation) {
			return false
		}
		for _, v := range scalar.GetArgs()[1:] {
			if _, ok := v.(*expression.Constant); !ok {
				return false
			}
		}
		return true
	case ast.Like:
		return c.checkLikeFunc(scalar)
	case ast.GetParam:
		return true
	}
	return false
}

func (c *conditionChecker) checkLikeFunc(scalar *expression.ScalarFunction) bool {
	_, collation := scalar.CharsetAndCollation()
	if collate.NewCollationEnabled() && !collate.IsBinCollation(collation) {
		// The algorithm constructs the range in byte-level: for example, ab% is mapped to [ab, ac] by adding 1 to the last byte.
		// However, this is incorrect for non-binary collation strings because the sort key order is not the same as byte order.
		// For example, "`%" is mapped to the range [`, a](where ` is 0x60 and a is 0x61).
		// Because the collation utf8_general_ci is case-insensitive, a and A have the same sort key.
		// Finally, the range comes to be [`, A], which is actually an empty range.
		// See https://github.com/pingcap/tidb/issues/31174 for more details.
		// In short, when the column type is non-binary collation string, we cannot use `like` expressions to generate the range.
		return false
	}
	if !collate.CompatibleCollate(scalar.GetArgs()[0].GetType().GetCollate(), collation) {
		return false
	}
	if !c.checkColumn(scalar.GetArgs()[0]) {
		return false
	}
	pattern, ok := scalar.GetArgs()[1].(*expression.Constant)
	if !ok {
		return false

	}
	if pattern.Value.IsNull() {
		return false
	}
	patternStr, err := pattern.Value.ToString()
	if err != nil {
		return false
	}
	if len(patternStr) == 0 {
		return true
	}
	escape := byte(scalar.GetArgs()[2].(*expression.Constant).Value.GetInt64())
	for i := 0; i < len(patternStr); i++ {
		if patternStr[i] == escape {
			i++
			if i < len(patternStr)-1 {
				continue
			}
			break
		}
		if i == 0 && (patternStr[i] == '%' || patternStr[i] == '_') {
			return false
		}
		if patternStr[i] == '%' {
			// We currently do not support using `enum like 'xxx%'` to build range
			// see https://github.com/pingcap/tidb/issues/27130 for more details
			if scalar.GetArgs()[0].GetType().GetType() == mysql.TypeEnum {
				return false
			}
			if i != len(patternStr)-1 {
				c.shouldReserve = true
			}
			break
		}
		if patternStr[i] == '_' {
			// We currently do not support using `enum like 'xxx_'` to build range
			// see https://github.com/pingcap/tidb/issues/27130 for more details
			if scalar.GetArgs()[0].GetType().GetType() == mysql.TypeEnum {
				return false
			}
			c.shouldReserve = true
			break
		}
	}
	return true
}

func (c *conditionChecker) checkColumn(expr expression.Expression) bool {
	col, ok := expr.(*expression.Column)
	if !ok {
		return false
	}
	// Check if virtual expression column matched
	if c.checkerCol != nil {
		return c.checkerCol.EqualByExprAndID(nil, col)
	}
	return c.colUniqueID == col.UniqueID
}

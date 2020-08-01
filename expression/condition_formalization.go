package expression

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type formatCondSolver struct {
	ctx sessionctx.Context
}

func (s *formatCondSolver) format(conditions []Expression) []Expression {
	arrays := s.conditions2Arrays(conditions)
	return s.arrays2Conditions(arrays)
}

// conditions2Arrays convert the conditions to two dimensional array.
// The relationship between the elements of the inner array is `and`.
// The relationship between the elements of the outer array is `or`.
// For example,`a=1 and (b = 2 or c = 3)` will be converted to
// `{{`a=1`,`b=2`},{`a=1`,`c=3`}}`.
func (s *formatCondSolver) conditions2Arrays(conditions []Expression) [][]Expression {
	arrays := s.condition2Arrays(conditions[0], false)
	for i := 1; i < len(conditions); i++ {
		arrays = s.and(arrays, s.condition2Arrays(conditions[i], false))
	}
	return arrays
}

func (s *formatCondSolver) condition2Arrays(condition Expression, isParentNot bool) [][]Expression {
	fn, ok := condition.(*ScalarFunction)
	if ok {
		switch fn.FuncName.L {
		case ast.UnaryNot:
			return s.condition2Arrays(fn.GetArgs()[0], !isParentNot)
		case ast.LogicAnd, ast.LogicOr:
			left := s.condition2Arrays(fn.GetArgs()[0], isParentNot)
			right := s.condition2Arrays(fn.GetArgs()[1], isParentNot)
			if fn.FuncName.L == ast.LogicOr {
				if !isParentNot {
					return s.or(left, right)
				}
				return s.and(left, right)
			}
			if !isParentNot {
				return s.and(left, right)
			}
			return s.or(left, right)
		}
	}

	if !isParentNot {
		return [][]Expression{{condition}}
	}
	newCond := NewFunctionInternal(s.ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), condition)
	return [][]Expression{{newCond}}
}

func (s *formatCondSolver) and(left [][]Expression, right [][]Expression) [][]Expression {
	if len(left) == 0 {
		return right
	} else if len(right) == 0 {
		return left
	} else {
		newArrays := make([][]Expression, 0, len(left)*len(right))
		for _, l := range left {
			for _, r := range right {
				newArray := make([]Expression, 0, len(left)+len(right))
				newArray = append(newArray, l...)
				newArray = append(newArray, r...)
				newArrays = append(newArrays, newArray)
			}
		}
		return newArrays
	}
}

func (s *formatCondSolver) or(left [][]Expression, right [][]Expression) [][]Expression {
	if len(left) == 0 {
		return right
	} else if len(right) == 0 {
		return left
	} else {
		return append(left, right...)
	}
}

func (s *formatCondSolver) arrays2Conditions(arrays [][]Expression) []Expression {
	if len(arrays) == 0 {
		return []Expression{}
	} else if len(arrays) == 1 {
		return arrays[0]
	} else {
		condition := s.array2LogicAndTree(arrays[0])
		for i := 1; i < len(arrays); i++ {
			nextCond := s.array2LogicAndTree(arrays[i])
			condition = NewFunctionInternal(s.ctx, ast.LogicOr, types.NewFieldType(mysql.TypeTiny), []Expression{condition, nextCond}...)
		}
		return []Expression{condition}
	}
}

func (s *formatCondSolver) array2LogicAndTree(array []Expression) Expression {
	cond := array[0]
	for i := 1; i < len(array); i++ {
		cond = NewFunctionInternal(s.ctx, ast.LogicAnd, types.NewFieldType(mysql.TypeTiny), []Expression{cond, array[i]}...)
	}
	return cond
}

// FormalizeConditions format the conditions to `(cond1 and cond2...) or (cond3 and cond4...) or ...`.
func FormalizeConditions(ctx sessionctx.Context, conditions []Expression) []Expression {
	if len(conditions) == 0 {
		return conditions
	}

	formatCondSolver := formatCondSolver{ctx}
	return formatCondSolver.format(conditions)
}

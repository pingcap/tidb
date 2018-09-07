// Copyright 2018 PingCAP, Inc.
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

package expression

import (
	"bytes"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// exprSet is a Set container for expressions, each expression in it is unique.
// `tombstone` is deleted mark, if tombstone[i] is false, data[i] is invalid.
// `index` use expr.HashCode() as key.
type exprSet struct {
	data      []Expression
	tombstone []bool
	index     map[string]struct{}
}

func (s *exprSet) Append(e Expression) bool {
	if _, ok := s.index[string(e.HashCode(nil))]; ok {
		return false
	}

	s.data = append(s.data, e)
	s.tombstone = append(s.tombstone, false)
	s.index[string(e.HashCode(nil))] = struct{}{}
	return true
}

func (s *exprSet) Slice() []Expression {
	idx := 0
	for i := 0; i < len(s.data); i++ {
		if !s.tombstone[i] {
			s.data[idx] = s.data[i]
			idx++
		}
	}
	return s.data[:idx]
}
func (s *exprSet) SetConstFalse() {
	for i := 0; i < len(s.data); i++ {
		s.tombstone[i] = true
	}
	s.Append(&Constant{
		Value:   types.NewDatum(false),
		RetType: types.NewFieldType(mysql.TypeTiny),
	})
}

// PropagateConstantNew propagate constant values of deterministic predicates in a condition.
func PropagateConstantNew(ctx sessionctx.Context, conditions []Expression) []Expression {
	var exprs exprSet
	exprs.data = make([]Expression, 0, len(conditions))
	exprs.tombstone = make([]bool, 0, len(conditions))
	exprs.index = make(map[string]struct{}, len(conditions))
	for _, v := range conditions {
		exprs.Append(v)
	}

	fixPoint(ctx, &exprs)
	return exprs.Slice()
}

// fixPoint is the core of the constant propagation algorithm.
// It will iterate the expression set over and over again, pick two expressions,
// apply one to another.
// If new conditions can be infered, they will be append into the expression set.
// Until no more conditions can be infered from the set, the algorithm finish.
func fixPoint(ctx sessionctx.Context, exprs *exprSet) {
	for {
		saveLen := len(exprs.data)
		iterOnce(ctx, exprs)
		if saveLen == len(exprs.data) {
			break
		}
	}
	return
}

// iterOnce picks two expressions from the set, try to propagate new conditions from them.
func iterOnce(ctx sessionctx.Context, exprs *exprSet) {
	for i := 0; i < len(exprs.data); i++ {
		if exprs.tombstone[i] {
			continue
		}
		for j := 0; j < len(exprs.data); j++ {
			if exprs.tombstone[j] {
				continue
			}
			if i == j {
				continue
			}
			solve(ctx, i, j, exprs)
		}
	}
}

func solve(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	cond := exprs.data[i]
	if cons, ok := cond.(*Constant); ok {
		switch cons.RetType.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong:
			if cons.Value.GetInt64() == 0 {
				exprs.tombstone[j] = true
				return
			}
		}
	}
	col, cons := validEqualCond(cond)
	if col != nil {
		solveColumnEQConst(ctx.GetSessionVars().StmtCtx, col, cons, i, j, exprs)
		return
	}
	switch {
	case matchColumnEQColumn(cond, func(_ *ScalarFunction, col1, col2 *Column) {
		solveColumnEQColumn(ctx, col1, col2, i, j, exprs)
	}):
	case matchColumnGTConst(cond, func(_ *ScalarFunction, col1 *Column, cons1 *Constant) {
		solveColumnGTConst(ctx, col1, cons1, i, j, exprs)
	}):
	case matchColumnGEConst(cond, func(_ *ScalarFunction, col1 *Column, cons1 *Constant) {
		solveColumnGEConst(ctx, col1, cons1, i, j, exprs)
	}):
	}
}

func solveColumnEQConst(ctx *stmtctx.StatementContext, col *Column, cons *Constant, i, j int, exprs *exprSet) {
	expr := ColumnSubstitute(exprs.data[j], NewSchema(col), []Expression{cons})
	if bytes.Compare(expr.HashCode(ctx), exprs.data[j].HashCode(ctx)) != 0 {
		exprs.Append(expr)
		exprs.tombstone[j] = true
	}
}

func solveColumnEQColumn(ctx sessionctx.Context, col1, col2 *Column, i, j int, exprs *exprSet) {
	expr := exprs.data[j]
	if !matchColumnEQColumn(expr, func(_ *ScalarFunction, _, _ *Column) {}) {
		if replaced, _, newExpr := tryToReplaceCond(ctx, col1, col2, expr); replaced {
			exprs.Append(newExpr)
		}
		if replaced, _, newExpr := tryToReplaceCond(ctx, col2, col1, expr); replaced {
			exprs.Append(newExpr)
		}
	}
}

func solveColumnGTConst(ctx sessionctx.Context, col *Column, cons *Constant, i, j int, exprs *exprSet) {
	expr := exprs.data[j]
	switch {
	case matchColumnEQColumn(expr, func(op *ScalarFunction, col1, col2 *Column) {
		if col.UniqueID == col1.UniqueID {
			// x > const, x = y => y > const
			newExpr := NewFunctionInternal(ctx, ast.GT, op.RetType, col2, cons)
			exprs.Append(newExpr)
		}
		if col.UniqueID == col2.UniqueID {
			// y > const, x = y => x > const
			newExpr := NewFunctionInternal(ctx, ast.GT, op.RetType, col1, cons)
			exprs.Append(newExpr)
		}
	}):
	case matchColumnGTConst(expr, func(op *ScalarFunction, col1 *Column, cons1 *Constant) {
		if col.UniqueID == col1.UniqueID {
			// x > const1, x > const2  => x > max(const1, const2)
			cmp := NewFunctionInternal(ctx, ast.GT, op.RetType, cons, cons1)
			val, isNull, err := cmp.EvalInt(ctx, chunk.Row{})
			if err == nil && !isNull {
				if val > 0 {
					exprs.tombstone[j] = true
				}
			}
		}
	}):
	case matchColumnLTConst(expr, func(op *ScalarFunction, col1 *Column, cons1 *Constant) {
		if col.UniqueID == col1.UniqueID {
			// x > const1, x < const2 && const1 >= const2 => false
			cmp := NewFunctionInternal(ctx, ast.GE, op.RetType, cons, cons1)
			val, isNull, err := cmp.EvalInt(ctx, chunk.Row{})
			if err == nil && !isNull {
				if val > 0 {
					exprs.SetConstFalse()
				}
			}
		}
	}):
	}
}

func solveColumnGEConst(ctx sessionctx.Context, col *Column, cons *Constant, i, j int, exprs *exprSet) {
	expr := exprs.data[j]
	switch {
	case matchColumnEQColumn(expr, func(op *ScalarFunction, col1, col2 *Column) {
		if col.UniqueID == col1.UniqueID {
			// x >= const, x = y => y >= const
			newExpr := NewFunctionInternal(ctx, ast.GE, op.RetType, col2, cons)
			exprs.Append(newExpr)
		}
		if col.UniqueID == col2.UniqueID {
			// y >= const, x = y => x >= const
			newExpr := NewFunctionInternal(ctx, ast.GE, op.RetType, col1, cons)
			exprs.Append(newExpr)
		}
	}):
	case matchColumnGEConst(expr, func(op *ScalarFunction, col1 *Column, cons1 *Constant) {
		if col.UniqueID == col1.UniqueID {
			// x >= const1, x >= const2  => x >= max(const1, const2)
			cmp := NewFunctionInternal(ctx, ast.GE, op.RetType, cons, cons1)
			val, isNull, err := cmp.EvalInt(ctx, chunk.Row{})
			if err == nil && !isNull {
				if val > 0 {
					exprs.tombstone[j] = true
				}
			}
		}
	}):
	case matchColumnLTConst(expr, func(op *ScalarFunction, col1 *Column, cons1 *Constant) {
		if col.UniqueID == col1.UniqueID {
			// x >= const1, x < const2 && const1 > const2 => false
			cmp := NewFunctionInternal(ctx, ast.GE, op.RetType, cons, cons1)
			val, isNull, err := cmp.EvalInt(ctx, chunk.Row{})
			if err == nil && !isNull {
				if val > 0 {
					exprs.SetConstFalse()
				}
			}
		}
	}):
	}
}

func matchColumnGTConst(expr Expression, action func(*ScalarFunction, *Column, *Constant)) bool {
	return matchColumnOPConst(expr, ast.GT, action)
}

func matchColumnGEConst(expr Expression, action func(*ScalarFunction, *Column, *Constant)) bool {
	return matchColumnOPConst(expr, ast.GE, action)
}

func matchColumnLTConst(expr Expression, action func(*ScalarFunction, *Column, *Constant)) bool {
	return matchColumnOPConst(expr, ast.LT, action)
}

func matchColumnOPConst(expr Expression, OP string, action func(*ScalarFunction, *Column, *Constant)) bool {
	fun, ok := expr.(*ScalarFunction)
	if !ok || fun.FuncName.L != OP {
		return false
	}
	col, ok := fun.GetArgs()[0].(*Column)
	if !ok {
		return false
	}
	con, ok := fun.GetArgs()[1].(*Constant)
	if !ok {
		return false
	}
	action(fun, col, con)
	return true
}

func matchColumnEQColumn(expr Expression, action func(*ScalarFunction, *Column, *Column)) bool {
	fun, ok := expr.(*ScalarFunction)
	if !ok || fun.FuncName.L != ast.EQ {
		return false
	}
	col1, ok := fun.GetArgs()[0].(*Column)
	if !ok {
		return false
	}
	col2, ok := fun.GetArgs()[1].(*Column)
	if !ok {
		return false
	}
	action(fun, col1, col2)
	return true
}

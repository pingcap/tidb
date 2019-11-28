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
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// exprSet is a Set container for expressions, each expression in it is unique.
// `tombstone` is deleted mark, if tombstone[i] is true, data[i] is invalid.
// `index` use expr.HashCode() as key, to implement the unique property.
type exprSet struct {
	data       []Expression
	tombstone  []bool
	exists     map[string]struct{}
	constfalse bool
}

func (s *exprSet) Append(sc *stmtctx.StatementContext, e Expression) bool {
	if _, ok := s.exists[string(e.HashCode(sc))]; ok {
		return false
	}

	s.data = append(s.data, e)
	s.tombstone = append(s.tombstone, false)
	s.exists[string(e.HashCode(sc))] = struct{}{}
	return true
}

// Slice returns the valid expressions in the exprSet, this function has side effect.
func (s *exprSet) Slice() []Expression {
	if s.constfalse {
		return []Expression{&Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}}
	}

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
	s.constfalse = true
}

func newExprSet(ctx sessionctx.Context, conditions []Expression) *exprSet {
	var exprs exprSet
	exprs.data = make([]Expression, 0, len(conditions))
	exprs.tombstone = make([]bool, 0, len(conditions))
	exprs.exists = make(map[string]struct{}, len(conditions))
	sc := ctx.GetSessionVars().StmtCtx
	for _, v := range conditions {
		exprs.Append(sc, v)
	}
	return &exprs
}

type constraintSolver []constraintPropagateRule

func newConstraintSolver(rules ...constraintPropagateRule) constraintSolver {
	return constraintSolver(rules)
}

type pgSolver2 struct{}

func (s pgSolver2) PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression {
	solver := newConstraintSolver(ruleConstantFalse, ruleColumnEQConst)
	return solver.Solve(ctx, conditions)
}

// Solve propagate constraint according to the rules in the constraintSolver.
func (s constraintSolver) Solve(ctx sessionctx.Context, conditions []Expression) []Expression {
	exprs := newExprSet(ctx, conditions)
	s.fixPoint(ctx, exprs)
	return exprs.Slice()
}

// fixPoint is the core of the constraint propagation algorithm.
// It will iterate the expression set over and over again, pick two expressions,
// apply one to another.
// If new conditions can be inferred, they will be append into the expression set.
// Until no more conditions can be inferred from the set, the algorithm finish.
func (s constraintSolver) fixPoint(ctx sessionctx.Context, exprs *exprSet) {
	for {
		saveLen := len(exprs.data)
		s.iterOnce(ctx, exprs)
		if saveLen == len(exprs.data) {
			break
		}
	}
}

// iterOnce picks two expressions from the set, try to propagate new conditions from them.
func (s constraintSolver) iterOnce(ctx sessionctx.Context, exprs *exprSet) {
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
			s.solve(ctx, i, j, exprs)
		}
	}
}

// solve uses exprs[i] exprs[j] to propagate new conditions.
func (s constraintSolver) solve(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	for _, rule := range s {
		rule(ctx, i, j, exprs)
	}
}

type constraintPropagateRule func(ctx sessionctx.Context, i, j int, exprs *exprSet)

// ruleConstantFalse propagates from CNF condition that false plus anything returns false.
// false, a = 1, b = c ... => false
func ruleConstantFalse(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	cond := exprs.data[i]
	if cons, ok := cond.(*Constant); ok {
		v, isNull, err := cons.EvalInt(ctx, chunk.Row{})
		if err != nil {
			logutil.Logger(context.Background()).Warn("eval constant", zap.Error(err))
			return
		}
		if !isNull && v == 0 {
			exprs.SetConstFalse()
		}
	}
}

// ruleColumnEQConst propagates the "column = const" condition.
// "a = 3, b = a, c = a, d = b" => "a = 3, b = 3, c = 3, d = 3"
func ruleColumnEQConst(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	col, cons := validEqualCond(exprs.data[i])
	if col != nil {
		expr := ColumnSubstitute(exprs.data[j], NewSchema(col), []Expression{cons})
		stmtctx := ctx.GetSessionVars().StmtCtx
		if !bytes.Equal(expr.HashCode(stmtctx), exprs.data[j].HashCode(stmtctx)) {
			exprs.Append(stmtctx, expr)
			exprs.tombstone[j] = true
		}
	}
}

// ruleColumnOPConst propagates the "column OP const" condition.
func ruleColumnOPConst(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	cond := exprs.data[i]
	f1, ok := cond.(*ScalarFunction)
	if !ok {
		return
	}
	if f1.FuncName.L != ast.GE && f1.FuncName.L != ast.GT &&
		f1.FuncName.L != ast.LE && f1.FuncName.L != ast.LT {
		return
	}
	OP1 := f1.FuncName.L

	var col1 *Column
	var con1 *Constant
	col1, ok = f1.GetArgs()[0].(*Column)
	if !ok {
		return
	}
	con1, ok = f1.GetArgs()[1].(*Constant)
	if !ok {
		return
	}

	expr := exprs.data[j]
	f2, ok := expr.(*ScalarFunction)
	if !ok {
		return
	}

	// The simple case:
	// col >= c1, col < c2, c1 >= c2 => false
	// col >= c1, col <= c2, c1 > c2 => false
	// col >= c1, col OP c2, c1 ^OP c2, where OP in [< , <=] => false
	// col OP1 c1 where OP1 in [>= , <], col OP2 c2 where OP1 opsite OP2, c1 ^OP2 c2 => false
	//
	// The extended case:
	// col >= c1, f(col) < c2, f is monotonous, f(c1) >= c2 => false
	//
	// Proof:
	// col > c1, f is monotonous => f(col) > f(c1)
	// f(col) > f(c1), f(col) < c2, f(c1) >= c2 => false
	OP2 := f2.FuncName.L
	if !opsiteOP(OP1, OP2) {
		return
	}

	con2, ok := f2.GetArgs()[1].(*Constant)
	if !ok {
		return
	}
	arg0 := f2.GetArgs()[0]
	// The simple case.
	var fc1 Expression
	col2, ok := arg0.(*Column)
	if ok {
		fc1 = con1
	} else {
		// The extended case.
		scalarFunc, ok := arg0.(*ScalarFunction)
		if !ok {
			return
		}
		_, ok = monotoneIncFuncs[scalarFunc.FuncName.L]
		if !ok {
			return
		}
		col2, ok = scalarFunc.GetArgs()[0].(*Column)
		if !ok {
			return
		}
		var err error
		fc1, err = NewFunction(ctx, scalarFunc.FuncName.L, scalarFunc.RetType, con1)
		if err != nil {
			logutil.Logger(context.Background()).Warn("build new function in ruleColumnOPConst", zap.Error(err))
			return
		}
	}

	// Make sure col1 and col2 are the same column.
	// Can't use col1.Equal(ctx, col2) here, because they are not generated in one
	// expression and their UniqueID are not the same.
	if col1.ColName.L != col2.ColName.L {
		return
	}
	if col1.OrigColName.L != "" &&
		col2.OrigColName.L != "" &&
		col1.OrigColName.L != col2.OrigColName.L {
		return
	}
	if col1.OrigTblName.L != "" &&
		col2.OrigTblName.L != "" &&
		col1.OrigColName.L != col2.OrigColName.L {
		return
	}
	v, isNull, err := compareConstant(ctx, negOP(OP2), fc1, con2)
	if err != nil {
		logutil.Logger(context.Background()).Warn("comparing constant in ruleColumnOPConst", zap.Error(err))
		return
	}
	if !isNull && v > 0 {
		exprs.SetConstFalse()
	}
}

// opsiteOP the opsite direction of a compare operation, used in ruleColumnOPConst.
func opsiteOP(op1, op2 string) bool {
	switch {
	case op1 == ast.GE || op1 == ast.GT:
		return op2 == ast.LT || op2 == ast.LE
	case op1 == ast.LE || op1 == ast.LT:
		return op2 == ast.GT || op2 == ast.GE
	}
	return false
}

func negOP(cmp string) string {
	switch cmp {
	case ast.LT:
		return ast.GE
	case ast.LE:
		return ast.GT
	case ast.GT:
		return ast.LE
	case ast.GE:
		return ast.LT
	}
	return ""
}

// monotoneIncFuncs are those functions that for any x y, if x > y => f(x) > f(y)
var monotoneIncFuncs = map[string]struct{}{
	ast.ToDays:        {},
	ast.UnixTimestamp: {},
}

// compareConstant compares two expressions. c1 and c2 should be constant with the same type.
func compareConstant(ctx sessionctx.Context, fn string, c1, c2 Expression) (int64, bool, error) {
	cmp, err := NewFunction(ctx, fn, types.NewFieldType(mysql.TypeTiny), c1, c2)
	if err != nil {
		return 0, false, err
	}
	return cmp.EvalInt(ctx, chunk.Row{})
}

// NewPartitionPruneSolver returns a constraintSolver for partition pruning.
func NewPartitionPruneSolver() constraintSolver {
	return newConstraintSolver(ruleColumnOPConst)
}

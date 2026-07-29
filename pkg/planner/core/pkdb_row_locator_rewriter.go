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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/types"
)

func (er *expressionRewriter) tidbRowIDEqualToExpression(v *ast.BinaryOperationExpr) bool {
	leftIsRowID := isDirectTiDBRowID(v.L)
	rightIsRowID := isDirectTiDBRowID(v.R)
	if leftIsRowID == rightIsRowID {
		return false
	}

	stackLength := len(er.ctxStack)
	left := er.ctxStack[stackLength-2]
	right := er.ctxStack[stackLength-1]
	if expression.GetRowLen(left) != 1 || expression.GetRowLen(right) != 1 {
		return false
	}
	rowID, candidate := left, right
	if rightIsRowID {
		rowID, candidate = right, left
	}
	predicate, err := expression.NewTiDBRowIDPredicate(er.sctx, false, rowID, candidate)
	if err != nil {
		er.err = err
		return true
	}
	er.ctxStackPop(2)
	er.ctxStackAppend(predicate, types.EmptyName)
	return true
}

func (er *expressionRewriter) tidbRowIDComparisonToExpression(v *ast.BinaryOperationExpr) bool {
	if v.Op == opcode.EQ {
		return er.tidbRowIDEqualToExpression(v)
	}
	if v.Op != opcode.LT && v.Op != opcode.LE && v.Op != opcode.GT && v.Op != opcode.GE {
		return false
	}
	leftIsRowID := isDirectTiDBRowID(v.L)
	rightIsRowID := isDirectTiDBRowID(v.R)
	if leftIsRowID == rightIsRowID {
		return false
	}

	stackLength := len(er.ctxStack)
	left := er.ctxStack[stackLength-2]
	right := er.ctxStack[stackLength-1]
	if expression.GetRowLen(left) != 1 || expression.GetRowLen(right) != 1 {
		return false
	}
	rowID, bound := left, right
	op := v.Op
	if rightIsRowID {
		rowID, bound = right, left
		switch op {
		case opcode.LT:
			op = opcode.GT
		case opcode.LE:
			op = opcode.GE
		case opcode.GT:
			op = opcode.LT
		case opcode.GE:
			op = opcode.LE
		}
	}
	rangeOp := expression.TiDBRowIDRangeLT
	switch op {
	case opcode.LE:
		rangeOp = expression.TiDBRowIDRangeLE
	case opcode.GT:
		rangeOp = expression.TiDBRowIDRangeGT
	case opcode.GE:
		rangeOp = expression.TiDBRowIDRangeGE
	}
	predicate, err := expression.NewTiDBRowIDRangePredicate(er.sctx, rangeOp, false, rowID, bound)
	if err != nil {
		er.err = err
		return true
	}
	er.ctxStackPop(2)
	er.ctxStackAppend(predicate, types.EmptyName)
	return true
}

func (er *expressionRewriter) tidbRowIDBetweenToExpression(v *ast.BetweenExpr) bool {
	if !isDirectTiDBRowID(v.Expr) || isDirectTiDBRowID(v.Left) || isDirectTiDBRowID(v.Right) {
		return false
	}
	stackLength := len(er.ctxStack)
	args := er.ctxStack[stackLength-3:]
	for _, arg := range args {
		if expression.GetRowLen(arg) != 1 {
			return false
		}
	}
	predicate, err := expression.NewTiDBRowIDRangePredicate(
		er.sctx,
		expression.TiDBRowIDRangeBetween,
		v.Not,
		args[0],
		args[1],
		args[2],
	)
	if err != nil {
		er.err = err
		return true
	}
	er.ctxStackPop(3)
	er.ctxStackAppend(predicate, types.EmptyName)
	return true
}

func (er *expressionRewriter) tidbRowIDInToExpression(v *ast.PatternInExpr) bool {
	if !isDirectTiDBRowID(v.Expr) {
		return false
	}
	for _, item := range v.List {
		if isDirectTiDBRowID(item) {
			return false
		}
	}

	stackLength := len(er.ctxStack)
	args := er.ctxStack[stackLength-len(v.List)-1:]
	for _, arg := range args {
		if expression.GetRowLen(arg) != 1 {
			return false
		}
	}
	predicate, err := expression.NewTiDBRowIDPredicate(er.sctx, v.Not, args[0], args[1:]...)
	if err != nil {
		er.err = err
		return true
	}
	er.ctxStackPop(len(args))
	er.ctxStackAppend(predicate, types.EmptyName)
	return true
}

func isDirectTiDBRowID(expr ast.ExprNode) bool {
	for {
		parentheses, ok := expr.(*ast.ParenthesesExpr)
		if !ok {
			break
		}
		expr = parentheses.Expr
	}
	call, ok := expr.(*ast.FuncCallExpr)
	return ok && call.FnName.L == ast.TiDBRowID
}

func (er *expressionRewriter) collectTableSelectPrivs() {
	er.collectPrivsForCount()
}

// Copyright 2015 PingCAP, Inc.
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

package optimizer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
)

// Evaluator is a ast visitor that evaluates an expression.
type Evaluator struct {
	// columnMap is the map from ColumnName to the position of the rowStack.
	// It is used to find the value of the column.
	columnMap map[*ast.ColumnName]position
	// rowStack is the current row values while scanning.
	// It should be updated after scaned a new row.
	rowStack []*plan.Row

	// the map from AggregateFuncExpr to aggregator index.
	aggregateMap map[*ast.AggregateFuncExpr]int

	// aggregators for the current row
	// only outer query aggregate functions are handled.
	aggregators []Aggregator

	// when aggregation phase is done, the input is
	aggregateDone bool

	err error
}

type position struct {
	stackOffset  int
	fieldList    bool
	columnOffset int
}

// Enter implements ast.Visitor interface.
func (e *Evaluator) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return
}

// Leave implements ast.Visitor interface.
func (e *Evaluator) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch v := in.(type) {
	case *ast.ValueExpr:
		ok = true
	case *ast.BetweenExpr:
		ok = e.between(v)
	case *ast.BinaryOperationExpr:
		ok = e.binaryOperation(v)
	case *ast.WhenClause:
		ok = e.whenClause(v)
	case *ast.CaseExpr:
		ok = e.caseExpr(v)
	case *ast.SubqueryExpr:
		ok = e.subquery(v)
	case *ast.CompareSubqueryExpr:
		ok = e.compareSubquery(v)
	case *ast.ColumnNameExpr:
		ok = e.columnName(v)
	case *ast.DefaultExpr:
		ok = e.defaultExpr(v)
	case *ast.IdentifierExpr:
		ok = e.identifier(v)
	case *ast.ExistsSubqueryExpr:
		ok = e.existsSubquery(v)
	case *ast.PatternInExpr:
		ok = e.patternIn(v)
	case *ast.IsNullExpr:
		ok = e.isNull(v)
	case *ast.IsTruthExpr:
		ok = e.isTruth(v)
	case *ast.PatternLikeExpr:
		ok = e.patternLike(v)
	case *ast.ParamMarkerExpr:
		ok = e.paramMarker(v)
	case *ast.ParenthesesExpr:
		ok = e.parentheses(v)
	case *ast.PositionExpr:
		ok = e.position(v)
	case *ast.PatternRegexpExpr:
		ok = e.patternRegexp(v)
	case *ast.RowExpr:
		ok = e.row(v)
	case *ast.UnaryOperationExpr:
		ok = e.unaryOperation(v)
	case *ast.ValuesExpr:
		ok = e.values(v)
	case *ast.VariableExpr:
		ok = e.variable(v)
	case *ast.FuncCallExpr:
		ok = e.funcCall(v)
	case *ast.FuncExtractExpr:
		ok = e.funcExtract(v)
	case *ast.FuncConvertExpr:
		ok = e.funcConvert(v)
	case *ast.FuncCastExpr:
		ok = e.funcCast(v)
	case *ast.FuncSubstringExpr:
		ok = e.funcSubstring(v)
	case *ast.FuncLocateExpr:
		ok = e.funcLocate(v)
	case *ast.FuncTrimExpr:
		ok = e.funcTrim(v)
	case *ast.AggregateFuncExpr:
		ok = e.aggregateFunc(v)
	}
	out = in
	return
}

func checkAllOneColumn(exprs ...ast.ExprNode) bool {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *ast.RowExpr:
			return false
		case *ast.SubqueryExpr:
			if len(v.Query.GetResultFields()) != 1 {
				return false
			}
		}
	}
	return true
}

func (e *Evaluator) between(v *ast.BetweenExpr) bool {
	if !checkAllOneColumn(v.Expr, v.Left, v.Right) {
		e.err = errors.Errorf("Operand should contain 1 column(s)")
		return false
	}

	var l, r ast.ExprNode
	op := opcode.AndAnd

	if v.Not {
		// v < lv || v > rv
		op = opcode.OrOr
		l = &ast.BinaryOperationExpr{Op: opcode.LT, L: v.Expr, R: v.Left}
		r = &ast.BinaryOperationExpr{Op: opcode.GT, L: v.Expr, R: v.Right}
	} else {
		// v >= lv && v <= rv
		l = &ast.BinaryOperationExpr{Op: opcode.GE, L: v.Expr, R: v.Left}
		r = &ast.BinaryOperationExpr{Op: opcode.LE, L: v.Expr, R: v.Right}
	}

	ret := &ast.BinaryOperationExpr{Op: op, L: l, R: r}
	ret.Accept(e)
	return e.err == nil
}

func columnCount(e ast.ExprNode) (int, error) {
	switch x := e.(type) {
	case *ast.RowExpr:
		n := len(x.Values)
		if n <= 1 {
			return 0, errors.Errorf("Operand should contain >= 2 columns for Row")
		}
		return n, nil
	case *ast.SubqueryExpr:
		return len(x.Query.GetResultFields()), nil
	default:
		return 1, nil
	}
}

func hasSameColumnCount(e ast.ExprNode, args ...ast.ExprNode) error {
	l, err := columnCount(e)
	if err != nil {
		return errors.Trace(err)
	}
	var n int
	for _, arg := range args {
		n, err = columnCount(arg)
		if err != nil {
			return errors.Trace(err)
		}

		if n != l {
			return errors.Errorf("Operand should contain %d column(s)", l)
		}
	}
	return nil
}

func (e *Evaluator) whenClause(v *ast.WhenClause) bool {
	return true
}

func (e *Evaluator) caseExpr(v *ast.CaseExpr) bool {
	return true
}

func (e *Evaluator) subquery(v *ast.SubqueryExpr) bool {
	return true
}

func (e *Evaluator) compareSubquery(v *ast.CompareSubqueryExpr) bool {
	return true
}

func (e *Evaluator) columnName(v *ast.ColumnNameExpr) bool {
	return true
}

func (e *Evaluator) defaultExpr(v *ast.DefaultExpr) bool {
	return true
}

func (e *Evaluator) identifier(v *ast.IdentifierExpr) bool {
	return true
}

func (e *Evaluator) existsSubquery(v *ast.ExistsSubqueryExpr) bool {
	return true
}

func (e *Evaluator) patternIn(v *ast.PatternInExpr) bool {
	return true
}

func (e *Evaluator) isNull(v *ast.IsNullExpr) bool {
	return true
}

func (e *Evaluator) isTruth(v *ast.IsTruthExpr) bool {
	return true
}

func (e *Evaluator) patternLike(v *ast.PatternLikeExpr) bool {
	return true
}

func (e *Evaluator) paramMarker(v *ast.ParamMarkerExpr) bool {
	return true
}

func (e *Evaluator) parentheses(v *ast.ParenthesesExpr) bool {
	return true
}

func (e *Evaluator) position(v *ast.PositionExpr) bool {
	return true
}

func (e *Evaluator) patternRegexp(v *ast.PatternRegexpExpr) bool {
	return true
}

func (e *Evaluator) row(v *ast.RowExpr) bool {
	return true
}

func (e *Evaluator) unaryOperation(v *ast.UnaryOperationExpr) bool {
	return true
}

func (e *Evaluator) values(v *ast.ValuesExpr) bool {
	return true
}

func (e *Evaluator) variable(v *ast.VariableExpr) bool {
	return true
}

func (e *Evaluator) funcCall(v *ast.FuncCallExpr) bool {
	return true
}

func (e *Evaluator) funcExtract(v *ast.FuncExtractExpr) bool {
	return true
}

func (e *Evaluator) funcConvert(v *ast.FuncConvertExpr) bool {
	return true
}

func (e *Evaluator) funcCast(v *ast.FuncCastExpr) bool {
	return true
}

func (e *Evaluator) funcSubstring(v *ast.FuncSubstringExpr) bool {
	return true
}

func (e *Evaluator) funcLocate(v *ast.FuncLocateExpr) bool {
	return true
}

func (e *Evaluator) funcTrim(v *ast.FuncTrimExpr) bool {
	return true
}

func (e *Evaluator) aggregateFunc(v *ast.AggregateFuncExpr) bool {
	idx := e.aggregateMap[v]
	aggr := e.aggregators[idx]
	if e.aggregateDone {
		v.SetValue(aggr.Output())
		return true
	}
	// TODO: currently only single argument aggregate functions are supported.
	e.err = aggr.Input(v.Args[0].GetValue())
	return e.err == nil
}

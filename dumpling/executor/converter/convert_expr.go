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

package converter

import (
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/subquery"
	"github.com/pingcap/tidb/model"
)

func convertExpr(converter *expressionConverter, expr ast.ExprNode) (expression.Expression, error) {
	expr.Accept(converter)
	if converter.err != nil {
		return nil, errors.Trace(converter.err)
	}
	return converter.exprMap[expr], nil
}

// expressionConverter converts ast expression to
// old expression for transition state.
type expressionConverter struct {
	exprMap      map[ast.Node]expression.Expression
	paramMarkers paramMarkers
	err          error
}

func newExpressionConverter() *expressionConverter {
	return &expressionConverter{
		exprMap: map[ast.Node]expression.Expression{},
	}
}

// Enter implements ast.Visitor interface.
func (c *expressionConverter) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

// Leave implements ast.Visitor interface.
func (c *expressionConverter) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch v := in.(type) {
	case *ast.ValueExpr:
		c.value(v)
	case *ast.BetweenExpr:
		c.between(v)
	case *ast.BinaryOperationExpr:
		c.binaryOperation(v)
	case *ast.WhenClause:
		c.whenClause(v)
	case *ast.CaseExpr:
		c.caseExpr(v)
	case *ast.SubqueryExpr:
		c.subquery(v)
	case *ast.CompareSubqueryExpr:
		c.compareSubquery(v)
	case *ast.ColumnNameExpr:
		c.columnNameExpr(v)
	case *ast.DefaultExpr:
		c.defaultExpr(v)
	case *ast.ExistsSubqueryExpr:
		c.existsSubquery(v)
	case *ast.PatternInExpr:
		c.patternIn(v)
	case *ast.IsNullExpr:
		c.isNull(v)
	case *ast.IsTruthExpr:
		c.isTruth(v)
	case *ast.PatternLikeExpr:
		c.patternLike(v)
	case *ast.ParamMarkerExpr:
		c.paramMarker(v)
	case *ast.ParenthesesExpr:
		c.parentheses(v)
	case *ast.PositionExpr:
		c.position(v)
	case *ast.PatternRegexpExpr:
		c.patternRegexp(v)
	case *ast.RowExpr:
		c.row(v)
	case *ast.UnaryOperationExpr:
		c.unaryOperation(v)
	case *ast.ValuesExpr:
		c.values(v)
	case *ast.VariableExpr:
		c.variable(v)
	case *ast.FuncCallExpr:
		c.funcCall(v)
	case *ast.FuncExtractExpr:
		c.funcExtract(v)
	case *ast.FuncConvertExpr:
		c.funcConvert(v)
	case *ast.FuncCastExpr:
		c.funcCast(v)
	case *ast.FuncSubstringExpr:
		c.funcSubstring(v)
	case *ast.FuncSubstringIndexExpr:
		c.funcSubstringIndex(v)
	case *ast.FuncLocateExpr:
		c.funcLocate(v)
	case *ast.FuncTrimExpr:
		c.funcTrim(v)
	case *ast.FuncDateArithExpr:
		c.funcDateArith(v)
	case *ast.AggregateFuncExpr:
		c.aggregateFunc(v)
	case ast.ExprNode:
		log.Errorf("Unknown expr node %T", v)
	}
	return in, c.err == nil
}

func (c *expressionConverter) value(v *ast.ValueExpr) {
	c.exprMap[v] = expression.Value{Val: v.GetValue()}
}

func (c *expressionConverter) between(v *ast.BetweenExpr) {
	oldExpr := c.exprMap[v.Expr]
	oldLo := c.exprMap[v.Left]
	oldHi := c.exprMap[v.Right]
	oldBetween, err := expression.NewBetween(oldExpr, oldLo, oldHi, v.Not)
	if err != nil {
		c.err = err
		return
	}
	c.exprMap[v] = oldBetween
}

func (c *expressionConverter) binaryOperation(v *ast.BinaryOperationExpr) {
	oldeLeft := c.exprMap[v.L]
	oldRight := c.exprMap[v.R]
	oldBinop := expression.NewBinaryOperation(v.Op, oldeLeft, oldRight)
	c.exprMap[v] = oldBinop
}

func (c *expressionConverter) whenClause(v *ast.WhenClause) {
	oldExpr := c.exprMap[v.Expr]
	oldResult := c.exprMap[v.Result]
	oldWhenClause := &expression.WhenClause{Expr: oldExpr, Result: oldResult}
	c.exprMap[v] = oldWhenClause
}

func (c *expressionConverter) caseExpr(v *ast.CaseExpr) {
	oldValue := c.exprMap[v.Value]
	oldWhenClauses := make([]*expression.WhenClause, len(v.WhenClauses))
	for i, val := range v.WhenClauses {
		oldWhenClauses[i] = c.exprMap[val].(*expression.WhenClause)
	}
	oldElse := c.exprMap[v.ElseClause]
	oldCaseExpr := &expression.FunctionCase{
		Value:       oldValue,
		WhenClauses: oldWhenClauses,
		ElseClause:  oldElse,
	}
	c.exprMap[v] = oldCaseExpr
}

func (c *expressionConverter) subquery(v *ast.SubqueryExpr) {
	oldSubquery := &subquery.SubQuery{}
	switch x := v.Query.(type) {
	case *ast.SelectStmt:
		oldSelect, err := convertSelect(c, x)
		if err != nil {
			c.err = err
			return
		}
		oldSubquery.Stmt = oldSelect
	case *ast.UnionStmt:
		oldUnion, err := convertUnion(c, x)
		if err != nil {
			c.err = err
			return
		}
		oldSubquery.Stmt = oldUnion
	}
	c.exprMap[v] = oldSubquery
}

func (c *expressionConverter) compareSubquery(v *ast.CompareSubqueryExpr) {
	expr := c.exprMap[v.L]
	subquery := c.exprMap[v.R]
	oldCmpSubquery := expression.NewCompareSubQuery(v.Op, expr, subquery.(expression.SubQuery), v.All)
	c.exprMap[v] = oldCmpSubquery
}

func joinColumnName(columnName *ast.ColumnName) string {
	var originStrs []string
	if columnName.Schema.O != "" {
		originStrs = append(originStrs, columnName.Schema.O)
	}
	if columnName.Table.O != "" {
		originStrs = append(originStrs, columnName.Table.O)
	}
	originStrs = append(originStrs, columnName.Name.O)
	return strings.Join(originStrs, ".")
}

func (c *expressionConverter) columnNameExpr(v *ast.ColumnNameExpr) {
	ident := &expression.Ident{}
	ident.CIStr = model.NewCIStr(joinColumnName(v.Name))
	c.exprMap[v] = ident
}

func (c *expressionConverter) defaultExpr(v *ast.DefaultExpr) {
	oldDefault := &expression.Default{}
	if v.Name != nil {
		oldDefault.Name = joinColumnName(v.Name)
	}
	c.exprMap[v] = oldDefault
}

func (c *expressionConverter) existsSubquery(v *ast.ExistsSubqueryExpr) {
	subquery := c.exprMap[v.Sel].(expression.SubQuery)
	c.exprMap[v] = &expression.ExistsSubQuery{Sel: subquery}
}

func (c *expressionConverter) patternIn(v *ast.PatternInExpr) {
	oldPatternIn := &expression.PatternIn{Not: v.Not}
	if v.Sel != nil {
		oldPatternIn.Sel = c.exprMap[v.Sel].(expression.SubQuery)
	}
	oldPatternIn.Expr = c.exprMap[v.Expr]
	if v.List != nil {
		oldPatternIn.List = make([]expression.Expression, len(v.List))
		for i, v := range v.List {
			oldPatternIn.List[i] = c.exprMap[v]
		}
	}
	c.exprMap[v] = oldPatternIn
}

func (c *expressionConverter) isNull(v *ast.IsNullExpr) {
	oldIsNull := &expression.IsNull{Not: v.Not}
	oldIsNull.Expr = c.exprMap[v.Expr]
	c.exprMap[v] = oldIsNull
}

func (c *expressionConverter) isTruth(v *ast.IsTruthExpr) {
	oldIsTruth := &expression.IsTruth{Not: v.Not, True: v.True}
	oldIsTruth.Expr = c.exprMap[v.Expr]
	c.exprMap[v] = oldIsTruth
}

func (c *expressionConverter) patternLike(v *ast.PatternLikeExpr) {
	oldPatternLike := &expression.PatternLike{
		Not:     v.Not,
		Escape:  v.Escape,
		Expr:    c.exprMap[v.Expr],
		Pattern: c.exprMap[v.Pattern],
	}
	c.exprMap[v] = oldPatternLike
}

func (c *expressionConverter) paramMarker(v *ast.ParamMarkerExpr) {
	if c.exprMap[v] == nil {
		c.exprMap[v] = &expression.ParamMarker{}
		c.paramMarkers = append(c.paramMarkers, v)
	}
}

func (c *expressionConverter) parentheses(v *ast.ParenthesesExpr) {
	oldExpr := c.exprMap[v.Expr]
	c.exprMap[v] = &expression.PExpr{Expr: oldExpr}
}

func (c *expressionConverter) position(v *ast.PositionExpr) {
	c.exprMap[v] = expression.Value{Val: int64(v.N)}
}

func (c *expressionConverter) patternRegexp(v *ast.PatternRegexpExpr) {
	oldPatternRegexp := &expression.PatternRegexp{
		Not:     v.Not,
		Expr:    c.exprMap[v.Expr],
		Pattern: c.exprMap[v.Pattern],
	}
	c.exprMap[v] = oldPatternRegexp
}

func (c *expressionConverter) row(v *ast.RowExpr) {
	oldRow := &expression.Row{}
	oldRow.Values = make([]expression.Expression, len(v.Values))
	for i, val := range v.Values {
		oldRow.Values[i] = c.exprMap[val]
	}
	c.exprMap[v] = oldRow
}

func (c *expressionConverter) unaryOperation(v *ast.UnaryOperationExpr) {
	oldUnary := &expression.UnaryOperation{
		Op: v.Op,
		V:  c.exprMap[v.V],
	}
	c.exprMap[v] = oldUnary
}

func (c *expressionConverter) values(v *ast.ValuesExpr) {
	nameStr := joinColumnName(v.Column)
	c.exprMap[v] = &expression.Values{CIStr: model.NewCIStr(nameStr)}
}

func (c *expressionConverter) variable(v *ast.VariableExpr) {
	c.exprMap[v] = &expression.Variable{
		IsGlobal: v.IsGlobal,
		IsSystem: v.IsSystem,
		Name:     v.Name,
	}
}

func (c *expressionConverter) funcCall(v *ast.FuncCallExpr) {
	oldCall := &expression.Call{
		F: v.FnName.O,
	}
	oldCall.Args = make([]expression.Expression, len(v.Args))
	for i, val := range v.Args {
		oldCall.Args[i] = c.exprMap[val]
	}
	c.exprMap[v] = oldCall
}

func (c *expressionConverter) funcExtract(v *ast.FuncExtractExpr) {
	oldExtract := &expression.Extract{Unit: v.Unit}
	oldExtract.Date = c.exprMap[v.Date]
	c.exprMap[v] = oldExtract
}

func (c *expressionConverter) funcConvert(v *ast.FuncConvertExpr) {
	c.exprMap[v] = &expression.FunctionConvert{
		Expr:    c.exprMap[v.Expr],
		Charset: v.Charset,
	}
}

func (c *expressionConverter) funcCast(v *ast.FuncCastExpr) {
	oldCast := &expression.FunctionCast{
		Expr: c.exprMap[v.Expr],
		Tp:   v.Tp,
	}
	switch v.FunctionType {
	case ast.CastBinaryOperator:
		oldCast.FunctionType = expression.BinaryOperator
	case ast.CastConvertFunction:
		oldCast.FunctionType = expression.ConvertFunction
	case ast.CastFunction:
		oldCast.FunctionType = expression.CastFunction
	}
	c.exprMap[v] = oldCast
}

func (c *expressionConverter) funcSubstring(v *ast.FuncSubstringExpr) {
	oldSubstring := &expression.FunctionSubstring{
		Len:     c.exprMap[v.Len],
		Pos:     c.exprMap[v.Pos],
		StrExpr: c.exprMap[v.StrExpr],
	}
	c.exprMap[v] = oldSubstring
}

func (c *expressionConverter) funcSubstringIndex(v *ast.FuncSubstringIndexExpr) {
	oldSubstrIdx := &expression.FunctionSubstringIndex{
		Delim:   c.exprMap[v.Delim],
		Count:   c.exprMap[v.Count],
		StrExpr: c.exprMap[v.StrExpr],
	}
	c.exprMap[v] = oldSubstrIdx
}

func (c *expressionConverter) funcLocate(v *ast.FuncLocateExpr) {
	oldLocate := &expression.FunctionLocate{
		Pos:    c.exprMap[v.Pos],
		Str:    c.exprMap[v.Str],
		SubStr: c.exprMap[v.SubStr],
	}
	c.exprMap[v] = oldLocate
}

func (c *expressionConverter) funcTrim(v *ast.FuncTrimExpr) {
	oldTrim := &expression.FunctionTrim{
		Str:    c.exprMap[v.Str],
		RemStr: c.exprMap[v.RemStr],
	}
	switch v.Direction {
	case ast.TrimBoth:
		oldTrim.Direction = expression.TrimBoth
	case ast.TrimBothDefault:
		oldTrim.Direction = expression.TrimBothDefault
	case ast.TrimLeading:
		oldTrim.Direction = expression.TrimLeading
	case ast.TrimTrailing:
		oldTrim.Direction = expression.TrimTrailing
	}
	c.exprMap[v] = oldTrim
}

func (c *expressionConverter) funcDateArith(v *ast.FuncDateArithExpr) {
	oldDateArith := &expression.DateArith{
		Unit:     v.Unit,
		Date:     c.exprMap[v.Date],
		Interval: c.exprMap[v.Interval],
	}
	switch v.Op {
	case ast.DateAdd:
		oldDateArith.Op = expression.DateAdd
	case ast.DateSub:
		oldDateArith.Op = expression.DateSub
	}
	if v.Form == ast.DateArithDaysForm {
		oldDateArith.Form = expression.DateArithDaysForm
	}
	c.exprMap[v] = oldDateArith
}

func (c *expressionConverter) aggregateFunc(v *ast.AggregateFuncExpr) {
	oldAggregate := &expression.Call{
		F:        v.F,
		Distinct: v.Distinct,
	}
	for _, val := range v.Args {
		oldAggregate.Args = append(oldAggregate.Args, c.exprMap[val])
	}
	c.exprMap[v] = oldAggregate
}

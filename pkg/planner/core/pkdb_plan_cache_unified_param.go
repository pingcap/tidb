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
	"bytes"
	"errors"
	"reflect"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

type nonPreparedSelectionMode uint8

const (
	nonPreparedSelectionAllowed nonPreparedSelectionMode = iota
	nonPreparedSelectionPreserve
)

type nonPreparedPlanCacheAnalysis struct {
	sctx        base.PlanContext
	supported   bool
	reason      string
	restoreSafe bool

	mode             nonPreparedSelectionMode
	modeStack        []nonPreparedSelectionMode
	modeStackNodes   []ast.Node
	selected         map[*driver.ValueExpr]struct{}
	structured       map[ast.Node]struct{}
	boundaryStack    []ast.Node
	literalCount     int
	maxLiteral       int
	tooManyLiterals  bool
	hasCharset       bool
	hasLimit         bool
	hasSelectInto    bool
	hasMultiUpdate   bool
	hasMultiDelete   bool
	hasParamMarker   bool
	hasIgnoreHint    bool
	uncacheableAST   string
	duplicateLiteral bool
	rootSupported    bool
}

func newNonPreparedPlanCacheAnalysis(sctx base.PlanContext) *nonPreparedPlanCacheAnalysis {
	return &nonPreparedPlanCacheAnalysis{
		sctx:          sctx,
		supported:     true,
		restoreSafe:   true,
		mode:          nonPreparedSelectionAllowed,
		maxLiteral:    getMaxParamLimit(sctx),
		rootSupported: true,
	}
}

func (a *nonPreparedPlanCacheAnalysis) analyze(stmt ast.StmtNode) {
	switch stmt.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
	default:
		a.rootSupported = false
	}
	a.visit(stmt, nonPreparedSelectionAllowed)
	a.finalize()
}

func (a *nonPreparedPlanCacheAnalysis) visit(node ast.Node, requested nonPreparedSelectionMode) {
	if node == nil || reflect.ValueOf(node).IsNil() {
		return
	}
	old := a.mode
	if old == nonPreparedSelectionPreserve || requested == nonPreparedSelectionPreserve {
		a.mode = nonPreparedSelectionPreserve
	} else {
		a.mode = nonPreparedSelectionAllowed
	}
	node.Accept(a)
	a.mode = old
}

func (a *nonPreparedPlanCacheAnalysis) visitResultSet(node ast.ResultSetNode, requested nonPreparedSelectionMode) {
	if node == nil || reflect.ValueOf(node).IsNil() {
		return
	}
	switch node.(type) {
	case *ast.Join, *ast.TableSource, *ast.TableName, *ast.SelectStmt, *ast.SetOprStmt, *ast.SubqueryExpr:
		a.visit(node, requested)
	default:
		a.visit(node, nonPreparedSelectionPreserve)
	}
}

func (a *nonPreparedPlanCacheAnalysis) preserveUnknown(node ast.Node) {
	a.modeStack = append(a.modeStack, a.mode)
	a.modeStackNodes = append(a.modeStackNodes, node)
	a.mode = nonPreparedSelectionPreserve
}

func (a *nonPreparedPlanCacheAnalysis) collectMetadata(in ast.Node) {
	switch node := in.(type) {
	case *driver.ValueExpr:
		a.literalCount++
		if a.maxLiteral > 0 && a.literalCount > a.maxLiteral {
			a.tooManyLiterals = true
		}
		if node.GetType().GetFlag()&mysql.UnderScoreCharsetFlag != 0 {
			a.hasCharset = true
		}
	case *driver.ParamMarkerExpr:
		a.hasParamMarker = true
	case *ast.FuncCallExpr, *ast.VariableExpr:
		if a.uncacheableAST == "" {
			a.uncacheableAST = uncacheableRawASTReason(node)
		}
	case *ast.Limit:
		if !a.sctx.GetSessionVars().EnablePlanCacheForParamLimit {
			a.hasLimit = true
		}
	case *ast.SelectStmt:
		a.hasSelectInto = a.hasSelectInto || node.SelectIntoOpt != nil
		a.collectIgnoreHint(node.TableHints)
	case *ast.UpdateStmt:
		a.hasMultiUpdate = a.hasMultiUpdate || node.MultipleTable ||
			(node.TableRefs != nil && node.TableRefs.TableRefs != nil && node.TableRefs.TableRefs.Right != nil)
		a.collectIgnoreHint(node.TableHints)
	case *ast.DeleteStmt:
		a.hasMultiDelete = a.hasMultiDelete || node.IsMultiTable
		a.collectIgnoreHint(node.TableHints)
	case *ast.InsertStmt:
		a.collectIgnoreHint(node.TableHints)
	}
}

func (a *nonPreparedPlanCacheAnalysis) collectIgnoreHint(hints []*ast.TableOptimizerHint) {
	for _, hint := range hints {
		if hint != nil && hint.HintName.L == h.HintIgnorePlanCache {
			a.hasIgnoreHint = true
		}
	}
}

func (a *nonPreparedPlanCacheAnalysis) selectLiteral(node *driver.ValueExpr) {
	if a.mode != nonPreparedSelectionAllowed || node.IsNull() || node.Kind() == types.KindBinaryLiteral ||
		node.GetType().GetFlag()&mysql.UnderScoreCharsetFlag != 0 {
		return
	}
	if _, exists := a.selected[node]; exists {
		a.duplicateLiteral = true
		return
	}
	if a.selected == nil {
		a.selected = make(map[*driver.ValueExpr]struct{})
	}
	a.selected[node] = struct{}{}
	if len(a.boundaryStack) > 0 && a.structured == nil {
		a.structured = make(map[ast.Node]struct{}, len(a.boundaryStack))
	}
	for _, boundary := range a.boundaryStack {
		a.structured[boundary] = struct{}{}
	}
}

func (a *nonPreparedPlanCacheAnalysis) Enter(in ast.Node) (ast.Node, bool) {
	if stmt, ok := in.(*ast.SetOprStmt); ok {
		ownership := validateSetOprWithOwnership(stmt)
		if ownership.shared {
			a.restoreSafe = false
			a.reason = "unsupported_shared_setop_with_owner"
			return in, true
		}
		if ownership.unknownShape {
			a.restoreSafe = false
			if a.reason != "unsupported_shared_setop_with_owner" {
				a.reason = "unsupported_setop_restore_shape"
			}
		}
	}

	a.collectMetadata(in)
	switch node := in.(type) {
	case *driver.ValueExpr:
		a.selectLiteral(node)
		return in, true
	case *driver.ParamMarkerExpr:
		return in, true
	case *ast.SelectStmt:
		a.visitOptional(node.SelectIntoOpt, nonPreparedSelectionPreserve)
		a.visitOptional(node.With, a.mode)
		for _, hint := range node.TableHints {
			a.visitOptional(hint, nonPreparedSelectionPreserve)
		}
		a.visitOptional(node.Fields, nonPreparedSelectionPreserve)
		if node.From != nil {
			a.boundaryStack = append(a.boundaryStack, node.From)
			a.visit(node.From, a.mode)
			a.boundaryStack = a.boundaryStack[:len(a.boundaryStack)-1]
		}
		a.visitExpr(node.Where, a.mode)
		a.visitOptional(node.GroupBy, nonPreparedSelectionPreserve)
		a.visitOptional(node.Having, a.mode)
		for _, row := range node.Lists {
			a.visitOptional(row, nonPreparedSelectionPreserve)
		}
		for i := range node.WindowSpecs {
			a.visit(&node.WindowSpecs[i], nonPreparedSelectionPreserve)
		}
		a.visitOptional(node.OrderBy, nonPreparedSelectionPreserve)
		a.visitOptional(node.Limit, nonPreparedSelectionPreserve)
		if node.LockInfo != nil {
			for _, table := range node.LockInfo.Tables {
				a.visitOptional(table, nonPreparedSelectionPreserve)
			}
		}
		return in, true
	case *ast.SetOprStmt:
		childMode := a.mode
		if !a.restoreSafe {
			childMode = nonPreparedSelectionPreserve
		}
		a.visitOptional(node.With, childMode)
		a.visitOptional(node.SelectList, childMode)
		a.visitOptional(node.OrderBy, nonPreparedSelectionPreserve)
		a.visitOptional(node.Limit, nonPreparedSelectionPreserve)
		return in, true
	case *ast.SetOprSelectList:
		a.visitOptional(node.With, a.mode)
		for _, child := range node.Selects {
			switch child.(type) {
			case *ast.SelectStmt, *ast.SetOprSelectList:
				a.visitOptional(child, a.mode)
			default:
				a.visitOptional(child, nonPreparedSelectionPreserve)
			}
		}
		a.visitOptional(node.OrderBy, nonPreparedSelectionPreserve)
		a.visitOptional(node.Limit, nonPreparedSelectionPreserve)
		return in, true
	case *ast.InsertStmt:
		a.visitOptional(node.Table, a.mode)
		for _, column := range node.Columns {
			a.visitOptional(column, nonPreparedSelectionPreserve)
		}
		for _, hint := range node.TableHints {
			a.visitOptional(hint, nonPreparedSelectionPreserve)
		}
		for _, row := range node.Lists {
			for _, expr := range row {
				a.visitExpr(expr, a.mode)
			}
		}
		for _, assignment := range node.OnDuplicate {
			a.visitOptional(assignment, a.mode)
		}
		a.visitResultSet(node.Select, a.mode)
		return in, true
	case *ast.UpdateStmt:
		a.visitOptional(node.With, a.mode)
		a.visitOptional(node.TableRefs, a.mode)
		for _, assignment := range node.List {
			a.visitOptional(assignment, a.mode)
		}
		a.visitExpr(node.Where, a.mode)
		a.visitOptional(node.Order, nonPreparedSelectionPreserve)
		a.visitOptional(node.Limit, nonPreparedSelectionPreserve)
		for _, hint := range node.TableHints {
			a.visitOptional(hint, nonPreparedSelectionPreserve)
		}
		return in, true
	case *ast.DeleteStmt:
		a.visitOptional(node.With, a.mode)
		a.visitOptional(node.TableRefs, a.mode)
		a.visitOptional(node.Tables, nonPreparedSelectionPreserve)
		a.visitExpr(node.Where, a.mode)
		a.visitOptional(node.Order, nonPreparedSelectionPreserve)
		a.visitOptional(node.Limit, nonPreparedSelectionPreserve)
		for _, hint := range node.TableHints {
			a.visitOptional(hint, nonPreparedSelectionPreserve)
		}
		return in, true
	case *ast.WithClause:
		for _, cte := range node.CTEs {
			a.visitOptional(cte, a.mode)
		}
		return in, true
	case *ast.CommonTableExpression:
		a.visitOptional(node.Query, a.mode)
		return in, true
	case *ast.SubqueryExpr:
		a.visitResultSet(node.Query, a.mode)
		return in, true
	case *ast.TableRefsClause:
		a.visitResultSet(node.TableRefs, a.mode)
		return in, true
	case *ast.Join:
		a.visitResultSet(node.Left, a.mode)
		a.visitResultSet(node.Right, a.mode)
		a.visitOptional(node.On, a.mode)
		for _, column := range node.Using {
			a.visitOptional(column, nonPreparedSelectionPreserve)
		}
		return in, true
	case *ast.TableSource:
		a.visitResultSet(node.Source, a.mode)
		return in, true
	case *ast.TableName:
		a.visitOptional(node.TableSample, nonPreparedSelectionPreserve)
		a.visitOptional(node.TableSplit, nonPreparedSelectionPreserve)
		a.visitOptional(node.AsOf, nonPreparedSelectionPreserve)
		return in, true
	case *ast.OnCondition:
		a.visitExpr(node.Expr, a.mode)
		return in, true
	case *ast.HavingClause:
		a.visitExpr(node.Expr, a.mode)
		return in, true
	case *ast.Assignment:
		a.visitOptional(node.Column, nonPreparedSelectionPreserve)
		a.visitExpr(node.Expr, a.mode)
		return in, true
	case *ast.FuncCallExpr:
		return in, a.visitFunction(node)
	case *ast.AggregateFuncExpr:
		if node.F == ast.AggFuncApproxPercentile {
			if len(node.Args) > 0 {
				a.visitExpr(node.Args[0], a.mode)
			}
			for i := 1; i < len(node.Args); i++ {
				a.visitExpr(node.Args[i], nonPreparedSelectionPreserve)
			}
			return in, true
		}
		return in, false
	case *ast.FrameBound, *ast.SelectField, *ast.GroupByClause, *ast.OrderByClause,
		*ast.Limit, *ast.ByItem, *ast.ColumnName:
		if a.mode == nonPreparedSelectionAllowed {
			a.preserveUnknown(in)
		}
		return in, false
	case *ast.BetweenExpr, *ast.BinaryOperationExpr, *ast.CaseExpr,
		*ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr, *ast.IsNullExpr,
		*ast.IsTruthExpr, *ast.ParenthesesExpr, *ast.PatternInExpr,
		*ast.PatternLikeOrIlikeExpr, *ast.PatternRegexpExpr, *ast.PositionExpr,
		*ast.RowExpr, *ast.UnaryOperationExpr, *ast.ValuesExpr,
		*ast.VariableExpr, *ast.MatchAgainst, *ast.SetCollationExpr,
		*ast.TableNameExpr, *ast.ColumnNameExpr, *ast.DefaultExpr,
		*ast.MaxValueExpr, *ast.FuncCastExpr, *ast.TrimDirectionExpr,
		*ast.WindowFuncExpr, *ast.TimeUnitExpr, *ast.GetFormatSelectorExpr,
		*ast.WhenClause, *ast.WindowSpec, *ast.FrameClause:
		return in, false
	default:
		a.preserveUnknown(in)
		return in, false
	}
}

func (a *nonPreparedPlanCacheAnalysis) visitOptional(node ast.Node, mode nonPreparedSelectionMode) {
	if node != nil {
		a.visit(node, mode)
	}
}

func (a *nonPreparedPlanCacheAnalysis) visitExpr(node ast.ExprNode, mode nonPreparedSelectionMode) {
	if node != nil {
		a.visit(node, mode)
	}
}

func (a *nonPreparedPlanCacheAnalysis) visitFunction(node *ast.FuncCallExpr) bool {
	visitArg := func(index int, mode nonPreparedSelectionMode) {
		if index < len(node.Args) {
			a.visitExpr(node.Args[index], mode)
		}
	}
	switch node.FnName.L {
	case ast.DateFormat, ast.StrToDate, ast.TimeFormat, ast.WeightString:
		visitArg(0, a.mode)
		for i := 1; i < len(node.Args); i++ {
			visitArg(i, nonPreparedSelectionPreserve)
		}
	case ast.FromUnixTime:
		for i := range node.Args {
			visitArg(i, nonPreparedSelectionPreserve)
		}
	case ast.CharFunc:
		for i := range node.Args {
			mode := a.mode
			if i == len(node.Args)-1 {
				mode = nonPreparedSelectionPreserve
			}
			visitArg(i, mode)
		}
	case ast.Lpad, ast.Rpad:
		for i := range node.Args {
			mode := nonPreparedSelectionPreserve
			if i == 0 || i == 2 {
				mode = a.mode
			}
			visitArg(i, mode)
		}
	case ast.ConvertTz, ast.UnixTimestamp:
		for i := range node.Args {
			mode := a.mode
			if i == 0 {
				mode = nonPreparedSelectionPreserve
			}
			visitArg(i, mode)
		}
	case ast.Time, ast.CurrentTime, ast.Curtime, ast.LocalTime,
		ast.Now, ast.CurrentTimestamp, ast.LocalTimestamp, ast.UTCTime,
		ast.UTCTimestamp, ast.Sysdate, ast.TimeDiff, ast.Timestamp,
		ast.AddTime, ast.SubTime, ast.Round, ast.Truncate, ast.Rand:
		for i := range node.Args {
			visitArg(i, nonPreparedSelectionPreserve)
		}
	case ast.Benchmark:
		for i := range node.Args {
			mode := nonPreparedSelectionPreserve
			if i == 1 {
				mode = a.mode
			}
			visitArg(i, mode)
		}
	default:
		return false
	}
	return true
}

func (a *nonPreparedPlanCacheAnalysis) Leave(in ast.Node) (ast.Node, bool) {
	last := len(a.modeStackNodes) - 1
	if last >= 0 && a.modeStackNodes[last] == in {
		a.mode = a.modeStack[last]
		a.modeStack = a.modeStack[:last]
		a.modeStackNodes = a.modeStackNodes[:last]
	}
	return in, true
}

func (a *nonPreparedPlanCacheAnalysis) finalize() {
	if !a.restoreSafe {
		a.supported = false
		return
	}
	switch {
	case a.hasSelectInto:
		a.reason = "SELECT INTO is not supported"
	case a.hasMultiUpdate:
		a.reason = "multiple-table UPDATE is not supported"
	case a.hasMultiDelete:
		a.reason = "multiple-table DELETE is not supported"
	case a.hasParamMarker:
		a.reason = "query has parameter markers"
	case a.tooManyLiterals:
		a.reason = "query has too many constants"
	case a.hasCharset:
		a.reason = "query has values with under-score charset that cannot be preserved safely"
	case a.hasLimit:
		a.reason = "query has 'limit ?' is un-cacheable"
	case a.hasIgnoreHint:
		a.reason = "ignore plan cache by hint"
	case a.uncacheableAST != "":
		a.reason = a.uncacheableAST
	case a.duplicateLiteral:
		a.reason = "duplicate selected literal"
	case !a.rootSupported:
		a.reason = "not a SELECT/UPDATE/INSERT/DELETE/SET statement"
	}
	a.supported = a.reason == ""
}

type setOprWithOwnership struct {
	shared       bool
	unknownShape bool
}

func validateSetOprWithOwnership(stmt *ast.SetOprStmt) setOprWithOwnership {
	result := setOprWithOwnership{}
	var firstWith *ast.WithClause
	var firstOwner ast.Node
	var owners map[*ast.WithClause]ast.Node
	addOwner := func(owner ast.Node, with *ast.WithClause) {
		if with == nil || result.shared {
			return
		}
		if firstWith == nil {
			firstWith, firstOwner = with, owner
			return
		}
		if with == firstWith {
			result.shared = true
			return
		}
		if owners == nil {
			owners = map[*ast.WithClause]ast.Node{firstWith: firstOwner}
		}
		if _, exists := owners[with]; exists {
			result.shared = true
			return
		}
		owners[with] = owner
	}
	var walkList func(*ast.SetOprSelectList)
	walkList = func(list *ast.SetOprSelectList) {
		if list == nil {
			result.unknownShape = true
			return
		}
		addOwner(list, list.With)
		for _, child := range list.Selects {
			switch node := child.(type) {
			case *ast.SelectStmt:
				addOwner(node, node.With)
			case *ast.SetOprSelectList:
				walkList(node)
			default:
				result.unknownShape = true
			}
		}
	}
	addOwner(stmt, stmt.With)
	walkList(stmt.SelectList)
	return result
}

type nonPreparedRestoreInterceptor struct {
	selected    map[*driver.ValueExpr]struct{}
	structured  map[ast.Node]struct{}
	handled     map[*driver.ValueExpr]struct{}
	paramValues []types.Datum
}

func (i *nonPreparedRestoreInterceptor) RestoreLiteral(ctx *format.RestoreCtx, literal any) (bool, error) {
	value, ok := literal.(*driver.ValueExpr)
	if !ok {
		return false, nil
	}
	if _, selected := i.selected[value]; !selected {
		return false, nil
	}
	if _, duplicated := i.handled[value]; duplicated {
		return false, errors.New("selected literal restored more than once")
	}
	if i.handled == nil {
		i.handled = make(map[*driver.ValueExpr]struct{})
	}
	i.handled[value] = struct{}{}
	var datum types.Datum
	value.Datum.Copy(&datum)
	ctx.WritePlain("?")
	i.paramValues = append(i.paramValues, datum)
	return true, nil
}

func (i *nonPreparedRestoreInterceptor) AllowOriginalText(node any) bool {
	astNode, ok := node.(ast.Node)
	if !ok {
		return true
	}
	_, forceStructured := i.structured[astNode]
	return !forceStructured
}

func parameterizeForNonPreparedPlanCache(
	sctx base.PlanContext,
	stmt ast.StmtNode,
) (result NonPreparedPlanCacheParamResult, supported bool, reason string) {
	analysis := newNonPreparedPlanCacheAnalysis(sctx)
	analysis.analyze(stmt)
	if !analysis.supported || !analysis.restoreSafe {
		return result, false, analysis.reason
	}
	interceptor := &nonPreparedRestoreInterceptor{
		selected:   analysis.selected,
		structured: analysis.structured,
	}
	pCtx := paramCtxPool.Get().(*format.RestoreCtx)
	pCtx.SetRestoreInterceptor(interceptor)
	defer func() {
		pCtx.SetRestoreInterceptor(nil)
		pCtx.In.(*bytes.Buffer).Reset()
		paramCtxPool.Put(pCtx)
	}()
	if err := stmt.Restore(pCtx); err != nil {
		return result, false, "failed to restore parameterized SQL"
	}
	if len(interceptor.handled) != len(analysis.selected) || len(interceptor.paramValues) != len(analysis.selected) {
		return result, false, "parameterized literal invariant failed"
	}
	for literal := range analysis.selected {
		if _, ok := interceptor.handled[literal]; !ok {
			return result, false, "parameterized literal invariant failed"
		}
	}
	return NonPreparedPlanCacheParamResult{
		ParamSQL:    pCtx.In.(*bytes.Buffer).String(),
		ParamValues: interceptor.paramValues,
	}, true, ""
}

type nonPreparedParamMarkerCounter struct {
	count int
}

func (c *nonPreparedParamMarkerCounter) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*driver.ParamMarkerExpr); ok {
		c.count++
		return in, true
	}
	return in, false
}

func (*nonPreparedParamMarkerCounter) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// ValidateNonPreparedParamMarkerCount verifies the parsed marker count without
// assigning marker order or mutating marker Datum values.
func ValidateNonPreparedParamMarkerCount(stmt ast.StmtNode, expected int) bool {
	counter := nonPreparedParamMarkerCounter{}
	stmt.Accept(&counter)
	return counter.count == expected
}

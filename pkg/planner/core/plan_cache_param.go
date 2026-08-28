// Copyright 2022 PingCAP, Inc.
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
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	parserutil "github.com/pingcap/tidb/pkg/util/parser"
)

var (
	paramReplacerPool = sync.Pool{New: func() any {
		pr := new(paramReplacer)
		pr.Reset()
		return pr
	}}
	paramRestorerPool = sync.Pool{New: func() any {
		pr := new(paramRestorer)
		pr.Reset()
		return pr
	}}
	paramCtxPool = sync.Pool{New: func() any {
		buf := new(bytes.Buffer)
		restoreCtx := format.NewRestoreCtx(format.RestoreForNonPrepPlanCache|format.RestoreStringWithoutCharset|format.RestoreStringSingleQuotes|format.RestoreNameBackQuotes, buf)
		return restoreCtx
	}}
	paramMakerPool = sync.Pool{New: func() any {
		return ast.NewParamMarkerExpr(0)
	}}
)

// paramReplacer is an ast.Visitor that replaces all values with `?` and collects them.
type paramReplacer struct {
	params       []*driver.ValueExpr
	parameterize map[*driver.ValueExpr]struct{}
}

func (pr *paramReplacer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	case *ast.SelectField, *ast.GroupByClause, *ast.Limit, *ast.OrderByClause:
		// Skip replacing values in these case:
		// 1. SelectField: to keep the output field names be corresponding to these values.
		// 2. GroupByClause, OrderByClause: to avoid breaking the full_group_by check.
		// 3. Limit: to generate different plans for queries with different limit values.
		return in, true
	case *ast.FuncCallExpr:
		switch n.FnName.L {
		case ast.DateFormat, ast.StrToDate, ast.TimeFormat, ast.FromUnixTime:
			// skip the second format argument: date_format('2020', '%Y') --> date_format(?, '%Y')
			if len(n.Args) == 0 {
				// A malformed zero-arg call (wrong arity, rejected later during
				// type checking); don't index Args here.
				return in, true
			}
			ret, _ := n.Args[0].Accept(pr)
			n.Args[0] = ret.(ast.ExprNode)
			return in, true
		default:
			return in, false
		}
	case *driver.ValueExpr:
		if pr.parameterize != nil {
			if _, ok := pr.parameterize[n]; !ok {
				return in, true
			}
		}
		pr.params = append(pr.params, n)
		param := paramMakerPool.Get().(*driver.ParamMarkerExpr)
		param.Offset = len(pr.params) - 1 // offset is used as order in non-prepared plan cache.
		n.Datum.Copy(&param.Datum)        // init the ParamMakerExpr's Datum
		return param, true
	}
	return in, false
}

func (*paramReplacer) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (pr *paramReplacer) Reset() {
	pr.params = make([]*driver.ValueExpr, 0, 4)
	pr.parameterize = nil
}

// GetParamSQLFromAST returns the parameterized SQL of this AST.
// NOTICE: this function does not modify the original AST.
// paramVals are copied from this AST.
func GetParamSQLFromAST(stmt ast.StmtNode) (paramSQL string, paramVals []types.Datum, err error) {
	paramSQL, paramVals, paramSQLErr, restoreErr := parameterizeAndRestoreAST(stmt, nil)
	if err := errors.Join(paramSQLErr, restoreErr); err != nil {
		return "", nil, err
	}
	return paramSQL, paramVals, nil
}

// ParameterizeAST parameterizes this StmtNode.
// e.g. `select * from t where a<10 and b<23` --> `select * from t where a<? and b<?`, [10, 23].
// NOTICE: this function may modify the input stmt.
func ParameterizeAST(stmt ast.StmtNode) (paramSQL string, params []*driver.ValueExpr, err error) {
	return parameterizeAST(stmt, nil)
}

func parameterizeAST(stmt ast.StmtNode, selected map[*driver.ValueExpr]struct{}) (paramSQL string, params []*driver.ValueExpr, err error) {
	pr := paramReplacerPool.Get().(*paramReplacer)
	pCtx := paramCtxPool.Get().(*format.RestoreCtx)
	defer func() {
		pr.Reset()
		paramReplacerPool.Put(pr)
		pCtx.In.(*bytes.Buffer).Reset()
		paramCtxPool.Put(pCtx)
	}()
	pr.parameterize = selected
	stmt.Accept(pr)
	params = append(params, pr.params...)
	if err := stmt.Restore(pCtx); err != nil {
		return "", params, err
	}
	paramSQL = pCtx.In.(*bytes.Buffer).String()
	return
}

func parameterizeAndRestoreAST(stmt ast.StmtNode, selected map[*driver.ValueExpr]struct{}) (
	paramSQL string,
	paramVals []types.Datum,
	paramSQLErr error,
	restoreErr error,
) {
	var params []*driver.ValueExpr
	paramSQL, params, paramSQLErr = parameterizeAST(stmt, selected)
	paramVals = make([]types.Datum, len(params))
	for i, param := range params {
		param.Datum.Copy(&paramVals[i])
	}
	restoreErr = RestoreASTWithParams(stmt, params)
	return
}

type paramRestorer struct {
	params []*driver.ValueExpr
	err    error
}

type paramMarkerValidator struct {
	paramCount int
	err        error
}

func (v *paramMarkerValidator) Enter(in ast.Node) (ast.Node, bool) {
	if marker, ok := in.(*driver.ParamMarkerExpr); ok && (marker.Offset < 0 || marker.Offset >= v.paramCount) {
		v.err = errors.New("failed to restore ast.Node")
		return in, true
	}
	return in, v.err != nil
}

func (v *paramMarkerValidator) Leave(in ast.Node) (ast.Node, bool) {
	return in, v.err == nil
}

func (pr *paramRestorer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if n, ok := in.(*driver.ParamMarkerExpr); ok {
		if n.Offset >= len(pr.params) {
			pr.err = errors.New("failed to restore ast.Node")
			return nil, true
		}
		// offset is used as order in non-prepared plan cache.
		offset := n.Offset
		paramMakerPool.Put(n)
		return pr.params[offset], true
	}
	if pr.err != nil {
		return nil, true
	}
	return in, false
}

func (*paramRestorer) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (pr *paramRestorer) Reset() {
	pr.params, pr.err = nil, nil
}

// RestoreASTWithParams restore this parameterized AST with specific parameters.
// e.g. `select * from t where a<? and b<?`, [10, 23] --> `select * from t where a<10 and b<23`.
func RestoreASTWithParams(stmt ast.StmtNode, params []*driver.ValueExpr) error {
	validator := paramMarkerValidator{paramCount: len(params)}
	stmt.Accept(&validator)
	if validator.err != nil {
		return validator.err
	}

	pr := paramRestorerPool.Get().(*paramRestorer)
	defer func() {
		pr.Reset()
		paramRestorerPool.Put(pr)
	}()
	pr.params = params
	stmt.Accept(pr)
	failpoint.Inject("mockNonPreparedPlanCacheASTRestoreError", func(val failpoint.Value) {
		// Keep the AST fully restored, but let tests verify that callers propagate
		// an AST restore error instead of falling back to ordinary optimization.
		_ = val
		pr.err = errors.New("failed to restore ast.Node")
	})
	return pr.err
}

// NonPreparedPlanCacheParamResult is the result of parameterizing a normal SQL
// statement for the non-prepared plan cache.
type NonPreparedPlanCacheParamResult struct {
	ParamSQL    string
	ParamValues []types.Datum
}

// ParameterizeForNonPreparedPlanCache validates and parameterizes stmt for the
// non-prepared plan cache. supported=false means the statement should use normal
// planning instead. An error means the original AST could not be restored safely.
func ParameterizeForNonPreparedPlanCache(
	sctx base.PlanContext,
	stmt ast.StmtNode,
) (result NonPreparedPlanCacheParamResult, supported bool, reason string, err error) {
	checker := nonPreparedPlanCachePrechecker{
		sctx:       sctx,
		supported:  true,
		maxLiteral: getMaxParamLimit(sctx),
	}
	stmt.Accept(&checker)
	checker.finalize()
	if !checker.supported {
		return result, false, checker.reason, nil
	}

	selector := nonPreparedPlanCacheParamSelector{
		selected: make(map[*driver.ValueExpr]struct{}),
	}
	if !selector.selectStatement(stmt) {
		return result, false, "not a SELECT/UPDATE/INSERT/DELETE/SET statement", nil
	}

	paramSQL, paramValues, paramSQLErr, restoreErr := parameterizeAndRestoreAST(stmt, selector.selected)
	if restoreErr != nil {
		return result, false, "", errors.Join(paramSQLErr, restoreErr)
	}
	if paramSQLErr != nil {
		return result, false, "failed to restore parameterized SQL", nil
	}
	return NonPreparedPlanCacheParamResult{
		ParamSQL:    paramSQL,
		ParamValues: paramValues,
	}, true, "", nil
}

type nonPreparedPlanCachePrechecker struct {
	sctx                 base.PlanContext
	supported            bool
	reason               string
	literalCount         int
	maxLiteral           int
	tooManyLiterals      bool
	hasCharsetIntroducer bool
	hasLimit             bool
	hasSelectInto        bool
	hasMultiTableUpdate  bool
	hasMultiTableDelete  bool
	hasParamMarker       bool
}

func (c *nonPreparedPlanCachePrechecker) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *driver.ValueExpr:
		c.literalCount++
		if c.maxLiteral > 0 && c.literalCount > c.maxLiteral {
			c.tooManyLiterals = true
		}
		if node.GetType().GetFlag()&mysql.UnderScoreCharsetFlag != 0 {
			c.hasCharsetIntroducer = true
		}
	case *driver.ParamMarkerExpr:
		// A marker in the original non-prepared statement is not one of the
		// markers generated by this parameterization pass. Keeping it in the
		// AST would make RestoreASTWithParams unable to distinguish the two
		// kinds of markers safely, so bypass the unified path for the whole
		// statement.
		c.hasParamMarker = true
	case *ast.Limit:
		if !c.sctx.GetSessionVars().EnablePlanCacheForParamLimit {
			c.hasLimit = true
		}
	case *ast.SelectStmt:
		if node.SelectIntoOpt != nil {
			c.hasSelectInto = true
		}
	case *ast.UpdateStmt:
		if node.MultipleTable || (node.TableRefs != nil && node.TableRefs.TableRefs != nil && node.TableRefs.TableRefs.Right != nil) {
			c.hasMultiTableUpdate = true
		}
	case *ast.DeleteStmt:
		if node.IsMultiTable {
			c.hasMultiTableDelete = true
		}
	}
	return in, false
}

func (c *nonPreparedPlanCachePrechecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (c *nonPreparedPlanCachePrechecker) finalize() {
	// Keep statement-level gates ahead of literal-level gates. The order is
	// explicit so the reported bypass reason does not depend on AST traversal.
	switch {
	case c.hasSelectInto:
		c.reason = "SELECT INTO is not supported"
	case c.hasMultiTableUpdate:
		c.reason = "multiple-table UPDATE is not supported"
	case c.hasMultiTableDelete:
		c.reason = "multiple-table DELETE is not supported"
	case c.hasParamMarker:
		c.reason = "query has parameter markers"
	case c.tooManyLiterals:
		c.reason = "query has too many constants"
	case c.hasCharsetIntroducer:
		c.reason = "query has values with under-score charset that cannot be preserved safely"
	case c.hasLimit:
		c.reason = "query has 'limit ?' is un-cacheable"
	}
	c.supported = c.reason == ""
}

type nonPreparedPlanCacheParamSelector struct {
	selected map[*driver.ValueExpr]struct{}
}

func (s *nonPreparedPlanCacheParamSelector) selectStatement(stmt ast.StmtNode) bool {
	switch node := stmt.(type) {
	case *ast.SelectStmt:
		s.selectSelect(node)
	case *ast.SetOprStmt:
		s.selectSetOperation(node)
	case *ast.InsertStmt:
		s.selectResultSet(node.Table.TableRefs)
		for _, row := range node.Lists {
			for _, expr := range row {
				s.selectExpression(expr)
			}
		}
		for _, assignment := range node.OnDuplicate {
			s.selectExpression(assignment.Expr)
		}
		if node.Select != nil {
			s.selectResultSet(node.Select)
		}
	case *ast.UpdateStmt:
		s.selectWith(node.With)
		s.selectResultSet(node.TableRefs.TableRefs)
		for _, assignment := range node.List {
			s.selectExpression(assignment.Expr)
		}
		s.selectExpression(node.Where)
	case *ast.DeleteStmt:
		s.selectWith(node.With)
		s.selectResultSet(node.TableRefs.TableRefs)
		s.selectExpression(node.Where)
	default:
		return false
	}
	return true
}

func (s *nonPreparedPlanCacheParamSelector) selectSelect(stmt *ast.SelectStmt) {
	s.selectWith(stmt.With)
	if stmt.From != nil {
		s.selectResultSet(stmt.From.TableRefs)
	}
	s.selectExpression(stmt.Where)
	if stmt.Having != nil {
		s.selectExpression(stmt.Having.Expr)
	}
}

func (s *nonPreparedPlanCacheParamSelector) selectSetOperation(stmt *ast.SetOprStmt) {
	s.selectWith(stmt.With)
	s.selectSetOperationList(stmt.SelectList)
}

func (s *nonPreparedPlanCacheParamSelector) selectSetOperationList(list *ast.SetOprSelectList) {
	if list == nil {
		return
	}
	s.selectWith(list.With)
	for _, node := range list.Selects {
		switch selectNode := node.(type) {
		case *ast.SelectStmt:
			s.selectSelect(selectNode)
		case *ast.SetOprSelectList:
			s.selectSetOperationList(selectNode)
		}
	}
}

func (s *nonPreparedPlanCacheParamSelector) selectWith(with *ast.WithClause) {
	if with == nil {
		return
	}
	for _, cte := range with.CTEs {
		if cte != nil && cte.Query != nil {
			s.selectResultSet(cte.Query.Query)
		}
	}
}

func (s *nonPreparedPlanCacheParamSelector) selectResultSet(resultSet ast.ResultSetNode) {
	switch node := resultSet.(type) {
	case *ast.Join:
		s.selectResultSet(node.Left)
		s.selectResultSet(node.Right)
		if node.On != nil {
			s.selectExpression(node.On.Expr)
		}
	case *ast.TableSource:
		s.selectResultSet(node.Source)
	case *ast.SelectStmt:
		s.selectSelect(node)
	case *ast.SetOprStmt:
		s.selectSetOperation(node)
	case *ast.SubqueryExpr:
		s.selectResultSet(node.Query)
	}
}

func (s *nonPreparedPlanCacheParamSelector) selectExpression(expr ast.ExprNode) {
	if expr == nil {
		return
	}
	expr.Accept(&nonPreparedPlanCacheExprSelector{selector: s})
}

type nonPreparedPlanCacheExprSelector struct {
	selector *nonPreparedPlanCacheParamSelector
}

func (s *nonPreparedPlanCacheExprSelector) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *driver.ValueExpr:
		if node.IsNull() || node.Kind() == types.KindBinaryLiteral || node.GetType().GetFlag()&mysql.UnderScoreCharsetFlag != 0 {
			return in, true
		}
		s.selector.selected[node] = struct{}{}
		return in, true
	case *ast.SubqueryExpr:
		s.selector.selectResultSet(node.Query)
		return in, true
	case *ast.FuncCallExpr:
		switch node.FnName.L {
		case ast.DateFormat, ast.StrToDate, ast.TimeFormat:
			if len(node.Args) > 0 {
				s.selector.selectExpression(node.Args[0])
			}
			return in, true
		case ast.WeightString:
			// WEIGHT_STRING's AS CHAR/BINARY and length arguments are syntax
			// parameters, not expressions. Preserve them so the restored
			// parameterized SQL remains valid and the built-in keeps its padding
			// mode and length.
			if len(node.Args) > 0 {
				s.selector.selectExpression(node.Args[0])
			}
			return in, true
		case ast.FromUnixTime:
			// FROM_UNIXTIME derives the one-argument result FSP from the first
			// timestamp argument while building the function signature. Preserve
			// all arguments so a different timestamp precision gets a new carrier.
			return in, true
		case ast.CharFunc:
			// CHAR's last argument is the charset selected by USING. It is
			// evaluated while building the function signature, so it must stay
			// a constant in the parameterized AST.
			for i, arg := range node.Args {
				if i == len(node.Args)-1 {
					continue
				}
				s.selector.selectExpression(arg)
			}
			return in, true
		case ast.Lpad, ast.Rpad:
			// LPAD/RPAD derive the result Flen from the length argument at
			// build time. Preserve that argument while parameterizing the
			// string and padding expressions.
			if len(node.Args) > 0 {
				s.selector.selectExpression(node.Args[0])
			}
			if len(node.Args) > 2 {
				s.selector.selectExpression(node.Args[2])
			}
			return in, true
		case ast.ConvertTz, ast.UnixTimestamp:
			// These functions derive their result precision from the first
			// argument's value/type while the plan is built. Preserve that
			// argument so a later execution cannot reuse a signature with a
			// different temporal precision.
			for i, arg := range node.Args {
				if i == 0 {
					continue
				}
				s.selector.selectExpression(arg)
			}
			return in, true
		case ast.Time, ast.CurrentTime, ast.Curtime, ast.LocalTime,
			ast.Now, ast.CurrentTimestamp, ast.LocalTimestamp, ast.UTCTime,
			ast.UTCTimestamp, ast.Sysdate:
			// These functions derive their result FSP from a constant argument
			// while building the function signature. Keep the argument in the
			// statement identity instead of replacing it with a marker.
			return in, true
		case ast.TimeDiff, ast.Timestamp:
			// Both arguments contribute to the temporal result type (FSP, and for
			// TIMESTAMP the first argument's numeric-vs-string type).
			return in, true
		case ast.AddTime, ast.SubTime:
			// ADDTIME and SUBTIME derive the result FSP and Flen from both
			// arguments while building the function signature.
			return in, true
		case ast.Round, ast.Truncate:
			// The value type and the requested scale are both used to construct
			// the result type. Preserve the whole argument list so neither can be
			// changed while reusing a cached signature.
			return in, true
		case ast.Rand:
			// A constant RAND seed initializes the RNG when the signature is
			// built. Preserve it so a different seed cannot reuse that state.
			return in, true
		case ast.Benchmark:
			// BENCHMARK caches a positive constant loop count in its signature.
			// Preserve that argument, but still select literals in the expression
			// being benchmarked using the regular expression rules.
			if len(node.Args) > 1 {
				s.selector.selectExpression(node.Args[1])
			}
			return in, true
		default:
			return in, false
		}
	case *ast.AggregateFuncExpr:
		if node.F == ast.AggFuncApproxPercentile {
			// The percentage argument must remain a constant because the
			// aggregate descriptor evaluates it while the plan is built.
			// Parameterize only the data expression and preserve the percentage.
			if len(node.Args) > 0 {
				s.selector.selectExpression(node.Args[0])
			}
			return in, true
		}
	case *ast.FrameBound:
		return in, true
	case *ast.WhenClause, *ast.WindowSpec, *ast.FrameClause:
		// These are structural nodes whose expression children are safe to
		// inspect. FrameBound itself is handled above because frame literals
		// must remain preserved.
		return in, false
	case *ast.SelectField, *ast.GroupByClause, *ast.OrderByClause, *ast.Limit,
		*ast.ByItem, *ast.ColumnName, *ast.TableName:
		// Projection, grouping, ordering, limits, and identifier nodes are
		// preserve-first contexts. Do not descend into a future child added to
		// one of these nodes accidentally.
		return in, true
	case *ast.BetweenExpr, *ast.BinaryOperationExpr, *ast.CaseExpr,
		*ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr, *ast.IsNullExpr,
		*ast.IsTruthExpr, *ast.ParenthesesExpr, *ast.PatternInExpr,
		*ast.PatternLikeOrIlikeExpr, *ast.PatternRegexpExpr, *ast.PositionExpr,
		*ast.RowExpr, *ast.UnaryOperationExpr, *ast.ValuesExpr,
		*ast.VariableExpr, *ast.MatchAgainst, *ast.SetCollationExpr,
		*ast.TableNameExpr, *ast.ColumnNameExpr, *ast.DefaultExpr,
		*ast.MaxValueExpr, *ast.FuncCastExpr, *ast.TrimDirectionExpr,
		*ast.WindowFuncExpr, *ast.TimeUnitExpr,
		*ast.GetFormatSelectorExpr:
		return in, false
	}
	// Unknown expression and structural nodes default to preserve. This is a
	// forward-compatibility fence: a new AST node must opt into parameterization
	// explicitly after its literal semantics are reviewed.
	return in, true
}

func (*nonPreparedPlanCacheExprSelector) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// Params2Expressions converts these parameters to an expression list.
func Params2Expressions(params []types.Datum) []expression.Expression {
	exprs := make([]expression.Expression, 0, len(params))
	for _, p := range params {
		// TODO: add a sync.Pool for type.FieldType and expression.Constant here.
		tp := new(types.FieldType)
		types.InferParamTypeFromDatum(&p, tp)
		exprs = append(exprs, &expression.Constant{
			Value:   p,
			RetType: tp,
		})
	}
	return exprs
}

// ParseParameterizedSQL parse this parameterized SQL with the specified sctx.
func ParseParameterizedSQL(sctx sessionctx.Context, paramSQL string) (ast.StmtNode, error) {
	p := parserutil.GetParser()
	defer func() {
		parserutil.DestroyParser(p)
	}()
	p.SetSQLMode(sctx.GetSessionVars().SQLMode)
	p.SetParserConfig(sctx.GetSessionVars().BuildParserConfig())
	tmp, _, err := p.ParseSQL(paramSQL, sctx.GetSessionVars().GetParseParams()...)
	if err != nil {
		return nil, err
	}
	if len(tmp) != 1 {
		return nil, errors.New("unexpected multiple statements")
	}
	return tmp[0], nil
}

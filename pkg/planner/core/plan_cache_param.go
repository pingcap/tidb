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
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
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
	params []*driver.ValueExpr
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
				return in, true
			}
			ret, _ := n.Args[0].Accept(pr)
			n.Args[0] = ret.(ast.ExprNode)
			return in, true
		default:
			return in, false
		}
	case *driver.ValueExpr:
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
}

// GetParamSQLFromAST returns the parameterized SQL of this AST.
// NOTICE: this function does not modify the original AST.
// paramVals are copied from this AST.
func GetParamSQLFromAST(stmt ast.StmtNode) (paramSQL string, paramVals []types.Datum, err error) {
	paramSQL, paramVals, paramSQLErr, restoreErr := parameterizeAndRestoreAST(stmt)
	if err := errors.Join(paramSQLErr, restoreErr); err != nil {
		return "", nil, err
	}
	return paramSQL, paramVals, nil
}

// ParameterizeAST parameterizes this StmtNode.
// e.g. `select * from t where a<10 and b<23` --> `select * from t where a<? and b<?`, [10, 23].
// NOTICE: this function may modify the input stmt.
func ParameterizeAST(stmt ast.StmtNode) (paramSQL string, params []*driver.ValueExpr, err error) {
	return parameterizeAST(stmt)
}

func parameterizeAST(stmt ast.StmtNode) (paramSQL string, params []*driver.ValueExpr, err error) {
	pr := paramReplacerPool.Get().(*paramReplacer)
	pCtx := paramCtxPool.Get().(*format.RestoreCtx)
	defer func() {
		pr.Reset()
		paramReplacerPool.Put(pr)
		pCtx.In.(*bytes.Buffer).Reset()
		paramCtxPool.Put(pCtx)
	}()
	stmt.Accept(pr)
	params = append(params, pr.params...)
	if err := stmt.Restore(pCtx); err != nil {
		return "", params, err
	}
	paramSQL = pCtx.In.(*bytes.Buffer).String()
	return
}

func parameterizeAndRestoreAST(stmt ast.StmtNode) (
	paramSQL string,
	paramVals []types.Datum,
	paramSQLErr error,
	restoreErr error,
) {
	var params []*driver.ValueExpr
	paramSQL, params, paramSQLErr = parameterizeAST(stmt)
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
// planning instead. It never mutates the input AST, so Restore and invariant
// failures can safely return supported=false and fall back to normal planning.
func ParameterizeForNonPreparedPlanCache(
	sctx base.PlanContext,
	stmt ast.StmtNode,
) (result NonPreparedPlanCacheParamResult, supported bool, reason string) {
	return parameterizeForNonPreparedPlanCache(sctx, stmt)
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

var parserPool = &sync.Pool{New: func() any { return parser.New() }}

// ParseParameterizedSQL parse this parameterized SQL with the specified sctx.
func ParseParameterizedSQL(sctx sessionctx.Context, paramSQL string) (ast.StmtNode, error) {
	p := parserPool.Get().(*parser.Parser)
	defer parserPool.Put(p)
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

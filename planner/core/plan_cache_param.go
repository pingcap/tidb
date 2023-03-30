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
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	paramReplacerPool = sync.Pool{New: func() interface{} {
		pr := new(paramReplacer)
		pr.Reset()
		return pr
	}}
	paramRestorerPool = sync.Pool{New: func() interface{} {
		pr := new(paramRestorer)
		pr.Reset()
		return pr
	}}
	paramCtxPool = sync.Pool{New: func() interface{} {
		buf := new(strings.Builder)
		buf.Reset()
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		return restoreCtx
	}}
)

// paramReplacer is an ast.Visitor that replaces all values with `?` and collects them.
type paramReplacer struct {
	params []*driver.ValueExpr

	// Skip all values in SelectField, e.g.
	// `select a+1 from t where a<10 and b<23` should be parameterized to
	// `select a+1 from t where a<? and b<?`, instead of
	// `select a+? from t where a<? and b<?`.
	// This is to make the output field names be corresponding to these values.
	// Use int instead of bool to support nested SelectField.
	selFieldsCnt int
}

func (pr *paramReplacer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	case *ast.SelectField:
		pr.selFieldsCnt++
	case *driver.ValueExpr:
		if pr.selFieldsCnt == 0 { // not in SelectField
			pr.params = append(pr.params, n)
			param := ast.NewParamMarkerExpr(len(pr.params) - 1)      // offset is used as order in non-prepared plan cache.
			param.(*driver.ParamMarkerExpr).Datum = *n.Datum.Clone() // init the ParamMakerExpr's Datum
			return param, true
		}
	}
	return in, false
}

func (pr *paramReplacer) Leave(in ast.Node) (out ast.Node, ok bool) {
	if _, ok := in.(*ast.SelectField); ok {
		pr.selFieldsCnt--
	}
	return in, true
}

func (pr *paramReplacer) Reset() { pr.params = nil }

// ParameterizeAST parameterizes this StmtNode.
// e.g. `select * from t where a<10 and b<23` --> `select * from t where a<? and b<?`, [10, 23].
// NOTICE: this function may modify the input stmt.
func ParameterizeAST(ctx context.Context, sctx sessionctx.Context, stmt ast.StmtNode) (paramSQL string, params []*driver.ValueExpr, err error) {
	pr := paramReplacerPool.Get().(*paramReplacer)
	pCtx := paramCtxPool.Get().(*format.RestoreCtx)
	defer func() {
		pr.Reset()
		paramReplacerPool.Put(pr)
		pCtx.In.(*strings.Builder).Reset()
		paramCtxPool.Put(pCtx)
	}()
	stmt.Accept(pr)
	if err := stmt.Restore(pCtx); err != nil {
		err = RestoreASTWithParams(ctx, sctx, stmt, pr.params) // keep the stmt unchanged if err
		return "", nil, err
	}
	paramSQL, params = pCtx.In.(*strings.Builder).String(), pr.params
	return
}

type paramRestorer struct {
	params []*driver.ValueExpr
	err    error
}

func (pr *paramRestorer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	case *driver.ParamMarkerExpr:
		if n.Offset >= len(pr.params) {
			pr.err = errors.New("failed to restore ast.Node")
			return nil, true
		}
		// offset is used as order in non-prepared plan cache.
		return pr.params[n.Offset], true
	}
	if pr.err != nil {
		return nil, true
	}
	return in, false
}

func (pr *paramRestorer) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (pr *paramRestorer) Reset() {
	pr.params, pr.err = nil, nil
}

// RestoreASTWithParams restore this parameterized AST with specific parameters.
// e.g. `select * from t where a<? and b<?`, [10, 23] --> `select * from t where a<10 and b<23`.
func RestoreASTWithParams(ctx context.Context, _ sessionctx.Context, stmt ast.StmtNode, params []*driver.ValueExpr) error {
	pr := paramRestorerPool.Get().(*paramRestorer)
	defer func() {
		pr.Reset()
		paramRestorerPool.Put(pr)
	}()
	pr.params = params
	stmt.Accept(pr)
	return pr.err
}

// Params2Expressions converts these parameters to an expression list.
func Params2Expressions(params []*driver.ValueExpr) []expression.Expression {
	exprs := make([]expression.Expression, 0, len(params))
	for _, p := range params {
		tp := new(types.FieldType)
		types.InferParamTypeFromDatum(&p.Datum, tp)
		exprs = append(exprs, &expression.Constant{
			Value:   p.Datum,
			RetType: tp,
		})
	}
	return exprs
}

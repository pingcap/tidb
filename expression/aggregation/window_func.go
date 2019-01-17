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

package aggregation

import (
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// WindowFuncDesc describes a window function signature, only used in planner.
type WindowFuncDesc struct {
	baseFuncDesc
}

// NewWindowFuncDesc creates a window function signature descriptor.
func NewWindowFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression) *WindowFuncDesc {
	b := baseFuncDesc{Name: strings.ToLower(name), Args: args}
	if _, ok := aggFuncs[b.Name]; ok {
		b.typeInfer(ctx)
		return &WindowFuncDesc{newBaseFuncDesc(ctx, name, args)}
	}
	wf := &WindowFuncDesc{b}
	wf.typeInfer(ctx)
	return wf
}

var aggFuncs = map[string]struct{}{
	ast.AggFuncCount:       {},
	ast.AggFuncSum:         {},
	ast.AggFuncAvg:         {},
	ast.AggFuncGroupConcat: {},
	ast.AggFuncMax:         {},
	ast.AggFuncMin:         {},
	ast.AggFuncFirstRow:    {},
	ast.AggFuncBitAnd:      {},
	ast.AggFuncBitOr:       {},
	ast.AggFuncBitXor:      {},
}

func (wf *WindowFuncDesc) typeInfer(ctx sessionctx.Context) {
	switch wf.Name {
	case ast.WindowFuncRowNumber:
		wf.typeInfer4RowNumber()
	default:
		panic("unsupported window function: " + wf.Name)
	}
	return
}

func (wf *WindowFuncDesc) typeInfer4RowNumber() {
	wf.RetTp = types.NewFieldType(mysql.TypeLonglong)
	wf.RetTp.Flen = 21
	types.SetBinChsClnFlag(wf.RetTp)
}

// WrapCastForAggArgs wraps the args of an aggregate function with a cast function.
func (wf *WindowFuncDesc) WrapCastForAggArgs(ctx sessionctx.Context) {
	if _, ok := aggFuncs[wf.Name]; ok {
		wf.baseFuncDesc.WrapCastForAggArgs(ctx)
	}
}

// IsAggFuncs checks if the function is one of agg funcs.
func IsAggFuncs(name string) bool {
	_, ok := aggFuncs[name]
	return ok
}

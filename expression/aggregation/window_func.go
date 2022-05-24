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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"strings"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tipb/go-tipb"
)

// WindowFuncDesc describes a window function signature, only used in planner.
type WindowFuncDesc struct {
	baseFuncDesc
}

// NewWindowFuncDesc creates a window function signature descriptor.
func NewWindowFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression, skipCheckArgs bool) (*WindowFuncDesc, error) {
	// if we are in the prepare statement, skip the params check since it's not been initialized.
	if !skipCheckArgs {
		switch strings.ToLower(name) {
		case ast.WindowFuncNthValue:
			val, isNull, ok := expression.GetUint64FromConstant(args[1])
			// nth_value does not allow `0`, but allows `null`.
			if !ok || (val == 0 && !isNull) {
				return nil, nil
			}
		case ast.WindowFuncNtile:
			val, isNull, ok := expression.GetUint64FromConstant(args[0])
			// ntile does not allow `0`, but allows `null`.
			if !ok || (val == 0 && !isNull) {
				return nil, nil
			}
		case ast.WindowFuncLead, ast.WindowFuncLag:
			if len(args) < 2 {
				break
			}
			_, isNull, ok := expression.GetUint64FromConstant(args[1])
			if !ok || isNull {
				return nil, nil
			}
		}
	}

	base, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		return nil, err
	}
	return &WindowFuncDesc{base}, nil
}

// noFrameWindowFuncs is the functions that operate on the entire partition,
// they should not have frame specifications.
var noFrameWindowFuncs = map[string]struct{}{
	ast.WindowFuncCumeDist:    {},
	ast.WindowFuncDenseRank:   {},
	ast.WindowFuncLag:         {},
	ast.WindowFuncLead:        {},
	ast.WindowFuncNtile:       {},
	ast.WindowFuncPercentRank: {},
	ast.WindowFuncRank:        {},
	ast.WindowFuncRowNumber:   {},
}

var useDefaultFrameWindowFuncs = map[string]ast.FrameClause{
	ast.WindowFuncRowNumber: {
		Type: ast.Rows,
		Extent: ast.FrameExtent{
			Start: ast.FrameBound{Type: ast.CurrentRow},
			End:   ast.FrameBound{Type: ast.CurrentRow},
		},
	},
}

// UseDefaultFrame indicates if the window function has a provided frame that will override user's designation
func UseDefaultFrame(name string) (bool, ast.FrameClause) {
	frame, ok := useDefaultFrameWindowFuncs[strings.ToLower(name)]
	return ok, frame
}

// NeedFrame checks if the function need frame specification.
func NeedFrame(name string) bool {
	_, ok := noFrameWindowFuncs[strings.ToLower(name)]
	return !ok
}

// Clone makes a copy of SortItem.
func (s *WindowFuncDesc) Clone() *WindowFuncDesc {
	return &WindowFuncDesc{*s.baseFuncDesc.clone()}
}

// WindowFuncToPBExpr converts aggregate function to pb.
func WindowFuncToPBExpr(sctx sessionctx.Context, client kv.Client, desc *WindowFuncDesc) *tipb.Expr {
	pc := expression.NewPBConverter(client, sctx.GetSessionVars().StmtCtx)
	tp := desc.GetTiPBExpr(true)
	if !client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}

	children := make([]*tipb.Expr, 0, len(desc.Args))
	for _, arg := range desc.Args {
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	return &tipb.Expr{Tp: tp, Children: children, FieldType: expression.ToPBFieldType(desc.RetTp)}
}

// CanPushDownToTiFlash control whether a window function desc can be push down to tiflash.
func (s *WindowFuncDesc) CanPushDownToTiFlash() bool {
	// window functions
	switch s.Name {
	case ast.WindowFuncRowNumber, ast.WindowFuncRank, ast.WindowFuncDenseRank:
		return true
		// TODO: support aggregate functions
		//case ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncMax, ast.AggFuncMin:
		//	return true
	}

	return false
}

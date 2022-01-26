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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
	"strings"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

// WindowFuncDesc describes a window function signature, only used in planner.
type WindowFuncDesc struct {
	baseFuncDesc
}

// NewWindowFuncDesc creates a window function signature descriptor.
func NewWindowFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression) (*WindowFuncDesc, error) {
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
// TODO before review: merge this function and AggFuncToPBExpr.
// TODO before review: add all window functions and control them in planner.
func WindowFuncToPBExpr(sctx sessionctx.Context, client kv.Client, desc *WindowFuncDesc) *tipb.Expr {
	pc := expression.NewPBConverter(client, sctx.GetSessionVars().StmtCtx)

	var tp tipb.ExprType
	switch desc.Name {
	case ast.AggFuncCount:
		tp = tipb.ExprType_Count
	case ast.AggFuncApproxCountDistinct:
		tp = tipb.ExprType_ApproxCountDistinct
	case ast.AggFuncFirstRow:
		tp = tipb.ExprType_First
	//case ast.AggFuncGroupConcat:
	//	tp = tipb.ExprType_GroupConcat
	case ast.AggFuncMax:
		tp = tipb.ExprType_Max
	case ast.AggFuncMin:
		tp = tipb.ExprType_Min
	case ast.AggFuncSum:
		tp = tipb.ExprType_Sum
	case ast.AggFuncAvg:
		tp = tipb.ExprType_Avg
	case ast.AggFuncBitOr:
		tp = tipb.ExprType_Agg_BitOr
	case ast.AggFuncBitXor:
		tp = tipb.ExprType_Agg_BitXor
	case ast.AggFuncBitAnd:
		tp = tipb.ExprType_Agg_BitAnd
	case ast.AggFuncVarPop:
		tp = tipb.ExprType_VarPop
	case ast.AggFuncJsonArrayagg:
		tp = tipb.ExprType_JsonArrayAgg
	case ast.AggFuncJsonObjectAgg:
		tp = tipb.ExprType_JsonObjectAgg
	case ast.AggFuncStddevPop:
		tp = tipb.ExprType_StddevPop
	case ast.AggFuncVarSamp:
		tp = tipb.ExprType_VarSamp
	case ast.AggFuncStddevSamp:
		tp = tipb.ExprType_StddevSamp

	case ast.WindowFuncRowNumber:
		tp = tipb.ExprType_RowNumber
	case ast.WindowFuncRank:
		tp = tipb.ExprType_Rank
	case ast.WindowFuncDenseRank:
		tp = tipb.ExprType_DenseRank
	case ast.WindowFuncLag:
		tp = tipb.ExprType_Lag
	case ast.WindowFuncLead:
		tp = tipb.ExprType_Lead
	}
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

// Copyright 2017 PingCAP, Inc.
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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

// AggFuncToPBExpr converts aggregate function to pb.
func AggFuncToPBExpr(sctx sessionctx.Context, client kv.Client, aggFunc *AggFuncDesc) *tipb.Expr {
	pc := expression.NewPBConverter(client, sctx.GetSessionVars().StmtCtx)
	var tp tipb.ExprType
	switch aggFunc.Name {
	case ast.AggFuncCount:
		tp = tipb.ExprType_Count
	case ast.AggFuncApproxCountDistinct:
		tp = tipb.ExprType_ApproxCountDistinct
	case ast.AggFuncFirstRow:
		tp = tipb.ExprType_First
	case ast.AggFuncGroupConcat:
		tp = tipb.ExprType_GroupConcat
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
	}
	if !client.IsRequestTypeSupported(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}

	children := make([]*tipb.Expr, 0, len(aggFunc.Args))
	for _, arg := range aggFunc.Args {
		pbArg := pc.ExprToPB(arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	if tp == tipb.ExprType_GroupConcat {
		orderBy := make([]*tipb.ByItem, 0, len(aggFunc.OrderByItems))
		sc := sctx.GetSessionVars().StmtCtx
		for _, arg := range aggFunc.OrderByItems {
			pbArg := expression.SortByItemToPB(sc, client, arg.Expr, arg.Desc)
			if pbArg == nil {
				return nil
			}
			orderBy = append(orderBy, pbArg)
		}
		// encode GroupConcatMaxLen
		GCMaxLen, err := variable.GetSessionOrGlobalSystemVar(sctx.GetSessionVars(), variable.GroupConcatMaxLen)
		if err != nil {
			sc.AppendWarning(errors.Errorf("Error happened when buildGroupConcat: no system variable named '%s'", variable.GroupConcatMaxLen))
			return nil
		}
		maxLen, err := strconv.ParseUint(GCMaxLen, 10, 64)
		// Should never happen
		if err != nil {
			sc.AppendWarning(errors.Errorf("Error happened when buildGroupConcat: %s", err.Error()))
			return nil
		}
		return &tipb.Expr{Tp: tp, Val: codec.EncodeUint(nil, maxLen), Children: children, FieldType: expression.ToPBFieldType(aggFunc.RetTp), HasDistinct: aggFunc.HasDistinct, OrderBy: orderBy}
	}
	return &tipb.Expr{Tp: tp, Children: children, FieldType: expression.ToPBFieldType(aggFunc.RetTp), HasDistinct: aggFunc.HasDistinct}
}

// PBExprToAggFuncDesc converts pb to aggregate function.
func PBExprToAggFuncDesc(ctx sessionctx.Context, aggFunc *tipb.Expr, fieldTps []*types.FieldType) (*AggFuncDesc, error) {
	var name string
	switch aggFunc.Tp {
	case tipb.ExprType_Count:
		name = ast.AggFuncCount
	case tipb.ExprType_ApproxCountDistinct:
		name = ast.AggFuncApproxCountDistinct
	case tipb.ExprType_First:
		name = ast.AggFuncFirstRow
	case tipb.ExprType_GroupConcat:
		name = ast.AggFuncGroupConcat
	case tipb.ExprType_Max:
		name = ast.AggFuncMax
	case tipb.ExprType_Min:
		name = ast.AggFuncMin
	case tipb.ExprType_Sum:
		name = ast.AggFuncSum
	case tipb.ExprType_Avg:
		name = ast.AggFuncAvg
	case tipb.ExprType_Agg_BitOr:
		name = ast.AggFuncBitOr
	case tipb.ExprType_Agg_BitXor:
		name = ast.AggFuncBitXor
	case tipb.ExprType_Agg_BitAnd:
		name = ast.AggFuncBitAnd
	default:
		return nil, errors.Errorf("unknown aggregation function type: %v", aggFunc.Tp)
	}

	args, err := expression.PBToExprs(aggFunc.Children, fieldTps, ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return nil, err
	}
	base := baseFuncDesc{
		Name:  name,
		Args:  args,
		RetTp: expression.FieldTypeFromPB(aggFunc.FieldType),
	}
	base.WrapCastForAggArgs(ctx)
	return &AggFuncDesc{
		baseFuncDesc: base,
		Mode:         Partial1Mode,
		HasDistinct:  false,
	}, nil
}

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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tipb/go-tipb"
)

// AggFuncToPBExpr converts aggregate function to pb.
func AggFuncToPBExpr(sc *stmtctx.StatementContext, client kv.Client, aggFunc *AggFuncDesc) *tipb.Expr {
	if aggFunc.HasDistinct {
		// do nothing and ignore aggFunc.HasDistinct
	}
	pc := expression.NewPBConverter(client, sc)
	var tp tipb.ExprType
	switch aggFunc.Name {
	case ast.AggFuncCount:
		tp = tipb.ExprType_Count
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
	case ast.AggFuncJsonObjectAgg:
		tp = tipb.ExprType_JsonObjectAgg
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
	return &tipb.Expr{Tp: tp, Children: children, FieldType: expression.ToPBFieldType(aggFunc.RetTp)}
}

// PBExprToAggFuncDesc converts pb to aggregate function.
func PBExprToAggFuncDesc(ctx sessionctx.Context, aggFunc *tipb.Expr, fieldTps []*types.FieldType) (*AggFuncDesc, error) {
	var name string
	switch aggFunc.Tp {
	case tipb.ExprType_Count:
		name = ast.AggFuncCount
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

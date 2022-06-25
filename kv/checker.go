// Copyright 2019 PingCAP, Inc.
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

package kv

import (
	"github.com/pingcap/tipb/go-tipb"
)

// RequestTypeSupportedChecker is used to check expression can be pushed down.
type RequestTypeSupportedChecker struct{}

// IsRequestTypeSupported checks whether reqType is supported.
func (d RequestTypeSupportedChecker) IsRequestTypeSupported(reqType, subType int64) bool {
	switch reqType {
	case ReqTypeSelect, ReqTypeIndex:
		switch subType {
		case ReqSubTypeGroupBy, ReqSubTypeBasic, ReqSubTypeTopN:
			return true
		default:
			return d.supportExpr(tipb.ExprType(subType))
		}
	case ReqTypeDAG:
		return d.supportExpr(tipb.ExprType(subType))
	case ReqTypeAnalyze:
		return true
	}
	return false
}

// TODO: deprecate this function, because:
// 1. we have more than one storage engine available for push-down.
// 2. we'd better do an accurate push-down check at the planner stage, instead of here.
// currently, we do aggregation push-down check in `CheckAggCanPushCop`.
func (d RequestTypeSupportedChecker) supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlTime, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_Float32, tipb.ExprType_Float64, tipb.ExprType_ColumnRef, tipb.ExprType_MysqlEnum, tipb.ExprType_MysqlBit:
		return true
	// aggregate functions.
	// NOTE: tipb.ExprType_GroupConcat is only supported by TiFlash, So checking it for TiKV case outside.
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min, tipb.ExprType_Sum, tipb.ExprType_Avg,
		tipb.ExprType_Agg_BitXor, tipb.ExprType_Agg_BitAnd, tipb.ExprType_Agg_BitOr, tipb.ExprType_ApproxCountDistinct, tipb.ExprType_GroupConcat:
		return true
	// window functions.
	case tipb.ExprType_RowNumber, tipb.ExprType_Rank, tipb.ExprType_DenseRank, tipb.ExprType_CumeDist, tipb.ExprType_PercentRank,
		tipb.ExprType_Ntile, tipb.ExprType_Lead, tipb.ExprType_Lag, tipb.ExprType_FirstValue, tipb.ExprType_LastValue, tipb.ExprType_NthValue:
		return true
	case ReqSubTypeDesc:
		return true
	case ReqSubTypeSignature:
		return true
	default:
		return false
	}
}

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
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import "github.com/pingcap/tipb/go-tipb"

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

func (d RequestTypeSupportedChecker) supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlTime, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_Float32, tipb.ExprType_Float64, tipb.ExprType_ColumnRef:
		return true
	// aggregate functions.
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min, tipb.ExprType_Sum, tipb.ExprType_Avg,
		tipb.ExprType_Agg_BitXor, tipb.ExprType_Agg_BitAnd, tipb.ExprType_Agg_BitOr, tipb.ExprType_ApproxCountDistinct:
		return true
	case ReqSubTypeDesc:
		return true
	case ReqSubTypeSignature:
		return true
	default:
		return false
	}
}

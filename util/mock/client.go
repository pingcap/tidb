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

package mock

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

// Client implement kv.Client interface, mocked from "CopClient" defined in
// "store/tikv/copprocessor.go".
type Client struct {
	MockResponse kv.Response
}

// Send implement kv.Client interface.
func (c *Client) Send(ctx context.Context, req *kv.Request) kv.Response {
	return c.MockResponse
}

// IsRequestTypeSupported implement kv.Client interface.
func (c *Client) IsRequestTypeSupported(reqType, subType int64) bool {
	switch reqType {
	case kv.ReqTypeSelect, kv.ReqTypeIndex:
		switch subType {
		case kv.ReqSubTypeGroupBy, kv.ReqSubTypeBasic, kv.ReqSubTypeTopN:
			return true
		default:
			return c.supportExpr(tipb.ExprType(subType))
		}
	}
	return false
}

func (c *Client) supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlTime, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_Float32, tipb.ExprType_Float64, tipb.ExprType_ColumnRef:
		return true
	// logic operators.
	case tipb.ExprType_And, tipb.ExprType_Or, tipb.ExprType_Not:
		return true
	// compare operators.
	case tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList, tipb.ExprType_IsNull,
		tipb.ExprType_Like:
		return true
	// arithmetic operators.
	case tipb.ExprType_Plus, tipb.ExprType_Div, tipb.ExprType_Minus, tipb.ExprType_Mul:
		return true
	// control functions
	case tipb.ExprType_Case, tipb.ExprType_If, tipb.ExprType_IfNull, tipb.ExprType_Coalesce:
		return true
	// aggregate functions.
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min, tipb.ExprType_Sum, tipb.ExprType_Avg,
		tipb.ExprType_Agg_BitXor, tipb.ExprType_Agg_BitAnd, tipb.ExprType_Agg_BitOr:
		return true
	// json functions.
	case tipb.ExprType_JsonType, tipb.ExprType_JsonExtract, tipb.ExprType_JsonUnquote,
		tipb.ExprType_JsonObject, tipb.ExprType_JsonArray, tipb.ExprType_JsonMerge,
		tipb.ExprType_JsonSet, tipb.ExprType_JsonInsert, tipb.ExprType_JsonReplace, tipb.ExprType_JsonRemove:
		return true
	// date functions.
	case tipb.ExprType_DateFormat:
		return true
	case kv.ReqSubTypeDesc:
		return true
	case kv.ReqSubTypeSignature:
		return true
	default:
		return false
	}
}

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
	"encoding/json"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var _ = Suite(&testEvaluatorSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

// mockKvClient is mocked from tikv.CopClient to avoid circular dependency.
type mockKvClient struct {
}

// IsRequestTypeSupported implements the kv.Client interface..
func (c *mockKvClient) IsRequestTypeSupported(reqType, subType int64) bool {
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

// Send implements the kv.Client interface..
func (c *mockKvClient) Send(ctx goctx.Context, req *kv.Request) kv.Response {
	return nil
}

func (c *mockKvClient) supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlTime, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_ColumnRef,
		tipb.ExprType_And, tipb.ExprType_Or,
		tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList,
		tipb.ExprType_Like, tipb.ExprType_Not:
		return true
	case tipb.ExprType_Plus, tipb.ExprType_Div:
		return true
	case tipb.ExprType_Case, tipb.ExprType_If:
		return true
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min, tipb.ExprType_Sum, tipb.ExprType_Avg:
		return true
	case tipb.ExprType_JsonType, tipb.ExprType_JsonExtract, tipb.ExprType_JsonUnquote, tipb.ExprType_JsonValid,
		tipb.ExprType_JsonObject, tipb.ExprType_JsonArray, tipb.ExprType_JsonMerge, tipb.ExprType_JsonSet,
		tipb.ExprType_JsonInsert, tipb.ExprType_JsonReplace, tipb.ExprType_JsonRemove, tipb.ExprType_JsonContains:
		return false
	case kv.ReqSubTypeDesc:
		return true
	default:
		return false
	}
}

type dataGen4Expr2PbTest struct {
}

func (dg *dataGen4Expr2PbTest) genColumn(tp byte, id int64) *expression.Column {
	return &expression.Column{
		RetType: types.NewFieldType(tp),
		ID:      id,
		Index:   int(id),
	}
}

type testEvaluatorSuite struct {
	*parser.Parser
	ctx context.Context
}

func (s *testEvaluatorSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	s.ctx = mock.NewContext()
}

func (s *testEvaluatorSuite) TearDownSuite(c *C) {
}

func (s *testEvaluatorSuite) TestAggFunc2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mockKvClient)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncGroupConcat, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow}
	for _, funcName := range funcNames {
		aggFunc := &AggFuncDesc{
			Name:        funcName,
			Args:        []expression.Expression{dg.genColumn(mysql.TypeDouble, 1)},
			HasDistinct: true,
		}
		pbExpr := AggFuncToPBExpr(sc, client, aggFunc)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, "null")
	}

	jsons := []string{
		"{\"tp\":3002,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0}],\"sig\":0}",
		"{\"tp\":3001,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0}],\"sig\":0}",
		"{\"tp\":3003,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0}],\"sig\":0}",
		"null",
		"{\"tp\":3005,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0}],\"sig\":0}",
		"{\"tp\":3004,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0}],\"sig\":0}",
		"{\"tp\":3006,\"children\":[{\"tp\":201,\"val\":\"gAAAAAAAAAE=\",\"sig\":0}],\"sig\":0}",
	}
	for i, funcName := range funcNames {
		aggFunc := &AggFuncDesc{
			Name:        funcName,
			Args:        []expression.Expression{dg.genColumn(mysql.TypeDouble, 1)},
			HasDistinct: false,
		}
		pbExpr := AggFuncToPBExpr(sc, client, aggFunc)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

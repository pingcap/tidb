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
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testEvaluatorSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
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
	ctx sessionctx.Context
}

func (s *testEvaluatorSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	s.ctx = mock.NewContext()
}

func (s *testEvaluatorSuite) TearDownSuite(c *C) {
}

func (s *testEvaluatorSuite) TestAggFunc2Pb(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)

	funcNames := []string{ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncGroupConcat, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow}
	funcTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeDouble),
	}

	jsons := []string{
		`{"tp":3002,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}`,
		`{"tp":3001,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}],"sig":0,"field_type":{"tp":8,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}`,
		`{"tp":3003,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}`,
		"null",
		`{"tp":3005,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}`,
		`{"tp":3004,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}`,
		`{"tp":3006,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":""}}`,
	}
	for i, funcName := range funcNames {
		args := []expression.Expression{dg.genColumn(mysql.TypeDouble, 1)}
		aggFunc, err := NewAggFuncDesc(s.ctx, funcName, args, false)
		c.Assert(err, IsNil)
		aggFunc.RetTp = funcTypes[i]
		pbExpr := AggFuncToPBExpr(sc, client, aggFunc)
		js, err := json.Marshal(pbExpr)
		c.Assert(err, IsNil)
		c.Assert(string(js), Equals, jsons[i])
	}
}

// Copyright 2020 PingCAP, Inc.
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

package expression_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testFlagSimplifySuite{})

type testFlagSimplifySuite struct {
	store    kv.Storage
	dom      *domain.Domain
	ctx      sessionctx.Context
	testData testutil.TestData
}

func (s *testFlagSimplifySuite) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
	s.testData, err = testutil.LoadTestSuiteData("testdata", "flag_simplify")
	c.Assert(err, IsNil)
}

func (s *testFlagSimplifySuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
	s.dom.Close()
	s.store.Close()
}

func (s *testFlagSimplifySuite) TestSimplifyExpressionByFlag(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, a bigint unsigned not null, b bigint unsigned)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
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


func (s *testFlagSimplifySuite) TestExprOnlyPushDownToFlash(c *C) {
	sc := new(stmtctx.StatementContext)
	client := new(mock.Client)
	dg := new(dataGen4Expr2PbTest)
	exprs := make([]expression.Expression, 0)

	//jsonColumn := dg.genColumn(mysql.TypeJSON, 1)
	intColumn := dg.genColumn(mysql.TypeLonglong, 2)
	//realColumn := dg.genColumn(mysql.TypeDouble, 3)
	decimalColumn := dg.genColumn(mysql.TypeNewDecimal, 4)
	stringColumn := dg.genColumn(mysql.TypeString, 5)
	datetimeColumn := dg.genColumn(mysql.TypeDatetime, 6)
	binaryStringColumn := dg.genColumn(mysql.TypeString, 7)
	binaryStringColumn.RetType.Collate = charset.CollationBin

	function, err := expression.NewFunction(mock.NewContext(), ast.Substr, types.NewFieldType(mysql.TypeString), stringColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = expression.NewFunction(mock.NewContext(), ast.Substring, types.NewFieldType(mysql.TypeString), stringColumn, intColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = expression.NewFunction(mock.NewContext(), ast.DateAdd, types.NewFieldType(mysql.TypeDatetime), datetimeColumn, intColumn, stringColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = expression.NewFunction(mock.NewContext(), ast.TimestampDiff, types.NewFieldType(mysql.TypeLonglong), stringColumn, datetimeColumn, datetimeColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = expression.NewFunction(mock.NewContext(), ast.FromUnixTime, types.NewFieldType(mysql.TypeDatetime), decimalColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	function, err = expression.NewFunction(mock.NewContext(), ast.Extract, types.NewFieldType(mysql.TypeLonglong), stringColumn, datetimeColumn)
	c.Assert(err, IsNil)
	exprs = append(exprs, function)

	pushed, remained := expression.PushDownExprs(sc, exprs, client, kv.UnSpecified)
	c.Assert(len(pushed), Equals, len(exprs))
	c.Assert(len(remained), Equals, 0)

	canPush := expression.CanExprsPushDown(sc, exprs, client, kv.TiFlash)
	c.Assert(canPush, Equals, true)
	canPush = expression.CanExprsPushDown(sc, exprs, client, kv.TiKV)
	c.Assert(canPush, Equals, false)

	pushed, remained = expression.PushDownExprs(sc, exprs, client, kv.TiFlash)
	c.Assert(len(pushed), Equals, len(exprs))
	c.Assert(len(remained), Equals, 0)

	pushed, remained = expression.PushDownExprs(sc, exprs, client, kv.TiKV)
	c.Assert(len(pushed), Equals, 0)
	c.Assert(len(remained), Equals, len(exprs))
}

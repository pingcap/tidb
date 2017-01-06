// Copyright 2016 PingCAP, Inc.
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

package plan

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct {
	*parser.Parser
	ctx context.Context
}

func (s *testExpressionSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	s.ctx = mock.NewContext()
}

func (s *testExpressionSuite) parseExpr(c *C, expr string) ast.ExprNode {
	st, err := s.ParseOneStmt("select "+expr, "", "")
	c.Assert(err, IsNil)
	stmt := st.(*ast.SelectStmt)
	return stmt.Fields.Fields[0].Expr
}

type testCase struct {
	exprStr   string
	resultStr string
}

func (s *testExpressionSuite) runTests(c *C, cases []testCase) {
	for _, ca := range cases {
		expr := s.parseExpr(c, ca.exprStr)
		val, err := evalAstExpr(expr, s.ctx)
		c.Assert(err, IsNil)
		valStr := fmt.Sprintf("%v", val.GetValue())
		c.Assert(valStr, Equals, ca.resultStr, Commentf("for %s", ca.exprStr))
	}
}

func (s *testExpressionSuite) TestBetween(c *C) {
	defer testleak.AfterTest(c)()
	cases := []testCase{
		{exprStr: "1 between 2 and 3", resultStr: "0"},
		{exprStr: "1 not between 2 and 3", resultStr: "1"},
	}
	s.runTests(c, cases)
}

func (s *testExpressionSuite) TestCaseWhen(c *C) {
	defer testleak.AfterTest(c)()
	cases := []testCase{
		{
			exprStr:   "case 1 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "str1",
		},
		{
			exprStr:   "case 2 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "str2",
		},
		{
			exprStr:   "case 3 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "<nil>",
		},
		{
			exprStr:   "case 4 when 1 then 'str1' when 2 then 'str2' else 'str3' end",
			resultStr: "str3",
		},
	}
	s.runTests(c, cases)

	// When expression value changed, result set back to null.
	valExpr := ast.NewValueExpr(1)
	whenClause := &ast.WhenClause{Expr: ast.NewValueExpr(1), Result: ast.NewValueExpr(1)}
	caseExpr := &ast.CaseExpr{
		Value:       valExpr,
		WhenClauses: []*ast.WhenClause{whenClause},
	}
	v, err := evalAstExpr(caseExpr, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(v, testutil.DatumEquals, types.NewDatum(int64(1)))
	valExpr.SetValue(4)
	v, err = evalAstExpr(caseExpr, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)
}

func (s *testExpressionSuite) TestCast(c *C) {
	defer testleak.AfterTest(c)()
	f := types.NewFieldType(mysql.TypeLonglong)

	expr := &ast.FuncCastExpr{
		Expr: ast.NewValueExpr(1),
		Tp:   f,
	}
	ast.SetFlag(expr)
	v, err := evalAstExpr(expr, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(v, testutil.DatumEquals, types.NewDatum(int64(1)))

	f.Flag |= mysql.UnsignedFlag
	v, err = evalAstExpr(expr, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(v, testutil.DatumEquals, types.NewDatum(uint64(1)))

	f.Tp = mysql.TypeString
	f.Charset = charset.CharsetBin
	v, err = evalAstExpr(expr, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(v, testutil.DatumEquals, types.NewDatum([]byte("1")))

	f.Tp = mysql.TypeString
	f.Charset = "utf8"
	v, err = evalAstExpr(expr, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(v, testutil.DatumEquals, types.NewDatum("1"))

	expr.Expr = ast.NewValueExpr(nil)
	v, err = evalAstExpr(expr, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)
}

func (s *testExpressionSuite) TestPatternIn(c *C) {
	defer testleak.AfterTest(c)()
	cases := []testCase{
		{
			exprStr:   "1 not in (1, 2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "1 in (1, 2, 3)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "NULL in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL not in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL in (NULL, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "1 in (1, NULL)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (NULL, 1)",
			resultStr: "1",
		},
		{
			exprStr:   "2 in (1, NULL)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "(-(23)++46/51*+51) in (+23)",
			resultStr: "0",
		},
	}
	s.runTests(c, cases)
}

func (s *testExpressionSuite) TestIsNull(c *C) {
	defer testleak.AfterTest(c)()
	cases := []testCase{
		{
			exprStr:   "1 IS NULL",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS NOT NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT NULL",
			resultStr: "0",
		},
	}
	s.runTests(c, cases)
}

func (s *testExpressionSuite) TestIsTruth(c *C) {
	defer testleak.AfterTest(c)()
	cases := []testCase{
		{
			exprStr:   "1 IS TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "2 IS TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "0 IS TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "NULL IS TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "2 IS FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "0 IS FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "1 IS NOT TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "2 IS NOT TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "0 IS NOT TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "1 IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "2 IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "0 IS NOT FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "NULL IS NOT FALSE",
			resultStr: "1",
		},
	}
	s.runTests(c, cases)
}

func (s *testExpressionSuite) TestDateArith(c *C) {
	defer testleak.AfterTest(c)()

	// list all test cases
	tests := []struct {
		Date      interface{}
		Interval  interface{}
		Unit      string
		AddResult interface{}
		SubResult interface{}
		error     bool
	}{
		// basic test
		{"2011-11-11", 1, "DAY", "2011-11-12", "2011-11-10", false},
		// nil test
		{nil, 1, "DAY", nil, nil, false},
		{"2011-11-11", nil, "DAY", nil, nil, false},
		// tests for inner function call
		{"2011-11-11", s.parseExpr(c, "LEAST(1, 2)"), "DAY", "2011-11-12", "2011-11-10", false},
		{"2011-11-11", s.parseExpr(c, "LEAST(NULL, 2)"), "DAY", nil, nil, false},
		// tests for different units
		{"2011-11-11 10:10:10", 1000, "MICROSECOND", "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000", false},
		{"2011-11-11 10:10:10", "10", "SECOND", "2011-11-11 10:10:20", "2011-11-11 10:10:00", false},
		{"2011-11-11 10:10:10", "10", "MINUTE", "2011-11-11 10:20:10", "2011-11-11 10:00:10", false},
		{"2011-11-11 10:10:10", "10", "HOUR", "2011-11-11 20:10:10", "2011-11-11 00:10:10", false},
		{"2011-11-11 10:10:10", "11", "DAY", "2011-11-22 10:10:10", "2011-10-31 10:10:10", false},
		{"2011-11-11 10:10:10", "2", "WEEK", "2011-11-25 10:10:10", "2011-10-28 10:10:10", false},
		{"2011-11-11 10:10:10", "2", "MONTH", "2012-01-11 10:10:10", "2011-09-11 10:10:10", false},
		{"2011-11-11 10:10:10", "4", "QUARTER", "2012-11-11 10:10:10", "2010-11-11 10:10:10", false},
		{"2011-11-11 10:10:10", "2", "YEAR", "2013-11-11 10:10:10", "2009-11-11 10:10:10", false},
		{"2011-11-11 10:10:10", "10.00100000", "SECOND_MICROSECOND", "2011-11-11 10:10:20.100000", "2011-11-11 10:09:59.900000", false},
		{"2011-11-11 10:10:10", "10.0010000000", "SECOND_MICROSECOND", "2011-11-11 10:10:30", "2011-11-11 10:09:50", false},
		{"2011-11-11 10:10:10", "10.0010000010", "SECOND_MICROSECOND", "2011-11-11 10:10:30.000010", "2011-11-11 10:09:49.999990", false},
		{"2011-11-11 10:10:10", "10:10.100", "MINUTE_MICROSECOND", "2011-11-11 10:20:20.100000", "2011-11-11 09:59:59.900000", false},
		{"2011-11-11 10:10:10", "10:10", "MINUTE_SECOND", "2011-11-11 10:20:20", "2011-11-11 10:00:00", false},
		{"2011-11-11 10:10:10", "10:10:10.100", "HOUR_MICROSECOND", "2011-11-11 20:20:20.100000", "2011-11-10 23:59:59.900000", false},
		{"2011-11-11 10:10:10", "10:10:10", "HOUR_SECOND", "2011-11-11 20:20:20", "2011-11-11 00:00:00", false},
		{"2011-11-11 10:10:10", "10:10", "HOUR_MINUTE", "2011-11-11 20:20:10", "2011-11-11 00:00:10", false},
		{"2011-11-11 10:10:10", "11 10:10:10.100", "DAY_MICROSECOND", "2011-11-22 20:20:20.100000", "2011-10-30 23:59:59.900000", false},
		{"2011-11-11 10:10:10", "11 10:10:10", "DAY_SECOND", "2011-11-22 20:20:20", "2011-10-31 00:00:00", false},
		{"2011-11-11 10:10:10", "11 10:10", "DAY_MINUTE", "2011-11-22 20:20:10", "2011-10-31 00:00:10", false},
		{"2011-11-11 10:10:10", "11 10", "DAY_HOUR", "2011-11-22 20:10:10", "2011-10-31 00:10:10", false},
		{"2011-11-11 10:10:10", "11-1", "YEAR_MONTH", "2022-12-11 10:10:10", "2000-10-11 10:10:10", false},
		{"2011-11-11 10:10:10", "11-11", "YEAR_MONTH", "2023-10-11 10:10:10", "1999-12-11 10:10:10", false},
		// tests for interval in day forms
		{"2011-11-11 10:10:10", "20", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10", false},
		{"2011-11-11 10:10:10", 19.88, "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10", false},
		{"2011-11-11 10:10:10", "19.88", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10", false},
		{"2011-11-11 10:10:10", "prefix19suffix", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10", false},
		{"2011-11-11 10:10:10", "20-11", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10", false},
		{"2011-11-11 10:10:10", "20,11", "daY", "2011-12-01 10:10:10", "2011-10-22 10:10:10", false},
		{"2011-11-11 10:10:10", "1000", "dAy", "2014-08-07 10:10:10", "2009-02-14 10:10:10", false},
		{"2011-11-11 10:10:10", "true", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10", false},
		{"2011-11-11 10:10:10", true, "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10", false},
		// test for different return data types
		{"2011-11-11", 1, "DAY", "2011-11-12", "2011-11-10", false},
		{"2011-11-11", 10, "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00", false},
		{"2011-11-11", 10, "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00", false},
		{"2011-11-11", 10, "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50", false},
		{"2011-11-11", "10:10", "HOUR_MINUTE", "2011-11-11 10:10:00", "2011-11-10 13:50:00", false},
		{"2011-11-11", "10:10:10", "HOUR_SECOND", "2011-11-11 10:10:10", "2011-11-10 13:49:50", false},
		{"2011-11-11", "10:10:10.101010", "HOUR_MICROSECOND", "2011-11-11 10:10:10.101010", "2011-11-10 13:49:49.898990", false},
		{"2011-11-11", "10:10", "MINUTE_SECOND", "2011-11-11 00:10:10", "2011-11-10 23:49:50", false},
		{"2011-11-11", "10:10.101010", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.101010", "2011-11-10 23:49:49.898990", false},
		{"2011-11-11", "10.101010", "SECOND_MICROSECOND", "2011-11-11 00:00:10.101010", "2011-11-10 23:59:49.898990", false},
		{"2011-11-11 00:00:00", 1, "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00", false},
		{"2011-11-11 00:00:00", 10, "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00", false},
		{"2011-11-11 00:00:00", 10, "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00", false},
		{"2011-11-11 00:00:00", 10, "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50", false},
		// tests for invalid input
		{"2011-11-11", "abc1000", "MICROSECOND", nil, nil, true},
		{"20111111 10:10:10", "1", "DAY", nil, nil, true},
		{"2011-11-11", "10", "SECOND_MICROSECOND", nil, nil, true},
		{"2011-11-11", "10.0000", "MINUTE_MICROSECOND", nil, nil, true},
		{"2011-11-11", "10:10:10", "MINUTE_MICROSECOND", nil, nil, true},
	}

	// run the test cases
	for _, t := range tests {
		var interval ast.ExprNode
		if n, ok := t.Interval.(ast.ExprNode); ok {
			interval = n
		} else {
			interval = ast.NewValueExpr(t.Interval)
		}
		for _, x := range []struct {
			fnName string
			result interface{}
		}{
			{ast.DateAdd, t.AddResult},
			{ast.DateSub, t.SubResult},
			{ast.AddDate, t.AddResult},
			{ast.SubDate, t.SubResult},
		} {
			expr := &ast.FuncCallExpr{
				FnName: model.NewCIStr(x.fnName),
				Args: []ast.ExprNode{
					ast.NewValueExpr(t.Date),
					interval,
					ast.NewValueExpr(t.Unit),
				},
			}
			ast.SetFlag(expr)
			v, err := evalAstExpr(expr, s.ctx)
			if t.error == true {
				c.Assert(err, NotNil)
			} else {
				c.Assert(err, IsNil)
				if v.IsNull() {
					c.Assert(nil, Equals, x.result)
				} else {
					c.Assert(v.Kind(), Equals, types.KindMysqlTime)
					value := v.GetMysqlTime()
					c.Assert(value.String(), Equals, x.result)
				}
			}
		}
	}
}

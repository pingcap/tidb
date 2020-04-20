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

package ast_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	. "github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/test_driver"
)

var _ = Suite(&testFunctionsSuite{})

type testFunctionsSuite struct {
}

func (ts *testFunctionsSuite) TestFunctionsVisitorCover(c *C) {
	valueExpr := NewValueExpr(42, mysql.DefaultCharset, mysql.DefaultCollationName)
	stmts := []Node{
		&AggregateFuncExpr{Args: []ExprNode{valueExpr}},
		&FuncCallExpr{Args: []ExprNode{valueExpr}},
		&FuncCastExpr{Expr: valueExpr},
		&WindowFuncExpr{Spec: WindowSpec{}},
	}

	for _, stmt := range stmts {
		stmt.Accept(visitor{})
		stmt.Accept(visitor1{})
	}
}

func (ts *testFunctionsSuite) TestFuncCallExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"JSON_ARRAYAGG(attribute)", "JSON_ARRAYAGG(`attribute`)"},
		{"JSON_OBJECTAGG(attribute, value)", "JSON_OBJECTAGG(`attribute`, `value`)"},
		{"ABS(-1024)", "ABS(-1024)"},
		{"ACOS(3.14)", "ACOS(3.14)"},
		{"CONV('a',16,2)", "CONV('a', 16, 2)"},
		{"COS(PI())", "COS(PI())"},
		{"RAND()", "RAND()"},
		{"ADDDATE('2000-01-01', 1)", "ADDDATE('2000-01-01', INTERVAL 1 DAY)"},
		{"DATE_ADD('2000-01-01', INTERVAL 1 DAY)", "DATE_ADD('2000-01-01', INTERVAL 1 DAY)"},
		{"DATE_ADD('2000-01-01', INTERVAL '1 1:12:23.100000' DAY_MICROSECOND)", "DATE_ADD('2000-01-01', INTERVAL '1 1:12:23.100000' DAY_MICROSECOND)"},
		{"EXTRACT(DAY FROM '2000-01-01')", "EXTRACT(DAY FROM '2000-01-01')"},
		{"extract(day from '1999-01-01')", "EXTRACT(DAY FROM '1999-01-01')"},
		{"GET_FORMAT(DATE, 'EUR')", "GET_FORMAT(DATE, 'EUR')"},
		{"POSITION('a' IN 'abc')", "POSITION('a' IN 'abc')"},
		{"TRIM('  bar   ')", "TRIM('  bar   ')"},
		{"TRIM('a' FROM '  bar   ')", "TRIM('a' FROM '  bar   ')"},
		{"TRIM(LEADING FROM '  bar   ')", "TRIM(LEADING FROM '  bar   ')"},
		{"TRIM(BOTH FROM '  bar   ')", "TRIM(BOTH FROM '  bar   ')"},
		{"TRIM(TRAILING FROM '  bar   ')", "TRIM(TRAILING FROM '  bar   ')"},
		{"TRIM(LEADING 'x' FROM 'xxxyxxx')", "TRIM(LEADING 'x' FROM 'xxxyxxx')"},
		{"TRIM(BOTH 'x' FROM 'xxxyxxx')", "TRIM(BOTH 'x' FROM 'xxxyxxx')"},
		{"TRIM(TRAILING 'x' FROM 'xxxyxxx')", "TRIM(TRAILING 'x' FROM 'xxxyxxx')"},
		{"DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY)", "DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY)"},
		{"BENCHMARK(1000000, AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3')))", "BENCHMARK(1000000, AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3')))"},
		{"SUBSTRING('Quadratically', 5)", "SUBSTRING('Quadratically', 5)"},
		{"SUBSTRING('Quadratically' FROM 5)", "SUBSTRING('Quadratically', 5)"},
		{"SUBSTRING('Quadratically', 5, 6)", "SUBSTRING('Quadratically', 5, 6)"},
		{"SUBSTRING('Quadratically' FROM 5 FOR 6)", "SUBSTRING('Quadratically', 5, 6)"},
		{"MASTER_POS_WAIT(@log_name, @log_pos, @timeout, @channel_name)", "MASTER_POS_WAIT(@`log_name`, @`log_pos`, @`timeout`, @`channel_name`)"},
		{"JSON_TYPE('[123]')", "JSON_TYPE('[123]')"},
		{"bit_and(all c1)", "BIT_AND(`c1`)"},
		{"nextval(seq)", "NEXTVAL(`seq`)"},
		{"nextval(test.seq)", "NEXTVAL(`test`.`seq`)"},
		{"lastval(seq)", "LASTVAL(`seq`)"},
		{"lastval(test.seq)", "LASTVAL(`test`.`seq`)"},
		{"setval(seq, 100)", "SETVAL(`seq`, 100)"},
		{"setval(test.seq, 100)", "SETVAL(`test`.`seq`, 100)"},
		{"next value for seq", "NEXTVAL(`seq`)"},
		{"next value for test.seq", "NEXTVAL(`test`.`seq`)"},
		{"next value for sequence", "NEXTVAL(`sequence`)"},
		{"NeXt vAluE for seQuEncE2", "NEXTVAL(`seQuEncE2`)"},
		{"NeXt vAluE for test.seQuEncE2", "NEXTVAL(`test`.`seQuEncE2`)"},
		{"weight_string(a)", "WEIGHT_STRING(`a`)"},
		{"Weight_stRing(test.a)", "WEIGHT_STRING(`test`.`a`)"},
		{"weight_string('a')", "WEIGHT_STRING('a')"},
		// Expressions with collations of different charsets will lead to an error in MySQL, but the error check should be done in TiDB, so it's valid here.
		{"weight_string('a' collate utf8_general_ci collate utf8mb4_general_ci)", "WEIGHT_STRING('a' COLLATE utf8_general_ci COLLATE utf8mb4_general_ci)"},
		{"weight_string(_utf8 'a' collate utf8_general_ci)", "WEIGHT_STRING(_UTF8'a' COLLATE utf8_general_ci)"},
		{"weight_string(_utf8 'a')", "WEIGHT_STRING(_UTF8'a')"},
		{"weight_string(a as char(5))", "WEIGHT_STRING(`a` AS CHAR(5))"},
		{"weight_string(a as character(5))", "WEIGHT_STRING(`a` AS CHAR(5))"},
		{"weight_string(a as binary(5))", "WEIGHT_STRING(`a` AS BINARY(5))"},
		{"hex(weight_string('abc' as binary(5)))", "HEX(WEIGHT_STRING('abc' AS BINARY(5)))"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (ts *testFunctionsSuite) TestFuncCastExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"CONVERT('Müller' USING UtF8Mb4)", "CONVERT('Müller' USING UTF8MB4)"},
		{"CONVERT('Müller', CHAR(32) CHARACTER SET UtF8)", "CONVERT('Müller', CHAR(32) CHARSET UTF8)"},
		{"CAST('test' AS CHAR CHARACTER SET UtF8)", "CAST('test' AS CHAR CHARSET UTF8)"},
		{"BINARY 'New York'", "BINARY 'New York'"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (ts *testFunctionsSuite) TestAggregateFuncExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"AVG(test_score)", "AVG(`test_score`)"},
		{"AVG(distinct test_score)", "AVG(DISTINCT `test_score`)"},
		{"BIT_AND(test_score)", "BIT_AND(`test_score`)"},
		{"BIT_OR(test_score)", "BIT_OR(`test_score`)"},
		{"BIT_XOR(test_score)", "BIT_XOR(`test_score`)"},
		{"COUNT(test_score)", "COUNT(`test_score`)"},
		{"COUNT(*)", "COUNT(1)"},
		{"COUNT(DISTINCT scores, results)", "COUNT(DISTINCT `scores`, `results`)"},
		{"MIN(test_score)", "MIN(`test_score`)"},
		{"MIN(DISTINCT test_score)", "MIN(DISTINCT `test_score`)"},
		{"MAX(test_score)", "MAX(`test_score`)"},
		{"MAX(DISTINCT test_score)", "MAX(DISTINCT `test_score`)"},
		{"STD(test_score)", "STD(`test_score`)"},
		{"STDDEV(test_score)", "STDDEV(`test_score`)"},
		{"STDDEV_POP(test_score)", "STDDEV_POP(`test_score`)"},
		{"STDDEV_SAMP(test_score)", "STDDEV_SAMP(`test_score`)"},
		{"SUM(test_score)", "SUM(`test_score`)"},
		{"SUM(DISTINCT test_score)", "SUM(DISTINCT `test_score`)"},
		{"VAR_POP(test_score)", "VAR_POP(`test_score`)"},
		{"VAR_SAMP(test_score)", "VAR_SAMP(`test_score`)"},
		{"VARIANCE(test_score)", "VAR_POP(`test_score`)"},
		{"JSON_OBJECTAGG(test_score, results)", "JSON_OBJECTAGG(`test_score`, `results`)"},
		{"GROUP_CONCAT(a)", "GROUP_CONCAT(`a` SEPARATOR ',')"},
		{"GROUP_CONCAT(a separator '--')", "GROUP_CONCAT(`a` SEPARATOR '--')"},
		{"GROUP_CONCAT(a order by b desc, c)", "GROUP_CONCAT(`a` ORDER BY `b` DESC,`c` SEPARATOR ',')"},
		{"GROUP_CONCAT(a order by b desc, c separator '--')", "GROUP_CONCAT(`a` ORDER BY `b` DESC,`c` SEPARATOR '--')"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (ts *testFunctionsSuite) TestConvert(c *C) {
	// Test case for CONVERT(expr USING transcoding_name).
	cases := []struct {
		SQL          string
		CharsetName  string
		ErrorMessage string
	}{
		{`SELECT CONVERT("abc" USING "latin1")`, "latin1", ""},
		{`SELECT CONVERT("abc" USING laTiN1)`, "latin1", ""},
		{`SELECT CONVERT("abc" USING "binary")`, "binary", ""},
		{`SELECT CONVERT("abc" USING biNaRy)`, "binary", ""},
		{`SELECT CONVERT(a USING a)`, "", `[parser:1115]Unknown character set: 'a'`}, // TiDB issue #4436.
		{`SELECT CONVERT("abc" USING CONCAT("utf", "8"))`, "", `[parser:1115]Unknown character set: 'CONCAT'`},
	}
	for _, testCase := range cases {
		stmt, err := parser.New().ParseOneStmt(testCase.SQL, "", "")
		if testCase.ErrorMessage != "" {
			c.Assert(err.Error(), Equals, testCase.ErrorMessage)
			continue
		}
		c.Assert(err, IsNil)

		st := stmt.(*SelectStmt)
		expr := st.Fields.Fields[0].Expr.(*FuncCallExpr)
		charsetArg := expr.Args[1].(*test_driver.ValueExpr)
		c.Assert(charsetArg.GetString(), Equals, testCase.CharsetName)
	}
}

func (ts *testFunctionsSuite) TestChar(c *C) {
	// Test case for CHAR(N USING charset_name)
	cases := []struct {
		SQL          string
		CharsetName  string
		ErrorMessage string
	}{
		{`SELECT CHAR("abc" USING "latin1")`, "latin1", ""},
		{`SELECT CHAR("abc" USING laTiN1)`, "latin1", ""},
		{`SELECT CHAR("abc" USING "binary")`, "binary", ""},
		{`SELECT CHAR("abc" USING binary)`, "binary", ""},
		{`SELECT CHAR(a USING a)`, "", `[parser:1115]Unknown character set: 'a'`},
		{`SELECT CHAR("abc" USING CONCAT("utf", "8"))`, "", `[parser:1115]Unknown character set: 'CONCAT'`},
	}
	for _, testCase := range cases {
		stmt, err := parser.New().ParseOneStmt(testCase.SQL, "", "")
		if testCase.ErrorMessage != "" {
			c.Assert(err.Error(), Equals, testCase.ErrorMessage)
			continue
		}
		c.Assert(err, IsNil)

		st := stmt.(*SelectStmt)
		expr := st.Fields.Fields[0].Expr.(*FuncCallExpr)
		charsetArg := expr.Args[1].(*test_driver.ValueExpr)
		c.Assert(charsetArg.GetString(), Equals, testCase.CharsetName)
	}
}

func (ts *testDMLSuite) TestWindowFuncExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"RANK() OVER w", "RANK() OVER `w`"},
		{"RANK() OVER (PARTITION BY a)", "RANK() OVER (PARTITION BY `a`)"},
		{"MAX(DISTINCT a) OVER (PARTITION BY a)", "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)"},
		{"MAX(DISTINCTROW a) OVER (PARTITION BY a)", "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)"},
		{"MAX(DISTINCT ALL a) OVER (PARTITION BY a)", "MAX(DISTINCT `a`) OVER (PARTITION BY `a`)"},
		{"MAX(ALL a) OVER (PARTITION BY a)", "MAX(`a`) OVER (PARTITION BY `a`)"},
		{"FIRST_VALUE(val) IGNORE NULLS OVER (w)", "FIRST_VALUE(`val`) IGNORE NULLS OVER (`w`)"},
		{"FIRST_VALUE(val) RESPECT NULLS OVER w", "FIRST_VALUE(`val`) OVER `w`"},
		{"NTH_VALUE(val, 233) FROM LAST IGNORE NULLS OVER w", "NTH_VALUE(`val`, 233) FROM LAST IGNORE NULLS OVER `w`"},
		{"NTH_VALUE(val, 233) FROM FIRST IGNORE NULLS OVER (w)", "NTH_VALUE(`val`, 233) IGNORE NULLS OVER (`w`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s from t", extractNodeFunc)
}

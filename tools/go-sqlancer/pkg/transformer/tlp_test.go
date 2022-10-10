// Copyright 2022 PingCAP, Inc.
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

package transformer

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	"github.com/stretchr/testify/assert"
)

type (
	TLPTestCase struct {
		TestCase
		tp   TLPType
		expr string
	}
)

var (
	TLPTestCases = []TLPTestCase{
		{
			// normal transform in where-clause
			tp:   WHERE,
			expr: "t.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t",
				expect: "SELECT * FROM t WHERE t.c IS TRUE " +
					"UNION ALL SELECT * FROM t WHERE t.c IS FALSE " +
					"UNION ALL SELECT * FROM t WHERE t.c IS NULL",
			},
		},
		{
			// transform in on-condition without join, should fail
			tp:   OnCondition,
			expr: "t.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT * FROM t",
				expect: "",
			},
		},
		{
			// transform in having-clause without group by, should fail
			tp:   HAVING,
			expr: "t.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT * FROM t",
				expect: "",
			},
		},
		{
			// normal transform in where-clause
			tp:   WHERE,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1",
				expect: "SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS TRUE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS FALSE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS NULL",
			},
		},
		{
			// normal transform in on-condition
			tp:   OnCondition,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1",
				expect: "SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS TRUE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS FALSE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS NULL",
			},
		},

		{
			// transform in on-condition after outer join, should fail
			tp:   OnCondition,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT * FROM (t0 JOIN t1 RIGHT JOIN t2 ON true) JOIN t3",
				expect: "",
			},
		},

		{
			// transform in having-clause without group by, should fail
			tp:   HAVING,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT * FROM t0 JOIN t1",
				expect: "",
			},
		},
		{
			// normal transform in where-clause
			tp:   WHERE,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
				expect: "SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS TRUE GROUP BY t0.c " +
					"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS FALSE GROUP BY t0.c " +
					"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS NULL GROUP BY t0.c",
			},
		},

		{
			// normal transform in on-condition
			tp:   OnCondition,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
				expect: "SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS TRUE GROUP BY t0.c " +
					"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS FALSE GROUP BY t0.c " +
					"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS NULL GROUP BY t0.c",
			},
		},
		{
			// normal transform in having-clause
			tp:   HAVING,
			expr: "SUM(t0.c) > 1",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
				expect: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS TRUE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS FALSE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS NULL",
			},
		},
		{
			// normal transform with distinct
			tp:   WHERE,
			expr: "t.c",
			TestCase: TestCase{
				origin: "SELECT DISTINCT * FROM t",
				expect: "SELECT * FROM t WHERE t.c IS TRUE " +
					"UNION SELECT * FROM t WHERE t.c IS FALSE " +
					"UNION SELECT * FROM t WHERE t.c IS NULL",
			},
		},
		{
			// normal transform with aggregate functions
			tp:   WHERE,
			expr: "t.c",
			TestCase: TestCase{
				origin: "SELECT MAX(c), MIN(c), SUM(c) FROM t",
				expect: "SELECT MAX(tmp.c0), MIN(tmp.c1), SUM(tmp.c2) FROM " +
					"(" +
					"SELECT MAX(c) as c0, MIN(c) as c1, SUM(c) as c2 FROM t WHERE t.c IS TRUE " +
					"UNION ALL SELECT MAX(c) as c0, MIN(c) as c1, SUM(c) as c2 FROM t WHERE t.c IS FALSE " +
					"UNION ALL SELECT MAX(c) as c0, MIN(c) as c1, SUM(c) as c2 FROM t WHERE t.c IS NULL" +
					")" +
					" as tmp",
			},
		},

		{
			// transform with aggregate functions, partially with normal fields
			tp:   WHERE,
			expr: "t.c",
			TestCase: TestCase{
				origin: "SELECT MAX(c), MIN(c), SUM(c), c FROM t",
				expect: "SELECT MAX(tmp.c0), MIN(tmp.c1), SUM(tmp.c2), tmp.c3 FROM " +
					"(" +
					"SELECT MAX(c) as c0, MIN(c) as c1, SUM(c) as c2, c as c3 FROM t WHERE t.c IS TRUE " +
					"UNION ALL SELECT MAX(c) as c0, MIN(c) as c1, SUM(c) as c2, c as c3 FROM t WHERE t.c IS FALSE " +
					"UNION ALL SELECT MAX(c) as c0, MIN(c) as c1, SUM(c) as c2, c as c3 FROM t WHERE t.c IS NULL" +
					")" +
					" as tmp",
			},
		},
		{
			// transform with both wildcard selection and aggregate function selection
			tp:   WHERE,
			expr: "t.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT MAX(c), * FROM t",
				expect: "",
			},
		},
		{
			// transform with aggregate function embedded in normal expression
			tp:   WHERE,
			expr: "t.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT IFNULL(SUM(c), 0) FROM t",
				expect: "",
			},
		},
		{
			// transform with aggregate function embedded in normal expression
			tp:   WHERE,
			expr: "t.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT 1 + SUM(c) FROM t",
				expect: "",
			},
		},
	}

	AggregateDetectorTestCases = []TestCase{
		{
			origin: "1",
			fail:   false,
		},
		{
			origin: "1 + true",
			fail:   false,
		},
		{
			origin: "c",
			fail:   false,
		},
		{
			origin: "c+1",
			fail:   false,
		},
		{
			origin: "IFNULL((1 + NULL) IS NULL, true)",
			fail:   false,
		},
		{
			origin: "1 + SUM(c)",
			fail:   true,
		},
		{
			origin: "IFNULL(SUM(c), 0)",
			fail:   true,
		},
	}
)

func TLPTransTest(t *testing.T, parser *parser.Parser, testCase TLPTestCase) {
	exprNode, warns, err := parseExpr(parser, testCase.expr)
	assert.Nil(t, err)
	assert.Empty(t, warns)
	tlpTrans := &TLPTrans{Expr: exprNode, Tp: testCase.tp}
	nodes, warns, err := parser.Parse(testCase.origin, "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.True(t, len(nodes) == 1)
	selectStmt, ok := nodes[0].(*ast.SelectStmt)
	assert.True(t, ok)
	resultSetNode := tlpTrans.Transform([]ast.ResultSetNode{selectStmt})

	if testCase.fail {
		assert.True(t, assert.True(t, len(resultSetNode) == 1))
	} else {
		assert.True(t, len(resultSetNode) >= 2)
		output, err := util.BufferOut(resultSetNode[1])
		assert.Nil(t, err)

		expectNodes, warns, err := parser.Parse(testCase.expect, "", "")
		assert.Nil(t, err)
		assert.Empty(t, warns)
		assert.True(t, len(nodes) == 1)

		expect, err := util.BufferOut(expectNodes[0])
		assert.Nil(t, err)
		assert.Equal(t, expect, output)
	}
}

func TestTLPTrans_Trans(t *testing.T) {
	for _, testCase := range TLPTestCases {
		TLPTransTest(t, parser.New(), testCase)
	}
}

func parseExpr(parser *parser.Parser, expr string) (node ast.ExprNode, warns []error, err error) {
	nodes, warns, err := parser.Parse(fmt.Sprintf("SELECT * FROM t WHERE %s", expr), "", "")
	if err != nil || len(warns) != 0 || len(nodes) == 0 {
		return
	}
	if stmt, ok := nodes[0].(*ast.SelectStmt); ok {
		node = stmt.Where
	}
	return
}

func TestAggregateDetector(t *testing.T) {
	for _, testCase := range AggregateDetectorTestCases {
		testAggregateDetector(t, parser.New(), testCase)
	}
}

func testAggregateDetector(t *testing.T, parser *parser.Parser, testCase TestCase) {
	exprNode, warns, err := parseExpr(parser, testCase.origin)
	assert.Nil(t, err)
	assert.Empty(t, warns)
	detector := AggregateDetector{}
	exprNode.Accept(&detector)
	assert.Equal(t, testCase.fail, detector.detected)
}

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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func parseExpr(t *testing.T, expr string) ast.ExprNode {
	p := parser.New()
	st, err := p.ParseOneStmt("select "+expr, "", "")
	require.NoError(t, err)
	stmt := st.(*ast.SelectStmt)
	return stmt.Fields.Fields[0].Expr
}

type testCase struct {
	exprStr   string
	resultStr string
}

func runTests(t *testing.T, tests []testCase) {
	ctx := MockContext()
	for _, tt := range tests {
		expr := parseExpr(t, tt.exprStr)
		val, err := evalAstExpr(ctx, expr)
		require.NoError(t, err)
		valStr := fmt.Sprintf("%v", val.GetValue())
		require.Equalf(t, tt.resultStr, valStr, "for %s", tt.exprStr)
	}
}

func TestBetween(t *testing.T) {
	tests := []testCase{
		{exprStr: "1 between 2 and 3", resultStr: "0"},
		{exprStr: "1 not between 2 and 3", resultStr: "1"},
		{exprStr: "'2001-04-10 12:34:56' between cast('2001-01-01 01:01:01' as datetime) and '01-05-01'", resultStr: "1"},
		{exprStr: "20010410123456 between cast('2001-01-01 01:01:01' as datetime) and 010501", resultStr: "0"},
		{exprStr: "20010410123456 between cast('2001-01-01 01:01:01' as datetime) and 20010501123456", resultStr: "1"},
	}
	runTests(t, tests)
}

func TestCaseWhen(t *testing.T) {
	tests := []testCase{
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
	runTests(t, tests)

	// When expression value changed, result set back to null.
	valExpr := ast.NewValueExpr(1, "", "")
	whenClause := &ast.WhenClause{Expr: ast.NewValueExpr(1, "", ""), Result: ast.NewValueExpr(1, "", "")}
	caseExpr := &ast.CaseExpr{
		Value:       valExpr,
		WhenClauses: []*ast.WhenClause{whenClause},
	}
	ctx := MockContext()
	v, err := evalAstExpr(ctx, caseExpr)
	require.NoError(t, err)
	require.Equal(t, types.NewDatum(int64(1)), v)
	valExpr.SetValue(4)
	v, err = evalAstExpr(ctx, caseExpr)
	require.NoError(t, err)
	require.Equal(t, types.KindNull, v.Kind())
}

func TestCast(t *testing.T) {
	f := types.NewFieldType(mysql.TypeLonglong)

	expr := &ast.FuncCastExpr{
		Expr: ast.NewValueExpr(1, "", ""),
		Tp:   f,
	}

	ctx := MockContext()

	ast.SetFlag(expr)
	v, err := evalAstExpr(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, types.NewDatum(int64(1)), v)

	f.AddFlag(mysql.UnsignedFlag)
	v, err = evalAstExpr(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, types.NewDatum(uint64(1)), v)

	f.SetType(mysql.TypeString)
	f.SetCharset(charset.CharsetBin)
	v, err = evalAstExpr(ctx, expr)
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum([]byte("1")), v)

	f.SetType(mysql.TypeString)
	f.SetCharset(charset.CharsetUTF8)
	v, err = evalAstExpr(ctx, expr)
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum([]byte("1")), v)

	expr.Expr = ast.NewValueExpr(nil, "", "")
	v, err = evalAstExpr(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, types.KindNull, v.Kind())
}

func TestPatternIn(t *testing.T) {
	tests := []testCase{
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
	runTests(t, tests)
}

func TestIsNull(t *testing.T) {
	tests := []testCase{
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
	runTests(t, tests)
}

func TestCompareRow(t *testing.T) {
	tests := []testCase{
		{
			exprStr:   "row(1,2,3)=row(1,2,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)=row(1+3,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<>row(1,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<>row(1+3,2,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1+3,2,3)<>row(1+3,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<row(1,NULL,3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "row(1,2,3)<row(2,NULL,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)>=row(0,NULL,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)<=row(2,NULL,3)",
			resultStr: "1",
		},
	}
	runTests(t, tests)
}

func TestIsTruth(t *testing.T) {
	tests := []testCase{
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
	runTests(t, tests)
}

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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ranger_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestTableRange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, c int unsigned)")

	tests := []struct {
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			exprStr:     "a = 1",
			accessConds: "[eq(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
		},
		{
			exprStr:     "1 = a",
			accessConds: "[eq(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
		},
		{
			exprStr:     "a != 1",
			accessConds: "[ne(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
		},
		{
			exprStr:     "1 != a",
			accessConds: "[ne(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
		},
		{
			exprStr:     "a > 1",
			accessConds: "[gt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
		},
		{
			exprStr:     "1 < a",
			accessConds: "[lt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
		},
		{
			exprStr:     "a >= 1",
			accessConds: "[ge(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
		},
		{
			exprStr:     "1 <= a",
			accessConds: "[le(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
		},
		{
			exprStr:     "a < 1",
			accessConds: "[lt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
		},
		{
			exprStr:     "1 > a",
			accessConds: "[gt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
		},
		{
			exprStr:     "a <= 1",
			accessConds: "[le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
		},
		{
			exprStr:     "1 >= test.t.a",
			accessConds: "[ge(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
		},
		{
			exprStr:     "(a)",
			accessConds: "[test.t.a]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
		},
		{
			exprStr:     "a in (1, 3, NULL, 2)",
			accessConds: "[in(test.t.a, 1, 3, <nil>, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3]]",
		},
		{
			exprStr:     `a IN (8,8,81,45)`,
			accessConds: "[in(test.t.a, 8, 8, 81, 45)]",
			filterConds: "[]",
			resultStr:   `[[8,8] [45,45] [81,81]]`,
		},
		{
			exprStr:     "a between 1 and 2",
			accessConds: "[ge(test.t.a, 1) le(test.t.a, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,2]]",
		},
		{
			exprStr:     "a not between 1 and 2",
			accessConds: "[or(lt(test.t.a, 1), gt(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (2,+inf]]",
		},
		{
			exprStr:     "a between 2 and 1",
			accessConds: "[ge(test.t.a, 2) le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			exprStr:     "a not between 2 and 1",
			accessConds: "[or(lt(test.t.a, 2), gt(test.t.a, 1))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			exprStr:     "a IS NULL",
			accessConds: "[isnull(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			exprStr:     "a IS NOT NULL",
			accessConds: "[not(isnull(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			exprStr:     "a IS TRUE",
			accessConds: "[istrue(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
		},
		{
			exprStr:     "a IS NOT TRUE",
			accessConds: "[not(istrue(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[0,0]]",
		},
		{
			exprStr:     "a IS FALSE",
			accessConds: "[isfalse(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[0,0]]",
		},
		{
			exprStr:     "a IS NOT FALSE",
			accessConds: "[not(isfalse(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
		},
		{
			exprStr:     "a = 1 or a = 3 or a = 4 or (a > 1 and (a = -1 or a = 5))",
			accessConds: "[or(or(eq(test.t.a, 1), eq(test.t.a, 3)), or(eq(test.t.a, 4), and(gt(test.t.a, 1), or(eq(test.t.a, -1), eq(test.t.a, 5)))))]",
			filterConds: "[]",
			resultStr:   "[[1,1] [3,3] [4,4] [5,5]]",
		},
		{
			exprStr:     "(a = 1 and b = 1) or (a = 2 and b = 2)",
			accessConds: "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
			filterConds: "[or(and(eq(test.t.a, 1), eq(test.t.b, 1)), and(eq(test.t.a, 2), eq(test.t.b, 2)))]",
			resultStr:   "[[1,1] [2,2]]",
		},
		{
			exprStr:     "a = 1 or a = 3 or a = 4 or (b > 1 and (a = -1 or a = 5))",
			accessConds: "[or(or(eq(test.t.a, 1), eq(test.t.a, 3)), or(eq(test.t.a, 4), or(eq(test.t.a, -1), eq(test.t.a, 5))))]",
			filterConds: "[or(or(or(eq(test.t.a, 1), eq(test.t.a, 3)), eq(test.t.a, 4)), and(gt(test.t.b, 1), or(eq(test.t.a, -1), eq(test.t.a, 5))))]",
			resultStr:   "[[-1,-1] [1,1] [3,3] [4,4] [5,5]]",
		},
		{
			exprStr:     "a in (1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)",
			accessConds: "[in(test.t.a, 1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3] [4,4]]",
		},
		{
			exprStr:     "a not in (1, 2, 3)",
			accessConds: "[not(in(test.t.a, 1, 2, 3))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (3,+inf]]",
		},
		{
			exprStr:     "a > 9223372036854775807",
			accessConds: "[gt(test.t.a, 9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			exprStr:     "a >= 9223372036854775807",
			accessConds: "[ge(test.t.a, 9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[[9223372036854775807,+inf]]",
		},
		{
			exprStr:     "a < -9223372036854775807",
			accessConds: "[lt(test.t.a, -9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[[-inf,-9223372036854775807)]",
		},
		{
			exprStr:     "a < -9223372036854775808",
			accessConds: "[lt(test.t.a, -9223372036854775808)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.exprStr, func(t *testing.T) {
			sql := "select * from t where " + tt.exprStr
			sctx := testKit.Session().(sessionctx.Context)
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx, cond)
			}
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			col := expression.ColInfo2Col(selection.Schema().Columns, tbl.Columns[0])
			require.NotNil(t, col)
			var filter []expression.Expression
			conds, filter = ranger.DetachCondsForColumn(sctx, conds, col)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", conds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", filter))
			result, err := ranger.BuildTableRange(conds, sctx, col.RetType)
			require.NoError(t, err)
			got := fmt.Sprintf("%v", result)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

// for issue #6661
func TestIndexRangeForUnsignedAndOverflow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
create table t(
	a smallint(5) unsigned,
	decimal_unsigned decimal unsigned,
	float_unsigned float unsigned,
	double_unsigned double unsigned,
	col_int bigint,
	col_float float,
	index idx_a(a),
	index idx_decimal_unsigned(decimal_unsigned),
	index idx_float_unsigned(float_unsigned),
	index idx_double_unsigned(double_unsigned),
	index idx_int(col_int),
	index idx_float(col_float)
)`)
	tests := []struct {
		indexPos    int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			indexPos:    0,
			exprStr:     `a not in (0, 1, 2)`,
			accessConds: "[not(in(test.t.a, 0, 1, 2))]",
			filterConds: "[]",
			resultStr:   `[(NULL,0) (2,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (-1, 1, 2)`,
			accessConds: "[not(in(test.t.a, -1, 1, 2))]",
			filterConds: "[]",
			resultStr:   `[(NULL,1) (2,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (-2, -1, 1, 2)`,
			accessConds: "[not(in(test.t.a, -2, -1, 1, 2))]",
			filterConds: "[]",
			resultStr:   `[(NULL,1) (2,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (111)`,
			accessConds: "[ne(test.t.a, 111)]",
			filterConds: "[]",
			resultStr:   `[[-inf,111) (111,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (1, 2, 9223372036854775810)`,
			accessConds: "[not(in(test.t.a, 1, 2, 9223372036854775810))]",
			filterConds: "[]",
			resultStr:   `[(NULL,1) (2,9223372036854775810) (9223372036854775810,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a >= -2147483648`,
			accessConds: "[ge(test.t.a, -2147483648)]",
			filterConds: "[]",
			resultStr:   `[[0,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a > -2147483648`,
			accessConds: "[gt(test.t.a, -2147483648)]",
			filterConds: "[]",
			resultStr:   `[[0,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a != -2147483648`,
			accessConds: "[ne(test.t.a, -2147483648)]",
			filterConds: "[]",
			resultStr:   `[[0,+inf]]`,
		},
		{
			exprStr:     "a < -1 or a < 1",
			accessConds: "[or(lt(test.t.a, -1), lt(test.t.a, 1))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
		},
		{
			exprStr:     "a < -1 and a < 1",
			accessConds: "[]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    1,
			exprStr:     "decimal_unsigned > -100",
			accessConds: "[gt(test.t.decimal_unsigned, -100)]",
			filterConds: "[]",
			resultStr:   "[[0,+inf]]",
		},
		{
			indexPos:    2,
			exprStr:     "float_unsigned > -100",
			accessConds: "[gt(test.t.float_unsigned, -100)]",
			filterConds: "[]",
			resultStr:   "[[0,+inf]]",
		},
		{
			indexPos:    3,
			exprStr:     "double_unsigned > -100",
			accessConds: "[gt(test.t.double_unsigned, -100)]",
			filterConds: "[]",
			resultStr:   "[[0,+inf]]",
		},
		// test for overflow value access index
		{
			indexPos:    4,
			exprStr:     "col_int != 9223372036854775808",
			accessConds: "[ne(test.t.col_int, 9223372036854775808)]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			indexPos:    4,
			exprStr:     "col_int > 9223372036854775808",
			accessConds: "[gt(test.t.col_int, 9223372036854775808)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    4,
			exprStr:     "col_int < 9223372036854775808",
			accessConds: "[lt(test.t.col_int, 9223372036854775808)]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			indexPos:    5,
			exprStr:     "col_float > 1000000000000000000000000000000000000000",
			accessConds: "[gt(test.t.col_float, 1e+39)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    5,
			exprStr:     "col_float < -1000000000000000000000000000000000000000",
			accessConds: "[lt(test.t.col_float, -1e+39)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.exprStr, func(t *testing.T) {
			sql := "select * from t where " + tt.exprStr
			sctx := testKit.Session().(sessionctx.Context)
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			require.NotNil(t, selection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx, cond)
			}
			cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
			require.NotNil(t, cols)
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, conds, cols, lengths)
			require.NoError(t, err)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", res.AccessConds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", res.RemainedConds))
			got := fmt.Sprintf("%v", res.Ranges)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

func TestColumnRange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b double, c float(3, 2), d varchar(3), e bigint unsigned)")

	tests := []struct {
		colPos      int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
		length      int
	}{
		{
			colPos:      0,
			exprStr:     "(a = 2 or a = 2) and (a = 2 or a = 2)",
			accessConds: "[or(eq(test.t.a, 2), eq(test.t.a, 2)) or(eq(test.t.a, 2), eq(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[2,2]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "(a = 2 or a = 1) and (a = 3 or a = 4)",
			accessConds: "[or(eq(test.t.a, 2), eq(test.t.a, 1)) or(eq(test.t.a, 3), eq(test.t.a, 4))]",
			filterConds: "[]",
			resultStr:   "[]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a = 1 and b > 1",
			accessConds: "[eq(test.t.a, 1)]",
			filterConds: "[gt(test.t.b, 1)]",
			resultStr:   "[[1,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      1,
			exprStr:     "b > 1",
			accessConds: "[gt(test.t.b, 1)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "1 = a",
			accessConds: "[eq(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a != 1",
			accessConds: "[ne(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "1 != a",
			accessConds: "[ne(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a > 1",
			accessConds: "[gt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "1 < a",
			accessConds: "[lt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a >= 1",
			accessConds: "[ge(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "1 <= a",
			accessConds: "[le(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a < 1",
			accessConds: "[lt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "1 > a",
			accessConds: "[gt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a <= 1",
			accessConds: "[le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "1 >= a",
			accessConds: "[ge(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "(a)",
			accessConds: "[test.t.a]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a in (1, 3, NULL, 2)",
			accessConds: "[in(test.t.a, 1, 3, <nil>, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     `a IN (8,8,81,45)`,
			accessConds: "[in(test.t.a, 8, 8, 81, 45)]",
			filterConds: "[]",
			resultStr:   `[[8,8] [45,45] [81,81]]`,
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a between 1 and 2",
			accessConds: "[ge(test.t.a, 1) le(test.t.a, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,2]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a not between 1 and 2",
			accessConds: "[or(lt(test.t.a, 1), gt(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (2,+inf]]",
			length:      types.UnspecifiedLength,
		},
		// {
		//  `a > null` will be converted to `castAsString(a) > null` which can not be extracted as access condition.
		// 	exprStr:   "a not between null and 0",
		// 	resultStr[(0,+inf]]
		// },
		{
			colPos:      0,
			exprStr:     "a between 2 and 1",
			accessConds: "[ge(test.t.a, 2) le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a not between 2 and 1",
			accessConds: "[or(lt(test.t.a, 2), gt(test.t.a, 1))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a IS NULL",
			accessConds: "[isnull(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a IS NOT NULL",
			accessConds: "[not(isnull(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a IS TRUE",
			accessConds: "[istrue(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a IS NOT TRUE",
			accessConds: "[not(istrue(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL] [0,0]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a IS FALSE",
			accessConds: "[isfalse(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[0,0]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a IS NOT FALSE",
			accessConds: "[not(isfalse(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[NULL,0) (0,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      1,
			exprStr:     `b in (1, '2.1')`,
			accessConds: "[in(test.t.b, 1, 2.1)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2.1,2.1]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     `a > 9223372036854775807`,
			accessConds: "[gt(test.t.a, 9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[(9223372036854775807,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      2,
			exprStr:     `c > 111.11111111`,
			accessConds: "[gt(test.t.c, 111.11111111)]",
			filterConds: "[]",
			resultStr:   "[[111.111115,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      3,
			exprStr:     `d > 'aaaaaaaaaaaaaa'`,
			accessConds: "[gt(test.t.d, aaaaaaaaaaaaaa)]",
			filterConds: "[]",
			resultStr:   "[(\"aaaaaaaaaaaaaa\",+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      4,
			exprStr:     `e > 18446744073709500000`,
			accessConds: "[gt(test.t.e, 18446744073709500000)]",
			filterConds: "[]",
			resultStr:   "[(18446744073709500000,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      4,
			exprStr:     `e > -2147483648`,
			accessConds: "[gt(test.t.e, -2147483648)]",
			filterConds: "[]",
			resultStr:   "[[0,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      3,
			exprStr:     "d = 'aab' or d = 'aac'",
			accessConds: "[or(eq(test.t.d, aab), eq(test.t.d, aac))]",
			filterConds: "[]",
			resultStr:   "[[\"a\",\"a\"]]",
			length:      1,
		},
		// This test case cannot be simplified to [1, 3] otherwise the index join will executes wrongly.
		{
			colPos:      0,
			exprStr:     "a in (1, 2, 3)",
			accessConds: "[in(test.t.a, 1, 2, 3)]",
			filterConds: "",
			resultStr:   "[[1,1] [2,2] [3,3]]",
			length:      types.UnspecifiedLength,
		},
		// test cases for nulleq
		{
			colPos:      0,
			exprStr:     "a <=> 1",
			accessConds: "[nulleq(test.t.a, 1)]",
			filterConds: "",
			resultStr:   "[[1,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			colPos:      0,
			exprStr:     "a <=> null",
			accessConds: "[nulleq(test.t.a, <nil>)]",
			filterConds: "",
			resultStr:   "[[NULL,NULL]]",
			length:      types.UnspecifiedLength,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.exprStr, func(t *testing.T) {
			sql := "select * from t where " + tt.exprStr
			sctx := testKit.Session().(sessionctx.Context)
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			sel := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			ds, ok := sel.Children()[0].(*plannercore.DataSource)
			require.True(t, ok)
			conds := make([]expression.Expression, len(sel.Conditions))
			for i, cond := range sel.Conditions {
				conds[i] = expression.PushDownNot(sctx, cond)
			}
			col := expression.ColInfo2Col(sel.Schema().Columns, ds.TableInfo().Columns[tt.colPos])
			require.NotNil(t, col)
			conds = ranger.ExtractAccessConditionsForColumn(conds, col)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", conds))
			result, err := ranger.BuildColumnRange(conds, sctx, col.RetType, tt.length)
			require.NoError(t, err)
			got := fmt.Sprintf("%v", result)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

func TestIndexRangeEliminatedProjection(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	testKit.MustExec("create table t(a int not null, b int not null, primary key(a,b))")
	testKit.MustExec("insert into t values(1,2)")
	testKit.MustExec("analyze table t")
	testKit.MustQuery("explain format = 'brief' select * from (select * from t union all select ifnull(a,b), b from t) sub where a > 0").Check(testkit.Rows(
		"Union 2.00 root  ",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, index:PRIMARY(a, b) range:(0,+inf], keep order:false",
		"└─IndexReader 1.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 1.00 cop[tikv] table:t, index:PRIMARY(a, b) range:(0,+inf], keep order:false",
	))
	testKit.MustQuery("select * from (select * from t union all select ifnull(a,b), b from t) sub where a > 0").Check(testkit.Rows(
		"1 2",
		"1 2",
	))
}

func TestCompIndexInExprCorrCol(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int primary key, b int, c int, d int, e int, index idx(b,c,d))")
	testKit.MustExec("insert into t values(1,1,1,1,2),(2,1,2,1,0)")
	testKit.MustExec("analyze table t")

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIndexStringIsTrueRange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t0")
	testKit.MustExec("CREATE TABLE t0(c0 TEXT(10));")
	testKit.MustExec("INSERT INTO t0(c0) VALUES (1);")
	testKit.MustExec("CREATE INDEX i0 ON t0(c0(255));")
	testKit.MustExec("analyze table t0;")

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestCompIndexDNFMatch(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec(`set @@session.tidb_regard_null_as_point=false`)
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, c int, key(a,b,c));")
	testKit.MustExec("insert into t values(1,2,2)")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestCompIndexMultiColDNF1(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a,b));")
	testKit.MustExec("insert into t values(1,1,1),(2,2,3)")
	testKit.MustExec("analyze table t")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestCompIndexMultiColDNF2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b int, c int, primary key(a,b,c));")
	testKit.MustExec("insert into t values(1,1,1),(2,2,3)")
	testKit.MustExec("analyze table t")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestPrefixIndexMultiColDNF(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test;")
	testKit.MustExec("drop table if exists t2;")
	testKit.MustExec("create table t2 (id int unsigned not null auto_increment primary key, t text, index(t(3)));")
	testKit.MustExec("insert into t2 (t) values ('aaaa'),('a');")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	inputLen := len(input)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
		if i+1 == inputLen/2 {
			testKit.MustExec("analyze table t2;")
		}
	}
}

func TestIndexRangeForBit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test;")
	testKit.MustExec("set @@tidb_partition_prune_mode = 'static';")
	testKit.MustExec("set @@tidb_executor_concurrency = 1;")
	testKit.MustExec("drop table if exists t;")
	testKit.MustExec("CREATE TABLE `t` (" +
		"a bit(1) DEFAULT NULL," +
		"b int(11) DEFAULT NULL" +
		") PARTITION BY HASH(a)" +
		"PARTITIONS 3;")
	testKit.MustExec("insert ignore into t values(-1, -1), (0, 0), (1, 1), (3, 3);")
	testKit.MustExec("analyze table t;")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIndexRangeForYear(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)

	// for issue #20101: overflow when converting integer to year
	testKit.MustExec("use test")
	testKit.MustExec("DROP TABLE IF EXISTS `table_30_utf8_undef`")
	testKit.MustExec("CREATE TABLE `table_30_utf8_undef` (\n  `pk` int(11) NOT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
	testKit.MustExec("INSERT INTO `table_30_utf8_undef` VALUES (29)")

	testKit.MustExec("DROP TABLE IF EXISTS `table_40_utf8_4`")
	testKit.MustExec("CREATE TABLE `table_40_utf8_4`(\n  `pk` int(11) NOT NULL,\n  `col_int_key_unsigned` int(10) unsigned DEFAULT NULL,\n  `col_year_key_signed` year(4) DEFAULT NULL,\n" +
		"PRIMARY KEY (`pk`),\n  KEY `col_int_key_unsigned` (`col_int_key_unsigned`),\n  KEY `col_year_key_signed` (`col_year_key_signed`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")

	testKit.MustExec("INSERT INTO `table_40_utf8_4` VALUES (36, 10 ,1)")

	testKit.MustQuery("SELECT sum(tmp.val) AS val FROM (" +
		"SELECT count(1) AS val FROM table_40_utf8_4 JOIN table_30_utf8_undef\n" +
		"WHERE table_40_utf8_4.col_year_key_signed!=table_40_utf8_4.col_int_key_unsigned\n" +
		"AND table_40_utf8_4.col_int_key_unsigned=\"15698\") AS tmp").
		Check(testkit.Rows("0"))

	// test index range
	testKit.MustExec("DROP TABLE IF EXISTS t")
	testKit.MustExec("CREATE TABLE t (a year(4), key(a))")
	testKit.MustExec("INSERT INTO t VALUES (1), (70), (99), (0), ('0'), (NULL)")
	testKit.MustQuery("SELECT * FROM t WHERE a < 15698").Check(testkit.Rows("0", "1970", "1999", "2000", "2001"))
	testKit.MustQuery("SELECT * FROM t WHERE a <= 0").Check(testkit.Rows("0"))
	testKit.MustQuery("SELECT * FROM t WHERE a <= 1").Check(testkit.Rows("0", "1970", "1999", "2000", "2001"))
	testKit.MustQuery("SELECT * FROM t WHERE a < 2000").Check(testkit.Rows("0", "1970", "1999"))
	testKit.MustQuery("SELECT * FROM t WHERE a > -1").Check(testkit.Rows("0", "1970", "1999", "2000", "2001"))
	testKit.MustQuery("SELECT * FROM t WHERE a <=> NULL").Check(testkit.Rows("<nil>"))

	tests := []struct {
		indexPos    int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			indexPos:    0,
			exprStr:     `a not in (0, 1, 2)`,
			accessConds: "[not(in(test.t.a, 0, 1, 2))]",
			filterConds: "[]",
			resultStr:   `[(NULL,0) (0,2001) (2002,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (-1, 1, 2)`,
			accessConds: "[not(in(test.t.a, -1, 1, 2))]",
			filterConds: "[]",
			resultStr:   `[(NULL,2001) (2002,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (1, 2, 70)`,
			accessConds: "[not(in(test.t.a, 1, 2, 70))]", // this is in accordance with MySQL, MySQL won't interpret 70 here as 1970
			filterConds: "[]",
			resultStr:   `[(NULL,1970) (1970,2001) (2002,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a = 1 or a = 2 or a = 70`,
			accessConds: "[or(eq(test.t.a, 2001), or(eq(test.t.a, 2002), eq(test.t.a, 1970)))]", // this is in accordance with MySQL, MySQL won't interpret 70 here as 1970
			filterConds: "[]",
			resultStr:   `[[1970,1970] [2001,2002]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (99)`,
			accessConds: "[ne(test.t.a, 1999)]", // this is in accordance with MySQL
			filterConds: "[]",
			resultStr:   `[[-inf,1999) (1999,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (1, 2, 15698)`,
			accessConds: "[not(in(test.t.a, 1, 2, 15698))]",
			filterConds: "[]",
			resultStr:   `[(NULL,2001) (2002,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a >= -1000`,
			accessConds: "[ge(test.t.a, -1000)]",
			filterConds: "[]",
			resultStr:   `[[0,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a > -1000`,
			accessConds: "[gt(test.t.a, -1000)]",
			filterConds: "[]",
			resultStr:   `[[0,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a != 1`,
			accessConds: "[ne(test.t.a, 2001)]",
			filterConds: "[]",
			resultStr:   `[[-inf,2001) (2001,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a != 2156`,
			accessConds: "[ne(test.t.a, 2156)]",
			filterConds: "[]",
			resultStr:   `[[-inf,+inf]]`,
		},
		{
			exprStr:     "a < 99 or a > 01",
			accessConds: "[or(lt(test.t.a, 1999), gt(test.t.a, 2001))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1999) (2001,+inf]]",
		},
		{
			exprStr:     "a >= 70 and a <= 69",
			accessConds: "[ge(test.t.a, 1970) le(test.t.a, 2069)]",
			filterConds: "[]",
			resultStr:   "[[1970,2069]]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.exprStr, func(t *testing.T) {
			sql := "select * from t where " + tt.exprStr
			sctx := testKit.Session().(sessionctx.Context)
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			require.NotNil(t, selection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx, cond)
			}
			cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
			require.NotNil(t, cols)
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, conds, cols, lengths)
			require.NoError(t, err)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", res.AccessConds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", res.RemainedConds))
			got := fmt.Sprintf("%v", res.Ranges)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

// For https://github.com/pingcap/tidb/issues/22032
func TestPrefixIndexRangeScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)

	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a varchar(50), b varchar(50), index idx_a(a(2)), index idx_ab(a(2), b(2)))")
	testKit.MustExec("insert into t values ('aa', 'bb'), ('aaa', 'bbb')")
	testKit.MustQuery("select * from t use index (idx_a) where a > 'aa'").Check(testkit.Rows("aaa bbb"))
	testKit.MustQuery("select * from t use index (idx_ab) where a = 'aaa' and b > 'bb' and b < 'cc'").Check(testkit.Rows("aaa bbb"))

	tests := []struct {
		indexPos    int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			indexPos:    0,
			exprStr:     "a > 'aa'",
			accessConds: "[gt(test.t.a, aa)]",
			filterConds: "[gt(test.t.a, aa)]",
			resultStr:   "[[\"aa\",+inf]]",
		},
		{
			indexPos:    1,
			exprStr:     "a = 'aaa' and b > 'bb' and b < 'cc'",
			accessConds: "[eq(test.t.a, aaa) gt(test.t.b, bb) lt(test.t.b, cc)]",
			filterConds: "[eq(test.t.a, aaa) gt(test.t.b, bb) lt(test.t.b, cc)]",
			resultStr:   "[[\"aa\" \"bb\",\"aa\" \"cc\")]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.exprStr, func(t *testing.T) {
			sql := "select * from t where " + tt.exprStr
			sctx := testKit.Session().(sessionctx.Context)
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			require.NotNil(t, selection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx, cond)
			}
			cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
			require.NotNil(t, cols)
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, conds, cols, lengths)
			require.NoError(t, err)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", res.AccessConds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", res.RemainedConds))
			got := fmt.Sprintf("%v", res.Ranges)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

func TestIndexRangeForDecimal(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test;")
	testKit.MustExec("drop table if exists t1, t2;")
	testKit.MustExec("create table t1(a decimal unsigned, key(a));")
	testKit.MustExec("insert into t1 values(0),(null);")
	testKit.MustExec("create table t2(a int, b decimal unsigned, key idx(a,b));")
	testKit.MustExec("insert into t2 values(1,0),(1,null);")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestPrefixIndexAppendPointRanges(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("USE test")
	testKit.MustExec("DROP TABLE IF EXISTS IDT_20755")
	testKit.MustExec("CREATE TABLE `IDT_20755` (\n" +
		"  `COL1` varchar(20) DEFAULT NULL,\n" +
		"  `COL2` tinyint(16) DEFAULT NULL,\n" +
		"  `COL3` timestamp NULL DEFAULT NULL,\n" +
		"  KEY `u_m_col` (`COL1`(10),`COL2`,`COL3`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	testKit.MustExec("INSERT INTO IDT_20755 VALUES(\"牾窓螎刳闌蜹瑦詬鍖湪槢壿玟瞏膍敗特森撇縆\", 73, \"2010-06-03 07:29:05\")")
	testKit.MustExec("INSERT INTO IDT_20755 VALUES(\"xxxxxxxxxxxxxxx\", 73, \"2010-06-03 07:29:05\")")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	rangerSuiteData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIndexRange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec(`
create table t(
	a varchar(50),
	b int,
	c double,
	d varchar(10),
	e binary(10),
	f varchar(10) collate utf8mb4_general_ci,
	g enum('A','B','C') collate utf8mb4_general_ci,
	h varchar(10) collate utf8_bin,
	index idx_ab(a(50), b),
	index idx_cb(c, a),
	index idx_d(d(2)),
	index idx_e(e(2)),
	index idx_f(f),
	index idx_de(d(2), e),
	index idx_g(g),
	index idx_h(h(3))
)`)

	tests := []struct {
		indexPos    int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			indexPos:    0,
			exprStr:     `a LIKE 'abc%'`,
			accessConds: `[like(test.t.a, abc%, 92)]`,
			filterConds: "[]",
			resultStr:   "[[\"abc\",\"abd\")]",
		},
		{
			indexPos:    0,
			exprStr:     "a LIKE 'abc_'",
			accessConds: "[like(test.t.a, abc_, 92)]",
			filterConds: "[like(test.t.a, abc_, 92)]",
			resultStr:   "[(\"abc\",\"abd\")]",
		},
		{
			indexPos:    0,
			exprStr:     "a LIKE 'abc'",
			accessConds: "[like(test.t.a, abc, 92)]",
			filterConds: "[]",
			resultStr:   "[[\"abc\",\"abc\"]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "ab\_c"`,
			accessConds: "[like(test.t.a, ab\\_c, 92)]",
			filterConds: "[]",
			resultStr:   "[[\"ab_c\",\"ab_c\"]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE '%'`,
			accessConds: "[]",
			filterConds: `[like(test.t.a, %, 92)]`,
			resultStr:   "[[NULL,+inf]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE '\%a'`,
			accessConds: "[like(test.t.a, \\%a, 92)]",
			filterConds: "[]",
			resultStr:   `[["%a","%a"]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "\\"`,
			accessConds: "[like(test.t.a, \\, 92)]",
			filterConds: "[]",
			resultStr:   "[[\"\\\",\"\\\"]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "\\\\a%"`,
			accessConds: `[like(test.t.a, \\a%, 92)]`,
			filterConds: "[]",
			resultStr:   "[[\"\\a\",\"\\b\")]",
		},
		{
			indexPos:    0,
			exprStr:     `a > NULL`,
			accessConds: "[gt(test.t.a, <nil>)]",
			filterConds: "[]",
			resultStr:   `[]`,
		},
		{
			indexPos:    0,
			exprStr:     `a = 'a' and b in (1, 2, 3)`,
			accessConds: "[eq(test.t.a, a) in(test.t.b, 1, 2, 3)]",
			filterConds: "[]",
			resultStr:   "[[\"a\" 1,\"a\" 1] [\"a\" 2,\"a\" 2] [\"a\" 3,\"a\" 3]]",
		},
		{
			indexPos:    0,
			exprStr:     `a = 'a' and b not in (1, 2, 3)`,
			accessConds: "[eq(test.t.a, a) not(in(test.t.b, 1, 2, 3))]",
			filterConds: "[]",
			resultStr:   "[(\"a\" NULL,\"a\" 1) (\"a\" 3,\"a\" +inf]]",
		},
		{
			indexPos:    0,
			exprStr:     `a in ('a') and b in ('1', 2.0, NULL)`,
			accessConds: "[eq(test.t.a, a) in(test.t.b, 1, 2, <nil>)]",
			filterConds: "[]",
			resultStr:   `[["a" 1,"a" 1] ["a" 2,"a" 2]]`,
		},
		{
			indexPos:    1,
			exprStr:     `c in ('1.1', 1, 1.1) and a in ('1', 'a', NULL)`,
			accessConds: "[in(test.t.c, 1.1, 1, 1.1) in(test.t.a, 1, a, <nil>)]",
			filterConds: "[]",
			resultStr:   "[[1 \"1\",1 \"1\"] [1 \"a\",1 \"a\"] [1.1 \"1\",1.1 \"1\"] [1.1 \"a\",1.1 \"a\"]]",
		},
		{
			indexPos:    1,
			exprStr:     "c in (1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)",
			accessConds: "[in(test.t.c, 1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3] [4,4]]",
		},
		{
			indexPos:    1,
			exprStr:     "c not in (1, 2, 3)",
			accessConds: "[not(in(test.t.c, 1, 2, 3))]",
			filterConds: "[]",
			resultStr:   "[(NULL,1) (1,2) (2,3) (3,+inf]]",
		},
		{
			indexPos:    1,
			exprStr:     "c in (1, 2) and c in (1, 3)",
			accessConds: "[eq(test.t.c, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
		},
		{
			indexPos:    1,
			exprStr:     "c = 1 and c = 2",
			accessConds: "[]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "a in (NULL)",
			accessConds: "[eq(test.t.a, <nil>)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "a not in (NULL, '1', '2', '3')",
			accessConds: "[not(in(test.t.a, <nil>, 1, 2, 3))]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "not (a not in (NULL, '1', '2', '3') and a > '2')",
			accessConds: "[or(in(test.t.a, <nil>, 1, 2, 3), le(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,\"2\"] [\"3\",\"3\"]]",
		},
		{
			indexPos:    0,
			exprStr:     "not (a not in (NULL) and a > '2')",
			accessConds: "[or(eq(test.t.a, <nil>), le(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,\"2\"]]",
		},
		{
			indexPos:    0,
			exprStr:     "not (a not in (NULL) or a > '2')",
			accessConds: "[and(eq(test.t.a, <nil>), le(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'b' and a < 'bbb') or (a < 'cb' and a > 'a')",
			accessConds: "[or(and(gt(test.t.a, b), lt(test.t.a, bbb)), and(lt(test.t.a, cb), gt(test.t.a, a)))]",
			filterConds: "[]",
			resultStr:   "[(\"a\",\"cb\")]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'a' and a < 'b') or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(gt(test.t.a, a), lt(test.t.a, b)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[]",
			resultStr:   "[(\"a\",\"c\")]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'a' and a < 'b' and b < 1) or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(gt(test.t.a, a), lt(test.t.a, b)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[or(and(and(gt(test.t.a, a), lt(test.t.a, b)), lt(test.t.b, 1)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			resultStr:   "[(\"a\",\"c\")]",
		},
		{
			indexPos:    0,
			exprStr:     "(a in ('a', 'b') and b < 1) or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(in(test.t.a, a, b), lt(test.t.b, 1)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[]",
			resultStr:   `[["a" -inf,"a" 1) ["b","c")]`,
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'a') or (c > 1)",
			accessConds: "[]",
			filterConds: "[or(gt(test.t.a, a), gt(test.t.c, 1))]",
			resultStr:   "[[NULL,+inf]]",
		},
		{
			indexPos:    2,
			exprStr:     `d = "你好啊"`,
			accessConds: "[eq(test.t.d, 你好啊)]",
			filterConds: "[eq(test.t.d, 你好啊)]",
			resultStr:   "[[\"你好\",\"你好\"]]",
		},
		{
			indexPos:    3,
			exprStr:     `e = "你好啊"`,
			accessConds: "[eq(test.t.e, 你好啊)]",
			filterConds: "[eq(test.t.e, 你好啊)]",
			resultStr:   "[[0xE4BD,0xE4BD]]",
		},
		{
			indexPos:    2,
			exprStr:     `d in ("你好啊", "再见")`,
			accessConds: "[in(test.t.d, 你好啊, 再见)]",
			filterConds: "[in(test.t.d, 你好啊, 再见)]",
			resultStr:   "[[\"你好\",\"你好\"] [\"再见\",\"再见\"]]",
		},
		{
			indexPos:    2,
			exprStr:     `d not in ("你好啊")`,
			accessConds: "[]",
			filterConds: "[ne(test.t.d, 你好啊)]",
			resultStr:   "[[NULL,+inf]]",
		},
		{
			indexPos:    2,
			exprStr:     `d < "你好" || d > "你好"`,
			accessConds: "[or(lt(test.t.d, 你好), gt(test.t.d, 你好))]",
			filterConds: "[or(lt(test.t.d, 你好), gt(test.t.d, 你好))]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			indexPos:    2,
			exprStr:     `not(d < "你好" || d > "你好")`,
			accessConds: "[and(ge(test.t.d, 你好), le(test.t.d, 你好))]",
			filterConds: "[and(ge(test.t.d, 你好), le(test.t.d, 你好))]",
			resultStr:   "[[\"你好\",\"你好\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f >= 'a' and f <= 'B'",
			accessConds: "[ge(test.t.f, a) le(test.t.f, B)]",
			filterConds: "[]",
			resultStr:   "[[\"a\",\"B\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f in ('a', 'B')",
			accessConds: "[in(test.t.f, a, B)]",
			filterConds: "[]",
			resultStr:   "[[\"a\",\"a\"] [\"B\",\"B\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f = 'a' and f = 'B' collate utf8mb4_bin",
			accessConds: "[eq(test.t.f, a)]",
			filterConds: "[eq(test.t.f, B)]",
			resultStr:   "[[\"a\",\"a\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f like '@%' collate utf8mb4_bin",
			accessConds: "[]",
			filterConds: "[like(test.t.f, @%, 92)]",
			resultStr:   "[[NULL,+inf]]",
		},
		{
			indexPos:    5,
			exprStr:     "d in ('aab', 'aac') and e = 'a'",
			accessConds: "[in(test.t.d, aab, aac) eq(test.t.e, a)]",
			filterConds: "[in(test.t.d, aab, aac)]",
			resultStr:   "[[\"aa\" 0x61,\"aa\" 0x61]]",
		},
		{
			indexPos:    6,
			exprStr:     "g = 'a'",
			accessConds: "[eq(test.t.g, a)]",
			filterConds: "[]",
			resultStr:   "[[\"A\",\"A\"]]",
		},
		{
			indexPos:    7,
			exprStr:     `h LIKE 'ÿÿ%'`,
			accessConds: `[like(test.t.h, ÿÿ%, 92)]`,
			filterConds: "[like(test.t.h, ÿÿ%, 92)]",
			resultStr:   "[[\"ÿÿ\",\"ÿ\xc3\xc0\")]", // The decoding error is ignored.
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.exprStr, func(t *testing.T) {
			sql := "select * from t where " + tt.exprStr
			sctx := testKit.Session().(sessionctx.Context)
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, _, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			require.NotNil(t, selection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx, cond)
			}
			cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
			require.NotNil(t, cols)
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, conds, cols, lengths)
			require.NoError(t, err)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", res.AccessConds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", res.RemainedConds))
			got := fmt.Sprintf("%v", res.Ranges)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

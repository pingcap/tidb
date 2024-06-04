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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestTableRange(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
			sctx := testKit.Session()
			rctx := sctx.GetRangerCtx()
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
			}
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			col := expression.ColInfo2Col(selection.Schema().Columns, tbl.Columns[0])
			require.NotNil(t, col)
			var filter []expression.Expression
			conds, filter = ranger.DetachCondsForColumn(rctx, conds, col)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", conds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", filter))
			result, _, _, err := ranger.BuildTableRange(conds, rctx, col.RetType, 0)
			require.NoError(t, err)
			got := fmt.Sprintf("%v", result)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

// for issue #6661
func TestIndexRangeForUnsignedAndOverflow(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	index idx_float(col_float),
	index idx_int_bigint(a, col_int)
)`)
	tests := []struct {
		indexPos    int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			indexPos:    6,
			exprStr:     `a = 1 and a = 2`,
			accessConds: "[]",
			filterConds: "[]",
			resultStr:   `[]`,
		},
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
			sctx := testKit.Session()
			rctx := sctx.GetRangerCtx()
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			require.NotNil(t, selection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
			}
			cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
			require.NotNil(t, cols)
			res, err := ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
			require.NoError(t, err)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", res.AccessConds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", res.RemainedConds))
			got := fmt.Sprintf("%v", res.Ranges)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

func TestColumnRange(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
			sctx := testKit.Session()
			rctx := sctx.GetRangerCtx()
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			sel := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			ds, ok := sel.Children()[0].(*plannercore.DataSource)
			require.True(t, ok)
			conds := make([]expression.Expression, len(sel.Conditions))
			for i, cond := range sel.Conditions {
				conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
			}
			col := expression.ColInfo2Col(sel.Schema().Columns, ds.TableInfo().Columns[tt.colPos])
			require.NotNil(t, col)
			conds = ranger.ExtractAccessConditionsForColumn(rctx, conds, col)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", conds))
			result, _, _, err := ranger.BuildColumnRange(conds, rctx, col.RetType, tt.length, 0)
			require.NoError(t, err)
			got := fmt.Sprintf("%v", result)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

func TestIndexRangeForYear(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
			sctx := testKit.Session()
			rctx := sctx.GetRangerCtx()
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			require.NotNil(t, selection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
			}
			cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
			require.NotNil(t, cols)
			res, err := ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
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
	store := testkit.CreateMockStore(t)

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
			sctx := testKit.Session()
			rctx := sctx.GetRangerCtx()
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			require.NotNil(t, selection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
			}
			cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
			require.NotNil(t, cols)
			res, err := ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
			require.NoError(t, err)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", res.AccessConds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", res.RemainedConds))
			got := fmt.Sprintf("%v", res.Ranges)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

func TestIndexRange(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
			filterConds: "[like(test.t.a, abc%, 92)]",
			resultStr:   "[[\"abc\",\"abd\")]",
		},
		{
			indexPos:    0,
			exprStr:     "a LIKE 'abc_'",
			accessConds: "[like(test.t.a, abc_, 92)]",
			filterConds: "[like(test.t.a, abc_, 92)]",
			resultStr:   "[[\"abc\",\"abd\")]",
		},
		{
			indexPos:    0,
			exprStr:     "a LIKE 'abc'",
			accessConds: "[like(test.t.a, abc, 92)]",
			filterConds: "[like(test.t.a, abc, 92)]",
			resultStr:   "[[\"abc\",\"abc\"]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "ab\_c"`,
			accessConds: "[like(test.t.a, ab\\_c, 92)]",
			filterConds: "[like(test.t.a, ab\\_c, 92)]",
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
			filterConds: "[like(test.t.a, \\%a, 92)]",
			resultStr:   `[["%a","%a"]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "\\"`,
			accessConds: "[like(test.t.a, \\, 92)]",
			filterConds: "[like(test.t.a, \\, 92)]",
			resultStr:   "[[\"\\\\\",\"\\\\\"]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "\\\\a%"`,
			accessConds: `[like(test.t.a, \\a%, 92)]`,
			filterConds: "[like(test.t.a, \\\\a%, 92)]",
			resultStr:   "[[\"\\\\a\",\"\\\\b\")]",
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
			resultStr:   "[[\"\\xe4\\xbd\",\"\\xe4\\xbd\"]]",
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
			resultStr:   "[[\"\\x00A\",\"\\x00B\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f in ('a', 'B')",
			accessConds: "[in(test.t.f, a, B)]",
			filterConds: "[]",
			resultStr:   "[[\"\\x00A\",\"\\x00A\"] [\"\\x00B\",\"\\x00B\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f = 'a' and f = 'B' collate utf8mb4_bin",
			accessConds: "[eq(test.t.f, a)]",
			filterConds: "[eq(test.t.f, B)]",
			resultStr:   "[[\"\\x00A\",\"\\x00A\"]]",
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
			resultStr:   "[[\"aa\" \"a\",\"aa\" \"a\"]]",
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
			resultStr:   "[[\"ÿÿ\",\"ÿ\\xc3\\xc0\")]", // The decoding error is ignored.
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.exprStr, func(t *testing.T) {
			sql := "select * from t where " + tt.exprStr
			sctx := testKit.Session()
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
			require.NotNil(t, selection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
			}
			cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
			require.NotNil(t, cols)
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx.GetRangerCtx(), conds, cols, lengths, 0)
			require.NoError(t, err)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", res.AccessConds))
			require.Equal(t, tt.filterConds, fmt.Sprintf("%s", res.RemainedConds))
			got := fmt.Sprintf("%v", res.Ranges)
			require.Equal(t, tt.resultStr, got)
		})
	}
}

func TestTableShardIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = true
	})
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists test3")
	testKit.MustExec("create table test3(id int primary key clustered, a int, b int, unique key uk_expr((tidb_shard(a)),a))")
	testKit.MustExec("create table test33(id int primary key clustered, a int, b int, unique key a(a))")
	testKit.MustExec("create table test4(id int primary key clustered, a int, b int, " +
		"unique key uk_expr((tidb_shard(a)),a),unique key uk_b_expr((tidb_shard(b)),b))")
	testKit.MustExec("create table test5(id int primary key clustered, a int, b int, " +
		"unique key uk_expr((tidb_shard(a)),a,b))")
	testKit.MustExec("create table test6(id int primary key clustered, a int, b int, c int, " +
		"unique key uk_expr((tidb_shard(a)), a))")
	testKit.MustExec("create table testx(id int primary key clustered, a int, b int, unique key a(a))")
	testKit.MustExec("create table testy(id int primary key clustered, a int, b int, " +
		"unique key uk_expr((tidb_shard(b)),a))")
	testKit.MustExec("create table testz(id int primary key clustered, a int, b int, " +
		"unique key uk_expr((tidb_shard(a+b)),a))")

	tests := []struct {
		exprStr     string
		accessConds string
		childLevel  int
		tableName   string
	}{
		{
			exprStr:     "a = 1",
			accessConds: "[eq(tidb_shard(test.test3.a), 214) eq(test.test3.a, 1)]",
			tableName:   "test3",
		},
		{
			exprStr: "a=100 and (b = 100 or b = 200)",
			accessConds: "[or(eq(test.test3.b, 100), eq(test.test3.b, 200)) eq(tidb_shard(test.test3.a), 8) " +
				"eq(test.test3.a, 100)]",
			tableName: "test3",
		},
		{
			// don't add prefix
			exprStr:     " tidb_shard(a) = 8",
			accessConds: "[eq(tidb_shard(test.test3.a), 8)]",
			tableName:   "test3",
		},
		{
			exprStr: "a=100 or b = 200",
			accessConds: "[or(and(eq(tidb_shard(test.test3.a), 8), eq(test.test3.a, 100)), " +
				"eq(test.test3.b, 200))]",
			tableName: "test3",
		},
		{
			exprStr: "a=100 or b > 200",
			accessConds: "[or(and(eq(tidb_shard(test.test3.a), 8), eq(test.test3.a, 100)), " +
				"gt(test.test3.b, 200))]",
			tableName: "test3",
		},
		{
			exprStr: "a=100 or a = 200  or 1",
			accessConds: "[or(and(eq(tidb_shard(test.test3.a), 8), eq(test.test3.a, 100)), " +
				"or(and(eq(tidb_shard(test.test3.a), 161), eq(test.test3.a, 200)), 1))]",
			tableName: "test3",
		},
		{
			exprStr: "(a=100 and b = 100) or a = 300",
			accessConds: "[or(and(eq(test.test3.b, 100), and(eq(tidb_shard(test.test3.a), 8), eq(test.test3.a, 100))), " +
				"and(eq(tidb_shard(test.test3.a), 227), eq(test.test3.a, 300)))]",
			tableName: "test3",
		},
		{
			exprStr: "((a=100 and b = 100) or a = 200) or a = 300",
			accessConds: "[or(and(eq(test.test3.b, 100), and(eq(tidb_shard(test.test3.a), 8), eq(test.test3.a, 100))), " +
				"or(and(eq(tidb_shard(test.test3.a), 161), eq(test.test3.a, 200)), " +
				"and(eq(tidb_shard(test.test3.a), 227), eq(test.test3.a, 300))))]",
			tableName: "test3",
		},
		{
			exprStr: "a IN (100, 200, 300)",
			accessConds: "[or(or(and(eq(tidb_shard(test.test3.a), 8), eq(test.test3.a, 100)), " +
				"and(eq(tidb_shard(test.test3.a), 161), eq(test.test3.a, 200))), and(eq(tidb_shard(test.test3.a), 227), eq(test.test3.a, 300)))]",
			tableName: "test3",
		},
		{
			exprStr:     "a IN (100)",
			accessConds: "[eq(tidb_shard(test.test3.a), 8) eq(test.test3.a, 100)]",
			tableName:   "test3",
		},
		{
			exprStr: "a IN (100, 200, 300) or a = 400",
			accessConds: "[or(or(or(and(eq(tidb_shard(test.test3.a), 8), eq(test.test3.a, 100)), " +
				"and(eq(tidb_shard(test.test3.a), 161), eq(test.test3.a, 200))), and(eq(tidb_shard(test.test3.a), 227), eq(test.test3.a, 300))), and(eq(tidb_shard(test.test3.a), 85), eq(test.test3.a, 400)))]",
			tableName: "test3",
		},
		{
			// don't add prefix
			exprStr: "((a=100 and b = 100) or a = 200) and b = 300",
			accessConds: "[or(and(eq(test.test3.a, 100), eq(test.test3.b, 100)), eq(test.test3.a, 200)) " +
				"eq(test.test3.b, 300)]",
			tableName: "test3",
		},
		{
			// don't add prefix
			exprStr:     "a = b",
			accessConds: "[eq(test.test3.a, test.test3.b)]",
			tableName:   "test3",
		},
		{
			// don't add prefix
			exprStr:     "a = b and b = 100",
			accessConds: "[eq(test.test3.a, test.test3.b) eq(test.test3.b, 100)]",
			tableName:   "test3",
		},
		{
			// don't add prefix
			exprStr:     "a > 900",
			accessConds: "[gt(test.test3.a, 900)]",
			tableName:   "test3",
		},
		{
			// add prefix
			exprStr:     "a = 3 or a > 900",
			accessConds: "[or(and(eq(tidb_shard(test.test3.a), 156), eq(test.test3.a, 3)), gt(test.test3.a, 900))]",
			tableName:   "test3",
		},
		// two shard index in one table
		{
			exprStr:     "a = 100",
			accessConds: "[eq(tidb_shard(test.test4.a), 8) eq(test.test4.a, 100)]",
			tableName:   "test4",
		},
		{
			exprStr:     "b = 100",
			accessConds: "[eq(tidb_shard(test.test4.b), 8) eq(test.test4.b, 100)]",
			tableName:   "test4",
		},
		{
			exprStr: "a = 100 and b = 100",
			accessConds: "[eq(tidb_shard(test.test4.a), 8) eq(test.test4.a, 100) " +
				"eq(tidb_shard(test.test4.b), 8) eq(test.test4.b, 100)]",
			tableName: "test4",
		},
		{
			exprStr: "a = 100 or b = 100",
			accessConds: "[or(and(eq(tidb_shard(test.test4.a), 8), eq(test.test4.a, 100)), " +
				"and(eq(tidb_shard(test.test4.b), 8), eq(test.test4.b, 100)))]",
			tableName: "test4",
		},
		// shard index cotans three fields
		{
			exprStr:     "a = 100 and b = 100",
			accessConds: "[eq(tidb_shard(test.test5.a), 8) eq(test.test5.a, 100) eq(test.test5.b, 100)]",
			tableName:   "test5",
		},
		{
			exprStr: "(a=100 and b = 100) or  (a=200 and b = 200)",
			accessConds: "[or(and(eq(tidb_shard(test.test5.a), 8), and(eq(test.test5.a, 100), eq(test.test5.b, 100))), " +
				"and(eq(tidb_shard(test.test5.a), 161), and(eq(test.test5.a, 200), eq(test.test5.b, 200))))]",
			tableName: "test5",
		},
		{
			exprStr: "(a, b) in ((100, 100), (200, 200))",
			accessConds: "[or(and(eq(tidb_shard(test.test5.a), 8), and(eq(test.test5.a, 100), eq(test.test5.b, 100))), " +
				"and(eq(tidb_shard(test.test5.a), 161), and(eq(test.test5.a, 200), eq(test.test5.b, 200))))]",
			tableName: "test5",
		},
		{
			exprStr:     "(a, b) in ((100, 100))",
			accessConds: "[eq(tidb_shard(test.test5.a), 8) eq(test.test5.a, 100) eq(test.test5.b, 100)]",
			tableName:   "test5",
		},
		// don't add prefix
		{
			exprStr:     "a=100",
			accessConds: "[eq(test.testy.a, 100)]",
			tableName:   "testy",
		},
		// don't add prefix
		{
			exprStr:     "a=100",
			accessConds: "[eq(test.testz.a, 100)]",
			tableName:   "testz",
		},
		// test join
		{
			exprStr:     "test3.a = 100",
			accessConds: "[eq(tidb_shard(test.test3.a), 8) eq(test.test3.a, 100)]",
			childLevel:  4,
			tableName:   "test3 JOIN test33 ON test3.b = test33.b",
		},
		{
			exprStr:     "test3.a = 100 and test33.a > 10",
			accessConds: "[gt(test.test33.a, 10) eq(tidb_shard(test.test3.a), 8) eq(test.test3.a, 100)]",
			childLevel:  4,
			tableName:   "test3 JOIN test33 ON test3.b = test33.b",
		},
		{
			exprStr:     "test3.a = 100 AND test6.a = 10",
			accessConds: "[eq(test.test6.a, 10) eq(tidb_shard(test.test3.a), 8) eq(test.test3.a, 100)]",
			childLevel:  4,
			tableName:   "test3 JOIN test6 ON test3.b = test6.b",
		},
		{
			exprStr:     "test3.a = 100 or test6.a = 10",
			accessConds: "[or(and(eq(tidb_shard(test.test3.a), 8), eq(test.test3.a, 100)), eq(test.test6.a, 10))]",
			childLevel:  4,
			tableName:   "test3 JOIN test6 ON test3.b = test6.b",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.exprStr, func(t *testing.T) {
			sql := "select * from " + tt.tableName + " where " + tt.exprStr
			sctx := testKit.Session()
			stmts, err := session.Parse(sctx, sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			selection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
			conds := make([]expression.Expression, len(selection.Conditions))
			for i, cond := range selection.Conditions {
				conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
			}
			ds, ok := selection.Children()[0].(*plannercore.DataSource)
			if !ok {
				if tt.childLevel == 4 {
					ds = selection.Children()[0].Children()[0].Children()[0].(*plannercore.DataSource)
				}
			}
			newConds := ds.AddPrefix4ShardIndexes(ds.SCtx(), conds)
			require.Equal(t, tt.accessConds, fmt.Sprintf("%s", newConds))
		})
	}

	// test update statement
	t.Run("", func(t *testing.T) {
		sql := "update test6 set c = 1000 where a=50 and b = 50"
		sctx := testKit.Session()
		stmts, err := session.Parse(sctx, sql)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
		require.NoError(t, err)
		selection, ok := p.(*plannercore.Update).SelectPlan.(*plannercore.PhysicalSelection)
		require.True(t, ok)
		_, ok = selection.Children()[0].(*plannercore.PointGetPlan)
		require.True(t, ok)
	})

	// test delete statement
	t.Run("", func(t *testing.T) {
		sql := "delete from test6 where a = 45 and b = 45;"
		sctx := testKit.Session()
		stmts, err := session.Parse(sctx, sql)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
		require.NoError(t, err)
		selection, ok := p.(*plannercore.Delete).SelectPlan.(*plannercore.PhysicalSelection)
		require.True(t, ok)
		_, ok = selection.Children()[0].(*plannercore.PointGetPlan)
		require.True(t, ok)
	})
}

func TestShardIndexFuncSuites(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	sctx := testKit.Session()

	// -------------------------------------------
	// test IsValidShardIndex function
	// -------------------------------------------
	longlongType := types.NewFieldType(mysql.TypeLonglong)
	col0 := &expression.Column{UniqueID: 0, ID: 0, RetType: longlongType}
	col1 := &expression.Column{UniqueID: 1, ID: 1, RetType: longlongType}
	// col2 is GC column and VirtualExpr = tidb_shard(col0)
	col2 := &expression.Column{UniqueID: 2, ID: 2, RetType: longlongType}
	col2.VirtualExpr = expression.NewFunctionInternal(sctx.GetExprCtx(), ast.TiDBShard, col2.RetType, col0)
	// col3 is GC column and VirtualExpr = abs(col0)
	col3 := &expression.Column{UniqueID: 3, ID: 3, RetType: longlongType}
	col3.VirtualExpr = expression.NewFunctionInternal(sctx.GetExprCtx(), ast.Abs, col2.RetType, col0)
	col4 := &expression.Column{UniqueID: 4, ID: 4, RetType: longlongType}

	cols := []*expression.Column{col0, col1}

	// input is nil
	require.False(t, ranger.IsValidShardIndex(nil))
	// only 1 column
	require.False(t, ranger.IsValidShardIndex([]*expression.Column{col2}))
	// first col is not expression
	require.False(t, ranger.IsValidShardIndex(cols))
	// field in tidb_shard is not the secondary column
	require.False(t, ranger.IsValidShardIndex([]*expression.Column{col2, col1}))
	// expressioin is abs that is not tidb_shard
	require.False(t, ranger.IsValidShardIndex([]*expression.Column{col3, col0}))
	// normal case
	require.True(t, ranger.IsValidShardIndex([]*expression.Column{col2, col0}))

	// -------------------------------------------
	// test ExtractColumnsFromExpr function
	// -------------------------------------------
	// normal case
	con1 := &expression.Constant{Value: types.NewDatum(1), RetType: longlongType}
	con5 := &expression.Constant{Value: types.NewDatum(5), RetType: longlongType}
	exprEq := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.EQ, col0.RetType, col0, con1)
	exprIn := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.In, col0.RetType, col0, con1, con5)
	require.NotNil(t, exprEq)
	require.NotNil(t, exprIn)
	// input is nil
	require.Equal(t, len(ranger.ExtractColumnsFromExpr(nil)), 0)
	// input is column
	require.Equal(t, len(ranger.ExtractColumnsFromExpr(exprEq.(*expression.ScalarFunction))), 1)
	// (col0 = 1 and col3 > 1) or (col4 < 5 and 5)
	exprGt := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.GT, longlongType, col3, con1)
	require.NotNil(t, exprGt)
	andExpr1 := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.And, longlongType, exprEq, exprGt)
	require.NotNil(t, andExpr1)
	exprLt := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.LT, longlongType, col4, con5)
	andExpr2 := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.And, longlongType, exprLt, con5)
	orExpr2 := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.Or, longlongType, andExpr1, andExpr2)
	require.Equal(t, len(ranger.ExtractColumnsFromExpr(orExpr2.(*expression.ScalarFunction))), 3)

	// -------------------------------------------
	// test NeedAddColumn4InCond function
	// -------------------------------------------
	// normal case
	sfIn, ok := exprIn.(*expression.ScalarFunction)
	require.True(t, ok)
	accessCond := []expression.Expression{nil, exprIn}
	shardIndexCols := []*expression.Column{col2, col0}
	require.True(t, ranger.NeedAddColumn4InCond(shardIndexCols, accessCond, sfIn))

	// input nil
	require.False(t, ranger.NeedAddColumn4InCond(nil, accessCond, sfIn))
	require.False(t, ranger.NeedAddColumn4InCond(shardIndexCols, nil, sfIn))
	require.False(t, ranger.NeedAddColumn4InCond(shardIndexCols, accessCond, nil))

	// col1 in (1, 5)
	exprIn2 := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.In, col1.RetType, col1, con1, con5)
	accessCond[1] = exprIn2
	require.False(t, ranger.NeedAddColumn4InCond(shardIndexCols, accessCond, exprIn2.(*expression.ScalarFunction)))

	// col0 in (1, col1)
	exprIn3 := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.In, col0.RetType, col1, con1, col1)
	accessCond[1] = exprIn3
	require.False(t, ranger.NeedAddColumn4InCond(shardIndexCols, accessCond, exprIn3.(*expression.ScalarFunction)))

	// -------------------------------------------
	// test NeedAddColumn4EqCond function
	// -------------------------------------------
	// ranger.valueInfo is not export by package, we can.t test NeedAddColumn4EqCond
	eqAccessCond := []expression.Expression{nil, exprEq}
	require.False(t, ranger.NeedAddColumn4EqCond(shardIndexCols, eqAccessCond, nil))

	// -------------------------------------------
	// test NeedAddGcColumn4ShardIndex function
	// -------------------------------------------
	// ranger.valueInfo is not export by package, we can.t test NeedAddGcColumn4ShardIndex
	require.False(t, ranger.NeedAddGcColumn4ShardIndex(shardIndexCols, nil, nil))

	// -------------------------------------------
	// test AddExpr4EqAndInCondition function
	// -------------------------------------------
	exprIn4 := expression.NewFunctionInternal(sctx.GetExprCtx(), ast.In, col0.RetType, col0, con1)
	test := []struct {
		inputConds  []expression.Expression
		outputConds string
	}{
		{
			// col0 = 1 => tidb_shard(col0) = 214 and col0 = 1
			inputConds:  []expression.Expression{exprEq},
			outputConds: "[eq(Column#2, 214) eq(Column#0, 1)]",
		},
		{
			// col0 in (1) => cols2 = 214 and col0 = 1
			inputConds:  []expression.Expression{exprIn4},
			outputConds: "[and(eq(Column#2, 214), eq(Column#0, 1))]",
		},
		{
			// col0 in (1, 5) => (cols2 = 214 and col0 = 1) or (cols2 = 122 and col0 = 5)
			inputConds: []expression.Expression{exprIn},
			outputConds: "[or(and(eq(Column#2, 214), eq(Column#0, 1)), " +
				"and(eq(Column#2, 122), eq(Column#0, 5)))]",
		},
	}

	for _, tt := range test {
		newConds, _ := ranger.AddExpr4EqAndInCondition(sctx.GetRangerCtx(), tt.inputConds, shardIndexCols)
		require.Equal(t, fmt.Sprintf("%s", newConds), tt.outputConds)
	}
}

func getSelectionFromQuery(t *testing.T, sctx sessionctx.Context, sql string) *plannercore.LogicalSelection {
	ctx := context.Background()
	stmts, err := session.Parse(sctx, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	ret := &plannercore.PreprocessorReturn{}
	err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
	require.NoError(t, err)
	p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
	require.NoError(t, err)
	selection, isSelection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
	require.True(t, isSelection)
	return selection
}

func checkDetachRangeResult(t *testing.T, res *ranger.DetachRangeResult, expectedAccessConds, expectedRemainedConds, expectedRanges string) {
	require.Equal(t, expectedAccessConds, fmt.Sprintf("%v", res.AccessConds))
	require.Equal(t, expectedRemainedConds, fmt.Sprintf("%v", res.RemainedConds))
	require.Equal(t, expectedRanges, fmt.Sprintf("%v", res.Ranges))
}

func checkRangeFallbackAndReset(t *testing.T, sctx sessionctx.Context, expectedRangeFallback bool) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	hasRangeFallbackWarn := false
	for _, warn := range stmtCtx.GetWarnings() {
		hasRangeFallbackWarn = hasRangeFallbackWarn || strings.Contains(warn.Err.Error(), "'tidb_opt_range_max_size' exceeded when building ranges")
	}
	require.Equal(t, expectedRangeFallback, hasRangeFallbackWarn)
	stmtCtx.PlanCacheTracker = contextutil.NewPlanCacheTracker(stmtCtx)
	stmtCtx.RangeFallbackHandler = contextutil.NewRangeFallbackHandler(&stmtCtx.PlanCacheTracker, stmtCtx)
	stmtCtx.SetWarnings(nil)
}

func TestRangeFallbackForDetachCondAndBuildRangeForIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, c int, d int, index idx(a, b, c))")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	sctx := tk.Session()
	rctx := sctx.GetRangerCtx()

	// test CNF condition
	sql := "select * from t1 where a in (10,20,30) and b in (40,50,60) and c >= 70 and c <= 80"
	selection := getSelectionFromQuery(t, sctx, sql)
	conds := selection.Conditions
	require.Equal(t, 4, len(conds))
	cols, lengths := expression.IndexInfo2PrefixCols(tblInfo.Columns, selection.Schema().Columns, tblInfo.Indices[0])
	res, err := ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[in(test.t1.a, 10, 20, 30) in(test.t1.b, 40, 50, 60) ge(test.t1.c, 70) le(test.t1.c, 80)]",
		"[]",
		"[[10 40 70,10 40 80] [10 50 70,10 50 80] [10 60 70,10 60 80] [20 40 70,20 40 80] [20 50 70,20 50 80] [20 60 70,20 60 80] [30 40 70,30 40 80] [30 50 70,30 50 80] [30 60 70,30 60 80]]")
	checkRangeFallbackAndReset(t, sctx, false)
	quota := res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[in(test.t1.a, 10, 20, 30) in(test.t1.b, 40, 50, 60)]",
		"[ge(test.t1.c, 70) le(test.t1.c, 80)]",
		"[[10 40,10 40] [10 50,10 50] [10 60,10 60] [20 40,20 40] [20 50,20 50] [20 60,20 60] [30 40,30 40] [30 50,30 50] [30 60,30 60]]")
	checkRangeFallbackAndReset(t, sctx, true)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[in(test.t1.a, 10, 20, 30)]",
		"[in(test.t1.b, 40, 50, 60) ge(test.t1.c, 70) le(test.t1.c, 80)]",
		"[[10,10] [20,20] [30,30]]")
	checkRangeFallbackAndReset(t, sctx, true)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[]",
		"[ge(test.t1.c, 70) le(test.t1.c, 80) in(test.t1.b, 40, 50, 60) in(test.t1.a, 10, 20, 30)]",
		"[[NULL,+inf]]")
	checkRangeFallbackAndReset(t, sctx, true)

	// test DNF condition
	sql = "select * from t1 where a = 10 or a = 20 or a = 30"
	selection = getSelectionFromQuery(t, sctx, sql)
	conds = selection.Conditions
	require.Equal(t, 1, len(conds))
	cols, lengths = expression.IndexInfo2PrefixCols(tblInfo.Columns, selection.Schema().Columns, tblInfo.Indices[0])
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[or(eq(test.t1.a, 10), or(eq(test.t1.a, 20), eq(test.t1.a, 30)))]",
		"[]",
		"[[10,10] [20,20] [30,30]]")
	checkRangeFallbackAndReset(t, sctx, false)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[]",
		"[or(or(eq(test.t1.a, 10), eq(test.t1.a, 20)), eq(test.t1.a, 30))]",
		"[[NULL,+inf]]")
	checkRangeFallbackAndReset(t, sctx, true)

	sql = "select * from t1 where (a = 10 and b = 40) or (a = 20 and b = 50) or (a = 30 and b = 60)"
	selection = getSelectionFromQuery(t, sctx, sql)
	conds = selection.Conditions
	require.Equal(t, 1, len(conds))
	cols, lengths = expression.IndexInfo2PrefixCols(tblInfo.Columns, selection.Schema().Columns, tblInfo.Indices[0])
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[or(and(eq(test.t1.a, 10), eq(test.t1.b, 40)), or(and(eq(test.t1.a, 20), eq(test.t1.b, 50)), and(eq(test.t1.a, 30), eq(test.t1.b, 60))))]",
		"[]",
		"[[10 40,10 40] [20 50,20 50] [30 60,30 60]]")
	checkRangeFallbackAndReset(t, sctx, false)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[]",
		"[or(or(and(eq(test.t1.a, 10), eq(test.t1.b, 40)), and(eq(test.t1.a, 20), eq(test.t1.b, 50))), and(eq(test.t1.a, 30), eq(test.t1.b, 60)))]",
		"[[NULL,+inf]]")
	checkRangeFallbackAndReset(t, sctx, true)

	// test considerDNF code path
	sql = "select * from t1 where (a, b) in ((10, 20), (30, 40)) and c = 50"
	selection = getSelectionFromQuery(t, sctx, sql)
	conds = selection.Conditions
	require.Equal(t, 2, len(conds))
	cols, lengths = expression.IndexInfo2PrefixCols(tblInfo.Columns, selection.Schema().Columns, tblInfo.Indices[0])
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[or(and(eq(test.t1.a, 10), eq(test.t1.b, 20)), and(eq(test.t1.a, 30), eq(test.t1.b, 40))) eq(test.t1.c, 50)]",
		"[]",
		"[[10 20 50,10 20 50] [30 40 50,30 40 50]]")
	checkRangeFallbackAndReset(t, sctx, false)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[or(and(eq(test.t1.a, 10), eq(test.t1.b, 20)), and(eq(test.t1.a, 30), eq(test.t1.b, 40)))]",
		"[eq(test.t1.c, 50)]",
		"[[10 20,10 20] [30 40,30 40]]")
	checkRangeFallbackAndReset(t, sctx, true)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[or(eq(test.t1.a, 10), eq(test.t1.a, 30))]",
		"[eq(test.t1.c, 50) or(and(eq(test.t1.a, 10), eq(test.t1.b, 20)), and(eq(test.t1.a, 30), eq(test.t1.b, 40)))]",
		"[[10,10] [30,30]]")
	checkRangeFallbackAndReset(t, sctx, true)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[]",
		// Ideal RemainedConds should be [eq(test.t1.c, 50) or(and(eq(test.t1.a, 10), eq(test.t1.b, 20)), and(eq(test.t1.a, 30), eq(test.t1.b, 40)))], but we don't remove redundant or(eq(test.t1.a, 10), eq(test.t1.a, 30)) for now.
		"[eq(test.t1.c, 50) or(and(eq(test.t1.a, 10), eq(test.t1.b, 20)), and(eq(test.t1.a, 30), eq(test.t1.b, 40))) or(eq(test.t1.a, 10), eq(test.t1.a, 30))]",
		"[[NULL,+inf]]")
	checkRangeFallbackAndReset(t, sctx, true)

	// test prefix index
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (a varchar(10), b varchar(10), c varchar(10), d varchar(10), index idx(a(2), b(2), c(2)))")
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tblInfo = tbl.Meta()

	// test CNF condition
	sql = "select * from t2 where a in ('aaa','bbb','ccc') and b in ('ddd','eee','fff') and c >= 'ggg' and c <= 'iii'"
	selection = getSelectionFromQuery(t, sctx, sql)
	conds = selection.Conditions
	require.Equal(t, 4, len(conds))
	cols, lengths = expression.IndexInfo2PrefixCols(tblInfo.Columns, selection.Schema().Columns, tblInfo.Indices[0])
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[in(test.t2.a, aaa, bbb, ccc) in(test.t2.b, ddd, eee, fff) ge(test.t2.c, ggg) le(test.t2.c, iii)]",
		"[in(test.t2.a, aaa, bbb, ccc) in(test.t2.b, ddd, eee, fff) ge(test.t2.c, ggg) le(test.t2.c, iii)]",
		"[[\"aa\" \"dd\" \"gg\",\"aa\" \"dd\" \"ii\"] [\"aa\" \"ee\" \"gg\",\"aa\" \"ee\" \"ii\"] [\"aa\" \"ff\" \"gg\",\"aa\" \"ff\" \"ii\"] [\"bb\" \"dd\" \"gg\",\"bb\" \"dd\" \"ii\"] [\"bb\" \"ee\" \"gg\",\"bb\" \"ee\" \"ii\"] [\"bb\" \"ff\" \"gg\",\"bb\" \"ff\" \"ii\"] [\"cc\" \"dd\" \"gg\",\"cc\" \"dd\" \"ii\"] [\"cc\" \"ee\" \"gg\",\"cc\" \"ee\" \"ii\"] [\"cc\" \"ff\" \"gg\",\"cc\" \"ff\" \"ii\"]]")
	checkRangeFallbackAndReset(t, sctx, false)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[in(test.t2.a, aaa, bbb, ccc) in(test.t2.b, ddd, eee, fff)]",
		"[in(test.t2.a, aaa, bbb, ccc) in(test.t2.b, ddd, eee, fff) ge(test.t2.c, ggg) le(test.t2.c, iii)]",
		"[[\"aa\" \"dd\",\"aa\" \"dd\"] [\"aa\" \"ee\",\"aa\" \"ee\"] [\"aa\" \"ff\",\"aa\" \"ff\"] [\"bb\" \"dd\",\"bb\" \"dd\"] [\"bb\" \"ee\",\"bb\" \"ee\"] [\"bb\" \"ff\",\"bb\" \"ff\"] [\"cc\" \"dd\",\"cc\" \"dd\"] [\"cc\" \"ee\",\"cc\" \"ee\"] [\"cc\" \"ff\",\"cc\" \"ff\"]]")
	checkRangeFallbackAndReset(t, sctx, true)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[in(test.t2.a, aaa, bbb, ccc)]",
		"[in(test.t2.a, aaa, bbb, ccc) in(test.t2.b, ddd, eee, fff) ge(test.t2.c, ggg) le(test.t2.c, iii)]",
		"[[\"aa\",\"aa\"] [\"bb\",\"bb\"] [\"cc\",\"cc\"]]")
	checkRangeFallbackAndReset(t, sctx, true)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[]",
		"[ge(test.t2.c, ggg) le(test.t2.c, iii) in(test.t2.a, aaa, bbb, ccc) in(test.t2.b, ddd, eee, fff)]",
		"[[NULL,+inf]]")
	checkRangeFallbackAndReset(t, sctx, true)

	// test DNF condition
	sql = "select * from t2 where a = 'aaa' or a = 'bbb' or a = 'ccc'"
	selection = getSelectionFromQuery(t, sctx, sql)
	conds = selection.Conditions
	require.Equal(t, 1, len(conds))
	cols, lengths = expression.IndexInfo2PrefixCols(tblInfo.Columns, selection.Schema().Columns, tblInfo.Indices[0])
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[or(eq(test.t2.a, aaa), or(eq(test.t2.a, bbb), eq(test.t2.a, ccc)))]",
		"[or(or(eq(test.t2.a, aaa), eq(test.t2.a, bbb)), eq(test.t2.a, ccc))]",
		"[[\"aa\",\"aa\"] [\"bb\",\"bb\"] [\"cc\",\"cc\"]]")
	checkRangeFallbackAndReset(t, sctx, false)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[]",
		"[or(or(eq(test.t2.a, aaa), eq(test.t2.a, bbb)), eq(test.t2.a, ccc))]",
		"[[NULL,+inf]]")
	checkRangeFallbackAndReset(t, sctx, true)

	sql = "select * from t2 where (a = 'aaa' and b = 'ddd') or (a = 'bbb' and b = 'eee') or (a = 'ccc' and b = 'fff')"
	selection = getSelectionFromQuery(t, sctx, sql)
	conds = selection.Conditions
	require.Equal(t, 1, len(conds))
	cols, lengths = expression.IndexInfo2PrefixCols(tblInfo.Columns, selection.Schema().Columns, tblInfo.Indices[0])
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[or(and(eq(test.t2.a, aaa), eq(test.t2.b, ddd)), or(and(eq(test.t2.a, bbb), eq(test.t2.b, eee)), and(eq(test.t2.a, ccc), eq(test.t2.b, fff))))]",
		"[or(or(and(eq(test.t2.a, aaa), eq(test.t2.b, ddd)), and(eq(test.t2.a, bbb), eq(test.t2.b, eee))), and(eq(test.t2.a, ccc), eq(test.t2.b, fff)))]",
		"[[\"aa\" \"dd\",\"aa\" \"dd\"] [\"bb\" \"ee\",\"bb\" \"ee\"] [\"cc\" \"ff\",\"cc\" \"ff\"]]")
	checkRangeFallbackAndReset(t, sctx, false)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[]",
		"[or(or(and(eq(test.t2.a, aaa), eq(test.t2.b, ddd)), and(eq(test.t2.a, bbb), eq(test.t2.b, eee))), and(eq(test.t2.a, ccc), eq(test.t2.b, fff)))]",
		"[[NULL,+inf]]")
	checkRangeFallbackAndReset(t, sctx, true)

	// test considerDNF code path
	sql = "select * from t2 where (a, b) in (('aaa', 'bbb'), ('ccc', 'ddd')) and c = 'eee'"
	selection = getSelectionFromQuery(t, sctx, sql)
	conds = selection.Conditions
	require.Equal(t, 2, len(conds))
	cols, lengths = expression.IndexInfo2PrefixCols(tblInfo.Columns, selection.Schema().Columns, tblInfo.Indices[0])
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, 0)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[or(eq(test.t2.a, aaa), eq(test.t2.a, ccc))]",
		"[eq(test.t2.c, eee) or(and(eq(test.t2.a, aaa), eq(test.t2.b, bbb)), and(eq(test.t2.a, ccc), eq(test.t2.b, ddd)))]",
		"[[\"aa\",\"aa\"] [\"cc\",\"cc\"]]")
	checkRangeFallbackAndReset(t, sctx, false)
	quota = res.Ranges.MemUsage() - 1
	res, err = ranger.DetachCondAndBuildRangeForIndex(rctx, conds, cols, lengths, quota)
	require.NoError(t, err)
	checkDetachRangeResult(t, res,
		"[]",
		"[eq(test.t2.c, eee) or(and(eq(test.t2.a, aaa), eq(test.t2.b, bbb)), and(eq(test.t2.a, ccc), eq(test.t2.b, ddd))) or(eq(test.t2.a, aaa), eq(test.t2.a, ccc))]",
		"[[NULL,+inf]]")
	checkRangeFallbackAndReset(t, sctx, true)
}

func TestRangeFallbackForBuildTableRange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	sctx := tk.Session()
	rctx := sctx.GetRangerCtx()
	sql := "select * from t where a in (10,20,30,40,50)"
	selection := getSelectionFromQuery(t, sctx, sql)
	conds := selection.Conditions
	require.Equal(t, 1, len(conds))
	col := expression.ColInfo2Col(selection.Schema().Columns, tblInfo.Columns[0])
	var filters []expression.Expression
	conds, filters = ranger.DetachCondsForColumn(rctx, conds, col)
	require.Equal(t, 1, len(conds))
	require.Equal(t, 0, len(filters))
	ranges, access, remained, err := ranger.BuildTableRange(conds, rctx, col.RetType, 0)
	require.NoError(t, err)
	require.Equal(t, "[[10,10] [20,20] [30,30] [40,40] [50,50]]", fmt.Sprintf("%v", ranges))
	require.Equal(t, "[in(test.t.a, 10, 20, 30, 40, 50)]", fmt.Sprintf("%v", access))
	require.Equal(t, "[]", fmt.Sprintf("%v", remained))
	checkRangeFallbackAndReset(t, sctx, false)
	quota := ranges.MemUsage() - 1
	ranges, access, remained, err = ranger.BuildTableRange(conds, rctx, col.RetType, quota)
	require.NoError(t, err)
	require.Equal(t, "[[-inf,+inf]]", fmt.Sprintf("%v", ranges))
	require.Equal(t, "[]", fmt.Sprintf("%v", access))
	require.Equal(t, "[in(test.t.a, 10, 20, 30, 40, 50)]", fmt.Sprintf("%v", remained))
	checkRangeFallbackAndReset(t, sctx, true)
}

func TestRangeFallbackForBuildColumnRange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(20), b int not null)")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	sctx := tk.Session()
	rctx := sctx.GetRangerCtx()
	sql := "select * from t where a in ('aaa','bbb','ccc','ddd','eee')"
	selection := getSelectionFromQuery(t, sctx, sql)
	conds := selection.Conditions
	require.Equal(t, 1, len(conds))
	cola := expression.ColInfo2Col(selection.Schema().Columns, tblInfo.Columns[0])
	var filters []expression.Expression
	conds, filters = ranger.DetachCondsForColumn(rctx, conds, cola)
	require.Equal(t, 1, len(conds))
	require.Equal(t, 0, len(filters))
	ranges, access, remained, err := ranger.BuildColumnRange(conds, rctx, cola.RetType, types.UnspecifiedLength, 0)
	require.NoError(t, err)
	require.Equal(t, "[[\"aaa\",\"aaa\"] [\"bbb\",\"bbb\"] [\"ccc\",\"ccc\"] [\"ddd\",\"ddd\"] [\"eee\",\"eee\"]]", fmt.Sprintf("%v", ranges))
	require.Equal(t, "[in(test.t.a, aaa, bbb, ccc, ddd, eee)]", fmt.Sprintf("%v", access))
	require.Equal(t, "[]", fmt.Sprintf("%v", remained))
	checkRangeFallbackAndReset(t, sctx, false)
	quota := ranges.MemUsage() - 1
	ranges, access, remained, err = ranger.BuildColumnRange(conds, rctx, cola.RetType, types.UnspecifiedLength, quota)
	require.NoError(t, err)
	require.Equal(t, "[[NULL,+inf]]", fmt.Sprintf("%v", ranges))
	require.Equal(t, "[]", fmt.Sprintf("%v", access))
	require.Equal(t, "[in(test.t.a, aaa, bbb, ccc, ddd, eee)]", fmt.Sprintf("%v", remained))
	checkRangeFallbackAndReset(t, sctx, true)
	sql = "select * from t where b in (10,20,30)"
	selection = getSelectionFromQuery(t, sctx, sql)
	conds = selection.Conditions
	require.Equal(t, 1, len(conds))
	colb := expression.ColInfo2Col(selection.Schema().Columns, tblInfo.Columns[1])
	conds, filters = ranger.DetachCondsForColumn(rctx, conds, colb)
	require.Equal(t, 1, len(conds))
	require.Equal(t, 0, len(filters))
	ranges, access, remained, err = ranger.BuildColumnRange(conds, rctx, colb.RetType, types.UnspecifiedLength, 1)
	require.NoError(t, err)
	require.Equal(t, "[[-inf,+inf]]", fmt.Sprintf("%v", ranges))
	require.Equal(t, "[]", fmt.Sprintf("%v", access))
	require.Equal(t, "[in(test.t.b, 10, 20, 30)]", fmt.Sprintf("%v", remained))
}

func TestPrefixIndexRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`
create table t(
	a varchar(50),
	b varchar(50),
	c text(50),
	d varbinary(50),
	index idx_a(a(2)),
	index idx_ab(a(2), b(2)),
	index idx_c(c(2)),
	index idx_d(d(2))
)`)
	tk.MustExec("set tidb_opt_prefix_index_single_scan = 1")

	tests := []struct {
		indexPos    int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			indexPos:    0,
			exprStr:     "a is null",
			accessConds: "[isnull(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL]]",
		},
		{
			indexPos:    0,
			exprStr:     "a is not null",
			accessConds: "[not(isnull(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			indexPos:    1,
			exprStr:     "a = 'a' and b is null",
			accessConds: "[eq(test.t.a, a) isnull(test.t.b)]",
			filterConds: "[eq(test.t.a, a)]",
			resultStr:   "[[\"a\" NULL,\"a\" NULL]]",
		},
		{
			indexPos:    1,
			exprStr:     "a = 'a' and b is not null",
			accessConds: "[eq(test.t.a, a) not(isnull(test.t.b))]",
			filterConds: "[eq(test.t.a, a)]",
			resultStr:   "[[\"a\" -inf,\"a\" +inf]]",
		},
		{
			indexPos:    2,
			exprStr:     "c is null",
			accessConds: "[isnull(test.t.c)]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL]]",
		},
		{
			indexPos:    2,
			exprStr:     "c is not null",
			accessConds: "[not(isnull(test.t.c))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			indexPos:    3,
			exprStr:     "d is null",
			accessConds: "[isnull(test.t.d)]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL]]",
		},
		{
			indexPos:    3,
			exprStr:     "d is not null",
			accessConds: "[not(isnull(test.t.d))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
	}

	collate.SetNewCollationEnabledForTest(true)
	defer func() { collate.SetNewCollationEnabledForTest(false) }()
	ctx := context.Background()
	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		sctx := tk.Session()
		stmts, err := session.Parse(sctx, sql)
		require.NoError(t, err, fmt.Sprintf("error %v, for expr %s", err, tt.exprStr))
		require.Len(t, stmts, 1)
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), sctx, stmts[0], plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err, fmt.Sprintf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, err := plannercore.BuildLogicalPlanForTest(ctx, sctx, stmts[0], ret.InfoSchema)
		require.NoError(t, err, fmt.Sprintf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(base.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		tbl := selection.Children()[0].(*plannercore.DataSource).TableInfo()
		require.NotNil(t, selection, fmt.Sprintf("expr:%v", tt.exprStr))
		conds := make([]expression.Expression, len(selection.Conditions))
		for i, cond := range selection.Conditions {
			conds[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
		}
		cols, lengths := expression.IndexInfo2PrefixCols(tbl.Columns, selection.Schema().Columns, tbl.Indices[tt.indexPos])
		require.NotNil(t, cols)
		res, err := ranger.DetachCondAndBuildRangeForIndex(sctx.GetRangerCtx(), conds, cols, lengths, 0)
		require.NoError(t, err)
		require.Equal(t, tt.accessConds, fmt.Sprintf("%s", res.AccessConds), fmt.Sprintf("wrong access conditions for expr: %s", tt.exprStr))
		require.Equal(t, tt.filterConds, fmt.Sprintf("%s", res.RemainedConds), fmt.Sprintf("wrong filter conditions for expr: %s", tt.exprStr))
		got := fmt.Sprintf("%v", res.Ranges)
		require.Equal(t, tt.resultStr, got, fmt.Sprintf("different for expr %s", tt.exprStr))
	}
}

func TestIssue40997(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	err := tk.ExecToErr(`
	CREATE TABLE t71706696 (
		dt char(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
		db_id bigint(20) NOT NULL,
		tbl_id bigint(20) NOT NULL,
		db_name varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
		UNIQUE KEY dt_2 (dt, db_id, tbl_id)
	);
	`)
	require.NoError(t, err)
	tk.MustQuery(`
    EXPLAIN
    SELECT *
    FROM t71706696 FORCE INDEX(dt_2)
    WHERE (
        (
            dt = '20210112'
            AND db_id = '62812'
            AND tbl_id > '228892694'
        ) OR (
            dt = '20210112'
            AND db_id = '62813'
            AND tbl_id <= '226785696'
        ) OR (
            dt = '20210112'
            AND db_id > '62812'
            AND db_id < '62813'
        )
    )
	`).Check(testkit.Rows(
		"IndexLookUp_7 0.67 root  ",
		"├─IndexRangeScan_5(Build) 0.67 cop[tikv] table:t71706696, index:dt_2(dt, db_id, tbl_id) range:(\"20210112\" 62812 228892694,\"20210112\" 62812 +inf], [\"20210112\" 62813 -inf,\"20210112\" 62813 226785696], keep order:false, stats:pseudo",
		"└─TableRowIDScan_6(Probe) 0.67 cop[tikv] table:t71706696 keep order:false, stats:pseudo",
	))
}

func TestIssue50051(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tt")
	tk.MustExec("CREATE TABLE tt (c bigint UNSIGNED not null, d int not null, PRIMARY KEY (c,d));")
	tk.MustExec("insert into tt values (9223372036854775810, 3);")
	tk.MustQuery("SELECT c FROM tt WHERE c>9223372036854775807 AND c>1;").Check(testkit.Rows("9223372036854775810"))

	tk.MustExec("drop table if exists t5")
	tk.MustExec("drop table if exists t6")
	tk.MustExec("CREATE TABLE `t5` (`d` int not null, `c` int not null, PRIMARY KEY (`d`, `c`));")
	tk.MustExec("CREATE TABLE `t6` (`d` bigint UNSIGNED not null);")
	tk.MustExec("insert into t5 values (-3, 6);")
	tk.MustExec("insert into t6 values (0), (1), (2), (3);")
	tk.MustQuery("select d from t5 where d < (select min(d) from t6) and d < 3;").Check(testkit.Rows("-3"))
}

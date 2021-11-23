// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestIndexRange(t *testing.T) {
	dom, store, err := newDomainStoreWithBootstrap(t)
	defer func() {
		dom.Close()
		require.NoError(t, store.Close())
	}()
	require.NoError(t, err)
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
	index idx_ab(a(50), b),
	index idx_cb(c, a),
	index idx_d(d(2)),
	index idx_e(e(2)),
	index idx_f(f),
	index idx_de(d(2), e),
	index idx_g(g)
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
	}

	collate.SetNewCollationEnabledForTest(true)
	defer func() { collate.SetNewCollationEnabledForTest(false) }()
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

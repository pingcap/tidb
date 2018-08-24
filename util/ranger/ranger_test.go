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

package ranger_test

import (
	"fmt"
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRangerSuite{})

type testRangerSuite struct {
	*parser.Parser
}

func (s *testRangerSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
}

func newDomainStoreWithBootstrap(c *C) (*domain.Domain, kv.Storage, error) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	mvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
		mockstore.WithMVCCStore(mvccStore),
	)
	c.Assert(err, IsNil)
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	dom, err := session.BootstrapSession(store)
	return dom, store, errors.Trace(err)
}

func (s *testRangerSuite) TestTableRange(c *C) {
	defer testleak.AfterTest(c)()
	dom, store, err := newDomainStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		store.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
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

	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		ctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := domain.GetDomain(ctx).InfoSchema()
		err = plan.Preprocess(ctx, stmts[0], is, false)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(plan.LogicalPlan).Children()[0].(*plan.LogicalSelection)
		conds := make([]expression.Expression, 0, len(selection.Conditions))
		for _, cond := range selection.Conditions {
			conds = append(conds, expression.PushDownNot(ctx, cond, false))
		}
		tbl := selection.Children()[0].(*plan.DataSource).TableInfo()
		col := expression.ColInfo2Col(selection.Schema().Columns, tbl.Columns[0])
		c.Assert(col, NotNil)
		var filter []expression.Expression
		conds, filter = ranger.DetachCondsForTableRange(ctx, conds, col)
		c.Assert(fmt.Sprintf("%s", conds), Equals, tt.accessConds, Commentf("wrong access conditions for expr: %s", tt.exprStr))
		c.Assert(fmt.Sprintf("%s", filter), Equals, tt.filterConds, Commentf("wrong filter conditions for expr: %s", tt.exprStr))
		result, err := ranger.BuildTableRange(conds, new(stmtctx.StatementContext), col.RetType)
		c.Assert(err, IsNil, Commentf("failed to build table range for expr %s", tt.exprStr))
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s", tt.exprStr))
	}
}

func (s *testRangerSuite) TestIndexRange(c *C) {
	defer testleak.AfterTest(c)()
	dom, store, err := newDomainStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		store.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a varchar(50), b int, c double, d varchar(10), e binary(10), index idx_ab(a(50), b), index idx_cb(c, a), index idx_d(d(2)), index idx_e(e(2)))")

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
			resultStr:   "[[\"abc\" NULL,\"abd\" NULL)]",
		},
		{
			indexPos:    0,
			exprStr:     "a LIKE 'abc_'",
			accessConds: "[like(test.t.a, abc_, 92)]",
			filterConds: "[like(test.t.a, abc_, 92)]",
			resultStr:   "[(\"abc\" +inf,\"abd\" NULL)]",
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
			resultStr:   "[[\"\\a\" NULL,\"\\b\" NULL)]",
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
			accessConds: "[in(test.t.a, a) in(test.t.b, 1, 2, <nil>)]",
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
			resultStr:   "[(NULL +inf,1 NULL) (1 +inf,2 NULL) (2 +inf,3 NULL) (3 +inf,+inf +inf]]",
		},
		{
			indexPos:    0,
			exprStr:     "a in (NULL)",
			accessConds: "[in(test.t.a, <nil>)]",
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
			accessConds: "[or(in(test.t.a, <nil>), le(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,\"2\"]]",
		},
		{
			indexPos:    0,
			exprStr:     "not (a not in (NULL) or a > '2')",
			accessConds: "[and(in(test.t.a, <nil>), le(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'b' and a < 'bbb') or (a < 'cb' and a > 'a')",
			accessConds: "[or(and(gt(test.t.a, b), lt(test.t.a, bbb)), and(lt(test.t.a, cb), gt(test.t.a, a)))]",
			filterConds: "[]",
			resultStr:   "[(\"a\" +inf,\"cb\" NULL)]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'a' and a < 'b') or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(gt(test.t.a, a), lt(test.t.a, b)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[]",
			resultStr:   "[(\"a\" +inf,\"c\" NULL)]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'a' and a < 'b' and b < 1) or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(gt(test.t.a, a), lt(test.t.a, b)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[or(and(and(gt(test.t.a, a), lt(test.t.a, b)), lt(test.t.b, 1)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			resultStr:   "[(\"a\" +inf,\"c\" NULL)]",
		},
		{
			indexPos:    0,
			exprStr:     "(a in ('a', 'b') and b < 1) or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(in(test.t.a, a, b), lt(test.t.b, 1)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[]",
			resultStr:   `[["a" -inf,"a" 1) ["b" NULL,"c" NULL)]`,
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
			resultStr:   "[[\"[228 189]\",\"[228 189]\"]]",
		},
	}

	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		ctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := domain.GetDomain(ctx).InfoSchema()
		err = plan.Preprocess(ctx, stmts[0], is, false)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(plan.LogicalPlan).Children()[0].(*plan.LogicalSelection)
		tbl := selection.Children()[0].(*plan.DataSource).TableInfo()
		c.Assert(selection, NotNil, Commentf("expr:%v", tt.exprStr))
		conds := make([]expression.Expression, 0, len(selection.Conditions))
		for _, cond := range selection.Conditions {
			conds = append(conds, expression.PushDownNot(ctx, cond, false))
		}
		cols, lengths := expression.IndexInfo2Cols(selection.Schema().Columns, tbl.Indices[tt.indexPos])
		c.Assert(cols, NotNil)
		ranges, conds, filter, _, err := ranger.DetachCondAndBuildRangeForIndex(ctx, conds, cols, lengths)
		c.Assert(err, IsNil)
		c.Assert(fmt.Sprintf("%s", conds), Equals, tt.accessConds, Commentf("wrong access conditions for expr: %s", tt.exprStr))
		c.Assert(fmt.Sprintf("%s", filter), Equals, tt.filterConds, Commentf("wrong filter conditions for expr: %s", tt.exprStr))
		got := fmt.Sprintf("%v", ranges)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s", tt.exprStr))
	}
}

// for issue #6661
func (s *testRangerSuite) TestIndexRangeForUnsignedInt(c *C) {
	defer testleak.AfterTest(c)()
	dom, store, err := newDomainStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		store.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a smallint(5) unsigned,key (a) )")

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
			accessConds: "[not(in(test.t.a, 111))]",
			filterConds: "[]",
			resultStr:   `[(NULL,111) (111,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (1, 2, 9223372036854775810)`,
			accessConds: "[not(in(test.t.a, 1, 2, 9223372036854775810))]",
			filterConds: "[]",
			resultStr:   `[(NULL,1) (2,9223372036854775810) (9223372036854775810,+inf]]`,
		},
	}

	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		ctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := domain.GetDomain(ctx).InfoSchema()
		err = plan.Preprocess(ctx, stmts[0], is, false)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(plan.LogicalPlan).Children()[0].(*plan.LogicalSelection)
		tbl := selection.Children()[0].(*plan.DataSource).TableInfo()
		c.Assert(selection, NotNil, Commentf("expr:%v", tt.exprStr))
		conds := make([]expression.Expression, 0, len(selection.Conditions))
		for _, cond := range selection.Conditions {
			conds = append(conds, expression.PushDownNot(ctx, cond, false))
		}
		cols, lengths := expression.IndexInfo2Cols(selection.Schema().Columns, tbl.Indices[tt.indexPos])
		c.Assert(cols, NotNil)
		ranges, conds, filter, _, err := ranger.DetachCondAndBuildRangeForIndex(ctx, conds, cols, lengths)
		c.Assert(err, IsNil)
		c.Assert(fmt.Sprintf("%s", conds), Equals, tt.accessConds, Commentf("wrong access conditions for expr: %s", tt.exprStr))
		c.Assert(fmt.Sprintf("%s", filter), Equals, tt.filterConds, Commentf("wrong filter conditions for expr: %s", tt.exprStr))
		got := fmt.Sprintf("%v", ranges)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s", tt.exprStr))
	}
}

func (s *testRangerSuite) TestColumnRange(c *C) {
	defer testleak.AfterTest(c)()
	dom, store, err := newDomainStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		store.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int, b double, c float(3, 2), d varchar(3), e bigint unsigned)")

	tests := []struct {
		colPos      int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			colPos:      0,
			exprStr:     "a = 1 and b > 1",
			accessConds: "[eq(test.t.a, 1)]",
			filterConds: "[gt(test.t.b, 1)]",
			resultStr:   "[[1,1]]",
		},
		{
			colPos:      1,
			exprStr:     "b > 1",
			accessConds: "[gt(test.t.b, 1)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "1 = a",
			accessConds: "[eq(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
		},
		{
			colPos:      0,
			exprStr:     "a != 1",
			accessConds: "[ne(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "1 != a",
			accessConds: "[ne(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "a > 1",
			accessConds: "[gt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "1 < a",
			accessConds: "[lt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "a >= 1",
			accessConds: "[ge(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "1 <= a",
			accessConds: "[le(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "a < 1",
			accessConds: "[lt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
		},
		{
			colPos:      0,
			exprStr:     "1 > a",
			accessConds: "[gt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
		},
		{
			colPos:      0,
			exprStr:     "a <= 1",
			accessConds: "[le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
		},
		{
			colPos:      0,
			exprStr:     "1 >= a",
			accessConds: "[ge(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
		},
		{
			colPos:      0,
			exprStr:     "(a)",
			accessConds: "[test.t.a]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "a in (1, 3, NULL, 2)",
			accessConds: "[in(test.t.a, 1, 3, <nil>, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3]]",
		},
		{
			colPos:      0,
			exprStr:     `a IN (8,8,81,45)`,
			accessConds: "[in(test.t.a, 8, 8, 81, 45)]",
			filterConds: "[]",
			resultStr:   `[[8,8] [45,45] [81,81]]`,
		},
		{
			colPos:      0,
			exprStr:     "a between 1 and 2",
			accessConds: "[ge(test.t.a, 1) le(test.t.a, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,2]]",
		},
		{
			colPos:      0,
			exprStr:     "a not between 1 and 2",
			accessConds: "[or(lt(test.t.a, 1), gt(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (2,+inf]]",
		},
		//{
		// `a > null` will be converted to `castAsString(a) > null` which can not be extracted as access condition.
		//	exprStr:   "a not between null and 0",
		//	resultStr[(0,+inf]]
		//},
		{
			colPos:      0,
			exprStr:     "a between 2 and 1",
			accessConds: "[ge(test.t.a, 2) le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			colPos:      0,
			exprStr:     "a not between 2 and 1",
			accessConds: "[or(lt(test.t.a, 2), gt(test.t.a, 1))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "a IS NULL",
			accessConds: "[isnull(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL]]",
		},
		{
			colPos:      0,
			exprStr:     "a IS NOT NULL",
			accessConds: "[not(isnull(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "a IS TRUE",
			accessConds: "[istrue(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
		},
		{
			colPos:      0,
			exprStr:     "a IS NOT TRUE",
			accessConds: "[not(istrue(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL] [0,0]]",
		},
		{
			colPos:      0,
			exprStr:     "a IS FALSE",
			accessConds: "[isfalse(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[0,0]]",
		},
		{
			colPos:      0,
			exprStr:     "a IS NOT FALSE",
			accessConds: "[not(isfalse(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[NULL,0) (0,+inf]]",
		},
		{
			colPos:      1,
			exprStr:     `b in (1, '2.1')`,
			accessConds: "[in(test.t.b, 1, 2.1)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2.1,2.1]]",
		},
		{
			colPos:      0,
			exprStr:     `a > 9223372036854775807`,
			accessConds: "[gt(test.t.a, 9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[(9223372036854775807,+inf]]",
		},
		{
			colPos:      2,
			exprStr:     `c > 111.11111111`,
			accessConds: "[gt(test.t.c, 111.11111111)]",
			filterConds: "[]",
			resultStr:   "[[111.111115,+inf]]",
		},
		{
			colPos:      3,
			exprStr:     `d > 'aaaaaaaaaaaaaa'`,
			accessConds: "[gt(test.t.d, aaaaaaaaaaaaaa)]",
			filterConds: "[]",
			resultStr:   "[(\"aaaaaaaaaaaaaa\",+inf]]",
		},
		{
			colPos:      4,
			exprStr:     `e > 18446744073709500000`,
			accessConds: "[gt(test.t.e, 18446744073709500000)]",
			filterConds: "[]",
			resultStr:   "[(18446744073709500000,+inf]]",
		},
	}

	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		ctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := domain.GetDomain(ctx).InfoSchema()
		err = plan.Preprocess(ctx, stmts[0], is, false)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		sel := p.(plan.LogicalPlan).Children()[0].(*plan.LogicalSelection)
		ds, ok := sel.Children()[0].(*plan.DataSource)
		c.Assert(ok, IsTrue, Commentf("expr:%v", tt.exprStr))
		conds := make([]expression.Expression, 0, len(sel.Conditions))
		for _, cond := range sel.Conditions {
			conds = append(conds, expression.PushDownNot(ctx, cond, false))
		}
		col := expression.ColInfo2Col(sel.Schema().Columns, ds.TableInfo().Columns[tt.colPos])
		c.Assert(col, NotNil)
		conds = ranger.ExtractAccessConditionsForColumn(conds, col.ColName)
		c.Assert(fmt.Sprintf("%s", conds), Equals, tt.accessConds, Commentf("wrong access conditions for expr: %s", tt.exprStr))
		result, err := ranger.BuildColumnRange(conds, new(stmtctx.StatementContext), col.RetType)
		c.Assert(err, IsNil)
		got := fmt.Sprintf("%s", result)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s, col: %v", tt.exprStr, col))
	}
}

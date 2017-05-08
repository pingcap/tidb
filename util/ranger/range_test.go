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
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
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

func newStoreWithBootstrap() (kv.Storage, error) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = tidb.BootstrapSession(store)
	return store, errors.Trace(err)
}

func (s *testRangerSuite) TestTableRange(c *C) {
	defer testleak.AfterTest(c)()
	store, err := newStoreWithBootstrap()
	defer store.Close()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a int)")

	tests := []struct {
		exprStr   string
		resultStr string
	}{
		{
			exprStr:   "a = 1",
			resultStr: "[[1,1]]",
		},
		{
			exprStr:   "1 = a",
			resultStr: "[[1,1]]",
		},
		{
			exprStr:   "a != 1",
			resultStr: "[(-inf,0] [2,+inf)]",
		},
		{
			exprStr:   "1 != a",
			resultStr: "[(-inf,0] [2,+inf)]",
		},
		{
			exprStr:   "a > 1",
			resultStr: "[[2,+inf)]",
		},
		{
			exprStr:   "1 < a",
			resultStr: "[[2,+inf)]",
		},
		{
			exprStr:   "a >= 1",
			resultStr: "[[1,+inf)]",
		},
		{
			exprStr:   "1 <= a",
			resultStr: "[[1,+inf)]",
		},
		{
			exprStr:   "a < 1",
			resultStr: "[(-inf,0]]",
		},
		{
			exprStr:   "1 > a",
			resultStr: "[(-inf,0]]",
		},
		{
			exprStr:   "a <= 1",
			resultStr: "[(-inf,1]]",
		},
		{
			exprStr:   "1 >= a",
			resultStr: "[(-inf,1]]",
		},
		{
			exprStr:   "(a)",
			resultStr: "[(-inf,-1] [1,+inf)]",
		},
		{
			exprStr:   "a in (1, 3, NULL, 2)",
			resultStr: "[(-inf,-inf) [1,1] [2,2] [3,3]]",
		},
		{
			exprStr:   `a IN (8,8,81,45)`,
			resultStr: `[[8,8] [45,45] [81,81]]`,
		},
		{
			exprStr:   "a between 1 and 2",
			resultStr: "[[1,2]]",
		},
		{
			exprStr:   "a not between 1 and 2",
			resultStr: "[(-inf,0] [3,+inf)]",
		},
		{
			exprStr:   "a not between null and 0",
			resultStr: "[[1,+inf)]",
		},
		{
			exprStr:   "a between 2 and 1",
			resultStr: "[]",
		},
		{
			exprStr:   "a not between 2 and 1",
			resultStr: "[(-inf,+inf)]",
		},
		{
			exprStr:   "a IS NULL",
			resultStr: "[(-inf,-inf)]",
		},
		{
			exprStr:   "a IS NOT NULL",
			resultStr: "[(-inf,+inf)]",
		},
		{
			exprStr:   "a IS TRUE",
			resultStr: "[(-inf,-1] [1,+inf)]",
		},
		{
			exprStr:   "a IS NOT TRUE",
			resultStr: "[(-inf,-inf) [0,0]]",
		},
		{
			exprStr:   "a IS FALSE",
			resultStr: "[[0,0]]",
		},
		{
			exprStr:   "a IS NOT FALSE",
			resultStr: "[(-inf,-1] [1,+inf)]",
		},
	}

	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		ctx := testKit.Se.(context.Context)
		stmts, err := tidb.Parse(ctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := sessionctx.GetDomain(ctx).InfoSchema()
		err = plan.ResolveName(stmts[0], is, ctx)

		p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		var selection *plan.Selection
		for _, child := range p.Children() {
			plan, ok := child.(*plan.Selection)
			if ok {
				selection = plan
				break
			}
		}
		c.Assert(selection, NotNil, Commentf("expr:%v", tt.exprStr))
		conds := make([]expression.Expression, 0, len(selection.Conditions))
		for _, cond := range selection.Conditions {
			conds = append(conds, expression.PushDownNot(cond, false, ctx))
		}
		result, err := ranger.BuildTableRange(conds, new(variable.StatementContext))
		c.Assert(err, IsNil)
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s", tt.exprStr))
	}
}

func (s *testRangerSuite) TestIndexRange(c *C) {
	defer testleak.AfterTest(c)()
	store, err := newStoreWithBootstrap()
	defer store.Close()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t(a varchar(50), b int, index idx_ab(a, b))")

	tests := []struct {
		exprStr    string
		resultStr  string
		inAndEqCnt int
	}{
		{
			exprStr:    "a LIKE 'abc%'",
			resultStr:  "[[abc <nil>,abd <nil>)]",
			inAndEqCnt: 0,
		},
		{
			exprStr:    "a LIKE 'abc_'",
			resultStr:  "[(abc +inf,abd <nil>)]",
			inAndEqCnt: 0,
		},
		{
			exprStr:    "a LIKE 'abc'",
			resultStr:  "[[abc,abc]]",
			inAndEqCnt: 0,
		},
		{
			exprStr:    `a LIKE "ab\_c"`,
			resultStr:  "[[ab_c,ab_c]]",
			inAndEqCnt: 0,
		},
		{
			exprStr:    "a LIKE '%'",
			resultStr:  "[[-inf,+inf]]",
			inAndEqCnt: 0,
		},
		{
			exprStr:    `a LIKE '\%a'`,
			resultStr:  `[[%a,%a]]`,
			inAndEqCnt: 0,
		},
		{
			exprStr:    `a LIKE "\\"`,
			resultStr:  `[[\,\]]`,
			inAndEqCnt: 0,
		},
		{
			exprStr:    `a LIKE "\\\\a%"`,
			resultStr:  `[[\a <nil>,\b <nil>)]`,
			inAndEqCnt: 0,
		},
		{
			exprStr:    `a > NULL`,
			resultStr:  `[]`,
			inAndEqCnt: 0,
		},
		{
			exprStr:    `a = 'a' and b in (1, 2, 3)`,
			resultStr:  `[[a 1,a 1] [a 2,a 2] [a 3,a 3]]`,
			inAndEqCnt: 2,
		},
	}

	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		ctx := testKit.Se.(context.Context)
		stmts, err := tidb.Parse(ctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := sessionctx.GetDomain(ctx).InfoSchema()
		err = plan.ResolveName(stmts[0], is, ctx)

		p, err := plan.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		var selection *plan.Selection
		for _, child := range p.Children() {
			plan, ok := child.(*plan.Selection)
			if ok {
				selection = plan
				break
			}
		}
		tbl := selection.Children()[0].(*plan.DataSource).TableInfo()
		c.Assert(selection, NotNil, Commentf("expr:%v", tt.exprStr))
		conds := make([]expression.Expression, 0, len(selection.Conditions))
		for _, cond := range selection.Conditions {
			conds = append(conds, expression.PushDownNot(cond, false, ctx))
		}
		result, err := ranger.BuildIndexRange(new(variable.StatementContext), tbl, tbl.Indices[0], tt.inAndEqCnt, conds)
		c.Assert(err, IsNil)
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s", tt.exprStr))
	}
}

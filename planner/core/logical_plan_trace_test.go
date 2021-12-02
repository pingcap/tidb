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

package core

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testPlanSuite) TestLogicalOptimizeWithTraceEnabled(c *C) {
	sql := "select * from t where a in (1,2)"
	defer testleak.AfterTest(c)()
	tt := []struct {
		flags []uint64
		steps int
	}{
		{
			flags: []uint64{
				flagEliminateAgg,
				flagPushDownAgg},
			steps: 2,
		},
		{
			flags: []uint64{
				flagEliminateAgg,
				flagPushDownAgg,
				flagPrunColumns,
				flagBuildKeyInfo,
			},
			steps: 4,
		},
		{
			flags: []uint64{},
			steps: 0,
		},
	}

	for i, tc := range tt {
		comment := Commentf("case:%v sql:%s", i, sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		c.Assert(err, IsNil, comment)
		sctx := MockContext()
		sctx.GetSessionVars().StmtCtx.EnableOptimizeTrace = true
		builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
		ctx := context.TODO()
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		flag := uint64(0)
		for _, f := range tc.flags {
			flag = flag | f
		}
		p, err = logicalOptimize(ctx, flag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		_, ok := p.(*LogicalProjection)
		c.Assert(ok, IsTrue)
		otrace := sctx.GetSessionVars().StmtCtx.LogicalOptimizeTrace
		c.Assert(otrace, NotNil)
		c.Assert(len(otrace.Steps), Equals, tc.steps)
	}
}

func (s *testPlanSuite) TestSingleRuleTraceStep(c *C) {
	defer testleak.AfterTest(c)()
	tt := []struct {
		sql             string
		flags           []uint64
		assertRuleName  string
		assertRuleSteps []assertTraceStep
	}{
		{
			sql:            "select min(distinct a) from t group by a",
			flags:          []uint64{flagBuildKeyInfo, flagEliminateAgg},
			assertRuleName: "aggregation_eliminate",
			assertRuleSteps: []assertTraceStep{
				{
					assertReason: "[test.t.a] is a unique key",
					assertAction: "min(distinct ...) is simplified to min(...)",
				},
				{
					assertReason: "[test.t.a] is a unique key",
					assertAction: "aggregation is simplified to a projection",
				},
			},
		},
		{
			sql:            "select 1+num from (select 1+a as num from t) t1;",
			flags:          []uint64{flagEliminateProjection},
			assertRuleName: "projection_eliminate",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "Proj[2] is eliminated, Proj[3]'s expressions changed into[plus(1, plus(1, test.t.a))]",
					assertReason: "Proj[3]'s child proj[2] is redundant",
				},
			},
		},
		{
			sql:            "select t1.b,t1.c from t as t1 left join t as t2 on t1.a = t2.a;",
			flags:          []uint64{flagBuildKeyInfo, flagEliminateOuterJoin},
			assertRuleName: "outer_join_eliminate",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "Outer join[3] is eliminated and become DataSource[1]",
					assertReason: "The columns[test.t.b,test.t.c] are from outer table, and the inner join keys[test.t.a] are unique",
				},
			},
		},
		{
			sql:            "select count(distinct t1.a, t1.b) from t t1 left join t t2 on t1.b = t2.b",
			flags:          []uint64{flagPrunColumns, flagBuildKeyInfo, flagEliminateOuterJoin},
			assertRuleName: "outer_join_eliminate",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "Outer join[3] is eliminated and become DataSource[1]",
					assertReason: "The columns[test.t.a,test.t.b] in aggregation functions are from outer table, and they are duplicate agnostic",
				},
			},
		},
	}

	for i, tc := range tt {
		sql := tc.sql
		comment := Commentf("case:%v sql:%s", i, sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		c.Assert(err, IsNil, comment)
		sctx := MockContext()
		sctx.GetSessionVars().StmtCtx.EnableOptimizeTrace = true
		builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
		ctx := context.TODO()
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		flag := uint64(0)
		for _, f := range tc.flags {
			flag = flag | f
		}
		p, err = logicalOptimize(ctx, flag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		_, ok := p.(*LogicalProjection)
		c.Assert(ok, IsTrue)
		otrace := sctx.GetSessionVars().StmtCtx.LogicalOptimizeTrace
		c.Assert(otrace, NotNil)
		assert := false
		for _, step := range otrace.Steps {
			if step.RuleName == tc.assertRuleName {
				assert = true
				for i, ruleStep := range step.Steps {
					c.Assert(ruleStep.Action, Equals, tc.assertRuleSteps[i].assertAction)
					c.Assert(ruleStep.Reason, Equals, tc.assertRuleSteps[i].assertReason)
				}
			}
		}
		c.Assert(assert, IsTrue)
	}
}

type assertTraceStep struct {
	assertReason string
	assertAction string
}

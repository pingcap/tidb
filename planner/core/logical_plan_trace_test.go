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
			sql:            "select count(*) from t a , t b, t c",
			flags:          []uint64{flagBuildKeyInfo, flagPrunColumns, flagPushDownAgg},
			assertRuleName: "aggregation_push_down",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "aggFuncs[count(Column#38)] pushed into join[5] right path",
					assertReason: "agg[6] push down into join[5]",
				},
			},
		},
		{
			sql:            "select sum(c1) from (select c c1, d c2 from t a union all select a c1, b c2 from t b) x group by c2",
			flags:          []uint64{flagBuildKeyInfo, flagPrunColumns, flagPushDownAgg},
			assertRuleName: "aggregation_push_down",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "union's children changed into[[id:11,tp:Aggregation],[id:12,tp:Aggregation]]",
					assertReason: "agg[8] push down across union[5]",
				},
				{
					assertAction: "agg's functions changed into[sum(test.t.c),firstrow(test.t.d)]",
					assertReason: "agg[11] push down across projection[6]",
				},
				{
					assertAction: "agg's functions changed into[sum(test.t.a),firstrow(test.t.b)]",
					assertReason: "agg[12] push down across projection[7]",
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
		sctx.GetSessionVars().AllowAggPushDown = true
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

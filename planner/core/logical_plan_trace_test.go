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
			sql:            "select * from pt3 where ptn > 3;",
			flags:          []uint64{flagPartitionProcessor, flagPredicatePushDown, flagBuildKeyInfo, flagPrunColumns},
			assertRuleName: "partition_processor",
			assertRuleSteps: []assertTraceStep{
				{
					assertReason: "Datasource[1] has multiple needed partitions[p1,p2] after pruning",
					assertAction: "Datasource[1] becomes PartitionUnion[6] with children[TableScan[1],TableScan[1]]",
				},
			},
		},
		{
			sql:            "select * from pt3 where ptn = 1;",
			flags:          []uint64{flagPartitionProcessor, flagPredicatePushDown, flagBuildKeyInfo, flagPrunColumns},
			assertRuleName: "partition_processor",
			assertRuleSteps: []assertTraceStep{
				{
					assertReason: "Datasource[1] has one needed partition[p1] after pruning",
					assertAction: "Datasource[1] becomes TableScan[1]",
				},
			},
		},
		{
			sql:            "select * from pt2 where ptn in (1,2,3);",
			flags:          []uint64{flagPartitionProcessor, flagPredicatePushDown, flagBuildKeyInfo, flagPrunColumns},
			assertRuleName: "partition_processor",
			assertRuleSteps: []assertTraceStep{
				{
					assertReason: "Datasource[1] has multiple needed partitions[p1,p2] after pruning",
					assertAction: "Datasource[1] becomes PartitionUnion[7] with children[TableScan[1],TableScan[1]]",
				},
			},
		},
		{
			sql:            "select * from pt2 where ptn = 1;",
			flags:          []uint64{flagPartitionProcessor, flagPredicatePushDown, flagBuildKeyInfo, flagPrunColumns},
			assertRuleName: "partition_processor",
			assertRuleSteps: []assertTraceStep{
				{
					assertReason: "Datasource[1] has one needed partition[p2] after pruning",
					assertAction: "Datasource[1] becomes TableScan[1]",
				},
			},
		},
		{
			sql:            "select * from pt1 where ptn > 100;",
			flags:          []uint64{flagPartitionProcessor, flagPredicatePushDown, flagBuildKeyInfo, flagPrunColumns},
			assertRuleName: "partition_processor",
			assertRuleSteps: []assertTraceStep{
				{
					assertReason: "Datasource[1] doesn't have needed partition table after pruning",
					assertAction: "Datasource[1] becomes TableDual[5]",
				},
			},
		},
		{
			sql:            "select * from pt1 where ptn in (10,20);",
			flags:          []uint64{flagPartitionProcessor, flagPredicatePushDown, flagBuildKeyInfo, flagPrunColumns},
			assertRuleName: "partition_processor",
			assertRuleSteps: []assertTraceStep{
				{
					assertReason: "Datasource[1] has multiple needed partitions[p1,p2] after pruning",
					assertAction: "Datasource[1] becomes PartitionUnion[7] with children[TableScan[1],TableScan[1]]",
				},
			},
		},
		{
			sql:            "select * from pt1 where ptn < 4;",
			flags:          []uint64{flagPartitionProcessor, flagPredicatePushDown, flagBuildKeyInfo, flagPrunColumns},
			assertRuleName: "partition_processor",
			assertRuleSteps: []assertTraceStep{
				{
					assertReason: "Datasource[1] has one needed partition[p1] after pruning",
					assertAction: "Datasource[1] becomes TableScan[1]",
				},
			},
		},
		{
			sql:            "select * from (t t1, t t2, t t3,t t4) union all select * from (t t5, t t6, t t7,t t8)",
			flags:          []uint64{flagBuildKeyInfo, flagPrunColumns, flagDecorrelate, flagPredicatePushDown, flagEliminateOuterJoin, flagJoinReOrder},
			assertRuleName: "join_reorder",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "join order becomes [((t1*t2)*(t3*t4)),((t5*t6)*(t7*t8))] from original [(((t1*t2)*t3)*t4),(((t5*t6)*t7)*t8)]",
					assertReason: "join cost during reorder: [[t1, cost:10000],[t2, cost:10000],[t3, cost:10000],[t4, cost:10000],[t5, cost:10000],[t6, cost:10000],[t7, cost:10000],[t8, cost:10000]]",
				},
			},
		},
		{
			sql:            "select * from t t1, t t2, t t3 where t1.a=t2.a and t3.a=t2.a and t1.a=t3.a",
			flags:          []uint64{flagBuildKeyInfo, flagPrunColumns, flagDecorrelate, flagPredicatePushDown, flagEliminateOuterJoin, flagJoinReOrder},
			assertRuleName: "join_reorder",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "join order becomes ((t1*t2)*t3) from original ((t1*t2)*t3)",
					assertReason: "join cost during reorder: [[((t1*t2)*t3), cost:58125],[(t1*t2), cost:32500],[(t1*t3), cost:32500],[t1, cost:10000],[t2, cost:10000],[t3, cost:10000]]",
				},
			},
		},
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
			sql:            "select count(*) from t a , t b, t c",
			flags:          []uint64{flagBuildKeyInfo, flagPrunColumns, flagPushDownAgg},
			assertRuleName: "aggregation_push_down",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "agg[6] pushed down across join[5], and join right path becomes agg[8]",
					assertReason: "agg[6]'s functions[count(Column#38)] are decomposable with join",
				},
			},
		},
		{
			sql:            "select sum(c1) from (select c c1, d c2 from t a union all select a c1, b c2 from t b) x group by c2",
			flags:          []uint64{flagBuildKeyInfo, flagPrunColumns, flagPushDownAgg},
			assertRuleName: "aggregation_push_down",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "agg[8] pushed down, and union[5]'s children changed into[[id:11,tp:Aggregation],[id:12,tp:Aggregation]]",
					assertReason: "agg[8] functions[sum(Column#28)] are decomposable with union",
				},
				{
					assertAction: "proj[6] is eliminated, and agg[11]'s functions changed into[sum(test.t.c),firstrow(test.t.d)]",
					assertReason: "Proj[6] is directly below an agg[11] and has no side effects",
				},
				{
					assertAction: "proj[7] is eliminated, and agg[12]'s functions changed into[sum(test.t.a),firstrow(test.t.b)]",
					assertReason: "Proj[7] is directly below an agg[12] and has no side effects",
				},
			},
		},
		{
			sql:            "select max(a)-min(a) from t",
			flags:          []uint64{flagBuildKeyInfo, flagPrunColumns, flagMaxMinEliminate},
			assertRuleName: "max_min_eliminate",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "add sort[8],add limit[9] during eliminating agg[4] max function",
					assertReason: "agg[4] has only one function[max] without group by, the columns in agg[4] should be sorted",
				},
				{
					assertAction: "add sort[10],add limit[11] during eliminating agg[6] min function",
					assertReason: "agg[6] has only one function[min] without group by, the columns in agg[6] should be sorted",
				},
				{
					assertAction: "agg[2] splited into aggs[4,6], and add joins[12] to connect them during eliminating agg[2] multi min/max functions",
					assertReason: "each column is sorted and can benefit from index/primary key in agg[4,6] and none of them has group by clause",
				},
			},
		},
		{
			sql:            "select max(e) from t",
			flags:          []uint64{flagBuildKeyInfo, flagPrunColumns, flagMaxMinEliminate},
			assertRuleName: "max_min_eliminate",
			assertRuleSteps: []assertTraceStep{
				{
					assertAction: "add selection[4],add sort[5],add limit[6] during eliminating agg[2] max function",
					assertReason: "agg[2] has only one function[max] without group by, the columns in agg[2] shouldn't be NULL and needs NULL to be filtered out, the columns in agg[2] should be sorted",
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
					assertReason: "The columns[test.t.a,test.t.b] in agg are from outer table, and the agg functions are duplicate agnostic",
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
		_, err = logicalOptimize(ctx, flag, p.(LogicalPlan))
		c.Assert(err, IsNil)
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

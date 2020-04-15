// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	"context"
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/infoschema"
	plannercore "github.com/pingcap/tidb/v4/planner/core"
	"github.com/pingcap/tidb/v4/planner/memo"
	"github.com/pingcap/tidb/v4/planner/property"
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testCascadesSuite{})

type testCascadesSuite struct {
	*parser.Parser
	is        infoschema.InfoSchema
	sctx      sessionctx.Context
	optimizer *Optimizer
}

func (s *testCascadesSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	s.sctx = plannercore.MockContext()
	s.Parser = parser.New()
	s.optimizer = NewOptimizer()
}

func (s *testCascadesSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testCascadesSuite) TestImplGroupZeroCost(c *C) {
	stmt, err := s.ParseOneStmt("select t1.a, t2.a from t as t1 left join t as t2 on t1.a = t2.a where t1.a < 1.0", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	rootGroup := memo.Convert2Group(logic)
	prop := &property.PhysicalProperty{
		ExpectedCnt: math.MaxFloat64,
	}
	impl, err := s.optimizer.implGroup(rootGroup, prop, 0.0)
	c.Assert(impl, IsNil)
	c.Assert(err, IsNil)
}

func (s *testCascadesSuite) TestInitGroupSchema(c *C) {
	stmt, err := s.ParseOneStmt("select a from t", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	g := memo.Convert2Group(logic)
	c.Assert(g, NotNil)
	c.Assert(g.Prop, NotNil)
	c.Assert(g.Prop.Schema.Len(), Equals, 1)
	c.Assert(g.Prop.Stats, IsNil)
}

func (s *testCascadesSuite) TestFillGroupStats(c *C) {
	stmt, err := s.ParseOneStmt("select * from t t1 join t t2 on t1.a = t2.a", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	rootGroup := memo.Convert2Group(logic)
	err = s.optimizer.fillGroupStats(rootGroup)
	c.Assert(err, IsNil)
	c.Assert(rootGroup.Prop.Stats, NotNil)
}

func (s *testCascadesSuite) TestPreparePossibleProperties(c *C) {
	s.optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandDataSource: {
			NewRuleEnumeratePaths(),
		},
	})
	defer func() {
		s.optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	stmt, err := s.ParseOneStmt("select f, sum(a) from t group by f", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	logic, err = s.optimizer.onPhasePreprocessing(s.sctx, logic)
	c.Assert(err, IsNil)
	// collect the target columns: f, a
	ds, ok := logic.Children()[0].Children()[0].(*plannercore.DataSource)
	c.Assert(ok, IsTrue)
	var columnF, columnA *expression.Column
	for i, col := range ds.Columns {
		if col.Name.L == "f" {
			columnF = ds.Schema().Columns[i]
		} else if col.Name.L == "a" {
			columnA = ds.Schema().Columns[i]
		}
	}
	c.Assert(columnF, NotNil)
	c.Assert(columnA, NotNil)

	agg, ok := logic.Children()[0].(*plannercore.LogicalAggregation)
	c.Assert(ok, IsTrue)
	group := memo.Convert2Group(agg)
	err = s.optimizer.onPhaseExploration(s.sctx, group)
	c.Assert(err, IsNil)
	// The memo looks like this:
	// Group#0 Schema:[Column#13,test.t.f]
	//   Aggregation_2 input:[Group#1], group by:test.t.f, funcs:sum(test.t.a), firstrow(test.t.f)
	// Group#1 Schema:[test.t.a,test.t.f]
	//   TiKVSingleGather_5 input:[Group#2], table:t
	//   TiKVSingleGather_9 input:[Group#3], table:t, index:f_g
	//   TiKVSingleGather_7 input:[Group#4], table:t, index:f
	// Group#2 Schema:[test.t.a,test.t.f]
	//   TableScan_4 table:t, pk col:test.t.a
	// Group#3 Schema:[test.t.a,test.t.f]
	//   IndexScan_8 table:t, index:f, g
	// Group#4 Schema:[test.t.a,test.t.f]
	//   IndexScan_6 table:t, index:f
	propMap := make(map[*memo.Group][][]*expression.Column)
	aggProp := preparePossibleProperties(group, propMap)
	// We only have one prop for Group0 : f
	c.Assert(len(aggProp), Equals, 1)
	c.Assert(aggProp[0][0].Equal(nil, columnF), IsTrue)

	gatherGroup := group.Equivalents.Front().Value.(*memo.GroupExpr).Children[0]
	gatherProp, ok := propMap[gatherGroup]
	c.Assert(ok, IsTrue)
	// We have 2 props for Group1: [f], [a]
	c.Assert(len(gatherProp), Equals, 2)
	for _, prop := range gatherProp {
		c.Assert(len(prop), Equals, 1)
		c.Assert(prop[0].Equal(nil, columnA) || prop[0].Equal(nil, columnF), IsTrue)
	}
}

// fakeTransformation is used for TestAppliedRuleSet.
type fakeTransformation struct {
	baseRule
	appliedTimes int
}

// OnTransform implements Transformation interface.
func (rule *fakeTransformation) OnTransform(old *memo.ExprIter) (newExprs []*memo.GroupExpr, eraseOld bool, eraseAll bool, err error) {
	rule.appliedTimes++
	old.GetExpr().AddAppliedRule(rule)
	return []*memo.GroupExpr{old.GetExpr()}, true, false, nil
}

func (s *testCascadesSuite) TestAppliedRuleSet(c *C) {
	rule := fakeTransformation{}
	rule.pattern = memo.NewPattern(memo.OperandProjection, memo.EngineAll)
	s.optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandProjection: {
			&rule,
		},
	})
	defer func() {
		s.optimizer.ResetTransformationRules(DefaultRuleBatches...)
	}()
	stmt, err := s.ParseOneStmt("select 1", "", "")
	c.Assert(err, IsNil)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
	c.Assert(err, IsNil)
	logic, ok := p.(plannercore.LogicalPlan)
	c.Assert(ok, IsTrue)
	group := memo.Convert2Group(logic)
	err = s.optimizer.onPhaseExploration(s.sctx, group)
	c.Assert(err, IsNil)
	c.Assert(rule.appliedTimes, Equals, 1)
}

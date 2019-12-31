// Copyright 2019 PingCAP, Inc.
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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testTransformationRuleSuite{})

type testTransformationRuleSuite struct {
	*parser.Parser
	is        infoschema.InfoSchema
	sctx      sessionctx.Context
	testData  testutil.TestData
	optimizer *Optimizer
}

func (s *testTransformationRuleSuite) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable()})
	s.sctx = plannercore.MockContext()
	s.Parser = parser.New()
	s.optimizer = NewOptimizer()
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "transformation_rules_suite")
	c.Assert(err, IsNil)
	s.Parser.EnableWindowFunc(true)
}

func (s *testTransformationRuleSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func testGroupToString(input []string, output []struct {
	SQL    string
	Result []string
}, s *testTransformationRuleSuite, c *C) {
	for i, sql := range input {
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
		c.Assert(err, IsNil)
		logic, ok := p.(plannercore.LogicalPlan)
		c.Assert(ok, IsTrue)
		logic, err = s.optimizer.onPhasePreprocessing(s.sctx, logic)
		c.Assert(err, IsNil)
		group := memo.Convert2Group(logic)
		err = s.optimizer.onPhaseExploration(s.sctx, group)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Result = ToString(group)
		})
		c.Assert(ToString(group), DeepEquals, output[i].Result)
	}
}

func (s *testTransformationRuleSuite) TestAggPushDownGather(c *C) {
	s.optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandAggregation: {
			NewRulePushAggDownGather(),
		},
		memo.OperandDataSource: {
			NewRuleEnumeratePaths(),
		},
	})
	defer func() {
		s.optimizer.ResetTransformationRules(defaultTransformationMap)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
		c.Assert(err, IsNil)
		logic, ok := p.(plannercore.LogicalPlan)
		c.Assert(ok, IsTrue)
		logic, err = s.optimizer.onPhasePreprocessing(s.sctx, logic)
		c.Assert(err, IsNil)
		group := memo.Convert2Group(logic)
		err = s.optimizer.onPhaseExploration(s.sctx, group)
		c.Assert(err, IsNil)
		// BuildKeyInfo here to test the KeyInfo for partialAgg.
		group.BuildKeyInfo()
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].Result = ToString(group)
		})
		c.Assert(ToString(group), DeepEquals, output[i].Result)
	}
}

func (s *testTransformationRuleSuite) TestPredicatePushDown(c *C) {
	s.optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandSelection: {
			NewRulePushSelDownTableScan(),
			NewRulePushSelDownTiKVSingleGather(),
			NewRulePushSelDownSort(),
			NewRulePushSelDownProjection(),
			NewRulePushSelDownAggregation(),
			NewRulePushSelDownJoin(),
			NewRulePushSelDownIndexScan(),
			NewRulePushSelDownUnionAll(),
			NewRulePushSelDownWindow(),
			NewRuleMergeAdjacentSelection(),
		},
		memo.OperandDataSource: {
			NewRuleEnumeratePaths(),
		},
	})
	defer func() {
		s.optimizer.ResetTransformationRules(defaultTransformationMap)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	testGroupToString(input, output, s, c)
}

func (s *testTransformationRuleSuite) TestTopNRules(c *C) {
	s.optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandLimit: {
			NewRuleTransformLimitToTopN(),
			NewRulePushLimitDownProjection(),
			NewRulePushLimitDownUnionAll(),
		},
		memo.OperandDataSource: {
			NewRuleEnumeratePaths(),
		},
		memo.OperandTopN: {
			NewRulePushTopNDownProjection(),
			NewRulePushTopNDownUnionAll(),
			NewRulePushTopNDownTiKVSingleGather(),
		},
	})
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	testGroupToString(input, output, s, c)
}

func (s *testTransformationRuleSuite) TestProjectionElimination(c *C) {
	s.optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandProjection: {
			NewRuleEliminateProjection(),
			NewRuleMergeAdjacentProjection(),
		},
	})
	defer func() {
		s.optimizer.ResetTransformationRules(defaultTransformationMap)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	testGroupToString(input, output, s, c)
}

func (s *testTransformationRuleSuite) TestMergeAggregationProjection(c *C) {
	s.optimizer.ResetTransformationRules(map[memo.Operand][]Transformation{
		memo.OperandAggregation: {
			NewRuleMergeAggregationProjection(),
		},
	})
	defer func() {
		s.optimizer.ResetTransformationRules(defaultTransformationMap)
	}()
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	testGroupToString(input, output, s, c)
}

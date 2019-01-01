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

package transformation

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/cascades"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testFilterSuite{})

type testFilterSuite struct {
	*parser.Parser
	sctx sessionctx.Context
	is   infoschema.InfoSchema
}

func (s *testFilterSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.Parser = parser.New()
	s.sctx = core.MockContext()
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{core.MockTable()})
}

func (s *testFilterSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testFilterSuite) TestCoveredByGbyCols(c *C) {
	charsetInfo, collation := s.sctx.GetSessionVars().GetCharsetInfo()
	stmts, warns, err := s.Parser.Parse("select count(*), a, b from t group by a, b having a > 10 and b < 10;", charsetInfo, collation)
	c.Assert(err, IsNil)
	c.Assert(warns, IsNil)
	c.Assert(len(stmts), Equals, 1)

	err = core.Preprocess(s.sctx, stmts[0], s.is, false)
	c.Assert(err, IsNil)

	plan, err := core.BuildLogicalPlan(s.sctx, stmts[0], s.is)
	c.Assert(err, IsNil)
	c.Assert(plan, NotNil)

	logicalPlan, err := core.PreOptimize(plan.(core.LogicalPlan))
	c.Assert(err, IsNil)
	c.Assert(logicalPlan, NotNil)

	g := memo.Convert2Group(logicalPlan)
	c.Assert(g, NotNil)

	result := memo.DumpGroupLogicalPlans(g)
	c.Assert(len(result), Equals, 1)
	c.Assert(result[0], Equals, "Projection_5(Selection_4(Aggregation_2(TableScan_1)))")

	cp := cascades.NewDefaultCascadesPlanner(s.sctx)
	cp.TransformationRules = map[memo.Operand][]cascades.Transformation{
		memo.OperandSelection: {
			NewFilterAggregateTransposeRule(),
		},
	}

	err = cp.OnPhaseExploration(g)
	c.Assert(err, IsNil)

	result = memo.DumpGroupLogicalPlans(g)
	c.Assert(len(result), Equals, 1)
	c.Assert(result[0], Equals, "Projection_5(Aggregation_7(Selection_6(TableScan_1)))")
}

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

package implementation

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testImplSuite{})

type testImplSuite struct {
	*parser.Parser
	is   infoschema.InfoSchema
	sctx sessionctx.Context
}

func (s *testImplSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockTable()})
	s.sctx = plannercore.MockContext()
	s.Parser = parser.New()
}

func (s *testImplSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testImplSuite) TestBaseImplementation(c *C) {
	p := plannercore.PhysicalLimit{}.Init(s.sctx, nil, nil)
	impl := &baseImpl{plan: p}
	c.Assert(impl.GetPlan(), Equals, p)

	childCosts := []float64{5.0}
	cost := impl.CalcCost(10, childCosts, nil)
	c.Assert(cost, Equals, 5.0)
	c.Assert(impl.GetCost(), Equals, 5.0)

	impl.SetCost(6.0)
	c.Assert(impl.GetCost(), Equals, 6.0)
}

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

package core_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTaskSuite{})

type testTaskSuiteBase struct {
	*parser.Parser
	is infoschema.InfoSchema
}

func (s *testTaskSuiteBase) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	s.Parser = parser.New()
	s.Parser.EnableWindowFunc(true)
}

type testTaskSuite struct {
	testPlanSuiteBase
}

func (s *testTaskSuite) SetUpSuite(c *C) {
	s.testPlanSuiteBase.SetUpSuite(c)
}

func (s *testTaskSuite) TearDownSuite(c *C) {
}

func (s *testTaskSuite) TestGetWindowFrameSize(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	se.GetSessionVars().WindowConcurrency = 4

	tests := []struct {
		sql   string
		count float64
		size  float64
	}{
		{
			sql:   "select b, sum(a) over(partition by b rows between 3 preceding and 3 following) from t",
			count: 100.0,
			size:  7.0,
		},
		{
			sql:   "select b, sum(a) over(partition by b rows between 3 preceding and 2 preceding) from t",
			count: 100.0,
			size:  2.0,
		},
		{
			sql:   "select b, sum(a) over(partition by b rows between 3 following and 3 following) from t",
			count: 100.0,
			size:  1.0,
		},
		{
			sql:   "select b, sum(a) over(partition by b rows between 3 following and 2 following) from t",
			count: 100.0,
			size:  0.0,
		},
		{
			sql:   "select b, sum(a) over(partition by b rows between unbounded preceding and 1 following) from t",
			count: 100.0,
			size:  51.5,
		},
		{
			sql:   "select b, sum(a) over(partition by b rows between unbounded preceding and current row) from t",
			count: 100.0,
			size:  50.5,
		},
		{
			sql:   "select b, sum(a) over(partition by b rows between current row and unbounded following) from t",
			count: 100.0,
			size:  50.5,
		},
		{
			sql:   "select b, sum(a) over(partition by b rows between unbounded preceding and unbounded following) from t",
			count: 100.0,
			size:  100.0,
		},
	}

	for i, tt := range tests {
		comment := Commentf("seq: %d, sql: %s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		core.Preprocess(se, stmt, s.is)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)

		comment = Commentf("seq: %d, sql: %s, plan: %s", i, tt.sql, core.ToString(p))
		w, ok := p.(core.PhysicalPlan).Children()[0].(*core.PhysicalWindowParallel)
		c.Assert(ok, Equals, true, comment)
		c.Assert(w.BasePhysicalWindow.GetWindowFrameSize(tt.count), Equals, tt.size, comment)
	}
}

func (s *testTaskSuite) TestGetWindowFuncsCostUnit(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	se.GetSessionVars().WindowConcurrency = 4

	tests := []struct {
		sql     string
		planIdx int
		count   float64
		cpu     float64
		mem     float64
	}{
		{
			sql:     "select b, sum(a) over(partition by b rows between 3 preceding and 3 following) from t",
			planIdx: 1,
			count:   100.0,
			cpu:     100.0 * 7.0 * 1.0,
			mem:     1.0,
		},
		{
			sql:     "select b, avg(a) over(partition by b rows between 3 preceding and 3 following) from t",
			planIdx: 1,
			count:   100.0,
			cpu:     100.0 * 7.0 * 2.0,
			mem:     1.0,
		},
		{
			sql:     "select b, last_value(a) over(partition by b rows between 3 preceding and 3 following) from t",
			planIdx: 1,
			count:   100.0,
			cpu:     100.0 * 7.0 * 1.0,
			mem:     1.0,
		},
		{
			sql:     "select b, first_value(a) over(partition by b rows between 3 preceding and 3 following) from t",
			planIdx: 1,
			count:   100.0,
			cpu:     100.0 * 1.0,
			mem:     1.0,
		},
		{
			sql:     "select b, row_number() over w from t window w as (partition by b rows between 3 preceding and 3 following) ",
			planIdx: 0,
			count:   100.0,
			cpu:     100.0 * 1.0,
			mem:     1.0,
		},
	}

	for i, tt := range tests {
		comment := Commentf("seq: %d, sql: %s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		core.Preprocess(se, stmt, s.is)
		p, _, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)

		comment = Commentf("seq: %d, sql: %s, plan: %s", i, tt.sql, core.ToString(p))
		for k := 0; k < tt.planIdx; k++ {
			p = p.(core.PhysicalPlan).Children()[0]
		}
		w, ok := p.(*core.PhysicalWindowParallel)
		c.Assert(ok, Equals, true, comment)
		cpu, mem := w.BasePhysicalWindow.GetWindowFuncsCostUnit(tt.count)
		c.Assert(cpu, Equals, tt.cpu, comment)
		c.Assert(mem, Equals, tt.mem, comment)
	}
}

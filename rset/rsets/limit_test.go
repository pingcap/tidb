// Copyright 2015 PingCAP, Inc.
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

package rsets

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/plan/plans"
)

var _ = Suite(&testLimitSuite{})

type testLimitSuite struct {
	or *OffsetRset
	lr *LimitRset
}

func (s *testLimitSuite) SetUpSuite(c *C) {
	names := []string{"id", "name"}
	tblPlan := newTestTablePlan(testData, names)

	s.or = &OffsetRset{Src: tblPlan, Count: 1}
	s.lr = &LimitRset{Src: tblPlan, Count: 1}
}

func (s *testLimitSuite) TestLimitRsetPlan(c *C) {
	p, err := s.or.Plan(nil)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.OffsetDefaultPlan)
	c.Assert(ok, IsTrue)
}

func (s *testLimitSuite) TestLimitRsetString(c *C) {
	str := s.lr.String()
	c.Assert(len(str), Greater, 0)
}

func (s *testLimitSuite) TestOffsetRsetPlan(c *C) {
	p, err := s.lr.Plan(nil)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.LimitDefaultPlan)
	c.Assert(ok, IsTrue)
}

func (s *testLimitSuite) TestOffsetRsetString(c *C) {
	str := s.or.String()
	c.Assert(len(str), Greater, 0)
}

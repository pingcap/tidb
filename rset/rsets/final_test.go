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

var _ = Suite(&testSelectFinalRsetSuite{})

type testSelectFinalRsetSuite struct {
	r *SelectFinalRset
}

func (s *testSelectFinalRsetSuite) SetUpSuite(c *C) {
	names := []string{"id", "name"}
	tblPlan := newTestTablePlan(testData, names)
	selectList := &plans.SelectList{
		HiddenFieldOffset: len(tblPlan.GetFields()),
	}

	s.r = &SelectFinalRset{Src: tblPlan, SelectList: selectList}
}

func (s *testSelectFinalRsetSuite) TestSelectFinalRsetPlan(c *C) {
	p, err := s.r.Plan(nil)
	c.Assert(err, IsNil)

	_, ok := p.(*plans.SelectFinalPlan)
	c.Assert(ok, IsTrue)
}

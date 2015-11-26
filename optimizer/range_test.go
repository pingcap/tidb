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

package optimizer

import (
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/optimizer/plan"
)

var _ = Suite(&testRangeSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testRangeSuite struct{}

func (s *testRangeSuite) TestMerge(c *C) {
	log.SetLevel(log.LOG_LEVEL_ERROR)
	a := []rangePoint{
		{start: true},
		{value: plan.MaxVal},
	}

	b := []rangePoint{
		{value: 1, start: true},
		{value: 1},
	}
	rb := &rangeBuilder{}
	merged := rb.merge(a, b, false)
	c.Assert(len(merged), Equals, 2)
	c.Assert(merged[0].value, Equals, 1)
	c.Assert(merged[1].value, Equals, 1)
}

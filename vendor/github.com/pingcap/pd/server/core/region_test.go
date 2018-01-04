// Copyright 2016 PingCAP, Inc.
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

package core

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
)

func TestCore(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRegionMapSuite{})

type testRegionMapSuite struct{}

func (s *testRegionMapSuite) TestRegionMap(c *C) {
	var empty *regionMap
	c.Assert(empty.Len(), Equals, 0)
	c.Assert(empty.Get(1), IsNil)

	rm := newRegionMap()
	s.check(c, rm)
	rm.Put(s.regionInfo(1))
	s.check(c, rm, 1)

	rm.Put(s.regionInfo(2))
	rm.Put(s.regionInfo(3))
	s.check(c, rm, 1, 2, 3)

	rm.Put(s.regionInfo(3))
	rm.Delete(4)
	s.check(c, rm, 1, 2, 3)

	rm.Delete(3)
	rm.Delete(1)
	s.check(c, rm, 2)

	rm.Put(s.regionInfo(3))
	s.check(c, rm, 2, 3)
}

func (s *testRegionMapSuite) regionInfo(id uint64) *RegionInfo {
	return &RegionInfo{
		Region: &metapb.Region{
			Id: id,
		},
	}
}

func (s *testRegionMapSuite) check(c *C, rm *regionMap, ids ...uint64) {
	// Check position.
	for _, r := range rm.m {
		c.Assert(rm.ids[r.pos], Equals, r.Id)
	}
	// Check Get.
	for _, id := range ids {
		c.Assert(rm.Get(id).Id, Equals, id)
	}
	// Check Len.
	c.Assert(rm.Len(), Equals, len(ids))
	// Check id set.
	expect := make(map[uint64]struct{})
	for _, id := range ids {
		expect[id] = struct{}{}
	}
	set1 := make(map[uint64]struct{})
	for _, r := range rm.m {
		set1[r.Id] = struct{}{}
	}
	set2 := make(map[uint64]struct{})
	for _, id := range rm.ids {
		set2[id] = struct{}{}
	}
	c.Assert(set1, DeepEquals, expect)
	c.Assert(set2, DeepEquals, expect)
}

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

package tikv

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	goctx "golang.org/x/net/context"
)

type testCoprocessorSuite struct{}

var _ = Suite(&testCoprocessorSuite{})

func (s *testCoprocessorSuite) TestBuildTasks(c *C) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	cluster := mocktikv.NewCluster()
	_, regionIDs, _ := mocktikv.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := &codecPDClient{mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(pdCli)

	bo := NewBackoffer(3000, goctx.Background())

	tasks, err := buildCopTasks(bo, cache, buildKeyRanges("a", "c"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("g", "n"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("m", "n"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("a", "k"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("a", "x"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[2], regionIDs[2], "n", "t")
	s.taskEqual(c, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("a", "b", "b", "c"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("a", "b", "e", "f"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("g", "n", "o", "p"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("h", "k", "m", "p"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "h", "k", "m", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "n", "p")
}

func (s *testCoprocessorSuite) TestRebuild(c *C) {
	// nil --- 'm' --- nil
	// <-  0  -> <- 1 ->
	cluster := mocktikv.NewCluster()
	storeID, regionIDs, peerIDs := mocktikv.BootstrapWithMultiRegions(cluster, []byte("m"))
	pdCli := &codecPDClient{mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(pdCli)
	bo := NewBackoffer(3000, goctx.Background())

	tasks, err := buildCopTasks(bo, cache, buildKeyRanges("a", "z"), false, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "m")
	s.taskEqual(c, tasks[1], regionIDs[1], "m", "z")

	// nil -- 'm' -- 'q' -- nil
	// <-  0 -> <--1-> <-2-->
	regionIDs = append(regionIDs, cluster.AllocID())
	peerIDs = append(peerIDs, cluster.AllocID())
	cluster.Split(regionIDs[1], regionIDs[2], []byte("q"), []uint64{peerIDs[2]}, storeID)
	cache.DropRegion(tasks[1].region)

	tasks, err = buildCopTasks(bo, cache, buildKeyRanges("a", "z"), true, false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 3)
	s.taskEqual(c, tasks[2], regionIDs[0], "a", "m")
	s.taskEqual(c, tasks[1], regionIDs[1], "m", "q")
	s.taskEqual(c, tasks[0], regionIDs[2], "q", "z")
}

func buildKeyRanges(keys ...string) *copRanges {
	var ranges []kv.KeyRange
	for i := 0; i < len(keys); i += 2 {
		ranges = append(ranges, kv.KeyRange{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
		})
	}
	return &copRanges{mid: ranges}
}

func (s *testCoprocessorSuite) taskEqual(c *C, task *copTask, regionID uint64, keys ...string) {
	c.Assert(task.region.id, Equals, regionID)
	for i := 0; i < task.ranges.len(); i++ {
		r := task.ranges.at(i)
		c.Assert(string(r.StartKey), Equals, keys[2*i])
		c.Assert(string(r.EndKey), Equals, keys[2*i+1])
	}
}

func (s *testCoprocessorSuite) TestCopRanges(c *C) {
	ranges := []kv.KeyRange{
		{StartKey: []byte("a"), EndKey: []byte("b")},
		{StartKey: []byte("c"), EndKey: []byte("d")},
		{StartKey: []byte("e"), EndKey: []byte("f")},
	}

	s.checkEqual(c, &copRanges{mid: ranges}, ranges, true)
	s.checkEqual(c, &copRanges{first: &ranges[0], mid: ranges[1:]}, ranges, true)
	s.checkEqual(c, &copRanges{mid: ranges[:2], last: &ranges[2]}, ranges, true)
	s.checkEqual(c, &copRanges{first: &ranges[0], mid: ranges[1:2], last: &ranges[2]}, ranges, true)
}

func (s *testCoprocessorSuite) checkEqual(c *C, copRanges *copRanges, ranges []kv.KeyRange, slice bool) {
	c.Assert(copRanges.len(), Equals, len(ranges))
	for i := range ranges {
		c.Assert(copRanges.at(i), DeepEquals, ranges[i])
	}
	if slice {
		for i := 0; i <= copRanges.len(); i++ {
			for j := i; j <= copRanges.len(); j++ {
				s.checkEqual(c, copRanges.slice(i, j), ranges[i:j], false)
			}
		}
	}
}

func (s *testCoprocessorSuite) TestCalculateRemain(c *C) {
	first := &kv.KeyRange{StartKey: []byte("a"), EndKey: []byte("b")}
	mid := []kv.KeyRange{
		{StartKey: []byte("c"), EndKey: []byte("d")},
		{StartKey: []byte("e"), EndKey: []byte("g")},
		{StartKey: []byte("l"), EndKey: []byte("o")},
	}
	last := &kv.KeyRange{StartKey: []byte("q"), EndKey: []byte("t")}
	desc := false

	// range:  [c-d) [e-g) [l-o)
	//
	// split:  [c-m)
	// result: [c-d) [e-g) [l-o)
	// split:  [d-g)
	// result: [e-g) [l-o)
	// split:  [f-g)
	// result: [f-g) [l-o)
	ranges := &copRanges{mid: mid}
	s.testZZZ(c, ranges, desc,
		zzzCase{
			coprocessorKeyRange("c", "m"),
			buildKeyRanges("c", "d", "e", "g", "l", "o"),
		},
		zzzCase{
			coprocessorKeyRange("d", "g"),
			buildKeyRanges("e", "g", "l", "o"),
		},
		zzzCase{
			coprocessorKeyRange("f", "g"),
			buildKeyRanges("f", "g", "l", "o"),
		},
	)

	// range:  [a-b) [c-d) [e-g) [l-o)
	//
	// split:  [a-d)
	// result: [a-b) [c-d) [e-g) [l-o)
	// split:  [b-d)
	// result: [c-d) [e-g) [l-o)
	// split:  [m-o)
	// result: [m-o)
	ranges = &copRanges{first: first, mid: mid}
	s.testZZZ(c, ranges, desc,
		zzzCase{
			coprocessorKeyRange("a", "d"),
			buildKeyRanges("a", "b", "c", "d", "e", "g", "l", "o"),
		},
		zzzCase{
			coprocessorKeyRange("b", "d"),
			buildKeyRanges("c", "d", "e", "g", "l", "o"),
		},
		zzzCase{
			coprocessorKeyRange("m", "o"),
			buildKeyRanges("m", "o"),
		},
	)

	// range:  [a-b) [c-d) [e-g) [l-o) [q-t)
	//
	// split:  [f-o)
	// result: [f-g) [l-o) [q-t)
	// split:  [h-p)
	// result: [l-o) [q-t)
	// split:  [r-t)
	// result: [r-t)
	ranges = &copRanges{first: first, mid: mid, last: last}
	s.testZZZ(c, ranges, desc,
		zzzCase{
			coprocessorKeyRange("f", "o"),
			buildKeyRanges("f", "g", "l", "o", "q", "t"),
		},
		zzzCase{
			coprocessorKeyRange("h", "p"),
			buildKeyRanges("l", "o", "q", "t"),
		},
		zzzCase{
			coprocessorKeyRange("r", "t"),
			buildKeyRanges("r", "t"),
		},
	)

	desc = true
	// reverse, test the desc direction
	// range:  [c-d) [e-g) [l-o)
	//
	// split:  [c-m)
	// result: [c-d) [e-g) [l-m)
	// split:  [d-g)
	// result: [c-d) [e-g)
	// split:  [f-g)
	// result: [c-d) [e-g)
	ranges = &copRanges{mid: mid}
	s.testZZZ(c, ranges, desc,
		zzzCase{
			coprocessorKeyRange("c", "m"),
			buildKeyRanges("c", "d", "e", "g", "l", "m"),
		},
		zzzCase{
			coprocessorKeyRange("d", "g"),
			buildKeyRanges("c", "d", "e", "g"),
		},
		zzzCase{
			coprocessorKeyRange("f", "g"),
			buildKeyRanges("c", "d", "e", "g"),
		},
	)

	// range:  [a-b) [c-d) [e-g) [l-o)
	//
	// split:  [a-d)
	// result: [a-b) [c-d)
	// split:  [b-d)
	// result: [a-b) [c-d)
	// split:  [m-o)
	// result: [a-b) [c-d) [e-g) [l-o)
	ranges = &copRanges{first: first, mid: mid}
	s.testZZZ(c, ranges, desc,
		zzzCase{
			coprocessorKeyRange("a", "d"),
			buildKeyRanges("a", "b", "c", "d"),
		},
		zzzCase{
			coprocessorKeyRange("b", "d"),
			buildKeyRanges("a", "b", "c", "d"),
		},
		zzzCase{
			coprocessorKeyRange("m", "o"),
			buildKeyRanges("a", "b", "c", "d", "e", "g", "l", "o"),
		},
	)

	// range:  [a-b) [c-d) [e-g) [l-o) [q-t)
	//
	// split:  [f-o)
	// result: [a-b) [c-d) [e-g) [l-o)
	// split:  [h-p)
	// result: [a-b) [c-d) [e-g) [l-o)
	// split:  [r-t)
	// result: [a-b) [c-d) [e-g) [l-o) [q-t)
	ranges = &copRanges{first: first, mid: mid, last: last}
	s.testZZZ(c, ranges, desc,
		zzzCase{
			coprocessorKeyRange("f", "o"),
			buildKeyRanges("a", "b", "c", "d", "e", "g", "l", "o"),
		},
		zzzCase{
			coprocessorKeyRange("h", "p"),
			buildKeyRanges("a", "b", "c", "d", "e", "g", "l", "o"),
		},
		zzzCase{
			coprocessorKeyRange("r", "t"),
			buildKeyRanges("a", "b", "c", "d", "e", "g", "l", "o", "q", "t"),
		},
	)
}

func coprocessorKeyRange(start, end string) *coprocessor.KeyRange {
	return &coprocessor.KeyRange{
		Start: []byte(start),
		End:   []byte(end),
	}
}

type zzzCase struct {
	*coprocessor.KeyRange
	*copRanges
}

func (s *testCoprocessorSuite) testZZZ(c *C, ranges *copRanges, desc bool, cases ...zzzCase) {
	for _, t := range cases {
		result := calculateRemain(ranges, t.KeyRange, desc)
		expect := t.copRanges

		s.checkEqual(c, result, expect.mid, false)
	}
}

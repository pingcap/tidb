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
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/store/mockstore/mocktikv"
)

type testCoprocessorSuite struct {
	OneByOneSuite
}

var _ = Suite(&testCoprocessorSuite{})

func (s *testCoprocessorSuite) TestBuildTasks(c *C) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	cluster := mocktikv.NewCluster()
	_, regionIDs, _ := mocktikv.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := &codecPDClient{mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(pdCli)
	defer cache.Close()

	bo := NewBackoffer(context.Background(), 3000)

	req := &kv.Request{}
	flashReq := &kv.Request{}
	flashReq.StoreType = kv.TiFlash
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "c"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "c"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("m", "n"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("m", "n"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "k"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "k"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "x"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[2], regionIDs[2], "n", "t")
	s.taskEqual(c, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "x"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[2], regionIDs[2], "n", "t")
	s.taskEqual(c, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "h", "k", "m", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "n", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), flashReq)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "h", "k", "m", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "n", "p")
}

func (s *testCoprocessorSuite) TestSplitRegionRanges(c *C) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := &codecPDClient{mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(pdCli)
	defer cache.Close()

	bo := NewBackoffer(context.Background(), 3000)

	ranges, err := SplitRegionRanges(bo, cache, buildKeyRanges("a", "c"))
	c.Assert(err, IsNil)
	c.Assert(ranges, HasLen, 1)
	s.rangeEqual(c, ranges, "a", "c")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("h", "y"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 3)
	s.rangeEqual(c, ranges, "h", "n", "n", "t", "t", "y")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("s", "z"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 2)
	s.rangeEqual(c, ranges, "s", "t", "t", "z")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("s", "s"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "s", "s")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("t", "t"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "t", "t")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("t", "u"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "t", "u")

	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("u", "z"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "u", "z")

	// min --> max
	ranges, err = SplitRegionRanges(bo, cache, buildKeyRanges("a", "z"))
	c.Assert(err, IsNil)
	c.Assert(ranges, HasLen, 4)
	s.rangeEqual(c, ranges, "a", "g", "g", "n", "n", "t", "t", "z")
}

func (s *testCoprocessorSuite) TestRebuild(c *C) {
	// nil --- 'm' --- nil
	// <-  0  -> <- 1 ->
	cluster := mocktikv.NewCluster()
	storeID, regionIDs, peerIDs := mocktikv.BootstrapWithMultiRegions(cluster, []byte("m"))
	pdCli := &codecPDClient{mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(pdCli)
	defer cache.Close()
	bo := NewBackoffer(context.Background(), 3000)

	req := &kv.Request{}
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "z"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "m")
	s.taskEqual(c, tasks[1], regionIDs[1], "m", "z")

	// nil -- 'm' -- 'q' -- nil
	// <-  0 -> <--1-> <-2-->
	regionIDs = append(regionIDs, cluster.AllocID())
	peerIDs = append(peerIDs, cluster.AllocID())
	cluster.Split(regionIDs[1], regionIDs[2], []byte("q"), []uint64{peerIDs[2]}, storeID)
	cache.InvalidateCachedRegion(tasks[1].region)

	req.Desc = true
	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "z"), req)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 3)
	s.taskEqual(c, tasks[2], regionIDs[0], "a", "m")
	s.taskEqual(c, tasks[1], regionIDs[1], "m", "q")
	s.taskEqual(c, tasks[0], regionIDs[2], "q", "z")
}

func buildKeyRanges(keys ...string) []kv.KeyRange {
	var ranges []kv.KeyRange
	for i := 0; i < len(keys); i += 2 {
		ranges = append(ranges, kv.KeyRange{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
		})
	}
	return ranges
}

func buildCopRanges(keys ...string) *copRanges {
	ranges := buildKeyRanges(keys...)
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

func (s *testCoprocessorSuite) rangeEqual(c *C, ranges []kv.KeyRange, keys ...string) {
	for i := 0; i < len(ranges); i++ {
		r := ranges[i]
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

func (s *testCoprocessorSuite) TestCopRangeSplit(c *C) {
	first := &kv.KeyRange{StartKey: []byte("a"), EndKey: []byte("b")}
	mid := []kv.KeyRange{
		{StartKey: []byte("c"), EndKey: []byte("d")},
		{StartKey: []byte("e"), EndKey: []byte("g")},
		{StartKey: []byte("l"), EndKey: []byte("o")},
	}
	last := &kv.KeyRange{StartKey: []byte("q"), EndKey: []byte("t")}
	left := true
	right := false

	// input range:  [c-d) [e-g) [l-o)
	ranges := &copRanges{mid: mid}
	s.testSplit(c, ranges, right,
		splitCase{"c", buildCopRanges("c", "d", "e", "g", "l", "o")},
		splitCase{"d", buildCopRanges("e", "g", "l", "o")},
		splitCase{"f", buildCopRanges("f", "g", "l", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o)
	ranges = &copRanges{first: first, mid: mid}
	s.testSplit(c, ranges, right,
		splitCase{"a", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"c", buildCopRanges("c", "d", "e", "g", "l", "o")},
		splitCase{"m", buildCopRanges("m", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o) [q-t)
	ranges = &copRanges{first: first, mid: mid, last: last}
	s.testSplit(c, ranges, right,
		splitCase{"f", buildCopRanges("f", "g", "l", "o", "q", "t")},
		splitCase{"h", buildCopRanges("l", "o", "q", "t")},
		splitCase{"r", buildCopRanges("r", "t")},
	)

	// input range:  [c-d) [e-g) [l-o)
	ranges = &copRanges{mid: mid}
	s.testSplit(c, ranges, left,
		splitCase{"m", buildCopRanges("c", "d", "e", "g", "l", "m")},
		splitCase{"g", buildCopRanges("c", "d", "e", "g")},
		splitCase{"g", buildCopRanges("c", "d", "e", "g")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o)
	ranges = &copRanges{first: first, mid: mid}
	s.testSplit(c, ranges, left,
		splitCase{"d", buildCopRanges("a", "b", "c", "d")},
		splitCase{"d", buildCopRanges("a", "b", "c", "d")},
		splitCase{"o", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o")},
	)

	// input range:  [a-b) [c-d) [e-g) [l-o) [q-t)
	ranges = &copRanges{first: first, mid: mid, last: last}
	s.testSplit(c, ranges, left,
		splitCase{"o", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"p", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o")},
		splitCase{"t", buildCopRanges("a", "b", "c", "d", "e", "g", "l", "o", "q", "t")},
	)
}

func (s *testCoprocessorSuite) TestRateLimit(c *C) {
	done := make(chan struct{}, 1)
	rl := newRateLimit(1)
	c.Assert(rl.putToken, PanicMatches, "put a redundant token")
	exit := rl.getToken(done)
	c.Assert(exit, Equals, false)
	rl.putToken()
	c.Assert(rl.putToken, PanicMatches, "put a redundant token")

	exit = rl.getToken(done)
	c.Assert(exit, Equals, false)
	done <- struct{}{}
	exit = rl.getToken(done) // blocked but exit
	c.Assert(exit, Equals, true)

	sig := make(chan int, 1)
	go func() {
		exit = rl.getToken(done) // blocked
		c.Assert(exit, Equals, false)
		close(sig)
	}()
	time.Sleep(200 * time.Millisecond)
	rl.putToken()
	<-sig
}

type splitCase struct {
	key string
	*copRanges
}

func (s *testCoprocessorSuite) testSplit(c *C, ranges *copRanges, checkLeft bool, cases ...splitCase) {
	for _, t := range cases {
		left, right := ranges.split([]byte(t.key))
		expect := t.copRanges
		if checkLeft {
			s.checkEqual(c, left, expect.mid, false)
		} else {
			s.checkEqual(c, right, expect.mid, false)
		}
	}
}

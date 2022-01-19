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

package copr

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/driver/backoff"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mockstore/mocktikv"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testCoprocessorSuite struct {
}

var _ = Suite(&testCoprocessorSuite{})

func (s *testCoprocessorSuite) TestBuildTasks(c *C) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	cluster := mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	_, regionIDs, _ := mocktikv.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := &tikv.CodecPDClient{Client: mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	flashReq := &kv.Request{}
	flashReq.StoreType = kv.TiFlash
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "c"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "c"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("m", "n"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("m", "n"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "k"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "k"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "x"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[2], regionIDs[2], "n", "t")
	s.taskEqual(c, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "x"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[2], regionIDs[2], "n", "t")
	s.taskEqual(c, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), req, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "h", "k", "m", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "n", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), flashReq, nil)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "h", "k", "m", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "n", "p")
}

func (s *testCoprocessorSuite) TestSplitRegionRanges(c *C) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	cluster := mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	mocktikv.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := &tikv.CodecPDClient{Client: mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	ranges, err := cache.SplitRegionRanges(bo, buildKeyRanges("a", "c"))
	c.Assert(err, IsNil)
	c.Assert(ranges, HasLen, 1)
	s.rangeEqual(c, ranges, "a", "c")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("h", "y"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 3)
	s.rangeEqual(c, ranges, "h", "n", "n", "t", "t", "y")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("s", "z"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 2)
	s.rangeEqual(c, ranges, "s", "t", "t", "z")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("s", "s"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "s", "s")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("t", "t"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "t", "t")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("t", "u"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "t", "u")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("u", "z"))
	c.Assert(err, IsNil)
	c.Assert(len(ranges), Equals, 1)
	s.rangeEqual(c, ranges, "u", "z")

	// min --> max
	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("a", "z"))
	c.Assert(err, IsNil)
	c.Assert(ranges, HasLen, 4)
	s.rangeEqual(c, ranges, "a", "g", "g", "n", "n", "t", "t", "z")
}

func (s *testCoprocessorSuite) TestRebuild(c *C) {
	// nil --- 'm' --- nil
	// <-  0  -> <- 1 ->
	cluster := mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	storeID, regionIDs, peerIDs := mocktikv.BootstrapWithMultiRegions(cluster, []byte("m"))
	pdCli := &tikv.CodecPDClient{Client: mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()
	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "z"), req, nil)
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
	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "z"), req, nil)
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

func buildCopRanges(keys ...string) *KeyRanges {
	return NewKeyRanges(buildKeyRanges(keys...))
}

func (s *testCoprocessorSuite) taskEqual(c *C, task *copTask, regionID uint64, keys ...string) {
	c.Assert(task.region.GetID(), Equals, regionID)
	for i := 0; i < task.ranges.Len(); i++ {
		r := task.ranges.At(i)
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

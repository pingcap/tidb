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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
)

type testCoprocessorSuite struct{}

var _ = Suite(&testCoprocessorSuite{})

func (s *testCoprocessorSuite) TestBuildTasks(c *C) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	cluster := mocktikv.NewCluster()
	_, regionIDs, _ := mocktikv.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	cache := NewRegionCache(mocktikv.NewPDClient(cluster))

	tasks, err := buildCopTasks(cache, s.buildKeyRanges("a", "c"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("g", "n"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("m", "n"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "k"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "x"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "g")
	s.taskEqual(c, tasks[1], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[2], regionIDs[2], "n", "t")
	s.taskEqual(c, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "b", "b", "c"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "b", "e", "f"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("g", "n", "o", "p"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[1], "g", "n")
	s.taskEqual(c, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("h", "k", "m", "p"), false)
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
	cache := NewRegionCache(mocktikv.NewPDClient(cluster))

	tasks, err := buildCopTasks(cache, s.buildKeyRanges("a", "z"), false)
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], regionIDs[0], "a", "m")
	s.taskEqual(c, tasks[1], regionIDs[1], "m", "z")

	// nil -- 'm' -- 'q' -- nil
	// <-  0 -> <--1-> <-2-->
	regionIDs = append(regionIDs, cluster.AllocID())
	peerIDs = append(peerIDs, cluster.AllocID())
	cluster.Split(regionIDs[1], regionIDs[2], []byte("q"), []uint64{peerIDs[2]}, storeID)
	cache.DropRegion(tasks[1].region.VerID())

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "z"), true)
	c.Assert(err, IsNil)
	iter := &copIterator{
		store: &tikvStore{
			regionCache: cache,
		},
		req: &kv.Request{
			Desc: true,
		},
		tasks: tasks,
	}
	err = iter.rebuildCurrentTask(iter.tasks[0])
	c.Assert(err, IsNil)
	c.Assert(iter.tasks, HasLen, 3)
	s.taskEqual(c, iter.tasks[2], regionIDs[0], "a", "m")
	s.taskEqual(c, iter.tasks[1], regionIDs[1], "m", "q")
	s.taskEqual(c, iter.tasks[0], regionIDs[2], "q", "z")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "z"), true)
	iter = &copIterator{
		store: &tikvStore{
			regionCache: cache,
		},
		req: &kv.Request{
			Desc: false,
		},
		tasks: tasks,
	}
	err = iter.rebuildCurrentTask(iter.tasks[2])
	c.Assert(err, IsNil)
	c.Assert(iter.tasks, HasLen, 3)
	s.taskEqual(c, iter.tasks[2], regionIDs[0], "a", "m")
	s.taskEqual(c, iter.tasks[1], regionIDs[1], "m", "q")
	s.taskEqual(c, iter.tasks[0], regionIDs[2], "q", "z")
}

func (s *testCoprocessorSuite) buildKeyRanges(keys ...string) []kv.KeyRange {
	var ranges []kv.KeyRange
	for i := 0; i < len(keys); i += 2 {
		ranges = append(ranges, kv.KeyRange{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
		})
	}
	return ranges
}

func (s *testCoprocessorSuite) taskEqual(c *C, task *copTask, regionID uint64, keys ...string) {
	c.Assert(task.region.GetID(), Equals, regionID)
	for i, r := range task.ranges {
		c.Assert(string(r.StartKey), Equals, keys[2*i])
		c.Assert(string(r.EndKey), Equals, keys[2*i+1])
	}
}

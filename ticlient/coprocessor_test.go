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

package ticlient

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
)

type testCoprocessorSuite struct{}

var _ = Suite(&testCoprocessorSuite{})

func (s *testCoprocessorSuite) TestBuildTasks(c *C) {
	pd := newMockPDClient()
	cache := NewRegionCache(pd)

	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  1  -> <- 2 -> <- 3 -> <- 4 ->
	pd.setStore(100, "addr100")
	pd.setRegion(1, nil, []byte("g"), []uint64{100})
	pd.setRegion(2, []byte("g"), []byte("n"), []uint64{100})
	pd.setRegion(3, []byte("n"), []byte("t"), []uint64{100})
	pd.setRegion(4, []byte("t"), nil, []uint64{100})

	tasks, err := buildCopTasks(cache, s.buildKeyRanges("a", "c"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], 1, "a", "c")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("g", "n"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], 2, "g", "n")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("m", "n"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], 2, "m", "n")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "k"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], 1, "a", "g")
	s.taskEqual(c, tasks[1], 2, "g", "k")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "x"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 4)
	s.taskEqual(c, tasks[0], 1, "a", "g")
	s.taskEqual(c, tasks[1], 2, "g", "n")
	s.taskEqual(c, tasks[2], 3, "n", "t")
	s.taskEqual(c, tasks[3], 4, "t", "x")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "b", "b", "c"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], 1, "a", "b", "b", "c")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("a", "b", "e", "f"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 1)
	s.taskEqual(c, tasks[0], 1, "a", "b", "e", "f")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("g", "n", "o", "p"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], 2, "g", "n")
	s.taskEqual(c, tasks[1], 3, "o", "p")

	tasks, err = buildCopTasks(cache, s.buildKeyRanges("h", "k", "m", "p"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], 2, "h", "k", "m", "n")
	s.taskEqual(c, tasks[1], 3, "n", "p")
}

func (s *testCoprocessorSuite) TestRebuild(c *C) {
	pd := newMockPDClient()
	cache := NewRegionCache(pd)

	// nil --- 'm' --- nil
	// <-  1  -> <- 2 ->
	pd.setStore(100, "addr100")
	pd.setRegion(1, nil, []byte("m"), []uint64{100})
	pd.setRegion(2, []byte("m"), nil, []uint64{100})

	tasks, err := buildCopTasks(cache, s.buildKeyRanges("a", "z"))
	c.Assert(err, IsNil)
	c.Assert(tasks, HasLen, 2)
	s.taskEqual(c, tasks[0], 1, "a", "m")
	s.taskEqual(c, tasks[1], 2, "m", "z")

	// nil -- 'm' -- 'q' -- nil
	// <-  1 -> <--2-> <-3-->
	pd.setRegion(2, []byte("m"), []byte("q"), []uint64{100})
	pd.setRegion(3, []byte("q"), nil, []uint64{100})
	cache.DropRegion(2)

	iter := &copIterator{
		store: &tikvStore{
			regionCache: cache,
		},
		req: &kv.Request{
			Desc: true,
		},
		tasks: tasks,
		index: 1,
	}
	err = iter.rebuildCurrentTask()
	c.Assert(err, IsNil)
	c.Assert(iter.tasks, HasLen, 3)
	s.taskEqual(c, iter.tasks[0], 1, "a", "m")
	s.taskEqual(c, iter.tasks[1], 2, "m", "q")
	s.taskEqual(c, iter.tasks[2], 3, "q", "z")

	iter = &copIterator{
		store: &tikvStore{
			regionCache: cache,
		},
		req: &kv.Request{
			Desc: false,
		},
		tasks: tasks,
		index: 1,
	}
	err = iter.rebuildCurrentTask()
	c.Assert(err, IsNil)
	c.Assert(iter.tasks, HasLen, 2)
	s.taskEqual(c, iter.tasks[0], 2, "m", "q")
	s.taskEqual(c, iter.tasks[1], 3, "q", "z")
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

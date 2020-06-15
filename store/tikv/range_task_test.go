// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	"context"
	"errors"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

type testRangeTaskSuite struct {
	OneByOneSuite
	cluster cluster.Cluster
	store   *tikvStore

	testRanges     []kv.KeyRange
	expectedRanges [][]kv.KeyRange
}

var _ = Suite(&testRangeTaskSuite{})

func makeRange(startKey string, endKey string) kv.KeyRange {
	return kv.KeyRange{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

func (s *testRangeTaskSuite) SetUpTest(c *C) {
	// Split the store at "a" to "z"
	splitKeys := make([][]byte, 0)
	for k := byte('a'); k <= byte('z'); k++ {
		splitKeys = append(splitKeys, []byte{k})
	}

	// Calculate all region's ranges
	allRegionRanges := []kv.KeyRange{makeRange("", "a")}
	for i := 0; i < len(splitKeys)-1; i++ {
		allRegionRanges = append(allRegionRanges, kv.KeyRange{
			StartKey: splitKeys[i],
			EndKey:   splitKeys[i+1],
		})
	}
	allRegionRanges = append(allRegionRanges, makeRange("z", ""))

	client, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient("")
	c.Assert(err, IsNil)
	mocktikv.BootstrapWithMultiRegions(cluster, splitKeys...)
	s.cluster = cluster

	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)

	// TODO: make this possible
	// store, err := mockstore.NewMockStore(
	// 	mockstore.WithStoreType(mockstore.MockTiKV),
	// 	mockstore.WithClusterInspector(func(c cluster.Cluster) {
	// 		mockstore.BootstrapWithMultiRegions(c, splitKeys...)
	// 		s.cluster = c
	// 	}),
	// )
	// c.Assert(err, IsNil)
	s.store = store.(*tikvStore)

	s.testRanges = []kv.KeyRange{
		makeRange("", ""),
		makeRange("", "b"),
		makeRange("b", ""),
		makeRange("b", "x"),
		makeRange("a", "d"),
		makeRange("a\x00", "d\x00"),
		makeRange("a\xff\xff\xff", "c\xff\xff\xff"),
		makeRange("a1", "a2"),
		makeRange("a", "a"),
		makeRange("a3", "a3"),
	}

	s.expectedRanges = [][]kv.KeyRange{
		allRegionRanges,
		allRegionRanges[:2],
		allRegionRanges[2:],
		allRegionRanges[2:24],
		{
			makeRange("a", "b"),
			makeRange("b", "c"),
			makeRange("c", "d"),
		},
		{
			makeRange("a\x00", "b"),
			makeRange("b", "c"),
			makeRange("c", "d"),
			makeRange("d", "d\x00"),
		},
		{
			makeRange("a\xff\xff\xff", "b"),
			makeRange("b", "c"),
			makeRange("c", "c\xff\xff\xff"),
		},
		{
			makeRange("a1", "a2"),
		},
		{},
		{},
	}
}

func (s *testRangeTaskSuite) TearDownTest(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func collect(c chan *kv.KeyRange) []kv.KeyRange {
	c <- nil
	ranges := make([]kv.KeyRange, 0)

	for {
		r := <-c
		if r == nil {
			break
		}

		ranges = append(ranges, *r)
	}
	return ranges
}

func (s *testRangeTaskSuite) checkRanges(c *C, obtained []kv.KeyRange, expected []kv.KeyRange) {
	sort.Slice(obtained, func(i, j int) bool {
		return bytes.Compare(obtained[i].StartKey, obtained[j].StartKey) < 0
	})

	c.Assert(obtained, DeepEquals, expected)
}

func batchRanges(ranges []kv.KeyRange, batchSize int) []kv.KeyRange {
	result := make([]kv.KeyRange, 0, len(ranges))

	for i := 0; i < len(ranges); i += batchSize {
		lastRange := i + batchSize - 1
		if lastRange >= len(ranges) {
			lastRange = len(ranges) - 1
		}

		result = append(result, kv.KeyRange{
			StartKey: ranges[i].StartKey,
			EndKey:   ranges[lastRange].EndKey,
		})
	}

	return result
}

func (s *testRangeTaskSuite) testRangeTaskImpl(c *C, concurrency int) {
	c.Logf("Test RangeTask, concurrency: %v", concurrency)

	ranges := make(chan *kv.KeyRange, 100)

	handler := func(ctx context.Context, r kv.KeyRange) (RangeTaskStat, error) {
		ranges <- &r
		stat := RangeTaskStat{
			CompletedRegions: 1,
		}
		return stat, nil
	}

	runner := NewRangeTaskRunner("test-runner", s.store, concurrency, handler)

	for regionsPerTask := 1; regionsPerTask <= 5; regionsPerTask++ {
		for i, r := range s.testRanges {
			runner.SetRegionsPerTask(regionsPerTask)

			expectedRanges := batchRanges(s.expectedRanges[i], regionsPerTask)

			err := runner.RunOnRange(context.Background(), r.StartKey, r.EndKey)
			c.Assert(err, IsNil)
			s.checkRanges(c, collect(ranges), expectedRanges)
			c.Assert(runner.CompletedRegions(), Equals, len(expectedRanges))
			c.Assert(runner.FailedRegions(), Equals, 0)
		}
	}
}

func (s *testRangeTaskSuite) TestRangeTask(c *C) {
	for concurrency := 1; concurrency < 5; concurrency++ {
		s.testRangeTaskImpl(c, concurrency)
	}
}

func (s *testRangeTaskSuite) testRangeTaskErrorImpl(c *C, concurrency int) {
	for i, r := range s.testRanges {
		// Iterate all sub tasks and make it an error
		subRanges := s.expectedRanges[i]
		for _, subRange := range subRanges {
			errKey := subRange.StartKey
			c.Logf("Test RangeTask Error concurrency: %v, range: [%+q, %+q), errKey: %+q", concurrency, r.StartKey, r.EndKey, errKey)

			handler := func(ctx context.Context, r kv.KeyRange) (RangeTaskStat, error) {
				stat := RangeTaskStat{0, 0}
				if bytes.Equal(r.StartKey, errKey) {
					stat.FailedRegions++
					return stat, errors.New("test error")

				}
				stat.CompletedRegions++
				return stat, nil
			}

			runner := NewRangeTaskRunner("test-error-runner", s.store, concurrency, handler)
			runner.SetRegionsPerTask(1)
			err := runner.RunOnRange(context.Background(), r.StartKey, r.EndKey)
			// RunOnRange returns no error only when all sub tasks are done successfully.
			c.Assert(err, NotNil)
			c.Assert(runner.CompletedRegions(), Less, len(subRanges))
			c.Assert(runner.FailedRegions(), Equals, 1)
		}
	}
}

func (s *testRangeTaskSuite) TestRangeTaskError(c *C) {
	for concurrency := 1; concurrency < 5; concurrency++ {
		s.testRangeTaskErrorImpl(c, concurrency)
	}
}

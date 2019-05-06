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
	"sort"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testRangeRequestSenderSuite struct {
	OneByOneSuite
	cluster *mocktikv.Cluster
	store   *tikvStore

	testRanges     []kv.KeyRange
	expectedRanges [][]kv.KeyRange
}

var _ = Suite(&testRangeRequestSenderSuite{})

func makeRange(startKey string, endKey string) kv.KeyRange {
	return kv.KeyRange{
		StartKey: []byte(startKey),
		EndKey:   []byte(endKey),
	}
}

func (s *testRangeRequestSenderSuite) SetUpTest(c *C) {
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

	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiRegions(s.cluster, splitKeys...)
	client, pdClient, err := mocktikv.NewTiKVAndPDClient(s.cluster, nil, "")
	c.Assert(err, IsNil)

	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
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

func (s *testRangeRequestSenderSuite) TearDownTest(c *C) {
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

func (s *testRangeRequestSenderSuite) checkRanges(c *C, obtained []kv.KeyRange, expected []kv.KeyRange) {
	sort.Slice(obtained, func(i, j int) bool {
		return bytes.Compare(obtained[i].StartKey, obtained[j].StartKey) < 0
	})

	c.Assert(obtained, DeepEquals, expected)
}

func (s *testRangeRequestSenderSuite) testRangeTaskImpl(c *C, concurrency int) {
	ranges := make(chan *kv.KeyRange, 10)

	makeRequest := func(startKey []byte, endKey []byte) *tikvrpc.Request {
		ranges <- &kv.KeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		}
		return nil
	}

	sender := NewRangeRequestSender(
		"test-sender",
		s.store,
		concurrency,
		makeRequest,
		func(response *tikvrpc.Response, err error) error {
			return nil
		})

	for i, r := range s.testRanges {
		expectedRanges := s.expectedRanges[i]

		err := sender.RunOnRange(context.Background(), r.StartKey, r.EndKey)
		c.Assert(err, IsNil)
		s.checkRanges(c, collect(ranges), expectedRanges)
	}
}

func (s *testRangeRequestSenderSuite) TestRangeTask(c *C) {
	for concurrency := 1; concurrency < 5; concurrency++ {
		s.testRangeTaskImpl(c, concurrency)
	}
}

func (s *testRangeRequestSenderSuite) TestRangeTaskWorker(c *C) {
	ranges := make(chan *kv.KeyRange, 10)

	makeRequest := func(startKey []byte, endKey []byte) *tikvrpc.Request {
		ranges <- &kv.KeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		}
		return nil
	}

	sender := NewRangeRequestSender(
		"test-range-request-worker-sender",
		s.store,
		1,
		makeRequest,
		func(response *tikvrpc.Response, err error) error {
			return nil
		})

	taskCh := make(chan *kv.KeyRange, 10)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		close(taskCh)
		wg.Wait()
	}()

	go sender.createWorker(taskCh, &wg).run(ctx, cancel)
	wg.Add(1)

	for i, r := range s.testRanges {
		expectedRanges := s.expectedRanges[i]
		taskCh <- &r
		s.checkRanges(c, collect(ranges), expectedRanges)
	}
}

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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/trxevents"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func buildTestCopTasks(bo *Backoffer, cache *RegionCache, ranges *KeyRanges, req *kv.Request, eventCb trxevents.EventCallback) ([]*copTask, error) {
	return buildCopTasks(bo, ranges, &buildCopTaskOpt{
		req:      req,
		cache:    cache,
		eventCb:  eventCb,
		respChan: true,
	})
}

func TestBuildTasksWithoutBuckets(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()

	_, regionIDs, _ := testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	flashReq := &kv.Request{}
	flashReq.StoreType = kv.TiFlash
	tasks, err := buildTestCopTasks(bo, cache, buildCopRanges("a", "c"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "c")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "c"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "c")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("g", "n"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], 0, "g", "n")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("g", "n"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], 0, "g", "n")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("m", "n"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], 0, "m", "n")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("m", "n"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], 0, "m", "n")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "k"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], 0, "g", "k")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "k"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], 0, "g", "k")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "x"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 4)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], 0, "g", "n")
	taskEqual(t, tasks[2], regionIDs[2], 0, "n", "t")
	taskEqual(t, tasks[3], regionIDs[3], 0, "t", "x")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "x"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 4)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], 0, "g", "n")
	taskEqual(t, tasks[2], regionIDs[2], 0, "n", "t")
	taskEqual(t, tasks[3], regionIDs[3], 0, "t", "x")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "b", "b", "c")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "b", "b", "c")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "b", "e", "f")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "b", "e", "f")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], 0, "g", "n")
	taskEqual(t, tasks[1], regionIDs[2], 0, "o", "p")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], 0, "g", "n")
	taskEqual(t, tasks[1], regionIDs[2], 0, "o", "p")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], 0, "h", "k", "m", "n")
	taskEqual(t, tasks[1], regionIDs[2], 0, "n", "p")

	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], 0, "h", "k", "m", "n")
	taskEqual(t, tasks[1], regionIDs[2], 0, "n", "p")
}

func TestBuildTasksByBuckets(t *testing.T) {
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()

	// region:  nil------------------n-----------x-----------nil
	// buckets: nil----c----g----k---n----t------x-----------nil
	_, regionIDs, _ := testutils.BootstrapWithMultiRegions(cluster, []byte("n"), []byte("x"))
	cluster.SplitRegionBuckets(regionIDs[0], [][]byte{{}, {'c'}, {'g'}, {'k'}, {'n'}}, regionIDs[0])
	cluster.SplitRegionBuckets(regionIDs[1], [][]byte{{'n'}, {'t'}, {'x'}}, regionIDs[1])
	cluster.SplitRegionBuckets(regionIDs[2], [][]byte{{'x'}, {}}, regionIDs[2])
	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	// one range per bucket
	// region:  nil------------------n-----------x-----------nil
	// buckets: nil----c----g----k---n----t------x-----------nil
	// range&task: a-b c-d   h-i k---n o-p    u--x-----------nil
	req := &kv.Request{}
	regionRanges := []struct {
		regionID uint64
		ranges   []string
	}{
		{regionIDs[0], []string{"a", "b", "c", "d", "h", "i", "k", "n"}},
		{regionIDs[1], []string{"o", "p", "u", "x"}},
		{regionIDs[2], []string{"x", ""}},
	}
	for _, regionRange := range regionRanges {
		regionID, ranges := regionRange.regionID, regionRange.ranges
		tasks, err := buildTestCopTasks(bo, cache, buildCopRanges(ranges...), req, nil)
		require.NoError(t, err)
		require.Len(t, tasks, len(ranges)/2)
		for i, task := range tasks {
			taskEqual(t, task, regionID, regionID, ranges[2*i], ranges[2*i+1])
		}
	}

	// one request multiple regions
	allRanges := []string{}
	for _, regionRange := range regionRanges {
		allRanges = append(allRanges, regionRange.ranges...)
	}
	tasks, err := buildTestCopTasks(bo, cache, buildCopRanges(allRanges...), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, len(allRanges)/2)
	taskIdx := 0
	for _, regionRange := range regionRanges {
		regionID, ranges := regionRange.regionID, regionRange.ranges
		for i := 0; i < len(ranges); i += 2 {
			taskEqual(t, tasks[taskIdx], regionID, regionID, ranges[i], ranges[i+1])
			taskIdx++
		}
	}

	// several ranges per bucket
	// region:  nil---------------------------n-----------x-----------nil
	// buckets: nil-----c-------g-------k-----n----t------x-----------nil
	// ranges:  nil-a b-c d-e f-g h-i j-k-l m-n
	// tasks:   nil-a b-c
	//                    d-e f-g
	//                            h-i j-k
	//                                  k-l m-n
	keyRanges := []string{
		"", "a", "b", "c",
		"d", "e", "f", "g",
		"h", "i", "j", "k",
		"k", "l", "m", "n",
	}
	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges(keyRanges...), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, len(keyRanges)/4)
	for i, task := range tasks {
		taskEqual(t, task, regionIDs[0], regionIDs[0], keyRanges[4*i], keyRanges[4*i+1], keyRanges[4*i+2], keyRanges[4*i+3])
	}

	// cross bucket ranges
	// buckets: nil-----c-------g---------k---n----t------x-----------nil
	// ranges:  nil-------d   e---h i---j
	// tasks:   nil-----c
	//                  c-d   e-g
	//                          g-h i---j
	keyRanges = []string{
		"", "d", "e", "h", "i", "j",
	}
	expectedTaskRanges := [][]string{
		{"", "c"},
		{"c", "d", "e", "g"},
		{"g", "h", "i", "j"},
	}
	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges(keyRanges...), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, len(expectedTaskRanges))
	for i, task := range tasks {
		taskEqual(t, task, regionIDs[0], regionIDs[0], expectedTaskRanges[i]...)
	}

	// cross several buckets ranges
	// region:    n  -----------------------------  x
	// buckets:   n   --   q -- r --  t -- u -- v -- x
	// ranges:    n--o  p--q       s  ------------ w
	// tasks:     n--o  p--q
	//                             s--t
	//								  t -- u
	//									   u -- v
	//											v--w
	expectedTaskRanges = [][]string{
		{"n", "o", "p", "q"},
		{"s", "t"},
		{"t", "u"},
		{"u", "v"},
		{"v", "w"},
	}
	cluster.SplitRegionBuckets(regionIDs[1], [][]byte{{'n'}, {'q'}, {'r'}, {'t'}, {'u'}, {'v'}, {'x'}}, regionIDs[1])
	cache = NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()
	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("n", "o", "p", "q", "s", "w"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, len(expectedTaskRanges))
	for i, task := range tasks {
		taskEqual(t, task, regionIDs[1], regionIDs[1], expectedTaskRanges[i]...)
	}

	// out of range buckets
	// region:  n------------------x
	// buckets:       q---s---u
	// ranges:  n-o p ----s t---v w-x
	// tasks:   n-o p-q
	//                 q--s
	//                      t-u
	//                        u-v w-x
	expectedTaskRanges = [][]string{
		{"n", "o", "p", "q"},
		{"q", "s"},
		{"t", "u"},
		{"u", "v", "w", "x"},
	}
	cluster.SplitRegionBuckets(regionIDs[1], [][]byte{{'q'}, {'s'}, {'u'}}, regionIDs[1])
	cache = NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()
	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("n", "o", "p", "s", "t", "v", "w", "x"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, len(expectedTaskRanges))
	for i, task := range tasks {
		taskEqual(t, task, regionIDs[1], regionIDs[1], expectedTaskRanges[i]...)
	}

	// out of range buckets
	// region:    n------------x
	// buckets: g-------t---------z
	// ranges:     o-p   u-w
	// tasks:      o-p
	//                   u-w
	expectedTaskRanges = [][]string{
		{"o", "p"},
		{"u", "w"},
	}
	cluster.SplitRegionBuckets(regionIDs[1], [][]byte{{'g'}, {'t'}, {'z'}}, regionIDs[1])
	cache = NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()
	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("o", "p", "u", "w"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, len(expectedTaskRanges))
	for i, task := range tasks {
		taskEqual(t, task, regionIDs[1], regionIDs[1], expectedTaskRanges[i]...)
	}

	// cover the whole region
	// region:    n--------------x
	// buckets:   n -- q -- r -- x
	// ranges:    n--------------x
	// tasks:     o -- q
	//                 q -- r
	//						r -- x
	expectedTaskRanges = [][]string{
		{"n", "q"},
		{"q", "r"},
		{"r", "x"},
	}
	cluster.SplitRegionBuckets(regionIDs[1], [][]byte{{'n'}, {'q'}, {'r'}, {'x'}}, regionIDs[1])
	cache = NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()
	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("n", "x"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, len(expectedTaskRanges))
	for i, task := range tasks {
		taskEqual(t, task, regionIDs[1], regionIDs[1], expectedTaskRanges[i]...)
	}
}

func TestSplitKeyRangesByLocationsWithoutBuckets(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()

	testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	locRanges, err := cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("a", "c")), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, locRanges, 1)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "a", "c")

	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("a", "c")), 0)
	require.NoError(t, err)
	require.Len(t, locRanges, 0)

	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("h", "y")), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, locRanges, 3)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "h", "n")
	rangeEqual(t, locRanges[1].Ranges.ToRanges(), "n", "t")
	rangeEqual(t, locRanges[2].Ranges.ToRanges(), "t", "y")

	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("h", "n")), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, locRanges, 1)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "h", "n")

	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("s", "s")), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, locRanges, 1)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "s", "s")

	// min --> max
	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("a", "z")), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, locRanges, 4)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "a", "g")
	rangeEqual(t, locRanges[1].Ranges.ToRanges(), "g", "n")
	rangeEqual(t, locRanges[2].Ranges.ToRanges(), "n", "t")
	rangeEqual(t, locRanges[3].Ranges.ToRanges(), "t", "z")

	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("a", "z")), 3)
	require.NoError(t, err)
	require.Len(t, locRanges, 3)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "a", "g")
	rangeEqual(t, locRanges[1].Ranges.ToRanges(), "g", "n")
	rangeEqual(t, locRanges[2].Ranges.ToRanges(), "n", "t")

	// many range
	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z")), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, locRanges, 4)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "a", "b", "c", "d", "e", "f", "f", "g")
	rangeEqual(t, locRanges[1].Ranges.ToRanges(), "g", "h", "i", "j", "k", "l", "m", "n")
	rangeEqual(t, locRanges[2].Ranges.ToRanges(), "o", "p", "q", "r", "s", "t")
	rangeEqual(t, locRanges[3].Ranges.ToRanges(), "u", "v", "w", "x", "y", "z")

	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("a", "b", "b", "h", "h", "m", "n", "t", "v", "w")), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, locRanges, 4)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "a", "b", "b", "g")
	rangeEqual(t, locRanges[1].Ranges.ToRanges(), "g", "h", "h", "m", "n")
	rangeEqual(t, locRanges[2].Ranges.ToRanges(), "n", "t")
	rangeEqual(t, locRanges[3].Ranges.ToRanges(), "v", "w")

	locRanges, err = cache.SplitKeyRangesByLocationsWithoutBuckets(bo, NewKeyRanges(BuildKeyRanges("a", "b", "v", "w")), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, locRanges, 2)
	rangeEqual(t, locRanges[0].Ranges.ToRanges(), "a", "b")
	rangeEqual(t, locRanges[1].Ranges.ToRanges(), "v", "w")
}

func TestSplitKeyRanges(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()

	testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	ranges, err := cache.SplitRegionRanges(bo, BuildKeyRanges("a", "c"), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "a", "c")

	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("a", "c"), 0)
	require.NoError(t, err)
	require.Len(t, ranges, 0)

	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("h", "y"), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, ranges, 3)
	rangeEqual(t, ranges, "h", "n", "n", "t", "t", "y")

	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("s", "z"), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, ranges, 2)
	rangeEqual(t, ranges, "s", "t", "t", "z")

	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("s", "s"), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "s", "s")

	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("t", "t"), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "t", "t")

	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("t", "u"), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "t", "u")

	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("u", "z"), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "u", "z")

	// min --> max
	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("a", "z"), UnspecifiedLimit)
	require.NoError(t, err)
	require.Len(t, ranges, 4)
	rangeEqual(t, ranges, "a", "g", "g", "n", "n", "t", "t", "z")

	ranges, err = cache.SplitRegionRanges(bo, BuildKeyRanges("a", "z"), 3)
	require.NoError(t, err)
	require.Len(t, ranges, 3)
	rangeEqual(t, ranges, "a", "g", "g", "n", "n", "t")
}

func TestRebuild(t *testing.T) {
	// nil --- 'm' --- nil
	// <-  0  -> <- 1 ->
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()

	storeID, regionIDs, peerIDs := testutils.BootstrapWithMultiRegions(cluster, []byte("m"))
	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()
	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()
	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	tasks, err := buildTestCopTasks(bo, cache, buildCopRanges("a", "z"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "m")
	taskEqual(t, tasks[1], regionIDs[1], 0, "m", "z")

	// nil -- 'm' -- 'q' -- nil
	// <-  0 -> <--1-> <-2-->
	regionIDs = append(regionIDs, cluster.AllocID())
	peerIDs = append(peerIDs, cluster.AllocID())
	cluster.Split(regionIDs[1], regionIDs[2], []byte("q"), []uint64{peerIDs[2]}, storeID)
	cache.InvalidateCachedRegion(tasks[1].region)

	req.Desc = true
	tasks, err = buildTestCopTasks(bo, cache, buildCopRanges("a", "z"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	taskEqual(t, tasks[2], regionIDs[0], 0, "a", "m")
	taskEqual(t, tasks[1], regionIDs[1], 0, "m", "q")
	taskEqual(t, tasks[0], regionIDs[2], 0, "q", "z")
}

func buildCopRanges(keys ...string) *KeyRanges {
	return NewKeyRanges(BuildKeyRanges(keys...))
}

func taskEqual(t *testing.T, task *copTask, regionID, bucketsVer uint64, keys ...string) {
	require.Equal(t, task.region.GetID(), regionID)
	require.Equal(t, task.bucketsVer, bucketsVer)
	for i := 0; i < task.ranges.Len(); i++ {
		r := task.ranges.At(i)
		require.Equal(t, string(r.StartKey), keys[2*i])
		require.Equal(t, string(r.EndKey), keys[2*i+1])
	}
}

func rangeEqual(t *testing.T, ranges []kv.KeyRange, keys ...string) {
	for i := 0; i < len(ranges); i++ {
		r := ranges[i]
		require.Equal(t, string(r.StartKey), keys[2*i])
		require.Equal(t, string(r.EndKey), keys[2*i+1])
	}
}

func TestBuildPagingTasks(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()

	_, regionIDs, _ := testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	req.Paging.Enable = true
	req.Paging.MinPagingSize = paging.MinPagingSize
	tasks, err := buildTestCopTasks(bo, cache, buildCopRanges("a", "c"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "c")
	require.True(t, tasks[0].paging)
	require.Equal(t, tasks[0].pagingSize, paging.MinPagingSize)
}

func TestBuildPagingTasksDisablePagingForSmallLimit(t *testing.T) {
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()
	_, regionIDs, _ := testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))

	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	req.Paging.Enable = true
	req.Paging.MinPagingSize = paging.MinPagingSize
	req.LimitSize = 1
	tasks, err := buildTestCopTasks(bo, cache, buildCopRanges("a", "c"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "c")
	require.False(t, tasks[0].paging)
	require.Equal(t, tasks[0].pagingSize, uint64(0))
}

func toCopRange(r kv.KeyRange) *coprocessor.KeyRange {
	coprRange := coprocessor.KeyRange{}
	coprRange.Start = r.StartKey
	coprRange.End = r.EndKey
	return &coprRange
}

func toRange(r *KeyRanges) []kv.KeyRange {
	ranges := make([]kv.KeyRange, 0, r.Len())
	if r.first != nil {
		ranges = append(ranges, *r.first)
	}
	ranges = append(ranges, r.mid...)
	if r.last != nil {
		ranges = append(ranges, *r.last)
	}
	return ranges
}

func TestCalculateRetry(t *testing.T) {
	worker := copIteratorWorker{}

	// split in one range
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("b", "c")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(retry), "b", "c", "e", "g")
	}
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("e", "f")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(retry), "a", "c", "e", "f")
	}

	// across ranges
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("b", "f")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(retry), "b", "c", "e", "g")
	}
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("b", "f")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(retry), "a", "c", "e", "f")
	}

	// exhaust the ranges
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("a", "g")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(retry), "a", "c", "e", "g")
	}
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("a", "g")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(retry), "a", "c", "e", "g")
	}

	// nil range
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		retry := worker.calculateRetry(NewKeyRanges(ranges), nil, false)
		rangeEqual(t, toRange(retry), "a", "c", "e", "g")
	}
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		retry := worker.calculateRetry(NewKeyRanges(ranges), nil, true)
		rangeEqual(t, toRange(retry), "a", "c", "e", "g")
	}
}

func TestCalculateRemain(t *testing.T) {
	worker := copIteratorWorker{}

	// split in one range
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("a", "b")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(remain), "b", "c", "e", "g")
	}
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("f", "g")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(remain), "a", "c", "e", "f")
	}

	// across ranges
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("a", "f")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(remain), "f", "g")
	}
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("b", "g")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(remain), "a", "b")
	}

	// exhaust the ranges
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("a", "g")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), false)
		require.Equal(t, remain.Len(), 0)
	}
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		split := BuildKeyRanges("a", "g")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), true)
		require.Equal(t, remain.Len(), 0)
	}

	// nil range
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		remain := worker.calculateRemain(NewKeyRanges(ranges), nil, false)
		rangeEqual(t, toRange(remain), "a", "c", "e", "g")
	}
	{
		ranges := BuildKeyRanges("a", "c", "e", "g")
		remain := worker.calculateRemain(NewKeyRanges(ranges), nil, true)
		rangeEqual(t, toRange(remain), "a", "c", "e", "g")
	}
}

func TestBasicSmallTaskConc(t *testing.T) {
	require.False(t, isSmallTask(&copTask{RowCountHint: -1}))
	require.False(t, isSmallTask(&copTask{RowCountHint: 0}))
	require.True(t, isSmallTask(&copTask{RowCountHint: 1}))
	require.True(t, isSmallTask(&copTask{RowCountHint: 6}))
	require.True(t, isSmallTask(&copTask{RowCountHint: CopSmallTaskRow}))
	require.False(t, isSmallTask(&copTask{RowCountHint: CopSmallTaskRow + 1}))
	_, conc := smallTaskConcurrency([]*copTask{}, 16)
	require.GreaterOrEqual(t, conc, 0)
}

func TestBuildCopTasksWithRowCountHint(t *testing.T) {
	// nil --- 'g' --- 'n' --- 't' --- nil
	// <-  0  -> <- 1 -> <- 2 -> <- 3 ->
	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()
	_, _, _ = testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()
	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)
	req := &kv.Request{}
	ranges := buildCopRanges("a", "c", "d", "e", "h", "x", "y", "z")
	tasks, err := buildCopTasks(bo, ranges, &buildCopTaskOpt{
		req:      req,
		cache:    cache,
		rowHints: []int{1, 1, 3, CopSmallTaskRow},
	})
	require.Nil(t, err)
	require.Equal(t, len(tasks), 4)
	// task[0] ["a"-"c", "d"-"e"]
	require.Equal(t, tasks[0].RowCountHint, 2)
	// task[1] ["h"-"n"]
	require.Equal(t, tasks[1].RowCountHint, 3)
	// task[2] ["n"-"t"]
	require.Equal(t, tasks[2].RowCountHint, 3)
	// task[3] ["t"-"x", "y"-"z"]
	require.Equal(t, tasks[3].RowCountHint, 3+CopSmallTaskRow)
	_, conc := smallTaskConcurrency(tasks, 16)
	require.Equal(t, conc, 1)

	ranges = buildCopRanges("a", "c", "d", "e", "h", "x", "y", "z")
	tasks, err = buildCopTasks(bo, ranges, &buildCopTaskOpt{
		req:      req,
		cache:    cache,
		rowHints: []int{1, 1, 3, 3},
	})
	require.Nil(t, err)
	require.Equal(t, len(tasks), 4)
	// task[0] ["a"-"c", "d"-"e"]
	require.Equal(t, tasks[0].RowCountHint, 2)
	// task[1] ["h"-"n"]
	require.Equal(t, tasks[1].RowCountHint, 3)
	// task[2] ["n"-"t"]
	require.Equal(t, tasks[2].RowCountHint, 3)
	// task[3] ["t"-"x", "y"-"z"]
	require.Equal(t, tasks[3].RowCountHint, 6)
	_, conc = smallTaskConcurrency(tasks, 16)
	require.Equal(t, conc, 2)

	// cross-region long range
	ranges = buildCopRanges("a", "z")
	tasks, err = buildCopTasks(bo, ranges, &buildCopTaskOpt{
		req:      req,
		cache:    cache,
		rowHints: []int{10},
	})
	require.Nil(t, err)
	require.Equal(t, len(tasks), 4)
	// task[0] ["a"-"g"]
	require.Equal(t, tasks[0].RowCountHint, 10)
	// task[1] ["g"-"n"]
	require.Equal(t, tasks[1].RowCountHint, 10)
	// task[2] ["n"-"t"]
	require.Equal(t, tasks[2].RowCountHint, 10)
	// task[3] ["t"-"z"]
	require.Equal(t, tasks[3].RowCountHint, 10)
}

func TestSmallTaskConcurrencyLimit(t *testing.T) {
	smallTaskCount := 1000
	tasks := make([]*copTask, 0, smallTaskCount)
	for i := 0; i < smallTaskCount; i++ {
		tasks = append(tasks, &copTask{
			RowCountHint: 1,
		})
	}
	count, conc := smallTaskConcurrency(tasks, 1)
	require.Equal(t, smallConcPerCore, conc)
	require.Equal(t, smallTaskCount, count)
	// also handle 0 value.
	count, conc = smallTaskConcurrency(tasks, 0)
	require.Equal(t, smallConcPerCore, conc)
	require.Equal(t, smallTaskCount, count)
}

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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/driver/backoff"
	"github.com/pingcap/tidb/util/paging"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

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
	pdCli := &tikv.CodecPDClient{Client: pdClient}
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	flashReq := &kv.Request{}
	flashReq.StoreType = kv.TiFlash
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "c"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "c"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], 0, "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], 0, "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("m", "n"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], 0, "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("m", "n"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], 0, "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "k"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], 0, "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "k"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], 0, "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "x"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 4)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], 0, "g", "n")
	taskEqual(t, tasks[2], regionIDs[2], 0, "n", "t")
	taskEqual(t, tasks[3], regionIDs[3], 0, "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "x"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 4)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], 0, "g", "n")
	taskEqual(t, tasks[2], regionIDs[2], 0, "n", "t")
	taskEqual(t, tasks[3], regionIDs[3], 0, "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], 0, "g", "n")
	taskEqual(t, tasks[1], regionIDs[2], 0, "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), flashReq, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], 0, "g", "n")
	taskEqual(t, tasks[1], regionIDs[2], 0, "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], 0, "h", "k", "m", "n")
	taskEqual(t, tasks[1], regionIDs[2], 0, "n", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), flashReq, nil)
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
	pdCli := &tikv.CodecPDClient{Client: pdClient}
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
		tasks, err := buildCopTasks(bo, cache, buildCopRanges(ranges...), req, nil)
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
	tasks, err := buildCopTasks(bo, cache, buildCopRanges(allRanges...), req, nil)
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

	// serveral ranges per bucket
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
	tasks, err = buildCopTasks(bo, cache, buildCopRanges(keyRanges...), req, nil)
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
	tasks, err = buildCopTasks(bo, cache, buildCopRanges(keyRanges...), req, nil)
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
	tasks, err = buildCopTasks(bo, cache, buildCopRanges("n", "o", "p", "q", "s", "w"), req, nil)
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
	tasks, err = buildCopTasks(bo, cache, buildCopRanges("n", "o", "p", "s", "t", "v", "w", "x"), req, nil)
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
	tasks, err = buildCopTasks(bo, cache, buildCopRanges("o", "p", "u", "w"), req, nil)
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
	tasks, err = buildCopTasks(bo, cache, buildCopRanges("n", "x"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, len(expectedTaskRanges))
	for i, task := range tasks {
		taskEqual(t, task, regionIDs[1], regionIDs[1], expectedTaskRanges[i]...)
	}
}

func TestSplitRegionRanges(t *testing.T) {
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
	pdCli := &tikv.CodecPDClient{Client: pdClient}
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	ranges, err := cache.SplitRegionRanges(bo, buildKeyRanges("a", "c"))
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "a", "c")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("h", "y"))
	require.NoError(t, err)
	require.Len(t, ranges, 3)
	rangeEqual(t, ranges, "h", "n", "n", "t", "t", "y")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("s", "z"))
	require.NoError(t, err)
	require.Len(t, ranges, 2)
	rangeEqual(t, ranges, "s", "t", "t", "z")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("s", "s"))
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "s", "s")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("t", "t"))
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "t", "t")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("t", "u"))
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "t", "u")

	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("u", "z"))
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	rangeEqual(t, ranges, "u", "z")

	// min --> max
	ranges, err = cache.SplitRegionRanges(bo, buildKeyRanges("a", "z"))
	require.NoError(t, err)
	require.Len(t, ranges, 4)
	rangeEqual(t, ranges, "a", "g", "g", "n", "n", "t", "t", "z")
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
	pdCli := &tikv.CodecPDClient{Client: pdClient}
	defer pdCli.Close()
	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()
	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "z"), req, nil)
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
	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "z"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	taskEqual(t, tasks[2], regionIDs[0], 0, "a", "m")
	taskEqual(t, tasks[1], regionIDs[1], 0, "m", "q")
	taskEqual(t, tasks[0], regionIDs[2], 0, "q", "z")
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
	pdCli := &tikv.CodecPDClient{Client: pdClient}
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)

	req := &kv.Request{}
	req.Paging = true
	flashReq := &kv.Request{}
	flashReq.StoreType = kv.TiFlash
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "c"), req, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], 0, "a", "c")
	require.True(t, tasks[0].paging)
	require.Equal(t, tasks[0].pagingSize, paging.MinPagingSize)
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
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("b", "c")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(retry), "b", "c", "e", "g")
	}
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("e", "f")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(retry), "a", "c", "e", "f")
	}

	// across ranges
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("b", "f")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(retry), "b", "c", "e", "g")
	}
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("b", "f")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(retry), "a", "c", "e", "f")
	}

	// exhaust the ranges
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("a", "g")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(retry), "a", "c", "e", "g")
	}
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("a", "g")[0]
		retry := worker.calculateRetry(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(retry), "a", "c", "e", "g")
	}

	// nil range
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		retry := worker.calculateRetry(NewKeyRanges(ranges), nil, false)
		rangeEqual(t, toRange(retry), "a", "c", "e", "g")
	}
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		retry := worker.calculateRetry(NewKeyRanges(ranges), nil, true)
		rangeEqual(t, toRange(retry), "a", "c", "e", "g")
	}
}

func TestCalculateRemain(t *testing.T) {
	worker := copIteratorWorker{}

	// split in one range
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("a", "b")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(remain), "b", "c", "e", "g")
	}
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("f", "g")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(remain), "a", "c", "e", "f")
	}

	// across ranges
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("a", "f")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), false)
		rangeEqual(t, toRange(remain), "f", "g")
	}
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("b", "g")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), true)
		rangeEqual(t, toRange(remain), "a", "b")
	}

	// exhaust the ranges
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("a", "g")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), false)
		require.Equal(t, remain.Len(), 0)
	}
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		split := buildKeyRanges("a", "g")[0]
		remain := worker.calculateRemain(NewKeyRanges(ranges), toCopRange(split), true)
		require.Equal(t, remain.Len(), 0)
	}

	// nil range
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		remain := worker.calculateRemain(NewKeyRanges(ranges), nil, false)
		rangeEqual(t, toRange(remain), "a", "c", "e", "g")
	}
	{
		ranges := buildKeyRanges("a", "c", "e", "g")
		remain := worker.calculateRemain(NewKeyRanges(ranges), nil, true)
		rangeEqual(t, toRange(remain), "a", "c", "e", "g")
	}
}

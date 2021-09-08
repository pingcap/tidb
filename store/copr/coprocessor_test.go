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

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/driver/backoff"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestBuildTasks(t *testing.T) {
	t.Parallel()
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
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "c"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "c"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], "a", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], "g", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("m", "n"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("m", "n"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[1], "m", "n")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "k"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[0], "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "k"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[0], "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], "g", "k")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "x"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 4)
	taskEqual(t, tasks[0], regionIDs[0], "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], "g", "n")
	taskEqual(t, tasks[2], regionIDs[2], "n", "t")
	taskEqual(t, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "x"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 4)
	taskEqual(t, tasks[0], regionIDs[0], "a", "g")
	taskEqual(t, tasks[1], regionIDs[1], "g", "n")
	taskEqual(t, tasks[2], regionIDs[2], "n", "t")
	taskEqual(t, tasks[3], regionIDs[3], "t", "x")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "b", "c"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], "a", "b", "b", "c")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "b", "e", "f"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	taskEqual(t, tasks[0], regionIDs[0], "a", "b", "e", "f")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], "g", "n")
	taskEqual(t, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("g", "n", "o", "p"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], "g", "n")
	taskEqual(t, tasks[1], regionIDs[2], "o", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], "h", "k", "m", "n")
	taskEqual(t, tasks[1], regionIDs[2], "n", "p")

	tasks, err = buildCopTasks(bo, cache, buildCopRanges("h", "k", "m", "p"), flashReq)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[1], "h", "k", "m", "n")
	taskEqual(t, tasks[1], regionIDs[2], "n", "p")
}

func TestSplitRegionRanges(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	tasks, err := buildCopTasks(bo, cache, buildCopRanges("a", "z"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	taskEqual(t, tasks[0], regionIDs[0], "a", "m")
	taskEqual(t, tasks[1], regionIDs[1], "m", "z")

	// nil -- 'm' -- 'q' -- nil
	// <-  0 -> <--1-> <-2-->
	regionIDs = append(regionIDs, cluster.AllocID())
	peerIDs = append(peerIDs, cluster.AllocID())
	cluster.Split(regionIDs[1], regionIDs[2], []byte("q"), []uint64{peerIDs[2]}, storeID)
	cache.InvalidateCachedRegion(tasks[1].region)

	req.Desc = true
	tasks, err = buildCopTasks(bo, cache, buildCopRanges("a", "z"), req)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	taskEqual(t, tasks[2], regionIDs[0], "a", "m")
	taskEqual(t, tasks[1], regionIDs[1], "m", "q")
	taskEqual(t, tasks[0], regionIDs[2], "q", "z")
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

func taskEqual(t *testing.T, task *copTask, regionID uint64, keys ...string) {
	require.Equal(t, task.region.GetID(), regionID)
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

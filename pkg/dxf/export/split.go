// Copyright 2026 PingCAP, Inc.
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

package export

import (
	"bytes"
	"context"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/tikv/client-go/v2/tikv"
)

const loadRegionMaxRetry = 8

// loadRegionBoundaries returns the sorted, continuous region boundaries
// covering [start, end), clamped to the range. The result has at least two
// elements: result[0] == start and result[len-1] == end.
func loadRegionBoundaries(ctx context.Context, store kv.Storage, start, end kv.Key) ([]kv.Key, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return nil, errors.New("storage does not support region cache")
	}
	var lastErr error
	for range loadRegionMaxRetry {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		regionCache := hStore.GetRegionCache()
		regions, err := regionCache.LoadRegionsInKeyRange(
			tikv.NewBackofferWithVars(ctx, 20000, nil), start, end)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(regions) == 0 {
			lastErr = errors.New("no region loaded for key range")
			continue
		}
		sort.Slice(regions, func(i, j int) bool {
			return bytes.Compare(regions[i].StartKey(), regions[j].StartKey()) < 0
		})
		continuous := true
		for i := 1; i < len(regions); i++ {
			if !bytes.Equal(regions[i-1].EndKey(), regions[i].StartKey()) {
				continuous = false
				break
			}
		}
		if !continuous {
			lastErr = errors.New("regions are not continuous")
			continue
		}
		boundaries := make([]kv.Key, 0, len(regions)+1)
		boundaries = append(boundaries, start)
		for _, r := range regions[:len(regions)-1] {
			k := kv.Key(r.EndKey())
			if bytes.Compare(k, start) > 0 && bytes.Compare(k, end) < 0 {
				boundaries = append(boundaries, k)
			}
		}
		boundaries = append(boundaries, end)
		return boundaries, nil
	}
	return nil, lastErr
}

// groupBoundaries splits the boundary list into at most groupCnt contiguous
// groups of roughly equal region count, in key order.
func groupBoundaries(boundaries []kv.Key, groupCnt int) [][]kv.Key {
	rangeCnt := len(boundaries) - 1
	sizes := mathutil.Divide2Batches(rangeCnt, max(groupCnt, 1))
	groups := make([][]kv.Key, 0, len(sizes))
	lo := 0
	for _, size := range sizes {
		groups = append(groups, boundaries[lo:lo+size+1])
		lo += size
	}
	return groups
}

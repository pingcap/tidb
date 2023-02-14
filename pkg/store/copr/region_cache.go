// Copyright 2021 PingCAP, Inc.
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
	"bytes"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/driver/options"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// RegionCache wraps tikv.RegionCache.
type RegionCache struct {
	*tikv.RegionCache
}

// NewRegionCache returns a new RegionCache.
func NewRegionCache(rc *tikv.RegionCache) *RegionCache {
	return &RegionCache{rc}
}

// SplitRegionRanges gets the split ranges from pd region.
func (c *RegionCache) SplitRegionRanges(bo *Backoffer, keyRanges []kv.KeyRange, limit int) ([]kv.KeyRange, error) {
	ranges := NewKeyRanges(keyRanges)

	locations, err := c.SplitKeyRangesByLocationsWithoutBuckets(bo, ranges, limit)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	var ret []kv.KeyRange
	for _, loc := range locations {
		for i := 0; i < loc.Ranges.Len(); i++ {
			ret = append(ret, loc.Ranges.At(i))
		}
	}
	return ret, nil
}

// LocationKeyRanges wrapps a real Location in PD and its logical ranges info.
type LocationKeyRanges struct {
	// Location is the real location in PD.
	Location *tikv.KeyLocation
	// Ranges is the logic ranges the current Location contains.
	Ranges  *KeyRanges
	rowHint *int
}

func (l *LocationKeyRanges) getBucketVersion() uint64 {
	return l.Location.GetBucketVersion()
}

// splitKeyRangeByBuckets splits ranges in the same location by buckets and returns a LocationKeyRanges array.
func (l *LocationKeyRanges) splitKeyRangesByBuckets() []*LocationKeyRanges {
	if l.Location.Buckets == nil || len(l.Location.Buckets.Keys) == 0 {
		return []*LocationKeyRanges{l}
	}

	ranges := l.Ranges
	loc := l.Location
	res := []*LocationKeyRanges{}
	for ranges.Len() > 0 {
		// ranges must be in loc.region, so the bucket returned by loc.LocateBucket is guaranteed to be not nil
		bucket := loc.LocateBucket(ranges.At(0).StartKey)

		// Iterate to the first range that is not complete in the bucket.
		var r kv.KeyRange
		var i int
		for ; i < ranges.Len(); i++ {
			r = ranges.At(i)
			if !(bucket.Contains(r.EndKey) || bytes.Equal(bucket.EndKey, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same bucket.
		if i == ranges.Len() {
			res = append(res, &LocationKeyRanges{Location: l.Location, Ranges: ranges})
			break
		}

		if bucket.Contains(r.StartKey) {
			// Part of r is not in the bucket. We need to split it.
			taskRanges := ranges.Slice(0, i)
			taskRanges.last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   bucket.EndKey,
			}
			res = append(res, &LocationKeyRanges{Location: l.Location, Ranges: taskRanges})

			ranges = ranges.Slice(i+1, ranges.Len())
			ranges.first = &kv.KeyRange{
				StartKey: bucket.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// ranges[i] is not in the bucket.
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{Location: l.Location, Ranges: taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
	}
	return res
}

func (c *RegionCache) splitKeyRangesByLocation(loc *tikv.KeyLocation, ranges *KeyRanges, res []*LocationKeyRanges) ([]*LocationKeyRanges, *KeyRanges, bool) {
	// Iterate to the first range that is not complete in the region.
	var r kv.KeyRange
	var i int
	for ; i < ranges.Len(); i++ {
		r = ranges.At(i)
		if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
			break
		}
	}
	// All rest ranges belong to the same region.
	if i == ranges.Len() {
		res = append(res, &LocationKeyRanges{Location: loc, Ranges: ranges})
		return res, ranges, true
	}
	if loc.Contains(r.StartKey) {
		// Part of r is not in the region. We need to split it.
		taskRanges := ranges.Slice(0, i)
		taskRanges.last = &kv.KeyRange{
			StartKey: r.StartKey,
			EndKey:   loc.EndKey,
		}
		res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})
		ranges = ranges.Slice(i+1, ranges.Len())
		ranges.first = &kv.KeyRange{
			StartKey: loc.EndKey,
			EndKey:   r.EndKey,
		}
	} else {
		// rs[i] is not in the region.
		taskRanges := ranges.Slice(0, i)
		res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})
		ranges = ranges.Slice(i, ranges.Len())
	}
	return res, ranges, false
}

// UnspecifiedLimit means no limit.
const UnspecifiedLimit = -1

// SplitKeyRangesByLocationsWithBuckets splits the KeyRanges by logical info in the cache.
// The buckets in the returned LocationKeyRanges are not empty if the region is split by bucket.
func (c *RegionCache) SplitKeyRangesByLocationsWithBuckets(bo *Backoffer, ranges *KeyRanges, limit int) ([]*LocationKeyRanges, error) {
	res := make([]*LocationKeyRanges, 0)
	for ranges.Len() > 0 {
		if limit != UnspecifiedLimit && len(res) >= limit {
			break
		}
		loc, err := c.LocateKey(bo.TiKVBackoffer(), ranges.At(0).StartKey)
		if err != nil {
			return res, derr.ToTiDBErr(err)
		}

		isBreak := false
		res, ranges, isBreak = c.splitKeyRangesByLocation(loc, ranges, res)
		if isBreak {
			break
		}
	}

	return res, nil
}

// SplitKeyRangesByLocationsWithoutBuckets splits the KeyRanges by logical info in the cache.
// The buckets in the returned LocationKeyRanges are empty, regardless of whether the region is split by bucket.
func (c *RegionCache) SplitKeyRangesByLocationsWithoutBuckets(bo *Backoffer, ranges *KeyRanges, limit int) ([]*LocationKeyRanges, error) {
	if limit == 0 || ranges.Len() <= 0 {
		return nil, nil
	}
	// Currently, LocationKeyRanges returned by `LocateKeyRange` doesn't contains buckets,
	// because of https://github.com/tikv/client-go/blob/09ecb550d383c1b048119b586fb5cda658312262/internal/locate/region_cache.go#L1550-L1551.
	locs, err := c.LocateKeyRange(bo.TiKVBackoffer(), ranges.RefAt(0).StartKey, ranges.RefAt(ranges.Len()-1).EndKey)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}

	resCap := len(locs)
	if limit != UnspecifiedLimit {
		resCap = min(resCap, limit)
	}
	res := make([]*LocationKeyRanges, 0, resCap)

	for ranges.Len() > 0 {
		if limit != UnspecifiedLimit && len(res) >= limit {
			break
		}

		nextLocIndex := len(res)
		if nextLocIndex >= len(locs) {
			err = errors.Errorf("Unexpected loc index %d, which should less than %d", nextLocIndex, len(locs))
			return nil, err
		}

		loc := locs[nextLocIndex]
		// For the last loc.
		if nextLocIndex == (len(locs) - 1) {
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: ranges})
			break
		}

		isBreak := false
		res, ranges, isBreak = c.splitKeyRangesByLocation(loc, ranges, res)
		if isBreak {
			break
		}
	}
	return res, nil
}

// SplitKeyRangesByBuckets splits the KeyRanges by buckets information in the cache. If regions don't have buckets,
// it's equal to SplitKeyRangesByLocations.
//
// TODO(youjiali1995): Try to do it in one round and reduce allocations if bucket is not enabled.
func (c *RegionCache) SplitKeyRangesByBuckets(bo *Backoffer, ranges *KeyRanges) ([]*LocationKeyRanges, error) {
	locs, err := c.SplitKeyRangesByLocationsWithBuckets(bo, ranges, UnspecifiedLimit)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	res := make([]*LocationKeyRanges, 0, len(locs))
	for _, loc := range locs {
		res = append(res, loc.splitKeyRangesByBuckets()...)
	}
	return res, nil
}

// OnSendFailForBatchRegions handles send request fail logic.
func (c *RegionCache) OnSendFailForBatchRegions(bo *Backoffer, store *tikv.Store, regionInfos []RegionInfo, scheduleReload bool, err error) {
	metrics.RegionCacheCounterWithSendFail.Add(float64(len(regionInfos)))
	if !store.IsTiFlash() {
		logutil.Logger(bo.GetCtx()).Info("Should not reach here, OnSendFailForBatchRegions only support TiFlash")
		return
	}
	logutil.Logger(bo.GetCtx()).Info("Send fail for " + strconv.Itoa(len(regionInfos)) + " regions, will switch region peer for these regions. Only first " + strconv.Itoa(min(10, len(regionInfos))) + " regions will be logged if the log level is higher than Debug")
	for index, ri := range regionInfos {
		if ri.Meta == nil {
			continue
		}
		c.OnSendFailForTiFlash(bo.TiKVBackoffer(), store, ri.Region, ri.Meta, scheduleReload, err, !(index < 10 || log.GetLevel() <= zap.DebugLevel))
	}
}

// BuildBatchTask fetches store and peer info for cop task, wrap it as `batchedCopTask`.
func (c *RegionCache) BuildBatchTask(bo *Backoffer, req *kv.Request, task *copTask, replicaRead kv.ReplicaReadType) (*batchedCopTask, error) {
	var (
		rpcContext *tikv.RPCContext
		err        error
	)
	if replicaRead == kv.ReplicaReadFollower {
		followerStoreSeed := uint32(0)
		leastEstWaitTime := time.Duration(math.MaxInt64)
		var (
			firstFollowerPeer *uint64
			followerContext   *tikv.RPCContext
		)
		for {
			followerContext, err = c.GetTiKVRPCContext(bo.TiKVBackoffer(), task.region, options.GetTiKVReplicaReadType(replicaRead), followerStoreSeed)
			if err != nil {
				return nil, err
			}
			if firstFollowerPeer == nil {
				firstFollowerPeer = &rpcContext.Peer.Id
			} else if *firstFollowerPeer == rpcContext.Peer.Id {
				break
			}
			estWaitTime := followerContext.Store.EstimatedWaitTime()
			// the wait time of this follower is under given threshold, choose it.
			if estWaitTime > req.StoreBusyThreshold {
				continue
			}
			if rpcContext == nil {
				rpcContext = followerContext
			} else if estWaitTime < leastEstWaitTime {
				leastEstWaitTime = estWaitTime
				rpcContext = followerContext
			}
			followerStoreSeed++
		}
		// all replicas are busy, fallback to leader.
		if rpcContext == nil {
			replicaRead = kv.ReplicaReadLeader
		}
	}

	if replicaRead == kv.ReplicaReadLeader {
		rpcContext, err = c.GetTiKVRPCContext(bo.TiKVBackoffer(), task.region, options.GetTiKVReplicaReadType(replicaRead), 0)
		if err != nil {
			return nil, err
		}
	}

	// fallback to non-batch path
	if rpcContext == nil {
		return nil, nil
	}
	return &batchedCopTask{
		task: task,
		region: coprocessor.RegionInfo{
			RegionId: rpcContext.Region.GetID(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: rpcContext.Region.GetConfVer(),
				Version: rpcContext.Region.GetVer(),
			},
			Ranges: task.ranges.ToPBRanges(),
		},
		storeID:               rpcContext.Store.StoreID(),
		peer:                  rpcContext.Peer,
		loadBasedReplicaRetry: replicaRead != kv.ReplicaReadLeader,
	}, nil
}

// SplitCachedKeyRangesByLocations splits the cached KeyRanges by logical info in the cache,
// there may be remained ranges.
// It may replace the usage of SplitKeyRangesByLocations.
func (c *RegionCache) SplitCachedKeyRangesByLocations(bo *Backoffer, ranges *KeyRanges, hints []int, keepOrder, tryCache bool,
	handler func(locs []*LocationKeyRanges) error,
) error {
	if ranges.Len() != len(hints) {
		hints = nil
	}
	// Turn off tryCache when the result tasks must be in order.
	tryCache = tryCache && !keepOrder
	var (
		locs      []*LocationKeyRanges
		misses    []kv.KeyRange
		missHints []int
	)
	if tryCache {
		locs = make([]*LocationKeyRanges, 0, 16)
		misses = make([]kv.KeyRange, 0, 16)
		if hints != nil {
			missHints = make([]int, 0, 16)
		}
	}
	for ranges.Len() > 0 {
		var loc *tikv.KeyLocation
		remain := ranges.Len()
		var i int
		for ; i < remain; i++ {
			keyRange := ranges.RefAt(i)
			if tryCache {
				loc = c.TryLocateKey(keyRange.StartKey)
			} else {
				var err error
				loc, err = c.LocateKey(bo.TiKVBackoffer(), keyRange.StartKey)
				if err != nil {
					return err
				}
			}
			if loc != nil {
				break
			}
			// When tryCache and cache miss, store the missed keyRange.
			hint := -1
			if hints != nil {
				hint = hints[i]
			}
			misses = append(misses, *keyRange)
			if hint >= 0 {
				missHints = append(missHints, hint)
			}
		}
		// ranges is drained.
		if loc == nil {
			break
		}

		// loc of ith range exist.
		ranges = ranges.Slice(i, ranges.Len())
		if hints != nil {
			hints = hints[i:]
		}

		// Iterate to the first range that is not complete in the region.
		var r *kv.KeyRange
		i = 0
		for ; i < ranges.Len(); i++ {
			r = ranges.RefAt(i)
			if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same region.
		if i == ranges.Len() {
			locKeyRanges := &LocationKeyRanges{Location: loc, Ranges: ranges}
			if hints != nil {
				hint := 0
				for j := 0; j < len(hints); j++ {
					hint += hints[j]
				}
				locKeyRanges.rowHint = &hint
			}
			locs = append(locs, locKeyRanges.splitKeyRangesByBuckets()...)
			break
		}

		if loc.Contains(r.StartKey) {
			// Part of r is not in the region. We need to split it.
			taskRanges := ranges.Slice(0, i)
			taskRanges.last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   loc.EndKey,
			}
			locKeyRanges := &LocationKeyRanges{Location: loc, Ranges: taskRanges}
			if hints != nil {
				hint := 0
				for j := 0; j <= i; j++ {
					hint += hints[j]
				}
				locKeyRanges.rowHint = &hint
			}

			// When tryCache, the cache-hit locs will be handled after the loop.
			if tryCache {
				locs = append(locs, locKeyRanges.splitKeyRangesByBuckets()...)
			} else {
				if err := handler(locKeyRanges.splitKeyRangesByBuckets()); err != nil {
					return err
				}
			}

			ranges = ranges.Slice(i+1, ranges.Len())
			ranges.first = &kv.KeyRange{
				StartKey: loc.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// rs[i] is not in the region.
			taskRanges := ranges.Slice(0, i)
			locKeyRanges := &LocationKeyRanges{Location: loc, Ranges: taskRanges}
			if hints != nil {
				hint := 0
				for j := 0; j < i; j++ {
					hint += hints[j]
				}
				locKeyRanges.rowHint = &hint
			}

			// When tryCache, the cache-hit locs will be handled after the loop.
			if tryCache {
				locs = append(locs, locKeyRanges.splitKeyRangesByBuckets()...)
			} else {
				if err := handler(locKeyRanges.splitKeyRangesByBuckets()); err != nil {
					return err
				}
			}

			ranges = ranges.Slice(i, ranges.Len())
		}
		if hints != nil {
			hints = hints[i:]
		}
	}
	if len(locs) > 0 {
		if err := handler(locs); err != nil {
			return nil
		}
	}
	if tryCache && len(misses) > 0 {
		if err := c.SplitCachedKeyRangesByLocations(bo, NewKeyRanges(misses), missHints, keepOrder, false, handler); err != nil {
			return err
		}
	}
	return nil
}

// SplitKeyRangesByLocationsDesc splits the cached KeyRanges by logical info in the cache in desc mode,
// Because desc scan must be in order, so load region regardless of cache.
func (c *RegionCache) SplitKeyRangesByLocationsDesc(bo *Backoffer, ranges *KeyRanges, hints []int,
	handler func(locs []*LocationKeyRanges) error,
) error {
	if ranges.Len() != len(hints) {
		hints = nil
	}

	getHint := func(i int) int {
		if hints == nil {
			return -1
		}
		return hints[i]
	}

	locs := make([]*tikv.KeyLocation, 0, 2)

	for i := ranges.Len() - 1; i >= 0; i-- {
		keyRange := ranges.RefAt(i)
		startKey := keyRange.StartKey
		locs = locs[:0]
		for {
			loc, err := c.LocateKey(bo.TiKVBackoffer(), startKey)
			if err != nil {
				return err
			}
			locs = append(locs, loc)
			if loc.Contains(keyRange.EndKey) || bytes.Equal(loc.EndKey, keyRange.EndKey) {
				// the range is drained.
				break
			}
			startKey = loc.EndKey
			continue
		}

		hint := getHint(i)
		endKey := keyRange.EndKey
		for j := len(locs) - 1; j >= 0; j-- {
			loc := locs[j]
			var keyRanges []kv.KeyRange
			if j > 0 {
				keyRanges = make([]kv.KeyRange, 0, 1)
			} else {
				keyRanges = make([]kv.KeyRange, 0, 4)
			}
			locStartKey := loc.StartKey
			if loc.Contains(keyRange.StartKey) {
				locStartKey = keyRange.StartKey
			}
			keyRanges = append(keyRanges, kv.KeyRange{StartKey: locStartKey, EndKey: endKey})
			endKey = loc.StartKey
			// the start key of the first range may cover the lower ranges.
			for j == 0 && i >= 1 {
				subKeyRange := ranges.RefAt(i - 1)
				if loc.Contains(subKeyRange.StartKey) {
					keyRanges = append(keyRanges, *subKeyRange)
					hint += getHint(i - 1)
					i--
					continue
				} else if loc.Contains(subKeyRange.EndKey) && !bytes.Equal(subKeyRange.EndKey, loc.StartKey) {
					keyRanges = append(keyRanges, kv.KeyRange{StartKey: subKeyRange.StartKey, EndKey: subKeyRange.EndKey})
					subKeyRange.EndKey = loc.StartKey
					hint += getHint(i - 1)
				}
				break
			}
			// the key ranges are built in reverse(descending order), so we need to reverse it back to ascending order.
			reverse(keyRanges)
			locKeyRanges := &LocationKeyRanges{Location: loc, Ranges: NewKeyRanges(keyRanges)}
			if hint >= 0 {
				h := hint
				locKeyRanges.rowHint = &h
			}
			buckets := locKeyRanges.splitKeyRangesByBuckets()
			reverse(buckets)
			if err := handler(buckets); err != nil {
				return err
			}
		}
	}
	return nil
}

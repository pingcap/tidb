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
	"context"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/driver/options"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Helper functions for logging
func formatLocation(loc *tikv.KeyLocation) zap.Field {
	return zap.Object("location", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		enc.AddUint64("regionID", loc.Region.GetID())
		enc.AddString("startKey", redact.Key(loc.StartKey))
		enc.AddString("endKey", redact.Key(loc.EndKey))
		if loc.Buckets != nil {
			enc.AddInt("bucketCount", len(loc.Buckets.Keys))
		}
		return nil
	}))
}

func keyField(name string, key []byte) zap.Field {
	return zap.String(name, redact.Key(key))
}

func formatRanges(ranges *KeyRanges) zap.Field {
	return zap.Object("ranges", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		enc.AddInt("count", ranges.Len())
		if ranges.Len() > 0 {
			enc.AddString("firstStart", redact.Key(ranges.At(0).StartKey))
			enc.AddString("lastEnd", redact.Key(ranges.At(ranges.Len()-1).EndKey))
		}
		return nil
	}))
}

func validateLocationCoverage(ctx context.Context, kvRanges []tikv.KeyRange, locs []*tikv.KeyLocation) {
	if len(kvRanges) == 0 {
		return
	}
	rangeIdx := 0
	mismatch := false
	for locIdx, loc := range locs {
		if rangeIdx >= len(kvRanges) {
			break
		}
		currentRange := kvRanges[rangeIdx]
		// Anomaly: location doesn't cover the range it should
		if len(currentRange.StartKey) > 0 && !loc.Contains(currentRange.StartKey) && !bytes.Equal(currentRange.StartKey, loc.StartKey) {
			mismatch = true
			if traceevent.IsEnabled(traceevent.CoprRegionCache) {
				traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "BatchLocateKeyRanges location does not cover range start",
					formatLocation(loc),
					keyField("rangeStart", currentRange.StartKey),
					keyField("rangeEnd", currentRange.EndKey),
					zap.Int("rangeIndex", rangeIdx),
					zap.Int("locationIndex", locIdx))
			}
		}
		for rangeIdx < len(kvRanges) {
			endKey := kvRanges[rangeIdx].EndKey
			if len(endKey) == 0 || len(loc.EndKey) == 0 || loc.Contains(endKey) || bytes.Equal(loc.EndKey, endKey) {
				rangeIdx++
			} else {
				break
			}
		}
	}
	if rangeIdx < len(kvRanges) {
		mismatch = true
		if traceevent.IsEnabled(traceevent.CoprRegionCache) {
			remaining := kvRanges[rangeIdx]
			traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "BatchLocateKeyRanges returned locations that do not cover all ranges",
				keyField("nextRangeStart", remaining.StartKey),
				keyField("nextRangeEnd", remaining.EndKey),
				zap.Int("remainingRangeCount", len(kvRanges)-rangeIdx),
				zap.Int("locationCount", len(locs)))
		}
	}
	// Log warning if coverage mismatch detected
	if mismatch {
		logutil.BgLogger().Warn("BatchLocateKeyRanges coverage mismatch",
			zap.Int("requestedRangeCount", len(kvRanges)),
			zap.Int("locationCount", len(locs)),
			zap.Int("firstUncoveredRangeIndex", rangeIdx))
	}
}

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

	locations, err := c.SplitKeyRangesByLocations(bo, ranges, limit, true, false)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	var ret []kv.KeyRange
	for _, loc := range locations {
		for i := range loc.Ranges.Len() {
			ret = append(ret, loc.Ranges.At(i))
		}
	}
	return ret, nil
}

// LocationKeyRanges wraps a real Location in PD and its logical ranges info.
type LocationKeyRanges struct {
	// Location is the real location in PD.
	Location *tikv.KeyLocation
	// Ranges is the logic ranges the current Location contains.
	Ranges *KeyRanges
}

func (l *LocationKeyRanges) getBucketVersion() uint64 {
	return l.Location.GetBucketVersion()
}

// splitKeyRangeByBuckets splits ranges in the same location by buckets and returns a LocationKeyRanges array.
func (l *LocationKeyRanges) splitKeyRangesByBuckets(ctx context.Context) []*LocationKeyRanges {
	if l.Location.Buckets == nil || len(l.Location.Buckets.Keys) == 0 {
		return []*LocationKeyRanges{l}
	}

	ranges := l.Ranges
	loc := l.Location
	res := []*LocationKeyRanges{}

	// Trace: entering bucket split with state
	if traceevent.IsEnabled(traceevent.CoprRegionCache) {
		traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "splitKeyRangesByBuckets begin",
			zap.Uint64("regionID", loc.Region.GetID()),
			zap.Int("rangeCount", ranges.Len()),
			zap.Int("bucketCount", len(loc.Buckets.Keys)),
			zap.Uint64("bucketVersion", loc.GetBucketVersion()))
	}

	for ranges.Len() > 0 {
		startKey := ranges.At(0).StartKey

		// Known anomaly: range key outside location - invariant violation
		if !loc.Contains(startKey) {
			logutil.Logger(ctx).Error("range startKey outside location boundaries",
				keyField("startKey", startKey),
				zap.Uint64("regionID", loc.Region.GetID()),
				keyField("locationStart", loc.StartKey),
				keyField("locationEnd", loc.EndKey))
		}

		bucket := loc.LocateBucket(startKey)

		// Trace: bucket located for this startKey
		if traceevent.IsEnabled(traceevent.CoprRegionCache) {
			if bucket != nil {
				traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "bucket located",
					keyField("startKey", startKey),
					keyField("bucketStart", bucket.StartKey),
					keyField("bucketEnd", bucket.EndKey))
			}
		}

		// Known anomaly: LocateBucket returned nil - THIS IS THE BUG
		// Defensive check with comprehensive diagnostics
		if bucket == nil {
			// Check various reasons why LocateBucket might return nil
			hasValidBuckets := loc.Buckets != nil && len(loc.Buckets.Keys) > 0
			keyInRegion := loc.Contains(startKey)

			// Log ERROR with all relevant context including bucket keys
			bucketKeys := func() []string {
				if loc.Buckets == nil {
					return []string{"<nil buckets>"}
				}
				keys := make([]string, len(loc.Buckets.Keys))
				for i, k := range loc.Buckets.Keys {
					keys[i] = redact.Key(k)
				}
				return keys
			}()

			logutil.Logger(ctx).Error("LocateBucket returned nil",
				keyField("key", startKey),
				zap.Uint64("regionID", loc.Region.GetID()),
				zap.Bool("keyInRegion", keyInRegion),
				zap.Bool("hasValidBuckets", hasValidBuckets),
				zap.Int("bucketCount", func() int {
					if loc.Buckets != nil {
						return len(loc.Buckets.Keys)
					}
					return 0
				}()),
				zap.Uint64("bucketVersion", loc.GetBucketVersion()),
				keyField("locationStart", loc.StartKey),
				keyField("locationEnd", loc.EndKey),
				zap.Strings("bucketKeys", bucketKeys))

			// Continue processing - don't panic, let caller handle it
			// This gives us a chance to see what happens next in traces
		}

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
			res = append(res, &LocationKeyRanges{l.Location, ranges})
			break
		}

		if bucket.Contains(r.StartKey) {
			// Part of r is not in the bucket. We need to split it.
			taskRanges := ranges.Slice(0, i)
			taskRanges.last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   bucket.EndKey,
			}
			res = append(res, &LocationKeyRanges{l.Location, taskRanges})

			ranges = ranges.Slice(i+1, ranges.Len())
			ranges.first = &kv.KeyRange{
				StartKey: bucket.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// Anomaly: range not in bucket - boundary issue
			if traceevent.IsEnabled(traceevent.CoprRegionCache) {
				traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "range outside bucket",
					formatLocation(loc),
					zap.Int("rangeIndex", i),
					keyField("rangeStart", r.StartKey),
					keyField("rangeEnd", r.EndKey),
					keyField("bucketStart", bucket.StartKey),
					keyField("bucketEnd", bucket.EndKey))
			}
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{l.Location, taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
	}
	return res
}

func (c *RegionCache) splitKeyRangesByLocation(ctx context.Context, loc *tikv.KeyLocation, ranges *KeyRanges, res []*LocationKeyRanges) ([]*LocationKeyRanges, *KeyRanges, bool) {
	// Known anomaly: invalid input range (defensive check)
	for i := 0; i < ranges.Len(); i++ {
		r := ranges.At(i)
		if bytes.Equal(r.StartKey, r.EndKey) {
			logutil.Logger(ctx).Error("invalid input range: start == end",
				keyField("key", r.StartKey),
				zap.Uint64("regionID", loc.Region.GetID()),
				zap.Int("rangeIndex", i))
		}
	}

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

	// Anomaly: range exceeds location boundaries
	if traceevent.IsEnabled(traceevent.CoprRegionCache) {
		traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "range exceeds location boundaries",
			formatLocation(loc),
			formatRanges(ranges),
			zap.Int("rangeIndex", i),
			keyField("rangeStart", r.StartKey),
			keyField("rangeEnd", r.EndKey),
			zap.Bool("containsStart", loc.Contains(r.StartKey)),
			zap.Bool("containsEnd", loc.Contains(r.EndKey)),
			zap.Uint64("regionVersion", loc.Region.GetVer()),
			zap.Uint64("regionConfVersion", loc.Region.GetConfVer()),
			zap.Uint64("bucketVersion", loc.GetBucketVersion()))
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
		// Anomaly: range start is outside location
		if traceevent.IsEnabled(traceevent.CoprRegionCache) {
			traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "range start outside location",
				formatLocation(loc),
				formatRanges(ranges),
				zap.Int("rangeIndex", i),
				keyField("rangeStart", r.StartKey),
				keyField("rangeEnd", r.EndKey))
		}
		if i > 0 {
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
	}
	return res, ranges, false
}

// UnspecifiedLimit means no limit.
const UnspecifiedLimit = -1

// SplitKeyRangesByLocations splits the KeyRanges by logical info in the cache.
// The buckets in the returned LocationKeyRanges are empty, regardless of whether the region is split by bucket.
func (c *RegionCache) SplitKeyRangesByLocations(bo *Backoffer, ranges *KeyRanges, limit int, needLeader, buckets bool) ([]*LocationKeyRanges, error) {
	if limit == 0 || ranges.Len() <= 0 {
		return nil, nil
	}

	kvRanges := make([]tikv.KeyRange, 0, ranges.Len())
	for i := range ranges.Len() {
		kvRanges = append(kvRanges, tikv.KeyRange{
			StartKey: ranges.At(i).StartKey,
			EndKey:   ranges.At(i).EndKey,
		})
	}
	opts := make([]tikv.BatchLocateKeyRangesOpt, 0, 2)
	if needLeader {
		opts = append(opts, tikv.WithNeedRegionHasLeaderPeer())
	}
	if buckets {
		opts = append(opts, tikv.WithNeedBuckets())
	}
	locs, err := c.BatchLocateKeyRanges(bo.TiKVBackoffer(), kvRanges, opts...)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}

	resCap := len(locs)
	if limit != UnspecifiedLimit {
		resCap = min(resCap, limit)
	}
	res := make([]*LocationKeyRanges, 0, resCap)

	ctx := bo.GetCtx()

	// Trace: batch locate completed
	if traceevent.IsEnabled(traceevent.CoprRegionCache) {
		traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "BatchLocateKeyRanges completed",
			zap.Int("requestedRanges", len(kvRanges)),
			zap.Int("returnedLocations", len(locs)),
			zap.Bool("needBuckets", buckets))
	}

	// Validate PD returned locations cover our requested ranges
	validateLocationCoverage(ctx, kvRanges, locs)

	nextLocIndex := 0
	for ranges.Len() > 0 {
		if limit != UnspecifiedLimit && len(res) >= limit {
			break
		}

		// Anomaly: index overflow - should not happen
		if nextLocIndex >= len(locs) {
			if traceevent.IsEnabled(traceevent.CoprRegionCache) {
				traceevent.TraceEvent(traceevent.CoprRegionCache, ctx, "batch locate index overflow",
					zap.Int("nextLocIndex", nextLocIndex),
					zap.Int("locations", len(locs)),
					formatRanges(ranges))
			}
			err = errors.Errorf("Unexpected loc index %d, which should less than %d", nextLocIndex, len(locs))
			return nil, err
		}

		loc := locs[nextLocIndex]
		if nextLocIndex == (len(locs) - 1) {
			// Last location: assign all remaining ranges
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: ranges})
			break
		}
		nextLocIndex++

		isBreak := false
		res, ranges, isBreak = c.splitKeyRangesByLocation(ctx, loc, ranges, res)
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
	locs, err := c.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit, false, true)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	ctx := bo.GetCtx()
	res := make([]*LocationKeyRanges, 0, len(locs))
	for _, loc := range locs {
		segments := loc.splitKeyRangesByBuckets(ctx)
		res = append(res, segments...)
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
	if replicaRead != kv.ReplicaReadLeader {
		return nil, nil
	}

	rpcContext, err := c.GetTiKVRPCContext(bo.TiKVBackoffer(), task.region, options.GetTiKVReplicaReadType(replicaRead), 0)
	if err != nil {
		return nil, err
	}

	// fallback to non-batch path
	if rpcContext == nil {
		return nil, nil
	}

	// when leader is busy, we don't batch the cop task to allow the load balance to work.
	if rpcContext.Store.EstimatedWaitTime() > req.StoreBusyThreshold {
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

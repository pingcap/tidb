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

func validateLocationCoverage(ctx context.Context, kvRanges []tikv.KeyRange, locs []*tikv.KeyLocation) /* valid */ bool {
	if len(kvRanges) == 0 {
		return true
	}

	// First, validate monotonicity of locations to catch PD corruption early
	for i := 1; i < len(locs); i++ {
		prev, curr := locs[i-1], locs[i]

		// Check that locations are ordered (prev.StartKey <= curr.StartKey)
		// Empty start key means beginning of key space (minimum)
		if len(curr.StartKey) == 0 && len(prev.StartKey) > 0 {
			// Current starts from beginning, but previous doesn't - wrong order!
			logutil.BgLogger().Error("BatchLocateKeyRanges locations not monotonic",
				zap.Int("locationIndex", i),
				zap.String("issue", "current location starts from beginning but appears after non-beginning location"),
				keyField("prevStart", prev.StartKey),
				keyField("currStart", curr.StartKey))
			return false
		}
		if len(prev.StartKey) > 0 && len(curr.StartKey) > 0 && bytes.Compare(prev.StartKey, curr.StartKey) > 0 {
			logutil.BgLogger().Error("BatchLocateKeyRanges locations not monotonic",
				zap.Int("locationIndex", i),
				keyField("prevStart", prev.StartKey),
				keyField("currStart", curr.StartKey))
			return false
		}

		// Check for overlaps/gaps: prev.EndKey should be <= curr.StartKey
		// Empty end key means infinity - there should be no next location
		if len(prev.EndKey) == 0 {
			logutil.BgLogger().Error("BatchLocateKeyRanges location extends to infinity but is not last",
				zap.Int("locationIndex", i-1),
				zap.Int("totalLocations", len(locs)),
				keyField("prevStart", prev.StartKey))
			return false
		}

		// Both keys non-empty - check for overlap
		if len(prev.EndKey) > 0 && len(curr.StartKey) > 0 && bytes.Compare(prev.EndKey, curr.StartKey) > 0 {
			logutil.BgLogger().Error("BatchLocateKeyRanges locations overlap",
				zap.Int("locationIndex", i),
				keyField("prevEnd", prev.EndKey),
				keyField("currStart", curr.StartKey))
			return false
		}
	}

	rangeIdx := 0
	locIdx := 0
	// Track the first mismatch for better diagnostics
	firstMismatchRangeIdx := -1
	firstMismatchLocIdx := -1
	firstMismatchReason := ""
	var firstMismatchLoc *tikv.KeyLocation
	var firstMismatchRange tikv.KeyRange
	// Track if current range continues from previous location (partial coverage)
	rangeContinuesFromPrevLoc := false
	var prevLocEndKey []byte

	for _, loc := range locs {
		if rangeIdx >= len(kvRanges) {
			// All ranges processed - remaining locations are okay
			break
		}
		currentRange := kvRanges[rangeIdx]

		// Only validate start coverage if this is the first location for this range
		if !rangeContinuesFromPrevLoc {
			startCovered := false
			if len(currentRange.StartKey) == 0 {
				// Empty start key means beginning of key space
				// Location must also start from beginning
				startCovered = len(loc.StartKey) == 0
			} else {
				// Non-empty start key
				startCovered = loc.Contains(currentRange.StartKey) || bytes.Equal(currentRange.StartKey, loc.StartKey)
			}

			if !startCovered && firstMismatchRangeIdx == -1 {
				firstMismatchRangeIdx = rangeIdx
				firstMismatchLocIdx = locIdx
				firstMismatchReason = "location does not cover range start"
				firstMismatchLoc = loc
				firstMismatchRange = currentRange
			}
		} else {
			// Range continues from previous location - verify no gap
			if !bytes.Equal(prevLocEndKey, loc.StartKey) && firstMismatchRangeIdx == -1 {
				firstMismatchRangeIdx = rangeIdx
				firstMismatchLocIdx = locIdx
				firstMismatchReason = "gap between locations"
				firstMismatchLoc = loc
				firstMismatchRange = currentRange
			}
		}

		locIdx++

		// Process all ranges that end within or at this location
		rangeContinuesFromPrevLoc = false
		for rangeIdx < len(kvRanges) {
			r := kvRanges[rangeIdx]

			// Check if this range's end is covered by this location
			endCovered := false
			if len(r.EndKey) == 0 {
				// Empty end key means infinity - location must also extend to infinity
				endCovered = len(loc.EndKey) == 0
			} else if len(loc.EndKey) == 0 {
				// Location extends to infinity, covers any finite end
				endCovered = true
			} else {
				// Both are non-empty - check containment or boundary match
				endCovered = loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)
			}

			if !endCovered {
				// This range extends beyond this location
				// Should be covered by next location
				rangeContinuesFromPrevLoc = true
				prevLocEndKey = loc.EndKey
				break
			}

			// Range fully covered, move to next range
			rangeIdx++
			rangeContinuesFromPrevLoc = false
		}
	}

	// Check if all ranges were covered
	if rangeIdx < len(kvRanges) && firstMismatchRangeIdx == -1 {
		firstMismatchRangeIdx = rangeIdx
		firstMismatchReason = "locations do not cover all ranges"
		firstMismatchRange = kvRanges[rangeIdx]
		// No specific location to blame, locIdx will be set to len(locs)
		firstMismatchLocIdx = locIdx
	}

	// Log error if coverage mismatch detected with full context
	if firstMismatchRangeIdx != -1 {
		fields := []zap.Field{
			zap.String("reason", firstMismatchReason),
			zap.Int("requestedRangeCount", len(kvRanges)),
			zap.Int("locationCount", len(locs)),
			zap.Int("firstMismatchRangeIndex", firstMismatchRangeIdx),
			zap.Int("firstMismatchLocationIndex", firstMismatchLocIdx),
			keyField("mismatchRangeStart", firstMismatchRange.StartKey),
			keyField("mismatchRangeEnd", firstMismatchRange.EndKey),
		}

		// Add location details if available
		if firstMismatchLoc != nil {
			fields = append(fields,
				zap.Uint64("mismatchLocationRegionID", firstMismatchLoc.Region.GetID()),
				keyField("mismatchLocationStart", firstMismatchLoc.StartKey),
				keyField("mismatchLocationEnd", firstMismatchLoc.EndKey))
		}

		// Add gap details if this was a gap error
		if firstMismatchReason == "gap between locations" && len(prevLocEndKey) > 0 {
			fields = append(fields, keyField("prevLocationEnd", prevLocEndKey))
		}

		// Add remaining uncovered range info for missing coverage
		if firstMismatchReason == "locations do not cover all ranges" {
			fields = append(fields, zap.Int("remainingRangeCount", len(kvRanges)-rangeIdx))
		}

		logutil.BgLogger().Error("BatchLocateKeyRanges coverage mismatch", fields...)
		return false
	}
	return true
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
	for ranges.Len() > 0 {
		startKey := ranges.At(0).StartKey
		bucket := loc.LocateBucket(startKey)

		// Known anomaly: LocateBucket returned nil
		// Based on LocateBucket implementation analysis:
		// - LocateBucket returns nil IFF !loc.Contains(startKey)
		// - This means startKey is outside location boundaries
		// - Bucket structure issues (gaps, sorting, etc.) cannot cause nil
		//   because fallback logic creates synthetic buckets
		if bucket == nil {
			currentRange := ranges.At(0)

			// Diagnostic checks to identify root cause:
			// 1. Is startKey from the current range we're processing?
			isFromCurrentRange := bytes.Equal(startKey, currentRange.StartKey)

			// 2. Does location contain the range boundaries?
			locContainsRangeStart := loc.Contains(currentRange.StartKey)
			locContainsRangeEnd := loc.Contains(currentRange.EndKey) ||
				bytes.Equal(loc.EndKey, currentRange.EndKey)

			// 3. Which direction is startKey outside the location?
			beforeLocation := bytes.Compare(startKey, loc.StartKey) < 0
			afterLocation := len(loc.EndKey) > 0 && bytes.Compare(startKey, loc.EndKey) >= 0

			// 4. Bucket structure info
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

			// Log ERROR with comprehensive diagnostics
			// Decision tree:
			// - If keyInRegion=false (expected):
			//   - If isFromCurrentRange=false → Bug: wrong startKey (iteration/variable bug)
			//   - If isFromCurrentRange=true:
			//     - If locContains* =false → Bug: LocationKeyRanges construction
			//     - If beforeLocation=true → Bug: startKey < loc.StartKey (range slicing)
			//     - If afterLocation=true → Bug: startKey >= loc.EndKey (range slicing)
			//     - Else → PD bug: location boundaries wrong
			// - If keyInRegion=true (impossible) → LocateBucket bug or memory corruption
			logutil.Logger(ctx).Error("LocateBucket returned nil",
				// Basic identification
				keyField("key", startKey),
				zap.Uint64("regionID", loc.Region.GetID()),
				zap.Bool("keyInRegion", loc.Contains(startKey)),

				// Root cause diagnostics
				zap.Bool("isFromCurrentRange", isFromCurrentRange),
				zap.Bool("locContainsRangeStart", locContainsRangeStart),
				zap.Bool("locContainsRangeEnd", locContainsRangeEnd),
				zap.Bool("beforeLocation", beforeLocation),
				zap.Bool("afterLocation", afterLocation),

				// Current range context
				keyField("currentRangeStart", currentRange.StartKey),
				keyField("currentRangeEnd", currentRange.EndKey),
				zap.Int("remainingRanges", ranges.Len()),

				// Location boundaries
				keyField("locationStart", loc.StartKey),
				keyField("locationEnd", loc.EndKey),

				// Bucket information
				zap.Bool("hasValidBuckets", loc.Buckets != nil && len(loc.Buckets.Keys) > 0),
				zap.Int("bucketCount", func() int {
					if loc.Buckets != nil {
						return len(loc.Buckets.Keys)
					}
					return 0
				}()),
				zap.Uint64("bucketVersion", loc.GetBucketVersion()),
				zap.Strings("bucketKeys", bucketKeys))

			// Cannot continue - would panic on bucket.Contains() below
			// Return what we've successfully processed so far
			break
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
			// ranges[i] is not in the bucket.
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{l.Location, taskRanges})
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

	ctx := bo.GetCtx()

	// Validate PD returned locations cover our requested ranges
	// Returns false on mismatch, but we continue processing (validation is for diagnostics)
	_ = validateLocationCoverage(ctx, kvRanges, locs)

	resCap := len(locs)
	if limit != UnspecifiedLimit {
		resCap = min(resCap, limit)
	}
	res := make([]*LocationKeyRanges, 0, resCap)

	nextLocIndex := 0
	for ranges.Len() > 0 {
		if limit != UnspecifiedLimit && len(res) >= limit {
			break
		}

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
		nextLocIndex++

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
	locs, err := c.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit, false, true)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	res := make([]*LocationKeyRanges, 0, len(locs))
	ctx := bo.GetCtx()
	for _, loc := range locs {
		res = append(res, loc.splitKeyRangesByBuckets(ctx)...)
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

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
	"github.com/pingcap/failpoint"
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
			// Log bucket keys as redacted strings
			bucketKeys := make([]string, 0, len(loc.Buckets.Keys))
			for _, key := range loc.Buckets.Keys {
				bucketKeys = append(bucketKeys, redact.Key(key))
			}
			if err := enc.AddArray("bucketKeys", zapcore.ArrayMarshalerFunc(func(ae zapcore.ArrayEncoder) error {
				for _, key := range bucketKeys {
					ae.AppendString(key)
				}
				return nil
			})); err != nil {
				return err
			}
		}
		return nil
	}))
}

func keyField(name string, key []byte) zap.Field {
	return zap.String(name, redact.Key(key))
}

func formatRanges(ranges *KeyRanges) zap.Field {
	return zap.Object("ranges", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		count := ranges.Len()
		enc.AddInt("count", count)

		if count > 0 {
			needRedact := redact.NeedRedact()
			if err := enc.AddArray("ranges", zapcore.ArrayMarshalerFunc(func(ae zapcore.ArrayEncoder) error {
				// Log all ranges for complete debugging (no limit)
				for i := range count {
					r := ranges.At(i)
					if err := ae.AppendObject(zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
						if needRedact {
							enc.AddString("start", "?")
							enc.AddString("end", "?")
						} else {
							// Use zap's binary encoding (base64) - faster and uses less memory
							enc.AddBinary("start", r.StartKey)
							enc.AddBinary("end", r.EndKey)
						}
						return nil
					})); err != nil {
						return err
					}
				}
				return nil
			})); err != nil {
				return err
			}
		}
		return nil
	}))
}

// compareKeyRangeBoundary compares two key range boundaries where empty end keys mean +infinity.
// Empty start keys are handled naturally by bytes.Compare (empty < any non-empty).
// The isStart parameters indicate whether each key is a start boundary or end boundary (+inf when empty).
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareKeyRangeBoundary(a, b []byte, aIsStart, bIsStart bool) int {
	if len(a) == 0 && !aIsStart {
		if len(b) == 0 && !bIsStart {
			return 0
		}
		return 1
	} else if len(b) == 0 && !bIsStart {
		return -1
	}

	return bytes.Compare(a, b)
}

// locContainsStartKey checks if loc contains key as a start key (key must be in [loc.Start, loc.End)).
func locContainsStartKey(loc *tikv.KeyLocation, key []byte) bool {
	if loc == nil {
		return false
	}
	// key >= loc.StartKey
	if compareKeyRangeBoundary(key, loc.StartKey, true, true) < 0 {
		return false
	}
	// key < loc.EndKey (empty EndKey means +inf)
	if len(loc.EndKey) == 0 {
		return true
	}
	return bytes.Compare(key, loc.EndKey) < 0
}

// locCoversEndKey checks if loc covers the end key (end key can equal loc.EndKey).
// The end key must be > loc.StartKey and <= loc.EndKey.
func locCoversEndKey(loc *tikv.KeyLocation, endKey []byte) bool {
	if loc == nil {
		return false
	}
	if len(endKey) > 0 && len(loc.StartKey) > 0 {
		if bytes.Compare(endKey, loc.StartKey) <= 0 {
			return false
		}
	}
	// Empty endKey means +inf, needs loc.EndKey to also be +inf
	if len(endKey) == 0 {
		return len(loc.EndKey) == 0
	}
	// endKey <= loc.EndKey (empty loc.EndKey means +inf, so always true)
	if len(loc.EndKey) == 0 {
		return true
	}
	return bytes.Compare(endKey, loc.EndKey) <= 0
}

// checkLocationsOrdered verifies that locations are ordered by StartKey and non-overlapping.
// Returns true if valid.
func checkLocationsOrdered(ctx context.Context, locs []*tikv.KeyLocation) bool {
	valid := true
	for i, loc := range locs {
		if loc == nil {
			logutil.Logger(ctx).Warn("[validateLocationCoverage] nil location",
				zap.Int("index", i))
			valid = false
			continue
		}
		if i == 0 {
			continue
		}
		prev := locs[i-1]
		if prev == nil {
			continue // already logged
		}

		// Check ordering: prev.StartKey < curr.StartKey
		if compareKeyRangeBoundary(prev.StartKey, loc.StartKey, true, true) >= 0 {
			logutil.Logger(ctx).Warn("[validateLocationCoverage] locations not ordered",
				zap.Int("index", i),
				zap.Uint64("prevRegionID", prev.Region.GetID()),
				zap.Uint64("currRegionID", loc.Region.GetID()),
				keyField("prevStart", prev.StartKey),
				keyField("currStart", loc.StartKey))
			valid = false
		}

		// Check non-overlapping: prev.EndKey <= curr.StartKey
		if compareKeyRangeBoundary(prev.EndKey, loc.StartKey, false, true) > 0 {
			logutil.Logger(ctx).Warn("[validateLocationCoverage] locations overlap",
				zap.Int("index", i),
				zap.Uint64("prevRegionID", prev.Region.GetID()),
				zap.Uint64("currRegionID", loc.Region.GetID()),
				keyField("prevEnd", prev.EndKey),
				keyField("currStart", loc.StartKey))
			valid = false
		}
	}
	return valid
}

// checkRangesCovered verifies that all ranges are covered by the locations.
// Returns (locUsed, valid) where locUsed tracks which locations were used.
func checkRangesCovered(ctx context.Context, kvRanges []tikv.KeyRange, locs []*tikv.KeyLocation) ([]bool, bool) {
	locUsed := make([]bool, len(locs))
	valid := true
	locIdx := 0

	for rangeIdx, r := range kvRanges {
		// Advance to the first location that might contain r.StartKey
		for locIdx < len(locs) {
			loc := locs[locIdx]
			if loc == nil {
				locIdx++
				continue
			}
			// If loc.EndKey <= r.StartKey, this loc is entirely before the range
			if len(loc.EndKey) > 0 && compareKeyRangeBoundary(loc.EndKey, r.StartKey, false, true) <= 0 {
				locIdx++
				continue
			}
			break
		}

		// Check if current location covers the range start
		if locIdx >= len(locs) || !locContainsStartKey(locs[locIdx], r.StartKey) {
			logutil.Logger(ctx).Warn("[validateLocationCoverage] range start not covered",
				zap.Int("rangeIndex", rangeIdx),
				keyField("rangeStart", r.StartKey),
				keyField("rangeEnd", r.EndKey),
				zap.Int("locIndex", locIdx),
				zap.Int("totalLocs", len(locs)))
			if locIdx < len(locs) && locs[locIdx] != nil {
				logutil.Logger(ctx).Warn("[validateLocationCoverage] nearest location",
					zap.Uint64("regionID", locs[locIdx].Region.GetID()),
					keyField("locStart", locs[locIdx].StartKey),
					keyField("locEnd", locs[locIdx].EndKey))
			}
			valid = false
			continue
		}

		// Walk through locations until range end is covered
		coverIdx := locIdx
		locUsed[coverIdx] = true
		for !locCoversEndKey(locs[coverIdx], r.EndKey) {
			prevEnd := locs[coverIdx].EndKey
			nextIdx := coverIdx + 1

			// Skip nil locations
			for nextIdx < len(locs) && locs[nextIdx] == nil {
				nextIdx++
			}

			if nextIdx >= len(locs) {
				logutil.Logger(ctx).Warn("[validateLocationCoverage] range end not covered (ran out of locations)",
					zap.Int("rangeIndex", rangeIdx),
					keyField("rangeStart", r.StartKey),
					keyField("rangeEnd", r.EndKey),
					zap.Int("lastLocIndex", coverIdx),
					zap.Uint64("lastLocRegionID", locs[coverIdx].Region.GetID()),
					keyField("lastLocEnd", locs[coverIdx].EndKey))
				valid = false
				break
			}

			// Check for gap: prevEnd must equal next.StartKey
			if !bytes.Equal(prevEnd, locs[nextIdx].StartKey) {
				logutil.Logger(ctx).Warn("[validateLocationCoverage] gap between locations",
					zap.Int("rangeIndex", rangeIdx),
					keyField("rangeStart", r.StartKey),
					keyField("rangeEnd", r.EndKey),
					zap.Int("prevLocIndex", coverIdx),
					zap.Uint64("prevLocRegionID", locs[coverIdx].Region.GetID()),
					keyField("prevLocEnd", prevEnd),
					zap.Int("nextLocIndex", nextIdx),
					zap.Uint64("nextLocRegionID", locs[nextIdx].Region.GetID()),
					keyField("nextLocStart", locs[nextIdx].StartKey))
				valid = false
				break
			}

			coverIdx = nextIdx
			locUsed[coverIdx] = true
		}
	}

	return locUsed, valid
}

// checkNoUnusedLocations verifies that all locations cover at least one range.
// Returns true if valid.
func checkNoUnusedLocations(ctx context.Context, locs []*tikv.KeyLocation, locUsed []bool) bool {
	valid := true
	for i, used := range locUsed {
		if !used && locs[i] != nil {
			logutil.Logger(ctx).Warn("[validateLocationCoverage] location does not cover any range",
				zap.Int("locIndex", i),
				zap.Uint64("regionID", locs[i].Region.GetID()),
				keyField("locStart", locs[i].StartKey),
				keyField("locEnd", locs[i].EndKey))
			valid = false
		}
	}
	return valid
}

// dumpValidationState logs the full state of ranges and locations for debugging.
func dumpValidationState(ctx context.Context, kvRanges []tikv.KeyRange, locs []*tikv.KeyLocation, locUsed []bool) {
	logutil.Logger(ctx).Warn("[validateLocationCoverage] validation failed - dumping full state",
		zap.Int("rangeCount", len(kvRanges)),
		zap.Int("locationCount", len(locs)))

	for i, r := range kvRanges {
		logutil.Logger(ctx).Warn("[validateLocationCoverage] range",
			zap.Int("index", i),
			keyField("start", r.StartKey),
			keyField("end", r.EndKey))
	}

	for i, loc := range locs {
		if loc == nil {
			logutil.Logger(ctx).Warn("[validateLocationCoverage] location (nil)",
				zap.Int("index", i))
		} else {
			used := false
			if i < len(locUsed) {
				used = locUsed[i]
			}
			logutil.Logger(ctx).Warn("[validateLocationCoverage] location",
				zap.Int("index", i),
				zap.Uint64("regionID", loc.Region.GetID()),
				zap.Uint64("regionVer", loc.Region.GetVer()),
				keyField("start", loc.StartKey),
				keyField("end", loc.EndKey),
				zap.Bool("used", used))
		}
	}
}

// validateLocationCoverage checks three properties:
//  1. Locations are ordered and non-overlapping
//  2. The union of ranges is covered by the union of locations
//  3. All locations cover some range (no extraneous locations)
//
// Returns true if all properties hold. Logs detailed diagnostics on failure.
func validateLocationCoverage(ctx context.Context, kvRanges []tikv.KeyRange, locs []*tikv.KeyLocation) bool {
	if len(kvRanges) == 0 {
		return len(locs) == 0
	}
	if len(locs) == 0 {
		logutil.Logger(ctx).Warn("[validateLocationCoverage] no locations but ranges exist",
			zap.Int("rangeCount", len(kvRanges)))
		return false
	}

	valid := true

	// Property 1: Locations are ordered and non-overlapping
	if !checkLocationsOrdered(ctx, locs) {
		valid = false
	}

	// Property 2: Union of ranges is covered by union of locations
	// Also tracks which locations are used for Property 3
	locUsed, covered := checkRangesCovered(ctx, kvRanges, locs)
	if !covered {
		valid = false
	}

	// Property 3: All locations cover some range
	if !checkNoUnusedLocations(ctx, locs, locUsed) {
		valid = false
	}

	if !valid {
		dumpValidationState(ctx, kvRanges, locs, locUsed)
	}

	return valid
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
	processedRangeCount := 0
	var continueSplit bool
	var expectedNextStart []byte

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
			// Prepare comprehensive diagnostics
			beforeLocation := bytes.Compare(startKey, loc.StartKey) < 0
			afterLocation := len(loc.EndKey) > 0 && bytes.Compare(startKey, loc.EndKey) >= 0

			// Bucket structure info
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

			// Queue state - log remaining ranges to see if upstream already wrong
			queueSummary := formatRanges(ranges)

			// Check if this is a gap from previous bucket split
			// Only flag gap if we actually split a range in previous iteration
			var gapDetected bool
			if continueSplit && !bytes.Equal(startKey, expectedNextStart) {
				gapDetected = true
			}

			// PD metadata - needed to correlate with PD logs and prove/disprove PD bug
			regionVer := loc.Region.GetVer()
			regionConfVer := loc.Region.GetConfVer()

			// Log comprehensive diagnostics
			fields := []zap.Field{
				// Basic identification
				keyField("startKey", startKey),
				zap.Uint64("regionID", loc.Region.GetID()),
				zap.Bool("keyInRegion", loc.Contains(startKey)),

				// Direction diagnostics
				zap.Bool("beforeLocation", beforeLocation),
				zap.Bool("afterLocation", afterLocation),

				// Loop state - shows where we are in processing
				zap.Int("processedRangeCount", processedRangeCount),
				zap.Int("remainingRangeCount", ranges.Len()),
				queueSummary, // All remaining ranges, not just first

				// Gap detection from bucket slicing
				zap.Bool("gapDetected", gapDetected),
			}

			if gapDetected {
				fields = append(fields,
					keyField("expectedNextStart", expectedNextStart))
			}

			fields = append(fields,
				// Location boundaries
				keyField("locationStart", loc.StartKey),
				keyField("locationEnd", loc.EndKey),

				// PD metadata - to correlate with PD logs
				zap.Uint64("regionVer", regionVer),
				zap.Uint64("regionConfVer", regionConfVer),

				// Bucket information
				zap.Int("bucketCount", len(loc.Buckets.Keys)),
				zap.Uint64("bucketVersion", loc.GetBucketVersion()),
				zap.Strings("bucketKeys", bucketKeys),
			)

			logutil.Logger(ctx).Warn("LocateBucket returned nil - falling back to unsplit ranges", fields...)

			// Fallback: return original LocationKeyRanges without bucket splitting.
			// This is safer than panicking - the request will still work, just without
			// bucket-level parallelism. The root cause (stale bucket metadata or
			// incorrect location assignment) should be investigated separately.
			return []*LocationKeyRanges{l}
		}

		processedRangeCount++

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
			// We split a range - track expected next start
			continueSplit = true
			expectedNextStart = bucket.EndKey
		} else {
			// Range start is not in this bucket, move to next bucket
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{l.Location, taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
			continueSplit = false
		}
	}
	return res
}

func (c *RegionCache) splitKeyRangesByLocation(ctx context.Context, loc *tikv.KeyLocation, ranges *KeyRanges, res []*LocationKeyRanges) ([]*LocationKeyRanges, *KeyRanges, bool) {
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
		// Defensive check: Verify first range actually starts in this location
		// This should never fail if caller is correct, but catches bugs in our splitting logic
		if ranges.Len() > 0 && !loc.Contains(ranges.At(0).StartKey) {
			logutil.Logger(ctx).Error("splitKeyRangesByLocation: all ranges added but first StartKey outside location",
				zap.Uint64("regionID", loc.Region.GetID()),
				zap.Uint64("regionVer", loc.Region.GetVer()),
				zap.Uint64("regionConfVer", loc.Region.GetConfVer()),
				formatLocation(loc),
				keyField("rangeStart", ranges.At(0).StartKey),
				zap.Int("rangeCount", ranges.Len()))
			panic("splitKeyRangesByLocation: invariant violated - range StartKey outside location")
		}
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
		// Range doesn't belong to this location - normal when processing sequential locations
		// Add ranges that did belong, return the rest
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
			// Defensive check: Verify remaining ranges start in last location
			// This should never fail if locations cover ranges correctly, but catches bugs
			if ranges.Len() > 0 && !loc.Contains(ranges.At(0).StartKey) {
				logutil.Logger(ctx).Error("SplitKeyRangesByLocations: last location but ranges start outside",
					zap.Uint64("regionID", loc.Region.GetID()),
					zap.Uint64("regionVer", loc.Region.GetVer()),
					zap.Uint64("regionConfVer", loc.Region.GetConfVer()),
					formatLocation(loc),
					keyField("rangeStart", ranges.At(0).StartKey),
					zap.Int("rangeCount", ranges.Len()),
					zap.Int("locationIndex", nextLocIndex),
					zap.Int("totalLocations", len(locs)))
				panic("SplitKeyRangesByLocations: invariant violated - remaining ranges start outside last location")
			}
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
	// Convert to tikv.KeyRange for validateLocationCoverage
	kvRanges := make([]tikv.KeyRange, 0, ranges.Len())
	for i := range ranges.Len() {
		kvRanges = append(kvRanges, tikv.KeyRange{
			StartKey: ranges.At(i).StartKey,
			EndKey:   ranges.At(i).EndKey,
		})
	}

	locs, err := c.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit, false, true)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}

	ctx := bo.GetCtx()

	// Track the index of location being processed for better diagnostics on panic
	locIdx := 0

	// Defensive: if bucket split panics, query PD directly to compare with cached data
	// This adds zero overhead in the normal case, only runs on panic
	defer func() {
		if r := recover(); r != nil {
			// Panic occurred - query PD for all regions to compare with cache
			logutil.Logger(ctx).Error("Panic during bucket splitting - querying PD for fresh region data",
				zap.Any("panicValue", r),
				zap.Int("panicLocationIndex", locIdx),
				zap.Int("cachedLocationCount", len(locs)))

			// Query PD directly for each region to see current state
			// Also extract tikv.KeyLocations for coverage validation
			tikvLocs := make([]*tikv.KeyLocation, 0, len(locs))
			for i, loc := range locs {
				tikvLocs = append(tikvLocs, loc.Location)

				logutil.Logger(ctx).Warn("Cached region info",
					zap.Int("index", i),
					zap.Uint64("regionID", loc.Location.Region.GetID()),
					zap.Uint64("regionVer", loc.Location.Region.GetVer()),
					zap.Uint64("regionConfVer", loc.Location.Region.GetConfVer()),
					formatLocation(loc.Location))

				// Query PD for current state
				pdLoc, err := c.RegionCache.LocateRegionByIDFromPD(bo.TiKVBackoffer(), loc.Location.Region.GetID())
				if err != nil {
					logutil.Logger(ctx).Warn("Failed to query PD for region",
						zap.Uint64("regionID", loc.Location.Region.GetID()),
						zap.Error(err))
					continue
				}

				logutil.Logger(ctx).Warn("PD region info",
					zap.Int("index", i),
					zap.Uint64("regionID", pdLoc.Region.GetID()),
					zap.Uint64("regionVer", pdLoc.Region.GetVer()),
					zap.Uint64("regionConfVer", pdLoc.Region.GetConfVer()),
					formatLocation(pdLoc),
					zap.Bool("versionChanged", loc.Location.Region.GetVer() != pdLoc.Region.GetVer()),
					zap.Bool("boundaryChanged", !bytes.Equal(loc.Location.StartKey, pdLoc.StartKey) || !bytes.Equal(loc.Location.EndKey, pdLoc.EndKey)))
			}

			// Validate if locations cover ranges
			valid := validateLocationCoverage(ctx, kvRanges, tikvLocs)
			logutil.Logger(ctx).Warn("Location coverage validation result",
				zap.Bool("valid", valid))

			// Re-panic with original error
			panic(r)
		}
	}()

	res := make([]*LocationKeyRanges, 0, len(locs))
	for ; locIdx < len(locs); locIdx++ {
		failpoint.Inject("panicInSplitKeyRangesByBuckets", func(val failpoint.Value) {
			if val.(int) == locIdx {
				panic("failpoint triggered panic in bucket splitting")
			}
		})
		res = append(res, locs[locIdx].splitKeyRangesByBuckets(ctx)...)
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

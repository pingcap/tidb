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
	"fmt"
	"strconv"
	"strings"

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

type bucketSplitFallbackInfo struct {
	reason   string
	startKey []byte
	endKey   []byte

	bucketStart []byte
	bucketEnd   []byte

	remainingRangeCount int
}

const (
	locationSummaryMaxDisplay = 5
)

// Helper functions for logging
func formatKeyLocation(name string, loc *tikv.KeyLocation) zap.Field {
	return zap.Object(name, zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		if loc == nil {
			return nil
		}
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

func formatLocation(loc *tikv.KeyLocation) zap.Field {
	return formatKeyLocation("location", loc)
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

func formatLocationCoverageSummary(name string, locs []*tikv.KeyLocation, focusIndex int) zap.Field {
	n := len(locs)
	if n == 0 {
		return zap.String(name, "count=0")
	}

	var gaps, overlaps, contiguous int
	for i := 1; i < n; i++ {
		prev, curr := locs[i-1], locs[i]
		if prev == nil || curr == nil {
			continue
		}
		switch rel := compareKeyRangeBoundary(prev.EndKey, curr.StartKey, false, true); {
		case rel < 0:
			gaps++
		case rel > 0:
			overlaps++
		default:
			contiguous++
		}
	}

	// Show a window of locations around focusIndex (region IDs + adjacency relation).
	focusIndex = max(0, min(focusIndex, n-1))
	windowStart := max(0, focusIndex-locationSummaryMaxDisplay/2)
	windowEnd := min(n, windowStart+locationSummaryMaxDisplay)

	var buf strings.Builder
	fmt.Fprintf(&buf, "count=%d gaps=%d overlaps=%d contiguous=%d focus=%d locs=[",
		n, gaps, overlaps, contiguous, focusIndex)
	for i := windowStart; i < windowEnd; i++ {
		if i > windowStart {
			buf.WriteString(", ")
		}
		loc := locs[i]
		if loc == nil {
			fmt.Fprintf(&buf, "%d:nil", i)
			continue
		}
		fmt.Fprintf(&buf, "%d:r%d", i, loc.Region.GetID())
		if i > 0 && locs[i-1] != nil {
			switch rel := compareKeyRangeBoundary(locs[i-1].EndKey, loc.StartKey, false, true); {
			case rel < 0:
				buf.WriteString("(gap)")
			case rel > 0:
				buf.WriteString("(overlap)")
			}
		}
	}
	if windowEnd < n {
		fmt.Fprintf(&buf, ", ...+%d", n-windowEnd)
	}
	buf.WriteByte(']')
	return zap.String(name, buf.String())
}

func rangeIssuesForTiKVKeyRanges(kvRanges []tikv.KeyRange) rangeIssueStats {
	var stats rangeIssueStats
	if len(kvRanges) == 0 {
		return stats
	}
	validateRange := func(r kv.KeyRange) {
		if len(r.EndKey) > 0 && bytes.Compare(r.StartKey, r.EndKey) > 0 {
			stats.add(rangeIssueInvalidBound)
		}
	}
	prev := kv.KeyRange{StartKey: kvRanges[0].StartKey, EndKey: kvRanges[0].EndKey}
	validateRange(prev)
	for i := 1; i < len(kvRanges); i++ {
		curr := kv.KeyRange{StartKey: kvRanges[i].StartKey, EndKey: kvRanges[i].EndKey}
		validateRange(curr)
		switch {
		case len(prev.EndKey) == 0:
			stats.add(rangeIssueInfiniteTail)
		case bytes.Compare(prev.EndKey, curr.StartKey) > 0:
			stats.add(classifyRangePair(prev, curr))
		}
		prev = curr
	}
	return stats
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
				logutil.Logger(ctx).Warn("[validateLocationCoverage] next location",
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
func (l *LocationKeyRanges) splitKeyRangesByBuckets(ctx context.Context) ([]*LocationKeyRanges, *bucketSplitFallbackInfo) {
	if l.Location.Buckets == nil || len(l.Location.Buckets.Keys) == 0 {
		return []*LocationKeyRanges{l}, nil
	}

	ranges := l.Ranges
	loc := l.Location
	res := []*LocationKeyRanges{}

	for ranges.Len() > 0 {
		startKey := ranges.At(0).StartKey

		// Input consistency guard: Bucket splitting assumes the first range starts inside this location.
		// If it doesn't, continuing can livelock (no progress) and/or over-split incorrectly.
		if !loc.Contains(startKey) {
			r := ranges.At(0)
			return []*LocationKeyRanges{l}, &bucketSplitFallbackInfo{
				reason:              "range_start_outside_location",
				startKey:            startKey,
				endKey:              r.EndKey,
				remainingRangeCount: ranges.Len(),
			}
		}

		bucket := loc.LocateBucket(startKey)
		// Defensive: LocateBucket should never return nil because startKey is inside location.
		// If it does, fall back to region-only splitting.
		if bucket == nil {
			r := ranges.At(0)
			return []*LocationKeyRanges{l}, &bucketSplitFallbackInfo{
				reason:              "locate_bucket_nil",
				startKey:            startKey,
				endKey:              r.EndKey,
				remainingRangeCount: ranges.Len(),
			}
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
			// Range start is not in this bucket. This indicates stale bucket metadata or
			// location/range mismatch. If i==0, slicing would not make progress and can
			// livelock (allocating forever). Fall back to unsplit ranges.
			if i == 0 {
				return []*LocationKeyRanges{l}, &bucketSplitFallbackInfo{
					reason:              "bucket_not_contain_start_no_progress",
					startKey:            r.StartKey,
					endKey:              r.EndKey,
					bucketStart:         bucket.StartKey,
					bucketEnd:           bucket.EndKey,
					remainingRangeCount: ranges.Len(),
				}
			}
			// Move to next bucket.
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{l.Location, taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
	}
	return res, nil
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

// maxReLocateOnOverflow limits how many times SplitKeyRangesByLocations will
// re-locate overflow ranges that extend beyond the pre-fetched locations.
// This prevents infinite loops if LocateKey consistently returns locations
// that don't cover the remaining ranges.
const maxReLocateOnOverflow = 64

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
	reLocateCount := 0
	for ranges.Len() > 0 {
		if limit != UnspecifiedLimit && len(res) >= limit {
			break
		}

		if nextLocIndex >= len(locs) {
			// Pre-fetched locations exhausted but ranges remain.
			// This happens when locations don't fully cover the ranges (e.g., last-loc
			// boundary is narrower than expected, or upstream metadata anomalies).
			// Re-locate the remaining ranges via LocateKey instead of returning an error.
			if reLocateCount >= maxReLocateOnOverflow {
				logutil.Logger(ctx).Error("SplitKeyRangesByLocations: re-locate overflow budget exhausted",
					zap.Int("reLocateCount", reLocateCount),
					zap.Int("maxReLocateOnOverflow", maxReLocateOnOverflow),
					zap.Int("locationCount", len(locs)),
					zap.Int("remainingRangeCount", ranges.Len()),
					zap.Any("remainingRangeIssues", rangeIssuesForKeyRanges(ranges)),
					formatLocationCoverageSummary("locationCoverageSummary", locs, len(locs)-1),
					formatRanges(ranges),
					zap.Stack("stack"))
				err = errors.Errorf("SplitKeyRangesByLocations: re-locate overflow budget exhausted after %d attempts, %d ranges remaining",
					reLocateCount, ranges.Len())
				return nil, err
			}
			startKey := ranges.At(0).StartKey
			newLoc, locErr := c.LocateKey(bo.TiKVBackoffer(), startKey)
			if locErr != nil {
				return nil, derr.ToTiDBErr(locErr)
			}
			if reLocateCount == 0 {
				logutil.Logger(ctx).Warn("SplitKeyRangesByLocations: re-locating overflow ranges beyond pre-fetched locations",
					zap.Int("locationCount", len(locs)),
					zap.Int("remainingRangeCount", ranges.Len()),
					zap.Uint64("newRegionID", newLoc.Region.GetID()),
					keyField("newLocStart", newLoc.StartKey),
					keyField("newLocEnd", newLoc.EndKey),
					formatLocationCoverageSummary("locationCoverageSummary", locs, len(locs)-1))
			}
			locs = append(locs, newLoc)
			reLocateCount++
			// Don't increment nextLocIndex; it now points to the newly appended location.
			continue
		}

		loc := locs[nextLocIndex]
		nextLocIndex++

		isBreak := false
		res, ranges, isBreak = c.splitKeyRangesByLocation(ctx, loc, ranges, res)
		if isBreak && ranges.Len() > 0 && !loc.Contains(ranges.At(0).StartKey) {
			// This indicates an internal inconsistency between returned locations and remaining ranges.
			// Do not panic in production; report it as an error so upper layers can retry or surface it.
			logutil.Logger(ctx).Error("SplitKeyRangesByLocations: break early but remaining ranges start outside location",
				zap.Uint64("regionID", loc.Region.GetID()),
				zap.Uint64("regionVer", loc.Region.GetVer()),
				zap.Uint64("regionConfVer", loc.Region.GetConfVer()),
				formatLocation(loc),
				keyField("rangeStart", ranges.At(0).StartKey),
				zap.Int("rangeCount", ranges.Len()),
				zap.Int("locationIndex", nextLocIndex-1),
				zap.Int("totalLocations", len(locs)),
				zap.Any("rangeIssues", rangeIssuesForKeyRanges(ranges)),
				formatLocationCoverageSummary("locationCoverageSummary", locs, nextLocIndex-1),
				zap.Stack("stack"))
			return nil, errors.Errorf("SplitKeyRangesByLocations: remaining ranges start outside location")
		}
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
	var fallback *bucketSplitFallbackInfo
	for ; locIdx < len(locs); locIdx++ {
		failpoint.Inject("panicInSplitKeyRangesByBuckets", func(val failpoint.Value) {
			if val.(int) == locIdx {
				panic("failpoint triggered panic in bucket splitting")
			}
		})
		var r []*LocationKeyRanges
		r, fallback = locs[locIdx].splitKeyRangesByBuckets(ctx)
		if fallback != nil {
			break
		}
		res = append(res, r...)
	}
	if fallback != nil {
		// Buckets are a performance optimization. If bucket splitting encounters inconsistent
		// metadata (or would make no progress), fall back to region-only splitting to maximize
		// correctness and stability.
		tikvLocs := make([]*tikv.KeyLocation, 0, len(locs))
		for _, l := range locs {
			tikvLocs = append(tikvLocs, l.Location)
		}
		coverageValid := validateLocationCoverage(ctx, kvRanges, tikvLocs)
		rangeIssues := rangeIssuesForTiKVKeyRanges(kvRanges)
		// Always log fallback for production debugging. Buckets are an optimization; falling back indicates either
		// stale bucket metadata, range/location mismatch, or other unexpected inputs.
		cachedLoc := locs[locIdx].Location
		cachedLocRanges := locs[locIdx].Ranges
		badIdx, badRange, badReason := firstOutOfBoundKeyRangeInLocation(cachedLocRanges, cachedLoc.StartKey, cachedLoc.EndKey)
		locationHasBuckets := cachedLoc.Buckets != nil && len(cachedLoc.Buckets.Keys) > 0
		bucketKeyCount := 0
		if cachedLoc.Buckets != nil {
			bucketKeyCount = len(cachedLoc.Buckets.Keys)
		}
		locationRangeIssues := rangeIssuesForKeyRanges(cachedLocRanges)
		fields := []zap.Field{
			zap.String("reason", fallback.reason),
			zap.Int("locationIndex", locIdx),
			zap.Int("locationCount", len(locs)),
			zap.Int("rangeCount", len(kvRanges)),
			zap.Bool("coverageValid", coverageValid),
			zap.Any("rangeIssues", rangeIssues),
			zap.Int("locationRangeCount", cachedLocRanges.Len()),
			zap.Any("locationRangeIssues", locationRangeIssues),
			zap.Bool("locationHasBuckets", locationHasBuckets),
			zap.Uint64("locationBucketsVer", cachedLoc.GetBucketVersion()),
			zap.Int("locationBucketKeyCount", bucketKeyCount),
			keyField("fallbackRangeStartKey", fallback.startKey),
			keyField("fallbackRangeEndKey", fallback.endKey),
			zap.Uint64("regionID", cachedLoc.Region.GetID()),
			zap.Uint64("regionVer", cachedLoc.Region.GetVer()),
			zap.Uint64("regionConfVer", cachedLoc.Region.GetConfVer()),
			keyField("locationStart", cachedLoc.StartKey),
			keyField("locationEnd", cachedLoc.EndKey),
			zap.Int("remainingRangeCount", fallback.remainingRangeCount),
			formatLocationCoverageSummary("locationCoverageSummary", tikvLocs, locIdx),
		}
		if badIdx >= 0 {
			fields = append(fields,
				zap.Int("outOfBoundRangeIndex", badIdx),
				zap.String("outOfBoundReason", badReason),
				keyField("outOfBoundRangeStartKey", badRange.StartKey),
				keyField("outOfBoundRangeEndKey", badRange.EndKey),
			)
		}
		if len(fallback.startKey) > 0 {
			cacheLoc := c.RegionCache.TryLocateKey(fallback.startKey)
			if cacheLoc == nil {
				fields = append(fields,
					zap.Bool("cacheLocateByFallbackStartMissing", true),
				)
			} else {
				fields = append(fields,
					formatKeyLocation("cacheLocateByFallbackStart", cacheLoc),
				)
			}
		}
		// Best-effort: query PD directly for region boundary comparison.
		pdLoc, pdErr := c.RegionCache.LocateRegionByIDFromPD(bo.TiKVBackoffer(), cachedLoc.Region.GetID())
		if pdErr != nil {
			fields = append(fields, zap.Error(pdErr))
		} else {
			fields = append(fields,
				zap.Uint64("pdRegionVer", pdLoc.Region.GetVer()),
				zap.Uint64("pdRegionConfVer", pdLoc.Region.GetConfVer()),
				keyField("pdRegionStartKey", pdLoc.StartKey),
				keyField("pdRegionEndKey", pdLoc.EndKey),
				zap.Bool("pdEpochChanged", pdLoc.Region.GetVer() != cachedLoc.Region.GetVer() || pdLoc.Region.GetConfVer() != cachedLoc.Region.GetConfVer()),
				zap.Bool("pdBoundaryChanged", !bytes.Equal(pdLoc.StartKey, cachedLoc.StartKey) || !bytes.Equal(pdLoc.EndKey, cachedLoc.EndKey)),
			)
			if pdLoc.Buckets != nil {
				fields = append(fields,
					zap.Uint64("pdBucketsVer", pdLoc.GetBucketVersion()),
					zap.Int("pdBucketKeyCount", len(pdLoc.Buckets.Keys)),
				)
			} else {
				fields = append(fields, zap.Bool("pdBucketsNil", true))
			}
		}
		if fallback.bucketStart != nil || fallback.bucketEnd != nil {
			fields = append(fields,
				keyField("fallbackBucketStartKey", fallback.bucketStart),
				keyField("fallbackBucketEndKey", fallback.bucketEnd),
			)
		}
		fields = append(fields,
			formatLocation(cachedLoc),
			formatRanges(cachedLocRanges),
			zap.Stack("stack"),
		)
		logutil.Logger(ctx).Warn("SplitKeyRangesByBuckets fell back to region-only splitting", fields...)

		locs, err := c.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit, false, false)
		if err != nil {
			return nil, derr.ToTiDBErr(err)
		}
		return locs, nil
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

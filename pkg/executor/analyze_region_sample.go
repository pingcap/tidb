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

package executor

import (
	"context"
	"math/big"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// loadTableRecordRegionRanges returns one key range per region that overlaps the
// record (row) data of the given physical table id. It mirrors the region
// enumeration used by TABLESAMPLE (see splitIntoMultiRanges in sample.go): each
// region's [StartKey, EndKey) is clamped to the table's record prefix range.
func loadTableRecordRegionRanges(ctx context.Context, store kv.Storage, physicalID int64) ([]kv.KeyRange, error) {
	start := tablecodec.GenTableRecordPrefix(physicalID)
	end := start.PrefixNext()
	s, ok := store.(tikv.Storage)
	if !ok {
		// Non-TiKV stores (e.g. mock) have no regions; treat the table as one range.
		return []kv.KeyRange{{StartKey: start, EndKey: end}}, nil
	}
	bo := tikv.NewBackofferWithVars(ctx, 10000, nil)
	regions, err := s.GetRegionCache().LoadRegionsInKeyRange(bo, start, end)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ranges := make([]kv.KeyRange, 0, len(regions))
	for _, r := range regions {
		rs, re := kv.Key(r.StartKey()), kv.Key(r.EndKey())
		if rs.Cmp(start) < 0 {
			rs = start
		}
		if len(re) == 0 || re.Cmp(end) > 0 {
			re = end
		}
		ranges = append(ranges, kv.KeyRange{StartKey: rs, EndKey: re})
	}
	return ranges, nil
}

// tikvStoreCount returns the number of TiKV stores known to the region cache, or
// 1 when the store is not TiKV or none are cached. It is the divisor for the
// region-sampling target (see sampleRegionsByStoreShare).
func tikvStoreCount(store kv.Storage) int {
	s, ok := store.(tikv.Storage)
	if !ok {
		return 1
	}
	n := len(s.GetRegionCache().GetStoresByType(tikvrpc.TiKV))
	if n < 1 {
		return 1
	}
	return n
}

// sampleRegionsByStoreShare returns a uniform-random subset of regions of size
// ceil(total/numStores) — roughly the regions a single store holds — so the work
// scales with the cluster's store count rather than with table size. The caller
// extrapolates per-region aggregates back to the whole table using picked/total
// (see scaleAggregatesForRegionSample).
//
// Selection is an O(n) partial Fisher-Yates: each of the n result slots is drawn
// from the not-yet-chosen suffix. It reorders `regions` in place; the caller does
// not reuse it afterward.
func sampleRegionsByStoreShare(regions []kv.KeyRange, numStores int, r *rand.Rand) []kv.KeyRange {
	total := len(regions)
	if total == 0 {
		return nil
	}
	if numStores < 1 {
		numStores = 1
	}
	// ceil(total / numStores), always at least 1.
	n := (total + numStores - 1) / numStores
	if n >= total {
		return regions
	}
	picked := make([]kv.KeyRange, n)
	for i := range n {
		j := i + r.Intn(total-i)
		regions[i], regions[j] = regions[j], regions[i]
		picked[i] = regions[i]
	}
	return picked
}

// setupRegionSampling enumerates the table's regions and, when there is more than
// one, picks a uniform-random subset (sampleRegionsByStoreShare). The picked region
// key ranges become the column scan's ranges, and the picked/total counts drive the
// later extrapolation (scaleAggregatesForRegionSample). It takes effect only when an
// actual subset is chosen: a single region, a non-TiKV store, or numStores==1 leaves
// the executor's full-table scan untouched.
func (e *AnalyzeColumnsExec) setupRegionSampling(ctx context.Context) error {
	store := e.ctx.GetStore()
	regions, err := loadTableRecordRegionRanges(ctx, store, e.TableID.GetStatisticsID())
	if err != nil {
		return err
	}
	if len(regions) <= 1 {
		return nil
	}
	numStores := tikvStoreCount(store)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	picked := sampleRegionsByStoreShare(regions, numStores, r)
	// ndvrate sets how much of each picked region to scan: replace each region's
	// full key range with a random sub-range covering that fraction of its keys.
	keyRangeFraction := e.analyzePB.ColReq.GetNdvRate()
	if keyRangeFraction <= 0 || keyRangeFraction > 1 {
		keyRangeFraction = 1
	}
	for i := range picked {
		picked[i] = randomSubKeyRange(picked[i].StartKey, picked[i].EndKey, keyRangeFraction, r)
	}
	// The scan visits these ranges in order; sort so it proceeds in key order, which
	// the per-column correlation estimate (the request uses KeepOrder) relies on.
	sortRanges(picked, false)
	e.sampledKeyRanges = picked
	e.regionSampleTotal = len(regions)
	e.regionSamplePicked = len(picked)
	e.regionSampleKeyRangeFraction = keyRangeFraction
	return nil
}

// randomSubKeyRange returns a contiguous sub-range of [start, end) covering about
// `fraction` of the key interval at a random offset. Keys are interpreted as
// big-endian numbers (right-padded to equal length) to interpolate, and the result
// is clamped back into [start, end). A fraction >= 1, a non-positive fraction, or a
// degenerate interval returns the full range. NOTE: a fraction of the key space
// equals the row fraction only under ~uniform key density.
func randomSubKeyRange(start, end kv.Key, fraction float64, r *rand.Rand) kv.KeyRange {
	full := kv.KeyRange{StartKey: start, EndKey: end}
	if fraction <= 0 || fraction >= 1 || len(end) == 0 {
		return full
	}
	width := max(len(start), len(end))
	lo := keyToPaddedBigInt(start, width)
	hi := keyToPaddedBigInt(end, width)
	interval := new(big.Int).Sub(hi, lo)
	if interval.Sign() <= 0 {
		return full
	}
	span := scaleBigIntByFraction(interval, fraction)
	if span.Sign() <= 0 {
		return full
	}
	// Random start offset in [0, interval-span].
	room := new(big.Int).Sub(interval, span)
	room.Add(room, big.NewInt(1))
	a := new(big.Int).Add(lo, new(big.Int).Rand(r, room))
	b := new(big.Int).Add(a, span)
	aKey := kv.Key(bigIntToKey(a, width))
	bKey := kv.Key(bigIntToKey(b, width))
	if aKey.Cmp(start) < 0 {
		aKey = start
	}
	if bKey.Cmp(end) > 0 {
		bKey = end
	}
	if aKey.Cmp(bKey) >= 0 {
		return full
	}
	return kv.KeyRange{StartKey: aKey, EndKey: bKey}
}

// keyToPaddedBigInt interprets k, right-padded with zeros to width bytes, as a
// big-endian unsigned integer. Right-padding keeps lexicographic order equal to
// numeric order across the equal-length keys.
func keyToPaddedBigInt(k kv.Key, width int) *big.Int {
	buf := make([]byte, width)
	copy(buf, k)
	return new(big.Int).SetBytes(buf)
}

// bigIntToKey is the inverse of keyToPaddedBigInt: a width-byte big-endian key.
func bigIntToKey(v *big.Int, width int) []byte {
	b := v.Bytes()
	if len(b) >= width {
		return b
	}
	buf := make([]byte, width)
	copy(buf[width-len(b):], b)
	return buf
}

// scaleBigIntByFraction returns floor(v * fraction).
func scaleBigIntByFraction(v *big.Int, fraction float64) *big.Int {
	f := new(big.Float).SetInt(v)
	f.Mul(f, big.NewFloat(fraction))
	out, _ := f.Int(nil)
	return out
}

// scaleSampledValue extrapolates a sampled value by `scale` (>= 1), rounding half
// up. Non-positive values pass through (counts and sizes are non-negative).
func scaleSampledValue(value int64, scale float64) int64 {
	if value <= 0 || scale <= 1 {
		return value
	}
	return int64(float64(value)*scale + 0.5)
}

// scaleAggregatesForRegionSample extrapolates the merged region-sampled aggregates
// back to the whole table: the row count, per-column null counts, and per-column
// total value sizes are each scaled by total/(picked*keyRangeFraction) — the inverse
// of the effective row fraction, which is the region fraction (picked/total) times
// the within-region key-range fraction. The key-range part assumes ~uniform key
// density. The histogram is left untouched; the NDV is extrapolated by
// estimateSamplingNDV via GEE, using the scaled count as the population size N.
func scaleAggregatesForRegionSample(count *int64, nullCount, totalSizes []int64, total, picked int, keyRangeFraction float64) {
	if picked <= 0 || keyRangeFraction <= 0 {
		return
	}
	scale := float64(total) / (float64(picked) * keyRangeFraction)
	if scale <= 1 {
		return
	}
	*count = scaleSampledValue(*count, scale)
	for i := range nullCount {
		nullCount[i] = scaleSampledValue(nullCount[i], scale)
	}
	for i := range totalSizes {
		totalSizes[i] = scaleSampledValue(totalSizes[i], scale)
	}
}

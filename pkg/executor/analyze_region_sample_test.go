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
	"math/big"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func TestSampleRegionsByStoreShare(t *testing.T) {
	mkRegions := func(n int) []kv.KeyRange {
		rs := make([]kv.KeyRange, n)
		for i := range rs {
			rs[i] = kv.KeyRange{StartKey: kv.Key{byte(i)}, EndKey: kv.Key{byte(i + 1)}}
		}
		return rs
	}
	r := rand.New(rand.NewSource(1))

	t.Run("ceil division by store count", func(t *testing.T) {
		require.Len(t, sampleRegionsByStoreShare(mkRegions(100), 5, r), 20) // ceil(100/5)
		require.Len(t, sampleRegionsByStoreShare(mkRegions(7), 2, r), 4)    // ceil(7/2)
	})

	t.Run("picked regions are distinct and from the input", func(t *testing.T) {
		input := mkRegions(100)
		picked := sampleRegionsByStoreShare(input, 5, r)
		seen := make(map[byte]struct{}, len(picked))
		for _, p := range picked {
			_, dup := seen[p.StartKey[0]]
			require.False(t, dup, "region picked twice")
			seen[p.StartKey[0]] = struct{}{}
		}
	})

	t.Run("single store takes the whole table", func(t *testing.T) {
		require.Len(t, sampleRegionsByStoreShare(mkRegions(10), 1, r), 10)
	})

	t.Run("degenerate inputs", func(t *testing.T) {
		require.Len(t, sampleRegionsByStoreShare(mkRegions(4), 0, r), 4) // numStores<1 -> whole table
		require.Nil(t, sampleRegionsByStoreShare(nil, 3, r))
	})
}

func TestScaleAggregatesForRegionSample(t *testing.T) {
	t.Run("scales by total/(picked*keyRangeFraction)", func(t *testing.T) {
		// 20 of 100 regions, full key range (kf=1) -> 5x.
		count := int64(1000)
		nullCount := []int64{40, 0, 7}
		totalSizes := []int64{2000, 500, 13}
		scaleAggregatesForRegionSample(&count, nullCount, totalSizes, 100, 20, 1.0)
		require.Equal(t, int64(5000), count)
		require.Equal(t, []int64{200, 0, 35}, nullCount)
		require.Equal(t, []int64{10000, 2500, 65}, totalSizes)
	})

	t.Run("key-range fraction multiplies the scale", func(t *testing.T) {
		// 20/100 regions * 0.5 key-range = 0.1 effective -> 10x.
		count := int64(1000)
		scaleAggregatesForRegionSample(&count, nil, nil, 100, 20, 0.5)
		require.Equal(t, int64(10000), count)
	})

	t.Run("round half up", func(t *testing.T) {
		require.Equal(t, int64(12), scaleSampledValue(5, 7.0/3.0)) // 11.67 -> 12
		require.Equal(t, int64(2), scaleSampledValue(1, 7.0/3.0))  // 2.33 -> 2
	})

	t.Run("no-op when nothing to extrapolate", func(t *testing.T) {
		require.Equal(t, int64(42), scaleSampledValue(42, 1.0)) // scale 1
		require.Equal(t, int64(0), scaleSampledValue(0, 5.0))   // zero value
		count := int64(1000)
		scaleAggregatesForRegionSample(&count, nil, nil, 10, 10, 1.0) // scale 1
		require.Equal(t, int64(1000), count)
	})
}

func TestKeyRangeFractionForNDVRate(t *testing.T) {
	// With 5 stores the picked regions hold ~20% of the table, so 0.2 is the ceiling.
	const regionFraction = 0.2

	// ndvrate is the target fraction of the WHOLE table: the overall scanned fraction
	// (regionFraction × keyRangeFraction) must equal ndvrate, clamped to the ceiling.
	for _, ndvRate := range []float64{0.02, 0.1, 0.2, 0.5, 1.0} {
		overall := regionFraction * keyRangeFractionForNDVRate(ndvRate, regionFraction)
		require.InDelta(t, min(ndvRate, regionFraction), overall, 1e-9, "ndvRate=%v", ndvRate)
	}

	require.InDelta(t, 0.5, keyRangeFractionForNDVRate(0.1, regionFraction), 1e-9) // below ceiling: 0.1/0.2
	require.Equal(t, 1.0, keyRangeFractionForNDVRate(0.5, regionFraction))         // above ceiling: clamped
	require.Equal(t, 1.0, keyRangeFractionForNDVRate(0.5, 0))                      // degenerate regionFraction
}

func TestRandomSubKeyRange(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	start := kv.Key{0x10, 0x00} // 0x1000 = 4096
	end := kv.Key{0x20, 0x00}   // 0x2000 = 8192

	t.Run("sub-range stays within [start,end) and is non-empty", func(t *testing.T) {
		for range 100 {
			sub := randomSubKeyRange(start, end, 0.25, r)
			require.GreaterOrEqual(t, kv.Key(sub.StartKey).Cmp(start), 0)
			require.LessOrEqual(t, kv.Key(sub.EndKey).Cmp(end), 0)
			require.Less(t, kv.Key(sub.StartKey).Cmp(sub.EndKey), 0)
		}
	})

	t.Run("covers approximately the requested fraction", func(t *testing.T) {
		// interval = 4096; 0.25 -> width ~1024.
		sub := randomSubKeyRange(start, end, 0.25, r)
		w := new(big.Int).Sub(keyToPaddedBigInt(sub.EndKey, 2), keyToPaddedBigInt(sub.StartKey, 2))
		require.InDelta(t, 1024, float64(w.Int64()), 2)
	})

	t.Run("fraction>=1 or degenerate returns the full range", func(t *testing.T) {
		full := randomSubKeyRange(start, end, 1.0, r)
		require.Equal(t, start, kv.Key(full.StartKey))
		require.Equal(t, end, kv.Key(full.EndKey))
		// Empty interval.
		deg := randomSubKeyRange(start, start, 0.5, r)
		require.Equal(t, start, kv.Key(deg.StartKey))
		require.Equal(t, start, kv.Key(deg.EndKey))
	})
}

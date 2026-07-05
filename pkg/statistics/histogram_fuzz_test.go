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

package statistics

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

// FuzzMergePartTopNAndHistToGlobal exercises MergePartTopNAndHistToGlobal
// with randomized per-partition histograms and TopN entries. It checks
// the structural invariants any correct merge must preserve regardless
// of input shape, so the corpus seeds and any newly-discovered failing
// inputs are reusable across future algorithm changes.
//
// Generated input dimensions are: number of partitions, buckets per
// partition, TopN entries per partition, target global bucket count,
// target global TopN size, and whether partitions span overlapping or
// disjoint value ranges. Within a partition's range bucket bounds are
// sorted and non-overlapping; counts and repeats are positive.
//
// Verified output invariants:
//
//  1. Each global bucket has lower <= upper.
//  2. Consecutive global buckets are ordered: bucket[i].lower >= bucket[i-1].upper.
//  3. Global cumulative bucket counts are monotonically non-decreasing.
//  4. Total rows are preserved: histogram-final-cumulative + sum(global TopN counts) == sum of input partition rows.
//  5. Global bucket count <= expBuckets and global TopN size <= globalTopN.
//  6. Global TopN entries are sorted by encoded bytes (downstream binary search relies on this).
//  7. If a global bucket's upper bound encodes to a value present in the global TopN, the bucket's Repeat is zero (the TopN counter owns those rows).
//  8. Every global bucket has strictly positive mass (no ghost bucket emitted from a no-mass merged group).
//
// Input dimensions covered: int (TypeLong) and varchar columns,
// column histograms and index histograms (isIndex=true). Time / decimal
// types are not fuzzed today, those code paths in topNMetaToDatum
// have type-specific decoding but the merge's compare/cut logic does
// not branch on them.
func FuzzMergePartTopNAndHistToGlobal(f *testing.F) {
	// Seed corpus: explicit tests in histogram_test.go plus a couple
	// of larger shapes. Seeds always run as part of `go test`; ad-hoc
	// fuzzing happens under `go test -fuzz`. Last two ints are
	// tpKind (0=Long, 1=Varchar) and isIndex (0=column, 1=index).
	f.Add(int64(20150401), uint8(2), uint8(2), uint8(0), uint8(2), uint8(0), false, uint8(0), uint8(0)) // gap preservation
	f.Add(int64(20150401), uint8(1), uint8(6), uint8(0), uint8(2), uint8(0), false, uint8(0), uint8(0)) // bucket splitting
	f.Add(int64(20150401), uint8(3), uint8(1), uint8(0), uint8(3), uint8(0), false, uint8(0), uint8(0)) // prevUpperAfterEmit
	f.Add(int64(1), uint8(2), uint8(2), uint8(2), uint8(2), uint8(2), false, uint8(0), uint8(0))
	f.Add(int64(2), uint8(4), uint8(3), uint8(2), uint8(4), uint8(3), true, uint8(0), uint8(0))
	f.Add(int64(3), uint8(8), uint8(5), uint8(3), uint8(8), uint8(5), true, uint8(0), uint8(0))
	f.Add(int64(4), uint8(8), uint8(5), uint8(3), uint8(8), uint8(5), false, uint8(0), uint8(0))
	// Varchar column seeds.
	f.Add(int64(10), uint8(3), uint8(3), uint8(2), uint8(4), uint8(3), false, uint8(1), uint8(0))
	f.Add(int64(11), uint8(4), uint8(4), uint8(2), uint8(6), uint8(4), true, uint8(1), uint8(0))
	// Index seeds: int and varchar source values.
	f.Add(int64(20), uint8(3), uint8(3), uint8(2), uint8(4), uint8(3), false, uint8(0), uint8(1))
	f.Add(int64(21), uint8(4), uint8(4), uint8(2), uint8(6), uint8(4), true, uint8(1), uint8(1))

	f.Fuzz(func(t *testing.T, seed int64,
		numParts, bucketsPerPart, topNPerPart, expBuckets, globalTopN uint8,
		overlap bool,
		tpKind, isIndexFlag uint8,
	) {
		// Bound the input shape so each fuzz iteration is fast. These
		// limits are large enough to exercise the algorithm's branches
		// while keeping individual runs under a few milliseconds.
		switch {
		case numParts == 0 || numParts > 16,
			bucketsPerPart == 0 || bucketsPerPart > 10,
			topNPerPart > 5,
			expBuckets == 0 || expBuckets > 32,
			globalTopN > 16,
			tpKind > 1,
			isIndexFlag > 1:
			t.Skip("input dimensions out of range")
		}
		isIndex := isIndexFlag == 1

		sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
		killer := sqlkiller.SQLKiller{}
		tp := fuzzFieldType(tpKind, isIndex)
		rng := rand.New(rand.NewSource(seed))

		gen := newFuzzValueGen(tpKind, rng)

		var totalRows int64
		hists := make([]*Histogram, numParts)
		topNs := make([]*TopN, numParts)
		for p := uint8(0); p < numParts; p++ {
			// Pick 2*bucketsPerPart distinct datums within this
			// partition's band, sort by encoded bytes (the merge
			// orders by encoded form regardless of source type), and
			// pair them up as (lower, upper) per bucket.
			vals := make([]types.Datum, 0, 2*int(bucketsPerPart))
			seen := make(map[string]struct{}, 2*int(bucketsPerPart))
			attempts := 0
			for len(vals) < 2*int(bucketsPerPart) {
				attempts++
				if attempts > 1000 {
					t.Skip("could not find enough distinct bucket bounds")
				}
				v := gen.value(p, overlap)
				enc, err := codec.EncodeKey(sc.TimeZone(), nil, v)
				if err != nil {
					t.Skip("encode key failed")
				}
				key := string(enc)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				vals = append(vals, v)
			}
			sort.Slice(vals, func(i, j int) bool {
				ai, _ := codec.EncodeKey(sc.TimeZone(), nil, vals[i])
				aj, _ := codec.EncodeKey(sc.TimeZone(), nil, vals[j])
				return bytes.Compare(ai, aj) < 0
			})

			h := NewHistogram(1, 0, 0, 0, tp, chunk.InitialCapacity, 0)
			cumulative := int64(0)
			for b := 0; b < int(bucketsPerPart); b++ {
				lo, up := vals[2*b], vals[2*b+1]
				if isIndex {
					lo = boundAsBytes(t, sc, lo)
					up = boundAsBytes(t, sc, up)
				}
				cnt := int64(rng.Intn(20) + 1)
				cumulative += cnt
				h.AppendBucket(&lo, &up, cumulative, 1)
			}
			hists[p] = h
			totalRows += cumulative

			// TopN entries: pick values from the partition's range
			// (may or may not collide with bucket bounds).
			tn := NewTopN(int(topNPerPart))
			for k := uint8(0); k < topNPerPart; k++ {
				v := gen.value(p, overlap)
				key, err := codec.EncodeKey(sc.TimeZone(), nil, v)
				if err != nil {
					t.Skip("encode key failed")
				}
				cnt := uint64(rng.Intn(20) + 1)
				tn.AppendTopN(key, cnt)
				totalRows += int64(cnt)
			}
			topNs[p] = tn
		}

		gTopN, hist, err := MergePartTopNAndHistToGlobal(
			sc, &killer, topNs, hists, uint32(globalTopN), int64(expBuckets), isIndex,
		)
		require.NoError(t, err)
		if hist == nil {
			require.Equal(t, int64(0), totalRows, "nil result implies no input rows")
			return
		}

		// Inv 1: bucket bounds not inverted (type-agnostic compare).
		for i := 0; i < hist.Len(); i++ {
			lower, upper := hist.GetLower(i), hist.GetUpper(i)
			cmp, err := lower.Compare(sc.TypeCtx(), upper, collate.GetBinaryCollator())
			require.NoError(t, err)
			require.LessOrEqualf(t, cmp, 0, "bucket %d has inverted bounds", i)
		}
		// Inv 2 + 3: cumulative non-decreasing, no overlap with previous.
		for i := 1; i < hist.Len(); i++ {
			require.GreaterOrEqualf(t,
				hist.Buckets[i].Count, hist.Buckets[i-1].Count,
				"bucket %d cumulative regressed: %d < %d",
				i, hist.Buckets[i].Count, hist.Buckets[i-1].Count)
			prevUpper, curLower := hist.GetUpper(i-1), hist.GetLower(i)
			cmp, err := curLower.Compare(sc.TypeCtx(), prevUpper, collate.GetBinaryCollator())
			require.NoError(t, err)
			require.GreaterOrEqualf(t, cmp, 0,
				"bucket %d lower below previous upper (overlap)", i)
		}
		// Inv 4: total rows preserved across histogram + global TopN.
		var globalTopNSum int64
		if gTopN != nil {
			for _, m := range gTopN.TopN {
				globalTopNSum += int64(m.Count)
			}
		}
		var histFinal int64
		if hist.Len() > 0 {
			histFinal = hist.Buckets[hist.Len()-1].Count
		}
		require.Equalf(t, totalRows, histFinal+globalTopNSum,
			"total rows must be preserved: hist=%d + topN=%d != input=%d",
			histFinal, globalTopNSum, totalRows)
		// Inv 5: bucket count and TopN size respect their caps.
		require.LessOrEqualf(t, hist.Len(), int(expBuckets),
			"bucket count %d exceeds expBuckets=%d", hist.Len(), expBuckets)
		if gTopN != nil {
			require.LessOrEqualf(t, len(gTopN.TopN), int(globalTopN),
				"global TopN size %d exceeds globalTopN=%d", len(gTopN.TopN), globalTopN)
			// Inv 6: TopN sorted by encoded bytes.
			for i := 1; i < len(gTopN.TopN); i++ {
				require.LessOrEqualf(t,
					bytes.Compare(gTopN.TopN[i-1].Encoded, gTopN.TopN[i].Encoded), 0,
					"global TopN must be sorted by encoded; entries %d and %d out of order",
					i-1, i)
			}
			// Inv 7: bucket upper matching a global TopN value must
			// have Repeat = 0. For index histograms upper.GetBytes()
			// is already the encoded form; for column histograms
			// re-encode the typed upper.
			topNSet := make(map[string]struct{}, len(gTopN.TopN))
			for _, m := range gTopN.TopN {
				topNSet[string(m.Encoded)] = struct{}{}
			}
			for i := 0; i < hist.Len(); i++ {
				upper := hist.GetUpper(i)
				var encoded []byte
				if isIndex {
					encoded = upper.GetBytes()
				} else {
					encoded, err = codec.EncodeKey(sc.TimeZone(), nil, *upper)
					require.NoError(t, err)
				}
				if _, hit := topNSet[string(encoded)]; hit {
					require.Equalf(t, int64(0), hist.Buckets[i].Repeat,
						"bucket %d upper matches a global TopN value; Repeat must be 0, got %d",
						i, hist.Buckets[i].Repeat)
				}
			}
			// Inv 8: every global bucket has strictly positive mass.
			prev := int64(0)
			for i := 0; i < hist.Len(); i++ {
				mass := hist.Buckets[i].Count - prev
				require.Greaterf(t, mass, int64(0),
					"bucket %d has zero mass (ghost bucket)", i)
				prev = hist.Buckets[i].Count
			}
		}
	})
}

// fuzzFieldType picks the histogram field type for the given tpKind
// (0=Long, 1=Varchar). Index histograms always use TypeBlob.
func fuzzFieldType(tpKind uint8, isIndex bool) *types.FieldType {
	if isIndex {
		return types.NewFieldType(mysql.TypeBlob)
	}
	if tpKind == 1 {
		return types.NewFieldType(mysql.TypeVarchar)
	}
	return types.NewFieldType(mysql.TypeLong)
}

// boundAsBytes converts a typed datum into the Bytes-kind form an
// index histogram stores as a bucket bound (the encoded key).
func boundAsBytes(t *testing.T, sc *stmtctx.StatementContext, d types.Datum) types.Datum {
	enc, err := codec.EncodeKey(sc.TimeZone(), nil, d)
	require.NoError(t, err)
	var out types.Datum
	out.SetBytes(enc)
	return out
}

// fuzzValueGen produces random datums of the configured type. Each
// partition gets a band of values; with overlap=false the bands are
// disjoint, with overlap=true they share a common space.
type fuzzValueGen struct {
	rng    *rand.Rand
	tpKind uint8
}

func newFuzzValueGen(tpKind uint8, rng *rand.Rand) *fuzzValueGen {
	return &fuzzValueGen{rng: rng, tpKind: tpKind}
}

const fuzzIntraPartRange = 100

// value returns a random datum within the given partition's band.
func (g *fuzzValueGen) value(part uint8, overlap bool) types.Datum {
	switch g.tpKind {
	case 1:
		// Varchar: 4 lowercase ASCII letters. With overlap=false the
		// first character is set from a per-partition prefix to keep
		// bands lexicographically disjoint.
		buf := make([]byte, 4)
		for i := range buf {
			buf[i] = byte('a' + g.rng.Intn(26))
		}
		if !overlap {
			buf[0] = 'A' + part // 'A'..'P' for parts 0..15
		}
		return types.NewStringDatum(string(buf))
	default:
		base := int64(0)
		if !overlap {
			base = int64(part) * fuzzIntraPartRange * 4
		}
		return types.NewIntDatum(base + int64(g.rng.Intn(fuzzIntraPartRange)))
	}
}

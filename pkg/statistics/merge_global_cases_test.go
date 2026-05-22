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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

// mergeCases returns the table of declarative test cases for
// MergePartTopNAndHistToGlobal. Each case is one fixture; the driver
// in merge_global_test.go runs structural invariants on every case
// and additionally pins wantTopN / wantBuckets where they matter.
func mergeCases() []mergeCase {
	return []mergeCase{
		// ----------------------------------------------------------------
		// Range / gap layout
		// ----------------------------------------------------------------
		{
			// Two partitions with disjoint ranges. The 90-unit gap
			// between p0's [1,10] and p1's [100,110] must be kept as
			// a gap between two global buckets, not collapsed.
			name:       "disjoint_partition_ranges_keep_gap",
			expBuckets: 2,
			parts: []partSpec{
				{hist: []bucketSpec{
					{lo: di(1), up: di(5), mass: 3, repeat: 1},
					{lo: di(6), up: di(10), mass: 5, repeat: 1},
				}},
				{hist: []bucketSpec{
					{lo: di(100), up: di(105), mass: 3, repeat: 1},
					{lo: di(106), up: di(110), mass: 5, repeat: 1},
				}},
			},
			wantBuckets: []bucketSpec{
				{lo: di(1), up: di(10), mass: 8},
				{lo: di(100), up: di(110), mass: 8},
			},
		},
		{
			// One partition, six narrow buckets of 10 → two
			// equi-depth global buckets of 30 rows each.
			name:       "single_partition_collapses_to_equidepth",
			expBuckets: 2,
			parts: []partSpec{
				{hist: []bucketSpec{
					{lo: di(1), up: di(10), mass: 10, repeat: 1},
					{lo: di(11), up: di(20), mass: 10, repeat: 1},
					{lo: di(21), up: di(30), mass: 10, repeat: 1},
					{lo: di(31), up: di(40), mass: 10, repeat: 1},
					{lo: di(41), up: di(50), mass: 10, repeat: 1},
					{lo: di(51), up: di(60), mass: 10, repeat: 1},
				}},
			},
			wantBuckets: []bucketSpec{
				{lo: di(1), up: di(30), mass: 30},
				{lo: di(31), up: di(60), mass: 30},
			},
		},
		{
			// Disjoint, gap-separated single-bucket partitions where
			// the unguarded cut-at-prev would re-use partition 0's
			// upper as the start of bucket 1 and produce inverted
			// bounds. The guard rejects that and falls back to a
			// clean 2-bucket layout.
			name:       "disjoint_with_gap_after_filled_bucket",
			expBuckets: 4,
			parts: []partSpec{
				{hist: []bucketSpec{{lo: di(8), up: di(15), mass: 4, repeat: 1}}},
				{hist: []bucketSpec{{lo: di(28), up: di(49), mass: 7, repeat: 1}}},
			},
			wantBuckets: []bucketSpec{
				{lo: di(8), up: di(15), mass: 4},
				{lo: di(28), up: di(49), mass: 7},
			},
		},
		{
			// Three disjoint, growing partitions. emitGroup at the
			// second group cuts at lastUpper and at the third group
			// cuts at prevUpper; without the carry-over fix the
			// final cumulative drops 14 rows.
			name:       "three_disjoint_growing_partitions",
			expBuckets: 3,
			parts: []partSpec{
				{hist: []bucketSpec{{lo: di(1), up: di(10), mass: 5, repeat: 1}}},
				{hist: []bucketSpec{{lo: di(12), up: di(20), mass: 11, repeat: 1}}},
				{hist: []bucketSpec{{lo: di(22), up: di(30), mass: 14, repeat: 1}}},
			},
		},
		{
			// Two partitions with overlapping value ranges. The
			// overlap scan must split refs whose [lower, upper]
			// crosses the chosen cut so that adjacent global
			// buckets don't overlap each other.
			name:       "overlapping_three_bucket_partitions",
			expBuckets: 4,
			parts: []partSpec{
				{hist: []bucketSpec{
					{lo: di(1), up: di(10), mass: 5, repeat: 1},
					{lo: di(12), up: di(30), mass: 8, repeat: 1},
					{lo: di(32), up: di(50), mass: 6, repeat: 1},
				}},
				{hist: []bucketSpec{
					{lo: di(15), up: di(20), mass: 4, repeat: 1},
					{lo: di(25), up: di(40), mass: 7, repeat: 1},
					{lo: di(45), up: di(60), mass: 5, repeat: 1},
				}},
			},
		},

		// ----------------------------------------------------------------
		// Equi-depth quality
		// ----------------------------------------------------------------
		{
			// 50 narrow buckets of 20 rows each → exactly 10 global
			// buckets with max/min mass < 2 (tight equi-depth).
			name:       "uniform_input_packs_equidepth",
			expBuckets: 10,
			parts:      []partSpec{{hist: uniformBuckets(50, 20, 5)}},
			extra: func(t *testing.T, hist *Histogram, _ *TopN) {
				require.Equal(t, 10, hist.Len(),
					"uniform input should produce exactly expBucketNumber buckets")
				masses := bucketMasses(hist)
				require.Less(t, maxOverMin(masses), 2.0,
					"uniform input should produce nearly equi-depth buckets, masses=%v", masses)
			},
		},

		// ----------------------------------------------------------------
		// Fat / hot value handling
		// ----------------------------------------------------------------
		{
			// 10 small uniform buckets + 1 fat [200,200] of 1000.
			// Fat bucket must carry full mass; non-fat tail packs
			// at ~target.
			name:       "one_fat_value_in_uniform_tail",
			expBuckets: 10,
			parts: []partSpec{
				{hist: append(
					uniformBuckets(10, 100, 10),
					bucketSpec{lo: di(200), up: di(200), mass: 1000, repeat: 1000},
				)},
			},
			extra: func(t *testing.T, hist *Histogram, _ *TopN) {
				masses := bucketMasses(hist)
				var hotMass int64
				for i, m := range masses {
					if hist.GetUpper(i).GetInt64() == 200 {
						hotMass = m
					}
				}
				require.GreaterOrEqualf(t, hotMass, int64(1000),
					"fat bucket must carry full hot-value mass; got %d", hotMass)
				// Non-fat target ~111 (1000/9). Cap: 2× target.
				const nonFatTarget = 111
				for i, m := range masses {
					if hist.GetUpper(i).GetInt64() == 200 {
						continue
					}
					require.LessOrEqualf(t, m, int64(2*nonFatTarget),
						"bucket %d mass %d exceeds 2× non-fat target", i, m)
				}
			},
		},
		{
			// Same skewed input as above, but numTopN=1 promotes
			// the hot value into global TopN; histogram holds the
			// 1000 small rows.
			name:       "fat_value_promoted_to_global_topn",
			numTopN:    1,
			expBuckets: 10,
			parts: []partSpec{
				{hist: append(
					uniformBuckets(10, 100, 10),
					bucketSpec{lo: di(200), up: di(200), mass: 1000, repeat: 1000},
				)},
			},
			wantTopN: []topnSpec{{val: di(200), count: 1000}},
			extra: func(t *testing.T, hist *Histogram, _ *TopN) {
				masses := bucketMasses(hist)
				require.Less(t, maxOverMin(masses), 2.0,
					"promoted fat group should leave non-fat tail uniform, masses=%v", masses)
			},
		},
		{
			// 8 partitions × {tail bucket + [500,500] hot of 200}.
			// Hot value's per-partition Repeat aggregates across
			// partitions into one global bucket of mass >= 1600.
			name:       "hot_value_at_upper_across_partitions",
			expBuckets: 10,
			parts: func() []partSpec {
				out := make([]partSpec, 8)
				for p := int64(0); p < 8; p++ {
					out[p] = partSpec{hist: []bucketSpec{
						{lo: di(p*10 + 1), up: di(p*10 + 10), mass: 50, repeat: 0},
						{lo: di(500), up: di(500), mass: 200, repeat: 200},
					}}
				}
				return out
			}(),
			extra: func(t *testing.T, hist *Histogram, _ *TopN) {
				masses := bucketMasses(hist)
				hotIdx := -1
				for i := 0; i < hist.Len(); i++ {
					if hist.GetUpper(i).GetInt64() == 500 {
						hotIdx = i
						break
					}
				}
				require.NotEqualf(t, -1, hotIdx, "fat bucket containing hot value 500 not found")
				require.GreaterOrEqualf(t, masses[hotIdx], int64(1600),
					"fat bucket at value 500 must carry the aggregated cross-partition mass")
			},
		},
		{
			// 1 mega [1,1]:1000 + 100 narrow buckets of 5. Post-fat
			// tail must pack at ~target instead of producing a long
			// run of single-group buckets.
			name:       "tail_after_fat_packs_at_target",
			expBuckets: 10,
			parts: []partSpec{
				{hist: append(
					[]bucketSpec{{lo: di(1), up: di(1), mass: 1000, repeat: 1000}},
					uniformBucketsOffset(100, 5, 10, 10)...,
				)},
			},
			extra: func(t *testing.T, hist *Histogram, _ *TopN) {
				// The right-to-left merge processes the fat [1,1]:1000
				// bucket LAST (it has the smallest upper). By the time
				// the walk reaches it, three target-sized tail buckets
				// have already been merged, the fourth is in progress,
				// and the fat lands in that final merged bucket
				// together with whatever narrow buckets had already
				// accumulated. The fat therefore does NOT get its own
				// dedicated [1, 1] global bucket; instead it is folded
				// into the leftmost (final, lowest-value) global
				// bucket. This differs from a left-to-right walk with
				// cut-at-prev rebalancing, which can choose to merge
				// the fat as its own bucket; the trade-off is that
				// the right-to-left walk's per-ref decision is O(1)
				// and never peeks at unvisited mass.
				//
				// What we still verify here is the tail-packing
				// property (the test's actual name): the narrow
				// buckets get packed into target-sized merged buckets
				// rather than a long run of tiny single-group buckets.
				masses := bucketMasses(hist)
				require.Equalf(t, int64(1), hist.GetLower(0).GetInt64(),
					"leftmost bucket must extend down to the fat value 1; got lower=%d",
					hist.GetLower(0).GetInt64())
				require.GreaterOrEqualf(t, masses[0], int64(1000),
					"leftmost bucket must carry at least the fat's mass (1000); got %d, masses=%v",
					masses[0], masses)
				// Tail (non-leftmost) buckets should mostly be at or
				// near target; at most one tiny straggler.
				const tinyThreshold = int64(20)
				tinyCount := 0
				for i := 1; i < len(masses); i++ {
					if masses[i] < tinyThreshold {
						tinyCount++
					}
				}
				require.LessOrEqualf(t, tinyCount, 1,
					"bucket-run regression: %d tail buckets are tiny (<%d), masses=%v",
					tinyCount, tinyThreshold, masses)
			},
		},

		// ----------------------------------------------------------------
		// TopN promotion / suppression
		// ----------------------------------------------------------------
		{
			// 20 partitions; val=50 is in p0/p1 TopN and at upper-
			// bound Repeat of p2..p19. Repeat extraction must lift
			// val=50 into global TopN with the exact aggregated
			// count.
			//
			// True totals: val_spread(50) = 500*2 + 80*18 = 2440.
			//              val_common(200) = 100*2 + 85*18 = 1730.
			//              val_X(300) = 90*18 = 1620 (excluded).
			name:       "spread_value_repeat_extraction",
			numTopN:    2,
			expBuckets: 100,
			parts: func() []partSpec {
				out := make([]partSpec, 20)
				for i := 0; i < 20; i++ {
					if i < 2 {
						out[i] = partSpec{
							topN: []topnSpec{
								{val: di(50), count: 500},
								{val: di(200), count: 100},
							},
							hist: []bucketSpec{
								{lo: di(1), up: di(20), mass: 60, repeat: 8},
								{lo: di(21), up: di(49), mass: 60, repeat: 6},
								{lo: di(51), up: di(100), mass: 80, repeat: 10},
								{lo: di(101), up: di(199), mass: 80, repeat: 12},
							},
						}
					} else {
						out[i] = partSpec{
							topN: []topnSpec{
								{val: di(300), count: 90},
								{val: di(200), count: 85},
							},
							hist: []bucketSpec{
								{lo: di(1), up: di(20), mass: 80, repeat: 10},
								{lo: di(21), up: di(50), mass: 120, repeat: 80}, // val_spread at upper bound
								{lo: di(51), up: di(80), mass: 80, repeat: 15},
								{lo: di(81), up: di(100), mass: 70, repeat: 12},
							},
						}
					}
				}
				return out
			}(),
			// Encoded order for ints: 50 < 200, but 300 won't make
			// the cut. Asserts both winning entries with exact counts.
			wantTopN: []topnSpec{
				{val: di(50), count: 2440},
				{val: di(200), count: 1730},
			},
		},
		{
			// val_rare=50 is INSIDE [26,55] (not at upper bound) in
			// p2..p19. The merge must not inflate it from the
			// upper-bound Repeat of value 55.
			name:       "inside_bucket_value_not_inflated",
			numTopN:    2,
			expBuckets: 100,
			parts: func() []partSpec {
				out := make([]partSpec, 20)
				for i := 0; i < 20; i++ {
					if i < 2 {
						out[i] = partSpec{
							topN: []topnSpec{
								{val: di(50), count: 60},
								{val: di(200), count: 40},
							},
							hist: []bucketSpec{
								{lo: di(1), up: di(25), mass: 50, repeat: 5},
								{lo: di(26), up: di(49), mass: 50, repeat: 6},
								{lo: di(51), up: di(100), mass: 80, repeat: 10},
								{lo: di(101), up: di(199), mass: 70, repeat: 12},
							},
						}
					} else {
						out[i] = partSpec{
							topN: []topnSpec{
								{val: di(200), count: 40},
								{val: di(300), count: 35},
							},
							hist: []bucketSpec{
								{lo: di(1), up: di(25), mass: 100, repeat: 10},
								{lo: di(26), up: di(55), mass: 150, repeat: 20}, // val_rare(50) is INSIDE this bucket
								{lo: di(56), up: di(80), mass: 130, repeat: 15},
								{lo: di(81), up: di(100), mass: 120, repeat: 12},
							},
						}
					}
				}
				return out
			}(),
			// val_common = 40*20 = 800; val_filler(300) = 35*18 = 630.
			// val_rare = 60*2 = 120 (TopN-only), not promoted.
			wantTopN: []topnSpec{
				{val: di(200), count: 800},
				{val: di(300), count: 630},
			},
		},
		{
			// Three TopN entries whose count-desc order does NOT
			// match their encoded order. The merge must re-sort
			// before returning so binary-search consumers
			// (LowerBound / BetweenCount) work correctly.
			name:       "topn_sorted_by_encoded_bytes",
			numTopN:    3,
			expBuckets: 100,
			parts: []partSpec{
				{
					topN: []topnSpec{
						{val: di(5), count: 10},
						{val: di(10), count: 20},
						{val: di(15), count: 5},
					},
					// Minimal histogram with values outside the TopN
					// range so they don't perturb the result.
					hist: []bucketSpec{{lo: di(100), up: di(200), mass: 1, repeat: 0}},
				},
			},
			// Encoded order for positive ints is numeric.
			wantTopN: []topnSpec{
				{val: di(5), count: 10},
				{val: di(10), count: 20},
				{val: di(15), count: 5},
			},
		},
		{
			// numTopN=1 picks val=2 (3 + 1 repeat = 4); subtracting
			// val=2's hist contribution leaves a no-mass merged
			// group between val=1 and val=3. Inv 8 (no zero-mass
			// bucket) catches the regression.
			name:       "topn_subtraction_leaves_no_zero_mass_bucket",
			numTopN:    1,
			expBuckets: 3,
			parts: []partSpec{
				{
					topN: []topnSpec{{val: di(3), count: 3}},
					hist: []bucketSpec{{lo: di(2), up: di(2), mass: 1, repeat: 1}},
				},
				{
					topN: []topnSpec{{val: di(2), count: 3}},
					hist: []bucketSpec{
						{lo: di(1), up: di(3), mass: 2, repeat: 1},
						{lo: di(4), up: di(4), mass: 1, repeat: 1},
					},
				},
				{
					topN: []topnSpec{{val: di(1), count: 2}},
					// Empty histogram (TopN-only partition).
				},
			},
		},

		// ----------------------------------------------------------------
		// Coverage additions
		// ----------------------------------------------------------------
		{
			// Pre-arm killer signal; merge must surface it from
			// the in-loop killer.HandleSignal() checkpoint.
			name:       "killer_propagates_error",
			preKill:    true,
			numTopN:    2,
			expBuckets: 3,
			parts: []partSpec{
				{hist: uniformBuckets(5, 10, 5)},
				{hist: uniformBuckets(5, 10, 5)},
			},
			wantErrSub: "Query execution was interrupted",
		},
		{
			// Index path. Same shape as a basic column case;
			// buildInputs converts bucket bounds into the Bytes-kind
			// encoded form an index histogram expects, and Inv 7
			// reads upper.GetBytes() instead of re-encoding.
			name:       "index_path_basic",
			isIndex:    true,
			numTopN:    2,
			expBuckets: 4,
			parts: []partSpec{
				{
					topN: []topnSpec{{val: di(1), count: 50}, {val: di(2), count: 30}},
					hist: uniformBuckets(4, 25, 5),
				},
				{
					topN: []topnSpec{{val: di(1), count: 40}, {val: di(3), count: 20}},
					hist: uniformBuckets(4, 25, 5),
				},
			},
		},
		{
			// Varchar / binary collation. Picks string values whose
			// lexicographic order matches the integer test shapes so
			// the merge logic is exercised on a different encoded
			// form.
			name:       "varchar_with_binary_collation",
			colTp:      mysql.TypeVarchar,
			numTopN:    2,
			expBuckets: 3,
			parts: []partSpec{
				{
					topN: []topnSpec{{val: ds("aaa"), count: 5}, {val: ds("zzz"), count: 3}},
					hist: []bucketSpec{
						{lo: ds("bbb"), up: ds("ccc"), mass: 4, repeat: 1},
						{lo: ds("ddd"), up: ds("eee"), mass: 6, repeat: 2},
					},
				},
				{
					topN: []topnSpec{{val: ds("aaa"), count: 4}, {val: ds("yyy"), count: 2}},
					hist: []bucketSpec{
						{lo: ds("bbb"), up: ds("ccc"), mass: 3, repeat: 1},
						{lo: ds("ddd"), up: ds("eee"), mass: 5, repeat: 2},
					},
				},
			},
		},
		{
			// expBuckets=1, every bucket collapses into one.
			name:       "exp_buckets_one_collapses_all",
			expBuckets: 1,
			parts: []partSpec{
				{hist: uniformBuckets(5, 10, 5)},
				{hist: uniformBucketsOffset(5, 10, 5, 100)},
			},
		},
		{
			// expBuckets larger than the total source bucket count.
			// Every source bucket can stay distinct; the cap is
			// harmless.
			name:       "exp_buckets_above_total_source",
			expBuckets: 50,
			parts: []partSpec{
				{hist: uniformBuckets(3, 5, 5)},
				{hist: uniformBucketsOffset(3, 5, 5, 100)},
			},
		},
		{
			// numTopN=0 with histograms whose upper-bound Repeats
			// are large. The Repeat-extraction code must be skipped
			// since no global TopN is requested.
			name:       "numtopn_zero_with_high_repeats",
			expBuckets: 3,
			parts: []partSpec{
				{hist: []bucketSpec{
					{lo: di(1), up: di(10), mass: 30, repeat: 20},
					{lo: di(11), up: di(20), mass: 40, repeat: 25},
				}},
				{hist: []bucketSpec{
					{lo: di(1), up: di(10), mass: 30, repeat: 20},
					{lo: di(11), up: di(20), mass: 40, repeat: 25},
				}},
			},
		},
	}
}

// uniformBuckets returns n contiguous int buckets of `mass` rows
// each, starting at lower=1 with width `width`.
func uniformBuckets(n int, mass int64, width int64) []bucketSpec {
	return uniformBucketsOffset(n, mass, width, 0)
}

// uniformBucketsOffset returns n contiguous int buckets of `mass`
// rows each starting at `offset+1`, each of width `width`.
func uniformBucketsOffset(n int, mass int64, width int64, offset int64) []bucketSpec {
	out := make([]bucketSpec, n)
	for i := int64(0); i < int64(n); i++ {
		out[i] = bucketSpec{
			lo:     di(offset + i*width + 1),
			up:     di(offset + i*width + width),
			mass:   mass,
			repeat: 0,
		}
	}
	return out
}

// TestMergePartTopNAndHistToGlobalErrors covers the error returns
// that can't be expressed via the partSpec fixture (which always
// constructs a non-nil histogram per partition).
func TestMergePartTopNAndHistToGlobalErrors(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}

	t.Run("nil_topns_and_hists", func(t *testing.T) {
		_, _, err := MergePartTopNAndHistToGlobal(sc, &killer, nil, nil, 2, 10, false)
		require.Error(t, err)
	})

	t.Run("all_nil_histograms", func(t *testing.T) {
		topN := NewTopN(2)
		key, err := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(1))
		require.NoError(t, err)
		topN.AppendTopN(key, 5)
		_, _, err = MergePartTopNAndHistToGlobal(
			sc, &killer,
			[]*TopN{topN, topN},
			[]*Histogram{nil, nil},
			2, 10, false,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no partition histograms")
	})
}

// TestMergePartTopNAndHistToGlobalVirtualHistChunking exercises the
// chunked-virtual-histogram path with a per-partition-categorical
// layout that matches the production-scale shape:
//
//	8192 partitions, 256 buckets per partition, 100 TopN per
//	partition, with each partition's values confined to its own
//	non-overlapping band so partition TopN entries never match any
//	partition bucket upper. After global TopN selection (100
//	winners), ~819,100 unmatched entries flow into the virtual
//	histogram — well over the math.MaxUint16 single-Histogram
//	capacity, so the chunked path must engage.
//
// The merge must preserve total row count and produce a reasonably
// equi-depth global histogram. The pre-chunking implementation
// silently wraps the virtual bucket index, mis-attributes mass and
// loses rows.
func TestMergePartTopNAndHistToGlobalVirtualHistChunking(t *testing.T) {
	const (
		numParts       = 8192
		bucketsPerPart = 256
		topNPerPart    = 100
		bandWidth      = 10000
		bandStride     = 12500 // gaps between bands ensure no inter-partition overlap
		bucketMass     = 12
		bucketRepeat   = 4
		topNCount      = 4
		numTopN        = 100
		expBuckets     = 256
	)
	parts := make([]partSpec, numParts)
	bucketWidth := int64(bandWidth) / bucketsPerPart
	for p := 0; p < numParts; p++ {
		base := int64(p) * int64(bandStride)
		// TopN values at fixed positions inside the band; offset by
		// 1 so they never equal any bucket upper.
		tn := make([]topnSpec, topNPerPart)
		for i := 0; i < topNPerPart; i++ {
			tn[i] = topnSpec{val: di(base + int64(i)*3 + 1), count: topNCount}
		}
		hist := make([]bucketSpec, bucketsPerPart)
		for b := 0; b < bucketsPerPart; b++ {
			bLo := base + int64(b)*bucketWidth + int64(topNPerPart)*3 + 2
			bUp := bLo + bucketWidth - 1
			hist[b] = bucketSpec{lo: di(bLo), up: di(bUp), mass: bucketMass, repeat: bucketRepeat}
		}
		parts[p] = partSpec{topN: tn, hist: hist}
	}
	tc := mergeCase{
		name:       "virtual_hist_chunking",
		parts:      parts,
		numTopN:    numTopN,
		expBuckets: expBuckets,
	}
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}
	topNs, hists := buildInputs(t, sc, tc)
	gTopN, gHist, err := MergePartTopNAndHistToGlobal(
		sc, &killer, topNs, hists, tc.numTopN, tc.expBuckets, tc.isIndex,
	)
	require.NoError(t, err)
	require.NotNil(t, gHist)

	// The fixture must push the virtual histogram past MaxUint16 so
	// the chunked code path is exercised.
	virtualSize := numParts*topNPerPart - numTopN
	require.Greater(t, virtualSize, math.MaxUint16,
		"fixture must produce a virtual hist larger than math.MaxUint16; got %d", virtualSize)

	// No rows may be lost.
	wantTotal := totalInputRows(tc)
	gotTotal := int64(0)
	if gHist.Len() > 0 {
		gotTotal = gHist.Buckets[gHist.Len()-1].Count
	}
	if gTopN != nil {
		for _, m := range gTopN.TopN {
			gotTotal += int64(m.Count)
		}
	}
	require.Equalf(t, wantTotal, gotTotal,
		"total rows must be preserved (lost=%d)", wantTotal-gotTotal)

	// The global histogram must be near full capacity and reasonably
	// equi-depth. Without chunking, mass collapses into a handful of
	// wildly uneven buckets (ratios in the thousands).
	require.GreaterOrEqual(t, gHist.Len(), 200,
		"global histogram should be close to expBuckets=256; got %d", gHist.Len())
	masses := bucketMasses(gHist)
	var maxM, minM int64 = 0, math.MaxInt64
	for _, m := range masses {
		if m > maxM {
			maxM = m
		}
		if m < minM {
			minM = m
		}
	}
	require.Greater(t, minM, int64(0), "every bucket must have positive mass")
	require.Lessf(t, float64(maxM)/float64(minM), 10.0,
		"mass distribution must not be wildly skewed (max=%d min=%d)", maxM, minM)
}

// TestMergePartTopNAndHistToGlobalSingletonFilter verifies the
// singleton filter on global TopN admission. A value with
// accumulated count == 1 carries no more selectivity signal than
// the histogram + NDV fallback already provides, and the specific
// singletons that win heap ties are arbitrary. When the heap fills
// to capacity (more distinct candidates than numTopN slots),
// remaining singletons must be filtered out. Filtered values'
// rows must still appear in the global histogram so total row
// count is preserved.
//
// The fixture provides one partition with five buckets whose upper
// value appears exactly once (Repeat=1) and numTopN=3, which is
// smaller than the candidate pool, so the heap fills to capacity
// with singleton candidates and the filter triggers. Without the
// filter, the heap would surface three of the five singletons.
// With the filter, global TopN is empty and the histogram retains
// all 20 rows.
//
// A second small-table check confirms the inverse: when numTopN
// exceeds the candidate count (NDV < numTopN), singletons are
// kept because they enumerate the complete distinct-value set and
// carry exact range information that the optimizer relies on.
func TestMergePartTopNAndHistToGlobalSingletonFilter(t *testing.T) {
	tc := mergeCase{
		name:       "global_topn_filters_singletons_at_capacity",
		numTopN:    3,
		expBuckets: 5,
		parts: []partSpec{
			{hist: []bucketSpec{
				{lo: di(1), up: di(5), mass: 4, repeat: 1},
				{lo: di(6), up: di(10), mass: 4, repeat: 1},
				{lo: di(11), up: di(15), mass: 4, repeat: 1},
				{lo: di(16), up: di(20), mass: 4, repeat: 1},
				{lo: di(21), up: di(25), mass: 4, repeat: 1},
			}},
		},
	}
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}
	topNs, hists := buildInputs(t, sc, tc)
	gTopN, gHist, err := MergePartTopNAndHistToGlobal(
		sc, &killer, topNs, hists, tc.numTopN, tc.expBuckets, tc.isIndex,
	)
	require.NoError(t, err)
	require.NotNil(t, gHist)

	// No singleton-count value may appear in the global TopN.
	if gTopN != nil {
		for _, m := range gTopN.TopN {
			require.GreaterOrEqualf(t, m.Count, uint64(2),
				"global TopN must not contain a value with count < 2; got %d", m.Count)
		}
	}

	// Total row count preserved.
	want := totalInputRows(tc)
	got := int64(0)
	if gHist.Len() > 0 {
		got = gHist.Buckets[gHist.Len()-1].Count
	}
	if gTopN != nil {
		for _, m := range gTopN.TopN {
			got += int64(m.Count)
		}
	}
	require.Equalf(t, want, got, "rows preserved across hist + global TopN (lost=%d)", want-got)

	// Small-table inverse: NDV < numTopN, so singletons enumerate
	// the complete distinct-value set and must be retained.
	smallTC := mergeCase{
		name:       "global_topn_keeps_singletons_below_capacity",
		numTopN:    20,
		expBuckets: 4,
		parts: []partSpec{
			{hist: []bucketSpec{
				{lo: di(1), up: di(5), mass: 4, repeat: 1},
				{lo: di(6), up: di(10), mass: 4, repeat: 1},
				{lo: di(11), up: di(15), mass: 4, repeat: 1},
				{lo: di(16), up: di(20), mass: 4, repeat: 1},
			}},
		},
	}
	smallTopNs, smallHists := buildInputs(t, sc, smallTC)
	smallGTopN, smallGHist, err := MergePartTopNAndHistToGlobal(
		sc, &killer, smallTopNs, smallHists, smallTC.numTopN, smallTC.expBuckets, smallTC.isIndex,
	)
	require.NoError(t, err)
	require.NotNil(t, smallGHist)
	require.NotNil(t, smallGTopN, "singletons must be retained when heap is below capacity")
	require.Equalf(t, 4, len(smallGTopN.TopN),
		"all four singleton bucket uppers must appear in global TopN; got %d", len(smallGTopN.TopN))
}

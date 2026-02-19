// Copyright 2017 PingCAP, Inc.
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
	"slices"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"go.uber.org/zap"
)

var bucket4MergingPool = sync.Pool{
	New: func() any {
		return newBucket4Meging()
	},
}

func newbucket4MergingForRecycle() *bucket4Merging {
	return bucket4MergingPool.Get().(*bucket4Merging)
}

func releasebucket4MergingForRecycle(b *bucket4Merging) {
	b.disjointNDV = 0
	b.Repeat = 0
	b.NDV = 0
	b.Count = 0
	bucket4MergingPool.Put(b)
}

// bucket4Merging is only used for merging partition hists to global hist.
type bucket4Merging struct {
	lower *types.Datum
	upper *types.Datum
	Bucket
	// disjointNDV is used for merging bucket NDV, see mergeBucketNDV for more details.
	disjointNDV int64
}

// newBucket4Meging creates a new bucket4Merging.
// but we create it from bucket4MergingPool as soon as possible to reduce the cost of GC.
func newBucket4Meging() *bucket4Merging {
	return &bucket4Merging{
		lower: new(types.Datum),
		upper: new(types.Datum),
		Bucket: Bucket{
			Repeat: 0,
			NDV:    0,
			Count:  0,
		},
		disjointNDV: 0,
	}
}

// buildBucket4Merging builds bucket4Merging from Histogram
// Notice: Count in Histogram.Buckets is prefix sum but in bucket4Merging is not.
func (hg *Histogram) buildBucket4Merging() []*bucket4Merging {
	buckets := make([]*bucket4Merging, 0, hg.Len())
	for i := range hg.Len() {
		b := newbucket4MergingForRecycle()
		hg.LowerToDatum(i, b.lower)
		hg.UpperToDatum(i, b.upper)
		b.Repeat = hg.Buckets[i].Repeat
		b.NDV = hg.Buckets[i].NDV
		b.Count = hg.Buckets[i].Count
		if i != 0 {
			b.Count -= hg.Buckets[i-1].Count
		}
		buckets = append(buckets, b)
	}
	return buckets
}

func (b *bucket4Merging) Clone() bucket4Merging {
	result := newbucket4MergingForRecycle()
	result.Repeat = b.Repeat
	result.NDV = b.NDV
	b.upper.Copy(result.upper)
	b.lower.Copy(result.lower)
	result.Count = b.Count
	result.disjointNDV = b.disjointNDV
	return *result
}

// mergeBucketNDV merges bucket NDV from tow bucket `right` & `left`.
// Before merging, you need to make sure that when using (upper, lower) as the comparison key, `right` is greater than `left`
func mergeBucketNDV(sc *stmtctx.StatementContext, left *bucket4Merging, right *bucket4Merging) (*bucket4Merging, error) {
	res := right.Clone()
	if left.Count == 0 {
		return &res, nil
	}
	if right.Count == 0 {
		left.lower.Copy(res.lower)
		left.upper.Copy(res.upper)
		res.NDV = left.NDV
		return &res, nil
	}
	upperCompare, err := right.upper.Compare(sc.TypeCtx(), left.upper, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	// __right__|
	// _______left____|
	// illegal order.
	if upperCompare < 0 {
		err := errors.Errorf("illegal bucket order")
		statslogutil.StatsLogger().Warn("fail to mergeBucketNDV", zap.Error(err))
		return nil, err
	}
	//  ___right_|
	//  ___left__|
	// They have the same upper.
	if upperCompare == 0 {
		lowerCompare, err := right.lower.Compare(sc.TypeCtx(), left.lower, collate.GetBinaryCollator())
		if err != nil {
			return nil, err
		}
		//      |____right____|
		//         |__left____|
		// illegal order.
		if lowerCompare < 0 {
			err := errors.Errorf("illegal bucket order")
			statslogutil.StatsLogger().Warn("fail to mergeBucketNDV", zap.Error(err))
			return nil, err
		}
		// |___right___|
		// |____left___|
		// ndv = max(right.ndv, left.ndv)
		if lowerCompare == 0 {
			if left.NDV > right.NDV {
				res.NDV = left.NDV
			}
			return &res, nil
		}
		//         |_right_|
		// |_____left______|
		// |-ratio-|
		// ndv = ratio * left.ndv + max((1-ratio) * left.ndv, right.ndv)
		ratio := calcFraction4Datums(left.lower, left.upper, right.lower)
		res.NDV = int64(ratio*float64(left.NDV) + math.Max((1-ratio)*float64(left.NDV), float64(right.NDV)))
		res.lower = left.lower.Clone()
		return &res, nil
	}
	// ____right___|
	// ____left__|
	// right.upper > left.upper
	lowerCompareUpper, err := right.lower.Compare(sc.TypeCtx(), left.upper, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	//                  |_right_|
	//  |___left____|
	// `left` and `right` do not intersect
	// We add right.ndv in `disjointNDV`, and let `right.ndv = left.ndv` be used for subsequent merge.
	// This is because, for the merging of many buckets, we merge them from back to front.
	if lowerCompareUpper >= 0 {
		left.upper.Copy(res.upper)
		left.lower.Copy(res.lower)
		res.disjointNDV += right.NDV
		res.NDV = left.NDV
		return &res, nil
	}
	upperRatio := calcFraction4Datums(right.lower, right.upper, left.upper)
	lowerCompare, err := right.lower.Compare(sc.TypeCtx(), left.lower, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	//              |-upperRatio-|
	//              |_______right_____|
	// |_______left______________|
	// |-lowerRatio-|
	// ndv = lowerRatio * left.ndv
	//		+ max((1-lowerRatio) * left.ndv, upperRatio * right.ndv)
	//		+ (1-upperRatio) * right.ndv
	if lowerCompare >= 0 {
		lowerRatio := calcFraction4Datums(left.lower, left.upper, right.lower)
		res.NDV = int64(lowerRatio*float64(left.NDV) +
			math.Max((1-lowerRatio)*float64(left.NDV), upperRatio*float64(right.NDV)) +
			(1-upperRatio)*float64(right.NDV))
		res.lower = left.lower.Clone()
		return &res, nil
	}
	// |------upperRatio--------|
	// |-lowerRatio-|
	// |____________right______________|
	//              |___left____|
	// ndv = lowerRatio * right.ndv
	//		+ max(left.ndv + (upperRatio - lowerRatio) * right.ndv)
	//		+ (1-upperRatio) * right.ndv
	lowerRatio := calcFraction4Datums(right.lower, right.upper, left.lower)
	res.NDV = int64(lowerRatio*float64(right.NDV) +
		math.Max(float64(left.NDV), (upperRatio-lowerRatio)*float64(right.NDV)) +
		(1-upperRatio)*float64(right.NDV))
	return &res, nil
}

// mergeParitionBuckets merges buckets[l...r) to one global bucket.
// global bucket:
//
//	upper = buckets[r-1].upper
//	count = sum of buckets[l...r).count
//	repeat = sum of buckets[i] (buckets[i].upper == global bucket.upper && i in [l...r))
//	ndv = merge bucket ndv from r-1 to l by mergeBucketNDV
//
// Notice: lower is not calculated here.
func mergePartitionBuckets(sc *stmtctx.StatementContext, buckets []*bucket4Merging) (*bucket4Merging, error) {
	if len(buckets) == 0 {
		return nil, errors.Errorf("not enough buckets to merge")
	}
	res := newbucket4MergingForRecycle()
	buckets[len(buckets)-1].upper.Copy(res.upper)
	right := buckets[len(buckets)-1].Clone()

	totNDV := int64(0)
	intest.Assert(res.Count == 0, "Count in the new bucket4Merging should be 0")
	intest.Assert(res.Repeat == 0, "Repeat in the new bucket4Merging should be 0")
	intest.Assert(res.NDV == 0, "NDV in the new bucket4Merging bucket4Merging should be 0")
	for i := len(buckets) - 1; i >= 0; i-- {
		totNDV += buckets[i].NDV
		res.Count += buckets[i].Count
		compare, err := buckets[i].upper.Compare(sc.TypeCtx(), res.upper, collate.GetBinaryCollator())
		if err != nil {
			return nil, err
		}
		if compare == 0 {
			res.Repeat += buckets[i].Repeat
		}
		if i != len(buckets)-1 {
			tmp, err := mergeBucketNDV(sc, buckets[i], &right)
			if err != nil {
				return nil, err
			}
			right = *tmp
		}
	}
	res.NDV = right.NDV + right.disjointNDV

	// since `mergeBucketNDV` is based on uniform and inclusion assumptions, it has the trend to under-estimate,
	// and as the number of buckets increases, these assumptions become weak,
	// so to mitigate this problem, a damping factor based on the number of buckets is introduced.
	res.NDV = min(int64(float64(res.NDV)*math.Pow(1.15, float64(len(buckets)-1))), totNDV)
	return res, nil
}

func (t *TopNMeta) buildBucket4Merging(d *types.Datum, analyzeVer int) *bucket4Merging {
	res := newbucket4MergingForRecycle()
	d.Copy(res.lower)
	d.Copy(res.upper)
	res.Count = int64(t.Count)
	res.Repeat = int64(t.Count)
	if analyzeVer <= Version2 {
		res.NDV = 0
	}
	failpoint.Inject("github.com/pingcap/pkg/statistics/enableTopNNDV", func(_ failpoint.Value) {
		res.NDV = 1
	})
	intest.Assert(analyzeVer <= Version2)
	return res
}

// MergePartitionHist2GlobalHist merges hists (partition-level Histogram) to a global-level Histogram
func MergePartitionHist2GlobalHist(sc *stmtctx.StatementContext, hists []*Histogram, popedTopN []TopNMeta, expBucketNumber int64, isIndex bool, analyzeVer int) (*Histogram, error) {
	var totCount, totNull, totColSize int64
	var bucketNumber int
	if expBucketNumber == 0 {
		return nil, errors.Errorf("expBucketNumber can not be zero")
	}
	// This only occurs when there are no histogram records in the histogram system table.
	// It happens only to tables whose DDL events haven't been processed yet and that have no indexes or keys,
	// with the predicate column feature enabled.
	if len(hists) == 0 {
		return nil, nil
	}
	for _, hist := range hists {
		totColSize += hist.TotColSize
		totNull += hist.NullCount
		histLen := hist.Len()
		if histLen > 0 {
			bucketNumber += histLen
			totCount += hist.Buckets[hist.Len()-1].Count
		}
	}
	// If all the hist and the topn is empty, return a empty hist.
	if bucketNumber+len(popedTopN) == 0 {
		return NewHistogram(hists[0].ID, 0, totNull, hists[0].LastUpdateVersion, hists[0].Tp, 0, totColSize), nil
	}

	bucketNumber += len(popedTopN)
	buckets := make([]*bucket4Merging, 0, bucketNumber)
	globalBuckets := make([]*bucket4Merging, 0, expBucketNumber)

	// init `buckets`.
	for _, hist := range hists {
		buckets = append(buckets, hist.buildBucket4Merging()...)
	}

	for _, meta := range popedTopN {
		totCount += int64(meta.Count)
		d, err := topNMetaToDatum(meta, hists[0].Tp.GetType(), isIndex, sc.TimeZone())
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, meta.buildBucket4Merging(&d, analyzeVer))
	}

	// Remove empty buckets
	tail := 0
	for i := range buckets {
		if buckets[i].Count != 0 {
			// Because we will reuse the tail of the slice in `releasebucket4MergingForRecycle`,
			// we need to shift the non-empty buckets to the front.
			buckets[tail], buckets[i] = buckets[i], buckets[tail]
			tail++
		}
	}
	for n := tail; n < len(buckets); n++ {
		releasebucket4MergingForRecycle(buckets[n])
	}
	buckets = buckets[:tail]

	err := sortBucketsByUpperBound(sc.TypeCtx(), buckets)
	if err != nil {
		return nil, err
	}

	var sum, prevSum int64
	r := len(buckets)
	bucketCount := int64(1)
	gBucketCountThreshold := (totCount / expBucketNumber) * 80 / 100 // expectedBucketSize * 0.8
	mergeBuffer := make([]*bucket4Merging, 0, (len(buckets)+int(expBucketNumber)-1)/int(expBucketNumber))
	cutAndFixBuffer := make([]*bucket4Merging, 0, (len(buckets)+int(expBucketNumber))/int(expBucketNumber))
	var currentLeftMost *types.Datum
	for i := len(buckets) - 1; i >= 0; i-- {
		if currentLeftMost == nil {
			currentLeftMost = buckets[i].lower
		} else {
			res, err := currentLeftMost.Compare(sc.TypeCtx(), buckets[i].lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				currentLeftMost = buckets[i].lower
			}
		}
		sum += buckets[i].Count
		if sum >= totCount*bucketCount/expBucketNumber && sum-prevSum >= gBucketCountThreshold {
			// If the buckets have the same upper, we merge them into the same new buckets.
			// We don't need to update the currentLeftMost in the for loop because the leftmost bucket's lower
			// will be the smallest when their upper is the same.
			// We just need to update it after the for loop.
			for ; i > 0; i-- {
				res, err := buckets[i-1].upper.Compare(sc.TypeCtx(), buckets[i].upper, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				if res != 0 {
					break
				}
				sum += buckets[i-1].Count
			}
			res, err := currentLeftMost.Compare(sc.TypeCtx(), buckets[i].lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				currentLeftMost = buckets[i].lower
			}

			// Iterate possible overlapped ones.
			// We need to re-sort this part.
			mergeBuffer = mergeBuffer[:0]
			cutAndFixBuffer = cutAndFixBuffer[:0]
			leftMostValidPosForNonOverlapping := i
			for ; i > 0; i-- {
				res, err := buckets[i-1].upper.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				// If buckets[i-1].upper < currentLeftMost, this bucket has no overlap with current merging one. Break it.
				if res < 0 {
					break
				}
				// Now the bucket[i-1].upper >= currentLeftMost, they are overlapped.
				res, err = buckets[i-1].lower.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				// If buckets[i-1].lower >= currentLeftMost, this bucket is totally inside. So it can be totally merged.
				if res >= 0 {
					sum += buckets[i-1].Count
					mergeBuffer = append(mergeBuffer, buckets[i-1])
					continue
				}
				// Now buckets[i-1].lower < currentLeftMost < buckets[i-1].upper
				// calcFraction4Datums calc the value: (currentLeftMost - lower_bound) / (upper_bound - lower_bound)
				overlapping := 1 - calcFraction4Datums(buckets[i-1].lower, buckets[i-1].upper, currentLeftMost)
				overlappedCount := int64(float64(buckets[i-1].Count) * overlapping)
				overlappedNDV := int64(float64(buckets[i-1].NDV) * overlapping)
				sum += overlappedCount
				buckets[i-1].Count -= overlappedCount
				buckets[i-1].NDV -= overlappedNDV
				buckets[i-1].Repeat = 0
				if buckets[i-1].NDV < 0 {
					buckets[i-1].NDV = 0
				}
				if buckets[i-1].Count < 0 {
					buckets[i-1].Count = 0
				}

				// Cut it.
				cutBkt := newbucket4MergingForRecycle()
				buckets[i-1].upper.Copy(cutBkt.upper)
				currentLeftMost.Copy(cutBkt.lower)
				currentLeftMost.Copy(buckets[i-1].upper)
				cutBkt.Count = overlappedCount
				cutBkt.NDV = overlappedNDV
				mergeBuffer = append(mergeBuffer, cutBkt)
				cutAndFixBuffer = append(cutAndFixBuffer, cutBkt)
			}
			var merged *bucket4Merging
			if len(cutAndFixBuffer) == 0 {
				merged, err = mergePartitionBuckets(sc, buckets[i:r])
				if err != nil {
					return nil, err
				}
			} else {
				// mergedBuffer is in reverse order, we need to reverse it.
				slices.Reverse(mergeBuffer)
				// The content in the merge buffer don't need a re-sort since we just fix some lower bound for them.
				mergeBuffer = append(mergeBuffer, buckets[leftMostValidPosForNonOverlapping:r]...)
				checkBucket4MergingIsSorted(sc.TypeCtx(), mergeBuffer)
				merged, err = mergePartitionBuckets(sc, mergeBuffer)
				if err != nil {
					return nil, err
				}
				for _, bkt := range cutAndFixBuffer {
					releasebucket4MergingForRecycle(bkt)
				}
				// The buckets in buckets[i:origI] needs a re-sort.
				err = sortBucketsByUpperBound(sc.TypeCtx(), buckets[i:leftMostValidPosForNonOverlapping])
				if err != nil {
					return nil, err
				}
				// After the operation, the buckets in buckets[i:origI] contains two kinds of buckets:
				// 1. The buckets that are totally inside the merged bucket. => lower_bound >= currentLeftMost
				//    It's not changed. [lower_bound_i, upper_bound_i] with lower_bound_i >= currentLeftMost
				// 2. The buckets that are overlapped with the merged bucket. lower_bound < currentLeftMost < upper_bound
				//    After cutting, the remained part is [lower_bound_i, currentLeftMost]
				// To do the next round of merging, we need to kick out the 1st kind of buckets.
				// And after the re-sort, the 2nd kind of buckets will be in the front.
				leftMostInvalidPosForNextRound := leftMostValidPosForNonOverlapping
				for ; leftMostInvalidPosForNextRound > i; leftMostInvalidPosForNextRound-- {
					res, err := buckets[leftMostInvalidPosForNextRound-1].lower.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
					if err != nil {
						return nil, err
					}
					// Once the lower bound < currentLeftMost, we've skipped all the 1st kind of bucket.
					// We can break here.
					if res < 0 {
						break
					}
				}
				checkBucket4MergingIsSorted(sc.TypeCtx(), buckets[i:leftMostInvalidPosForNextRound])
				i = leftMostInvalidPosForNextRound
			}
			currentLeftMost.Copy(merged.lower)
			currentLeftMost = nil
			globalBuckets = append(globalBuckets, merged)
			r = i
			bucketCount++
			prevSum = sum
		}
	}
	if r > 0 {
		leftMost := buckets[0].lower
		for i, b := range buckets[:r] {
			if i == 0 {
				continue
			}
			res, err := leftMost.Compare(sc.TypeCtx(), b.lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				leftMost = b.lower
			}
		}

		merged, err := mergePartitionBuckets(sc, buckets[:r])
		if err != nil {
			return nil, err
		}
		leftMost.Copy(merged.lower)
		globalBuckets = append(globalBuckets, merged)
	}
	for i := range buckets {
		releasebucket4MergingForRecycle(buckets[i])
	}
	// Because we merge backwards, we need to flip the slices.
	slices.Reverse(globalBuckets)

	for i := 1; i < len(globalBuckets); i++ {
		globalBuckets[i].Count = globalBuckets[i].Count + globalBuckets[i-1].Count
	}

	// Recalculate repeats
	// TODO: optimize it later since it's a simple but not the fastest implementation whose complexity is O(nBkt * nHist * log(nBkt))
	for _, bucket := range globalBuckets {
		var repeat float64
		for _, hist := range hists {
			histRowCount, _ := hist.EqualRowCount(nil, *bucket.upper, isIndex)
			repeat += histRowCount // only hists of indexes have bucket.NDV
		}
		if int64(repeat) > bucket.Repeat {
			bucket.Repeat = int64(repeat)
		}
	}

	globalHist := NewHistogram(hists[0].ID, 0, totNull, hists[0].LastUpdateVersion, hists[0].Tp, len(globalBuckets), totColSize)
	for _, bucket := range globalBuckets {
		if !isIndex {
			bucket.NDV = 0 // bucket.NDV is not maintained for column histograms
		}
		globalHist.AppendBucketWithNDV(bucket.lower, bucket.upper, bucket.Count, bucket.Repeat, bucket.NDV)
	}
	return globalHist, nil
}

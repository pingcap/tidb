// Copyright 2023 PingCAP, Inc.
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

package globalstats

import (
	"bytes"
	"container/heap"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/tiancaiamao/gp"
)

func mergeGlobalStatsTopN(gp *gp.Pool, sc sessionctx.Context, wrapper *StatsWrapper,
	timeZone *time.Location, version int, n uint32, isIndex bool) (*statistics.TopN,
	[]statistics.TopNMeta, []*statistics.Histogram, error) {
	if statistics.CheckEmptyTopNs(wrapper.AllTopN) {
		return nil, nil, wrapper.AllHg, nil
	}
	mergeConcurrency := sc.GetSessionVars().AnalyzePartitionMergeConcurrency
	killer := &sc.GetSessionVars().SQLKiller

	// use original method if concurrency equals 1 or for version1
	if mergeConcurrency < 2 {
		if version == 1 {
			return MergePartTopN2GlobalTopNForAnalyzeVer1(timeZone, wrapper.AllTopN, n, wrapper.AllHg, isIndex, killer)
		}
		return MergePartTopN2GlobalTopN(timeZone, wrapper.AllTopN, n, wrapper.AllHg, isIndex, killer)
	}
	batchSize := len(wrapper.AllTopN) / mergeConcurrency
	if batchSize < 1 {
		batchSize = 1
	} else if batchSize > MaxPartitionMergeBatchSize {
		batchSize = MaxPartitionMergeBatchSize
	}
	return MergeGlobalStatsTopNByConcurrency(gp, mergeConcurrency, batchSize, wrapper, timeZone, version, n, isIndex, killer)
}

// MergeGlobalStatsTopNByConcurrency merge partition topN by concurrency.
// To merge global stats topN by concurrency,
// we will separate the partition topN in concurrency part and deal it with different worker.
// mergeConcurrency is used to control the total concurrency of the running worker,
// and mergeBatchSize is sued to control the partition size for each worker to solve it
func MergeGlobalStatsTopNByConcurrency(
	gp *gp.Pool,
	mergeConcurrency, mergeBatchSize int,
	wrapper *StatsWrapper,
	timeZone *time.Location,
	version int,
	n uint32,
	isIndex bool,
	killer *sqlkiller.SQLKiller,
) (*statistics.TopN,
	[]statistics.TopNMeta, []*statistics.Histogram, error) {
	if len(wrapper.AllTopN) < mergeConcurrency {
		mergeConcurrency = len(wrapper.AllTopN)
	}
	tasks := make([]*TopnStatsMergeTask, 0)
	for start := 0; start < len(wrapper.AllTopN); {
		end := start + mergeBatchSize
		if end > len(wrapper.AllTopN) {
			end = len(wrapper.AllTopN)
		}
		task := NewTopnStatsMergeTask(start, end)
		tasks = append(tasks, task)
		start = end
	}
	var wg sync.WaitGroup
	taskNum := len(tasks)
	taskCh := make(chan *TopnStatsMergeTask, taskNum)
	respCh := make(chan *TopnStatsMergeResponse, taskNum)
	worker := NewTopnStatsMergeWorker(taskCh, respCh, wrapper, killer)
	for i := 0; i < mergeConcurrency; i++ {
		wg.Add(1)
		gp.Go(func() {
			defer wg.Done()
			worker.Run(timeZone, isIndex, version)
		})
	}
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()
	close(respCh)
	// handle Error
	hasErr := false
	errMsg := make([]string, 0)
	for resp := range respCh {
		if resp.Err != nil {
			hasErr = true
			errMsg = append(errMsg, resp.Err.Error())
		}
	}
	if hasErr {
		return nil, nil, nil, errors.New(strings.Join(errMsg, ","))
	}

	// fetch the response from each worker and merge them into global topn stats
	counter := worker.Result()
	numTop := len(counter)
	sorted := make([]statistics.TopNMeta, 0, numTop)
	for value, cnt := range counter {
		data := hack.Slice(string(value))
		sorted = append(sorted, statistics.TopNMeta{Encoded: data, Count: uint64(cnt)})
	}
	globalTopN, popedTopn := statistics.GetMergedTopNFromSortedSlice(sorted, n)
	return globalTopN, popedTopn, wrapper.AllHg, nil
}

// The histIter is maintained to remove the topn values from the histogram.
// It's used under such assumption: the removed topn item is given in order.
// And we record the totalSubstracted because our bucket count is the cumulative one.
// We will need to subtract the topn count for [i-th, n] buckets if we remove it from the i-th bucket.
type histIter struct {
	hist             *statistics.Histogram
	totalSubstracted int64
	curBucketPos     int
}

// remove removes the value from the histogram. The removed value is always bigger than the previous one.
func (hi *histIter) remove(v *types.Datum) int64 {
	for {
		if hi.curBucketPos >= len(hi.hist.Buckets) {
			return 0
		}
		// The value is smaller than the lower bound. We skip it since the histogram doesn't contain this value.
		// This situation is not very reasonable :(
		cmp := chunk.Compare(hi.hist.Bounds.GetRow(hi.curBucketPos*2), 0, v)
		if cmp > 0 {
			return 0
		}
		cmp = chunk.Compare(hi.hist.Bounds.GetRow(hi.curBucketPos*2+1), 0, v)
		if cmp < 0 {
			// This value is bigger than current bucket's upper bound, goto next bucket.
			if hi.hist.Buckets[hi.curBucketPos].Count < hi.totalSubstracted {
				hi.hist.Buckets[hi.curBucketPos].Count = 0
			} else {
				hi.hist.Buckets[hi.curBucketPos].Count -= hi.totalSubstracted
			}
			hi.curBucketPos++
			continue
		}
		if cmp == 0 {
			// This value is just the upper bound of the bucket. Remove it.
			ret := hi.hist.Buckets[hi.curBucketPos].Repeat
			hi.totalSubstracted += ret
			hi.hist.Buckets[hi.curBucketPos].Repeat = 0
			hi.hist.Buckets[hi.curBucketPos].Count -= hi.totalSubstracted
			hi.curBucketPos++
			return ret
		}
		// The value falls in the current bucket.
		ret := int64(math.Max(hi.hist.NotNullCount()-float64(hi.totalSubstracted), 0) / float64(hi.hist.NDV))
		hi.totalSubstracted += ret
		return ret
	}
}

// finish cleans the unfinished subtraction.
func (hi *histIter) finish() {
	for i := hi.curBucketPos; i < len(hi.hist.Buckets); i++ {
		// Avoid the negative.
		if hi.hist.Buckets[i].Count < hi.totalSubstracted {
			hi.hist.Buckets[i].Count = 0
			continue
		}
		hi.hist.Buckets[i].Count -= hi.totalSubstracted
	}
}

type heapItem struct {
	item          *statistics.TopNMeta
	idx           int
	nextPosInTopN int
}

type topnHeap []*heapItem

func (h topnHeap) Len() int {
	return len(h)
}

func (h topnHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].item.Encoded, h[j].item.Encoded) < 0
}

func (h topnHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *topnHeap) Push(x any) {
	*h = append(*h, x.(*heapItem))
}

func (h *topnHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type topNMeataHeap []statistics.TopNMeta

func (h topNMeataHeap) Len() int {
	return len(h)
}

func (h topNMeataHeap) Less(i, j int) bool {
	return h[i].Count < h[j].Count
}

func (h topNMeataHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *topNMeataHeap) Push(x any) {
	*h = append(*h, x.(statistics.TopNMeta))
}

func (h *topNMeataHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type topNMerger struct {
	loc *time.Location

	multiwayMergingHeap topnHeap
	maxPossibleAdded    []int64
	histIters           []histIter
	affectedHist        []int
	finalTopNs          topNMeataHeap
	remainedTopNs       []statistics.TopNMeta
	cur                 struct {
		affectedTopNs *bitset.BitSet
		item          statistics.TopNMeta
	}

	n       int
	isIndex bool
	tp      byte
}

func (merger *topNMerger) checkCurrentAndMoveForward(nextVal *statistics.TopNMeta, position uint) error {
	// It's perf-sensitive path. Don't use defer.
	// Initializing the datum.
	d, err := statistics.TopNMetaValToDatum(merger.cur.item.Encoded, merger.tp, merger.isIndex, merger.loc)
	if err != nil {
		return err
	}
	merger.affectedHist = merger.affectedHist[:0]
	// The following codes might access the NextClear loop twice. Record it here for saving CPU.
	for histPos, found := merger.cur.affectedTopNs.NextClear(0); found; histPos, found = merger.cur.affectedTopNs.NextClear(histPos + 1) {
		merger.affectedHist = append(merger.affectedHist, int(histPos))
	}
	// Hacking skip.
	if len(merger.finalTopNs) >= merger.n {
		maxPossible := int64(0)
		for _, histPos := range merger.affectedHist {
			maxPossible += merger.maxPossibleAdded[histPos]
		}
		// The maximum possible added value still cannot make it replace the smallest topn.
		if maxPossible+int64(merger.cur.item.Count) < int64(merger.finalTopNs[0].Count) {
			merger.remainedTopNs = append(merger.remainedTopNs, statistics.TopNMeta{Encoded: merger.cur.item.Encoded, Count: merger.cur.item.Count})
			// Set the cur maintained to the next value.
			merger.cur.item.Encoded = nextVal.Encoded
			merger.cur.item.Count = nextVal.Count
			merger.cur.affectedTopNs.ClearAll()
			merger.cur.affectedTopNs.Set(position)
			return nil
		}
	}
	for _, histPos := range merger.affectedHist {
		// Remove the value from the hist and add it into the current maintained value.
		merger.cur.item.Count += uint64(merger.histIters[histPos].remove(&d))
	}
	// Size reaches the n, maintaining the heap.
	if merger.finalTopNs.Len() == merger.n {
		if merger.finalTopNs[0].Count < merger.cur.item.Count {
			merger.remainedTopNs = append(merger.remainedTopNs, merger.finalTopNs[0])
			merger.finalTopNs[0].Encoded = merger.cur.item.Encoded
			merger.finalTopNs[0].Count = merger.cur.item.Count
			heap.Fix(&merger.finalTopNs, 0)
		} else {
			merger.remainedTopNs = append(merger.remainedTopNs, merger.cur.item)
		}
	} else {
		// Otherwise the heap is not fulfilled.
		merger.finalTopNs = append(merger.finalTopNs, statistics.TopNMeta{Encoded: merger.cur.item.Encoded, Count: merger.cur.item.Count})
		if merger.finalTopNs.Len() == merger.n {
			heap.Init(&merger.finalTopNs)
		}
	}
	// Set the cur maintained to the next value.
	merger.cur.item.Encoded = nextVal.Encoded
	merger.cur.item.Count = nextVal.Count
	merger.cur.affectedTopNs.ClearAll()
	merger.cur.affectedTopNs.Set(position)
	return nil
}

func newMerger(
	loc *time.Location,
	topNs []*statistics.TopN,
	n uint32,
	hists []*statistics.Histogram,
	isIndex bool,
) *topNMerger {
	merger := &topNMerger{
		isIndex: isIndex,
		loc:     loc,
		n:       int(n),
	}
	merger.multiwayMergingHeap = make([]*heapItem, 0, len(topNs))
	for i, topN := range topNs {
		if topN.Num() == 0 {
			continue
		}
		heap.Push(&merger.multiwayMergingHeap, &heapItem{
			item:          &topN.TopN[0],
			idx:           i,
			nextPosInTopN: 1,
		})
	}
	if merger.multiwayMergingHeap.Len() == 0 {
		// If there's no topn to merge, return a nil merger.
		return nil
	}
	merger.tp = hists[0].Tp.GetType()
	merger.maxPossibleAdded = make([]int64, len(hists))
	for i, hist := range hists {
		curMax := int64(hist.NotNullCount() / float64(hist.NDV))
		for _, bkt := range hist.Buckets {
			curMax = max(curMax, bkt.Repeat)
		}
		merger.maxPossibleAdded[i] = curMax
	}
	merger.histIters = make([]histIter, len(hists))
	for i, hist := range hists {
		merger.histIters[i].hist = hist
	}
	merger.cur.affectedTopNs = bitset.New(uint(len(hists)))
	merger.finalTopNs = make([]statistics.TopNMeta, 0, n)
	merger.remainedTopNs = make([]statistics.TopNMeta, 0, n)
	merger.affectedHist = make([]int, 0, len(hists))
	return merger
}

// MergePartTopN2GlobalTopN is used to merge the partition-level topN to global-level topN.
// The input parameters:
//  1. `topNs` are the partition-level topNs to be merged.
//  2. `n` is the size of the global-level topN.
//     Notice: This value can be 0 and has no default value, we must explicitly specify this value.
//  3. `hists` are the partition-level histograms.
//     Some values not in topN may be placed in the histogram.
//     We need it here to make the value in the global-level TopN more accurate.
//
// The output parameters:
//  1. `*TopN` is the final global-level topN.
//  2. `[]TopNMeta` is the left topN value from the partition-level TopNs,
//     but is not placed to global-level TopN. We should put them back to histogram latter.
//  3. `[]*Histogram` are the partition-level histograms which
//     just delete some values when we merge the global-level topN.
//
// The function use the merging operation based on the property that the topn items are ordered.
// And it does a heuristic short-cutting:
// - for newly seen merged topn item, it's total occurrence is the sum in the topn and the sum in the hist.
// - its occurrence in each hist is calculated as row_num_hist / ndv_in_hist or 0 or the bucket bound.
// - so we can calculate its maximum possible added value to max(row_num_hist / ndv_in_hist, bucket bound of each bucket) for each hist + sum_in_topn
// - we don't need to actually check it histogram by histogram if the maximum possible occurrence is still smaller than smallest maintained topn.
// This short-cutting will save the CPU time.
func MergePartTopN2GlobalTopN(
	loc *time.Location,
	topNs []*statistics.TopN,
	n uint32,
	hists []*statistics.Histogram,
	isIndex bool,
	killer *sqlkiller.SQLKiller,
) (*statistics.TopN, []statistics.TopNMeta, []*statistics.Histogram, error) {
	merger := newMerger(loc, topNs, n, hists, isIndex)
	if merger == nil {
		return nil, nil, hists, nil
	}
	firstTime := true
	for {
		if err := killer.HandleSignal(); err != nil {
			return nil, nil, nil, err
		}
		if merger.multiwayMergingHeap.Len() == 0 {
			break
		}
		head := merger.multiwayMergingHeap[0]
		headTopN := head.item
		if head.nextPosInTopN < topNs[head.idx].Num() {
			head.item = &topNs[head.idx].TopN[head.nextPosInTopN]
			head.nextPosInTopN++
			heap.Fix(&merger.multiwayMergingHeap, 0)
		} else {
			heap.Pop(&merger.multiwayMergingHeap)
		}
		// Init the cur when we first enter the heap.
		if firstTime {
			merger.cur.item.Encoded = headTopN.Encoded
			merger.cur.item.Count = headTopN.Count
			merger.cur.affectedTopNs.Set(uint(head.idx))
			firstTime = false
			continue
		}
		cmp := bytes.Compare(merger.cur.item.Encoded, headTopN.Encoded)
		// The heap's head move forward.
		if cmp < 0 {
			err := merger.checkCurrentAndMoveForward(headTopN, uint(head.idx))
			if err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		// The cmp result cannot be 1 because the value is strictly increasing.
		// Here is cmp == 0.
		merger.cur.item.Count += headTopN.Count
		merger.cur.affectedTopNs.Set(uint(head.idx))
	}

	// Next val and the position is useless
	err := merger.checkCurrentAndMoveForward(&merger.cur.item, 0)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, iter := range merger.histIters {
		if err := killer.HandleSignal(); err != nil {
			return nil, nil, nil, err
		}
		iter.finish()
	}
	statistics.SortTopnMeta(merger.finalTopNs)
	statistics.SortTopnMeta(merger.remainedTopNs)
	var globalTopN statistics.TopN
	globalTopN.TopN = merger.finalTopNs
	globalTopN.Sort()
	return &globalTopN, merger.remainedTopNs, hists, nil
}

// MergePartTopN2GlobalTopNForAnalyzeVer1 is the old implementation for the deprecated analyze_version = 1;
func MergePartTopN2GlobalTopNForAnalyzeVer1(
	loc *time.Location,
	topNs []*statistics.TopN,
	n uint32,
	hists []*statistics.Histogram,
	isIndex bool,
	killer *sqlkiller.SQLKiller,
) (*statistics.TopN, []statistics.TopNMeta, []*statistics.Histogram, error) {
	partNum := len(topNs)
	// Different TopN structures may hold the same value, we have to merge them.
	counter := make(map[hack.MutableString]float64)
	// datumMap is used to store the mapping from the string type to datum type.
	// The datum is used to find the value in the histogram.
	datumMap := statistics.NewDatumMapCache()
	for _, topN := range topNs {
		if err := killer.HandleSignal(); err != nil {
			return nil, nil, nil, err
		}
		// Ignore the empty topN.
		if topN.TotalCount() == 0 {
			continue
		}

		for _, val := range topN.TopN {
			encodedVal := hack.String(val.Encoded)
			_, exists := counter[encodedVal]
			counter[encodedVal] += float64(val.Count)
			if exists {
				// We have already calculated the encodedVal from the histogram, so just continue to next topN value.
				continue
			}

			// We need to check whether the value corresponding to encodedVal is contained in other partition-level stats.
			// 1. Check the topN first.
			// 2. If the topN doesn't contain the value corresponding to encodedVal. We should check the histogram.
			for j := 0; j < partNum; j++ {
				if err := killer.HandleSignal(); err != nil {
					return nil, nil, nil, err
				}

				if topNs[j].FindTopN(val.Encoded) != -1 {
					continue
				}
				// Get the encodedVal from the hists[j]
				datum, exists := datumMap.Get(encodedVal)
				if !exists {
					d, err := datumMap.Put(val, encodedVal, hists[0].Tp.GetType(), isIndex, loc)
					if err != nil {
						return nil, nil, nil, err
					}
					datum = d
				}
				// Get the row count which the value is equal to the encodedVal from histogram.
				count, _ := hists[j].EqualRowCount(nil, datum, isIndex)
				if count != 0 {
					counter[encodedVal] += count
					// Remove the value corresponding to encodedVal from the histogram.
					hists[j].BinarySearchRemoveVal(statistics.TopNMeta{Encoded: datum.GetBytes(), Count: uint64(count)})
				}
			}
		}
	}

	numTop := len(counter)
	if numTop == 0 {
		return nil, nil, hists, nil
	}
	sorted := make([]statistics.TopNMeta, 0, numTop)
	for value, cnt := range counter {
		data := hack.Slice(string(value))
		sorted = append(sorted, statistics.TopNMeta{Encoded: data, Count: uint64(cnt)})
	}
	globalTopN, leftTopN := statistics.GetMergedTopNFromSortedSlice(sorted, n)
	return globalTopN, leftTopN, hists, nil
}

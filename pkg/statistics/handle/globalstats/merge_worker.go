// Copyright 2022 PingCAP, Inc.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// TopN4Merge is used to merge topn
type TopN4Merge struct {
	*statistics.TopN
	bm *bloom.BloomFilter
}

// FindTopN finds topn
func (t *TopN4Merge) FindTopN(d []byte) int {
	if !t.bm.Test(d) {
		return -1
	}
	return t.TopN.FindTopN(d)
}

// CheckEmptyTopNs checks whether all TopNs are empty.
func CheckEmptyTopNs(topNs []*TopN4Merge) bool {
	count := uint64(0)
	for _, topN := range topNs {
		count += topN.TotalCount()
		if count != 0 {
			return false
		}
	}
	return count == 0
}

// MergePartTopN2GlobalTopN is used to merge the partition-level topN to global-level topN.
// The input parameters:
//  1. `topNs` are the partition-level topNs to be merged.
//  2. `n` is the size of the global-level topN. Notice: This value can be 0 and has no default value, we must explicitly specify this value.
//  3. `hists` are the partition-level histograms. Some values not in topN may be placed in the histogram. We need it here to make the value in the global-level TopN more accurate.
//
// The output parameters:
//  1. `*TopN` is the final global-level topN.
//  2. `[]TopNMeta` is the left topN value from the partition-level TopNs, but is not placed to global-level TopN. We should put them back to histogram latter.
//  3. `[]*Histogram` are the partition-level histograms which just delete some values when we merge the global-level topN.
func MergePartTopN2GlobalTopN(loc *time.Location, version int, topNs []*TopN4Merge, n uint32, hists []*statistics.Histogram,
	isIndex bool, killed *uint32) (*statistics.TopN, []statistics.TopNMeta, []*statistics.Histogram, error) {
	if CheckEmptyTopNs(topNs) {
		return nil, nil, hists, nil
	}
	partNum := len(topNs)
	// Different TopN structures may hold the same value, we have to merge them.
	counter := make(map[hack.MutableString]float64)
	// datumMap is used to store the mapping from the string type to datum type.
	// The datum is used to find the value in the histogram.
	datumMap := statistics.NewDatumMapCache()
	for i, topN := range topNs {
		if atomic.LoadUint32(killed) == 1 {
			return nil, nil, nil, errors.Trace(statistics.ErrQueryInterrupted)
		}
		if topN.TotalCount() == 0 {
			continue
		}
		for _, val := range topN.TopN.TopN {
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
				if atomic.LoadUint32(killed) == 1 {
					return nil, nil, nil, errors.Trace(statistics.ErrQueryInterrupted)
				}
				if (j == i && version >= 2) || topNs[j].FindTopN(val.Encoded) != -1 {
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

// StatsWrapper wrapper stats
type StatsWrapper struct {
	AllHg   []*statistics.Histogram
	AllTopN []*TopN4Merge
}

// NewStatsWrapper returns wrapper
func NewStatsWrapper(hg []*statistics.Histogram, topN []*statistics.TopN) *StatsWrapper {
	t := make([]*TopN4Merge, 0, len(topN))
	for _, top := range topN {
		bm := bloom.NewWithEstimates(500, 0.01)
		for _, raw := range top.TopN {
			bm.Add(raw.Encoded)
		}
		t = append(t, &TopN4Merge{
			TopN: top,
			bm:   bm,
		})
	}
	return &StatsWrapper{
		AllHg:   hg,
		AllTopN: t,
	}
}

type topnStatsMergeWorker struct {
	killed *uint32
	taskCh <-chan *TopnStatsMergeTask
	respCh chan<- *TopnStatsMergeResponse
	// the stats in the wrapper should only be read during the worker
	statsWrapper *StatsWrapper
	// shardMutex is used to protect `statsWrapper.AllHg`
	shardMutex []sync.Mutex
}

// NewTopnStatsMergeWorker returns topn merge worker
func NewTopnStatsMergeWorker(
	taskCh <-chan *TopnStatsMergeTask,
	respCh chan<- *TopnStatsMergeResponse,
	wrapper *StatsWrapper,
	killed *uint32) *topnStatsMergeWorker {
	worker := &topnStatsMergeWorker{
		taskCh: taskCh,
		respCh: respCh,
	}
	worker.statsWrapper = wrapper
	worker.shardMutex = make([]sync.Mutex, len(wrapper.AllHg))
	worker.killed = killed
	return worker
}

// TopnStatsMergeTask indicates a task for merge topn stats
type TopnStatsMergeTask struct {
	start int
	end   int
}

// NewTopnStatsMergeTask returns task
func NewTopnStatsMergeTask(start, end int) *TopnStatsMergeTask {
	return &TopnStatsMergeTask{
		start: start,
		end:   end,
	}
}

// TopnStatsMergeResponse indicates topn merge worker response
type TopnStatsMergeResponse struct {
	Err       error
	TopN      *statistics.TopN
	PopedTopn []statistics.TopNMeta
}

// Run runs topn merge like statistics.MergePartTopN2GlobalTopN
func (worker *topnStatsMergeWorker) Run(timeZone *time.Location, isIndex bool,
	n uint32,
	version int) {
	for task := range worker.taskCh {
		start := task.start
		end := task.end
		checkTopNs := worker.statsWrapper.AllTopN[start:end]
		allTopNs := worker.statsWrapper.AllTopN
		allHists := worker.statsWrapper.AllHg
		resp := &TopnStatsMergeResponse{}
		if CheckEmptyTopNs(checkTopNs) {
			worker.respCh <- resp
			return
		}
		partNum := len(allTopNs)
		// Different TopN structures may hold the same value, we have to merge them.
		counter := make(map[hack.MutableString]float64)
		// datumMap is used to store the mapping from the string type to datum type.
		// The datum is used to find the value in the histogram.
		datumMap := statistics.NewDatumMapCache()

		for i, topN := range checkTopNs {
			if atomic.LoadUint32(worker.killed) == 1 {
				resp.Err = errors.Trace(statistics.ErrQueryInterrupted)
				worker.respCh <- resp
				return
			}
			if topN.TotalCount() == 0 {
				continue
			}
			for _, val := range topN.TopN.TopN {
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
					if atomic.LoadUint32(worker.killed) == 1 {
						resp.Err = errors.Trace(statistics.ErrQueryInterrupted)
						worker.respCh <- resp
						return
					}
					if (j == i && version >= 2) || allTopNs[j].FindTopN(val.Encoded) != -1 {
						continue
					}
					// Get the encodedVal from the hists[j]
					datum, exists := datumMap.Get(encodedVal)
					if !exists {
						d, err := datumMap.Put(val, encodedVal, allHists[0].Tp.GetType(), isIndex, timeZone)
						if err != nil {
							resp.Err = err
							worker.respCh <- resp
							return
						}
						datum = d
					}
					// Get the row count which the value is equal to the encodedVal from histogram.
					count, _ := allHists[j].EqualRowCount(nil, datum, isIndex)
					if count != 0 {
						counter[encodedVal] += count
						// Remove the value corresponding to encodedVal from the histogram.
						worker.shardMutex[j].Lock()
						worker.statsWrapper.AllHg[j].BinarySearchRemoveVal(statistics.TopNMeta{Encoded: datum.GetBytes(), Count: uint64(count)})
						worker.shardMutex[j].Unlock()
					}
				}
			}
		}
		numTop := len(counter)
		if numTop == 0 {
			worker.respCh <- resp
			continue
		}
		sorted := make([]statistics.TopNMeta, 0, numTop)
		for value, cnt := range counter {
			data := hack.Slice(string(value))
			sorted = append(sorted, statistics.TopNMeta{Encoded: data, Count: uint64(cnt)})
		}
		globalTopN, leftTopN := statistics.GetMergedTopNFromSortedSlice(sorted, n)
		resp.TopN = globalTopN
		resp.PopedTopn = leftTopN
		worker.respCh <- resp
	}
}

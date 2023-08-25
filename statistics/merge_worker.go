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

package statistics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/hack"
)

// StatsWrapper wrapper stats
type StatsWrapper struct {
	AllHg   []*Histogram
	AllTopN []*TopN
}

// NewStatsWrapper returns wrapper
func NewStatsWrapper(hg []*Histogram, topN []*TopN) *StatsWrapper {
	return &StatsWrapper{
		AllHg:   hg,
		AllTopN: topN,
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
	TopN      *TopN
	PopedTopn []TopNMeta
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
		if checkEmptyTopNs(checkTopNs) {
			worker.respCh <- resp
			return
		}
		partNum := len(allTopNs)
		// Different TopN structures may hold the same value, we have to merge them.
		counter := make(map[hack.MutableString]float64)
		// datumMap is used to store the mapping from the string type to datum type.
		// The datum is used to find the value in the histogram.
		datumMap := newDatumMapCache()

		for i, topN := range checkTopNs {
			if atomic.LoadUint32(worker.killed) == 1 {
				resp.Err = errors.Trace(ErrQueryInterrupted)
				worker.respCh <- resp
				return
			}
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
					if atomic.LoadUint32(worker.killed) == 1 {
						resp.Err = errors.Trace(ErrQueryInterrupted)
						worker.respCh <- resp
						return
					}
					if (j == i && version >= 2) || allTopNs[j].findTopN(val.Encoded) != -1 {
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
						worker.statsWrapper.AllHg[j].BinarySearchRemoveVal(TopNMeta{Encoded: datum.GetBytes(), Count: uint64(count)})
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
		sorted := make([]TopNMeta, 0, numTop)
		for value, cnt := range counter {
			data := hack.Slice(string(value))
			sorted = append(sorted, TopNMeta{Encoded: data, Count: uint64(cnt)})
		}
		globalTopN, leftTopN := GetMergedTopNFromSortedSlice(sorted, n)
		resp.TopN = globalTopN
		resp.PopedTopn = leftTopN
		worker.respCh <- resp
	}
}

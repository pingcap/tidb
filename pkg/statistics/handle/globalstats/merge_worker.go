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
	"time"

	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

// StatsWrapper wrapper stats
type StatsWrapper struct {
	AllHg   []*statistics.Histogram
	AllTopN []*statistics.TopN
}

// NewStatsWrapper returns wrapper
func NewStatsWrapper(hg []*statistics.Histogram, topN []*statistics.TopN) *StatsWrapper {
	return &StatsWrapper{
		AllHg:   hg,
		AllTopN: topN,
	}
}

type topnStatsMergeWorker struct {
	killer *sqlkiller.SQLKiller
	taskCh <-chan *TopnStatsMergeTask
	respCh chan<- *TopnStatsMergeResponse
	// the stats in the wrapper should only be read during the worker
	statsWrapper *StatsWrapper
	// Different TopN structures may hold the same value, we have to merge them.
	counter map[hack.MutableString]float64
	// shardMutex is used to protect `statsWrapper.AllHg`
	shardMutex []sync.Mutex
	mu         sync.Mutex
}

// NewTopnStatsMergeWorker returns topn merge worker
func NewTopnStatsMergeWorker(
	taskCh <-chan *TopnStatsMergeTask,
	respCh chan<- *TopnStatsMergeResponse,
	wrapper *StatsWrapper,
	killer *sqlkiller.SQLKiller) *topnStatsMergeWorker {
	worker := &topnStatsMergeWorker{
		taskCh:  taskCh,
		respCh:  respCh,
		counter: make(map[hack.MutableString]float64),
	}
	worker.statsWrapper = wrapper
	worker.shardMutex = make([]sync.Mutex, len(wrapper.AllHg))
	worker.killer = killer
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
	Err error
}

// Run runs topn merge like statistics.MergePartTopN2GlobalTopN
func (worker *topnStatsMergeWorker) Run(timeZone *time.Location, isIndex bool, version int) {
	for task := range worker.taskCh {
		start := task.start
		end := task.end
		checkTopNs := worker.statsWrapper.AllTopN[start:end]
		allTopNs := worker.statsWrapper.AllTopN
		allHists := worker.statsWrapper.AllHg
		resp := &TopnStatsMergeResponse{}

		partNum := len(allTopNs)

		// datumMap is used to store the mapping from the string type to datum type.
		// The datum is used to find the value in the histogram.
		datumMap := statistics.NewDatumMapCache()
		for i, topN := range checkTopNs {
			i = i + start
			if err := worker.killer.HandleSignal(); err != nil {
				resp.Err = err
				worker.respCh <- resp
				return
			}
			if topN.TotalCount() == 0 {
				continue
			}
			for _, val := range topN.TopN {
				encodedVal := hack.String(val.Encoded)
				worker.mu.Lock()
				_, exists := worker.counter[encodedVal]
				worker.counter[encodedVal] += float64(val.Count)
				if exists {
					worker.mu.Unlock()
					// We have already calculated the encodedVal from the histogram, so just continue to next topN value.
					continue
				}
				worker.mu.Unlock()
				// We need to check whether the value corresponding to encodedVal is contained in other partition-level stats.
				// 1. Check the topN first.
				// 2. If the topN doesn't contain the value corresponding to encodedVal. We should check the histogram.
				for j := 0; j < partNum; j++ {
					if err := worker.killer.HandleSignal(); err != nil {
						resp.Err = err
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
					worker.shardMutex[j].Lock()
					// Get the row count which the value is equal to the encodedVal from histogram.
					count, _ := allHists[j].EqualRowCount(nil, datum, isIndex)
					if count != 0 {
						// Remove the value corresponding to encodedVal from the histogram.
						worker.statsWrapper.AllHg[j].BinarySearchRemoveVal(statistics.TopNMeta{Encoded: datum.GetBytes(), Count: uint64(count)})
					}
					worker.shardMutex[j].Unlock()
					if count != 0 {
						worker.mu.Lock()
						worker.counter[encodedVal] += count
						worker.mu.Unlock()
					}
				}
			}
		}
		worker.respCh <- resp
	}
}

func (worker *topnStatsMergeWorker) Result() map[hack.MutableString]float64 {
	return worker.counter
}

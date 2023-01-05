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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
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
	TopN       *TopN
	PopedTopn  []TopNMeta
	RemoveVals [][]TopNMeta
	Err        error
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
		checkNum := len(checkTopNs)
		topNsNum := make([]int, checkNum)
		removeVals := make([][]TopNMeta, partNum)
		for i, topN := range checkTopNs {
			if topN == nil {
				topNsNum[i] = 0
				continue
			}
			topNsNum[i] = len(topN.TopN)
		}
		// Different TopN structures may hold the same value, we have to merge them.
		counter := make(map[hack.MutableString]float64)
		// datumMap is used to store the mapping from the string type to datum type.
		// The datum is used to find the value in the histogram.
		datumMap := make(map[hack.MutableString]types.Datum)

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
					if (j == i && version >= 2) || allTopNs[j].findTopN(val.Encoded) != -1 {
						continue
					}
					// Get the encodedVal from the hists[j]
					datum, exists := datumMap[encodedVal]
					if !exists {
						// If the datumMap does not have the encodedVal datum,
						// we should generate the datum based on the encoded value.
						// This part is copied from the function MergePartitionHist2GlobalHist.
						var d types.Datum
						if isIndex {
							d.SetBytes(val.Encoded)
						} else {
							var err error
							if types.IsTypeTime(allHists[0].Tp.GetType()) {
								// handle datetime values specially since they are encoded to int and we'll get int values if using DecodeOne.
								_, d, err = codec.DecodeAsDateTime(val.Encoded, allHists[0].Tp.GetType(), timeZone)
							} else if types.IsTypeFloat(allHists[0].Tp.GetType()) {
								_, d, err = codec.DecodeAsFloat32(val.Encoded, allHists[0].Tp.GetType())
							} else {
								_, d, err = codec.DecodeOne(val.Encoded)
							}
							if err != nil {
								resp.Err = err
								worker.respCh <- resp
								return
							}
						}
						datumMap[encodedVal] = d
						datum = d
					}
					// Get the row count which the value is equal to the encodedVal from histogram.
					count, _ := allHists[j].equalRowCount(datum, isIndex)
					if count != 0 {
						counter[encodedVal] += count
						// Remove the value corresponding to encodedVal from the histogram.
						removeVals[j] = append(removeVals[j], TopNMeta{Encoded: datum.GetBytes(), Count: uint64(count)})
					}
				}
			}
		}
		// record remove values
		resp.RemoveVals = removeVals

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
		globalTopN, leftTopN := getMergedTopNFromSortedSlice(sorted, n)
		resp.TopN = globalTopN
		resp.PopedTopn = leftTopN
		worker.respCh <- resp
	}
}

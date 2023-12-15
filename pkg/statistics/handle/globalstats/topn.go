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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
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
		return MergePartTopN2GlobalTopN(timeZone, version, wrapper.AllTopN, n, wrapper.AllHg, isIndex, killer)
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
func MergePartTopN2GlobalTopN(
	loc *time.Location,
	version int,
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
	for i, topN := range topNs {
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

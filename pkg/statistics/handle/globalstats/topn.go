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
	"github.com/tiancaiamao/gp"
)

func mergeGlobalStatsTopN(gp *gp.Pool, sc sessionctx.Context, wrapper *StatsWrapper,
	timeZone *time.Location, version int, n uint32, isIndex bool) (*statistics.TopN,
	[]statistics.TopNMeta, []*statistics.Histogram, error) {
	mergeConcurrency := sc.GetSessionVars().AnalyzePartitionMergeConcurrency
	killed := &sc.GetSessionVars().Killed
	// use original method if concurrency equals 1 or for version1
	if mergeConcurrency < 2 {
		return statistics.MergePartTopN2GlobalTopN(timeZone, version, wrapper.AllTopN, n, wrapper.AllHg, isIndex, killed)
	}
	batchSize := len(wrapper.AllTopN) / mergeConcurrency
	if batchSize < 1 {
		batchSize = 1
	} else if batchSize > MaxPartitionMergeBatchSize {
		batchSize = MaxPartitionMergeBatchSize
	}
	return MergeGlobalStatsTopNByConcurrency(gp, mergeConcurrency, batchSize, wrapper, timeZone, version, n, isIndex, killed)
}

// MergeGlobalStatsTopNByConcurrency merge partition topN by concurrency
// To merge global stats topn by concurrency, we will separate the partition topn in concurrency part and deal it with different worker.
// mergeConcurrency is used to control the total concurrency of the running worker, and mergeBatchSize is sued to control
// the partition size for each worker to solve it
func MergeGlobalStatsTopNByConcurrency(gp *gp.Pool, mergeConcurrency, mergeBatchSize int, wrapper *StatsWrapper,
	timeZone *time.Location, version int, n uint32, isIndex bool, killed *uint32) (*statistics.TopN,
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
	for i := 0; i < mergeConcurrency; i++ {
		worker := NewTopnStatsMergeWorker(taskCh, respCh, wrapper, killed)
		wg.Add(1)
		gp.Go(func() {
			defer wg.Done()
			worker.Run(timeZone, isIndex, n, version)
		})
	}
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()
	close(respCh)
	resps := make([]*TopnStatsMergeResponse, 0)

	// handle Error
	hasErr := false
	errMsg := make([]string, 0)
	for resp := range respCh {
		if resp.Err != nil {
			hasErr = true
			errMsg = append(errMsg, resp.Err.Error())
		}
		resps = append(resps, resp)
	}
	if hasErr {
		return nil, nil, nil, errors.New(strings.Join(errMsg, ","))
	}

	// fetch the response from each worker and merge them into global topn stats
	sorted := make([]statistics.TopNMeta, 0, mergeConcurrency)
	leftTopn := make([]statistics.TopNMeta, 0)
	for _, resp := range resps {
		if resp.TopN != nil {
			sorted = append(sorted, resp.TopN.TopN...)
		}
		leftTopn = append(leftTopn, resp.PopedTopn...)
	}

	globalTopN, popedTopn := statistics.GetMergedTopNFromSortedSlice(sorted, n)

	result := append(leftTopn, popedTopn...)
	statistics.SortTopnMeta(result)
	return globalTopN, result, wrapper.AllHg, nil
}

// Copyright 2016 PingCAP, Inc.
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

package copr

import (
	"bytes"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/tikv"
	"slices"
)

// isFakeRegionError matches the pre-dispatch sentinel in the downstream client-go.
// The downstream version does not expose config/retry.IsFakeRegionError.
func isFakeRegionError(err *errorpb.Error) bool {
	return err != nil && err.GetEpochNotMatch() != nil && len(err.GetEpochNotMatch().CurrentRegions) == 0
}

// canRebuildWholeStoreBatch permits a rebuild only before any task was dispatched.
func (worker *copIteratorWorker) canRebuildWholeStoreBatch(rpcCtx *tikv.RPCContext, resp *coprocessor.Response, task *copTask) bool {
	if rpcCtx != nil || !worker.req.AllowBatchTaskDataMerge || len(task.batchTaskList) == 0 {
		return false
	}
	if !isFakeRegionError(resp.GetRegionError()) {
		return false
	}
	return len(resp.Data) == 0 && len(resp.GetBatchResponses()) == 0
}

// rebuildStoreBatchAfterRegionCacheMiss restores batching without mutating the shared request.
func (worker *copIteratorWorker) rebuildStoreBatchAfterRegionCacheMiss(bo *Backoffer, task *copTask) (*copTaskResult, error) {
	ranges := task.ranges.ToRanges()
	for _, child := range task.batchTaskList {
		ranges = append(ranges, child.task.ranges.ToRanges()...)
	}
	// batchTaskList is a map, so collecting children loses key order. Sort here
	// rather than relying on buildCopTasks' repair path, which logs non-monotonic
	// input as an error.
	slices.SortFunc(ranges, func(a, b kv.KeyRange) int {
		if cmp := bytes.Compare(a.StartKey, b.StartKey); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.EndKey, b.EndKey)
	})

	// BuildCopIterator cleared StoreBatchSize on the shared request so ordinary
	// retries stay flat. Restore it on a copy; mutating the shared request would
	// accidentally enable batching for unrelated retries.
	req := *worker.req
	req.StoreBatchSize = len(task.batchTaskList)
	remains, err := buildCopTasks(bo, NewKeyRanges(ranges), &buildCopTaskOpt{
		req:                         &req,
		cache:                       worker.store.GetRegionCache(),
		respChan:                    false,
		eventCb:                     task.eventCb,
		ignoreTiKVClientReadTimeout: true,
	})
	if err != nil {
		return nil, err
	}
	return &copTaskResult{remains: remains}, nil
}

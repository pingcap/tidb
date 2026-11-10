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
	"context"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	tikvutil "github.com/tikv/client-go/v2/util"
	"sync/atomic"
	"testing"
	"time"
)

type recordingRunawayChecker struct {
	resourcegroup.RunawayChecker
	processedKeys atomic.Int64
}

func (c *recordingRunawayChecker) CheckThresholds(_ *tikvutil.RUDetails, processedKeys int64, _ error) error {
	c.processedKeys.Add(processedKeys)
	return nil
}

func TestHandleBatchCopResponseMergedAndUnansweredTasks(t *testing.T) {
	bo := backoff.NewBackofferWithVars(context.Background(), 3000, nil)
	var batched, fallback atomic.Uint64
	executionStats := &copIteratorRuntimeStats{}
	runawayChecker := &recordingRunawayChecker{}
	worker := &copIteratorWorker{
		req:                     &kv.Request{RunawayChecker: runawayChecker},
		kvclient:                &txnsnapshot.ClientHelper{},
		storeBatchedNum:         &batched,
		storeBatchedFallbackNum: &fallback,
		stats:                   executionStats,
	}

	mergedTask := &copTask{taskID: 1}
	inlineTask := &copTask{taskID: 2}
	inlineData := []byte("inline-task-data")
	responses, remains, err := worker.handleBatchCopResponse(bo, nil, &coprocessor.Response{
		BatchResponses: []*coprocessor.StoreBatchTaskResponse{
			{
				TaskId:                 mergedTask.taskID,
				DataMergedIntoResponse: true,
				ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
					ScanDetailV2: &kvrpcpb.ScanDetailV2{ProcessedVersions: 7},
				},
			},
			{TaskId: inlineTask.taskID, Data: inlineData},
		},
	}, map[uint64]*batchedCopTask{
		mergedTask.taskID: {task: mergedTask},
		inlineTask.taskID: {task: inlineTask},
	})
	require.NoError(t, err)
	require.Empty(t, remains)
	require.Len(t, responses, 1)
	require.Equal(t, inlineData, []byte(responses[0].pbResp.Data))
	require.Equal(t, uint64(2), batched.Load())
	require.Zero(t, fallback.Load())
	collectedStats := (&copIterator{stats: executionStats}).CollectUnconsumedCopRuntimeStats()
	require.Len(t, collectedStats, 1)
	require.Equal(t, int64(7), collectedStats[0].ScanDetail.ProcessedKeys)
	require.Equal(t, int64(7), runawayChecker.processedKeys.Load())

	unansweredTask := &copTask{taskID: 3}
	responses, remains, err = worker.handleBatchCopResponse(bo, nil, &coprocessor.Response{}, map[uint64]*batchedCopTask{
		unansweredTask.taskID: {task: unansweredTask},
	})
	require.NoError(t, err)
	require.Empty(t, responses)
	require.Len(t, remains, 1)
	require.Same(t, unansweredTask, remains[0])
	require.Equal(t, uint64(2), batched.Load())
	require.Equal(t, uint64(1), fallback.Load())
}

func TestHandleStoreBatchRegionCacheMiss(t *testing.T) {
	// A false positive would replay ranges whose child result may already be in
	// the response, so every uncertain response shape must use flat reconciliation.
	worker := &copIteratorWorker{req: &kv.Request{}}
	task := &copTask{batchTaskList: map[uint64]*batchedCopTask{1: {}}}
	resp := &coprocessor.Response{RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}}
	require.False(t, worker.canRebuildWholeStoreBatch(nil, resp, task))
	worker.req.AllowBatchTaskDataMerge = true
	require.False(t, worker.canRebuildWholeStoreBatch(&tikv.RPCContext{}, resp, task))
	resp.BatchResponses = []*coprocessor.StoreBatchTaskResponse{{TaskId: 1}}
	require.False(t, worker.canRebuildWholeStoreBatch(nil, resp, task))
	resp.BatchResponses = nil
	resp.Data = []byte{1}
	require.False(t, worker.canRebuildWholeStoreBatch(nil, resp, task))
	resp.Data = nil
	resp.RegionError = &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}
	require.False(t, worker.canRebuildWholeStoreBatch(nil, resp, task))

	mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	_, regionIDs, _ := testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
	tikvStore, err := tikv.NewTestTiKVStore(mockClient, pdClient, nil, nil, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tikvStore.Close()) })
	copStore, err := NewStore(tikvStore, nil)
	require.NoError(t, err)
	t.Cleanup(copStore.Close)

	ctx := context.Background()
	killed := uint32(0)
	req := &kv.Request{
		Tp:                      kv.ReqTypeAnalyze,
		StoreType:               kv.TiKV,
		KeyRanges:               kv.NewNonPartitionedKeyRanges(BuildKeyRanges("a", "z")),
		Concurrency:             1,
		StoreBatchSize:          3,
		AllowBatchTaskDataMerge: true,
	}
	req.RequestSource.RequestSourceInternal = true
	it, errRes := (&CopClient{store: copStore}).BuildCopIterator(ctx, req, kv.NewVariables(&killed), &kv.ClientSendOption{})
	require.Nil(t, errRes)
	require.Len(t, it.tasks, 1)
	require.Zero(t, req.StoreBatchSize)
	failedTask := it.tasks[0]
	require.Len(t, failedTask.batchTaskList, 3)

	newRegionID, newPeerID := cluster.AllocID(), cluster.AllocID()
	cluster.Split(regionIDs[0], newRegionID, []byte("d"), []uint64{newPeerID}, newPeerID)

	cache := copStore.GetRegionCache()
	cache.InvalidateCachedRegion(failedTask.region)
	for _, child := range failedTask.batchTaskList {
		cache.InvalidateCachedRegion(child.task.region)
	}

	result, err := newCopIteratorWorker(it, nil).handleCopResponse(
		backoff.NewBackofferWithVars(ctx, 3000, nil), nil,
		&copResponse{pbResp: &coprocessor.Response{RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}}},
		nil, nil, failedTask, 0,
	)
	require.NoError(t, err)
	require.Zero(t, req.StoreBatchSize)
	require.Len(t, result.remains, 2)
	require.Len(t, result.remains[0].batchTaskList, 3)
	require.Empty(t, result.remains[1].batchTaskList)
}

// copTimeoutRecorder records the timeout of every coprocessor RPC and answers
// it with an empty response.
type copTimeoutRecorder struct {
	tikv.Client
	copTimeouts []time.Duration
}

func (c *copTimeoutRecorder) SendRequest(
	ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration,
) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdCop {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	c.copTimeouts = append(c.copTimeouts, timeout)
	return &tikvrpc.Response{Resp: &coprocessor.Response{}}, nil
}

func TestStoreBatchCopRPCTimeout(t *testing.T) {
	// Four Regions on one store build one primary task carrying three batched
	// children, so the RPC holds four tasks in total.
	const batchedChildren = 3
	sendStoreBatch := func(t *testing.T, tweak func(req *kv.Request)) time.Duration {
		mockClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
		require.NoError(t, err)
		testutils.BootstrapWithMultiRegions(cluster, []byte("g"), []byte("n"), []byte("t"))
		recorder := &copTimeoutRecorder{Client: mockClient}
		tikvStore, err := tikv.NewTestTiKVStore(recorder, pdClient, nil, nil, 0)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tikvStore.Close()) })
		copStore, err := NewStore(tikvStore, nil)
		require.NoError(t, err)
		t.Cleanup(copStore.Close)

		ctx := context.Background()
		req := &kv.Request{
			Tp:                      kv.ReqTypeAnalyze,
			StoreType:               kv.TiKV,
			KeyRanges:               kv.NewNonPartitionedKeyRanges(BuildKeyRanges("a", "z")),
			StoreBatchSize:          batchedChildren,
			AllowBatchTaskDataMerge: true,
		}
		tweak(req)
		it, errRes := (&CopClient{store: copStore}).BuildCopIterator(ctx, req, kv.NewVariables(nil), &kv.ClientSendOption{})
		require.Nil(t, errRes)
		require.Len(t, it.tasks, 1)
		require.Len(t, it.tasks[0].batchTaskList, batchedChildren)

		_, err = newCopIteratorWorker(it, nil).handleTaskOnce(backoff.NewBackofferWithVars(ctx, 3000, nil), it.tasks[0])
		require.NoError(t, err)
		require.Len(t, recorder.copTimeouts, 1)
		return recorder.copTimeouts[0]
	}

	coprReqTimeout := config.GetGlobalConfig().TiKVClient.CoprReqTimeout
	require.Positive(t, coprReqTimeout)

	t.Run("serial batch gets the per-task budget for every task", func(t *testing.T) {
		// TiKV runs a serial store batch under the top task's deadline, which
		// client-go derives from this RPC timeout.
		timeout := sendStoreBatch(t, func(req *kv.Request) {
			req.ExecuteBatchTasksSerially = true
		})
		require.Equal(t, coprReqTimeout*(batchedChildren+1), timeout)
	})

	t.Run("serial batch scales tikv_client_read_timeout", func(t *testing.T) {
		timeout := sendStoreBatch(t, func(req *kv.Request) {
			req.ExecuteBatchTasksSerially = true
			req.TiKVClientReadTimeout = 500
		})
		require.Equal(t, 500*time.Millisecond*(batchedChildren+1), timeout)
	})

	t.Run("concurrent batch keeps the single-request timeout", func(t *testing.T) {
		// Concurrently executed children each get their own deadline on TiKV,
		// so the RPC budget must not grow with the batch.
		timeout := sendStoreBatch(t, func(*kv.Request) {})
		require.Equal(t, coprReqTimeout, timeout)
	})
}

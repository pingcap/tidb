// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type MPPClient struct {
	store *tikvStore
}

func (c *batchCopTask) GetAddress() string {
	return c.storeAddr
}

func (c *MPPClient) ScheduleMPPTasks(ctx context.Context, req *kv.MPPScheduleRequest) ([]kv.MPPTask, error) {
	ctx = context.WithValue(ctx, txnStartKey, req.StartTS)
	bo := NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, nil)
	tasks, err := buildBatchCopTasks(bo, c.store.regionCache, &copRanges{mid: req.KeyRanges}, kv.TiFlash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mppTasks := make([]kv.MPPTask, 0, len(tasks))
	for _, copTask := range tasks {
		mppTasks = append(mppTasks, copTask)
	}
	return mppTasks, nil
}

type MPPResponse struct {
	pbResp *mpp.MPPDataPacket

	err error
}

func (m *MPPResponse) GetData() []byte {
	return m.pbResp.Data
}

func (m *MPPResponse) GetStartKey() kv.Key {
	return nil
}

func (m *MPPResponse) GetCopRuntimeStats() *CopRuntimeStats {
	return nil
}

func (m *MPPResponse) MemSize() int64 {
	return int64(m.pbResp.Size())
}

func (m *MPPResponse) RespTime() time.Duration {
	return 0
}

type MPPIterator struct {
	store *tikvStore

	tasks    []*kv.MPPDispatchRequest
	finishCh chan struct{}

	startTs uint64

	respChan chan *MPPResponse

	rpcCancel *RPCCanceller

	wg sync.WaitGroup

	closed uint32
}

func (m *MPPIterator) run(ctx context.Context) {
	for _, task := range m.tasks {
		m.wg.Add(1)
		bo := NewBackoffer(ctx, copNextMaxBackoff)
		go m.handleDispatchReq(ctx, bo, task)
	}
	m.wg.Wait()
	close(m.respChan)
}

func (m *MPPIterator) sendError(err error) {
	m.sendToRespCh(&MPPResponse{err: err})
}

func (m *MPPIterator) sendToRespCh(resp *MPPResponse) (exit bool) {
	select {
	case m.respChan <- resp:
	case <-m.finishCh:
		exit = true
	}
	return
}

// TODO:: Consider that which way is better:
// - dispatch all tasks at once, and connect tasks at second.
// - dispatch tasks and establish connection at the same time.
func (m *MPPIterator) handleDispatchReq(ctx context.Context, bo *Backoffer, req *kv.MPPDispatchRequest) {
	defer func() {
		m.wg.Done()
	}()
	sender := NewRegionBatchRequestSender(m.store.regionCache, m.store.client)
	var regionInfos []*coprocessor.RegionInfo
	originalTask := req.Task.(*batchCopTask)
	for _, task := range originalTask.copTasks {
		regionInfos = append(regionInfos, &coprocessor.RegionInfo{
			RegionId: task.task.region.id,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: task.task.region.confVer,
				Version: task.task.region.ver,
			},
			Ranges: task.task.ranges.toPBRanges(),
		})
	}

	taskMeta := &mpp.TaskMeta{QueryTs: req.StartTs, TaskId: req.ID, Address: originalTask.storeAddr}

	mppReq := &mpp.DispatchTaskRequest{
		Meta:        taskMeta,
		EncodedPlan: req.Data,
		Timeout:     10,
		SchemaVer:   req.SchemaVar,
		Regions:     regionInfos,
	}

	wrappedReq := tikvrpc.NewRequest(tikvrpc.CmdMPPTask, mppReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = kv.TiFlash

	// TODO: Handle dispatch task response correctly, including retry logic and cancel logic.
	rpcResp, _, _, err := sender.sendStreamReqToAddr(bo, originalTask.copTasks, wrappedReq, readTimeoutShort)

	if err != nil {
		m.sendError(err)
		return
	}

	// No matter what the rpc error is, we won't retry the mpp dispatch tasks.
	// TODO: If we want to retry, we must redo the plan fragment cutting and task scheduling.
	// That's a hard job but we can try it in the future.
	if sender.rpcError != nil {
		m.sendError(sender.rpcError)
		return
	}

	realResp := rpcResp.Resp.(*mpp.DispatchTaskResponse)

	if realResp.Error != nil {
		m.sendError(errors.New(realResp.Error.Error.Msg))
		return
	}

	if !req.IsRoot {
		return
	}

	connReq := &mpp.EstablishMPPConnectionRequest{
		ServerMeta: taskMeta,
		ClientMeta: &mpp.TaskMeta{
			QueryTs: req.StartTs,
			TaskId:  -1,
		},
	}

	wrappedReq = tikvrpc.NewRequest(tikvrpc.CmdMPPConn, connReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = kv.TiFlash

	// Drain result from root task.
	// We don't need to process any special error. When we meet errors, just let it fail.
	rpcResp, err = m.store.client.SendRequest(bo.ctx, originalTask.storeAddr, wrappedReq, ReadTimeoutUltraLong)

	if err != nil {
		m.sendError(err)
		return
	}

	stream := rpcResp.Resp.(*tikvrpc.MPPStreamResponse)
	defer stream.Close()

	resp := stream.MPPDataPacket
	if resp == nil {
		return
	}

	// TODO: cancel the whole process when some error happens
	for {
		err := m.handleMPPStreamResponse(resp, req)
		resp, err = stream.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return
			}

			if err1 := bo.Backoff(boTiKVRPC, errors.Errorf("recv stream response error: %v", err)); err1 != nil {
				if errors.Cause(err) == context.Canceled {
					logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
				} else {
					logutil.BgLogger().Info("stream unknown error", zap.Error(err))
				}
			}
			m.sendToRespCh(&MPPResponse{
				err: errors.New(realResp.Error.Error.Msg),
			})
			return
		}
	}
}

// TODO: Test the case that user cancels the query.
func (m *MPPIterator) Close() error {
	if atomic.CompareAndSwapUint32(&m.closed, 0, 1) {
		close(m.finishCh)
	}
	m.rpcCancel.CancelAll()
	m.wg.Wait()
	return nil
}

func (m *MPPIterator) handleMPPStreamResponse(response *mpp.MPPDataPacket, req *kv.MPPDispatchRequest) (err error) {
	if response.Error != nil {
		err = errors.Errorf("other error for mpp stream: %s", response.Error.Error.Msg)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", req.StartTs),
			zap.String("storeAddr", req.Task.GetAddress()),
			zap.Error(err))
		return err
	}

	resp := &MPPResponse{
		pbResp: response,
	}

	m.sendToRespCh(resp)
	return
}

func (m *MPPIterator) nextImpl(ctx context.Context) (resp *MPPResponse, ok bool, exit bool, err error) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-m.respChan:
			return
		case <-ticker.C:
			//TODO: kill query
		case <-m.finishCh:
			exit = true
			return
		case <-ctx.Done():
			if atomic.CompareAndSwapUint32(&m.closed, 0, 1) {
				close(m.finishCh)
			}
			exit = true
			return
		}
	}
}

func (m *MPPIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	resp, ok, closed, err := m.nextImpl(ctx)
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err = m.store.CheckVisibility(m.startTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (c *MPPClient) DispatchMPPTasks(ctx context.Context, dispatchReqs []*kv.MPPDispatchRequest) kv.Response {
	iter := &MPPIterator{
		store:     c.store,
		tasks:     dispatchReqs,
		finishCh:  make(chan struct{}),
		rpcCancel: NewRPCanceller(),
		respChan:  make(chan *MPPResponse, 4096),
	}
	ctx = context.WithValue(ctx, RPCCancellerCtxKey{}, iter.rpcCancel)

	// TODO: Process the case of query cancellation.
	go iter.run(ctx)
	return iter
}

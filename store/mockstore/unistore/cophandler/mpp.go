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

package cophandler

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/unistore/client"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/uber-go/atomic"
)

const (
	// MPPErrTunnelNotFound means you can't find an expected tunnel.
	MPPErrTunnelNotFound = iota
	// MPPErrEstablishConnMultiTimes means we receive the Establish requests at least twice.
	MPPErrEstablishConnMultiTimes
)

const (
	taskInit int32 = iota
	taskRunning
	taskFailed
	taskFinished
)

// MPPTaskHandler exists in a single store.
type MPPTaskHandler struct {
	// When a connect request comes, it contains server task (source) and client task (target), Exchanger dataCh set will find dataCh by client task.
	tunnelSetLock sync.Mutex
	TunnelSet     map[int64]*ExchangerTunnel

	Meta      *mpp.TaskMeta
	RPCClient client.Client

	Status atomic.Int32
	Err    error
}

// HandleMPPDispatch handle DispatchTaskRequest
func (h *MPPTaskHandler) HandleMPPDispatch(ctx context.Context, req *mpp.DispatchTaskRequest, storeAddr string, storeID uint64) (*mpp.DispatchTaskResponse, error) {
	// At first register task to store.
	kvContext := kvrpcpb.Context{
		RegionId:    req.Regions[0].RegionId,
		RegionEpoch: req.Regions[0].RegionEpoch,
		// this is a hack to reuse task id in kvContext to pass mpp task id
		TaskId: uint64(h.Meta.TaskId),
		Peer:   &metapb.Peer{StoreId: storeID},
	}
	copReq := &coprocessor.Request{
		Tp:      kv.ReqTypeDAG,
		Data:    req.EncodedPlan,
		StartTs: req.Meta.StartTs,
		Context: &kvContext,
	}
	for _, regionMeta := range req.Regions {
		copReq.Ranges = append(copReq.Ranges, regionMeta.Ranges...)
	}
	rpcReq := &tikvrpc.Request{
		Type:    tikvrpc.CmdCop,
		Req:     copReq,
		Context: kvContext,
	}
	go h.run(ctx, storeAddr, rpcReq, time.Hour)
	return &mpp.DispatchTaskResponse{}, nil
}

func (h *MPPTaskHandler) run(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) {
	h.Status.Store(taskRunning)
	_, err := h.RPCClient.SendRequest(ctx, addr, req, timeout)
	// TODO: Remove itself after execution is closed.
	if err != nil {
		h.Err = err
		h.Status.Store(taskFailed)
	} else {
		h.Status.Store(taskFinished)
	}
}

// HandleEstablishConn handles EstablishMPPConnectionRequest
func (h *MPPTaskHandler) HandleEstablishConn(_ context.Context, req *mpp.EstablishMPPConnectionRequest) (*ExchangerTunnel, error) {
	meta := req.ReceiverMeta
	for i := 0; i < 10; i++ {
		h.tunnelSetLock.Lock()
		tunnel, ok := h.TunnelSet[meta.TaskId]
		h.tunnelSetLock.Unlock()
		if ok {
			return tunnel, nil
		}
		time.Sleep(time.Second)
	}
	return nil, errors.Errorf("cannot find client task %d registered in server task %d", meta.TaskId, req.SenderMeta.TaskId)
}

func (h *MPPTaskHandler) registerTunnel(tunnel *ExchangerTunnel) error {
	taskID := tunnel.targetTask.TaskId
	h.tunnelSetLock.Lock()
	defer h.tunnelSetLock.Unlock()
	_, ok := h.TunnelSet[taskID]
	if ok {
		return errors.Errorf("task id %d has been registered", taskID)
	}
	h.TunnelSet[taskID] = tunnel
	return nil
}

func (h *MPPTaskHandler) getAndActiveTunnel(req *mpp.EstablishMPPConnectionRequest) (*ExchangerTunnel, *mpp.Error, error) {
	targetID := req.ReceiverMeta.TaskId
	if tunnel, ok := h.TunnelSet[targetID]; ok {
		if tunnel.active {
			// We find the dataCh, but the dataCh has been used.
			return nil, &mpp.Error{Code: MPPErrEstablishConnMultiTimes, Msg: "dataCh has been connected"}, nil
		}
		tunnel.active = true
		return tunnel, nil, nil
	}
	// We dont find this dataCh, may be task not ready or have been deleted.
	return nil, &mpp.Error{Code: MPPErrTunnelNotFound, Msg: "task not found, please wait for a while"}, nil
}

// ExchangerTunnel contains a channel that can transfer data.
// Only One Sender and Receiver use this channel, so it's safe to close it by sender.
type ExchangerTunnel struct {
	DataCh chan *tipb.Chunk

	sourceTask *mpp.TaskMeta // source task is nearer to the data source
	targetTask *mpp.TaskMeta // target task is nearer to the client end , as tidb.

	active bool
	ErrCh  chan error
}

// RecvChunk recive tipb chunk
func (tunnel *ExchangerTunnel) RecvChunk() (tipbChunk *tipb.Chunk, err error) {
	tipbChunk = <-tunnel.DataCh
	select {
	case err = <-tunnel.ErrCh:
	default:
	}
	return tipbChunk, err
}

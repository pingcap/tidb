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

package mocktikv

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
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

// mppTaskHandler exists in a single store.
type mppTaskHandler struct {
	*rpcHandler

	// When a connect request comes, it contains server task (source) and client task (target), Exchanger dataCh set will find dataCh by client task.
	tunnelSet map[int64]*exchangerTunnel

	meta   *mpp.TaskMeta
	client *RPCClient

	exec executor // the executor that running on this task

	status atomic.Int32
	err    error
}

func (h *mppTaskHandler) run() {
	h.status.Store(taskRunning)
	_, err := h.exec.Next(context.Background())
	// TODO: Remove itself after execution is closed.
	if err != nil {
		h.err = err
		h.status.Store(taskFailed)
	} else {
		h.status.Store(taskFinished)
	}
}

func (h *mppTaskHandler) registerTunnel(tunnel *exchangerTunnel) error {
	taskID := tunnel.targetTask.TaskId
	_, ok := h.tunnelSet[taskID]
	if ok {
		return errors.Errorf("task id %d has been registered", taskID)
	}
	h.tunnelSet[taskID] = tunnel
	return nil
}

func (h *mppTaskHandler) getAndActiveTunnel(req *mpp.EstablishMPPConnectionRequest) (*exchangerTunnel, *mpp.Error, error) {
	targetID := req.ReceiverMeta.TaskId
	if tunnel, ok := h.tunnelSet[targetID]; ok {
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

// exchangerTunnel contains a channel that can transfer data.
// Only One Sender and Receiver use this channel, so it's safe to close it by sender.
type exchangerTunnel struct {
	dataCh chan [][]byte

	sourceTask *mpp.TaskMeta // source task is nearer to the data source
	targetTask *mpp.TaskMeta // target task is nearer to the client end , as tidb.

	active bool
	errCh  chan error
}

type exchangeServer struct {
	*tipb.ExchangeSender

	tunnels       []*exchangerTunnel
	outputOffsets []uint32

	meta *mpp.TaskMeta

	err error
	src executor
}

func (e *exchangeServer) SetSrcExec(ch executor) {
	e.src = ch
}

func (e *exchangeServer) GetSrcExec() executor {
	return e.src
}

func (e *exchangeServer) Counts() []int64 {
	return e.src.Counts()
}

func (e *exchangeServer) ExecDetails() []*execDetail {
	return e.src.ExecDetails()
}

func (e *exchangeServer) ResetCounts() {
	e.src.ResetCounts()
}

func (e *exchangeServer) Cursor() ([]byte, bool) {
	return e.src.Cursor()
}

func (e *exchangeServer) Next(ctx context.Context) ([][]byte, error) {
	defer e.Close()
	var row [][]byte
	for {
		row, e.err = e.src.Next(ctx)
		if e.err != nil {
			return nil, nil
		}
		if row == nil {
			// Close all the tunnels and return.
			return nil, nil
		}
		var nrow [][]byte
		for _, offset := range e.outputOffsets {
			nrow = append(nrow, row[offset])
		}
		// TODO: the target side may crash. We should check timeout here.
		switch e.Tp {
		case tipb.ExchangeType_Broadcast:
			for _, tunnel := range e.tunnels {
				tunnel.dataCh <- nrow
			}
		case tipb.ExchangeType_PassThrough:
			e.tunnels[0].dataCh <- nrow
		default:
			e.err = errors.New("Unsupported exchange type")
		}
	}
}

func (e *exchangeServer) Close() {
	// close every dataCh whenever the sender finishes work.
	for _, tunnel := range e.tunnels {
		if e.err != nil {
			tunnel.errCh <- e.err
		}
		close(tunnel.dataCh)
	}
}

// There are many sender and single receiver, we should close channel by receiver.
type exchangeClient struct {
	*tipb.ExchangeReceiver

	result chan [][]byte

	err error

	closeCh chan struct{}

	sync.Once
	wg sync.WaitGroup
}

func (e *exchangeClient) waitAllWorkersDone() {
	e.wg.Wait() // wait all workers is done.
	e.Do(func() { close(e.closeCh) })
}

func (e *exchangeClient) getResultAfterClosed() ([][]byte, error) {
	e.wg.Wait() // wait all workers is done.
	if e.err != nil {
		return nil, e.err
	}
	select {
	case row := <-e.result:
		return row, nil
	default:
		return nil, nil
	}
}

func (e *exchangeClient) Next(ctx context.Context) ([][]byte, error) {
	select {
	case <-e.closeCh:
		return e.getResultAfterClosed()
	default:
		select {
		case row := <-e.result:
			return row, nil
		case <-e.closeCh:
			return e.getResultAfterClosed()
		}
	}
}

type mockMPPConnStream struct {
	mockClientStream

	tunnel *exchangerTunnel
}

func (mock *mockMPPConnStream) RecvRow() (row [][]byte, err error) {
	row = <-mock.tunnel.dataCh
	select {
	case err = <-mock.tunnel.errCh:
	default:
	}
	return row, err
}

func (mock *mockMPPConnStream) Recv() (packet *mpp.MPPDataPacket, err error) {
	row, err := mock.RecvRow()
	if err != nil {
		return &mpp.MPPDataPacket{Error: &mpp.Error{Msg: err.Error()}}, nil
	}
	if row == nil {
		return nil, io.EOF
	}
	var chunk tipb.Chunk
	for _, col := range row {
		chunk.RowsData = append(chunk.RowsData, col...)
	}
	res := tipb.SelectResponse{
		Chunks: []tipb.Chunk{chunk},
	}
	raw, err := res.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &mpp.MPPDataPacket{
		Data: raw,
	}, nil
}

func (e *exchangeClient) runTunnelWorker(h *mppTaskHandler, meta *mpp.TaskMeta) {
	e.wg.Add(1)
	var (
		maxRetryTime = 3
		retryTime    = 0
		err          error
	)

	for retryTime < maxRetryTime {
		err = e.EstablishConnAndReceiveData(h, meta)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
		retryTime++
	}
	if err != nil {
		e.Do(func() {
			close(e.closeCh)
		})
	}
	e.wg.Done()
}

func (e *exchangeClient) EstablishConnAndReceiveData(h *mppTaskHandler, meta *mpp.TaskMeta) error {
	req := &mpp.EstablishMPPConnectionRequest{ReceiverMeta: h.meta, SenderMeta: meta}
	rpcReq := tikvrpc.NewRequest(tikvrpc.CmdMPPConn, req, kvrpcpb.Context{})
	rpcResp, err := h.client.SendRequest(context.Background(), meta.Address, rpcReq, 3600*time.Second)
	if err != nil {
		return errors.Trace(err)
	}

	resp := rpcResp.Resp.(*tikvrpc.MPPStreamResponse)
	stream := resp.Tikv_EstablishMPPConnectionClient.(*mockMPPConnStream)
	for {
		row, err := stream.RecvRow()
		if err != nil {
			return errors.Trace(err)
		}
		// if row is finished, the wg of e should sub by one
		if row == nil {
			return nil
		}
		select {
		case <-e.closeCh:
		default:
			select {
			case e.result <- row:
			case <-e.closeCh:
				return nil
			}
		}
	}
}

func (e *exchangeClient) SetSrcExec(executor) {}

func (e *exchangeClient) GetSrcExec() executor {
	return nil
}

func (e *exchangeClient) Counts() []int64 {
	return nil
}

func (e *exchangeClient) ExecDetails() []*execDetail {
	return nil
}

func (e *exchangeClient) ResetCounts() {}

func (e *exchangeClient) Cursor() ([]byte, bool) {
	return nil, false
}

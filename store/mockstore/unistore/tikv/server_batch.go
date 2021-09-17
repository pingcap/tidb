// Copyright 2019-present PingCAP, Inc.
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

package tikv

import (
	"context"
	"io"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type respIDPair struct {
	resp *tikvpb.BatchCommandsResponse_Response
	id   uint64
}

func (p respIDPair) appendTo(batchResp *tikvpb.BatchCommandsResponse) {
	batchResp.Responses = append(batchResp.Responses, p.resp)
	batchResp.RequestIds = append(batchResp.RequestIds, p.id)
}

type batchRequestHandler struct {
	respCh  chan respIDPair
	closeCh chan struct{}

	svr    *Server
	stream tikvpb.Tikv_BatchCommandsServer
}

const (
	respChanSize = 1024
)

// BatchCommands implements the TiKVServer interface.
func (svr *Server) BatchCommands(stream tikvpb.Tikv_BatchCommandsServer) error {
	h := &batchRequestHandler{
		respCh:  make(chan respIDPair, respChanSize),
		closeCh: make(chan struct{}),
		svr:     svr,
		stream:  stream,
	}
	return h.start()
}

func (h *batchRequestHandler) start() error {
	ctx, cancel := context.WithCancel(h.stream.Context())
	go func() {
		if err := h.dispatchBatchRequest(ctx); err != nil {
			log.Warn("dispatch batch request failed", zap.Error(err))
		}
		close(h.closeCh)
	}()

	err := h.collectAndSendResponse()
	cancel()
	return err
}

func (h *batchRequestHandler) handleRequest(id uint64, req *tikvpb.BatchCommandsRequest_Request) {
	resp, err := h.svr.handleBatchRequest(h.stream.Context(), req)
	if err != nil {
		log.Warn("handle batch request failed", zap.Error(err))
		return
	}
	h.respCh <- respIDPair{id: id, resp: resp}
}

func (h *batchRequestHandler) dispatchBatchRequest(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchReq, err := h.stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		for i, req := range batchReq.GetRequests() {
			id := batchReq.GetRequestIds()[i]
			go h.handleRequest(id, req)
		}
	}
}

func (h *batchRequestHandler) collectAndSendResponse() error {
	batchResp := &tikvpb.BatchCommandsResponse{}
	for {
		for i := range batchResp.Responses {
			batchResp.Responses[i] = nil
		}
		batchResp.Responses = batchResp.Responses[:0]
		batchResp.RequestIds = batchResp.RequestIds[:0]
		select {
		case <-h.closeCh:
		case resp := <-h.respCh:
			resp.appendTo(batchResp)
		}
		chLen := len(h.respCh)
		for i := 0; i < chLen; i++ {
			resp := <-h.respCh
			resp.appendTo(batchResp)
		}
		if len(batchResp.Responses) == 0 {
			// closeCh must have been closed or there would be at least one resp.
			return nil
		}
		if err := h.stream.Send(batchResp); err != nil {
			return err
		}
	}
}

func (svr *Server) handleBatchRequest(ctx context.Context, req *tikvpb.BatchCommandsRequest_Request) (*tikvpb.BatchCommandsResponse_Response, error) {
	switch req := req.GetCmd().(type) {
	case *tikvpb.BatchCommandsRequest_Request_Get:
		res, err := svr.KvGet(ctx, req.Get)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Get{Get: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Scan:
		res, err := svr.KvScan(ctx, req.Scan)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Scan{Scan: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_PessimisticLock:
		res, err := svr.KvPessimisticLock(ctx, req.PessimisticLock)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_PessimisticLock{PessimisticLock: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_PessimisticRollback:
		res, err := svr.KVPessimisticRollback(ctx, req.PessimisticRollback)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_PessimisticRollback{PessimisticRollback: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_TxnHeartBeat:
		res, err := svr.KvTxnHeartBeat(ctx, req.TxnHeartBeat)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_TxnHeartBeat{TxnHeartBeat: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_CheckTxnStatus:
		res, err := svr.KvCheckTxnStatus(ctx, req.CheckTxnStatus)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_CheckTxnStatus{CheckTxnStatus: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Prewrite:
		res, err := svr.KvPrewrite(ctx, req.Prewrite)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Prewrite{Prewrite: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Commit:
		res, err := svr.KvCommit(ctx, req.Commit)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Commit{Commit: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Cleanup:
		res, err := svr.KvCleanup(ctx, req.Cleanup)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Cleanup{Cleanup: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_BatchGet:
		res, err := svr.KvBatchGet(ctx, req.BatchGet)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_BatchGet{BatchGet: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_BatchRollback:
		res, err := svr.KvBatchRollback(ctx, req.BatchRollback)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_BatchRollback{BatchRollback: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_ScanLock:
		res, err := svr.KvScanLock(ctx, req.ScanLock)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_ScanLock{ScanLock: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_ResolveLock:
		res, err := svr.KvResolveLock(ctx, req.ResolveLock)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_ResolveLock{ResolveLock: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_GC:
		res, err := svr.KvGC(ctx, req.GC)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_GC{GC: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_DeleteRange:
		res, err := svr.KvDeleteRange(ctx, req.DeleteRange)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_DeleteRange{DeleteRange: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawGet:
		res, err := svr.RawGet(ctx, req.RawGet)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawGet{RawGet: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawBatchGet:
		res, err := svr.RawBatchGet(ctx, req.RawBatchGet)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawBatchGet{RawBatchGet: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawPut:
		res, err := svr.RawPut(ctx, req.RawPut)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawPut{RawPut: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawBatchPut:
		res, err := svr.RawBatchPut(ctx, req.RawBatchPut)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawBatchPut{RawBatchPut: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawDelete:
		res, err := svr.RawDelete(ctx, req.RawDelete)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawDelete{RawDelete: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawBatchDelete:
		res, err := svr.RawBatchDelete(ctx, req.RawBatchDelete)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawBatchDelete{RawBatchDelete: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawScan:
		res, err := svr.RawScan(ctx, req.RawScan)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawScan{RawScan: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_RawDeleteRange:
		res, err := svr.RawDeleteRange(ctx, req.RawDeleteRange)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_RawDeleteRange{RawDeleteRange: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Coprocessor:
		res, err := svr.Coprocessor(ctx, req.Coprocessor)
		if err != nil {
			return nil, err
		}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Coprocessor{Coprocessor: res}}, nil
	case *tikvpb.BatchCommandsRequest_Request_Empty:
		res := &tikvpb.BatchCommandsEmptyResponse{TestId: req.Empty.TestId}
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Empty{Empty: res}}, nil
	}
	return nil, nil
}

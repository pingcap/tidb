// Copyright 2021 PingCAP, Inc.
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

package mockcopr

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type coprRPCHandler struct {
	streamTimeout chan *tikvrpc.Lease
	done          chan struct{}
}

// NewCoprRPCHandler creates a handler to process coprocessor requests.
func NewCoprRPCHandler() testutils.CoprRPCHandler {
	ch := make(chan *tikvrpc.Lease, 1024)
	done := make(chan struct{})
	go tikvrpc.CheckStreamTimeoutLoop(ch, done)
	return &coprRPCHandler{
		streamTimeout: ch,
		done:          done,
	}
}

func (mc *coprRPCHandler) HandleCmdCop(reqCtx *kvrpcpb.Context, session *testutils.RPCSession, r *coprocessor.Request) *coprocessor.Response {
	if err := session.CheckRequestContext(reqCtx); err != nil {
		return &coprocessor.Response{RegionError: err}
	}
	var res *coprocessor.Response
	switch r.GetTp() {
	case kv.ReqTypeDAG:
		res = coprHandler{session}.handleCopDAGRequest(r)
	case kv.ReqTypeAnalyze:
		res = coprHandler{session}.handleCopAnalyzeRequest(r)
	case kv.ReqTypeChecksum:
		res = coprHandler{session}.handleCopChecksumRequest(r)
	default:
		panic(fmt.Sprintf("unknown coprocessor request type: %v", r.GetTp()))
	}
	return res
}

func (mc *coprRPCHandler) HandleBatchCop(ctx context.Context, reqCtx *kvrpcpb.Context, session *testutils.RPCSession, r *coprocessor.BatchRequest, timeout time.Duration) (*tikvrpc.BatchCopStreamResponse, error) {
	if err := session.CheckRequestContext(reqCtx); err != nil {
		return &tikvrpc.BatchCopStreamResponse{
			Tikv_BatchCoprocessorClient: &mockBathCopErrClient{Error: err},
			BatchResponse: &coprocessor.BatchResponse{
				OtherError: err.Message,
			},
		}, nil
	}
	ctx1, cancel := context.WithCancel(ctx)
	batchCopStream, err := coprHandler{session}.handleBatchCopRequest(ctx1, r)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	batchResp := &tikvrpc.BatchCopStreamResponse{Tikv_BatchCoprocessorClient: batchCopStream}
	batchResp.Lease.Cancel = cancel
	batchResp.Timeout = timeout
	mc.streamTimeout <- &batchResp.Lease

	first, err := batchResp.Recv()
	if err != nil {
		return nil, errors.Trace(err)
	}
	batchResp.BatchResponse = first
	return batchResp, nil
}

func (mc *coprRPCHandler) HandleCopStream(ctx context.Context, reqCtx *kvrpcpb.Context, session *testutils.RPCSession, r *coprocessor.Request, timeout time.Duration) (*tikvrpc.CopStreamResponse, error) {
	if err := session.CheckRequestContext(reqCtx); err != nil {
		return &tikvrpc.CopStreamResponse{
			Tikv_CoprocessorStreamClient: &mockCopStreamErrClient{Error: err},
			Response: &coprocessor.Response{
				RegionError: err,
			},
		}, nil
	}
	ctx1, cancel := context.WithCancel(ctx)
	copStream, err := coprHandler{session}.handleCopStream(ctx1, r)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	streamResp := &tikvrpc.CopStreamResponse{
		Tikv_CoprocessorStreamClient: copStream,
	}
	streamResp.Lease.Cancel = cancel
	streamResp.Timeout = timeout
	mc.streamTimeout <- &streamResp.Lease

	first, err := streamResp.Recv()
	if err != nil {
		return nil, errors.Trace(err)
	}
	streamResp.Response = first
	return streamResp, nil
}

func (mc *coprRPCHandler) Close() {
	close(mc.done)
}

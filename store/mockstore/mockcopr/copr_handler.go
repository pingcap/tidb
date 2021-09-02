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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/testutils"
)

type coprHandler struct {
	*testutils.RPCSession
}

func (h coprHandler) handleBatchCopRequest(ctx context.Context, req *coprocessor.BatchRequest) (*mockBatchCopDataClient, error) {
	client := &mockBatchCopDataClient{}
	for _, ri := range req.Regions {
		cop := coprocessor.Request{
			Tp:      kv.ReqTypeDAG,
			Data:    req.Data,
			StartTs: req.StartTs,
			Ranges:  ri.Ranges,
		}
		_, exec, dagReq, err := h.buildDAGExecutor(&cop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chunk, err := drainRowsFromExecutor(ctx, exec, dagReq)
		if err != nil {
			return nil, errors.Trace(err)
		}
		client.chunks = append(client.chunks, chunk)
	}
	return client, nil
}

func drainRowsFromExecutor(ctx context.Context, e executor, req *tipb.DAGRequest) (tipb.Chunk, error) {
	var chunk tipb.Chunk
	for {
		row, err := e.Next(ctx)
		if err != nil {
			return chunk, errors.Trace(err)
		}
		if row == nil {
			return chunk, nil
		}
		for _, offset := range req.OutputOffsets {
			chunk.RowsData = append(chunk.RowsData, row[offset]...)
		}
	}
}

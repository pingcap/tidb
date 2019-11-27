// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

// CoprocessorDAGHandler uses to handle cop dag request.
type CoprocessorDAGHandler struct {
	sctx    sessionctx.Context
	resp    *coprocessor.Response
	selResp *tipb.SelectResponse
	dagReq  *tipb.DAGRequest
}

// NewCoprocessorDAGHandler creates a new CoprocessorDAGHandler.
func NewCoprocessorDAGHandler(sctx sessionctx.Context, resp *coprocessor.Response) *CoprocessorDAGHandler {
	return &CoprocessorDAGHandler{
		sctx:    sctx,
		resp:    resp,
		selResp: &tipb.SelectResponse{},
	}
}

// HandleCopDAGRequest handles the coprocessor request.
func (h *CoprocessorDAGHandler) HandleCopDAGRequest(ctx context.Context, req *coprocessor.Request) *coprocessor.Response {
	e, err := h.buildDAGExecutor(req)
	if err != nil {
		return h.buildResp(err)
	}

	err = e.Open(ctx)
	if err != nil {
		return h.buildResp(err)
	}

	chk := newFirstChunk(e)
	tps := e.base().retFieldTypes
	for {
		chk.Reset()
		err = Next(ctx, e, chk)
		if err != nil {
			break
		}
		if chk.NumRows() == 0 {
			break
		}
		err = h.fillUpData4SelectResponse(chk, tps)
		if err != nil {
			break
		}
	}
	return h.buildResp(err)
}

func (h *CoprocessorDAGHandler) buildDAGExecutor(req *coprocessor.Request) (Executor, error) {
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}
	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, errors.Trace(err)
	}

	h.sctx.GetSessionVars().StmtCtx.SetFlagsFromPBFlag(dagReq.Flags)
	h.sctx.GetSessionVars().StmtCtx.TimeZone, err = timeutil.ConstructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	if err != nil {
		return nil, errors.Trace(err)
	}
	h.dagReq = dagReq
	e, err := h.buildDAG(dagReq.Executors)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e, nil
}

func (h *CoprocessorDAGHandler) buildDAG(executors []*tipb.Executor) (Executor, error) {
	is := h.sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
	bp := core.NewPBPlanBuilder(h.sctx, is)
	plan, err := bp.BuildPhysicalPlanFromPB(executors)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b := newExecutorBuilder(h.sctx, is)
	return b.build(plan), nil
}

func (h *CoprocessorDAGHandler) fillUpData4SelectResponse(chk *chunk.Chunk, tps []*types.FieldType) error {
	var err error
	switch h.dagReq.EncodeType {
	case tipb.EncodeType_TypeDefault:
		err = h.encodeDefault(chk, tps)
	case tipb.EncodeType_TypeChunk:
		err = h.encodeChunk(chk, tps)
	default:
		return errors.Errorf("unknown dag encode type, %v", h.dagReq.EncodeType)
	}
	h.selResp.EncodeType = h.dagReq.EncodeType
	return err
}

func (h *CoprocessorDAGHandler) buildResp(err error) *coprocessor.Response {
	resp := h.resp
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	data, err := proto.Marshal(h.selResp)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	resp.Data = data
	return resp
}

func (h *CoprocessorDAGHandler) encodeChunk(chk *chunk.Chunk, colTypes []*types.FieldType) error {
	colOrdinal := h.dagReq.OutputOffsets
	chunks := h.selResp.Chunks
	respColTypes := make([]*types.FieldType, 0, len(colOrdinal))
	for _, ordinal := range colOrdinal {
		respColTypes = append(respColTypes, colTypes[ordinal])
	}
	encoder := chunk.NewCodec(respColTypes)
	chunks = append(chunks, tipb.Chunk{})
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
	h.selResp.Chunks = chunks
	return nil
}

func (h *CoprocessorDAGHandler) encodeDefault(chk *chunk.Chunk, tps []*types.FieldType) error {
	colOrdinal := h.dagReq.OutputOffsets
	chunks := h.selResp.Chunks
	for i := 0; i < chk.NumRows(); i++ {
		requestedRow := make([]byte, 0)
		row := chk.GetRow(i)
		for _, ordinal := range colOrdinal {
			data, err := codec.EncodeValue(h.sctx.GetSessionVars().StmtCtx, nil, row.GetDatum(int(ordinal), tps[ordinal]))
			if err != nil {
				return err
			}
			requestedRow = append(requestedRow, data...)
		}
		chunks = h.appendRow(chunks, requestedRow, i)
	}
	h.selResp.Chunks = chunks
	return nil
}

const rowsPerChunk = 64

func (h *CoprocessorDAGHandler) appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

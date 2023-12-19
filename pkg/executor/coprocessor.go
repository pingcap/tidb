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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
)

func copHandlerCtx(ctx context.Context, req *coprocessor.Request) context.Context {
	source := req.Context.SourceStmt
	if source == nil {
		return ctx
	}

	traceInfo := &model.TraceInfo{
		ConnectionID: source.ConnectionId,
		SessionAlias: source.SessionAlias,
	}

	ctx = tracing.ContextWithTraceInfo(ctx, traceInfo)
	ctx = logutil.WithTraceFields(ctx, traceInfo)
	return ctx
}

// CoprocessorDAGHandler uses to handle cop dag request.
type CoprocessorDAGHandler struct {
	sctx   sessionctx.Context
	dagReq *tipb.DAGRequest
}

// NewCoprocessorDAGHandler creates a new CoprocessorDAGHandler.
func NewCoprocessorDAGHandler(sctx sessionctx.Context) *CoprocessorDAGHandler {
	return &CoprocessorDAGHandler{
		sctx: sctx,
	}
}

// HandleRequest handles the coprocessor request.
func (h *CoprocessorDAGHandler) HandleRequest(ctx context.Context, req *coprocessor.Request) *coprocessor.Response {
	ctx = copHandlerCtx(ctx, req)

	e, err := h.buildDAGExecutor(req)
	if err != nil {
		return h.buildErrorResponse(err)
	}

	err = exec.Open(ctx, e)
	if err != nil {
		return h.buildErrorResponse(err)
	}

	chk := exec.TryNewCacheChunk(e)
	tps := e.Base().RetFieldTypes()
	var totalChunks, partChunks []tipb.Chunk
	memTracker := h.sctx.GetSessionVars().StmtCtx.MemTracker
	for {
		chk.Reset()
		err = exec.Next(ctx, e, chk)
		if err != nil {
			return h.buildErrorResponse(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		partChunks, err = h.buildChunk(chk, tps)
		if err != nil {
			return h.buildErrorResponse(err)
		}
		for _, ch := range partChunks {
			memTracker.Consume(int64(ch.Size()))
		}
		totalChunks = append(totalChunks, partChunks...)
	}
	if err := exec.Close(e); err != nil {
		return h.buildErrorResponse(err)
	}
	return h.buildUnaryResponse(totalChunks)
}

// HandleStreamRequest handles the coprocessor stream request.
func (h *CoprocessorDAGHandler) HandleStreamRequest(ctx context.Context, req *coprocessor.Request, stream tikvpb.Tikv_CoprocessorStreamServer) error {
	ctx = copHandlerCtx(ctx, req)
	logutil.Logger(ctx).Debug("handle coprocessor stream request")

	e, err := h.buildDAGExecutor(req)
	if err != nil {
		return stream.Send(h.buildErrorResponse(err))
	}

	err = exec.Open(ctx, e)
	if err != nil {
		return stream.Send(h.buildErrorResponse(err))
	}

	chk := exec.TryNewCacheChunk(e)
	tps := e.Base().RetFieldTypes()
	for {
		chk.Reset()
		if err = exec.Next(ctx, e, chk); err != nil {
			return stream.Send(h.buildErrorResponse(err))
		}
		if chk.NumRows() == 0 {
			return h.buildResponseAndSendToStream(chk, tps, stream)
		}
		if err = h.buildResponseAndSendToStream(chk, tps, stream); err != nil {
			return stream.Send(h.buildErrorResponse(err))
		}
	}
}

func (h *CoprocessorDAGHandler) buildResponseAndSendToStream(chk *chunk.Chunk, tps []*types.FieldType, stream tikvpb.Tikv_CoprocessorStreamServer) error {
	chunks, err := h.buildChunk(chk, tps)
	if err != nil {
		return stream.Send(h.buildErrorResponse(err))
	}

	for i := range chunks {
		resp := h.buildStreamResponse(&chunks[i])
		if err = stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func (h *CoprocessorDAGHandler) buildDAGExecutor(req *coprocessor.Request) (exec.Executor, error) {
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}
	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if dagReq.User != nil {
		pm := privilege.GetPrivilegeManager(h.sctx)
		if pm != nil {
			h.sctx.GetSessionVars().User = &auth.UserIdentity{
				Username: dagReq.User.UserName,
				Hostname: dagReq.User.UserHost,
			}
			authName, authHost, success := pm.MatchIdentity(dagReq.User.UserName, dagReq.User.UserHost, false)
			if success && pm.GetAuthWithoutVerification(authName, authHost) {
				h.sctx.GetSessionVars().User.AuthUsername = authName
				h.sctx.GetSessionVars().User.AuthHostname = authHost
				h.sctx.GetSessionVars().ActiveRoles = pm.GetDefaultRoles(authName, authHost)
			}
		}
	}

	stmtCtx := h.sctx.GetSessionVars().StmtCtx

	tz, err := timeutil.ConstructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	if err != nil {
		return nil, errors.Trace(err)
	}
	h.sctx.GetSessionVars().TimeZone = tz
	stmtCtx.InitFromPBFlagAndTz(dagReq.Flags, tz)

	h.dagReq = dagReq
	is := h.sctx.GetInfoSchema().(infoschema.InfoSchema)
	// Build physical plan.
	bp := core.NewPBPlanBuilder(h.sctx, is, req.Ranges)
	plan, err := bp.Build(dagReq.Executors)
	if err != nil {
		return nil, errors.Trace(err)
	}
	plan = core.InjectExtraProjection(plan)
	// Build executor.
	b := newExecutorBuilder(h.sctx, is, nil)
	return b.build(plan), nil
}

func (h *CoprocessorDAGHandler) buildChunk(chk *chunk.Chunk, tps []*types.FieldType) (chunks []tipb.Chunk, err error) {
	switch h.dagReq.EncodeType {
	case tipb.EncodeType_TypeDefault:
		chunks, err = h.encodeDefault(chk, tps)
	case tipb.EncodeType_TypeChunk:
		chunks, err = h.encodeChunk(chk, tps)
	default:
		return nil, errors.Errorf("unknown DAG encode type: %v", h.dagReq.EncodeType)
	}
	return chunks, err
}

func (h *CoprocessorDAGHandler) buildUnaryResponse(chunks []tipb.Chunk) *coprocessor.Response {
	selResp := tipb.SelectResponse{
		Chunks:     chunks,
		EncodeType: h.dagReq.EncodeType,
	}
	if h.dagReq.CollectExecutionSummaries != nil && *h.dagReq.CollectExecutionSummaries {
		execSummary := make([]*tipb.ExecutorExecutionSummary, len(h.dagReq.Executors))
		for i := range execSummary {
			// TODO: Add real executor execution summary information.
			execSummary[i] = &tipb.ExecutorExecutionSummary{}
		}
		selResp.ExecutionSummaries = execSummary
	}
	data, err := proto.Marshal(&selResp)
	if err != nil {
		return h.buildErrorResponse(err)
	}
	return &coprocessor.Response{
		Data: data,
	}
}

func (h *CoprocessorDAGHandler) buildStreamResponse(chunk *tipb.Chunk) *coprocessor.Response {
	data, err := chunk.Marshal()
	if err != nil {
		return h.buildErrorResponse(err)
	}
	streamResponse := tipb.StreamResponse{
		Data: data,
	}
	var resp = &coprocessor.Response{}
	resp.Data, err = proto.Marshal(&streamResponse)
	if err != nil {
		resp.OtherError = err.Error()
	}
	return resp
}

func (*CoprocessorDAGHandler) buildErrorResponse(err error) *coprocessor.Response {
	return &coprocessor.Response{
		OtherError: err.Error(),
	}
}

func (h *CoprocessorDAGHandler) encodeChunk(chk *chunk.Chunk, colTypes []*types.FieldType) ([]tipb.Chunk, error) {
	colOrdinal := h.dagReq.OutputOffsets
	respColTypes := make([]*types.FieldType, 0, len(colOrdinal))
	for _, ordinal := range colOrdinal {
		respColTypes = append(respColTypes, colTypes[ordinal])
	}
	encoder := chunk.NewCodec(respColTypes)
	cur := tipb.Chunk{}
	cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
	return []tipb.Chunk{cur}, nil
}

func (h *CoprocessorDAGHandler) encodeDefault(chk *chunk.Chunk, tps []*types.FieldType) ([]tipb.Chunk, error) {
	colOrdinal := h.dagReq.OutputOffsets
	stmtCtx := h.sctx.GetSessionVars().StmtCtx
	requestedRow := make([]byte, 0)
	chunks := []tipb.Chunk{}
	errCtx := stmtCtx.ErrCtx()
	for i := 0; i < chk.NumRows(); i++ {
		requestedRow = requestedRow[:0]
		row := chk.GetRow(i)
		for _, ordinal := range colOrdinal {
			data, err := codec.EncodeValue(stmtCtx.TimeZone(), nil, row.GetDatum(int(ordinal), tps[ordinal]))
			err = errCtx.HandleError(err)
			if err != nil {
				return nil, err
			}
			requestedRow = append(requestedRow, data...)
		}
		chunks = h.appendRow(chunks, requestedRow, i)
	}
	return chunks, nil
}

const rowsPerChunk = 64

func (*CoprocessorDAGHandler) appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

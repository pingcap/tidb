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

// HandleCopDAGRequest handles the coprocessor request.
func HandleCopDAGRequest(ctx context.Context, sctx sessionctx.Context, req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	e, dagReq, err := buildDAGExecutor(sctx, req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}

	err = e.Open(ctx)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}

	selResp := &tipb.SelectResponse{}
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
		err = fillUpData4SelectResponse(selResp, dagReq, sctx, chk, tps)
		if err != nil {
			break
		}
	}
	return buildResp(selResp, err)
}

func buildDAGExecutor(sctx sessionctx.Context, req *coprocessor.Request) (Executor, *tipb.DAGRequest, error) {
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}
	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	sctx.GetSessionVars().StmtCtx.SetFlagsFromPBFlag(dagReq.Flags)
	sctx.GetSessionVars().StmtCtx.TimeZone, err = timeutil.ConstructTimeZone(dagReq.TimeZoneName, int(dagReq.TimeZoneOffset))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	e, err := buildDAG(sctx, dagReq.Executors)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return e, dagReq, nil
}

func buildDAG(sctx sessionctx.Context, executors []*tipb.Executor) (Executor, error) {
	var src core.PhysicalPlan
	is := sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
	bp := core.NewPBPlanBuilder(sctx, is)
	for i := 0; i < len(executors); i++ {
		curr, err := bp.PBToPhysicalPlan(executors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetChildren(src)
		src = curr
	}
	b := newExecutorBuilder(sctx, is)
	return b.build(src), nil
}

func fillUpData4SelectResponse(selResp *tipb.SelectResponse, dagReq *tipb.DAGRequest, sctx sessionctx.Context, chk *chunk.Chunk, tps []*types.FieldType) error {
	var err error
	switch dagReq.EncodeType {
	case tipb.EncodeType_TypeDefault:
		err = encodeDefault(sctx, selResp, chk, tps, dagReq.OutputOffsets)
	case tipb.EncodeType_TypeChunk:
		err = encodeChunk(selResp, chk, tps, dagReq.OutputOffsets)
	}
	return err
}

func buildResp(selResp *tipb.SelectResponse, err error) *coprocessor.Response {
	resp := &coprocessor.Response{}
	if err != nil {
		resp.OtherError = err.Error()
	}
	data, err := proto.Marshal(selResp)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	resp.Data = data
	return resp
}

func encodeChunk(selResp *tipb.SelectResponse, chk *chunk.Chunk, colTypes []*types.FieldType, colOrdinal []uint32) error {
	chunks := selResp.Chunks
	respColTypes := make([]*types.FieldType, 0, len(colOrdinal))
	for _, ordinal := range colOrdinal {
		respColTypes = append(respColTypes, colTypes[ordinal])
	}
	encoder := chunk.NewCodec(respColTypes)
	chunks = append(chunks, tipb.Chunk{})
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, encoder.Encode(chk)...)
	selResp.Chunks = chunks
	selResp.EncodeType = tipb.EncodeType_TypeChunk
	return nil
}

func encodeDefault(sctx sessionctx.Context, selResp *tipb.SelectResponse, chk *chunk.Chunk, tps []*types.FieldType, colOrdinal []uint32) error {
	chunks := selResp.Chunks
	for i := 0; i < chk.NumRows(); i++ {
		requestedRow := make([]byte, 0)
		row := chk.GetRow(i)
		for _, ordinal := range colOrdinal {
			data, err := codec.EncodeValue(sctx.GetSessionVars().StmtCtx, nil, row.GetDatum(int(ordinal), tps[ordinal]))
			if err != nil {
				return err
			}
			requestedRow = append(requestedRow, data...)
		}
		chunks = appendRow(chunks, requestedRow, i)
	}
	selResp.Chunks = chunks
	selResp.EncodeType = tipb.EncodeType_TypeDefault
	return nil
}

const rowsPerChunk = 64

func appendRow(chunks []tipb.Chunk, data []byte, rowCnt int) []tipb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, tipb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.RowsData = append(cur.RowsData, data...)
	return chunks
}

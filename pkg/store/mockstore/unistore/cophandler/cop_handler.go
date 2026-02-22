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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cophandler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/client"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tipb/go-tipb"
)

var globalLocationMap *locationMap = newLocationMap()

type locationMap struct {
	lmap map[string]*time.Location
	mu   sync.RWMutex
}

func newLocationMap() *locationMap {
	return &locationMap{
		lmap: make(map[string]*time.Location),
	}
}

func (l *locationMap) getLocation(name string) (*time.Location, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result, ok := l.lmap[name]
	return result, ok
}

func (l *locationMap) setLocation(name string, value *time.Location) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lmap[name] = value
}

// MPPCtx is the mpp execution context
type MPPCtx struct {
	RPCClient   client.Client
	StoreAddr   string
	TaskHandler *MPPTaskHandler
	Ctx         context.Context
}

// HandleCopRequest handles coprocessor request.
func HandleCopRequest(dbReader *dbreader.DBReader, lockStore *lockstore.MemStore, req *coprocessor.Request) *coprocessor.Response {
	return HandleCopRequestWithMPPCtx(dbReader, lockStore, req, nil)
}

// HandleCopRequestWithMPPCtx handles coprocessor request, actually, this is the updated version for
// HandleCopRequest(after mpp test is supported), however, go does not support function overloading,
// I have to rename it to HandleCopRequestWithMPPCtx.
func HandleCopRequestWithMPPCtx(dbReader *dbreader.DBReader, lockStore *lockstore.MemStore, req *coprocessor.Request, mppCtx *MPPCtx) *coprocessor.Response {
	switch req.Tp {
	case kv.ReqTypeDAG:
		if mppCtx != nil && mppCtx.TaskHandler != nil {
			return HandleMPPDAGReq(dbReader, req, mppCtx)
		}
		return handleCopDAGRequest(dbReader, lockStore, req)
	case kv.ReqTypeAnalyze:
		return handleCopAnalyzeRequest(dbReader, req)
	case kv.ReqTypeChecksum:
		return handleCopChecksumRequest(dbReader, req)
	}
	return &coprocessor.Response{OtherError: fmt.Sprintf("unsupported request type %d", req.GetTp())}
}

type dagContext struct {
	*evalContext
	keyspaceID    uint32
	dbReader      *dbreader.DBReader
	lockStore     *lockstore.MemStore
	resolvedLocks []uint64
	dagReq        *tipb.DAGRequest
	keyRanges     []*coprocessor.KeyRange
	startTS       uint64
}

// ExecutorListsToTree converts a list of executors to a tree.
func ExecutorListsToTree(exec []*tipb.Executor) *tipb.Executor {
	appendChild := func(parent *tipb.Executor, child *tipb.Executor) {
		switch parent.Tp {
		case tipb.ExecType_TypeAggregation:
			parent.Aggregation.Child = child
		case tipb.ExecType_TypeProjection:
			parent.Projection.Child = child
		case tipb.ExecType_TypeTopN:
			parent.TopN.Child = child
		case tipb.ExecType_TypeLimit:
			parent.Limit.Child = child
		case tipb.ExecType_TypeSelection:
			parent.Selection.Child = child
		case tipb.ExecType_TypeStreamAgg:
			parent.Aggregation.Child = child
		case tipb.ExecType_TypeIndexLookUp:
			parent.IndexLookup.Children = append(parent.IndexLookup.Children, child)
		default:
			panic("unsupported dag executor type")
		}
	}

	for i := range len(exec) - 1 {
		child := exec[i]
		parentIdx := i + 1
		if child.ParentIdx != nil {
			parentIdx = int(*child.ParentIdx)
		}
		if parentIdx <= i || parentIdx >= len(exec) {
			panic(fmt.Sprintf("invalid parentIdx: %d, for index: %d", parentIdx, i))
		}
		appendChild(exec[parentIdx], child)
	}

	return exec[len(exec)-1]
}

// handleCopDAGRequest handles coprocessor DAG request using MPP executors.
func handleCopDAGRequest(dbReader *dbreader.DBReader, lockStore *lockstore.MemStore, req *coprocessor.Request) (resp *coprocessor.Response) {
	startTime := time.Now()
	resp = &coprocessor.Response{}
	failpoint.Inject("mockCopCacheInUnistore", func(cacheVersion failpoint.Value) {
		if req.IsCacheEnabled {
			if uint64(cacheVersion.(int)) == req.CacheIfMatchVersion {
				failpoint.Return(&coprocessor.Response{IsCacheHit: true, CacheLastVersion: uint64(cacheVersion.(int))})
			} else {
				defer func() {
					resp.CanBeCached = true
					resp.CacheLastVersion = uint64(cacheVersion.(int))
					if resp.ExecDetails == nil {
						resp.ExecDetails = &kvrpcpb.ExecDetails{TimeDetail: &kvrpcpb.TimeDetail{ProcessWallTimeMs: 500}}
					} else if resp.ExecDetails.TimeDetail == nil {
						resp.ExecDetails.TimeDetail = &kvrpcpb.TimeDetail{ProcessWallTimeMs: 500}
					} else {
						resp.ExecDetails.TimeDetail.ProcessWallTimeMs = 500
					}
				}()
			}
		}
	})
	dagCtx, dagReq, err := buildDAG(dbReader, lockStore, req)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}

	exec, chunks, intermediateOutput, lastRange, counts, ndvs, err := buildAndRunMPPExecutor(dagCtx, dagReq, req.PagingSize)

	sc := dagCtx.sctx.GetSessionVars().StmtCtx
	if err != nil {
		errMsg := err.Error()
		if strings.HasPrefix(errMsg, ErrExecutorNotSupportedMsg) {
			resp.OtherError = err.Error()
			return resp
		}
		return genRespWithMPPExec(nil, nil, lastRange, nil, nil, exec, dagReq, err, sc.GetWarnings(), time.Since(startTime))
	}
	return genRespWithMPPExec(chunks, intermediateOutput, lastRange, counts, ndvs, exec, dagReq, err, sc.GetWarnings(), time.Since(startTime))
}

func buildAndRunMPPExecutor(dagCtx *dagContext, dagReq *tipb.DAGRequest, pagingSize uint64) (mppExec, []tipb.Chunk, []*tipb.IntermediateOutput, *coprocessor.KeyRange, []int64, []int64, error) {
	rootExec := dagReq.RootExecutor
	if rootExec == nil {
		rootExec = ExecutorListsToTree(dagReq.Executors)
	}

	var counts, ndvs []int64

	if dagReq.GetCollectRangeCounts() {
		counts = make([]int64, len(dagCtx.keyRanges))
		ndvs = make([]int64, len(dagCtx.keyRanges))
	}

	builder := &mppExecBuilder{
		sctx:     dagCtx.sctx,
		dbReader: dagCtx.dbReader,
		dagReq:   dagReq,
		dagCtx:   dagCtx,
		mppCtx:   nil,
		counts:   counts,
		ndvs:     ndvs,
	}
	var lastRange *coprocessor.KeyRange
	if pagingSize > 0 {
		lastRange = &coprocessor.KeyRange{}
		builder.paging = lastRange
		builder.pagingSize = pagingSize
	}
	exec, err := builder.buildMPPExecutor(rootExec)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	chunks, intermediateOutputs, err := mppExecute(exec, dagCtx, dagReq, pagingSize)
	if lastRange != nil && len(lastRange.Start) == 0 && len(lastRange.End) == 0 {
		// When should this happen, something is wrong?
		lastRange = nil
	}
	return exec, chunks, intermediateOutputs, lastRange, counts, ndvs, err
}

func mppExecute(exec mppExec, dagCtx *dagContext, dagReq *tipb.DAGRequest, pagingSize uint64) (chunks []tipb.Chunk, intermediateOutputs []*tipb.IntermediateOutput, err error) {
	err = exec.open()
	defer func() {
		err := exec.stop()
		if err != nil {
			panic(err)
		}
	}()
	if err != nil {
		return
	}

	var intermediateOutputExecutors []mppExec
	if channels := dagReq.GetIntermediateOutputChannels(); len(channels) > 0 {
		intermediateOutputs = make([]*tipb.IntermediateOutput, len(channels))
		intermediateOutputExecutors = make([]mppExec, len(channels))
		allExecs := flattenMppExec(exec, nil)
		for i, ch := range channels {
			intermediateOutputs[i] = &tipb.IntermediateOutput{
				EncodeType: dagReq.EncodeType,
			}
			intermediateOutputExecutors[i] = allExecs[ch.ExecutorIdx]
		}
	}

	var totalRows uint64
	var chk *chunk.Chunk
	fields := exec.getFieldTypes()
	for {
		chk, err = exec.next()
		if err != nil {
			return
		}

		curRowCnt := 0
		if chk != nil && chk.NumRows() > 0 {
			curRowCnt += chk.NumRows()
			chunks, err = encodeChunk(dagCtx.sctx.GetSessionVars().StmtCtx, dagReq.EncodeType, fields, dagReq.OutputOffsets, chk, chunks)
			if err != nil {
				return
			}
		}

		for i, e := range intermediateOutputExecutors {
			intermediateChunks := e.takeIntermediateResults()
			if len(intermediateChunks) == 0 {
				continue
			}
			for _, iChk := range intermediateChunks {
				curRowCnt += iChk.NumRows()
				output := intermediateOutputs[i]
				channel := dagReq.GetIntermediateOutputChannels()[i]
				output.Chunks, err = encodeChunk(
					dagCtx.sctx.GetSessionVars().StmtCtx,
					output.EncodeType,
					e.getIntermediateFieldTypes(),
					channel.OutputOffsets,
					iChk,
					output.Chunks,
				)

				if err != nil {
					return
				}
			}
		}

		if curRowCnt == 0 {
			return
		}
		totalRows += uint64(curRowCnt)
		if dagReq.EncodeType == tipb.EncodeType_TypeChunk {
			if pagingSize > 0 {
				if totalRows > pagingSize {
					return
				}
			}
		}
	}
}

func encodeChunk(
	sc *stmtctx.StatementContext,
	encodeType tipb.EncodeType,
	fields []*types.FieldType,
	outputOffsets []uint32,
	chk *chunk.Chunk,
	chunks []tipb.Chunk,
) ([]tipb.Chunk, error) {
	switch encodeType {
	case tipb.EncodeType_TypeDefault:
		return useDefaultEncoding(chk, sc, fields, outputOffsets, chunks)
	case tipb.EncodeType_TypeChunk:
		return useChunkEncoding(chk, fields, outputOffsets, chunks), nil
	default:
		return nil, fmt.Errorf("unsupported DAG request encode type %s", encodeType)
	}
}

func useDefaultEncoding(chk *chunk.Chunk, sc *stmtctx.StatementContext,
	fields []*types.FieldType, outputOffsets []uint32, chunks []tipb.Chunk) ([]tipb.Chunk, error) {
	var buf []byte
	var datums []types.Datum
	var err error
	numRows := chk.NumRows()
	errCtx := sc.ErrCtx()
	for i := range numRows {
		datums = datums[:0]
		if outputOffsets != nil {
			for _, j := range outputOffsets {
				datums = append(datums, chk.GetRow(i).GetDatum(int(j), fields[j]))
			}
		} else {
			for j, ft := range fields {
				datums = append(datums, chk.GetRow(i).GetDatum(j, ft))
			}
		}
		buf, err = codec.EncodeValue(sc.TimeZone(), buf[:0], datums...)
		err = errCtx.HandleError(err)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chunks = appendRow(chunks, buf, i)
	}
	return chunks, nil
}

func useChunkEncoding(chk *chunk.Chunk, fields []*types.FieldType, outputOffsets []uint32, chunks []tipb.Chunk) []tipb.Chunk {
	if outputOffsets != nil {
		offsets := make([]int, len(outputOffsets))
		newFields := make([]*types.FieldType, len(outputOffsets))
		for i := range outputOffsets {
			offset := outputOffsets[i]
			offsets[i] = int(offset)
			newFields[i] = fields[offset]
		}
		chk = chk.Prune(offsets)
		fields = newFields
	}

	c := chunk.NewCodec(fields)
	buffer := c.Encode(chk)
	chunks = append(chunks, tipb.Chunk{
		RowsData: buffer,
	})
	return chunks
}

func buildDAG(reader *dbreader.DBReader, lockStore *lockstore.MemStore, req *coprocessor.Request) (*dagContext, *tipb.DAGRequest, error) {
	if len(req.Ranges) == 0 {
		return nil, nil, errors.New("request range is null")
	}
	if req.GetTp() != kv.ReqTypeDAG {
		return nil, nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}

	dagReq := new(tipb.DAGRequest)
	err := proto.Unmarshal(req.Data, dagReq)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	var tz *time.Location
	switch dagReq.TimeZoneName {
	case "":
		tz = time.FixedZone("UTC", int(dagReq.TimeZoneOffset))
	case "System":
		tz = time.Local
	default:
		var ok bool
		tz, ok = globalLocationMap.getLocation(dagReq.TimeZoneName)
		if !ok {
			tz, err = time.LoadLocation(dagReq.TimeZoneName)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			globalLocationMap.setLocation(dagReq.TimeZoneName, tz)
		}
	}
	sctx := flagsAndTzToSessionContext(dagReq.Flags, tz)
	if dagReq.DivPrecisionIncrement != nil {
		sctx.GetSessionVars().DivPrecisionIncrement = int(*dagReq.DivPrecisionIncrement)
	} else {
		sctx.GetSessionVars().DivPrecisionIncrement = vardef.DefDivPrecisionIncrement
	}
	ctx := &dagContext{
		evalContext:   &evalContext{sctx: sctx},
		dbReader:      reader,
		lockStore:     lockStore,
		dagReq:        dagReq,
		keyRanges:     req.Ranges,
		startTS:       req.StartTs,
		resolvedLocks: req.Context.ResolvedLocks,
	}
	if reqCtx := req.Context; reqCtx != nil {
		ctx.keyspaceID = reqCtx.KeyspaceId
	}
	return ctx, dagReq, err
}

func getAggInfo(ctx *dagContext, pbAgg *tipb.Aggregation) ([]aggregation.Aggregation, []expression.Expression, error) {
	length := len(pbAgg.AggFunc)
	aggs := make([]aggregation.Aggregation, 0, length)
	var err error
	for _, expr := range pbAgg.AggFunc {
		var aggExpr aggregation.Aggregation
		aggExpr, _, err = aggregation.NewDistAggFunc(expr, ctx.fieldTps, ctx.sctx.GetExprCtx())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		aggs = append(aggs, aggExpr)
	}
	groupBys, err := convertToExprs(ctx.sctx, ctx.fieldTps, pbAgg.GetGroupBy())
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return aggs, groupBys, nil
}

// Copyright 2017 PingCAP, Inc.
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

package distsql

import (
	"context"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

func DispatchMPPTasks(ctx context.Context, sctx sessionctx.Context, tasks []*kv.MPPDispatchRequest, fieldTypes []*types.FieldType) (SelectResult, error) {
	resp := sctx.GetMPPClient().DispatchMPPTasks(ctx, tasks)
	if resp == nil {
		err := errors.New("client returns nil response")
		return nil, err
	}

	encodeType := tipb.EncodeType_TypeDefault
	if canUseChunkRPC(sctx) {
		encodeType = tipb.EncodeType_TypeChunk
	}
	// TODO: Add metric label and set open tracing.
	return &selectResult{
		label:      "mpp",
		resp:       resp,
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        sctx,
		feedback:   statistics.NewQueryFeedback(0, nil, 0, false),
		encodeType: encodeType,
	}, nil

}

// Select sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func Select(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request, fieldTypes []*types.FieldType, fb *statistics.QueryFeedback) (SelectResult, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("distsql.Select", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// For testing purpose.
	if hook := ctx.Value("CheckSelectRequestHook"); hook != nil {
		hook.(func(*kv.Request))(kvReq)
	}

	if !sctx.GetSessionVars().EnableStreaming {
		kvReq.Streaming = false
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars)
	if resp == nil {
		err := errors.New("client returns nil response")
		return nil, err
	}

	label := metrics.LblGeneral
	if sctx.GetSessionVars().InRestrictedSQL {
		label = metrics.LblInternal
	}

	// kvReq.MemTracker is used to trace and control memory usage in DistSQL layer;
	// for streamResult, since it is a pipeline which has no buffer, it's not necessary to trace it;
	// for selectResult, we just use the kvReq.MemTracker prepared for co-processor
	// instead of creating a new one for simplification.
	if kvReq.Streaming {
		return &streamResult{
			label:      "dag-stream",
			sqlType:    label,
			resp:       resp,
			rowLen:     len(fieldTypes),
			fieldTypes: fieldTypes,
			ctx:        sctx,
			feedback:   fb,
		}, nil
	}
	encodetype := tipb.EncodeType_TypeDefault
	if canUseChunkRPC(sctx) {
		encodetype = tipb.EncodeType_TypeChunk
	}
	return &selectResult{
		label:      "dag",
		resp:       resp,
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        sctx,
		feedback:   fb,
		sqlType:    label,
		memTracker: kvReq.MemTracker,
		encodeType: encodetype,
	}, nil
}

// SelectWithRuntimeStats sends a DAG request, returns SelectResult.
// The difference from Select is that SelectWithRuntimeStats will set copPlanIDs into selectResult,
// which can help selectResult to collect runtime stats.
func SelectWithRuntimeStats(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []int, rootPlanID int) (SelectResult, error) {
	sr, err := Select(ctx, sctx, kvReq, fieldTypes, fb)
	if err == nil {
		if selectResult, ok := sr.(*selectResult); ok {
			selectResult.copPlanIDs = copPlanIDs
			selectResult.rootPlanID = rootPlanID
		}
	}
	return sr, err
}

// Analyze do a analyze request.
func Analyze(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables,
	isRestrict bool) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	label := metrics.LblGeneral
	if isRestrict {
		label = metrics.LblInternal
	}
	result := &selectResult{
		label:      "analyze",
		resp:       resp,
		feedback:   statistics.NewQueryFeedback(0, nil, 0, false),
		sqlType:    label,
		encodeType: tipb.EncodeType_TypeDefault,
	}
	return result, nil
}

// Checksum sends a checksum request.
func Checksum(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label:      "checksum",
		resp:       resp,
		feedback:   statistics.NewQueryFeedback(0, nil, 0, false),
		sqlType:    metrics.LblGeneral,
		encodeType: tipb.EncodeType_TypeDefault,
	}
	return result, nil
}

// SetEncodeType sets the encoding method for the DAGRequest. The supported encoding
// methods are:
// 1. TypeChunk: the result is encoded using the Chunk format, refer util/chunk/chunk.go
// 2. TypeDefault: the result is encoded row by row
func SetEncodeType(ctx sessionctx.Context, dagReq *tipb.DAGRequest) {
	if canUseChunkRPC(ctx) {
		dagReq.EncodeType = tipb.EncodeType_TypeChunk
		setChunkMemoryLayout(dagReq)
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeDefault
	}
}

func canUseChunkRPC(ctx sessionctx.Context) bool {
	if !ctx.GetSessionVars().EnableChunkRPC {
		return false
	}
	if ctx.GetSessionVars().EnableStreaming {
		return false
	}
	if !checkAlignment() {
		return false
	}
	return true
}

var supportedAlignment = unsafe.Sizeof(types.MyDecimal{}) == 40

// checkAlignment checks the alignment in current system environment.
// The alignment is influenced by system, machine and Golang version.
// Using this function can guarantee the alignment is we want.
func checkAlignment() bool {
	return supportedAlignment
}

var systemEndian tipb.Endian

// setChunkMemoryLayout sets the chunk memory layout for the DAGRequest.
func setChunkMemoryLayout(dagReq *tipb.DAGRequest) {
	dagReq.ChunkMemoryLayout = &tipb.ChunkMemoryLayout{Endian: GetSystemEndian()}
}

// GetSystemEndian gets the system endian.
func GetSystemEndian() tipb.Endian {
	return systemEndian
}

func init() {
	i := 0x0100
	ptr := unsafe.Pointer(&i)
	if 0x01 == *(*byte)(ptr) {
		systemEndian = tipb.Endian_BigEndian
	} else {
		systemEndian = tipb.Endian_LittleEndian
	}
}

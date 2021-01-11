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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
)

// Select sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func Select(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request, fieldTypes []*types.FieldType, fb *statistics.QueryFeedback) (SelectResult, error) {
	return nil, errors.New("client returns nil response")
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
	isRestrict bool, sessionMemTracker *memory.Tracker) (SelectResult, error) {
	return nil, errors.New("client returns nil response")
}

// Checksum sends a checksum request.
func Checksum(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables) (SelectResult, error) {
	return nil, errors.New("client returns nil response")
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
	var i int = 0x0100
	ptr := unsafe.Pointer(&i)
	if 0x01 == *(*byte)(ptr) {
		systemEndian = tipb.Endian_BigEndian
	} else {
		systemEndian = tipb.Endian_LittleEndian
	}
}

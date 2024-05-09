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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"context"
	"strconv"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tidb/pkg/util/trxevents"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

// GenSelectResultFromMPPResponse generates an iterator from response.
func GenSelectResultFromMPPResponse(dctx *distsqlctx.DistSQLContext, fieldTypes []*types.FieldType, planIDs []int, rootID int, resp kv.Response) SelectResult {
	// TODO: Add metric label and set open tracing.
	return &selectResult{
		label:      "mpp",
		resp:       resp,
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        dctx,
		copPlanIDs: planIDs,
		rootPlanID: rootID,
		storeType:  kv.TiFlash,
	}
}

// Select sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func Select(ctx context.Context, dctx *distsqlctx.DistSQLContext, kvReq *kv.Request, fieldTypes []*types.FieldType) (SelectResult, error) {
	r, ctx := tracing.StartRegionEx(ctx, "distsql.Select")
	defer r.End()

	// For testing purpose.
	if hook := ctx.Value("CheckSelectRequestHook"); hook != nil {
		hook.(func(*kv.Request))(kvReq)
	}

	enabledRateLimitAction := dctx.EnabledRateLimitAction
	originalSQL := dctx.OriginalSQL
	eventCb := func(event trxevents.TransactionEvent) {
		// Note: Do not assume this callback will be invoked within the same goroutine.
		if copMeetLock := event.GetCopMeetLock(); copMeetLock != nil {
			logutil.Logger(ctx).Debug("coprocessor encounters lock",
				zap.Uint64("startTS", kvReq.StartTs),
				zap.Stringer("lock", copMeetLock.LockInfo),
				zap.String("stmt", originalSQL))
		}
	}

	ctx = WithSQLKvExecCounterInterceptor(ctx, dctx.KvExecCounter)
	option := &kv.ClientSendOption{
		SessionMemTracker:          dctx.SessionMemTracker,
		EnabledRateLimitAction:     enabledRateLimitAction,
		EventCb:                    eventCb,
		EnableCollectExecutionInfo: config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load(),
	}

	if kvReq.StoreType == kv.TiFlash {
		ctx = SetTiFlashConfVarsInContext(ctx, dctx)
		option.TiFlashReplicaRead = dctx.TiFlashReplicaRead
		option.AppendWarning = dctx.AppendWarning
	}

	resp := dctx.Client.Send(ctx, kvReq, dctx.KVVars, option)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}

	label := metrics.LblGeneral
	if dctx.InRestrictedSQL {
		label = metrics.LblInternal
	}

	// kvReq.MemTracker is used to trace and control memory usage in DistSQL layer;
	// for selectResult, we just use the kvReq.MemTracker prepared for co-processor
	// instead of creating a new one for simplification.
	return &selectResult{
		label:              "dag",
		resp:               resp,
		rowLen:             len(fieldTypes),
		fieldTypes:         fieldTypes,
		ctx:                dctx,
		sqlType:            label,
		memTracker:         kvReq.MemTracker,
		storeType:          kvReq.StoreType,
		paging:             kvReq.Paging.Enable,
		distSQLConcurrency: kvReq.Concurrency,
	}, nil
}

// SetTiFlashConfVarsInContext set some TiFlash config variables in context.
func SetTiFlashConfVarsInContext(ctx context.Context, dctx *distsqlctx.DistSQLContext) context.Context {
	if dctx.TiFlashMaxThreads != -1 {
		ctx = metadata.AppendToOutgoingContext(ctx, variable.TiDBMaxTiFlashThreads, strconv.FormatInt(dctx.TiFlashMaxThreads, 10))
	}
	if dctx.TiFlashMaxBytesBeforeExternalJoin != -1 {
		ctx = metadata.AppendToOutgoingContext(ctx, variable.TiDBMaxBytesBeforeTiFlashExternalJoin, strconv.FormatInt(dctx.TiFlashMaxBytesBeforeExternalJoin, 10))
	}
	if dctx.TiFlashMaxBytesBeforeExternalGroupBy != -1 {
		ctx = metadata.AppendToOutgoingContext(ctx, variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy, strconv.FormatInt(dctx.TiFlashMaxBytesBeforeExternalGroupBy, 10))
	}
	if dctx.TiFlashMaxBytesBeforeExternalSort != -1 {
		ctx = metadata.AppendToOutgoingContext(ctx, variable.TiDBMaxBytesBeforeTiFlashExternalSort, strconv.FormatInt(dctx.TiFlashMaxBytesBeforeExternalSort, 10))
	}
	if dctx.TiFlashMaxQueryMemoryPerNode <= 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, variable.TiFlashMemQuotaQueryPerNode, "0")
	} else {
		ctx = metadata.AppendToOutgoingContext(ctx, variable.TiFlashMemQuotaQueryPerNode, strconv.FormatInt(dctx.TiFlashMaxQueryMemoryPerNode, 10))
	}
	ctx = metadata.AppendToOutgoingContext(ctx, variable.TiFlashQuerySpillRatio, strconv.FormatFloat(dctx.TiFlashQuerySpillRatio, 'f', -1, 64))
	return ctx
}

// SelectWithRuntimeStats sends a DAG request, returns SelectResult.
// The difference from Select is that SelectWithRuntimeStats will set copPlanIDs into selectResult,
// which can help selectResult to collect runtime stats.
func SelectWithRuntimeStats(ctx context.Context, dctx *distsqlctx.DistSQLContext, kvReq *kv.Request,
	fieldTypes []*types.FieldType, copPlanIDs []int, rootPlanID int) (SelectResult, error) {
	sr, err := Select(ctx, dctx, kvReq, fieldTypes)
	if err != nil {
		return nil, err
	}
	if selectResult, ok := sr.(*selectResult); ok {
		selectResult.copPlanIDs = copPlanIDs
		selectResult.rootPlanID = rootPlanID
	}
	return sr, nil
}

// Analyze do a analyze request.
func Analyze(ctx context.Context, client kv.Client, kvReq *kv.Request, vars any,
	isRestrict bool, dctx *distsqlctx.DistSQLContext) (SelectResult, error) {
	ctx = WithSQLKvExecCounterInterceptor(ctx, dctx.KvExecCounter)
	kvReq.RequestSource.RequestSourceInternal = true
	kvReq.RequestSource.RequestSourceType = kv.InternalTxnStats
	resp := client.Send(ctx, kvReq, vars, &kv.ClientSendOption{})
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	label := metrics.LblGeneral
	if isRestrict {
		label = metrics.LblInternal
	}
	result := &selectResult{
		label:     "analyze",
		resp:      resp,
		sqlType:   label,
		storeType: kvReq.StoreType,
	}
	return result, nil
}

// Checksum sends a checksum request.
func Checksum(ctx context.Context, client kv.Client, kvReq *kv.Request, vars any) (SelectResult, error) {
	// FIXME: As BR have dependency of `Checksum` and TiDB also introduced BR as dependency, Currently we can't edit
	// Checksum function signature. The two-way dependence should be removed in the future.
	resp := client.Send(ctx, kvReq, vars, &kv.ClientSendOption{})
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label:     "checksum",
		resp:      resp,
		sqlType:   metrics.LblGeneral,
		storeType: kvReq.StoreType,
	}
	return result, nil
}

// SetEncodeType sets the encoding method for the DAGRequest. The supported encoding
// methods are:
// 1. TypeChunk: the result is encoded using the Chunk format, refer util/chunk/chunk.go
// 2. TypeDefault: the result is encoded row by row
func SetEncodeType(ctx *distsqlctx.DistSQLContext, dagReq *tipb.DAGRequest) {
	if canUseChunkRPC(ctx) {
		dagReq.EncodeType = tipb.EncodeType_TypeChunk
		setChunkMemoryLayout(dagReq)
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeDefault
	}
}

func canUseChunkRPC(ctx *distsqlctx.DistSQLContext) bool {
	if !ctx.EnableChunkRPC {
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

// WithSQLKvExecCounterInterceptor binds an interceptor for client-go to count the
// number of SQL executions of each TiKV (if any).
func WithSQLKvExecCounterInterceptor(ctx context.Context, counter *stmtstats.KvExecCounter) context.Context {
	if counter != nil {
		// Unlike calling Transaction or Snapshot interface, in distsql package we directly
		// face tikv Request. So we need to manually bind RPCInterceptor to ctx. Instead of
		// calling SetRPCInterceptor on Transaction or Snapshot.
		return interceptor.WithRPCInterceptor(ctx, counter.RPCInterceptor())
	}
	return ctx
}

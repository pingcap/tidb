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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

// XAPI error codes.
const (
	codeInvalidResp = 1
)

// Select sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func Select(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request, fieldTypes []*types.FieldType, fb *statistics.QueryFeedback) (SelectResult, error) {
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
		return nil, errors.Trace(err)
	}

	if kvReq.Streaming {
		return &streamResult{
			resp:       resp,
			rowLen:     len(fieldTypes),
			fieldTypes: fieldTypes,
			ctx:        sctx,
			feedback:   fb,
		}, nil
	}

	return &selectResult{
		label:      "dag",
		resp:       resp,
		results:    make(chan resultWithErr, kvReq.Concurrency),
		closed:     make(chan struct{}),
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        sctx,
		feedback:   fb,
	}, nil
}

// Analyze do a analyze request.
func Analyze(ctx context.Context, client kv.Client, kvReq *kv.Request, vars *kv.Variables) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label:    "analyze",
		resp:     resp,
		results:  make(chan resultWithErr, kvReq.Concurrency),
		closed:   make(chan struct{}),
		feedback: statistics.NewQueryFeedback(0, nil, 0, false),
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
		label:    "checksum",
		resp:     resp,
		results:  make(chan resultWithErr, kvReq.Concurrency),
		closed:   make(chan struct{}),
		feedback: statistics.NewQueryFeedback(0, nil, 0, false),
	}
	return result, nil
}

func SetEncodeType(dagReq *tipb.DAGRequest) {
	if useChunkIPC(dagReq) {
		dagReq.EncodeType = tipb.EncodeType_TypeArrow
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeDefault
	}
}

func useChunkIPC(dagReq *tipb.DAGRequest) bool {
	if config.GetGlobalConfig().EnableArrow == false {
		return false
	}
	if config.GetGlobalConfig().EnableStreaming == true {
		return false
	}
	root := dagReq.Executors[len(dagReq.Executors)-1]
	if root.Aggregation != nil {
		hashAgg := root.Aggregation
		for i := range hashAgg.AggFunc {
			if !typeSupported(hashAgg.AggFunc[i].FieldType.Tp) {
				return false
			}
		}
		for i := range hashAgg.GroupBy {
			if !typeSupported(hashAgg.GroupBy[i].FieldType.Tp) {
				return false
			}
		}
	} else if root.StreamAgg != nil {
		streamAgg := root.StreamAgg
		for i := range streamAgg.AggFunc {
			if !typeSupported(streamAgg.AggFunc[i].FieldType.Tp) {
				return false
			}
		}
		for i := range streamAgg.GroupBy {
			if !typeSupported(streamAgg.GroupBy[i].FieldType.Tp) {
				return false
			}
		}
	} else {
		for _, executor := range dagReq.Executors {
			switch executor.GetTp() {
			case tipb.ExecType_TypeIndexScan:
				for _, col := range executor.IdxScan.Columns {
					if !typeSupported(col.Tp) {
						return false
					}
				}
			case tipb.ExecType_TypeTableScan:
				for _, col := range executor.TblScan.Columns {
					if !typeSupported(col.Tp) {
						return false
					}
				}
			}
		}
	}
	return true
}

func typeSupported(tpInput int32) bool {
	tp := byte(tpInput)
	return tp != mysql.TypeBit && tp != mysql.TypeEnum && tp != mysql.TypeSet
}

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

package mocktikv

import (
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (h *rpcHandler) handleCopAnalyzeRequest(req *coprocessor.Request) (*coprocessor.Response, error) {
	resp := &coprocessor.Response{}
	if len(req.Ranges) == 0 {
		return resp, nil
	}
	if req.GetTp() != kv.ReqTypeAnalyze {
		return resp, nil
	}
	if err := h.checkRequestContext(req.GetContext()); err != nil {
		resp.RegionError = err
		return resp, nil
	}
	analyzeReq := new(tipb.AnalyzeReq)
	err := proto.Unmarshal(req.Data, analyzeReq)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if analyzeReq.Tp != tipb.AnalyzeType_TypeIndex {
		return resp, nil
	}
	return h.handleAnalyzeIndexReq(req, analyzeReq)
}

func (h *rpcHandler) handleAnalyzeIndexReq(req *coprocessor.Request, analyzeReq *tipb.AnalyzeReq) (*coprocessor.Response, error) {
	e := &indexScanExec{
		colsLen:        int(analyzeReq.IdxReq.NumColumns),
		kvRanges:       h.extractKVRanges(req.Ranges, false),
		startTS:        analyzeReq.StartTs,
		isolationLevel: h.isolationLevel,
		mvccStore:      h.mvccStore,
		IndexScan:      &tipb.IndexScan{Desc: false},
	}
	statsBuilder := statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0)
	for {
		_, values, err := e.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if values == nil {
			break
		}
		var value []byte
		for _, val := range values {
			value = append(value, val...)
		}
		err = statsBuilder.Iterate(types.NewBytesDatum(value))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	hg := statistics.HistogramToProto(statsBuilder.Hist())
	data, err := proto.Marshal(&tipb.AnalyzeIndexResp{Hist: hg})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &coprocessor.Response{Data: data}, nil
}

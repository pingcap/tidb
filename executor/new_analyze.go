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

package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func analyzeIndexPushdown(idxExec *AnalyzeIndexExec) statistics.AnalyzeResult {
	hist, err := idxExec.buildHistogram()
	if err != nil {
		return statistics.AnalyzeResult{Err: err}
	}
	result := statistics.AnalyzeResult{
		TableID: idxExec.tblInfo.ID,
		Hist:    []*statistics.Histogram{hist},
		IsIndex: 1,
	}
	if len(hist.Buckets) > 0 {
		result.Count = hist.Buckets[len(hist.Buckets)-1].Count
	}
	return result
}

// AnalyzeIndexExec represents analyze index push down executor.
type AnalyzeIndexExec struct {
	ctx         context.Context
	tblInfo     *model.TableInfo
	idxInfo     *model.IndexInfo
	concurrency int
	priority    int
	analyzePB   *tipb.AnalyzeReq
	result      distsql.SelectResult
}

func (e *AnalyzeIndexExec) open() error {
	fieldTypes := make([]*types.FieldType, len(e.idxInfo.Columns))
	for i, v := range e.idxInfo.Columns {
		fieldTypes[i] = &(e.tblInfo.Columns[v.Offset].FieldType)
	}
	idxRange := &types.IndexRange{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: []types.Datum{types.MaxValueDatum()}}
	keyRanges, err := indexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.tblInfo.ID, e.idxInfo.ID, []*types.IndexRange{idxRange}, fieldTypes)
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.Analyze(e.ctx.GetClient(), e.ctx.GoCtx(), e.analyzePB, keyRanges, e.concurrency, true, e.priority)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

func (e *AnalyzeIndexExec) buildHistogram() (hist *statistics.Histogram, err error) {
	if err := e.open(); err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err1 := e.result.Close(); err1 != nil {
			hist = nil
			err = errors.Trace(err1)
		}
	}()
	hist = &statistics.Histogram{}
	for {
		data, err := e.result.NextRaw()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		resp := &tipb.AnalyzeIndexResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		hist, err = statistics.MergeHistograms(e.ctx.GetSessionVars().StmtCtx, hist, statistics.HistogramFromProto(resp.Hist), maxBucketSize)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	hist.ID = e.idxInfo.ID
	return hist, nil
}

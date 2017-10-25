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
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/tablecodec"
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
	result      distsql.NewSelectResult
}

func (e *AnalyzeIndexExec) open() error {
	fieldTypes := make([]*types.FieldType, len(e.idxInfo.Columns))
	for i, v := range e.idxInfo.Columns {
		fieldTypes[i] = &(e.tblInfo.Columns[v.Offset].FieldType)
	}
	idxRange := &types.IndexRange{LowVal: []types.Datum{types.MinNotNullDatum()}, HighVal: []types.Datum{types.MaxValueDatum()}}
	var builder requestBuilder
	kvReq, err := builder.SetIndexRanges(e.ctx.GetSessionVars().StmtCtx, e.tblInfo.ID, e.idxInfo.ID, []*types.IndexRange{idxRange}, fieldTypes).
		SetAnalyzeRequest(e.analyzePB).
		SetKeepOrder(true).
		SetPriority(e.priority).
		Build()
	kvReq.Concurrency = e.concurrency
	kvReq.IsolationLevel = kv.RC
	e.result, err = distsql.NewAnalyze(e.ctx.GoCtx(), e.ctx.GetClient(), kvReq)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

func (e *AnalyzeIndexExec) buildHistogram() (hist *statistics.Histogram, err error) {
	if err = e.open(); err != nil {
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

func analyzeColumnsPushdown(colExec *AnalyzeColumnsExec) statistics.AnalyzeResult {
	hists, err := colExec.buildHistograms()
	if err != nil {
		return statistics.AnalyzeResult{Err: err}
	}
	result := statistics.AnalyzeResult{
		TableID: colExec.tblInfo.ID,
		Hist:    hists,
	}
	hist := hists[0]
	result.Count = hist.NullCount
	if len(hist.Buckets) > 0 {
		result.Count += hist.Buckets[len(hist.Buckets)-1].Count
	}
	return result
}

// AnalyzeColumnsExec represents Analyze columns push down executor.
type AnalyzeColumnsExec struct {
	ctx         context.Context
	tblInfo     *model.TableInfo
	colsInfo    []*model.ColumnInfo
	pkInfo      *model.ColumnInfo
	concurrency int
	priority    int
	keepOrder   bool
	analyzePB   *tipb.AnalyzeReq
	result      distsql.NewSelectResult
}

func (e *AnalyzeColumnsExec) open() error {
	ranges := []types.IntColumnRange{{LowVal: math.MinInt64, HighVal: math.MaxInt64}}
	var builder requestBuilder
	kvReq, err := builder.SetTableRanges(e.tblInfo.ID, ranges).
		SetAnalyzeRequest(e.analyzePB).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		Build()
	kvReq.IsolationLevel = kv.RC
	kvReq.Concurrency = e.concurrency
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.NewAnalyze(e.ctx.GoCtx(), e.ctx.GetClient(), kvReq)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

func (e *AnalyzeColumnsExec) buildHistograms() (hists []*statistics.Histogram, err error) {
	if err = e.open(); err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err1 := e.result.Close(); err1 != nil {
			hists = nil
			err = errors.Trace(err1)
		}
	}()
	pkHist := &statistics.Histogram{}
	collectors := make([]*statistics.SampleCollector, len(e.colsInfo))
	for i := range collectors {
		collectors[i] = &statistics.SampleCollector{
			IsMerger:      true,
			Sketch:        statistics.NewFMSketch(maxSketchSize),
			MaxSampleSize: maxSampleSize,
		}
	}
	for {
		data, err1 := e.result.NextRaw()
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if data == nil {
			break
		}
		resp := &tipb.AnalyzeColumnsResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if e.pkInfo != nil {
			pkHist, err = statistics.MergeHistograms(e.ctx.GetSessionVars().StmtCtx, pkHist, statistics.HistogramFromProto(resp.PkHist), maxBucketSize)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		for i, rc := range resp.Collectors {
			collectors[i].MergeSampleCollector(statistics.SampleCollectorFromProto(rc))
		}
	}
	timeZone := e.ctx.GetSessionVars().GetTimeZone()
	if e.pkInfo != nil {
		pkHist.ID = e.pkInfo.ID
		for i, bkt := range pkHist.Buckets {
			pkHist.Buckets[i].LowerBound, err = tablecodec.DecodeColumnValue(bkt.LowerBound.GetBytes(), &e.pkInfo.FieldType, timeZone)
			if err != nil {
				return nil, errors.Trace(err)
			}
			pkHist.Buckets[i].UpperBound, err = tablecodec.DecodeColumnValue(bkt.UpperBound.GetBytes(), &e.pkInfo.FieldType, timeZone)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		hists = append(hists, pkHist)
	}
	for i, col := range e.colsInfo {
		for j, s := range collectors[i].Samples {
			collectors[i].Samples[j], err = tablecodec.DecodeColumnValue(s.GetBytes(), &col.FieldType, timeZone)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		hg, err := statistics.BuildColumn(e.ctx, maxBucketSize, col.ID, collectors[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		hists = append(hists, hg)
	}
	return hists, nil
}

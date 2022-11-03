// Copyright 2022 PingCAP, Inc.
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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/ranger"
)

type analyzeIndexIncrementalExec struct {
	AnalyzeIndexExec
	oldHist *statistics.Histogram
	oldCMS  *statistics.CMSketch
	oldTopN *statistics.TopN
}

func analyzeIndexIncremental(idxExec *analyzeIndexIncrementalExec) *statistics.AnalyzeResults {
	var statsVer = statistics.Version1
	if idxExec.analyzePB.IdxReq.Version != nil {
		statsVer = int(*idxExec.analyzePB.IdxReq.Version)
	}
	pruneMode := variable.PartitionPruneMode(idxExec.ctx.GetSessionVars().PartitionPruneMode.Load())
	if idxExec.tableID.IsPartitionTable() && pruneMode == variable.Dynamic {
		err := errors.Errorf("[stats]: global statistics for partitioned tables unavailable in ANALYZE INCREMENTAL")
		return &statistics.AnalyzeResults{Err: err, Job: idxExec.job}
	}
	startPos := idxExec.oldHist.GetUpper(idxExec.oldHist.Len() - 1)
	values, _, err := codec.DecodeRange(startPos.GetBytes(), len(idxExec.idxInfo.Columns), nil, nil)
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: idxExec.job}
	}
	ran := ranger.Range{LowVal: values, HighVal: []types.Datum{types.MaxValueDatum()}, Collators: collate.GetBinaryCollatorSlice(1)}
	hist, cms, fms, topN, err := idxExec.buildStats([]*ranger.Range{&ran}, false)
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: idxExec.job}
	}
	hist, err = statistics.MergeHistograms(idxExec.ctx.GetSessionVars().StmtCtx, idxExec.oldHist, hist, int(idxExec.opts[ast.AnalyzeOptNumBuckets]), statsVer)
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: idxExec.job}
	}
	if idxExec.oldCMS != nil && cms != nil {
		err = cms.MergeCMSketch4IncrementalAnalyze(idxExec.oldCMS, uint32(idxExec.opts[ast.AnalyzeOptNumTopN]))
		if err != nil {
			return &statistics.AnalyzeResults{Err: err, Job: idxExec.job}
		}
		cms.CalcDefaultValForAnalyze(uint64(hist.NDV))
	}
	if statsVer >= statistics.Version2 {
		poped := statistics.MergeTopNAndUpdateCMSketch(topN, idxExec.oldTopN, cms, uint32(idxExec.opts[ast.AnalyzeOptNumTopN]))
		hist.AddIdxVals(poped)
	}
	result := &statistics.AnalyzeResult{
		Hist:    []*statistics.Histogram{hist},
		Cms:     []*statistics.CMSketch{cms},
		TopNs:   []*statistics.TopN{topN},
		Fms:     []*statistics.FMSketch{fms},
		IsIndex: 1,
	}
	cnt := hist.NullCount
	if hist.Len() > 0 {
		cnt += hist.Buckets[hist.Len()-1].Count
	}
	return &statistics.AnalyzeResults{
		TableID:  idxExec.tableID,
		Ars:      []*statistics.AnalyzeResult{result},
		Job:      idxExec.job,
		StatsVer: statsVer,
		Count:    cnt,
		Snapshot: idxExec.snapshot,
	}
}

type analyzePKIncrementalExec struct {
	AnalyzeColumnsExec
	oldHist *statistics.Histogram
}

func analyzePKIncremental(colExec *analyzePKIncrementalExec) *statistics.AnalyzeResults {
	var maxVal types.Datum
	pkInfo := colExec.handleCols.GetCol(0)
	if mysql.HasUnsignedFlag(pkInfo.RetType.GetFlag()) {
		maxVal = types.NewUintDatum(math.MaxUint64)
	} else {
		maxVal = types.NewIntDatum(math.MaxInt64)
	}
	startPos := *colExec.oldHist.GetUpper(colExec.oldHist.Len() - 1)
	ran := ranger.Range{LowVal: []types.Datum{startPos}, LowExclude: true, HighVal: []types.Datum{maxVal}, Collators: collate.GetBinaryCollatorSlice(1)}
	hists, _, _, _, _, err := colExec.buildStats([]*ranger.Range{&ran}, false)
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: colExec.job}
	}
	hist := hists[0]
	hist, err = statistics.MergeHistograms(colExec.ctx.GetSessionVars().StmtCtx, colExec.oldHist, hist, int(colExec.opts[ast.AnalyzeOptNumBuckets]), statistics.Version1)
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: colExec.job}
	}
	result := &statistics.AnalyzeResult{
		Hist:  []*statistics.Histogram{hist},
		Cms:   []*statistics.CMSketch{nil},
		TopNs: []*statistics.TopN{nil},
		Fms:   []*statistics.FMSketch{nil},
	}
	var cnt int64
	if hist.Len() > 0 {
		cnt = hist.Buckets[hist.Len()-1].Count
	}
	return &statistics.AnalyzeResults{
		TableID:  colExec.tableID,
		Ars:      []*statistics.AnalyzeResult{result},
		Job:      colExec.job,
		StatsVer: statistics.Version1,
		Count:    cnt,
		Snapshot: colExec.snapshot,
	}
}

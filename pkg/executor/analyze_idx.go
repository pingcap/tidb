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
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// AnalyzeIndexExec represents analyze index push down executor.
type AnalyzeIndexExec struct {
	baseAnalyzeExec

	idxInfo        *model.IndexInfo
	isCommonHandle bool
	result         distsql.SelectResult
	countNullRes   distsql.SelectResult
}

func analyzeIndexPushdown(idxExec *AnalyzeIndexExec) *statistics.AnalyzeResults {
	ranges := ranger.FullRange()
	// For single-column index, we do not load null rows from TiKV, so the built histogram would not include
	// null values, and its `NullCount` would be set by result of another distsql call to get null rows.
	// For multi-column index, we cannot define null for the rows, so we still use full range, and the rows
	// containing null fields would exist in built histograms. Note that, the `NullCount` of histograms for
	// multi-column index is always 0 then.
	if len(idxExec.idxInfo.Columns) == 1 {
		ranges = ranger.FullNotNullRange()
	}
	hist, cms, fms, topN, err := idxExec.buildStats(ranges, true)
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: idxExec.job}
	}
	var statsVer = statistics.Version1
	if idxExec.analyzePB.IdxReq.Version != nil {
		statsVer = int(*idxExec.analyzePB.IdxReq.Version)
	}
	idxResult := &statistics.AnalyzeResult{
		Hist:    []*statistics.Histogram{hist},
		TopNs:   []*statistics.TopN{topN},
		Fms:     []*statistics.FMSketch{fms},
		IsIndex: 1,
	}
	if statsVer != statistics.Version2 {
		idxResult.Cms = []*statistics.CMSketch{cms}
	}
	cnt := hist.NullCount
	if hist.Len() > 0 {
		cnt += hist.Buckets[hist.Len()-1].Count
	}
	if topN.TotalCount() > 0 {
		cnt += int64(topN.TotalCount())
	}
	result := &statistics.AnalyzeResults{
		TableID:  idxExec.tableID,
		Ars:      []*statistics.AnalyzeResult{idxResult},
		Job:      idxExec.job,
		StatsVer: statsVer,
		Count:    cnt,
		Snapshot: idxExec.snapshot,
	}
	if idxExec.idxInfo.MVIndex {
		result.ForMVIndex = true
	}
	return result
}

func (e *AnalyzeIndexExec) buildStats(ranges []*ranger.Range, considerNull bool) (hist *statistics.Histogram, cms *statistics.CMSketch, fms *statistics.FMSketch, topN *statistics.TopN, err error) {
	if err = e.open(ranges, considerNull); err != nil {
		return nil, nil, nil, nil, err
	}
	defer func() {
		err1 := closeAll(e.result, e.countNullRes)
		if err == nil {
			err = err1
		}
	}()
	hist, cms, fms, topN, err = e.buildStatsFromResult(e.result, true)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if e.countNullRes != nil {
		nullHist, _, _, _, err := e.buildStatsFromResult(e.countNullRes, false)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if l := nullHist.Len(); l > 0 {
			hist.NullCount = nullHist.Buckets[l-1].Count
		}
	}
	hist.ID = e.idxInfo.ID
	return hist, cms, fms, topN, nil
}

func (e *AnalyzeIndexExec) open(ranges []*ranger.Range, considerNull bool) error {
	err := e.fetchAnalyzeResult(ranges, false)
	if err != nil {
		return err
	}
	if considerNull && len(e.idxInfo.Columns) == 1 {
		ranges = ranger.NullRange()
		err = e.fetchAnalyzeResult(ranges, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// fetchAnalyzeResult builds and dispatches the `kv.Request` from given ranges, and stores the `SelectResult`
// in corresponding fields based on the input `isNullRange` argument, which indicates if the range is the
// special null range for single-column index to get the null count.
func (e *AnalyzeIndexExec) fetchAnalyzeResult(ranges []*ranger.Range, isNullRange bool) error {
	var builder distsql.RequestBuilder
	var kvReqBuilder *distsql.RequestBuilder
	if e.isCommonHandle && e.idxInfo.Primary {
		kvReqBuilder = builder.SetHandleRangesForTables(e.ctx.GetDistSQLCtx(), []int64{e.tableID.GetStatisticsID()}, true, ranges)
	} else {
		kvReqBuilder = builder.SetIndexRangesForTables(e.ctx.GetDistSQLCtx(), []int64{e.tableID.GetStatisticsID()}, e.idxInfo.ID, ranges)
	}
	kvReqBuilder.SetResourceGroupTagger(e.ctx.GetSessionVars().StmtCtx.GetResourceGroupTagger())
	startTS := uint64(math.MaxUint64)
	isoLevel := kv.RC
	if e.ctx.GetSessionVars().EnableAnalyzeSnapshot {
		startTS = e.snapshot
		isoLevel = kv.SI
	}
	kvReq, err := kvReqBuilder.
		SetAnalyzeRequest(e.analyzePB, isoLevel).
		SetStartTS(startTS).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		SetResourceGroupName(e.ctx.GetSessionVars().StmtCtx.ResourceGroupName).
		SetExplicitRequestSourceType(e.ctx.GetSessionVars().ExplicitRequestSourceType).
		Build()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL, e.ctx.GetDistSQLCtx())
	if err != nil {
		return err
	}
	if isNullRange {
		e.countNullRes = result
	} else {
		e.result = result
	}
	return nil
}

func (e *AnalyzeIndexExec) buildStatsFromResult(result distsql.SelectResult, needCMS bool) (*statistics.Histogram, *statistics.CMSketch, *statistics.FMSketch, *statistics.TopN, error) {
	failpoint.Inject("buildStatsFromResult", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, nil, nil, nil, errors.New("mock buildStatsFromResult error"))
		}
	})
	hist := &statistics.Histogram{}
	var cms *statistics.CMSketch
	var topn *statistics.TopN
	if needCMS {
		cms = statistics.NewCMSketch(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]))
		topn = statistics.NewTopN(int(e.opts[ast.AnalyzeOptNumTopN]))
	}
	fms := statistics.NewFMSketch(statistics.MaxSketchSize)
	statsVer := statistics.Version1
	if e.analyzePB.IdxReq.Version != nil {
		statsVer = int(*e.analyzePB.IdxReq.Version)
	}
	for {
		failpoint.Inject("mockKillRunningAnalyzeIndexJob", func() {
			dom := domain.GetDomain(e.ctx)
			dom.SysProcTracker().KillSysProcess(dom.GetAutoAnalyzeProcID())
		})
		if err := e.ctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return nil, nil, nil, nil, err
		}
		failpoint.Inject("mockSlowAnalyzeIndex", func() {
			time.Sleep(1000 * time.Second)
		})
		data, err := result.NextRaw(context.TODO())
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if data == nil {
			break
		}
		resp := &tipb.AnalyzeIndexResp{}
		err = resp.Unmarshal(data)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		hist, cms, fms, topn, err = updateIndexResult(e.ctx, resp, e.job, hist, cms, fms, topn,
			e.idxInfo, int(e.opts[ast.AnalyzeOptNumBuckets]), int(e.opts[ast.AnalyzeOptNumTopN]), statsVer)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	if needCMS && topn.TotalCount() > 0 {
		hist.RemoveVals(topn.TopN)
	}
	if statsVer == statistics.Version2 {
		hist.StandardizeForV2AnalyzeIndex()
	}
	if needCMS && cms != nil {
		cms.CalcDefaultValForAnalyze(uint64(hist.NDV))
	}
	return hist, cms, fms, topn, nil
}

func (e *AnalyzeIndexExec) buildSimpleStats(ranges []*ranger.Range, considerNull bool) (fms *statistics.FMSketch, nullHist *statistics.Histogram, err error) {
	if err = e.open(ranges, considerNull); err != nil {
		return nil, nil, err
	}
	defer func() {
		err1 := closeAll(e.result, e.countNullRes)
		if err == nil {
			err = err1
		}
	}()
	_, _, fms, _, err = e.buildStatsFromResult(e.result, false)
	if e.countNullRes != nil {
		nullHist, _, _, _, err := e.buildStatsFromResult(e.countNullRes, false)
		if err != nil {
			return nil, nil, err
		}
		if l := nullHist.Len(); l > 0 {
			return fms, nullHist, nil
		}
	}
	return fms, nil, nil
}

func analyzeIndexNDVPushDown(idxExec *AnalyzeIndexExec) *statistics.AnalyzeResults {
	ranges := ranger.FullRange()
	// For single-column index, we do not load null rows from TiKV, so the built histogram would not include
	// null values, and its `NullCount` would be set by result of another distsql call to get null rows.
	// For multi-column index, we cannot define null for the rows, so we still use full range, and the rows
	// containing null fields would exist in built histograms. Note that, the `NullCount` of histograms for
	// multi-column index is always 0 then.
	if len(idxExec.idxInfo.Columns) == 1 {
		ranges = ranger.FullNotNullRange()
	}
	fms, nullHist, err := idxExec.buildSimpleStats(ranges, len(idxExec.idxInfo.Columns) == 1)
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: idxExec.job}
	}
	result := &statistics.AnalyzeResult{
		Fms: []*statistics.FMSketch{fms},
		// We use histogram to get the Index's ID.
		Hist:    []*statistics.Histogram{statistics.NewHistogram(idxExec.idxInfo.ID, 0, 0, statistics.Version1, types.NewFieldType(mysql.TypeBlob), 0, 0)},
		IsIndex: 1,
	}
	r := &statistics.AnalyzeResults{
		TableID: idxExec.tableID,
		Ars:     []*statistics.AnalyzeResult{result},
		Job:     idxExec.job,
		// TODO: avoid reusing Version1.
		StatsVer: statistics.Version1,
	}
	if nullHist != nil && nullHist.Len() > 0 {
		r.Count = nullHist.Buckets[nullHist.Len()-1].Count
	}
	return r
}

func updateIndexResult(
	ctx sessionctx.Context,
	resp *tipb.AnalyzeIndexResp,
	job *statistics.AnalyzeJob,
	hist *statistics.Histogram,
	cms *statistics.CMSketch,
	fms *statistics.FMSketch,
	topn *statistics.TopN,
	idxInfo *model.IndexInfo,
	numBuckets int,
	numTopN int,
	statsVer int,
) (
	*statistics.Histogram,
	*statistics.CMSketch,
	*statistics.FMSketch,
	*statistics.TopN,
	error,
) {
	var err error
	needCMS := cms != nil
	respHist := statistics.HistogramFromProto(resp.Hist)
	if job != nil {
		UpdateAnalyzeJob(ctx, job, int64(respHist.TotalRowCount()))
	}
	hist, err = statistics.MergeHistograms(ctx.GetSessionVars().StmtCtx, hist, respHist, numBuckets, statsVer)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if needCMS {
		if resp.Cms == nil {
			logutil.Logger(context.TODO()).Warn("nil CMS in response", zap.String("table", idxInfo.Table.O), zap.String("index", idxInfo.Name.O))
		} else {
			cm, tmpTopN := statistics.CMSketchAndTopNFromProto(resp.Cms)
			if err := cms.MergeCMSketch(cm); err != nil {
				return nil, nil, nil, nil, err
			}
			statistics.MergeTopNAndUpdateCMSketch(topn, tmpTopN, cms, uint32(numTopN))
		}
	}
	if fms != nil && resp.Collector != nil && resp.Collector.FmSketch != nil {
		fms.MergeFMSketch(statistics.FMSketchFromProto(resp.Collector.FmSketch))
	}
	return hist, cms, fms, topn, nil
}

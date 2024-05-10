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
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tiancaiamao/gp"
)

// AnalyzeColumnsExec represents Analyze columns push down executor.
type AnalyzeColumnsExec struct {
	baseAnalyzeExec

	tableInfo     *model.TableInfo
	colsInfo      []*model.ColumnInfo
	handleCols    plannerutil.HandleCols
	commonHandle  *model.IndexInfo
	resultHandler *tableResultHandler
	indexes       []*model.IndexInfo
	core.AnalyzeInfo

	samplingBuilderWg *notifyErrorWaitGroupWrapper
	samplingMergeWg   *util.WaitGroupWrapper

	schemaForVirtualColEval *expression.Schema
	baseCount               int64
	baseModifyCnt           int64

	memTracker *memory.Tracker
}

func analyzeColumnsPushDownEntry(gp *gp.Pool, e *AnalyzeColumnsExec) *statistics.AnalyzeResults {
	if e.AnalyzeInfo.StatsVersion >= statistics.Version2 {
		return e.toV2().analyzeColumnsPushDownV2(gp)
	}
	return e.toV1().analyzeColumnsPushDownV1()
}

func (e *AnalyzeColumnsExec) toV1() *AnalyzeColumnsExecV1 {
	return &AnalyzeColumnsExecV1{
		AnalyzeColumnsExec: e,
	}
}

func (e *AnalyzeColumnsExec) toV2() *AnalyzeColumnsExecV2 {
	return &AnalyzeColumnsExecV2{
		AnalyzeColumnsExec: e,
	}
}

func (e *AnalyzeColumnsExec) open(ranges []*ranger.Range) error {
	e.memTracker = memory.NewTracker(int(e.ctx.GetSessionVars().PlanID.Load()), -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := distsql.SplitRangesAcrossInt64Boundary(ranges, true, false, !hasPkHist(e.handleCols))
	firstResult, err := e.buildResp(firstPartRanges)
	if err != nil {
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(secondPartRanges)
	if err != nil {
		return err
	}
	e.resultHandler.open(firstResult, secondResult)

	return nil
}

func (e *AnalyzeColumnsExec) buildResp(ranges []*ranger.Range) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	reqBuilder := builder.SetHandleRangesForTables(e.ctx.GetDistSQLCtx(), []int64{e.TableID.GetStatisticsID()}, e.handleCols != nil && !e.handleCols.IsInt(), ranges)
	builder.SetResourceGroupTagger(e.ctx.GetSessionVars().StmtCtx.GetResourceGroupTagger())
	startTS := uint64(math.MaxUint64)
	isoLevel := kv.RC
	if e.ctx.GetSessionVars().EnableAnalyzeSnapshot {
		startTS = e.snapshot
		isoLevel = kv.SI
	}
	// Always set KeepOrder of the request to be true, in order to compute
	// correct `correlation` of columns.
	kvReq, err := reqBuilder.
		SetAnalyzeRequest(e.analyzePB, isoLevel).
		SetStartTS(startTS).
		SetKeepOrder(true).
		SetConcurrency(e.concurrency).
		SetMemTracker(e.memTracker).
		SetResourceGroupName(e.ctx.GetSessionVars().StmtCtx.ResourceGroupName).
		SetExplicitRequestSourceType(e.ctx.GetSessionVars().ExplicitRequestSourceType).
		Build()
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	result, err := distsql.Analyze(ctx, e.ctx.GetClient(), kvReq, e.ctx.GetSessionVars().KVVars, e.ctx.GetSessionVars().InRestrictedSQL, e.ctx.GetDistSQLCtx())
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *AnalyzeColumnsExec) buildStats(ranges []*ranger.Range, needExtStats bool) (hists []*statistics.Histogram, cms []*statistics.CMSketch, topNs []*statistics.TopN, fms []*statistics.FMSketch, extStats *statistics.ExtendedStatsColl, err error) {
	if err = e.open(ranges); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	defer func() {
		if err1 := e.resultHandler.Close(); err1 != nil {
			hists = nil
			cms = nil
			extStats = nil
			err = err1
		}
	}()
	var handleHist *statistics.Histogram
	var handleCms *statistics.CMSketch
	var handleFms *statistics.FMSketch
	var handleTopn *statistics.TopN
	statsVer := statistics.Version1
	if e.analyzePB.Tp == tipb.AnalyzeType_TypeMixed {
		handleHist = &statistics.Histogram{}
		handleCms = statistics.NewCMSketch(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]))
		handleTopn = statistics.NewTopN(int(e.opts[ast.AnalyzeOptNumTopN]))
		handleFms = statistics.NewFMSketch(statistics.MaxSketchSize)
		if e.analyzePB.IdxReq.Version != nil {
			statsVer = int(*e.analyzePB.IdxReq.Version)
		}
	}
	pkHist := &statistics.Histogram{}
	collectors := make([]*statistics.SampleCollector, len(e.colsInfo))
	for i := range collectors {
		collectors[i] = &statistics.SampleCollector{
			IsMerger:      true,
			FMSketch:      statistics.NewFMSketch(statistics.MaxSketchSize),
			MaxSampleSize: int64(e.opts[ast.AnalyzeOptNumSamples]),
			CMSketch:      statistics.NewCMSketch(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth])),
		}
	}
	for {
		failpoint.Inject("mockKillRunningV1AnalyzeJob", func() {
			dom := domain.GetDomain(e.ctx)
			dom.SysProcTracker().KillSysProcess(dom.GetAutoAnalyzeProcID())
		})
		if err := e.ctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		failpoint.Inject("mockSlowAnalyzeV1", func() {
			time.Sleep(1000 * time.Second)
		})
		data, err1 := e.resultHandler.nextRaw(context.TODO())
		if err1 != nil {
			return nil, nil, nil, nil, nil, err1
		}
		if data == nil {
			break
		}
		var colResp *tipb.AnalyzeColumnsResp
		if e.analyzePB.Tp == tipb.AnalyzeType_TypeMixed {
			resp := &tipb.AnalyzeMixedResp{}
			err = resp.Unmarshal(data)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			colResp = resp.ColumnsResp
			handleHist, handleCms, handleFms, handleTopn, err = updateIndexResult(e.ctx, resp.IndexResp, nil, handleHist,
				handleCms, handleFms, handleTopn, e.commonHandle, int(e.opts[ast.AnalyzeOptNumBuckets]),
				int(e.opts[ast.AnalyzeOptNumTopN]), statsVer)

			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
		} else {
			colResp = &tipb.AnalyzeColumnsResp{}
			err = colResp.Unmarshal(data)
		}
		sc := e.ctx.GetSessionVars().StmtCtx
		rowCount := int64(0)
		if hasPkHist(e.handleCols) {
			respHist := statistics.HistogramFromProto(colResp.PkHist)
			rowCount = int64(respHist.TotalRowCount())
			pkHist, err = statistics.MergeHistograms(sc, pkHist, respHist, int(e.opts[ast.AnalyzeOptNumBuckets]), statistics.Version1)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
		}
		for i, rc := range colResp.Collectors {
			respSample := statistics.SampleCollectorFromProto(rc)
			rowCount = respSample.Count + respSample.NullCount
			collectors[i].MergeSampleCollector(sc, respSample)
		}
		UpdateAnalyzeJob(e.ctx, e.job, rowCount)
	}
	timeZone := e.ctx.GetSessionVars().Location()
	if hasPkHist(e.handleCols) {
		pkInfo := e.handleCols.GetCol(0)
		pkHist.ID = pkInfo.ID
		err = pkHist.DecodeTo(pkInfo.RetType, timeZone)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		hists = append(hists, pkHist)
		cms = append(cms, nil)
		topNs = append(topNs, nil)
		fms = append(fms, nil)
	}
	for i, col := range e.colsInfo {
		if e.StatsVersion < 2 {
			// In analyze version 2, we don't collect TopN this way. We will collect TopN from samples in `BuildColumnHistAndTopN()` below.
			err := collectors[i].ExtractTopN(uint32(e.opts[ast.AnalyzeOptNumTopN]), e.ctx.GetSessionVars().StmtCtx, &col.FieldType, timeZone)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			topNs = append(topNs, collectors[i].TopN)
		}
		for j, s := range collectors[i].Samples {
			s.Ordinal = j
			s.Value, err = tablecodec.DecodeColumnValue(s.Value.GetBytes(), &col.FieldType, timeZone)
			if err != nil {
				return nil, nil, nil, nil, nil, err
			}
			// When collation is enabled, we store the Key representation of the sampling data. So we set it to kind `Bytes` here
			// to avoid to convert it to its Key representation once more.
			if s.Value.Kind() == types.KindString {
				s.Value.SetBytes(s.Value.GetBytes())
			}
		}
		var hg *statistics.Histogram
		var err error
		var topn *statistics.TopN
		if e.StatsVersion < 2 {
			hg, err = statistics.BuildColumn(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), col.ID, collectors[i], &col.FieldType)
		} else {
			hg, topn, err = statistics.BuildHistAndTopN(e.ctx, int(e.opts[ast.AnalyzeOptNumBuckets]), int(e.opts[ast.AnalyzeOptNumTopN]), col.ID, collectors[i], &col.FieldType, true, nil, true)
			topNs = append(topNs, topn)
		}
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		hists = append(hists, hg)
		collectors[i].CMSketch.CalcDefaultValForAnalyze(uint64(hg.NDV))
		cms = append(cms, collectors[i].CMSketch)
		fms = append(fms, collectors[i].FMSketch)
	}
	if needExtStats {
		extStats, err = statistics.BuildExtendedStats(e.ctx, e.TableID.GetStatisticsID(), e.colsInfo, collectors)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}
	if handleHist != nil {
		handleHist.ID = e.commonHandle.ID
		if handleTopn != nil && handleTopn.TotalCount() > 0 {
			handleHist.RemoveVals(handleTopn.TopN)
		}
		if handleCms != nil {
			handleCms.CalcDefaultValForAnalyze(uint64(handleHist.NDV))
		}
		hists = append([]*statistics.Histogram{handleHist}, hists...)
		cms = append([]*statistics.CMSketch{handleCms}, cms...)
		fms = append([]*statistics.FMSketch{handleFms}, fms...)
		topNs = append([]*statistics.TopN{handleTopn}, topNs...)
	}
	return hists, cms, topNs, fms, extStats, nil
}

// AnalyzeColumnsExecV1 is used to maintain v1 analyze process
type AnalyzeColumnsExecV1 struct {
	*AnalyzeColumnsExec
}

func (e *AnalyzeColumnsExecV1) analyzeColumnsPushDownV1() *statistics.AnalyzeResults {
	var ranges []*ranger.Range
	if hc := e.handleCols; hc != nil {
		if hc.IsInt() {
			ranges = ranger.FullIntRange(mysql.HasUnsignedFlag(hc.GetCol(0).RetType.GetFlag()))
		} else {
			ranges = ranger.FullNotNullRange()
		}
	} else {
		ranges = ranger.FullIntRange(false)
	}
	collExtStats := e.ctx.GetSessionVars().EnableExtendedStats
	hists, cms, topNs, fms, extStats, err := e.buildStats(ranges, collExtStats)
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: e.job}
	}

	if hasPkHist(e.handleCols) {
		pkResult := &statistics.AnalyzeResult{
			Hist:  hists[:1],
			Cms:   cms[:1],
			TopNs: topNs[:1],
			Fms:   fms[:1],
		}
		restResult := &statistics.AnalyzeResult{
			Hist:  hists[1:],
			Cms:   cms[1:],
			TopNs: topNs[1:],
			Fms:   fms[1:],
		}
		return &statistics.AnalyzeResults{
			TableID:  e.tableID,
			Ars:      []*statistics.AnalyzeResult{pkResult, restResult},
			ExtStats: extStats,
			Job:      e.job,
			StatsVer: e.StatsVersion,
			Count:    int64(pkResult.Hist[0].TotalRowCount()),
			Snapshot: e.snapshot,
		}
	}
	var ars []*statistics.AnalyzeResult
	if e.analyzePB.Tp == tipb.AnalyzeType_TypeMixed {
		ars = append(ars, &statistics.AnalyzeResult{
			Hist:    []*statistics.Histogram{hists[0]},
			Cms:     []*statistics.CMSketch{cms[0]},
			TopNs:   []*statistics.TopN{topNs[0]},
			Fms:     []*statistics.FMSketch{nil},
			IsIndex: 1,
		})
		hists = hists[1:]
		cms = cms[1:]
		topNs = topNs[1:]
	}
	colResult := &statistics.AnalyzeResult{
		Hist:  hists,
		Cms:   cms,
		TopNs: topNs,
		Fms:   fms,
	}
	ars = append(ars, colResult)
	cnt := int64(hists[0].TotalRowCount())
	if e.StatsVersion >= statistics.Version2 {
		cnt += int64(topNs[0].TotalCount())
	}
	return &statistics.AnalyzeResults{
		TableID:  e.tableID,
		Ars:      ars,
		Job:      e.job,
		StatsVer: e.StatsVersion,
		ExtStats: extStats,
		Count:    cnt,
		Snapshot: e.snapshot,
	}
}

func hasPkHist(handleCols plannerutil.HandleCols) bool {
	return handleCols != nil && handleCols.IsInt()
}

func prepareV2AnalyzeJobInfo(e *AnalyzeColumnsExec, retry bool) {
	if e == nil || e.StatsVersion != statistics.Version2 {
		return
	}
	opts := e.opts
	cols := e.colsInfo
	if e.V2Options != nil {
		opts = e.V2Options.FilledOpts
	}
	sampleRate := *e.analyzePB.ColReq.SampleRate
	var b strings.Builder
	if retry {
		b.WriteString("retry ")
	}
	if e.ctx.GetSessionVars().InRestrictedSQL {
		b.WriteString("auto ")
	}
	b.WriteString("analyze table")
	if len(cols) > 0 && cols[len(cols)-1].ID == model.ExtraHandleID {
		cols = cols[:len(cols)-1]
	}
	if len(cols) < len(e.tableInfo.Columns) {
		b.WriteString(" columns ")
		for i, col := range cols {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(col.Name.O)
		}
	} else {
		b.WriteString(" all columns")
	}
	var needComma bool
	b.WriteString(" with ")
	printOption := func(optType ast.AnalyzeOptionType) {
		if val, ok := opts[optType]; ok {
			if needComma {
				b.WriteString(", ")
			} else {
				needComma = true
			}
			b.WriteString(fmt.Sprintf("%v %s", val, strings.ToLower(ast.AnalyzeOptionString[optType])))
		}
	}
	printOption(ast.AnalyzeOptNumBuckets)
	printOption(ast.AnalyzeOptNumTopN)
	if opts[ast.AnalyzeOptNumSamples] != 0 {
		printOption(ast.AnalyzeOptNumSamples)
	} else {
		if needComma {
			b.WriteString(", ")
		} else {
			needComma = true
		}
		b.WriteString(fmt.Sprintf("%v samplerate", sampleRate))
	}
	e.job.JobInfo = b.String()
}

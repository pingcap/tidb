// Copyright 2015 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/pdhelper"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
)

func (b *executorBuilder) buildAnalyzeIndexPushdown(task plannercore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64, autoAnalyze string) *analyzeTask {
	job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze index " + task.IndexInfo.Name.O}
	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	sc := b.ctx.GetSessionVars().StmtCtx
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	failpoint.Inject("injectAnalyzeSnapshot", func(val failpoint.Value) {
		startTS = uint64(val.(int))
	})
	concurrency := adaptiveAnlayzeDistSQLConcurrency(context.Background(), b.ctx)
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: concurrency,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeIndex,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts:     opts,
		job:      job,
		snapshot: startTS,
	}
	e := &AnalyzeIndexExec{
		baseAnalyzeExec: base,
		isCommonHandle:  task.TblInfo.IsCommonHandle,
		idxInfo:         task.IndexInfo,
	}
	topNSize := new(int32)
	*topNSize = int32(opts[ast.AnalyzeOptNumTopN])
	statsVersion := new(int32)
	*statsVersion = int32(task.StatsVersion)
	e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
		BucketSize: int64(opts[ast.AnalyzeOptNumBuckets]),
		NumColumns: int32(len(task.IndexInfo.Columns)),
		TopNSize:   topNSize,
		Version:    statsVersion,
		SketchSize: statistics.MaxSketchSize,
	}
	if e.isCommonHandle && e.idxInfo.Primary {
		e.analyzePB.Tp = tipb.AnalyzeType_TypeCommonHandle
	}
	depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
	width := int32(opts[ast.AnalyzeOptCMSketchWidth])
	e.analyzePB.IdxReq.CmsketchDepth = &depth
	e.analyzePB.IdxReq.CmsketchWidth = &width
	return &analyzeTask{taskType: idxTask, idxExec: e, job: job}
}

func (b *executorBuilder) buildAnalyzeSamplingPushdown(
	task plannercore.AnalyzeColumnsTask,
	opts map[ast.AnalyzeOptionType]uint64,
	schemaForVirtualColEval *expression.Schema,
) *analyzeTask {
	if task.V2Options != nil {
		opts = task.V2Options.FilledOpts
	}
	availableIdx := make([]*model.IndexInfo, 0, len(task.Indexes))
	colGroups := make([]*tipb.AnalyzeColumnGroup, 0, len(task.Indexes))
	if len(task.Indexes) > 0 {
		for _, idx := range task.Indexes {
			availableIdx = append(availableIdx, idx)
			colGroup := &tipb.AnalyzeColumnGroup{
				ColumnOffsets: make([]int64, 0, len(idx.Columns)),
			}
			for _, col := range idx.Columns {
				colGroup.ColumnOffsets = append(colGroup.ColumnOffsets, int64(col.Offset))
			}
			colGroups = append(colGroups, colGroup)
		}
	}

	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	sc := b.ctx.GetSessionVars().StmtCtx
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	failpoint.Inject("injectAnalyzeSnapshot", func(val failpoint.Value) {
		startTS = uint64(val.(int))
	})
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	count, modifyCount, err := statsHandle.StatsMetaCountAndModifyCount(task.TableID.GetStatisticsID())
	if err != nil {
		b.err = err
		return nil
	}
	failpoint.Inject("injectBaseCount", func(val failpoint.Value) {
		count = int64(val.(int))
	})
	failpoint.Inject("injectBaseModifyCount", func(val failpoint.Value) {
		modifyCount = int64(val.(int))
	})
	sampleRate := new(float64)
	var sampleRateReason string
	if opts[ast.AnalyzeOptNumSamples] == 0 {
		*sampleRate = math.Float64frombits(opts[ast.AnalyzeOptSampleRate])
		if *sampleRate < 0 {
			*sampleRate, sampleRateReason = b.getAdjustedSampleRate(task)
			if task.PartitionName != "" {
				sc.AppendNote(errors.NewNoStackErrorf(
					`Analyze use auto adjusted sample rate %f for table %s.%s's partition %s, reason to use this rate is "%s"`,
					*sampleRate,
					task.DBName,
					task.TableName,
					task.PartitionName,
					sampleRateReason,
				))
			} else {
				sc.AppendNote(errors.NewNoStackErrorf(
					`Analyze use auto adjusted sample rate %f for table %s.%s, reason to use this rate is "%s"`,
					*sampleRate,
					task.DBName,
					task.TableName,
					sampleRateReason,
				))
			}
		}
	}
	job := &statistics.AnalyzeJob{
		DBName:           task.DBName,
		TableName:        task.TableName,
		PartitionName:    task.PartitionName,
		SampleRateReason: sampleRateReason,
	}
	concurrency := adaptiveAnlayzeDistSQLConcurrency(context.Background(), b.ctx)
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: concurrency,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeFullSampling,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts:     opts,
		job:      job,
		snapshot: startTS,
	}
	e := &AnalyzeColumnsExec{
		baseAnalyzeExec:         base,
		tableInfo:               task.TblInfo,
		colsInfo:                task.ColsInfo,
		handleCols:              task.HandleCols,
		indexes:                 availableIdx,
		AnalyzeInfo:             task.AnalyzeInfo,
		schemaForVirtualColEval: schemaForVirtualColEval,
		baseCount:               count,
		baseModifyCnt:           modifyCount,
	}
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:   int64(opts[ast.AnalyzeOptNumBuckets]),
		SampleSize:   int64(opts[ast.AnalyzeOptNumSamples]),
		SampleRate:   sampleRate,
		SketchSize:   statistics.MaxSketchSize,
		ColumnsInfo:  util.ColumnsToProto(task.ColsInfo, task.TblInfo.PKIsHandle, false, false),
		ColumnGroups: colGroups,
	}
	if task.TblInfo != nil {
		e.analyzePB.ColReq.PrimaryColumnIds = tables.TryGetCommonPkColumnIds(task.TblInfo)
		if task.TblInfo.IsCommonHandle {
			e.analyzePB.ColReq.PrimaryPrefixColumnIds = tables.PrimaryPrefixColumnIDs(task.TblInfo)
		}
	}
	b.err = tables.SetPBColumnsDefaultValue(b.ctx.GetExprCtx(), e.analyzePB.ColReq.ColumnsInfo, task.ColsInfo)
	return &analyzeTask{taskType: colTask, colExec: e, job: job}
}

// getAdjustedSampleRate calculate the sample rate by the table size. If we cannot get the table size. We use the 0.001 as the default sample rate.
// From the paper "Random sampling for histogram construction: how much is enough?"'s Corollary 1 to Theorem 5,
// for a table size n, histogram size k, maximum relative error in bin size f, and error probability gamma,
// the minimum random sample size is
//
//	r = 4 * k * ln(2*n/gamma) / f^2
//
// If we take f = 0.5, gamma = 0.01, n =1e6, we would got r = 305.82* k.
// Since the there's log function over the table size n, the r grows slowly when the n increases.
// If we take n = 1e12, a 300*k sample still gives <= 0.66 bin size error with probability 0.99.
// So if we don't consider the top-n values, we can keep the sample size at 300*256.
// But we may take some top-n before building the histogram, so we increase the sample a little.
func (b *executorBuilder) getAdjustedSampleRate(task plannercore.AnalyzeColumnsTask) (sampleRate float64, reason string) {
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	defaultRate := 0.001
	if statsHandle == nil {
		return defaultRate, fmt.Sprintf("statsHandler is nil, use the default-rate=%v", defaultRate)
	}
	var statsTbl *statistics.Table
	tid := task.TableID.GetStatisticsID()
	if tid == task.TblInfo.ID {
		statsTbl = statsHandle.GetPhysicalTableStats(task.TblInfo.ID, task.TblInfo)
	} else {
		statsTbl = statsHandle.GetPhysicalTableStats(tid, task.TblInfo)
	}
	approxiCount, hasPD := b.getApproximateTableCountFromStorage(tid, task)
	// If there's no stats meta and no pd, return the default rate.
	if statsTbl == nil && !hasPD {
		return defaultRate, fmt.Sprintf("TiDB cannot get the row count of the table, use the default-rate=%v", defaultRate)
	}
	// If the count in stats_meta is still 0 and there's no information from pd side, we scan all rows.
	if statsTbl.RealtimeCount == 0 && !hasPD {
		return 1, "TiDB assumes that the table is empty and cannot get row count from PD, use sample-rate=1"
	}
	// we have issue https://github.com/pingcap/tidb/issues/29216.
	// To do a workaround for this issue, we check the approxiCount from the pd side to do a comparison.
	// If the count from the stats_meta is extremely smaller than the approximate count from the pd,
	// we think that we meet this issue and use the approximate count to calculate the sample rate.
	if float64(statsTbl.RealtimeCount*5) < approxiCount {
		// Confirmed by TiKV side, the experience error rate of the approximate count is about 20%.
		// So we increase the number to 150000 to reduce this error rate.
		sampleRate = math.Min(1, 150000/approxiCount)
		return sampleRate, fmt.Sprintf("Row count in stats_meta is much smaller compared with the row count got by PD, use min(1, 150000/%v) as the sample-rate=%v", approxiCount, sampleRate)
	}
	// If we don't go into the above if branch and we still detect the count is zero. Return 1 to prevent the dividing zero.
	if statsTbl.RealtimeCount == 0 {
		return 1, "TiDB assumes that the table is empty, use sample-rate=1"
	}
	// We are expected to scan about 100000 rows or so.
	// Since there's tiny error rate around the count from the stats meta, we use 110000 to get a little big result
	sampleRate = math.Min(1, config.DefRowsForSampleRate/float64(statsTbl.RealtimeCount))
	return sampleRate, fmt.Sprintf("use min(1, %v/%v) as the sample-rate=%v", config.DefRowsForSampleRate, statsTbl.RealtimeCount, sampleRate)
}

func (b *executorBuilder) getApproximateTableCountFromStorage(tid int64, task plannercore.AnalyzeColumnsTask) (float64, bool) {
	return pdhelper.GlobalPDHelper.GetApproximateTableCountFromStorage(context.Background(), b.ctx, tid, task.DBName, task.TableName, task.PartitionName)
}

func (b *executorBuilder) buildAnalyzeColumnsPushdown(
	task plannercore.AnalyzeColumnsTask,
	opts map[ast.AnalyzeOptionType]uint64,
	autoAnalyze string,
	schemaForVirtualColEval *expression.Schema,
) *analyzeTask {
	if task.StatsVersion == statistics.Version2 {
		return b.buildAnalyzeSamplingPushdown(task, opts, schemaForVirtualColEval)
	}
	job := &statistics.AnalyzeJob{DBName: task.DBName, TableName: task.TableName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze columns"}
	cols := task.ColsInfo
	if hasPkHist(task.HandleCols) {
		colInfo := task.TblInfo.Columns[task.HandleCols.GetCol(0).Index]
		cols = append([]*model.ColumnInfo{colInfo}, cols...)
	} else if task.HandleCols != nil && !task.HandleCols.IsInt() {
		cols = make([]*model.ColumnInfo, 0, len(task.ColsInfo)+task.HandleCols.NumCols())
		for col := range task.HandleCols.IterColumns() {
			cols = append(cols, task.TblInfo.Columns[col.Index])
		}
		cols = append(cols, task.ColsInfo...)
		task.ColsInfo = cols
	}

	_, offset := timeutil.Zone(b.ctx.GetSessionVars().Location())
	sc := b.ctx.GetSessionVars().StmtCtx
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	failpoint.Inject("injectAnalyzeSnapshot", func(val failpoint.Value) {
		startTS = uint64(val.(int))
	})
	concurrency := adaptiveAnlayzeDistSQLConcurrency(context.Background(), b.ctx)
	base := baseAnalyzeExec{
		ctx:         b.ctx,
		tableID:     task.TableID,
		concurrency: concurrency,
		analyzePB: &tipb.AnalyzeReq{
			Tp:             tipb.AnalyzeType_TypeColumn,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts:     opts,
		job:      job,
		snapshot: startTS,
	}
	e := &AnalyzeColumnsExec{
		baseAnalyzeExec: base,
		colsInfo:        task.ColsInfo,
		handleCols:      task.HandleCols,
		AnalyzeInfo:     task.AnalyzeInfo,
	}
	depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
	width := int32(opts[ast.AnalyzeOptCMSketchWidth])
	e.analyzePB.ColReq = &tipb.AnalyzeColumnsReq{
		BucketSize:    int64(opts[ast.AnalyzeOptNumBuckets]),
		SampleSize:    MaxRegionSampleSize,
		SketchSize:    statistics.MaxSketchSize,
		ColumnsInfo:   util.ColumnsToProto(cols, task.HandleCols != nil && task.HandleCols.IsInt(), false, false),
		CmsketchDepth: &depth,
		CmsketchWidth: &width,
	}
	if task.TblInfo != nil {
		e.analyzePB.ColReq.PrimaryColumnIds = tables.TryGetCommonPkColumnIds(task.TblInfo)
		if task.TblInfo.IsCommonHandle {
			e.analyzePB.ColReq.PrimaryPrefixColumnIds = tables.PrimaryPrefixColumnIDs(task.TblInfo)
		}
	}
	if task.CommonHandleInfo != nil {
		topNSize := new(int32)
		*topNSize = int32(opts[ast.AnalyzeOptNumTopN])
		statsVersion := new(int32)
		*statsVersion = int32(task.StatsVersion)
		e.analyzePB.IdxReq = &tipb.AnalyzeIndexReq{
			BucketSize: int64(opts[ast.AnalyzeOptNumBuckets]),
			NumColumns: int32(len(task.CommonHandleInfo.Columns)),
			TopNSize:   topNSize,
			Version:    statsVersion,
		}
		depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
		width := int32(opts[ast.AnalyzeOptCMSketchWidth])
		e.analyzePB.IdxReq.CmsketchDepth = &depth
		e.analyzePB.IdxReq.CmsketchWidth = &width
		e.analyzePB.IdxReq.SketchSize = statistics.MaxSketchSize
		e.analyzePB.ColReq.PrimaryColumnIds = tables.TryGetCommonPkColumnIds(task.TblInfo)
		e.analyzePB.Tp = tipb.AnalyzeType_TypeMixed
		e.commonHandle = task.CommonHandleInfo
	}
	b.err = tables.SetPBColumnsDefaultValue(b.ctx.GetExprCtx(), e.analyzePB.ColReq.ColumnsInfo, cols)
	return &analyzeTask{taskType: colTask, colExec: e, job: job}
}

func (b *executorBuilder) buildAnalyze(v *plannercore.Analyze) exec.Executor {
	gp := domain.GetDomain(b.ctx).StatsHandle().GPool()
	e := &AnalyzeExec{
		BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		tasks:        make([]*analyzeTask, 0, len(v.ColTasks)+len(v.IdxTasks)),
		opts:         v.Opts,
		OptionsMap:   v.OptionsMap,
		wg:           util.NewWaitGroupPool(gp),
		gp:           gp,
		errExitCh:    make(chan struct{}),
	}
	autoAnalyze := ""
	if b.ctx.GetSessionVars().InRestrictedSQL {
		autoAnalyze = "auto "
	}
	exprCtx := b.ctx.GetExprCtx()
	for _, task := range v.ColTasks {
		// ColumnInfos2ColumnsAndNames will use the `colInfos` to find the unique id for the column,
		// so we need to make sure all the columns pass into it.
		columns, _, err := expression.ColumnInfos2ColumnsAndNames(
			exprCtx,
			ast.NewCIStr(task.AnalyzeInfo.DBName),
			task.TblInfo.Name,
			append(task.ColsInfo, task.SkipColsInfo...),
			task.TblInfo,
		)
		columns = slices.DeleteFunc(columns, func(expr *expression.Column) bool {
			for _, col := range task.SkipColsInfo {
				if col.ID == expr.ID {
					return true
				}
			}
			return false
		})
		if err != nil {
			b.err = err
			return nil
		}
		schema := expression.NewSchema(columns...)
		e.tasks = append(e.tasks, b.buildAnalyzeColumnsPushdown(task, v.Opts, autoAnalyze, schema))
		// Other functions may set b.err, so we need to check it here.
		if b.err != nil {
			return nil
		}
	}
	for _, task := range v.IdxTasks {
		e.tasks = append(e.tasks, b.buildAnalyzeIndexPushdown(task, v.Opts, autoAnalyze))
		if b.err != nil {
			return nil
		}
	}
	return e
}


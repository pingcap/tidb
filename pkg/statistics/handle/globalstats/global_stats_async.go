// Copyright 2023 PingCAP, Inc.
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

package globalstats

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type mergeItem[T any] struct {
	item T
	idx  int
}

type skipItem struct {
	histID      int64
	partitionID int64
}

// toSQLIndex is used to convert bool to int64.
func toSQLIndex(isIndex bool) int {
	var index = int(0)
	if isIndex {
		index = 1
	}
	return index
}

// AsyncMergePartitionStats2GlobalStats is used to merge partition stats to global stats.
// it divides the merge task into two parts.
// - IOWorker: load stats from storage. it will load fmsketch, cmsketch, histogram and topn. and send them to cpuWorker.
// - CPUWorker: merge the stats from IOWorker and generate global stats.
//
// ┌────────────────────────┐        ┌───────────────────────┐
// │                        │        │                       │
// │                        │        │                       │
// │                        │        │                       │
// │    IOWorker            │        │   CPUWorker           │
// │                        │  ────► │                       │
// │                        │        │                       │
// │                        │        │                       │
// │                        │        │                       │
// └────────────────────────┘        └───────────────────────┘
type AsyncMergePartitionStats2GlobalStats struct {
	is                  infoschema.InfoSchema
	statsHandle         statstypes.StatsHandle
	globalStats         *GlobalStats
	cmsketch            chan mergeItem[*statistics.CMSketch]
	fmsketch            chan mergeItem[*statistics.FMSketch]
	histogramAndTopn    chan mergeItem[*StatsWrapper]
	allPartitionStats   map[int64]*statistics.Table
	PartitionDefinition map[int64]model.PartitionDefinition
	tableInfo           map[int64]*model.TableInfo
	// key is partition id and histID
	skipPartition map[skipItem]struct{}
	// ioWorker meet error, it will close this channel to notify cpuWorker.
	ioWorkerExitWhenErrChan chan struct{}
	// cpuWorker exit, it will close this channel to notify ioWorker.
	cpuWorkerExitChan         chan struct{}
	globalTableInfo           *model.TableInfo
	histIDs                   []int64
	globalStatsNDV            []int64
	partitionIDs              []int64
	partitionNum              int
	skipMissingPartitionStats bool
}

// NewAsyncMergePartitionStats2GlobalStats creates a new AsyncMergePartitionStats2GlobalStats.
func NewAsyncMergePartitionStats2GlobalStats(
	statsHandle statstypes.StatsHandle,
	globalTableInfo *model.TableInfo,
	histIDs []int64,
	is infoschema.InfoSchema) (*AsyncMergePartitionStats2GlobalStats, error) {
	partitionNum := len(globalTableInfo.Partition.Definitions)
	return &AsyncMergePartitionStats2GlobalStats{
		statsHandle:             statsHandle,
		cmsketch:                make(chan mergeItem[*statistics.CMSketch], 5),
		fmsketch:                make(chan mergeItem[*statistics.FMSketch], 5),
		histogramAndTopn:        make(chan mergeItem[*StatsWrapper]),
		PartitionDefinition:     make(map[int64]model.PartitionDefinition),
		tableInfo:               make(map[int64]*model.TableInfo),
		partitionIDs:            make([]int64, 0, partitionNum),
		ioWorkerExitWhenErrChan: make(chan struct{}),
		cpuWorkerExitChan:       make(chan struct{}),
		skipPartition:           make(map[skipItem]struct{}),
		allPartitionStats:       make(map[int64]*statistics.Table),
		globalTableInfo:         globalTableInfo,
		histIDs:                 histIDs,
		is:                      is,
		partitionNum:            partitionNum,
	}, nil
}

func (a *AsyncMergePartitionStats2GlobalStats) prepare(sctx sessionctx.Context, isIndex bool) (err error) {
	if len(a.histIDs) == 0 {
		for _, col := range a.globalTableInfo.Columns {
			// The virtual generated column stats can not be merged to the global stats.
			if col.IsVirtualGenerated() {
				continue
			}
			a.histIDs = append(a.histIDs, col.ID)
		}
	}
	a.globalStats = newGlobalStats(len(a.histIDs))
	a.globalStats.Num = len(a.histIDs)
	a.globalStatsNDV = make([]int64, 0, a.globalStats.Num)
	// get all partition stats
	for _, def := range a.globalTableInfo.Partition.Definitions {
		partitionID := def.ID
		a.partitionIDs = append(a.partitionIDs, partitionID)
		a.PartitionDefinition[partitionID] = def
		partitionTable, ok := a.statsHandle.TableInfoByID(a.is, partitionID)
		if !ok {
			return errors.Errorf("unknown physical ID %d in stats meta table, maybe it has been dropped", partitionID)
		}
		tableInfo := partitionTable.Meta()
		a.tableInfo[partitionID] = tableInfo
		realtimeCount, modifyCount, isNull, err := storage.StatsMetaCountAndModifyCount(sctx, partitionID)
		if err != nil {
			return err
		}
		if !isNull {
			// In a partition, we will only update globalStats.Count once.
			a.globalStats.Count += realtimeCount
			a.globalStats.ModifyCount += modifyCount
		}
		err1 := skipPartition(sctx, partitionID, isIndex)
		if err1 != nil {
			// no idx so idx = 0
			err := a.dealWithSkipPartition(partitionID, isIndex, 0, err1)
			if err != nil {
				return err
			}
			if types.ErrPartitionStatsMissing.Equal(err1) {
				continue
			}
		}
		for idx, hist := range a.histIDs {
			err1 := skipColumnPartition(sctx, partitionID, isIndex, hist)
			if err1 != nil {
				err := a.dealWithSkipPartition(partitionID, isIndex, idx, err1)
				if err != nil {
					return err
				}
				if types.ErrPartitionStatsMissing.Equal(err1) {
					break
				}
			}
		}
	}
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) dealWithSkipPartition(partitionID int64, isIndex bool, idx int, err error) error {
	switch {
	case types.ErrPartitionStatsMissing.Equal(err):
		return a.dealErrPartitionStatsMissing(partitionID)
	case types.ErrPartitionColumnStatsMissing.Equal(err):
		return a.dealErrPartitionColumnStatsMissing(isIndex, partitionID, idx)
	default:
		return err
	}
}

func (a *AsyncMergePartitionStats2GlobalStats) dealErrPartitionStatsMissing(partitionID int64) error {
	missingPart := fmt.Sprintf("partition `%s`", a.PartitionDefinition[partitionID].Name.L)
	a.globalStats.MissingPartitionStats = append(a.globalStats.MissingPartitionStats, missingPart)
	for _, histID := range a.histIDs {
		a.skipPartition[skipItem{
			histID:      histID,
			partitionID: partitionID,
		}] = struct{}{}
	}
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) dealErrPartitionColumnStatsMissing(isIndex bool, partitionID int64, idx int) error {
	var missingPart string
	if isIndex {
		missingPart = fmt.Sprintf("partition `%s` index `%s`", a.PartitionDefinition[partitionID].Name.L, a.tableInfo[partitionID].FindIndexNameByID(a.histIDs[idx]))
	} else {
		missingPart = fmt.Sprintf("partition `%s` column `%s`", a.PartitionDefinition[partitionID].Name.L, a.tableInfo[partitionID].FindColumnNameByID(a.histIDs[idx]))
	}
	if !a.skipMissingPartitionStats {
		return types.ErrPartitionColumnStatsMissing.GenWithStackByArgs(fmt.Sprintf("table `%s` %s", a.tableInfo[partitionID].Name.L, missingPart))
	}
	a.globalStats.MissingPartitionStats = append(a.globalStats.MissingPartitionStats, missingPart)
	a.skipPartition[skipItem{
		histID:      a.histIDs[idx],
		partitionID: partitionID,
	}] = struct{}{}
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) ioWorker(sctx sessionctx.Context, isIndex bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Warn("ioWorker panic", zap.Stack("stack"), zap.Any("error", r))
			close(a.ioWorkerExitWhenErrChan)
			err = errors.New(fmt.Sprint(r))
		}
	}()
	err = a.loadFmsketch(sctx, isIndex)
	if err != nil {
		close(a.ioWorkerExitWhenErrChan)
		return err
	}
	close(a.fmsketch)
	err = a.loadCMsketch(sctx, isIndex)
	if err != nil {
		close(a.ioWorkerExitWhenErrChan)
		return err
	}
	close(a.cmsketch)
	failpoint.Inject("PanicSameTime", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			time.Sleep(1 * time.Second)
			panic("test for PanicSameTime")
		}
	})
	err = a.loadHistogramAndTopN(sctx, a.globalTableInfo, isIndex)
	if err != nil {
		close(a.ioWorkerExitWhenErrChan)
		return err
	}
	close(a.histogramAndTopn)
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) cpuWorker(stmtCtx *stmtctx.StatementContext, sctx sessionctx.Context, opts map[ast.AnalyzeOptionType]uint64, isIndex bool, tz *time.Location, analyzeVersion int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			statslogutil.StatsLogger().Warn("cpuWorker panic", zap.Stack("stack"), zap.Any("error", r))
			err = errors.New(fmt.Sprint(r))
		}
		close(a.cpuWorkerExitChan)
	}()
	a.dealFMSketch()
	select {
	case <-a.ioWorkerExitWhenErrChan:
		return nil
	default:
		for i := 0; i < a.globalStats.Num; i++ {
			// Update the global NDV.
			globalStatsNDV := a.globalStats.Fms[i].NDV()
			if globalStatsNDV > a.globalStats.Count {
				globalStatsNDV = a.globalStats.Count
			}
			a.globalStatsNDV = append(a.globalStatsNDV, globalStatsNDV)
			a.globalStats.Fms[i].DestroyAndPutToPool()
		}
	}
	err = a.dealCMSketch()
	if err != nil {
		statslogutil.StatsLogger().Warn("dealCMSketch failed", zap.Error(err))
		return err
	}
	failpoint.Inject("PanicSameTime", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			time.Sleep(1 * time.Second)
			panic("test for PanicSameTime")
		}
	})
	err = a.dealHistogramAndTopN(stmtCtx, sctx, opts, isIndex, tz, analyzeVersion)
	if err != nil {
		statslogutil.StatsLogger().Warn("dealHistogramAndTopN failed", zap.Error(err))
		return err
	}
	return nil
}

// Result returns the global stats.
func (a *AsyncMergePartitionStats2GlobalStats) Result() *GlobalStats {
	return a.globalStats
}

// MergePartitionStats2GlobalStats merges partition stats to global stats.
func (a *AsyncMergePartitionStats2GlobalStats) MergePartitionStats2GlobalStats(
	sctx sessionctx.Context,
	opts map[ast.AnalyzeOptionType]uint64,
	isIndex bool,
) error {
	a.skipMissingPartitionStats = sctx.GetSessionVars().SkipMissingPartitionStats
	tz := sctx.GetSessionVars().StmtCtx.TimeZone()
	analyzeVersion := sctx.GetSessionVars().AnalyzeVersion
	stmtCtx := sctx.GetSessionVars().StmtCtx
	return util.CallWithSCtx(a.statsHandle.SPool(),
		func(sctx sessionctx.Context) error {
			err := a.prepare(sctx, isIndex)
			if err != nil {
				return err
			}
			ctx := context.Background()
			metawg, _ := errgroup.WithContext(ctx)
			mergeWg, _ := errgroup.WithContext(ctx)
			metawg.Go(func() error {
				return a.ioWorker(sctx, isIndex)
			})
			mergeWg.Go(func() error {
				return a.cpuWorker(stmtCtx, sctx, opts, isIndex, tz, analyzeVersion)
			})
			err = metawg.Wait()
			if err != nil {
				if err1 := mergeWg.Wait(); err1 != nil {
					err = stderrors.Join(err, err1)
				}
				return err
			}
			return mergeWg.Wait()
		},
	)
}

func (a *AsyncMergePartitionStats2GlobalStats) loadFmsketch(sctx sessionctx.Context, isIndex bool) error {
	for i := 0; i < a.globalStats.Num; i++ {
		// load fmsketch from tikv
		for _, partitionID := range a.partitionIDs {
			_, ok := a.skipPartition[skipItem{
				histID:      a.histIDs[i],
				partitionID: partitionID,
			}]
			if ok {
				continue
			}
			fmsketch, err := storage.FMSketchFromStorage(sctx, partitionID, int64(toSQLIndex(isIndex)), a.histIDs[i])
			if err != nil {
				return err
			}
			select {
			case a.fmsketch <- mergeItem[*statistics.FMSketch]{
				fmsketch, i,
			}:
			case <-a.cpuWorkerExitChan:
				statslogutil.StatsLogger().Warn("ioWorker detects CPUWorker has exited")
				return nil
			}
		}
	}
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) loadCMsketch(sctx sessionctx.Context, isIndex bool) error {
	failpoint.Inject("PanicInIOWorker", nil)
	for i := 0; i < a.globalStats.Num; i++ {
		for _, partitionID := range a.partitionIDs {
			_, ok := a.skipPartition[skipItem{
				histID:      a.histIDs[i],
				partitionID: partitionID,
			}]
			if ok {
				continue
			}
			cmsketch, err := storage.CMSketchFromStorage(sctx, partitionID, toSQLIndex(isIndex), a.histIDs[i])
			if err != nil {
				return err
			}
			a.cmsketch <- mergeItem[*statistics.CMSketch]{
				cmsketch, i,
			}
			select {
			case a.cmsketch <- mergeItem[*statistics.CMSketch]{
				cmsketch, i,
			}:
			case <-a.cpuWorkerExitChan:
				statslogutil.StatsLogger().Warn("ioWorker detects CPUWorker has exited")
				return nil
			}
		}
	}
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) loadHistogramAndTopN(sctx sessionctx.Context, tableInfo *model.TableInfo, isIndex bool) error {
	failpoint.Inject("ErrorSameTime", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			time.Sleep(1 * time.Second)
			failpoint.Return(errors.New("ErrorSameTime returned error"))
		}
	})
	for i := 0; i < a.globalStats.Num; i++ {
		hists := make([]*statistics.Histogram, 0, a.partitionNum)
		topn := make([]*statistics.TopN, 0, a.partitionNum)
		for _, partitionID := range a.partitionIDs {
			_, ok := a.skipPartition[skipItem{
				histID:      a.histIDs[i],
				partitionID: partitionID,
			}]
			if ok {
				continue
			}
			h, err := storage.LoadHistogram(sctx, partitionID, toSQLIndex(isIndex), a.histIDs[i], tableInfo)
			if err != nil {
				return err
			}
			t, err := storage.TopNFromStorage(sctx, partitionID, toSQLIndex(isIndex), a.histIDs[i])
			if err != nil {
				return err
			}
			hists = append(hists, h)
			topn = append(topn, t)
		}
		select {
		case a.histogramAndTopn <- mergeItem[*StatsWrapper]{
			NewStatsWrapper(hists, topn), i,
		}:
		case <-a.cpuWorkerExitChan:
			statslogutil.StatsLogger().Warn("ioWorker detects CPUWorker has exited")
			return nil
		}
	}
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) dealFMSketch() {
	failpoint.Inject("PanicInCPUWorker", nil)
	for {
		select {
		case fms, ok := <-a.fmsketch:
			if !ok {
				return
			}
			if a.globalStats.Fms[fms.idx] == nil {
				a.globalStats.Fms[fms.idx] = fms.item
			} else {
				a.globalStats.Fms[fms.idx].MergeFMSketch(fms.item)
			}
		case <-a.ioWorkerExitWhenErrChan:
			return
		}
	}
}

func (a *AsyncMergePartitionStats2GlobalStats) dealCMSketch() error {
	failpoint.Inject("dealCMSketchErr", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			failpoint.Return(errors.New("dealCMSketch returned error"))
		}
	})
	for {
		select {
		case cms, ok := <-a.cmsketch:
			if !ok {
				return nil
			}
			if a.globalStats.Cms[cms.idx] == nil {
				a.globalStats.Cms[cms.idx] = cms.item
			} else {
				err := a.globalStats.Cms[cms.idx].MergeCMSketch(cms.item)
				if err != nil {
					return err
				}
			}
		case <-a.ioWorkerExitWhenErrChan:
			return nil
		}
	}
}

func (a *AsyncMergePartitionStats2GlobalStats) dealHistogramAndTopN(stmtCtx *stmtctx.StatementContext, sctx sessionctx.Context, opts map[ast.AnalyzeOptionType]uint64, isIndex bool, tz *time.Location, analyzeVersion int) (err error) {
	failpoint.Inject("dealHistogramAndTopNErr", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			failpoint.Return(errors.New("dealHistogramAndTopNErr returned error"))
		}
	})
	failpoint.Inject("ErrorSameTime", func(val failpoint.Value) {
		if val, _ := val.(bool); val {
			time.Sleep(1 * time.Second)
			failpoint.Return(errors.New("ErrorSameTime returned error"))
		}
	})
	for {
		select {
		case item, ok := <-a.histogramAndTopn:
			if !ok {
				return nil
			}
			var err error
			var poppedTopN []statistics.TopNMeta
			var allhg []*statistics.Histogram
			wrapper := item.item
			a.globalStats.TopN[item.idx], poppedTopN, allhg, err = mergeGlobalStatsTopN(a.statsHandle.GPool(), sctx, wrapper,
				tz, analyzeVersion, uint32(opts[ast.AnalyzeOptNumTopN]), isIndex)
			if err != nil {
				return err
			}

			// Merge histogram.
			globalHg := &(a.globalStats.Hg[item.idx])
			*globalHg, err = statistics.MergePartitionHist2GlobalHist(stmtCtx, allhg, poppedTopN,
				int64(opts[ast.AnalyzeOptNumBuckets]), isIndex)
			if err != nil {
				return err
			}

			// NOTICE: after merging bucket NDVs have the trend to be underestimated, so for safe we don't use them.
			for j := range (*globalHg).Buckets {
				(*globalHg).Buckets[j].NDV = 0
			}
			(*globalHg).NDV = a.globalStatsNDV[item.idx]
		case <-a.ioWorkerExitWhenErrChan:
			return nil
		}
	}
}

func skipPartition(sctx sessionctx.Context, partitionID int64, isIndex bool) error {
	return storage.CheckSkipPartition(sctx, partitionID, toSQLIndex(isIndex))
}

func skipColumnPartition(sctx sessionctx.Context, partitionID int64, isIndex bool, histsID int64) error {
	return storage.CheckSkipColumnPartiion(sctx, partitionID, toSQLIndex(isIndex), histsID)
}

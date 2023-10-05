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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/storage"
	"github.com/pingcap/tidb/types"
	"github.com/tiancaiamao/gp"
	"golang.org/x/sync/errgroup"
)

type mergeItem[T any] struct {
	item T
	idx  int
}

// AsyncMergePartitionStats2GlobalStats is used to merge partition stats to global stats.
type AsyncMergePartitionStats2GlobalStats struct {
	is                        infoschema.InfoSchema
	globalStats               *GlobalStats
	cmsketch                  chan mergeItem[*statistics.CMSketch]
	fmsketch                  chan mergeItem[*statistics.FMSketch]
	topn                      chan mergeItem[*statistics.TopN]
	histogram                 chan mergeItem[*statistics.Histogram]
	gpool                     *gp.Pool
	allPartitionStats         map[int64]*statistics.Table
	PartitionDefinition       map[int64]model.PartitionDefinition
	tableInfo                 map[int64]*model.TableInfo
	getTableByPhysicalIDFn    getTableByPhysicalIDFunc
	loadTablePartitionStatsFn loadTablePartitionStatsFunc
	globalTableInfo           *model.TableInfo
	histIDs                   []int64
	globalStatsNDV            []int64
	partitionIDs              []int64
	partitionNum              int
	skipMissingPartitionStats bool
}

// NewAsyncMergePartitionStats2GlobalStats creates a new AsyncMergePartitionStats2GlobalStats.
func NewAsyncMergePartitionStats2GlobalStats(
	gpool *gp.Pool, allPartitionStats map[int64]*statistics.Table,
	globalTableInfo *model.TableInfo,
	sc sessionctx.Context,
	histIDs []int64,
	is infoschema.InfoSchema,
	getTableByPhysicalIDFn getTableByPhysicalIDFunc,
	loadTablePartitionStatsFn loadTablePartitionStatsFunc) *AsyncMergePartitionStats2GlobalStats {
	skipMissingPartitionStats := sc.GetSessionVars().SkipMissingPartitionStats
	globalStats := newGlobalStats(len(histIDs))
	globalStats.Num = len(histIDs)
	partitionNum := len(globalTableInfo.Partition.Definitions)
	globalStatsNDV := make([]int64, 0, globalStats.Num)
	return &AsyncMergePartitionStats2GlobalStats{
		cmsketch:                  make(chan mergeItem[*statistics.CMSketch], 10),
		fmsketch:                  make(chan mergeItem[*statistics.FMSketch], 10),
		topn:                      make(chan mergeItem[*statistics.TopN], 10),
		histogram:                 make(chan mergeItem[*statistics.Histogram], 10),
		PartitionDefinition:       make(map[int64]model.PartitionDefinition),
		tableInfo:                 make(map[int64]*model.TableInfo),
		partitionIDs:              make([]int64, 0, partitionNum),
		gpool:                     gpool,
		allPartitionStats:         allPartitionStats,
		globalTableInfo:           globalTableInfo,
		getTableByPhysicalIDFn:    getTableByPhysicalIDFn,
		loadTablePartitionStatsFn: loadTablePartitionStatsFn,
		histIDs:                   histIDs,
		is:                        is,
		skipMissingPartitionStats: skipMissingPartitionStats,
		globalStats:               globalStats,
		partitionNum:              partitionNum,
		globalStatsNDV:            globalStatsNDV,
	}
}

func (a *AsyncMergePartitionStats2GlobalStats) prepare() (err error) {
	if a.allPartitionStats == nil {
		a.allPartitionStats = make(map[int64]*statistics.Table)
	}
	if len(a.histIDs) == 0 {
		for _, col := range a.globalTableInfo.Columns {
			// The virtual generated column stats can not be merged to the global stats.
			if col.IsVirtualGenerated() {
				continue
			}
			a.histIDs = append(a.histIDs, col.ID)
		}
	}
	// get all partition stats
	for _, def := range a.globalTableInfo.Partition.Definitions {
		partitionID := def.ID
		a.partitionIDs = append(a.partitionIDs, partitionID)
		a.PartitionDefinition[partitionID] = def
		partitionTable, ok := a.getTableByPhysicalIDFn(a.is, partitionID)
		if !ok {
			return errors.Errorf("unknown physical ID %d in stats meta table, maybe it has been dropped", partitionID)
		}
		tableInfo := partitionTable.Meta()
		a.tableInfo[partitionID] = tableInfo
		partitionStats, okLoad := a.allPartitionStats[partitionID]
		if !okLoad {
			var err1 error
			partitionStats, err1 = a.loadTablePartitionStatsFn(tableInfo, &def)
			if err1 != nil {
				if a.skipMissingPartitionStats && types.ErrPartitionStatsMissing.Equal(err) {
					a.globalStats.MissingPartitionStats = append(a.globalStats.MissingPartitionStats, fmt.Sprintf("partition `%s`", def.Name.L))
					continue
				}
				err = err1
				return
			}
			a.allPartitionStats[partitionID] = partitionStats
		}
		// In a partition, we will only update globalStats.Count once.
		a.globalStats.Count += partitionStats.RealtimeCount
		a.globalStats.ModifyCount += partitionStats.ModifyCount
	}
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) dealWithSkipPartition(isIndex bool, partitionID int64, idx int, err *terror.Error) error {
	var missingPart string
	if isIndex {
		missingPart = fmt.Sprintf("partition `%s` index `%s`", a.PartitionDefinition[partitionID].Name.L, a.tableInfo[partitionID].FindIndexNameByID(a.histIDs[idx]))
	} else {
		missingPart = fmt.Sprintf("partition `%s` column `%s`", a.PartitionDefinition[partitionID].Name.L, a.tableInfo[partitionID].FindColumnNameByID(a.histIDs[idx]))
	}
	if !a.skipMissingPartitionStats {
		return err.GenWithStackByArgs(fmt.Sprintf("table `%s` %s", a.tableInfo[partitionID].Name.L, missingPart))
	}
	a.globalStats.MissingPartitionStats = append(a.globalStats.MissingPartitionStats, missingPart)
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) ioWorker(sc sessionctx.Context, isIndex bool) error {
	err := a.loadFmsketch(sc, isIndex)
	if err != nil {
		return err
	}
	close(a.fmsketch)
	for i := 0; i < a.globalStats.Num; i++ {
		for partitionID, partitionStats := range a.allPartitionStats {
			isContinue, err := a.checkSkipPartition(partitionStats, partitionID, i, isIndex)
			if err != nil {
				return err
			}
			if isContinue {
				continue
			}
			cmsketch, find := partitionStats.GetCMSketch(a.histIDs[i], isIndex)
			if find {
				a.cmsketch <- mergeItem[*statistics.CMSketch]{
					cmsketch, i,
				}
			}
		}
	}
	close(a.cmsketch)
	for i := 0; i < a.globalStats.Num; i++ {
		for partitionID, partitionStats := range a.allPartitionStats {
			isContinue, err := a.checkSkipPartition(partitionStats, partitionID, i, isIndex)
			if err != nil {
				return err
			}
			if isContinue {
				continue
			}
			topn, find := partitionStats.GetTopN(a.histIDs[i], isIndex)
			if find {
				a.topn <- mergeItem[*statistics.TopN]{
					topn, i,
				}
			}
		}
	}
	close(a.topn)
	for i := 0; i < a.globalStats.Num; i++ {
		for partitionID, partitionStats := range a.allPartitionStats {
			isContinue, err := a.checkSkipPartition(partitionStats, partitionID, i, isIndex)
			if err != nil {
				return err
			}
			if isContinue {
				continue
			}
			hists, find := partitionStats.GetHistogram(a.histIDs[i], isIndex)
			if find {
				a.histogram <- mergeItem[*statistics.Histogram]{
					hists, i,
				}
			}
		}
	}
	close(a.histogram)
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) cpuWorker(sc sessionctx.Context,
	opts map[ast.AnalyzeOptionType]uint64, isIndex bool) error {
	for fms := range a.fmsketch {
		if a.globalStats.Fms[fms.idx] == nil {
			a.globalStats.Fms[fms.idx] = fms.item
		} else {
			a.globalStats.Fms[fms.idx].MergeFMSketch(fms.item)
		}
	}
	for i := 0; i < a.globalStats.Num; i++ {
		// Update the global NDV.
		globalStatsNDV := a.globalStats.Fms[i].NDV()
		if globalStatsNDV > a.globalStats.Count {
			globalStatsNDV = a.globalStats.Count
		}
		a.globalStatsNDV = append(a.globalStatsNDV, globalStatsNDV)
		a.globalStats.Fms[i].DestroyAndPutToPool()
	}
	for cms := range a.cmsketch {
		if a.globalStats.Cms[cms.idx] == nil {
			a.globalStats.Cms[cms.idx] = cms.item
		} else {
			err := a.globalStats.Cms[cms.idx].MergeCMSketch(cms.item)
			if err != nil {
				return err
			}
		}
	}
	allHg := make([][]*statistics.Histogram, a.globalStats.Num)
	allTopN := make([][]*statistics.TopN, a.globalStats.Num)
	for i := 0; i < a.globalStats.Num; i++ {
		allHg[i] = make([]*statistics.Histogram, 0, a.partitionNum)
		allTopN[i] = make([]*statistics.TopN, 0, a.partitionNum)
	}
	for topn := range a.topn {
		allTopN[topn.idx] = append(allTopN[topn.idx], topn.item)
	}
	for hists := range a.histogram {
		allHg[hists.idx] = append(allHg[hists.idx], hists.item)
	}
	for i := 0; i < a.globalStats.Num; i++ {
		if len(allHg[i]) == 0 {
			// If all partitions have no stats, we skip merging global stats because it may not handle the case `len(allHg[i]) == 0`
			// correctly. It can avoid unexpected behaviors such as nil pointer panic.
			continue
		}
		// Merge topN.
		// Note: We need to merge TopN before merging the histogram.
		// Because after merging TopN, some numbers will be left.
		// These remaining topN numbers will be used as a separate bucket for later histogram merging.
		var err error
		var poppedTopN []statistics.TopNMeta
		wrapper := NewStatsWrapper(allHg[i], allTopN[i])
		a.globalStats.TopN[i], poppedTopN, allHg[i], err = mergeGlobalStatsTopN(a.gpool, sc, wrapper,
			sc.GetSessionVars().StmtCtx.TimeZone, sc.GetSessionVars().AnalyzeVersion, uint32(opts[ast.AnalyzeOptNumTopN]), isIndex)
		if err != nil {
			return err
		}

		// Merge histogram.
		a.globalStats.Hg[i], err = statistics.MergePartitionHist2GlobalHist(sc.GetSessionVars().StmtCtx, allHg[i], poppedTopN,
			int64(opts[ast.AnalyzeOptNumBuckets]), isIndex)
		if err != nil {
			return err
		}

		// NOTICE: after merging bucket NDVs have the trend to be underestimated, so for safe we don't use them.
		for j := range a.globalStats.Hg[i].Buckets {
			a.globalStats.Hg[i].Buckets[j].NDV = 0
		}
		a.globalStats.Hg[i].NDV = a.globalStatsNDV[i]
	}
	return nil
}

// Result returns the global stats.
func (a *AsyncMergePartitionStats2GlobalStats) Result() *GlobalStats {
	return a.globalStats
}

// MergePartitionStats2GlobalStats merges partition stats to global stats.
func (a *AsyncMergePartitionStats2GlobalStats) MergePartitionStats2GlobalStats(
	sc sessionctx.Context,
	opts map[ast.AnalyzeOptionType]uint64,
	isIndex bool,
) error {
	ctx := context.Background()
	metawg, _ := errgroup.WithContext(ctx)
	mergeWg, _ := errgroup.WithContext(ctx)
	metawg.Go(func() error {
		return a.ioWorker(sc, isIndex)
	})
	mergeWg.Go(func() error {
		return a.cpuWorker(sc, opts, isIndex)
	})
	err := metawg.Wait()
	if err != nil {
		if err1 := mergeWg.Wait(); err1 != nil {
			err = stderrors.Join(err, err1)
		}
		return err
	}
	return mergeWg.Wait()
}

func (a *AsyncMergePartitionStats2GlobalStats) loadFmsketch(sc sessionctx.Context, isIndex bool) error {
	for i := 0; i < a.globalStats.Num; i++ {
		if a.allPartitionStats != nil {
			// use cache to load fmsketch
			for _, partitionStats := range a.allPartitionStats {
				fmsketch, find := partitionStats.GetFMSketch(a.histIDs[i], isIndex)
				if find {
					a.fmsketch <- mergeItem[*statistics.FMSketch]{
						fmsketch, i,
					}
				}
			}
		} else {
			// load fmsketch from tikv
			for _, partitionID := range a.partitionIDs {
				var index = int64(0)
				if isIndex {
					index = 1
				}
				fmsketch, err := storage.FMSketchFromStorage(sc, partitionID, index, a.histIDs[i])
				if err != nil {
					return err
				}
				a.fmsketch <- mergeItem[*statistics.FMSketch]{
					fmsketch, i,
				}
			}
		}
	}
	return nil
}

func (a *AsyncMergePartitionStats2GlobalStats) checkSkipPartition(partitionStats *statistics.Table, partitionID int64, i int, isIndex bool) (isContine bool, err error) {
	var analyzed, isSkip bool
	isSkip, analyzed = partitionStats.IsSkipPartition(a.histIDs[i], isIndex)
	if !analyzed {
		err := a.dealWithSkipPartition(isIndex, partitionID, i, types.ErrPartitionStatsMissing)
		if err != nil {
			close(a.cmsketch)
			close(a.histogram)
			close(a.topn)
			return false, err
		}
		return true, nil
	}
	if partitionStats.RealtimeCount > 0 && isSkip {
		err := a.dealWithSkipPartition(isIndex, partitionID, i, types.ErrPartitionColumnStatsMissing)
		if err != nil {
			close(a.cmsketch)
			close(a.histogram)
			close(a.topn)
			return false, err
		}
		return true, nil
	}
	return false, nil
}

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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type globalStatsKey struct {
	tableID int64
	indexID int64
}

// globalStatsMap is a map used to store which partition tables and the corresponding indexes need global-level stats.
// The meaning of key in map is the structure that used to store the tableID and indexID.
// The meaning of value in map is some additional information needed to build global-level stats.
type globalStatsMap map[globalStatsKey]statstypes.GlobalStatsInfo

func (e *AnalyzeExec) handleGlobalStats(statsHandle *handle.Handle, globalStatsMap globalStatsMap) error {
	globalStatsTableIDs := make(map[int64]struct{}, len(globalStatsMap))
	for globalStatsID := range globalStatsMap {
		globalStatsTableIDs[globalStatsID.tableID] = struct{}{}
	}

	// Load saved samples for non-analyzed partitions and merge into global collectors.
	statslogutil.StatsLogger().Info("analyze global: loading saved samples",
		zap.Int("tables", len(globalStatsTableIDs)))
	e.loadAndMergeSavedSamples(globalStatsTableIDs)
	statslogutil.StatsLogger().Info("analyze global: loading saved samples done")
	// Save pruned samples for freshly analyzed partitions.
	statslogutil.StatsLogger().Info("analyze global: saving partition samples",
		zap.Int("partitions", len(e.partitionSamplesToSave)))
	e.savePartitionSamplesToStorage()
	statslogutil.StatsLogger().Info("analyze global: saving partition samples done")

	tableIDs := make(map[int64]struct{}, len(globalStatsTableIDs))
	for tableID := range globalStatsTableIDs {
		tableIDs[tableID] = struct{}{}
		for globalStatsID, info := range globalStatsMap {
			if globalStatsID.tableID != tableID {
				continue
			}
			job := e.newAnalyzeHandleGlobalStatsJob(globalStatsID)
			if job == nil {
				logutil.BgLogger().Warn("cannot find the partitioned table, skip merging global stats", zap.Int64("tableID", globalStatsID.tableID))
				continue
			}
			statslogutil.StatsLogger().Info("analyze global: merge entry start",
				zap.String("job", job.JobInfo), zap.Int64("tableID", globalStatsID.tableID),
				zap.Int64("indexID", globalStatsID.indexID))
			AddNewAnalyzeJob(e.Ctx(), job)
			statsHandle.StartAnalyzeJob(job)

			mergeStatsErr := func() error {
				globalOpts := e.opts
				if e.OptionsMap != nil {
					if v2Options, ok := e.OptionsMap[globalStatsID.tableID]; ok {
						globalOpts = v2Options.FilledOpts
					}
				}

				// Try sample-based path first.
				if gs, err := e.buildGlobalStatsFromSamples(globalStatsID, &info, globalOpts); err != nil {
					logutil.ErrVerboseLogger().Warn("build sample-based global stats failed, falling back to merge",
						zap.String("info", job.JobInfo), zap.Error(err), zap.Int64("tableID", tableID))
				} else if gs != nil {
					statslogutil.StatsLogger().Info("analyze global: merge entry built (sample-based)",
						zap.String("job", job.JobInfo), zap.Int64("tableID", globalStatsID.tableID))
					writeErr := globalstats.WriteGlobalStatsToStorage(statsHandle, gs, &info, globalStatsID.tableID)
					statslogutil.StatsLogger().Info("analyze global: merge entry written",
						zap.String("job", job.JobInfo), zap.Int64("tableID", globalStatsID.tableID),
						zap.Error(writeErr))
					return writeErr
				}

				// Fallback to existing merge-based path.
				statslogutil.StatsLogger().Info("analyze global: merge entry using partition-merge path",
					zap.String("job", job.JobInfo), zap.Int64("tableID", globalStatsID.tableID))
				err := statsHandle.MergePartitionStats2GlobalStatsByTableID(
					e.Ctx(),
					globalOpts, e.Ctx().GetInfoSchema().(infoschema.InfoSchema),
					&info,
					globalStatsID.tableID,
				)
				if err != nil {
					logutil.ErrVerboseLogger().Warn("merge global stats failed",
						zap.String("info", job.JobInfo), zap.Error(err), zap.Int64("tableID", tableID))
				}
				statslogutil.StatsLogger().Info("analyze global: merge entry done (partition-merge)",
					zap.String("job", job.JobInfo), zap.Int64("tableID", globalStatsID.tableID),
					zap.Error(err))
				return err
			}()
			statsHandle.FinishAnalyzeJob(job, mergeStatsErr, statistics.GlobalStatsMergeJob)
		}
	}

	for tableID := range tableIDs {
		// Dump stats to historical storage.
		if err := recordHistoricalStats(e.Ctx(), tableID); err != nil {
			logutil.BgLogger().Error("record historical stats failed", zap.Error(err))
		}
	}

	return nil
}

// buildGlobalStatsFromSamples attempts to build global stats from the accumulated
// sample collector. Returns (nil, nil) if sample-based path is not available for
// this particular stats entry, signaling the caller to fall back.
func (e *AnalyzeExec) buildGlobalStatsFromSamples(
	key globalStatsKey,
	info *statstypes.GlobalStatsInfo,
	opts map[ast.AnalyzeOptionType]uint64,
) (*globalstats.GlobalStats, error) {
	if e.globalSampleCollectors == nil {
		return nil, nil
	}
	collector, ok := e.globalSampleCollectors[key.tableID]
	if !ok || collector == nil || collector.Base().Count == 0 {
		return nil, nil
	}

	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	table, ok := is.TableByID(context.Background(), key.tableID)
	if !ok {
		return nil, nil
	}
	colsInfo := table.Meta().Columns
	indexes := table.Meta().Indices

	isIndex := info.IsIndex == 1
	return globalstats.BuildGlobalStatsFromSamples(
		e.Ctx(), collector, opts,
		colsInfo, indexes, info.HistIDs, isIndex,
	)
}

// loadAndMergeSavedSamples loads persisted sample collectors for partitions
// that were NOT freshly analyzed and merges them into the global collectors.
func (e *AnalyzeExec) loadAndMergeSavedSamples(tableIDs map[int64]struct{}) {
	if e.globalSampleCollectors == nil {
		return
	}
	is := e.Ctx().GetInfoSchema().(infoschema.InfoSchema)
	sctx := e.Ctx()

	for tableID := range tableIDs {
		globalCollector, ok := e.globalSampleCollectors[tableID]
		if !ok || globalCollector == nil {
			continue
		}

		tbl, ok := is.TableByID(context.Background(), tableID)
		if !ok {
			continue
		}
		pi := tbl.Meta().GetPartitionInfo()
		if pi == nil {
			continue
		}

		analyzed := e.analyzedPartitions[tableID]
		// If all partitions were analyzed, no loading needed.
		if len(analyzed) >= len(pi.Definitions) {
			statslogutil.StatsLogger().Info("analyze global: all partitions freshly analyzed, skip loading saved samples",
				zap.Int64("tableID", tableID), zap.Int("partitions", len(pi.Definitions)))
			continue
		}
		toLoad := len(pi.Definitions) - len(analyzed)
		statslogutil.StatsLogger().Info("analyze global: loading saved samples for table",
			zap.Int64("tableID", tableID), zap.Int("analyzed", len(analyzed)),
			zap.Int("toLoad", toLoad), zap.Int("total", len(pi.Definitions)))

		for _, def := range pi.Definitions {
			if _, freshlyAnalyzed := analyzed[def.ID]; freshlyAnalyzed {
				continue
			}
			saved, err := storage.LoadPartitionSample(sctx, def.ID)
			if err != nil {
				logutil.BgLogger().Warn("failed to load saved partition samples",
					zap.Int64("partitionID", def.ID), zap.Error(err))
				continue
			}
			if saved == nil {
				continue
			}
			// Validate schema compatibility by FM sketch count.
			if len(saved.Base().FMSketches) != len(globalCollector.Base().FMSketches) {
				logutil.BgLogger().Warn("saved partition sample schema mismatch, skipping",
					zap.Int64("partitionID", def.ID),
					zap.Int("savedCols", len(saved.Base().FMSketches)),
					zap.Int("expectedCols", len(globalCollector.Base().FMSketches)))
				saved.DestroyAndPutToPool()
				continue
			}
			globalCollector.MergeCollector(saved)
			saved.DestroyAndPutToPool()
		}
	}
}

// savePartitionSamplesToStorage persists the pre-serialized per-partition
// sample data to mysql.stats_global_merge_data.
func (e *AnalyzeExec) savePartitionSamplesToStorage() {
	if len(e.partitionSamplesToSave) == 0 {
		return
	}
	sctx := e.Ctx()
	for partitionID, data := range e.partitionSamplesToSave {
		if err := storage.SavePartitionSampleData(sctx, partitionID, data); err != nil {
			logutil.BgLogger().Warn("failed to save partition samples",
				zap.Int64("partitionID", partitionID), zap.Error(err))
		}
		delete(e.partitionSamplesToSave, partitionID)
	}
}

func (e *AnalyzeExec) newAnalyzeHandleGlobalStatsJob(key globalStatsKey) *statistics.AnalyzeJob {
	dom := domain.GetDomain(e.Ctx())
	is := dom.InfoSchema()
	table, ok := is.TableByID(context.Background(), key.tableID)
	if !ok {
		return nil
	}
	db, ok := infoschema.SchemaByTable(is, table.Meta())
	if !ok {
		return nil
	}
	dbName := db.Name.String()
	tableName := table.Meta().Name.String()
	jobInfo := fmt.Sprintf("merge global stats for %v.%v columns", dbName, tableName)
	if key.indexID != -1 {
		idxName := table.Meta().FindIndexNameByID(key.indexID)
		jobInfo = fmt.Sprintf("merge global stats for %v.%v's index %v", dbName, tableName, idxName)
	}
	return &statistics.AnalyzeJob{
		DBName:    db.Name.String(),
		TableName: table.Meta().Name.String(),
		JobInfo:   jobInfo,
	}
}

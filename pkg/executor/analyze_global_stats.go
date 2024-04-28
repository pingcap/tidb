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
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/globalstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type globalStatsKey struct {
	tableID int64
	indexID int64
}

type globalStatsInfo struct {
	isIndex int
	// When the `isIndex == 0`, histIDs will be the column IDs.
	// Otherwise, histIDs will only contain the index ID.
	histIDs      []int64
	statsVersion int
}

// globalStatsMap is a map used to store which partition tables and the corresponding indexes need global-level stats.
// The meaning of key in map is the structure that used to store the tableID and indexID.
// The meaning of value in map is some additional information needed to build global-level stats.
type globalStatsMap map[globalStatsKey]globalStatsInfo

func (e *AnalyzeExec) handleGlobalStats(ctx context.Context, globalStatsMap globalStatsMap) error {
	globalStatsTableIDs := make(map[int64]struct{}, len(globalStatsMap))
	for globalStatsID := range globalStatsMap {
		globalStatsTableIDs[globalStatsID.tableID] = struct{}{}
	}

	statsHandle := domain.GetDomain(e.Ctx()).StatsHandle()
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
			AddNewAnalyzeJob(e.Ctx(), job)
			StartAnalyzeJob(e.Ctx(), job)

			mergeStatsErr := func() error {
				globalOpts := e.opts
				if e.OptionsMap != nil {
					if v2Options, ok := e.OptionsMap[globalStatsID.tableID]; ok {
						globalOpts = v2Options.FilledOpts
					}
				}
				globalStatsI, err := statsHandle.MergePartitionStats2GlobalStatsByTableID(
					e.Ctx(),
					globalOpts, e.Ctx().GetInfoSchema().(infoschema.InfoSchema),
					globalStatsID.tableID,
					info.isIndex == 1,
					info.histIDs,
				)
				if err != nil {
					logutil.BgLogger().Warn("merge global stats failed",
						zap.String("info", job.JobInfo), zap.Error(err), zap.Int64("tableID", tableID))
					if types.ErrPartitionStatsMissing.Equal(err) || types.ErrPartitionColumnStatsMissing.Equal(err) {
						// When we find some partition-level stats are missing, we need to report warning.
						e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
					}
					return err
				}
				globalStats := globalStatsI.(*globalstats.GlobalStats)
				// Dump global-level stats to kv.
				for i := 0; i < globalStats.Num; i++ {
					hg, cms, topN := globalStats.Hg[i], globalStats.Cms[i], globalStats.TopN[i]
					if hg == nil {
						// All partitions have no stats so global stats are not created.
						continue
					}
					// fms for global stats doesn't need to dump to kv.
					err = statsHandle.SaveStatsToStorage(globalStatsID.tableID,
						globalStats.Count,
						globalStats.ModifyCount,
						info.isIndex,
						hg,
						cms,
						topN,
						info.statsVersion,
						1,
						true,
						util.StatsMetaHistorySourceAnalyze,
					)
					if err != nil {
						logutil.Logger(ctx).Error("save global-level stats to storage failed", zap.String("info", job.JobInfo),
							zap.Int64("histID", hg.ID), zap.Error(err), zap.Int64("tableID", tableID))
					}
				}
				return err
			}()
			FinishAnalyzeMergeJob(e.Ctx(), job, mergeStatsErr)
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

func (e *AnalyzeExec) newAnalyzeHandleGlobalStatsJob(key globalStatsKey) *statistics.AnalyzeJob {
	dom := domain.GetDomain(e.Ctx())
	is := dom.InfoSchema()
	table, ok := is.TableByID(key.tableID)
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

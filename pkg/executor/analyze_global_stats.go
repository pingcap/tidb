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
			statsHandle.StartAnalyzeJob(job)

			mergeStatsErr := func() error {
				globalOpts := e.opts
				if e.OptionsMap != nil {
					if v2Options, ok := e.OptionsMap[globalStatsID.tableID]; ok {
						globalOpts = v2Options.FilledOpts
					}
				}

				// Try sample-based path first.
				if gs, err := e.buildGlobalStatsFromSamples(statsHandle, globalStatsID, &info, globalOpts); err != nil {
					logutil.ErrVerboseLogger().Warn("build sample-based global stats failed, falling back to merge",
						zap.String("info", job.JobInfo), zap.Error(err), zap.Int64("tableID", tableID))
				} else if gs != nil {
					return globalstats.WriteGlobalStatsToStorage(statsHandle, gs, &info, globalStatsID.tableID)
				}

				// Fallback to existing merge-based path.
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
	statsHandle *handle.Handle,
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

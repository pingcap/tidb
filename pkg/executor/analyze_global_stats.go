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
	"fmt"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/statistics"
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

func (e *AnalyzeExec) handleGlobalStats(globalStatsMap globalStatsMap) error {
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
				err := statsHandle.MergePartitionStats2GlobalStatsByTableID(
					e.Ctx(),
					globalOpts, e.Ctx().GetInfoSchema().(infoschema.InfoSchema),
					&info,
					globalStatsID.tableID,
				)
				if err != nil {
					logutil.BgLogger().Warn("merge global stats failed",
						zap.String("info", job.JobInfo), zap.Error(err), zap.Int64("tableID", tableID))
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

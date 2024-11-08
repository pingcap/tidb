// Copyright 2024 PingCAP, Inc.
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

package priorityqueue

import (
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	// unanalyzedTableDefaultChangePercentage is the default change percentage of unanalyzed table.
	unanalyzedTableDefaultChangePercentage = 1
	// unanalyzedTableDefaultLastUpdateDuration is the default last update duration of unanalyzed table.
	unanalyzedTableDefaultLastUpdateDuration = -30 * time.Minute
)

// AnalysisJobFactory is responsible for creating different types of analysis jobs.
// NOTE: This struct is not thread-safe.
type AnalysisJobFactory struct {
	sctx             sessionctx.Context
	autoAnalyzeRatio float64
	// The current TSO.
	currentTs uint64
}

// NewAnalysisJobFactory creates a new AnalysisJobFactory.
func NewAnalysisJobFactory(sctx sessionctx.Context, autoAnalyzeRatio float64, currentTs uint64) *AnalysisJobFactory {
	return &AnalysisJobFactory{
		sctx:             sctx,
		autoAnalyzeRatio: autoAnalyzeRatio,
		currentTs:        currentTs,
	}
}

// CreateNonPartitionedTableAnalysisJob creates a job for non-partitioned tables.
func (f *AnalysisJobFactory) CreateNonPartitionedTableAnalysisJob(
	tableSchema string,
	tblInfo *model.TableInfo,
	tblStats *statistics.Table,
) AnalysisJob {
	if !tblStats.IsEligibleForAnalysis() {
		return nil
	}

	tableStatsVer := f.sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(tblStats, &tableStatsVer)

	changePercentage := f.CalculateChangePercentage(tblStats)
	tableSize := f.CalculateTableSize(tblStats)
	lastAnalysisDuration := f.GetTableLastAnalyzeDuration(tblStats)
	indexes := f.CheckIndexesNeedAnalyze(tblInfo, tblStats)

	// No need to analyze.
	// We perform a separate check because users may set the auto analyze ratio to 0,
	// yet still wish to analyze newly added indexes and tables that have not been analyzed.
	if changePercentage == 0 && len(indexes) == 0 {
		return nil
	}

	return NewNonPartitionedTableAnalysisJob(
		tableSchema,
		tblInfo.Name.O,
		tblInfo.ID,
		indexes,
		tableStatsVer,
		changePercentage,
		tableSize,
		lastAnalysisDuration,
	)
}

// CreateStaticPartitionAnalysisJob creates a job for static partitions.
func (f *AnalysisJobFactory) CreateStaticPartitionAnalysisJob(
	tableSchema string,
	globalTblInfo *model.TableInfo,
	partitionID int64,
	partitionName string,
	partitionStats *statistics.Table,
) AnalysisJob {
	if !partitionStats.IsEligibleForAnalysis() {
		return nil
	}

	tableStatsVer := f.sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(partitionStats, &tableStatsVer)

	changePercentage := f.CalculateChangePercentage(partitionStats)
	tableSize := f.CalculateTableSize(partitionStats)
	lastAnalysisDuration := f.GetTableLastAnalyzeDuration(partitionStats)
	indexes := f.CheckIndexesNeedAnalyze(globalTblInfo, partitionStats)

	// No need to analyze.
	// We perform a separate check because users may set the auto analyze ratio to 0,
	// yet still wish to analyze newly added indexes and tables that have not been analyzed.
	if changePercentage == 0 && len(indexes) == 0 {
		return nil
	}

	return NewStaticPartitionTableAnalysisJob(
		tableSchema,
		globalTblInfo.Name.O,
		globalTblInfo.ID,
		partitionName,
		partitionID,
		indexes,
		tableStatsVer,
		changePercentage,
		tableSize,
		lastAnalysisDuration,
	)
}

// CreateDynamicPartitionedTableAnalysisJob creates a job for dynamic partitioned tables.
func (f *AnalysisJobFactory) CreateDynamicPartitionedTableAnalysisJob(
	tableSchema string,
	globalTblInfo *model.TableInfo,
	globalTblStats *statistics.Table,
	partitionStats map[PartitionIDAndName]*statistics.Table,
) AnalysisJob {
	if !globalTblStats.IsEligibleForAnalysis() {
		return nil
	}

	// TODO: figure out how to check the table stats version correctly for partitioned tables.
	tableStatsVer := f.sctx.GetSessionVars().AnalyzeVersion
	statistics.CheckAnalyzeVerOnTable(globalTblStats, &tableStatsVer)

	avgChange, avgSize, minLastAnalyzeDuration, partitionNames := f.CalculateIndicatorsForPartitions(globalTblStats, partitionStats)
	partitionIndexes := f.CheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable(globalTblInfo, partitionStats)

	// No need to analyze.
	// We perform a separate check because users may set the auto analyze ratio to 0,
	// yet still wish to analyze newly added indexes and tables that have not been analyzed.
	if len(partitionNames) == 0 && len(partitionIndexes) == 0 {
		return nil
	}

	return NewDynamicPartitionedTableAnalysisJob(
		tableSchema,
		globalTblInfo.Name.O,
		globalTblInfo.ID,
		partitionNames,
		partitionIndexes,
		tableStatsVer,
		avgChange,
		avgSize,
		minLastAnalyzeDuration,
	)
}

// CalculateChangePercentage calculates the change percentage of the table
// based on the change count and the analysis count.
func (f *AnalysisJobFactory) CalculateChangePercentage(tblStats *statistics.Table) float64 {
	if !tblStats.IsAnalyzed() {
		return unanalyzedTableDefaultChangePercentage
	}

	// Auto analyze based on the change percentage is disabled.
	// However, this check should not affect the analysis of indexes,
	// as index analysis is still needed for query performance.
	if f.autoAnalyzeRatio == 0 {
		return 0
	}

	tblCnt := float64(tblStats.RealtimeCount)
	if histCnt := tblStats.GetAnalyzeRowCount(); histCnt > 0 {
		tblCnt = histCnt
	}
	res := float64(tblStats.ModifyCount) / tblCnt
	if res > f.autoAnalyzeRatio {
		return res
	}

	return 0
}

// CalculateTableSize calculates the size of the table.
func (*AnalysisJobFactory) CalculateTableSize(tblStats *statistics.Table) float64 {
	tblCnt := float64(tblStats.RealtimeCount)
	colCnt := float64(tblStats.ColAndIdxExistenceMap.ColNum())
	intest.Assert(colCnt != 0, "Column count should not be 0")

	return tblCnt * colCnt
}

// GetTableLastAnalyzeDuration gets the last analyze duration of the table.
func (f *AnalysisJobFactory) GetTableLastAnalyzeDuration(tblStats *statistics.Table) time.Duration {
	lastTime := f.FindLastAnalyzeTime(tblStats)
	currentTime := oracle.GetTimeFromTS(f.currentTs)

	// Calculate the duration since last analyze.
	return currentTime.Sub(lastTime)
}

// FindLastAnalyzeTime finds the last analyze time of the table.
// It uses `LastUpdateVersion` to find the last analyze time.
// The `LastUpdateVersion` is the version of the transaction that updates the statistics.
// It always not null(default 0), so we can use it to find the last analyze time.
func (f *AnalysisJobFactory) FindLastAnalyzeTime(tblStats *statistics.Table) time.Time {
	if !tblStats.IsAnalyzed() {
		phy := oracle.GetTimeFromTS(f.currentTs)
		return phy.Add(unanalyzedTableDefaultLastUpdateDuration)
	}
	return oracle.GetTimeFromTS(tblStats.LastAnalyzeVersion)
}

// CheckIndexesNeedAnalyze checks if the indexes need to be analyzed.
func (*AnalysisJobFactory) CheckIndexesNeedAnalyze(tblInfo *model.TableInfo, tblStats *statistics.Table) []string {
	// If table is not analyzed, we need to analyze whole table.
	// So we don't need to check indexes.
	if !tblStats.IsAnalyzed() {
		return nil
	}

	indexes := make([]string, 0, len(tblInfo.Indices))
	// Check if missing index stats.
	for _, idx := range tblInfo.Indices {
		if idxStats := tblStats.GetIdx(idx.ID); idxStats == nil && !tblStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true) && idx.State == model.StatePublic {
			// Vector index doesn't have stats currently.
			if idx.VectorInfo != nil {
				continue
			}
			indexes = append(indexes, idx.Name.O)
		}
	}

	return indexes
}

// CalculateIndicatorsForPartitions calculates the average change percentage,
// average size and average last analyze duration for the partitions that meet the threshold.
// Change percentage is the ratio of the number of modified rows to the total number of rows.
// Size is the product of the number of rows and the number of columns.
// Last analyze duration is the duration since the last analyze.
func (f *AnalysisJobFactory) CalculateIndicatorsForPartitions(
	globalStats *statistics.Table,
	partitionStats map[PartitionIDAndName]*statistics.Table,
) (
	avgChange float64,
	avgSize float64,
	avgLastAnalyzeDuration time.Duration,
	partitionNames []string,
) {
	totalChangePercent := 0.0
	totalSize := 0.0
	count := 0.0
	partitionNames = make([]string, 0, len(partitionStats))
	cols := float64(globalStats.ColAndIdxExistenceMap.ColNum())
	intest.Assert(cols != 0, "Column count should not be 0")
	totalLastAnalyzeDuration := time.Duration(0)

	for pIDAndName, tblStats := range partitionStats {
		// Skip partition analysis if it doesn't meet the threshold, stats are not yet loaded,
		// or the auto analyze ratio is set to 0 by the user.
		changePercent := f.CalculateChangePercentage(tblStats)
		if changePercent == 0 {
			continue
		}

		totalChangePercent += changePercent
		// size = count * cols
		totalSize += float64(tblStats.RealtimeCount) * cols
		lastAnalyzeDuration := f.GetTableLastAnalyzeDuration(tblStats)
		totalLastAnalyzeDuration += lastAnalyzeDuration
		partitionNames = append(partitionNames, pIDAndName.Name)
		count++
	}
	if len(partitionNames) == 0 {
		return 0, 0, 0, partitionNames
	}

	avgChange = totalChangePercent / count
	avgSize = totalSize / count
	avgLastAnalyzeDuration = totalLastAnalyzeDuration / time.Duration(count)

	return avgChange, avgSize, avgLastAnalyzeDuration, partitionNames
}

// CheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable checks if the indexes of the partitioned table need to be analyzed.
// It returns a map from index name to the names of the partitions that need to be analyzed.
// NOTE: This is only for newly added indexes.
func (*AnalysisJobFactory) CheckNewlyAddedIndexesNeedAnalyzeForPartitionedTable(
	tblInfo *model.TableInfo,
	partitionStats map[PartitionIDAndName]*statistics.Table,
) map[string][]string {
	partitionIndexes := make(map[string][]string, len(tblInfo.Indices))

	for _, idx := range tblInfo.Indices {
		// No need to analyze the index if it's not public.
		// Special global index also no need to trigger by auto analyze.
		if idx.State != model.StatePublic || util.IsSpecialGlobalIndex(idx, tblInfo) {
			continue
		}
		// Index on vector type doesn't have stats currently.
		if idx.VectorInfo != nil {
			continue
		}

		// Find all the partitions that need to analyze this index.
		names := make([]string, 0, len(partitionStats))
		for pIDAndName, tblStats := range partitionStats {
			if idxStats := tblStats.GetIdx(idx.ID); idxStats == nil && !tblStats.ColAndIdxExistenceMap.HasAnalyzed(idx.ID, true) {
				names = append(names, pIDAndName.Name)
			}
		}

		if len(names) > 0 {
			partitionIndexes[idx.Name.O] = names
		}
	}

	return partitionIndexes
}

// PartitionIDAndName is a struct that contains the ID and name of a partition.
// Exported for testing purposes. Do not use it in other packages.
type PartitionIDAndName struct {
	Name string
	ID   int64
}

// NewPartitionIDAndName creates a new PartitionIDAndName.
func NewPartitionIDAndName(name string, id int64) PartitionIDAndName {
	return PartitionIDAndName{
		Name: name,
		ID:   id,
	}
}

// GetPartitionStats gets the partition stats.
func GetPartitionStats(
	statsHandle statstypes.StatsHandle,
	tblInfo *model.TableInfo,
	defs []model.PartitionDefinition,
) map[PartitionIDAndName]*statistics.Table {
	partitionStats := make(map[PartitionIDAndName]*statistics.Table, len(defs))

	for _, def := range defs {
		stats := statsHandle.GetPartitionStatsForAutoAnalyze(tblInfo, def.ID)
		// Ignore the partition if it's not ready to analyze.
		if !stats.IsEligibleForAnalysis() {
			continue
		}
		d := NewPartitionIDAndName(def.Name.O, def.ID)
		partitionStats[d] = stats
	}

	return partitionStats
}

// AutoAnalysisTimeWindow is a struct that contains the start and end time of the auto analyze time window.
type AutoAnalysisTimeWindow struct {
	start time.Time
	end   time.Time
}

// NewAutoAnalysisTimeWindow creates a new AutoAnalysisTimeWindow.
func NewAutoAnalysisTimeWindow(start, end time.Time) AutoAnalysisTimeWindow {
	return AutoAnalysisTimeWindow{
		start: start,
		end:   end,
	}
}

// IsWithinTimeWindow checks if the current time is within the time window.
// If the auto analyze time window is not set or the current time is not in the window, return false.
func (a AutoAnalysisTimeWindow) IsWithinTimeWindow(currentTime time.Time) bool {
	if a.start.IsZero() || a.end.IsZero() {
		return false
	}
	return timeutil.WithinDayTimePeriod(a.start, a.end, currentTime)
}

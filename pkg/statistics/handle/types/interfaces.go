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

package types

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statsutil "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/types"
)

// ColStatsTimeInfo records usage information of this column stats.
type ColStatsTimeInfo struct {
	LastUsedAt     *types.Time // last time the column is used
	LastAnalyzedAt *types.Time // last time the column is analyzed
}

// PredicateColumnsGetter gives the predicate columns of a table.
type PredicateColumnsGetter interface {
	// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
	GetPredicateColumns(tableID int64) ([]int64, error)
}

// PriorityQueueSnapshot is the snapshot of the stats priority queue.
type PriorityQueueSnapshot struct {
	CurrentJobs     []AnalysisJobJSON `json:"current_jobs"`
	MustRetryTables []int64           `json:"must_retry_tables"`
}

// AnalysisJobJSON represents the JSON format of an AnalysisJob.
//
//nolint:fieldalignment
type AnalysisJobJSON struct {
	Type               string            `json:"type"`
	TableID            int64             `json:"table_id"`
	Weight             float64           `json:"weight"`
	PartitionIDs       []int64           `json:"partition_ids"`
	IndexIDs           []int64           `json:"index_ids"`
	PartitionIndexIDs  map[int64][]int64 `json:"partition_index_ids"`
	Indicators         IndicatorsJSON    `json:"indicators"`
	HasNewlyAddedIndex bool              `json:"has_newly_added_index"`
}

// IndicatorsJSON represents the JSON format of Indicators.
type IndicatorsJSON struct {
	ChangePercentage     string `json:"change_percentage"`
	TableSize            string `json:"table_size"`
	LastAnalysisDuration string `json:"last_analysis_duration"`
}

// AnalyzeJobRecorder records the state of analyze jobs in mysql.analyze_jobs.
type AnalyzeJobRecorder interface {
	// InsertAnalyzeJob inserts analyze job into mysql.analyze_jobs and gets job ID for further updating job.
	InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error

	// StartAnalyzeJob updates the job status to `running` and sets the start time.
	// There is no guarantee that the job record will actually be updated. If the job fails to start, an error will be logged.
	// It is OK because this won't affect the analysis job's success.
	StartAnalyzeJob(job *statistics.AnalyzeJob)

	// UpdateAnalyzeJobProgress updates the current progress of the analyze job.
	// There is no guarantee that the job record will actually be updated. If the job fails to update, an error will be logged.
	// It is OK because this won't affect the analysis job's success.
	UpdateAnalyzeJobProgress(job *statistics.AnalyzeJob, rowCount int64)

	// FinishAnalyzeJob updates the job status to `finished`, sets the end time, and updates the job info.
	// There is no guarantee that the job record will actually be updated. If the job fails to finish, an error will be logged.
	// It is OK because this won't affect the analysis job's success.
	FinishAnalyzeJob(job *statistics.AnalyzeJob, failReason error, analyzeType statistics.JobType)
}

// CacheUpdate encapsulates changes to be made to the stats cache
type CacheUpdate struct {
	Updated []*statistics.Table
	Deleted []int64
	Options UpdateOptions
}

// UpdateOptions contains configuration for cache updates
type UpdateOptions struct {
	// SkipMoveForward keeps the cache's max version unchanged after this update. Set it when the
	// update reads only some tables: a larger max version can make the next full update skip
	// newer stats_meta rows of the other tables.
	SkipMoveForward bool
}

// StatsCacheGetter gives the cached statistics of a table.
type StatsCacheGetter interface {
	// Get returns the specified table's stats.
	Get(tableID int64) (*statistics.Table, bool)
}

// StatsCacheUpdater applies changes to the cached statistics.
type StatsCacheUpdater interface {
	// UpdateStatsCache applies a batch of changes to the cache
	UpdateStatsCache(update CacheUpdate)
}

// StatsCacheLoader loads the statistics of tables from the storage into the cache.
type StatsCacheLoader interface {
	// Update reads stats meta from store and updates the stats cache.
	// To work with auto-analyze's needs, we'll update all table's stats meta into memory.
	Update(ctx context.Context, is infoschema.InfoSchema, tableAndPartitionIDs ...int64) error
}

// StatsCacheLister lists the cached statistics.
type StatsCacheLister interface {
	// Values returns all values in this cache.
	Values() []*statistics.Table

	// Len returns the length of this cache.
	Len() int
}

// StatsCacheVersionGetter gives the version from which the cache fetches updated stats.
type StatsCacheVersionGetter interface {
	// GetNextCheckVersionWithOffset returns the last version with offset.
	// It is used to fetch updated statistics from the stats meta table.
	GetNextCheckVersionWithOffset() uint64
}

// StatsLockTable is the table info of which will be locked.
type StatsLockTable struct {
	PartitionInfo map[int64]string
	// schema name + table name.
	FullName string
}

// LockedTablesGetter gives the tables whose statistics are locked.
type LockedTablesGetter interface {
	// GetLockedTables returns the locked status of the given tables.
	// Note: This function query locked tables from store, so please try to batch the query.
	GetLockedTables(tableIDs ...int64) (map[int64]struct{}, error)
}

// PartitionStatisticLoadTask currently records a partition-level jsontable.
type PartitionStatisticLoadTask struct {
	JSONTable  *statsutil.JSONTable
	PhysicalID int64
}

// PersistFunc is used to persist JSONTable in the partition level.
type PersistFunc func(ctx context.Context, jsonTable *statsutil.JSONTable, physicalID int64) error

// MetaUpdate records a meta update for a partition or table.
type MetaUpdate struct {
	PhysicalID  int64
	Count       int64
	ModifyCount int64
}

// StatsMetaReader reads the stats meta of a table from the storage.
type StatsMetaReader interface {
	// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
	StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error)
}

// AnalyzeResultSaver saves the results of ANALYZE to the storage.
type AnalyzeResultSaver interface {
	// SaveAnalyzeResultToStorage saves the analyze result to the storage.
	SaveAnalyzeResultToStorage(results *statistics.AnalyzeResults, analyzeSnapshot bool) (err error)
}

// ColOrIdxStatsSaver saves the statistics of one column or index to the storage.
type ColOrIdxStatsSaver interface {
	// SaveColOrIdxStatsToStorage save the column or index stats to storage.
	SaveColOrIdxStatsToStorage(tableID int64, count, modifyCount int64, isIndex int, hg *statistics.Histogram,
		cms *statistics.CMSketch, topN *statistics.TopN, fms *statistics.FMSketch, statsVersion int, updateAnalyzeTime bool) (err error)
}

// NeededItemTask represents one needed column/indices with expire time.
type NeededItemTask struct {
	ToTimeout time.Time
	ResultCh  chan stmtctx.StatsLoadResult
	Item      model.StatsLoadItem
	Retry     int
}

// GlobalStatsInfo represents the contextual information pertaining to global statistics.
type GlobalStatsInfo struct {
	HistIDs []int64
	// When the `isIndex == 0`, HistIDs will be the column IDs.
	// Otherwise, HistIDs will only contain the index ID.
	IsIndex      int
	StatsVersion int
}

// GlobalStatsMerger merges the stats of partitions into the global stats of a table.
type GlobalStatsMerger interface {
	// MergePartitionStats2GlobalStatsByTableID merges partition stats to global stats by table ID.
	MergePartitionStats2GlobalStatsByTableID(sc sessionctx.Context,
		opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema,
		info *GlobalStatsInfo,
		physicalID int64,
	) (err error)
}

// PhysicalTableStatsGetter gives the statistics of a table or partition.
type PhysicalTableStatsGetter interface {
	// GetPhysicalTableStats retrieves the statistics for a physical table from cache or creates a pseudo statistics table.
	// physicalTableID can be a table ID or partition ID.
	GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table
}

// NonPseudoPhysicalTableStatsGetter gives the cached statistics of a table or partition.
type NonPseudoPhysicalTableStatsGetter interface {
	// GetNonPseudoPhysicalTableStats retrieves the statistics for a physical table from cache. Unlike
	// GetPhysicalTableStats, it does not create pseudo stats, but it can return pseudo stats that are already cached.
	// physicalTableID can be a table ID or partition ID.
	// Note: this function may return nil if the table is not found in the cache.
	GetNonPseudoPhysicalTableStats(physicalTableID int64) (*statistics.Table, bool)
}

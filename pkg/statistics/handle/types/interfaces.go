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

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	statsutil "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// StatsGC is used to GC unnecessary stats.
type StatsGC interface {
	// GCStats will garbage collect the useless stats' info.
	// For dropped tables, we will first update their version
	// so that other tidb could know that table is deleted.
	GCStats(is infoschema.InfoSchema, ddlLease time.Duration) (err error)

	// ClearOutdatedHistoryStats clear outdated historical stats.
	// Only for test.
	ClearOutdatedHistoryStats() error

	// DeleteTableStatsFromKV deletes table statistics from kv.
	// A statsID refers to statistic of a table or a partition.
	DeleteTableStatsFromKV(statsIDs []int64, soft bool) (err error)
}

// ColStatsTimeInfo records usage information of this column stats.
type ColStatsTimeInfo struct {
	LastUsedAt     *types.Time // last time the column is used
	LastAnalyzedAt *types.Time // last time the column is analyzed
}

// StatsUsage is used to track the usage of column / index statistics.
type StatsUsage interface {
	// Below methods are for predicated columns.

	// LoadColumnStatsUsage returns all columns' usage information.
	LoadColumnStatsUsage(loc *time.Location) (map[model.TableItemID]ColStatsTimeInfo, error)

	// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
	GetPredicateColumns(tableID int64) ([]int64, error)

	// CollectColumnsInExtendedStats returns IDs of the columns involved in extended stats.
	CollectColumnsInExtendedStats(tableID int64) ([]int64, error)

	IndexUsage

	// TODO: extract these function to a new interface only for delta/stats usage, like `IndexUsage`.
	// Blow methods are for table delta and stats usage.

	// NewSessionStatsItem allocates a stats collector for a session.
	// TODO: use interface{} to avoid cycle import, remove this interface{}.
	NewSessionStatsItem() any

	// ResetSessionStatsList resets the sessions stats list.
	ResetSessionStatsList()

	// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
	DumpStatsDeltaToKV(dumpAll bool) error

	// DumpColStatsUsageToKV sweeps the whole list, updates the column stats usage map and dumps it to KV.
	DumpColStatsUsageToKV() error
}

// IndexUsage is an interface to define the function of collecting index usage stats.
type IndexUsage interface {
	// NewSessionIndexUsageCollector creates a new Collector for a session.
	NewSessionIndexUsageCollector() *indexusage.SessionIndexUsageCollector

	// GCIndexUsage removes unnecessary index usage data.
	GCIndexUsage() error

	// StartWorker starts for the collector worker.
	StartWorker()

	// Close closes and waits for the index usage collector worker.
	Close()

	// GetIndexUsage returns the index usage information
	GetIndexUsage(tableID int64, indexID int64) indexusage.Sample
}

// StatsHistory is used to manage historical stats.
type StatsHistory interface {
	// RecordHistoricalStatsMeta records the historical stats meta in mysql.stats_meta_history one by one.
	RecordHistoricalStatsMeta(version uint64, source string, enforce bool, tableIDs ...int64)

	// CheckHistoricalStatsEnable check whether historical stats is enabled.
	CheckHistoricalStatsEnable() (enable bool, err error)

	// RecordHistoricalStatsToStorage records the given table's stats data to mysql.stats_history
	RecordHistoricalStatsToStorage(dbName string, tableInfo *model.TableInfo, physicalID int64, isPartition bool) (uint64, error)
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

// StatsAnalyze is used to handle auto-analyze and manage analyze jobs.
// We need to read all the tables's last_analyze_time, modified_count, and row_count into memory.
// Because the current auto analyze' scheduling needs the whole information.
type StatsAnalyze interface {
	owner.Listener

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

	// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
	DeleteAnalyzeJobs(updateTime time.Time) error

	// CleanupCorruptedAnalyzeJobsOnCurrentInstance cleans up the corrupted analyze job.
	// A corrupted analyze job is one that is in a 'pending' or 'running' state,
	// but is associated with a TiDB instance that is either not currently running or has been restarted.
	// We use current running analyze jobs to check whether the analyze job is corrupted.
	CleanupCorruptedAnalyzeJobsOnCurrentInstance(currentRunningProcessIDs map[uint64]struct{}) error

	// CleanupCorruptedAnalyzeJobsOnDeadInstances purges analyze jobs that are associated with non-existent instances.
	// This function is specifically designed to handle jobs that have become corrupted due to
	// their associated instances being removed from the current cluster.
	CleanupCorruptedAnalyzeJobsOnDeadInstances() error

	// HandleAutoAnalyze analyzes the outdated tables. (The change percent of the table exceeds the threshold)
	// It also analyzes newly created tables and newly added indexes.
	HandleAutoAnalyze() (analyzed bool)

	// CheckAnalyzeVersion checks whether all the statistics versions of this table's columns and indexes are the same.
	CheckAnalyzeVersion(tblInfo *model.TableInfo, physicalIDs []int64, version *int) bool

	// GetPriorityQueueSnapshot returns the stats priority queue.
	GetPriorityQueueSnapshot() (PriorityQueueSnapshot, error)

	// Close closes the analyze worker.
	Close()
}

// CacheUpdate encapsulates changes to be made to the stats cache
type CacheUpdate struct {
	Updated []*statistics.Table
	Deleted []int64
	Options UpdateOptions
}

// UpdateOptions contains configuration for cache updates
type UpdateOptions struct {
	// SkipMoveForward controls whether to skip updating the cache's max version number.
	// When true, the cache max version number stays unchanged even after updates.
	// This improves performance when analyzing a small number of tables by avoiding
	// unnecessary full cache reloads that would normally be triggered by version changes.
	SkipMoveForward bool
}

// StatsCache is used to manage all table statistics in memory.
type StatsCache interface {
	// Close closes this cache.
	Close()

	// Clear clears this cache.
	Clear()

	// Update reads stats meta from store and updates the stats map.
	// To work with auto-analyze's needs, we'll update all table's stats meta into memory.
	Update(ctx context.Context, is infoschema.InfoSchema, tableAndPartitionIDs ...int64) error

	// MemConsumed returns its memory usage.
	MemConsumed() (size int64)

	// Get returns the specified table's stats.
	Get(tableID int64) (*statistics.Table, bool)

	// Put puts this table stats into the cache.
	Put(tableID int64, t *statistics.Table)

	// UpdateStatsCache applies a batch of changes to the cache
	UpdateStatsCache(update CacheUpdate)

	// GetNextCheckVersionWithOffset returns the last version with offset.
	// It is used to fetch updated statistics from the stats meta table.
	GetNextCheckVersionWithOffset() uint64

	// MaxTableStatsVersion returns the version of the current cache, which is defined as
	// the max table stats version the cache has in its lifecycle.
	MaxTableStatsVersion() uint64

	// Values returns all values in this cache.
	Values() []*statistics.Table

	// Len returns the length of this cache.
	Len() int

	// SetStatsCacheCapacity sets the cache's capacity.
	SetStatsCacheCapacity(capBytes int64)

	// Replace replaces this cache.
	Replace(cache StatsCache)

	// UpdateStatsHealthyMetrics updates stats healthy distribution metrics according to stats cache.
	UpdateStatsHealthyMetrics()

	// TriggerEvict triggers the cache to evict some items
	TriggerEvict()
}

// StatsLockTable is the table info of which will be locked.
type StatsLockTable struct {
	PartitionInfo map[int64]string
	// schema name + table name.
	FullName string
}

// StatsLock is used to manage locked stats.
type StatsLock interface {
	// LockTables add locked tables id to store.
	// - tables: tables that will be locked.
	// Return the message of skipped tables and error.
	LockTables(tables map[int64]*StatsLockTable) (skipped string, err error)

	// LockPartitions add locked partitions id to store.
	// If the whole table is locked, then skip all partitions of the table.
	// - tid: table id of which will be locked.
	// - tableName: table name of which will be locked.
	// - pidNames: partition ids of which will be locked.
	// Return the message of skipped tables and error.
	// Note: If the whole table is locked, then skip all partitions of the table.
	LockPartitions(
		tid int64,
		tableName string,
		pidNames map[int64]string,
	) (skipped string, err error)

	// RemoveLockedTables remove tables from table locked records.
	// - tables: tables of which will be unlocked.
	// Return the message of skipped tables and error.
	RemoveLockedTables(tables map[int64]*StatsLockTable) (skipped string, err error)

	// RemoveLockedPartitions remove partitions from table locked records.
	// - tid: table id of which will be unlocked.
	// - tableName: table name of which will be unlocked.
	// - pidNames: partition ids of which will be unlocked.
	// Note: If the whole table is locked, then skip all partitions of the table.
	RemoveLockedPartitions(
		tid int64,
		tableName string,
		pidNames map[int64]string,
	) (skipped string, err error)

	// GetLockedTables returns the locked status of the given tables.
	// Note: This function query locked tables from store, so please try to batch the query.
	GetLockedTables(tableIDs ...int64) (map[int64]struct{}, error)

	// GetTableLockedAndClearForTest for unit test only.
	GetTableLockedAndClearForTest() (map[int64]struct{}, error)
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

// StatsReadWriter is used to read and write stats to the storage.
// TODO: merge and remove some methods.
type StatsReadWriter interface {
	// TableStatsFromStorage loads table stats info from storage.
	TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (statsTbl *statistics.Table, err error)

	// LoadTablePartitionStats loads partition stats info from storage.
	LoadTablePartitionStats(tableInfo *model.TableInfo, partitionDef *model.PartitionDefinition) (*statistics.Table, error)

	// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
	StatsMetaCountAndModifyCount(tableID int64) (count, modifyCount int64, err error)

	// LoadNeededHistograms will load histograms for those needed columns/indices and put them into the cache.
	LoadNeededHistograms(is infoschema.InfoSchema) (err error)

	// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
	ReloadExtendedStatistics() error

	// SaveColOrIdxStatsToStorage save the column or index stats to storage.
	SaveColOrIdxStatsToStorage(tableID int64, count, modifyCount int64, isIndex int, hg *statistics.Histogram,
		cms *statistics.CMSketch, topN *statistics.TopN, statsVersion int, updateAnalyzeTime bool, source string) (err error)

	// SaveAnalyzeResultToStorage saves the analyze result to the storage.
	SaveAnalyzeResultToStorage(results *statistics.AnalyzeResults, analyzeSnapshot bool, source string) (err error)

	// SaveMetaToStorage saves the stats meta of a table to storage.
	// Use the param `refreshLastHistVer` to indicate whether we need to update the last_histograms_versions in stats_meta table.
	// Set it to true if the column/index stats is updated.
	SaveMetaToStorage(source string, needRefreshLastHistVer bool, metaUpdates ...MetaUpdate) (err error)

	// UpdateStatsMetaVersionForGC updates the version of mysql.stats_meta,
	// ensuring it is greater than the last garbage collection (GC) time.
	// The GC worker deletes old stats based on a safe time point,
	// calculated as now() - 10 * max(stats lease, ddl lease).
	// The range [last GC time, safe time point) is chosen to prevent
	// the simultaneous deletion of numerous stats, minimizing potential
	// performance issues.
	// This function ensures the version is updated beyond the last GC time,
	// allowing the GC worker to delete outdated stats.
	//
	// Explanation:
	// - ddl lease: 10
	// - stats lease: 3
	// - safe time point: now() - 10 * 10 = now() - 100
	// - now: 200
	// - last GC time: 90
	// - [last GC time, safe time point) = [90, 100)
	// - To trigger stats deletion, the version must be updated beyond 90.
	//
	// This safeguards scenarios where a table remains unchanged for an extended period.
	// For instance, if a table was created at time 90, and it's now time 200,
	// with the last GC time at 95 and the safe time point at 100,
	// updating the version beyond 95 ensures eventual deletion of stats.
	UpdateStatsMetaVersionForGC(physicalID int64) (err error)

	// ChangeGlobalStatsID changes the global stats ID.
	ChangeGlobalStatsID(from, to int64) (err error)

	// TableStatsToJSON dumps table stats to JSON.
	TableStatsToJSON(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*statsutil.JSONTable, error)

	// DumpStatsToJSON dumps statistic to json.
	DumpStatsToJSON(dbName string, tableInfo *model.TableInfo,
		historyStatsExec sqlexec.RestrictedSQLExecutor, dumpPartitionStats bool) (*statsutil.JSONTable, error)

	// DumpHistoricalStatsBySnapshot dumped json tables from mysql.stats_meta_history and mysql.stats_history.
	// As implemented in getTableHistoricalStatsToJSONWithFallback, if historical stats are nonexistent, it will fall back
	// to the latest stats, and these table names (and partition names) will be returned in fallbackTbls.
	DumpHistoricalStatsBySnapshot(
		dbName string,
		tableInfo *model.TableInfo,
		snapshot uint64,
	) (
		jt *statsutil.JSONTable,
		fallbackTbls []string,
		err error,
	)

	// DumpStatsToJSONBySnapshot dumps statistic to json.
	DumpStatsToJSONBySnapshot(dbName string, tableInfo *model.TableInfo, snapshot uint64, dumpPartitionStats bool) (*statsutil.JSONTable, error)

	// PersistStatsBySnapshot dumps statistic to json and call the function for each partition statistic to persist.
	// Notice:
	//  1. It might call the function `persist` with nil jsontable.
	//  2. It is only used by BR, so partitions' statistic are always dumped.
	PersistStatsBySnapshot(ctx context.Context, dbName string, tableInfo *model.TableInfo, snapshot uint64, persist PersistFunc) error

	// LoadStatsFromJSONConcurrently consumes concurrently the statistic task from `taskCh`.
	LoadStatsFromJSONConcurrently(ctx context.Context, tableInfo *model.TableInfo, taskCh chan *PartitionStatisticLoadTask, concurrencyForPartition int) error

	// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
	// In final, it will also udpate the stats cache.
	LoadStatsFromJSON(ctx context.Context, is infoschema.InfoSchema, jsonTbl *statsutil.JSONTable, concurrencyForPartition int) error

	// LoadStatsFromJSONNoUpdate will load statistic from JSONTable, and save it to the storage.
	LoadStatsFromJSONNoUpdate(ctx context.Context, is infoschema.InfoSchema, jsonTbl *statsutil.JSONTable, concurrencyForPartition int) error

	// Methods for extended stast.

	// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
	InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error)

	// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
	MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error)

	// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
	SaveExtendedStatsToStorage(tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error)
}

// NeededItemTask represents one needed column/indices with expire time.
type NeededItemTask struct {
	ToTimeout time.Time
	ResultCh  chan stmtctx.StatsLoadResult
	Item      model.StatsLoadItem
	Retry     int
}

// StatsSyncLoad implement the sync-load feature.
type StatsSyncLoad interface {
	// SendLoadRequests sends load requests to the channel.
	SendLoadRequests(sc *stmtctx.StatementContext, neededHistItems []model.StatsLoadItem, timeout time.Duration) error

	// SyncWaitStatsLoad will wait for the load requests to finish.
	SyncWaitStatsLoad(sc *stmtctx.StatementContext) error

	// AppendNeededItem appends a needed item to the channel.
	AppendNeededItem(task *NeededItemTask, timeout time.Duration) error

	// SubLoadWorker will start a goroutine to handle the load requests.
	SubLoadWorker(exit chan struct{}, exitWg *util.WaitGroupEnhancedWrapper)

	// HandleOneTask will handle one task.
	HandleOneTask(lastTask *NeededItemTask, exit chan struct{}) (task *NeededItemTask, err error)
}

// GlobalStatsInfo represents the contextual information pertaining to global statistics.
type GlobalStatsInfo struct {
	HistIDs []int64
	// When the `isIndex == 0`, HistIDs will be the column IDs.
	// Otherwise, HistIDs will only contain the index ID.
	IsIndex      int
	StatsVersion int
}

// StatsGlobal is used to manage partition table global stats.
type StatsGlobal interface {
	// MergePartitionStats2GlobalStatsByTableID merges partition stats to global stats by table ID.
	MergePartitionStats2GlobalStatsByTableID(sc sessionctx.Context,
		opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema,
		info *GlobalStatsInfo,
		physicalID int64,
	) (err error)
}

// DDL is used to handle ddl events.
type DDL interface {
	// HandleDDLEvent handles ddl events.
	HandleDDLEvent(ctx context.Context, sctx sessionctx.Context, changeEvent *notifier.SchemaChangeEvent) error
	// DDLEventCh returns ddl events channel in handle.
	DDLEventCh() chan *notifier.SchemaChangeEvent
}

// StatsHandle is used to manage TiDB Statistics.
type StatsHandle interface {
	// Pool is used to get a session or a goroutine to execute stats updating.
	handleutil.Pool

	// AutoAnalyzeProcIDGenerator is used to generate auto analyze proc ID.
	handleutil.AutoAnalyzeProcIDGenerator

	// LeaseGetter is used to get stats lease.
	handleutil.LeaseGetter

	// TableInfoGetter is used to get table meta info.
	handleutil.TableInfoGetter

	// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
	GetTableStats(tblInfo *model.TableInfo) *statistics.Table

	// GetTableStatsForAutoAnalyze retrieves the statistics table from cache, but it will not return pseudo.
	GetTableStatsForAutoAnalyze(tblInfo *model.TableInfo) *statistics.Table

	// GetPartitionStats retrieves the partition stats from cache.
	GetPartitionStats(tblInfo *model.TableInfo, pid int64) *statistics.Table

	// GetPartitionStatsByID retrieves the partition stats from cache by partition ID.
	GetPartitionStatsByID(is infoschema.InfoSchema, pid int64) *statistics.Table

	// GetPartitionStatsForAutoAnalyze retrieves the partition stats from cache, but it will not return pseudo.
	GetPartitionStatsForAutoAnalyze(tblInfo *model.TableInfo, pid int64) *statistics.Table

	// StatsGC is used to do the GC job.
	StatsGC

	// StatsUsage is used to handle table delta and stats usage.
	StatsUsage

	// StatsHistory is used to manage historical stats.
	StatsHistory

	// StatsAnalyze is used to handle auto-analyze and manage analyze jobs.
	StatsAnalyze

	// StatsCache is used to manage all table statistics in memory.
	StatsCache

	// StatsLock is used to manage locked stats.
	StatsLock

	// StatsReadWriter is used to read and write stats to the storage.
	StatsReadWriter

	// StatsGlobal is used to manage partition table global stats.
	StatsGlobal

	// DDL is used to handle ddl events.
	DDL
}

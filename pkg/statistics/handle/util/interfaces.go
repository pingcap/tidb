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

package util

import (
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
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
	DeleteTableStatsFromKV(statsIDs []int64) (err error)
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

	// Below methods are for index usage.

	// NewSessionIndexUsageCollector creates a new IndexUsageCollector on the list.
	// The returned value's type should be *usage.SessionIndexUsageCollector, use interface{} to avoid cycle import now.
	// TODO: use *usage.SessionIndexUsageCollector instead of interface{}.
	NewSessionIndexUsageCollector() interface{}

	// DumpIndexUsageToKV dumps all collected index usage info to storage.
	DumpIndexUsageToKV() error

	// GCIndexUsage removes unnecessary index usage data.
	GCIndexUsage() error

	// Blow methods are for table delta and stats usage.

	// NewSessionStatsItem allocates a stats collector for a session.
	// TODO: use interface{} to avoid cycle import, remove this interface{}.
	NewSessionStatsItem() interface{}

	// ResetSessionStatsList resets the sessions stats list.
	ResetSessionStatsList()

	// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
	DumpStatsDeltaToKV(dumpAll bool) error

	// DumpColStatsUsageToKV sweeps the whole list, updates the column stats usage map and dumps it to KV.
	DumpColStatsUsageToKV() error
}

// StatsHistory is used to manage historical stats.
type StatsHistory interface {
	// RecordHistoricalStatsMeta records stats meta of the specified version to stats_meta_history.
	RecordHistoricalStatsMeta(tableID int64, version uint64, source string)

	// CheckHistoricalStatsEnable check whether historical stats is enabled.
	CheckHistoricalStatsEnable() (enable bool, err error)

	// RecordHistoricalStatsToStorage records the given table's stats data to mysql.stats_history
	RecordHistoricalStatsToStorage(dbName string, tableInfo *model.TableInfo, physicalID int64, isPartition bool) (uint64, error)
}

// StatsAnalyze is used to handle auto-analyze and manage analyze jobs.
type StatsAnalyze interface {
	// InsertAnalyzeJob inserts analyze job into mysql.analyze_jobs and gets job ID for further updating job.
	InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error

	// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
	DeleteAnalyzeJobs(updateTime time.Time) error

	// HandleAutoAnalyze analyzes the newly created table or index.
	HandleAutoAnalyze(is infoschema.InfoSchema) (analyzed bool)
}

// StatsCache is used to manage all table statistics in memory.
type StatsCache interface {
	// Close closes this cache.
	Close()

	// Clear clears this cache.
	Clear()

	// Update reads stats meta from store and updates the stats map.
	Update(is infoschema.InfoSchema) error

	// MemConsumed returns its memory usage.
	MemConsumed() (size int64)

	// Get returns the specified table's stats.
	Get(tableID int64) (*statistics.Table, bool)

	// Put puts this table stats into the cache.
	Put(tableID int64, t *statistics.Table)

	// UpdateStatsCache updates the cache.
	UpdateStatsCache(addedTables []*statistics.Table, deletedTableIDs []int64)

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

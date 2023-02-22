// Copyright 2017 PingCAP, Inc.
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

package handle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	ddlUtil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/syncutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	// TiDBGlobalStats represents the global-stats for a partitioned table.
	TiDBGlobalStats = "global"

	// maxPartitionMergeBatchSize indicates the max batch size for a worker to merge partition stats
	maxPartitionMergeBatchSize = 256
)

// Handle can update stats info periodically.
type Handle struct {

	// initStatsCtx is the ctx only used for initStats
	initStatsCtx sessionctx.Context

	mu struct {
		syncutil.RWMutex
		ctx sessionctx.Context
		// rateMap contains the error rate delta from feedback.
		rateMap errorRateDeltaMap
	}

	schemaMu struct {
		sync.RWMutex
		// pid2tid is the map from partition ID to table ID.
		pid2tid map[int64]int64
		// schemaVersion is the version of information schema when `pid2tid` is built.
		schemaVersion int64
	}

	// It can be read by multiple readers at the same time without acquiring lock, but it can be
	// written only after acquiring the lock.
	statsCache struct {
		sync.Mutex
		atomic.Value
		memTracker *memory.Tracker
	}

	pool sessionPool

	// ddlEventCh is a channel to notify a ddl operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	ddlEventCh chan *ddlUtil.Event
	// listHead contains all the stats collector required by session.
	listHead *SessionStatsCollector
	// globalMap contains all the delta map from collectors when we dump them to KV.
	globalMap struct {
		sync.Mutex
		data tableDeltaMap
	}
	// feedback is used to store query feedback info.
	feedback struct {
		sync.Mutex
		data *statistics.QueryFeedbackMap
	}
	// colMap contains all the column stats usage information from collectors when we dump them to KV.
	colMap struct {
		sync.Mutex
		data colStatsUsageMap
	}

	lease atomic2.Duration

	// idxUsageListHead contains all the index usage collectors required by session.
	idxUsageListHead *SessionIndexUsageCollector

	// StatsLoad is used to load stats concurrently
	StatsLoad StatsLoad

	// sysProcTracker is used to track sys process like analyze
	sysProcTracker sessionctx.SysProcTracker
	// serverIDGetter is used to get server ID for generating auto analyze ID.
	serverIDGetter func() uint64
	// tableLocked used to store locked tables
	tableLocked []int64
}

// GetTableLockedAndClearForTest for unit test only
func (h *Handle) GetTableLockedAndClearForTest() []int64 {
	tableLocked := h.tableLocked
	h.tableLocked = make([]int64, 0)
	return tableLocked
}

// LoadLockedTables load locked tables from store
func (h *Handle) LoadLockedTables() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_table_locked")
	if err != nil {
		return errors.Trace(err)
	}

	h.tableLocked = make([]int64, len(rows))
	for i, row := range rows {
		h.tableLocked[i] = row.GetInt64(0)
	}

	return nil
}

// AddLockedTables add locked tables id  to store
func (h *Handle) AddLockedTables(tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	exec := h.mu.ctx.(sqlexec.SQLExecutor)

	_, err := exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return "", err
	}

	//load tables to check duplicate when insert
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_table_locked")
	if err != nil {
		return "", err
	}

	dupTables := make([]string, 0)
	tableLocked := make([]int64, 0)
	for _, row := range rows {
		tableLocked = append(tableLocked, row.GetInt64(0))
	}

	strTids := fmt.Sprintf("%v", tids)
	logutil.BgLogger().Info("[stats] lock table ", zap.String("tableIDs", strTids))
	for i, tid := range tids {
		_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_table_locked(table_id) select %? from dual where not exists(select table_id from mysql.stats_table_locked where table_id = %?)", tid, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when insert mysql.stats_table_locked ", zap.Error(err))
			return "", err
		}
		// update handle
		if !isTableLocked(tableLocked, tid) {
			tableLocked = append(tableLocked, tid)
		} else {
			dupTables = append(dupTables, tables[i].Schema.L+"."+tables[i].Name.L)
		}
	}

	//insert related partitions while don't warning duplicate partitions
	for _, tid := range pids {
		_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_table_locked(table_id) select %? from dual where not exists(select table_id from mysql.stats_table_locked where table_id = %?)", tid, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when insert mysql.stats_table_locked ", zap.Error(err))
			return "", err
		}
		if !isTableLocked(tableLocked, tid) {
			tableLocked = append(tableLocked, tid)
		}
	}

	err = finishTransaction(ctx, exec, err)
	if err != nil {
		return "", err
	}
	// update handle.tableLocked after transaction success, if txn failed, tableLocked won't be updated
	h.tableLocked = tableLocked

	if len(dupTables) > 0 {
		tables := dupTables[0]
		for i, table := range dupTables {
			if i == 0 {
				continue
			}
			tables += ", " + table
		}
		var msg string
		if len(tids) > 1 {
			if len(tids) > len(dupTables) {
				msg = "skip locking locked tables: " + tables + ", other tables locked successfully"
			} else {
				msg = "skip locking locked tables: " + tables
			}
		} else {
			msg = "skip locking locked table: " + tables
		}
		return msg, err
	}
	return "", err
}

// getStatsDeltaFromTableLocked get count, modify_count and version for the given table from mysql.stats_table_locked.
func (h *Handle) getStatsDeltaFromTableLocked(ctx context.Context, tableID int64) (int64, int64, uint64, error) {
	rows, _, err := h.execRestrictedSQL(ctx, "select count, modify_count, version from mysql.stats_table_locked where table_id = %?", tableID)
	if err != nil {
		return 0, 0, 0, err
	}

	if len(rows) == 0 {
		return 0, 0, 0, nil
	}
	count := rows[0].GetInt64(0)
	modifyCount := rows[0].GetInt64(1)
	version := rows[0].GetUint64(2)
	return count, modifyCount, version, nil
}

// RemoveLockedTables remove tables from table locked array
func (h *Handle) RemoveLockedTables(tids []int64, pids []int64, tables []*ast.TableName) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)

	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return "", err
	}

	//load tables to check unlock the unlock table
	rows, _, err := h.execRestrictedSQL(ctx, "select table_id from mysql.stats_table_locked")
	if err != nil {
		return "", err
	}

	nonlockedTables := make([]string, 0)
	tableLocked := make([]int64, 0)
	for _, row := range rows {
		tableLocked = append(tableLocked, row.GetInt64(0))
	}

	strTids := fmt.Sprintf("%v", tids)
	logutil.BgLogger().Info("[stats] unlock table ", zap.String("tableIDs", strTids))
	for i, tid := range tids {
		// get stats delta during table locked
		count, modifyCount, version, err := h.getStatsDeltaFromTableLocked(ctx, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when getStatsDeltaFromTableLocked", zap.Error(err))
			return "", err
		}
		// update stats_meta with stats delta
		_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?", version, count, modifyCount, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when update mysql.stats_meta", zap.Error(err))
			return "", err
		}

		_, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_table_locked where table_id = %?", tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when delete from mysql.stats_table_locked ", zap.Error(err))
			return "", err
		}
		var exist bool
		exist, tableLocked = removeIfTableLocked(tableLocked, tid)
		if !exist {
			nonlockedTables = append(nonlockedTables, tables[i].Schema.L+"."+tables[i].Name.L)
		}
	}
	//delete related partitions while don't warning delete empty partitions
	for _, tid := range pids {
		// get stats delta during table locked
		count, modifyCount, version, err := h.getStatsDeltaFromTableLocked(ctx, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when getStatsDeltaFromTableLocked", zap.Error(err))
			return "", err
		}
		// update stats_meta with stats delta
		_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?", version, count, modifyCount, tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when update mysql.stats_meta", zap.Error(err))
			return "", err
		}

		_, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_table_locked where table_id = %?", tid)
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when delete from mysql.stats_table_locked ", zap.Error(err))
			return "", err
		}
		_, tableLocked = removeIfTableLocked(tableLocked, tid)
	}

	err = finishTransaction(ctx, exec, err)
	if err != nil {
		return "", err
	}
	// update handle.tableLocked after transaction success, if txn failed, tableLocked won't be updated
	h.tableLocked = tableLocked

	if len(nonlockedTables) > 0 {
		tables := nonlockedTables[0]
		for i, table := range nonlockedTables {
			if i == 0 {
				continue
			}
			tables += ", " + table
		}
		var msg string
		if len(tids) > 1 {
			if len(tids) > len(nonlockedTables) {
				msg = "skip unlocking non-locked tables: " + tables + ", other tables unlocked successfully"
			} else {
				msg = "skip unlocking non-locked tables: " + tables
			}
		} else {
			msg = "skip unlocking non-locked table: " + tables
		}
		return msg, err
	}
	return "", err
}

// IsTableLocked check whether table is locked in handle with Handle.Mutex
func (h *Handle) IsTableLocked(tableID int64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.isTableLocked(tableID)
}

// IsTableLocked check whether table is locked in handle without Handle.Mutex
func (h *Handle) isTableLocked(tableID int64) bool {
	return isTableLocked(h.tableLocked, tableID)
}

// isTableLocked check whether table is locked
func isTableLocked(tableLocked []int64, tableID int64) bool {
	return lockTableIndexOf(tableLocked, tableID) > -1
}

// lockTableIndexOf get the locked table's index in the array
func lockTableIndexOf(tableLocked []int64, tableID int64) int {
	for idx, id := range tableLocked {
		if id == tableID {
			return idx
		}
	}
	return -1
}

// removeIfTableLocked try to remove the table from table locked array
func removeIfTableLocked(tableLocked []int64, tableID int64) (bool, []int64) {
	idx := lockTableIndexOf(tableLocked, tableID)
	if idx > -1 {
		tableLocked = append(tableLocked[:idx], tableLocked[idx+1:]...)
	}
	return idx > -1, tableLocked
}

func (h *Handle) withRestrictedSQLExecutor(ctx context.Context, fn func(context.Context, sqlexec.RestrictedSQLExecutor) ([]chunk.Row, []*ast.ResultField, error)) ([]chunk.Row, []*ast.ResultField, error) {
	se, err := h.pool.Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer h.pool.Put(se)

	exec := se.(sqlexec.RestrictedSQLExecutor)
	return fn(ctx, exec)
}

func (h *Handle) execRestrictedSQL(ctx context.Context, sql string, params ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	return h.withRestrictedSQLExecutor(ctx, func(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) ([]chunk.Row, []*ast.ResultField, error) {
		return exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, params...)
	})
}

func (h *Handle) execRestrictedSQLWithStatsVer(ctx context.Context, statsVer int, procTrackID uint64, analyzeSnapshot bool, sql string, params ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	return h.withRestrictedSQLExecutor(ctx, func(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) ([]chunk.Row, []*ast.ResultField, error) {
		optFuncs := []sqlexec.OptionFuncAlias{
			execOptionForAnalyze[statsVer],
			sqlexec.GetAnalyzeSnapshotOption(analyzeSnapshot),
			sqlexec.GetPartitionPruneModeOption(string(h.CurrentPruneMode())),
			sqlexec.ExecOptionUseCurSession,
			sqlexec.ExecOptionWithSysProcTrack(procTrackID, h.sysProcTracker.Track, h.sysProcTracker.UnTrack),
		}
		return exec.ExecRestrictedSQL(ctx, optFuncs, sql, params...)
	})
}

func (h *Handle) execRestrictedSQLWithSnapshot(ctx context.Context, sql string, snapshot uint64, params ...interface{}) ([]chunk.Row, []*ast.ResultField, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)
	return h.withRestrictedSQLExecutor(ctx, func(ctx context.Context, exec sqlexec.RestrictedSQLExecutor) ([]chunk.Row, []*ast.ResultField, error) {
		optFuncs := []sqlexec.OptionFuncAlias{
			sqlexec.ExecOptionWithSnapshot(snapshot),
			sqlexec.ExecOptionUseCurSession,
		}
		return exec.ExecRestrictedSQL(ctx, optFuncs, sql, params...)
	})
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	// TODO: Here h.mu seems to protect all the fields of Handle. Is is reasonable?
	h.mu.Lock()
	h.statsCache.Lock()
	h.statsCache.Store(newStatsCache())
	h.statsCache.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)
	h.statsCache.Unlock()
	for len(h.ddlEventCh) > 0 {
		<-h.ddlEventCh
	}
	h.feedback.Lock()
	h.feedback.data = statistics.NewQueryFeedbackMap()
	h.feedback.Unlock()
	h.mu.ctx.GetSessionVars().InitChunkSize = 1
	h.mu.ctx.GetSessionVars().MaxChunkSize = 1
	h.mu.ctx.GetSessionVars().EnableChunkRPC = false
	h.mu.ctx.GetSessionVars().SetProjectionConcurrency(0)
	h.listHead = &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)}
	h.globalMap.Lock()
	h.globalMap.data = make(tableDeltaMap)
	h.globalMap.Unlock()
	h.colMap.Lock()
	h.colMap.data = make(colStatsUsageMap)
	h.colMap.Unlock()
	h.mu.rateMap = make(errorRateDeltaMap)
	h.mu.Unlock()
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// NewHandle creates a Handle for update stats.
func NewHandle(ctx, initStatsCtx sessionctx.Context, lease time.Duration, pool sessionPool, tracker sessionctx.SysProcTracker, serverIDGetter func() uint64) (*Handle, error) {
	cfg := config.GetGlobalConfig()
	handle := &Handle{
		ddlEventCh:       make(chan *ddlUtil.Event, 1000),
		listHead:         &SessionStatsCollector{mapper: make(tableDeltaMap), rateMap: make(errorRateDeltaMap)},
		idxUsageListHead: &SessionIndexUsageCollector{mapper: make(indexUsageMap)},
		pool:             pool,
		sysProcTracker:   tracker,
		serverIDGetter:   serverIDGetter,
	}
	handle.initStatsCtx = initStatsCtx
	handle.lease.Store(lease)
	handle.statsCache.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)
	handle.mu.ctx = ctx
	handle.mu.rateMap = make(errorRateDeltaMap)
	handle.statsCache.Store(newStatsCache())
	handle.globalMap.data = make(tableDeltaMap)
	handle.feedback.data = statistics.NewQueryFeedbackMap()
	handle.colMap.data = make(colStatsUsageMap)
	handle.StatsLoad.SubCtxs = make([]sessionctx.Context, cfg.Performance.StatsLoadConcurrency)
	handle.StatsLoad.NeededItemsCh = make(chan *NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	handle.StatsLoad.TimeoutItemsCh = make(chan *NeededItemTask, cfg.Performance.StatsLoadQueueSize)
	handle.StatsLoad.WorkingColMap = map[model.TableItemID][]chan stmtctx.StatsLoadResult{}
	err := handle.RefreshVars()
	if err != nil {
		return nil, err
	}
	return handle, nil
}

// Lease returns the stats lease.
func (h *Handle) Lease() time.Duration {
	return h.lease.Load()
}

// SetLease sets the stats lease.
func (h *Handle) SetLease(lease time.Duration) {
	h.lease.Store(lease)
}

// GetQueryFeedback gets the query feedback. It is only used in test.
func (h *Handle) GetQueryFeedback() *statistics.QueryFeedbackMap {
	h.feedback.Lock()
	defer func() {
		h.feedback.data = statistics.NewQueryFeedbackMap()
		h.feedback.Unlock()
	}()
	return h.feedback.data
}

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

var statsHealthyGauges = []prometheus.Gauge{
	metrics.StatsHealthyGauge.WithLabelValues("[0,50)"),
	metrics.StatsHealthyGauge.WithLabelValues("[50,80)"),
	metrics.StatsHealthyGauge.WithLabelValues("[80,100)"),
	metrics.StatsHealthyGauge.WithLabelValues("[100,100]"),
	// [0,100] should always be the last
	metrics.StatsHealthyGauge.WithLabelValues("[0,100]"),
}

// UpdateStatsHealthyMetrics updates stats healthy distribution metrics according to stats cache.
func (h *Handle) UpdateStatsHealthyMetrics() {
	v := h.statsCache.Load()
	if v == nil {
		return
	}

	distribution := make([]int64, 5)
	for _, tbl := range v.(statsCache).Values() {
		healthy, ok := tbl.GetStatsHealthy()
		if !ok {
			continue
		}
		if healthy < 50 {
			distribution[0] += 1
		} else if healthy < 80 {
			distribution[1] += 1
		} else if healthy < 100 {
			distribution[2] += 1
		} else {
			distribution[3] += 1
		}
		distribution[4] += 1
	}
	for i, val := range distribution {
		statsHealthyGauges[i].Set(float64(val))
	}
}

// Update reads stats meta from store and updates the stats map.
func (h *Handle) Update(is infoschema.InfoSchema, opts ...TableStatsOpt) error {
	oldCache := h.statsCache.Load().(statsCache)
	lastVersion := oldCache.version
	// We need this because for two tables, the smaller version may write later than the one with larger version.
	// Consider the case that there are two tables A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and update the lastVersion to B0, but we cannot read
	// the table stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than three lease.
	offset := DurationToTS(3 * h.Lease())
	if oldCache.version >= offset {
		lastVersion = lastVersion - offset
	} else {
		lastVersion = 0
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := h.execRestrictedSQL(ctx, "SELECT version, table_id, modify_count, count from mysql.stats_meta where version > %? order by version", lastVersion)
	if err != nil {
		return errors.Trace(err)
	}
	option := &tableStatsOption{}
	for _, opt := range opts {
		opt(option)
	}
	tables := make([]*statistics.Table, 0, len(rows))
	deletedTableIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		version := row.GetUint64(0)
		physicalID := row.GetInt64(1)
		modifyCount := row.GetInt64(2)
		count := row.GetInt64(3)
		lastVersion = version
		table, ok := h.getTableByPhysicalID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta table, maybe it has been dropped", zap.Int64("ID", physicalID))
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tableInfo := table.Meta()
		if oldTbl, ok := oldCache.Get(physicalID); ok && oldTbl.Version >= version && tableInfo.UpdateTS == oldTbl.TblInfoUpdateTS {
			continue
		}
		tbl, err := h.TableStatsFromStorage(tableInfo, physicalID, false, 0)
		// Error is not nil may mean that there are some ddl changes on this table, we will not update it.
		if err != nil {
			logutil.BgLogger().Error("[stats] error occurred when read table stats", zap.String("table", tableInfo.Name.O), zap.Error(err))
			continue
		}
		if tbl == nil {
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tbl.Version = version
		tbl.Count = count
		tbl.ModifyCount = modifyCount
		tbl.Name = getFullTableName(is, tableInfo)
		tbl.TblInfoUpdateTS = tableInfo.UpdateTS
		tables = append(tables, tbl)
	}
	h.updateStatsCache(oldCache.update(tables, deletedTableIDs, lastVersion, opts...))
	return nil
}

// UpdateSessionVar updates the necessary session variables for the stats reader.
func (h *Handle) UpdateSessionVar() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	verInString, err := h.mu.ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBAnalyzeVersion)
	if err != nil {
		return err
	}
	ver, err := strconv.ParseInt(verInString, 10, 64)
	if err != nil {
		return err
	}
	h.mu.ctx.GetSessionVars().AnalyzeVersion = int(ver)
	return err
}

// GlobalStats is used to store the statistics contained in the global-level stats
// which is generated by the merge of partition-level stats.
// It will both store the column stats and index stats.
// In the column statistics, the variable `num` is equal to the number of columns in the partition table.
// In the index statistics, the variable `num` is always equal to one.
type GlobalStats struct {
	Num         int
	Count       int64
	ModifyCount int64
	Hg          []*statistics.Histogram
	Cms         []*statistics.CMSketch
	TopN        []*statistics.TopN
	Fms         []*statistics.FMSketch
}

// MergePartitionStats2GlobalStatsByTableID merge the partition-level stats to global-level stats based on the tableID.
func (h *Handle) MergePartitionStats2GlobalStatsByTableID(sc sessionctx.Context,
	opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema,
	physicalID int64, isIndex int, histIDs []int64,
	tablePartitionStats map[int64]*statistics.Table) (globalStats *GlobalStats, err error) {
	// get the partition table IDs
	globalTable, ok := h.getTableByPhysicalID(is, physicalID)
	if !ok {
		err = errors.Errorf("unknown physical ID %d in stats meta table, maybe it has been dropped", physicalID)
		return
	}
	globalTableInfo := globalTable.Meta()
	return h.mergePartitionStats2GlobalStats(sc, opts, is, globalTableInfo, isIndex, histIDs, tablePartitionStats)
}

func (h *Handle) loadTablePartitionStats(tableInfo *model.TableInfo, partitionDef *model.PartitionDefinition) (*statistics.Table, error) {
	var partitionStats *statistics.Table
	partitionStats, err := h.TableStatsFromStorage(tableInfo, partitionDef.ID, true, 0)
	if err != nil {
		return nil, err
	}
	// if the err == nil && partitionStats == nil, it means we lack the partition-level stats which the physicalID is equal to partitionID.
	if partitionStats == nil {
		errMsg := fmt.Sprintf("table `%s` partition `%s`", tableInfo.Name.L, partitionDef.Name.L)
		err = types.ErrPartitionStatsMissing.GenWithStackByArgs(errMsg)
		return nil, err
	}
	return partitionStats, nil
}

// MergePartitionStats2GlobalStatsByTableID merge the partition-level stats to global-level stats based on the tableInfo.
func (h *Handle) mergePartitionStats2GlobalStats(sc sessionctx.Context,
	opts map[ast.AnalyzeOptionType]uint64, is infoschema.InfoSchema, globalTableInfo *model.TableInfo,
	isIndex int, histIDs []int64,
	allPartitionStats map[int64]*statistics.Table) (globalStats *GlobalStats, err error) {
	partitionNum := len(globalTableInfo.Partition.Definitions)
	partitionIDs := make([]int64, 0, partitionNum)
	for i := 0; i < partitionNum; i++ {
		partitionIDs = append(partitionIDs, globalTableInfo.Partition.Definitions[i].ID)
	}

	// initialized the globalStats
	globalStats = new(GlobalStats)
	if len(histIDs) == 0 {
		for _, col := range globalTableInfo.Columns {
			// The virtual generated column stats can not be merged to the global stats.
			if col.IsGenerated() && !col.GeneratedStored {
				continue
			}
			histIDs = append(histIDs, col.ID)
		}
	}
	globalStats.Num = len(histIDs)
	globalStats.Count = 0
	globalStats.Hg = make([]*statistics.Histogram, globalStats.Num)
	globalStats.Cms = make([]*statistics.CMSketch, globalStats.Num)
	globalStats.TopN = make([]*statistics.TopN, globalStats.Num)
	globalStats.Fms = make([]*statistics.FMSketch, globalStats.Num)

	// The first dimension of slice is means the number of column or index stats in the globalStats.
	// The second dimension of slice is means the number of partition tables.
	// Because all topN and histograms need to be collected before they can be merged.
	// So we should store all of the partition-level stats first, and merge them together.
	allHg := make([][]*statistics.Histogram, globalStats.Num)
	allCms := make([][]*statistics.CMSketch, globalStats.Num)
	allTopN := make([][]*statistics.TopN, globalStats.Num)
	allFms := make([][]*statistics.FMSketch, globalStats.Num)
	for i := 0; i < globalStats.Num; i++ {
		allHg[i] = make([]*statistics.Histogram, 0, partitionNum)
		allCms[i] = make([]*statistics.CMSketch, 0, partitionNum)
		allTopN[i] = make([]*statistics.TopN, 0, partitionNum)
		allFms[i] = make([]*statistics.FMSketch, 0, partitionNum)
	}

	for _, def := range globalTableInfo.Partition.Definitions {
		partitionID := def.ID
		partitionTable, ok := h.getTableByPhysicalID(is, partitionID)
		if !ok {
			err = errors.Errorf("unknown physical ID %d in stats meta table, maybe it has been dropped", partitionID)
			return
		}
		tableInfo := partitionTable.Meta()
		var partitionStats *statistics.Table
		if allPartitionStats != nil {
			partitionStats, ok = allPartitionStats[partitionID]
		}
		// If pre-load partition stats isn't provided, then we load partition stats directly and set it into allPartitionStats
		if allPartitionStats == nil || partitionStats == nil || !ok {
			partitionStats, err = h.loadTablePartitionStats(tableInfo, &def)
			if err != nil {
				return
			}
			if allPartitionStats == nil {
				allPartitionStats = make(map[int64]*statistics.Table)
			}
			allPartitionStats[partitionID] = partitionStats
		}
		for i := 0; i < globalStats.Num; i++ {
			_, hg, cms, topN, fms, analyzed := partitionStats.GetStatsInfo(histIDs[i], isIndex == 1)
			if !analyzed {
				var errMsg string
				if isIndex == 0 {
					errMsg = fmt.Sprintf("table `%s` partition `%s` column `%s`", tableInfo.Name.L, def.Name.L, tableInfo.FindColumnNameByID(histIDs[i]))
				} else {
					errMsg = fmt.Sprintf("table `%s` partition `%s` index `%s`", tableInfo.Name.L, def.Name.L, tableInfo.FindIndexNameByID(histIDs[i]))
				}
				err = types.ErrPartitionStatsMissing.GenWithStackByArgs(errMsg)
				return
			}
			// partition stats is not empty but column stats(hist, topn) is missing
			if partitionStats.Count > 0 && (hg == nil || hg.TotalRowCount() <= 0) && (topN == nil || topN.TotalCount() <= 0) {
				var errMsg string
				if isIndex == 0 {
					errMsg = fmt.Sprintf("table `%s` partition `%s` column `%s`", tableInfo.Name.L, def.Name.L, tableInfo.FindColumnNameByID(histIDs[i]))
				} else {
					errMsg = fmt.Sprintf("table `%s` partition `%s` index `%s`", tableInfo.Name.L, def.Name.L, tableInfo.FindIndexNameByID(histIDs[i]))
				}
				err = types.ErrPartitionColumnStatsMissing.GenWithStackByArgs(errMsg)
				return
			}
			if i == 0 {
				// In a partition, we will only update globalStats.Count once
				globalStats.Count += partitionStats.Count
				globalStats.ModifyCount += partitionStats.ModifyCount
			}
			allHg[i] = append(allHg[i], hg)
			allCms[i] = append(allCms[i], cms)
			allTopN[i] = append(allTopN[i], topN)
			allFms[i] = append(allFms[i], fms)
		}
	}

	// After collect all of the statistics from the partition-level stats,
	// we should merge them together.
	for i := 0; i < globalStats.Num; i++ {
		// Merge CMSketch
		globalStats.Cms[i] = allCms[i][0].Copy()
		for j := 1; j < partitionNum; j++ {
			err = globalStats.Cms[i].MergeCMSketch(allCms[i][j])
			if err != nil {
				return
			}
		}

		// Merge topN. We need to merge TopN before merging the histogram.
		// Because after merging TopN, some numbers will be left.
		// These remaining topN numbers will be used as a separate bucket for later histogram merging.
		var popedTopN []statistics.TopNMeta
		wrapper := statistics.NewStatsWrapper(allHg[i], allTopN[i])
		globalStats.TopN[i], popedTopN, allHg[i], err = h.mergeGlobalStatsTopN(sc, wrapper, sc.GetSessionVars().StmtCtx.TimeZone, sc.GetSessionVars().AnalyzeVersion, uint32(opts[ast.AnalyzeOptNumTopN]), isIndex == 1)
		if err != nil {
			return
		}

		// Merge histogram
		globalStats.Hg[i], err = statistics.MergePartitionHist2GlobalHist(sc.GetSessionVars().StmtCtx, allHg[i], popedTopN, int64(opts[ast.AnalyzeOptNumBuckets]), isIndex == 1)
		if err != nil {
			return
		}

		// NOTICE: after merging bucket NDVs have the trend to be underestimated, so for safe we don't use them.
		for j := range globalStats.Hg[i].Buckets {
			globalStats.Hg[i].Buckets[j].NDV = 0
		}

		// Update NDV of global-level stats
		globalStats.Fms[i] = allFms[i][0].Copy()
		for j := 1; j < partitionNum; j++ {
			globalStats.Fms[i].MergeFMSketch(allFms[i][j])
		}

		// update the NDV
		globalStatsNDV := globalStats.Fms[i].NDV()
		if globalStatsNDV > globalStats.Count {
			globalStatsNDV = globalStats.Count
		}
		globalStats.Hg[i].NDV = globalStatsNDV
	}
	return
}

func (h *Handle) mergeGlobalStatsTopN(sc sessionctx.Context, wrapper *statistics.StatsWrapper,
	timeZone *time.Location, version int, n uint32, isIndex bool) (*statistics.TopN,
	[]statistics.TopNMeta, []*statistics.Histogram, error) {
	mergeConcurrency := sc.GetSessionVars().AnalyzePartitionMergeConcurrency
	killed := &sc.GetSessionVars().Killed
	// use original method if concurrency equals 1 or for version1
	if mergeConcurrency < 2 {
		return statistics.MergePartTopN2GlobalTopN(timeZone, version, wrapper.AllTopN, n, wrapper.AllHg, isIndex, killed)
	}
	batchSize := len(wrapper.AllTopN) / mergeConcurrency
	if batchSize < 1 {
		batchSize = 1
	} else if batchSize > maxPartitionMergeBatchSize {
		batchSize = maxPartitionMergeBatchSize
	}
	return h.mergeGlobalStatsTopNByConcurrency(mergeConcurrency, batchSize, wrapper, timeZone, version, n, isIndex, killed)
}

// mergeGlobalStatsTopNByConcurrency merge partition topN by concurrency
// To merge global stats topn by concurrency, we will separate the partition topn in concurrency part and deal it with different worker.
// mergeConcurrency is used to control the total concurrency of the running worker, and mergeBatchSize is sued to control
// the partition size for each worker to solve it
func (h *Handle) mergeGlobalStatsTopNByConcurrency(mergeConcurrency, mergeBatchSize int, wrapper *statistics.StatsWrapper,
	timeZone *time.Location, version int, n uint32, isIndex bool, killed *uint32) (*statistics.TopN,
	[]statistics.TopNMeta, []*statistics.Histogram, error) {
	if len(wrapper.AllTopN) < mergeConcurrency {
		mergeConcurrency = len(wrapper.AllTopN)
	}
	tasks := make([]*statistics.TopnStatsMergeTask, 0)
	for start := 0; start < len(wrapper.AllTopN); {
		end := start + mergeBatchSize
		if end > len(wrapper.AllTopN) {
			end = len(wrapper.AllTopN)
		}
		task := statistics.NewTopnStatsMergeTask(start, end)
		tasks = append(tasks, task)
		start = end
	}
	var wg util.WaitGroupWrapper
	taskNum := len(tasks)
	taskCh := make(chan *statistics.TopnStatsMergeTask, taskNum)
	respCh := make(chan *statistics.TopnStatsMergeResponse, taskNum)
	for i := 0; i < mergeConcurrency; i++ {
		worker := statistics.NewTopnStatsMergeWorker(taskCh, respCh, wrapper, killed)
		wg.Run(func() {
			worker.Run(timeZone, isIndex, n, version)
		})
	}
	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()
	close(respCh)
	resps := make([]*statistics.TopnStatsMergeResponse, 0)

	// handle Error
	hasErr := false
	for resp := range respCh {
		if resp.Err != nil {
			hasErr = true
		}
		resps = append(resps, resp)
	}
	if hasErr {
		errMsg := make([]string, 0)
		for _, resp := range resps {
			if resp.Err != nil {
				errMsg = append(errMsg, resp.Err.Error())
			}
		}
		return nil, nil, nil, errors.New(strings.Join(errMsg, ","))
	}

	// fetch the response from each worker and merge them into global topn stats
	sorted := make([]statistics.TopNMeta, 0, mergeConcurrency)
	leftTopn := make([]statistics.TopNMeta, 0)
	for _, resp := range resps {
		if resp.TopN != nil {
			sorted = append(sorted, resp.TopN.TopN...)
		}
		leftTopn = append(leftTopn, resp.PopedTopn...)
		for i, removeTopn := range resp.RemoveVals {
			// Remove the value from the Hists.
			if len(removeTopn) > 0 {
				tmp := removeTopn
				slices.SortFunc(tmp, func(i, j statistics.TopNMeta) bool {
					cmpResult := bytes.Compare(i.Encoded, j.Encoded)
					return cmpResult < 0
				})
				wrapper.AllHg[i].RemoveVals(tmp)
			}
		}
	}

	globalTopN, popedTopn := statistics.GetMergedTopNFromSortedSlice(sorted, n)
	return globalTopN, statistics.SortTopnMeta(append(leftTopn, popedTopn...)), wrapper.AllHg, nil
}

func (h *Handle) getTableByPhysicalID(is infoschema.InfoSchema, physicalID int64) (table.Table, bool) {
	h.schemaMu.Lock()
	defer h.schemaMu.Unlock()
	if is.SchemaMetaVersion() != h.schemaMu.schemaVersion {
		h.schemaMu.schemaVersion = is.SchemaMetaVersion()
		h.schemaMu.pid2tid = buildPartitionID2TableID(is)
	}
	if id, ok := h.schemaMu.pid2tid[physicalID]; ok {
		return is.TableByID(id)
	}
	return is.TableByID(physicalID)
}

func buildPartitionID2TableID(is infoschema.InfoSchema) map[int64]int64 {
	mapper := make(map[int64]int64)
	for _, db := range is.AllSchemas() {
		tbls := db.Tables
		for _, tbl := range tbls {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				continue
			}
			for _, def := range pi.Definitions {
				mapper[def.ID] = tbl.ID
			}
		}
	}
	return mapper
}

// GetMemConsumed returns the mem size of statscache consumed
func (h *Handle) GetMemConsumed() (size int64) {
	size = h.statsCache.memTracker.BytesConsumed()
	return
}

// GetTableStats retrieves the statistics table from cache, and the cache will be updated by a goroutine.
func (h *Handle) GetTableStats(tblInfo *model.TableInfo, opts ...TableStatsOpt) *statistics.Table {
	return h.GetPartitionStats(tblInfo, tblInfo.ID, opts...)
}

// GetPartitionStats retrieves the partition stats from cache.
func (h *Handle) GetPartitionStats(tblInfo *model.TableInfo, pid int64, opts ...TableStatsOpt) *statistics.Table {
	var tbl *statistics.Table
	if h == nil {
		tbl = statistics.PseudoTable(tblInfo)
		tbl.PhysicalID = pid
		return tbl
	}
	statsCache := h.statsCache.Load().(statsCache)
	var ok bool
	option := &tableStatsOption{}
	for _, opt := range opts {
		opt(option)
	}
	if option.byQuery {
		tbl, ok = statsCache.GetByQuery(pid)
	} else {
		tbl, ok = statsCache.Get(pid)
	}
	if !ok {
		tbl = statistics.PseudoTable(tblInfo)
		tbl.PhysicalID = pid
		h.updateStatsCache(statsCache.update([]*statistics.Table{tbl}, nil, statsCache.version))
		return tbl
	}
	return tbl
}

// updateStatsCache overrides the global statsCache with a new one, it may fail
// if the global statsCache has been modified by others already.
// Callers should add retry loop if necessary.
func (h *Handle) updateStatsCache(newCache statsCache) (updated bool) {
	h.statsCache.Lock()
	oldCache := h.statsCache.Load().(statsCache)
	enableQuota := oldCache.EnableQuota()
	newCost := newCache.Cost()
	if oldCache.version < newCache.version || (oldCache.version == newCache.version && oldCache.minorVersion < newCache.minorVersion) {
		h.statsCache.memTracker.Consume(newCost - oldCache.Cost())
		h.statsCache.Store(newCache)
		updated = true
	}
	h.statsCache.Unlock()
	if updated && enableQuota {
		costGauge.Set(float64(newCost))
	}
	return
}

// LoadNeededHistograms will load histograms for those needed columns/indices.
func (h *Handle) LoadNeededHistograms() (err error) {
	items := statistics.HistogramNeededItems.AllItems()
	reader, err := h.getGlobalStatsReader(0)
	if err != nil {
		return err
	}

	defer func() {
		err1 := h.releaseGlobalStatsReader(reader)
		if err1 != nil && err == nil {
			err = err1
		}
	}()
	loadFMSketch := config.GetGlobalConfig().Performance.EnableLoadFMSketch

	for _, item := range items {
		if !item.IsIndex {
			err = h.loadNeededColumnHistograms(reader, item, loadFMSketch)
		} else {
			err = h.loadNeededIndexHistograms(reader, item, loadFMSketch)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *Handle) loadNeededColumnHistograms(reader *statistics.StatsReader, col model.TableItemID, loadFMSketch bool) (err error) {
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.Get(col.TableID)
	if !ok {
		return nil
	}
	c, ok := tbl.Columns[col.ID]
	if !ok || !c.IsLoadNeeded() {
		statistics.HistogramNeededItems.Delete(col)
		return nil
	}
	hg, err := statistics.HistogramFromStorage(reader, col.TableID, c.ID, &c.Info.FieldType, c.Histogram.NDV, 0, c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation)
	if err != nil {
		return errors.Trace(err)
	}
	cms, topN, err := statistics.CMSketchAndTopNFromStorage(reader, col.TableID, 0, col.ID)
	if err != nil {
		return errors.Trace(err)
	}
	var fms *statistics.FMSketch
	if loadFMSketch {
		fms, err = statistics.FMSketchFromStorage(reader, col.TableID, 0, col.ID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	rows, _, err := reader.Read("select stats_ver from mysql.stats_histograms where is_index = 0 and table_id = %? and hist_id = %?", col.TableID, col.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", col.TableID), zap.Int64("hist_id", col.ID))
		return errors.Trace(fmt.Errorf("fail to get stats version for this histogram, table_id:%v, hist_id:%v", col.TableID, col.ID))
	}
	statsVer := rows[0].GetInt64(0)
	colHist := &statistics.Column{
		PhysicalID: col.TableID,
		Histogram:  *hg,
		Info:       c.Info,
		CMSketch:   cms,
		TopN:       topN,
		FMSketch:   fms,
		IsHandle:   c.IsHandle,
		StatsVer:   statsVer,
	}
	// Column.Count is calculated by Column.TotalRowCount(). Hence we don't set Column.Count when initializing colHist.
	colHist.Count = int64(colHist.TotalRowCount())
	// When adding/modifying a column, we create its stats(all values are default values) without setting stats_ver.
	// So we need add colHist.Count > 0 here.
	if statsVer != statistics.Version0 || colHist.Count > 0 {
		colHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
	}
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	oldCache = h.statsCache.Load().(statsCache)
	tbl, ok = oldCache.Get(col.TableID)
	if !ok {
		return nil
	}
	tbl = tbl.Copy()
	tbl.Columns[c.ID] = colHist
	if h.updateStatsCache(oldCache.update([]*statistics.Table{tbl}, nil, oldCache.version)) {
		statistics.HistogramNeededItems.Delete(col)
	}
	return nil
}

func (h *Handle) loadNeededIndexHistograms(reader *statistics.StatsReader, idx model.TableItemID, loadFMSketch bool) (err error) {
	oldCache := h.statsCache.Load().(statsCache)
	tbl, ok := oldCache.Get(idx.TableID)
	if !ok {
		return nil
	}
	index, ok := tbl.Indices[idx.ID]
	if !ok {
		statistics.HistogramNeededItems.Delete(idx)
		return nil
	}
	hg, err := statistics.HistogramFromStorage(reader, idx.TableID, index.ID, types.NewFieldType(mysql.TypeBlob), index.Histogram.NDV, 1, index.LastUpdateVersion, index.NullCount, index.TotColSize, index.Correlation)
	if err != nil {
		return errors.Trace(err)
	}
	cms, topN, err := statistics.CMSketchAndTopNFromStorage(reader, idx.TableID, 1, idx.ID)
	if err != nil {
		return errors.Trace(err)
	}
	var fms *statistics.FMSketch
	if loadFMSketch {
		fms, err = statistics.FMSketchFromStorage(reader, idx.TableID, 1, idx.ID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	rows, _, err := reader.Read("select stats_ver from mysql.stats_histograms where is_index = 1 and table_id = %? and hist_id = %?", idx.TableID, idx.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", idx.TableID), zap.Int64("hist_id", idx.ID))
		return errors.Trace(fmt.Errorf("fail to get stats version for this histogram, table_id:%v, hist_id:%v", idx.TableID, idx.ID))
	}
	idxHist := &statistics.Index{Histogram: *hg, CMSketch: cms, TopN: topN, FMSketch: fms,
		Info: index.Info, ErrorRate: index.ErrorRate, StatsVer: rows[0].GetInt64(0),
		Flag: index.Flag, PhysicalID: idx.TableID,
		StatsLoadedStatus: statistics.NewStatsFullLoadStatus()}
	index.LastAnalyzePos.Copy(&idxHist.LastAnalyzePos)

	oldCache = h.statsCache.Load().(statsCache)
	tbl, ok = oldCache.Get(idx.TableID)
	if !ok {
		return nil
	}
	tbl = tbl.Copy()
	tbl.Indices[idx.ID] = idxHist
	if h.updateStatsCache(oldCache.update([]*statistics.Table{tbl}, nil, oldCache.version)) {
		statistics.HistogramNeededItems.Delete(idx)
	}
	return nil
}

// LastUpdateVersion gets the last update version.
func (h *Handle) LastUpdateVersion() uint64 {
	return h.statsCache.Load().(statsCache).version
}

// SetLastUpdateVersion sets the last update version.
func (h *Handle) SetLastUpdateVersion(version uint64) {
	statsCache := h.statsCache.Load().(statsCache)
	h.updateStatsCache(statsCache.update(nil, nil, version))
}

// FlushStats flushes the cached stats update into store.
func (h *Handle) FlushStats() {
	for len(h.ddlEventCh) > 0 {
		e := <-h.ddlEventCh
		if err := h.HandleDDLEvent(e); err != nil {
			logutil.BgLogger().Error("[stats] handle ddl event fail", zap.Error(err))
		}
	}
	if err := h.DumpStatsDeltaToKV(DumpAll); err != nil {
		logutil.BgLogger().Error("[stats] dump stats delta fail", zap.Error(err))
	}
	if err := h.DumpStatsFeedbackToKV(); err != nil {
		logutil.BgLogger().Error("[stats] dump stats feedback fail", zap.Error(err))
	}
}

func (h *Handle) indexStatsFromStorage(reader *statistics.StatsReader, row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	statsVer := row.GetInt64(7)
	idx := table.Indices[histID]
	errorRate := statistics.ErrorRate{}
	flag := row.GetInt64(8)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	if statistics.IsAnalyzed(flag) && !reader.IsHistory() {
		h.mu.rateMap.clear(table.PhysicalID, histID, true)
	} else if idx != nil {
		errorRate = idx.ErrorRate
	}
	for _, idxInfo := range tableInfo.Indices {
		if histID != idxInfo.ID {
			continue
		}
		if idx == nil || idx.LastUpdateVersion < histVer {
			hg, err := statistics.HistogramFromStorage(reader, table.PhysicalID, histID, types.NewFieldType(mysql.TypeBlob), distinct, 1, histVer, nullCount, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}
			cms, topN, err := statistics.CMSketchAndTopNFromStorage(reader, table.PhysicalID, 1, idxInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			fmSketch, err := statistics.FMSketchFromStorage(reader, table.PhysicalID, 1, histID)
			if err != nil {
				return errors.Trace(err)
			}
			idx = &statistics.Index{
				Histogram:  *hg,
				CMSketch:   cms,
				TopN:       topN,
				FMSketch:   fmSketch,
				Info:       idxInfo,
				ErrorRate:  errorRate,
				StatsVer:   statsVer,
				Flag:       flag,
				PhysicalID: table.PhysicalID,
			}
			if statsVer != statistics.Version0 {
				idx.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			}
			lastAnalyzePos.Copy(&idx.LastAnalyzePos)
		}
		break
	}
	if idx != nil {
		table.Indices[histID] = idx
	} else {
		logutil.BgLogger().Debug("we cannot find index id in table info. It may be deleted.", zap.Int64("indexID", histID), zap.String("table", tableInfo.Name.O))
	}
	return nil
}

func (h *Handle) columnStatsFromStorage(reader *statistics.StatsReader, row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo, loadAll bool) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	totColSize := row.GetInt64(6)
	statsVer := row.GetInt64(7)
	correlation := row.GetFloat64(9)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	col := table.Columns[histID]
	errorRate := statistics.ErrorRate{}
	flag := row.GetInt64(8)
	if statistics.IsAnalyzed(flag) && !reader.IsHistory() {
		h.mu.rateMap.clear(table.PhysicalID, histID, false)
	} else if col != nil {
		errorRate = col.ErrorRate
	}
	for _, colInfo := range tableInfo.Columns {
		if histID != colInfo.ID {
			continue
		}
		isHandle := tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag())
		// We will not load buckets if:
		// 1. Lease > 0, and:
		// 2. this column is not handle, and:
		// 3. the column doesn't has any statistics before, and:
		// 4. loadAll is false.
		notNeedLoad := h.Lease() > 0 &&
			!isHandle &&
			(col == nil || !col.IsStatsInitialized() && col.LastUpdateVersion < histVer) &&
			!loadAll
		if notNeedLoad {
			count, err := statistics.ColumnCountFromStorage(reader, table.PhysicalID, histID, statsVer)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *statistics.NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totColSize),
				Info:       colInfo,
				Count:      count + nullCount,
				ErrorRate:  errorRate,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				Flag:       flag,
				StatsVer:   statsVer,
			}
			// When adding/modifying a column, we create its stats(all values are default values) without setting stats_ver.
			// So we need add col.Count > 0 here.
			if statsVer != statistics.Version0 || col.Count > 0 {
				col.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			col.Histogram.Correlation = correlation
			break
		}
		if col == nil || col.LastUpdateVersion < histVer || loadAll {
			hg, err := statistics.HistogramFromStorage(reader, table.PhysicalID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount, totColSize, correlation)
			if err != nil {
				return errors.Trace(err)
			}
			cms, topN, err := statistics.CMSketchAndTopNFromStorage(reader, table.PhysicalID, 0, colInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			var fmSketch *statistics.FMSketch
			if loadAll {
				// FMSketch is only used when merging partition stats into global stats. When merging partition stats into global stats,
				// we load all the statistics, i.e., loadAll is true.
				fmSketch, err = statistics.FMSketchFromStorage(reader, table.PhysicalID, 0, histID)
				if err != nil {
					return errors.Trace(err)
				}
			}
			col = &statistics.Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *hg,
				Info:       colInfo,
				CMSketch:   cms,
				TopN:       topN,
				FMSketch:   fmSketch,
				ErrorRate:  errorRate,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				Flag:       flag,
				StatsVer:   statsVer,
			}
			// Column.Count is calculated by Column.TotalRowCount(). Hence we don't set Column.Count when initializing col.
			col.Count = int64(col.TotalRowCount())
			// When adding/modifying a column, we create its stats(all values are default values) without setting stats_ver.
			// So we need add colHist.Count > 0 here.
			if statsVer != statistics.Version0 || col.Count > 0 {
				col.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			break
		}
		if col.TotColSize != totColSize {
			newCol := *col
			newCol.TotColSize = totColSize
			col = &newCol
		}
		break
	}
	if col != nil {
		table.Columns[col.ID] = col
	} else {
		// If we didn't find a Column or Index in tableInfo, we won't load the histogram for it.
		// But don't worry, next lease the ddl will be updated, and we will load a same table for two times to
		// avoid error.
		logutil.BgLogger().Debug("we cannot find column in table info now. It may be deleted", zap.Int64("colID", histID), zap.String("table", tableInfo.Name.O))
	}
	return nil
}

// TableStatsFromStorage loads table stats info from storage.
func (h *Handle) TableStatsFromStorage(tableInfo *model.TableInfo, physicalID int64, loadAll bool, snapshot uint64) (_ *statistics.Table, err error) {
	reader, err := h.getGlobalStatsReader(snapshot)
	if err != nil {
		return nil, err
	}
	defer func() {
		err1 := h.releaseGlobalStatsReader(reader)
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	statsTbl, ok := h.statsCache.Load().(statsCache).Get(physicalID)
	if !ok {
		statsTbl = nil
	}
	statsTbl, err = statistics.TableStatsFromStorage(reader, tableInfo, physicalID, loadAll, h.Lease(), statsTbl)
	if err != nil {
		return nil, err
	}
	if reader.IsHistory() || statsTbl == nil {
		return statsTbl, nil
	}
	for histID, idx := range statsTbl.Indices {
		if statistics.IsAnalyzed(idx.Flag) {
			h.mu.rateMap.clear(physicalID, histID, true)
		}
	}
	for histID, col := range statsTbl.Columns {
		if statistics.IsAnalyzed(col.Flag) {
			h.mu.rateMap.clear(physicalID, histID, false)
		}
	}
	return statsTbl, nil
}

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func (h *Handle) StatsMetaCountAndModifyCount(tableID int64) (int64, int64, error) {
	reader, err := h.getGlobalStatsReader(0)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		err1 := h.releaseGlobalStatsReader(reader)
		if err1 != nil && err == nil {
			err = err1
		}
	}()
	rows, _, err := reader.Read("select count, modify_count from mysql.stats_meta where table_id = %?", tableID)
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}
	count := int64(rows[0].GetUint64(0))
	modifyCount := rows[0].GetInt64(1)
	return count, modifyCount, nil
}

func saveTopNToStorage(ctx context.Context, exec sqlexec.SQLExecutor, tableID int64, isIndex int, histID int64, topN *statistics.TopN) error {
	if topN == nil {
		return nil
	}
	for i := 0; i < len(topN.TopN); {
		end := i + batchInsertSize
		if end > len(topN.TopN) {
			end = len(topN.TopN)
		}
		sql := new(strings.Builder)
		sql.WriteString("insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values ")
		for j := i; j < end; j++ {
			topn := topN.TopN[j]
			val := sqlexec.MustEscapeSQL("(%?, %?, %?, %?, %?)", tableID, isIndex, histID, topn.Encoded, topn.Count)
			if j > i {
				val = "," + val
			}
			if j > i && sql.Len()+len(val) > maxInsertLength {
				end = j
				break
			}
			sql.WriteString(val)
		}
		i = end
		if _, err := exec.ExecuteInternal(ctx, sql.String()); err != nil {
			return err
		}
	}
	return nil
}

func saveBucketsToStorage(ctx context.Context, exec sqlexec.SQLExecutor, sc *stmtctx.StatementContext, tableID int64, isIndex int, hg *statistics.Histogram) (lastAnalyzePos []byte, err error) {
	if hg == nil {
		return
	}
	for i := 0; i < len(hg.Buckets); {
		end := i + batchInsertSize
		if end > len(hg.Buckets) {
			end = len(hg.Buckets)
		}
		sql := new(strings.Builder)
		sql.WriteString("insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values ")
		for j := i; j < end; j++ {
			bucket := hg.Buckets[j]
			count := bucket.Count
			if j > 0 {
				count -= hg.Buckets[j-1].Count
			}
			var upperBound types.Datum
			upperBound, err = hg.GetUpper(j).ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
			if err != nil {
				return
			}
			if j == len(hg.Buckets)-1 {
				lastAnalyzePos = upperBound.GetBytes()
			}
			var lowerBound types.Datum
			lowerBound, err = hg.GetLower(j).ConvertTo(sc, types.NewFieldType(mysql.TypeBlob))
			if err != nil {
				return
			}
			val := sqlexec.MustEscapeSQL("(%?, %?, %?, %?, %?, %?, %?, %?, %?)", tableID, isIndex, hg.ID, j, count, bucket.Repeat, lowerBound.GetBytes(), upperBound.GetBytes(), bucket.NDV)
			if j > i {
				val = "," + val
			}
			if j > i && sql.Len()+len(val) > maxInsertLength {
				end = j
				break
			}
			sql.WriteString(val)
		}
		i = end
		if _, err = exec.ExecuteInternal(ctx, sql.String()); err != nil {
			return
		}
	}
	return
}

// SaveTableStatsToStorage saves the stats of a table to storage.
func (h *Handle) SaveTableStatsToStorage(results *statistics.AnalyzeResults, analyzeSnapshot bool, source string) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return SaveTableStatsToStorage(h.mu.ctx, results, analyzeSnapshot, source)
}

// SaveTableStatsToStorage saves the stats of a table to storage.
func SaveTableStatsToStorage(sctx sessionctx.Context, results *statistics.AnalyzeResults, analyzeSnapshot bool, source string) (err error) {
	needDumpFMS := results.TableID.IsPartitionTable()
	tableID := results.TableID.GetStatisticsID()
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			if err1 := recordHistoricalStatsMeta(sctx, tableID, statsVer, source); err1 != nil {
				logutil.BgLogger().Error("record historical stats meta failed",
					zap.Int64("table-id", tableID),
					zap.Uint64("version", statsVer),
					zap.String("source", source),
					zap.Error(err1))
			}
		}
	}()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := sctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return err
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}
	version := txn.StartTS()
	// 1. Save mysql.stats_meta.
	var rs sqlexec.RecordSet
	// Lock this row to prevent writing of concurrent analyze.
	rs, err = exec.ExecuteInternal(ctx, "select snapshot, count, modify_count from mysql.stats_meta where table_id = %? for update", tableID)
	if err != nil {
		return err
	}
	var rows []chunk.Row
	rows, err = sqlexec.DrainRecordSet(ctx, rs, sctx.GetSessionVars().MaxChunkSize)
	if err != nil {
		return err
	}
	err = rs.Close()
	if err != nil {
		return err
	}
	var curCnt, curModifyCnt int64
	if len(rows) > 0 {
		snapshot := rows[0].GetUint64(0)
		// A newer version analyze result has been written, so skip this writing.
		if snapshot >= results.Snapshot && results.StatsVer == statistics.Version2 {
			return nil
		}
		curCnt = int64(rows[0].GetUint64(1))
		curModifyCnt = rows[0].GetInt64(2)
	}
	if len(rows) == 0 || results.StatsVer != statistics.Version2 {
		if _, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_meta (version, table_id, count, snapshot) values (%?, %?, %?, %?)", version, tableID, results.Count, results.Snapshot); err != nil {
			return err
		}
		statsVer = version
	} else {
		modifyCnt := curModifyCnt - results.BaseModifyCnt
		if modifyCnt < 0 {
			modifyCnt = 0
		}
		logutil.BgLogger().Info("[stats] incrementally update modifyCount",
			zap.Int64("tableID", tableID),
			zap.Int64("curModifyCnt", curModifyCnt),
			zap.Int64("results.BaseModifyCnt", results.BaseModifyCnt),
			zap.Int64("modifyCount", modifyCnt))
		var cnt int64
		if analyzeSnapshot {
			cnt = curCnt + results.Count - results.BaseCount
			if cnt < 0 {
				cnt = 0
			}
			logutil.BgLogger().Info("[stats] incrementally update count",
				zap.Int64("tableID", tableID),
				zap.Int64("curCnt", curCnt),
				zap.Int64("results.Count", results.Count),
				zap.Int64("results.BaseCount", results.BaseCount),
				zap.Int64("count", cnt))
		} else {
			cnt = results.Count
			if cnt < 0 {
				cnt = 0
			}
			logutil.BgLogger().Info("[stats] directly update count",
				zap.Int64("tableID", tableID),
				zap.Int64("results.Count", results.Count),
				zap.Int64("count", cnt))
		}
		if _, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version=%?, modify_count=%?, count=%?, snapshot=%? where table_id=%?", version, modifyCnt, cnt, results.Snapshot, tableID); err != nil {
			return err
		}
		statsVer = version
	}
	// 2. Save histograms.
	for _, result := range results.Ars {
		for i, hg := range result.Hist {
			// It's normal virtual column, skip it.
			if hg == nil {
				continue
			}
			var cms *statistics.CMSketch
			if results.StatsVer != statistics.Version2 {
				cms = result.Cms[i]
			}
			cmSketch, err := statistics.EncodeCMSketchWithoutTopN(cms)
			if err != nil {
				return err
			}
			fmSketch, err := statistics.EncodeFMSketch(result.Fms[i])
			if err != nil {
				return err
			}
			// Delete outdated data
			if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tableID, result.IsIndex, hg.ID); err != nil {
				return err
			}
			if err = saveTopNToStorage(ctx, exec, tableID, result.IsIndex, hg.ID, result.TopNs[i]); err != nil {
				return err
			}
			if _, err := exec.ExecuteInternal(ctx, "delete from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?", tableID, result.IsIndex, hg.ID); err != nil {
				return err
			}
			if fmSketch != nil && needDumpFMS {
				if _, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_fm_sketch (table_id, is_index, hist_id, value) values (%?, %?, %?, %?)", tableID, result.IsIndex, hg.ID, fmSketch); err != nil {
					return err
				}
			}
			if _, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, flag, correlation) values (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)",
				tableID, result.IsIndex, hg.ID, hg.NDV, version, hg.NullCount, cmSketch, hg.TotColSize, results.StatsVer, statistics.AnalyzeFlag, hg.Correlation); err != nil {
				return err
			}
			if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %?", tableID, result.IsIndex, hg.ID); err != nil {
				return err
			}
			sc := sctx.GetSessionVars().StmtCtx
			var lastAnalyzePos []byte
			lastAnalyzePos, err = saveBucketsToStorage(ctx, exec, sc, tableID, result.IsIndex, hg)
			if err != nil {
				return err
			}
			if len(lastAnalyzePos) > 0 {
				if _, err = exec.ExecuteInternal(ctx, "update mysql.stats_histograms set last_analyze_pos = %? where table_id = %? and is_index = %? and hist_id = %?", lastAnalyzePos, tableID, result.IsIndex, hg.ID); err != nil {
					return err
				}
			}
			if result.IsIndex == 0 {
				if _, err = exec.ExecuteInternal(ctx, "insert into mysql.column_stats_usage (table_id, column_id, last_analyzed_at) values(%?, %?, current_timestamp()) on duplicate key update last_analyzed_at = values(last_analyzed_at)", tableID, hg.ID); err != nil {
					return err
				}
			}
		}
	}
	// 3. Save extended statistics.
	extStats := results.ExtStats
	if extStats == nil || len(extStats.Stats) == 0 {
		return nil
	}
	var bytes []byte
	var statsStr string
	for name, item := range extStats.Stats {
		bytes, err = json.Marshal(item.ColIDs)
		if err != nil {
			return err
		}
		strColIDs := string(bytes)
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			statsStr = fmt.Sprintf("%f", item.ScalarVals)
		case ast.StatsTypeDependency:
			statsStr = item.StringVals
		}
		if _, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_extended values (%?, %?, %?, %?, %?, %?, %?)", name, item.Tp, tableID, strColIDs, statsStr, version, statistics.ExtendedStatsAnalyzed); err != nil {
			return err
		}
	}
	return
}

// SaveStatsToStorage saves the stats to storage.
// If count is negative, both count and modify count would not be used and not be written to the table. Unless, corresponding
// fields in the stats_meta table will be updated.
// TODO: refactor to reduce the number of parameters
func (h *Handle) SaveStatsToStorage(tableID int64, count, modifyCount int64, isIndex int, hg *statistics.Histogram,
	cms *statistics.CMSketch, topN *statistics.TopN, statsVersion int, isAnalyzed int64, updateAnalyzeTime bool, source string) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			h.recordHistoricalStatsMeta(tableID, statsVer, source)
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}

	version := txn.StartTS()
	// If the count is less than 0, then we do not want to update the modify count and count.
	if count >= 0 {
		_, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_meta (version, table_id, count, modify_count) values (%?, %?, %?, %?)", version, tableID, count, modifyCount)
	} else {
		_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %? where table_id = %?", version, tableID)
	}
	if err != nil {
		return err
	}
	statsVer = version
	cmSketch, err := statistics.EncodeCMSketchWithoutTopN(cms)
	if err != nil {
		return err
	}
	// Delete outdated data
	if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return err
	}
	if err = saveTopNToStorage(ctx, exec, tableID, isIndex, hg.ID, topN); err != nil {
		return err
	}
	if _, err := exec.ExecuteInternal(ctx, "delete from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return err
	}
	flag := 0
	if isAnalyzed == 1 {
		flag = statistics.AnalyzeFlag
	}
	if _, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, flag, correlation) values (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)",
		tableID, isIndex, hg.ID, hg.NDV, version, hg.NullCount, cmSketch, hg.TotColSize, statsVersion, flag, hg.Correlation); err != nil {
		return err
	}
	if _, err = exec.ExecuteInternal(ctx, "delete from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return err
	}
	sc := h.mu.ctx.GetSessionVars().StmtCtx
	var lastAnalyzePos []byte
	lastAnalyzePos, err = saveBucketsToStorage(ctx, exec, sc, tableID, isIndex, hg)
	if err != nil {
		return err
	}
	if isAnalyzed == 1 && len(lastAnalyzePos) > 0 {
		if _, err = exec.ExecuteInternal(ctx, "update mysql.stats_histograms set last_analyze_pos = %? where table_id = %? and is_index = %? and hist_id = %?", lastAnalyzePos, tableID, isIndex, hg.ID); err != nil {
			return err
		}
	}
	if updateAnalyzeTime && isIndex == 0 {
		if _, err = exec.ExecuteInternal(ctx, "insert into mysql.column_stats_usage (table_id, column_id, last_analyzed_at) values(%?, %?, current_timestamp()) on duplicate key update last_analyzed_at = current_timestamp()", tableID, hg.ID); err != nil {
			return err
		}
	}
	return
}

// SaveMetaToStorage will save stats_meta to storage.
func (h *Handle) SaveMetaToStorage(tableID, count, modifyCount int64, source string) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			h.recordHistoricalStatsMeta(tableID, statsVer, source)
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	_, err = exec.ExecuteInternal(ctx, "replace into mysql.stats_meta (version, table_id, count, modify_count) values (%?, %?, %?, %?)", version, tableID, count, modifyCount)
	statsVer = version
	return err
}

func (h *Handle) statsMetaByTableIDFromStorage(tableID int64, snapshot uint64) (version uint64, modifyCount, count int64, err error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	var rows []chunk.Row
	if snapshot == 0 {
		rows, _, err = h.execRestrictedSQL(ctx, "SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version", tableID)
	} else {
		rows, _, err = h.execRestrictedSQLWithSnapshot(ctx, "SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version", snapshot, tableID)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	if err != nil || len(rows) == 0 {
		return
	}
	version = rows[0].GetUint64(0)
	modifyCount = rows[0].GetInt64(1)
	count = rows[0].GetInt64(2)
	return
}

func (h *Handle) getGlobalStatsReader(snapshot uint64) (reader *statistics.StatsReader, err error) {
	h.mu.Lock()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("getGlobalStatsReader panic %v", r)
		}
		if err != nil {
			h.mu.Unlock()
		}
	}()
	return statistics.GetStatsReader(snapshot, h.mu.ctx.(sqlexec.RestrictedSQLExecutor))
}

func (h *Handle) releaseGlobalStatsReader(reader *statistics.StatsReader) error {
	defer h.mu.Unlock()
	return reader.Close()
}

// InsertExtendedStats inserts a record into mysql.stats_extended and update version in mysql.stats_meta.
func (h *Handle) InsertExtendedStats(statsName string, colIDs []int64, tp int, tableID int64, ifNotExists bool) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			h.recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
		}
	}()
	slices.Sort(colIDs)
	bytes, err := json.Marshal(colIDs)
	if err != nil {
		return errors.Trace(err)
	}
	strColIDs := string(bytes)
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	// No need to use `exec.ExecuteInternal` since we have acquired the lock.
	rows, _, err := h.execRestrictedSQL(ctx, "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)", tableID, statistics.ExtendedStatsInited, statistics.ExtendedStatsAnalyzed)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		currStatsName := row.GetString(0)
		currTp := row.GetInt64(1)
		currStrColIDs := row.GetString(2)
		if currStatsName == statsName {
			if ifNotExists {
				return nil
			}
			return errors.Errorf("extended statistics '%s' for the specified table already exists", statsName)
		}
		if tp == int(currTp) && currStrColIDs == strColIDs {
			return errors.Errorf("extended statistics '%s' with same type on same columns already exists", statsName)
		}
	}
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	// Bump version in `mysql.stats_meta` to trigger stats cache refresh.
	if _, err = exec.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
		return err
	}
	statsVer = version
	// Remove the existing 'deleted' records.
	if _, err = exec.ExecuteInternal(ctx, "DELETE FROM mysql.stats_extended WHERE name = %? and table_id = %?", statsName, tableID); err != nil {
		return err
	}
	// Remove the cache item, which is necessary for cases like a cluster with 3 tidb instances, e.g, a, b and c.
	// If tidb-a executes `alter table drop stats_extended` to mark the record as 'deleted', and before this operation
	// is synchronized to other tidb instances, tidb-b executes `alter table add stats_extended`, which would delete
	// the record from the table, tidb-b should delete the cached item synchronously. While for tidb-c, it has to wait for
	// next `Update()` to remove the cached item then.
	h.removeExtendedStatsItem(tableID, statsName)
	const sql = "INSERT INTO mysql.stats_extended(name, type, table_id, column_ids, version, status) VALUES (%?, %?, %?, %?, %?, %?)"
	if _, err = exec.ExecuteInternal(ctx, sql, statsName, tp, tableID, strColIDs, version, statistics.ExtendedStatsInited); err != nil {
		return err
	}
	return
}

// MarkExtendedStatsDeleted update the status of mysql.stats_extended to be `deleted` and the version of mysql.stats_meta.
func (h *Handle) MarkExtendedStatsDeleted(statsName string, tableID int64, ifExists bool) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			h.recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
		}
	}()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := h.execRestrictedSQL(ctx, "SELECT name FROM mysql.stats_extended WHERE name = %? and table_id = %? and status in (%?, %?)", statsName, tableID, statistics.ExtendedStatsInited, statistics.ExtendedStatsAnalyzed)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		if ifExists {
			return nil
		}
		return fmt.Errorf("extended statistics '%s' for the specified table does not exist", statsName)
	}
	if len(rows) > 1 {
		logutil.BgLogger().Warn("unexpected duplicate extended stats records found", zap.String("name", statsName), zap.Int64("table_id", tableID))
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err1 := finishTransaction(ctx, exec, err)
		if err == nil && err1 == nil {
			h.removeExtendedStatsItem(tableID, statsName)
		}
		err = err1
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	if _, err = exec.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
		return err
	}
	statsVer = version
	if _, err = exec.ExecuteInternal(ctx, "UPDATE mysql.stats_extended SET version = %?, status = %? WHERE name = %? and table_id = %?", version, statistics.ExtendedStatsDeleted, statsName, tableID); err != nil {
		return err
	}
	return nil
}

const updateStatsCacheRetryCnt = 5

func (h *Handle) removeExtendedStatsItem(tableID int64, statsName string) {
	for retry := updateStatsCacheRetryCnt; retry > 0; retry-- {
		oldCache := h.statsCache.Load().(statsCache)
		tbl, ok := oldCache.Get(tableID)
		if !ok || tbl.ExtendedStats == nil || len(tbl.ExtendedStats.Stats) == 0 {
			return
		}
		newTbl := tbl.Copy()
		delete(newTbl.ExtendedStats.Stats, statsName)
		if h.updateStatsCache(oldCache.update([]*statistics.Table{newTbl}, nil, oldCache.version)) {
			return
		}
		if retry == 1 {
			logutil.BgLogger().Info("remove extended stats cache failed", zap.String("stats_name", statsName), zap.Int64("table_id", tableID))
		} else {
			logutil.BgLogger().Info("remove extended stats cache failed, retrying", zap.String("stats_name", statsName), zap.Int64("table_id", tableID))
		}
	}
}

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from mysql.stats_extended.
func (h *Handle) ReloadExtendedStatistics() error {
	reader, err := h.getGlobalStatsReader(0)
	if err != nil {
		return err
	}
	defer func() {
		err1 := h.releaseGlobalStatsReader(reader)
		if err1 != nil && err == nil {
			err = err1
		}
	}()
	for retry := updateStatsCacheRetryCnt; retry > 0; retry-- {
		oldCache := h.statsCache.Load().(statsCache)
		tables := make([]*statistics.Table, 0, oldCache.Len())
		for physicalID, tbl := range oldCache.Map() {
			t, err := statistics.ExtendedStatsFromStorage(reader, tbl.Copy(), physicalID, true)
			if err != nil {
				return err
			}
			tables = append(tables, t)
		}
		if h.updateStatsCache(oldCache.update(tables, nil, oldCache.version)) {
			return nil
		}
	}
	return fmt.Errorf("update stats cache failed for %d attempts", updateStatsCacheRetryCnt)
}

// BuildExtendedStats build extended stats for column groups if needed based on the column samples.
func (h *Handle) BuildExtendedStats(tableID int64, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) (*statistics.ExtendedStatsColl, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	const sql = "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)"
	rows, _, err := h.execRestrictedSQL(ctx, sql, tableID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	statsColl := statistics.NewExtendedStatsColl()
	for _, row := range rows {
		name := row.GetString(0)
		item := &statistics.ExtendedStatsItem{Tp: uint8(row.GetInt64(1))}
		colIDs := row.GetString(2)
		err := json.Unmarshal([]byte(colIDs), &item.ColIDs)
		if err != nil {
			logutil.BgLogger().Error("invalid column_ids in mysql.stats_extended, skip collecting extended stats for this row", zap.String("column_ids", colIDs), zap.Error(err))
			continue
		}
		item = h.fillExtendedStatsItemVals(item, cols, collectors)
		if item != nil {
			statsColl.Stats[name] = item
		}
	}
	if len(statsColl.Stats) == 0 {
		return nil, nil
	}
	return statsColl, nil
}

func (h *Handle) fillExtendedStatsItemVals(item *statistics.ExtendedStatsItem, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) *statistics.ExtendedStatsItem {
	switch item.Tp {
	case ast.StatsTypeCardinality, ast.StatsTypeDependency:
		return nil
	case ast.StatsTypeCorrelation:
		return h.fillExtStatsCorrVals(item, cols, collectors)
	}
	return nil
}

func (h *Handle) fillExtStatsCorrVals(item *statistics.ExtendedStatsItem, cols []*model.ColumnInfo, collectors []*statistics.SampleCollector) *statistics.ExtendedStatsItem {
	colOffsets := make([]int, 0, 2)
	for _, id := range item.ColIDs {
		for i, col := range cols {
			if col.ID == id {
				colOffsets = append(colOffsets, i)
				break
			}
		}
	}
	if len(colOffsets) != 2 {
		return nil
	}
	// samplesX and samplesY are in order of handle, i.e, their SampleItem.Ordinals are in order.
	samplesX := collectors[colOffsets[0]].Samples
	// We would modify Ordinal of samplesY, so we make a deep copy.
	samplesY := statistics.CopySampleItems(collectors[colOffsets[1]].Samples)
	sampleNum := mathutil.Min(len(samplesX), len(samplesY))
	if sampleNum == 1 {
		item.ScalarVals = 1
		return item
	}
	if sampleNum <= 0 {
		item.ScalarVals = 0
		return item
	}
	h.mu.Lock()
	sc := h.mu.ctx.GetSessionVars().StmtCtx
	h.mu.Unlock()
	var err error
	samplesX, err = statistics.SortSampleItems(sc, samplesX)
	if err != nil {
		return nil
	}
	samplesYInXOrder := make([]*statistics.SampleItem, 0, sampleNum)
	for i, itemX := range samplesX {
		if itemX.Ordinal >= len(samplesY) {
			continue
		}
		itemY := samplesY[itemX.Ordinal]
		itemY.Ordinal = i
		samplesYInXOrder = append(samplesYInXOrder, itemY)
	}
	samplesYInYOrder, err := statistics.SortSampleItems(sc, samplesYInXOrder)
	if err != nil {
		return nil
	}
	var corrXYSum float64
	for i := 1; i < len(samplesYInYOrder); i++ {
		corrXYSum += float64(i) * float64(samplesYInYOrder[i].Ordinal)
	}
	// X means the ordinal of the item in original sequence, Y means the oridnal of the item in the
	// sorted sequence, we know that X and Y value sets are both:
	// 0, 1, ..., sampleNum-1
	// we can simply compute sum(X) = sum(Y) =
	//    (sampleNum-1)*sampleNum / 2
	// and sum(X^2) = sum(Y^2) =
	//    (sampleNum-1)*sampleNum*(2*sampleNum-1) / 6
	// We use "Pearson correlation coefficient" to compute the order correlation of columns,
	// the formula is based on https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
	// Note that (itemsCount*corrX2Sum - corrXSum*corrXSum) would never be zero when sampleNum is larger than 1.
	itemsCount := float64(sampleNum)
	corrXSum := (itemsCount - 1) * itemsCount / 2.0
	corrX2Sum := (itemsCount - 1) * itemsCount * (2*itemsCount - 1) / 6.0
	item.ScalarVals = (itemsCount*corrXYSum - corrXSum*corrXSum) / (itemsCount*corrX2Sum - corrXSum*corrXSum)
	return item
}

// SaveExtendedStatsToStorage writes extended stats of a table into mysql.stats_extended.
func (h *Handle) SaveExtendedStatsToStorage(tableID int64, extStats *statistics.ExtendedStatsColl, isLoad bool) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			h.recordHistoricalStatsMeta(tableID, statsVer, StatsMetaHistorySourceExtendedStats)
		}
	}()
	if extStats == nil || len(extStats.Stats) == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	for name, item := range extStats.Stats {
		bytes, err := json.Marshal(item.ColIDs)
		if err != nil {
			return errors.Trace(err)
		}
		strColIDs := string(bytes)
		var statsStr string
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			statsStr = fmt.Sprintf("%f", item.ScalarVals)
		case ast.StatsTypeDependency:
			statsStr = item.StringVals
		}
		// If isLoad is true, it's INSERT; otherwise, it's UPDATE.
		if _, err := exec.ExecuteInternal(ctx, "replace into mysql.stats_extended values (%?, %?, %?, %?, %?, %?, %?)", name, item.Tp, tableID, strColIDs, statsStr, version, statistics.ExtendedStatsAnalyzed); err != nil {
			return err
		}
	}
	if !isLoad {
		if _, err := exec.ExecuteInternal(ctx, "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?", version, tableID); err != nil {
			return err
		}
		statsVer = version
	}
	return nil
}

// CurrentPruneMode indicates whether tbl support runtime prune for table and first partition id.
func (h *Handle) CurrentPruneMode() variable.PartitionPruneMode {
	return variable.PartitionPruneMode(h.mu.ctx.GetSessionVars().PartitionPruneMode.Load())
}

// RefreshVars uses to pull PartitionPruneMethod vars from kv storage.
func (h *Handle) RefreshVars() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.mu.ctx.RefreshVars(context.Background())
}

// CheckAnalyzeVersion checks whether all the statistics versions of this table's columns and indexes are the same.
func (h *Handle) CheckAnalyzeVersion(tblInfo *model.TableInfo, physicalIDs []int64, version *int) bool {
	// We simply choose one physical id to get its stats.
	var tbl *statistics.Table
	for _, pid := range physicalIDs {
		tbl = h.GetPartitionStats(tblInfo, pid)
		if !tbl.Pseudo {
			break
		}
	}
	if tbl == nil || tbl.Pseudo {
		return true
	}
	return statistics.CheckAnalyzeVerOnTable(tbl, version)
}

type colStatsTimeInfo struct {
	LastUsedAt     *types.Time
	LastAnalyzedAt *types.Time
}

// getDisableColumnTrackingTime reads the value of tidb_disable_column_tracking_time from mysql.tidb if it exists.
func (h *Handle) getDisableColumnTrackingTime() (*time.Time, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, fields, err := h.execRestrictedSQL(ctx, "SELECT variable_value FROM %n.%n WHERE variable_name = %?", mysql.SystemDB, mysql.TiDBTable, variable.TiDBDisableColumnTrackingTime)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	// The string represents the UTC time when tidb_enable_column_tracking is set to 0.
	value, err := d.ToString()
	if err != nil {
		return nil, err
	}
	t, err := time.Parse(types.UTCTimeFormat, value)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// LoadColumnStatsUsage loads column stats usage information from disk.
func (h *Handle) LoadColumnStatsUsage(loc *time.Location) (map[model.TableItemID]colStatsTimeInfo, error) {
	disableTime, err := h.getDisableColumnTrackingTime()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	// Since we use another session from session pool to read mysql.column_stats_usage, which may have different @@time_zone, so we do time zone conversion here.
	rows, _, err := h.execRestrictedSQL(ctx, "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage")
	if err != nil {
		return nil, errors.Trace(err)
	}
	colStatsMap := make(map[model.TableItemID]colStatsTimeInfo, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		tblColID := model.TableItemID{TableID: row.GetInt64(0), ID: row.GetInt64(1), IsIndex: false}
		var statsUsage colStatsTimeInfo
		if !row.IsNull(2) {
			gt, err := row.GetTime(2).GoTime(time.UTC)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// If `last_used_at` is before the time when `set global enable_column_tracking = 0`, we should ignore it because
			// `set global enable_column_tracking = 0` indicates all the predicate columns collected before.
			if disableTime == nil || gt.After(*disableTime) {
				t := types.NewTime(types.FromGoTime(gt.In(loc)), mysql.TypeTimestamp, types.DefaultFsp)
				statsUsage.LastUsedAt = &t
			}
		}
		if !row.IsNull(3) {
			gt, err := row.GetTime(3).GoTime(time.UTC)
			if err != nil {
				return nil, errors.Trace(err)
			}
			t := types.NewTime(types.FromGoTime(gt.In(loc)), mysql.TypeTimestamp, types.DefaultFsp)
			statsUsage.LastAnalyzedAt = &t
		}
		colStatsMap[tblColID] = statsUsage
	}
	return colStatsMap, nil
}

// CollectColumnsInExtendedStats returns IDs of the columns involved in extended stats.
func (h *Handle) CollectColumnsInExtendedStats(tableID int64) ([]int64, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	const sql = "SELECT name, type, column_ids FROM mysql.stats_extended WHERE table_id = %? and status in (%?, %?)"
	rows, _, err := h.execRestrictedSQL(ctx, sql, tableID, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsInited)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	columnIDs := make([]int64, 0, len(rows)*2)
	for _, row := range rows {
		twoIDs := make([]int64, 0, 2)
		data := row.GetString(2)
		err := json.Unmarshal([]byte(data), &twoIDs)
		if err != nil {
			logutil.BgLogger().Error("invalid column_ids in mysql.stats_extended, skip collecting extended stats for this row", zap.String("column_ids", data), zap.Error(err))
			continue
		}
		columnIDs = append(columnIDs, twoIDs...)
	}
	return columnIDs, nil
}

// GetPredicateColumns returns IDs of predicate columns, which are the columns whose stats are used(needed) when generating query plans.
func (h *Handle) GetPredicateColumns(tableID int64) ([]int64, error) {
	disableTime, err := h.getDisableColumnTrackingTime()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := h.execRestrictedSQL(ctx, "SELECT column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %? AND last_used_at IS NOT NULL", tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	columnIDs := make([]int64, 0, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		colID := row.GetInt64(0)
		gt, err := row.GetTime(1).GoTime(time.UTC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// If `last_used_at` is before the time when `set global enable_column_tracking = 0`, we don't regard the column as predicate column because
		// `set global enable_column_tracking = 0` indicates all the predicate columns collected before.
		if disableTime == nil || gt.After(*disableTime) {
			columnIDs = append(columnIDs, colID)
		}
	}
	return columnIDs, nil
}

// Max column size is 6MB. Refer https://docs.pingcap.com/tidb/dev/tidb-limitations/#limitation-on-a-single-column
const maxColumnSize = 6 << 20

// RecordHistoricalStatsToStorage records the given table's stats data to mysql.stats_history
func (h *Handle) RecordHistoricalStatsToStorage(dbName string, tableInfo *model.TableInfo, physicalID int64, isPartition bool) (uint64, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	var js *JSONTable
	var err error
	if isPartition {
		js, err = h.tableStatsToJSON(dbName, tableInfo, physicalID, 0)
	} else {
		js, err = h.DumpStatsToJSON(dbName, tableInfo, nil, true)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	version := uint64(0)
	if len(js.Partitions) == 0 {
		version = js.Version
	} else {
		for _, p := range js.Partitions {
			version = p.Version
			if version != 0 {
				break
			}
		}
	}
	blocks, err := JSONTableToBlocks(js, maxColumnSize)
	if err != nil {
		return version, errors.Trace(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return version, errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	ts := time.Now().Format("2006-01-02 15:04:05.999999")

	const sql = "INSERT INTO mysql.stats_history(table_id, stats_data, seq_no, version, create_time) VALUES (%?, %?, %?, %?, %?)"
	for i := 0; i < len(blocks); i++ {
		if _, err := exec.ExecuteInternal(ctx, sql, physicalID, blocks[i], i, version, ts); err != nil {
			return version, errors.Trace(err)
		}
	}
	return version, nil
}

func checkHistoricalStatsEnable(sctx sessionctx.Context) (enable bool, err error) {
	val, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableHistoricalStats)
	if err != nil {
		return false, errors.Trace(err)
	}
	return variable.TiDBOptOn(val), nil
}

// CheckHistoricalStatsEnable is used to check whether TiDBEnableHistoricalStats is enabled.
func (h *Handle) CheckHistoricalStatsEnable() (enable bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return checkHistoricalStatsEnable(h.mu.ctx)
}

// InsertAnalyzeJob inserts analyze job into mysql.analyze_jobs and gets job ID for further updating job.
func (h *Handle) InsertAnalyzeJob(job *statistics.AnalyzeJob, instance string, procID uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.RestrictedSQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	jobInfo := job.JobInfo
	const textMaxLength = 65535
	if len(jobInfo) > textMaxLength {
		jobInfo = jobInfo[:textMaxLength]
	}
	const insertJob = "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, state, instance, process_id) VALUES (%?, %?, %?, %?, %?, %?, %?)"
	_, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, insertJob, job.DBName, job.TableName, job.PartitionName, jobInfo, statistics.AnalyzePending, instance, procID)
	if err != nil {
		return err
	}
	const getJobID = "SELECT LAST_INSERT_ID()"
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, getJobID)
	if err != nil {
		return err
	}
	job.ID = new(uint64)
	*job.ID = rows[0].GetUint64(0)
	return nil
}

// DeleteAnalyzeJobs deletes the analyze jobs whose update time is earlier than updateTime.
func (h *Handle) DeleteAnalyzeJobs(updateTime time.Time) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	_, _, err := h.execRestrictedSQL(ctx, "DELETE FROM mysql.analyze_jobs WHERE update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)", updateTime.UTC().Format(types.TimeFormat))
	return err
}

type tableStatsOption struct {
	byQuery bool
}

// TableStatsOpt used to edit getTableStatsOption
type TableStatsOpt func(*tableStatsOption)

// WithTableStatsByQuery indicates user needed
func WithTableStatsByQuery() TableStatsOpt {
	return func(option *tableStatsOption) {
		option.byQuery = true
	}
}

// SetStatsCacheCapacity sets capacity
func (h *Handle) SetStatsCacheCapacity(c int64) {
	if h == nil {
		return
	}
	v := h.statsCache.Load()
	if v == nil {
		return
	}
	sc := v.(statsCache)
	sc.SetCapacity(c)
}

// GetStatsCacheFrontTable gets front table in statsCacheInner implementation
// only used for test
func (h *Handle) GetStatsCacheFrontTable() int64 {
	if h == nil {
		return 0
	}
	v := h.statsCache.Load()
	if v == nil {
		return 0
	}
	sc := v.(statsCache)
	return sc.Front()
}

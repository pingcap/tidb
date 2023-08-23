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
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"go.uber.org/zap"
)

type tableDeltaMap map[int64]variable.TableDelta

func (m tableDeltaMap) update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	if item.ColSize == nil {
		item.ColSize = make(map[int64]int64)
	}
	if colSize != nil {
		for key, val := range *colSize {
			item.ColSize[key] += val
		}
	}
	m[id] = item
}

func (m tableDeltaMap) merge(deltaMap tableDeltaMap) {
	for id, item := range deltaMap {
		m.update(id, item.Delta, item.Count, &item.ColSize)
	}
}

// colStatsUsageMap maps (tableID, columnID) to the last time when the column stats are used(needed).
type colStatsUsageMap map[model.TableItemID]time.Time

func (m colStatsUsageMap) merge(other colStatsUsageMap) {
	for id, t := range other {
		if mt, ok := m[id]; !ok || mt.Before(t) {
			m[id] = t
		}
	}
}

func merge(s *SessionStatsCollector, deltaMap tableDeltaMap, colMap colStatsUsageMap) {
	deltaMap.merge(s.mapper)
	s.mapper = make(tableDeltaMap)
	colMap.merge(s.colMap)
	s.colMap = make(colStatsUsageMap)
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	mapper tableDeltaMap
	colMap colStatsUsageMap
	next   *SessionStatsCollector
	sync.Mutex

	// deleted is set to true when a session is closed. Every time we sweep the list, we will remove the useless collector.
	deleted bool
}

// NewSessionStatsCollector initializes a new SessionStatsCollector.
func NewSessionStatsCollector() *SessionStatsCollector {
	return &SessionStatsCollector{
		mapper: make(tableDeltaMap),
		colMap: make(colStatsUsageMap),
	}
}

// Delete only sets the deleted flag true, it will be deleted from list when DumpStatsDeltaToKV is called.
func (s *SessionStatsCollector) Delete() {
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// Update will updates the delta and count for one table id.
func (s *SessionStatsCollector) Update(id int64, delta int64, count int64, colSize *map[int64]int64) {
	s.Lock()
	defer s.Unlock()
	s.mapper.update(id, delta, count, colSize)
}

// ClearForTest clears the mapper for test.
func (s *SessionStatsCollector) ClearForTest() {
	s.Lock()
	defer s.Unlock()
	s.mapper = make(tableDeltaMap)
	s.colMap = make(colStatsUsageMap)
	s.next = nil
	s.deleted = false
}

// UpdateColStatsUsage updates the last time when the column stats are used(needed).
func (s *SessionStatsCollector) UpdateColStatsUsage(colMap colStatsUsageMap) {
	s.Lock()
	defer s.Unlock()
	s.colMap.merge(colMap)
}

// NewSessionStatsCollector allocates a stats collector for a session.
func (h *Handle) NewSessionStatsCollector() *SessionStatsCollector {
	h.listHead.Lock()
	defer h.listHead.Unlock()
	newCollector := &SessionStatsCollector{
		mapper: make(tableDeltaMap),
		next:   h.listHead.next,
		colMap: make(colStatsUsageMap),
	}
	h.listHead.next = newCollector
	return newCollector
}

// IndexUsageInformation is the data struct to store index usage information.
type IndexUsageInformation struct {
	LastUsedAt   string
	QueryCount   int64
	RowsSelected int64
}

// GlobalIndexID is the key type for indexUsageMap.
type GlobalIndexID struct {
	TableID int64
	IndexID int64
}

type indexUsageMap map[GlobalIndexID]IndexUsageInformation

// SessionIndexUsageCollector is a list item that holds the index usage mapper. If you want to write or read mapper, you must lock it.
type SessionIndexUsageCollector struct {
	mapper indexUsageMap
	next   *SessionIndexUsageCollector
	sync.Mutex

	deleted bool
}

func (m indexUsageMap) updateByKey(id GlobalIndexID, value *IndexUsageInformation) {
	item := m[id]
	item.QueryCount += value.QueryCount
	item.RowsSelected += value.RowsSelected
	if item.LastUsedAt < value.LastUsedAt {
		item.LastUsedAt = value.LastUsedAt
	}
	m[id] = item
}

func (m indexUsageMap) update(tableID int64, indexID int64, value *IndexUsageInformation) {
	id := GlobalIndexID{TableID: tableID, IndexID: indexID}
	m.updateByKey(id, value)
}

func (m indexUsageMap) merge(destMap indexUsageMap) {
	for id := range destMap {
		item := destMap[id]
		m.updateByKey(id, &item)
	}
}

// Update updates the mapper in SessionIndexUsageCollector.
func (s *SessionIndexUsageCollector) Update(tableID int64, indexID int64, value *IndexUsageInformation) {
	value.LastUsedAt = time.Now().Format(types.TimeFSPFormat)
	s.Lock()
	defer s.Unlock()
	s.mapper.update(tableID, indexID, value)
}

// Delete will set s.deleted to true which means it can be deleted from linked list.
func (s *SessionIndexUsageCollector) Delete() {
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// NewSessionIndexUsageCollector will add a new SessionIndexUsageCollector into linked list headed by idxUsageListHead.
// idxUsageListHead always points to an empty SessionIndexUsageCollector as a sentinel node. So we let idxUsageListHead.next
// points to new item. It's helpful to sweepIdxUsageList.
func (h *Handle) NewSessionIndexUsageCollector() *SessionIndexUsageCollector {
	h.idxUsageListHead.Lock()
	defer h.idxUsageListHead.Unlock()
	newCollector := &SessionIndexUsageCollector{
		mapper: make(indexUsageMap),
		next:   h.idxUsageListHead.next,
	}
	h.idxUsageListHead.next = newCollector
	return newCollector
}

// sweepIdxUsageList will loop over the list, merge each session's local index usage information into handle
// and remove closed session's collector.
// For convenience, we keep idxUsageListHead always points to sentinel node. So that we don't need to consider corner case.
func (h *Handle) sweepIdxUsageList() indexUsageMap {
	prev := h.idxUsageListHead
	prev.Lock()
	mapper := make(indexUsageMap)
	for curr := prev.next; curr != nil; curr = curr.next {
		curr.Lock()
		mapper.merge(curr.mapper)
		if curr.deleted {
			prev.next = curr.next
			curr.Unlock()
		} else {
			prev.Unlock()
			curr.mapper = make(indexUsageMap)
			prev = curr
		}
	}
	prev.Unlock()
	return mapper
}

// batchInsertSize is the batch size used by internal SQL to insert values to some system table.
const batchInsertSize = 10

// maxInsertLength is the length limit for internal insert SQL.
const maxInsertLength = 1024 * 1024

// DumpIndexUsageToKV will dump in-memory index usage information to KV.
func (h *Handle) DumpIndexUsageToKV() error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	mapper := h.sweepIdxUsageList()
	type FullIndexUsageInformation struct {
		information IndexUsageInformation
		id          GlobalIndexID
	}
	indexInformationSlice := make([]FullIndexUsageInformation, 0, len(mapper))
	for id, value := range mapper {
		indexInformationSlice = append(indexInformationSlice, FullIndexUsageInformation{id: id, information: value})
	}
	for i := 0; i < len(mapper); i += batchInsertSize {
		end := i + batchInsertSize
		if end > len(mapper) {
			end = len(mapper)
		}
		sql := new(strings.Builder)
		sqlexec.MustFormatSQL(sql, "insert into mysql.SCHEMA_INDEX_USAGE (table_id,index_id,query_count,rows_selected,last_used_at) values")
		for j := i; j < end; j++ {
			index := indexInformationSlice[j]
			sqlexec.MustFormatSQL(sql, "(%?, %?, %?, %?, %?)", index.id.TableID, index.id.IndexID,
				index.information.QueryCount, index.information.RowsSelected, index.information.LastUsedAt)
			if j < end-1 {
				sqlexec.MustFormatSQL(sql, ",")
			}
		}
		sqlexec.MustFormatSQL(sql, "on duplicate key update query_count=query_count+values(query_count),rows_selected=rows_selected+values(rows_selected),last_used_at=greatest(last_used_at, values(last_used_at))")
		if _, _, err := h.execRestrictedSQL(ctx, sql.String()); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// GCIndexUsage will delete the usage information of those indexes that do not exist.
func (h *Handle) GCIndexUsage() error {
	// For performance and implementation reasons, mysql.schema_index_usage doesn't handle DDL.
	// We periodically delete the usage information of non-existent indexes through information_schema.tidb_indexes.
	// This sql will delete the usage information of those indexes that not in information_schema.tidb_indexes.
	sql := `delete from mysql.SCHEMA_INDEX_USAGE as stats where stats.index_id not in (select idx.index_id from information_schema.tidb_indexes as idx)`
	_, _, err := h.execRestrictedSQL(context.Background(), sql)
	return err
}

var (
	// DumpStatsDeltaRatio is the lower bound of `Modify Count / Table Count` for stats delta to be dumped.
	DumpStatsDeltaRatio = 1 / 10000.0
	// dumpStatsMaxDuration is the max duration since last update.
	dumpStatsMaxDuration = time.Hour
)

// needDumpStatsDelta checks whether to dump stats delta.
// 1. If the table doesn't exist or is a mem table or system table, then return false.
// 2. If the mode is DumpAll, then return true.
// 3. If the stats delta haven't been dumped in the past hour, then return true.
// 4. If the table stats is pseudo or empty or `Modify Count / Table Count` exceeds the threshold.
func (h *Handle) needDumpStatsDelta(is infoschema.InfoSchema, mode dumpMode, id int64, item variable.TableDelta, currentTime time.Time) bool {
	tbl, ok := h.getTableByPhysicalID(is, id)
	if !ok {
		return false
	}
	dbInfo, ok := is.SchemaByTable(tbl.Meta())
	if !ok {
		return false
	}
	if util.IsMemOrSysDB(dbInfo.Name.L) {
		return false
	}
	if mode == DumpAll {
		return true
	}
	if item.InitTime.IsZero() {
		item.InitTime = currentTime
	}
	if currentTime.Sub(item.InitTime) > dumpStatsMaxDuration {
		// Dump the stats to kv at least once an hour.
		return true
	}
	statsTbl := h.GetPartitionStats(tbl.Meta(), id)
	if statsTbl.Pseudo || statsTbl.RealtimeCount == 0 || float64(item.Count)/float64(statsTbl.RealtimeCount) > DumpStatsDeltaRatio {
		// Dump the stats when there are many modifications.
		return true
	}
	return false
}

type dumpMode bool

const (
	// DumpAll indicates dump all the delta info in to kv.
	DumpAll dumpMode = true
	// DumpDelta indicates dump part of the delta info in to kv.
	DumpDelta dumpMode = false
)

// sweepList will loop over the list, merge each session's local stats into handle
// and remove closed session's collector.
func (h *Handle) sweepList() {
	deltaMap := make(tableDeltaMap)
	colMap := make(colStatsUsageMap)
	prev := h.listHead
	prev.Lock()
	for curr := prev.next; curr != nil; curr = curr.next {
		curr.Lock()
		// Merge the session stats into deltaMap respectively.
		merge(curr, deltaMap, colMap)
		if curr.deleted {
			prev.next = curr.next
			// Since the session is already closed, we can safely unlock it here.
			curr.Unlock()
		} else {
			// Unlock the previous lock, so we only holds at most two session's lock at the same time.
			prev.Unlock()
			prev = curr
		}
	}
	prev.Unlock()
	h.globalMap.Lock()
	h.globalMap.data.merge(deltaMap)
	h.globalMap.Unlock()
	h.colMap.Lock()
	h.colMap.data.merge(colMap)
	h.colMap.Unlock()
}

// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
// If the mode is `DumpDelta`, it will only dump that delta info that `Modify Count / Table Count` greater than a ratio.
func (h *Handle) DumpStatsDeltaToKV(mode dumpMode) error {
	h.sweepList()
	h.globalMap.Lock()
	deltaMap := h.globalMap.data
	h.globalMap.data = make(tableDeltaMap)
	h.globalMap.Unlock()
	defer func() {
		h.globalMap.Lock()
		deltaMap.merge(h.globalMap.data)
		h.globalMap.data = deltaMap
		h.globalMap.Unlock()
	}()
	// TODO: pass in do.InfoSchema() to DumpStatsDeltaToKV.
	is := func() infoschema.InfoSchema {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.mu.ctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	}()
	currentTime := time.Now()
	for id, item := range deltaMap {
		if !h.needDumpStatsDelta(is, mode, id, item, currentTime) {
			continue
		}
		updated, err := h.dumpTableStatCountToKV(id, item)
		if err != nil {
			return errors.Trace(err)
		}
		if updated {
			deltaMap.update(id, -item.Delta, -item.Count, nil)
		}
		if err = h.dumpTableStatColSizeToKV(id, item); err != nil {
			delete(deltaMap, id)
			return errors.Trace(err)
		}
		if updated {
			delete(deltaMap, id)
		} else {
			m := deltaMap[id]
			m.ColSize = nil
			deltaMap[id] = m
		}
	}
	return nil
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
func (h *Handle) dumpTableStatCountToKV(id int64, delta variable.TableDelta) (updated bool, err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			h.recordHistoricalStatsMeta(id, statsVer, StatsMetaHistorySourceFlushStats)
		}
	}()
	if delta.Count == 0 {
		return true, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return false, errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()

	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return false, errors.Trace(err)
	}
	startTS := txn.StartTS()
	updateStatsMeta := func(id int64) error {
		var err error
		// This lock is already locked on it so it use isTableLocked without lock.
		if h.isTableLocked(id) {
			if delta.Delta < 0 {
				_, err = exec.ExecuteInternal(ctx, "update mysql.stats_table_locked set version = %?, count = count - %?, modify_count = modify_count + %? where table_id = %? and count >= %?", startTS, -delta.Delta, delta.Count, id, -delta.Delta)
			} else {
				_, err = exec.ExecuteInternal(ctx, "update mysql.stats_table_locked set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?", startTS, delta.Delta, delta.Count, id)
			}
		} else {
			if delta.Delta < 0 {
				// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_meta.
				_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_meta (version, table_id, modify_count, count) values (%?, %?, %?, 0) on duplicate key "+
					"update version = values(version), modify_count = modify_count + values(modify_count), count = if(count > %?, count - %?, 0)", startTS, id, delta.Count, -delta.Delta, -delta.Delta)
			} else {
				// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_meta.
				_, err = exec.ExecuteInternal(ctx, "insert into mysql.stats_meta (version, table_id, modify_count, count) values (%?, %?, %?, %?) on duplicate key "+
					"update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)", startTS, id, delta.Count, delta.Delta)
			}
			cache.TableRowStatsCache.Invalidate(id)
		}
		statsVer = startTS
		return errors.Trace(err)
	}
	if err = updateStatsMeta(id); err != nil {
		return
	}
	affectedRows := h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows()

	// if it's a partitioned table and its global-stats exists, update its count and modify_count as well.
	is := h.mu.ctx.GetInfoSchema().(infoschema.InfoSchema)
	if is == nil {
		return false, errors.New("cannot get the information schema")
	}
	if tbl, _, _ := is.FindTableByPartitionID(id); tbl != nil {
		if err = updateStatsMeta(tbl.Meta().ID); err != nil {
			return
		}
	}

	affectedRows += h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows()
	updated = affectedRows > 0
	return
}

func (h *Handle) dumpTableStatColSizeToKV(id int64, delta variable.TableDelta) error {
	if len(delta.ColSize) == 0 {
		return nil
	}
	values := make([]string, 0, len(delta.ColSize))
	for histID, deltaColSize := range delta.ColSize {
		if deltaColSize == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d, 0, %d, 0, %d)", id, histID, deltaColSize))
	}
	if len(values) == 0 {
		return nil
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	sql := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, tot_col_size) "+
		"values %s on duplicate key update tot_col_size = tot_col_size + values(tot_col_size)", strings.Join(values, ","))
	_, _, err := h.execRestrictedSQL(ctx, sql)
	return errors.Trace(err)
}

// DumpColStatsUsageToKV sweeps the whole list, updates the column stats usage map and dumps it to KV.
func (h *Handle) DumpColStatsUsageToKV() error {
	if !variable.EnableColumnTracking.Load() {
		return nil
	}
	h.sweepList()
	h.colMap.Lock()
	colMap := h.colMap.data
	h.colMap.data = make(colStatsUsageMap)
	h.colMap.Unlock()
	defer func() {
		h.colMap.Lock()
		h.colMap.data.merge(colMap)
		h.colMap.Unlock()
	}()
	type pair struct {
		lastUsedAt string
		tblColID   model.TableItemID
	}
	pairs := make([]pair, 0, len(colMap))
	for id, t := range colMap {
		pairs = append(pairs, pair{tblColID: id, lastUsedAt: t.UTC().Format(types.TimeFormat)})
	}
	slices.SortFunc(pairs, func(i, j pair) int {
		if i.tblColID.TableID == j.tblColID.TableID {
			return cmp.Compare(i.tblColID.ID, j.tblColID.ID)
		}
		return cmp.Compare(i.tblColID.TableID, j.tblColID.TableID)
	})
	// Use batch insert to reduce cost.
	for i := 0; i < len(pairs); i += batchInsertSize {
		end := i + batchInsertSize
		if end > len(pairs) {
			end = len(pairs)
		}
		sql := new(strings.Builder)
		sqlexec.MustFormatSQL(sql, "INSERT INTO mysql.column_stats_usage (table_id, column_id, last_used_at) VALUES ")
		for j := i; j < end; j++ {
			// Since we will use some session from session pool to execute the insert statement, we pass in UTC time here and covert it
			// to the session's time zone when executing the insert statement. In this way we can make the stored time right.
			sqlexec.MustFormatSQL(sql, "(%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))", pairs[j].tblColID.TableID, pairs[j].tblColID.ID, pairs[j].lastUsedAt)
			if j < end-1 {
				sqlexec.MustFormatSQL(sql, ",")
			}
		}
		sqlexec.MustFormatSQL(sql, " ON DUPLICATE KEY UPDATE last_used_at = CASE WHEN last_used_at IS NULL THEN VALUES(last_used_at) ELSE GREATEST(last_used_at, VALUES(last_used_at)) END")
		if _, _, err := h.execRestrictedSQL(context.Background(), sql.String()); err != nil {
			return errors.Trace(err)
		}
		for j := i; j < end; j++ {
			delete(colMap, pairs[j].tblColID)
		}
	}
	return nil
}

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

// AutoAnalyzeMinCnt means if the count of table is less than this value, we needn't do auto analyze.
var AutoAnalyzeMinCnt int64 = 1000

// TableAnalyzed checks if the table is analyzed.
func TableAnalyzed(tbl *statistics.Table) bool {
	for _, col := range tbl.Columns {
		if col.IsAnalyzed() {
			return true
		}
	}
	for _, idx := range tbl.Indices {
		if idx.IsAnalyzed() {
			return true
		}
	}
	return false
}

// NeedAnalyzeTable checks if we need to analyze the table:
//  1. If the table has never been analyzed, we need to analyze it when it has
//     not been modified for a while.
//  2. If the table had been analyzed before, we need to analyze it when
//     "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio" and the current time is
//     between `start` and `end`.
func NeedAnalyzeTable(tbl *statistics.Table, _ time.Duration, autoAnalyzeRatio float64) (bool, string) {
	analyzed := TableAnalyzed(tbl)
	if !analyzed {
		return true, "table unanalyzed"
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false, ""
	}
	// No need to analyze it.
	tblCnt := float64(tbl.RealtimeCount)
	if histCnt := tbl.GetColRowCount(); histCnt > 0 {
		tblCnt = histCnt
	}
	if float64(tbl.ModifyCount)/tblCnt <= autoAnalyzeRatio {
		return false, ""
	}
	return true, fmt.Sprintf("too many modifications(%v/%v>%v)", tbl.ModifyCount, tblCnt, autoAnalyzeRatio)
}

func (h *Handle) getAutoAnalyzeParameters() map[string]string {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	sql := "select variable_name, variable_value from mysql.global_variables where variable_name in (%?, %?, %?)"
	rows, _, err := h.execRestrictedSQL(ctx, sql, variable.TiDBAutoAnalyzeRatio, variable.TiDBAutoAnalyzeStartTime, variable.TiDBAutoAnalyzeEndTime)
	if err != nil {
		return map[string]string{}
	}
	parameters := make(map[string]string, len(rows))
	for _, row := range rows {
		parameters[row.GetString(0)] = row.GetString(1)
	}
	return parameters
}

func parseAutoAnalyzeRatio(ratio string) float64 {
	autoAnalyzeRatio, err := strconv.ParseFloat(ratio, 64)
	if err != nil {
		return variable.DefAutoAnalyzeRatio
	}
	return math.Max(autoAnalyzeRatio, 0)
}

func parseAnalyzePeriod(start, end string) (time.Time, time.Time, error) {
	if start == "" {
		start = variable.DefAutoAnalyzeStartTime
	}
	if end == "" {
		end = variable.DefAutoAnalyzeEndTime
	}
	s, err := time.ParseInLocation(variable.FullDayTimeFormat, start, time.UTC)
	if err != nil {
		return s, s, errors.Trace(err)
	}
	e, err := time.ParseInLocation(variable.FullDayTimeFormat, end, time.UTC)
	return s, e, err
}

func (h *Handle) getAnalyzeSnapshot() (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	analyzeSnapshot, err := h.mu.ctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableAnalyzeSnapshot)
	if err != nil {
		return false, err
	}
	return variable.TiDBOptOn(analyzeSnapshot), nil
}

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) (analyzed bool) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("HandleAutoAnalyze panicked", zap.Any("error", r), zap.Stack("stack"))
		}
	}()
	err := h.UpdateSessionVar()
	if err != nil {
		logutil.BgLogger().Error("update analyze version for auto analyze session failed", zap.String("category", "stats"), zap.Error(err))
		return false
	}
	dbs := is.AllSchemaNames()
	parameters := h.getAutoAnalyzeParameters()
	autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	start, end, err := parseAnalyzePeriod(parameters[variable.TiDBAutoAnalyzeStartTime], parameters[variable.TiDBAutoAnalyzeEndTime])
	if err != nil {
		logutil.BgLogger().Error("parse auto analyze period failed", zap.String("category", "stats"), zap.Error(err))
		return false
	}
	if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
		return false
	}
	pruneMode := h.CurrentPruneMode()
	analyzeSnapshot, err := h.getAnalyzeSnapshot()
	if err != nil {
		logutil.BgLogger().Error("load tidb_enable_analyze_snapshot for auto analyze session failed", zap.String("category", "stats"), zap.Error(err))
		return false
	}
	rd := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
	rd.Shuffle(len(dbs), func(i, j int) {
		dbs[i], dbs[j] = dbs[j], dbs[i]
	})
	for _, db := range dbs {
		if util.IsMemOrSysDB(strings.ToLower(db)) {
			continue
		}
		tbls := is.SchemaTables(model.NewCIStr(db))
		// We shuffle dbs and tbls so that the order of iterating tables is random. If the order is fixed and the auto
		// analyze job of one table fails for some reason, it may always analyze the same table and fail again and again
		// when the HandleAutoAnalyze is triggered. Randomizing the order can avoid the problem.
		// TODO: Design a priority queue to place the table which needs analyze most in the front.
		rd.Shuffle(len(tbls), func(i, j int) {
			tbls[i], tbls[j] = tbls[j], tbls[i]
		})
		for _, tbl := range tbls {
			//if table locked, skip analyze
			if h.IsTableLocked(tbl.Meta().ID) {
				continue
			}
			tblInfo := tbl.Meta()
			if tblInfo.IsView() {
				continue
			}
			pi := tblInfo.GetPartitionInfo()
			if pi == nil {
				statsTbl := h.GetTableStats(tblInfo)
				sql := "analyze table %n.%n"
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, autoAnalyzeRatio, analyzeSnapshot, sql, db, tblInfo.Name.O)
				if analyzed {
					// analyze one table at a time to let it get the freshest parameters.
					// others will be analyzed next round which is just 3s later.
					return true
				}
				continue
			}
			if pruneMode == variable.Dynamic {
				analyzed := h.autoAnalyzePartitionTableInDynamicMode(tblInfo, pi, db, autoAnalyzeRatio, analyzeSnapshot)
				if analyzed {
					return true
				}
				continue
			}
			for _, def := range pi.Definitions {
				sql := "analyze table %n.%n partition %n"
				statsTbl := h.GetPartitionStats(tblInfo, def.ID)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, autoAnalyzeRatio, analyzeSnapshot, sql, db, tblInfo.Name.O, def.Name.O)
				if analyzed {
					return true
				}
			}
		}
	}
	return false
}

func (h *Handle) autoAnalyzeTable(tblInfo *model.TableInfo, statsTbl *statistics.Table, ratio float64, analyzeSnapshot bool, sql string, params ...interface{}) bool {
	if statsTbl.Pseudo || statsTbl.RealtimeCount < AutoAnalyzeMinCnt {
		return false
	}
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, 20*h.Lease(), ratio); needAnalyze {
		escaped, err := sqlexec.EscapeSQL(sql, params...)
		if err != nil {
			return false
		}
		logutil.BgLogger().Info("auto analyze triggered", zap.String("category", "stats"), zap.String("sql", escaped), zap.String("reason", reason))
		tableStatsVer := h.mu.ctx.GetSessionVars().AnalyzeVersion
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		h.execAutoAnalyze(tableStatsVer, analyzeSnapshot, sql, params...)
		return true
	}
	for _, idx := range tblInfo.Indices {
		if _, ok := statsTbl.Indices[idx.ID]; !ok && idx.State == model.StatePublic {
			sqlWithIdx := sql + " index %n"
			paramsWithIdx := append(params, idx.Name.O)
			escaped, err := sqlexec.EscapeSQL(sqlWithIdx, paramsWithIdx...)
			if err != nil {
				return false
			}
			logutil.BgLogger().Info("auto analyze for unanalyzed", zap.String("category", "stats"), zap.String("sql", escaped))
			tableStatsVer := h.mu.ctx.GetSessionVars().AnalyzeVersion
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			h.execAutoAnalyze(tableStatsVer, analyzeSnapshot, sqlWithIdx, paramsWithIdx...)
			return true
		}
	}
	return false
}

func (h *Handle) autoAnalyzePartitionTableInDynamicMode(tblInfo *model.TableInfo, pi *model.PartitionInfo, db string, ratio float64, analyzeSnapshot bool) bool {
	h.mu.RLock()
	tableStatsVer := h.mu.ctx.GetSessionVars().AnalyzeVersion
	h.mu.RUnlock()
	analyzePartitionBatchSize := int(variable.AutoAnalyzePartitionBatchSize.Load())
	partitionNames := make([]interface{}, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		partitionStatsTbl := h.GetPartitionStats(tblInfo, def.ID)
		if partitionStatsTbl.Pseudo || partitionStatsTbl.RealtimeCount < AutoAnalyzeMinCnt {
			continue
		}
		if needAnalyze, _ := NeedAnalyzeTable(partitionStatsTbl, 20*h.Lease(), ratio); needAnalyze {
			partitionNames = append(partitionNames, def.Name.O)
			statistics.CheckAnalyzeVerOnTable(partitionStatsTbl, &tableStatsVer)
		}
	}
	getSQL := func(prefix, suffix string, numPartitions int) string {
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(prefix)
		for i := 0; i < numPartitions; i++ {
			if i != 0 {
				sqlBuilder.WriteString(",")
			}
			sqlBuilder.WriteString(" %n")
		}
		sqlBuilder.WriteString(suffix)
		return sqlBuilder.String()
	}
	if len(partitionNames) > 0 {
		logutil.BgLogger().Info("start to auto analyze", zap.String("category", "stats"),
			zap.String("table", tblInfo.Name.String()),
			zap.Any("partitions", partitionNames),
			zap.Int("analyze partition batch size", analyzePartitionBatchSize))
		statsTbl := h.GetTableStats(tblInfo)
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		for i := 0; i < len(partitionNames); i += analyzePartitionBatchSize {
			start := i
			end := start + analyzePartitionBatchSize
			if end >= len(partitionNames) {
				end = len(partitionNames)
			}
			sql := getSQL("analyze table %n.%n partition", "", end-start)
			params := append([]interface{}{db, tblInfo.Name.O}, partitionNames[start:end]...)
			logutil.BgLogger().Info("auto analyze triggered", zap.String("category", "stats"),
				zap.String("table", tblInfo.Name.String()),
				zap.Any("partitions", partitionNames[start:end]))
			h.execAutoAnalyze(tableStatsVer, analyzeSnapshot, sql, params...)
		}
		return true
	}
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		for _, def := range pi.Definitions {
			partitionStatsTbl := h.GetPartitionStats(tblInfo, def.ID)
			if _, ok := partitionStatsTbl.Indices[idx.ID]; !ok {
				partitionNames = append(partitionNames, def.Name.O)
				statistics.CheckAnalyzeVerOnTable(partitionStatsTbl, &tableStatsVer)
			}
		}
		if len(partitionNames) > 0 {
			statsTbl := h.GetTableStats(tblInfo)
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			for i := 0; i < len(partitionNames); i += analyzePartitionBatchSize {
				start := i
				end := start + analyzePartitionBatchSize
				if end >= len(partitionNames) {
					end = len(partitionNames)
				}
				sql := getSQL("analyze table %n.%n partition", " index %n", end-start)
				params := append([]interface{}{db, tblInfo.Name.O}, partitionNames[start:end]...)
				params = append(params, idx.Name.O)
				logutil.BgLogger().Info("auto analyze for unanalyzed", zap.String("category", "stats"),
					zap.String("table", tblInfo.Name.String()),
					zap.String("index", idx.Name.String()),
					zap.Any("partitions", partitionNames[start:end]))
				h.execAutoAnalyze(tableStatsVer, analyzeSnapshot, sql, params...)
			}
			return true
		}
	}
	return false
}

var execOptionForAnalyze = map[int]sqlexec.OptionFuncAlias{
	statistics.Version0: sqlexec.ExecOptionAnalyzeVer1,
	statistics.Version1: sqlexec.ExecOptionAnalyzeVer1,
	statistics.Version2: sqlexec.ExecOptionAnalyzeVer2,
}

func (h *Handle) execAutoAnalyze(statsVer int, analyzeSnapshot bool, sql string, params ...interface{}) {
	startTime := time.Now()
	autoAnalyzeProcID := h.autoAnalyzeProcIDGetter()
	_, _, err := h.execRestrictedSQLWithStatsVer(context.Background(), statsVer, autoAnalyzeProcID, analyzeSnapshot, sql, params...)
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		escaped, err1 := sqlexec.EscapeSQL(sql, params...)
		if err1 != nil {
			escaped = ""
		}
		logutil.BgLogger().Error("auto analyze failed", zap.String("category", "stats"), zap.String("sql", escaped), zap.Duration("cost_time", dur), zap.Error(err))
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	}
}

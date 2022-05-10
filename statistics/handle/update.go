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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
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

type errorRateDelta struct {
	PkID         int64
	PkErrorRate  *statistics.ErrorRate
	IdxErrorRate map[int64]*statistics.ErrorRate
}

type errorRateDeltaMap map[int64]errorRateDelta

func (m errorRateDeltaMap) update(tableID int64, histID int64, rate float64, isIndex bool) {
	item := m[tableID]
	if isIndex {
		if item.IdxErrorRate == nil {
			item.IdxErrorRate = make(map[int64]*statistics.ErrorRate)
		}
		if item.IdxErrorRate[histID] == nil {
			item.IdxErrorRate[histID] = &statistics.ErrorRate{}
		}
		item.IdxErrorRate[histID].Update(rate)
	} else {
		if item.PkErrorRate == nil {
			item.PkID = histID
			item.PkErrorRate = &statistics.ErrorRate{}
		}
		item.PkErrorRate.Update(rate)
	}
	m[tableID] = item
}

func (m errorRateDeltaMap) merge(deltaMap errorRateDeltaMap) {
	for tableID, item := range deltaMap {
		tbl := m[tableID]
		for histID, errorRate := range item.IdxErrorRate {
			if tbl.IdxErrorRate == nil {
				tbl.IdxErrorRate = make(map[int64]*statistics.ErrorRate)
			}
			if tbl.IdxErrorRate[histID] == nil {
				tbl.IdxErrorRate[histID] = &statistics.ErrorRate{}
			}
			tbl.IdxErrorRate[histID].Merge(errorRate)
		}
		if item.PkErrorRate != nil {
			if tbl.PkErrorRate == nil {
				tbl.PkID = item.PkID
				tbl.PkErrorRate = &statistics.ErrorRate{}
			}
			tbl.PkErrorRate.Merge(item.PkErrorRate)
		}
		m[tableID] = tbl
	}
}

func (m errorRateDeltaMap) clear(tableID int64, histID int64, isIndex bool) {
	item := m[tableID]
	if isIndex {
		delete(item.IdxErrorRate, histID)
	} else {
		item.PkErrorRate = nil
	}
	m[tableID] = item
}

// colStatsUsageMap maps (tableID, columnID) to the last time when the column stats are used(needed).
type colStatsUsageMap map[model.TableColumnID]time.Time

func (m colStatsUsageMap) merge(other colStatsUsageMap) {
	for id, t := range other {
		if mt, ok := m[id]; !ok || mt.Before(t) {
			m[id] = t
		}
	}
}

func merge(s *SessionStatsCollector, deltaMap tableDeltaMap, rateMap errorRateDeltaMap, feedback *statistics.QueryFeedbackMap, colMap colStatsUsageMap) {
	deltaMap.merge(s.mapper)
	s.mapper = make(tableDeltaMap)
	rateMap.merge(s.rateMap)
	s.rateMap = make(errorRateDeltaMap)
	feedback.Merge(s.feedback)
	s.feedback = statistics.NewQueryFeedbackMap()
	colMap.merge(s.colMap)
	s.colMap = make(colStatsUsageMap)
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	sync.Mutex

	mapper   tableDeltaMap
	feedback *statistics.QueryFeedbackMap
	rateMap  errorRateDeltaMap
	colMap   colStatsUsageMap
	next     *SessionStatsCollector
	// deleted is set to true when a session is closed. Every time we sweep the list, we will remove the useless collector.
	deleted bool
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

var (
	// MinLogScanCount is the minimum scan count for a feedback to be logged.
	MinLogScanCount = atomic.NewInt64(1000)
	// MinLogErrorRate is the minimum error rate for a feedback to be logged.
	MinLogErrorRate = atomic.NewFloat64(0.5)
)

// StoreQueryFeedback merges the feedback into stats collector. Deprecated.
func (s *SessionStatsCollector) StoreQueryFeedback(feedback interface{}, h *Handle) error {
	q := feedback.(*statistics.QueryFeedback)
	if !q.Valid || q.Hist == nil {
		return nil
	}
	err := h.RecalculateExpectCount(q)
	if err != nil {
		return errors.Trace(err)
	}
	rate := q.CalcErrorRate()
	minScanCnt := MinLogScanCount.Load()
	minErrRate := MinLogErrorRate.Load()
	if !(rate >= minErrRate && (q.Actual() >= minScanCnt || q.Expected >= minScanCnt)) {
		return nil
	}
	metrics.SignificantFeedbackCounter.Inc()
	metrics.StatsInaccuracyRate.Observe(rate)
	if log.GetLevel() == zap.DebugLevel {
		h.logDetailedInfo(q)
	}
	s.Lock()
	defer s.Unlock()
	isIndex := q.Tp == statistics.IndexType
	s.rateMap.update(q.PhysicalID, q.Hist.ID, rate, isIndex)
	s.feedback.Append(q)
	return nil
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
		mapper:   make(tableDeltaMap),
		rateMap:  make(errorRateDeltaMap),
		next:     h.listHead.next,
		feedback: statistics.NewQueryFeedbackMap(),
		colMap:   make(colStatsUsageMap),
	}
	h.listHead.next = newCollector
	return newCollector
}

// IndexUsageInformation is the data struct to store index usage information.
type IndexUsageInformation struct {
	QueryCount   int64
	RowsSelected int64
	LastUsedAt   string
}

// GlobalIndexID is the key type for indexUsageMap.
type GlobalIndexID struct {
	TableID int64
	IndexID int64
}

type indexUsageMap map[GlobalIndexID]IndexUsageInformation

// SessionIndexUsageCollector is a list item that holds the index usage mapper. If you want to write or read mapper, you must lock it.
type SessionIndexUsageCollector struct {
	sync.Mutex

	mapper  indexUsageMap
	next    *SessionIndexUsageCollector
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

//batchInsertSize is the every insert values size limit.Used in such as DumpIndexUsageToKV,DumpColStatsUsageToKV
const batchInsertSize = 10

// DumpIndexUsageToKV will dump in-memory index usage information to KV.
func (h *Handle) DumpIndexUsageToKV() error {
	ctx := context.Background()
	mapper := h.sweepIdxUsageList()
	type FullIndexUsageInformation struct {
		id          GlobalIndexID
		information IndexUsageInformation
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

// needDumpStatsDelta returns true when only updates a small portion of the table and the time since last update
// do not exceed one hour.
func needDumpStatsDelta(h *Handle, id int64, item variable.TableDelta, currentTime time.Time) bool {
	if item.InitTime.IsZero() {
		item.InitTime = currentTime
	}
	tbl, ok := h.statsCache.Load().(statsCache).Get(id)
	if !ok {
		// No need to dump if the stats is invalid.
		return false
	}
	if currentTime.Sub(item.InitTime) > dumpStatsMaxDuration {
		// Dump the stats to kv at least once an hour.
		return true
	}
	if tbl.Count == 0 || float64(item.Count)/float64(tbl.Count) > DumpStatsDeltaRatio {
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
	errorRateMap := make(errorRateDeltaMap)
	feedback := statistics.NewQueryFeedbackMap()
	colMap := make(colStatsUsageMap)
	prev := h.listHead
	prev.Lock()
	for curr := prev.next; curr != nil; curr = curr.next {
		curr.Lock()
		// Merge the session stats into deltaMap, errorRateMap and feedback respectively.
		merge(curr, deltaMap, errorRateMap, feedback, colMap)
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
	h.mu.Lock()
	h.mu.rateMap.merge(errorRateMap)
	h.mu.Unlock()
	h.feedback.Lock()
	h.feedback.data.Merge(feedback)
	h.feedback.data.SiftFeedbacks()
	h.feedback.Unlock()
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
	currentTime := time.Now()
	for id, item := range deltaMap {
		if mode == DumpDelta && !needDumpStatsDelta(h, id, item, currentTime) {
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
			err = h.recordHistoricalStatsMeta(id, statsVer)
		}
	}()
	if delta.Count == 0 {
		return true, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return false, errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(context.Background(), exec, err)
	}()

	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return false, errors.Trace(err)
	}
	startTS := txn.StartTS()
	updateStatsMeta := func(id int64) error {
		var err error
		if delta.Delta < 0 {
			_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %?, count = count - %?, modify_count = modify_count + %? where table_id = %? and count >= %?", startTS, -delta.Delta, delta.Count, id, -delta.Delta)
		} else {
			_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %?, count = count + %?, modify_count = modify_count + %? where table_id = %?", startTS, delta.Delta, delta.Count, id)
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
	sql := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, tot_col_size) "+
		"values %s on duplicate key update tot_col_size = tot_col_size + values(tot_col_size)", strings.Join(values, ","))
	_, _, err := h.execRestrictedSQL(context.Background(), sql)
	return errors.Trace(err)
}

// DumpStatsFeedbackToKV dumps the stats feedback to KV. Deprecated.
func (h *Handle) DumpStatsFeedbackToKV() error {
	h.feedback.Lock()
	feedback := h.feedback.data
	h.feedback.data = statistics.NewQueryFeedbackMap()
	h.feedback.Unlock()
	var err error
	for _, fbs := range feedback.Feedbacks {
		for _, fb := range fbs {
			if fb.Tp == statistics.PkType {
				err = h.DumpFeedbackToKV(fb)
			} else {
				t, ok := h.statsCache.Load().(statsCache).Get(fb.PhysicalID)
				if !ok {
					continue
				}
				idx, ok := t.Indices[fb.Hist.ID]
				if !ok {
					continue
				}
				if idx.StatsVer == statistics.Version1 {
					err = h.DumpFeedbackForIndex(fb, t)
				} else {
					err = h.DumpFeedbackToKV(fb)
				}
			}
			if err != nil {
				// For simplicity, we just drop other feedbacks in case of error.
				break
			}
		}
	}
	return errors.Trace(err)
}

// DumpFeedbackToKV dumps the given feedback to physical kv layer.
func (h *Handle) DumpFeedbackToKV(fb *statistics.QueryFeedback) error {
	vals, err := statistics.EncodeFeedback(fb)
	if err != nil {
		logutil.BgLogger().Debug("error occurred when encoding feedback", zap.Error(err))
		return nil
	}
	var isIndex int64
	if fb.Tp == statistics.IndexType {
		isIndex = 1
	}
	const sql = "insert into mysql.stats_feedback (table_id, hist_id, is_index, feedback) values (%?, %?, %?, %?)"
	h.mu.Lock()
	_, err = h.mu.ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), sql, fb.PhysicalID, fb.Hist.ID, isIndex, vals)
	h.mu.Unlock()
	if err != nil {
		metrics.DumpFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
	} else {
		metrics.DumpFeedbackCounter.WithLabelValues(metrics.LblOK).Inc()
	}
	return errors.Trace(err)
}

// UpdateStatsByLocalFeedback will update statistics by the local feedback.
// Currently, we dump the feedback with the period of 10 minutes, which means
// it takes 10 minutes for a feedback to take effect. However, we can use the
// feedback locally on this tidb-server, so it could be used more timely.
func (h *Handle) UpdateStatsByLocalFeedback(is infoschema.InfoSchema) {
	h.sweepList()
	h.feedback.Lock()
	feedback := h.feedback.data
	h.feedback.data = statistics.NewQueryFeedbackMap()
	h.feedback.Unlock()
OUTER:
	for _, fbs := range feedback.Feedbacks {
		for _, fb := range fbs {
			h.mu.Lock()
			table, ok := h.getTableByPhysicalID(is, fb.PhysicalID)
			h.mu.Unlock()
			if !ok {
				continue
			}
			if table.Meta().Partition != nil {
				// If the table is partition table, the feedback will not work.
				continue
			}
			tblStats := h.GetPartitionStats(table.Meta(), fb.PhysicalID)
			newTblStats := tblStats.Copy()
			if fb.Tp == statistics.IndexType {
				idx, ok := tblStats.Indices[fb.Hist.ID]
				if !ok || idx.Histogram.Len() == 0 {
					continue
				}
				newIdx := *idx
				eqFB, ranFB := statistics.SplitFeedbackByQueryType(fb.Feedback)
				if idx.StatsVer >= statistics.Version2 {
					// // For StatsVersion higher than Version1, the topn is extracted out of histogram. So we don't update the histogram if the feedback overlaps with some topn.
					// ranFB = statistics.CleanRangeFeedbackByTopN(ranFB, idx.TopN)
					continue OUTER
				}
				newIdx.CMSketch, newIdx.TopN = statistics.UpdateCMSketchAndTopN(idx.CMSketch, idx.TopN, eqFB)
				newIdx.Histogram = *statistics.UpdateHistogram(&idx.Histogram, &statistics.QueryFeedback{Feedback: ranFB}, int(idx.StatsVer))
				newIdx.Histogram.PreCalculateScalar()
				newIdx.Flag = statistics.ResetAnalyzeFlag(newIdx.Flag)
				newTblStats.Indices[fb.Hist.ID] = &newIdx
			} else {
				col, ok := tblStats.Columns[fb.Hist.ID]
				if !ok || col.Histogram.Len() == 0 {
					continue
				}
				if col.StatsVer >= statistics.Version2 {
					// // For StatsVersion higher than Version1, the topn is extracted out of histogram. So we don't update the histogram if the feedback overlaps with some topn.
					// ranFB = statistics.CleanRangeFeedbackByTopN(ranFB, idx.TopN)
					continue OUTER
				}
				newCol := *col
				// only use the range query to update primary key
				_, ranFB := statistics.SplitFeedbackByQueryType(fb.Feedback)
				newFB := &statistics.QueryFeedback{Feedback: ranFB}
				newFB = newFB.DecodeIntValues()
				newCol.Histogram = *statistics.UpdateHistogram(&col.Histogram, newFB, statistics.Version1)
				newCol.Flag = statistics.ResetAnalyzeFlag(newCol.Flag)
				newTblStats.Columns[fb.Hist.ID] = &newCol
			}
			for retry := updateStatsCacheRetryCnt; retry > 0; retry-- {
				oldCache := h.statsCache.Load().(statsCache)
				if h.updateStatsCache(oldCache.update([]*statistics.Table{newTblStats}, nil, oldCache.version)) {
					break
				}
			}
		}
	}
}

// UpdateErrorRate updates the error rate of columns from h.rateMap to cache.
func (h *Handle) UpdateErrorRate(is infoschema.InfoSchema) {
	h.mu.Lock()
	tbls := make([]*statistics.Table, 0, len(h.mu.rateMap))
	for id, item := range h.mu.rateMap {
		table, ok := h.getTableByPhysicalID(is, id)
		if !ok {
			continue
		}
		tbl := h.GetPartitionStats(table.Meta(), id).Copy()
		if item.PkErrorRate != nil && tbl.Columns[item.PkID] != nil {
			col := *tbl.Columns[item.PkID]
			col.ErrorRate.Merge(item.PkErrorRate)
			tbl.Columns[item.PkID] = &col
		}
		for key, val := range item.IdxErrorRate {
			if tbl.Indices[key] == nil {
				continue
			}
			idx := *tbl.Indices[key]
			idx.ErrorRate.Merge(val)
			tbl.Indices[key] = &idx
		}
		tbls = append(tbls, tbl)
		delete(h.mu.rateMap, id)
	}
	h.mu.Unlock()
	for retry := updateStatsCacheRetryCnt; retry > 0; retry-- {
		oldCache := h.statsCache.Load().(statsCache)
		if h.updateStatsCache(oldCache.update(tbls, nil, oldCache.version)) {
			break
		}
	}
}

// HandleUpdateStats update the stats using feedback.
func (h *Handle) HandleUpdateStats(is infoschema.InfoSchema) error {
	ctx := context.Background()
	tables, _, err := h.execRestrictedSQL(ctx, "SELECT distinct table_id from mysql.stats_feedback")
	if err != nil {
		return errors.Trace(err)
	}
	if len(tables) == 0 {
		return nil
	}

	for _, ptbl := range tables {
		tableID, histID, isIndex := ptbl.GetInt64(0), int64(-1), int64(-1)
		for {
			// fetch at most 100000 rows each time to avoid OOM
			const sql = "select table_id, hist_id, is_index, feedback from mysql.stats_feedback where table_id = %? and is_index >= %? and hist_id > %? order by is_index, hist_id limit 100000"
			rows, _, err := h.execRestrictedSQL(ctx, sql, tableID, histID, isIndex)
			if err != nil {
				return errors.Trace(err)
			}
			if len(rows) == 0 {
				break
			}
			startIdx := 0
			for i, row := range rows {
				if row.GetInt64(1) != histID || row.GetInt64(2) != isIndex {
					if i > 0 {
						if err = h.handleSingleHistogramUpdate(is, rows[startIdx:i]); err != nil {
							return errors.Trace(err)
						}
					}
					histID, isIndex = row.GetInt64(1), row.GetInt64(2)
					startIdx = i
				}
			}
			if err = h.handleSingleHistogramUpdate(is, rows[startIdx:]); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// handleSingleHistogramUpdate updates the Histogram and CM Sketch using these feedbacks. All the feedbacks for
// the same index or column are gathered in `rows`.
func (h *Handle) handleSingleHistogramUpdate(is infoschema.InfoSchema, rows []chunk.Row) (err error) {
	physicalTableID, histID, isIndex := rows[0].GetInt64(0), rows[0].GetInt64(1), rows[0].GetInt64(2)
	defer func() {
		if err == nil {
			err = errors.Trace(h.deleteOutdatedFeedback(physicalTableID, histID, isIndex))
		}
	}()
	h.mu.Lock()
	table, ok := h.getTableByPhysicalID(is, physicalTableID)
	h.mu.Unlock()
	// The table has been deleted.
	if !ok {
		return nil
	}
	var tbl *statistics.Table
	// feedback for partition is not ready
	if table.Meta().GetPartitionInfo() != nil {
		return nil
	}
	tbl = h.GetTableStats(table.Meta())
	var cms *statistics.CMSketch
	var hist *statistics.Histogram
	var topN *statistics.TopN
	var fms *statistics.FMSketch
	var statsVer int64 = statistics.Version1
	if isIndex == 1 {
		idx, ok := tbl.Indices[histID]
		statsVer = idx.StatsVer
		if statsVer >= 2 {
			logutil.BgLogger().Warn("[stats] Feedback is discarded because statistics on this table is version 2, which is incompatible with feedback. "+
				"Please consider setting feedback-probability to 0.0 in config file to disable query feedback.",
				zap.Int64("table_id", physicalTableID), zap.Int64("hist_id", histID), zap.Int64("is_index", isIndex))
			return err
		}
		if ok && idx.Histogram.Len() > 0 {
			statsVer = idx.StatsVer
			idxHist := idx.Histogram
			hist = &idxHist
			cms = idx.CMSketch.Copy()
			topN = idx.TopN.Copy()
		}
	} else {
		col, ok := tbl.Columns[histID]
		if ok && col.StatsVer >= 2 {
			logutil.BgLogger().Warn("[stats] Feedback is discarded because statistics on this table is version 2, which is incompatible with feedback. "+
				"Please consider setting feedback-probability to 0.0 in config file to disable query feedback.",
				zap.Int64("table_id", physicalTableID), zap.Int64("hist_id", histID), zap.Int64("is_index", isIndex))
			return err
		}
		if ok && col.Histogram.Len() > 0 {
			colHist := col.Histogram
			hist = &colHist
			fms = col.FMSketch
		}
	}
	// The column or index has been deleted.
	if hist == nil {
		return nil
	}
	q := &statistics.QueryFeedback{}
	for _, row := range rows {
		err1 := statistics.DecodeFeedback(row.GetBytes(3), q, cms, topN, hist.Tp)
		if err1 != nil {
			logutil.BgLogger().Debug("decode feedback failed", zap.Error(err1))
		}
	}
	err = h.dumpStatsUpdateToKV(physicalTableID, isIndex, q, hist, cms, topN, fms, statsVer)
	return errors.Trace(err)
}

func (h *Handle) deleteOutdatedFeedback(tableID, histID, isIndex int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	hasData := true
	for hasData {
		sql := "delete from mysql.stats_feedback where table_id = %? and hist_id = %? and is_index = %? limit 10000"
		_, err := h.mu.ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), sql, tableID, histID, isIndex)
		if err != nil {
			return errors.Trace(err)
		}
		hasData = h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0
	}
	return nil
}

func (h *Handle) dumpStatsUpdateToKV(tableID, isIndex int64, q *statistics.QueryFeedback, hist *statistics.Histogram, cms *statistics.CMSketch, topN *statistics.TopN, fms *statistics.FMSketch, statsVersion int64) error {
	hist = statistics.UpdateHistogram(hist, q, int(statsVersion))
	// feedback for partition is not ready.
	err := h.SaveStatsToStorage(tableID, -1, int(isIndex), hist, cms, topN, fms, int(statsVersion), 0, false, false)
	metrics.UpdateStatsCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
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
		tblColID   model.TableColumnID
		lastUsedAt string
	}
	pairs := make([]pair, 0, len(colMap))
	for id, t := range colMap {
		pairs = append(pairs, pair{tblColID: id, lastUsedAt: t.UTC().Format(types.TimeFormat)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].tblColID.TableID == pairs[j].tblColID.TableID {
			return pairs[i].tblColID.ColumnID < pairs[j].tblColID.ColumnID
		}
		return pairs[i].tblColID.TableID < pairs[j].tblColID.TableID
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
			sqlexec.MustFormatSQL(sql, "(%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))", pairs[j].tblColID.TableID, pairs[j].tblColID.ColumnID, pairs[j].lastUsedAt)
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
		if col.Count > 0 {
			return true
		}
	}
	for _, idx := range tbl.Indices {
		if idx.Histogram.Len() > 0 {
			return true
		}
	}
	return false
}

// NeedAnalyzeTable checks if we need to analyze the table:
// 1. If the table has never been analyzed, we need to analyze it when it has
//    not been modified for a while.
// 2. If the table had been analyzed before, we need to analyze it when
//    "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio" and the current time is
//    between `start` and `end`.
func NeedAnalyzeTable(tbl *statistics.Table, limit time.Duration, autoAnalyzeRatio float64) (bool, string) {
	analyzed := TableAnalyzed(tbl)
	if !analyzed {
		t := time.Unix(0, oracle.ExtractPhysical(tbl.Version)*int64(time.Millisecond))
		dur := time.Since(t)
		return dur >= limit, fmt.Sprintf("table unanalyzed, time since last updated %v", dur)
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false, ""
	}
	// No need to analyze it.
	tblCnt := float64(tbl.Count)
	if histCnt := tbl.GetColRowCount(); histCnt > 0 {
		tblCnt = histCnt
	}
	if float64(tbl.ModifyCount)/tblCnt <= autoAnalyzeRatio {
		return false, ""
	}
	return true, fmt.Sprintf("too many modifications(%v/%v>%v)", tbl.ModifyCount, tblCnt, autoAnalyzeRatio)
}

func (h *Handle) getAutoAnalyzeParameters() map[string]string {
	ctx := context.Background()
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

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) (analyzed bool) {
	err := h.UpdateSessionVar()
	if err != nil {
		logutil.BgLogger().Error("[stats] update analyze version for auto analyze session failed", zap.Error(err))
		return false
	}
	dbs := is.AllSchemaNames()
	parameters := h.getAutoAnalyzeParameters()
	autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	start, end, err := parseAnalyzePeriod(parameters[variable.TiDBAutoAnalyzeStartTime], parameters[variable.TiDBAutoAnalyzeEndTime])
	if err != nil {
		logutil.BgLogger().Error("[stats] parse auto analyze period failed", zap.Error(err))
		return false
	}
	if !timeutil.WithinDayTimePeriod(start, end, time.Now()) {
		return false
	}
	pruneMode := h.CurrentPruneMode()
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
			tblInfo := tbl.Meta()
			if tblInfo.IsView() {
				continue
			}
			pi := tblInfo.GetPartitionInfo()
			if pi == nil {
				statsTbl := h.GetTableStats(tblInfo)
				sql := "analyze table %n.%n"
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, sql, db, tblInfo.Name.O)
				if analyzed {
					// analyze one table at a time to let it get the freshest parameters.
					// others will be analyzed next round which is just 3s later.
					return true
				}
				continue
			}
			if pruneMode == variable.Dynamic {
				analyzed := h.autoAnalyzePartitionTable(tblInfo, pi, db, start, end, autoAnalyzeRatio)
				if analyzed {
					return true
				}
				continue
			}
			for _, def := range pi.Definitions {
				sql := "analyze table %n.%n partition %n"
				statsTbl := h.GetPartitionStats(tblInfo, def.ID)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, sql, db, tblInfo.Name.O, def.Name.O)
				if analyzed {
					return true
				}
			}
		}
	}
	return false
}

func (h *Handle) autoAnalyzeTable(tblInfo *model.TableInfo, statsTbl *statistics.Table, start, end time.Time, ratio float64, sql string, params ...interface{}) bool {
	if statsTbl.Pseudo || statsTbl.Count < AutoAnalyzeMinCnt {
		return false
	}
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, 20*h.Lease(), ratio); needAnalyze {
		escaped, err := sqlexec.EscapeSQL(sql, params...)
		if err != nil {
			return false
		}
		logutil.BgLogger().Info("[stats] auto analyze triggered", zap.String("sql", escaped), zap.String("reason", reason))
		tableStatsVer := h.mu.ctx.GetSessionVars().AnalyzeVersion
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		h.execAutoAnalyze(tableStatsVer, sql, params...)
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
			logutil.BgLogger().Info("[stats] auto analyze for unanalyzed", zap.String("sql", escaped))
			tableStatsVer := h.mu.ctx.GetSessionVars().AnalyzeVersion
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			h.execAutoAnalyze(tableStatsVer, sqlWithIdx, paramsWithIdx...)
			return true
		}
	}
	return false
}

func (h *Handle) autoAnalyzePartitionTable(tblInfo *model.TableInfo, pi *model.PartitionInfo, db string, start, end time.Time, ratio float64) bool {
	h.mu.RLock()
	tableStatsVer := h.mu.ctx.GetSessionVars().AnalyzeVersion
	h.mu.RUnlock()
	partitionNames := make([]interface{}, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		partitionStatsTbl := h.GetPartitionStats(tblInfo, def.ID)
		if partitionStatsTbl.Pseudo || partitionStatsTbl.Count < AutoAnalyzeMinCnt {
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
		logutil.BgLogger().Info("[stats] auto analyze triggered")
		sql := getSQL("analyze table %n.%n partition", "", len(partitionNames))
		params := append([]interface{}{db, tblInfo.Name.O}, partitionNames...)
		statsTbl := h.GetTableStats(tblInfo)
		statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
		h.execAutoAnalyze(tableStatsVer, sql, params...)
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
			logutil.BgLogger().Info("[stats] auto analyze for unanalyzed")
			sql := getSQL("analyze table %n.%n partition", " index %n", len(partitionNames))
			params := append([]interface{}{db, tblInfo.Name.O}, partitionNames...)
			params = append(params, idx.Name.O)
			statsTbl := h.GetTableStats(tblInfo)
			statistics.CheckAnalyzeVerOnTable(statsTbl, &tableStatsVer)
			h.execAutoAnalyze(tableStatsVer, sql, params...)
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

func (h *Handle) execAutoAnalyze(statsVer int, sql string, params ...interface{}) {
	startTime := time.Now()
	_, _, err := h.execRestrictedSQLWithStatsVer(context.Background(), statsVer, util.GetAutoAnalyzeProcID(h.serverIDGetter), sql, params...)
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		escaped, err1 := sqlexec.EscapeSQL(sql, params...)
		if err1 != nil {
			escaped = ""
		}
		logutil.BgLogger().Error("[stats] auto analyze failed", zap.String("sql", escaped), zap.Duration("cost_time", dur), zap.Error(err))
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	}
}

// formatBuckets formats bucket from lowBkt to highBkt.
func formatBuckets(hg *statistics.Histogram, lowBkt, highBkt, idxCols int) string {
	if lowBkt == highBkt {
		return hg.BucketToString(lowBkt, idxCols)
	}
	if lowBkt+1 == highBkt {
		return fmt.Sprintf("%s, %s", hg.BucketToString(lowBkt, idxCols), hg.BucketToString(highBkt, idxCols))
	}
	// do not care the middle buckets
	return fmt.Sprintf("%s, (%d buckets, total count %d), %s", hg.BucketToString(lowBkt, idxCols),
		highBkt-lowBkt-1, hg.Buckets[highBkt-1].Count-hg.Buckets[lowBkt].Count, hg.BucketToString(highBkt, idxCols))
}

func colRangeToStr(c *statistics.Column, ran *ranger.Range, actual int64, factor float64) string {
	lowCount, lowBkt := c.LessRowCountWithBktIdx(ran.LowVal[0])
	highCount, highBkt := c.LessRowCountWithBktIdx(ran.HighVal[0])
	return fmt.Sprintf("range: %s, actual: %d, expected: %d, buckets: {%s}", ran.String(), actual,
		int64((highCount-lowCount)*factor), formatBuckets(&c.Histogram, lowBkt, highBkt, 0))
}

func logForIndexRange(idx *statistics.Index, ran *ranger.Range, actual int64, factor float64) string {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	lb, err := codec.EncodeKey(sc, nil, ran.LowVal...)
	if err != nil {
		return ""
	}
	rb, err := codec.EncodeKey(sc, nil, ran.HighVal...)
	if err != nil {
		return ""
	}
	if idx.CMSketch != nil && bytes.Compare(kv.Key(lb).PrefixNext(), rb) >= 0 {
		str, err := types.DatumsToString(ran.LowVal, true)
		if err != nil {
			return ""
		}
		return fmt.Sprintf("value: %s, actual: %d, expected: %d", str, actual, int64(float64(idx.QueryBytes(lb))*factor))
	}
	l, r := types.NewBytesDatum(lb), types.NewBytesDatum(rb)
	lowCount, lowBkt := idx.LessRowCountWithBktIdx(l)
	highCount, highBkt := idx.LessRowCountWithBktIdx(r)
	return fmt.Sprintf("range: %s, actual: %d, expected: %d, histogram: {%s}", ran.String(), actual,
		int64((highCount-lowCount)*factor), formatBuckets(&idx.Histogram, lowBkt, highBkt, len(idx.Info.Columns)))
}

func logForIndex(prefix string, t *statistics.Table, idx *statistics.Index, ranges []*ranger.Range, actual []int64, factor float64) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	if idx.CMSketch == nil || idx.StatsVer != statistics.Version1 {
		for i, ran := range ranges {
			logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.String("rangeStr", logForIndexRange(idx, ran, actual[i], factor)))
		}
		return
	}
	for i, ran := range ranges {
		rangePosition := statistics.GetOrdinalOfRangeCond(sc, ran)
		// only contains range or equality query
		if rangePosition == 0 || rangePosition == len(ran.LowVal) {
			logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.String("rangeStr", logForIndexRange(idx, ran, actual[i], factor)))
			continue
		}
		equalityString, err := types.DatumsToString(ran.LowVal[:rangePosition], true)
		if err != nil {
			continue
		}
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			continue
		}
		equalityCount := idx.QueryBytes(bytes)
		rang := ranger.Range{
			LowVal:    []types.Datum{ran.LowVal[rangePosition]},
			HighVal:   []types.Datum{ran.HighVal[rangePosition]},
			Collators: collate.GetBinaryCollatorSlice(1),
		}
		colName := idx.Info.Columns[rangePosition].Name.L
		// prefer index stats over column stats
		if idxHist := t.IndexStartWithColumn(colName); idxHist != nil && idxHist.Histogram.Len() > 0 {
			rangeString := logForIndexRange(idxHist, &rang, -1, factor)
			logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
				zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
				zap.String("range", rangeString))
		} else if colHist := t.ColumnByName(colName); colHist != nil && colHist.Histogram.Len() > 0 {
			err = convertRangeType(&rang, colHist.Tp, time.UTC)
			if err == nil {
				rangeString := colRangeToStr(colHist, &rang, -1, factor)
				logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
					zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
					zap.String("range", rangeString))
			}
		} else {
			count, err := statistics.GetPseudoRowCountByColumnRanges(sc, float64(t.Count), []*ranger.Range{&rang}, 0)
			if err == nil {
				logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
					zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
					zap.Stringer("range", &rang), zap.Float64("pseudo count", math.Round(count)))
			}
		}
	}
}

func (h *Handle) logDetailedInfo(q *statistics.QueryFeedback) {
	t, ok := h.statsCache.Load().(statsCache).Get(q.PhysicalID)
	if !ok {
		return
	}
	isIndex := q.Hist.IsIndexHist()
	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		logutil.BgLogger().Debug("decode to ranges failed", zap.Error(err))
		return
	}
	actual := make([]int64, 0, len(q.Feedback))
	for _, fb := range q.Feedback {
		actual = append(actual, fb.Count)
	}
	logPrefix := fmt.Sprintf("[stats-feedback] %s", t.Name)
	if isIndex {
		idx := t.Indices[q.Hist.ID]
		if idx == nil || idx.Histogram.Len() == 0 {
			return
		}
		logForIndex(logPrefix, t, idx, ranges, actual, idx.GetIncreaseFactor(t.Count))
	} else {
		c := t.Columns[q.Hist.ID]
		if c == nil || c.Histogram.Len() == 0 {
			return
		}
		logForPK(logPrefix, c, ranges, actual, c.GetIncreaseFactor(t.Count))
	}
}

func logForPK(prefix string, c *statistics.Column, ranges []*ranger.Range, actual []int64, factor float64) {
	for i, ran := range ranges {
		if ran.LowVal[0].GetInt64()+1 >= ran.HighVal[0].GetInt64() {
			continue
		}
		logutil.BgLogger().Debug(prefix, zap.String("column", c.Info.Name.O), zap.String("rangeStr", colRangeToStr(c, ran, actual[i], factor)))
	}
}

// RecalculateExpectCount recalculates the expect row count if the origin row count is estimated by pseudo. Deprecated.
func (h *Handle) RecalculateExpectCount(q *statistics.QueryFeedback) error {
	t, ok := h.statsCache.Load().(statsCache).Get(q.PhysicalID)
	if !ok {
		return nil
	}
	tablePseudo := t.Pseudo
	if h.mu.ctx.GetSessionVars().GetEnablePseudoForOutdatedStats() {
		tablePseudo = t.Pseudo || t.IsOutdated()
	}
	if !tablePseudo {
		return nil
	}
	isIndex := q.Hist.Tp.GetType() == mysql.TypeBlob
	id := q.Hist.ID
	if isIndex && (t.Indices[id] == nil || !t.Indices[id].NotAccurate()) {
		return nil
	}
	if !isIndex && (t.Columns[id] == nil || !t.Columns[id].NotAccurate()) {
		return nil
	}

	se, err := h.pool.Get()
	if err != nil {
		return err
	}
	sctx := se.(sessionctx.Context)
	timeZone := sctx.GetSessionVars().StmtCtx.TimeZone
	defer func() {
		sctx.GetSessionVars().StmtCtx.TimeZone = timeZone
		h.pool.Put(se)
	}()
	sctx.GetSessionVars().StmtCtx.TimeZone = time.UTC

	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		return errors.Trace(err)
	}
	expected := 0.0
	if isIndex {
		idx := t.Indices[id]
		expected, err = idx.GetRowCount(sctx, nil, ranges, t.Count)
	} else {
		c := t.Columns[id]
		expected, err = c.GetColumnRowCount(sctx, ranges, t.Count, true)
	}
	q.Expected = int64(expected)
	return err
}

func (h *Handle) dumpRangeFeedback(sc *stmtctx.StatementContext, ran *ranger.Range, rangeCount float64, q *statistics.QueryFeedback) error {
	lowIsNull := ran.LowVal[0].IsNull()
	if q.Tp == statistics.IndexType {
		lower, err := codec.EncodeKey(sc, nil, ran.LowVal[0])
		if err != nil {
			return errors.Trace(err)
		}
		upper, err := codec.EncodeKey(sc, nil, ran.HighVal[0])
		if err != nil {
			return errors.Trace(err)
		}
		ran.LowVal[0].SetBytes(lower)
		ran.HighVal[0].SetBytes(upper)
	} else {
		if !statistics.SupportColumnType(q.Hist.Tp) {
			return nil
		}
		if ran.LowVal[0].Kind() == types.KindMinNotNull {
			ran.LowVal[0] = types.GetMinValue(q.Hist.Tp)
		}
		if ran.HighVal[0].Kind() == types.KindMaxValue {
			ran.HighVal[0] = types.GetMaxValue(q.Hist.Tp)
		}
	}
	ranges, ok := q.Hist.SplitRange(sc, []*ranger.Range{ran}, q.Tp == statistics.IndexType)
	if !ok {
		logutil.BgLogger().Debug("type of histogram and ranges mismatch")
		return nil
	}
	counts := make([]float64, 0, len(ranges))
	sum := 0.0
	for i, r := range ranges {
		// Though after `SplitRange`, we may have ranges like `[l, r]`, we still use
		// `betweenRowCount` to compute the estimation since the ranges of feedback are all in `[l, r)`
		// form, that is to say, we ignore the exclusiveness of ranges from `SplitRange` and just use
		// its result of boundary values.
		count := q.Hist.BetweenRowCount(r.LowVal[0], r.HighVal[0])
		// We have to include `NullCount` of histogram for [l, r) cases where l is null because `betweenRowCount`
		// does not include null values of lower bound.
		if i == 0 && lowIsNull {
			count += float64(q.Hist.NullCount)
		}
		sum += count
		counts = append(counts, count)
	}
	if sum <= 1 {
		return nil
	}
	// We assume that each part contributes the same error rate.
	adjustFactor := rangeCount / sum
	for i, r := range ranges {
		q.Feedback = append(q.Feedback, statistics.Feedback{Lower: &r.LowVal[0], Upper: &r.HighVal[0], Count: int64(counts[i] * adjustFactor)})
	}
	return errors.Trace(h.DumpFeedbackToKV(q))
}

func convertRangeType(ran *ranger.Range, ft *types.FieldType, loc *time.Location) error {
	err := statistics.ConvertDatumsType(ran.LowVal, ft, loc)
	if err != nil {
		return err
	}
	return statistics.ConvertDatumsType(ran.HighVal, ft, loc)
}

// DumpFeedbackForIndex dumps the feedback for index. Deprecated.
// For queries that contains both equality and range query, we will split them and Update accordingly.
func (h *Handle) DumpFeedbackForIndex(q *statistics.QueryFeedback, t *statistics.Table) error {
	idx, ok := t.Indices[q.Hist.ID]
	if !ok {
		return nil
	}

	se, err := h.pool.Get()
	if err != nil {
		return err
	}
	sctx := se.(sessionctx.Context)
	sc := sctx.GetSessionVars().StmtCtx
	timeZone := sc.TimeZone
	defer func() {
		sctx.GetSessionVars().StmtCtx.TimeZone = timeZone
		h.pool.Put(se)
	}()
	sc.TimeZone = time.UTC

	if idx.CMSketch == nil || idx.StatsVer < statistics.Version1 {
		return h.DumpFeedbackToKV(q)
	}
	ranges, err := q.DecodeToRanges(true)
	if err != nil {
		logutil.BgLogger().Debug("decode feedback ranges fail", zap.Error(err))
		return nil
	}
	for i, ran := range ranges {
		rangePosition := statistics.GetOrdinalOfRangeCond(sc, ran)
		// only contains range or equality query
		if rangePosition == 0 || rangePosition == len(ran.LowVal) {
			continue
		}
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			logutil.BgLogger().Debug("encode keys fail", zap.Error(err))
			continue
		}
		equalityCount := float64(idx.QueryBytes(bytes)) * idx.GetIncreaseFactor(t.Count)
		rang := &ranger.Range{
			LowVal:    []types.Datum{ran.LowVal[rangePosition]},
			HighVal:   []types.Datum{ran.HighVal[rangePosition]},
			Collators: collate.GetBinaryCollatorSlice(1),
		}
		colName := idx.Info.Columns[rangePosition].Name.L
		var rangeCount float64
		rangeFB := &statistics.QueryFeedback{PhysicalID: q.PhysicalID}
		// prefer index stats over column stats
		if idx := t.IndexStartWithColumn(colName); idx != nil && idx.Histogram.Len() != 0 {
			rangeCount, err = t.GetRowCountByIndexRanges(sctx, idx.ID, []*ranger.Range{rang})
			rangeFB.Tp, rangeFB.Hist = statistics.IndexType, &idx.Histogram
		} else if col := t.ColumnByName(colName); col != nil && col.Histogram.Len() != 0 {
			err = convertRangeType(rang, col.Tp, time.UTC)
			if err == nil {
				rangeCount, err = t.GetRowCountByColumnRanges(sctx, col.ID, []*ranger.Range{rang})
				rangeFB.Tp, rangeFB.Hist = statistics.ColType, &col.Histogram
			}
		} else {
			continue
		}
		if err != nil {
			logutil.BgLogger().Debug("get row count by ranges fail", zap.Error(err))
			continue
		}

		equalityCount, rangeCount = getNewCountForIndex(equalityCount, rangeCount, float64(t.Count), float64(q.Feedback[i].Count))
		value := types.NewBytesDatum(bytes)
		q.Feedback[i] = statistics.Feedback{Lower: &value, Upper: &value, Count: int64(equalityCount)}
		err = h.dumpRangeFeedback(sc, rang, rangeCount, rangeFB)
		if err != nil {
			logutil.BgLogger().Debug("dump range feedback fail", zap.Error(err))
			continue
		}
	}
	return errors.Trace(h.DumpFeedbackToKV(q))
}

// minAdjustFactor is the minimum adjust factor of each index feedback.
// We use it to avoid adjusting too much when the assumption of independence failed.
const minAdjustFactor = 0.7

// getNewCountForIndex adjust the estimated `eqCount` and `rangeCount` according to the real count.
// We assumes that `eqCount` and `rangeCount` contribute the same error rate.
func getNewCountForIndex(eqCount, rangeCount, totalCount, realCount float64) (float64, float64) {
	estimate := (eqCount / totalCount) * (rangeCount / totalCount) * totalCount
	if estimate <= 1 {
		return eqCount, rangeCount
	}
	adjustFactor := math.Sqrt(realCount / estimate)
	adjustFactor = math.Max(adjustFactor, minAdjustFactor)
	return eqCount * adjustFactor, rangeCount * adjustFactor
}

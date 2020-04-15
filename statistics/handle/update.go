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
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/infoschema"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/metrics"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
	"github.com/pingcap/tidb/v4/statistics"
	"github.com/pingcap/tidb/v4/store/tikv/oracle"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/codec"
	"github.com/pingcap/tidb/v4/util/logutil"
	"github.com/pingcap/tidb/v4/util/ranger"
	"github.com/pingcap/tidb/v4/util/sqlexec"
	"github.com/pingcap/tidb/v4/util/timeutil"
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

func (h *Handle) merge(s *SessionStatsCollector, rateMap errorRateDeltaMap) {
	for id, item := range s.mapper {
		h.globalMap.update(id, item.Delta, item.Count, &item.ColSize)
	}
	s.mapper = make(tableDeltaMap)
	rateMap.merge(s.rateMap)
	s.rateMap = make(errorRateDeltaMap)
	h.feedback = mergeQueryFeedback(h.feedback, s.feedback)
	s.feedback = s.feedback[:0]
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	sync.Mutex

	mapper   tableDeltaMap
	feedback []*statistics.QueryFeedback
	rateMap  errorRateDeltaMap
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

func mergeQueryFeedback(lq []*statistics.QueryFeedback, rq []*statistics.QueryFeedback) []*statistics.QueryFeedback {
	remained := mathutil.MinInt64(int64(len(rq)), MaxQueryFeedbackCount.Load()-int64(len(lq)))
	remained = mathutil.MaxInt64(0, remained)
	return append(lq, rq[:remained]...)
}

var (
	// MinLogScanCount is the minimum scan count for a feedback to be logged.
	MinLogScanCount = int64(1000)
	// MinLogErrorRate is the minimum error rate for a feedback to be logged.
	MinLogErrorRate = 0.5
)

// StoreQueryFeedback will merges the feedback into stats collector.
func (s *SessionStatsCollector) StoreQueryFeedback(feedback interface{}, h *Handle) error {
	q := feedback.(*statistics.QueryFeedback)
	// TODO: If the error rate is small or actual scan count is small, we do not need to store the feed back.
	if !q.Valid || q.Hist == nil {
		return nil
	}
	err := h.RecalculateExpectCount(q)
	if err != nil {
		return errors.Trace(err)
	}
	rate := q.CalcErrorRate()
	if rate >= MinLogErrorRate && (q.Actual() >= MinLogScanCount || q.Expected >= MinLogScanCount) {
		metrics.SignificantFeedbackCounter.Inc()
		if log.GetLevel() == zap.DebugLevel {
			h.logDetailedInfo(q)
		}
	}
	metrics.StatsInaccuracyRate.Observe(rate)
	s.Lock()
	defer s.Unlock()
	isIndex := q.Tp == statistics.IndexType
	s.rateMap.update(q.PhysicalID, q.Hist.ID, rate, isIndex)
	if len(s.feedback) < int(MaxQueryFeedbackCount.Load()) {
		s.feedback = append(s.feedback, q)
	}
	return nil
}

// NewSessionStatsCollector allocates a stats collector for a session.
func (h *Handle) NewSessionStatsCollector() *SessionStatsCollector {
	h.listHead.Lock()
	defer h.listHead.Unlock()
	newCollector := &SessionStatsCollector{
		mapper:  make(tableDeltaMap),
		rateMap: make(errorRateDeltaMap),
		next:    h.listHead.next,
	}
	h.listHead.next = newCollector
	return newCollector
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
	tbl, ok := h.statsCache.Load().(statsCache).tables[id]
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
	prev := h.listHead
	prev.Lock()
	errorRateMap := make(errorRateDeltaMap)
	for curr := prev.next; curr != nil; curr = curr.next {
		curr.Lock()
		// Merge the session stats into handle and error rate map.
		h.merge(curr, errorRateMap)
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
	h.mu.Lock()
	h.mu.rateMap.merge(errorRateMap)
	h.mu.Unlock()
}

// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
// If the mode is `DumpDelta`, it will only dump that delta info that `Modify Count / Table Count` greater than a ratio.
func (h *Handle) DumpStatsDeltaToKV(mode dumpMode) error {
	h.sweepList()
	currentTime := time.Now()
	for id, item := range h.globalMap {
		if mode == DumpDelta && !needDumpStatsDelta(h, id, item, currentTime) {
			continue
		}
		updated, err := h.dumpTableStatCountToKV(id, item)
		if err != nil {
			return errors.Trace(err)
		}
		if updated {
			h.globalMap.update(id, -item.Delta, -item.Count, nil)
		}
		if err = h.dumpTableStatColSizeToKV(id, item); err != nil {
			return errors.Trace(err)
		}
		if updated {
			delete(h.globalMap, id)
		} else {
			m := h.globalMap[id]
			m.ColSize = nil
			h.globalMap[id] = m
		}
	}
	return nil
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
func (h *Handle) dumpTableStatCountToKV(id int64, delta variable.TableDelta) (updated bool, err error) {
	if delta.Count == 0 {
		return true, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "begin")
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
	var sql string
	if delta.Delta < 0 {
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count - %d, modify_count = modify_count + %d where table_id = %d and count >= %d", startTS, -delta.Delta, delta.Count, id, -delta.Delta)
	} else {
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count + %d, modify_count = modify_count + %d where table_id = %d", startTS, delta.Delta, delta.Count, id)
	}
	err = execSQLs(context.Background(), exec, []string{sql})
	updated = h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0
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
	_, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	return errors.Trace(err)
}

// DumpStatsFeedbackToKV dumps the stats feedback to KV.
func (h *Handle) DumpStatsFeedbackToKV() error {
	var err error
	var successCount int
	for _, fb := range h.feedback {
		if fb.Tp == statistics.PkType {
			err = h.DumpFeedbackToKV(fb)
		} else {
			t, ok := h.statsCache.Load().(statsCache).tables[fb.PhysicalID]
			if ok {
				err = h.DumpFeedbackForIndex(fb, t)
			}
		}
		if err != nil {
			break
		}
		successCount++
	}
	h.feedback = h.feedback[successCount:]
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
	sql := fmt.Sprintf("insert into mysql.stats_feedback (table_id, hist_id, is_index, feedback) values "+
		"(%d, %d, %d, X'%X')", fb.PhysicalID, fb.Hist.ID, isIndex, vals)
	h.mu.Lock()
	_, err = h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
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
	for _, fb := range h.feedback {
		h.mu.Lock()
		table, ok := h.getTableByPhysicalID(is, fb.PhysicalID)
		h.mu.Unlock()
		if !ok {
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
			newIdx.CMSketch = statistics.UpdateCMSketch(idx.CMSketch, eqFB)
			newIdx.Histogram = *statistics.UpdateHistogram(&idx.Histogram, &statistics.QueryFeedback{Feedback: ranFB})
			newIdx.Histogram.PreCalculateScalar()
			newIdx.Flag = statistics.ResetAnalyzeFlag(newIdx.Flag)
			newTblStats.Indices[fb.Hist.ID] = &newIdx
		} else {
			col, ok := tblStats.Columns[fb.Hist.ID]
			if !ok || col.Histogram.Len() == 0 {
				continue
			}
			newCol := *col
			// only use the range query to update primary key
			_, ranFB := statistics.SplitFeedbackByQueryType(fb.Feedback)
			newFB := &statistics.QueryFeedback{Feedback: ranFB}
			newFB = newFB.DecodeIntValues()
			newCol.Histogram = *statistics.UpdateHistogram(&col.Histogram, newFB)
			newCol.Flag = statistics.ResetAnalyzeFlag(newCol.Flag)
			newTblStats.Columns[fb.Hist.ID] = &newCol
		}
		oldCache := h.statsCache.Load().(statsCache)
		h.updateStatsCache(oldCache.update([]*statistics.Table{newTblStats}, nil, oldCache.version))
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
	oldCache := h.statsCache.Load().(statsCache)
	h.updateStatsCache(oldCache.update(tbls, nil, oldCache.version))
}

// HandleUpdateStats update the stats using feedback.
func (h *Handle) HandleUpdateStats(is infoschema.InfoSchema) error {
	sql := "select table_id, hist_id, is_index, feedback from mysql.stats_feedback order by table_id, hist_id, is_index"
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	if len(rows) == 0 || err != nil {
		return errors.Trace(err)
	}

	var groupedRows [][]chunk.Row
	preIdx := 0
	tableID, histID, isIndex := rows[0].GetInt64(0), rows[0].GetInt64(1), rows[0].GetInt64(2)
	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if row.GetInt64(0) != tableID || row.GetInt64(1) != histID || row.GetInt64(2) != isIndex {
			groupedRows = append(groupedRows, rows[preIdx:i])
			tableID, histID, isIndex = row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)
			preIdx = i
		}
	}
	groupedRows = append(groupedRows, rows[preIdx:])

	for _, rows := range groupedRows {
		if err := h.handleSingleHistogramUpdate(is, rows); err != nil {
			return errors.Trace(err)
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
	if table.Meta().GetPartitionInfo() != nil {
		tbl = h.GetPartitionStats(table.Meta(), physicalTableID)
	} else {
		tbl = h.GetTableStats(table.Meta())
	}
	var cms *statistics.CMSketch
	var hist *statistics.Histogram
	if isIndex == 1 {
		idx, ok := tbl.Indices[histID]
		if ok && idx.Histogram.Len() > 0 {
			idxHist := idx.Histogram
			hist = &idxHist
			cms = idx.CMSketch.Copy()
		}
	} else {
		col, ok := tbl.Columns[histID]
		if ok && col.Histogram.Len() > 0 {
			colHist := col.Histogram
			hist = &colHist
		}
	}
	// The column or index has been deleted.
	if hist == nil {
		return nil
	}
	q := &statistics.QueryFeedback{}
	for _, row := range rows {
		err1 := statistics.DecodeFeedback(row.GetBytes(3), q, cms, hist.Tp)
		if err1 != nil {
			logutil.BgLogger().Debug("decode feedback failed", zap.Error(err))
		}
	}
	err = h.dumpStatsUpdateToKV(physicalTableID, isIndex, q, hist, cms)
	return errors.Trace(err)
}

func (h *Handle) deleteOutdatedFeedback(tableID, histID, isIndex int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	hasData := true
	for hasData {
		sql := fmt.Sprintf("delete from mysql.stats_feedback where table_id = %d and hist_id = %d and is_index = %d limit 10000", tableID, histID, isIndex)
		_, err := h.mu.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
		if err != nil {
			return errors.Trace(err)
		}
		hasData = h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0
	}
	return nil
}

func (h *Handle) dumpStatsUpdateToKV(tableID, isIndex int64, q *statistics.QueryFeedback, hist *statistics.Histogram, cms *statistics.CMSketch) error {
	hist = statistics.UpdateHistogram(hist, q)
	err := h.SaveStatsToStorage(tableID, -1, int(isIndex), hist, cms, 0)
	metrics.UpdateStatsCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
	return errors.Trace(err)
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
func NeedAnalyzeTable(tbl *statistics.Table, limit time.Duration, autoAnalyzeRatio float64, start, end, now time.Time) (bool, string) {
	analyzed := TableAnalyzed(tbl)
	if !analyzed {
		t := time.Unix(0, oracle.ExtractPhysical(tbl.Version)*int64(time.Millisecond))
		dur := time.Since(t)
		return dur >= limit, fmt.Sprintf("table unanalyzed, time since last updated %vs", dur)
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false, ""
	}
	// No need to analyze it.
	if float64(tbl.ModifyCount)/float64(tbl.Count) <= autoAnalyzeRatio {
		return false, ""
	}
	// Tests if current time is within the time period.
	return timeutil.WithinDayTimePeriod(start, end, now), fmt.Sprintf("too many modifications(%v/%v>%v)", tbl.ModifyCount, tbl.Count, autoAnalyzeRatio)
}

func (h *Handle) getAutoAnalyzeParameters() map[string]string {
	sql := fmt.Sprintf("select variable_name, variable_value from mysql.global_variables where variable_name in ('%s', '%s', '%s')",
		variable.TiDBAutoAnalyzeRatio, variable.TiDBAutoAnalyzeStartTime, variable.TiDBAutoAnalyzeEndTime)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
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
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) {
	dbs := is.AllSchemaNames()
	parameters := h.getAutoAnalyzeParameters()
	autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.TiDBAutoAnalyzeRatio])
	start, end, err := parseAnalyzePeriod(parameters[variable.TiDBAutoAnalyzeStartTime], parameters[variable.TiDBAutoAnalyzeEndTime])
	if err != nil {
		logutil.BgLogger().Error("[stats] parse auto analyze period failed", zap.Error(err))
		return
	}
	for _, db := range dbs {
		tbls := is.SchemaTables(model.NewCIStr(db))
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			pi := tblInfo.GetPartitionInfo()
			tblName := "`" + db + "`.`" + tblInfo.Name.O + "`"
			if pi == nil {
				statsTbl := h.GetTableStats(tblInfo)
				sql := fmt.Sprintf("analyze table %s", tblName)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, sql)
				if analyzed {
					return
				}
				continue
			}
			for _, def := range pi.Definitions {
				sql := fmt.Sprintf("analyze table %s partition `%s`", tblName, def.Name.O)
				statsTbl := h.GetPartitionStats(tblInfo, def.ID)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, sql)
				if analyzed {
					return
				}
				continue
			}
		}
	}
}

func (h *Handle) autoAnalyzeTable(tblInfo *model.TableInfo, statsTbl *statistics.Table, start, end time.Time, ratio float64, sql string) bool {
	if statsTbl.Pseudo || statsTbl.Count < AutoAnalyzeMinCnt {
		return false
	}
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, 20*h.Lease(), ratio, start, end, time.Now()); needAnalyze {
		logutil.BgLogger().Info("[stats] auto analyze triggered", zap.String("sql", sql), zap.String("reason", reason))
		h.execAutoAnalyze(sql)
		return true
	}
	for _, idx := range tblInfo.Indices {
		if _, ok := statsTbl.Indices[idx.ID]; !ok && idx.State == model.StatePublic {
			sql = fmt.Sprintf("%s index `%s`", sql, idx.Name.O)
			logutil.BgLogger().Info("[stats] auto analyze for unanalyzed", zap.String("sql", sql))
			h.execAutoAnalyze(sql)
			return true
		}
	}
	return false
}

func (h *Handle) execAutoAnalyze(sql string) {
	startTime := time.Now()
	_, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		logutil.BgLogger().Error("[stats] auto analyze failed", zap.String("sql", sql), zap.Duration("cost_time", dur), zap.Error(err))
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
		equalityCount := idx.CMSketch.QueryBytes(bytes)
		rang := ranger.Range{
			LowVal:  []types.Datum{ran.LowVal[rangePosition]},
			HighVal: []types.Datum{ran.HighVal[rangePosition]},
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
	t, ok := h.statsCache.Load().(statsCache).tables[q.PhysicalID]
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

// RecalculateExpectCount recalculates the expect row count if the origin row count is estimated by pseudo.
func (h *Handle) RecalculateExpectCount(q *statistics.QueryFeedback) error {
	t, ok := h.statsCache.Load().(statsCache).tables[q.PhysicalID]
	if !ok {
		return nil
	}
	tablePseudo := t.Pseudo || t.IsOutdated()
	if !tablePseudo {
		return nil
	}
	isIndex := q.Hist.Tp.Tp == mysql.TypeBlob
	id := q.Hist.ID
	if isIndex && (t.Indices[id] == nil || !t.Indices[id].NotAccurate()) {
		return nil
	}
	if !isIndex && (t.Columns[id] == nil || !t.Columns[id].NotAccurate()) {
		return nil
	}

	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		return errors.Trace(err)
	}
	expected := 0.0
	if isIndex {
		idx := t.Indices[id]
		expected, err = idx.GetRowCount(sc, ranges, t.ModifyCount)
		expected *= idx.GetIncreaseFactor(t.Count)
	} else {
		c := t.Columns[id]
		expected, err = c.GetColumnRowCount(sc, ranges, t.ModifyCount, true)
		expected *= c.GetIncreaseFactor(t.Count)
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

// DumpFeedbackForIndex dumps the feedback for index.
// For queries that contains both equality and range query, we will split them and Update accordingly.
func (h *Handle) DumpFeedbackForIndex(q *statistics.QueryFeedback, t *statistics.Table) error {
	idx, ok := t.Indices[q.Hist.ID]
	if !ok {
		return nil
	}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	if idx.CMSketch == nil || idx.StatsVer != statistics.Version1 {
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
		equalityCount := float64(idx.CMSketch.QueryBytes(bytes)) * idx.GetIncreaseFactor(t.Count)
		rang := &ranger.Range{
			LowVal:  []types.Datum{ran.LowVal[rangePosition]},
			HighVal: []types.Datum{ran.HighVal[rangePosition]},
		}
		colName := idx.Info.Columns[rangePosition].Name.L
		var rangeCount float64
		rangeFB := &statistics.QueryFeedback{PhysicalID: q.PhysicalID}
		// prefer index stats over column stats
		if idx := t.IndexStartWithColumn(colName); idx != nil && idx.Histogram.Len() != 0 {
			rangeCount, err = t.GetRowCountByIndexRanges(sc, idx.ID, []*ranger.Range{rang})
			rangeFB.Tp, rangeFB.Hist = statistics.IndexType, &idx.Histogram
		} else if col := t.ColumnByName(colName); col != nil && col.Histogram.Len() != 0 {
			err = convertRangeType(rang, col.Tp, time.UTC)
			if err == nil {
				rangeCount, err = t.GetRowCountByColumnRanges(sc, col.ID, []*ranger.Range{rang})
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

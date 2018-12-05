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

package statistics

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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

func (h *Handle) merge(s *SessionStatsCollector) {
	s.Lock()
	defer s.Unlock()
	for id, item := range s.mapper {
		h.globalMap.update(id, item.Delta, item.Count, &item.ColSize)
	}
	h.feedback = mergeQueryFeedback(h.feedback, s.feedback)
	s.mapper = make(tableDeltaMap)
	s.feedback = s.feedback[:0]
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	sync.Mutex

	mapper   tableDeltaMap
	feedback []*QueryFeedback
	prev     *SessionStatsCollector
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

func mergeQueryFeedback(lq []*QueryFeedback, rq []*QueryFeedback) []*QueryFeedback {
	for _, q := range rq {
		if len(lq) >= MaxQueryFeedbackCount {
			break
		}
		lq = append(lq, q)
	}
	return lq
}

// StoreQueryFeedback will merges the feedback into stats collector.
func (s *SessionStatsCollector) StoreQueryFeedback(feedback interface{}) {
	q := feedback.(*QueryFeedback)
	// TODO: If the error rate is small or actual scan count is small, we do not need to store the feed back.
	if !q.valid || q.hist == nil {
		return
	}

	var rate float64
	if q.actual == 0 {
		if q.expected == 0 {
			rate = 0
		} else {
			rate = 1
		}
	} else {
		rate = math.Abs(float64(q.expected-q.actual) / float64(q.actual))
	}
	metrics.StatsInaccuracyRate.Observe(rate)

	s.Lock()
	defer s.Unlock()
	if len(s.feedback) >= MaxQueryFeedbackCount {
		return
	}
	s.feedback = append(s.feedback, q)
}

// tryToRemoveFromList will remove this collector from the list if it's deleted flag is set.
func (s *SessionStatsCollector) tryToRemoveFromList() {
	s.Lock()
	defer s.Unlock()
	if !s.deleted {
		return
	}
	next := s.next
	prev := s.prev
	prev.next = next
	if next != nil {
		next.prev = prev
	}
}

// NewSessionStatsCollector allocates a stats collector for a session.
func (h *Handle) NewSessionStatsCollector() *SessionStatsCollector {
	h.listHead.Lock()
	defer h.listHead.Unlock()
	newCollector := &SessionStatsCollector{
		mapper: make(tableDeltaMap),
		next:   h.listHead.next,
		prev:   h.listHead,
	}
	if h.listHead.next != nil {
		h.listHead.next.prev = newCollector
	}
	h.listHead.next = newCollector
	return newCollector
}

// DumpStatsDeltaToKV sweeps the whole list and updates the global map. Then we dumps every table that held in map to KV.
func (h *Handle) DumpStatsDeltaToKV() error {
	h.listHead.Lock()
	for collector := h.listHead.next; collector != nil; collector = collector.next {
		collector.tryToRemoveFromList()
		h.merge(collector)
	}
	h.listHead.Unlock()
	for id, item := range h.globalMap {
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
func (h *Handle) dumpTableStatCountToKV(id int64, delta variable.TableDelta) (bool, error) {
	if delta.Count == 0 {
		return true, nil
	}
	ctx := context.TODO()
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute(ctx, "begin")
	if err != nil {
		return false, errors.Trace(err)
	}
	var sql string
	if delta.Delta < 0 {
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count - %d, modify_count = modify_count + %d where table_id = %d and count >= %d", h.ctx.Txn(true).StartTS(), -delta.Delta, delta.Count, id, -delta.Delta)
	} else {
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count + %d, modify_count = modify_count + %d where table_id = %d", h.ctx.Txn(true).StartTS(), delta.Delta, delta.Count, id)
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	updated := h.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(ctx, "commit")
	return updated, errors.Trace(err)
}

func (h *Handle) dumpTableStatColSizeToKV(id int64, delta variable.TableDelta) error {
	if len(delta.ColSize) == 0 {
		return nil
	}
	ctx := context.TODO()
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	version := h.ctx.Txn(true).StartTS()
	values := make([]string, 0, len(delta.ColSize))
	for histID, deltaColSize := range delta.ColSize {
		if deltaColSize == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d, 0, %d, 0, %d, %d)", id, histID, deltaColSize, version))
	}
	if len(values) == 0 {
		_, err = h.ctx.(sqlexec.SQLExecutor).Execute(ctx, "rollback")
		return errors.Trace(err)
	}
	sql := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, tot_col_size, version) "+
		"values %s on duplicate key update tot_col_size = tot_col_size + values(tot_col_size), version = values(version)", strings.Join(values, ","))
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(ctx, "commit")
	return errors.Trace(err)
}

// DumpStatsFeedbackToKV dumps the stats feedback to KV.
func (h *Handle) DumpStatsFeedbackToKV() error {
	var err error
	var successCount int
	for _, fb := range h.feedback {
		err = h.dumpFeedbackToKV(fb)
		if err != nil {
			break
		}
		successCount++
	}
	h.feedback = h.feedback[successCount:]
	return errors.Trace(err)
}

func (h *Handle) dumpFeedbackToKV(fb *QueryFeedback) error {
	vals, err := encodeFeedback(fb)
	if err != nil {
		log.Debugf("error occurred when encoding feedback, err: %s", errors.ErrorStack(err))
		return nil
	}
	var isIndex int64
	if fb.hist.tp.Tp == mysql.TypeBlob {
		isIndex = 1
	} else {
		isIndex = 0
	}
	sql := fmt.Sprintf("insert into mysql.stats_feedback (table_id, hist_id, is_index, feedback) values "+
		"(%d, %d, %d, X'%X')", fb.tableID, fb.hist.ID, isIndex, vals)
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	if err != nil {
		metrics.DumpFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
	} else {
		metrics.DumpFeedbackCounter.WithLabelValues(metrics.LblOK).Inc()
	}
	return errors.Trace(err)
}

// HandleUpdateStats update the stats using feedback.
func (h *Handle) HandleUpdateStats(is infoschema.InfoSchema) error {
	sql := "select table_id, hist_id, is_index, feedback from mysql.stats_feedback order by table_id, hist_id, is_index"
	rows, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	if len(rows) == 0 || err != nil {
		return errors.Trace(err)
	}

	var groupedRows [][]types.Row
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
func (h *Handle) handleSingleHistogramUpdate(is infoschema.InfoSchema, rows []types.Row) (err error) {
	tableID, histID, isIndex := rows[0].GetInt64(0), rows[0].GetInt64(1), rows[0].GetInt64(2)
	defer func() {
		if err == nil {
			err = errors.Trace(h.deleteOutdatedFeedback(tableID, histID, isIndex))
		}
	}()
	table, ok := is.TableByID(tableID)
	// The table has been deleted.
	if !ok {
		return nil
	}
	tbl := h.GetTableStats(table.Meta())
	var cms *CMSketch
	var hist *Histogram
	if isIndex == 1 {
		idx, ok := tbl.Indices[histID]
		if ok {
			idxHist := idx.Histogram
			hist = &idxHist
			cms = idx.CMSketch.copy()
		}
	} else {
		col, ok := tbl.Columns[histID]
		if ok {
			colHist := col.Histogram
			hist = &colHist
		}
	}
	// The column or index has been deleted.
	if hist == nil {
		return nil
	}
	q := &QueryFeedback{}
	for _, row := range rows {
		err1 := decodeFeedback(row.GetBytes(3), q, cms)
		if err1 != nil {
			log.Debugf("decode feedback failed, err: %v", errors.ErrorStack(err))
		}
	}
	// Update the NDV of primary key column.
	if table.Meta().PKIsHandle && isIndex == 0 {
		hist.NDV = int64(hist.totalRowCount())
	}
	err = h.dumpStatsUpdateToKV(tableID, isIndex, q, hist, cms)
	return errors.Trace(err)
}

func (h *Handle) deleteOutdatedFeedback(tableID, histID, isIndex int64) error {
	h.ctx.GetSessionVars().BatchDelete = true
	sql := fmt.Sprintf("delete from mysql.stats_feedback where table_id = %d and hist_id = %d and is_index = %d", tableID, histID, isIndex)
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), sql)
	h.ctx.GetSessionVars().BatchDelete = false
	return errors.Trace(err)
}

func (h *Handle) dumpStatsUpdateToKV(tableID, isIndex int64, q *QueryFeedback, hist *Histogram, cms *CMSketch) error {
	hist = UpdateHistogram(hist, q)
	err := SaveStatsToStorage(h.ctx, tableID, -1, int(isIndex), hist, cms)
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
func TableAnalyzed(tbl *Table) bool {
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

// needAnalyzeTable checks if we need to analyze the table:
// 1. If the table has never been analyzed, we need to analyze it when it has
//    not been modified for a time.
// 2. If the table had been analyzed before, we need to analyze it when
//    "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio".
func needAnalyzeTable(tbl *Table, limit time.Duration, autoAnalyzeRatio float64) bool {
	analyzed := TableAnalyzed(tbl)
	if !analyzed {
		t := time.Unix(0, oracle.ExtractPhysical(tbl.Version)*int64(time.Millisecond))
		return time.Since(t) >= limit
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false
	}
	return float64(tbl.ModifyCount)/float64(tbl.Count) > autoAnalyzeRatio
}

const minAutoAnalyzeRatio = 0.3

func getAutoAnalyzeRatio(sctx sessionctx.Context) float64 {
	sql := fmt.Sprintf("select variable_value from mysql.global_variables where variable_name = '%s'", variable.TiDBAutoAnalyzeRatio)
	rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sctx, sql)
	if err != nil {
		return variable.DefAutoAnalyzeRatio
	}
	autoAnalyzeRatio := variable.DefAutoAnalyzeRatio
	if len(rows) > 0 {
		autoAnalyzeRatio, err = strconv.ParseFloat(rows[0].GetString(0), 64)
		if err != nil {
			return variable.DefAutoAnalyzeRatio
		}
	}
	if autoAnalyzeRatio > 0 {
		autoAnalyzeRatio = math.Max(autoAnalyzeRatio, minAutoAnalyzeRatio)
	}
	return autoAnalyzeRatio
}

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) error {
	dbs := is.AllSchemaNames()
	autoAnalyzeRatio := getAutoAnalyzeRatio(h.ctx)
	for _, db := range dbs {
		tbls := is.SchemaTables(model.NewCIStr(db))
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			statsTbl := h.GetTableStats(tblInfo)
			if statsTbl.Pseudo || statsTbl.Count < AutoAnalyzeMinCnt {
				continue
			}
			tblName := "`" + db + "`.`" + tblInfo.Name.O + "`"
			if needAnalyzeTable(statsTbl, 20*h.Lease, autoAnalyzeRatio) {
				sql := fmt.Sprintf("analyze table %s", tblName)
				log.Infof("[stats] auto analyze table %s now", tblName)
				return errors.Trace(h.execAutoAnalyze(sql))
			}
			for _, idx := range tblInfo.Indices {
				if idx.State != model.StatePublic {
					continue
				}
				if _, ok := statsTbl.Indices[idx.ID]; !ok {
					sql := fmt.Sprintf("analyze table %s index `%s`", tblName, idx.Name.O)
					log.Infof("[stats] auto analyze index `%s` for table %s now", idx.Name.O, tblName)
					return errors.Trace(h.execAutoAnalyze(sql))
				}
			}
		}
	}
	return nil
}

func (h *Handle) execAutoAnalyze(sql string) error {
	startTime := time.Now()
	_, _, err := h.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(h.ctx, sql)
	metrics.AutoAnalyzeHistogram.Observe(time.Since(startTime).Seconds())
	if err != nil {
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	}
	return errors.Trace(err)
}

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
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
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
		if len(lq) >= maxQueryFeedBackCount {
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
	if len(s.feedback) >= maxQueryFeedBackCount {
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
		m := h.globalMap[id]
		m.ColSize = nil
		h.globalMap[id] = m
		if updated {
			delete(h.globalMap, id)
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
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count - %d, modify_count = modify_count + %d where table_id = %d and count >= %d", h.ctx.Txn().StartTS(), -delta.Delta, delta.Count, id, -delta.Delta)
	} else {
		sql = fmt.Sprintf("update mysql.stats_meta set version = %d, count = count + %d, modify_count = modify_count + %d where table_id = %d", h.ctx.Txn().StartTS(), delta.Delta, delta.Count, id)
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
	if delta.ColSize == nil {
		return nil
	}
	ctx := context.TODO()
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	version := h.ctx.Txn().StartTS()

	for key, val := range delta.ColSize {
		sql := fmt.Sprintf("update mysql.stats_histograms set version = %d, tot_col_size = tot_col_size + %d where hist_id = %d and table_id = %d and is_index = 0", version, val, key, id)
		_, err = h.ctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(ctx, "commit")
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

func needAnalyzeTable(tbl *Table, limit time.Duration) bool {
	if tbl.ModifyCount == 0 || tbl.Count < AutoAnalyzeMinCnt {
		return false
	}
	t := time.Unix(0, oracle.ExtractPhysical(tbl.Version)*int64(time.Millisecond))
	if time.Since(t) < limit {
		return false
	}
	for _, col := range tbl.Columns {
		if col.Count > 0 {
			return false
		}
	}
	for _, idx := range tbl.Indices {
		if idx.Len() > 0 {
			return false
		}
	}
	return true
}

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) error {
	dbs := is.AllSchemaNames()
	for _, db := range dbs {
		tbls := is.SchemaTables(model.NewCIStr(db))
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			statsTbl := h.GetTableStats(tblInfo.ID)
			if statsTbl.Pseudo || statsTbl.Count == 0 {
				continue
			}
			tblName := "`" + db + "`.`" + tblInfo.Name.O + "`"
			if needAnalyzeTable(statsTbl, 20*h.Lease) {
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

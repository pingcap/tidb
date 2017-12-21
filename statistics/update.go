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
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

type tableDeltaMap map[int64]variable.TableDelta

func (m tableDeltaMap) update(id int64, delta int64, count int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	m[id] = item
}

func (h *Handle) merge(s *SessionStatsCollector) {
	s.Lock()
	defer s.Unlock()
	for id, item := range s.mapper {
		h.globalMap.update(id, item.Delta, item.Count)
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
func (s *SessionStatsCollector) Update(id int64, delta int64, count int64) {
	s.Lock()
	defer s.Unlock()
	s.mapper.update(id, delta, count)
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
	if q.histVersion == 0 || q.actual < 0 || !q.valid {
		return
	}
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
func (h *Handle) DumpStatsDeltaToKV() {
	h.listHead.Lock()
	for collector := h.listHead.next; collector != nil; collector = collector.next {
		collector.tryToRemoveFromList()
		h.merge(collector)
	}
	h.listHead.Unlock()
	for id, item := range h.globalMap {
		err := h.dumpTableStatDeltaToKV(id, item)
		if err == nil {
			delete(h.globalMap, id)
		} else {
			log.Warnf("Error happens when updating stats table, the error message is %s.", err.Error())
		}
	}
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
func (h *Handle) dumpTableStatDeltaToKV(id int64, delta variable.TableDelta) error {
	if delta.Count == 0 {
		return nil
	}
	goCtx := goctx.TODO()
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute(goCtx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	op := "+"
	if delta.Delta < 0 {
		op = "-"
		delta.Delta = -delta.Delta
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(goCtx, fmt.Sprintf("update mysql.stats_meta set version = %d, count = count %s %d, modify_count = modify_count + %d where table_id = %d", h.ctx.Txn().StartTS(), op, delta.Delta, delta.Count, id))
	if err != nil {
		return errors.Trace(err)
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(goCtx, "commit")
	return errors.Trace(err)
}

// QueryFeedback is used to represent the query feedback info. It contains the expected scan row count and
// the actual scan row count, so that we could use these info to adjust the statistics.
type QueryFeedback struct {
	tableID     int64
	colID       int64
	isIndex     bool
	idxRanges   []*ranger.IndexRange
	intRanges   []ranger.IntColumnRange
	histVersion uint64 // histVersion is the version of the histogram when we issue the query.
	expected    int64
	actual      int64
	valid       bool
}

// NewQueryFeedback returns a new query feedback.
func NewQueryFeedback(tableID int64, colID int64, isIndex bool, histVer uint64, expected int64) *QueryFeedback {
	return &QueryFeedback{
		tableID:     tableID,
		colID:       colID,
		isIndex:     isIndex,
		histVersion: histVer,
		expected:    expected,
		valid:       true,
	}
}

// SetIndexRanges sets the index ranges.
func (q *QueryFeedback) SetIndexRanges(ranges []*ranger.IndexRange) *QueryFeedback {
	q.idxRanges = ranges
	return q
}

// SetIntRanges sets the int column ranges.
func (q *QueryFeedback) SetIntRanges(ranges []ranger.IntColumnRange) *QueryFeedback {
	q.intRanges = ranges
	return q
}

// SetActual sets the actual count.
func (q *QueryFeedback) SetActual(count int64) *QueryFeedback {
	q.actual = count
	return q
}

// Invalidate is used to invalidate the query feedback.
func (q *QueryFeedback) Invalidate() {
	q.valid = false
}

// Actual gets the actual row count.
func (q *QueryFeedback) Actual() int64 {
	return q.actual
}

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

func needAnalyzeTable(tbl *Table, limit time.Duration) bool {
	if tbl.ModifyCount == 0 {
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
		if len(idx.Buckets) > 0 {
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
	autoAnalyzeHistgram.Observe(time.Since(startTime).Seconds())
	if err != nil {
		autoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		autoAnalyzeCounter.WithLabelValues("succ").Inc()
	}
	return errors.Trace(err)
}

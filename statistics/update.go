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
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/sqlexec"
)

type tableDeltaMap map[int64]variable.TableDelta

func (m tableDeltaMap) update(id int64, delta int64, count int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	m[id] = item
}

func (m tableDeltaMap) merge(handle *SessionStatsCollector) {
	handle.Lock()
	defer handle.Unlock()
	for id, item := range handle.mapper {
		m.update(id, item.Delta, item.Count)
	}
	handle.mapper = make(tableDeltaMap)
}

// SessionStatsCollector is a list item that holds the delta mapper. If you want to write or read mapper, you must lock it.
type SessionStatsCollector struct {
	sync.Mutex

	mapper tableDeltaMap
	prev   *SessionStatsCollector
	next   *SessionStatsCollector
	// If a session is closed, it only sets this flag true. Every time we sweep the list, we will remove the useless collector.
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
		h.globalMap.merge(collector)
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
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	op := "+"
	if delta.Delta < 0 {
		op = "-"
		delta.Delta = -delta.Delta
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("update mysql.stats_meta set version = %d, count = count %s %d, modify_count = modify_count + %d where table_id = %d", h.ctx.Txn().StartTS(), op, delta.Delta, delta.Count, id))
	if err != nil {
		return errors.Trace(err)
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute("commit")
	return errors.Trace(err)
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
		if len(col.Buckets) > 0 {
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

func encode(dbName, tblName, idxName string) string {
	return dbName + "`" + tblName + "`" + idxName
}

func decode(str string) (string, string, string) {
	strs := strings.Split(str, "`")
	return strs[0], strs[1], strs[2]
}

func (h *Handle) needAnalyze(is infoschema.InfoSchema, dbName, tblName, idxName string) bool {
	tbl, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
	if tbl == nil || err != nil {
		return false
	}
	tblInfo := tbl.Meta()
	statsTbl := h.GetTableStats(tblInfo.ID)
	if statsTbl.Pseudo || statsTbl.Count == 0 {
		return false
	}
	if idxName == "" {
		return needAnalyzeTable(statsTbl, 20*h.Lease)
	}
	for _, idx := range tblInfo.Indices {
		if idx.Name.O == idxName {
			if _, ok := statsTbl.Indices[idx.ID]; !ok {
				return true
			}
			return false
		}
	}
	return false
}

// GetAutoAnalyzeJobs returns all the tables and indices that need to be analyzed.
func (h *Handle) GetAutoAnalyzeJobs(is infoschema.InfoSchema) []string {
	dbs := is.AllSchemaNames()
	var jobs []string
	for _, db := range dbs {
		tbls := is.SchemaTables(model.NewCIStr(db))
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			statsTbl := h.GetTableStats(tblInfo.ID)
			if statsTbl.Pseudo || statsTbl.Count == 0 {
				continue
			}
			if needAnalyzeTable(statsTbl, 20*h.Lease) {
				jobs = append(jobs, encode(db, tblInfo.Name.O, ""))
				continue
			}
			for _, idx := range tblInfo.Indices {
				if _, ok := statsTbl.Indices[idx.ID]; !ok {
					jobs = append(jobs, encode(db, tblInfo.Name.O, idx.Name.O))
				}
			}
		}
	}
	return jobs
}

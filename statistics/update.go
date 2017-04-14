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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/sqlexec"
)

type tableDeltaMap map[int64]*variable.TableDelta

func (m tableDeltaMap) update(id int64, delta int64, count int64) {
	item, ok := m[id]
	if !ok {
		m[id] = &variable.TableDelta{Delta: delta, Count: count}
	} else {
		item.Delta += delta
		item.Count += count
	}
}

func (m tableDeltaMap) merge(handle *SessionStatsCollector) {
	handle.Lock()
	defer handle.Unlock()
	for id, item := range handle.mapper {
		m.update(id, item.Delta, item.Count)
	}
	handle.mapper = make(tableDeltaMap)
}

// SessionStatsCollector is a list item that holds the update mapper. If you want write or read mapper, you must add the lock.
type SessionStatsCollector struct {
	sync.Mutex

	mapper tableDeltaMap
	prev   *SessionStatsCollector
	next   *SessionStatsCollector
}

// Update will updates the delta and count for one table id.
func (h *SessionStatsCollector) Update(id int64, delta int64, count int64) {
	h.Lock()
	defer h.Unlock()
	h.mapper.update(id, delta, count)
}

// updateManager management the delta information in tidb. It collects the delta map from all sessions and write them to tikv.
type updateManager struct {
	listHead *SessionStatsCollector
	mapper   struct {
		sync.Mutex
		tableDeltaMap
	}
	ctx context.Context
}

func (m *updateManager) newSessionStatsCollector() *SessionStatsCollector {
	m.listHead.Lock()
	defer m.listHead.Unlock()
	newHandle := &SessionStatsCollector{
		mapper: make(tableDeltaMap),
		next:   m.listHead.next,
		prev:   m.listHead,
	}
	if m.listHead.next != nil {
		m.listHead.next.prev = newHandle
	}
	m.listHead.next = newHandle
	return newHandle
}

func (m *updateManager) delStatsUpdateHandle(handle *SessionStatsCollector) {
	m.listHead.Lock()
	nextHandle := handle.next
	prevHandle := handle.prev
	prevHandle.next = nextHandle
	if nextHandle != nil {
		nextHandle.prev = prevHandle
	}
	m.listHead.Unlock()
	m.mapper.Lock()
	m.mapper.merge(handle)
	m.mapper.Unlock()
}

func (m *updateManager) dumpUpdateMapper2KV() {
	m.listHead.Lock()
	m.mapper.Lock()
	for item := m.listHead.next; item != nil; item = item.next {
		m.mapper.merge(item)
	}
	m.listHead.Unlock()
	for id, item := range m.mapper.tableDeltaMap {
		err := m.dumpTableStatToKV(id, item)
		if err == nil {
			delete(m.mapper.tableDeltaMap, id)
		} else {
			log.Warnf("Error happens when updating stats table, the error message is %s.", err.Error())
		}
	}
	m.mapper.Unlock()
}

func (m *updateManager) dumpTableStatToKV(id int64, delta *variable.TableDelta) error {
	_, err := m.ctx.(sqlexec.SQLExecutor).Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("update mysql.stats_meta set version = %d, count = count + %d, modify_count = modify_count + %d where table_id = %d", m.ctx.Txn().StartTS(), delta.Delta, delta.Count, id))
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.ctx.(sqlexec.SQLExecutor).Execute("commit")
	return errors.Trace(err)
}

// NewStatsUpdateHandle creates a new stats update handle. It is called when a session is created first time.
func (h *Handle) NewStatsUpdateHandle() *SessionStatsCollector {
	return h.updateManager.newSessionStatsCollector()
}

// DelStatsUpdateHandle deletes a stats update handle from updateManager. It is called when a session is closed.
func (h *Handle) DelStatsUpdateHandle(handle *SessionStatsCollector) {
	h.updateManager.delStatsUpdateHandle(handle)
}

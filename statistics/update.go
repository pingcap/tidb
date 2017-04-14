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
	"github.com/pingcap/tidb/util/sqlexec"
)

type updateItem struct {
	delta int64
	count int64
}

type updateMapper map[int64]*updateItem

func (m updateMapper) update(id int64, delta int64, count int64) {
	item, ok := m[id]
	if !ok {
		m[id] = &updateItem{delta: delta, count: count}
	} else {
		item.delta += delta
		item.count += count
	}
}

func (m updateMapper) merge(handle *StatsUpdateHandle) {
	handle.Lock()
	defer handle.Unlock()
	for id, item := range handle.mapper {
		m.update(id, item.delta, item.count)
	}
	handle.mapper = make(updateMapper)
}

// StatsUpdateHandle is a list item that holds the update mapper. If you want write or read mapper, you must add the lock.
type StatsUpdateHandle struct {
	sync.Mutex

	mapper updateMapper
	prev   *StatsUpdateHandle
	next   *StatsUpdateHandle
}

// Update will updates the delta and count for one table id.
func (h *StatsUpdateHandle) Update(id int64, delta int64, count int64) {
	h.Lock()
	defer h.Unlock()
	h.mapper.update(id, delta, count)
}

type updateManager struct {
	listHead *StatsUpdateHandle
	mapper   struct {
		sync.Mutex
		updateMapper
	}
	ctx context.Context
}

func (m *updateManager) newStatsUpdateHandle() *StatsUpdateHandle {
	m.listHead.Lock()
	defer m.listHead.Unlock()
	mapper := &StatsUpdateHandle{
		mapper: make(updateMapper),
		next:   m.listHead.next,
		prev:   m.listHead,
	}
	if m.listHead.next != nil {
		m.listHead.next.prev = mapper
	}
	m.listHead.next = mapper
	return mapper
}

func (m *updateManager) delStatsUpdateHandle(handle *StatsUpdateHandle) {
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
	for id, item := range m.mapper.updateMapper {
		err := m.dumpTableStatToKV(id, item)
		if err == nil {
			delete(m.mapper.updateMapper, id)
		} else {
			log.Warnf("Error happens when updating stats table, the error message is %s.", err.Error())
		}
	}
	m.mapper.Unlock()
}

func (m *updateManager) dumpTableStatToKV(id int64, item *updateItem) error {
	_, err := m.ctx.(sqlexec.SQLExecutor).Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("update mysql.stats_meta set version = %d, count = count + %d, modify_count = modify_count + %d where table_id = %d", m.ctx.Txn().StartTS(), item.delta, item.count, id))
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.ctx.(sqlexec.SQLExecutor).Execute("commit")
	return errors.Trace(err)
}

// NewStatsUpdateHandle creates a new stats update handle. It is called when a session is created first time.
func (h *Handle) NewStatsUpdateHandle() *StatsUpdateHandle {
	return h.updateManager.newStatsUpdateHandle()
}

// DelStatsUpdateHandle deletes a stats update handle from updateManager. It is called when a session is closed.
func (h *Handle) DelStatsUpdateHandle(handle *StatsUpdateHandle) {
	h.updateManager.delStatsUpdateHandle(handle)
}

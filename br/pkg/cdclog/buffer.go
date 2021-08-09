// Copyright 2020 PingCAP, Inc.
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

package cdclog

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

// TableBuffer represents the kv buffer of this table.
// we restore one tableBuffer in one goroutine.
// this is the concurrent unit of log restore.
type TableBuffer struct {
	KvPairs []kv.Row
	count   int
	size    int64

	KvEncoder kv.Encoder
	tableInfo table.Table
	allocator autoid.Allocators

	flushKVSize  int64
	flushKVPairs int

	colNames []string
	colPerm  []int
}

func newKVEncoder(allocators autoid.Allocators, tbl table.Table) (kv.Encoder, error) {
	encTable, err := table.TableFromMeta(allocators, tbl.Meta())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return kv.NewTableKVEncoder(encTable, &kv.SessionOptions{
		Timestamp: time.Now().Unix(),
		// TODO get the version from TiDB cluster
		// currently TiDB only support v1 and v2, and since 4.0
		// the default RowFormatVersion is 2, so I think
		// we can implement the row version retrieve from cluster in the future
		// when TiDB decide to support v3 RowFormatVersion.
		RowFormatVersion: "2",
	}), nil
}

// NewTableBuffer creates TableBuffer.
func NewTableBuffer(tbl table.Table, allocators autoid.Allocators, flushKVPairs int, flushKVSize int64) *TableBuffer {
	tb := &TableBuffer{
		KvPairs:      make([]kv.Row, 0, flushKVPairs),
		flushKVPairs: flushKVPairs,
		flushKVSize:  flushKVSize,
	}
	if tbl != nil {
		tb.ReloadMeta(tbl, allocators)
	}
	return tb
}

// ResetTableInfo set tableInfo to nil for next reload.
func (t *TableBuffer) ResetTableInfo() {
	t.tableInfo = nil
}

// TableInfo returns the table info of this buffer.
func (t *TableBuffer) TableInfo() table.Table {
	return t.tableInfo
}

// TableID returns the table id of this buffer.
func (t *TableBuffer) TableID() int64 {
	if t.tableInfo != nil {
		return t.tableInfo.Meta().ID
	}
	return 0
}

// ReloadMeta reload columns after
// 1. table buffer created.
// 2. every ddl executed.
func (t *TableBuffer) ReloadMeta(tbl table.Table, allocator autoid.Allocators) {
	columns := tbl.Meta().Cols()
	colNames := make([]string, 0, len(columns))
	colPerm := make([]int, 0, len(columns)+1)

	for i, col := range columns {
		colNames = append(colNames, col.Name.String())
		colPerm = append(colPerm, i)
	}
	if kv.TableHasAutoRowID(tbl.Meta()) {
		colPerm = append(colPerm, -1)
	}
	if t.allocator == nil {
		t.allocator = allocator
	}
	t.tableInfo = tbl
	t.colNames = colNames
	t.colPerm = colPerm
	// reset kv encoder after meta changed
	t.KvEncoder = nil
}

func (t *TableBuffer) translateToDatum(row map[string]Column) ([]types.Datum, error) {
	cols := make([]types.Datum, 0, len(row))
	for _, col := range t.colNames {
		val, err := row[col].ToDatum()
		if err != nil {
			return nil, errors.Trace(err)
		}
		cols = append(cols, val)
	}
	return cols, nil
}

func (t *TableBuffer) appendRow(
	row map[string]Column,
	item *SortItem,
	encodeFn func(row []types.Datum,
		rowID int64,
		columnPermutation []int) (kv.Row, int, error),
) error {
	cols, err := t.translateToDatum(row)
	if err != nil {
		return errors.Trace(err)
	}
	pair, size, err := encodeFn(cols, item.RowID, t.colPerm)
	if err != nil {
		return errors.Trace(err)
	}
	t.KvPairs = append(t.KvPairs, pair)
	t.size += int64(size)
	t.count++
	return nil
}

// Append appends the item to this buffer.
func (t *TableBuffer) Append(item *SortItem) error {
	var err error
	log.Debug("Append item to buffer",
		zap.Stringer("table", t.tableInfo.Meta().Name),
		zap.Any("item", item),
	)
	row := item.Data.(*MessageRow)

	if t.KvEncoder == nil {
		// lazy create kv encoder
		log.Debug("create kv encoder lazily",
			zap.Any("alloc", t.allocator), zap.Any("tbl", t.tableInfo))
		t.KvEncoder, err = newKVEncoder(t.allocator, t.tableInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if row.PreColumns != nil {
		// remove old keys
		log.Debug("process update event", zap.Any("row", row))
		err := t.appendRow(row.PreColumns, item, t.KvEncoder.RemoveRecord)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if row.Update != nil {
		// Add new columns
		if row.PreColumns == nil {
			log.Debug("process insert event", zap.Any("row", row))
		}
		err := t.appendRow(row.Update, item, t.KvEncoder.AddRecord)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if row.Delete != nil {
		// Remove current columns
		log.Debug("process delete event", zap.Any("row", row))
		err := t.appendRow(row.Delete, item, t.KvEncoder.RemoveRecord)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ShouldApply tells whether we should flush memory kv buffer to storage.
func (t *TableBuffer) ShouldApply() bool {
	// flush when reached flush kv len or flush size
	return t.size >= t.flushKVSize || t.count >= t.flushKVPairs
}

// IsEmpty tells buffer is empty.
func (t *TableBuffer) IsEmpty() bool {
	return t.size == 0
}

// Clear reset the buffer.
func (t *TableBuffer) Clear() {
	t.KvPairs = t.KvPairs[:0]
	t.count = 0
	t.size = 0
}

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package tables

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
)

var (
	errRowNotFound = errors.New("Can not find the row")
)

// MemoryTable implements table.Table interface.
type MemoryTable struct {
	ID      int64
	Name    model.CIStr
	Columns []*column.Col

	recordPrefix kv.Key
	alloc        autoid.Allocator
	meta         *model.TableInfo

	rows [][]interface{}
}

// MemoryTableFromMeta creates a Table instance from model.TableInfo.
func MemoryTableFromMeta(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error) {
	columns := make([]*column.Col, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		col := &column.Col{ColumnInfo: *colInfo}
		columns = append(columns, col)
	}
	t := newMemoryTable(tblInfo.ID, tblInfo.Name.O, columns, alloc)
	t.meta = tblInfo
	return t, nil
}

// newMemoryTable constructs a MemoryTable instance.
func newMemoryTable(tableID int64, tableName string, cols []*column.Col, alloc autoid.Allocator) *MemoryTable {
	name := model.NewCIStr(tableName)
	t := &MemoryTable{
		ID:           tableID,
		Name:         name,
		alloc:        alloc,
		Columns:      cols,
		recordPrefix: genTableRecordPrefix(tableID),
		rows:         [][]interface{}{},
	}
	return t
}

// Seek seeks the handle
func (t *MemoryTable) Seek(ctx context.Context, handle int64) (int64, bool, error) {
	if handle < 0 {
		handle = 0
	}
	if handle >= int64(len(t.rows)) {
		return 0, false, nil
	}
	return handle, true, nil
}

// Indices implements table.Table Indices interface.
func (t *MemoryTable) Indices() []*column.IndexedCol {
	return nil
}

// Meta implements table.Table Meta interface.
func (t *MemoryTable) Meta() *model.TableInfo {
	return t.meta
}

// Cols implements table.Table Cols interface.
func (t *MemoryTable) Cols() []*column.Col {
	return t.Columns
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (t *MemoryTable) RecordPrefix() kv.Key {
	return t.recordPrefix
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (t *MemoryTable) IndexPrefix() kv.Key {
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (t *MemoryTable) RecordKey(h int64, col *column.Col) kv.Key {
	colID := int64(0)
	if col != nil {
		colID = col.ID
	}
	return encodeRecordKey(t.recordPrefix, h, colID)
}

// FirstKey implements table.Table FirstKey interface.
func (t *MemoryTable) FirstKey() kv.Key {
	return t.RecordKey(0, nil)
}

// Truncate implements table.Table Truncate interface.
func (t *MemoryTable) Truncate(ctx context.Context) error {
	t.rows = [][]interface{}{}
	return nil
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (t *MemoryTable) UpdateRecord(ctx context.Context, h int64, oldData []interface{}, newData []interface{}, touched map[int]bool) error {
	// Unsupport
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *MemoryTable) AddRecord(ctx context.Context, r []interface{}) (recordID int64, err error) {
	recordID = int64(len(t.rows))
	t.rows = append(t.rows, r)
	return
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *MemoryTable) RowWithCols(ctx context.Context, h int64, cols []*column.Col) ([]interface{}, error) {
	if h >= int64(len(t.rows)) || h < 0 {
		return nil, errRowNotFound
	}
	row := t.rows[h]
	if row == nil {
		return nil, errRowNotFound
	}
	v := make([]interface{}, len(cols))
	for i, col := range cols {
		v[i] = row[col.Offset]
	}
	return v, nil
}

// Row implements table.Table Row interface.
func (t *MemoryTable) Row(ctx context.Context, h int64) ([]interface{}, error) {
	r, err := t.RowWithCols(nil, h, t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r, nil
}

// LockRow implements table.Table LockRow interface.
func (t *MemoryTable) LockRow(ctx context.Context, h int64, forRead bool) error {
	return nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *MemoryTable) RemoveRecord(ctx context.Context, h int64, r []interface{}) error {
	if h >= int64(len(t.rows)) || h < 0 {
		return errRowNotFound
	}
	t.rows[h] = nil
	return nil
}

// AllocAutoID implements table.Table AllocAutoID interface.
func (t *MemoryTable) AllocAutoID() (int64, error) {
	return t.alloc.Alloc(t.ID)
}

// IterRecords implements table.Table IterRecords interface.
func (t *MemoryTable) IterRecords(ctx context.Context, startKey kv.Key, cols []*column.Col,
	fn table.RecordIterFunc) error {
	return nil
}

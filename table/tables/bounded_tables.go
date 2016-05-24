// Copyright 2016 PingCAP, Inc.
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
	"sync/atomic"
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/types"
)

const (
	// We want to ensure our initial record id greater than a threshold.
	initialRecordID int64 = 123
	// In our case, a valid record will always greater than 0, so we use a
	// macro to mark an invalid one.
	invalidRecordID int64 = 0
)

var (
	errOpNotSupported = errors.New("Operation not supported")
)

type boundedItem struct {
	handle int64
	data   []types.Datum
}

// BoundedTable implements table.Table interface.
type BoundedTable struct {
	// cursor must be aligned in i386, or we will meet panic when calling atomic.AddInt64
	// https://golang.org/src/sync/atomic/doc.go#L41
	cursor      int64
	ID          int64
	Name        model.CIStr
	Columns     []*table.Column
	pkHandleCol *table.Column

	recordPrefix kv.Key
	alloc        autoid.Allocator
	meta         *model.TableInfo

	records  []unsafe.Pointer
	capacity int64
}

// BoundedTableFromMeta creates a Table instance from model.TableInfo.
func BoundedTableFromMeta(alloc autoid.Allocator, tblInfo *model.TableInfo, capacity int64) table.Table {
	columns := make([]*table.Column, 0, len(tblInfo.Columns))
	var pkHandleColumn *table.Column
	for _, colInfo := range tblInfo.Columns {
		col := &table.Column{ColumnInfo: *colInfo}
		columns = append(columns, col)
		if col.IsPKHandleColumn(tblInfo) {
			pkHandleColumn = col
		}
	}
	t := newBoundedTable(tblInfo.ID, tblInfo.Name.O, columns, alloc, capacity)
	t.pkHandleCol = pkHandleColumn
	t.meta = tblInfo
	return t
}

// newBoundedTable constructs a BoundedTable instance.
func newBoundedTable(tableID int64, tableName string, cols []*table.Column, alloc autoid.Allocator, capacity int64) *BoundedTable {
	name := model.NewCIStr(tableName)
	t := &BoundedTable{
		ID:           tableID,
		Name:         name,
		alloc:        alloc,
		Columns:      cols,
		recordPrefix: genTableRecordPrefix(tableID),
		records:      make([]unsafe.Pointer, capacity),
		capacity:     capacity,
		cursor:       0,
	}
	return t
}

// newBoundedItem constructs a boundedItem instance.
func newBoundedItem(handle int64, data []types.Datum) *boundedItem {
	i := &boundedItem{
		handle: handle,
		data:   data,
	}
	return i
}

// Seek seeks the handle.
func (t *BoundedTable) Seek(ctx context.Context, handle int64) (int64, bool, error) {
	result := (*boundedItem)(nil)
	if handle < invalidRecordID {
		// this is the first seek call.
		result = (*boundedItem)(atomic.LoadPointer(&t.records[0]))
	} else {
		for i := int64(0); i < t.capacity; i++ {
			record := (*boundedItem)(atomic.LoadPointer(&t.records[i]))
			if record == nil {
				break
			}
			if handle == record.handle {
				result = record
				break
			}
		}
	}
	if result == nil {
		// handle not found.
		return invalidRecordID, false, nil
	}
	if result.handle != invalidRecordID {
		// this record is valid.
		return result.handle, true, nil
	}
	// this record is invalid.
	return invalidRecordID, false, nil
}

// Indices implements table.Table Indices interface.
func (t *BoundedTable) Indices() []*table.IndexedColumn {
	return nil
}

// Meta implements table.Table Meta interface.
func (t *BoundedTable) Meta() *model.TableInfo {
	return t.meta
}

// Cols implements table.Table Cols interface.
func (t *BoundedTable) Cols() []*table.Column {
	return t.Columns
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (t *BoundedTable) RecordPrefix() kv.Key {
	return t.recordPrefix
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (t *BoundedTable) IndexPrefix() kv.Key {
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (t *BoundedTable) RecordKey(h int64, col *table.Column) kv.Key {
	colID := int64(0)
	if col != nil {
		colID = col.ID
	}
	return encodeRecordKey(t.recordPrefix, h, colID)
}

// FirstKey implements table.Table FirstKey interface.
func (t *BoundedTable) FirstKey() kv.Key {
	return t.RecordKey(0, nil)
}

// Truncate implements table.Table Truncate interface.
func (t *BoundedTable) Truncate(ctx context.Context) error {
	// just reset everything.
	for i := int64(0); i < t.capacity; i++ {
		atomic.StorePointer(&t.records[i], unsafe.Pointer(nil))
	}
	t.cursor = 0
	return nil
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (t *BoundedTable) UpdateRecord(ctx context.Context, h int64, oldData []types.Datum, newData []types.Datum, touched map[int]bool) error {
	for i := int64(0); i < t.capacity; i++ {
		record := (*boundedItem)(atomic.LoadPointer(&t.records[i]))
		if record == nil {
			// A nil record means consecutive nil records.
			break
		}
		if record.handle == h {
			newRec := newBoundedItem(h, newData)
			atomic.StorePointer(&t.records[i], unsafe.Pointer(newRec))
			break
		}
	}
	// UPDATE always succeeds for BoundedTable.
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *BoundedTable) AddRecord(ctx context.Context, r []types.Datum) (int64, error) {
	var recordID int64
	var err error
	if t.pkHandleCol != nil {
		recordID, err = types.ToInt64(r[t.pkHandleCol.Offset].GetValue())
		if err != nil {
			return invalidRecordID, errors.Trace(err)
		}
	} else {
		recordID, err = t.alloc.Alloc(t.ID)
		if err != nil {
			return invalidRecordID, errors.Trace(err)
		}
	}
	cursor := atomic.AddInt64(&t.cursor, 1) - 1
	index := int64(cursor % t.capacity)
	record := newBoundedItem(recordID+initialRecordID, r)
	atomic.StorePointer(&t.records[index], unsafe.Pointer(record))
	return recordID + initialRecordID, nil
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *BoundedTable) RowWithCols(ctx context.Context, h int64, cols []*table.Column) ([]types.Datum, error) {
	row := []types.Datum(nil)
	for i := int64(0); i < t.capacity; i++ {
		record := (*boundedItem)(atomic.LoadPointer(&t.records[i]))
		if record == nil {
			// A nil record means consecutive nil records.
			break
		}
		if record.handle == h {
			row = record.data
			break
		}
	}
	if row == nil {
		return nil, errRowNotFound
	}
	v := make([]types.Datum, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		v[i] = row[col.Offset]
	}
	return v, nil
}

// Row implements table.Table Row interface.
func (t *BoundedTable) Row(ctx context.Context, h int64) ([]types.Datum, error) {
	r, err := t.RowWithCols(nil, h, t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r, nil
}

// LockRow implements table.Table LockRow interface.
func (t *BoundedTable) LockRow(ctx context.Context, h int64, forRead bool) error {
	return nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *BoundedTable) RemoveRecord(ctx context.Context, h int64, r []types.Datum) error {
	// not supported, BoundedTable is TRUNCATE only
	return errOpNotSupported
}

// AllocAutoID implements table.Table AllocAutoID interface.
func (t *BoundedTable) AllocAutoID() (int64, error) {
	recordID, err := t.alloc.Alloc(t.ID)
	if err != nil {
		return invalidRecordID, errors.Trace(err)
	}
	return recordID + initialRecordID, nil
}

// RebaseAutoID implements table.Table RebaseAutoID interface.
func (t *BoundedTable) RebaseAutoID(newBase int64, isSetStep bool) error {
	return t.alloc.Rebase(t.ID, newBase, isSetStep)
}

// IterRecords implements table.Table IterRecords interface.
func (t *BoundedTable) IterRecords(ctx context.Context, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc) error {
	return nil
}

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
	"reflect"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
)

var store kv.Storage

// MemoryTable implements table.Table interface.
type MemoryTable struct {
	ID      int64
	Name    model.CIStr
	Columns []*column.Col

	recordPrefix kv.Key
	alloc        autoid.Allocator
	meta         *model.TableInfo

	//store kv.Storage
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

type iterator struct {
	rows   [][]interface{}
	cursor int
	tid    int64
}

func (it *iterator) Valid() bool {
	if it.cursor < 0 || it.cursor >= len(it.rows) {
		return false
	}
	return true
}

func (it *iterator) Key() kv.Key {
	return EncodeRecordKey(it.tid, int64(it.cursor), 0)
}

func (it *iterator) Value() []byte {
	return nil
}

func (it *iterator) Next() error {
	it.cursor++
	return nil
}

func (it *iterator) Close() {
	it.cursor = -1
	return
}

// Seek seeks the handle
func (t *MemoryTable) Seek(ctx context.Context, handle int64) (kv.Iterator, error) {
	it := &iterator{
		rows:   t.rows,
		cursor: 0,
		tid:    t.TableID(),
	}
	if handle < 0 {
		handle = 0
	}
	if handle >= int64(len(t.rows)) {
		it.cursor = -1
		return it, nil
	}
	it.cursor = int(handle)

	return it, nil
}

// TableID implements table.Table TableID interface.
func (t *MemoryTable) TableID() int64 {
	return t.ID
}

// Indices implements table.Table Indices interface.
func (t *MemoryTable) Indices() []*column.IndexedCol {
	return nil
}

// AddIndex implements table.Table AddIndex interface.
func (t *MemoryTable) AddIndex(idxCol *column.IndexedCol) {
	return
}

// TableName implements table.Table TableName interface.
func (t *MemoryTable) TableName() model.CIStr {
	return t.Name
}

// Meta implements table.Table Meta interface.
func (t *MemoryTable) Meta() *model.TableInfo {
	return t.meta
}

// Cols implements table.Table Cols interface.
func (t *MemoryTable) Cols() []*column.Col {
	return t.Columns
}

func (t *MemoryTable) unflatten(rec interface{}, col *column.Col) (interface{}, error) {
	if rec == nil {
		return nil, nil
	}
	switch col.Tp {
	case mysql.TypeFloat:
		return float32(rec.(float64)), nil
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeDouble, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob,
		mysql.TypeVarchar, mysql.TypeString:
		return rec, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t mysql.Time
		t.Type = col.Tp
		t.Fsp = col.Decimal
		err := t.Unmarshal(rec.([]byte))
		if err != nil {
			return nil, errors.Trace(err)
		}
		return t, nil
	case mysql.TypeDuration:
		return mysql.Duration{Duration: time.Duration(rec.(int64)), Fsp: col.Decimal}, nil
	case mysql.TypeNewDecimal, mysql.TypeDecimal:
		return mysql.ParseDecimal(string(rec.([]byte)))
	case mysql.TypeEnum:
		return mysql.ParseEnumValue(col.Elems, rec.(uint64))
	case mysql.TypeSet:
		return mysql.ParseSetValue(col.Elems, rec.(uint64))
	case mysql.TypeBit:
		return mysql.Bit{Value: rec.(uint64), Width: col.Flen}, nil
	}
	log.Error(col.Tp, rec, reflect.TypeOf(rec))
	return nil, nil
}

func (t *MemoryTable) flatten(data interface{}) (interface{}, error) {
	switch x := data.(type) {
	case mysql.Time:
		// for mysql datetime, timestamp and date type
		return x.Marshal()
	case mysql.Duration:
		// for mysql time type
		return int64(x.Duration), nil
	case mysql.Decimal:
		return x.String(), nil
	case mysql.Enum:
		return x.Value, nil
	case mysql.Set:
		return x.Value, nil
	case mysql.Bit:
		return x.Value, nil
	default:
		return data, nil
	}
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

// FindIndexByColName implements table.Table FindIndexByColName interface.
func (t *MemoryTable) FindIndexByColName(name string) *column.IndexedCol {
	return nil
}

// Truncate implements table.Table Truncate interface.
func (t *MemoryTable) Truncate(rm kv.RetrieverMutator) error {
	t.rows = [][]interface{}{}
	return nil
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (t *MemoryTable) UpdateRecord(ctx context.Context, h int64, oldData []interface{}, newData []interface{}, touched map[int]bool) error {
	// Unsupport
	return nil
}

// SetColValue implements table.Table SetColValue interface.
func (t *MemoryTable) SetColValue(rm kv.RetrieverMutator, key []byte, data interface{}) error {
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *MemoryTable) AddRecord(ctx context.Context, r []interface{}) (recordID int64, err error) {

	recordID = int64(len(t.rows))
	t.rows = append(t.rows, r)
	return
}

// EncodeValue implements table.Table EncodeValue interface.
func (t *MemoryTable) EncodeValue(raw interface{}) ([]byte, error) {
	v, err := t.flatten(raw)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := codec.EncodeValue(nil, v)
	return b, errors.Trace(err)
}

// DecodeValue implements table.Table DecodeValue interface.
func (t *MemoryTable) DecodeValue(data []byte, col *column.Col) (interface{}, error) {
	values, err := codec.Decode(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.unflatten(values[0], col)
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *MemoryTable) RowWithCols(retriever kv.Retriever, h int64, cols []*column.Col) ([]interface{}, error) {
	if h >= int64(len(t.rows)) || h < 0 {
		return nil, errors.New("Can not find the row")
	}
	row := t.rows[h]
	if row == nil {
		return nil, errors.New("Can not find row")
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
func (t *MemoryTable) LockRow(ctx context.Context, h int64) error {
	return nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *MemoryTable) RemoveRecord(ctx context.Context, h int64, r []interface{}) error {
	if h >= int64(len(t.rows)) || h < 0 {
		return errors.New("Can not find the row")
	}
	t.rows[h] = nil
	return nil
}

// RemoveRowIndex implements table.Table RemoveRowIndex interface.
func (t *MemoryTable) RemoveRowIndex(rm kv.RetrieverMutator, h int64, vals []interface{}, idx *column.IndexedCol) error {
	return nil
}

// BuildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *MemoryTable) BuildIndexForRow(rm kv.RetrieverMutator, h int64, vals []interface{}, idx *column.IndexedCol) error {
	return nil
}

// IterRecords implements table.Table IterRecords interface.
func (t *MemoryTable) IterRecords(retriever kv.Retriever, startKey kv.Key, cols []*column.Col,
	fn table.RecordIterFunc) error {
	return nil
}

// AllocAutoID implements table.Table AllocAutoID interface.
func (t *MemoryTable) AllocAutoID() (int64, error) {
	return t.alloc.Alloc(t.ID)
}

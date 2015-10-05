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
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/errors2"
)

// Table implements table.Table interface.
type Table struct {
	ID      int64
	Name    model.CIStr
	Columns []*column.Col

	indices      []*column.IndexedCol
	recordPrefix string
	indexPrefix  string
	alloc        autoid.Allocator
}

// TableFromMeta creates a Table instance from model.TableInfo.
func TableFromMeta(dbname string, alloc autoid.Allocator, tblInfo *model.TableInfo) table.Table {
	t := NewTable(tblInfo.ID, tblInfo.Name.O, dbname, nil, alloc)

	for _, colInfo := range tblInfo.Columns {
		c := column.Col{ColumnInfo: *colInfo}
		t.Columns = append(t.Columns, &c)
	}

	for _, idxInfo := range tblInfo.Indices {
		idx := &column.IndexedCol{
			IndexInfo: *idxInfo,
			X:         kv.NewKVIndex(t.indexPrefix, idxInfo.Name.L, idxInfo.Unique),
		}
		t.AddIndex(idx)
	}

	return t
}

// NewTable constructs a Table instance.
func NewTable(tableID int64, tableName string, dbName string, cols []*column.Col, alloc autoid.Allocator) *Table {
	name := model.NewCIStr(tableName)
	t := &Table{
		ID:           tableID,
		Name:         name,
		recordPrefix: fmt.Sprintf("%d_r", tableID),
		indexPrefix:  fmt.Sprintf("%d_i", tableID),
		alloc:        alloc,
		Columns:      cols,
	}
	return t
}

// TableID implements table.Table TableID interface.
func (t *Table) TableID() int64 {
	return t.ID
}

// Indices implements table.Table Indices interface.
func (t *Table) Indices() []*column.IndexedCol {
	return t.indices
}

// AddIndex implements table.Table AddIndex interface.
func (t *Table) AddIndex(idxCol *column.IndexedCol) {
	t.indices = append(t.indices, idxCol)
}

// TableName implements table.Table TableName interface.
func (t *Table) TableName() model.CIStr {
	return t.Name
}

// Meta implements table.Table Meta interface.
func (t *Table) Meta() *model.TableInfo {
	ti := &model.TableInfo{
		Name: t.Name,
		ID:   t.ID,
	}
	// load table meta
	for _, col := range t.Columns {
		ti.Columns = append(ti.Columns, &col.ColumnInfo)
	}

	// load table indices
	for _, idx := range t.indices {
		ti.Indices = append(ti.Indices, &idx.IndexInfo)
	}

	return ti
}

// Cols implements table.Table Cols interface.
func (t *Table) Cols() []*column.Col {
	return t.Columns
}

func (t *Table) unflatten(rec interface{}, col *column.Col) (interface{}, error) {
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
		return mysql.ParseDecimal(rec.(string))
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

func (t *Table) flatten(data interface{}) (interface{}, error) {
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

// KeyPrefix implements table.Table KeyPrefix interface.
func (t *Table) KeyPrefix() string {
	return t.recordPrefix
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (t *Table) IndexPrefix() string {
	return t.indexPrefix
}

// RecordKey implements table.Table RecordKey interface.
func (t *Table) RecordKey(h int64, col *column.Col) []byte {
	if col != nil {
		return util.EncodeRecordKey(t.KeyPrefix(), h, col.ID)
	}
	return util.EncodeRecordKey(t.KeyPrefix(), h, 0)
}

// FirstKey implements table.Table FirstKey interface.
func (t *Table) FirstKey() string {
	return string(t.RecordKey(0, nil))
}

// FindIndexByColName implements table.Table FindIndexByColName interface.
func (t *Table) FindIndexByColName(name string) *column.IndexedCol {
	for _, idx := range t.indices {
		if len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0].Name.L, name) {
			return idx
		}
	}

	return nil
}

// Truncate implements table.Table Truncate interface.
func (t *Table) Truncate(ctx context.Context) (err error) {
	err = util.DelKeyWithPrefix(ctx, t.KeyPrefix())
	if err != nil {
		return
	}
	return util.DelKeyWithPrefix(ctx, t.IndexPrefix())
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (t *Table) UpdateRecord(ctx context.Context, h int64, currData []interface{}, newData []interface{}, touched []bool) error {
	// if they are not set, and other data are changed, they will be updated by current timestamp too.
	// set on update value
	err := t.setOnUpdateData(ctx, touched, newData)
	if err != nil {
		return err
	}

	// set new value
	if err := t.setNewData(ctx, h, newData); err != nil {
		return err
	}

	// rebuild index
	if err := t.rebuildIndices(ctx, h, touched, currData, newData); err != nil {
		return err
	}
	return nil
}

func (t *Table) setOnUpdateData(ctx context.Context, touched []bool, data []interface{}) error {
	ucols := column.FindOnUpdateCols(t.Cols())
	for _, c := range ucols {
		if !touched[c.Offset] {
			v, err := expression.GetTimeValue(ctx, expression.CurrentTimestamp, c.Tp, c.Decimal)
			if err != nil {
				return errors.Trace(err)
			}
			data[c.Offset] = v
			touched[c.Offset] = true
		}
	}
	return nil
}

func (t *Table) setNewData(ctx context.Context, h int64, data []interface{}) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return err
	}
	for _, col := range t.Cols() {
		// set new value
		// If column untouched, we do not need to do this
		k := t.RecordKey(h, col)
		v, err := t.EncodeValue(data[col.Offset])
		if err != nil {
			return err
		}
		if err := txn.Set([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) rebuildIndices(ctx context.Context, h int64, touched []bool, oldData, newData []interface{}) error {
	for _, idx := range t.Indices() {
		idxTouched := false
		for _, ic := range idx.Columns {
			if touched[ic.Offset] {
				idxTouched = true
				break
			}
		}
		if !idxTouched {
			continue
		}

		oldVs, err := idx.FetchValues(oldData)
		if err != nil {
			return err
		}

		if t.RemoveRowIndex(ctx, h, oldVs, idx); err != nil {
			return err
		}

		newVs, err := idx.FetchValues(newData)
		if err != nil {
			return err
		}
		if err := t.BuildIndexForRow(ctx, h, newVs, idx); err != nil {
			return err
		}
	}
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *Table) AddRecord(ctx context.Context, r []interface{}) (recordID int64, err error) {
	id := variable.GetSessionVars(ctx).LastInsertID
	// Already have auto increment ID
	if id != 0 {
		recordID = int64(id)
	} else {
		recordID, err = t.alloc.Alloc(t.ID)
		if err != nil {
			return 0, err
		}
	}
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return 0, err
	}
	for _, v := range t.indices {
		if v == nil {
			continue
		}
		colVals, _ := v.FetchValues(r)
		if err = v.X.Create(txn, colVals, recordID); err != nil {
			if errors2.ErrorEqual(err, kv.ErrKeyExists) {
				// Get the duplicate row handle
				// For insert on duplicate syntax, we should update the row
				iter, _, terr := v.X.Seek(txn, colVals)
				if terr != nil {
					return 0, errors.Trace(terr)
				}
				_, h, terr := iter.Next()
				if terr != nil {
					return 0, errors.Trace(terr)
				}
				return h, errors.Trace(err)
			}
			return 0, errors.Trace(err)
		}
	}

	// split a record into multiple kv pair
	// first key -> LOCK
	k := t.RecordKey(recordID, nil)
	// A new row with current txn-id as lockKey
	err = txn.Set([]byte(k), []byte(txn.String()))
	if err != nil {
		return 0, err
	}
	// column key -> column value
	for _, c := range t.Cols() {
		colKey := t.RecordKey(recordID, c)
		data, err := t.EncodeValue(r[c.Offset])
		if err != nil {
			return 0, err
		}
		err = txn.Set([]byte(colKey), data)
		if err != nil {
			return 0, err
		}
	}
	variable.GetSessionVars(ctx).AddAffectedRows(1)
	return recordID, nil
}

// EncodeValue implements table.Table EncodeValue interface.
func (t *Table) EncodeValue(raw interface{}) ([]byte, error) {
	v, err := t.flatten(raw)
	if err != nil {
		return nil, err
	}
	return kv.EncodeValue(v)
}

// DecodeValue implements table.Table DecodeValue interface.
func (t *Table) DecodeValue(data []byte, col *column.Col) (interface{}, error) {
	values, err := kv.DecodeValue(data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return t.unflatten(values[0], col)
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *Table) RowWithCols(ctx context.Context, h int64, cols []*column.Col) ([]interface{}, error) {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return nil, err
	}
	// use the length of t.Cols() for alignment
	v := make([]interface{}, len(t.Cols()))
	for _, c := range cols {
		k := t.RecordKey(h, c)
		data, err := txn.Get([]byte(k))
		if err != nil {
			return nil, errors.Trace(err)
		}

		val, err := t.DecodeValue(data, c)
		if err != nil {
			return nil, errors.Trace(err)
		}
		v[c.Offset] = val
	}
	return v, nil
}

// Row implements table.Table Row interface.
func (t *Table) Row(ctx context.Context, h int64) ([]interface{}, error) {
	// TODO: we only interested in mentioned cols
	cols := t.Cols()
	r, err := t.RowWithCols(ctx, h, cols)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// LockRow implements table.Table LockRow interface.
func (t *Table) LockRow(ctx context.Context, h int64, update bool) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	// Get row lock key
	lockKey := t.RecordKey(h, nil)
	err = txn.LockKeys([]byte(lockKey))
	if err != nil {
		return errors.Trace(err)
	}
	if !update {
		return nil
	}
	// set row lock key to current txn
	err = txn.Set([]byte(lockKey), []byte(txn.String()))
	return errors.Trace(err)
}

// RemoveRow implements table.Table RemoveRow interface.
func (t *Table) RemoveRow(ctx context.Context, h int64) error {
	if err := t.LockRow(ctx, h, false); err != nil {
		return errors.Trace(err)
	}
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	// Remove row's colume one by one
	for _, col := range t.Cols() {
		k := t.RecordKey(h, col)
		err := txn.Delete([]byte(k))
		if err != nil {
			return errors.Trace(err)
		}
	}
	// Remove row lock
	err = txn.Delete([]byte(t.RecordKey(h, nil)))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RemoveRowIndex implements table.Table RemoveRowIndex interface.
func (t *Table) RemoveRowIndex(ctx context.Context, h int64, vals []interface{}, idx *column.IndexedCol) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return err
	}
	if err = idx.X.Delete(txn, vals, h); err != nil {
		return err
	}
	return nil
}

// RemoveRowAllIndex implements table.Table RemoveRowAllIndex interface.
func (t *Table) RemoveRowAllIndex(ctx context.Context, h int64, rec []interface{}) error {
	for _, v := range t.indices {
		vals, err := v.FetchValues(rec)
		if vals == nil {
			// TODO: check this
			continue
		}
		txn, err := ctx.GetTxn(false)
		if err != nil {
			return err
		}
		if err = v.X.Delete(txn, vals, h); err != nil {
			return err
		}
	}
	return nil
}

// BuildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *Table) BuildIndexForRow(ctx context.Context, h int64, vals []interface{}, idx *column.IndexedCol) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return err
	}
	if err = idx.X.Create(txn, vals, h); err != nil {
		return err
	}
	return nil
}

// IterRecords implements table.Table IterRecords interface.
func (t *Table) IterRecords(ctx context.Context, startKey string, cols []*column.Col, fn table.RecordIterFunc) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return err
	}

	it, err := txn.Seek([]byte(startKey), nil)
	if err != nil {
		return err
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	log.Debugf("startKey %q, key:%q,value:%q", startKey, it.Key(), it.Value())

	prefix := t.KeyPrefix()
	for it.Valid() && strings.HasPrefix(it.Key(), prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		var err error
		handle, err := util.DecodeHandleFromRowKey(it.Key())
		if err != nil {
			return err
		}

		data, err := t.RowWithCols(ctx, handle, cols)
		if err != nil {
			return err
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return err
		}

		rk := t.RecordKey(handle, nil)
		it, err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// AllocAutoID implements table.Table AllocAutoID interface.
func (t *Table) AllocAutoID() (int64, error) {
	return t.alloc.Alloc(t.ID)
}

func init() {
	table.TableFromMeta = TableFromMeta
}

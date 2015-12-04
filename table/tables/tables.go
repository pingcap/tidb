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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
)

// Table implements table.Table interface.
type Table struct {
	ID      int64
	Name    model.CIStr
	Columns []*column.Col

	publicColumns   []*column.Col
	writableColumns []*column.Col
	indices         []*column.IndexedCol
	recordPrefix    string
	indexPrefix     string
	alloc           autoid.Allocator
	state           model.SchemaState
}

// TableFromMeta creates a Table instance from model.TableInfo.
func TableFromMeta(alloc autoid.Allocator, tblInfo *model.TableInfo) table.Table {
	if tblInfo.State == model.StateNone {
		log.Fatalf("table %s can't be in none state", tblInfo.Name)
	}

	columns := make([]*column.Col, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		if colInfo.State == model.StateNone {
			log.Fatalf("column %s can't be in none state", colInfo.Name)
		}

		col := &column.Col{ColumnInfo: *colInfo}
		columns = append(columns, col)
	}

	t := NewTable(tblInfo.ID, tblInfo.Name.O, columns, alloc)

	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.State == model.StateNone {
			log.Fatalf("index %s can't be in none state", idxInfo.Name)
		}

		idx := &column.IndexedCol{
			IndexInfo: *idxInfo,
			X:         kv.NewKVIndex(t.indexPrefix, idxInfo.Name.L, idxInfo.ID, idxInfo.Unique),
		}
		t.AddIndex(idx)
	}

	t.state = tblInfo.State
	return t
}

// NewTable constructs a Table instance.
func NewTable(tableID int64, tableName string, cols []*column.Col, alloc autoid.Allocator) *Table {
	name := model.NewCIStr(tableName)
	t := &Table{
		ID:           tableID,
		Name:         name,
		recordPrefix: fmt.Sprintf("t%d_r", tableID),
		indexPrefix:  fmt.Sprintf("t%d_i", tableID),
		alloc:        alloc,
		Columns:      cols,
		state:        model.StatePublic,
	}

	t.publicColumns = t.Cols()
	t.writableColumns = t.writableCols()
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
		Name:  t.Name,
		ID:    t.ID,
		State: t.state,
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
	if len(t.publicColumns) > 0 {
		return t.publicColumns
	}

	t.publicColumns = make([]*column.Col, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.State == model.StatePublic {
			t.publicColumns = append(t.publicColumns, col)
		}
	}

	return t.publicColumns
}

func (t *Table) writableCols() []*column.Col {
	if len(t.writableColumns) > 0 {
		return t.writableColumns
	}

	t.writableColumns = make([]*column.Col, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			continue
		}

		t.writableColumns = append(t.writableColumns, col)
	}

	return t.writableColumns
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
		// only public index can be read.
		if idx.State != model.StatePublic {
			continue
		}

		if len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0].Name.L, name) {
			return idx
		}
	}

	return nil
}

// Truncate implements table.Table Truncate interface.
func (t *Table) Truncate(ctx context.Context) error {
	err := util.DelKeyWithPrefix(ctx, t.KeyPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	return util.DelKeyWithPrefix(ctx, t.IndexPrefix())
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (t *Table) UpdateRecord(ctx context.Context, h int64, oldData []interface{}, newData []interface{}, touched map[int]bool) error {
	// We should check whether this table has on update column which state is write only.
	currentData := make([]interface{}, len(t.writableCols()))
	copy(currentData, newData)

	// If they are not set, and other data are changed, they will be updated by current timestamp too.
	err := t.setOnUpdateData(ctx, touched, currentData)
	if err != nil {
		return errors.Trace(err)
	}

	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}

	bs := kv.NewBufferStore(txn)
	defer bs.Release()

	// set new value
	if err := t.setNewData(bs, h, touched, currentData); err != nil {
		return errors.Trace(err)
	}

	// rebuild index
	if err := t.rebuildIndices(bs, h, touched, oldData, currentData); err != nil {
		return errors.Trace(err)
	}

	err = bs.SaveTo(txn)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (t *Table) setOnUpdateData(ctx context.Context, touched map[int]bool, data []interface{}) error {
	ucols := column.FindOnUpdateCols(t.writableCols())
	for _, col := range ucols {
		if !touched[col.Offset] {
			value, err := expression.GetTimeValue(ctx, expression.CurrentTimestamp, col.Tp, col.Decimal)
			if err != nil {
				return errors.Trace(err)
			}

			data[col.Offset] = value
			touched[col.Offset] = true
		}
	}
	return nil
}

// SetColValue implements table.Table SetColValue interface.
func (t *Table) SetColValue(rm kv.RetrieverMutator, key []byte, data interface{}) error {
	v, err := t.EncodeValue(data)
	if err != nil {
		return errors.Trace(err)
	}
	if err := rm.Set(key, v); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (t *Table) setNewData(rm kv.RetrieverMutator, h int64, touched map[int]bool, data []interface{}) error {
	for _, col := range t.Cols() {
		if !touched[col.Offset] {
			continue
		}

		k := t.RecordKey(h, col)
		if err := t.SetColValue(rm, k, data[col.Offset]); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (t *Table) rebuildIndices(rm kv.RetrieverMutator, h int64, touched map[int]bool, oldData []interface{}, newData []interface{}) error {
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
			return errors.Trace(err)
		}

		if t.RemoveRowIndex(rm, h, oldVs, idx); err != nil {
			return errors.Trace(err)
		}

		newVs, err := idx.FetchValues(newData)
		if err != nil {
			return errors.Trace(err)
		}

		if err := t.BuildIndexForRow(rm, h, newVs, idx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *Table) AddRecord(ctx context.Context, r []interface{}, h int64) (recordID int64, err error) {
	// Already have recordID
	if h != 0 {
		recordID = int64(h)
	} else {
		recordID, err = t.alloc.Alloc(t.ID)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return 0, errors.Trace(err)
	}
	bs := kv.NewBufferStore(txn)
	defer bs.Release()

	for _, v := range t.indices {
		if v == nil || v.State == model.StateDeleteOnly || v.State == model.StateDeleteReorganization {
			// if index is in delete only or delete reorganization state, we can't add it.
			continue
		}
		colVals, _ := v.FetchValues(r)
		if err = v.X.Create(bs, colVals, recordID); err != nil {
			if terror.ErrorEqual(err, kv.ErrKeyExists) {
				// Get the duplicate row handle
				// For insert on duplicate syntax, we should update the row
				iter, _, err1 := v.X.Seek(bs, colVals)
				if err1 != nil {
					return 0, errors.Trace(err1)
				}
				_, h, err1 := iter.Next()
				if err1 != nil {
					return 0, errors.Trace(err1)
				}
				return h, errors.Trace(err)
			}
			return 0, errors.Trace(err)
		}
	}

	if err = t.LockRow(ctx, recordID); err != nil {
		return 0, errors.Trace(err)
	}

	// Set public and write only column value.
	for _, col := range t.writableCols() {
		var value interface{}
		key := t.RecordKey(recordID, col)

		if col.State == model.StateWriteOnly || col.State == model.StateWriteReorganization {
			// if col is in write only or write reorganization state, we must add it with its default value.
			value, _, err = GetColDefaultValue(ctx, &col.ColumnInfo)
			if err != nil {
				return 0, errors.Trace(err)
			}
			value, err = types.Convert(value, &col.FieldType)
			if err != nil {
				return 0, errors.Trace(err)
			}
		} else {
			value = r[col.Offset]
		}

		err = t.SetColValue(txn, key, value)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	if err = bs.SaveTo(txn); err != nil {
		return 0, errors.Trace(err)
	}

	variable.GetSessionVars(ctx).AddAffectedRows(1)
	return recordID, nil
}

// EncodeValue implements table.Table EncodeValue interface.
func (t *Table) EncodeValue(raw interface{}) ([]byte, error) {
	v, err := t.flatten(raw)
	if err != nil {
		return nil, errors.Trace(err)
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
		return nil, errors.Trace(err)
	}

	// use the length of t.Cols() for alignment
	v := make([]interface{}, len(t.Cols()))
	for _, col := range cols {
		if col.State != model.StatePublic {
			return nil, errors.Errorf("Cannot use none public column - %v", cols)
		}

		k := t.RecordKey(h, col)
		data, err := txn.Get([]byte(k))
		if err != nil {
			return nil, errors.Trace(err)
		}

		val, err := t.DecodeValue(data, col)
		if err != nil {
			return nil, errors.Trace(err)
		}
		v[col.Offset] = val
	}
	return v, nil
}

// Row implements table.Table Row interface.
func (t *Table) Row(ctx context.Context, h int64) ([]interface{}, error) {
	// TODO: we only interested in mentioned cols
	cols := t.Cols()
	r, err := t.RowWithCols(ctx, h, cols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r, nil
}

// LockRow implements table.Table LockRow interface.
func (t *Table) LockRow(ctx context.Context, h int64) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	// Get row lock key
	lockKey := t.RecordKey(h, nil)
	// set row lock key to current txn
	err = txn.Set([]byte(lockKey), []byte(txn.String()))
	return errors.Trace(err)
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *Table) RemoveRecord(ctx context.Context, h int64, r []interface{}) error {
	err := t.removeRowData(ctx, h)
	if err != nil {
		return errors.Trace(err)
	}

	err = t.removeRowIndices(ctx, h, r)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (t *Table) removeRowData(ctx context.Context, h int64) error {
	if err := t.LockRow(ctx, h); err != nil {
		return errors.Trace(err)
	}
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	// Remove row's colume one by one
	for _, col := range t.Columns {
		k := t.RecordKey(h, col)
		err = txn.Delete([]byte(k))
		if err != nil {
			if col.State != model.StatePublic && terror.ErrorEqual(err, kv.ErrNotExist) {
				// If the column is not in public state, we may have not added the column,
				// or already deleted the column, so skip ErrNotExist error.
				continue
			}

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

// removeRowAllIndex removes all the indices of a row.
func (t *Table) removeRowIndices(ctx context.Context, h int64, rec []interface{}) error {
	for _, v := range t.indices {
		vals, err := v.FetchValues(rec)
		if vals == nil {
			// TODO: check this
			continue
		}
		txn, err := ctx.GetTxn(false)
		if err != nil {
			return errors.Trace(err)
		}
		if err = v.X.Delete(txn, vals, h); err != nil {
			if v.State != model.StatePublic && terror.ErrorEqual(err, kv.ErrNotExist) {
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				continue
			}

			return errors.Trace(err)
		}
	}
	return nil
}

// RemoveRowIndex implements table.Table RemoveRowIndex interface.
func (t *Table) RemoveRowIndex(rm kv.RetrieverMutator, h int64, vals []interface{}, idx *column.IndexedCol) error {
	if err := idx.X.Delete(rm, vals, h); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// BuildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *Table) BuildIndexForRow(rm kv.RetrieverMutator, h int64, vals []interface{}, idx *column.IndexedCol) error {
	if idx.State == model.StateDeleteOnly || idx.State == model.StateDeleteReorganization {
		// If the index is in delete only or write reorganization state, we can not add index.
		return nil
	}

	if err := idx.X.Create(rm, vals, h); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// IterRecords implements table.Table IterRecords interface.
func (t *Table) IterRecords(ctx context.Context, startKey string, cols []*column.Col, fn table.RecordIterFunc) error {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}

	it, err := txn.Seek([]byte(startKey))
	if err != nil {
		return errors.Trace(err)
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
			return errors.Trace(err)
		}

		data, err := t.RowWithCols(ctx, handle, cols)
		if err != nil {
			return errors.Trace(err)
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return errors.Trace(err)
		}

		rk := t.RecordKey(handle, nil)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
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

// GetColDefaultValue gets default value of the column.
func GetColDefaultValue(ctx context.Context, col *model.ColumnInfo) (interface{}, bool, error) {
	// Check no default value flag.
	if mysql.HasNoDefaultValueFlag(col.Flag) && col.Tp != mysql.TypeEnum {
		return nil, false, errors.Errorf("Field '%s' doesn't have a default value", col.Name)
	}

	// Check and get timestamp/datetime default value.
	if col.Tp == mysql.TypeTimestamp || col.Tp == mysql.TypeDatetime {
		if col.DefaultValue == nil {
			return nil, true, nil
		}

		value, err := expression.GetTimeValue(ctx, col.DefaultValue, col.Tp, col.Decimal)
		if err != nil {
			return nil, true, errors.Errorf("Field '%s' get default value fail - %s", col.Name, errors.Trace(err))
		}

		return value, true, nil
	} else if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		if col.DefaultValue == nil && mysql.HasNotNullFlag(col.Flag) {
			return col.FieldType.Elems[0], true, nil
		}
	}

	return col.DefaultValue, true, nil
}

func init() {
	table.TableFromMeta = TableFromMeta
}

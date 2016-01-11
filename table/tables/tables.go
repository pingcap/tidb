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
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

// TablePrefix is the prefix for table record and index key.
var TablePrefix = []byte{'t'}

// Table implements table.Table interface.
type Table struct {
	ID      int64
	Name    model.CIStr
	Columns []*column.Col

	publicColumns   []*column.Col
	writableColumns []*column.Col
	indices         []*column.IndexedCol
	recordPrefix    kv.Key
	indexPrefix     kv.Key
	alloc           autoid.Allocator
	meta            *model.TableInfo
}

// TableFromMeta creates a Table instance from model.TableInfo.
func TableFromMeta(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error) {
	if tblInfo.State == model.StateNone {
		return nil, errors.Errorf("table %s can't be in none state", tblInfo.Name)
	}

	columns := make([]*column.Col, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		if colInfo.State == model.StateNone {
			return nil, errors.Errorf("column %s can't be in none state", colInfo.Name)
		}

		col := &column.Col{ColumnInfo: *colInfo}
		columns = append(columns, col)
	}

	t := newTable(tblInfo.ID, tblInfo.Name.O, columns, alloc)

	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.State == model.StateNone {
			return nil, errors.Errorf("index %s can't be in none state", idxInfo.Name)
		}

		idx := &column.IndexedCol{
			IndexInfo: *idxInfo,
		}

		idx.X = kv.NewKVIndex(t.IndexPrefix(), idxInfo.Name.L, idxInfo.ID, idxInfo.Unique)

		t.AddIndex(idx)
	}
	t.meta = tblInfo
	return t, nil
}

// NewTable constructs a Table instance.
func newTable(tableID int64, tableName string, cols []*column.Col, alloc autoid.Allocator) *Table {
	name := model.NewCIStr(tableName)
	t := &Table{
		ID:           tableID,
		Name:         name,
		recordPrefix: genTableRecordPrefix(tableID),
		indexPrefix:  genTableIndexPrefix(tableID),
		alloc:        alloc,
		Columns:      cols,
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
	return t.meta
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

// RecordPrefix implements table.Table RecordPrefix interface.
func (t *Table) RecordPrefix() kv.Key {
	return t.recordPrefix
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (t *Table) IndexPrefix() kv.Key {
	return t.indexPrefix
}

// RecordKey implements table.Table RecordKey interface.
func (t *Table) RecordKey(h int64, col *column.Col) kv.Key {
	colID := int64(0)
	if col != nil {
		colID = col.ID
	}
	return encodeRecordKey(t.recordPrefix, h, colID)
}

// FirstKey implements table.Table FirstKey interface.
func (t *Table) FirstKey() kv.Key {
	return t.RecordKey(0, nil)
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
func (t *Table) Truncate(rm kv.RetrieverMutator) error {
	err := util.DelKeyWithPrefix(rm, t.RecordPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	return util.DelKeyWithPrefix(rm, t.IndexPrefix())
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
	if err = t.setNewData(bs, h, touched, currentData); err != nil {
		return errors.Trace(err)
	}

	// rebuild index
	if err = t.rebuildIndices(bs, h, touched, oldData, currentData); err != nil {
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
func (t *Table) AddRecord(ctx context.Context, r []interface{}) (recordID int64, err error) {
	var hasRecordID bool
	for _, col := range t.Cols() {
		if col.IsPKHandleColumn(t.meta) {
			recordID, err = types.ToInt64(r[col.Offset])
			if err != nil {
				return 0, errors.Trace(err)
			}
			hasRecordID = true
			break
		}
	}
	if !hasRecordID {
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

	// Insert new entries into indices.
	h, err := t.addIndices(ctx, recordID, r, bs)
	if err != nil {
		return h, errors.Trace(err)
	}

	if err = t.LockRow(ctx, recordID); err != nil {
		return 0, errors.Trace(err)
	}

	// Set public and write only column value.
	for _, col := range t.writableCols() {
		if col.IsPKHandleColumn(t.meta) {
			continue
		}
		var value interface{}
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

		key := t.RecordKey(recordID, col)
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

// Generate index content string representation.
func (t *Table) genIndexKeyStr(colVals []interface{}) (string, error) {
	// Pass pre-composed error to txn.
	strVals := make([]string, 0, len(colVals))
	for _, cv := range colVals {
		cvs := "NULL"
		var err error
		if cv != nil {
			cvs, err = types.ToString(cv)
			if err != nil {
				return "", errors.Trace(err)
			}
		}
		strVals = append(strVals, cvs)
	}
	return strings.Join(strVals, "-"), nil
}

// Add data into indices.
func (t *Table) addIndices(ctx context.Context, recordID int64, r []interface{}, bs *kv.BufferStore) (int64, error) {
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// Clean up lazy check error environment
	defer txn.DelOption(kv.PresumeKeyNotExistsError)
	if t.meta.PKIsHandle {
		// Check key exists.
		recordKey := t.RecordKey(recordID, nil)
		e := kv.ErrKeyExists.Gen("Duplicate entry '%d' for key 'PRIMARY'", recordID)
		txn.SetOption(kv.PresumeKeyNotExistsError, e)
		_, err = txn.Get(recordKey)
		if err == nil {
			return recordID, errors.Trace(e)
		} else if !terror.ErrorEqual(err, kv.ErrNotExist) {
			return 0, errors.Trace(err)
		}
		txn.DelOption(kv.PresumeKeyNotExistsError)
	}

	for _, v := range t.indices {
		if v == nil || v.State == model.StateDeleteOnly || v.State == model.StateDeleteReorganization {
			// if index is in delete only or delete reorganization state, we can't add it.
			continue
		}
		colVals, _ := v.FetchValues(r)
		var dupKeyErr error
		if v.Unique || v.Primary {
			entryKey, err1 := t.genIndexKeyStr(colVals)
			if err1 != nil {
				return 0, errors.Trace(err1)
			}
			dupKeyErr = kv.ErrKeyExists.Gen("Duplicate entry '%s' for key '%s'", entryKey, v.Name)
			txn.SetOption(kv.PresumeKeyNotExistsError, dupKeyErr)
		}
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
				return h, errors.Trace(dupKeyErr)
			}
			return 0, errors.Trace(err)
		}
		txn.DelOption(kv.PresumeKeyNotExistsError)
	}
	return 0, nil
}

// EncodeValue implements table.Table EncodeValue interface.
func (t *Table) EncodeValue(raw interface{}) ([]byte, error) {
	v, err := t.flatten(raw)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := codec.EncodeValue(nil, v)
	return b, errors.Trace(err)
}

// DecodeValue implements table.Table DecodeValue interface.
func (t *Table) DecodeValue(data []byte, col *column.Col) (interface{}, error) {
	values, err := codec.Decode(data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return t.unflatten(values[0], col)
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *Table) RowWithCols(retriever kv.Retriever, h int64, cols []*column.Col) ([]interface{}, error) {
	v := make([]interface{}, len(cols))
	for i, col := range cols {
		if col.State != model.StatePublic {
			return nil, errors.Errorf("Cannot use none public column - %v", cols)
		}
		if col.IsPKHandleColumn(t.meta) {
			v[i] = h
			continue
		}

		k := t.RecordKey(h, col)
		data, err := retriever.Get(k)
		if err != nil {
			return nil, errors.Trace(err)
		}

		val, err := t.DecodeValue(data, col)
		if err != nil {
			return nil, errors.Trace(err)
		}
		v[i] = val
	}
	return v, nil
}

// Row implements table.Table Row interface.
func (t *Table) Row(ctx context.Context, h int64) ([]interface{}, error) {
	// TODO: we only interested in mentioned cols
	txn, err := ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	r, err := t.RowWithCols(txn, h, t.Cols())
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
	err = txn.Set(lockKey, []byte(txn.String()))
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
func (t *Table) IterRecords(retriever kv.Retriever, startKey kv.Key, cols []*column.Col,
	fn table.RecordIterFunc) error {
	it, err := retriever.Seek(startKey)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	log.Debugf("startKey:%q, key:%q, value:%q", startKey, it.Key(), it.Value())

	prefix := t.RecordPrefix()
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := DecodeRecordKeyHandle(it.Key())
		if err != nil {
			return errors.Trace(err)
		}

		data, err := t.RowWithCols(retriever, handle, cols)
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

var (
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
)

// record prefix is "t[tableID]_r"
func genTableRecordPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(TablePrefix)+8+len(recordPrefixSep))
	buf = append(buf, TablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// index prefix is "t[tableID]_i"
func genTableIndexPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(TablePrefix)+8+len(indexPrefixSep))
	buf = append(buf, TablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

func encodeRecordKey(recordPrefix kv.Key, h int64, columnID int64) kv.Key {
	buf := make([]byte, 0, len(recordPrefix)+16)
	buf = append(buf, recordPrefix...)
	buf = codec.EncodeInt(buf, h)

	if columnID != 0 {
		buf = codec.EncodeInt(buf, columnID)
	}
	return buf
}

// EncodeRecordKey encodes the record key for a table column.
func EncodeRecordKey(tableID int64, h int64, columnID int64) kv.Key {
	prefix := genTableRecordPrefix(tableID)
	return encodeRecordKey(prefix, h, columnID)
}

// DecodeRecordKey decodes the key and gets the tableID, handle and columnID.
func DecodeRecordKey(key kv.Key) (tableID int64, handle int64, columnID int64, err error) {
	k := key
	if !key.HasPrefix(TablePrefix) {
		return 0, 0, 0, errors.Errorf("invalid record key - %q", k)
	}

	key = key[len(TablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}

	if !key.HasPrefix(recordPrefixSep) {
		return 0, 0, 0, errors.Errorf("invalid record key - %q", k)
	}

	key = key[len(recordPrefixSep):]

	key, handle, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}
	if len(key) == 0 {
		return
	}

	key, columnID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}

	return
}

// DecodeRecordKeyHandle decodes the key and gets the record handle.
func DecodeRecordKeyHandle(key kv.Key) (int64, error) {
	_, handle, _, err := DecodeRecordKey(key)
	return handle, errors.Trace(err)
}

func init() {
	table.TableFromMeta = TableFromMeta
}

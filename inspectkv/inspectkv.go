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

package inspectkv

import (
	"io"
	"reflect"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
)

// DDLInfo is for DDL information.
type DDLInfo struct {
	SchemaVer   int64
	ReorgHandle int64
	Owner       *model.Owner
	Job         *model.Job
}

// GetDDLInfo returns DDL information.
func GetDDLInfo(txn kv.Transaction) (*DDLInfo, error) {
	var err error
	info := &DDLInfo{}
	t := meta.NewMeta(txn)

	info.Owner, err = t.GetDDLOwner()
	if err != nil {
		return nil, errors.Trace(err)
	}
	info.Job, err = t.GetDDLJob(0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info.SchemaVer, err = t.GetSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info.Job == nil {
		return info, nil
	}

	info.ReorgHandle, err = t.GetDDLReorgHandle(info.Job)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return info, nil
}

func nextIndexVals(data []interface{}) []interface{} {
	// Add 0x0 to the end of data.
	return append(data, nil)
}

// RecordData is the record data composed of a handle and values.
type RecordData struct {
	Handle int64
	Values []interface{}
}

// Equal returns ture if r is equal to rd.
func (rd *RecordData) Equal(r *RecordData) bool {
	if r == nil {
		return false
	}
	if rd.Handle != r.Handle || !reflect.DeepEqual(rd.Values, r.Values) {
		return false
	}

	return true
}

// ScanIndexData scans the index handles and values in a limited number, according to the index information.
// It returns data and the next startVals until it doesn't have data, then returns data is nil and
// the next startVals is the values which can't get data.
// If limit = -1, it returns the index data of the whole.
func ScanIndexData(txn kv.Transaction, kvIndex kv.Index, startVals []interface{}, limit int64) (
	[]*RecordData, []interface{}, error) {
	it, _, err := kvIndex.Seek(txn, startVals)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer it.Close()

	var idxRows []*RecordData
	var curVals []interface{}
	for limit != 0 {
		val, h, err1 := it.Next()
		if terror.ErrorEqual(err1, io.EOF) {
			return idxRows, nextIndexVals(curVals), nil
		} else if err1 != nil {
			return nil, nil, errors.Trace(err1)
		}
		idxRows = append(idxRows, &RecordData{Handle: h, Values: val})
		limit--
		curVals = val
	}

	nextVals, _, err := it.Next()
	if terror.ErrorEqual(err, io.EOF) {
		return idxRows, nextIndexVals(curVals), nil
	} else if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return idxRows, nextVals, nil
}

// ScanIndexColData scans the index handles and values in a limited number, according to the corresponding column.
// It returns data and the next startHandle until it doesn't have data, then returns data is nil and
// the next startHandle is the handle which can't get data.
// If limit = -1, it returns the index data of the whole.
func ScanIndexColData(retriever kv.Retriever, t table.Table, idx *column.IndexedCol, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	cols := make([]*column.Col, len(idx.Columns))
	for i, col := range idx.Columns {
		cols[i] = t.Cols()[col.Offset]
	}

	return scanTableData(retriever, t, cols, startHandle, limit)
}

// EqualRecords returns ture if a is equal to b.
func EqualRecords(a, b []*RecordData) bool {
	if len(a) != len(b) {
		return false
	}

	for i, r := range a {
		if !r.Equal(b[i]) {
			return false
		}
	}

	return true
}

// DiffRecords compares records one by one.
// It returns the difference between a and b.
// e.g,
// a {{1, "val0"}, {2, "val2"}}
// b {{1, "val1"}, {3, "val3"}}
// returns {{1, "val0"}, {2, "val2"}, nil}, {{1, "val1"}, nil, {3, "val3"}}, nil
func DiffRecords(a, b []*RecordData) ([]*RecordData, []*RecordData, error) {
	if len(a) == 0 {
		return make([]*RecordData, len(b)), b, nil
	}

	var ret1, ret2 []*RecordData
	k := 0
	for i := 0; i < len(a); i++ {
		if len(b[k:]) == 0 {
			ret1 = append(ret1, a[i:]...)
			ret2 = append(ret2, make([]*RecordData, len(a[i:]))...)
			break
		}

		for j := k; j < len(b); j++ {
			if a[i].Handle > b[j].Handle {
				ret1 = append(ret1, nil)
				ret2 = append(ret2, b[j])
				continue
			}
			if a[i].Handle < b[j].Handle {
				ret1 = append(ret1, a[i])
				ret2 = append(ret2, nil)
				if len(a[i+1:]) == 0 {
					ret1 = append(ret1, make([]*RecordData, len(b[j:]))...)
					ret2 = append(ret2, b[j:]...)
				}
				break
			}
			if !reflect.DeepEqual(a[i].Values, b[j].Values) {
				ret1 = append(ret1, a[i])
				ret2 = append(ret2, b[j])
			}
			k = j + 1
			break
		}
	}

	return ret1, ret2, nil
}

// EqualIndexData returns ture if the data from the index is equal to the data from the table columns.
func EqualIndexData(txn kv.Transaction, t table.Table, idx *column.IndexedCol) (bool, error) {
	kvIndex := kv.NewKVIndex(t.IndexPrefix(), idx.Name.L, idx.ID, idx.Unique)

	rs1, _, err := ScanIndexData(txn, kvIndex, nil, -1)
	if err != nil {
		return false, errors.Trace(err)
	}
	rs2, _, err := ScanIndexColData(txn, t, idx, 0, -1)
	if err != nil {
		return false, errors.Trace(err)
	}

	return EqualRecords(rs1, rs2), nil
}

// DiffIndexData compares index data one by one.
// It returns the difference between data from the index and data from the table columns.
func DiffIndexData(txn kv.Transaction, t table.Table, idx *column.IndexedCol) (
	[]*RecordData, []*RecordData, error) {
	kvIndex := kv.NewKVIndex(t.IndexPrefix(), idx.Name.L, idx.ID, idx.Unique)

	rs1, _, err := ScanIndexData(txn, kvIndex, nil, -1)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	rs2, _, err := ScanIndexColData(txn, t, idx, 0, -1)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return DiffRecords(rs1, rs2)
}

func scanTableData(retriever kv.Retriever, t table.Table, cols []*column.Col, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	var records []*RecordData

	startKey := t.RecordKey(startHandle, nil)
	err := t.IterRecords(retriever, string(startKey), cols,
		func(h int64, d []interface{}, cols []*column.Col) (bool, error) {
			if limit != 0 {
				r := &RecordData{
					Handle: h,
					Values: d,
				}
				records = append(records, r)
				limit--
				return true, nil
			}

			return false, nil
		})
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	if len(records) == 0 {
		return records, startHandle, nil
	}

	nextHandle := records[len(records)-1].Handle + 1

	return records, nextHandle, nil
}

// ScanTableData scans table row handles and column values in a limited number.
// It returns data and the next startHandle until it doesn't have data, then returns data is nil and
// the next startHandle is the handle which can't get data.
// If limit = -1, it returns the table data of the whole.
func ScanTableData(retriever kv.Retriever, t table.Table, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	return scanTableData(retriever, t, t.Cols(), startHandle, limit)
}

// ScanSnapshotTableData scans the ver version of the table data in a limited number.
// It returns data and the next startHandle until it doesn't have data, then returns data is nil and
// the next startHandle is the handle which can't get data.
// If limit = -1, it returns the table data of the whole.
func ScanSnapshotTableData(store kv.Storage, ver kv.Version, t table.Table, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	snap, err := store.GetSnapshot(ver)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer snap.Release()

	records, nextHandle, err := ScanTableData(snap, t, startHandle, limit)

	return records, nextHandle, errors.Trace(err)
}

// EqualTableData returns ture if records is equal to the data that scans from the the start handle.
// If limit = -1, it compares the table data of the whole.
func EqualTableData(retriever kv.Retriever, t table.Table, records []*RecordData, startHandle, limit int64) (
	bool, error) {
	rs, _, err := ScanTableData(retriever, t, startHandle, limit)
	if err != nil {
		return false, errors.Trace(err)
	}

	return EqualRecords(rs, records), nil
}

// DiffTableData compares records and the corresponding table data one by one.
// It returns the difference between the table data that scans from the start handle and records .
// If limit = -1, it compares the table data of the whole.
func DiffTableData(retriever kv.Retriever, t table.Table, records []*RecordData, startHandle, limit int64) (
	[]*RecordData, []*RecordData, error) {
	rs, _, err := ScanTableData(retriever, t, startHandle, limit)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return DiffRecords(rs, records)
}

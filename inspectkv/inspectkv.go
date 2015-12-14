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

func nextIdxVals(data []interface{}) []interface{} {
	// Add 0x0 to the end of data.
	return append(data, nil)
}

// IndexRow is the index information composed of a handle and values.
type IndexRow struct {
	Handle int64
	Values []interface{}
}

// ScanIndexData scans the index handles and values in a limited number.
// It returns data and the next startVals.
func ScanIndexData(txn kv.Transaction, kvIndex kv.Index, startVals []interface{}, limit int64) (
	[]*IndexRow, []interface{}, error) {
	it, _, err := kvIndex.Seek(txn, startVals)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var idxRows []*IndexRow
	var curVals []interface{}
	for limit != 0 {
		val, h, err1 := it.Next()
		if terror.ErrorEqual(err1, io.EOF) {
			return idxRows, nextIdxVals(curVals), nil
		} else if err1 != nil {
			return nil, nil, errors.Trace(err1)
		}
		idxRows = append(idxRows, &IndexRow{Handle: h, Values: val})
		limit--
		curVals = val
	}

	nextVals, _, err := it.Next()
	if terror.ErrorEqual(err, io.EOF) {
		return idxRows, nextIdxVals(curVals), nil
	} else if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return idxRows, nextVals, nil
}

// RecordData is the record data composed of a handle and column values.
type RecordData struct {
	Handle int64
	Values []interface{}
}

// ScanTableData scans table row handles and column values in a limited number.
// It returns data and the next startKey.
func ScanTableData(t table.Table, retriever kv.Retriever, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	var records []*RecordData

	startKey := t.RecordKey(startHandle, nil)
	err := t.IterRecords(retriever, string(startKey), t.Cols(),
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

// ScanSnapshotTableData scans the ver version of the table data in a limited number.
// It returns data and the next startKey.
func ScanSnapshotTableData(store kv.Storage, ver kv.Version, t table.Table, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	snap, err := store.GetSnapshot(ver)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer snap.Release()

	records, nextHandle, err := ScanTableData(t, snap, startHandle, limit)

	return records, nextHandle, errors.Trace(err)
}

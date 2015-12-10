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

func next(data []interface{}) []interface{} {
	// Add 0x0 to the end of data.
	return append(data, []byte{})
}

// ScanIndexData scans the index handles and values in a limited number.
// It returns data and the next startVals.
func ScanIndexData(tableIndexPrefix string, idx *column.IndexedCol, txn kv.Transaction,
	startVals []interface{}, limit int) ([]int64, [][]interface{}, []interface{}, error) {
	kvIndex := kv.NewKVIndex(tableIndexPrefix, idx.Name.L, idx.ID, idx.Unique)
	it, _, err := kvIndex.Seek(txn, startVals)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	var (
		count   int
		handles []int64
		vals    [][]interface{}
		curVals []interface{}
	)
	for !table.IsLimit(count, limit) {
		val, h, err1 := it.Next()
		if terror.ErrorEqual(err1, io.EOF) {
			return handles, vals, next(curVals), nil
		} else if err1 != nil {
			return nil, nil, nil, errors.Trace(err1)
		}
		handles = append(handles, h)
		vals = append(vals, val)
		count++
		curVals = val
	}
	nextVals, _, err := it.Next()
	if terror.ErrorEqual(err, io.EOF) {
		return handles, vals, next(curVals), nil
	} else if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	return handles, vals, nextVals, nil
}

// ScanTableData scans table row handles and column values in a limited number.
// It returns data and the next startKey.
func ScanTableData(t table.Table, retriever kv.Retriever, startKey string, limit int) (
	[]int64, [][]interface{}, string, error) {
	var handles []int64
	var data [][]interface{}

	nextKey, err := t.ScanRecords(retriever, startKey, limit, t.Cols(),
		func(h int64, d []interface{}, cols []*column.Col) (bool, error) {
			data = append(data, d)
			handles = append(handles, h)
			return true, nil
		})
	if err != nil {
		return nil, nil, "", errors.Trace(err)
	}

	return handles, data, nextKey, nil
}

// ScanSnapshotTableData scans the ver version of the table data in a limited number.
// It returns data and the next startKey.
func ScanSnapshotTableData(store kv.Storage, ver kv.Version, t table.Table, startKey string, limit int) (
	[][]interface{}, string, error) {
	snap, err := store.GetSnapshot(ver)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	defer snap.Release()

	_, data, key, err := ScanTableData(t, snap, startKey, limit)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	return data, key, nil
}

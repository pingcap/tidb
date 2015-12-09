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

// GetIndexVals returns index values.
func GetIndexVals(table table.Table, txn kv.Transaction, indexInfo model.IndexInfo, handle int64) ([]interface{}, error) {
	vals := make([]interface{}, len(indexInfo.Columns))
	cols := table.Cols()

	for i, col := range indexInfo.Columns {
		key := table.RecordKey(handle, cols[col.Offset])
		data, err := txn.Get(key)
		if err != nil {
			return nil, errors.Trace(err)
		}

		val, err := table.DecodeValue(data, cols[col.Offset])
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals[i] = val
	}

	return vals, nil
}

// GetIndexHandles returns index handles.
func GetIndexHandles(table table.Table, txn kv.Transaction, idx *column.IndexedCol) ([]int64, error) {
	var handles []int64
	kvIndex := kv.NewKVIndex(table.IndexPrefix(), idx.Name.L, idx.ID, idx.Unique)
	it, err := kvIndex.SeekFirst(txn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		} else if err != nil {
			return nil, errors.Trace(err)
		}
		handles = append(handles, h)
	}

	return handles, nil
}

// GetTableData gets table row handles and column values.
// If there is no special iterator, it will be nil.
func GetTableData(table table.Table, retriever kv.Retriever) ([]int64, []interface{}, error) {
	var handles []int64
	var data []interface{}

	err := table.IterRecords(retriever, table.FirstKey(), table.Cols(),
		func(h int64, d []interface{}, cols []*column.Col) (bool, error) {
			data = append(data, d)
			handles = append(handles, h)
			return true, nil
		})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return handles, data, nil
}

// GetTableSnapshot gets the ver version of the table data.
func GetTableSnapshot(store kv.Storage, ver kv.Version, table table.Table) ([]interface{}, error) {
	snap, err := store.GetSnapshot(ver)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer snap.Release()

	_, data, err := GetTableData(table, snap)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return data, nil
}

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

package ddl

import (
	"bytes"
	"strings"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/errors2"
)

func (d *ddl) getTableInfo(t *meta.Meta, job *model.Job) (*model.TableInfo, error) {
	schemaID := job.SchemaID
	tableID := job.TableID
	tblInfo, err := t.GetTable(schemaID, tableID)
	if errors2.ErrorEqual(err, meta.ErrDBNotExists) {
		job.State = model.JobCancelled
		return nil, errors.Trace(ErrNotExists)
	} else if err != nil {
		return nil, errors.Trace(err)
	} else if tblInfo == nil {
		job.State = model.JobCancelled
		return nil, errors.Trace(ErrNotExists)
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobCancelled
		return nil, errors.Errorf("table %s is not in public, but %s", tblInfo.Name.L, tblInfo.State)
	}

	return tblInfo, nil
}

// FindCol finds column in cols by name.
func findCol(cols []*model.ColumnInfo, name string) (c *model.ColumnInfo) {
	name = strings.ToLower(name)
	for _, c = range cols {
		if c.Name.L == name {
			return
		}
	}
	return nil
}

func buildIndexInfo(tblInfo *model.TableInfo, unique bool, indexName model.CIStr, idxColNames []*coldef.IndexColName) (*model.IndexInfo, error) {
	for _, col := range tblInfo.Columns {
		if col.Name.L == indexName.L {
			return nil, errors.Errorf("CREATE INDEX: index name collision with existing column: %s", indexName)
		}
	}

	// build offsets
	idxColumns := make([]*model.IndexColumn, 0, len(idxColNames))
	for _, ic := range idxColNames {
		col := findCol(tblInfo.Columns, ic.ColumnName)
		if col == nil {
			return nil, errors.Errorf("CREATE INDEX: column does not exist: %s", ic.ColumnName)
		}

		idxColumns = append(idxColumns, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ic.Length,
		})
	}
	// create index info
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		Unique:  unique,
		State:   model.StateNone,
	}

	return idxInfo, nil
}

func addIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].Flag |= mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[col.Offset].Flag |= mysql.MultipleKeyFlag
	}

}

func dropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	col := indexInfo.Columns[0]

	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].Flag &= ^uint(mysql.UniqueKeyFlag)
	} else {
		tblInfo.Columns[col.Offset].Flag &= ^uint(mysql.MultipleKeyFlag)
	}

	// other index may still cover this col
	for _, index := range tblInfo.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.Columns[0].Name.L != col.Name.L {
			continue
		}

		addIndexColumnFlag(tblInfo, index)
	}
}

func (d *ddl) onIndexCreate(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := d.getTableInfo(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	var (
		unique      bool
		indexName   model.CIStr
		idxColNames []*coldef.IndexColName
	)

	err = job.DecodeArgs(&unique, &indexName, &idxColNames)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	var indexInfo *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == indexName.L {
			if idx.State == model.StatePublic {
				// we already have a index with same index name
				job.State = model.JobCancelled
				return errors.Errorf("CREATE INDEX: index already exist %s", indexName)
			}

			indexInfo = idx
		}
	}

	if indexInfo == nil {
		indexInfo, err = buildIndexInfo(tblInfo, unique, indexName, idxColNames)
		if err != nil {
			job.State = model.JobCancelled
			return errors.Trace(err)
		}
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> public
		job.SchemaState = model.StateReorgnization
		indexInfo.State = model.StateReorgnization
		// initialize SnapshotVer to 0 for later reorgnization check.
		job.SnapshotVer = 0
		// initialize reorg handle to 0
		job.ReOrgHandle = 0
		atomic.StoreInt64(&d.reOrgHandle, 0)
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateReorgnization:
		// reorganization -> public
		// get the current version for reorgnization if we don't have
		if job.SnapshotVer == 0 {
			var ver kv.Version
			ver, err = d.store.CurrentVersion()
			if err != nil {
				return errors.Trace(err)
			}

			job.SnapshotVer = ver.Ver
		}

		var tbl table.Table
		tbl, err = d.getTable(t, schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.addTableIndex(tbl, indexInfo, job.SnapshotVer, job.ReOrgHandle)
		})

		// addTableIndex updates ReOrgHandle after one batch.
		// so we update the job ReOrgHandle here.
		job.ReOrgHandle = atomic.LoadInt64(&d.reOrgHandle)

		if errors2.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		indexInfo.State = model.StatePublic
		// set column index flag.
		addIndexColumnFlag(tblInfo, indexInfo)
		if err = t.UpdateTable(schemaID, tblInfo); err != nil {
			return errors.Trace(err)
		}

		// finish this job
		job.SchemaState = model.StatePublic
		job.State = model.JobDone
		return nil
	default:
		return errors.Errorf("invalid index state %v", tblInfo.State)
	}
}

func (d *ddl) onIndexDrop(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := d.getTableInfo(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	var indexName model.CIStr
	if err = job.DecodeArgs(&indexName); err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	var indexInfo *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == indexName.L {
			indexInfo = idx
		}
	}

	if indexInfo == nil {
		job.State = model.JobCancelled
		return errors.Errorf("index %s doesn't exist", indexName)
	}

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch indexInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateReorgnization
		indexInfo.State = model.StateReorgnization
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateReorgnization:
		// reorganization -> absent
		tbl, err := d.getTable(t, schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.dropTableIndex(tbl, indexInfo)
		})

		if errors2.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// all reorgnization jobs done, drop this index
		newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			if idx.Name.L != indexName.L {
				newIndices = append(newIndices, idx)
			}
		}
		tblInfo.Indices = newIndices
		// set column index flag.
		dropIndexColumnFlag(tblInfo, indexInfo)
		if err = t.UpdateTable(schemaID, tblInfo); err != nil {
			return errors.Trace(err)
		}

		// finish this job
		job.SchemaState = model.StateNone
		job.State = model.JobDone
		return nil
	default:
		return errors.Errorf("invalid table state %v", tblInfo.State)
	}
}

func checkRowExist(txn kv.Transaction, t table.Table, handle int64) (bool, error) {
	_, err := txn.Get([]byte(t.RecordKey(handle, nil)))
	if errors2.ErrorEqual(err, kv.ErrNotExist) {
		// If row doesn't exist, we may have deleted the row already,
		// no need to add index again.
		return false, nil
	} else if err != nil {
		return false, errors.Trace(err)
	}

	return true, nil
}

func fetchRowColVals(txn kv.Transaction, t table.Table, handle int64, indexInfo *model.IndexInfo) ([]interface{}, error) {
	// fetch datas
	cols := t.Cols()
	var vals []interface{}
	for _, v := range indexInfo.Columns {
		var val interface{}

		col := cols[v.Offset]
		k := t.RecordKey(handle, col)
		data, err := txn.Get([]byte(k))
		if err != nil {
			return nil, errors.Trace(err)
		}
		val, err = t.DecodeValue(data, col)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, val)
	}

	return vals, nil
}

const maxBatchSize = 1024

// How to add index in reorgnization state?
//  1, Generate a snapshot with special version.
//  2, Traverse the snapshot, get every row in the table.
//  3, For one row, if the row has been already deleted, skip to next row.
//  4, If not deleted, check whether index has existed, if existed, skip to next row.
//  5, If index doesn't exist, create the index and then continue to handle next row.
func (d *ddl) addTableIndex(t table.Table, indexInfo *model.IndexInfo, version uint64, seekHandle int64) error {
	for {
		handles, err := d.getSnapshotRows(t, version, seekHandle)
		if err != nil {
			return errors.Trace(err)
		} else if len(handles) == 0 {
			return nil
		}

		seekHandle = handles[len(handles)-1] + 1

		err = d.backfillTableIndex(t, indexInfo, handles)
		if err != nil {
			return errors.Trace(err)
		}

		// update reOrgHandle here after every successful batch.
		atomic.StoreInt64(&d.reOrgHandle, seekHandle)
	}
}

func (d *ddl) getSnapshotRows(t table.Table, version uint64, seekHandle int64) ([]int64, error) {
	ver := kv.Version{Ver: version}

	snap, err := d.store.GetSnapshot(ver)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer snap.MvccRelease()

	firstKey := t.RecordKey(seekHandle, nil)
	prefix := []byte(t.KeyPrefix())

	it := snap.NewMvccIterator(kv.EncodeKey([]byte(firstKey)), ver)
	defer it.Close()

	handles := make([]int64, 0, maxBatchSize)

	for it.Valid() {
		key := kv.DecodeKey([]byte(it.Key()))
		if !bytes.HasPrefix(key, prefix) {
			break
		}

		var handle int64
		handle, err = util.DecodeHandleFromRowKey(string(key))
		if err != nil {
			return nil, errors.Trace(err)
		}

		rk := kv.EncodeKey(t.RecordKey(handle, nil))

		handles = append(handles, handle)
		if len(handles) == maxBatchSize {
			seekHandle = handle + 1
			break
		}

		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if errors2.ErrorEqual(err, kv.ErrNotExist) {
			break
		} else if err != nil {
			return nil, errors.Trace(err)
		}
	}

	snap.MvccRelease()

	return handles, nil
}

func (d *ddl) backfillTableIndex(t table.Table, indexInfo *model.IndexInfo, handles []int64) error {
	kvX := kv.NewKVIndex(t.IndexPrefix(), indexInfo.Name.L, indexInfo.Unique)

	for _, handle := range handles {
		log.Debug("building index...", handle)

		err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
			// first check row exists
			exist, err := checkRowExist(txn, t, handle)
			if err != nil {
				return errors.Trace(err)
			} else if !exist {
				// row doesn't exist, skip it.
				return nil
			}

			var vals []interface{}
			vals, err = fetchRowColVals(txn, t, handle, indexInfo)
			if err != nil {
				return errors.Trace(err)
			}

			exist, _, err = kvX.Exist(txn, vals, handle)
			if err != nil {
				return errors.Trace(err)
			} else if exist {
				// index already exists, skip it.
				return nil
			}

			// mean we haven't already added this index.
			// lock row first
			err = txn.LockKeys(t.RecordKey(handle, nil))
			if err != nil {
				return errors.Trace(err)
			}

			// create the index.
			err = kvX.Create(txn, vals, handle)
			return errors.Trace(err)
		})

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) dropTableIndex(t table.Table, indexInfo *model.IndexInfo) error {
	err := d.delKeysWithPrefix(kv.GenIndexPrefix(t.IndexPrefix(), indexInfo.Name.L))
	return errors.Trace(err)
}

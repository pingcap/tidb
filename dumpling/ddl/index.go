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
	"io"
	"strings"

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

func (d *ddl) checkTablePublic(t *meta.Meta, job *model.Job) (*model.TableInfo, error) {
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
	for i, ic := range idxColNames {
		col := findCol(tblInfo.Columns, ic.ColumnName)
		if col == nil {
			return nil, errors.Errorf("CREATE INDEX: column does not exist: %s", ic.ColumnName)
		}

		idxColumns = append(idxColumns, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ic.Length,
		})
		// Set ColumnInfo flag
		if i == 0 {
			if unique && len(idxColNames) == 1 {
				tblInfo.Columns[col.Offset].Flag |= mysql.UniqueKeyFlag
			} else {
				tblInfo.Columns[col.Offset].Flag |= mysql.MultipleKeyFlag
			}
		}
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

func (d *ddl) onIndexCreate(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID

	tblInfo, err := d.checkTablePublic(t, job)
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

		// get the current version for later Reorgnization.
		var ver kv.Version
		ver, err = d.store.CurrentVersion()
		if err != nil {
			return errors.Trace(err)
		}

		job.SnapshotVer = ver.Ver

		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateReorgnization:
		// reorganization -> public
		var tbl table.Table
		tbl, err = d.getTable(t, schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.addTableIndex(tbl, indexInfo, job.SnapshotVer)
		})

		if errors2.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		indexInfo.State = model.StatePublic
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

	tblInfo, err := d.checkTablePublic(t, job)
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
		var tbl table.Table
		tbl, err = d.getTable(t, schemaID, tblInfo)
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
		// if row doesn't exist, we may delete the row already, no need to add index again.
		return false, nil
	} else if err != nil {
		return false, errors.Trace(err)
	}

	return true, nil
}

func fetchCurrentRowColVals(txn kv.Transaction, t table.Table, handle int64, indexInfo *model.IndexInfo) ([]interface{}, error) {
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

func fetchSnapRowColVals(snap kv.MvccSnapshot, ver kv.Version, t table.Table, handle int64, indexInfo *model.IndexInfo) ([]interface{}, error) {
	// fetch datas
	cols := t.Cols()
	var vals []interface{}
	for _, v := range indexInfo.Columns {
		var val interface{}

		col := cols[v.Offset]
		k := t.RecordKey(handle, col)
		data, err := snap.MvccGet([]byte(k), ver)
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

func checkIndexExist(txn kv.Transaction, kvIndex kv.Index, h int64, vals []interface{}) (bool, error) {
	it, hit, err := kvIndex.Seek(txn, vals)
	if err != nil || !hit {
		return false, errors.Trace(err)
	}
	defer it.Close()
	for {
		var handle int64
		_, handle, err = it.Next()
		if errors2.ErrorEqual(err, io.EOF) {
			err = nil
		} else if err != nil {
			return false, errors.Trace(err)
		}

		// we must check handle equation to prevent multi row with same index data in none unique index.
		if handle == h {
			return true, nil
		}
	}
}

func (d *ddl) needAddingIndexForRow(txn kv.Transaction, t table.Table, handle int64, kvIndex kv.Index, indexInfo *model.IndexInfo) (bool, error) {
	if ok, err := checkRowExist(txn, t, handle); err != nil {
		return false, errors.Trace(err)
	} else if !ok {
		// if row doesn't exist, we don't need to add index
		return false, nil
	}

	vals, err := fetchCurrentRowColVals(txn, t, handle, indexInfo)
	if err != nil {
		return false, errors.Trace(err)
	}

	if ok, err := checkIndexExist(txn, kvIndex, handle, vals); err != nil {
		return false, errors.Trace(err)
	} else if ok {
		// index exists, we don't need to add again
		return false, nil
	}

	return true, nil
}

func (d *ddl) addTableIndex(t table.Table, indexInfo *model.IndexInfo, version uint64) error {
	ver := kv.Version{Ver: version}

	snap, err := d.store.GetSnapshot(ver)
	if err != nil {
		return errors.Trace(err)
	}

	defer snap.MvccRelease()

	firstKey := t.FirstKey()
	prefix := t.KeyPrefix()

	ctx := d.newReorgContext()
	txn, err := ctx.GetTxn(true)
	if err != nil {
		return errors.Trace(err)
	}
	defer txn.Rollback()

	it := snap.NewMvccIterator([]byte(firstKey), ver)
	defer it.Close()

	kvX := kv.NewKVIndex(t.IndexPrefix(), indexInfo.Name.L, indexInfo.Unique)

	for it.Valid() && strings.HasPrefix(it.Key(), prefix) {
		var handle int64
		handle, err = util.DecodeHandleFromRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("building index...", handle)

		// first check need adding index or not.
		var need bool
		need, err = d.needAddingIndexForRow(txn, t, handle, kvX, indexInfo)
		if err != nil {
			return errors.Trace(err)
		}

		if need {
			var vals []interface{}
			vals, err = fetchSnapRowColVals(snap, ver, t, handle, indexInfo)
			if err != nil {
				return errors.Trace(err)
			}

			var indexExist bool
			indexExist, err = checkIndexExist(txn, kvX, handle, vals)
			if err != nil {
				return errors.Trace(err)
			}

			if !indexExist {
				// mean we haven't already added this index.
				// should lock row here???
				err = t.LockRow(ctx, handle, true)
				if err != nil {
					return errors.Trace(err)
				}

				// create the index.
				err = kvX.Create(txn, vals, handle)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		rk := []byte(t.RecordKey(handle, nil))
		it, err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(txn.Commit())
}

func (d *ddl) dropTableIndex(t table.Table, indexInfo *model.IndexInfo) error {
	ctx := d.newReorgContext()
	txn, err := ctx.GetTxn(true)

	if err != nil {
		return errors.Trace(err)
	}

	// Remove indices.
	for _, v := range t.Indices() {
		if v != nil && v.X != nil && v.Name.L == indexInfo.Name.L {
			if err = v.X.Drop(txn); err != nil {
				ctx.FinishTxn(true)
				return errors.Trace(err)
			}
		}
	}

	return errors.Trace(ctx.FinishTxn(false))
}

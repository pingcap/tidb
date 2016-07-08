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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
)

func buildIndexInfo(tblInfo *model.TableInfo, unique bool, indexName model.CIStr, indexID int64,
	idxColNames []*ast.IndexColName) (*model.IndexInfo, error) {
	// build offsets
	idxColumns := make([]*model.IndexColumn, 0, len(idxColNames))
	for _, ic := range idxColNames {
		col := findCol(tblInfo.Columns, ic.Column.Name.O)
		if col == nil {
			return nil, infoschema.ErrColumnNotExists.Gen("CREATE INDEX: column does not exist: %s",
				ic.Column.Name.O)
		}

		// Length must be specified for BLOB and TEXT column indexes.
		if types.IsTypeBlob(col.FieldType.Tp) && ic.Length == types.UnspecifiedLength {
			return nil, errors.Trace(errBlobKeyWithoutLength)
		}

		if ic.Length != types.UnspecifiedLength &&
			!types.IsTypeChar(col.FieldType.Tp) &&
			!types.IsTypeBlob(col.FieldType.Tp) {
			return nil, errors.Trace(errIncorrectPrefixKey)
		}

		idxColumns = append(idxColumns, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ic.Length,
		})
	}
	// create index info
	idxInfo := &model.IndexInfo{
		ID:      indexID,
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

func (d *ddl) onCreateIndex(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := d.getTableInfo(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	var (
		unique      bool
		indexName   model.CIStr
		indexID     int64
		idxColNames []*ast.IndexColName
	)

	err = job.DecodeArgs(&unique, &indexName, &indexID, &idxColNames)
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
				return infoschema.ErrIndexExists.Gen("CREATE INDEX: index already exist %s", indexName)
			}

			indexInfo = idx
		}
	}

	if indexInfo == nil {
		indexInfo, err = buildIndexInfo(tblInfo, unique, indexName, indexID, idxColNames)
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
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		indexInfo.State = model.StateWriteReorganization
		// initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateWriteReorganization:
		// reorganization -> public
		reorgInfo, err := d.getReorgInfo(t, job)
		if err != nil || reorgInfo.first {
			// if we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return errors.Trace(err)
		}

		var tbl table.Table
		tbl, err = d.getTable(schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.addTableIndex(tbl, indexInfo, reorgInfo)
		})

		if terror.ErrorEqual(err, errWaitReorgTimeout) {
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
		return ErrInvalidIndexState.Gen("invalid index state %v", tblInfo.State)
	}
}

func (d *ddl) onDropIndex(t *meta.Meta, job *model.Job) error {
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
		return ErrCantDropFieldOrKey.Gen("index %s doesn't exist", indexName)
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
		job.SchemaState = model.StateDeleteReorganization
		indexInfo.State = model.StateDeleteReorganization
		err = t.UpdateTable(schemaID, tblInfo)
		return errors.Trace(err)
	case model.StateDeleteReorganization:
		// reorganization -> absent
		tbl, err := d.getTable(schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		err = d.runReorgJob(func() error {
			return d.dropTableIndex(tbl, indexInfo)
		})

		if terror.ErrorEqual(err, errWaitReorgTimeout) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		// all reorganization jobs done, drop this index
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
		return ErrInvalidTableState.Gen("invalid table state %v", tblInfo.State)
	}
}

func fetchRowColVals(txn kv.Transaction, t table.Table, handle int64, indexInfo *model.IndexInfo) ([]types.Datum, error) {
	// fetch datas
	cols := t.Cols()
	colMap := make(map[int64]*types.FieldType)
	for _, v := range indexInfo.Columns {
		col := cols[v.Offset]
		colMap[col.ID] = &col.FieldType
	}
	rowKey := tablecodec.EncodeRecordKey(t.RecordPrefix(), handle)
	rowVal, err := txn.Get(rowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	row, err := tablecodec.DecodeRow(rowVal, colMap)
	if err != nil {
		return nil, errors.Trace(err)
	}
	vals := make([]types.Datum, 0, len(indexInfo.Columns))
	for _, v := range indexInfo.Columns {
		col := cols[v.Offset]
		vals = append(vals, row[col.ID])
	}
	return vals, nil
}

const maxBatchSize = 1024

// How to add index in reorganization state?
//  1. Generate a snapshot with special version.
//  2. Traverse the snapshot, get every row in the table.
//  3. For one row, if the row has been already deleted, skip to next row.
//  4. If not deleted, check whether index has existed, if existed, skip to next row.
//  5. If index doesn't exist, create the index and then continue to handle next row.
func (d *ddl) addTableIndex(t table.Table, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) error {
	seekHandle := reorgInfo.Handle
	version := reorgInfo.SnapshotVer
	for {
		handles, err := d.getSnapshotRows(t, version, seekHandle)
		if err != nil {
			return errors.Trace(err)
		} else if len(handles) == 0 {
			return nil
		}

		seekHandle = handles[len(handles)-1] + 1

		err = d.backfillTableIndex(t, indexInfo, handles, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func (d *ddl) getSnapshotRows(t table.Table, version uint64, seekHandle int64) ([]int64, error) {
	ver := kv.Version{Ver: version}

	snap, err := d.store.GetSnapshot(ver)
	if err != nil {
		return nil, errors.Trace(err)
	}

	firstKey := t.RecordKey(seekHandle)

	it, err := snap.Seek(firstKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Close()

	handles := make([]int64, 0, maxBatchSize)

	for it.Valid() {
		if !it.Key().HasPrefix(t.RecordPrefix()) {
			break
		}

		var handle int64
		handle, err = tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return nil, errors.Trace(err)
		}

		rk := t.RecordKey(handle)

		handles = append(handles, handle)
		if len(handles) == maxBatchSize {
			break
		}

		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if terror.ErrorEqual(err, kv.ErrNotExist) {
			break
		} else if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return handles, nil
}

func (d *ddl) backfillTableIndex(t table.Table, indexInfo *model.IndexInfo, handles []int64, reorgInfo *reorgInfo) error {
	kvX := tables.NewIndex(t.Meta(), indexInfo)

	for _, handle := range handles {
		log.Debug("[ddl] building index...", handle)

		err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
			if err := d.isReorgRunnable(txn); err != nil {
				return errors.Trace(err)
			}

			vals, err1 := fetchRowColVals(txn, t, handle, indexInfo)
			if terror.ErrorEqual(err1, kv.ErrNotExist) {
				// row doesn't exist, skip it.
				return nil
			}
			if err1 != nil {
				return errors.Trace(err1)
			}

			exist, _, err1 := kvX.Exist(txn, vals, handle)
			if err1 != nil {
				return errors.Trace(err1)
			} else if exist {
				// index already exists, skip it.
				return nil
			}
			rowKey := tablecodec.EncodeRecordKey(t.RecordPrefix(), handle)
			err1 = txn.LockKeys(rowKey)
			if err1 != nil {
				return errors.Trace(err1)
			}

			// create the index.
			err1 = kvX.Create(txn, vals, handle)
			if err1 != nil {
				return errors.Trace(err1)
			}

			// update reorg next handle
			return errors.Trace(reorgInfo.UpdateHandle(txn, handle))
		})

		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) dropTableIndex(t table.Table, indexInfo *model.IndexInfo) error {
	prefix := tablecodec.EncodeTableIndexPrefix(t.Meta().ID, indexInfo.ID)
	err := d.delKeysWithPrefix(prefix)

	return errors.Trace(err)
}

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

package admin

import (
	"fmt"
	"io"
	"reflect"
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
)

// DDLInfo is for DDL information.
type DDLInfo struct {
	SchemaVer   int64
	ReorgHandle int64        // It's only used for DDL information.
	Jobs        []*model.Job // It's the currently running jobs.
}

// GetDDLInfo returns DDL information.
func GetDDLInfo(txn kv.Transaction) (*DDLInfo, error) {
	var err error
	info := &DDLInfo{}
	t := meta.NewMeta(txn)

	info.Jobs = make([]*model.Job, 0, 2)
	job, err := t.GetDDLJobByIdx(0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if job != nil {
		info.Jobs = append(info.Jobs, job)
	}
	addIdxJob, err := t.GetDDLJobByIdx(0, meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if addIdxJob != nil {
		info.Jobs = append(info.Jobs, addIdxJob)
	}

	info.SchemaVer, err = t.GetSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if addIdxJob == nil {
		return info, nil
	}

	info.ReorgHandle, _, _, err = t.GetDDLReorgHandle(addIdxJob)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return info, nil
}

// CancelJobs cancels the DDL jobs.
func CancelJobs(txn kv.Transaction, ids []int64) ([]error, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	jobs, err := GetDDLJobs(txn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	errs := make([]error, len(ids))
	t := meta.NewMeta(txn)
	for i, id := range ids {
		found := false
		for j, job := range jobs {
			if id != job.ID {
				log.Debugf("the job ID %d that needs to be canceled isn't equal to current job ID %d", id, job.ID)
				continue
			}
			found = true
			// These states can't be cancelled.
			if job.IsDone() || job.IsSynced() {
				errs[i] = errors.New("This job is finished, so can't be cancelled")
				continue
			}
			// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
			if job.IsCancelled() || job.IsRollingback() || job.IsRollbackDone() {
				continue
			}
			job.State = model.JobStateCancelling
			// Make sure RawArgs isn't overwritten.
			err := job.DecodeArgs(job.RawArgs)
			if err != nil {
				errs[i] = errors.Trace(err)
				continue
			}
			if job.Type == model.ActionAddIndex {
				err = t.UpdateDDLJob(int64(j), job, true, meta.AddIndexJobListKey)
			} else {
				err = t.UpdateDDLJob(int64(j), job, true)
			}
			if err != nil {
				errs[i] = errors.Trace(err)
			}
		}
		if !found {
			errs[i] = errors.New("Can't find this job")
		}
	}
	return errs, nil
}

func getDDLJobsInQueue(t *meta.Meta, jobListKey meta.JobListKeyType) ([]*model.Job, error) {
	cnt, err := t.DDLJobQueueLen(jobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*model.Job, cnt)
	for i := range jobs {
		jobs[i], err = t.GetDDLJobByIdx(int64(i), jobListKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return jobs, nil
}

// GetDDLJobs get all DDL jobs and sorts jobs by job.ID.
func GetDDLJobs(txn kv.Transaction) ([]*model.Job, error) {
	t := meta.NewMeta(txn)
	generalJobs, err := getDDLJobsInQueue(t, meta.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	addIdxJobs, err := getDDLJobsInQueue(t, meta.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := append(generalJobs, addIdxJobs...)
	sort.Sort(jobArray(jobs))
	return jobs, nil
}

type jobArray []*model.Job

func (v jobArray) Len() int {
	return len(v)
}

func (v jobArray) Less(i, j int) bool {
	return v[i].ID < v[j].ID
}

func (v jobArray) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

// MaxHistoryJobs is exported for testing.
const MaxHistoryJobs = 10

// DefNumHistoryJobs is default value of the default number of history job
const DefNumHistoryJobs = 10

// GetHistoryDDLJobs returns the DDL history jobs and an error.
// The maximum count of history jobs is num.
func GetHistoryDDLJobs(txn kv.Transaction, maxNumJobs int) ([]*model.Job, error) {
	t := meta.NewMeta(txn)
	jobs, err := t.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	jobsLen := len(jobs)
	if jobsLen > maxNumJobs {
		start := jobsLen - maxNumJobs
		jobs = jobs[start:]
	}
	jobsLen = len(jobs)
	ret := make([]*model.Job, 0, jobsLen)
	for i := jobsLen - 1; i >= 0; i-- {
		ret = append(ret, jobs[i])
	}
	return ret, nil
}

func nextIndexVals(data []types.Datum) []types.Datum {
	// Add 0x0 to the end of data.
	return append(data, types.Datum{})
}

// RecordData is the record data composed of a handle and values.
type RecordData struct {
	Handle int64
	Values []types.Datum
}

func getCount(ctx sessionctx.Context, sql string) (int64, error) {
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) != 1 {
		return 0, errors.Errorf("can not get count, sql %s result rows %d", sql, len(rows))
	}
	return rows[0].GetInt64(0), nil
}

// CheckIndicesCount compares indices count with table count.
// It returns nil if the count from the index is equal to the count from the table columns,
// otherwise it returns an error with a different information.
func CheckIndicesCount(ctx sessionctx.Context, dbName, tableName string, indices []string) error {
	// Add `` for some names like `table name`.
	sql := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", dbName, tableName)
	tblCnt, err := getCount(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, idx := range indices {
		sql = fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` USE INDEX(`%s`)", dbName, tableName, idx)
		idxCnt, err := getCount(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		if tblCnt != idxCnt {
			return errors.Errorf("table count %d != index(%s) count %d", tblCnt, idx, idxCnt)
		}
	}

	return nil
}

// ScanIndexData scans the index handles and values in a limited number, according to the index information.
// It returns data and the next startVals until it doesn't have data, then returns data is nil and
// the next startVals is the values which can't get data. If startVals = nil and limit = -1,
// it returns the index data of the whole.
func ScanIndexData(sc *stmtctx.StatementContext, txn kv.Transaction, kvIndex table.Index, startVals []types.Datum, limit int64) (
	[]*RecordData, []types.Datum, error) {
	it, _, err := kvIndex.Seek(sc, txn, startVals)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer it.Close()

	var idxRows []*RecordData
	var curVals []types.Datum
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

// CompareIndexData compares index data one by one.
// It returns nil if the data from the index is equal to the data from the table columns,
// otherwise it returns an error with a different set of records.
func CompareIndexData(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index) error {
	err := checkIndexAndRecord(sessCtx, txn, t, idx)
	if err != nil {
		return errors.Trace(err)
	}

	return CheckRecordAndIndex(sessCtx, txn, t, idx)
}

func getIndexFieldTypes(t table.Table, idx table.Index) ([]*types.FieldType, error) {
	idxColumns := idx.Meta().Columns
	tblColumns := t.Meta().Columns
	fieldTypes := make([]*types.FieldType, 0, len(idxColumns))
	for _, col := range idxColumns {
		colInfo := model.FindColumnInfo(tblColumns, col.Name.L)
		if colInfo == nil {
			return nil, errors.Errorf("index col:%v not found in table:%v", col.Name.String(), t.Meta().Name.String())
		}

		fieldTypes = append(fieldTypes, &colInfo.FieldType)
	}
	return fieldTypes, nil
}

// adjustDatumKind treats KindString as KindBytes.
func adjustDatumKind(vals1, vals2 []types.Datum) {
	if len(vals1) != len(vals2) {
		return
	}

	for i, val1 := range vals1 {
		val2 := vals2[i]
		if val1.Kind() != val2.Kind() {
			if (val1.Kind() == types.KindBytes || val1.Kind() == types.KindString) &&
				(val2.Kind() == types.KindBytes || val2.Kind() == types.KindString) {
				vals1[i].SetBytes(val1.GetBytes())
				vals2[i].SetBytes(val2.GetBytes())
			}
		}
	}
}

func checkIndexAndRecord(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index) error {
	it, err := idx.SeekFirst(txn)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	cols := make([]*table.Column, len(idx.Meta().Columns))
	for i, col := range idx.Meta().Columns {
		cols[i] = t.Cols()[col.Offset]
	}

	fieldTypes, err := getIndexFieldTypes(t, idx)
	if err != nil {
		return errors.Trace(err)
	}
	for {
		vals1, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		} else if err != nil {
			return errors.Trace(err)
		}

		vals1, err = tablecodec.UnflattenDatums(vals1, fieldTypes, sessCtx.GetSessionVars().Location())
		if err != nil {
			return errors.Trace(err)
		}
		vals2, err := rowWithCols(sessCtx, txn, t, h, cols)
		vals2 = tables.TruncateIndexValuesIfNeeded(t.Meta(), idx.Meta(), vals2)
		if kv.ErrNotExist.Equal(err) {
			record := &RecordData{Handle: h, Values: vals1}
			err = ErrDataInConsistent.Gen("index:%#v != record:%#v", record, nil)
		}
		if err != nil {
			return errors.Trace(err)
		}
		adjustDatumKind(vals1, vals2)
		if !reflect.DeepEqual(vals1, vals2) {
			record1 := &RecordData{Handle: h, Values: vals1}
			record2 := &RecordData{Handle: h, Values: vals2}
			return ErrDataInConsistent.Gen("index:%#v != record:%#v", record1, record2)
		}
	}

	return nil
}

// CheckRecordAndIndex is exported for testing.
func CheckRecordAndIndex(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index) error {
	sc := sessCtx.GetSessionVars().StmtCtx
	cols := make([]*table.Column, len(idx.Meta().Columns))
	for i, col := range idx.Meta().Columns {
		cols[i] = t.Cols()[col.Offset]
	}

	startKey := t.RecordKey(0)
	filterFunc := func(h1 int64, vals1 []types.Datum, cols []*table.Column) (bool, error) {
		for i, val := range vals1 {
			col := cols[i]
			if val.IsNull() {
				if mysql.HasNotNullFlag(col.Flag) {
					return false, errors.New("Miss")
				}
				// NULL value is regarded as its default value.
				colDefVal, err := table.GetColOriginDefaultValue(sessCtx, col.ToInfo())
				if err != nil {
					return false, errors.Trace(err)
				}
				vals1[i] = colDefVal
			}
		}
		isExist, h2, err := idx.Exist(sc, txn, vals1, h1)
		if kv.ErrKeyExists.Equal(err) {
			record1 := &RecordData{Handle: h1, Values: vals1}
			record2 := &RecordData{Handle: h2, Values: vals1}
			return false, ErrDataInConsistent.Gen("index:%#v != record:%#v", record2, record1)
		}
		if err != nil {
			return false, errors.Trace(err)
		}
		if !isExist {
			record := &RecordData{Handle: h1, Values: vals1}
			return false, ErrDataInConsistent.Gen("index:%#v != record:%#v", nil, record)
		}

		return true, nil
	}
	err := iterRecords(sessCtx, txn, t, startKey, cols, filterFunc)

	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func scanTableData(sessCtx sessionctx.Context, retriever kv.Retriever, t table.Table, cols []*table.Column, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	var records []*RecordData

	startKey := t.RecordKey(startHandle)
	filterFunc := func(h int64, d []types.Datum, cols []*table.Column) (bool, error) {
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
	}
	err := iterRecords(sessCtx, retriever, t, startKey, cols, filterFunc)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	if len(records) == 0 {
		return records, startHandle, nil
	}

	nextHandle := records[len(records)-1].Handle + 1

	return records, nextHandle, nil
}

// ScanTableRecord scans table row handles and column values in a limited number.
// It returns data and the next startHandle until it doesn't have data, then returns data is nil and
// the next startHandle is the handle which can't get data. If startHandle = 0 and limit = -1,
// it returns the table data of the whole.
func ScanTableRecord(sessCtx sessionctx.Context, retriever kv.Retriever, t table.Table, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	return scanTableData(sessCtx, retriever, t, t.Cols(), startHandle, limit)
}

// ScanSnapshotTableRecord scans the ver version of the table data in a limited number.
// It returns data and the next startHandle until it doesn't have data, then returns data is nil and
// the next startHandle is the handle which can't get data. If startHandle = 0 and limit = -1,
// it returns the table data of the whole.
func ScanSnapshotTableRecord(sessCtx sessionctx.Context, store kv.Storage, ver kv.Version, t table.Table, startHandle, limit int64) (
	[]*RecordData, int64, error) {
	snap, err := store.GetSnapshot(ver)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	records, nextHandle, err := ScanTableRecord(sessCtx, snap, t, startHandle, limit)

	return records, nextHandle, errors.Trace(err)
}

// CompareTableRecord compares data and the corresponding table data one by one.
// It returns nil if data is equal to the data that scans from table, otherwise
// it returns an error with a different set of records. If exact is false, only compares handle.
func CompareTableRecord(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, data []*RecordData, exact bool) error {
	m := make(map[int64][]types.Datum, len(data))
	for _, r := range data {
		if _, ok := m[r.Handle]; ok {
			return errRepeatHandle.Gen("handle:%d is repeated in data", r.Handle)
		}
		m[r.Handle] = r.Values
	}

	startKey := t.RecordKey(0)
	filterFunc := func(h int64, vals []types.Datum, cols []*table.Column) (bool, error) {
		vals2, ok := m[h]
		if !ok {
			record := &RecordData{Handle: h, Values: vals}
			return false, ErrDataInConsistent.Gen("data:%#v != record:%#v", nil, record)
		}
		if !exact {
			delete(m, h)
			return true, nil
		}

		if !reflect.DeepEqual(vals, vals2) {
			record1 := &RecordData{Handle: h, Values: vals2}
			record2 := &RecordData{Handle: h, Values: vals}
			return false, ErrDataInConsistent.Gen("data:%#v != record:%#v", record1, record2)
		}

		delete(m, h)

		return true, nil
	}
	err := iterRecords(sessCtx, txn, t, startKey, t.Cols(), filterFunc)
	if err != nil {
		return errors.Trace(err)
	}

	for h, vals := range m {
		record := &RecordData{Handle: h, Values: vals}
		return ErrDataInConsistent.Gen("data:%#v != record:%#v", record, nil)
	}

	return nil
}

func rowWithCols(sessCtx sessionctx.Context, txn kv.Retriever, t table.Table, h int64, cols []*table.Column) ([]types.Datum, error) {
	key := t.RecordKey(h)
	value, err := txn.Get(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.State != model.StatePublic {
			return nil, errInvalidColumnState.Gen("Cannot use none public column - %v", cols)
		}
		if col.IsPKHandleColumn(t.Meta()) {
			if mysql.HasUnsignedFlag(col.Flag) {
				v[i].SetUint64(uint64(h))
			} else {
				v[i].SetInt64(h)
			}
			continue
		}
		colTps[col.ID] = &col.FieldType
	}
	row, err := tablecodec.DecodeRow(value, colTps, sessCtx.GetSessionVars().Location())
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.State != model.StatePublic {
			// TODO: check this
			return nil, errInvalidColumnState.Gen("Cannot use none public column - %v", cols)
		}
		if col.IsPKHandleColumn(t.Meta()) {
			continue
		}
		ri, ok := row[col.ID]
		if !ok {
			if mysql.HasNotNullFlag(col.Flag) {
				return nil, errors.New("Miss")
			}
			// NULL value is regarded as its default value.
			colDefVal, err := table.GetColOriginDefaultValue(sessCtx, col.ToInfo())
			if err != nil {
				return nil, errors.Trace(err)
			}
			v[i] = colDefVal
			continue
		}
		v[i] = ri
	}
	return v, nil
}

func iterRecords(sessCtx sessionctx.Context, retriever kv.Retriever, t table.Table, startKey kv.Key, cols []*table.Column,
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

	colMap := make(map[int64]*types.FieldType, len(cols))
	for _, col := range cols {
		colMap[col.ID] = &col.FieldType
	}
	prefix := t.RecordPrefix()
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}

		rowMap, err := tablecodec.DecodeRow(it.Value(), colMap, sessCtx.GetSessionVars().Location())
		if err != nil {
			return errors.Trace(err)
		}
		data := make([]types.Datum, 0, len(cols))
		for _, col := range cols {
			if col.IsPKHandleColumn(t.Meta()) {
				if mysql.HasUnsignedFlag(col.Flag) {
					data = append(data, types.NewUintDatum(uint64(handle)))
				} else {
					data = append(data, types.NewIntDatum(handle))
				}
			} else {
				data = append(data, rowMap[col.ID])
			}
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return errors.Trace(err)
		}

		rk := t.RecordKey(handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// admin error codes.
const (
	codeDataNotEqual       terror.ErrCode = 1
	codeRepeatHandle                      = 2
	codeInvalidColumnState                = 3
)

var (
	// ErrDataInConsistent indicate that meets inconsistent data.
	ErrDataInConsistent   = terror.ClassAdmin.New(codeDataNotEqual, "data isn't equal")
	errRepeatHandle       = terror.ClassAdmin.New(codeRepeatHandle, "handle is repeated")
	errInvalidColumnState = terror.ClassAdmin.New(codeInvalidColumnState, "invalid column state")
)

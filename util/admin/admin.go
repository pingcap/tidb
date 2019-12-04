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
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
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

// IsJobRollbackable checks whether the job can be rollback.
func IsJobRollbackable(job *model.Job) bool {
	switch job.Type {
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		// We can't cancel if index current state is in StateDeleteOnly or StateDeleteReorganization, otherwise will cause inconsistent between record and index.
		if job.SchemaState == model.StateDeleteOnly ||
			job.SchemaState == model.StateDeleteReorganization {
			return false
		}
	case model.ActionDropSchema, model.ActionDropTable:
		// To simplify the rollback logic, cannot be canceled in the following states.
		if job.SchemaState == model.StateWriteOnly ||
			job.SchemaState == model.StateDeleteOnly {
			return false
		}
	case model.ActionDropColumn, model.ActionModifyColumn,
		model.ActionDropTablePartition, model.ActionAddTablePartition,
		model.ActionRebaseAutoID, model.ActionShardRowID,
		model.ActionTruncateTable, model.ActionAddForeignKey,
		model.ActionDropForeignKey, model.ActionRenameTable,
		model.ActionModifyTableCharsetAndCollate, model.ActionTruncateTablePartition,
		model.ActionModifySchemaCharsetAndCollate, model.ActionRepairTable:
		return job.SchemaState == model.StateNone
	}
	return true
}

// CancelJobs cancels the DDL jobs.
func CancelJobs(txn kv.Transaction, ids []int64) ([]error, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	errs := make([]error, len(ids))
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

	for i, id := range ids {
		found := false
		for j, job := range jobs {
			if id != job.ID {
				logutil.BgLogger().Debug("the job that needs to be canceled isn't equal to current job",
					zap.Int64("need to canceled job ID", id),
					zap.Int64("current job ID", job.ID))
				continue
			}
			found = true
			// These states can't be cancelled.
			if job.IsDone() || job.IsSynced() {
				errs[i] = ErrCancelFinishedDDLJob.GenWithStackByArgs(id)
				continue
			}
			// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
			if job.IsCancelled() || job.IsRollingback() || job.IsRollbackDone() {
				continue
			}
			if !IsJobRollbackable(job) {
				errs[i] = ErrCannotCancelDDLJob.GenWithStackByArgs(job.ID)
				continue
			}

			job.State = model.JobStateCancelling
			// Make sure RawArgs isn't overwritten.
			err := job.DecodeArgs(job.RawArgs)
			if err != nil {
				errs[i] = errors.Trace(err)
				continue
			}
			if job.Type == model.ActionAddIndex || job.Type == model.ActionAddPrimaryKey {
				offset := int64(j - len(generalJobs))
				err = t.UpdateDDLJob(offset, job, true, meta.AddIndexJobListKey)
			} else {
				err = t.UpdateDDLJob(int64(j), job, true)
			}
			if err != nil {
				errs[i] = errors.Trace(err)
			}
		}
		if !found {
			errs[i] = ErrDDLJobNotFound.GenWithStackByArgs(id)
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
	jobs, err := t.GetLastNHistoryDDLJobs(maxNumJobs)
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

// RecordData is the record data composed of a handle and values.
type RecordData struct {
	Handle int64
	Values []types.Datum
}

func getCount(ctx sessionctx.Context, sql string) (int64, error) {
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithSnapshot(sql)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) != 1 {
		return 0, errors.Errorf("can not get count, sql %s result rows %d", sql, len(rows))
	}
	return rows[0].GetInt64(0), nil
}

// Count greater Types
const (
	// TblCntGreater means that the number of table rows is more than the number of index rows.
	TblCntGreater byte = 1
	// IdxCntGreater means that the number of index rows is more than the number of table rows.
	IdxCntGreater byte = 2
)

// CheckIndicesCount compares indices count with table count.
// It returns the count greater type, the index offset and an error.
// It returns nil if the count from the index is equal to the count from the table columns,
// otherwise it returns an error and the corresponding index's offset.
func CheckIndicesCount(ctx sessionctx.Context, dbName, tableName string, indices []string) (byte, int, error) {
	// Add `` for some names like `table name`.
	sql := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` USE INDEX()", dbName, tableName)
	tblCnt, err := getCount(ctx, sql)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	for i, idx := range indices {
		sql = fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` USE INDEX(`%s`)", dbName, tableName, idx)
		idxCnt, err := getCount(ctx, sql)
		if err != nil {
			return 0, i, errors.Trace(err)
		}
		logutil.Logger(context.Background()).Info("check indices count",
			zap.String("table", tableName), zap.Int64("cnt", tblCnt), zap.Reflect("index", idx), zap.Int64("cnt", idxCnt))
		if tblCnt == idxCnt {
			continue
		}

		var ret byte
		if tblCnt > idxCnt {
			ret = TblCntGreater
		} else if idxCnt > tblCnt {
			ret = IdxCntGreater
		}
		return ret, i, errors.Errorf("table count %d != index(%s) count %d", tblCnt, idx, idxCnt)
	}
	return 0, 0, nil
}

// CheckRecordAndIndex is exported for testing.
func CheckRecordAndIndex(sessCtx sessionctx.Context, txn kv.Transaction, t table.Table, idx table.Index, genExprs map[model.TableColumnID]expression.Expression) error {
	sc := sessCtx.GetSessionVars().StmtCtx
	cols := make([]*table.Column, len(idx.Meta().Columns))
	for i, col := range idx.Meta().Columns {
		cols[i] = t.Cols()[col.Offset]
	}

	startKey := t.RecordKey(math.MinInt64)
	filterFunc := func(h1 int64, vals1 []types.Datum, cols []*table.Column) (bool, error) {
		for i, val := range vals1 {
			col := cols[i]
			if val.IsNull() {
				if mysql.HasNotNullFlag(col.Flag) && col.ToInfo().OriginDefaultValue == nil {
					return false, errors.Errorf("Column %v define as not null, but can't find the value where handle is %v", col.Name, h1)
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
			return false, ErrDataInConsistent.GenWithStack("index:%#v != record:%#v", record2, record1)
		}
		if err != nil {
			return false, errors.Trace(err)
		}
		if !isExist {
			record := &RecordData{Handle: h1, Values: vals1}
			return false, ErrDataInConsistent.GenWithStack("index:%#v != record:%#v", nil, record)
		}

		return true, nil
	}
	err := iterRecords(sessCtx, txn, t, startKey, cols, filterFunc, genExprs)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func makeRowDecoder(t table.Table, decodeCol []*table.Column, genExpr map[model.TableColumnID]expression.Expression) *decoder.RowDecoder {
	var containsVirtualCol bool
	decodeColsMap, ignored := decoder.BuildFullDecodeColMap(decodeCol, t, func(genCol *table.Column) (expression.Expression, error) {
		containsVirtualCol = true
		return genExpr[model.TableColumnID{TableID: t.Meta().ID, ColumnID: genCol.ID}], nil
	})
	_ = ignored

	if containsVirtualCol {
		decoder.SubstituteGenColsInDecodeColMap(decodeColsMap)
		decoder.RemoveUnusedVirtualCols(decodeColsMap, decodeCol)
	}
	return decoder.NewRowDecoder(t, decodeColsMap)
}

// genExprs use to calculate generated column value.
func iterRecords(sessCtx sessionctx.Context, retriever kv.Retriever, t table.Table, startKey kv.Key, cols []*table.Column,
	fn table.RecordIterFunc, genExprs map[model.TableColumnID]expression.Expression) error {
	prefix := t.RecordPrefix()
	keyUpperBound := prefix.PrefixNext()

	it, err := retriever.Iter(startKey, keyUpperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	logutil.BgLogger().Debug("record",
		zap.Stringer("startKey", startKey),
		zap.Stringer("key", it.Key()),
		zap.Binary("value", it.Value()))
	rowDecoder := makeRowDecoder(t, cols, genExprs)
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}

		rowMap, err := rowDecoder.DecodeAndEvalRowWithMap(sessCtx, handle, it.Value(), sessCtx.GetSessionVars().Location(), time.UTC, nil)
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
	codeDataNotEqual terror.ErrCode = iota + 1
	codeDDLJobNotFound
	codeCancelFinishedJob
	codeCannotCancelDDLJob
)

var (
	// ErrDataInConsistent indicate that meets inconsistent data.
	ErrDataInConsistent = terror.ClassAdmin.New(codeDataNotEqual, "data isn't equal")
	// ErrDDLJobNotFound indicates the job id was not found.
	ErrDDLJobNotFound = terror.ClassAdmin.New(codeDDLJobNotFound, "DDL Job:%v not found")
	// ErrCancelFinishedDDLJob returns when cancel a finished ddl job.
	ErrCancelFinishedDDLJob = terror.ClassAdmin.New(codeCancelFinishedJob, "This job:%v is finished, so can't be cancelled")
	// ErrCannotCancelDDLJob returns when cancel a almost finished ddl job, because cancel in now may cause data inconsistency.
	ErrCannotCancelDDLJob = terror.ClassAdmin.New(codeCannotCancelDDLJob, "This job:%v is almost finished, can't be cancelled now")
)

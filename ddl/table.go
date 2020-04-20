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
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	field_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/gcutil"
)

func onCreateTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("mock do job error"))
		}
	})

	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tbInfo.State = model.StateNone
	err := checkTableNotExists(d, t, schemaID, tbInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		err = createTableOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionCreateTable, TableInfo: tbInfo})
		return ver, nil
	default:
		return ver, ErrInvalidDDLState.GenWithStackByArgs("table", tbInfo.State)
	}
}

func createTableOrViewWithCheck(t *meta.Meta, job *model.Job, schemaID int64, tbInfo *model.TableInfo) error {
	err := checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	return t.CreateTableOrView(schemaID, tbInfo)
}

func repairTableOrViewWithCheck(t *meta.Meta, job *model.Job, schemaID int64, tbInfo *model.TableInfo) error {
	err := checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	return t.UpdateTable(schemaID, tbInfo)
}

func onCreateView(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	var orReplace bool
	var oldTbInfoID int64
	if err := job.DecodeArgs(tbInfo, &orReplace, &oldTbInfoID); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tbInfo.State = model.StateNone
	err := checkTableNotExists(d, t, schemaID, tbInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		} else if infoschema.ErrTableExists.Equal(err) {
			if !orReplace {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		} else {
			return ver, errors.Trace(err)
		}
	}
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		if oldTbInfoID > 0 && orReplace {
			err = t.DropTableOrView(schemaID, oldTbInfoID, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		err = createTableOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionCreateView, TableInfo: tbInfo})
		return ver, nil
	default:
		return ver, ErrInvalidDDLState.GenWithStackByArgs("table", tbInfo.State)
	}
}

func onDropTableOrView(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	originalState := job.SchemaState
	switch tblInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		tblInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
	case model.StateDeleteOnly:
		tblInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if tblInfo.IsSequence() {
			if err = t.DropSequence(job.SchemaID, job.TableID, true); err != nil {
				break
			}
		} else {
			if err = t.DropTableOrView(job.SchemaID, job.TableID, true); err != nil {
				break
			}
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		startKey := tablecodec.EncodeTablePrefix(job.TableID)
		job.Args = append(job.Args, startKey, getPartitionIDs(tblInfo))
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

const (
	recoverTableCheckFlagNone int64 = iota
	recoverTableCheckFlagEnableGC
	recoverTableCheckFlagDisableGC
)

func (w *worker) onRecoverTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	schemaID := job.SchemaID
	tblInfo := &model.TableInfo{}
	var autoIncID, autoRandID, dropJobID, recoverTableCheckFlag int64
	var snapshotTS uint64
	const checkFlagIndexInJobArgs = 4 // The index of `recoverTableCheckFlag` in job arg list.
	if err = job.DecodeArgs(tblInfo, &autoIncID, &dropJobID, &snapshotTS, &recoverTableCheckFlag, &autoRandID); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// check GC and safe point
	gcEnable, err := checkGCEnable(w)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = checkTableNotExists(d, t, schemaID, tblInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	err = checkTableIDNotExists(t, schemaID, tblInfo.ID)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	// Recover table divide into 2 steps:
	// 1. Check GC enable status, to decided whether enable GC after recover table.
	//     a. Why not disable GC before put the job to DDL job queue?
	//        Think about concurrency problem. If a recover job-1 is doing and already disabled GC,
	//        then, another recover table job-2 check GC enable will get disable before into the job queue.
	//        then, after recover table job-2 finished, the GC will be disabled.
	//     b. Why split into 2 steps? 1 step also can finish this job: check GC -> disable GC -> recover table -> finish job.
	//        What if the transaction commit failed? then, the job will retry, but the GC already disabled when first running.
	//        So, after this job retry succeed, the GC will be disabled.
	// 2. Do recover table job.
	//     a. Check whether GC enabled, if enabled, disable GC first.
	//     b. Check GC safe point. If drop table time if after safe point time, then can do recover.
	//        otherwise, can't recover table, because the records of the table may already delete by gc.
	//     c. Remove GC task of the table from gc_delete_range table.
	//     d. Create table and rebase table auto ID.
	//     e. Finish.
	switch tblInfo.State {
	case model.StateNone:
		// none -> write only
		// check GC enable and update flag.
		if gcEnable {
			job.Args[checkFlagIndexInJobArgs] = recoverTableCheckFlagEnableGC
		} else {
			job.Args[checkFlagIndexInJobArgs] = recoverTableCheckFlagDisableGC
		}

		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, false)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> public
		// do recover table.
		if gcEnable {
			err = disableGC(w)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Errorf("disable gc failed, try again later. err: %v", err)
			}
		}
		// check GC safe point
		err = checkSafePoint(w, snapshotTS)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Remove dropped table DDL job from gc_delete_range table.
		var tids []int64
		if tblInfo.GetPartitionInfo() != nil {
			tids = getPartitionIDs(tblInfo)
		} else {
			tids = []int64{tblInfo.ID}
		}
		err = w.delRangeManager.removeFromGCDeleteRange(dropJobID, tids)
		if err != nil {
			return ver, errors.Trace(err)
		}

		tblInfo.State = model.StatePublic
		tblInfo.UpdateTS = t.StartTS
		err = t.CreateTableAndSetAutoID(schemaID, tblInfo, autoIncID, autoRandID)
		if err != nil {
			return ver, errors.Trace(err)
		}

		failpoint.Inject("mockRecoverTableCommitErr", func(val failpoint.Value) {
			if val.(bool) && atomic.CompareAndSwapUint32(&mockRecoverTableCommitErrOnce, 0, 1) {
				kv.MockCommitErrorEnable()
			}
		})

		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		return ver, ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State)
	}
	return ver, nil
}

// mockRecoverTableCommitErrOnce uses to make sure
// `mockRecoverTableCommitErr` only mock error once.
var mockRecoverTableCommitErrOnce uint32

func enableGC(w *worker) error {
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return gcutil.EnableGC(ctx)
}

func disableGC(w *worker) error {
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return gcutil.DisableGC(ctx)
}

func checkGCEnable(w *worker) (enable bool, err error) {
	ctx, err := w.sessPool.get()
	if err != nil {
		return false, errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return gcutil.CheckGCEnable(ctx)
}

func checkSafePoint(w *worker, snapshotTS uint64) error {
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	return gcutil.ValidateSnapshot(ctx, snapshotTS)
}

func getTable(store kv.Storage, schemaID int64, tblInfo *model.TableInfo) (table.Table, error) {
	allocs := autoid.NewAllocatorsFromTblInfo(store, schemaID, tblInfo)
	tbl, err := table.TableFromMeta(allocs, tblInfo)
	return tbl, errors.Trace(err)
}

func getTableInfoAndCancelFaultJob(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, schemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return nil, ErrInvalidDDLState.GenWithStack("table %s is not in public, but %s", tblInfo.Name, tblInfo.State)
	}

	return tblInfo, nil
}

func checkTableExistAndCancelNonExistJob(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tblInfo, err := getTableInfo(t, job.TableID, schemaID)
	if err == nil {
		return tblInfo, nil
	}
	if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
		job.State = model.JobStateCancelled
	}
	return nil, err
}

func getTableInfo(t *meta.Meta, tableID, schemaID int64) (*model.TableInfo, error) {
	// Check this table's database.
	tblInfo, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return nil, errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(
				fmt.Sprintf("(Schema ID %d)", schemaID),
			))
		}
		return nil, errors.Trace(err)
	}

	// Check the table.
	if tblInfo == nil {
		return nil, errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", schemaID),
			fmt.Sprintf("(Table ID %d)", tableID),
		))
	}
	return tblInfo, nil
}

// onTruncateTable delete old table meta, and creates a new table identical to old table except for table ID.
// As all the old data is encoded with old table ID, it can not be accessed any more.
// A background job will be created to delete old data.
func onTruncateTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tableID := job.TableID
	var newTableID int64
	err := job.DecodeArgs(&newTableID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = t.DropTableOrView(schemaID, tblInfo.ID, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	failpoint.Inject("truncateTableErr", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after dropping table"))
		}
	})

	var oldPartitionIDs []int64
	if tblInfo.GetPartitionInfo() != nil {
		oldPartitionIDs = getPartitionIDs(tblInfo)
		// We use the new partition ID because all the old data is encoded with the old partition ID, it can not be accessed anymore.
		err = truncateTableByReassignPartitionIDs(t, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	// Clear the tiflash replica available status.
	if tblInfo.TiFlashReplica != nil {
		tblInfo.TiFlashReplica.AvailablePartitionIDs = nil
		tblInfo.TiFlashReplica.Available = false
	}

	tblInfo.ID = newTableID
	err = t.CreateTableOrView(schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	asyncNotifyEvent(d, &util.Event{Tp: model.ActionTruncateTable, TableInfo: tblInfo})
	startKey := tablecodec.EncodeTablePrefix(tableID)
	job.Args = []interface{}{startKey, oldPartitionIDs}
	return ver, nil
}

func onRebaseAutoID(store kv.Storage, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	var newBase int64
	err := job.DecodeArgs(&newBase)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// No need to check `newBase` again, because `RebaseAutoID` will do this check.
	tblInfo.AutoIncID = newBase
	tbl, err := getTable(store, schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// The operation of the minus 1 to make sure that the current value doesn't be used,
	// the next Alloc operation will get this value.
	// Its behavior is consistent with MySQL.
	err = tbl.RebaseAutoID(nil, tblInfo.AutoIncID-1, false)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableAutoIDCache(t *meta.Meta, job *model.Job) (int64, error) {
	var cache int64
	if err := job.DecodeArgs(&cache); err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}

	tblInfo.AutoIdCache = cache
	ver, err := updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onShardRowID(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var shardRowIDBits uint64
	err := job.DecodeArgs(&shardRowIDBits)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if shardRowIDBits < tblInfo.ShardRowIDBits {
		tblInfo.ShardRowIDBits = shardRowIDBits
	} else {
		tbl, err := getTable(d.store, job.SchemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = verifyNoOverflowShardBits(w.sessPool, tbl, shardRowIDBits)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		tblInfo.ShardRowIDBits = shardRowIDBits
		// MaxShardRowIDBits use to check the overflow of auto ID.
		tblInfo.MaxShardRowIDBits = shardRowIDBits
	}
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func verifyNoOverflowShardBits(s *sessionPool, tbl table.Table, shardRowIDBits uint64) error {
	ctx, err := s.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.put(ctx)

	// Check next global max auto ID first.
	autoIncID, err := tbl.Allocators(ctx).Get(autoid.RowIDAllocType).NextGlobalAutoID(tbl.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	if tables.OverflowShardBits(autoIncID, shardRowIDBits, autoid.RowIDBitLength) {
		return autoid.ErrAutoincReadFailed.GenWithStack("shard_row_id_bits %d will cause next global auto ID %v overflow", shardRowIDBits, autoIncID)
	}
	return nil
}

func onRenameTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var oldSchemaID int64
	var tableName model.CIStr
	if err := job.DecodeArgs(&oldSchemaID, &tableName); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, oldSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	newSchemaID := job.SchemaID
	err = checkTableNotExists(d, t, newSchemaID, tableName.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	var baseID int64
	shouldDelAutoID := false
	if newSchemaID != oldSchemaID {
		shouldDelAutoID = true
		baseID, err = t.GetAutoTableID(tblInfo.GetDBID(oldSchemaID), tblInfo.ID)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// It's compatible with old version.
		// TODO: Remove it.
		tblInfo.OldSchemaID = 0
	}

	err = t.DropTableOrView(oldSchemaID, tblInfo.ID, shouldDelAutoID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("renameTableErr", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("occur an error after renaming table"))
		}
	})

	tblInfo.Name = tableName
	err = t.CreateTableOrView(newSchemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// Update the table's auto-increment ID.
	if newSchemaID != oldSchemaID {
		_, err = t.GenAutoTableID(newSchemaID, tblInfo.ID, baseID)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableComment(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var comment string
	if err := job.DecodeArgs(&comment); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo.Comment = comment
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableCharsetAndCollate(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var toCharset, toCollate string
	var needsOverwriteCols bool
	if err := job.DecodeArgs(&toCharset, &toCollate, &needsOverwriteCols); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// double check.
	_, err = checkAlterTableCharset(tblInfo, dbInfo, toCharset, toCollate, needsOverwriteCols)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo.Charset = toCharset
	tblInfo.Collate = toCollate

	if needsOverwriteCols {
		// update column charset.
		for _, col := range tblInfo.Columns {
			if field_types.HasCharset(&col.FieldType) {
				col.Charset = toCharset
				col.Collate = toCollate
			} else {
				col.Charset = charset.CharsetBin
				col.Collate = charset.CharsetBin
			}
		}
	}

	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onSetTableFlashReplica(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var replicaInfo ast.TiFlashReplicaSpec
	if err := job.DecodeArgs(&replicaInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if replicaInfo.Count > 0 {
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:          replicaInfo.Count,
			LocationLabels: replicaInfo.Labels,
		}
	} else {
		tblInfo.TiFlashReplica = nil
	}

	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onUpdateFlashReplicaStatus(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var available bool
	var physicalID int64
	if err := job.DecodeArgs(&available, &physicalID); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TiFlashReplica == nil || (tblInfo.ID == physicalID && tblInfo.TiFlashReplica.Available == available) ||
		(tblInfo.ID != physicalID && available == tblInfo.TiFlashReplica.IsPartitionAvailable(physicalID)) {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("the replica available status of table %s is already updated", tblInfo.Name.String())
	}

	if tblInfo.ID == physicalID {
		tblInfo.TiFlashReplica.Available = available
	} else if pi := tblInfo.GetPartitionInfo(); pi != nil {
		// Partition replica become available.
		if available {
			allAvailable := true
			for _, p := range pi.Definitions {
				if p.ID == physicalID {
					tblInfo.TiFlashReplica.AvailablePartitionIDs = append(tblInfo.TiFlashReplica.AvailablePartitionIDs, physicalID)
				}
				allAvailable = allAvailable && tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID)
			}
			tblInfo.TiFlashReplica.Available = allAvailable
		} else {
			// Partition replica become unavailable.
			for i, id := range tblInfo.TiFlashReplica.AvailablePartitionIDs {
				if id == physicalID {
					newIDs := tblInfo.TiFlashReplica.AvailablePartitionIDs[:i]
					newIDs = append(newIDs, tblInfo.TiFlashReplica.AvailablePartitionIDs[i+1:]...)
					tblInfo.TiFlashReplica.AvailablePartitionIDs = newIDs
					tblInfo.TiFlashReplica.Available = false
					break
				}
			}
		}
	} else {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("unknown physical ID %v in table %v", physicalID, tblInfo.Name.O)
	}

	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func checkTableNotExists(d *ddlCtx, t *meta.Meta, schemaID int64, tableName string) error {
	// d.infoHandle maybe nil in some test.
	if d.infoHandle == nil || !d.infoHandle.IsValid() {
		return checkTableNotExistsFromStore(t, schemaID, tableName)
	}
	// Try to use memory schema info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoHandle.Get()
	if is.SchemaMetaVersion() == currVer {
		return checkTableNotExistsFromInfoSchema(is, schemaID, tableName)
	}

	return checkTableNotExistsFromStore(t, schemaID, tableName)
}

func checkTableIDNotExists(t *meta.Meta, schemaID, tableID int64) error {
	tbl, err := t.GetTable(schemaID, tableID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return errors.Trace(err)
	}
	if tbl != nil {
		return infoschema.ErrTableExists.GenWithStackByArgs(tbl.Name)
	}
	return nil
}

func checkTableNotExistsFromInfoSchema(is infoschema.InfoSchema, schemaID int64, tableName string) error {
	// Check this table's database.
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	if is.TableExists(schema.Name, model.NewCIStr(tableName)) {
		return infoschema.ErrTableExists.GenWithStackByArgs(tableName)
	}
	return nil
}

func checkTableNotExistsFromStore(t *meta.Meta, schemaID int64, tableName string) error {
	// Check this table's database.
	tables, err := t.ListTables(schemaID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return errors.Trace(err)
	}

	// Check the table.
	for _, tbl := range tables {
		if tbl.Name.L == tableName {
			return infoschema.ErrTableExists.GenWithStackByArgs(tbl.Name)
		}
	}

	return nil
}

// updateVersionAndTableInfoWithCheck checks table info validate and updates the schema version and the table information
func updateVersionAndTableInfoWithCheck(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool) (
	ver int64, err error) {
	err = checkTableInfoValid(tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	return updateVersionAndTableInfo(t, job, tblInfo, shouldUpdateVer)

}

// updateVersionAndTableInfo updates the schema version and the table information.
func updateVersionAndTableInfo(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool) (
	ver int64, err error) {
	if shouldUpdateVer {
		ver, err = updateSchemaVersion(t, job)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	if tblInfo.State == model.StatePublic {
		tblInfo.UpdateTS = t.StartTS
	}
	return ver, t.UpdateTable(job.SchemaID, tblInfo)
}

// TODO: It may have the issue when two clients concurrently add partitions to a table.
func onAddTablePartition(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	partInfo := &model.PartitionInfo{}
	err := job.DecodeArgs(&partInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	err = checkAddPartitionTooManyPartitions(uint64(len(tblInfo.Partition.Definitions) + len(partInfo.Definitions)))
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = checkAddPartitionValue(tblInfo, partInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = checkAddPartitionNameUnique(tblInfo, partInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	updatePartitionInfo(partInfo, tblInfo)
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	asyncNotifyEvent(d, &util.Event{Tp: model.ActionAddTablePartition, TableInfo: tblInfo, PartInfo: partInfo})
	return ver, errors.Trace(err)
}

func updatePartitionInfo(partitionInfo *model.PartitionInfo, tblInfo *model.TableInfo) {
	parInfo := &model.PartitionInfo{}
	oldDefs, newDefs := tblInfo.Partition.Definitions, partitionInfo.Definitions
	parInfo.Definitions = make([]model.PartitionDefinition, 0, len(newDefs)+len(oldDefs))
	parInfo.Definitions = append(parInfo.Definitions, oldDefs...)
	parInfo.Definitions = append(parInfo.Definitions, newDefs...)
	tblInfo.Partition.Definitions = parInfo.Definitions
}

// checkAddPartitionValue values less than value must be strictly increasing for each partition.
func checkAddPartitionValue(meta *model.TableInfo, part *model.PartitionInfo) error {
	if meta.Partition.Type == model.PartitionTypeRange && len(meta.Partition.Columns) == 0 {
		newDefs, oldDefs := part.Definitions, meta.Partition.Definitions
		rangeValue := oldDefs[len(oldDefs)-1].LessThan[0]
		if strings.EqualFold(rangeValue, "MAXVALUE") {
			return errors.Trace(ErrPartitionMaxvalue)
		}

		currentRangeValue, err := strconv.Atoi(rangeValue)
		if err != nil {
			return errors.Trace(err)
		}

		for i := 0; i < len(newDefs); i++ {
			ifMaxvalue := strings.EqualFold(newDefs[i].LessThan[0], "MAXVALUE")
			if ifMaxvalue && i == len(newDefs)-1 {
				return nil
			} else if ifMaxvalue && i != len(newDefs)-1 {
				return errors.Trace(ErrPartitionMaxvalue)
			}

			nextRangeValue, err := strconv.Atoi(newDefs[i].LessThan[0])
			if err != nil {
				return errors.Trace(err)
			}
			if nextRangeValue <= currentRangeValue {
				return errors.Trace(ErrRangeNotIncreasing)
			}
			currentRangeValue = nextRangeValue
		}
	}
	return nil
}

func onRepairTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo := &model.TableInfo{}

	if err := job.DecodeArgs(tblInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo.State = model.StateNone

	// Check the old DB and old table exist.
	_, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// When in repair mode, the repaired table in a server is not access to user,
	// the table after repairing will be removed from repair list. Other server left
	// behind alive may need to restart to get the latest schema version.
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch tblInfo.State {
	case model.StateNone:
		// none -> public
		tblInfo.State = model.StatePublic
		tblInfo.UpdateTS = t.StartTS
		err = repairTableOrViewWithCheck(t, job, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionRepairTable, TableInfo: tblInfo})
		return ver, nil
	default:
		return ver, ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State)
	}
}

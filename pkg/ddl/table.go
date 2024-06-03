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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	tidb_util "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"go.uber.org/zap"
)

const tiflashCheckTiDBHTTPAPIHalfInterval = 2500 * time.Millisecond

// DANGER: it is an internal function used by onCreateTable and onCreateTables, for reusing code. Be careful.
// 1. it expects the argument of job has been deserialized.
// 2. it won't call updateSchemaVersion, FinishTableJob and asyncNotifyEvent.
func createTable(d *ddlCtx, t *meta.Meta, job *model.Job, fkCheck bool) (*model.TableInfo, error) {
	schemaID := job.SchemaID
	tbInfo := job.Args[0].(*model.TableInfo)

	tbInfo.State = model.StateNone
	var err error
	if variable.EnableFastCreateTable.Load() {
		err = checkTableNotExistsByName(d, t, schemaID, job.SchemaName, tbInfo.Name.L)
	} else {
		err = checkTableNotExists(d, t, schemaID, tbInfo.Name.L)
	}
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return tbInfo, errors.Trace(err)
	}

	err = checkConstraintNamesNotExists(t, schemaID, tbInfo.Constraints)
	if err != nil {
		if infoschema.ErrCheckConstraintDupName.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return tbInfo, errors.Trace(err)
	}

	retryable, err := checkTableForeignKeyValidInOwner(d, t, job, tbInfo, fkCheck)
	if err != nil {
		if !retryable {
			job.State = model.JobStateCancelled
		}
		return tbInfo, errors.Trace(err)
	}
	// Allocate foreign key ID.
	for _, fkInfo := range tbInfo.ForeignKeys {
		fkInfo.ID = allocateFKIndexID(tbInfo)
		fkInfo.State = model.StatePublic
	}
	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		err = createTableOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return tbInfo, errors.Trace(err)
		}

		failpoint.Inject("checkOwnerCheckAllVersionsWaitTime", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(tbInfo, errors.New("mock create table error"))
			}
		})

		// build table & partition bundles if any.
		if err = checkAllTablePlacementPoliciesExistAndCancelNonExistJob(t, job, tbInfo); err != nil {
			return tbInfo, errors.Trace(err)
		}

		if tbInfo.TiFlashReplica != nil {
			replicaInfo := tbInfo.TiFlashReplica
			if pi := tbInfo.GetPartitionInfo(); pi != nil {
				logutil.DDLLogger().Info("Set TiFlash replica pd rule for partitioned table when creating", zap.Int64("tableID", tbInfo.ID))
				if e := infosync.ConfigureTiFlashPDForPartitions(false, &pi.Definitions, replicaInfo.Count, &replicaInfo.LocationLabels, tbInfo.ID); e != nil {
					job.State = model.JobStateCancelled
					return tbInfo, errors.Trace(e)
				}
				// Partitions that in adding mid-state. They have high priorities, so we should set accordingly pd rules.
				if e := infosync.ConfigureTiFlashPDForPartitions(true, &pi.AddingDefinitions, replicaInfo.Count, &replicaInfo.LocationLabels, tbInfo.ID); e != nil {
					job.State = model.JobStateCancelled
					return tbInfo, errors.Trace(e)
				}
			} else {
				logutil.DDLLogger().Info("Set TiFlash replica pd rule when creating", zap.Int64("tableID", tbInfo.ID))
				if e := infosync.ConfigureTiFlashPDForTable(tbInfo.ID, replicaInfo.Count, &replicaInfo.LocationLabels); e != nil {
					job.State = model.JobStateCancelled
					return tbInfo, errors.Trace(e)
				}
			}
		}

		bundles, err := placement.NewFullTableBundles(t, tbInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return tbInfo, errors.Trace(err)
		}

		// Send the placement bundle to PD.
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
		if err != nil {
			job.State = model.JobStateCancelled
			return tbInfo, errors.Wrapf(err, "failed to notify PD the placement rules")
		}

		return tbInfo, nil
	default:
		return tbInfo, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tbInfo.State)
	}
}

func onCreateTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("mock do job error"))
		}
	})

	// just decode, createTable will use it as Args[0]
	tbInfo := &model.TableInfo{}
	fkCheck := false
	if err := job.DecodeArgs(tbInfo, &fkCheck); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if len(tbInfo.ForeignKeys) > 0 {
		return createTableWithForeignKeys(d, t, job, tbInfo, fkCheck)
	}

	tbInfo, err := createTable(d, t, job, fkCheck)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	createTableEvent := statsutil.NewCreateTableEvent(
		job.SchemaID,
		tbInfo,
	)
	asyncNotifyEvent(d, createTableEvent)
	return ver, errors.Trace(err)
}

func createTableWithForeignKeys(d *ddlCtx, t *meta.Meta, job *model.Job, tbInfo *model.TableInfo, fkCheck bool) (ver int64, err error) {
	switch tbInfo.State {
	case model.StateNone, model.StatePublic:
		// create table in non-public or public state. The function `createTable` will always reset
		// the `tbInfo.State` with `model.StateNone`, so it's fine to just call the `createTable` with
		// public state.
		// when `br` restores table, the state of `tbInfo` will be public.
		tbInfo, err = createTable(d, t, job, fkCheck)
		if err != nil {
			return ver, errors.Trace(err)
		}
		tbInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(d, t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		tbInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(d, t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		createTableEvent := statsutil.NewCreateTableEvent(
			job.SchemaID,
			tbInfo,
		)
		asyncNotifyEvent(d, createTableEvent)
		return ver, nil
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLJob.GenWithStackByArgs("table", tbInfo.State))
	}
	return ver, errors.Trace(err)
}

func onCreateTables(d *ddlCtx, t *meta.Meta, job *model.Job) (int64, error) {
	var ver int64

	var args []*model.TableInfo
	fkCheck := false
	err := job.DecodeArgs(&args, &fkCheck)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// We don't construct jobs for every table, but only tableInfo
	// The following loop creates a stub job for every table
	//
	// it clones a stub job from the ActionCreateTables job
	stubJob := job.Clone()
	stubJob.Args = make([]any, 1)
	for i := range args {
		stubJob.TableID = args[i].ID
		stubJob.Args[0] = args[i]
		if args[i].Sequence != nil {
			err := createSequenceWithCheck(t, stubJob, args[i])
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		} else {
			tbInfo, err := createTable(d, t, stubJob, fkCheck)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			args[i] = tbInfo
		}
	}

	ver, err = updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.State = model.JobStateDone
	job.SchemaState = model.StatePublic
	job.BinlogInfo.SetTableInfos(ver, args)

	for i := range args {
		createTableEvent := statsutil.NewCreateTableEvent(
			job.SchemaID,
			args[i],
		)
		asyncNotifyEvent(d, createTableEvent)
	}

	return ver, errors.Trace(err)
}

func createTableOrViewWithCheck(t *meta.Meta, job *model.Job, schemaID int64, tbInfo *model.TableInfo) error {
	err := checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	return t.CreateTableOrView(schemaID, job.SchemaName, tbInfo)
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
	var _placeholder int64 // oldTblInfoID
	if err := job.DecodeArgs(tbInfo, &orReplace, &_placeholder); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tbInfo.State = model.StateNone

	oldTableID, err := findTableIDByName(d, t, schemaID, tbInfo.Name.L)
	if infoschema.ErrTableNotExists.Equal(err) {
		err = nil
	}
	failpoint.InjectCall("onDDLCreateView", job)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		} else if !infoschema.ErrTableExists.Equal(err) {
			return ver, errors.Trace(err)
		}
		if !orReplace {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}
	ver, err = updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		if oldTableID > 0 && orReplace {
			err = t.DropTableOrView(schemaID, job.SchemaName, oldTableID, tbInfo.Name.L)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			err = t.GetAutoIDAccessors(schemaID, oldTableID).Del()
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
		err = createTableOrViewWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tbInfo.State)
	}
}

func onDropTableOrView(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	originalState := job.SchemaState
	switch tblInfo.State {
	case model.StatePublic:
		// public -> write only
		if job.Type == model.ActionDropTable {
			err = checkDropTableHasForeignKeyReferredInOwner(d, t, job)
			if err != nil {
				return ver, err
			}
		}
		tblInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		tblInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		tblInfo.State = model.StateNone
		oldIDs := getPartitionIDs(tblInfo)
		ruleIDs := append(getPartitionRuleIDs(job.SchemaName, tblInfo), fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L))
		job.CtxVars = []any{oldIDs}

		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if tblInfo.IsSequence() {
			if err = t.DropSequence(job.SchemaID, job.SchemaName, job.TableID, job.TableName); err != nil {
				return ver, errors.Trace(err)
			}
		} else {
			if err = t.DropTableOrView(job.SchemaID, job.SchemaName, job.TableID, job.TableName); err != nil {
				return ver, errors.Trace(err)
			}
			if err = t.GetAutoIDAccessors(job.SchemaID, job.TableID).Del(); err != nil {
				return ver, errors.Trace(err)
			}
		}
		if tblInfo.TiFlashReplica != nil {
			e := infosync.DeleteTiFlashTableSyncProgress(tblInfo)
			if e != nil {
				logutil.DDLLogger().Error("DeleteTiFlashTableSyncProgress fails", zap.Error(e))
			}
		}
		// Placement rules cannot be removed immediately after drop table / truncate table, because the
		// tables can be flashed back or recovered, therefore it moved to doGCPlacementRules in gc_worker.go.

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		startKey := tablecodec.EncodeTablePrefix(job.TableID)
		job.Args = append(job.Args, startKey, oldIDs, ruleIDs)
		if !tblInfo.IsSequence() && !tblInfo.IsView() {
			dropTableEvent := statsutil.NewDropTableEvent(
				job.SchemaID,
				tblInfo,
			)
			asyncNotifyEvent(d, dropTableEvent)
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State))
	}
	job.SchemaState = tblInfo.State
	return ver, errors.Trace(err)
}

func (w *worker) onRecoverTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	var (
		recoverInfo           *RecoverInfo
		recoverTableCheckFlag int64
	)
	if err = job.DecodeArgs(&recoverInfo, &recoverTableCheckFlag); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	schemaID := recoverInfo.SchemaID
	tblInfo := recoverInfo.TableInfo
	if tblInfo.TTLInfo != nil {
		// force disable TTL job schedule for recovered table
		tblInfo.TTLInfo.Enable = false
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
			job.Args[checkFlagIndexInJobArgs] = recoverCheckFlagEnableGC
		} else {
			job.Args[checkFlagIndexInJobArgs] = recoverCheckFlagDisableGC
		}

		// Clear all placement when recover
		err = clearTablePlacementAndBundles(tblInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
		}

		job.SchemaState = model.StateWriteOnly
		tblInfo.State = model.StateWriteOnly
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
		err = checkSafePoint(w, recoverInfo.SnapshotTS)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		ver, err = w.recoverTable(t, job, recoverInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		tableInfo := tblInfo.Clone()
		tableInfo.State = model.StatePublic
		tableInfo.UpdateTS = t.StartTS
		ver, err = updateVersionAndTableInfo(d, t, job, tableInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		tblInfo.State = model.StatePublic
		tblInfo.UpdateTS = t.StartTS
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State)
	}
	return ver, nil
}

func (w *worker) recoverTable(t *meta.Meta, job *model.Job, recoverInfo *RecoverInfo) (ver int64, err error) {
	var tids []int64
	if recoverInfo.TableInfo.GetPartitionInfo() != nil {
		tids = getPartitionIDs(recoverInfo.TableInfo)
		tids = append(tids, recoverInfo.TableInfo.ID)
	} else {
		tids = []int64{recoverInfo.TableInfo.ID}
	}
	tableRuleID, partRuleIDs, oldRuleIDs, oldRules, err := getOldLabelRules(recoverInfo.TableInfo, recoverInfo.OldSchemaName, recoverInfo.OldTableName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to get old label rules from PD")
	}
	// Remove dropped table DDL job from gc_delete_range table.
	err = w.delRangeManager.removeFromGCDeleteRange(w.ctx, recoverInfo.DropJobID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	tableInfo := recoverInfo.TableInfo.Clone()
	tableInfo.State = model.StatePublic
	tableInfo.UpdateTS = t.StartTS
	err = t.CreateTableAndSetAutoID(recoverInfo.SchemaID, recoverInfo.OldSchemaName, tableInfo, recoverInfo.AutoIDs)
	if err != nil {
		return ver, errors.Trace(err)
	}

	failpoint.Inject("mockRecoverTableCommitErr", func(val failpoint.Value) {
		if val.(bool) && atomic.CompareAndSwapUint32(&mockRecoverTableCommitErrOnce, 0, 1) {
			err = failpoint.Enable(`tikvclient/mockCommitErrorOpt`, "return(true)")
			if err != nil {
				return
			}
		}
	})

	err = updateLabelRules(job, recoverInfo.TableInfo, oldRules, tableRuleID, partRuleIDs, oldRuleIDs, recoverInfo.TableInfo.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to update the label rule to PD")
	}
	job.CtxVars = []any{tids}
	return ver, nil
}

func clearTablePlacementAndBundles(tblInfo *model.TableInfo) error {
	var bundles []*placement.Bundle
	if tblInfo.PlacementPolicyRef != nil {
		tblInfo.PlacementPolicyRef = nil
		bundles = append(bundles, placement.NewBundle(tblInfo.ID))
	}

	if tblInfo.Partition != nil {
		for i := range tblInfo.Partition.Definitions {
			par := &tblInfo.Partition.Definitions[i]
			if par.PlacementPolicyRef != nil {
				par.PlacementPolicyRef = nil
				bundles = append(bundles, placement.NewBundle(par.ID))
			}
		}
	}

	if len(bundles) == 0 {
		return nil
	}

	return infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
}

// mockRecoverTableCommitErrOnce uses to make sure
// `mockRecoverTableCommitErr` only mock error once.
var mockRecoverTableCommitErrOnce uint32

func enableGC(w *worker) error {
	ctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(ctx)

	return gcutil.EnableGC(ctx)
}

func disableGC(w *worker) error {
	ctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(ctx)

	return gcutil.DisableGC(ctx)
}

func checkGCEnable(w *worker) (enable bool, err error) {
	ctx, err := w.sessPool.Get()
	if err != nil {
		return false, errors.Trace(err)
	}
	defer w.sessPool.Put(ctx)

	return gcutil.CheckGCEnable(ctx)
}

func checkSafePoint(w *worker, snapshotTS uint64) error {
	ctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(ctx)

	return gcutil.ValidateSnapshot(ctx, snapshotTS)
}

func getTable(r autoid.Requirement, schemaID int64, tblInfo *model.TableInfo) (table.Table, error) {
	allocs := autoid.NewAllocatorsFromTblInfo(r, schemaID, tblInfo)
	tbl, err := table.TableFromMeta(allocs, tblInfo)
	return tbl, errors.Trace(err)
}

// GetTableInfoAndCancelFaultJob is exported for test.
func GetTableInfoAndCancelFaultJob(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, schemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if tblInfo.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return nil, dbterror.ErrInvalidDDLState.GenWithStack("table %s is not in public, but %s", tblInfo.Name, tblInfo.State)
	}

	return tblInfo, nil
}

func checkTableExistAndCancelNonExistJob(t *meta.Meta, job *model.Job, schemaID int64) (*model.TableInfo, error) {
	tblInfo, err := getTableInfo(t, job.TableID, schemaID)
	if err == nil {
		// Check if table name is renamed.
		if job.TableName != "" && tblInfo.Name.L != job.TableName && job.Type != model.ActionRepairTable {
			job.State = model.JobStateCancelled
			return nil, infoschema.ErrTableNotExists.GenWithStackByArgs(job.SchemaName, job.TableName)
		}
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
// As all the old data is encoded with old table ID, it can not be accessed anymore.
// A background job will be created to delete old data.
func (w *worker) onTruncateTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tableID := job.TableID
	var newTableID int64
	var fkCheck bool
	var newPartitionIDs []int64
	err := job.DecodeArgs(&newTableID, &fkCheck, &newPartitionIDs)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.IsView() || tblInfo.IsSequence() {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrTableNotExists.GenWithStackByArgs(job.SchemaName, tblInfo.Name.O)
	}
	// Copy the old tableInfo for later usage.
	oldTblInfo := tblInfo.Clone()
	err = checkTruncateTableHasForeignKeyReferredInOwner(d, t, job, tblInfo, fkCheck)
	if err != nil {
		return ver, err
	}
	err = t.DropTableOrView(schemaID, job.SchemaName, tblInfo.ID, tblInfo.Name.L)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	err = t.GetAutoIDAccessors(schemaID, tblInfo.ID).Del()
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

	// Clear the TiFlash replica progress from ETCD.
	if tblInfo.TiFlashReplica != nil {
		e := infosync.DeleteTiFlashTableSyncProgress(tblInfo)
		if e != nil {
			logutil.DDLLogger().Error("DeleteTiFlashTableSyncProgress fails", zap.Error(e))
		}
	}

	var oldPartitionIDs []int64
	if tblInfo.GetPartitionInfo() != nil {
		oldPartitionIDs = getPartitionIDs(tblInfo)
		// We use the new partition ID because all the old data is encoded with the old partition ID, it can not be accessed anymore.
		err = truncateTableByReassignPartitionIDs(t, tblInfo, newPartitionIDs)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		oldIDs := make([]int64, 0, len(oldPartitionIDs))
		newIDs := make([]int64, 0, len(oldPartitionIDs))
		newDefs := pi.Definitions
		for i := range oldPartitionIDs {
			newDef := &newDefs[i]
			newID := newDef.ID
			if newDef.PlacementPolicyRef != nil {
				oldIDs = append(oldIDs, oldPartitionIDs[i])
				newIDs = append(newIDs, newID)
			}
		}
		job.CtxVars = []any{oldIDs, newIDs}
	}

	tableRuleID, partRuleIDs, _, oldRules, err := getOldLabelRules(tblInfo, job.SchemaName, tblInfo.Name.L)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to get old label rules from PD")
	}

	err = updateLabelRules(job, tblInfo, oldRules, tableRuleID, partRuleIDs, []string{}, newTableID)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to update the label rule to PD")
	}

	// Clear the TiFlash replica available status.
	if tblInfo.TiFlashReplica != nil {
		// Set PD rules for TiFlash
		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			if e := infosync.ConfigureTiFlashPDForPartitions(true, &pi.Definitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID); e != nil {
				logutil.DDLLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(err))
				job.State = model.JobStateCancelled
				return ver, errors.Trace(e)
			}
		} else {
			if e := infosync.ConfigureTiFlashPDForTable(newTableID, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels); e != nil {
				logutil.DDLLogger().Error("ConfigureTiFlashPDForTable fails", zap.Error(err))
				job.State = model.JobStateCancelled
				return ver, errors.Trace(e)
			}
		}
		tblInfo.TiFlashReplica.AvailablePartitionIDs = nil
		tblInfo.TiFlashReplica.Available = false
	}

	tblInfo.ID = newTableID

	// build table & partition bundles if any.
	bundles, err := placement.NewFullTableBundles(t, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	err = t.CreateTableOrView(schemaID, job.SchemaName, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("mockTruncateTableUpdateVersionError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("mock update version error"))
		}
	})

	var partitions []model.PartitionDefinition
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		partitions = tblInfo.GetPartitionInfo().Definitions
	}
	preSplitAndScatter(w.sess.Context, d.store, tblInfo, partitions)

	ver, err = updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	truncateTableEvent := statsutil.NewTruncateTableEvent(
		job.SchemaID,
		tblInfo,
		oldTblInfo,
	)
	asyncNotifyEvent(d, truncateTableEvent)
	startKey := tablecodec.EncodeTablePrefix(tableID)
	job.Args = []any{startKey, oldPartitionIDs}
	return ver, nil
}

func onRebaseAutoIncrementIDType(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	return onRebaseAutoID(d, d.store, t, job, autoid.AutoIncrementType)
}

func onRebaseAutoRandomType(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	return onRebaseAutoID(d, d.store, t, job, autoid.AutoRandomType)
}

func onRebaseAutoID(d *ddlCtx, _ kv.Storage, t *meta.Meta, job *model.Job, tp autoid.AllocatorType) (ver int64, _ error) {
	schemaID := job.SchemaID
	var (
		newBase int64
		force   bool
	)
	err := job.DecodeArgs(&newBase, &force)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tbl, err := getTable((*asAutoIDRequirement)(d), schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if !force {
		newBaseTemp, err := adjustNewBaseToNextGlobalID(nil, tbl, tp, newBase)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if newBase != newBaseTemp {
			job.Warning = toTError(fmt.Errorf("Can't reset AUTO_INCREMENT to %d without FORCE option, using %d instead",
				newBase, newBaseTemp,
			))
		}
		newBase = newBaseTemp
	}

	if tp == autoid.AutoIncrementType {
		tblInfo.AutoIncID = newBase
	} else {
		tblInfo.AutoRandID = newBase
	}

	if alloc := tbl.Allocators(nil).Get(tp); alloc != nil {
		// The next value to allocate is `newBase`.
		newEnd := newBase - 1
		if force {
			err = alloc.ForceRebase(newEnd)
		} else {
			err = alloc.Rebase(context.Background(), newEnd, false)
		}
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableAutoIDCache(d *ddlCtx, t *meta.Meta, job *model.Job) (int64, error) {
	var cache int64
	if err := job.DecodeArgs(&cache); err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}

	tblInfo.AutoIdCache = cache
	ver, err := updateVersionAndTableInfo(d, t, job, tblInfo, true)
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
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if shardRowIDBits < tblInfo.ShardRowIDBits {
		tblInfo.ShardRowIDBits = shardRowIDBits
	} else {
		tbl, err := getTable((*asAutoIDRequirement)(d), job.SchemaID, tblInfo)
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
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func verifyNoOverflowShardBits(s *sess.Pool, tbl table.Table, shardRowIDBits uint64) error {
	if shardRowIDBits == 0 {
		return nil
	}
	ctx, err := s.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.Put(ctx)
	// Check next global max auto ID first.
	autoIncID, err := tbl.Allocators(ctx.GetTableCtx()).Get(autoid.RowIDAllocType).NextGlobalAutoID()
	if err != nil {
		return errors.Trace(err)
	}
	if tables.OverflowShardBits(autoIncID, shardRowIDBits, autoid.RowIDBitLength, true) {
		return autoid.ErrAutoincReadFailed.GenWithStack("shard_row_id_bits %d will cause next global auto ID %v overflow", shardRowIDBits, autoIncID)
	}
	return nil
}

func onRenameTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var oldSchemaID int64
	var oldSchemaName model.CIStr
	var tableName model.CIStr
	if err := job.DecodeArgs(&oldSchemaID, &tableName, &oldSchemaName); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if job.SchemaState == model.StatePublic {
		return finishJobRenameTable(d, t, job)
	}
	newSchemaID := job.SchemaID
	err := checkTableNotExists(d, t, newSchemaID, tableName.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, oldSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	oldTableName := tblInfo.Name
	ver, err = checkAndRenameTables(t, job, tblInfo, oldSchemaID, job.SchemaID, &oldSchemaName, &tableName)
	if err != nil {
		return ver, errors.Trace(err)
	}
	fkh := newForeignKeyHelper()
	err = adjustForeignKeyChildTableInfoAfterRenameTable(d, t, job, &fkh, tblInfo, oldSchemaName, oldTableName, tableName, newSchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(d, t, job, fkh.getLoadedTables()...)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.SchemaState = model.StatePublic
	return ver, nil
}

func onRenameTables(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	oldSchemaIDs := []int64{}
	newSchemaIDs := []int64{}
	tableNames := []*model.CIStr{}
	tableIDs := []int64{}
	oldSchemaNames := []*model.CIStr{}
	oldTableNames := []*model.CIStr{}
	if err := job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs, &tableNames, &tableIDs, &oldSchemaNames, &oldTableNames); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if job.SchemaState == model.StatePublic {
		return finishJobRenameTables(d, t, job, tableNames, tableIDs, newSchemaIDs)
	}

	var err error
	fkh := newForeignKeyHelper()
	for i, oldSchemaID := range oldSchemaIDs {
		job.TableID = tableIDs[i]
		job.TableName = oldTableNames[i].L
		tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, oldSchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err := checkAndRenameTables(t, job, tblInfo, oldSchemaID, newSchemaIDs[i], oldSchemaNames[i], tableNames[i])
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = adjustForeignKeyChildTableInfoAfterRenameTable(d, t, job, &fkh, tblInfo, *oldSchemaNames[i], *oldTableNames[i], *tableNames[i], newSchemaIDs[i])
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateSchemaVersion(d, t, job, fkh.getLoadedTables()...)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.SchemaState = model.StatePublic
	return ver, nil
}

func checkAndRenameTables(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, oldSchemaID, newSchemaID int64, oldSchemaName, tableName *model.CIStr) (ver int64, _ error) {
	err := t.DropTableOrView(oldSchemaID, oldSchemaName.L, tblInfo.ID, tblInfo.Name.L)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("renameTableErr", func(val failpoint.Value) {
		if valStr, ok := val.(string); ok {
			if tableName.L == valStr {
				job.State = model.JobStateCancelled
				failpoint.Return(ver, errors.New("occur an error after renaming table"))
			}
		}
	})

	oldTableName := tblInfo.Name
	tableRuleID, partRuleIDs, oldRuleIDs, oldRules, err := getOldLabelRules(tblInfo, oldSchemaName.L, oldTableName.L)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to get old label rules from PD")
	}

	if tblInfo.AutoIDSchemaID == 0 && newSchemaID != oldSchemaID {
		// The auto id is referenced by a schema id + table id
		// Table ID is not changed between renames, but schema id can change.
		// To allow concurrent use of the auto id during rename, keep the auto id
		// by always reference it with the schema id it was originally created in.
		tblInfo.AutoIDSchemaID = oldSchemaID
	}
	if newSchemaID == tblInfo.AutoIDSchemaID {
		// Back to the original schema id, no longer needed.
		tblInfo.AutoIDSchemaID = 0
	}

	tblInfo.Name = *tableName
	err = t.CreateTableOrView(newSchemaID, job.SchemaName, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = updateLabelRules(job, tblInfo, oldRules, tableRuleID, partRuleIDs, oldRuleIDs, tblInfo.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to update the label rule to PD")
	}

	return ver, nil
}

func adjustForeignKeyChildTableInfoAfterRenameTable(d *ddlCtx, t *meta.Meta, job *model.Job, fkh *foreignKeyHelper, tblInfo *model.TableInfo, oldSchemaName, oldTableName, newTableName model.CIStr, newSchemaID int64) error {
	if !variable.EnableForeignKey.Load() || newTableName.L == oldTableName.L {
		return nil
	}
	is, err := getAndCheckLatestInfoSchema(d, t)
	if err != nil {
		return err
	}
	newDB, ok := is.SchemaByID(newSchemaID)
	if !ok {
		job.State = model.JobStateCancelled
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("schema-ID: %v", newSchemaID))
	}
	referredFKs := is.GetTableReferredForeignKeys(oldSchemaName.L, oldTableName.L)
	if len(referredFKs) == 0 {
		return nil
	}
	fkh.addLoadedTable(oldSchemaName.L, oldTableName.L, newDB.ID, tblInfo)
	for _, referredFK := range referredFKs {
		childTableInfo, err := fkh.getTableFromStorage(is, t, referredFK.ChildSchema, referredFK.ChildTable)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err) {
				continue
			}
			return err
		}
		childFKInfo := model.FindFKInfoByName(childTableInfo.tblInfo.ForeignKeys, referredFK.ChildFKName.L)
		if childFKInfo == nil {
			continue
		}
		childFKInfo.RefSchema = newDB.Name
		childFKInfo.RefTable = newTableName
	}
	for _, info := range fkh.loaded {
		err = updateTable(t, info.schemaID, info.tblInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

// We split the renaming table job into two steps:
// 1. rename table and update the schema version.
// 2. update the job state to JobStateDone.
// This is the requirement from TiCDC because
//   - it uses the job state to check whether the DDL is finished.
//   - there is a gap between schema reloading and job state updating:
//     when the job state is updated to JobStateDone, before the new schema reloaded,
//     there may be DMLs that use the old schema.
//   - TiCDC cannot handle the DMLs that use the old schema, because
//     the commit TS of the DMLs are greater than the job state updating TS.
func finishJobRenameTable(d *ddlCtx, t *meta.Meta, job *model.Job) (int64, error) {
	tblInfo, err := getTableInfo(t, job.TableID, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	// Before updating the schema version, we need to reset the old schema ID to new schema ID, so that
	// the table info can be dropped normally in `ApplyDiff`. This is because renaming table requires two
	// schema versions to complete.
	oldRawArgs := job.RawArgs
	job.Args[0] = job.SchemaID
	job.RawArgs, err = json.Marshal(job.Args)
	if err != nil {
		return 0, errors.Trace(err)
	}
	ver, err := updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.RawArgs = oldRawArgs
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func finishJobRenameTables(d *ddlCtx, t *meta.Meta, job *model.Job,
	tableNames []*model.CIStr, tableIDs, newSchemaIDs []int64) (int64, error) {
	tblSchemaIDs := make(map[int64]int64, len(tableIDs))
	for i := range tableIDs {
		tblSchemaIDs[tableIDs[i]] = newSchemaIDs[i]
	}
	tblInfos := make([]*model.TableInfo, 0, len(tableNames))
	for i := range tableIDs {
		tblID := tableIDs[i]
		tblInfo, err := getTableInfo(t, tblID, tblSchemaIDs[tblID])
		if err != nil {
			job.State = model.JobStateCancelled
			return 0, errors.Trace(err)
		}
		tblInfos = append(tblInfos, tblInfo)
	}
	// Before updating the schema version, we need to reset the old schema ID to new schema ID, so that
	// the table info can be dropped normally in `ApplyDiff`. This is because renaming table requires two
	// schema versions to complete.
	var err error
	oldRawArgs := job.RawArgs
	job.Args[0] = newSchemaIDs
	job.RawArgs, err = json.Marshal(job.Args)
	if err != nil {
		return 0, errors.Trace(err)
	}
	ver, err := updateSchemaVersion(d, t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.RawArgs = oldRawArgs
	job.FinishMultipleTableJob(model.JobStateDone, model.StatePublic, ver, tblInfos)
	return ver, nil
}

func onModifyTableComment(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var comment string
	if err := job.DecodeArgs(&comment); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	tblInfo.Comment = comment
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableCharsetAndCollate(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
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

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// double check.
	_, err = checkAlterTableCharset(tblInfo, dbInfo, toCharset, toCollate, needsOverwriteCols)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	tblInfo.Charset = toCharset
	tblInfo.Collate = toCollate

	if needsOverwriteCols {
		// update column charset.
		for _, col := range tblInfo.Columns {
			if field_types.HasCharset(&col.FieldType) {
				col.SetCharset(toCharset)
				col.SetCollate(toCollate)
			} else {
				col.SetCharset(charset.CharsetBin)
				col.SetCollate(charset.CharsetBin)
			}
		}
	}

	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onSetTableFlashReplica(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var replicaInfo ast.TiFlashReplicaSpec
	if err := job.DecodeArgs(&replicaInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Ban setting replica count for tables in system database.
	if tidb_util.IsMemOrSysDB(job.SchemaName) {
		return ver, errors.Trace(dbterror.ErrUnsupportedTiFlashOperationForSysOrMemTable)
	}

	err = w.checkTiFlashReplicaCount(replicaInfo.Count)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// We should check this first, in order to avoid creating redundant DDL jobs.
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		logutil.DDLLogger().Info("Set TiFlash replica pd rule for partitioned table", zap.Int64("tableID", tblInfo.ID))
		if e := infosync.ConfigureTiFlashPDForPartitions(false, &pi.Definitions, replicaInfo.Count, &replicaInfo.Labels, tblInfo.ID); e != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(e)
		}
		// Partitions that in adding mid-state. They have high priorities, so we should set accordingly pd rules.
		if e := infosync.ConfigureTiFlashPDForPartitions(true, &pi.AddingDefinitions, replicaInfo.Count, &replicaInfo.Labels, tblInfo.ID); e != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(e)
		}
	} else {
		logutil.DDLLogger().Info("Set TiFlash replica pd rule", zap.Int64("tableID", tblInfo.ID))
		if e := infosync.ConfigureTiFlashPDForTable(tblInfo.ID, replicaInfo.Count, &replicaInfo.Labels); e != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(e)
		}
	}

	available := false
	if tblInfo.TiFlashReplica != nil {
		available = tblInfo.TiFlashReplica.Available
	}
	if replicaInfo.Count > 0 {
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:          replicaInfo.Count,
			LocationLabels: replicaInfo.Labels,
			Available:      available,
		}
	} else {
		if tblInfo.TiFlashReplica != nil {
			err = infosync.DeleteTiFlashTableSyncProgress(tblInfo)
			if err != nil {
				logutil.DDLLogger().Error("DeleteTiFlashTableSyncProgress fails", zap.Error(err))
			}
		}
		tblInfo.TiFlashReplica = nil
	}

	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) checkTiFlashReplicaCount(replicaCount uint64) error {
	ctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(ctx)

	return checkTiFlashReplicaCount(ctx, replicaCount)
}

func onUpdateFlashReplicaStatus(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	var available bool
	var physicalID int64
	if err := job.DecodeArgs(&available, &physicalID); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
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
					logutil.DDLLogger().Info("TiFlash replica become unavailable", zap.Int64("tableID", tblInfo.ID), zap.Int64("partitionID", id))
					break
				}
			}
		}
	} else {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("unknown physical ID %v in table %v", physicalID, tblInfo.Name.O)
	}

	if tblInfo.TiFlashReplica.Available {
		logutil.DDLLogger().Info("TiFlash replica available", zap.Int64("tableID", tblInfo.ID))
	}
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func checkTableNotExists(d *ddlCtx, t *meta.Meta, schemaID int64, tableName string) error {
	// Try to use memory schema info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
		return checkTableNotExistsFromInfoSchema(is, schemaID, tableName)
	}

	return checkTableNotExistsFromStore(t, schemaID, tableName)
}

func checkTableNotExistsByName(d *ddlCtx, t *meta.Meta, schemaID int64, schemaName, tableName string) error {
	// Try to use memory schema info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
		return checkTableNotExistsFromInfoSchema(is, schemaID, tableName)
	}
	return t.CheckTableNameNotExists(t.TableNameKey(schemaName, tableName))
}

func checkConstraintNamesNotExists(t *meta.Meta, schemaID int64, constraints []*model.ConstraintInfo) error {
	if len(constraints) == 0 {
		return nil
	}
	tbInfos, err := t.ListTables(schemaID)
	if err != nil {
		return err
	}

	for _, tb := range tbInfos {
		for _, constraint := range constraints {
			if constraint.State != model.StateWriteOnly {
				if constraintInfo := tb.FindConstraintInfoByName(constraint.Name.L); constraintInfo != nil {
					return infoschema.ErrCheckConstraintDupName.GenWithStackByArgs(constraint.Name.L)
				}
			}
		}
	}

	return nil
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
	tbls, err := t.ListSimpleTables(schemaID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return errors.Trace(err)
	}

	// Check the table.
	for _, tbl := range tbls {
		if tbl.Name.L == tableName {
			return infoschema.ErrTableExists.GenWithStackByArgs(tbl.Name)
		}
	}

	return nil
}

func findTableIDByName(d *ddlCtx, t *meta.Meta, schemaID int64, tableName string) (int64, error) {
	// Try to use memory schema info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return 0, err
	}
	is := d.infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
		return findTableIDFromInfoSchema(is, schemaID, tableName)
	}

	return findTableIDFromStore(t, schemaID, tableName)
}

func findTableIDFromInfoSchema(is infoschema.InfoSchema, schemaID int64, tableName string) (int64, error) {
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return 0, infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	tbl, err := is.TableByName(schema.Name, model.NewCIStr(tableName))
	if err != nil {
		return 0, err
	}
	return tbl.Meta().ID, nil
}

func findTableIDFromStore(t *meta.Meta, schemaID int64, tableName string) (int64, error) {
	tbls, err := t.ListSimpleTables(schemaID)
	if err != nil {
		if meta.ErrDBNotExists.Equal(err) {
			return 0, infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
		}
		return 0, errors.Trace(err)
	}
	for _, tbl := range tbls {
		if tbl.Name.L == tableName {
			return tbl.ID, nil
		}
	}
	return 0, infoschema.ErrTableNotExists.FastGenByArgs(tableName)
}

// updateVersionAndTableInfoWithCheck checks table info validate and updates the schema version and the table information
func updateVersionAndTableInfoWithCheck(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool, multiInfos ...schemaIDAndTableInfo) (
	ver int64, err error) {
	err = checkTableInfoValid(tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	for _, info := range multiInfos {
		err = checkTableInfoValid(info.tblInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}
	return updateVersionAndTableInfo(d, t, job, tblInfo, shouldUpdateVer, multiInfos...)
}

// updateVersionAndTableInfo updates the schema version and the table information.
func updateVersionAndTableInfo(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, shouldUpdateVer bool, multiInfos ...schemaIDAndTableInfo) (
	ver int64, err error) {
	failpoint.Inject("mockUpdateVersionAndTableInfoErr", func(val failpoint.Value) {
		switch val.(int) {
		case 1:
			failpoint.Return(ver, errors.New("mock update version and tableInfo error"))
		case 2:
			// We change it cancelled directly here, because we want to get the original error with the job id appended.
			// The job ID will be used to get the job from history queue and we will assert it's args.
			job.State = model.JobStateCancelled
			failpoint.Return(ver, errors.New("mock update version and tableInfo error, jobID="+strconv.Itoa(int(job.ID))))
		default:
		}
	})
	if shouldUpdateVer && (job.MultiSchemaInfo == nil || !job.MultiSchemaInfo.SkipVersion) {
		ver, err = updateSchemaVersion(d, t, job, multiInfos...)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	err = updateTable(t, job.SchemaID, tblInfo)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for _, info := range multiInfos {
		err = updateTable(t, info.schemaID, info.tblInfo)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	return ver, nil
}

func updateTable(t *meta.Meta, schemaID int64, tblInfo *model.TableInfo) error {
	if tblInfo.State == model.StatePublic {
		tblInfo.UpdateTS = t.StartTS
	}
	return t.UpdateTable(schemaID, tblInfo)
}

type schemaIDAndTableInfo struct {
	schemaID int64
	tblInfo  *model.TableInfo
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
	_, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// When in repair mode, the repaired table in a server is not access to user,
	// the table after repairing will be removed from repair list. Other server left
	// behind alive may need to restart to get the latest schema version.
	ver, err = updateSchemaVersion(d, t, job)
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
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State)
	}
}

func onAlterTableAttributes(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	rule := label.NewRule()
	err = job.DecodeArgs(rule)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	if len(rule.Labels) == 0 {
		patch := label.NewRulePatch([]*label.Rule{}, []string{rule.ID})
		err = infosync.UpdateLabelRules(context.TODO(), patch)
	} else {
		err = infosync.PutLabelRule(context.TODO(), rule)
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to notify PD the label rules")
	}
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)

	return ver, nil
}

func onAlterTablePartitionAttributes(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	var partitionID int64
	rule := label.NewRule()
	err = job.DecodeArgs(&partitionID, rule)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	ptInfo := tblInfo.GetPartitionInfo()
	if ptInfo.GetNameByID(partitionID) == "" {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O))
	}

	if len(rule.Labels) == 0 {
		patch := label.NewRulePatch([]*label.Rule{}, []string{rule.ID})
		err = infosync.UpdateLabelRules(context.TODO(), patch)
	} else {
		err = infosync.PutLabelRule(context.TODO(), rule)
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to notify PD the label rules")
	}
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)

	return ver, nil
}

func onAlterTablePartitionPlacement(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	var partitionID int64
	policyRefInfo := &model.PolicyRefInfo{}
	err = job.DecodeArgs(&partitionID, &policyRefInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	ptInfo := tblInfo.GetPartitionInfo()
	var partitionDef *model.PartitionDefinition
	definitions := ptInfo.Definitions
	oldPartitionEnablesPlacement := false
	for i := range definitions {
		if partitionID == definitions[i].ID {
			def := &definitions[i]
			oldPartitionEnablesPlacement = def.PlacementPolicyRef != nil
			def.PlacementPolicyRef = policyRefInfo
			partitionDef = &definitions[i]
			break
		}
	}

	if partitionDef == nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs("drop?", tblInfo.Name.O))
	}

	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(t, job, partitionDef.PlacementPolicyRef); err != nil {
		return ver, errors.Trace(err)
	}

	bundle, err := placement.NewPartitionBundle(t, *partitionDef)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if bundle == nil && oldPartitionEnablesPlacement {
		bundle = placement.NewBundle(partitionDef.ID)
	}

	// Send the placement bundle to PD.
	if bundle != nil {
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), []*placement.Bundle{bundle})
	}

	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onAlterTablePlacement(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	policyRefInfo := &model.PolicyRefInfo{}
	err = job.DecodeArgs(&policyRefInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, err
	}

	if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(t, job, policyRefInfo); err != nil {
		return 0, errors.Trace(err)
	}

	oldTableEnablesPlacement := tblInfo.PlacementPolicyRef != nil
	tblInfo.PlacementPolicyRef = policyRefInfo
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	bundle, err := placement.NewTableBundle(t, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if bundle == nil && oldTableEnablesPlacement {
		bundle = placement.NewBundle(tblInfo.ID)
	}

	// Send the placement bundle to PD.
	if bundle != nil {
		err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), []*placement.Bundle{bundle})
	}

	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)

	return ver, nil
}

func getOldLabelRules(tblInfo *model.TableInfo, oldSchemaName, oldTableName string) (string, []string, []string, map[string]*label.Rule, error) {
	tableRuleID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, oldSchemaName, oldTableName)
	oldRuleIDs := []string{tableRuleID}
	var partRuleIDs []string
	if tblInfo.GetPartitionInfo() != nil {
		for _, def := range tblInfo.GetPartitionInfo().Definitions {
			partRuleIDs = append(partRuleIDs, fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, oldSchemaName, oldTableName, def.Name.L))
		}
	}

	oldRuleIDs = append(oldRuleIDs, partRuleIDs...)
	oldRules, err := infosync.GetLabelRules(context.TODO(), oldRuleIDs)
	return tableRuleID, partRuleIDs, oldRuleIDs, oldRules, err
}

func updateLabelRules(job *model.Job, tblInfo *model.TableInfo, oldRules map[string]*label.Rule, tableRuleID string, partRuleIDs, oldRuleIDs []string, tID int64) error {
	if oldRules == nil {
		return nil
	}
	var newRules []*label.Rule
	if tblInfo.GetPartitionInfo() != nil {
		for idx, def := range tblInfo.GetPartitionInfo().Definitions {
			if r, ok := oldRules[partRuleIDs[idx]]; ok {
				newRules = append(newRules, r.Clone().Reset(job.SchemaName, tblInfo.Name.L, def.Name.L, def.ID))
			}
		}
	}
	ids := []int64{tID}
	if r, ok := oldRules[tableRuleID]; ok {
		if tblInfo.GetPartitionInfo() != nil {
			for _, def := range tblInfo.GetPartitionInfo().Definitions {
				ids = append(ids, def.ID)
			}
		}
		newRules = append(newRules, r.Clone().Reset(job.SchemaName, tblInfo.Name.L, "", ids...))
	}

	patch := label.NewRulePatch(newRules, oldRuleIDs)
	return infosync.UpdateLabelRules(context.TODO(), patch)
}

func onAlterCacheTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	tbInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// If the table is already in the cache state
	if tbInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		return ver, nil
	}

	if tbInfo.TempTableType != model.TempTableNone {
		return ver, errors.Trace(dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache"))
	}

	if tbInfo.Partition != nil {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("partition mode"))
	}

	switch tbInfo.TableCacheStatusType {
	case model.TableCacheStatusDisable:
		// disable -> switching
		tbInfo.TableCacheStatusType = model.TableCacheStatusSwitching
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tbInfo, true)
		if err != nil {
			return ver, err
		}
	case model.TableCacheStatusSwitching:
		// switching -> enable
		tbInfo.TableCacheStatusType = model.TableCacheStatusEnable
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tbInfo, true)
		if err != nil {
			return ver, err
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	default:
		job.State = model.JobStateCancelled
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("alter table cache", tbInfo.TableCacheStatusType.String())
	}
	return ver, err
}

func onAlterNoCacheTable(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	tbInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// If the table is not in the cache state
	if tbInfo.TableCacheStatusType == model.TableCacheStatusDisable {
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		return ver, nil
	}

	switch tbInfo.TableCacheStatusType {
	case model.TableCacheStatusEnable:
		//  enable ->  switching
		tbInfo.TableCacheStatusType = model.TableCacheStatusSwitching
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tbInfo, true)
		if err != nil {
			return ver, err
		}
	case model.TableCacheStatusSwitching:
		// switching -> disable
		tbInfo.TableCacheStatusType = model.TableCacheStatusDisable
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tbInfo, true)
		if err != nil {
			return ver, err
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	default:
		job.State = model.JobStateCancelled
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("alter table no cache", tbInfo.TableCacheStatusType.String())
	}
	return ver, err
}

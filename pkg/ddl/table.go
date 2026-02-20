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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"go.uber.org/zap"
)

const tiflashCheckTiDBHTTPAPIHalfInterval = 2500 * time.Millisecond

func repairTableOrViewWithCheck(t *meta.Mutator, job *model.Job, schemaID int64, tbInfo *model.TableInfo) error {
	err := checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	return t.UpdateTable(schemaID, tbInfo)
}

func (w *worker) onDropTableOrView(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetDropTableArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	tblInfo, err := checkTableExistAndCancelNonExistJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	originalState := job.SchemaState
	switch tblInfo.State {
	case model.StatePublic:
		// public -> write only
		if job.Type == model.ActionDropTable {
			err = checkDropTableHasForeignKeyReferredInOwner(jobCtx.infoCache, job, args)
			if err != nil {
				return ver, err
			}
		}
		tblInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		tblInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		tblInfo.State = model.StateNone
		oldIDs := getPartitionIDs(tblInfo)
		ruleIDs := append(getPartitionRuleIDs(job.SchemaName, tblInfo), fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L))

		args.OldPartitionIDs = oldIDs
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != tblInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		metaMut := jobCtx.metaMut
		if tblInfo.IsSequence() {
			if err = metaMut.DropSequence(job.SchemaID, job.TableID); err != nil {
				return ver, errors.Trace(err)
			}
		} else {
			if err = metaMut.DropTableOrView(job.SchemaID, job.TableID); err != nil {
				return ver, errors.Trace(err)
			}
			if err = metaMut.GetAutoIDAccessors(job.SchemaID, job.TableID).Del(); err != nil {
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
		if !tblInfo.IsSequence() && !tblInfo.IsView() {
			dropTableEvent := notifier.NewDropTableEvent(tblInfo)
			err = asyncNotifyEvent(jobCtx, dropTableEvent, job, noSubJob, w.sess)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}

		if err := deleteTableAffinityGroupsInPD(jobCtx, tblInfo, nil); err != nil {
			logutil.DDLLogger().Error("failed to delete affinity groups from PD", zap.Error(err), zap.Int64("tableID", tblInfo.ID))
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		startKey := tablecodec.EncodeTablePrefix(job.TableID)
		job.FillFinishedArgs(&model.DropTableArgs{
			StartKey:        startKey,
			OldPartitionIDs: oldIDs,
			OldRuleIDs:      ruleIDs,
		})
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State))
	}
	job.SchemaState = tblInfo.State
	return ver, errors.Trace(err)
}

func (w *worker) onRecoverTable(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetRecoverArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	recoverInfo := args.RecoverTableInfos()[0]

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

	err = checkTableNotExists(jobCtx.infoCache, schemaID, tblInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	metaMut := jobCtx.metaMut
	err = checkTableIDNotExists(metaMut, schemaID, tblInfo.ID)
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
			args.CheckFlag = recoverCheckFlagEnableGC
		} else {
			args.CheckFlag = recoverCheckFlagDisableGC
		}
		job.FillArgs(args)

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
		ver, err = w.recoverTable(jobCtx.stepCtx, metaMut, job, recoverInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		tableInfo := tblInfo.Clone()
		tableInfo.State = model.StatePublic
		tableInfo.UpdateTS = metaMut.StartTS

		var tids []int64
		if recoverInfo.TableInfo.GetPartitionInfo() != nil {
			tids = getPartitionIDs(recoverInfo.TableInfo)
			tids = append(tids, recoverInfo.TableInfo.ID)
		} else {
			tids = []int64{recoverInfo.TableInfo.ID}
		}
		args.AffectedPhysicalIDs = tids
		ver, err = updateVersionAndTableInfo(jobCtx, job, tableInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		tblInfo.State = model.StatePublic
		tblInfo.UpdateTS = metaMut.StartTS
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("table", tblInfo.State)
	}
	return ver, nil
}

func (w *worker) recoverTable(
	ctx context.Context,
	t *meta.Mutator,
	job *model.Job,
	recoverInfo *model.RecoverTableInfo,
) (ver int64, err error) {
	tableRuleID, partRuleIDs, oldRuleIDs, oldRules, err := getOldLabelRules(recoverInfo.TableInfo, recoverInfo.OldSchemaName, recoverInfo.OldTableName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to get old label rules from PD")
	}
	// Remove dropped table DDL job from gc_delete_range table.
	err = w.delRangeManager.removeFromGCDeleteRange(ctx, recoverInfo.DropJobID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	err = clearTablePlacementAndBundles(ctx, recoverInfo.TableInfo)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tableInfo := recoverInfo.TableInfo.Clone()
	tableInfo.State = model.StatePublic
	tableInfo.UpdateTS = t.StartTS
	err = t.CreateTableAndSetAutoID(recoverInfo.SchemaID, tableInfo, recoverInfo.AutoIDs)
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

	return ver, nil
}

func clearTablePlacementAndBundles(ctx context.Context, tblInfo *model.TableInfo) error {
	failpoint.Inject("mockClearTablePlacementAndBundlesErr", func() {
		failpoint.Return(errors.New("mock error for clearTablePlacementAndBundles"))
	})
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

	return infosync.PutRuleBundlesWithDefaultRetry(ctx, bundles)
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
func GetTableInfoAndCancelFaultJob(t *meta.Mutator, job *model.Job, schemaID int64) (*model.TableInfo, error) {
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

func checkTableExistAndCancelNonExistJob(t *meta.Mutator, job *model.Job, schemaID int64) (*model.TableInfo, error) {
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

func getTableInfo(t *meta.Mutator, tableID, schemaID int64) (*model.TableInfo, error) {
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
func (w *worker) onTruncateTable(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	args, err := model.GetTruncateTableArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.IsView() || tblInfo.IsSequence() {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrTableNotExists.GenWithStackByArgs(job.SchemaName, tblInfo.Name.O)
	}
	// Copy the old tableInfo for later usage.
	oldTblInfo := tblInfo.Clone()
	err = checkTruncateTableHasForeignKeyReferredInOwner(jobCtx.infoCache, job, tblInfo, args.FKCheck)
	if err != nil {
		return ver, err
	}
	err = metaMut.DropTableOrView(schemaID, tblInfo.ID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	err = metaMut.GetAutoIDAccessors(schemaID, tblInfo.ID).Del()
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

	var (
		oldPartitionIDs []int64
		newPartitionIDs = args.NewPartitionIDs
	)
	if tblInfo.GetPartitionInfo() != nil {
		oldPartitionIDs = getPartitionIDs(tblInfo)
		// We use the new partition ID because all the old data is encoded with the old partition ID, it can not be accessed anymore.
		newPartitionIDs, err = truncateTableByReassignPartitionIDs(metaMut, tblInfo, newPartitionIDs)
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
		args.OldPartIDsWithPolicy = oldIDs
		args.NewPartIDsWithPolicy = newIDs
	}

	tableRuleID, partRuleIDs, _, oldRules, err := getOldLabelRules(tblInfo, job.SchemaName, tblInfo.Name.L)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to get old label rules from PD")
	}

	err = updateLabelRules(job, tblInfo, oldRules, tableRuleID, partRuleIDs, []string{}, args.NewTableID)
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
			if e := infosync.ConfigureTiFlashPDForTable(args.NewTableID, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels); e != nil {
				logutil.DDLLogger().Error("ConfigureTiFlashPDForTable fails", zap.Error(err))
				job.State = model.JobStateCancelled
				return ver, errors.Trace(e)
			}
		}
		tblInfo.TiFlashReplica.AvailablePartitionIDs = nil
		tblInfo.TiFlashReplica.Available = false
	}

	tblInfo.ID = args.NewTableID

	// build table & partition bundles if any.
	bundles, err := placement.NewFullTableBundles(metaMut, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	err = metaMut.CreateTableOrView(schemaID, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	failpoint.Inject("mockTruncateTableUpdateVersionError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("mock update version error"))
		}
	})

	var scatterScope string
	if val, ok := job.GetSystemVars(vardef.TiDBScatterRegion); ok {
		scatterScope = val
	}
	preSplitAndScatterTable(w.sess.Context, jobCtx.store, tblInfo, scatterScope)

	// Create new affinity groups first (critical operation - must succeed)
	if tblInfo.Affinity != nil {
		if err = createTableAffinityGroupsInPD(jobCtx, tblInfo); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	// Delete old affinity groups (best-effort cleanup - ignore errors)
	// TRUNCATE TABLE: always try to delete old table's affinity groups
	if oldTblInfo.Affinity != nil {
		if err := deleteTableAffinityGroupsInPD(jobCtx, oldTblInfo, nil); err != nil {
			logutil.DDLLogger().Error("failed to delete old affinity groups from PD", zap.Error(err), zap.Int64("tableID", oldTblInfo.ID))
		}
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	truncateTableEvent := notifier.NewTruncateTableEvent(tblInfo, oldTblInfo)
	err = asyncNotifyEvent(jobCtx, truncateTableEvent, job, noSubJob, w.sess)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	// see truncateTableByReassignPartitionIDs for why they might change.
	args.OldPartitionIDs = oldPartitionIDs
	args.NewPartitionIDs = newPartitionIDs
	job.FillFinishedArgs(args)
	return ver, nil
}

func onRebaseAutoIncrementIDType(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	return onRebaseAutoID(jobCtx, job, autoid.AutoIncrementType)
}

func onRebaseAutoRandomType(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	return onRebaseAutoID(jobCtx, job, autoid.AutoRandomType)
}

func onRebaseAutoID(jobCtx *jobContext, job *model.Job, tp autoid.AllocatorType) (ver int64, _ error) {
	args, err := model.GetRebaseAutoIDArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	schemaID := job.SchemaID
	newBase, force := args.NewBase, args.Force

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
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
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableAutoIDCache(jobCtx *jobContext, job *model.Job) (int64, error) {
	args, err := model.GetModifyTableAutoIDCacheArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}

	tblInfo.AutoIDCache = args.NewCache
	ver, err := updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func (w *worker) onShardRowID(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetShardRowIDArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	shardRowIDBits := args.ShardRowIDBits
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if shardRowIDBits < tblInfo.ShardRowIDBits {
		tblInfo.ShardRowIDBits = shardRowIDBits
	} else {
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
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
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
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


func onModifyTableComment(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifyTableCommentArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}

	tblInfo.Comment = args.Comment
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func onModifyTableCharsetAndCollate(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifyTableCharsetAndCollateArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	toCharset, toCollate, needsOverwriteCols := args.ToCharset, args.ToCollate, args.NeedsOverwriteCols
	metaMut := jobCtx.metaMut
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(metaMut, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
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

	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}


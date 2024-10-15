// Copyright 2024 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
)

// SetSchemaDiffForCreateTables set SchemaDiff for ActionCreateTables.
func SetSchemaDiffForCreateTables(diff *model.SchemaDiff, job *model.Job) error {
	args, err := model.GetBatchCreateTableArgs(job)
	if err != nil {
		return errors.Trace(err)
	}
	diff.AffectedOpts = make([]*model.AffectedOption, len(args.Tables))
	for i := range args.Tables {
		tblInfo := args.Tables[i].TableInfo
		diff.AffectedOpts[i] = &model.AffectedOption{
			SchemaID:    job.SchemaID,
			OldSchemaID: job.SchemaID,
			TableID:     tblInfo.ID,
			OldTableID:  tblInfo.ID,
		}
	}
	return nil
}

// SetSchemaDiffForTruncateTable set SchemaDiff for ActionTruncateTable.
func SetSchemaDiffForTruncateTable(diff *model.SchemaDiff, job *model.Job, jobCtx *jobContext) error {
	// Truncate table has two table ID, should be handled differently.
	args := jobCtx.jobArgs.(*model.TruncateTableArgs)
	diff.TableID = args.NewTableID
	diff.OldTableID = job.TableID

	// affects are used to update placement rule cache
	if len(args.OldPartIDsWithPolicy) > 0 {
		diff.AffectedOpts = buildPlacementAffects(args.OldPartIDsWithPolicy, args.NewPartIDsWithPolicy)
	}
	return nil
}

// SetSchemaDiffForCreateView set SchemaDiff for ActionCreateView.
func SetSchemaDiffForCreateView(diff *model.SchemaDiff, job *model.Job) error {
	args, err := model.GetCreateTableArgs(job)
	if err != nil {
		return errors.Trace(err)
	}
	tbInfo, orReplace, oldTbInfoID := args.TableInfo, args.OnExistReplace, args.OldViewTblID
	// When the statement is "create or replace view " and we need to drop the old view,
	// it has two table IDs and should be handled differently.
	if oldTbInfoID > 0 && orReplace {
		diff.OldTableID = oldTbInfoID
	}
	diff.TableID = tbInfo.ID
	return nil
}

// SetSchemaDiffForRenameTable set SchemaDiff for ActionRenameTable.
func SetSchemaDiffForRenameTable(diff *model.SchemaDiff, job *model.Job, jobCtx *jobContext) error {
	args := jobCtx.jobArgs.(*model.RenameTableArgs)
	adjustedOldSchemaID := args.OldSchemaID
	if args.OldSchemaIDForSchemaDiff > 0 {
		adjustedOldSchemaID = args.OldSchemaIDForSchemaDiff
	}

	diff.OldSchemaID = adjustedOldSchemaID
	diff.TableID = job.TableID
	return nil
}

// SetSchemaDiffForRenameTables set SchemaDiff for ActionRenameTables.
func SetSchemaDiffForRenameTables(diff *model.SchemaDiff, _ *model.Job, jobCtx *jobContext) error {
	args := jobCtx.jobArgs.(*model.RenameTablesArgs)
	affects := make([]*model.AffectedOption, len(args.RenameTableInfos)-1)
	for i, info := range args.RenameTableInfos {
		// Do not add the first table to AffectedOpts. Related issue tidb#47064.
		if i == 0 {
			continue
		}
		adjustedOldSchemaID := info.OldSchemaID
		if info.OldSchemaIDForSchemaDiff > 0 {
			adjustedOldSchemaID = info.OldSchemaIDForSchemaDiff
		}
		affects[i-1] = &model.AffectedOption{
			SchemaID:    info.NewSchemaID,
			TableID:     info.TableID,
			OldTableID:  info.TableID,
			OldSchemaID: adjustedOldSchemaID,
		}
	}
	adjustedOldSchemaID := args.RenameTableInfos[0].OldSchemaID
	if args.RenameTableInfos[0].OldSchemaIDForSchemaDiff > 0 {
		adjustedOldSchemaID = args.RenameTableInfos[0].OldSchemaIDForSchemaDiff
	}
	diff.TableID = args.RenameTableInfos[0].TableID
	diff.SchemaID = args.RenameTableInfos[0].NewSchemaID
	diff.OldSchemaID = adjustedOldSchemaID
	diff.AffectedOpts = affects
	return nil
}

// SetSchemaDiffForExchangeTablePartition set SchemaDiff for ActionExchangeTablePartition.
func SetSchemaDiffForExchangeTablePartition(diff *model.SchemaDiff, job *model.Job, multiInfos ...schemaIDAndTableInfo) error {
	// From start of function: diff.SchemaID = job.SchemaID
	// Old is original non partitioned table
	diff.OldTableID = job.TableID
	diff.OldSchemaID = job.SchemaID
	// Update the partitioned table (it is only done in the last state)
	// See ddl.ExchangeTablePartition
	args, err := model.GetExchangeTablePartitionArgs(job)
	if err != nil {
		return errors.Trace(err)
	}
	// This is needed for not crashing TiFlash!
	// TODO: Update TiFlash, to handle StateWriteOnly
	diff.AffectedOpts = []*model.AffectedOption{{
		TableID: args.PTTableID,
	}}
	if job.SchemaState != model.StatePublic {
		// No change, just to refresh the non-partitioned table
		// with its new ExchangePartitionInfo.
		diff.TableID = job.TableID
		// Keep this as Schema ID of non-partitioned table
		// to avoid trigger early rename in TiFlash
		diff.AffectedOpts[0].SchemaID = job.SchemaID
		// Need reload partition table, use diff.AffectedOpts[0].OldSchemaID to mark it.
		if len(multiInfos) > 0 {
			diff.AffectedOpts[0].OldSchemaID = args.PTSchemaID
		}
	} else {
		// Swap
		diff.TableID = args.PartitionID
		// Also add correct SchemaID in case different schemas
		diff.AffectedOpts[0].SchemaID = args.PTSchemaID
	}
	return nil
}

// SetSchemaDiffForTruncateTablePartition set SchemaDiff for ActionTruncateTablePartition.
func SetSchemaDiffForTruncateTablePartition(diff *model.SchemaDiff, job *model.Job, jobCtx *jobContext) {
	diff.TableID = job.TableID
	args := jobCtx.jobArgs.(*model.TruncateTableArgs)
	if args.ShouldUpdateAffectedPartitions {
		diff.AffectedOpts = buildPlacementAffects(args.OldPartitionIDs, args.NewPartitionIDs)
	}
}

// SetSchemaDiffForDropTable set SchemaDiff for ActionDropTable.
func SetSchemaDiffForDropTable(diff *model.SchemaDiff, job *model.Job, jobCtx *jobContext) {
	// affects are used to update placement rule cache
	diff.TableID = job.TableID
	args := jobCtx.jobArgs.(*model.DropTableArgs)
	if len(args.OldPartitionIDs) > 0 {
		diff.AffectedOpts = buildPlacementAffects(args.OldPartitionIDs, args.OldPartitionIDs)
	}
}

// SetSchemaDiffForDropTablePartition set SchemaDiff for ActionDropTablePartition.
func SetSchemaDiffForDropTablePartition(diff *model.SchemaDiff, job *model.Job, jobCtx *jobContext) {
	// affects are used to update placement rule cache
	diff.TableID = job.TableID
	args := jobCtx.jobArgs.(*model.TablePartitionArgs)
	if len(args.OldPhysicalTblIDs) > 0 {
		diff.AffectedOpts = buildPlacementAffects(args.OldPhysicalTblIDs, args.OldPhysicalTblIDs)
	}
}

// SetSchemaDiffForRecoverTable set SchemaDiff for ActionRecoverTable.
func SetSchemaDiffForRecoverTable(diff *model.SchemaDiff, job *model.Job, jobCtx *jobContext) {
	// affects are used to update placement rule cache
	diff.TableID = job.TableID
	args := jobCtx.jobArgs.(*model.RecoverArgs)
	if len(args.AffectedPhysicalIDs) > 0 {
		diff.AffectedOpts = buildPlacementAffects(args.AffectedPhysicalIDs, args.AffectedPhysicalIDs)
	}
}

// SetSchemaDiffForReorganizePartition set SchemaDiff for ActionReorganizePartition.
func SetSchemaDiffForReorganizePartition(diff *model.SchemaDiff, job *model.Job) {
	diff.TableID = job.TableID
	// TODO: should this be for every state of Reorganize?
	if len(job.CtxVars) > 0 {
		if droppedIDs, ok := job.CtxVars[0].([]int64); ok {
			if addedIDs, ok := job.CtxVars[1].([]int64); ok {
				// to use AffectedOpts we need both new and old to have the same length
				maxParts := mathutil.Max[int](len(droppedIDs), len(addedIDs))
				// Also initialize them to 0!
				oldIDs := make([]int64, maxParts)
				copy(oldIDs, droppedIDs)
				newIDs := make([]int64, maxParts)
				copy(newIDs, addedIDs)
				diff.AffectedOpts = buildPlacementAffects(oldIDs, newIDs)
			}
		}
	}
}

// SetSchemaDiffForPartitionModify set SchemaDiff for ActionRemovePartitioning, ActionAlterTablePartitioning.
func SetSchemaDiffForPartitionModify(diff *model.SchemaDiff, job *model.Job) error {
	diff.TableID = job.TableID
	diff.OldTableID = job.TableID
	if job.SchemaState == model.StateDeleteReorganization {
		args, err := model.GetTablePartitionArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		partInfo := args.PartInfo
		// Final part, new table id is assigned
		diff.TableID = partInfo.NewTableID
		if len(job.CtxVars) > 0 { // TODO remove it.
			if droppedIDs, ok := job.CtxVars[0].([]int64); ok {
				if addedIDs, ok := job.CtxVars[1].([]int64); ok {
					// to use AffectedOpts we need both new and old to have the same length
					maxParts := mathutil.Max[int](len(droppedIDs), len(addedIDs))
					// Also initialize them to 0!
					oldIDs := make([]int64, maxParts)
					copy(oldIDs, droppedIDs)
					newIDs := make([]int64, maxParts)
					copy(newIDs, addedIDs)
					diff.AffectedOpts = buildPlacementAffects(oldIDs, newIDs)
				}
			}
		}
	}
	return nil
}

// SetSchemaDiffForCreateTable set SchemaDiff for ActionCreateTable.
func SetSchemaDiffForCreateTable(diff *model.SchemaDiff, job *model.Job) error {
	diff.TableID = job.TableID
	var tbInfo *model.TableInfo
	// create table with foreign key will update tableInfo in the job args, so we
	// must reuse already decoded ones.
	// TODO make DecodeArgs can reuse already decoded args, so we can use GetCreateTableArgs.
	if job.Version == model.JobVersion1 {
		tbInfo, _ = job.Args[0].(*model.TableInfo)
	} else {
		tbInfo = job.Args[0].(*model.CreateTableArgs).TableInfo
	}
	// When create table with foreign key, there are two schema status change:
	// 1. none -> write-only
	// 2. write-only -> public
	// In the second status change write-only -> public, infoschema loader should apply drop old table first, then
	// apply create new table. So need to set diff.OldTableID here to make sure it.
	if tbInfo.State == model.StatePublic && len(tbInfo.ForeignKeys) > 0 {
		diff.OldTableID = job.TableID
	}
	return nil
}

// SetSchemaDiffForRecoverSchema set SchemaDiff for ActionRecoverSchema.
func SetSchemaDiffForRecoverSchema(diff *model.SchemaDiff, job *model.Job) error {
	args, err := model.GetRecoverArgs(job)
	if err != nil {
		return errors.Trace(err)
	}
	recoverTabsInfo := args.RecoverTableInfos()
	diff.AffectedOpts = make([]*model.AffectedOption, len(recoverTabsInfo))
	for i := range recoverTabsInfo {
		diff.AffectedOpts[i] = &model.AffectedOption{
			SchemaID:    job.SchemaID,
			OldSchemaID: job.SchemaID,
			TableID:     recoverTabsInfo[i].TableInfo.ID,
			OldTableID:  recoverTabsInfo[i].TableInfo.ID,
		}
	}
	diff.ReadTableFromMeta = true
	return nil
}

// SetSchemaDiffForFlashbackCluster set SchemaDiff for ActionFlashbackCluster.
func SetSchemaDiffForFlashbackCluster(diff *model.SchemaDiff, job *model.Job) {
	diff.TableID = -1
	if job.SchemaState == model.StatePublic {
		diff.RegenerateSchemaMap = true
	}
}

// SetSchemaDiffForMultiInfos set SchemaDiff for multiInfos.
func SetSchemaDiffForMultiInfos(diff *model.SchemaDiff, multiInfos ...schemaIDAndTableInfo) {
	if len(multiInfos) > 0 {
		existsMap := make(map[int64]struct{})
		existsMap[diff.TableID] = struct{}{}
		for _, affect := range diff.AffectedOpts {
			existsMap[affect.TableID] = struct{}{}
		}
		for _, info := range multiInfos {
			_, exist := existsMap[info.tblInfo.ID]
			if exist {
				continue
			}
			existsMap[info.tblInfo.ID] = struct{}{}
			diff.AffectedOpts = append(diff.AffectedOpts, &model.AffectedOption{
				SchemaID:    info.schemaID,
				OldSchemaID: info.schemaID,
				TableID:     info.tblInfo.ID,
				OldTableID:  info.tblInfo.ID,
			})
		}
	}
}

// updateSchemaVersion increments the schema version by 1 and sets SchemaDiff.
func updateSchemaVersion(jobCtx *jobContext, job *model.Job, multiInfos ...schemaIDAndTableInfo) (int64, error) {
	schemaVersion, err := jobCtx.setSchemaVersion(job)
	if err != nil {
		return 0, errors.Trace(err)
	}
	diff := &model.SchemaDiff{
		Version:  schemaVersion,
		Type:     job.Type,
		SchemaID: job.SchemaID,
	}
	switch job.Type {
	case model.ActionCreateTables:
		err = SetSchemaDiffForCreateTables(diff, job)
	case model.ActionTruncateTable:
		err = SetSchemaDiffForTruncateTable(diff, job, jobCtx)
	case model.ActionCreateView:
		err = SetSchemaDiffForCreateView(diff, job)
	case model.ActionRenameTable:
		err = SetSchemaDiffForRenameTable(diff, job, jobCtx)
	case model.ActionRenameTables:
		err = SetSchemaDiffForRenameTables(diff, job, jobCtx)
	case model.ActionExchangeTablePartition:
		err = SetSchemaDiffForExchangeTablePartition(diff, job, multiInfos...)
	case model.ActionTruncateTablePartition:
		SetSchemaDiffForTruncateTablePartition(diff, job, jobCtx)
	case model.ActionDropTablePartition:
		SetSchemaDiffForDropTablePartition(diff, job, jobCtx)
	case model.ActionRecoverTable:
		SetSchemaDiffForRecoverTable(diff, job, jobCtx)
	case model.ActionDropTable:
		SetSchemaDiffForDropTable(diff, job, jobCtx)
	case model.ActionReorganizePartition:
		SetSchemaDiffForReorganizePartition(diff, job)
	case model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		err = SetSchemaDiffForPartitionModify(diff, job)
	case model.ActionCreateTable:
		err = SetSchemaDiffForCreateTable(diff, job)
	case model.ActionRecoverSchema:
		err = SetSchemaDiffForRecoverSchema(diff, job)
	case model.ActionFlashbackCluster:
		SetSchemaDiffForFlashbackCluster(diff, job)
	default:
		diff.TableID = job.TableID
	}
	if err != nil {
		return 0, err
	}
	SetSchemaDiffForMultiInfos(diff, multiInfos...)
	err = jobCtx.metaMut.SetSchemaDiff(diff)
	return schemaVersion, errors.Trace(err)
}

func waitVersionSynced(
	ctx context.Context,
	jobCtx *jobContext,
	job *model.Job,
	latestSchemaVersion int64,
) (err error) {
	failpoint.Inject("checkDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
		if val.(bool) {
			if mockDDLErrOnce > 0 && mockDDLErrOnce != latestSchemaVersion {
				panic("check down before update global version failed")
			}
			mockDDLErrOnce = -1
		}
	})
	timeStart := time.Now()
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerWaitSchemaChanged, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()
	// WaitVersionSynced returns only when all TiDB schemas are synced(exclude the isolated TiDB).
	err = jobCtx.schemaVerSyncer.WaitVersionSynced(ctx, job.ID, latestSchemaVersion)
	if err != nil {
		logutil.DDLLogger().Info("wait latest schema version encounter error", zap.Int64("ver", latestSchemaVersion),
			zap.Int64("jobID", job.ID), zap.Duration("take time", time.Since(timeStart)), zap.Error(err))
		return err
	}
	logutil.DDLLogger().Info("wait latest schema version changed(get the metadata lock if tidb_enable_metadata_lock is true)",
		zap.Int64("ver", latestSchemaVersion),
		zap.Duration("take time", time.Since(timeStart)),
		zap.String("job", job.String()))
	return nil
}

// waitVersionSyncedWithoutMDL handles the following situation:
// If the job enters a new state, and the worker crash when it's in the process of
// version sync, then the worker restarts quickly, we may run the job immediately again,
// but schema version might not sync.
// So here we get the latest schema version to make sure all servers' schema version
// update to the latest schema version in a cluster.
func waitVersionSyncedWithoutMDL(ctx context.Context, jobCtx *jobContext, job *model.Job) error {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return nil
	}

	ver, _ := jobCtx.store.CurrentVersion(kv.GlobalTxnScope)
	snapshot := jobCtx.store.GetSnapshot(ver)
	m := meta.NewReader(snapshot)
	latestSchemaVersion, err := m.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		logutil.DDLLogger().Warn("get global version failed", zap.Int64("jobID", job.ID), zap.Error(err))
		return err
	}

	failpoint.Inject("checkDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
		if val.(bool) {
			if mockDDLErrOnce > 0 && mockDDLErrOnce != latestSchemaVersion {
				panic("check down before update global version failed")
			}
			mockDDLErrOnce = -1
		}
	})

	return updateGlobalVersionAndWaitSynced(ctx, jobCtx, latestSchemaVersion, job)
}

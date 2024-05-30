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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
)

// SetSchemaDiffForCreateTables set SchemaDiff for ActionCreateTables.
func SetSchemaDiffForCreateTables(diff *model.SchemaDiff, job *model.Job) error {
	var tableInfos []*model.TableInfo
	err := job.DecodeArgs(&tableInfos)
	if err != nil {
		return errors.Trace(err)
	}
	diff.AffectedOpts = make([]*model.AffectedOption, len(tableInfos))
	for i := range tableInfos {
		diff.AffectedOpts[i] = &model.AffectedOption{
			SchemaID:    job.SchemaID,
			OldSchemaID: job.SchemaID,
			TableID:     tableInfos[i].ID,
			OldTableID:  tableInfos[i].ID,
		}
	}
	return nil
}

// SetSchemaDiffForTruncateTable set SchemaDiff for ActionTruncateTable.
func SetSchemaDiffForTruncateTable(diff *model.SchemaDiff, job *model.Job) error {
	// Truncate table has two table ID, should be handled differently.
	err := job.DecodeArgs(&diff.TableID)
	if err != nil {
		return errors.Trace(err)
	}
	diff.OldTableID = job.TableID

	// affects are used to update placement rule cache
	if len(job.CtxVars) > 0 {
		oldIDs := job.CtxVars[0].([]int64)
		newIDs := job.CtxVars[1].([]int64)
		diff.AffectedOpts = buildPlacementAffects(oldIDs, newIDs)
	}
	return nil
}

// SetSchemaDiffForCreateView set SchemaDiff for ActionCreateView.
func SetSchemaDiffForCreateView(diff *model.SchemaDiff, job *model.Job) error {
	tbInfo := &model.TableInfo{}
	var orReplace bool
	var oldTbInfoID int64
	if err := job.DecodeArgs(tbInfo, &orReplace, &oldTbInfoID); err != nil {
		return errors.Trace(err)
	}
	// When the statement is "create or replace view " and we need to drop the old view,
	// it has two table IDs and should be handled differently.
	if oldTbInfoID > 0 && orReplace {
		diff.OldTableID = oldTbInfoID
	}
	diff.TableID = tbInfo.ID
	return nil
}

// SetSchemaDiffForRenameTable set SchemaDiff for ActionRenameTable.
func SetSchemaDiffForRenameTable(diff *model.SchemaDiff, job *model.Job) error {
	err := job.DecodeArgs(&diff.OldSchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	diff.TableID = job.TableID
	return nil
}

// SetSchemaDiffForRenameTables set SchemaDiff for ActionRenameTables.
func SetSchemaDiffForRenameTables(diff *model.SchemaDiff, job *model.Job) error {
	var (
		oldSchemaIDs, newSchemaIDs, tableIDs []int64
		tableNames, oldSchemaNames           []*model.CIStr
	)
	err := job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs, &tableNames, &tableIDs, &oldSchemaNames)
	if err != nil {
		return errors.Trace(err)
	}
	affects := make([]*model.AffectedOption, len(newSchemaIDs)-1)
	for i, newSchemaID := range newSchemaIDs {
		// Do not add the first table to AffectedOpts. Related issue tidb#47064.
		if i == 0 {
			continue
		}
		affects[i-1] = &model.AffectedOption{
			SchemaID:    newSchemaID,
			TableID:     tableIDs[i],
			OldTableID:  tableIDs[i],
			OldSchemaID: oldSchemaIDs[i],
		}
	}
	diff.TableID = tableIDs[0]
	diff.SchemaID = newSchemaIDs[0]
	diff.OldSchemaID = oldSchemaIDs[0]
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
	var (
		ptSchemaID     int64
		ptTableID      int64
		ptDefID        int64
		partName       string // Not used
		withValidation bool   // Not used
	)
	// See ddl.ExchangeTablePartition
	err := job.DecodeArgs(&ptDefID, &ptSchemaID, &ptTableID, &partName, &withValidation)
	if err != nil {
		return errors.Trace(err)
	}
	// This is needed for not crashing TiFlash!
	// TODO: Update TiFlash, to handle StateWriteOnly
	diff.AffectedOpts = []*model.AffectedOption{{
		TableID: ptTableID,
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
			diff.AffectedOpts[0].OldSchemaID = ptSchemaID
		}
	} else {
		// Swap
		diff.TableID = ptDefID
		// Also add correct SchemaID in case different schemas
		diff.AffectedOpts[0].SchemaID = ptSchemaID
	}
	return nil
}

// SetSchemaDiffForTruncateTablePartition set SchemaDiff for ActionTruncateTablePartition.
func SetSchemaDiffForTruncateTablePartition(diff *model.SchemaDiff, job *model.Job) {
	diff.TableID = job.TableID
	if len(job.CtxVars) > 0 {
		oldIDs := job.CtxVars[0].([]int64)
		newIDs := job.CtxVars[1].([]int64)
		diff.AffectedOpts = buildPlacementAffects(oldIDs, newIDs)
	}
}

// SetSchemaDiffForDropTable set SchemaDiff for ActionDropTablePartition, ActionRecoverTable, ActionDropTable.
func SetSchemaDiffForDropTable(diff *model.SchemaDiff, job *model.Job) {
	// affects are used to update placement rule cache
	diff.TableID = job.TableID
	if len(job.CtxVars) > 0 {
		if oldIDs, ok := job.CtxVars[0].([]int64); ok {
			diff.AffectedOpts = buildPlacementAffects(oldIDs, oldIDs)
		}
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
		partInfo := &model.PartitionInfo{}
		var partNames []string
		err := job.DecodeArgs(&partNames, &partInfo)
		if err != nil {
			return errors.Trace(err)
		}
		// Final part, new table id is assigned
		diff.TableID = partInfo.NewTableID
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
	return nil
}

// SetSchemaDiffForCreateTable set SchemaDiff for ActionCreateTable.
func SetSchemaDiffForCreateTable(diff *model.SchemaDiff, job *model.Job) {
	diff.TableID = job.TableID
	if len(job.Args) > 0 {
		tbInfo, _ := job.Args[0].(*model.TableInfo)
		// When create table with foreign key, there are two schema status change:
		// 1. none -> write-only
		// 2. write-only -> public
		// In the second status change write-only -> public, infoschema loader should apply drop old table first, then
		// apply create new table. So need to set diff.OldTableID here to make sure it.
		if tbInfo != nil && tbInfo.State == model.StatePublic && len(tbInfo.ForeignKeys) > 0 {
			diff.OldTableID = job.TableID
		}
	}
}

// SetSchemaDiffForRecoverSchema set SchemaDiff for ActionRecoverSchema.
func SetSchemaDiffForRecoverSchema(diff *model.SchemaDiff, job *model.Job) error {
	var (
		recoverSchemaInfo      *RecoverSchemaInfo
		recoverSchemaCheckFlag int64
	)
	err := job.DecodeArgs(&recoverSchemaInfo, &recoverSchemaCheckFlag)
	if err != nil {
		return errors.Trace(err)
	}
	// Reserved recoverSchemaCheckFlag value for gc work judgment.
	job.Args[checkFlagIndexInJobArgs] = recoverSchemaCheckFlag
	recoverTabsInfo := recoverSchemaInfo.RecoverTabsInfo
	diff.AffectedOpts = make([]*model.AffectedOption, len(recoverTabsInfo))
	for i := range recoverTabsInfo {
		diff.AffectedOpts[i] = &model.AffectedOption{
			SchemaID:    job.SchemaID,
			OldSchemaID: job.SchemaID,
			TableID:     recoverTabsInfo[i].TableInfo.ID,
			OldTableID:  recoverTabsInfo[i].TableInfo.ID,
		}
	}
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
func updateSchemaVersion(d *ddlCtx, t *meta.Meta, job *model.Job, multiInfos ...schemaIDAndTableInfo) (int64, error) {
	schemaVersion, err := d.setSchemaVersion(job, d.store)
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
		err = SetSchemaDiffForTruncateTable(diff, job)
	case model.ActionCreateView:
		err = SetSchemaDiffForCreateView(diff, job)
	case model.ActionRenameTable:
		err = SetSchemaDiffForRenameTable(diff, job)
	case model.ActionRenameTables:
		err = SetSchemaDiffForRenameTables(diff, job)
	case model.ActionExchangeTablePartition:
		err = SetSchemaDiffForExchangeTablePartition(diff, job, multiInfos...)
	case model.ActionTruncateTablePartition:
		SetSchemaDiffForTruncateTablePartition(diff, job)
	case model.ActionDropTablePartition, model.ActionRecoverTable, model.ActionDropTable:
		SetSchemaDiffForDropTable(diff, job)
	case model.ActionReorganizePartition:
		SetSchemaDiffForReorganizePartition(diff, job)
	case model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		err = SetSchemaDiffForPartitionModify(diff, job)
	case model.ActionCreateTable:
		SetSchemaDiffForCreateTable(diff, job)
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
	err = t.SetSchemaDiff(diff)
	return schemaVersion, errors.Trace(err)
}

func checkAllVersions(ctx context.Context, d *ddlCtx, job *model.Job, latestSchemaVersion int64, timeStart time.Time) error {
	failpoint.Inject("checkDownBeforeUpdateGlobalVersion", func(val failpoint.Value) {
		if val.(bool) {
			if mockDDLErrOnce > 0 && mockDDLErrOnce != latestSchemaVersion {
				panic("check down before update global version failed")
			}
			mockDDLErrOnce = -1
		}
	})

	// OwnerCheckAllVersions returns only when all TiDB schemas are synced(exclude the isolated TiDB).
	err := d.schemaSyncer.OwnerCheckAllVersions(ctx, job.ID, latestSchemaVersion)
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

// waitSchemaSynced handles the following situation:
// If the job enters a new state, and the worker crashs when it's in the process of waiting for 2 * lease time,
// Then the worker restarts quickly, we may run the job immediately again,
// but in this case we don't wait enough 2 * lease time to let other servers update the schema.
// So here we get the latest schema version to make sure all servers' schema version update to the latest schema version
// in a cluster, or to wait for 2 * lease time.
func waitSchemaSynced(ctx context.Context, d *ddlCtx, job *model.Job, waitTime time.Duration) error {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return nil
	}

	ver, _ := d.store.CurrentVersion(kv.GlobalTxnScope)
	snapshot := d.store.GetSnapshot(ver)
	m := meta.NewSnapshotMeta(snapshot)
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

	return waitSchemaChanged(ctx, d, waitTime, latestSchemaVersion, job)
}

// Copyright 2018 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (w *worker) onReorganizePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	// Handle the rolling back job
	if job.IsRollingback() {
		return w.rollbackLikeDropPartition(jobCtx, job)
	}

	tblInfo, partNames, partInfo, err := getReorgPartitionInfo(jobCtx.metaMut, job, args)
	if err != nil {
		return ver, err
	}

	metaMut := jobCtx.metaMut
	switch job.SchemaState {
	case model.StateNone:
		if tblInfo.Affinity != nil {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("REORGANIZE PARTITION of a table with AFFINITY option")
		}

		// job.SchemaState == model.StateNone means the job is in the initial state of reorg partition.
		// Here should use partInfo from job directly and do some check action.
		// In case there was a race for queueing different schema changes on the same
		// table and the checks was not done on the current schema version.
		// The partInfo may have been checked against an older schema version for example.
		// If the check is done here, it does not need to be repeated, since no other
		// DDL on the same table can be run concurrently.
		tblInfo.Partition.DDLAction = job.Type
		num := len(partInfo.Definitions) - len(partNames) + len(tblInfo.Partition.Definitions)
		err = checkAddPartitionTooManyPartitions(uint64(num))
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkReorgPartitionNames(tblInfo.Partition, partNames, partInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		// Re-check that the dropped/added partitions are compatible with current definition
		firstPartIdx, lastPartIdx, idMap, err := getReplacedPartitionIDs(partNames, tblInfo.Partition)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		sctx := w.sess.Context
		if err = checkReorgPartitionDefs(sctx, job.Type, tblInfo, partInfo, firstPartIdx, lastPartIdx, idMap); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		if job.Type == model.ActionAlterTablePartitioning {
			// Also verify same things as in CREATE TABLE ... PARTITION BY
			if len(partInfo.Columns) > 0 {
				// shallow copy, only for reading/checking
				tmpTblInfo := *tblInfo
				tmpTblInfo.Partition = partInfo
				if err = checkColumnsPartitionType(&tmpTblInfo); err != nil {
					job.State = model.JobStateCancelled
					return ver, err
				}
			} else {
				if err = checkPartitionFuncType(sctx.GetExprCtx(), partInfo.Expr, job.SchemaName, tblInfo); err != nil {
					job.State = model.JobStateCancelled
					return ver, err
				}
			}
		}
		// move the adding definition into tableInfo.
		updateAddingPartitionInfo(partInfo, tblInfo)
		orgDefs := tblInfo.Partition.Definitions
		updateDroppingPartitionInfo(tblInfo, partNames)
		// Reset original partitions, and keep DroppedDefinitions
		tblInfo.Partition.Definitions = orgDefs

		// modify placement settings
		for _, def := range tblInfo.Partition.AddingDefinitions {
			if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(metaMut, job, def.PlacementPolicyRef); err != nil {
				// job.State = model.JobStateCancelled may be set depending on error in function above.
				return ver, errors.Trace(err)
			}
		}

		// All global indexes must be recreated, we cannot update them in-place, since we must have
		// both old and new set of partition ids in the unique index at the same time!
		// We also need to recreate and change between non-global unique indexes and global index,
		// in case a new PARTITION BY changes if all partition columns are included or not.
		for _, index := range tblInfo.Indices {
			newGlobal := getNewGlobal(partInfo, index)
			if job.Type == model.ActionRemovePartitioning {
				// When removing partitioning, set all indexes to 'local' since it will become a non-partitioned table!
				newGlobal = false
			}
			if !index.Global && !newGlobal {
				continue
			}
			inAllPartitionColumns, err := checkPartitionKeysConstraint(partInfo, index.Columns, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
			// Currently only support Explicit Global indexes for unique index.
			if !inAllPartitionColumns && !newGlobal && index.Unique {
				job.State = model.JobStateCancelled
				return ver, dbterror.ErrGlobalIndexNotExplicitlySet.GenWithStackByArgs(index.Name.O)
			}
			if tblInfo.Partition.DDLChangedIndex == nil {
				tblInfo.Partition.DDLChangedIndex = make(map[int64]bool)
			}
			// Duplicate the unique indexes with new index ids.
			// If previously was Global or will be Global:
			// it must be recreated with new index ID
			// TODO: Could we allow that session in StateWriteReorganization, when StateDeleteReorganization
			// has started, may not find changes through the global index that sessions in StateDeleteReorganization made?
			// If so, then we could avoid copying the full Global Index if it has not changed from LOCAL!
			// It might be possible to use the new, not yet public partitions to access those rows?!
			// Just that it would not work with explicit partition select SELECT FROM t PARTITION (p,...)
			newIndex := index.Clone()
			newIndex.State = model.StateDeleteOnly
			newIndex.ID = AllocateIndexID(tblInfo)
			tblInfo.Partition.DDLChangedIndex[index.ID] = false
			tblInfo.Partition.DDLChangedIndex[newIndex.ID] = true
			newIndex.Global = newGlobal
			setGlobalIndexVersion(tblInfo, newIndex)
			tblInfo.Indices = append(tblInfo.Indices, newIndex)
		}
		failpoint.Inject("reorgPartCancel1", func(val failpoint.Value) {
			if val.(bool) {
				job.State = model.JobStateCancelled
				failpoint.Return(ver, errors.New("Injected error by reorgPartCancel1"))
			}
		})
		// From now on we cannot just cancel the DDL, we must roll back if changesMade!
		changesMade := false
		if tblInfo.TiFlashReplica != nil {
			// Must set placement rule, and make sure it succeeds.
			if err := infosync.ConfigureTiFlashPDForPartitions(true, &tblInfo.Partition.AddingDefinitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID); err != nil {
				logutil.DDLLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(err))
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			changesMade = true
			// In the next step, StateDeleteOnly, wait to verify the TiFlash replicas are OK
		}

		changed, err := alterTablePartitionBundles(metaMut, tblInfo, tblInfo.Partition.AddingDefinitions)
		if err != nil {
			if !changesMade {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			return rollbackReorganizePartitionWithErr(jobCtx, job, err)
		}
		changesMade = changesMade || changed

		ids := getIDs([]*model.TableInfo{tblInfo})
		for _, p := range tblInfo.Partition.AddingDefinitions {
			ids = append(ids, p.ID)
		}
		changed, err = alterTableLabelRule(job.SchemaName, tblInfo, ids)
		changesMade = changesMade || changed
		if err != nil {
			if !changesMade {
				job.State = model.JobStateCancelled
				return ver, err
			}
			return rollbackReorganizePartitionWithErr(jobCtx, job, err)
		}

		// Doing the preSplitAndScatter here, since all checks are completed,
		// and we will soon start writing to the new partitions.
		if s, ok := jobCtx.store.(kv.SplittableStore); ok && s != nil {
			// 1. partInfo only contains the AddingPartitions
			// 2. ScatterTable control all new split region need waiting for scatter region finish at table level.
			splitPartitionTableRegion(w.sess.Context, s, tblInfo, partInfo.Definitions, vardef.ScatterTable)
		}

		if job.Type == model.ActionReorganizePartition {
			tblInfo.Partition.SetOriginalPartitionIDs()
		}

		// Assume we cannot have more than MaxUint64 rows, set the progress to 1/10 of that.
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String(), "").Set(0.1 / float64(math.MaxUint64))
		job.SchemaState = model.StateDeleteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		failpoint.Inject("reorgPartRollback1", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartRollback1")
				failpoint.Return(rollbackReorganizePartitionWithErr(jobCtx, job, err))
			}
		})

		// Is really both StateDeleteOnly AND StateWriteOnly needed?
		// If transaction A in WriteOnly inserts row 1 (into both new and old partition set)
		// and then transaction B in DeleteOnly deletes that row (in both new and old)
		// does really transaction B need to do the delete in the new partition?
		// Yes, otherwise it would still be there when the WriteReorg happens,
		// and WriteReorg would only copy existing rows to the new table, so unless it is
		// deleted it would result in a ghost row!
		// What about update then?
		// Updates also need to be handled for new partitions in DeleteOnly,
		// since it would not be overwritten during Reorganize phase.
		// BUT if the update results in adding in one partition and deleting in another,
		// THEN only the delete must happen in the new partition set, not the insert!
	case model.StateDeleteOnly:
		// This state is to confirm all servers can not see the new partitions when reorg is running,
		// so that all deletes will be done in both old and new partitions when in either DeleteOnly
		// or WriteOnly state.
		// Also using the state for checking that the optional TiFlash replica is available, making it
		// in a state without (much) data and easy to retry without side effects.

		// Reason for having it here, is to make it easy for retry, and better to make sure it is in-sync
		// as early as possible, to avoid a long wait after the data copying.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			// For available state, the new added partition should wait its replica to
			// be finished, otherwise the query to this partition will be blocked.
			count := tblInfo.TiFlashReplica.Count
			needRetry, err := checkPartitionReplica(count, tblInfo.Partition.AddingDefinitions, jobCtx)
			if err != nil {
				return rollbackReorganizePartitionWithErr(jobCtx, job, err)
			}
			if needRetry {
				// The new added partition hasn't been replicated.
				// Do nothing to the job this time, wait next worker round.
				time.Sleep(tiflashCheckTiDBHTTPAPIHalfInterval)
				// Set the error here which will lead this job exit when it's retry times beyond the limitation.
				return ver, errors.Errorf("[ddl] add partition wait for tiflash replica to complete")
			}

			// When TiFlash Replica is ready, we must move them into `AvailablePartitionIDs`.
			// Since onUpdateFlashReplicaStatus cannot see the partitions yet (not public)
			for _, d := range tblInfo.Partition.AddingDefinitions {
				tblInfo.TiFlashReplica.AvailablePartitionIDs = append(tblInfo.TiFlashReplica.AvailablePartitionIDs, d.ID)
			}
		}

		for i := range tblInfo.Indices {
			if tblInfo.Indices[i].State == model.StateDeleteOnly {
				tblInfo.Indices[i].State = model.StateWriteOnly
			}
		}
		tblInfo.Partition.DDLState = model.StateWriteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String(), "").Set(0.2 / float64(math.MaxUint64))
		failpoint.Inject("reorgPartRollback2", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartRollback2")
				failpoint.Return(rollbackReorganizePartitionWithErr(jobCtx, job, err))
			}
		})
		job.SchemaState = model.StateWriteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteOnly:
		// Insert this state to confirm all servers can see the new partitions when reorg is running,
		// so that new data will be updated in both old and new partitions when reorganizing.
		job.SnapshotVer = 0
		for i := range tblInfo.Indices {
			if tblInfo.Indices[i].State == model.StateWriteOnly {
				tblInfo.Indices[i].State = model.StateWriteReorganization
			}
		}
		job.SchemaState = model.StateWriteReorganization
		tblInfo.Partition.DDLState = job.SchemaState
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String(), "").Set(0.3 / float64(math.MaxUint64))
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteReorganization:
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		tbl, err2 := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
		if err2 != nil {
			return ver, errors.Trace(err2)
		}
		failpoint.Inject("reorgPartFail1", func(val failpoint.Value) {
			// Failures will retry, then do rollback
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail1"))
			}
		})
		failpoint.Inject("reorgPartRollback3", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartRollback3")
				failpoint.Return(rollbackReorganizePartitionWithErr(jobCtx, job, err))
			}
		})
		var done bool
		done, ver, err = doPartitionReorgWork(w, jobCtx, job, tbl, physicalTableIDs)

		if !done {
			return ver, err
		}

		failpoint.Inject("reorgPartRollback4", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartRollback4")
				failpoint.Return(rollbackReorganizePartitionWithErr(jobCtx, job, err))
			}
		})

		for _, index := range tblInfo.Indices {
			isNew, ok := tblInfo.Partition.DDLChangedIndex[index.ID]
			if !ok {
				continue
			}
			if isNew {
				// Newly created index, replacing old unique/global index
				index.State = model.StatePublic
				continue
			}
			// Old index, should not be visible any longer,
			// but needs to be kept up-to-date in case rollback happens.
			index.State = model.StateWriteOnly
		}
		firstPartIdx, lastPartIdx, idMap, err2 := getReplacedPartitionIDs(partNames, tblInfo.Partition)
		if err2 != nil {
			return ver, err2
		}
		newDefs := getReorganizedDefinitions(tblInfo.Partition, firstPartIdx, lastPartIdx, idMap)

		// From now on, use the new partitioning, but keep the Adding and Dropping for double write
		tblInfo.Partition.Definitions = newDefs
		tblInfo.Partition.Num = uint64(len(newDefs))
		if job.Type == model.ActionAlterTablePartitioning ||
			job.Type == model.ActionRemovePartitioning {
			tblInfo.Partition.Type, tblInfo.Partition.DDLType = tblInfo.Partition.DDLType, tblInfo.Partition.Type
			tblInfo.Partition.Expr, tblInfo.Partition.DDLExpr = tblInfo.Partition.DDLExpr, tblInfo.Partition.Expr
			tblInfo.Partition.Columns, tblInfo.Partition.DDLColumns = tblInfo.Partition.DDLColumns, tblInfo.Partition.Columns
		}

		failpoint.Inject("reorgPartFail2", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail2"))
			}
		})

		// Now all the data copying is done, but we cannot simply remove the droppingDefinitions
		// since they are a part of the normal Definitions that other nodes with
		// the current schema version. So we need to double write for one more schema version
		job.SchemaState = model.StateDeleteReorganization
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)

	case model.StateDeleteReorganization:
		// Need to have one more state before completing, due to:
		// - DeleteRanges could possibly start directly after DDL causing
		//   inserts during previous state (DeleteReorg) could insert after the cleanup
		//   leaving data in dropped partitions/indexes that will not be cleaned up again.
		// - Updates in previous state (DeleteReorg) could have duplicate errors, if the row
		//   was deleted or updated in after finish (so here we need to have DeleteOnly index state!
		// And we cannot rollback in this state!

		// Stop double writing to the indexes, only do Deletes!
		// so that previous could do inserts, we do delete and allow second insert for
		// previous state clients!
		for _, index := range tblInfo.Indices {
			isNew, ok := tblInfo.Partition.DDLChangedIndex[index.ID]
			if !ok || isNew {
				continue
			}
			// Old index, should not be visible any longer,
			// but needs to be deleted, in case previous state clients inserts.
			index.State = model.StateDeleteOnly
		}
		failpoint.Inject("reorgPartFail3", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail3"))
			}
		})
		job.SchemaState = model.StatePublic
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)

	case model.StatePublic:
		// Drop the droppingDefinitions and finish the DDL
		// This state is needed for the case where client A sees the schema
		// with version of StateWriteReorg and would not see updates of
		// client B that writes to the new partitions, previously
		// addingDefinitions, since it would not double write to
		// the droppingDefinitions during this time
		// By adding StateDeleteReorg state, client B will write to both
		// the new (previously addingDefinitions) AND droppingDefinitions

		// Register the droppingDefinitions ids for rangeDelete
		// and the addingDefinitions for handling in the updateSchemaVersion
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		newIDs := getPartitionIDsFromDefinitions(partInfo.Definitions)
		statisticsPartInfo := &model.PartitionInfo{Definitions: tblInfo.Partition.AddingDefinitions}
		droppedPartInfo := &model.PartitionInfo{Definitions: tblInfo.Partition.DroppingDefinitions}

		tblInfo.Partition.DroppingDefinitions = nil
		tblInfo.Partition.AddingDefinitions = nil
		tblInfo.Partition.DDLState = model.StateNone
		tblInfo.Partition.DDLAction = model.ActionNone
		tblInfo.Partition.OriginalPartitionIDsOrder = nil

		var dropIndices []*model.IndexInfo
		for _, indexInfo := range tblInfo.Indices {
			if indexInfo.State == model.StateDeleteOnly {
				// Drop the old indexes, see onDropIndex
				indexInfo.State = model.StateNone
				DropIndexColumnFlag(tblInfo, indexInfo)
				RemoveDependentHiddenColumns(tblInfo, indexInfo)
				dropIndices = append(dropIndices, indexInfo)
			}
		}
		// Local indexes is not an issue, since they will be gone with the dropped
		// partitions, but replaced global indexes should be checked!
		for _, indexInfo := range dropIndices {
			removeIndexInfo(tblInfo, indexInfo)
			if indexInfo.Global {
				args.OldGlobalIndexes = append(args.OldGlobalIndexes, model.TableIDIndexID{TableID: tblInfo.ID, IndexID: indexInfo.ID})
			}
		}
		failpoint.Inject("reorgPartFail4", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail4"))
			}
		})
		var oldTblID int64
		if job.Type != model.ActionReorganizePartition {
			// ALTER TABLE ... PARTITION BY
			// REMOVE PARTITIONING
			// Storing the old table ID, used for updating statistics.
			oldTblID = tblInfo.ID
			// TODO: Add concurrent test!
			// TODO: Will this result in big gaps?
			// TODO: How to carrie over AUTO_INCREMENT etc.?
			// Check if they are carried over in ApplyDiff?!?
			autoIDs, err := metaMut.GetAutoIDAccessors(job.SchemaID, tblInfo.ID).Get()
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = metaMut.DropTableOrView(job.SchemaID, tblInfo.ID)
			if err != nil {
				return ver, errors.Trace(err)
			}
			tblInfo.ID = partInfo.NewTableID
			if oldTblID != physicalTableIDs[0] {
				// if partitioned before, then also add the old table ID,
				// otherwise it will be the already included first partition
				physicalTableIDs = append(physicalTableIDs, oldTblID)
			}
			if job.Type == model.ActionRemovePartitioning {
				tblInfo.Partition = nil
			} else {
				// ALTER TABLE ... PARTITION BY
				tblInfo.Partition.ClearReorgIntermediateInfo()
			}
			err = metaMut.GetAutoIDAccessors(job.SchemaID, tblInfo.ID).Put(autoIDs)
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = metaMut.CreateTableOrView(job.SchemaID, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}

		// We need to update the Placement rule bundles with the final partitions.
		_, err = alterTablePartitionBundles(metaMut, tblInfo, nil)
		if err != nil {
			return ver, err
		}

		failpoint.Inject("reorgPartFail5", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail5"))
			}
		})
		failpoint.Inject("updateVersionAndTableInfoErrInStateDeleteReorganization", func() {
			failpoint.Return(ver, errors.New("Injected error in StateDeleteReorganization"))
		})
		args.OldPhysicalTblIDs = physicalTableIDs
		args.NewPartitionIDs = newIDs
		job.SchemaState = model.StateNone
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// How to handle this?
		// Seems to only trigger asynchronous update of statistics.
		// Should it actually be synchronous?
		// Include the old table ID, if changed, which may contain global statistics,
		// so it can be reused for the new (non)partitioned table.
		event, err := newStatsDDLEventForJob(job.Type, oldTblID, tblInfo, statisticsPartInfo, droppedPartInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = asyncNotifyEvent(jobCtx, event, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// A background job will be created to delete old partition data.
		job.FillFinishedArgs(args)

	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	return ver, errors.Trace(err)
}

// newStatsDDLEventForJob creates a util.SchemaChangeEvent for a job.
// It is used for reorganize partition, add partitioning and remove partitioning.
func newStatsDDLEventForJob(
	jobType model.ActionType,
	oldTblID int64,
	tblInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) (*notifier.SchemaChangeEvent, error) {
	var event *notifier.SchemaChangeEvent
	switch jobType {
	case model.ActionReorganizePartition:
		event = notifier.NewReorganizePartitionEvent(
			tblInfo,
			addedPartInfo,
			droppedPartInfo,
		)
	case model.ActionAlterTablePartitioning:
		event = notifier.NewAddPartitioningEvent(
			oldTblID,
			tblInfo,
			addedPartInfo,
		)
	case model.ActionRemovePartitioning:
		event = notifier.NewRemovePartitioningEvent(
			oldTblID,
			tblInfo,
			droppedPartInfo,
		)
	default:
		return nil, errors.Errorf("unknown job type: %s", jobType.String())
	}
	return event, nil
}

func doPartitionReorgWork(w *worker, jobCtx *jobContext, job *model.Job, tbl table.Table, physTblIDs []int64) (done bool, ver int64, err error) {
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		return done, ver, err1
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	reorgTblInfo := tbl.Meta().Clone()
	var elements []*meta.Element
	indices := make([]*model.IndexInfo, 0, len(tbl.Meta().Indices))
	for _, index := range tbl.Meta().Indices {
		if isNew, ok := tbl.Meta().GetPartitionInfo().DDLChangedIndex[index.ID]; ok && !isNew {
			// Skip old replaced indexes, but rebuild all other indexes
			continue
		}
		indices = append(indices, index)
	}
	elements = BuildElements(tbl.Meta().Columns[0], indices)
	reorgTbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, reorgTblInfo)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	partTbl, ok := reorgTbl.(table.PartitionedTable)
	if !ok {
		return false, ver, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfoFromPartitions(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, partTbl, physTblIDs, elements)
	err = w.runReorgJob(jobCtx, reorgInfo, reorgTbl.Meta(), func() (reorgErr error) {
		defer tidbutil.Recover(metrics.LabelDDL, "doPartitionReorgWork",
			func() {
				reorgErr = dbterror.ErrCancelledDDLJob.GenWithStack("reorganize partition for table `%v` panic", tbl.Meta().Name)
			}, false)
		return w.reorgPartitionDataAndIndex(jobCtx, reorgTbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}

		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// If timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.IsTxnRetryableError(err) {
			return false, ver, errors.Trace(err)
		}
		if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.DDLLogger().Warn("reorg partition job failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
				zap.Stringer("job", job), zap.Error(err1))
		}
		logutil.DDLLogger().Warn("reorg partition job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
		// TODO: Test and verify that this returns an error on the ALTER TABLE session.
		ver, err = rollbackReorganizePartitionWithErr(jobCtx, job, err)
		return false, ver, errors.Trace(err)
	}
	return true, ver, err
}

type reorgPartitionWorker struct {
	*backfillCtx
	// Static allocated to limit memory allocations
	rowRecords        []*rowRecord
	rowDecoder        *decoder.RowDecoder
	rowMap            map[int64]types.Datum
	writeColOffsetMap map[int64]int
	maxOffset         int
	reorgedTbl        table.PartitionedTable
}

func newReorgPartitionWorker(i int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*reorgPartitionWorker, error) {
	bCtx, err := newBackfillCtx(i, reorgInfo, reorgInfo.SchemaName, t, jc, metrics.LblReorgPartitionRate, false)
	if err != nil {
		return nil, err
	}
	reorgedTbl, err := tables.GetReorganizedPartitionedTable(t)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pt := t.GetPartitionedTable()
	if pt == nil {
		return nil, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	partColIDs := reorgedTbl.GetPartitionColumnIDs()
	writeColOffsetMap := make(map[int64]int, len(partColIDs))
	maxOffset := 0
	for _, id := range partColIDs {
		var offset int
		for _, col := range pt.Cols() {
			if col.ID == id {
				offset = col.Offset
				break
			}
		}
		writeColOffsetMap[id] = offset
		maxOffset = max(maxOffset, offset)
	}
	return &reorgPartitionWorker{
		backfillCtx:       bCtx,
		rowDecoder:        decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap),
		rowMap:            make(map[int64]types.Datum, len(decodeColMap)),
		writeColOffsetMap: writeColOffsetMap,
		maxOffset:         maxOffset,
		reorgedTbl:        reorgedTbl,
	}, nil
}

func (w *reorgPartitionWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		failpoint.InjectCall("PartitionBackfillData", len(w.rowRecords) > 0)
		// For non-clustered tables, we need to replace the _tidb_rowid handles since
		// there may be duplicates across different partitions, due to EXCHANGE PARTITION.
		// Meaning we need to check here if a record was double written to the new partition,
		// i.e. concurrently written by StateWriteOnly or StateWriteReorganization.
		// and if so, skip it.
		var found map[string][]byte
		lockKey := make([]byte, 0, tablecodec.RecordRowKeyLen)
		lockKey = append(lockKey, handleRange.startKey[:tablecodec.TableSplitKeyLen]...)
		if !w.table.Meta().HasClusteredIndex() && len(w.rowRecords) > 0 {
			failpoint.InjectCall("PartitionBackfillNonClustered", w.rowRecords[0].vals)
			// we must check if old IDs already been written,
			// i.e. double written by StateWriteOnly or StateWriteReorganization.

			// TODO: test how to use PresumeKeyNotExists/NeedConstraintCheckInPrewrite/DO_CONSTRAINT_CHECK
			// to delay the check until commit.
			// And handle commit errors and fall back to this method of checking all keys to see if we need to skip any.
			newKeys := make([]kv.Key, 0, len(w.rowRecords))
			for i := range w.rowRecords {
				newKeys = append(newKeys, w.rowRecords[i].key)
			}
			found, err = kv.BatchGetValue(ctx, txn, newKeys)
			if err != nil {
				return errors.Trace(err)
			}

			// TODO: Add test that kills (like `kill -9`) the currently running
			// ddl owner, to see how it handles re-running this backfill when some batches has
			// committed and reorgInfo has not been updated, so it needs to redo some batches.
		}
		tmpRow := make([]types.Datum, len(w.reorgedTbl.Cols()))

		for _, prr := range w.rowRecords {
			taskCtx.scanCount++
			key := prr.key
			lockKey = lockKey[:tablecodec.TableSplitKeyLen]
			lockKey = append(lockKey, key[tablecodec.TableSplitKeyLen:]...)
			// Lock the *old* key, since there can still be concurrent update happening on
			// the rows from fetchRowColVals(). If we cannot lock the keys in this
			// transaction and succeed when committing, then another transaction did update
			// the same key, and we will fail and retry. When retrying, this key would be found
			// through BatchGet and skipped.
			// TODO: would it help to accumulate the keys in a slice and then only call this once?
			err = txn.LockKeys(context.Background(), new(kv.LockCtx), lockKey)
			if err != nil {
				return errors.Trace(err)
			}

			if vals, ok := found[string(key)]; ok {
				if len(vals) == len(prr.vals) && bytes.Equal(vals, prr.vals) {
					// Already backfilled or double written earlier by concurrent DML
					continue
				}
				// Not same row, due to earlier EXCHANGE PARTITION.
				// Update the current read row by Remove it and Add it back (which will give it a new _tidb_rowid)
				// which then also will be used as unique id in the new partition.
				var h kv.Handle
				var currPartID int64
				currPartID, h, err = tablecodec.DecodeRecordKey(lockKey)
				if err != nil {
					return errors.Trace(err)
				}
				_, err = w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, h, prr.vals, w.loc, w.rowMap)
				if err != nil {
					return errors.Trace(err)
				}
				for _, col := range w.table.WritableCols() {
					d, ok := w.rowMap[col.ID]
					if !ok {
						return dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
					}
					tmpRow[col.Offset] = d
				}
				// Use RemoveRecord/AddRecord to keep the indexes in-sync!
				pt := w.table.GetPartitionedTable().GetPartition(currPartID)
				err = pt.RemoveRecord(w.tblCtx, txn, h, tmpRow)
				if err != nil {
					return errors.Trace(err)
				}
				h, err = pt.AddRecord(w.tblCtx, txn, tmpRow)
				if err != nil {
					return errors.Trace(err)
				}
				w.cleanRowMap()
				// tablecodec.prefixLen is not exported, but is just TableSplitKeyLen + 2 ("_r")
				key = tablecodec.EncodeRecordKey(key[:tablecodec.TableSplitKeyLen+2], h)
				// OK to only do txn.Set() for the new partition, and defer creating the indexes,
				// since any DML changes the record it will also update or create the indexes,
				// by doing RemoveRecord+UpdateRecord
			}
			err = txn.Set(key, prr.vals)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "BackfillData", 3000)

	return
}

func (w *reorgPartitionWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) (kv.Key, bool, error) {
	w.rowRecords = w.rowRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	sysTZ := w.loc

	tmpRow := make([]types.Datum, len(w.reorgedTbl.Cols()))
	var lastAccessedHandle kv.Key
	oprStartTime := startTime
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, w.table.RecordPrefix(), txn.StartTS(), taskRange.startKey, taskRange.endKey,
		func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in reorgPartitionWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.rowRecords) >= w.batchCnt {
				return false, nil
			}

			_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, handle, rawRow, sysTZ, w.rowMap)
			if err != nil {
				return false, errors.Trace(err)
			}

			// Set all partitioning columns and calculate which partition to write to
			for colID, offset := range w.writeColOffsetMap {
				d, ok := w.rowMap[colID]
				if !ok {
					return false, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
				}
				tmpRow[offset] = d
			}
			p, err := w.reorgedTbl.GetPartitionByRow(w.exprCtx.GetEvalCtx(), tmpRow)
			if err != nil {
				return false, errors.Trace(err)
			}
			newKey := tablecodec.EncodeTablePrefix(p.GetPhysicalID())
			newKey = append(newKey, recordKey[tablecodec.TableSplitKeyLen:]...)
			w.rowRecords = append(w.rowRecords, &rowRecord{key: newKey, vals: rawRow})

			w.cleanRowMap()
			lastAccessedHandle = recordKey
			if recordKey.Cmp(taskRange.endKey) == 0 {
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.rowRecords) == 0 {
		taskDone = true
	}

	logutil.DDLLogger().Debug("txn fetches handle info",
		zap.Uint64("txnStartTS", txn.StartTS()),
		zap.Stringer("taskRange", &taskRange),
		zap.Duration("takeTime", time.Since(startTime)))
	return getNextHandleKey(taskRange, taskDone, lastAccessedHandle), taskDone, errors.Trace(err)
}

func (w *reorgPartitionWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

func (w *reorgPartitionWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (*reorgPartitionWorker) String() string {
	return typeReorgPartitionWorker.String()
}

func (w *reorgPartitionWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

func (w *worker) reorgPartitionDataAndIndex(
	jobCtx *jobContext,
	t table.Table,
	reorgInfo *reorgInfo,
) (err error) {
	// First copy all table data to the new AddingDefinitions partitions
	// from each of the DroppingDefinitions partitions.
	// Then create all indexes on the AddingDefinitions partitions,
	// both new local and new global indexes
	// And last update new global indexes from the non-touched partitions
	// Note it is hard to update global indexes in-place due to:
	//   - Transactions on different TiDB nodes/domains may see different states of the table/partitions
	//   - We cannot have multiple partition ids for a unique index entry.

	// Copy the data from the DroppingDefinitions to the AddingDefinitions
	if bytes.Equal(reorgInfo.currElement.TypeKey, meta.ColumnElementKey) {
		err = w.updatePhysicalTableRow(jobCtx.stepCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		if len(reorgInfo.elements) <= 1 {
			// No indexes to (re)create, all done!
			return nil
		}
	}

	failpoint.Inject("reorgPartitionAfterDataCopy", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test in reorgPartitionAfterDataCopy")
		}
	})

	if !bytes.Equal(reorgInfo.currElement.TypeKey, meta.IndexElementKey) {
		// row data has been copied, now proceed with creating the indexes
		// on the new AddingDefinitions partitions
		reorgInfo.PhysicalTableID = t.Meta().Partition.AddingDefinitions[0].ID
		reorgInfo.currElement = reorgInfo.elements[1]
		var physTbl table.PhysicalTable
		if tbl, ok := t.(table.PartitionedTable); ok {
			physTbl = tbl.GetPartition(reorgInfo.PhysicalTableID)
		} else if tbl, ok := t.(table.PhysicalTable); ok {
			// This may be used when partitioning a non-partitioned table
			physTbl = tbl
		}
		// Get the original start handle and end handle.
		currentVer, err := getValidCurrentVersion(reorgInfo.jobCtx.store)
		if err != nil {
			return errors.Trace(err)
		}
		startHandle, endHandle, err := getTableRange(reorgInfo.NewJobContext(), reorgInfo.jobCtx.store, physTbl, currentVer.Ver, reorgInfo.Job.Priority)
		if err != nil {
			return errors.Trace(err)
		}

		// Always (re)start with the full PhysicalTable range
		reorgInfo.StartKey, reorgInfo.EndKey = startHandle, endHandle

		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("update column and indexes",
			zap.Int64("jobID", reorgInfo.Job.ID),
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.Int64("partitionTableId", physTbl.GetPhysicalID()),
			zap.String("startHandle", hex.EncodeToString(reorgInfo.StartKey)),
			zap.String("endHandle", hex.EncodeToString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
	}

	pi := t.Meta().GetPartitionInfo()
	if _, err = findNextPartitionID(reorgInfo.PhysicalTableID, pi.AddingDefinitions); err == nil {
		// Now build all the indexes in the new partitions.
		err = w.addTableIndex(jobCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		// All indexes are up-to-date for new partitions,
		// now we only need to add the existing non-touched partitions
		// to the global indexes
		reorgInfo.elements = reorgInfo.elements[:0]
		for _, indexInfo := range t.Meta().Indices {
			if indexInfo.Global && indexInfo.State == model.StateWriteReorganization {
				reorgInfo.elements = append(reorgInfo.elements, &meta.Element{ID: indexInfo.ID, TypeKey: meta.IndexElementKey})
			}
		}
		if len(reorgInfo.elements) == 0 {
			reorgInfo.PhysicalTableID = 0
		}
		if reorgInfo.PhysicalTableID != 0 {
			reorgInfo.currElement = reorgInfo.elements[0]
			// Find the first non-touched partition: one that is NOT in
			// AddingDefinitions (newly created, already indexed above) and
			// NOT in DroppingDefinitions (being removed).
			pid := int64(0)
			for _, def := range pi.Definitions {
				if _, addErr := findNextPartitionID(def.ID, pi.AddingDefinitions); addErr == nil {
					continue
				}
				if _, dropErr := findNextPartitionID(def.ID, pi.DroppingDefinitions); dropErr == nil {
					continue
				}
				pid = def.ID
				break
			}

			// if pid == 0 => All partitions will be dropped/added, nothing more to add to global indexes.
			reorgInfo.PhysicalTableID = pid
		}
		if reorgInfo.PhysicalTableID != 0 {
			var physTbl table.PhysicalTable
			if tbl, ok := t.(table.PartitionedTable); ok {
				physTbl = tbl.GetPartition(reorgInfo.PhysicalTableID)
			} else if tbl, ok := t.(table.PhysicalTable); ok {
				// This may be used when partitioning a non-partitioned table
				physTbl = tbl
			}
			// Get the original start handle and end handle.
			currentVer, err := getValidCurrentVersion(reorgInfo.jobCtx.store)
			if err != nil {
				return errors.Trace(err)
			}
			startHandle, endHandle, err := getTableRange(reorgInfo.NewJobContext(), reorgInfo.jobCtx.store, physTbl, currentVer.Ver, reorgInfo.Job.Priority)
			if err != nil {
				return errors.Trace(err)
			}

			// Always (re)start with the full PhysicalTable range
			reorgInfo.StartKey, reorgInfo.EndKey = startHandle, endHandle
		}

		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("update column and indexes",
			zap.Int64("jobID", reorgInfo.Job.ID),
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.String("startHandle", hex.EncodeToString(reorgInfo.StartKey)),
			zap.String("endHandle", hex.EncodeToString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
	}
	if _, err = findNextNonTouchedPartitionID(reorgInfo.PhysicalTableID, pi); err == nil {
		err = w.addTableIndex(jobCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		reorgInfo.PhysicalTableID = 0
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("Non touched partitions done",
			zap.Int64("jobID", reorgInfo.Job.ID), zap.Error(err))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func getNewGlobal(partInfo *model.PartitionInfo, idx *model.IndexInfo) bool {
	for _, newIdx := range partInfo.DDLUpdateIndexes {
		if strings.EqualFold(idx.Name.L, newIdx.IndexName) {
			return newIdx.Global
		}
	}
	return idx.Global
}

func getReorgPartitionInfo(t *meta.Mutator, job *model.Job, args *model.TablePartitionArgs) (*model.TableInfo, []string, *model.PartitionInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	partNames, partInfo := args.PartNames, args.PartInfo
	if job.SchemaState == model.StateNone {
		if tblInfo.Partition != nil {
			tblInfo.Partition.NewTableID = partInfo.NewTableID
			tblInfo.Partition.DDLType = partInfo.Type
			tblInfo.Partition.DDLExpr = partInfo.Expr
			tblInfo.Partition.DDLColumns = partInfo.Columns
		} else {
			tblInfo.Partition = getPartitionInfoTypeNone()
			tblInfo.Partition.NewTableID = partInfo.NewTableID
			tblInfo.Partition.Definitions[0].ID = tblInfo.ID
			tblInfo.Partition.DDLType = partInfo.Type
			tblInfo.Partition.DDLExpr = partInfo.Expr
			tblInfo.Partition.DDLColumns = partInfo.Columns
		}
	}
	return tblInfo, partNames, partInfo, nil
}

// onReorganizePartition reorganized the partitioning of a table including its indexes.
// ALTER TABLE t REORGANIZE PARTITION p0 [, p1...] INTO (PARTITION p0 ...)
//
//	Takes one set of partitions and copies the data to a newly defined set of partitions
//
// ALTER TABLE t REMOVE PARTITIONING
//
//	Makes a partitioned table non-partitioned, by first collapsing all partitions into a
//	single partition and then converts that partition to a non-partitioned table
//
// ALTER TABLE t PARTITION BY ...
//
//	Changes the partitioning to the newly defined partitioning type and definitions,
//	works for both partitioned and non-partitioned tables.
//	If the table is non-partitioned, then it will first convert it to a partitioned
//	table with a single partition, i.e. the full table as a single partition.
//
// job.SchemaState goes through the following SchemaState(s):
// StateNone -> StateDeleteOnly -> StateWriteOnly -> StateWriteReorganization
// -> StateDeleteOrganization -> StatePublic -> Done
// There are more details embedded in the implementation, but the high level changes are:
//
// StateNone -> StateDeleteOnly:
//
//	Various checks and validations.
//	Add possible new unique/global indexes. They share the same state as job.SchemaState
//	until end of StateWriteReorganization -> StateDeleteReorganization.
//	Set DroppingDefinitions and AddingDefinitions.
//	So both the new partitions and new indexes will be included in new delete/update DML.
//
// StateDeleteOnly -> StateWriteOnly:
//
//	So both the new partitions and new indexes will be included also in update/insert DML.
//
// StateWriteOnly -> StateWriteReorganization:
//
//	To make sure that when we are reorganizing the data,
//	both the old and new partitions/indexes will be updated.
//
// StateWriteReorganization -> StateDeleteOrganization:
//
//	Here is where all data is reorganized, both partitions and indexes.
//	It copies all data from the old set of partitions into the new set of partitions,
//	and creates the local indexes on the new set of partitions,
//	and if new unique indexes are added, it also updates them with the rest of data from
//	the non-touched partitions.
//	For indexes that are to be replaced with new ones (old/new global index),
//	mark the old indexes as StateWriteOnly and new ones as StatePublic
//	Finally make the table visible with the new partition definitions.
//	I.e. in this state clients will read from the old set of partitions,
//	and next state will read the new set of partitions in StateDeleteReorganization.
//
// StateDeleteOrganization -> StatePublic:
//
//	Now we mark all replaced (old) indexes as StateDeleteOnly
//	in case DeleteRange would be called directly after the DDL,
//	this way there will be no orphan records inserted after DeleteRanges
//	has cleaned up the old partitions and old global indexes.
//
// StatePublic -> Done:
//
//	Now all heavy lifting is done, and we just need to finalize and drop things, while still doing
//	double writes, since previous state sees the old partitions/indexes.
//	Remove the old indexes and old partitions from the TableInfo.
//	Add the old indexes and old partitions to the queue for later cleanup (see delete_range.go).
//	Queue new partitions for statistics update.
//	if ALTER TABLE t PARTITION BY/REMOVE PARTITIONING:
//	  Recreate the table with the new TableID, by DropTableOrView+CreateTableOrView
//
// Done:
//
//	Everything now looks as it should, no memory of old partitions/indexes,
//	and no more double writing, since the previous state is only using the new partitions/indexes.
//
// Note: Special handling is also required in tables.newPartitionedTable(),
// to get per partition indexes in the right state.

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
	"cmp"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

func onDropIndex(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, allIndexInfos, ifExists, err := checkDropIndex(jobCtx.infoCache, jobCtx.metaMut, job)
	if err != nil {
		if ifExists && dbterror.ErrCantDropFieldOrKey.Equal(err) {
			job.Warning = toTError(err)
			job.State = model.JobStateDone
			return ver, nil
		}
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}

	if job.MultiSchemaInfo != nil && !job.IsRollingback() && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		job.SchemaState = allIndexInfos[0].State
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}

	originalState := allIndexInfos[0].State
	switch allIndexInfos[0].State {
	case model.StatePublic:
		// public -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		// delete only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteReorganization
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteReorganization)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		isColumnarIndex := false
		indexIDs := make([]int64, 0, len(allIndexInfos))
		for _, indexInfo := range allIndexInfos {
			if indexInfo.IsColumnarIndex() {
				isColumnarIndex = true
			}
			indexInfo.State = model.StateNone
			// Set column index flag.
			DropIndexColumnFlag(tblInfo, indexInfo)
			RemoveDependentHiddenColumns(tblInfo, indexInfo)
			removeIndexInfo(tblInfo, indexInfo)
			indexIDs = append(indexIDs, indexInfo.ID)
		}

		failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
			//nolint:forcetypeassert
			if val.(bool) {
				panic("panic test in cancelling add index")
			}
		})

		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		if isColumnarIndex {
			// Send sync schema notification to TiFlash.
			if err := infosync.SyncTiFlashTableSchema(jobCtx.stepCtx, tblInfo.ID); err != nil {
				logutil.DDLLogger().Warn("run drop column index but syncing TiFlash schema failed", zap.Error(err))
			}
		}

		// Finish this job.
		if job.IsRollingback() {
			dropArgs, err := model.GetFinishedModifyIndexArgs(job)
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}

			// Convert drop index args to finished add index args again to finish add index job.
			// Only rolled back add index jobs will get here, since drop index jobs can only be cancelled, not rolled back.
			addIndexArgs := &model.ModifyIndexArgs{
				PartitionIDs: dropArgs.PartitionIDs,
				OpType:       model.OpAddIndex,
			}
			for i, indexID := range indexIDs {
				addIndexArgs.IndexArgs = append(addIndexArgs.IndexArgs,
					&model.IndexArg{
						IndexID: indexID,
						IfExist: dropArgs.IndexArgs[i].IfExist,
					})
			}
			job.FillFinishedArgs(addIndexArgs)
		} else {
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			// Global index key has t{tableID}_ prefix.
			// Assign partitionIDs empty to guarantee correct prefix in insertJobIntoDeleteRangeTable.
			dropArgs, err := model.GetDropIndexArgs(job)
			dropArgs.OpType = model.OpDropIndex
			if err != nil {
				return ver, errors.Trace(err)
			}
			dropArgs.IndexArgs[0].IndexID = indexIDs[0]
			dropArgs.IndexArgs[0].IsColumnar = allIndexInfos[0].IsColumnarIndex()
			dropArgs.IndexArgs[0].ColumnarIndexType = allIndexInfos[0].GetColumnarIndexType()
			if !allIndexInfos[0].Global {
				dropArgs.PartitionIDs = getPartitionIDs(tblInfo)
			}
			job.FillFinishedArgs(dropArgs)
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", allIndexInfos[0].State))
	}
	job.SchemaState = allIndexInfos[0].State
	return ver, errors.Trace(err)
}

// RemoveDependentHiddenColumns removes hidden columns by the indexInfo.
func RemoveDependentHiddenColumns(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	hiddenColOffs := make([]int, 0)
	for _, indexColumn := range idxInfo.Columns {
		col := tblInfo.Columns[indexColumn.Offset]
		if col.Hidden {
			hiddenColOffs = append(hiddenColOffs, col.Offset)
		}
	}
	// Sort the offset in descending order.
	slices.SortFunc(hiddenColOffs, func(a, b int) int { return cmp.Compare(b, a) })
	// Move all the dependent hidden columns to the end.
	endOffset := len(tblInfo.Columns) - 1
	for _, offset := range hiddenColOffs {
		tblInfo.MoveColumnInfo(offset, endOffset)
	}
	tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(hiddenColOffs)]
}

func removeIndexInfo(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	indices := tblInfo.Indices
	offset := -1
	for i, idx := range indices {
		if idxInfo.ID == idx.ID {
			offset = i
			break
		}
	}
	if offset == -1 {
		// The target index has been removed.
		return
	}
	// Remove the target index.
	tblInfo.Indices = slices.Delete(tblInfo.Indices, offset, offset+1)
}

func checkDropIndex(infoCache *infoschema.InfoCache, t *meta.Mutator, job *model.Job) (*model.TableInfo, []*model.IndexInfo, bool /* ifExists */, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	args, err := model.GetDropIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, false, errors.Trace(err)
	}

	indexInfos := make([]*model.IndexInfo, 0, len(args.IndexArgs))
	for _, idxArg := range args.IndexArgs {
		indexInfo := tblInfo.FindIndexByName(idxArg.IndexName.L)
		if indexInfo == nil {
			job.State = model.JobStateCancelled
			return nil, nil, idxArg.IfExist, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", idxArg.IndexName)
		}

		// Check that drop primary index will not cause invisible implicit primary index.
		if err := checkInvisibleIndexesOnPK(tblInfo, []*model.IndexInfo{indexInfo}, job); err != nil {
			job.State = model.JobStateCancelled
			return nil, nil, false, errors.Trace(err)
		}

		// Double check for drop index needed in foreign key.
		if err := checkIndexNeededInForeignKeyInOwner(infoCache, job, job.SchemaName, tblInfo, indexInfo); err != nil {
			return nil, nil, false, errors.Trace(err)
		}
		indexInfos = append(indexInfos, indexInfo)
	}
	return tblInfo, indexInfos, false, nil
}

func checkInvisibleIndexesOnPK(tblInfo *model.TableInfo, indexInfos []*model.IndexInfo, job *model.Job) error {
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, oidx := range tblInfo.Indices {
		needAppend := true
		for _, idx := range indexInfos {
			if idx.Name.L == oidx.Name.L {
				needAppend = false
				break
			}
		}
		if needAppend {
			newIndices = append(newIndices, oidx)
		}
	}
	newTbl := tblInfo.Clone()
	newTbl.Indices = newIndices
	if err := checkInvisibleIndexOnPK(newTbl); err != nil {
		job.State = model.JobStateCancelled
		return err
	}

	return nil
}

func checkRenameIndex(t *meta.Mutator, job *model.Job) (tblInfo *model.TableInfo, from, to ast.CIStr, err error) {
	schemaID := job.SchemaID
	tblInfo, err = GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, from, to, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	from, to = args.GetRenameIndexes()

	// Double check. See function `RenameIndex` in executor.go
	duplicate, err := ValidateRenameIndex(from, to, tblInfo)
	if duplicate {
		return nil, from, to, nil
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	return tblInfo, from, to, errors.Trace(err)
}

func checkAlterIndexVisibility(t *meta.Mutator, job *model.Job) (*model.TableInfo, ast.CIStr, bool, error) {
	var (
		indexName ast.CIStr
		invisible bool
	)

	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, indexName, invisible, errors.Trace(err)
	}

	args, err := model.GetAlterIndexVisibilityArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	indexName, invisible = args.IndexName, args.Invisible

	skip, err := validateAlterIndexVisibility(nil, indexName, invisible, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	if skip {
		job.State = model.JobStateDone
		return nil, indexName, invisible, nil
	}
	return tblInfo, indexName, invisible, nil
}



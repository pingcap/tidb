// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/dbterror"
)

// Reorg Composite index name design:
//
//	CIStr {
//	   O: _CIdx$_[origin name]
//	   L: _CIdx$_[origin name lower]_[origin index ID]
//	}
//
// This design will generate a different value in CIStr, so no one can generate this name from
// client.
const (
	tempCompositeIndexPrefix = "_CIdx$_"
)

// Util functions

func getOrCreateTempCompositeIndexes(tblInfo *model.TableInfo, colInfos []*model.ColumnInfo, idxInfos []*model.IndexInfo) ([]*model.IndexInfo, error) {
	ctidxInfos := getTempCompositeIndexes(tblInfo, idxInfos)
	if len(ctidxInfos) > 0 {
		return ctidxInfos, nil
	}
	ret := make([]*model.IndexInfo, 0)
	for _, idxInfo := range idxInfos {
		ctidxName := model.CIStr{
			O: fmt.Sprintf("%s%s", tempCompositeIndexPrefix, idxInfo.Name.O),
			L: fmt.Sprintf("%s%s_%d", tempCompositeIndexPrefix, idxInfo.Name.L, idxInfo.ID),
		}
		ctidxInfo := tblInfo.FindIndexByName(ctidxName.L)
		if ctidxInfo != nil {
			// If we have the index just ignore it.
			continue
		}
		nctidxInfo := buildCompositeIndexInfo(ctidxName, idxInfo, colInfos, model.StateNone)
		if nctidxInfo == nil {
			// No need to create temp index and reorg this index
			continue
		}
		nctidxInfo.ID = AllocateIndexID(tblInfo)
		tblInfo.Indices = append(tblInfo.Indices, nctidxInfo)
		ret = append(ret, nctidxInfo)
	}
	return ret, nil
}

func buildCompositeIndexInfo(idxName model.CIStr, idxInfo *model.IndexInfo, colInfos []*model.ColumnInfo, state model.SchemaState) *model.IndexInfo {
	// For now we do not need to check index name length
	idxColumns := make([]*model.IndexColumn, 0)
	// Just remove dropped column from origin index columns
	// We do not need to check the columns here.
	for _, col := range idxInfo.Columns {
		if inColumnInfos(col, colInfos) {
			continue
		}
		idxColumns = append(idxColumns, col)
	}
	if len(idxColumns) == 0 {
		// Index no columns, we should just drop this index and no need to reorg.
		return nil
	}
	// Except Name, Columns, State, just copy other fields from origin index info.
	nidxInfo := idxInfo.Clone()
	nidxInfo.Name = idxName
	nidxInfo.Columns = idxColumns
	nidxInfo.State = state
	return nidxInfo
}

func inColumnInfos(col *model.IndexColumn, colInfos []*model.ColumnInfo) bool {
	for _, colInfo := range colInfos {
		if col.Name.L == colInfo.Name.L {
			return true
		}
	}
	return false
}

func getTempCompositeIndexes(tblInfo *model.TableInfo, idxInfos []*model.IndexInfo) []*model.IndexInfo {
	ret := make([]*model.IndexInfo, 0)
	for _, cidxInfo := range idxInfos {
		ctidxOName := fmt.Sprintf("%s%s", tempCompositeIndexPrefix, cidxInfo.Name.O)
		ctidxLName := fmt.Sprintf("%s%s_%d", tempCompositeIndexPrefix, cidxInfo.Name.L, cidxInfo.ID)
		for _, idx := range tblInfo.Indices {
			if idx.Name.L == ctidxLName && idx.Name.O == ctidxOName {
				ret = append(ret, idx)
				break
			}
		}
	}
	return ret
}

func renameTempCompositeIdxToOrigin(idx *model.IndexInfo, cidxInfos []*model.IndexInfo) model.CIStr {
	for _, cidxInfo := range cidxInfos {
		ctidxOName := fmt.Sprintf("%s%s", tempCompositeIndexPrefix, cidxInfo.Name.O)
		ctidxLName := fmt.Sprintf("%s%s_%d", tempCompositeIndexPrefix, cidxInfo.Name.L, cidxInfo.ID)
		if ctidxLName == idx.Name.L && idx.Name.O == ctidxOName {
			return cidxInfo.Name
		}
	}
	// We should not hit here
	return model.NewCIStr(trimTempCompositeIdxPrefix(idx.Name.O))
}

func trimTempCompositeIdxPrefix(name string) string {
	return strings.TrimPrefix(name, tempCompositeIndexPrefix)
}

func listIndexesWithColumn(colName string, indices []*model.IndexInfo) ([]*model.IndexInfo, []*model.IndexInfo) {
	singleIndexes := make([]*model.IndexInfo, 0)
	compositeIndexes := make([]*model.IndexInfo, 0)
	for _, indexInfo := range indices {
		if len(indexInfo.Columns) == 1 && colName == indexInfo.Columns[0].Name.L {
			singleIndexes = append(singleIndexes, indexInfo)
		} else if len(indexInfo.Columns) > 1 {
			for _, col := range indexInfo.Columns {
				if colName == col.Name.L {
					compositeIndexes = append(compositeIndexes, indexInfo)
					break
				}
			}
		}
	}
	// We should sort compositeIndexes to make sure this list not changed by other DDL statements.
	// If this order changed, reorg job may got some problem on reorg multi indices.
	sort.Slice(compositeIndexes, func(i, j int) bool {
		return compositeIndexes[i].ID < compositeIndexes[j].ID
	})
	return singleIndexes, compositeIndexes
}

// drop column process functions

// dropColumnWithoutCompositeIndex will run the drop column process and the column not covered by composite index
func dropColumnWithoutCompositeIndex(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, colInfo *model.ColumnInfo, idxInfos []*model.IndexInfo, ifExists bool) (ver int64, err error) {
	originalState := colInfo.State
	switch colInfo.State {
	case model.StatePublic:
		// public -> write only
		colInfo.State = model.StateWriteOnly
		setIndicesState(idxInfos, model.StateWriteOnly)
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		err = checkDropColumnForStatePublic(colInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		colInfo.State = model.StateDeleteOnly
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		if len(idxInfos) > 0 {
			newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
			for _, idx := range tblInfo.Indices {
				if !indexInfoContains(idx.ID, idxInfos) {
					newIndices = append(newIndices, idx)
				}
			}
			tblInfo.Indices = newIndices
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.Args = append(job.Args, indexInfosToIDList(idxInfos))
	case model.StateDeleteOnly:
		// delete only -> reorganization
		colInfo.State = model.StateDeleteReorganization
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		colInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		} else {
			// We should set related index IDs for job
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, getPartitionIDs(tblInfo))
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLJob.GenWithStackByArgs("table", tblInfo.State))
	}
	job.SchemaState = colInfo.State
	return ver, errors.Trace(err)
}

/*
 * Drop column with composite index job status:
 *
 * Job Start                    RollbackDone (Job Done) -> DeleteRange (for temp composite indexes)
 *   |                             ^
 *   v                             |
 * CreateIndexDeleteOnly --*    DeleteReorganization
 *   |                     |       ^
 *   v                     |       |
 * CreateIndexWriteOnly ---+--> DeleteOnly
 *   |                     |
 *   v                     |
 * WriteReorganization ----*
 *   |
 *   v
 * WriteOnly (Cannot Rollback)
 *   |
 *   v
 * DeleteOnly (Cannot Rollback)
 *   |
 *   v
 * DeleteReorganization (Cannot Rollback)
 *   |
 *   v
 * None (Job Done) -> DeleteRange (for related indexes)
 */

// dropColumnWithoutCompositeIndex will run the drop column process and the column is covered by composite index
func dropColumnWithCompositeIndex(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, colInfo *model.ColumnInfo, idxInfos []*model.IndexInfo, cidxInfos []*model.IndexInfo, ifExists bool) (ver int64, err error) {
	colOriginalState := colInfo.State
	if job.IsRollingback() {
		// Handle rolling back
		ctidxInfos := getTempCompositeIndexes(tblInfo, cidxInfos)
		if len(ctidxInfos) == 0 {
			// No indices need to be dropped just finish job
			// Actually it will not run here, just for defense.
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, []int64{}, getPartitionIDs(tblInfo))
			return ver, nil
		}
		ver, err = doDropIndexes(d, t, job, tblInfo, ctidxInfos)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Create new composite index and name it to temp index
	if colOriginalState == model.StatePublic {
		ctidxInfos, err := getOrCreateTempCompositeIndexes(tblInfo, []*model.ColumnInfo{colInfo}, cidxInfos)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Create temp composite index
		schemaID := job.SchemaID
		fallThrough := false
		idxOriginalState := ctidxInfos[0].State
		switch ctidxInfos[0].State {
		case model.StateNone:
			// none -> delete only
			reorgTp := pickBackfillType(w, job)
			if reorgTp.NeedMergeProcess() {
				for _, idxInfo := range ctidxInfos {
					// Increase telemetryAddIndexIngestUsage
					telemetryAddIndexIngestUsage.Inc()
					idxInfo.BackfillState = model.BackfillStateRunning
				}
			}
			setIndicesState(ctidxInfos, model.StateDeleteOnly)
			ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, idxOriginalState != ctidxInfos[0].State)
			if err != nil {
				return ver, errors.Trace(err)
			}
			job.SchemaState = model.StateCreateIndexDeleteOnly
		case model.StateDeleteOnly:
			// delete only -> write only
			setIndicesState(ctidxInfos, model.StateWriteOnly)
			ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, idxOriginalState != ctidxInfos[0].State)
			if err != nil {
				return ver, errors.Trace(err)
			}
			job.SchemaState = model.StateCreateIndexWriteOnly
		case model.StateWriteOnly:
			// write only -> reorg
			setIndicesState(ctidxInfos, model.StateWriteReorganization)
			ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, idxOriginalState != ctidxInfos[0].State)
			if err != nil {
				return ver, errors.Trace(err)
			}
			// Update need reorg index list
			ctidxIDList := indexInfosToIDList(ctidxInfos)
			job.SetNeedReorgIndexIDs(ctidxIDList)
			job.CurrentReorgIndexIdx = 0
			// Initialize SnapshotVer to 0 for later reorganization check.
			job.SnapshotVer = 0
			job.SchemaState = model.StateWriteReorganization
			if job.MultiSchemaInfo == nil {
				initDistReorg(job.ReorgMeta)
			}
		case model.StateWriteReorganization:
			// reorganization -> public
			tbl, err := getTable(d.store, schemaID, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}

			for {
				var (
					done         bool
					allDone      bool
					reorgIdxInfo *model.IndexInfo
					originArgs   []interface{}
				)
				// Get current reorging index
				reorgIdxInfo, allDone = getCurrentReorgIndex(job, ctidxInfos)
				if allDone {
					// All indexes are reorged
					break
				}
				originArgs = job.Args

				// Do reorg jobs
				if job.MultiSchemaInfo != nil {
					done, ver, err = doReorgWorkForCreateIndexMultiSchema(w, d, t, job, tbl, reorgIdxInfo)
				} else {
					if job.ReorgMeta.IsDistReorg {
						done, ver, err = doReorgWorkForCreateIndexWithDistReorg(w, d, t, job, tbl, reorgIdxInfo)
					} else {
						done, ver, err = doReorgWorkForCreateIndex(w, d, t, job, tbl, reorgIdxInfo)
					}
				}

				if !done {
					// Current reorg not finish
					if err != nil {
						if kv.ErrKeyExists.Equal(err) {
							err = kv.ErrKeyExists.GenWithStackByArgs("", trimTempCompositeIdxPrefix(reorgIdxInfo.Name.O))
						}
						// If reorg job got error the origin process will change job.Args for create index job support.
						// So change the job.Args back to origin is required
						job.Args = originArgs
					}
					return ver, err
				} else {
					// Current reorg finish
					if err != nil {
						return ver, err
					}
					// Update current reorg job to done and check if has next index need reorg
					allDone = updateReorgDone(job, reorgIdxInfo)
					if allDone {
						// All indexes are reorged
						break
					} else {
						return ver, err
					}
				}
			}

			// Set column index flag.
			for _, idxInfo := range ctidxInfos {
				AddIndexColumnFlag(tblInfo, idxInfo)
			}

			ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, idxOriginalState != ctidxInfos[0].State)
			if err != nil {
				return ver, errors.Trace(err)
			}
			// set job status to StatePublic and fall-through then do the column drop process
			job.SchemaState = model.StatePublic
			fallThrough = true
		default:
			err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", tblInfo.State)
		}

		if !fallThrough {
			return ver, errors.Trace(err)
		}
	}

	// Drop column
	switch colInfo.State {
	case model.StatePublic:
		// public -> write only
		colInfo.State = model.StateWriteOnly
		setIndicesState(idxInfos, model.StateWriteOnly)
		setIndicesState(cidxInfos, model.StateWriteOnly)
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		err = checkDropColumnForStatePublic(colInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, colOriginalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		colInfo.State = model.StateDeleteOnly
		setIndicesState(idxInfos, model.StateDeleteOnly)
		setIndicesState(cidxInfos, model.StateDeleteOnly)
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, colOriginalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		// delete only -> reorganization
		colInfo.State = model.StateDeleteReorganization
		setIndicesState(idxInfos, model.StateDeleteReorganization)
		setIndicesState(cidxInfos, model.StateDeleteReorganization)
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, colOriginalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		if len(idxInfos) > 0 {
			newIndexes := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
			for _, idx := range tblInfo.Indices {
				if !indexInfoContains(idx.ID, idxInfos) {
					newIndexes = append(newIndexes, idx)
				}
			}
			tblInfo.Indices = newIndexes
		}
		indexIDs := indexInfosToIDList(idxInfos)

		if len(cidxInfos) > 0 {
			ctidxInfos := getTempCompositeIndexes(tblInfo, cidxInfos)
			// Drop origin indexes
			newIndexes := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
			for _, idx := range tblInfo.Indices {
				if !indexInfoContains(idx.ID, cidxInfos) {
					newIndexes = append(newIndexes, idx)
				}
			}
			tblInfo.Indices = newIndexes

			for _, idx := range ctidxInfos {
				// Rename temp index
				idx.Name = renameTempCompositeIdxToOrigin(idx, cidxInfos)
				// Set state to public
				idx.State = model.StatePublic
			}
			indexIDs = append(indexIDs, indexInfosToIDList(cidxInfos)...)
		}

		// All reorganization jobs are done, drop this column.
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		colInfo.State = model.StateNone

		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, colOriginalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		} else {
			// We should set related index IDs for job
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLJob.GenWithStackByArgs("table", tblInfo.State))
	}
	job.SchemaState = colInfo.State
	return ver, errors.Trace(err)
}

// getCurrentReorgIndex returns the current reorging index, if current reorging index not in
// jobs reorg index array, it will treat it as all index is reorged.
func getCurrentReorgIndex(job *model.Job, idxInfos []*model.IndexInfo) (*model.IndexInfo, bool) {
	reorgIdxIDs := job.GetNeedReorgIndexIDs()
	pos := job.CurrentReorgIndexIdx
	if pos >= len(reorgIdxIDs) {
		// Nothing need to reorg just return all done
		return nil, true
	}
	idxID := reorgIdxIDs[pos]
	for _, idx := range idxInfos {
		if idx.ID == idxID {
			return idx, false
		}
	}
	// Not found the index info need reorg just return all done
	// Actually should not hit here.
	return nil, true
}

// updateReorgDone is update the need reorg index array position and check
// if all indexes is reorged. If not move to next one and reset reorg related data.
func updateReorgDone(job *model.Job, indexInfo *model.IndexInfo) bool {
	reorgIdxIDs := job.GetNeedReorgIndexIDs()
	// Check reorg done index is in job.NeedReorgIndexIDs
	inList := false
	for _, idxID := range reorgIdxIDs {
		if indexInfo.ID == idxID {
			inList = true
			break
		}
	}
	if !inList {
		return false
	}

	job.CurrentReorgIndexIdx++
	if job.CurrentReorgIndexIdx < len(reorgIdxIDs) {
		// Move to next then update reorg related struct
		newReorgMeta := &model.DDLReorgMeta{
			SQLMode:       job.ReorgMeta.SQLMode,
			Warnings:      job.ReorgMeta.Warnings,
			WarningsCount: job.ReorgMeta.WarningsCount,
			Location:      job.ReorgMeta.Location,
			ReorgTp:       job.ReorgMeta.ReorgTp,
			IsDistReorg:   job.ReorgMeta.IsDistReorg,
		}
		// Reset reorg meta and snapshot version
		job.ReorgMeta = newReorgMeta
		job.SnapshotVer = 0
		return false
	}
	// All reorg finished
	return true
}

// rollback related functions

func getColumnMapFromTable(tblInfo *model.TableInfo) map[string]*model.ColumnInfo {
	ret := make(map[string]*model.ColumnInfo)
	for _, colInfo := range tblInfo.Columns {
		ret[colInfo.Name.L] = colInfo
	}
	return ret
}

// checkDropColumnsNeedReorg returns if the drop column job has composite index covered and index doesn't contains auto increment field.
func checkDropColumnsNeedReorg(ctx sessionctx.Context, tblInfo *model.TableInfo, colName model.CIStr) (bool, error) {
	_, cidxInfos := listIndexesWithColumn(colName.L, tblInfo.Indices)
	if len(cidxInfos) == 0 {
		return false, nil
	}
	colMaps := getColumnMapFromTable(tblInfo)

	for _, idxInfo := range cidxInfos {
		for _, icol := range idxInfo.Columns {
			if col, have := colMaps[icol.Name.L]; have && mysql.HasAutoIncrementFlag(col.GetFlag()) {
				if col.Name.L == colName.L && ctx.GetSessionVars().AllowRemoveAutoInc {
					// column is the only one auto increment column in composite index
					// and allow drop auto increment column
					// continue to allow this case?
					return false, dbterror.ErrCantDropColWithAutoInc
				}
				return false, dbterror.ErrCantDropColWithAutoInc
			}
		}
	}
	return true, nil
}

// convertDropColumnWithCompositeIdxJob2RollbackJob will set new created temp composite index to delete only
// and then fallback to drop column with rolling back job state, and then go to the drop indexes process.
func convertDropColumnWithCompositeIdxJob2RollbackJob(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfos []*model.IndexInfo, err error) (int64, error) {
	job.State = model.JobStateRollingback
	originalState := indexInfos[0].State
	setIndicesState(indexInfos, model.StateDeleteOnly)
	job.SchemaState = model.StateDeleteOnly
	ver, err1 := updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfos[0].State)
	if err1 != nil {
		return ver, errors.Trace(err1)
	}

	if kv.ErrKeyExists.Equal(err) {
		return ver, kv.ErrKeyExists.GenWithStackByArgs("", trimTempCompositeIdxPrefix(indexInfos[0].Name.O))
	}

	return ver, errors.Trace(err)
}

// doDropIndexes will do the drop indexes process
func doDropIndexes(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfos []*model.IndexInfo) (ver int64, err error) {
	// TODO: Do we need this check?
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}
	originalState := indexInfos[0].State
	switch indexInfos[0].State {
	case model.StatePublic:
		// public -> write only
		setIndicesState(indexInfos, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> delete only
		setIndicesState(indexInfos, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> reorganization
		setIndicesState(indexInfos, model.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteReorganization
	case model.StateDeleteReorganization:
		// reorganization -> absent
		setIndicesState(indexInfos, model.StateNone)
		// Set column index flag.
		for _, idxInfo := range indexInfos {
			DropIndexColumnFlag(tblInfo, idxInfo)
			RemoveDependentHiddenColumns(tblInfo, idxInfo)
			removeIndexInfo(tblInfo, idxInfo)
		}

		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job
		indexIDs := indexInfosToIDList(indexInfos)
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			if job.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
				ingest.LitBackCtxMgr.Unregister(job.ID)
			}
			job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
		} else {
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfos[0].State))
	}
	return ver, errors.Trace(err)
}

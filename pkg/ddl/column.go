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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/bits"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// InitAndAddColumnToTable initializes the ColumnInfo in-place and adds it to the table.
func InitAndAddColumnToTable(tblInfo *model.TableInfo, colInfo *model.ColumnInfo) *model.ColumnInfo {
	cols := tblInfo.Columns
	colInfo.ID = AllocateColumnID(tblInfo)
	colInfo.State = model.StateNone
	// To support add column asynchronous, we should mark its offset as the last column.
	// So that we can use origin column offset to get value from row.
	colInfo.Offset = len(cols)
	// Append the column info to the end of the tblInfo.Columns.
	// It will reorder to the right offset in "Columns" when it state change to public.
	tblInfo.Columns = append(cols, colInfo)
	return colInfo
}

func checkAddColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, *model.ColumnInfo,
	*ast.ColumnPosition, bool /* ifNotExists */, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, nil, false, errors.Trace(err)
	}
	col := &model.ColumnInfo{}
	pos := &ast.ColumnPosition{}
	offset := 0
	ifNotExists := false
	err = job.DecodeArgs(col, pos, &offset, &ifNotExists)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, nil, false, errors.Trace(err)
	}

	columnInfo := model.FindColumnInfo(tblInfo.Columns, col.Name.L)
	if columnInfo != nil {
		if columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			job.State = model.JobStateCancelled
			return nil, nil, nil, nil, ifNotExists, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name)
		}
	}

	err = CheckAfterPositionExists(tblInfo, pos)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, nil, false, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name)
	}

	return tblInfo, columnInfo, col, pos, false, nil
}

// CheckAfterPositionExists makes sure the column specified in AFTER clause is exists.
// For example, ALTER TABLE t ADD COLUMN c3 INT AFTER c1.
func CheckAfterPositionExists(tblInfo *model.TableInfo, pos *ast.ColumnPosition) error {
	if pos != nil && pos.Tp == ast.ColumnPositionAfter {
		c := model.FindColumnInfo(tblInfo.Columns, pos.RelativeColumn.Name.L)
		if c == nil {
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(pos.RelativeColumn, tblInfo.Name)
		}
	}
	return nil
}

func setIndicesState(indexInfos []*model.IndexInfo, state model.SchemaState) {
	for _, indexInfo := range indexInfos {
		indexInfo.State = state
	}
}

func checkDropColumnForStatePublic(colInfo *model.ColumnInfo) (err error) {
	// When the dropping column has not-null flag and it hasn't the default value, we can backfill the column value like "add column".
	// NOTE: If the state of StateWriteOnly can be rollbacked, we'd better reconsider the original default value.
	// And we need consider the column without not-null flag.
	if colInfo.GetOriginDefaultValue() == nil && mysql.HasNotNullFlag(colInfo.GetFlag()) {
		// If the column is timestamp default current_timestamp, and DDL owner is new version TiDB that set column.Version to 1,
		// then old TiDB update record in the column write only stage will uses the wrong default value of the dropping column.
		// Because new version of the column default value is UTC time, but old version TiDB will think the default value is the time in system timezone.
		// But currently will be ok, because we can't cancel the drop column job when the job is running,
		// so the column will be dropped succeed and client will never see the wrong default value of the dropped column.
		// More info about this problem, see PR#9115.
		originDefVal, err := generateOriginDefaultValue(colInfo, nil)
		if err != nil {
			return err
		}
		return colInfo.SetOriginDefaultValue(originDefVal)
	}
	return nil
}

func onDropColumn(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, colInfo, idxInfos, ifExists, err := checkDropColumn(jobCtx, t, job)
	if err != nil {
		if ifExists && dbterror.ErrCantDropFieldOrKey.Equal(err) {
			// Convert the "not exists" error to a warning.
			job.Warning = toTError(err)
			job.State = model.JobStateDone
			return ver, nil
		}
		return ver, errors.Trace(err)
	}
	if job.MultiSchemaInfo != nil && !job.IsRollingback() && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		job.SchemaState = colInfo.State
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, t, job, tblInfo, false)
	}

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
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		failpoint.InjectCall("onDropColumnStateWriteOnly")
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
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.Args = append(job.Args, indexInfosToIDList(idxInfos))
	case model.StateDeleteOnly:
		// delete only -> reorganization
		colInfo.State = model.StateDeleteReorganization
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
		tblInfo.MoveColumnInfo(colInfo.Offset, len(tblInfo.Columns)-1)
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		colInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tblInfo, originalState != colInfo.State)
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

func checkDropColumn(jobCtx *jobContext, t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, []*model.IndexInfo, bool /* ifExists */, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, false, errors.Trace(err)
	}

	var colName pmodel.CIStr
	var ifExists bool
	// indexIDs is used to make sure we don't truncate args when decoding the rawArgs.
	var indexIDs []int64
	err = job.DecodeArgs(&colName, &ifExists, &indexIDs)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, false, errors.Trace(err)
	}

	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil || colInfo.Hidden {
		job.State = model.JobStateCancelled
		return nil, nil, nil, ifExists, dbterror.ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, false, errors.Trace(err)
	}
	if err = checkDropColumnWithForeignKeyConstraintInOwner(jobCtx.infoCache, job, tblInfo, colName.L); err != nil {
		return nil, nil, nil, false, errors.Trace(err)
	}
	if err = checkDropColumnWithTTLConfig(tblInfo, colName.L); err != nil {
		return nil, nil, nil, false, errors.Trace(err)
	}
	idxInfos := listIndicesWithColumn(colName.L, tblInfo.Indices)
	return tblInfo, colInfo, idxInfos, false, nil
}

func isDroppableColumn(tblInfo *model.TableInfo, colName pmodel.CIStr) error {
	if ok, dep, isHidden := hasDependentByGeneratedColumn(tblInfo, colName); ok {
		if isHidden {
			return dbterror.ErrDependentByFunctionalIndex.GenWithStackByArgs(dep)
		}
		return dbterror.ErrDependentByGeneratedColumn.GenWithStackByArgs(dep)
	}

	if len(tblInfo.Columns) == 1 {
		return dbterror.ErrCantRemoveAllFields.GenWithStack("can't drop only column %s in table %s",
			colName, tblInfo.Name)
	}
	// We only support dropping column with single-value none Primary Key index covered now.
	err := isColumnCanDropWithIndex(colName.L, tblInfo.Indices)
	if err != nil {
		return err
	}
	err = IsColumnDroppableWithCheckConstraint(colName, tblInfo)
	if err != nil {
		return err
	}
	return nil
}

func onSetDefaultValue(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	err := job.DecodeArgs(newCol)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return updateColumnDefaultValue(jobCtx, t, job, newCol, &newCol.Name)
}

func setIdxIDName(idxInfo *model.IndexInfo, newID int64, newName pmodel.CIStr) {
	idxInfo.ID = newID
	idxInfo.Name = newName
}

// SetIdxColNameOffset sets index column name and offset from changing ColumnInfo.
func SetIdxColNameOffset(idxCol *model.IndexColumn, changingCol *model.ColumnInfo) {
	idxCol.Name = changingCol.Name
	idxCol.Offset = changingCol.Offset
	canPrefix := types.IsTypePrefixable(changingCol.GetType())
	if !canPrefix || (changingCol.GetFlen() <= idxCol.Length) {
		idxCol.Length = types.UnspecifiedLength
	}
}

func removeChangingColAndIdxs(tblInfo *model.TableInfo, changingColID int64) {
	restIdx := tblInfo.Indices[:0]
	for _, idx := range tblInfo.Indices {
		if !idx.HasColumnInIndexColumns(tblInfo, changingColID) {
			restIdx = append(restIdx, idx)
		}
	}
	tblInfo.Indices = restIdx

	restCols := tblInfo.Columns[:0]
	for _, c := range tblInfo.Columns {
		if c.ID != changingColID {
			restCols = append(restCols, c)
		}
	}
	tblInfo.Columns = restCols
}

func replaceOldColumn(tblInfo *model.TableInfo, oldCol, changingCol *model.ColumnInfo,
	newName pmodel.CIStr) *model.ColumnInfo {
	tblInfo.MoveColumnInfo(changingCol.Offset, len(tblInfo.Columns)-1)
	changingCol = updateChangingCol(changingCol, newName, oldCol.Offset)
	tblInfo.Columns[oldCol.Offset] = changingCol
	tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
	return changingCol
}

func replaceOldIndexes(tblInfo *model.TableInfo, changingIdxs []*model.IndexInfo) {
	// Remove the changing indexes.
	for i, idx := range tblInfo.Indices {
		for _, cIdx := range changingIdxs {
			if cIdx.ID == idx.ID {
				tblInfo.Indices[i] = nil
				break
			}
		}
	}
	tmp := tblInfo.Indices[:0]
	for _, idx := range tblInfo.Indices {
		if idx != nil {
			tmp = append(tmp, idx)
		}
	}
	tblInfo.Indices = tmp
	// Replace the old indexes with changing indexes.
	for _, cIdx := range changingIdxs {
		// The index name should be changed from '_Idx$_name' to 'name'.
		idxName := getChangingIndexOriginName(cIdx)
		for i, idx := range tblInfo.Indices {
			if strings.EqualFold(idxName, idx.Name.O) {
				cIdx.Name = pmodel.NewCIStr(idxName)
				tblInfo.Indices[i] = cIdx
				break
			}
		}
	}
}

// updateNewIdxColsNameOffset updates the name&offset of the index column.
func updateNewIdxColsNameOffset(changingIdxs []*model.IndexInfo,
	oldName pmodel.CIStr, changingCol *model.ColumnInfo) {
	for _, idx := range changingIdxs {
		for _, col := range idx.Columns {
			if col.Name.L == oldName.L {
				SetIdxColNameOffset(col, changingCol)
			}
		}
	}
}

// filterIndexesToRemove filters out the indexes that can be removed.
func filterIndexesToRemove(changingIdxs []*model.IndexInfo, colName pmodel.CIStr, tblInfo *model.TableInfo) []*model.IndexInfo {
	indexesToRemove := make([]*model.IndexInfo, 0, len(changingIdxs))
	for _, idx := range changingIdxs {
		var hasOtherChangingCol bool
		for _, col := range idx.Columns {
			if col.Name.L == colName.L {
				continue // ignore the current modifying column.
			}
			if !hasOtherChangingCol {
				hasOtherChangingCol = tblInfo.Columns[col.Offset].ChangeStateInfo != nil
			}
		}
		// For the indexes that still contains other changing column, skip removing it now.
		// We leave the removal work to the last modify column job.
		if !hasOtherChangingCol {
			indexesToRemove = append(indexesToRemove, idx)
		}
	}
	return indexesToRemove
}

func updateChangingCol(col *model.ColumnInfo, newName pmodel.CIStr, newOffset int) *model.ColumnInfo {
	col.Name = newName
	col.ChangeStateInfo = nil
	col.Offset = newOffset
	// After changing the column, the column's type is change, so it needs to set OriginDefaultValue back
	// so that there is no error in getting the default value from OriginDefaultValue.
	// Besides, nil data that was not backfilled in the "add column" is backfilled after the column is changed.
	// So it can set OriginDefaultValue to nil.
	col.OriginDefaultValue = nil
	return col
}

func buildRelatedIndexInfos(tblInfo *model.TableInfo, colID int64) []*model.IndexInfo {
	var indexInfos []*model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.HasColumnInIndexColumns(tblInfo, colID) {
			indexInfos = append(indexInfos, idx)
		}
	}
	return indexInfos
}

func buildRelatedIndexIDs(tblInfo *model.TableInfo, colID int64) []int64 {
	var oldIdxIDs []int64
	for _, idx := range tblInfo.Indices {
		if idx.HasColumnInIndexColumns(tblInfo, colID) {
			oldIdxIDs = append(oldIdxIDs, idx.ID)
		}
	}
	return oldIdxIDs
}

// LocateOffsetToMove returns the offset of the column to move.
func LocateOffsetToMove(currentOffset int, pos *ast.ColumnPosition, tblInfo *model.TableInfo) (destOffset int, err error) {
	if pos == nil {
		return currentOffset, nil
	}
	// Get column offset.
	switch pos.Tp {
	case ast.ColumnPositionFirst:
		return 0, nil
	case ast.ColumnPositionAfter:
		c := model.FindColumnInfo(tblInfo.Columns, pos.RelativeColumn.Name.L)
		if c == nil || c.State != model.StatePublic {
			return 0, infoschema.ErrColumnNotExists.GenWithStackByArgs(pos.RelativeColumn, tblInfo.Name)
		}
		if currentOffset <= c.Offset {
			return c.Offset, nil
		}
		return c.Offset + 1, nil
	case ast.ColumnPositionNone:
		return currentOffset, nil
	default:
		return 0, errors.Errorf("unknown column position type")
	}
}

// BuildElements is exported for testing.
func BuildElements(changingCol *model.ColumnInfo, changingIdxs []*model.IndexInfo) []*meta.Element {
	elements := make([]*meta.Element, 0, len(changingIdxs)+1)
	elements = append(elements, &meta.Element{ID: changingCol.ID, TypeKey: meta.ColumnElementKey})
	for _, idx := range changingIdxs {
		elements = append(elements, &meta.Element{ID: idx.ID, TypeKey: meta.IndexElementKey})
	}
	return elements
}

func (w *worker) updatePhysicalTableRow(t table.Table, reorgInfo *reorgInfo) error {
	logutil.DDLLogger().Info("start to update table row", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	if tbl, ok := t.(table.PartitionedTable); ok {
		done := false
		for !done {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			workType := typeReorgPartitionWorker
			switch reorgInfo.Job.Type {
			case model.ActionReorganizePartition,
				model.ActionRemovePartitioning,
				model.ActionAlterTablePartitioning:
				// Expected
			default:
				// workType = typeUpdateColumnWorker
				// TODO: Support Modify Column on partitioned table
				// https://github.com/pingcap/tidb/issues/38297
				return dbterror.ErrCancelledDDLJob.GenWithStack("Modify Column on partitioned table / typeUpdateColumnWorker not yet supported.")
			}
			err := w.writePhysicalTableRecord(w.ctx, w.sessPool, p, workType, reorgInfo)
			if err != nil {
				return err
			}
			done, err = updateReorgInfo(w.sessPool, tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}
	if tbl, ok := t.(table.PhysicalTable); ok {
		return w.writePhysicalTableRecord(w.ctx, w.sessPool, tbl, typeUpdateColumnWorker, reorgInfo)
	}
	return dbterror.ErrCancelledDDLJob.GenWithStack("internal error for phys tbl id: %d tbl id: %d", reorgInfo.PhysicalTableID, t.Meta().ID)
}

// TestReorgGoroutineRunning is only used in test to indicate the reorg goroutine has been started.
var TestReorgGoroutineRunning = make(chan any)

// updateCurrentElement update the current element for reorgInfo.
func (w *worker) updateCurrentElement(t table.Table, reorgInfo *reorgInfo) error {
	failpoint.Inject("mockInfiniteReorgLogic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			a := new(any)
			TestReorgGoroutineRunning <- a
			for {
				time.Sleep(30 * time.Millisecond)
				if w.isReorgCancelled(reorgInfo.Job.ID) {
					// Job is cancelled. So it can't be done.
					failpoint.Return(dbterror.ErrCancelledDDLJob)
				}
			}
		}
	})
	// TODO: Support partition tables.
	if bytes.Equal(reorgInfo.currElement.TypeKey, meta.ColumnElementKey) {
		//nolint:forcetypeassert
		err := w.updatePhysicalTableRow(t.(table.PhysicalTable), reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if _, ok := t.(table.PartitionedTable); ok {
		// TODO: remove when modify column of partitioned table is supported
		// https://github.com/pingcap/tidb/issues/38297
		return dbterror.ErrCancelledDDLJob.GenWithStack("Modify Column on partitioned table / typeUpdateColumnWorker not yet supported.")
	}
	// Get the original start handle and end handle.
	currentVer, err := getValidCurrentVersion(reorgInfo.jobCtx.store)
	if err != nil {
		return errors.Trace(err)
	}
	//nolint:forcetypeassert
	originalStartHandle, originalEndHandle, err := getTableRange(reorgInfo.NewJobContext(), reorgInfo.jobCtx.store, t.(table.PhysicalTable), currentVer.Ver, reorgInfo.Job.Priority)
	if err != nil {
		return errors.Trace(err)
	}

	startElementOffset := 0
	startElementOffsetToResetHandle := -1
	// This backfill job starts with backfilling index data, whose index ID is currElement.ID.
	if bytes.Equal(reorgInfo.currElement.TypeKey, meta.IndexElementKey) {
		for i, element := range reorgInfo.elements[1:] {
			if reorgInfo.currElement.ID == element.ID {
				startElementOffset = i
				startElementOffsetToResetHandle = i
				break
			}
		}
	}

	for i := startElementOffset; i < len(reorgInfo.elements[1:]); i++ {
		// This backfill job has been exited during processing. At that time, the element is reorgInfo.elements[i+1] and handle range is [reorgInfo.StartHandle, reorgInfo.EndHandle].
		// Then the handle range of the rest elements' is [originalStartHandle, originalEndHandle].
		if i == startElementOffsetToResetHandle+1 {
			reorgInfo.StartKey, reorgInfo.EndKey = originalStartHandle, originalEndHandle
		}

		// Update the element in the reorgInfo for updating the reorg meta below.
		reorgInfo.currElement = reorgInfo.elements[i+1]
		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err := reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("update column and indexes",
			zap.Int64("job ID", reorgInfo.Job.ID),
			zap.Stringer("element", reorgInfo.currElement),
			zap.String("start key", hex.EncodeToString(reorgInfo.StartKey)),
			zap.String("end key", hex.EncodeToString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
		err = w.addTableIndex(t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type updateColumnWorker struct {
	*backfillCtx
	oldColInfo *model.ColumnInfo
	newColInfo *model.ColumnInfo

	// The following attributes are used to reduce memory allocation.
	rowRecords []*rowRecord
	rowDecoder *decoder.RowDecoder

	rowMap map[int64]types.Datum

	checksumNeeded bool
}

func newUpdateColumnWorker(id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*updateColumnWorker, error) {
	bCtx, err := newBackfillCtx(id, reorgInfo, reorgInfo.SchemaName, t, jc, "update_col_rate", false, true)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(reorgInfo.currElement.TypeKey, meta.ColumnElementKey) {
		logutil.DDLLogger().Error("Element type for updateColumnWorker incorrect", zap.String("jobQuery", reorgInfo.Query),
			zap.Stringer("reorgInfo", reorgInfo))
		return nil, nil
	}
	var oldCol, newCol *model.ColumnInfo
	for _, col := range t.WritableCols() {
		if col.ID == reorgInfo.currElement.ID {
			newCol = col.ColumnInfo
			oldCol = table.FindCol(t.Cols(), getChangingColumnOriginName(newCol)).ColumnInfo
			break
		}
	}
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	failpoint.Inject("forceRowLevelChecksumOnUpdateColumnBackfill", func() {
		orig := variable.EnableRowLevelChecksum.Load()
		defer variable.EnableRowLevelChecksum.Store(orig)
		variable.EnableRowLevelChecksum.Store(true)
	})
	return &updateColumnWorker{
		backfillCtx:    bCtx,
		oldColInfo:     oldCol,
		newColInfo:     newCol,
		rowDecoder:     rowDecoder,
		rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
		checksumNeeded: variable.EnableRowLevelChecksum.Load(),
	}, nil
}

func (w *updateColumnWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (*updateColumnWorker) String() string {
	return typeUpdateColumnWorker.String()
}

func (w *updateColumnWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

type rowRecord struct {
	key     []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals    []byte        // It's the record.
	warning *terror.Error // It's used to record the cast warning of a record.
}

// getNextHandleKey gets next handle of entry that we are going to process.
func getNextHandleKey(taskRange reorgBackfillTask,
	taskDone bool, lastAccessedHandle kv.Key) (nextHandle kv.Key) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		return lastAccessedHandle.Next()
	}

	return taskRange.endKey.Next()
}

func (w *updateColumnWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*rowRecord, kv.Key, bool, error) {
	w.rowRecords = w.rowRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	var lastAccessedHandle kv.Key
	oprStartTime := startTime
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, taskRange.physicalTable.RecordPrefix(),
		txn.StartTS(), taskRange.startKey, taskRange.endKey, func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in updateColumnWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.rowRecords) >= w.batchCnt {
				return false, nil
			}

			if err1 := w.getRowRecord(handle, recordKey, rawRow); err1 != nil {
				return false, errors.Trace(err1)
			}
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
		zap.String("taskRange", taskRange.String()),
		zap.Duration("takeTime", time.Since(startTime)))
	return w.rowRecords, getNextHandleKey(taskRange, taskDone, lastAccessedHandle), taskDone, errors.Trace(err)
}

func (w *updateColumnWorker) getRowRecord(handle kv.Handle, recordKey []byte, rawRow []byte) error {
	sysTZ := w.loc
	_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, handle, rawRow, sysTZ, w.rowMap)
	if err != nil {
		return errors.Trace(dbterror.ErrCantDecodeRecord.GenWithStackByArgs("column", err))
	}

	if _, ok := w.rowMap[w.newColInfo.ID]; ok {
		// The column is already added by update or insert statement, skip it.
		w.cleanRowMap()
		return nil
	}

	var recordWarning *terror.Error
	// Since every updateColumnWorker handle their own work individually, we can cache warning in statement context when casting datum.
	oldWarn := w.warnings.GetWarnings()
	if oldWarn == nil {
		oldWarn = []contextutil.SQLWarn{}
	} else {
		oldWarn = oldWarn[:0]
	}
	w.warnings.SetWarnings(oldWarn)
	val := w.rowMap[w.oldColInfo.ID]
	col := w.newColInfo
	if val.Kind() == types.KindNull && col.FieldType.GetType() == mysql.TypeTimestamp && mysql.HasNotNullFlag(col.GetFlag()) {
		if v, err := expression.GetTimeCurrentTimestamp(w.exprCtx.GetEvalCtx(), col.GetType(), col.GetDecimal()); err == nil {
			// convert null value to timestamp should be substituted with current timestamp if NOT_NULL flag is set.
			w.rowMap[w.oldColInfo.ID] = v
		}
	}
	newColVal, err := table.CastColumnValue(w.exprCtx, w.rowMap[w.oldColInfo.ID], w.newColInfo, false, false)
	if err != nil {
		return w.reformatErrors(err)
	}
	warn := w.warnings.GetWarnings()
	if len(warn) != 0 {
		//nolint:forcetypeassert
		recordWarning = errors.Cause(w.reformatErrors(warn[0].Err)).(*terror.Error)
	}

	failpoint.Inject("MockReorgTimeoutInOneRegion", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			if handle.IntValue() == 3000 && atomic.CompareAndSwapInt32(&TestCheckReorgTimeout, 0, 1) {
				failpoint.Return(errors.Trace(dbterror.ErrWaitReorgTimeout))
			}
		}
	})

	w.rowMap[w.newColInfo.ID] = newColVal
	_, err = w.rowDecoder.EvalRemainedExprColumnMap(w.exprCtx, w.rowMap)
	if err != nil {
		return errors.Trace(err)
	}
	newColumnIDs := make([]int64, 0, len(w.rowMap))
	newRow := make([]types.Datum, 0, len(w.rowMap))
	for colID, val := range w.rowMap {
		newColumnIDs = append(newColumnIDs, colID)
		newRow = append(newRow, val)
	}
	rd := w.tblCtx.GetRowEncodingConfig().RowEncoder
	ec := w.exprCtx.GetEvalCtx().ErrCtx()
	var checksum rowcodec.Checksum
	if w.checksumNeeded {
		checksum = rowcodec.RawChecksum{Key: recordKey}
	}
	newRowVal, err := tablecodec.EncodeRow(sysTZ, newRow, newColumnIDs, nil, nil, checksum, rd)
	err = ec.HandleError(err)
	if err != nil {
		return errors.Trace(err)
	}

	w.rowRecords = append(w.rowRecords, &rowRecord{key: recordKey, vals: newRowVal, warning: recordWarning})
	w.cleanRowMap()
	return nil
}

// reformatErrors casted error because `convertTo` function couldn't package column name and datum value for some errors.
func (w *updateColumnWorker) reformatErrors(err error) error {
	// Since row count is not precious in concurrent reorganization, here we substitute row count with datum value.
	if types.ErrTruncated.Equal(err) || types.ErrDataTooLong.Equal(err) {
		dStr := datumToStringNoErr(w.rowMap[w.oldColInfo.ID])
		err = types.ErrTruncated.GenWithStack("Data truncated for column '%s', value is '%s'", w.oldColInfo.Name, dStr)
	}

	if types.ErrWarnDataOutOfRange.Equal(err) {
		dStr := datumToStringNoErr(w.rowMap[w.oldColInfo.ID])
		err = types.ErrWarnDataOutOfRange.GenWithStack("Out of range value for column '%s', the value is '%s'", w.oldColInfo.Name, dStr)
	}
	return err
}

func datumToStringNoErr(d types.Datum) string {
	if v, err := d.ToString(); err == nil {
		return v
	}
	return fmt.Sprintf("%v", d.GetValue())
}

func (w *updateColumnWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

// BackfillData will backfill the table record in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed.
func (w *updateColumnWorker) BackfillData(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)

		// Because TiCDC do not want this kind of change,
		// so we set the lossy DDL reorg txn source to 1 to
		// avoid TiCDC to replicate this kind of change.
		var txnSource uint64
		if val := txn.GetOption(kv.TxnSource); val != nil {
			txnSource, _ = val.(uint64)
		}
		err := kv.SetLossyDDLReorgSource(&txnSource, kv.LossyDDLColumnReorgSource)
		if err != nil {
			return errors.Trace(err)
		}
		txn.SetOption(kv.TxnSource, txnSource)

		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		rowRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		// Optimize for few warnings!
		warningsMap := make(map[errors.ErrorID]*terror.Error, 2)
		warningsCountMap := make(map[errors.ErrorID]int64, 2)
		for _, rowRecord := range rowRecords {
			taskCtx.scanCount++

			err = txn.Set(rowRecord.key, rowRecord.vals)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
			if rowRecord.warning != nil {
				if _, ok := warningsCountMap[rowRecord.warning.ID()]; ok {
					warningsCountMap[rowRecord.warning.ID()]++
				} else {
					warningsCountMap[rowRecord.warning.ID()] = 1
					warningsMap[rowRecord.warning.ID()] = rowRecord.warning
				}
			}
		}

		// Collect the warnings.
		taskCtx.warnings, taskCtx.warningsCount = warningsMap, warningsCountMap

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "BackfillData", 3000)

	return
}

func updateChangingObjState(changingCol *model.ColumnInfo, changingIdxs []*model.IndexInfo, schemaState model.SchemaState) {
	changingCol.State = schemaState
	for _, idx := range changingIdxs {
		idx.State = schemaState
	}
}

func checkAndApplyAutoRandomBits(jobCtx *jobContext, m *meta.Meta, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo, newCol *model.ColumnInfo, newAutoRandBits uint64) error {
	if newAutoRandBits == 0 {
		return nil
	}
	idAcc := m.GetAutoIDAccessors(dbInfo.ID, tblInfo.ID)
	err := checkNewAutoRandomBits(idAcc, oldCol, newCol, newAutoRandBits, tblInfo.AutoRandomRangeBits, tblInfo.SepAutoInc())
	if err != nil {
		return err
	}
	return applyNewAutoRandomBits(jobCtx, m, dbInfo, tblInfo, oldCol, newAutoRandBits)
}

// checkNewAutoRandomBits checks whether the new auto_random bits number can cause overflow.
func checkNewAutoRandomBits(idAccessors meta.AutoIDAccessors, oldCol *model.ColumnInfo,
	newCol *model.ColumnInfo, newShardBits, newRangeBits uint64, sepAutoInc bool) error {
	shardFmt := autoid.NewShardIDFormat(&newCol.FieldType, newShardBits, newRangeBits)

	idAcc := idAccessors.RandomID()
	convertedFromAutoInc := mysql.HasAutoIncrementFlag(oldCol.GetFlag())
	if convertedFromAutoInc {
		if sepAutoInc {
			idAcc = idAccessors.IncrementID(model.TableInfoVersion5)
		} else {
			idAcc = idAccessors.RowID()
		}
	}
	// Generate a new auto ID first to prevent concurrent update in DML.
	_, err := idAcc.Inc(1)
	if err != nil {
		return err
	}
	currentIncBitsVal, err := idAcc.Get()
	if err != nil {
		return err
	}
	// Find the max number of available shard bits by
	// counting leading zeros in current inc part of auto_random ID.
	usedBits := uint64(64 - bits.LeadingZeros64(uint64(currentIncBitsVal)))
	if usedBits > shardFmt.IncrementalBits {
		overflowCnt := usedBits - shardFmt.IncrementalBits
		errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg, newShardBits-overflowCnt, newShardBits, oldCol.Name.O)
		return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	}
	return nil
}

func (d *ddlCtx) getAutoIDRequirement() autoid.Requirement {
	return &asAutoIDRequirement{
		store:     d.store,
		autoidCli: d.autoidCli,
	}
}

type asAutoIDRequirement struct {
	store     kv.Storage
	autoidCli *autoid.ClientDiscover
}

var _ autoid.Requirement = &asAutoIDRequirement{}

func (r *asAutoIDRequirement) Store() kv.Storage {
	return r.store
}

func (r *asAutoIDRequirement) AutoIDClient() *autoid.ClientDiscover {
	return r.autoidCli
}

// applyNewAutoRandomBits set auto_random bits to TableInfo and
// migrate auto_increment ID to auto_random ID if possible.
func applyNewAutoRandomBits(jobCtx *jobContext, m *meta.Meta, dbInfo *model.DBInfo,
	tblInfo *model.TableInfo, oldCol *model.ColumnInfo, newAutoRandBits uint64) error {
	tblInfo.AutoRandomBits = newAutoRandBits
	needMigrateFromAutoIncToAutoRand := mysql.HasAutoIncrementFlag(oldCol.GetFlag())
	if !needMigrateFromAutoIncToAutoRand {
		return nil
	}
	autoRandAlloc := autoid.NewAllocatorsFromTblInfo(jobCtx.getAutoIDRequirement(), dbInfo.ID, tblInfo).Get(autoid.AutoRandomType)
	if autoRandAlloc == nil {
		errMsg := fmt.Sprintf(autoid.AutoRandomAllocatorNotFound, dbInfo.Name.O, tblInfo.Name.O)
		return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	}
	idAcc := m.GetAutoIDAccessors(dbInfo.ID, tblInfo.ID).RowID()
	nextAutoIncID, err := idAcc.Get()
	if err != nil {
		return errors.Trace(err)
	}
	err = autoRandAlloc.Rebase(context.Background(), nextAutoIncID, false)
	if err != nil {
		return errors.Trace(err)
	}
	if err := idAcc.Del(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// checkForNullValue ensure there are no null values of the column of this table.
// `isDataTruncated` indicates whether the new field and the old field type are the same, in order to be compatible with mysql.
func checkForNullValue(ctx context.Context, sctx sessionctx.Context, isDataTruncated bool, schema, table pmodel.CIStr, newCol *model.ColumnInfo, oldCols ...*model.ColumnInfo) error {
	needCheckNullValue := false
	for _, oldCol := range oldCols {
		if oldCol.GetType() != mysql.TypeTimestamp && newCol.GetType() == mysql.TypeTimestamp {
			// special case for convert null value of non-timestamp type to timestamp type, null value will be substituted with current timestamp.
			continue
		}
		needCheckNullValue = true
	}
	if !needCheckNullValue {
		return nil
	}
	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where ")
	paramsList := make([]any, 0, 2+len(oldCols))
	paramsList = append(paramsList, schema.L, table.L)
	for i, col := range oldCols {
		if i == 0 {
			buf.WriteString("%n is null")
			paramsList = append(paramsList, col.Name.L)
		} else {
			buf.WriteString(" or %n is null")
			paramsList = append(paramsList, col.Name.L)
		}
	}
	buf.WriteString(" limit 1")
	//nolint:forcetypeassert
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, buf.String(), paramsList...)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		if isDataTruncated {
			return dbterror.ErrWarnDataTruncated.GenWithStackByArgs(newCol.Name.L, rowCount)
		}
		return dbterror.ErrInvalidUseOfNull
	}
	return nil
}

func updateColumnDefaultValue(jobCtx *jobContext, t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldColName *pmodel.CIStr) (ver int64, _ error) {
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, t, job, tblInfo, false)
	}

	oldCol := model.FindColumnInfo(tblInfo.Columns, oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(newCol.Name, tblInfo.Name)
	}

	if hasDefaultValue, _, err := checkColumnDefaultValue(newReorgExprCtx(), table.ToColumn(oldCol.Clone()), newCol.DefaultValue); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	} else if !hasDefaultValue {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(newCol.Name)
	}

	// The newCol's offset may be the value of the old schema version, so we can't use newCol directly.
	oldCol.DefaultValue = newCol.DefaultValue
	oldCol.DefaultValueBit = newCol.DefaultValueBit
	oldCol.DefaultIsExpr = newCol.DefaultIsExpr
	if mysql.HasNoDefaultValueFlag(newCol.GetFlag()) {
		oldCol.AddFlag(mysql.NoDefaultValueFlag)
	} else {
		oldCol.DelFlag(mysql.NoDefaultValueFlag)
		err = checkDefaultValue(newReorgExprCtx(), table.ToColumn(oldCol), true)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
	}

	ver, err = updateVersionAndTableInfo(jobCtx, t, job, tblInfo, true)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func isColumnWithIndex(colName string, indices []*model.IndexInfo) bool {
	for _, indexInfo := range indices {
		for _, col := range indexInfo.Columns {
			if col.Name.L == colName {
				return true
			}
		}
	}
	return false
}

func isColumnCanDropWithIndex(colName string, indices []*model.IndexInfo) error {
	for _, indexInfo := range indices {
		if indexInfo.Primary || len(indexInfo.Columns) > 1 || indexInfo.VectorInfo != nil {
			for _, col := range indexInfo.Columns {
				if col.Name.L == colName {
					errMsg := "with composite index covered or Primary Key covered now"
					if indexInfo.VectorInfo != nil {
						errMsg = "with Vector Key covered now"
					}
					return dbterror.ErrCantDropColWithIndex.GenWithStack("can't drop column %s "+errMsg, colName)
				}
			}
		}
	}
	return nil
}

func listIndicesWithColumn(colName string, indices []*model.IndexInfo) []*model.IndexInfo {
	ret := make([]*model.IndexInfo, 0)
	for _, indexInfo := range indices {
		if len(indexInfo.Columns) == 1 && colName == indexInfo.Columns[0].Name.L {
			ret = append(ret, indexInfo)
		}
	}
	return ret
}

// GetColumnForeignKeyInfo returns the wanted foreign key info
func GetColumnForeignKeyInfo(colName string, fkInfos []*model.FKInfo) *model.FKInfo {
	for _, fkInfo := range fkInfos {
		for _, col := range fkInfo.Cols {
			if col.L == colName {
				return fkInfo
			}
		}
	}
	return nil
}

// AllocateColumnID allocates next column ID from TableInfo.
func AllocateColumnID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxColumnID++
	return tblInfo.MaxColumnID
}

func checkAddColumnTooManyColumns(colNum int) error {
	if uint32(colNum) > atomic.LoadUint32(&config.GetGlobalConfig().TableColumnCountLimit) {
		return dbterror.ErrTooManyFields
	}
	return nil
}

// modifyColsFromNull2NotNull modifies the type definitions of 'null' to 'not null'.
// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
func modifyColsFromNull2NotNull(w *worker, dbInfo *model.DBInfo, tblInfo *model.TableInfo, cols []*model.ColumnInfo, newCol *model.ColumnInfo, isDataTruncated bool) error {
	// Get sessionctx from context resource pool.
	var sctx sessionctx.Context
	sctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(sctx)

	skipCheck := false
	failpoint.Inject("skipMockContextDoExec", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			skipCheck = true
		}
	})
	if !skipCheck {
		// If there is a null value inserted, it cannot be modified and needs to be rollback.
		err = checkForNullValue(w.ctx, sctx, isDataTruncated, dbInfo.Name, tblInfo.Name, newCol, cols...)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Prevent this field from inserting null values.
	for _, col := range cols {
		col.AddFlag(mysql.PreventNullInsertFlag)
	}
	return nil
}

func generateOriginDefaultValue(col *model.ColumnInfo, ctx sessionctx.Context) (any, error) {
	var err error
	odValue := col.GetDefaultValue()
	if odValue == nil && mysql.HasNotNullFlag(col.GetFlag()) ||
		// It's for drop column and modify column.
		(col.DefaultIsExpr && odValue != strings.ToUpper(ast.CurrentTimestamp) && ctx == nil) {
		switch col.GetType() {
		// Just use enum field's first element for OriginDefaultValue.
		case mysql.TypeEnum:
			defEnum, verr := types.ParseEnumValue(col.GetElems(), 1)
			if verr != nil {
				return nil, errors.Trace(verr)
			}
			defVal := types.NewCollateMysqlEnumDatum(defEnum, col.GetCollate())
			return defVal.ToString()
		default:
			zeroVal := table.GetZeroValue(col)
			odValue, err = zeroVal.ToString()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	if odValue == strings.ToUpper(ast.CurrentTimestamp) {
		var t time.Time
		if ctx == nil {
			t = time.Now()
		} else {
			t, _ = expression.GetStmtTimestamp(ctx.GetExprCtx().GetEvalCtx())
		}
		if col.GetType() == mysql.TypeTimestamp {
			odValue = types.NewTime(types.FromGoTime(t.UTC()), col.GetType(), col.GetDecimal()).String()
		} else if col.GetType() == mysql.TypeDatetime {
			odValue = types.NewTime(types.FromGoTime(t), col.GetType(), col.GetDecimal()).String()
		}
		return odValue, nil
	}

	if col.DefaultIsExpr && ctx != nil {
		valStr, ok := odValue.(string)
		if !ok {
			return nil, dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed.GenWithStackByArgs(col.Name.String())
		}
		oldValue := strings.ToLower(valStr)
		// It's checked in getFuncCallDefaultValue.
		if !strings.Contains(oldValue, fmt.Sprintf("%s(%s(),", ast.DateFormat, ast.Now)) &&
			!strings.Contains(oldValue, ast.StrToDate) {
			return nil, errors.Trace(dbterror.ErrBinlogUnsafeSystemFunction)
		}

		defVal, err := table.GetColDefaultValue(ctx.GetExprCtx(), col)
		if err != nil {
			return nil, errors.Trace(err)
		}
		odValue, err = defVal.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return odValue, nil
}

func indexInfoContains(idxID int64, idxInfos []*model.IndexInfo) bool {
	for _, idxInfo := range idxInfos {
		if idxID == idxInfo.ID {
			return true
		}
	}
	return false
}

func indexInfosToIDList(idxInfos []*model.IndexInfo) []int64 {
	ids := make([]int64, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		ids = append(ids, idxInfo.ID)
	}
	return ids
}

func genChangingColumnUniqueName(tblInfo *model.TableInfo, oldCol *model.ColumnInfo) string {
	suffix := 0
	newColumnNamePrefix := fmt.Sprintf("%s%s", changingColumnPrefix, oldCol.Name.O)
	newColumnLowerName := fmt.Sprintf("%s_%d", strings.ToLower(newColumnNamePrefix), suffix)
	// Check whether the new column name is used.
	columnNameMap := make(map[string]bool, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		columnNameMap[col.Name.L] = true
	}
	for columnNameMap[newColumnLowerName] {
		suffix++
		newColumnLowerName = fmt.Sprintf("%s_%d", strings.ToLower(newColumnNamePrefix), suffix)
	}
	return fmt.Sprintf("%s_%d", newColumnNamePrefix, suffix)
}

func genChangingIndexUniqueName(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) string {
	suffix := 0
	newIndexNamePrefix := fmt.Sprintf("%s%s", changingIndexPrefix, idxInfo.Name.O)
	newIndexLowerName := fmt.Sprintf("%s_%d", strings.ToLower(newIndexNamePrefix), suffix)
	// Check whether the new index name is used.
	indexNameMap := make(map[string]bool, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		indexNameMap[idx.Name.L] = true
	}
	for indexNameMap[newIndexLowerName] {
		suffix++
		newIndexLowerName = fmt.Sprintf("%s_%d", strings.ToLower(newIndexNamePrefix), suffix)
	}
	return fmt.Sprintf("%s_%d", newIndexNamePrefix, suffix)
}

func getChangingIndexOriginName(changingIdx *model.IndexInfo) string {
	idxName := strings.TrimPrefix(changingIdx.Name.O, changingIndexPrefix)
	// Since the unique idxName may contain the suffix number (indexName_num), better trim the suffix.
	var pos int
	if pos = strings.LastIndex(idxName, "_"); pos == -1 {
		return idxName
	}
	return idxName[:pos]
}

func getChangingColumnOriginName(changingColumn *model.ColumnInfo) string {
	columnName := strings.TrimPrefix(changingColumn.Name.O, changingColumnPrefix)
	var pos int
	if pos = strings.LastIndex(columnName, "_"); pos == -1 {
		return columnName
	}
	return columnName[:pos]
}

func getExpressionIndexOriginName(expressionIdx *model.ColumnInfo) string {
	columnName := strings.TrimPrefix(expressionIdx.Name.O, expressionIndexPrefix+"_")
	var pos int
	if pos = strings.LastIndex(columnName, "_"); pos == -1 {
		return columnName
	}
	return columnName[:pos]
}

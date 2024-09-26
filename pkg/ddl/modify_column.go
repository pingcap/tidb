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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

type modifyingColInfo struct {
	newCol                *model.ColumnInfo
	oldColName            *pmodel.CIStr
	modifyColumnTp        byte
	updatedAutoRandomBits uint64
	changingCol           *model.ColumnInfo
	changingIdxs          []*model.IndexInfo
	pos                   *ast.ColumnPosition
	removedIdxs           []int64
}

func hasVectorIndexColumn(tblInfo *model.TableInfo, col *model.ColumnInfo) bool {
	indexesToChange := FindRelatedIndexesToChange(tblInfo, col.Name)
	for _, idx := range indexesToChange {
		if idx.IndexInfo.VectorInfo != nil {
			return true
		}
	}
	return false
}

func (w *worker) onModifyColumn(jobCtx *jobContext, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	dbInfo, tblInfo, oldCol, modifyInfo, err := getModifyColumnInfo(t, job)
	if err != nil {
		return ver, err
	}

	if job.IsRollingback() {
		// For those column-type-change jobs which don't reorg the data.
		if !needChangeColumnData(oldCol, modifyInfo.newCol) {
			return rollbackModifyColumnJob(jobCtx, t, tblInfo, job, modifyInfo.newCol, oldCol, modifyInfo.modifyColumnTp)
		}
		// For those column-type-change jobs which reorg the data.
		return rollbackModifyColumnJobWithData(jobCtx, t, tblInfo, job, oldCol, modifyInfo)
	}

	// If we want to rename the column name, we need to check whether it already exists.
	if modifyInfo.newCol.Name.L != modifyInfo.oldColName.L {
		c := model.FindColumnInfo(tblInfo.Columns, modifyInfo.newCol.Name.L)
		if c != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(infoschema.ErrColumnExists.GenWithStackByArgs(modifyInfo.newCol.Name))
		}
	}

	failpoint.Inject("uninitializedOffsetAndState", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			if modifyInfo.newCol.State != model.StatePublic {
				failpoint.Return(ver, errors.New("the column state is wrong"))
			}
		}
	})

	err = checkAndApplyAutoRandomBits(jobCtx, t, dbInfo, tblInfo, oldCol, modifyInfo.newCol, modifyInfo.updatedAutoRandomBits)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if !needChangeColumnData(oldCol, modifyInfo.newCol) {
		return w.doModifyColumn(jobCtx, t, job, dbInfo, tblInfo, modifyInfo.newCol, oldCol, modifyInfo.pos)
	}

	if err = isGeneratedRelatedColumn(tblInfo, modifyInfo.newCol, oldCol); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if tblInfo.Partition != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("table is partition table"))
	}
	if hasVectorIndexColumn(tblInfo, oldCol) {
		return ver, errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("vector indexes on the column"))
	}

	changingCol := modifyInfo.changingCol
	if changingCol == nil {
		newColName := pmodel.NewCIStr(genChangingColumnUniqueName(tblInfo, oldCol))
		if mysql.HasPriKeyFlag(oldCol.GetFlag()) {
			job.State = model.JobStateCancelled
			msg := "this column has primary key flag"
			return ver, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}

		changingCol = modifyInfo.newCol.Clone()
		changingCol.Name = newColName
		changingCol.ChangeStateInfo = &model.ChangeStateInfo{DependencyColumnOffset: oldCol.Offset}

		originDefVal, err := GetOriginDefaultValueForModifyColumn(newReorgExprCtx(), changingCol, oldCol)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = changingCol.SetOriginDefaultValue(originDefVal); err != nil {
			return ver, errors.Trace(err)
		}

		InitAndAddColumnToTable(tblInfo, changingCol)
		indexesToChange := FindRelatedIndexesToChange(tblInfo, oldCol.Name)
		for _, info := range indexesToChange {
			newIdxID := AllocateIndexID(tblInfo)
			if !info.isTemp {
				// We create a temp index for each normal index.
				tmpIdx := info.IndexInfo.Clone()
				tmpIdxName := genChangingIndexUniqueName(tblInfo, info.IndexInfo)
				setIdxIDName(tmpIdx, newIdxID, pmodel.NewCIStr(tmpIdxName))
				SetIdxColNameOffset(tmpIdx.Columns[info.Offset], changingCol)
				tblInfo.Indices = append(tblInfo.Indices, tmpIdx)
			} else {
				// The index is a temp index created by previous modify column job(s).
				// We can overwrite it to reduce reorg cost, because it will be dropped eventually.
				tmpIdx := info.IndexInfo
				oldTempIdxID := tmpIdx.ID
				setIdxIDName(tmpIdx, newIdxID, tmpIdx.Name /* unchanged */)
				SetIdxColNameOffset(tmpIdx.Columns[info.Offset], changingCol)
				modifyInfo.removedIdxs = append(modifyInfo.removedIdxs, oldTempIdxID)
			}
		}
	} else {
		changingCol = model.FindColumnInfoByID(tblInfo.Columns, modifyInfo.changingCol.ID)
		if changingCol == nil {
			logutil.DDLLogger().Error("the changing column has been removed", zap.Error(err))
			job.State = model.JobStateCancelled
			return ver, errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name))
		}
	}

	return w.doModifyColumnTypeWithData(jobCtx, t, job, dbInfo, tblInfo, changingCol, oldCol, modifyInfo.newCol.Name, modifyInfo.pos, modifyInfo.removedIdxs)
}

// rollbackModifyColumnJob rollbacks the job when an error occurs.
func rollbackModifyColumnJob(jobCtx *jobContext, t *meta.Meta, tblInfo *model.TableInfo, job *model.Job, newCol, oldCol *model.ColumnInfo, modifyColumnTp byte) (ver int64, _ error) {
	var err error
	if oldCol.ID == newCol.ID && modifyColumnTp == mysql.TypeNull {
		// field NotNullFlag flag reset.
		tblInfo.Columns[oldCol.Offset].SetFlag(oldCol.GetFlag() &^ mysql.NotNullFlag)
		// field PreventNullInsertFlag flag reset.
		tblInfo.Columns[oldCol.Offset].SetFlag(oldCol.GetFlag() &^ mysql.PreventNullInsertFlag)
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	// For those column-type-change type which doesn't need reorg data, we should also mock the job args for delete range.
	job.Args = []any{[]int64{}, []int64{}}
	return ver, nil
}

func getModifyColumnInfo(t *meta.Meta, job *model.Job) (*model.DBInfo, *model.TableInfo, *model.ColumnInfo, *modifyingColInfo, error) {
	modifyInfo := &modifyingColInfo{pos: &ast.ColumnPosition{}}
	err := job.DecodeArgs(&modifyInfo.newCol, &modifyInfo.oldColName, modifyInfo.pos, &modifyInfo.modifyColumnTp,
		&modifyInfo.updatedAutoRandomBits, &modifyInfo.changingCol, &modifyInfo.changingIdxs, &modifyInfo.removedIdxs)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, modifyInfo, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return nil, nil, nil, modifyInfo, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return nil, nil, nil, modifyInfo, errors.Trace(err)
	}

	oldCol := model.FindColumnInfo(tblInfo.Columns, modifyInfo.oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return nil, nil, nil, modifyInfo, errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(*(modifyInfo.oldColName), tblInfo.Name))
	}

	return dbInfo, tblInfo, oldCol, modifyInfo, errors.Trace(err)
}

// GetOriginDefaultValueForModifyColumn gets the original default value for modifying column.
// Since column type change is implemented as adding a new column then substituting the old one.
// Case exists when update-where statement fetch a NULL for not-null column without any default data,
// it will errors.
// So we set original default value here to prevent this error. If the oldCol has the original default value, we use it.
// Otherwise we set the zero value as original default value.
// Besides, in insert & update records, we have already implement using the casted value of relative column to insert
// rather than the original default value.
func GetOriginDefaultValueForModifyColumn(ctx exprctx.BuildContext, changingCol, oldCol *model.ColumnInfo) (any, error) {
	var err error
	originDefVal := oldCol.GetOriginDefaultValue()
	if originDefVal != nil {
		odv, err := table.CastColumnValue(ctx, types.NewDatum(originDefVal), changingCol, false, false)
		if err != nil {
			logutil.DDLLogger().Info("cast origin default value failed", zap.Error(err))
		}
		if !odv.IsNull() {
			if originDefVal, err = odv.ToString(); err != nil {
				originDefVal = nil
				logutil.DDLLogger().Info("convert default value to string failed", zap.Error(err))
			}
		}
	}
	if originDefVal == nil {
		originDefVal, err = generateOriginDefaultValue(changingCol, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return originDefVal, nil
}

// rollbackModifyColumnJobWithData is used to rollback modify-column job which need to reorg the data.
func rollbackModifyColumnJobWithData(jobCtx *jobContext, t *meta.Meta, tblInfo *model.TableInfo, job *model.Job, oldCol *model.ColumnInfo, modifyInfo *modifyingColInfo) (ver int64, err error) {
	// If the not-null change is included, we should clean the flag info in oldCol.
	if modifyInfo.modifyColumnTp == mysql.TypeNull {
		// Reset NotNullFlag flag.
		tblInfo.Columns[oldCol.Offset].SetFlag(oldCol.GetFlag() &^ mysql.NotNullFlag)
		// Reset PreventNullInsertFlag flag.
		tblInfo.Columns[oldCol.Offset].SetFlag(oldCol.GetFlag() &^ mysql.PreventNullInsertFlag)
	}
	var changingIdxIDs []int64
	if modifyInfo.changingCol != nil {
		changingIdxIDs = buildRelatedIndexIDs(tblInfo, modifyInfo.changingCol.ID)
		// The job is in the middle state. The appended changingCol and changingIndex should
		// be removed from the tableInfo as well.
		removeChangingColAndIdxs(tblInfo, modifyInfo.changingCol.ID)
	}
	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	// Reconstruct the job args to add the temporary index ids into delete range table.
	job.Args = []any{changingIdxIDs, getPartitionIDs(tblInfo)}
	return ver, nil
}

// doModifyColumn updates the column information and reorders all columns. It does not support modifying column data.
func (w *worker) doModifyColumn(
	jobCtx *jobContext, t *meta.Meta, job *model.Job, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	newCol, oldCol *model.ColumnInfo, pos *ast.ColumnPosition) (ver int64, _ error) {
	if oldCol.ID != newCol.ID {
		job.State = model.JobStateRollingback
		return ver, dbterror.ErrColumnInChange.GenWithStackByArgs(oldCol.Name, newCol.ID)
	}
	// Column from null to not null.
	if !mysql.HasNotNullFlag(oldCol.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag()) {
		noPreventNullFlag := !mysql.HasPreventNullInsertFlag(oldCol.GetFlag())

		// We need to check after the flag is set
		if !noPreventNullFlag {
			delayForAsyncCommit()
		}

		// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
		err := modifyColsFromNull2NotNull(w, dbInfo, tblInfo, []*model.ColumnInfo{oldCol}, newCol, oldCol.GetType() != newCol.GetType())
		if err != nil {
			if dbterror.ErrWarnDataTruncated.Equal(err) || dbterror.ErrInvalidUseOfNull.Equal(err) {
				job.State = model.JobStateRollingback
			}
			return ver, err
		}
		// The column should get into prevent null status first.
		if noPreventNullFlag {
			return updateVersionAndTableInfoWithCheck(jobCtx, t, job, tblInfo, true)
		}
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, t, job, tblInfo, false)
	}

	if err := adjustTableInfoAfterModifyColumn(tblInfo, newCol, oldCol, pos); err != nil {
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	childTableInfos, err := adjustForeignKeyChildTableInfoAfterModifyColumn(jobCtx.infoCache, t, job, tblInfo, newCol, oldCol)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, t, job, tblInfo, true, childTableInfos...)
	if err != nil {
		// Modified the type definition of 'null' to 'not null' before this, so rollBack the job when an error occurs.
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	// For those column-type-change type which doesn't need reorg data, we should also mock the job args for delete range.
	job.Args = []any{[]int64{}, []int64{}}
	return ver, nil
}

func adjustTableInfoAfterModifyColumn(
	tblInfo *model.TableInfo, newCol, oldCol *model.ColumnInfo, pos *ast.ColumnPosition) error {
	// We need the latest column's offset and state. This information can be obtained from the store.
	newCol.Offset = oldCol.Offset
	newCol.State = oldCol.State
	if pos != nil && pos.RelativeColumn != nil && oldCol.Name.L == pos.RelativeColumn.Name.L {
		// For cases like `modify column b after b`, it should report this error.
		return errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name))
	}
	destOffset, err := LocateOffsetToMove(oldCol.Offset, pos, tblInfo)
	if err != nil {
		return errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name))
	}
	tblInfo.Columns[oldCol.Offset] = newCol
	tblInfo.MoveColumnInfo(oldCol.Offset, destOffset)
	updateNewIdxColsNameOffset(tblInfo.Indices, oldCol.Name, newCol)
	updateFKInfoWhenModifyColumn(tblInfo, oldCol.Name, newCol.Name)
	updateTTLInfoWhenModifyColumn(tblInfo, oldCol.Name, newCol.Name)
	return nil
}

func updateFKInfoWhenModifyColumn(tblInfo *model.TableInfo, oldCol, newCol pmodel.CIStr) {
	if oldCol.L == newCol.L {
		return
	}
	for _, fk := range tblInfo.ForeignKeys {
		for i := range fk.Cols {
			if fk.Cols[i].L == oldCol.L {
				fk.Cols[i] = newCol
			}
		}
	}
}

func updateTTLInfoWhenModifyColumn(tblInfo *model.TableInfo, oldCol, newCol pmodel.CIStr) {
	if oldCol.L == newCol.L {
		return
	}
	if tblInfo.TTLInfo != nil {
		if tblInfo.TTLInfo.ColumnName.L == oldCol.L {
			tblInfo.TTLInfo.ColumnName = newCol
		}
	}
}

func adjustForeignKeyChildTableInfoAfterModifyColumn(infoCache *infoschema.InfoCache, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, newCol, oldCol *model.ColumnInfo) ([]schemaIDAndTableInfo, error) {
	if !variable.EnableForeignKey.Load() || newCol.Name.L == oldCol.Name.L {
		return nil, nil
	}
	is := infoCache.GetLatest()
	referredFKs := is.GetTableReferredForeignKeys(job.SchemaName, tblInfo.Name.L)
	if len(referredFKs) == 0 {
		return nil, nil
	}
	fkh := newForeignKeyHelper()
	fkh.addLoadedTable(job.SchemaName, tblInfo.Name.L, job.SchemaID, tblInfo)
	for _, referredFK := range referredFKs {
		info, err := fkh.getTableFromStorage(is, t, referredFK.ChildSchema, referredFK.ChildTable)
		if err != nil {
			if infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err) {
				continue
			}
			return nil, err
		}
		fkInfo := model.FindFKInfoByName(info.tblInfo.ForeignKeys, referredFK.ChildFKName.L)
		if fkInfo == nil {
			continue
		}
		for i := range fkInfo.RefCols {
			if fkInfo.RefCols[i].L == oldCol.Name.L {
				fkInfo.RefCols[i] = newCol.Name
			}
		}
	}
	infoList := make([]schemaIDAndTableInfo, 0, len(fkh.loaded))
	for _, info := range fkh.loaded {
		if info.tblInfo.ID == tblInfo.ID {
			continue
		}
		infoList = append(infoList, info)
	}
	return infoList, nil
}

func (w *worker) doModifyColumnTypeWithData(
	jobCtx *jobContext, t *meta.Meta, job *model.Job,
	dbInfo *model.DBInfo, tblInfo *model.TableInfo, changingCol, oldCol *model.ColumnInfo,
	colName pmodel.CIStr, pos *ast.ColumnPosition, rmIdxIDs []int64) (ver int64, _ error) {
	var err error
	originalState := changingCol.State
	targetCol := changingCol.Clone()
	targetCol.Name = colName
	changingIdxs := buildRelatedIndexInfos(tblInfo, changingCol.ID)
	switch changingCol.State {
	case model.StateNone:
		// Column from null to not null.
		if !mysql.HasNotNullFlag(oldCol.GetFlag()) && mysql.HasNotNullFlag(changingCol.GetFlag()) {
			// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
			err := modifyColsFromNull2NotNull(w, dbInfo, tblInfo, []*model.ColumnInfo{oldCol}, targetCol, oldCol.GetType() != changingCol.GetType())
			if err != nil {
				if dbterror.ErrWarnDataTruncated.Equal(err) || dbterror.ErrInvalidUseOfNull.Equal(err) {
					job.State = model.JobStateRollingback
				}
				return ver, err
			}
		}
		// none -> delete only
		updateChangingObjState(changingCol, changingIdxs, model.StateDeleteOnly)
		failpoint.Inject("mockInsertValueAfterCheckNull", func(val failpoint.Value) {
			if valStr, ok := val.(string); ok {
				var sctx sessionctx.Context
				sctx, err := w.sessPool.Get()
				if err != nil {
					failpoint.Return(ver, err)
				}
				defer w.sessPool.Put(sctx)

				ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
				//nolint:forcetypeassert
				_, _, err = sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, valStr)
				if err != nil {
					job.State = model.JobStateCancelled
					failpoint.Return(ver, err)
				}
			}
		})
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Make sure job args change after `updateVersionAndTableInfoWithCheck`, otherwise, the job args will
		// be updated in `updateDDLJob` even if it meets an error in `updateVersionAndTableInfoWithCheck`.
		job.SchemaState = model.StateDeleteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn, job.SchemaName, tblInfo.Name.String()).Set(0)
		job.Args = append(job.Args, changingCol, changingIdxs, rmIdxIDs)
	case model.StateDeleteOnly:
		// Column from null to not null.
		if !mysql.HasNotNullFlag(oldCol.GetFlag()) && mysql.HasNotNullFlag(changingCol.GetFlag()) {
			// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
			err := modifyColsFromNull2NotNull(w, dbInfo, tblInfo, []*model.ColumnInfo{oldCol}, targetCol, oldCol.GetType() != changingCol.GetType())
			if err != nil {
				if dbterror.ErrWarnDataTruncated.Equal(err) || dbterror.ErrInvalidUseOfNull.Equal(err) {
					job.State = model.JobStateRollingback
				}
				return ver, err
			}
		}
		// delete only -> write only
		updateChangingObjState(changingCol, changingIdxs, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		failpoint.InjectCall("afterModifyColumnStateDeleteOnly", job.ID)
	case model.StateWriteOnly:
		// write only -> reorganization
		updateChangingObjState(changingCol, changingIdxs, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), dbInfo.ID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		var done bool
		if job.MultiSchemaInfo != nil {
			done, ver, err = doReorgWorkForModifyColumnMultiSchema(w, jobCtx, t, job, tbl, oldCol, changingCol, changingIdxs)
		} else {
			done, ver, err = doReorgWorkForModifyColumn(w, jobCtx, t, job, tbl, oldCol, changingCol, changingIdxs)
		}
		if !done {
			return ver, err
		}

		rmIdxIDs = append(buildRelatedIndexIDs(tblInfo, oldCol.ID), rmIdxIDs...)

		err = adjustTableInfoAfterModifyColumnWithData(tblInfo, pos, oldCol, changingCol, colName, changingIdxs)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}

		updateChangingObjState(changingCol, changingIdxs, model.StatePublic)
		ver, err = updateVersionAndTableInfo(jobCtx, t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		// Refactor the job args to add the old index ids into delete range table.
		job.Args = []any{rmIdxIDs, getPartitionIDs(tblInfo)}
		modifyColumnEvent := &statsutil.DDLEvent{
			SchemaChangeEvent: ddlutil.NewModifyColumnEvent(tblInfo, []*model.ColumnInfo{changingCol}),
		}
		asyncNotifyEvent(jobCtx, modifyColumnEvent, job)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", changingCol.State)
	}

	return ver, errors.Trace(err)
}

func doReorgWorkForModifyColumnMultiSchema(w *worker, jobCtx *jobContext, t *meta.Meta, job *model.Job, tbl table.Table,
	oldCol, changingCol *model.ColumnInfo, changingIdxs []*model.IndexInfo) (done bool, ver int64, err error) {
	if job.MultiSchemaInfo.Revertible {
		done, ver, err = doReorgWorkForModifyColumn(w, jobCtx, t, job, tbl, oldCol, changingCol, changingIdxs)
		if done {
			// We need another round to wait for all the others sub-jobs to finish.
			job.MarkNonRevertible()
		}
		// We need another round to run the reorg process.
		return false, ver, err
	}
	// Non-revertible means all the sub jobs finished.
	return true, ver, err
}

func doReorgWorkForModifyColumn(w *worker, jobCtx *jobContext, t *meta.Meta, job *model.Job, tbl table.Table,
	oldCol, changingCol *model.ColumnInfo, changingIdxs []*model.IndexInfo) (done bool, ver int64, err error) {
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		err = errors.Trace(err1)
		return
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta),
		jobCtx, rh, job, dbInfo, tbl, BuildElements(changingCol, changingIdxs), false)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}

	// Inject a failpoint so that we can pause here and do verification on other components.
	// With a failpoint-enabled version of TiDB, you can trigger this failpoint by the following command:
	// enable: curl -X PUT -d "pause" "http://127.0.0.1:10080/fail/github.com/pingcap/tidb/pkg/ddl/mockDelayInModifyColumnTypeWithData".
	// disable: curl -X DELETE "http://127.0.0.1:10080/fail/github.com/pingcap/tidb/pkg/ddl/mockDelayInModifyColumnTypeWithData"
	failpoint.Inject("mockDelayInModifyColumnTypeWithData", func() {})
	err = w.runReorgJob(reorgInfo, tbl.Meta(), func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onModifyColumn",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("modify table `%v` column `%v` panic", tbl.Meta().Name, oldCol.Name)
			}, false)
		// Use old column name to generate less confusing error messages.
		changingColCpy := changingCol.Clone()
		changingColCpy.Name = oldCol.Name
		return w.updateCurrentElement(tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}

		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// If timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.IsTxnRetryableError(err) || dbterror.ErrNotOwner.Equal(err) {
			return false, ver, errors.Trace(err)
		}
		if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.DDLLogger().Warn("run modify column job failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
				zap.String("job", job.String()), zap.Error(err1))
		}
		logutil.DDLLogger().Warn("run modify column job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
		job.State = model.JobStateRollingback
		return false, ver, errors.Trace(err)
	}
	return true, ver, nil
}

func adjustTableInfoAfterModifyColumnWithData(tblInfo *model.TableInfo, pos *ast.ColumnPosition,
	oldCol, changingCol *model.ColumnInfo, newName pmodel.CIStr, changingIdxs []*model.IndexInfo) (err error) {
	if pos != nil && pos.RelativeColumn != nil && oldCol.Name.L == pos.RelativeColumn.Name.L {
		// For cases like `modify column b after b`, it should report this error.
		return errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name))
	}
	internalColName := changingCol.Name
	changingCol = replaceOldColumn(tblInfo, oldCol, changingCol, newName)
	if len(changingIdxs) > 0 {
		updateNewIdxColsNameOffset(changingIdxs, internalColName, changingCol)
		indexesToRemove := filterIndexesToRemove(changingIdxs, newName, tblInfo)
		replaceOldIndexes(tblInfo, indexesToRemove)
	}
	if tblInfo.TTLInfo != nil {
		updateTTLInfoWhenModifyColumn(tblInfo, oldCol.Name, changingCol.Name)
	}
	// Move the new column to a correct offset.
	destOffset, err := LocateOffsetToMove(changingCol.Offset, pos, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo.MoveColumnInfo(changingCol.Offset, destOffset)
	return nil
}

func checkModifyColumnWithGeneratedColumnsConstraint(allCols []*table.Column, oldColName pmodel.CIStr) error {
	for _, col := range allCols {
		if col.GeneratedExpr == nil {
			continue
		}
		dependedColNames := FindColumnNamesInExpr(col.GeneratedExpr.Internal())
		for _, name := range dependedColNames {
			if name.Name.L == oldColName.L {
				if col.Hidden {
					return dbterror.ErrDependentByFunctionalIndex.GenWithStackByArgs(oldColName.O)
				}
				return dbterror.ErrDependentByGeneratedColumn.GenWithStackByArgs(oldColName.O)
			}
		}
	}
	return nil
}

// GetModifiableColumnJob returns a DDL job of model.ActionModifyColumn.
func GetModifiableColumnJob(
	ctx context.Context,
	sctx sessionctx.Context,
	is infoschema.InfoSchema, // WARN: is maybe nil here.
	ident ast.Ident,
	originalColName pmodel.CIStr,
	schema *model.DBInfo,
	t table.Table,
	spec *ast.AlterTableSpec,
) (*model.Job, error) {
	var err error
	specNewColumn := spec.NewColumns[0]

	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name)
	}
	newColName := specNewColumn.Name.Name
	if newColName.L == model.ExtraHandleName.L {
		return nil, dbterror.ErrWrongColumnName.GenWithStackByArgs(newColName.L)
	}
	errG := checkModifyColumnWithGeneratedColumnsConstraint(t.Cols(), originalColName)

	// If we want to rename the column name, we need to check whether it already exists.
	if newColName.L != originalColName.L {
		c := table.FindCol(t.Cols(), newColName.L)
		if c != nil {
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(newColName)
		}

		// And also check the generated columns dependency, if some generated columns
		// depend on this column, we can't rename the column name.
		if errG != nil {
			return nil, errors.Trace(errG)
		}
	}

	// Constraints in the new column means adding new constraints. Errors should thrown,
	// which will be done by `processColumnOptions` later.
	if specNewColumn.Tp == nil {
		// Make sure the column definition is simple field type.
		return nil, errors.Trace(dbterror.ErrUnsupportedModifyColumn)
	}

	if err = checkColumnAttributes(specNewColumn.Name.OrigColName(), specNewColumn.Tp); err != nil {
		return nil, errors.Trace(err)
	}

	newCol := table.ToColumn(&model.ColumnInfo{
		ID: col.ID,
		// We use this PR(https://github.com/pingcap/tidb/pull/6274) as the dividing line to define whether it is a new version or an old version TiDB.
		// The old version TiDB initializes the column's offset and state here.
		// The new version TiDB doesn't initialize the column's offset and state, and it will do the initialization in run DDL function.
		// When we do the rolling upgrade the following may happen:
		// a new version TiDB builds the DDL job that doesn't be set the column's offset and state,
		// and the old version TiDB is the DDL owner, it doesn't get offset and state from the store. Then it will encounter errors.
		// So here we set offset and state to support the rolling upgrade.
		Offset:                col.Offset,
		State:                 col.State,
		OriginDefaultValue:    col.OriginDefaultValue,
		OriginDefaultValueBit: col.OriginDefaultValueBit,
		FieldType:             *specNewColumn.Tp,
		Name:                  newColName,
		Version:               col.Version,
	})

	if err = ProcessColumnCharsetAndCollation(sctx, col, newCol, t.Meta(), specNewColumn, schema); err != nil {
		return nil, err
	}

	if err = checkModifyColumnWithForeignKeyConstraint(is, schema.Name.L, t.Meta(), col.ColumnInfo, newCol.ColumnInfo); err != nil {
		return nil, errors.Trace(err)
	}

	// Copy index related options to the new spec.
	indexFlags := col.FieldType.GetFlag() & (mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	newCol.FieldType.AddFlag(indexFlags)
	if mysql.HasPriKeyFlag(col.FieldType.GetFlag()) {
		newCol.FieldType.AddFlag(mysql.NotNullFlag)
		// TODO: If user explicitly set NULL, we should throw error ErrPrimaryCantHaveNull.
	}

	if err = ProcessModifyColumnOptions(sctx, newCol, specNewColumn.Options); err != nil {
		return nil, errors.Trace(err)
	}

	if err = checkModifyTypes(&col.FieldType, &newCol.FieldType, isColumnWithIndex(col.Name.L, t.Meta().Indices)); err != nil {
		if strings.Contains(err.Error(), "Unsupported modifying collation") {
			colErrMsg := "Unsupported modifying collation of column '%s' from '%s' to '%s' when index is defined on it."
			err = dbterror.ErrUnsupportedModifyCollation.GenWithStack(colErrMsg, col.Name.L, col.GetCollate(), newCol.GetCollate())
		}
		return nil, errors.Trace(err)
	}
	needChangeColData := needChangeColumnData(col.ColumnInfo, newCol.ColumnInfo)
	if needChangeColData {
		if err = isGeneratedRelatedColumn(t.Meta(), newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		if t.Meta().Partition != nil {
			return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("table is partition table")
		}
		if hasVectorIndexColumn(t.Meta(), col.ColumnInfo) {
			return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("vector indexes on the column")
		}
	}

	// Check that the column change does not affect the partitioning column
	// It must keep the same type, int [unsigned], [var]char, date[time]
	if t.Meta().Partition != nil {
		pt, ok := t.(table.PartitionedTable)
		if !ok {
			// Should never happen!
			return nil, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
		}
		isPartitioningColumn := false
		for _, name := range pt.GetPartitionColumnNames() {
			if strings.EqualFold(name.L, col.Name.L) {
				isPartitioningColumn = true
				break
			}
		}
		if isPartitioningColumn {
			// TODO: update the partitioning columns with new names if column is renamed
			// Would be an extension from MySQL which does not support it.
			if col.Name.L != newCol.Name.L {
				return nil, dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(col.Name.L)
			}
			if !isColTypeAllowedAsPartitioningCol(t.Meta().Partition.Type, newCol.FieldType) {
				return nil, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
			}
			pi := pt.Meta().GetPartitionInfo()
			if len(pi.Columns) == 0 {
				// non COLUMNS partitioning, only checks INTs, not their actual range
				// There are many edge cases, like when truncating SQL Mode is allowed
				// which will change the partitioning expression value resulting in a
				// different partition. Better be safe and not allow decreasing of length.
				// TODO: Should we allow it in strict mode? Wait for a use case / request.
				if newCol.FieldType.GetFlen() < col.FieldType.GetFlen() {
					return nil, dbterror.ErrUnsupportedModifyCollation.GenWithStack("Unsupported modify column, decreasing length of int may result in truncation and change of partition")
				}
			}
			// Basically only allow changes of the length/decimals for the column
			// Note that enum is not allowed, so elems are not checked
			// TODO: support partition by ENUM
			if newCol.FieldType.EvalType() != col.FieldType.EvalType() ||
				newCol.FieldType.GetFlag() != col.FieldType.GetFlag() ||
				newCol.FieldType.GetCollate() != col.FieldType.GetCollate() ||
				newCol.FieldType.GetCharset() != col.FieldType.GetCharset() {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't change the partitioning column, since it would require reorganize all partitions")
			}
			// Generate a new PartitionInfo and validate it together with the new column definition
			// Checks if all partition definition values are compatible.
			// Similar to what buildRangePartitionDefinitions would do in terms of checks.

			tblInfo := pt.Meta()
			newTblInfo := *tblInfo
			// Replace col with newCol and see if we can generate a new SHOW CREATE TABLE
			// and reparse it and build new partition definitions (which will do additional
			// checks columns vs partition definition values
			newCols := make([]*model.ColumnInfo, 0, len(newTblInfo.Columns))
			for _, c := range newTblInfo.Columns {
				if c.ID == col.ID {
					newCols = append(newCols, newCol.ColumnInfo)
					continue
				}
				newCols = append(newCols, c)
			}
			newTblInfo.Columns = newCols

			var buf bytes.Buffer
			AppendPartitionInfo(tblInfo.GetPartitionInfo(), &buf, mysql.ModeNone)
			// The parser supports ALTER TABLE ... PARTITION BY ... even if the ddl code does not yet :)
			// Ignoring warnings
			stmt, _, err := parser.New().ParseSQL("ALTER TABLE t " + buf.String())
			if err != nil {
				// Should never happen!
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
			}
			at, ok := stmt[0].(*ast.AlterTableStmt)
			if !ok || len(at.Specs) != 1 || at.Specs[0].Partition == nil {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
			}
			pAst := at.Specs[0].Partition
			_, err = buildPartitionDefinitionsInfo(
				exprctx.CtxWithHandleTruncateErrLevel(sctx.GetExprCtx(), errctx.LevelError),
				pAst.Definitions, &newTblInfo, uint64(len(newTblInfo.Partition.Definitions)),
			)
			if err != nil {
				return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStack("New column does not match partition definitions: %s", err.Error())
			}
		}
	}

	// We don't support modifying column from not_auto_increment to auto_increment.
	if !mysql.HasAutoIncrementFlag(col.GetFlag()) && mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't set auto_increment")
	}
	// Not support auto id with default value.
	if mysql.HasAutoIncrementFlag(newCol.GetFlag()) && newCol.GetDefaultValue() != nil {
		return nil, dbterror.ErrInvalidDefaultValue.GenWithStackByArgs(newCol.Name)
	}
	// Disallow modifying column from auto_increment to not auto_increment if the session variable `AllowRemoveAutoInc` is false.
	if !sctx.GetSessionVars().AllowRemoveAutoInc && mysql.HasAutoIncrementFlag(col.GetFlag()) && !mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	var modifyColumnTp byte
	if !mysql.HasNotNullFlag(col.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag()) {
		if err = checkForNullValue(ctx, sctx, true, ident.Schema, ident.Name, newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		// `modifyColumnTp` indicates that there is a type modification.
		modifyColumnTp = mysql.TypeNull
	}

	if err = checkColumnWithIndexConstraint(t.Meta(), col.ColumnInfo, newCol.ColumnInfo); err != nil {
		return nil, err
	}

	// As same with MySQL, we don't support modifying the stored status for generated columns.
	if err = checkModifyGeneratedColumn(sctx, schema.Name, t, col, newCol, specNewColumn, spec.Position); err != nil {
		return nil, errors.Trace(err)
	}
	if errG != nil {
		// According to issue https://github.com/pingcap/tidb/issues/24321,
		// changing the type of a column involving generating a column is prohibited.
		return nil, dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs(errG.Error())
	}

	if t.Meta().TTLInfo != nil {
		// the column referenced by TTL should be a time type
		if t.Meta().TTLInfo.ColumnName.L == originalColName.L && !types.IsTypeTime(newCol.ColumnInfo.FieldType.GetType()) {
			return nil, errors.Trace(dbterror.ErrUnsupportedColumnInTTLConfig.GenWithStackByArgs(newCol.ColumnInfo.Name.O))
		}
	}

	var newAutoRandBits uint64
	if newAutoRandBits, err = checkAutoRandom(t.Meta(), col, specNewColumn); err != nil {
		return nil, errors.Trace(err)
	}

	txn, err := sctx.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bdrRole, err := meta.NewMeta(txn).GetBDRRole()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if bdrRole == string(ast.BDRRolePrimary) &&
		deniedByBDRWhenModifyColumn(newCol.FieldType, col.FieldType, specNewColumn.Options) {
		return nil, dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
	}

	job := &model.Job{
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionModifyColumn,
		BinlogInfo:     &model.HistoryInfo{},
		ReorgMeta:      NewDDLReorgMeta(sctx),
		CtxVars:        []any{needChangeColData},
		Args:           []any{&newCol.ColumnInfo, originalColName, spec.Position, modifyColumnTp, newAutoRandBits},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
	}
	return job, nil
}

func needChangeColumnData(oldCol, newCol *model.ColumnInfo) bool {
	toUnsigned := mysql.HasUnsignedFlag(newCol.GetFlag())
	originUnsigned := mysql.HasUnsignedFlag(oldCol.GetFlag())
	needTruncationOrToggleSign := func() bool {
		return (newCol.GetFlen() > 0 && (newCol.GetFlen() < oldCol.GetFlen() || newCol.GetDecimal() < oldCol.GetDecimal())) ||
			(toUnsigned != originUnsigned)
	}
	// Ignore the potential max display length represented by integer's flen, use default flen instead.
	defaultOldColFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(oldCol.GetType())
	defaultNewColFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(newCol.GetType())
	needTruncationOrToggleSignForInteger := func() bool {
		return (defaultNewColFlen > 0 && defaultNewColFlen < defaultOldColFlen) || (toUnsigned != originUnsigned)
	}

	// Deal with the same type.
	if oldCol.GetType() == newCol.GetType() {
		switch oldCol.GetType() {
		case mysql.TypeNewDecimal:
			// Since type decimal will encode the precision, frac, negative(signed) and wordBuf into storage together, there is no short
			// cut to eliminate data reorg change for column type change between decimal.
			return oldCol.GetFlen() != newCol.GetFlen() || oldCol.GetDecimal() != newCol.GetDecimal() || toUnsigned != originUnsigned
		case mysql.TypeEnum, mysql.TypeSet:
			return IsElemsChangedToModifyColumn(oldCol.GetElems(), newCol.GetElems())
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return toUnsigned != originUnsigned
		case mysql.TypeString:
			// Due to the behavior of padding \x00 at binary type, always change column data when binary length changed
			if types.IsBinaryStr(&oldCol.FieldType) {
				return newCol.GetFlen() != oldCol.GetFlen()
			}
		case mysql.TypeTiDBVectorFloat32:
			return newCol.GetFlen() != types.UnspecifiedLength && oldCol.GetFlen() != newCol.GetFlen()
		}

		return needTruncationOrToggleSign()
	}

	if ConvertBetweenCharAndVarchar(oldCol.GetType(), newCol.GetType()) {
		return true
	}

	// Deal with the different type.
	switch oldCol.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch newCol.GetType() {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			return needTruncationOrToggleSign()
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch newCol.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return needTruncationOrToggleSignForInteger()
		}
		// conversion between float and double needs reorganization, see issue #31372
	}

	return true
}

// ConvertBetweenCharAndVarchar check whether column converted between char and varchar
// TODO: it is used for plugins. so change plugin's using and remove it.
func ConvertBetweenCharAndVarchar(oldCol, newCol byte) bool {
	return types.ConvertBetweenCharAndVarchar(oldCol, newCol)
}

// IsElemsChangedToModifyColumn check elems changed
func IsElemsChangedToModifyColumn(oldElems, newElems []string) bool {
	if len(newElems) < len(oldElems) {
		return true
	}
	for index, oldElem := range oldElems {
		newElem := newElems[index]
		if oldElem != newElem {
			return true
		}
	}
	return false
}

// ProcessColumnCharsetAndCollation process column charset and collation
func ProcessColumnCharsetAndCollation(sctx sessionctx.Context, col *table.Column, newCol *table.Column, meta *model.TableInfo, specNewColumn *ast.ColumnDef, schema *model.DBInfo) error {
	var chs, coll string
	var err error
	// TODO: Remove it when all table versions are greater than or equal to TableInfoVersion1.
	// If newCol's charset is empty and the table's version less than TableInfoVersion1,
	// we will not modify the charset of the column. This behavior is not compatible with MySQL.
	if len(newCol.FieldType.GetCharset()) == 0 && meta.Version < model.TableInfoVersion1 {
		chs = col.FieldType.GetCharset()
		coll = col.FieldType.GetCollate()
	} else {
		chs, coll, err = getCharsetAndCollateInColumnDef(sctx.GetSessionVars(), specNewColumn)
		if err != nil {
			return errors.Trace(err)
		}
		chs, coll, err = ResolveCharsetCollation(sctx.GetSessionVars(),
			ast.CharsetOpt{Chs: chs, Col: coll},
			ast.CharsetOpt{Chs: meta.Charset, Col: meta.Collate},
			ast.CharsetOpt{Chs: schema.Charset, Col: schema.Collate},
		)
		chs, coll = OverwriteCollationWithBinaryFlag(sctx.GetSessionVars(), specNewColumn, chs, coll)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if err = setCharsetCollationFlenDecimal(&newCol.FieldType, newCol.Name.O, chs, coll, sctx.GetSessionVars()); err != nil {
		return errors.Trace(err)
	}
	decodeEnumSetBinaryLiteralToUTF8(&newCol.FieldType, chs)
	return nil
}

// checkColumnWithIndexConstraint is used to check the related index constraint of the modified column.
// Index has a max-prefix-length constraint. eg: a varchar(100), index idx(a), modifying column a to a varchar(4000)
// will cause index idx to break the max-prefix-length constraint.
func checkColumnWithIndexConstraint(tbInfo *model.TableInfo, originalCol, newCol *model.ColumnInfo) error {
	columns := make([]*model.ColumnInfo, 0, len(tbInfo.Columns))
	columns = append(columns, tbInfo.Columns...)
	// Replace old column with new column.
	for i, col := range columns {
		if col.Name.L != originalCol.Name.L {
			continue
		}
		columns[i] = newCol.Clone()
		columns[i].Name = originalCol.Name
		break
	}

	pkIndex := tables.FindPrimaryIndex(tbInfo)

	checkOneIndex := func(indexInfo *model.IndexInfo) (err error) {
		var modified bool
		for _, col := range indexInfo.Columns {
			if col.Name.L == originalCol.Name.L {
				modified = true
				break
			}
		}
		if !modified {
			return
		}
		err = checkIndexInModifiableColumns(columns, indexInfo.Columns, indexInfo.VectorInfo != nil)
		if err != nil {
			return
		}
		err = checkIndexPrefixLength(columns, indexInfo.Columns)
		return
	}

	// Check primary key first.
	var err error

	if pkIndex != nil {
		err = checkOneIndex(pkIndex)
		if err != nil {
			return err
		}
	}

	// Check secondary indexes.
	for _, indexInfo := range tbInfo.Indices {
		if indexInfo.Primary {
			continue
		}
		// the second param should always be set to true, check index length only if it was modified
		// checkOneIndex needs one param only.
		err = checkOneIndex(indexInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkIndexInModifiableColumns(columns []*model.ColumnInfo, idxColumns []*model.IndexColumn, isVectorIndex bool) error {
	for _, ic := range idxColumns {
		col := model.FindColumnInfo(columns, ic.Name.L)
		if col == nil {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		prefixLength := types.UnspecifiedLength
		if types.IsTypePrefixable(col.FieldType.GetType()) && col.FieldType.GetFlen() > ic.Length {
			// When the index column is changed, prefix length is only valid
			// if the type is still prefixable and larger than old prefix length.
			prefixLength = ic.Length
		}
		if err := checkIndexColumn(nil, col, prefixLength, isVectorIndex); err != nil {
			return err
		}
	}
	return nil
}

// checkModifyTypes checks if the 'origin' type can be modified to 'to' type no matter directly change
// or change by reorg. It returns error if the two types are incompatible and correlated change are not
// supported. However, even the two types can be change, if the "origin" type contains primary key, error will be returned.
func checkModifyTypes(origin *types.FieldType, to *types.FieldType, needRewriteCollationData bool) error {
	canReorg, err := types.CheckModifyTypeCompatible(origin, to)
	if err != nil {
		if !canReorg {
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(err.Error()))
		}
		if mysql.HasPriKeyFlag(origin.GetFlag()) {
			msg := "this column has primary key flag"
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}
	}

	err = checkModifyCharsetAndCollation(to.GetCharset(), to.GetCollate(), origin.GetCharset(), origin.GetCollate(), needRewriteCollationData)

	if err != nil {
		if to.GetCharset() == charset.CharsetGBK || origin.GetCharset() == charset.CharsetGBK {
			return errors.Trace(err)
		}
		// column type change can handle the charset change between these two types in the process of the reorg.
		if dbterror.ErrUnsupportedModifyCharset.Equal(err) && canReorg {
			return nil
		}
	}
	return errors.Trace(err)
}

// ProcessModifyColumnOptions process column options.
func ProcessModifyColumnOptions(ctx sessionctx.Context, col *table.Column, options []*ast.ColumnOption) error {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutSchemaName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	var hasDefaultValue, setOnUpdateNow bool
	var err error
	var hasNullFlag bool
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			hasDefaultValue, err = SetDefaultValue(ctx, col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionComment:
			err := setColumnComment(ctx, col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionNotNull:
			col.AddFlag(mysql.NotNullFlag)
		case ast.ColumnOptionNull:
			hasNullFlag = true
			col.DelFlag(mysql.NotNullFlag)
		case ast.ColumnOptionAutoIncrement:
			col.AddFlag(mysql.AutoIncrementFlag)
		case ast.ColumnOptionPrimaryKey:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStack("can't change column constraint (PRIMARY KEY)"))
		case ast.ColumnOptionUniqKey:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStack("can't change column constraint (UNIQUE KEY)"))
		case ast.ColumnOptionOnUpdate:
			// TODO: Support other time functions.
			if !(col.GetType() == mysql.TypeTimestamp || col.GetType() == mysql.TypeDatetime) {
				return dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			if !expression.IsValidCurrentTimestampExpr(opt.Expr, &col.FieldType) {
				return dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			col.AddFlag(mysql.OnUpdateNowFlag)
			setOnUpdateNow = true
		case ast.ColumnOptionGenerated:
			sb.Reset()
			err = opt.Expr.Restore(restoreCtx)
			if err != nil {
				return errors.Trace(err)
			}
			col.GeneratedExprString = sb.String()
			col.GeneratedStored = opt.Stored
			col.Dependences = make(map[string]struct{})
			// Only used by checkModifyGeneratedColumn, there is no need to set a ctor for it.
			col.GeneratedExpr = table.NewClonableExprNode(nil, opt.Expr)
			for _, colName := range FindColumnNamesInExpr(opt.Expr) {
				col.Dependences[colName.Name.L] = struct{}{}
			}
		case ast.ColumnOptionCollate:
			col.SetCollate(opt.StrValue)
		case ast.ColumnOptionReference:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with references"))
		case ast.ColumnOptionFulltext:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with full text"))
		case ast.ColumnOptionCheck:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with check"))
		// Ignore ColumnOptionAutoRandom. It will be handled later.
		case ast.ColumnOptionAutoRandom:
		default:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(fmt.Sprintf("unknown column option type: %d", opt.Tp)))
		}
	}

	if err = processAndCheckDefaultValueAndColumn(ctx, col, nil, hasDefaultValue, setOnUpdateNow, hasNullFlag); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkAutoRandom(tableInfo *model.TableInfo, originCol *table.Column, specNewColumn *ast.ColumnDef) (uint64, error) {
	var oldShardBits, oldRangeBits uint64
	if isClusteredPKColumn(originCol, tableInfo) {
		oldShardBits = tableInfo.AutoRandomBits
		oldRangeBits = tableInfo.AutoRandomRangeBits
	}
	newShardBits, newRangeBits, err := extractAutoRandomBitsFromColDef(specNewColumn)
	if err != nil {
		return 0, errors.Trace(err)
	}
	switch {
	case oldShardBits == newShardBits:
	case oldShardBits < newShardBits:
		addingAutoRandom := oldShardBits == 0
		if addingAutoRandom {
			convFromAutoInc := mysql.HasAutoIncrementFlag(originCol.GetFlag()) && originCol.IsPKHandleColumn(tableInfo)
			if !convFromAutoInc {
				return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterChangeFromAutoInc)
			}
		}
		if autoid.AutoRandomShardBitsMax < newShardBits {
			errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg,
				autoid.AutoRandomShardBitsMax, newShardBits, specNewColumn.Name.Name.O)
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
		}
		// increasing auto_random shard bits is allowed.
	case oldShardBits > newShardBits:
		if newShardBits == 0 {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterErrMsg)
		}
		return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomDecreaseBitErrMsg)
	}

	modifyingAutoRandCol := oldShardBits > 0 || newShardBits > 0
	if modifyingAutoRandCol {
		// Disallow changing the column field type.
		if originCol.GetType() != specNewColumn.Tp.GetType() {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomModifyColTypeErrMsg)
		}
		if originCol.GetType() != mysql.TypeLonglong {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(fmt.Sprintf(autoid.AutoRandomOnNonBigIntColumn, types.TypeStr(originCol.GetType())))
		}
		// Disallow changing from auto_random to auto_increment column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionAutoIncrement) {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
		}
		// Disallow specifying a default value on auto_random column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionDefaultValue) {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
	}
	if rangeBitsIsChanged(oldRangeBits, newRangeBits) {
		return 0, dbterror.ErrInvalidAutoRandom.FastGenByArgs(autoid.AutoRandomUnsupportedAlterRangeBits)
	}
	return newShardBits, nil
}

func isClusteredPKColumn(col *table.Column, tblInfo *model.TableInfo) bool {
	switch {
	case tblInfo.PKIsHandle:
		return mysql.HasPriKeyFlag(col.GetFlag())
	case tblInfo.IsCommonHandle:
		pk := tables.FindPrimaryIndex(tblInfo)
		for _, c := range pk.Columns {
			if c.Name.L == col.Name.L {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func rangeBitsIsChanged(oldBits, newBits uint64) bool {
	if oldBits == 0 {
		oldBits = autoid.AutoRandomRangeBitsDefault
	}
	if newBits == 0 {
		newBits = autoid.AutoRandomRangeBitsDefault
	}
	return oldBits != newBits
}

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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

func hasModifyFlag(col *model.ColumnInfo) bool {
	return col.ChangingFieldType != nil || mysql.HasPreventNullInsertFlag(col.GetFlag())
}

func isNullToNotNullChange(oldCol, newCol *model.ColumnInfo) bool {
	return !mysql.HasNotNullFlag(oldCol.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag())
}

func isIntegerChange(from, to *model.ColumnInfo) bool {
	return mysql.IsIntegerType(from.GetType()) && mysql.IsIntegerType(to.GetType())
}

func isCharChange(from, to *model.ColumnInfo) bool {
	return types.IsTypeChar(from.GetType()) && types.IsTypeChar(to.GetType())
}

// getModifyColumnType gets the modify column type.
//  1. ModifyTypeNoReorg: The range of new type is a superset of the old type
//  2. ModifyTypeNoReorgWithCheck: The range of new type is a subset of the old type, but we are running in strict SQL mode.
//     And there is no index on this column.
//     The difference between ModifyTypeNoReorgWithCheck and ModifyTypeNoReorg is that we need some additional checks.
//  3. ModifyTypeIndexReorg: The range of new type is a subset of the old type, and there are indexes on this column.
//  4. ModifyTypeReorg: other
//
// We need to ensure it's compatible with job submitted from older version of TiDB.
func getModifyColumnType(
	args *model.ModifyColumnArgs,
	tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo,
	sqlMode mysql.SQLMode) byte {
	newCol := args.Column
	if noReorgDataStrict(tblInfo, oldCol, args.Column) {
		// It's not NULL->NOTNULL change
		if !isNullToNotNullChange(oldCol, newCol) {
			return model.ModifyTypeNoReorg
		}
		return model.ModifyTypeNoReorgWithCheck
	}

	// For backward compatibility
	if args.ModifyColumnType == mysql.TypeNull {
		return model.ModifyTypeReorg
	}

	// FIXME(joechenrh): handle partition and TiFlash replica case
	if tblInfo.Partition != nil || (tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Count > 0) {
		return model.ModifyTypeReorg
	}

	failpoint.Inject("disableLossyDDLOptimization", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			failpoint.Return(model.ModifyTypeReorg)
		}
	})

	// FIXME(joechenrh): remove this when stats correctness is resolved.
	// Since stats may store bytes encoded by codec.EncodeKey, we should disable the optimization
	// if the same data produces different encoded bytes for the old and new types.
	if (isIntegerChange(oldCol, args.Column) &&
		mysql.HasUnsignedFlag(oldCol.GetFlag()) != mysql.HasUnsignedFlag(args.Column.GetFlag())) ||
		(isCharChange(oldCol, args.Column) &&
			!collate.CompatibleCollate(oldCol.GetCollate(), args.Column.GetCollate())) {
		return model.ModifyTypeReorg
	}

	if !sqlMode.HasStrictMode() {
		return model.ModifyTypeReorg
	}

	if needRowReorg(oldCol, args.Column) {
		return model.ModifyTypeReorg
	}

	relatedIndexes := getRelatedIndexIDs(tblInfo, oldCol.ID, false)
	if len(relatedIndexes) == 0 || !needIndexReorg(oldCol, args.Column) {
		return model.ModifyTypeNoReorgWithCheck
	}
	return model.ModifyTypeIndexReorg
}

func getChangingCol(
	args *model.ModifyColumnArgs,
	tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo,
) (*model.ColumnInfo, error) {
	changingCol := args.ChangingColumn
	if changingCol == nil {
		changingCol = args.Column.Clone()
		changingCol.Name = ast.NewCIStr(model.GenUniqueChangingColumnName(tblInfo, oldCol))
		changingCol.ChangeStateInfo = &model.ChangeStateInfo{DependencyColumnOffset: oldCol.Offset}
		InitAndAddColumnToTable(tblInfo, changingCol)

		var redundantIdxs []int64
		indexesToChange := FindRelatedIndexesToChange(tblInfo, oldCol.Name)
		for _, info := range indexesToChange {
			newIdxID := AllocateIndexID(tblInfo)
			if !info.isTemp {
				// We create a temp index for each normal index.
				tmpIdx := info.IndexInfo.Clone()
				tmpIdx.ID = newIdxID
				tmpIdx.Name = ast.NewCIStr(model.GenUniqueChangingIndexName(tblInfo, info.IndexInfo))
				UpdateIndexCol(tmpIdx.Columns[info.Offset], changingCol)
				tblInfo.Indices = append(tblInfo.Indices, tmpIdx)
			} else {
				// The index is a temp index created by previous modify column job(s).
				// We can overwrite it to reduce reorg cost, because it will be dropped eventually.
				tmpIdx := info.IndexInfo
				oldTempIdxID := tmpIdx.ID
				tmpIdx.ID = newIdxID
				UpdateIndexCol(tmpIdx.Columns[info.Offset], changingCol)
				redundantIdxs = append(redundantIdxs, oldTempIdxID)
			}
		}
		args.RedundantIdxs = redundantIdxs
		return changingCol, nil
	}

	changingCol = model.FindColumnInfoByID(tblInfo.Columns, args.ChangingColumn.ID)
	if changingCol == nil {
		logutil.DDLLogger().Error("the changing column has been removed")
		return nil, errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name))
	}

	return changingCol, nil
}

func initializeChangingIndexes(
	args *model.ModifyColumnArgs,
	tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo,
) error {
	if args.ChangingColumn != nil {
		return nil
	}

	if err := checkColumnAlreadyExists(tblInfo, args); err != nil {
		return errors.Trace(err)
	}

	tmpCol := oldCol.Clone()
	tmpCol.FieldType = args.Column.FieldType

	var redundantIdxs []int64
	indexesToChange := FindRelatedIndexesToChange(tblInfo, oldCol.Name)
	for _, info := range indexesToChange {
		newIdxID := AllocateIndexID(tblInfo)
		if !info.isTemp {
			// We create a temp index for each normal index.
			tmpIdx := info.IndexInfo.Clone()
			tmpIdx.State = model.StateNone
			tmpIdx.ID = newIdxID
			tmpIdx.Name = ast.NewCIStr(model.GenUniqueChangingIndexName(tblInfo, info.IndexInfo))
			tmpIdx.Columns[info.Offset].UseChangingType = true
			UpdateIndexCol(tmpIdx.Columns[info.Offset], tmpCol)
			tblInfo.Indices = append(tblInfo.Indices, tmpIdx)
		} else {
			// The index is a temp index created by previous modify column job(s).
			// We can just allocate a new ID, and gc the old one later.
			tmpIdx := info.IndexInfo
			tmpIdx.State = model.StateNone
			oldTempIdxID := tmpIdx.ID
			tmpIdx.ID = newIdxID
			tmpIdx.Columns[info.Offset].UseChangingType = true
			UpdateIndexCol(tmpIdx.Columns[info.Offset], tmpCol)
			redundantIdxs = append(redundantIdxs, oldTempIdxID)
		}
	}
	args.RedundantIdxs = redundantIdxs
	args.ChangingColumn = args.Column.Clone()
	return nil
}

func (w *worker) onModifyColumn(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifyColumnArgs(job)
	defer func() {
		failpoint.InjectCall("getModifyColumnType", args.ModifyColumnType)
	}()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, tblInfo, oldCol, err := getModifyColumnInfo(jobCtx.metaMut, job, args)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Check index prefix length for the first time
	if job.SchemaState == model.StateNone {
		columns := getReplacedColumns(tblInfo, oldCol, args.Column)
		allIdxs := buildRelatedIndexInfos(tblInfo, oldCol.ID)
		for _, idx := range allIdxs {
			if err := checkIndexInModifiableColumns(columns, idx); err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
	}

	// For first time running this job, or the job from older version,
	// we need to fill the ModifyColumnType.
	if args.ModifyColumnType == model.ModifyTypeNone ||
		args.ModifyColumnType == mysql.TypeNull {
		// Check the modify types again, as the type may be changed by other no-reorg modify-column jobs.
		// For example, there are two DDLs submitted in parallel:
		//      CREATE TABLE t (c VARCHAR(255) charset utf8);
		// 		ALTER TABLE t MODIFY COLUMN c VARCHAR(255) charset utf8mb4;
		// 		ALTER TABLE t MODIFY COLUMN c VARCHAR(100) charset utf8;
		// Previously, the second DDL will be submitted successfully (VARCHAR(255) utf8 -> VARCHAR(100) utf8 is OK)
		// but fail during execution, since the columnID has changed. However, as we now may reuse the old column,
		// we must check the type again here as utf8mb4->utf8 is an invalid change.
		if err = checkModifyTypes(oldCol, args.Column, isColumnWithIndex(oldCol.Name.L, tblInfo.Indices)); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		args.ModifyColumnType = getModifyColumnType(args, tblInfo, oldCol, job.SQLMode)
		logutil.DDLLogger().Info("get type for modify column",
			zap.String("query", job.Query),
			zap.String("oldColumnName", args.OldColumnName.L),
			zap.Int64("oldColumnID", oldCol.ID),
			zap.String("type", model.ModifyTypeToString(args.ModifyColumnType)),
		)
		if oldCol.GetType() == mysql.TypeVarchar && args.Column.GetType() == mysql.TypeString &&
			(args.ModifyColumnType == model.ModifyTypeNoReorgWithCheck ||
				args.ModifyColumnType == model.ModifyTypeIndexReorg) {
			logutil.DDLLogger().Info("meet varchar to char modify column, change type to precheck")
			args.ModifyColumnType = model.ModifyTypePrecheck
		}
	}

	if job.IsRollingback() {
		switch args.ModifyColumnType {
		case model.ModifyTypePrecheck, model.ModifyTypeNoReorg, model.ModifyTypeNoReorgWithCheck:
			// For those column-type-change jobs which don't need reorg, or checking.
			return rollbackModifyColumnJob(jobCtx, tblInfo, job, args.Column, oldCol)
		case model.ModifyTypeReorg:
			// For those column-type-change jobs which need reorg.
			return rollbackModifyColumnJobWithReorg(jobCtx, tblInfo, job, oldCol, args)
		case model.ModifyTypeIndexReorg:
			// For those column-type-change jobs which reorg the index only.
			return rollbackModifyColumnJobWithIndexReorg(jobCtx, tblInfo, job, oldCol, args)
		}
	}

	// Do some checks for all modify column types.
	err = checkAndApplyAutoRandomBits(jobCtx, dbInfo, tblInfo, oldCol, args.Column, args.NewShardBits)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if err = checkModifyTypes(oldCol, args.Column, isColumnWithIndex(oldCol.Name.L, tblInfo.Indices)); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if err = checkColumnReferencedByPartialCondition(tblInfo, args.Column.Name); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if args.ChangingColumn == nil {
		if err := checkColumnAlreadyExists(tblInfo, args); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	switch args.ModifyColumnType {
	case model.ModifyTypePrecheck:
		return w.precheckForVarcharToChar(jobCtx, job, args, dbInfo, tblInfo, oldCol)
	case model.ModifyTypeNoReorgWithCheck:
		return w.doModifyColumnWithCheck(jobCtx, job, dbInfo, tblInfo, args.Column, oldCol, args.Position)
	case model.ModifyTypeNoReorg:
		return w.doModifyColumnNoCheck(jobCtx, job, tblInfo, args.Column, oldCol, args.Position)
	}

	// Checks for ModifyTypeReorg and ModifyTypeIndexReorg.
	if err = isGeneratedRelatedColumn(tblInfo, args.Column, oldCol); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if isColumnarIndexColumn(tblInfo, oldCol) {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("columnar indexes on the column"))
	}
	if mysql.HasPriKeyFlag(oldCol.GetFlag()) {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify column in primary key")
	}

	if job.SchemaState == model.StateNone {
		err = postCheckPartitionModifiableColumn(w, tblInfo, oldCol, args.Column)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
	}

	defer checkTableInfo(tblInfo)

	if args.ModifyColumnType == model.ModifyTypeIndexReorg {
		if err := initializeChangingIndexes(args, tblInfo, oldCol); err != nil {
			return ver, errors.Trace(err)
		}
		return w.doModifyColumnIndexReorg(
			jobCtx, job, dbInfo, tblInfo, oldCol, args)
	}

	changingCol, err := getChangingCol(args, tblInfo, oldCol)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	return w.doModifyColumnTypeWithData(
		jobCtx, job, dbInfo, tblInfo, changingCol, oldCol, args)
}

func checkColumnAlreadyExists(tblInfo *model.TableInfo, args *model.ModifyColumnArgs) error {
	// If we want to rename the column name, we need to check whether it already exists.
	if args.Column.Name.L != args.OldColumnName.L {
		c := model.FindColumnInfo(tblInfo.Columns, args.Column.Name.L)
		if c != nil {
			return errors.Trace(infoschema.ErrColumnExists.GenWithStackByArgs(args.Column.Name))
		}
	}
	return nil
}

// rollbackModifyColumnJob rollbacks the job who doesn't need to reorg the data.
func rollbackModifyColumnJob(
	jobCtx *jobContext, tblInfo *model.TableInfo, job *model.Job,
	newCol, oldCol *model.ColumnInfo) (ver int64, err error) {
	if oldCol.ID == newCol.ID {
		// Clean the flag info in oldCol.
		if hasModifyFlag(tblInfo.Columns[oldCol.Offset]) {
			tblInfo.Columns[oldCol.Offset].DelFlag(mysql.PreventNullInsertFlag)
			tblInfo.Columns[oldCol.Offset].DelFlag(mysql.NotNullFlag)
			tblInfo.Columns[oldCol.Offset].ChangingFieldType = nil
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	// For those column changes which doesn't need reorg data, we should also mock empty args for delete range.
	job.FillFinishedArgs(&model.ModifyColumnArgs{})
	return ver, nil
}

// rollbackModifyColumnJobWithReorg is used to rollback modify-column job which need to
// reorg both the row and index data.
func rollbackModifyColumnJobWithReorg(
	jobCtx *jobContext, tblInfo *model.TableInfo, job *model.Job,
	oldCol *model.ColumnInfo, args *model.ModifyColumnArgs) (ver int64, err error) {
	// Clean the flag info in oldCol.
	if hasModifyFlag(tblInfo.Columns[oldCol.Offset]) {
		tblInfo.Columns[oldCol.Offset].DelFlag(mysql.PreventNullInsertFlag)
		tblInfo.Columns[oldCol.Offset].DelFlag(mysql.NotNullFlag)
		tblInfo.Columns[oldCol.Offset].ChangingFieldType = nil
	}

	var changingIdxIDs []int64
	if args.ChangingColumn != nil {
		changingIdxIDs = getRelatedIndexIDs(tblInfo, args.ChangingColumn.ID, job.ReorgMeta.ReorgTp.NeedMergeProcess())
		// The job is in the middle state. The appended changingCol and changingIndex should
		// be removed from the tableInfo as well.
		removeChangingColAndIdxs(tblInfo, args.ChangingColumn.ID)
	}
	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	args.IndexIDs = changingIdxIDs
	args.PartitionIDs = getPartitionIDs(tblInfo)
	job.FillFinishedArgs(args)
	return ver, nil
}

// rollbackModifyColumnJobWithIndexReorg is used to rollback modify-column job which
// only need to reorg the index.
func rollbackModifyColumnJobWithIndexReorg(
	jobCtx *jobContext, tblInfo *model.TableInfo, job *model.Job,
	oldCol *model.ColumnInfo, args *model.ModifyColumnArgs) (ver int64, err error) {
	// Clean the flag info in oldCol.
	if hasModifyFlag(tblInfo.Columns[oldCol.Offset]) {
		tblInfo.Columns[oldCol.Offset].DelFlag(mysql.PreventNullInsertFlag)
		tblInfo.Columns[oldCol.Offset].DelFlag(mysql.NotNullFlag)
		tblInfo.Columns[oldCol.Offset].ChangingFieldType = nil
	}

	allIdxs := buildRelatedIndexInfos(tblInfo, oldCol.ID)
	changingIdxInfos := make([]*model.IndexInfo, 0, len(allIdxs)/2)
	for _, idx := range allIdxs {
		for _, idxCol := range idx.Columns {
			if idxCol.Name.L == oldCol.Name.L && idxCol.UseChangingType {
				changingIdxInfos = append(changingIdxInfos, idx)
				break
			}
		}
	}
	for _, idx := range changingIdxInfos {
		args.IndexIDs = append(args.IndexIDs, idx.ID)
	}
	args.IndexIDs = append(args.IndexIDs, getIngestTempIndexIDs(job, changingIdxInfos)...)
	removeOldIndexes(tblInfo, changingIdxInfos)

	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	args.PartitionIDs = getPartitionIDs(tblInfo)
	job.FillFinishedArgs(args)
	return ver, nil
}

func getModifyColumnInfo(
	t *meta.Mutator, job *model.Job, args *model.ModifyColumnArgs,
) (*model.DBInfo, *model.TableInfo, *model.ColumnInfo, error) {
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	var oldCol *model.ColumnInfo
	// Use column ID to locate the old column.
	// It is persisted to job arguments after the first execution.
	if args.OldColumnID > 0 {
		oldCol = model.FindColumnInfoByID(tblInfo.Columns, args.OldColumnID)
	} else {
		// Lower version TiDB doesn't persist the old column ID to job arguments.
		// We have to use the old column name to locate the old column.
		oldCol = model.FindColumnInfo(tblInfo.Columns, model.GenRemovingObjName(args.OldColumnName.L))
		if oldCol == nil {
			// The old column maybe not in removing state.
			oldCol = model.FindColumnInfo(tblInfo.Columns, args.OldColumnName.L)
		}
		if oldCol != nil {
			args.OldColumnID = oldCol.ID
			logutil.DDLLogger().Info("run modify column job, find old column by name",
				zap.Int64("jobID", job.ID),
				zap.String("oldColumnName", args.OldColumnName.L),
				zap.Int64("oldColumnID", oldCol.ID),
			)
		}
	}
	if oldCol == nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(args.OldColumnName, tblInfo.Name))
	}
	return dbInfo, tblInfo, oldCol, errors.Trace(err)
}

func finishModifyColumnWithoutReorg(
	jobCtx *jobContext,
	job *model.Job,
	tblInfo *model.TableInfo,
	newCol, oldCol *model.ColumnInfo,
	pos *ast.ColumnPosition,
) (ver int64, _ error) {
	if err := adjustTableInfoAfterModifyColumn(tblInfo, newCol, oldCol, pos); err != nil {
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	childTableInfos, err := adjustForeignKeyChildTableInfoAfterModifyColumn(jobCtx.infoCache, jobCtx.metaMut, job, tblInfo, newCol, oldCol)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true, childTableInfos...)
	if err != nil {
		// Modified the type definition of 'null' to 'not null' before this, so rollBack the job when an error occurs.
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	// For those column-type-change type which doesn't need reorg data, we should also mock the job args for delete range.
	job.FillFinishedArgs(&model.ModifyColumnArgs{})
	return ver, nil
}

// doModifyColumnNoCheck updates the column information and reorders all columns. It does not support modifying column data.
func (*worker) doModifyColumnNoCheck(
	jobCtx *jobContext,
	job *model.Job,
	tblInfo *model.TableInfo,
	newCol, oldCol *model.ColumnInfo,
	pos *ast.ColumnPosition,
) (ver int64, _ error) {
	if oldCol.ID != newCol.ID {
		job.State = model.JobStateRollingback
		return ver, dbterror.ErrColumnInChange.GenWithStackByArgs(oldCol.Name, newCol.ID)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, false)
	}

	return finishModifyColumnWithoutReorg(jobCtx, job, tblInfo, newCol, oldCol, pos)
}

// precheckForVarcharToChar updates the column information and reorders all columns with data check.
func (w *worker) precheckForVarcharToChar(
	jobCtx *jobContext,
	job *model.Job,
	args *model.ModifyColumnArgs,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo,
) (ver int64, _ error) {
	newCol := args.Column
	if oldCol.ID != newCol.ID {
		job.State = model.JobStateRollingback
		return ver, dbterror.ErrColumnInChange.GenWithStackByArgs(oldCol.Name, newCol.ID)
	}

	// For the first time we get here, just add the flag
	// and check the existing data in the next round.
	if !hasModifyFlag(oldCol) {
		if isNullToNotNullChange(oldCol, newCol) {
			oldCol.AddFlag(mysql.PreventNullInsertFlag)
		}
		if !noReorgDataStrict(tblInfo, oldCol, newCol) {
			oldCol.ChangingFieldType = &newCol.FieldType
		}
		return updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
	}

	// Second time we get here, do the data check.
	checked, err := checkModifyColumnData(
		jobCtx.stepCtx, w,
		dbInfo.Name, tblInfo.Name,
		oldCol, newCol, true)

	// If existing data is in range, we continue running without row reorg.
	// The flag is retained, so string with trailing spaces still can't be inserted.
	if err == nil {
		args.ModifyColumnType = getModifyColumnType(args, tblInfo, oldCol, job.SQLMode)
		logutil.DDLLogger().Info("precheck done, change modify type",
			zap.String("query", job.Query),
			zap.String("oldColumnName", args.OldColumnName.L),
			zap.Int64("oldColumnID", oldCol.ID),
			zap.String("type", model.ModifyTypeToString(args.ModifyColumnType)),
		)
		return ver, nil
	}

	// Meet error during checking, let outer side handle it.
	if !checked {
		return ver, errors.Trace(err)
	}

	// Fallback to normal reorg type.
	oldCol.DelFlag(mysql.PreventNullInsertFlag)
	oldCol.ChangingFieldType = nil
	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	logutil.DDLLogger().Info("precheck done, fallback to normal reorg",
		zap.String("query", job.Query),
		zap.String("oldColumnName", args.OldColumnName.L),
		zap.Int64("oldColumnID", oldCol.ID),
	)
	args.ModifyColumnType = model.ModifyTypeReorg
	return ver, nil
}

// doModifyColumnWithCheck updates the column information and reorders all columns with data check.
func (w *worker) doModifyColumnWithCheck(
	jobCtx *jobContext,
	job *model.Job,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	newCol, oldCol *model.ColumnInfo,
	pos *ast.ColumnPosition,
) (ver int64, _ error) {
	if oldCol.ID != newCol.ID {
		job.State = model.JobStateRollingback
		return ver, dbterror.ErrColumnInChange.GenWithStackByArgs(oldCol.Name, newCol.ID)
	}

	// For the first time we get here, just add the flag
	// and check the existing data in the next round.
	if !hasModifyFlag(oldCol) {
		failpoint.InjectCall("beforeDoModifyColumnSkipReorgCheck")
		if isNullToNotNullChange(oldCol, newCol) {
			oldCol.AddFlag(mysql.PreventNullInsertFlag)
		}
		if !noReorgDataStrict(tblInfo, oldCol, newCol) {
			oldCol.ChangingFieldType = &newCol.FieldType
		}
		return updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
	}

	checked, err := checkModifyColumnData(
		jobCtx.stepCtx, w,
		dbInfo.Name, tblInfo.Name,
		oldCol, newCol,
		!noReorgDataStrict(tblInfo, oldCol, newCol),
	)
	if err != nil {
		// If checked is true, it means we have done the check and found invalid data.
		// Otherwise, it means we meet some internal error, just let outer side handle it (like retry).
		if checked {
			job.State = model.JobStateRollingback
		}
		return ver, errors.Trace(err)
	}

	failpoint.InjectCall("afterDoModifyColumnSkipReorgCheck")

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, false)
	}

	return finishModifyColumnWithoutReorg(jobCtx, job, tblInfo, newCol, oldCol, pos)
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

func updateFKInfoWhenModifyColumn(tblInfo *model.TableInfo, oldCol, newCol ast.CIStr) {
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

func updateTTLInfoWhenModifyColumn(tblInfo *model.TableInfo, oldCol, newCol ast.CIStr) {
	if oldCol.L == newCol.L {
		return
	}
	if tblInfo.TTLInfo != nil {
		if tblInfo.TTLInfo.ColumnName.L == oldCol.L {
			tblInfo.TTLInfo.ColumnName = newCol
		}
	}
}

func adjustForeignKeyChildTableInfoAfterModifyColumn(infoCache *infoschema.InfoCache, t *meta.Mutator, job *model.Job, tblInfo *model.TableInfo, newCol, oldCol *model.ColumnInfo) ([]schemaIDAndTableInfo, error) {
	if !vardef.EnableForeignKey.Load() || newCol.Name.L == oldCol.Name.L {
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


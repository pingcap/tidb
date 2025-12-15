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
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

func hasModifyFlag(col *model.ColumnInfo) bool {
	return col.ChangingFieldType != nil || mysql.HasPreventNullInsertFlag(col.GetFlag())
}

func isNullToNotNullChange(oldCol, newCol *model.ColumnInfo) bool {
	return !mysql.HasNotNullFlag(oldCol.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag())
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
		if !isNullToNotNullChange(oldCol, newCol) {
			return model.ModifyTypeNoReorg
		}
		return model.ModifyTypeNoReorgWithCheck
	}

	// For backward compatibility
	if args.ModifyColumnType == mysql.TypeNull {
		return model.ModifyTypeReorg
	}

	if !sqlMode.HasStrictMode() {
		return model.ModifyTypeReorg
	}

	// FIXME(joechenrh): handle partition and TiFlash replica case
	if tblInfo.Partition != nil || tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Count > 0 {
		return model.ModifyTypeReorg
	}

	// FIXME(joechenrh): remove this when resolve stats problem.
	if (types.IsIntegerChange(&oldCol.FieldType, &args.Column.FieldType) &&
		mysql.HasUnsignedFlag(oldCol.GetFlag()) != mysql.HasUnsignedFlag(args.Column.GetFlag())) ||
		(types.IsCharChange(&oldCol.FieldType, &args.Column.FieldType) &&
			!collate.CompatibleCollate(oldCol.GetCollate(), args.Column.GetCollate())) {
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

func needIndexReorg(oldCol, changingCol *model.ColumnInfo) bool {
	// Signed/unsigned change for integer types need index reorg.
	if mysql.IsIntegerType(oldCol.GetType()) && mysql.IsIntegerType(changingCol.GetType()) {
		return mysql.HasUnsignedFlag(oldCol.GetFlag()) != mysql.HasUnsignedFlag(changingCol.GetFlag())
	}

	intest.Assert(types.IsCharChange(&oldCol.FieldType, &changingCol.FieldType))

	// Check index key part, ref tablecodec.GenIndexKey
	if !collate.CompatibleCollate(oldCol.GetCollate(), changingCol.GetCollate()) {
		return true
	}

	// Check index value part, ref tablecodec.GenIndexValuePortal
	// TODO(joechenrh): It's better to check each index here, because not all indexes need
	// reorg even if the below condition is true.
	return types.NeedRestoredData(&oldCol.FieldType) != types.NeedRestoredData(&changingCol.FieldType)
}

func needRowReorg(oldCol, changingCol *model.ColumnInfo) bool {
	failpoint.Inject("disableLossyDDLOptimization", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			failpoint.Return(true)
		}
	})

	oldFt := &oldCol.FieldType
	changingFt := &changingCol.FieldType

	// Integer are guaranteed to not need row reorg.
	if types.IsIntegerChange(oldFt, changingFt) {
		return false
	}

	// Other changes except char changes need row reorg.
	if !types.IsCharChange(oldFt, changingFt) {
		return true
	}

	// We have checked charset before, so only need to check binary string here.
	if types.IsBinaryStr(oldFt) && types.IsBinaryStr(changingFt) {
		return oldCol.GetFlen() != changingCol.GetFlen()
	}
	return types.IsBinaryStr(oldFt) || types.IsBinaryStr(changingFt)
}

// checkModifyColumnData checks the values of the old column data
func checkModifyColumnData(
	ctx context.Context,
	w *worker,
	dbName, tblName ast.CIStr,
	oldCol, changingCol *model.ColumnInfo,
	checkValueRange bool,
) (checked bool, err error) {
	// Get sessionctx from context resource pool.
	var sctx sessionctx.Context
	sctx, err = w.sessPool.Get()
	if err != nil {
		return false, errors.Trace(err)
	}
	defer w.sessPool.Put(sctx)

	sql := buildCheckSQLFromModifyColumn(dbName, tblName, oldCol, changingCol, checkValueRange)
	if sql == "" {
		return false, nil
	}

	delayForAsyncCommit()
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(rows) != 0 {
		if rows[0].IsNull(0) {
			return true, dbterror.ErrInvalidUseOfNull
		}

		datum := rows[0].GetDatum(0, &oldCol.FieldType)
		dStr := datumToStringNoErr(datum)
		return true, types.ErrTruncated.GenWithStack("Data truncated for column '%s', value is '%s'", oldCol.Name.L, dStr)
	}

	return true, nil
}

// buildCheckSQLFromModifyColumn builds the SQL to check whether the data
// is valid after modifying to new type.
func buildCheckSQLFromModifyColumn(
	dbName, tblName ast.CIStr,
	oldCol, changingCol *model.ColumnInfo,
	checkValueRange bool,
) string {
	oldTp := oldCol.GetType()
	changingTp := changingCol.GetType()

	var conditions []string
	template := "SELECT %s FROM %s WHERE %s LIMIT 1"
	checkColName := fmt.Sprintf("`%s`", oldCol.Name.O)
	tableName := fmt.Sprintf("`%s`.`%s`", dbName.O, tblName.O)

	if checkValueRange {
		if mysql.IsIntegerType(oldTp) && mysql.IsIntegerType(changingTp) {
			// Integer conversion
			conditions = append(conditions, buildCheckRangeForIntegerTypes(oldCol, changingCol))
		} else {
			conditions = append(conditions, fmt.Sprintf("LENGTH(%s) > %d", checkColName, changingCol.FieldType.GetFlen()))
			if oldTp == mysql.TypeVarchar && changingTp == mysql.TypeString {
				conditions = append(conditions, fmt.Sprintf("%s LIKE '%% '", checkColName))
			}
		}
	}

	if isNullToNotNullChange(oldCol, changingCol) {
		if !(oldTp != mysql.TypeTimestamp && changingTp == mysql.TypeTimestamp) {
			conditions = append(conditions, fmt.Sprintf("`%s` IS NULL", oldCol.Name.O))
		}
	}

	if len(conditions) == 0 {
		return ""
	}

	return fmt.Sprintf(template, checkColName, tableName, strings.Join(conditions, " OR "))
}

// buildCheckRangeForIntegerTypes builds the range check condition for integer type conversion.
func buildCheckRangeForIntegerTypes(oldCol, changingCol *model.ColumnInfo) string {
	changingTp := changingCol.GetType()
	changingUnsigned := mysql.HasUnsignedFlag(changingCol.GetFlag())

	columnName := fmt.Sprintf("`%s`", oldCol.Name.O)

	if changingUnsigned {
		upperBound := types.IntegerUnsignedUpperBound(changingTp)
		return fmt.Sprintf("(%s < 0 OR %s > %d)", columnName, columnName, upperBound)
	}

	lowerBound := types.IntegerSignedLowerBound(changingTp)
	upperBound := types.IntegerSignedUpperBound(changingTp)
	return fmt.Sprintf("(%s < %d OR %s > %d)", columnName, lowerBound, columnName, upperBound)
}

// reorderChangingIdx reorders the changing index infos to match the order of old index infos.
func reorderChangingIdx(oldIdxInfos []*model.IndexInfo, changingIdxInfos []*model.IndexInfo) {
	nameToChanging := make(map[string]*model.IndexInfo, len(changingIdxInfos))
	for _, cIdx := range changingIdxInfos {
		origName := cIdx.Name.O
		if cIdx.State != model.StatePublic {
			origName = cIdx.GetChangingOriginName()
		}
		nameToChanging[origName] = cIdx
	}

	for i, oldIdx := range oldIdxInfos {
		changingIdxInfos[i] = nameToChanging[oldIdx.GetRemovingOriginName()]
	}
}

func (w *worker) doModifyColumnTypeWithData(
	jobCtx *jobContext,
	job *model.Job,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	changingCol, oldCol *model.ColumnInfo,
	args *model.ModifyColumnArgs,
) (ver int64, _ error) {
	colName, pos := args.Column.Name, args.Position

	var err error
	originalState := changingCol.State
	targetCol := changingCol.Clone()
	targetCol.Name = colName
	changingIdxs := buildRelatedIndexInfos(tblInfo, changingCol.ID)
	switch changingCol.State {
	case model.StateNone:
		err = validatePosition(tblInfo, oldCol, pos)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		// Column from null to not null.
		if isNullToNotNullChange(oldCol, changingCol) {
			oldCol.AddFlag(mysql.PreventNullInsertFlag)
		}
		// none -> delete only
		updateObjectState(changingCol, changingIdxs, model.StateDeleteOnly)
		job.ReorgMeta.Stage = model.ReorgStageModifyColumnUpdateColumn
		err = initForReorgIndexes(w, job, changingIdxs)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
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
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Make sure job args change after `updateVersionAndTableInfoWithCheck`, otherwise, the job args will
		// be updated in `updateDDLJob` even if it meets an error in `updateVersionAndTableInfoWithCheck`.
		job.SchemaState = model.StateDeleteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn, job.SchemaName, tblInfo.Name.String(), args.OldColumnName.O).Set(0)
		args.ChangingColumn = changingCol
		args.ChangingIdxs = changingIdxs
		failpoint.InjectCall("modifyColumnTypeWithData", job, args)
		job.FillArgs(args)
	case model.StateDeleteOnly:
		// Column from null to not null.
		if isNullToNotNullChange(oldCol, changingCol) {
			checked, err := checkModifyColumnData(
				jobCtx.stepCtx, w,
				dbInfo.Name, tblInfo.Name,
				oldCol, args.Column, false)
			if err != nil {
				if checked {
					job.State = model.JobStateRollingback
				}
				return ver, errors.Trace(err)
			}
		}
		// delete only -> write only
		updateObjectState(changingCol, changingIdxs, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		failpoint.InjectCall("afterModifyColumnStateDeleteOnly", job.ID)
	case model.StateWriteOnly:
		// write only -> reorganization
		updateObjectState(changingCol, changingIdxs, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != changingCol.State)
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
		switch job.ReorgMeta.AnalyzeState {
		case model.AnalyzeStateNone:
			switch job.ReorgMeta.Stage {
			case model.ReorgStageModifyColumnUpdateColumn:
				var done bool
				reorgElements := BuildElements(changingCol, changingIdxs)
				done, ver, err = doReorgWorkForModifyColumn(jobCtx, w, job, tbl, oldCol, reorgElements)
				if !done {
					return ver, err
				}
				if len(changingIdxs) > 0 {
					job.SnapshotVer = 0
					job.ReorgMeta.Stage = model.ReorgStageModifyColumnRecreateIndex
				} else {
					job.ReorgMeta.Stage = model.ReorgStageModifyColumnCompleted
				}
			case model.ReorgStageModifyColumnRecreateIndex:
				var done bool
				done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, changingIdxs)
				if !done {
					return ver, err
				}
				job.ReorgMeta.Stage = model.ReorgStageModifyColumnCompleted
			case model.ReorgStageModifyColumnCompleted:
				// For multi-schema change, analyze is done by parent job.
				if job.MultiSchemaInfo == nil && checkNeedAnalyze(job, tblInfo) {
					job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
				} else {
					job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
					checkAndMarkNonRevertible(job)
				}
			}
		case model.AnalyzeStateRunning:
			intest.Assert(job.MultiSchemaInfo == nil, "multi schema change shouldn't reach here")
			w.startAnalyzeAndWait(job, tblInfo)
		case model.AnalyzeStateDone, model.AnalyzeStateSkipped, model.AnalyzeStateTimeout, model.AnalyzeStateFailed:
			failpoint.InjectCall("afterReorgWorkForModifyColumn")
			oldIdxInfos := buildRelatedIndexInfos(tblInfo, oldCol.ID)
			if tblInfo.TTLInfo != nil {
				updateTTLInfoWhenModifyColumn(tblInfo, oldCol.Name, colName)
			}
			changingIdxInfos := buildRelatedIndexInfos(tblInfo, changingCol.ID)
			intest.Assert(len(oldIdxInfos) == len(changingIdxInfos))

			// In multi-schema change, the order of changingIdxInfos may not be the same as oldIdxInfos,
			// because we will allocate new indexID for previous temp index.
			reorderChangingIdx(oldIdxInfos, changingIdxInfos)

			updateObjectState(oldCol, oldIdxInfos, model.StateWriteOnly)
			updateObjectState(changingCol, changingIdxInfos, model.StatePublic)
			markOldObjectRemoving(oldCol, changingCol, oldIdxInfos, changingIdxInfos, colName)
			moveChangingColumnToDest(tblInfo, oldCol, changingCol, pos)
			moveOldColumnToBack(tblInfo, oldCol)
			moveIndexInfoToDest(tblInfo, changingCol, oldIdxInfos, changingIdxInfos)
			updateModifyingCols(oldCol, changingCol)

			job.SchemaState = model.StatePublic
			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
	case model.StatePublic:
		oldIdxInfos := buildRelatedIndexInfos(tblInfo, oldCol.ID)
		switch oldCol.State {
		case model.StateWriteOnly:
			updateObjectState(oldCol, oldIdxInfos, model.StateDeleteOnly)
			moveOldColumnToBack(tblInfo, oldCol)
			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		case model.StateDeleteOnly:
			removedIdxIDs := removeOldObjects(tblInfo, oldCol, oldIdxInfos)
			removedIdxIDs = append(removedIdxIDs, getIngestTempIndexIDs(job, changingIdxs)...)
			analyzed := job.ReorgMeta.AnalyzeState == model.AnalyzeStateDone
			modifyColumnEvent := notifier.NewModifyColumnEvent(tblInfo, []*model.ColumnInfo{changingCol}, analyzed)
			err = asyncNotifyEvent(jobCtx, modifyColumnEvent, job, noSubJob, w.sess)
			if err != nil {
				return ver, errors.Trace(err)
			}

			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
			// Finish this job.
			job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
			// Refactor the job args to add the old index ids into delete range table.
			rmIdxs := append(removedIdxIDs, args.RedundantIdxs...)
			args.IndexIDs = rmIdxs
			newIdxIDs := make([]int64, 0, len(changingIdxs))
			for _, idx := range changingIdxs {
				newIdxIDs = append(newIdxIDs, idx.ID)
			}
			args.NewIndexIDs = newIdxIDs
			args.PartitionIDs = getPartitionIDs(tblInfo)
			job.FillFinishedArgs(args)
		default:
			errMsg := fmt.Sprintf("unexpected column state %s in modify column job", oldCol.State)
			intest.Assert(false, errMsg)
			return ver, errors.Errorf(errMsg)
		}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", changingCol.State)
	}
	return ver, errors.Trace(err)
}

func (w *worker) doModifyColumnIndexReorg(
	jobCtx *jobContext,
	job *model.Job,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo,
	args *model.ModifyColumnArgs,
) (ver int64, err error) {
	colName, pos := args.Column.Name, args.Position

	allIdxs := buildRelatedIndexInfos(tblInfo, oldCol.ID)
	oldIdxInfos := make([]*model.IndexInfo, 0, len(allIdxs)/2)
	changingIdxInfos := make([]*model.IndexInfo, 0, len(allIdxs)/2)

	if job.SchemaState == model.StatePublic {
		// The constraint that the first half of allIdxs is oldIdxInfos and
		// the second half is changingIdxInfos isn't true now, so we need to
		// find the oldIdxInfos again.
		for _, idx := range allIdxs {
			if idx.IsRemoving() {
				oldIdxInfos = append(oldIdxInfos, idx)
			}
		}
	} else {
		changingIdxInfos = allIdxs[len(allIdxs)/2:]
		oldIdxInfos = allIdxs[:len(allIdxs)/2]
	}

	finishFunc := func() (ver int64, err error) {
		removedIdxIDs := make([]int64, 0, len(oldIdxInfos))
		for _, idx := range oldIdxInfos {
			removedIdxIDs = append(removedIdxIDs, idx.ID)
		}
		removedIdxIDs = append(removedIdxIDs, getIngestTempIndexIDs(job, changingIdxInfos)...)
		removeOldIndexes(tblInfo, oldIdxInfos)
		oldCol.ChangingFieldType = nil
		oldCol.DelFlag(mysql.PreventNullInsertFlag)

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		// Refactor the job args to add the old index ids into delete range table.
		rmIdxs := append(removedIdxIDs, args.RedundantIdxs...)
		args.IndexIDs = rmIdxs
		args.PartitionIDs = getPartitionIDs(tblInfo)
		job.FillFinishedArgs(args)
		return ver, nil
	}

	switch job.SchemaState {
	case model.StateNone:
		oldCol.AddFlag(mysql.PreventNullInsertFlag)
		oldCol.ChangingFieldType = &args.Column.FieldType
		// none -> delete only
		updateObjectState(nil, changingIdxInfos, model.StateDeleteOnly)
		job.ReorgMeta.Stage = model.ReorgStageModifyColumnUpdateColumn
		err := initForReorgIndexes(w, job, changingIdxInfos)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn, job.SchemaName, tblInfo.Name.String(), args.OldColumnName.O).Set(0)
		args.ChangingIdxs = changingIdxInfos
		failpoint.InjectCall("modifyColumnTypeWithData", job, args)
		job.FillArgs(args)
	case model.StateDeleteOnly:
		checked, err := checkModifyColumnData(
			jobCtx.stepCtx, w,
			dbInfo.Name, tblInfo.Name,
			oldCol, args.Column, true)
		if err != nil {
			if checked {
				job.State = model.JobStateRollingback
			}
			return ver, errors.Trace(err)
		}

		// delete only -> write only
		updateObjectState(nil, changingIdxInfos, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		failpoint.InjectCall("afterModifyColumnStateDeleteOnly", job.ID)
	case model.StateWriteOnly:
		// write only -> reorganization
		updateObjectState(nil, changingIdxInfos, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
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
		switch job.ReorgMeta.AnalyzeState {
		case model.AnalyzeStateNone:
			switch job.ReorgMeta.Stage {
			case model.ReorgStageModifyColumnUpdateColumn:
				// Now row reorg
				job.SnapshotVer = 0
				job.ReorgMeta.Stage = model.ReorgStageModifyColumnRecreateIndex
			case model.ReorgStageModifyColumnRecreateIndex:
				var done bool
				done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, changingIdxInfos)
				if !done {
					return ver, err
				}
				job.ReorgMeta.Stage = model.ReorgStageModifyColumnCompleted
			case model.ReorgStageModifyColumnCompleted:
				// For multi-schema change, analyze is done by parent job.
				if job.MultiSchemaInfo == nil && checkNeedAnalyze(job, tblInfo) {
					job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
				} else {
					job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
					checkAndMarkNonRevertible(job)
				}
			}
		case model.AnalyzeStateRunning:
			intest.Assert(job.MultiSchemaInfo == nil, "multi schema change shouldn't reach here")
			w.startAnalyzeAndWait(job, tblInfo)
		case model.AnalyzeStateDone, model.AnalyzeStateSkipped, model.AnalyzeStateTimeout, model.AnalyzeStateFailed:
			failpoint.InjectCall("afterReorgWorkForModifyColumn")
			reorderChangingIdx(oldIdxInfos, changingIdxInfos)
			oldTp := oldCol.FieldType
			oldName := oldCol.Name
			oldID := oldCol.ID
			tblInfo.Columns[oldCol.Offset] = args.Column.Clone()
			tblInfo.Columns[oldCol.Offset].ChangingFieldType = &oldTp
			tblInfo.Columns[oldCol.Offset].Offset = oldCol.Offset
			tblInfo.Columns[oldCol.Offset].ID = oldID
			tblInfo.Columns[oldCol.Offset].State = model.StatePublic
			oldCol = tblInfo.Columns[oldCol.Offset]

			updateObjectState(nil, oldIdxInfos, model.StateWriteOnly)
			updateObjectState(nil, changingIdxInfos, model.StatePublic)
			moveChangingColumnToDest(tblInfo, oldCol, oldCol, pos)
			moveIndexInfoToDest(tblInfo, oldCol, oldIdxInfos, changingIdxInfos)
			markOldIndexesRemoving(oldIdxInfos, changingIdxInfos)
			for i, idx := range changingIdxInfos {
				for j, idxCol := range idx.Columns {
					if idxCol.Name.L == oldName.L {
						oldIdxInfos[i].Columns[j].Name = colName
						oldIdxInfos[i].Columns[j].Offset = oldCol.Offset
						oldIdxInfos[i].Columns[j].UseChangingType = true
						changingIdxInfos[i].Columns[j].Name = colName
						changingIdxInfos[i].Columns[j].Offset = oldCol.Offset
						changingIdxInfos[i].Columns[j].UseChangingType = false
					}
				}
			}
			job.SchemaState = model.StatePublic
			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
	case model.StatePublic:
		if len(oldIdxInfos) == 0 {
			// All the old indexes has been deleted by previous modify column,
			// we can just finish the job.
			return finishFunc()
		}

		switch oldIdxInfos[0].State {
		case model.StateWriteOnly:
			updateObjectState(nil, oldIdxInfos, model.StateDeleteOnly)
			return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		case model.StateDeleteOnly:
			return finishFunc()
		default:
			errMsg := fmt.Sprintf("unexpected column state %s in modify column job", oldCol.State)
			intest.Assert(false, errMsg)
			return ver, errors.Errorf(errMsg)
		}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", oldIdxInfos[0].State)
	}
	return ver, errors.Trace(err)
}

// checkAndMarkNonRevertible should be called when the job is in the final revertible state before public.
func checkAndMarkNonRevertible(job *model.Job) {
	// previously, when reorg is done, and before we set the state to public, we need all other sub-task to
	// be ready as well which is via setting this job as NornRevertible, then we can continue skip current
	// sub and process the others.
	// And when all sub-task are ready, which means each of them could be public in one more ddl round. And
	// in onMultiSchemaChange, we just give all sub-jobs each one more round to public the schema, and only
	// use the schema version generated once.
	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
	}
}

func doReorgWorkForModifyColumn(
	jobCtx *jobContext,
	w *worker, job *model.Job, tbl table.Table,
	oldCol *model.ColumnInfo, elements []*meta.Element,
) (done bool, ver int64, err error) {
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		err = errors.Trace(err1)
		return
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta),
		jobCtx, rh, job, dbInfo, tbl, elements, false)
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
	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onModifyColumn",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("modify table `%v` column `%v` panic", tbl.Meta().Name, oldCol.Name)
			}, false)
		return w.modifyTableColumn(jobCtx, tbl, reorgInfo)
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

func checkModifyColumnWithGeneratedColumnsConstraint(allCols []*table.Column, oldColName ast.CIStr) error {
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

func preCheckPartitionModifiableColumn(sctx sessionctx.Context, t table.Table, col, newCol *table.Column) error {
	// Check that the column change does not affect the partitioning column
	// It must keep the same type, int [unsigned], [var]char, date[time]
	if t.Meta().Partition != nil {
		pt, ok := t.(table.PartitionedTable)
		if !ok {
			// Should never happen!
			return dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
		}
		for _, name := range pt.GetPartitionColumnNames() {
			if strings.EqualFold(name.L, col.Name.L) {
				return checkPartitionColumnModifiable(sctx, t.Meta(), col.ColumnInfo, newCol.ColumnInfo)
			}
		}
	}
	return nil
}

func postCheckPartitionModifiableColumn(w *worker, tblInfo *model.TableInfo, col, newCol *model.ColumnInfo) error {
	// Check that the column change does not affect the partitioning column
	// It must keep the same type, int [unsigned], [var]char, date[time]
	if tblInfo.Partition != nil {
		sctx, err := w.sessPool.Get()
		if err != nil {
			return err
		}
		defer w.sessPool.Put(sctx)
		if len(tblInfo.Partition.Columns) > 0 {
			for _, pc := range tblInfo.Partition.Columns {
				if strings.EqualFold(pc.L, col.Name.L) {
					err := checkPartitionColumnModifiable(sctx, tblInfo, col, newCol)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
			return nil
		}
		partCols, err := extractPartitionColumns(tblInfo.Partition.Expr, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
		var partCol *model.ColumnInfo
		for _, pc := range partCols {
			if strings.EqualFold(pc.Name.L, col.Name.L) {
				partCol = pc
				break
			}
		}
		if partCol != nil {
			return checkPartitionColumnModifiable(sctx, tblInfo, col, newCol)
		}
	}
	return nil
}

func checkPartitionColumnModifiable(sctx sessionctx.Context, tblInfo *model.TableInfo, col, newCol *model.ColumnInfo) error {
	if col.Name.L != newCol.Name.L {
		return dbterror.ErrDependentByPartitionFunctional.GenWithStackByArgs(col.Name.L)
	}
	if !isColTypeAllowedAsPartitioningCol(tblInfo.Partition.Type, newCol.FieldType) {
		return dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(newCol.Name.O)
	}
	pi := tblInfo.GetPartitionInfo()
	if len(pi.Columns) == 0 {
		// non COLUMNS partitioning, only checks INTs, not their actual range
		// There are many edge cases, like when truncating SQL Mode is allowed
		// which will change the partitioning expression value resulting in a
		// different partition. Better be safe and not allow decreasing of length.
		// TODO: Should we allow it in strict mode? Wait for a use case / request.
		if newCol.FieldType.GetFlen() < col.FieldType.GetFlen() {
			return dbterror.ErrUnsupportedModifyColumn.GenWithStack("Unsupported modify column, decreasing length of int may result in truncation and change of partition")
		}
	}
	// Basically only allow changes of the length/decimals for the column
	// Note that enum is not allowed, so elems are not checked
	// TODO: support partition by ENUM
	if newCol.FieldType.EvalType() != col.FieldType.EvalType() ||
		newCol.FieldType.GetFlag() != col.FieldType.GetFlag() ||
		newCol.FieldType.GetCollate() != col.FieldType.GetCollate() ||
		newCol.FieldType.GetCharset() != col.FieldType.GetCharset() {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't change the partitioning column, since it would require reorganize all partitions")
	}
	// Generate a new PartitionInfo and validate it together with the new column definition
	// Checks if all partition definition values are compatible.
	// Similar to what buildRangePartitionDefinitions would do in terms of checks.

	newTblInfo := *tblInfo
	// Replace col with newCol and see if we can generate a new SHOW CREATE TABLE
	// and reparse it and build new partition definitions (which will do additional
	// checks columns vs partition definition values
	newCols := make([]*model.ColumnInfo, 0, len(newTblInfo.Columns))
	for _, c := range newTblInfo.Columns {
		if c.ID == col.ID {
			newCols = append(newCols, newCol)
			continue
		}
		newCols = append(newCols, c)
	}
	newTblInfo.Columns = newCols

	var buf bytes.Buffer
	AppendPartitionInfo(tblInfo.GetPartitionInfo(), &buf, mysql.ModeNone)
	// Ignoring warnings
	stmt, _, err := parser.New().ParseSQL("ALTER TABLE t " + buf.String())
	if err != nil {
		// Should never happen!
		return dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
	}
	at, ok := stmt[0].(*ast.AlterTableStmt)
	if !ok || len(at.Specs) != 1 || at.Specs[0].Partition == nil {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStack("cannot parse generated PartitionInfo")
	}
	pAst := at.Specs[0].Partition
	_, err = buildPartitionDefinitionsInfo(
		exprctx.CtxWithHandleTruncateErrLevel(sctx.GetExprCtx(), errctx.LevelError),
		pAst.Definitions, &newTblInfo, uint64(len(newTblInfo.Partition.Definitions)),
	)
	if err != nil {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStack("New column does not match partition definitions: %s", err.Error())
	}
	return nil
}

var colStateOrd = map[model.SchemaState]int{
	model.StateNone:                 0,
	model.StateDeleteOnly:           1,
	model.StateDeleteReorganization: 2,
	model.StateWriteOnly:            3,
	model.StateWriteReorganization:  4,
	model.StatePublic:               5,
}

func checkTableInfo(tblInfo *model.TableInfo) {
	if !intest.InTest {
		return
	}

	// Check columns' order by state.
	minState := model.StatePublic
	for _, col := range tblInfo.Columns {
		if colStateOrd[col.State] < colStateOrd[minState] {
			minState = col.State
		} else if colStateOrd[col.State] > colStateOrd[minState] {
			intest.Assert(false, fmt.Sprintf("column %s state %s is not in order, expect at least %s", col.Name, col.State, minState))
		}
		if col.ChangeStateInfo != nil {
			offset := col.ChangeStateInfo.DependencyColumnOffset
			intest.Assert(offset >= 0 && offset < len(tblInfo.Columns))
			depCol := tblInfo.Columns[offset]
			switch {
			case col.IsChanging():
				name := col.GetChangingOriginName()
				intest.Assert(name == depCol.Name.O, "%s != %s", name, depCol.Name.O)
			case depCol.IsRemoving():
				name := depCol.GetRemovingOriginName()
				intest.Assert(name == col.Name.O, "%s != %s", name, col.Name.O)
			}
		}
	}

	// Check index names' uniqueness.
	allNames := make(map[string]struct{})
	for _, idx := range tblInfo.Indices {
		_, exists := allNames[idx.Name.O]
		intest.Assert(!exists, "duplicate index name %s", idx.Name.O)
		allNames[idx.Name.O] = struct{}{}
	}
}

// GetModifiableColumnJob returns a DDL job of model.ActionModifyColumn.
func GetModifiableColumnJob(
	ctx context.Context,
	sctx sessionctx.Context,
	is infoschema.InfoSchema, // WARN: is maybe nil here.
	ident ast.Ident,
	originalColName ast.CIStr,
	schema *model.DBInfo,
	t table.Table,
	spec *ast.AlterTableSpec,
) (*JobWrapper, error) {
	var err error
	specNewColumn := spec.NewColumns[0]

	col := table.FindCol(t.Cols(), originalColName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(originalColName, ident.Name)
	}
	err = checkColumnReferencedByPartialCondition(t.Meta(), col.ColumnInfo.Name)
	if err != nil {
		return nil, errors.Trace(err)
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

	if err = ProcessColumnCharsetAndCollation(NewMetaBuildContextWithSctx(sctx), col, newCol, t.Meta(), specNewColumn, schema); err != nil {
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

	if err = checkModifyTypes(col.ColumnInfo, newCol.ColumnInfo, isColumnWithIndex(col.Name.L, t.Meta().Indices)); err != nil {
		return nil, errors.Trace(err)
	}
	mayNeedChangeColData := !noReorgDataStrict(t.Meta(), col.ColumnInfo, newCol.ColumnInfo)
	if mayNeedChangeColData {
		if err = isGeneratedRelatedColumn(t.Meta(), newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
		if isColumnarIndexColumn(t.Meta(), col.ColumnInfo) {
			return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("columnar indexes on the column")
		}
		// new col's origin default value be the same as the new default value.
		originDefVal, err := generateOriginDefaultValue(newCol.ColumnInfo, sctx, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err = newCol.ColumnInfo.SetOriginDefaultValue(originDefVal); err != nil {
			return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("new column set origin default value failed")
		}
	}

	err = preCheckPartitionModifiableColumn(sctx, t, col, newCol)
	if err != nil {
		return nil, err
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
	//nolint:forbidigo
	if !sctx.GetSessionVars().AllowRemoveAutoInc && mysql.HasAutoIncrementFlag(col.GetFlag()) && !mysql.HasAutoIncrementFlag(newCol.GetFlag()) {
		return nil, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled")
	}

	// We support modifying the type definitions of 'null' to 'not null' now.
	if !mysql.HasNotNullFlag(col.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag()) {
		if err = checkForNullValue(ctx, sctx, true, ident.Schema, ident.Name, newCol.ColumnInfo, col.ColumnInfo); err != nil {
			return nil, errors.Trace(err)
		}
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
	bdrRole, err := meta.NewMutator(txn).GetBDRRole()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if bdrRole == string(ast.BDRRolePrimary) &&
		deniedByBDRWhenModifyColumn(newCol.FieldType, col.FieldType, specNewColumn.Options) && !filter.IsSystemSchema(schema.Name.L) {
		return nil, dbterror.ErrBDRRestrictedDDL.FastGenByArgs(bdrRole)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        t.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		Type:           model.ActionModifyColumn,
		BinlogInfo:     &model.HistoryInfo{},
		NeedReorg:      mayNeedChangeColData,
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		SessionVars:    make(map[string]string),
	}
	err = initJobReorgMetaFromVariables(ctx, job, t, sctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	args := &model.ModifyColumnArgs{
		Column:        newCol.ColumnInfo,
		OldColumnName: originalColName,
		Position:      spec.Position,
		NewShardBits:  newAutoRandBits,
	}
	return NewJobWrapperWithArgs(job, args, false), nil
}

// noReorgDataStrict is a strong check to decide whether we need to change the column data.
// If it returns true, it means we don't need the reorg no matter what the data is.
func noReorgDataStrict(tblInfo *model.TableInfo, oldCol, newCol *model.ColumnInfo) bool {
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
			return oldCol.GetFlen() == newCol.GetFlen() && oldCol.GetDecimal() == newCol.GetDecimal() && toUnsigned == originUnsigned
		case mysql.TypeEnum, mysql.TypeSet:
			return !IsElemsChangedToModifyColumn(oldCol.GetElems(), newCol.GetElems())
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return toUnsigned == originUnsigned
		case mysql.TypeString:
			// Due to the behavior of padding \x00 at binary type, always change column data when binary length changed
			if types.IsBinaryStr(&oldCol.FieldType) {
				return newCol.GetFlen() == oldCol.GetFlen()
			}
		case mysql.TypeTiDBVectorFloat32:
			return !(newCol.GetFlen() != types.UnspecifiedLength && oldCol.GetFlen() != newCol.GetFlen())
		}

		return !needTruncationOrToggleSign()
	}

	oldTp := oldCol.GetType()
	newTp := newCol.GetType()
	// VARCHAR->CHAR, may need reorg.
	if types.IsTypeVarchar(oldTp) && newTp == mysql.TypeString {
		return false
	}
	// CHAR->VARCHAR
	if oldTp == mysql.TypeString && types.IsTypeVarchar(newTp) {
		// If there are related index, the index may need reorg.
		relatedIndexes := getRelatedIndexIDs(tblInfo, oldCol.ID, false)
		if len(relatedIndexes) > 0 {
			return false
		}
	}

	// Deal with the different type.
	switch oldCol.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch newCol.GetType() {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			return !needTruncationOrToggleSign()
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch newCol.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return !needTruncationOrToggleSignForInteger()
		}
		// conversion between float and double needs reorganization, see issue #31372
	}

	return false
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
func ProcessColumnCharsetAndCollation(ctx *metabuild.Context, col *table.Column, newCol *table.Column, meta *model.TableInfo, specNewColumn *ast.ColumnDef, schema *model.DBInfo) error {
	var chs, coll string
	var err error
	// TODO: Remove it when all table versions are greater than or equal to TableInfoVersion1.
	// If newCol's charset is empty and the table's version less than TableInfoVersion1,
	// we will not modify the charset of the column. This behavior is not compatible with MySQL.
	if len(newCol.FieldType.GetCharset()) == 0 && meta.Version < model.TableInfoVersion1 {
		chs = col.FieldType.GetCharset()
		coll = col.FieldType.GetCollate()
	} else {
		chs, coll, err = getCharsetAndCollateInColumnDef(specNewColumn, ctx.GetDefaultCollationForUTF8MB4())
		if err != nil {
			return errors.Trace(err)
		}
		chs, coll, err = ResolveCharsetCollation([]ast.CharsetOpt{
			{Chs: chs, Col: coll},
			{Chs: meta.Charset, Col: meta.Collate},
			{Chs: schema.Charset, Col: schema.Collate},
		}, ctx.GetDefaultCollationForUTF8MB4())
		chs, coll = OverwriteCollationWithBinaryFlag(specNewColumn, chs, coll, ctx.GetDefaultCollationForUTF8MB4())
		if err != nil {
			return errors.Trace(err)
		}
	}

	if err = setCharsetCollationFlenDecimal(ctx, &newCol.FieldType, newCol.Name.O, chs, coll); err != nil {
		return errors.Trace(err)
	}
	decodeEnumSetBinaryLiteralToUTF8(&newCol.FieldType, chs)
	return nil
}

func getReplacedColumns(tbInfo *model.TableInfo, oldCol, newCol *model.ColumnInfo) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(tbInfo.Columns))
	columns = append(columns, tbInfo.Columns...)
	// Replace old column with new column.
	for i, col := range columns {
		if col.Name.L != oldCol.Name.L {
			continue
		}
		columns[i] = newCol.Clone()
		columns[i].Name = oldCol.Name
		return columns
	}

	return columns
}

// checkColumnWithIndexConstraint is used to check the related index constraint of the modified column.
// Index has a max-prefix-length constraint. eg: a varchar(100), index idx(a), modifying column a to a varchar(4000)
// will cause index idx to break the max-prefix-length constraint.
func checkColumnWithIndexConstraint(tbInfo *model.TableInfo, originalCol, newCol *model.ColumnInfo) error {
	columns := getReplacedColumns(tbInfo, originalCol, newCol)

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
		err = checkIndexInModifiableColumns(columns, indexInfo)
		if err != nil {
			return
		}
		err = checkIndexPrefixLength(columns, indexInfo.Columns, indexInfo.GetColumnarIndexType())
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

func checkIndexInModifiableColumns(columns []*model.ColumnInfo, idxInfo *model.IndexInfo) error {
	indexType := idxInfo.GetColumnarIndexType()
	for _, ic := range idxInfo.Columns {
		col := model.FindColumnInfo(columns, ic.Name.L)
		if col == nil {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		if indexType != model.ColumnarIndexTypeNA {
			continue
		}

		prefixLength := types.UnspecifiedLength
		if types.IsTypePrefixable(col.FieldType.GetType()) && col.FieldType.GetFlen() > ic.Length {
			// When the index column is changed, prefix length is only valid
			// if the type is still prefixable and larger than old prefix length.
			prefixLength = ic.Length
		}
		if err := checkIndexColumn(col, prefixLength, false); err != nil {
			return err
		}
	}
	return nil
}

// checkModifyTypes checks if the 'origin' type can be modified to 'to' type no matter directly change
// or change by reorg. It returns error if the two types are incompatible and correlated change are not
// supported. However, even the two types can be change, if the "origin" type contains primary key, error will be returned.
func checkModifyTypes(from, to *model.ColumnInfo, needRewriteCollationData bool) error {
	fromFt := &from.FieldType
	toFt := &to.FieldType
	canReorg, err := types.CheckModifyTypeCompatible(fromFt, toFt)
	if err != nil {
		if !canReorg {
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(err.Error()))
		}
		if mysql.HasPriKeyFlag(fromFt.GetFlag()) {
			msg := "this column has primary key flag"
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}
	}

	err = checkModifyCharsetAndCollation(toFt.GetCharset(), toFt.GetCollate(), fromFt.GetCharset(), fromFt.GetCollate(), needRewriteCollationData)

	if err != nil {
		if toFt.GetCharset() == charset.CharsetGBK || fromFt.GetCharset() == charset.CharsetGBK {
			return errors.Trace(err)
		}
		if strings.Contains(err.Error(), "Unsupported modifying collation") {
			colErrMsg := "Unsupported modifying collation of column '%s' from '%s' to '%s' when index is defined on it."
			err = dbterror.ErrUnsupportedModifyCollation.GenWithStack(colErrMsg, from.Name.L, from.GetCollate(), to.GetCollate())
		}

		// column type change can handle the charset change between these two types in the process of the reorg.
		if dbterror.ErrUnsupportedModifyCharset.Equal(err) && canReorg {
			return nil
		}
	}
	return errors.Trace(err)
}

// ProcessModifyColumnOptions process column options.
// Export for tiflow.
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
			hasDefaultValue, err = SetDefaultValue(ctx.GetExprCtx(), col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionComment:
			err := setColumnComment(ctx.GetExprCtx(), col, opt)
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

	if err = processAndCheckDefaultValueAndColumn(ctx.GetExprCtx(), col, nil, hasDefaultValue, setOnUpdateNow, hasNullFlag); err != nil {
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

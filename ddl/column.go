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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
)

// adjustColumnInfoInAddColumn is used to set the correct position of column info when adding column.
// 1. The added column was append at the end of tblInfo.Columns, due to ddl state was not public then.
//    It should be moved to the correct position when the ddl state to be changed to public.
// 2. The offset of column should also to be set to the right value.
func adjustColumnInfoInAddColumn(tblInfo *model.TableInfo, offset int) {
	oldCols := tblInfo.Columns
	newCols := make([]*model.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[len(oldCols)-1])
	newCols = append(newCols, oldCols[offset:len(oldCols)-1]...)
	// Adjust column offset.
	offsetChanged := make(map[int]int)
	for i := offset + 1; i < len(newCols); i++ {
		offsetChanged[newCols[i].Offset] = i
		newCols[i].Offset = i
	}
	newCols[offset].Offset = offset
	// Update index column offset info.
	// TODO: There may be some corner cases for index column offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}
	tblInfo.Columns = newCols
}

// adjustColumnInfoInDropColumn is used to set the correct position of column info when dropping column.
// 1. The offset of column should to be set to the last of the columns.
// 2. The dropped column is moved to the end of tblInfo.Columns, due to it was not public any more.
func adjustColumnInfoInDropColumn(tblInfo *model.TableInfo, offset int) {
	oldCols := tblInfo.Columns
	// Adjust column offset.
	offsetChanged := make(map[int]int)
	for i := offset + 1; i < len(oldCols); i++ {
		offsetChanged[oldCols[i].Offset] = i - 1
		oldCols[i].Offset = i - 1
	}
	oldCols[offset].Offset = len(oldCols) - 1
	// Update index column offset info.
	// TODO: There may be some corner cases for index column offsets, we may check this later.
	for _, idx := range tblInfo.Indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}
	newCols := make([]*model.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[offset+1:]...)
	newCols = append(newCols, oldCols[offset])
	tblInfo.Columns = newCols
}

func createColumnInfo(tblInfo *model.TableInfo, colInfo *model.ColumnInfo, pos *ast.ColumnPosition) (*model.ColumnInfo, int, error) {
	// Check column name duplicate.
	cols := tblInfo.Columns
	position := len(cols)

	// Get column position.
	if pos.Tp == ast.ColumnPositionFirst {
		position = 0
	} else if pos.Tp == ast.ColumnPositionAfter {
		c := model.FindColumnInfo(cols, pos.RelativeColumn.Name.L)
		if c == nil {
			return nil, 0, infoschema.ErrColumnNotExists.GenWithStackByArgs(pos.RelativeColumn, tblInfo.Name)
		}

		// Insert position is after the mentioned column.
		position = c.Offset + 1
	}
	colInfo.ID = allocateColumnID(tblInfo)
	colInfo.State = model.StateNone
	// To support add column asynchronous, we should mark its offset as the last column.
	// So that we can use origin column offset to get value from row.
	colInfo.Offset = len(cols)

	// Append the column info to the end of the tblInfo.Columns.
	// It will reorder to the right position in "Columns" when it state change to public.
	newCols := make([]*model.ColumnInfo, 0, len(cols)+1)
	newCols = append(newCols, cols...)
	newCols = append(newCols, colInfo)

	tblInfo.Columns = newCols
	return colInfo, position, nil
}

func onAddColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropColumn(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// gofail: var errorBeforeDecodeArgs bool
	// if errorBeforeDecodeArgs {
	// 	return ver, errors.New("occur an error before decode args")
	// }
	col := &model.ColumnInfo{}
	pos := &ast.ColumnPosition{}
	offset := 0
	err = job.DecodeArgs(col, pos, &offset)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	columnInfo := model.FindColumnInfo(tblInfo.Columns, col.Name.L)
	if columnInfo != nil {
		if columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			job.State = model.JobStateCancelled
			return ver, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name)
		}
	} else {
		columnInfo, offset, err = createColumnInfo(tblInfo, col, pos)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		log.Infof("[ddl] add column, run DDL job %s, column info %#v, offset %d", job, columnInfo, offset)
		// Set offset arg to job.
		if offset != 0 {
			job.Args = []interface{}{columnInfo, pos, offset}
		}
		if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	originalState := columnInfo.State
	switch columnInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		columnInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		columnInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		columnInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
	case model.StateWriteReorganization:
		// reorganization -> public
		// Adjust table column offset.
		adjustColumnInfoInAddColumn(tblInfo, offset)
		columnInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionAddColumn, TableInfo: tblInfo, ColumnInfo: columnInfo})
	default:
		err = ErrInvalidColumnState.GenWithStack("invalid column state %v", columnInfo.State)
	}

	return ver, errors.Trace(err)
}

func onDropColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfo(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var colName model.CIStr
	err = job.DecodeArgs(&colName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil {
		job.State = model.JobStateCancelled
		return ver, ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	originalState := colInfo.State
	switch colInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		colInfo.State = model.StateWriteOnly
		// Set this column's offset to the last and reset all following columns' offsets.
		adjustColumnInfoInDropColumn(tblInfo, colInfo.Offset)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		colInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		colInfo.State = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	case model.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		colInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		}
	default:
		err = ErrInvalidTableState.GenWithStack("invalid table state %v", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func onSetDefaultValue(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	err := job.DecodeArgs(newCol)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return updateColumn(t, job, newCol, &newCol.Name)
}

func (w *worker) onModifyColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	oldColName := &model.CIStr{}
	pos := &ast.ColumnPosition{}
	var modifyColumnTp byte
	err := job.DecodeArgs(newCol, oldColName, pos, &modifyColumnTp)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return w.doModifyColumn(t, job, newCol, oldColName, pos, modifyColumnTp)
}

// doModifyColumn updates the column information and reorders all columns.
func (w *worker) doModifyColumn(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldName *model.CIStr, pos *ast.ColumnPosition, modifyColumnTp byte) (ver int64, _ error) {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	oldCol := model.FindColumnInfo(tblInfo.Columns, oldName.L)
	if job.IsRollingback() {
		ver, err = rollbackModifyColumnJob(t, tblInfo, job, oldCol, modifyColumnTp)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		return ver, nil
	}

	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(oldName, tblInfo.Name)
	}
	// If we want to rename the column name, we need to check whether it already exists.
	if newCol.Name.L != oldName.L {
		c := model.FindColumnInfo(tblInfo.Columns, newCol.Name.L)
		if c != nil {
			job.State = model.JobStateCancelled
			return ver, infoschema.ErrColumnExists.GenWithStackByArgs(newCol.Name)
		}
	}

	// gofail: var uninitializedOffsetAndState bool
	// if uninitializedOffsetAndState {
	// if newCol.State != model.StatePublic {
	//      return ver, errors.New("the column state is wrong")
	// }
	// }

	if !mysql.HasNotNullFlag(oldCol.Flag) && mysql.HasNotNullFlag(newCol.Flag) {
		ver, err = modifyColumnFromNull2NotNull(w, t, dbInfo, tblInfo, job, oldCol, newCol)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Introduce the `mysql.HasPreventNullInsertFlag` flag to prevent users from inserting or updating null values.
		if !mysql.HasPreventNullInsertFlag(oldCol.Flag) {
			return ver, nil
		}
	}

	// We need the latest column's offset and state. This information can be obtained from the store.
	newCol.Offset = oldCol.Offset
	newCol.State = oldCol.State
	// Calculate column's new position.
	oldPos, newPos := oldCol.Offset, oldCol.Offset
	if pos.Tp == ast.ColumnPositionAfter {
		if oldName.L == pos.RelativeColumn.Name.L {
			// `alter table tableName modify column b int after b` will return ver,ErrColumnNotExists.
			// Modified the type definition of 'null' to 'not null' before this, so rollback the job when an error occurs.
			job.State = model.JobStateRollingback
			return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(oldName, tblInfo.Name)
		}

		relative := model.FindColumnInfo(tblInfo.Columns, pos.RelativeColumn.Name.L)
		if relative == nil || relative.State != model.StatePublic {
			job.State = model.JobStateRollingback
			return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(pos.RelativeColumn, tblInfo.Name)
		}

		if relative.Offset < oldPos {
			newPos = relative.Offset + 1
		} else {
			newPos = relative.Offset
		}
	} else if pos.Tp == ast.ColumnPositionFirst {
		newPos = 0
	}

	columnChanged := make(map[string]*model.ColumnInfo)
	columnChanged[oldName.L] = newCol

	if newPos == oldPos {
		tblInfo.Columns[newPos] = newCol
	} else {
		cols := tblInfo.Columns

		// Reorder columns in place.
		if newPos < oldPos {
			copy(cols[newPos+1:], cols[newPos:oldPos])
		} else {
			copy(cols[oldPos:], cols[oldPos+1:newPos+1])
		}
		cols[newPos] = newCol

		for i, col := range tblInfo.Columns {
			if col.Offset != i {
				columnChanged[col.Name.L] = col
				col.Offset = i
			}
		}
	}

	// Change offset and name in indices.
	for _, idx := range tblInfo.Indices {
		for _, c := range idx.Columns {
			if newCol, ok := columnChanged[c.Name.L]; ok {
				c.Name = newCol.Name
				c.Offset = newCol.Offset
			}
		}
	}

	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	if err != nil {
		// Modified the type definition of 'null' to 'not null' before this, so rollBack the job when an error occurs.
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

// checkForNullValue ensure there are no null values of the column of this table.
// `isDataTruncated` indicates whether the new field and the old field type are the same, in order to be compatible with mysql.
func checkForNullValue(ctx sessionctx.Context, isDataTruncated bool, schema, table, oldCol, newCol model.CIStr) error {
	sql := fmt.Sprintf("select count(*) from `%s`.`%s` where `%s` is null limit 1;", schema.L, table.L, oldCol.L)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := rows[0].GetInt64(0)
	if rowCount != 0 {
		if isDataTruncated {
			return errInvalidUseOfNull
		}
		return ErrWarnDataTruncated.GenWithStackByArgs(newCol.L, rowCount)
	}
	return nil
}

func updateColumn(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldColName *model.CIStr) (ver int64, _ error) {
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	oldCol := model.FindColumnInfo(tblInfo.Columns, oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(newCol.Name, tblInfo.Name)
	}
	*oldCol = *newCol

	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
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

func allocateColumnID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxColumnID++
	return tblInfo.MaxColumnID
}

func checkAddColumnTooManyColumns(oldCols int) error {
	if oldCols > TableColumnCountLimit {
		return errTooManyFields
	}
	return nil
}

// rollbackModifyColumnJob rollbacks the job when an error occurs.
func rollbackModifyColumnJob(t *meta.Meta, tblInfo *model.TableInfo, job *model.Job, oldCol *model.ColumnInfo, modifyColumnTp byte) (ver int64, _ error) {
	var err error
	if modifyColumnTp == mysql.TypeNull {
		// field NotNullFlag flag reset.
		tblInfo.Columns[oldCol.Offset].Flag = oldCol.Flag &^ mysql.NotNullFlag
		// field PreventNullInsertFlag flag reset.
		tblInfo.Columns[oldCol.Offset].Flag = oldCol.Flag &^ mysql.PreventNullInsertFlag
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}
	return ver, nil
}

// modifyColumnFromNull2NotNull modifies the type definitions of 'null' to 'not null'.
func modifyColumnFromNull2NotNull(w *worker, t *meta.Meta, dbInfo *model.DBInfo, tblInfo *model.TableInfo, job *model.Job, oldCol, newCol *model.ColumnInfo) (ver int64, _ error) {
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return ver, errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	// If there is a null value inserted, it cannot be modified and needs to be rollback.
	err = checkForNullValue(ctx, oldCol.Tp == newCol.Tp, dbInfo.Name, tblInfo.Name, oldCol.Name, newCol.Name)
	if err != nil {
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	// Prevent this field from inserting null values.
	tblInfo.Columns[oldCol.Offset].Flag |= mysql.PreventNullInsertFlag
	ver, err = updateVersionAndTableInfo(t, job, tblInfo, true)
	return ver, errors.Trace(err)
}

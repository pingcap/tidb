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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
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
		// When the dropping column has not-null flag and it hasn't the default value, we can backfill the column value like "add column".
		// NOTE: If the state of StateWriteOnly can be rollbacked, we'd better reconsider the original default value.
		// And we need consider the column without not-null flag.
		if colInfo.OriginDefaultValue == nil && mysql.HasNotNullFlag(colInfo.Flag) {
			// If the column is timestamp default current_timestamp, and DDL owner is new version TiDB that set column.Version to 1,
			// then old TiDB update record in the column write only stage will uses the wrong default value of the dropping column.
			// Because new version of the column default value is UTC time, but old version TiDB will think the default value is the time in system timezone.
			// But currently will be ok, because we can't cancel the drop column job when the job is running,
			// so the column will be dropped succeed and client will never see the wrong default value of the dropped column.
			// More info about this problem, see PR#9115.
			colInfo.OriginDefaultValue, err = generateOriginDefaultValue(colInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
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

func onModifyColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	oldColName := &model.CIStr{}
	pos := &ast.ColumnPosition{}
	err := job.DecodeArgs(newCol, oldColName, pos)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return doModifyColumn(t, job, newCol, oldColName, pos)
}

// doModifyColumn updates the column information and reorders all columns.
func doModifyColumn(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldName *model.CIStr, pos *ast.ColumnPosition) (ver int64, _ error) {
	tblInfo, err := getTableInfo(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	oldCol := model.FindColumnInfo(tblInfo.Columns, oldName.L)
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

	// We need the latest column's offset and state. This information can be obtained from the store.
	newCol.Offset = oldCol.Offset
	newCol.State = oldCol.State
	// Calculate column's new position.
	oldPos, newPos := oldCol.Offset, oldCol.Offset
	if pos.Tp == ast.ColumnPositionAfter {
		if oldName.L == pos.RelativeColumn.Name.L {
			// `alter table tableName modify column b int after b` will return ver,ErrColumnNotExists.
			job.State = model.JobStateCancelled
			return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(oldName, tblInfo.Name)
		}

		relative := model.FindColumnInfo(tblInfo.Columns, pos.RelativeColumn.Name.L)
		if relative == nil || relative.State != model.StatePublic {
			job.State = model.JobStateCancelled
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
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
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

func generateOriginDefaultValue(col *model.ColumnInfo) (interface{}, error) {
	var err error
	odValue := col.GetDefaultValue()
	if odValue == nil && mysql.HasNotNullFlag(col.Flag) {
		zeroVal := table.GetZeroValue(col)
		odValue, err = zeroVal.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if odValue == strings.ToUpper(ast.CurrentTimestamp) {
		if col.Tp == mysql.TypeTimestamp {
			odValue = time.Now().UTC().Format(types.TimeFormat)
			// Version = 1: For OriginDefaultValue and DefaultValue of timestamp column will stores the default time in UTC time zone.
			//              This will fix bug in version 0.
			// TODO: remove this version field after there is no old version 0.
			col.Version = model.ColumnInfoVersion1
		} else if col.Tp == mysql.TypeDatetime {
			odValue = time.Now().Format(types.TimeFormat)
		}
	}
	return odValue, nil
}

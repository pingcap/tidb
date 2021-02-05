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
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
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

func checkAddColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, *model.ColumnInfo, *ast.ColumnPosition, int, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, nil, 0, errors.Trace(err)
	}
	col := &model.ColumnInfo{}
	pos := &ast.ColumnPosition{}
	offset := 0
	err = job.DecodeArgs(col, pos, &offset)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, nil, 0, errors.Trace(err)
	}

	columnInfo := model.FindColumnInfo(tblInfo.Columns, col.Name.L)
	if columnInfo != nil {
		if columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			job.State = model.JobStateCancelled
			return nil, nil, nil, nil, 0, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name)
		}
	}
	return tblInfo, columnInfo, col, pos, offset, nil
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

	failpoint.Inject("errorBeforeDecodeArgs", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error before decode args"))
		}
	})

	tblInfo, columnInfo, col, pos, offset, err := checkAddColumn(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if columnInfo == nil {
		columnInfo, offset, err = createColumnInfo(tblInfo, col, pos)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		logutil.Logger(ddlLogCtx).Info("[ddl] run add column job", zap.String("job", job.String()), zap.Reflect("columnInfo", *columnInfo), zap.Int("offset", offset))
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
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != columnInfo.State)
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
<<<<<<< HEAD
=======
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &ddlutil.Event{Tp: model.ActionAddColumn, TableInfo: tblInfo, ColumnInfos: []*model.ColumnInfo{columnInfo}})
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("column", columnInfo.State)
	}

	return ver, errors.Trace(err)
}

func checkAddColumns(t *meta.Meta, job *model.Job) (*model.TableInfo, []*model.ColumnInfo, []*model.ColumnInfo, []*ast.ColumnPosition, []int, []bool, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, errors.Trace(err)
	}
	columns := []*model.ColumnInfo{}
	positions := []*ast.ColumnPosition{}
	offsets := []int{}
	ifNotExists := []bool{}
	err = job.DecodeArgs(&columns, &positions, &offsets, &ifNotExists)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, nil, nil, nil, errors.Trace(err)
	}

	columnInfos := make([]*model.ColumnInfo, 0, len(columns))
	newColumns := make([]*model.ColumnInfo, 0, len(columns))
	newPositions := make([]*ast.ColumnPosition, 0, len(columns))
	newOffsets := make([]int, 0, len(columns))
	newIfNotExists := make([]bool, 0, len(columns))
	for i, col := range columns {
		columnInfo := model.FindColumnInfo(tblInfo.Columns, col.Name.L)
		if columnInfo != nil {
			if columnInfo.State == model.StatePublic {
				// We already have a column with the same column name.
				if ifNotExists[i] {
					// TODO: Should return a warning.
					logutil.BgLogger().Warn("[ddl] check add columns, duplicate column", zap.Stringer("col", col.Name))
					continue
				}
				job.State = model.JobStateCancelled
				return nil, nil, nil, nil, nil, nil, infoschema.ErrColumnExists.GenWithStackByArgs(col.Name)
			}
			columnInfos = append(columnInfos, columnInfo)
		}
		newColumns = append(newColumns, columns[i])
		newPositions = append(newPositions, positions[i])
		newOffsets = append(newOffsets, offsets[i])
		newIfNotExists = append(newIfNotExists, ifNotExists[i])
	}
	return tblInfo, columnInfos, newColumns, newPositions, newOffsets, newIfNotExists, nil
}

func setColumnsState(columnInfos []*model.ColumnInfo, state model.SchemaState) {
	for i := range columnInfos {
		columnInfos[i].State = state
	}
}

func setIndicesState(indexInfos []*model.IndexInfo, state model.SchemaState) {
	for _, indexInfo := range indexInfos {
		indexInfo.State = state
	}
}

func onAddColumns(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropColumns(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	failpoint.Inject("errorBeforeDecodeArgs", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error before decode args"))
		}
	})

	tblInfo, columnInfos, columns, positions, offsets, ifNotExists, err := checkAddColumns(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(columnInfos) == 0 {
		if len(columns) == 0 {
			job.State = model.JobStateCancelled
			return ver, nil
		}
		for i := range columns {
			columnInfo, pos, offset, err := createColumnInfo(tblInfo, columns[i], positions[i])
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			logutil.BgLogger().Info("[ddl] run add columns job", zap.String("job", job.String()), zap.Reflect("columnInfo", *columnInfo), zap.Int("offset", offset))
			positions[i] = pos
			offsets[i] = offset
			if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			columnInfos = append(columnInfos, columnInfo)
		}
		// Set arg to job.
		job.Args = []interface{}{columnInfos, positions, offsets, ifNotExists}
	}

	originalState := columnInfos[0].State
	switch columnInfos[0].State {
	case model.StateNone:
		// none -> delete only
		setColumnsState(columnInfos, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != columnInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		setColumnsState(columnInfos, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		setColumnsState(columnInfos, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		// Adjust table column offsets.
		oldCols := tblInfo.Columns[:len(tblInfo.Columns)-len(offsets)]
		newCols := tblInfo.Columns[len(tblInfo.Columns)-len(offsets):]
		tblInfo.Columns = oldCols
		for i := range offsets {
			// For multiple columns with after position, should adjust offsets.
			// e.g. create table t(a int);
			// alter table t add column b int after a, add column c int after a;
			// alter table t add column a1 int after a, add column b1 int after b, add column c1 int after c;
			// alter table t add column a1 int after a, add column b1 int first;
			if positions[i].Tp == ast.ColumnPositionAfter {
				for j := 0; j < i; j++ {
					if (positions[j].Tp == ast.ColumnPositionAfter && offsets[j] < offsets[i]) || positions[j].Tp == ast.ColumnPositionFirst {
						offsets[i]++
					}
				}
			}
			tblInfo.Columns = append(tblInfo.Columns, newCols[i])
			adjustColumnInfoInAddColumn(tblInfo, offsets[i])
		}
		setColumnsState(columnInfos, model.StatePublic)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &ddlutil.Event{Tp: model.ActionAddColumns, TableInfo: tblInfo, ColumnInfos: columnInfos})
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("column", columnInfos[0].State)
	}

	return ver, errors.Trace(err)
}

func onDropColumns(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, colInfos, delCount, idxInfos, err := checkDropColumns(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if len(colInfos) == 0 {
		job.State = model.JobStateCancelled
		return ver, nil
	}

	originalState := colInfos[0].State
	switch colInfos[0].State {
	case model.StatePublic:
		// public -> write only
		setColumnsState(colInfos, model.StateWriteOnly)
		setIndicesState(idxInfos, model.StateWriteOnly)
		for _, colInfo := range colInfos {
			err = checkDropColumnForStatePublic(tblInfo, colInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != colInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> delete only
		setColumnsState(colInfos, model.StateDeleteOnly)
		setIndicesState(idxInfos, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> reorganization
		setColumnsState(colInfos, model.StateDeleteReorganization)
		setIndicesState(idxInfos, model.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteReorganization
	case model.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
		if len(idxInfos) > 0 {
			newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
			for _, idx := range tblInfo.Indices {
				if !indexInfoContains(idx.ID, idxInfos) {
					newIndices = append(newIndices, idx)
				}
			}
			tblInfo.Indices = newIndices
		}

		indexIDs := indexInfosToIDList(idxInfos)
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-delCount]
		setColumnsState(colInfos, model.StateNone)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
		}
	default:
		err = errInvalidDDLJob.GenWithStackByArgs("table", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropColumns(t *meta.Meta, job *model.Job) (*model.TableInfo, []*model.ColumnInfo, int, []*model.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, 0, nil, errors.Trace(err)
	}

	var colNames []model.CIStr
	var ifExists []bool
	err = job.DecodeArgs(&colNames, &ifExists)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, 0, nil, errors.Trace(err)
	}

	newColNames := make([]model.CIStr, 0, len(colNames))
	colInfos := make([]*model.ColumnInfo, 0, len(colNames))
	newIfExists := make([]bool, 0, len(colNames))
	indexInfos := make([]*model.IndexInfo, 0)
	for i, colName := range colNames {
		colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
		if colInfo == nil || colInfo.Hidden {
			if ifExists[i] {
				// TODO: Should return a warning.
				logutil.BgLogger().Warn(fmt.Sprintf("column %s doesn't exist", colName))
				continue
			}
			job.State = model.JobStateCancelled
			return nil, nil, 0, nil, ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
		}
		if err = isDroppableColumn(tblInfo, colName); err != nil {
			job.State = model.JobStateCancelled
			return nil, nil, 0, nil, errors.Trace(err)
		}
		newColNames = append(newColNames, colName)
		newIfExists = append(newIfExists, ifExists[i])
		colInfos = append(colInfos, colInfo)
		idxInfos := listIndicesWithColumn(colName.L, tblInfo.Indices)
		indexInfos = append(indexInfos, idxInfos...)
	}
	job.Args = []interface{}{newColNames, newIfExists}
	return tblInfo, colInfos, len(colInfos), indexInfos, nil
}

func checkDropColumnForStatePublic(tblInfo *model.TableInfo, colInfo *model.ColumnInfo) (err error) {
	// Set this column's offset to the last and reset all following columns' offsets.
	adjustColumnInfoInDropColumn(tblInfo, colInfo.Offset)
	// When the dropping column has not-null flag and it hasn't the default value, we can backfill the column value like "add column".
	// NOTE: If the state of StateWriteOnly can be rollbacked, we'd better reconsider the original default value.
	// And we need consider the column without not-null flag.
	if colInfo.GetOriginDefaultValue() == nil && mysql.HasNotNullFlag(colInfo.Flag) {
		// If the column is timestamp default current_timestamp, and DDL owner is new version TiDB that set column.Version to 1,
		// then old TiDB update record in the column write only stage will uses the wrong default value of the dropping column.
		// Because new version of the column default value is UTC time, but old version TiDB will think the default value is the time in system timezone.
		// But currently will be ok, because we can't cancel the drop column job when the job is running,
		// so the column will be dropped succeed and client will never see the wrong default value of the dropped column.
		// More info about this problem, see PR#9115.
		originDefVal, err := generateOriginDefaultValue(colInfo)
		if err != nil {
			return err
		}
		return colInfo.SetOriginDefaultValue(originDefVal)
	}
	return nil
}

func onDropColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, colInfo, idxInfos, err := checkDropColumn(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	originalState := colInfo.State
	switch colInfo.State {
	case model.StatePublic:
		// public -> write only
		colInfo.State = model.StateWriteOnly
		setIndicesState(idxInfos, model.StateWriteOnly)
		err = checkDropColumnForStatePublic(tblInfo, colInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> delete only
		colInfo.State = model.StateDeleteOnly
		setIndicesState(idxInfos, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> reorganization
		colInfo.State = model.StateDeleteReorganization
		setIndicesState(idxInfos, model.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteReorganization
	case model.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
		if len(idxInfos) > 0 {
			newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
			for _, idx := range tblInfo.Indices {
				if !indexInfoContains(idx.ID, idxInfos) {
					newIndices = append(newIndices, idx)
				}
			}
			tblInfo.Indices = newIndices
		}

		indexIDs := indexInfosToIDList(idxInfos)
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
			// We should set related index IDs for job
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
		}
	default:
		err = errInvalidDDLJob.GenWithStackByArgs("table", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, []*model.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	var colName model.CIStr
	err = job.DecodeArgs(&colName)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, errors.Trace(err)
	}

	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil || colInfo.Hidden {
		job.State = model.JobStateCancelled
		return nil, nil, nil, ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, errors.Trace(err)
	}
	idxInfos := listIndicesWithColumn(colName.L, tblInfo.Indices)
	if len(idxInfos) > 0 {
		for _, idxInfo := range idxInfos {
			err = checkDropIndexOnAutoIncrementColumn(tblInfo, idxInfo)
			if err != nil {
				job.State = model.JobStateCancelled
				return nil, nil, nil, err
			}
		}
	}
	return tblInfo, colInfo, idxInfos, nil
}

func onSetDefaultValue(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	err := job.DecodeArgs(newCol)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return updateColumnDefaultValue(t, job, newCol, &newCol.Name)
}

func needChangeColumnData(oldCol, newCol *model.ColumnInfo) bool {
	toUnsigned := mysql.HasUnsignedFlag(newCol.Flag)
	originUnsigned := mysql.HasUnsignedFlag(oldCol.Flag)
	needTruncationOrToggleSign := func() bool {
		return (newCol.Flen > 0 && newCol.Flen < oldCol.Flen) || (toUnsigned != originUnsigned)
	}
	// Ignore the potential max display length represented by integer's flen, use default flen instead.
	oldColFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(oldCol.Tp)
	newColFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(newCol.Tp)
	needTruncationOrToggleSignForInteger := func() bool {
		return (newColFlen > 0 && newColFlen < oldColFlen) || (toUnsigned != originUnsigned)
	}

	// Deal with the same type.
	if oldCol.Tp == newCol.Tp {
		switch oldCol.Tp {
		case mysql.TypeNewDecimal:
			// Since type decimal will encode the precision, frac, negative(signed) and wordBuf into storage together, there is no short
			// cut to eliminate data reorg change for column type change between decimal.
			return oldCol.Flen != newCol.Flen || oldCol.Decimal != newCol.Decimal || toUnsigned != originUnsigned
		case mysql.TypeEnum, mysql.TypeSet:
			return isElemsChangedToModifyColumn(oldCol.Elems, newCol.Elems)
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return toUnsigned != originUnsigned
		}

		return needTruncationOrToggleSign()
	}

	// Deal with the different type.
	switch oldCol.Tp {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch newCol.Tp {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			return needTruncationOrToggleSign()
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch newCol.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return needTruncationOrToggleSignForInteger()
		}
	case mysql.TypeFloat, mysql.TypeDouble:
		switch newCol.Tp {
		case mysql.TypeFloat, mysql.TypeDouble:
			return needTruncationOrToggleSign()
		}
	}

	return true
}

func isElemsChangedToModifyColumn(oldElems, newElems []string) bool {
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

type modifyColumnJobParameter struct {
	newCol                *model.ColumnInfo
	oldColName            *model.CIStr
	modifyColumnTp        byte
	updatedAutoRandomBits uint64
	changingCol           *model.ColumnInfo
	changingIdxs          []*model.IndexInfo
	pos                   *ast.ColumnPosition
}

func getModifyColumnInfo(t *meta.Meta, job *model.Job) (*model.DBInfo, *model.TableInfo, *model.ColumnInfo, *modifyColumnJobParameter, error) {
	jobParam := &modifyColumnJobParameter{pos: &ast.ColumnPosition{}}
	err := job.DecodeArgs(&jobParam.newCol, &jobParam.oldColName, jobParam.pos, &jobParam.modifyColumnTp, &jobParam.updatedAutoRandomBits, &jobParam.changingCol, &jobParam.changingIdxs)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, jobParam, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return nil, nil, nil, jobParam, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return nil, nil, nil, jobParam, errors.Trace(err)
	}

	oldCol := model.FindColumnInfo(tblInfo.Columns, jobParam.oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return nil, nil, nil, jobParam, errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(*(jobParam.oldColName), tblInfo.Name))
	}

	return dbInfo, tblInfo, oldCol, jobParam, errors.Trace(err)
}

func (w *worker) onModifyColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	dbInfo, tblInfo, oldCol, jobParam, err := getModifyColumnInfo(t, job)
	if err != nil {
		return ver, err
	}

	if job.IsRollingback() {
		// For those column-type-change jobs which don't reorg the data.
		if !needChangeColumnData(oldCol, jobParam.newCol) {
			return rollbackModifyColumnJob(t, tblInfo, job, oldCol, jobParam.modifyColumnTp)
		}
		// For those column-type-change jobs which reorg the data.
		return rollbackModifyColumnJobWithData(t, tblInfo, job, oldCol, jobParam)
	}

	// If we want to rename the column name, we need to check whether it already exists.
	if jobParam.newCol.Name.L != jobParam.oldColName.L {
		c := model.FindColumnInfo(tblInfo.Columns, jobParam.newCol.Name.L)
		if c != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(infoschema.ErrColumnExists.GenWithStackByArgs(jobParam.newCol.Name))
		}
	}

	failpoint.Inject("uninitializedOffsetAndState", func(val failpoint.Value) {
		if val.(bool) {
			if jobParam.newCol.State != model.StatePublic {
				failpoint.Return(ver, errors.New("the column state is wrong"))
			}
		}
	})

	if jobParam.updatedAutoRandomBits > 0 {
		if err := checkAndApplyNewAutoRandomBits(job, t, tblInfo, jobParam.newCol, jobParam.oldColName, jobParam.updatedAutoRandomBits); err != nil {
			return ver, errors.Trace(err)
		}
	}

	if !needChangeColumnData(oldCol, jobParam.newCol) {
		return w.doModifyColumn(t, job, dbInfo, tblInfo, jobParam.newCol, oldCol, jobParam.pos)
	}

	if jobParam.changingCol == nil {
		changingColPos := &ast.ColumnPosition{Tp: ast.ColumnPositionNone}
		newColName := model.NewCIStr(genChangingColumnUniqueName(tblInfo, oldCol))
		if mysql.HasPriKeyFlag(oldCol.Flag) {
			job.State = model.JobStateCancelled
			msg := "tidb_enable_change_column_type is true and this column has primary key flag"
			return ver, errUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}

		jobParam.changingCol = jobParam.newCol.Clone()
		jobParam.changingCol.Name = newColName
		jobParam.changingCol.ChangeStateInfo = &model.ChangeStateInfo{DependencyColumnOffset: oldCol.Offset}

		// Since column type change is implemented as adding a new column then substituting the old one.
		// Case exists when update-where statement fetch a NULL for not-null column without any default data,
		// it will errors.
		// So we set zero original default value here to prevent this error. besides, in insert & update records,
		// we have already implement using the casted value of relative column to insert rather than the origin
		// default value.
		originDefVal, err := generateOriginDefaultValue(jobParam.newCol)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = jobParam.changingCol.SetOriginDefaultValue(originDefVal); err != nil {
			return ver, errors.Trace(err)
		}

		_, _, _, err = createColumnInfo(tblInfo, jobParam.changingCol, changingColPos)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		idxInfos, offsets := findIndexesByColName(tblInfo.Indices, oldCol.Name.L)
		jobParam.changingIdxs = make([]*model.IndexInfo, 0, len(idxInfos))
		for i, idxInfo := range idxInfos {
			newIdxInfo := idxInfo.Clone()
			newIdxInfo.Name = model.NewCIStr(genChangingIndexUniqueName(tblInfo, idxInfo))
			newIdxInfo.ID = allocateIndexID(tblInfo)
			newIdxInfo.Columns[offsets[i]].Name = newColName
			newIdxInfo.Columns[offsets[i]].Offset = jobParam.changingCol.Offset
			jobParam.changingIdxs = append(jobParam.changingIdxs, newIdxInfo)
		}
		tblInfo.Indices = append(tblInfo.Indices, jobParam.changingIdxs...)
	} else {
		tblInfo.Columns[len(tblInfo.Columns)-1] = jobParam.changingCol
		copy(tblInfo.Indices[len(tblInfo.Indices)-len(jobParam.changingIdxs):], jobParam.changingIdxs)
	}

	return w.doModifyColumnTypeWithData(d, t, job, dbInfo, tblInfo, jobParam.changingCol, oldCol, jobParam.newCol.Name, jobParam.pos, jobParam.changingIdxs)
}

// rollbackModifyColumnJobWithData is used to rollback modify-column job which need to reorg the data.
func rollbackModifyColumnJobWithData(t *meta.Meta, tblInfo *model.TableInfo, job *model.Job, oldCol *model.ColumnInfo, jobParam *modifyColumnJobParameter) (ver int64, err error) {
	// If the not-null change is included, we should clean the flag info in oldCol.
	if jobParam.modifyColumnTp == mysql.TypeNull {
		// Reset NotNullFlag flag.
		tblInfo.Columns[oldCol.Offset].Flag = oldCol.Flag &^ mysql.NotNullFlag
		// Reset PreventNullInsertFlag flag.
		tblInfo.Columns[oldCol.Offset].Flag = oldCol.Flag &^ mysql.PreventNullInsertFlag
	}
	if jobParam.changingCol != nil {
		// changingCol isn't nil means the job has been in the mid state. These appended changingCol and changingIndex should
		// be removed from the tableInfo as well.
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		tblInfo.Indices = tblInfo.Indices[:len(tblInfo.Indices)-len(jobParam.changingIdxs)]
	}
	ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	// Refactor the job args to add the abandoned temporary index ids into delete range table.
	idxIDs := make([]int64, 0, len(jobParam.changingIdxs))
	for _, idx := range jobParam.changingIdxs {
		idxIDs = append(idxIDs, idx.ID)
	}
	job.Args = []interface{}{idxIDs, getPartitionIDs(tblInfo)}
	return ver, nil
}

func (w *worker) doModifyColumnTypeWithData(
	d *ddlCtx, t *meta.Meta, job *model.Job,
	dbInfo *model.DBInfo, tblInfo *model.TableInfo, changingCol, oldCol *model.ColumnInfo,
	colName model.CIStr, pos *ast.ColumnPosition, changingIdxs []*model.IndexInfo) (ver int64, _ error) {
	var err error
	originalState := changingCol.State
	switch changingCol.State {
	case model.StateNone:
		// Column from null to not null.
		if !mysql.HasNotNullFlag(oldCol.Flag) && mysql.HasNotNullFlag(changingCol.Flag) {
			// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
			err := modifyColsFromNull2NotNull(w, dbInfo, tblInfo, []*model.ColumnInfo{oldCol}, oldCol.Name, oldCol.Tp != changingCol.Tp)
			if err != nil {
				if ErrWarnDataTruncated.Equal(err) || errInvalidUseOfNull.Equal(err) {
					job.State = model.JobStateRollingback
				}
				return ver, err
			}
		}
		// none -> delete only
		updateChangingInfo(changingCol, changingIdxs, model.StateDeleteOnly)
		failpoint.Inject("mockInsertValueAfterCheckNull", func(val failpoint.Value) {
			if valStr, ok := val.(string); ok {
				var ctx sessionctx.Context
				ctx, err := w.sessPool.get()
				if err != nil {
					failpoint.Return(ver, err)
				}
				defer w.sessPool.put(ctx)

				stmt, err := ctx.(sqlexec.RestrictedSQLExecutor).ParseWithParams(context.Background(), valStr)
				if err != nil {
					job.State = model.JobStateCancelled
					failpoint.Return(ver, err)
				}
				_, _, err = ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedStmt(context.Background(), stmt)
				if err != nil {
					job.State = model.JobStateCancelled
					failpoint.Return(ver, err)
				}
			}
		})
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Make sure job args change after `updateVersionAndTableInfoWithCheck`, otherwise, the job args will
		// be updated in `updateDDLJob` even if it meets an error in `updateVersionAndTableInfoWithCheck`.
		job.SchemaState = model.StateDeleteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblModifyColumn).Set(0)
		job.Args = append(job.Args, changingCol, changingIdxs)
	case model.StateDeleteOnly:
		// Column from null to not null.
		if !mysql.HasNotNullFlag(oldCol.Flag) && mysql.HasNotNullFlag(changingCol.Flag) {
			// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
			err := modifyColsFromNull2NotNull(w, dbInfo, tblInfo, []*model.ColumnInfo{oldCol}, oldCol.Name, oldCol.Tp != changingCol.Tp)
			if err != nil {
				if ErrWarnDataTruncated.Equal(err) || errInvalidUseOfNull.Equal(err) {
					job.State = model.JobStateRollingback
				}
				return ver, err
			}
		}
		// delete only -> write only
		updateChangingInfo(changingCol, changingIdxs, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		updateChangingInfo(changingCol, changingIdxs, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
>>>>>>> 75f748568... ddl: migrate part of ddl package code from Execute/ExecRestricted to safe API (1) (#22670)
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
	tblInfo, colInfo, err := checkDropColumn(t, job)
	if err != nil {
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
		if colInfo.GetOriginDefaultValue() == nil && mysql.HasNotNullFlag(colInfo.Flag) {
			// If the column is timestamp default current_timestamp, and DDL owner is new version TiDB that set column.Version to 1,
			// then old TiDB update record in the column write only stage will uses the wrong default value of the dropping column.
			// Because new version of the column default value is UTC time, but old version TiDB will think the default value is the time in system timezone.
			// But currently will be ok, because we can't cancel the drop column job when the job is running,
			// so the column will be dropped succeed and client will never see the wrong default value of the dropped column.
			// More info about this problem, see PR#9115.
			origDefVal, err := generateOriginDefaultValue(colInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = colInfo.SetOriginDefaultValue(origDefVal)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != colInfo.State)
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

func checkDropColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var colName model.CIStr
	err = job.DecodeArgs(&colName)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil {
		job.State = model.JobStateCancelled
		return nil, nil, ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}
	if err = isDroppableColumn(tblInfo, colName); err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}
	return tblInfo, colInfo, nil
}

func onSetDefaultValue(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	err := job.DecodeArgs(newCol)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return updateColumnDefaultValue(t, job, newCol, &newCol.Name)
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
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
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

	failpoint.Inject("uninitializedOffsetAndState", func(val failpoint.Value) {
		if val.(bool) {
			if newCol.State != model.StatePublic {
				failpoint.Return(ver, errors.New("the column state is wrong"))
			}
		}
	})

	// Column from null to not null.
	if !mysql.HasNotNullFlag(oldCol.Flag) && mysql.HasNotNullFlag(newCol.Flag) {
		noPreventNullFlag := !mysql.HasPreventNullInsertFlag(oldCol.Flag)
		// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
		err = modifyColsFromNull2NotNull(w, dbInfo, tblInfo, []*model.ColumnInfo{oldCol}, newCol.Name, oldCol.Tp != newCol.Tp)
		if err != nil {
			if ErrWarnDataTruncated.Equal(err) || errInvalidUseOfNull.Equal(err) {
				job.State = model.JobStateRollingback
			}
			return ver, err
		}
		// The column should get into prevent null status first.
		if noPreventNullFlag {
			return updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
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

	ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
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
func checkForNullValue(ctx sessionctx.Context, isDataTruncated bool, schema, table, newCol model.CIStr, oldCols ...*model.ColumnInfo) error {
	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where ")
	paramsList := make([]interface{}, 0, 2+len(oldCols))
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
<<<<<<< HEAD
	sql := fmt.Sprintf("select 1 from `%s`.`%s` where %s limit 1;", schema.L, table.L, colsStr)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
=======
	buf.WriteString(" limit 1")
	stmt, err := ctx.(sqlexec.RestrictedSQLExecutor).ParseWithParams(context.Background(), buf.String(), paramsList...)
	if err != nil {
		return errors.Trace(err)
	}
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedStmt(context.Background(), stmt)
>>>>>>> 75f748568... ddl: migrate part of ddl package code from Execute/ExecRestricted to safe API (1) (#22670)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) != 0 {
		if isDataTruncated {
			return ErrWarnDataTruncated.GenWithStackByArgs(newCol.L, len(rows))
		}
		return errInvalidUseOfNull
	}
	return nil
}

func updateColumnDefaultValue(t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldColName *model.CIStr) (ver int64, _ error) {
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	oldCol := model.FindColumnInfo(tblInfo.Columns, oldColName.L)
	if oldCol == nil || oldCol.State != model.StatePublic {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(newCol.Name, tblInfo.Name)
	}
	// The newCol's offset may be the value of the old schema version, so we can't use newCol directly.
	oldCol.DefaultValue = newCol.DefaultValue
	oldCol.DefaultValueBit = newCol.DefaultValueBit
	oldCol.Flag = newCol.Flag

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

func getColumnForeignKeyInfo(colName string, fkInfos []*model.FKInfo) *model.FKInfo {
	for _, fkInfo := range fkInfos {
		for _, col := range fkInfo.Cols {
			if col.L == colName {
				return fkInfo
			}
		}
	}
	return nil
}

func allocateColumnID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxColumnID++
	return tblInfo.MaxColumnID
}

func checkAddColumnTooManyColumns(colNum int) error {
	if uint32(colNum) > atomic.LoadUint32(&TableColumnCountLimit) {
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

// modifyColsFromNull2NotNull modifies the type definitions of 'null' to 'not null'.
// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
func modifyColsFromNull2NotNull(w *worker, dbInfo *model.DBInfo, tblInfo *model.TableInfo, cols []*model.ColumnInfo,
	newColName model.CIStr, isModifiedType bool) error {
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)

	// If there is a null value inserted, it cannot be modified and needs to be rollback.
	err = checkForNullValue(ctx, isModifiedType, dbInfo.Name, tblInfo.Name, newColName, cols...)
	if err != nil {
		return errors.Trace(err)
	}

	// Prevent this field from inserting null values.
	for _, col := range cols {
		col.Flag |= mysql.PreventNullInsertFlag
	}
	return nil
}

func generateOriginDefaultValue(col *model.ColumnInfo) (interface{}, error) {
	var err error
	odValue := col.GetDefaultValue()
	if odValue == nil && mysql.HasNotNullFlag(col.Flag) {
		switch col.Tp {
		// Just use enum field's first element for OriginDefaultValue.
		case mysql.TypeEnum:
			defEnum, verr := types.ParseEnumValue(col.FieldType.Elems, 1)
			if verr != nil {
				return nil, errors.Trace(verr)
			}
			defVal := types.NewMysqlEnumDatum(defEnum)
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
		if col.Tp == mysql.TypeTimestamp {
			odValue = time.Now().UTC().Format(types.TimeFormat)
		} else if col.Tp == mysql.TypeDatetime {
			odValue = time.Now().Format(types.TimeFormat)
		}
	}
	return odValue, nil
}

func findColumnInIndexCols(c string, cols []*ast.IndexColName) bool {
	for _, c1 := range cols {
		if c == c1.Column.Name.L {
			return true
		}
	}
	return false
}

func getColumnInfoByName(tbInfo *model.TableInfo, column string) *model.ColumnInfo {
	for _, colInfo := range tbInfo.Cols() {
		if colInfo.Name.L == column {
			return colInfo
		}
	}
	return nil
}

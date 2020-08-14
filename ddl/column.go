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
	"math/bits"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
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
	offsetChanged := make(map[int]int, len(newCols)-offset-1)
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
	offsetChanged := make(map[int]int, len(oldCols)-offset-1)
	for i := offset + 1; i < len(oldCols); i++ {
		offsetChanged[oldCols[i].Offset] = i - 1
		oldCols[i].Offset = i - 1
	}
	oldCols[offset].Offset = len(oldCols) - 1
	// For expression index, we drop hidden columns and index simultaneously.
	// So we need to change the offset of expression index.
	offsetChanged[offset] = len(oldCols) - 1
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

func createColumnInfo(tblInfo *model.TableInfo, colInfo *model.ColumnInfo, pos *ast.ColumnPosition) (*model.ColumnInfo, *ast.ColumnPosition, int, error) {
	// Check column name duplicate.
	cols := tblInfo.Columns
	offset := len(cols)
	// Should initialize pos when it is nil.
	if pos == nil {
		pos = &ast.ColumnPosition{}
	}
	// Get column offset.
	if pos.Tp == ast.ColumnPositionFirst {
		offset = 0
	} else if pos.Tp == ast.ColumnPositionAfter {
		c := model.FindColumnInfo(cols, pos.RelativeColumn.Name.L)
		if c == nil {
			return nil, pos, 0, infoschema.ErrColumnNotExists.GenWithStackByArgs(pos.RelativeColumn, tblInfo.Name)
		}

		// Insert offset is after the mentioned column.
		offset = c.Offset + 1
	}
	colInfo.ID = allocateColumnID(tblInfo)
	colInfo.State = model.StateNone
	// To support add column asynchronous, we should mark its offset as the last column.
	// So that we can use origin column offset to get value from row.
	colInfo.Offset = len(cols)

	// Append the column info to the end of the tblInfo.Columns.
	// It will reorder to the right offset in "Columns" when it state change to public.
	tblInfo.Columns = append(cols, colInfo)
	return colInfo, pos, offset, nil
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
		columnInfo, _, offset, err = createColumnInfo(tblInfo, col, pos)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		logutil.BgLogger().Info("[ddl] run add column job", zap.String("job", job.String()), zap.Reflect("columnInfo", *columnInfo), zap.Int("offset", offset))
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
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionAddColumn, TableInfo: tblInfo, ColumnInfos: []*model.ColumnInfo{columnInfo}})
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
		job.SchemaState = model.StateDeleteOnly
		setColumnsState(columnInfos, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != columnInfos[0].State)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		setColumnsState(columnInfos, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfos[0].State)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		setColumnsState(columnInfos, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfos[0].State)
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
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionAddColumns, TableInfo: tblInfo, ColumnInfos: columnInfos})
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
		job.SchemaState = model.StateWriteOnly
		setColumnsState(colInfos, model.StateWriteOnly)
		setIndicesState(idxInfos, model.StateWriteOnly)
		for _, colInfo := range colInfos {
			err = checkDropColumnForStatePublic(tblInfo, colInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != colInfos[0].State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		setColumnsState(colInfos, model.StateDeleteOnly)
		setIndicesState(idxInfos, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfos[0].State)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		setColumnsState(colInfos, model.StateDeleteReorganization)
		setIndicesState(idxInfos, model.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfos[0].State)
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
	if colInfo.OriginDefaultValue == nil && mysql.HasNotNullFlag(colInfo.Flag) {
		// If the column is timestamp default current_timestamp, and DDL owner is new version TiDB that set column.Version to 1,
		// then old TiDB update record in the column write only stage will uses the wrong default value of the dropping column.
		// Because new version of the column default value is UTC time, but old version TiDB will think the default value is the time in system timezone.
		// But currently will be ok, because we can't cancel the drop column job when the job is running,
		// so the column will be dropped succeed and client will never see the wrong default value of the dropped column.
		// More info about this problem, see PR#9115.
		colInfo.OriginDefaultValue, err = generateOriginDefaultValue(colInfo)
		if err != nil {
			return err
		}
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
		job.SchemaState = model.StateWriteOnly
		colInfo.State = model.StateWriteOnly
		setIndicesState(idxInfos, model.StateWriteOnly)
		err = checkDropColumnForStatePublic(tblInfo, colInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != colInfo.State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		colInfo.State = model.StateDeleteOnly
		setIndicesState(idxInfos, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		colInfo.State = model.StateDeleteReorganization
		setIndicesState(idxInfos, model.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != colInfo.State)
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

func (w *worker) onModifyColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	oldColName := &model.CIStr{}
	pos := &ast.ColumnPosition{}
	var modifyColumnTp byte
	var updatedAutoRandomBits uint64
	err := job.DecodeArgs(newCol, oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return w.doModifyColumn(t, job, newCol, oldColName, pos, modifyColumnTp, updatedAutoRandomBits)
}

// doModifyColumn updates the column information and reorders all columns.
func (w *worker) doModifyColumn(
	t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldName *model.CIStr,
	pos *ast.ColumnPosition, modifyColumnTp byte, newAutoRandBits uint64) (ver int64, _ error) {
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

	if newAutoRandBits > 0 {
		if err := checkAndApplyNewAutoRandomBits(job, t, tblInfo, newCol, oldName, newAutoRandBits); err != nil {
			return ver, errors.Trace(err)
		}
	}

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

func checkAndApplyNewAutoRandomBits(job *model.Job, t *meta.Meta, tblInfo *model.TableInfo,
	newCol *model.ColumnInfo, oldName *model.CIStr, newAutoRandBits uint64) error {
	schemaID := job.SchemaID
	newLayout := autoid.NewAutoRandomIDLayout(&newCol.FieldType, newAutoRandBits)

	// GenAutoRandomID first to prevent concurrent update.
	_, err := t.GenAutoRandomID(schemaID, tblInfo.ID, 1)
	if err != nil {
		return err
	}
	currentIncBitsVal, err := t.GetAutoRandomID(schemaID, tblInfo.ID)
	if err != nil {
		return err
	}
	// Find the max number of available shard bits by
	// counting leading zeros in current inc part of auto_random ID.
	availableBits := bits.LeadingZeros64(uint64(currentIncBitsVal))
	isOccupyingIncBits := newLayout.TypeBitsLength-newLayout.IncrementalBits > uint64(availableBits)
	if isOccupyingIncBits {
		availableBits := mathutil.Min(autoid.MaxAutoRandomBits, availableBits)
		errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg, availableBits, newAutoRandBits, oldName.O)
		job.State = model.JobStateCancelled
		return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	}
	tblInfo.AutoRandomBits = newAutoRandBits
	return nil
}

// checkForNullValue ensure there are no null values of the column of this table.
// `isDataTruncated` indicates whether the new field and the old field type are the same, in order to be compatible with mysql.
func checkForNullValue(ctx sessionctx.Context, isDataTruncated bool, schema, table, newCol model.CIStr, oldCols ...*model.ColumnInfo) error {
	colsStr := ""
	for i, col := range oldCols {
		if i == 0 {
			colsStr += "`" + col.Name.L + "` is null"
		} else {
			colsStr += " or `" + col.Name.L + "` is null"
		}
	}
	sql := fmt.Sprintf("select 1 from `%s`.`%s` where %s limit 1;", schema.L, table.L, colsStr)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		if isDataTruncated {
			return ErrWarnDataTruncated.GenWithStackByArgs(newCol.L, rowCount)
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

func isColumnCanDropWithIndex(colName string, indices []*model.IndexInfo) bool {
	for _, indexInfo := range indices {
		if indexInfo.Primary || len(indexInfo.Columns) > 1 {
			for _, col := range indexInfo.Columns {
				if col.Name.L == colName {
					return false
				}
			}
		}
	}
	return true
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
		zeroVal := table.GetZeroValue(col)
		odValue, err = zeroVal.ToString()
		if err != nil {
			return nil, errors.Trace(err)
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

func findColumnInIndexCols(c string, cols []*model.IndexColumn) *model.IndexColumn {
	for _, c1 := range cols {
		if c == c1.Name.L {
			return c1
		}
	}
	return nil
}

func getColumnInfoByName(tbInfo *model.TableInfo, column string) *model.ColumnInfo {
	for _, colInfo := range tbInfo.Cols() {
		if colInfo.Name.L == column {
			return colInfo
		}
	}
	return nil
}

// isVirtualGeneratedColumn checks the column if it is virtual.
func isVirtualGeneratedColumn(col *model.ColumnInfo) bool {
	if col.IsGenerated() && !col.GeneratedStored {
		return true
	}
	return false
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

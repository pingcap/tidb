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
	"bytes"
	"context"
	"fmt"
	"math/bits"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/prometheus/client_golang/prometheus"
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
		columnInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		columnInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		columnInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateWriteReorganization
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

	err = checkAndApplyAutoRandomBits(d, t, dbInfo, tblInfo, oldCol, jobParam.newCol, jobParam.updatedAutoRandomBits)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if !needChangeColumnData(oldCol, jobParam.newCol) {
		return w.doModifyColumn(d, t, job, dbInfo, tblInfo, jobParam.newCol, oldCol, jobParam.pos)
	}

	if err = isGeneratedRelatedColumn(tblInfo, jobParam.newCol, oldCol); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
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
	case model.StateWriteReorganization:
		tbl, err := getTable(d.store, dbInfo.ID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		reorgInfo, err := getReorgInfo(d, t, job, tbl, BuildElements(changingCol, changingIdxs))
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		// Inject a failpoint so that we can pause here and do verification on other components.
		// With a failpoint-enabled version of TiDB, you can trigger this failpoint by the following command:
		// enable: curl -X PUT -d "pause" "http://127.0.0.1:10080/fail/github.com/pingcap/tidb/ddl/mockDelayInModifyColumnTypeWithData".
		// disable: curl -X DELETE "http://127.0.0.1:10080/fail/github.com/pingcap/tidb/ddl/mockDelayInModifyColumnTypeWithData"
		failpoint.Inject("mockDelayInModifyColumnTypeWithData", func() {})
		err = w.runReorgJob(t, reorgInfo, tbl.Meta(), d.lease, func() (addIndexErr error) {
			defer util.Recover(metrics.LabelDDL, "onModifyColumn",
				func() {
					addIndexErr = errCancelledDDLJob.GenWithStack("modify table `%v` column `%v` panic", tblInfo.Name, oldCol.Name)
				}, false)
			return w.updateColumnAndIndexes(tbl, oldCol, changingCol, changingIdxs, reorgInfo)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// If timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if kv.IsTxnRetryableError(err) {
				// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
				w.reorgCtx.cleanNotifyReorgCancel()
				return ver, errors.Trace(err)
			}
			if err1 := t.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
				logutil.BgLogger().Warn("[ddl] run modify column job failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
					zap.String("job", job.String()), zap.Error(err1))
				return ver, errors.Trace(err)
			}
			logutil.BgLogger().Warn("[ddl] run modify column job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()

		// Remove the old column and indexes. Update the relative column name and index names.
		oldIdxIDs := make([]int64, 0, len(changingIdxs))
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		for _, cIdx := range changingIdxs {
			idxName := getChangingIndexOriginName(cIdx)
			for i, idx := range tblInfo.Indices {
				if strings.EqualFold(idxName, idx.Name.O) {
					cIdx.Name = model.NewCIStr(idxName)
					tblInfo.Indices[i] = cIdx
					oldIdxIDs = append(oldIdxIDs, idx.ID)
					break
				}
			}
		}
		changingColumnUniqueName := changingCol.Name
		changingCol.Name = colName
		changingCol.ChangeStateInfo = nil
		tblInfo.Indices = tblInfo.Indices[:len(tblInfo.Indices)-len(changingIdxs)]
		// Adjust table column offset.
		if err = adjustColumnInfoInModifyColumn(job, tblInfo, changingCol, oldCol, pos, changingColumnUniqueName.L); err != nil {
			// TODO: Do rollback.
			return ver, errors.Trace(err)
		}
		updateChangingInfo(changingCol, changingIdxs, model.StatePublic)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		// Refactor the job args to add the old index ids into delete range table.
		job.Args = []interface{}{oldIdxIDs, getPartitionIDs(tblInfo)}
		asyncNotifyEvent(d, &ddlutil.Event{Tp: model.ActionModifyColumn, TableInfo: tblInfo, ColumnInfos: []*model.ColumnInfo{changingCol}})
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("column", changingCol.State)
	}

	return ver, errors.Trace(err)
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

func (w *worker) updatePhysicalTableRow(t table.PhysicalTable, oldColInfo, colInfo *model.ColumnInfo, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[ddl] start to update table row", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writePhysicalTableRecord(t.(table.PhysicalTable), typeUpdateColumnWorker, nil, oldColInfo, colInfo, reorgInfo)
}

// updateColumnAndIndexes handles the modify column reorganization state for a table.
func (w *worker) updateColumnAndIndexes(t table.Table, oldCol, col *model.ColumnInfo, idxes []*model.IndexInfo, reorgInfo *reorgInfo) error {
	// TODO: Support partition tables.
	if bytes.Equal(reorgInfo.currElement.TypeKey, meta.ColumnElementKey) {
		err := w.updatePhysicalTableRow(t.(table.PhysicalTable), oldCol, col, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Get the original start handle and end handle.
	currentVer, err := getValidCurrentVersion(reorgInfo.d.store)
	if err != nil {
		return errors.Trace(err)
	}
	originalStartHandle, originalEndHandle, err := getTableRange(reorgInfo.d, t.(table.PhysicalTable), currentVer.Ver, reorgInfo.Job.Priority)
	if err != nil {
		return errors.Trace(err)
	}

	startElementOffset := 0
	startElementOffsetToResetHandle := -1
	// This backfill job starts with backfilling index data, whose index ID is currElement.ID.
	if bytes.Equal(reorgInfo.currElement.TypeKey, meta.IndexElementKey) {
		for i, idx := range idxes {
			if reorgInfo.currElement.ID == idx.ID {
				startElementOffset = i
				startElementOffsetToResetHandle = i
				break
			}
		}
	}

	for i := startElementOffset; i < len(idxes); i++ {
		// This backfill job has been exited during processing. At that time, the element is reorgInfo.elements[i+1] and handle range is [reorgInfo.StartHandle, reorgInfo.EndHandle].
		// Then the handle range of the rest elements' is [originalStartHandle, originalEndHandle].
		if i == startElementOffsetToResetHandle+1 {
			reorgInfo.StartKey, reorgInfo.EndKey = originalStartHandle, originalEndHandle
		}

		// Update the element in the reorgCtx to keep the atomic access for daemon-worker.
		w.reorgCtx.setCurrentElement(reorgInfo.elements[i+1])

		// Update the element in the reorgInfo for updating the reorg meta below.
		reorgInfo.currElement = reorgInfo.elements[i+1]
		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err := reorgInfo.UpdateReorgMeta(reorgInfo.StartKey)
		logutil.BgLogger().Info("[ddl] update column and indexes",
			zap.Int64("jobID", reorgInfo.Job.ID),
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.String("startHandle", tryDecodeToHandleString(reorgInfo.StartKey)),
			zap.String("endHandle", tryDecodeToHandleString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
		err = w.addTableIndex(t, idxes[i], reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type updateColumnWorker struct {
	*backfillWorker
	oldColInfo    *model.ColumnInfo
	newColInfo    *model.ColumnInfo
	metricCounter prometheus.Counter

	// The following attributes are used to reduce memory allocation.
	rowRecords []*rowRecord
	rowDecoder *decoder.RowDecoder

	rowMap map[int64]types.Datum

	// For SQL Mode and warnings.
	sqlMode mysql.SQLMode
}

func newUpdateColumnWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, oldCol, newCol *model.ColumnInfo, decodeColMap map[int64]decoder.Column, sqlMode mysql.SQLMode) *updateColumnWorker {
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	return &updateColumnWorker{
		backfillWorker: newBackfillWorker(sessCtx, worker, id, t),
		oldColInfo:     oldCol,
		newColInfo:     newCol,
		metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("update_col_speed"),
		rowDecoder:     rowDecoder,
		rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
		sqlMode:        sqlMode,
	}
}

func (w *updateColumnWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

type rowRecord struct {
	key     []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals    []byte        // It's the record.
	warning *terror.Error // It's used to record the cast warning of a record.
}

// getNextKey gets next handle of entry that we are going to process.
func (w *updateColumnWorker) getNextKey(taskRange reorgBackfillTask,
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
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startKey, taskRange.endKey,
		func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in updateColumnWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) > 0

			if taskDone || len(w.rowRecords) >= w.batchCnt {
				return false, nil
			}

			if err1 := w.getRowRecord(handle, recordKey, rawRow); err1 != nil {
				return false, errors.Trace(err1)
			}
			lastAccessedHandle = recordKey
			if recordKey.Cmp(taskRange.endKey) == 0 {
				// If taskRange.endIncluded == false, we will not reach here when handle == taskRange.endHandle.
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.rowRecords) == 0 {
		taskDone = true
	}

	logutil.BgLogger().Debug("[ddl] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()), zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.rowRecords, w.getNextKey(taskRange, taskDone, lastAccessedHandle), taskDone, errors.Trace(err)
}

func (w *updateColumnWorker) getRowRecord(handle kv.Handle, recordKey []byte, rawRow []byte) error {
	_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.sessCtx, handle, rawRow, time.UTC, w.rowMap)
	if err != nil {
		return errors.Trace(errCantDecodeRecord.GenWithStackByArgs("column", err))
	}

	if _, ok := w.rowMap[w.newColInfo.ID]; ok {
		// The column is already added by update or insert statement, skip it.
		w.cleanRowMap()
		return nil
	}

	var recordWarning *terror.Error
	// Since every updateColumnWorker handle their own work individually, we can cache warning in statement context when casting datum.
	oldWarn := w.sessCtx.GetSessionVars().StmtCtx.GetWarnings()
	if oldWarn == nil {
		oldWarn = []stmtctx.SQLWarn{}
	} else {
		oldWarn = oldWarn[:0]
	}
	w.sessCtx.GetSessionVars().StmtCtx.SetWarnings(oldWarn)
	newColVal, err := table.CastValue(w.sessCtx, w.rowMap[w.oldColInfo.ID], w.newColInfo, false, false)
	if err != nil {
		return w.reformatErrors(err)
	}
	if w.sessCtx.GetSessionVars().StmtCtx.GetWarnings() != nil && len(w.sessCtx.GetSessionVars().StmtCtx.GetWarnings()) != 0 {
		warn := w.sessCtx.GetSessionVars().StmtCtx.GetWarnings()
		recordWarning = errors.Cause(w.reformatErrors(warn[0].Err)).(*terror.Error)
	}

	failpoint.Inject("MockReorgTimeoutInOneRegion", func(val failpoint.Value) {
		if val.(bool) {
			if handle.IntValue() == 3000 && atomic.CompareAndSwapInt32(&TestCheckReorgTimeout, 0, 1) {
				failpoint.Return(errors.Trace(errWaitReorgTimeout))
			}
		}
	})

	w.rowMap[w.newColInfo.ID] = newColVal
	_, err = w.rowDecoder.EvalRemainedExprColumnMap(w.sessCtx, timeutil.SystemLocation(), w.rowMap)
	if err != nil {
		return errors.Trace(err)
	}

	newColumnIDs := make([]int64, 0, len(w.rowMap))
	newRow := make([]types.Datum, 0, len(w.rowMap))
	for colID, val := range w.rowMap {
		newColumnIDs = append(newColumnIDs, colID)
		newRow = append(newRow, val)
	}
	sctx, rd := w.sessCtx.GetSessionVars().StmtCtx, &w.sessCtx.GetSessionVars().RowEncoder
	newRowVal, err := tablecodec.EncodeRow(sctx, newRow, newColumnIDs, nil, nil, rd)
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
	if types.ErrTruncated.Equal(err) {
		err = types.ErrTruncated.GenWithStack("Data truncated for column '%s', value is '%s'", w.oldColInfo.Name, w.rowMap[w.oldColInfo.ID])
	}

	if types.ErrInvalidYear.Equal(err) {
		err = types.ErrInvalidYear.GenWithStack("Invalid year value for column '%s', value is '%s'", w.oldColInfo.Name, w.rowMap[w.oldColInfo.ID])
	}
	return err
}

func (w *updateColumnWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

// BackfillDataInTxn will backfill the table record in a transaction, lock corresponding rowKey, if the value of rowKey is changed.
func (w *updateColumnWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(context.Background(), w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)

		rowRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		warningsMap := make(map[errors.ErrorID]*terror.Error, len(rowRecords))
		warningsCountMap := make(map[errors.ErrorID]int64, len(rowRecords))
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
	logSlowOperations(time.Since(oprStartTime), "BackfillDataInTxn", 3000)

	return
}

func updateChangingInfo(changingCol *model.ColumnInfo, changingIdxs []*model.IndexInfo, schemaState model.SchemaState) {
	changingCol.State = schemaState
	for _, idx := range changingIdxs {
		idx.State = schemaState
	}
}

// doModifyColumn updates the column information and reorders all columns. It does not support modifying column data.
func (w *worker) doModifyColumn(
	d *ddlCtx, t *meta.Meta, job *model.Job, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	newCol, oldCol *model.ColumnInfo, pos *ast.ColumnPosition) (ver int64, _ error) {
	// Column from null to not null.
	if !mysql.HasNotNullFlag(oldCol.Flag) && mysql.HasNotNullFlag(newCol.Flag) {
		noPreventNullFlag := !mysql.HasPreventNullInsertFlag(oldCol.Flag)

		// lease = 0 means it's in an integration test. In this case we don't delay so the test won't run too slowly.
		// We need to check after the flag is set
		if d.lease > 0 && !noPreventNullFlag {
			delayForAsyncCommit()
		}

		// Introduce the `mysql.PreventNullInsertFlag` flag to prevent users from inserting or updating null values.
		err := modifyColsFromNull2NotNull(w, dbInfo, tblInfo, []*model.ColumnInfo{oldCol}, newCol.Name, oldCol.Tp != newCol.Tp)
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

	if err := adjustColumnInfoInModifyColumn(job, tblInfo, newCol, oldCol, pos, ""); err != nil {
		return ver, errors.Trace(err)
	}

	ver, err := updateVersionAndTableInfoWithCheck(t, job, tblInfo, true)
	if err != nil {
		// Modified the type definition of 'null' to 'not null' before this, so rollBack the job when an error occurs.
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	// For those column-type-change type which doesn't need reorg data, we should also mock the job args for delete range.
	job.Args = []interface{}{[]int64{}, []int64{}}
	return ver, nil
}

func adjustColumnInfoInModifyColumn(
	job *model.Job, tblInfo *model.TableInfo, newCol, oldCol *model.ColumnInfo, pos *ast.ColumnPosition, changingColUniqueLowerName string) error {
	// We need the latest column's offset and state. This information can be obtained from the store.
	newCol.Offset = oldCol.Offset
	newCol.State = oldCol.State
	// Calculate column's new position.
	oldPos, newPos := oldCol.Offset, oldCol.Offset
	if pos.Tp == ast.ColumnPositionAfter {
		// TODO: The check of "RelativeColumn" can be checked in advance. When "EnableChangeColumnType" is true, unnecessary state changes can be reduced.
		if oldCol.Name.L == pos.RelativeColumn.Name.L {
			// `alter table tableName modify column b int after b` will return ver,ErrColumnNotExists.
			// Modified the type definition of 'null' to 'not null' before this, so rollback the job when an error occurs.
			job.State = model.JobStateRollingback
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name)
		}

		relative := model.FindColumnInfo(tblInfo.Columns, pos.RelativeColumn.Name.L)
		if relative == nil || relative.State != model.StatePublic {
			job.State = model.JobStateRollingback
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(pos.RelativeColumn, tblInfo.Name)
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
	columnChanged[oldCol.Name.L] = newCol

	if newPos == oldPos {
		tblInfo.Columns[newPos] = newCol
	} else {
		cols := tblInfo.Columns

		// Reorder columns in place.
		if newPos < oldPos {
			// ******** +(new) ****** -(old) ********
			// [newPos:old-1] should shift one step to the right.
			copy(cols[newPos+1:], cols[newPos:oldPos])
		} else {
			// ******** -(old) ****** +(new) ********
			// [old+1:newPos] should shift one step to the left.
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
			cName := c.Name.L
			// With the unique changing column name, the format is designed as `_Col$_xx_uniqueNum`.
			// The suffix `_uniqueNum` will be trimmed when we get the origin changing column name.
			// There is a possibility that some other column named as `xx_xx_xx`, and it will be get
			// trimmed as `xx_xx`. So here we check here and only do the trim for the changing index column.
			if len(changingColUniqueLowerName) != 0 && c.Name.L == changingColUniqueLowerName {
				cName = strings.ToLower(getChangingColumnOriginName(c))
			}
			if newCol, ok := columnChanged[cName]; ok {
				c.Name = newCol.Name
				c.Offset = newCol.Offset
			}
		}
	}
	return nil
}

func checkAndApplyAutoRandomBits(d *ddlCtx, m *meta.Meta, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo, newCol *model.ColumnInfo, newAutoRandBits uint64) error {
	if newAutoRandBits == 0 {
		return nil
	}
	err := checkNewAutoRandomBits(m, dbInfo.ID, tblInfo.ID, oldCol, newCol, newAutoRandBits)
	if err != nil {
		return err
	}
	return applyNewAutoRandomBits(d, m, dbInfo, tblInfo, oldCol, newAutoRandBits)
}

// checkNewAutoRandomBits checks whether the new auto_random bits number can cause overflow.
func checkNewAutoRandomBits(m *meta.Meta, schemaID, tblID int64,
	oldCol *model.ColumnInfo, newCol *model.ColumnInfo, newAutoRandBits uint64) error {
	newLayout := autoid.NewShardIDLayout(&newCol.FieldType, newAutoRandBits)

	allocTp := autoid.AutoRandomType
	convertedFromAutoInc := mysql.HasAutoIncrementFlag(oldCol.Flag)
	if convertedFromAutoInc {
		allocTp = autoid.AutoIncrementType
	}
	// GenerateAutoID first to prevent concurrent update in DML.
	_, err := autoid.GenerateAutoID(m, schemaID, tblID, 1, allocTp)
	if err != nil {
		return err
	}
	currentIncBitsVal, err := autoid.GetAutoID(m, schemaID, tblID, allocTp)
	if err != nil {
		return err
	}
	// Find the max number of available shard bits by
	// counting leading zeros in current inc part of auto_random ID.
	usedBits := uint64(64 - bits.LeadingZeros64(uint64(currentIncBitsVal)))
	if usedBits > newLayout.IncrementalBits {
		overflowCnt := usedBits - newLayout.IncrementalBits
		errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg, newAutoRandBits-overflowCnt, newAutoRandBits, oldCol.Name.O)
		return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	}
	return nil
}

// applyNewAutoRandomBits set auto_random bits to TableInfo and
// migrate auto_increment ID to auto_random ID if possible.
func applyNewAutoRandomBits(d *ddlCtx, m *meta.Meta, dbInfo *model.DBInfo,
	tblInfo *model.TableInfo, oldCol *model.ColumnInfo, newAutoRandBits uint64) error {
	tblInfo.AutoRandomBits = newAutoRandBits
	needMigrateFromAutoIncToAutoRand := mysql.HasAutoIncrementFlag(oldCol.Flag)
	if !needMigrateFromAutoIncToAutoRand {
		return nil
	}
	autoRandAlloc := autoid.NewAllocatorsFromTblInfo(d.store, dbInfo.ID, tblInfo).Get(autoid.AutoRandomType)
	if autoRandAlloc == nil {
		errMsg := fmt.Sprintf(autoid.AutoRandomAllocatorNotFound, dbInfo.Name.O, tblInfo.Name.O)
		return ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	}
	nextAutoIncID, err := m.GetAutoTableID(dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = autoRandAlloc.Rebase(tblInfo.ID, nextAutoIncID, false)
	if err != nil {
		return errors.Trace(err)
	}
	if err := m.CleanAutoID(dbInfo.ID, tblInfo.ID); err != nil {
		return errors.Trace(err)
	}
	return nil
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
	buf.WriteString(" limit 1")
	stmt, err := ctx.(sqlexec.RestrictedSQLExecutor).ParseWithParams(context.Background(), buf.String(), paramsList...)
	if err != nil {
		return errors.Trace(err)
	}
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedStmt(context.Background(), stmt)
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
	if uint32(colNum) > atomic.LoadUint32(&config.GetGlobalConfig().TableColumnCountLimit) {
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
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	// For those column-type-change type which doesn't need reorg data, we should also mock the job args for delete range.
	job.Args = []interface{}{[]int64{}, []int64{}}
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

	skipCheck := false
	failpoint.Inject("skipMockContextDoExec", func(val failpoint.Value) {
		if val.(bool) {
			skipCheck = true
		}
	})
	if !skipCheck {
		// If there is a null value inserted, it cannot be modified and needs to be rollback.
		err = checkForNullValue(ctx, isModifiedType, dbInfo.Name, tblInfo.Name, newColName, cols...)
		if err != nil {
			return errors.Trace(err)
		}
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
			defVal := types.NewCollateMysqlEnumDatum(defEnum, col.Collate)
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

func getChangingColumnOriginName(changingCol *model.IndexColumn) string {
	colName := strings.ToLower(strings.TrimPrefix(changingCol.Name.O, changingColumnPrefix))
	// Since the unique colName may contain the suffix number (columnName_num), better trim the suffix.
	var pos int
	if pos = strings.LastIndex(colName, "_"); pos == -1 {
		return colName
	}
	return colName[:pos]
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

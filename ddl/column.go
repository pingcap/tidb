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
	"fmt"
	"math/bits"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

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
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
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
		ver, err = onDropColumn(d, t, job)
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
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		columnInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		columnInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Update the job state when all affairs done.
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		// Adjust table column offset.
		tblInfo.MoveColumnInfo(columnInfo.Offset, offset)
		columnInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &ddlutil.Event{Tp: model.ActionAddColumn, TableInfo: tblInfo, ColumnInfos: []*model.ColumnInfo{columnInfo}})
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", columnInfo.State)
	}

	return ver, errors.Trace(err)
}

func checkAddColumns(t *meta.Meta, job *model.Job) (*model.TableInfo, []*model.ColumnInfo, []*model.ColumnInfo, []*ast.ColumnPosition, []int, []bool, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
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
		ver, err = onDropColumns(d, t, job)
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
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != columnInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		setColumnsState(columnInfos, model.StateWriteOnly)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != columnInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		setColumnsState(columnInfos, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != columnInfos[0].State)
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
			tblInfo.MoveColumnInfo(len(tblInfo.Columns)-1, offsets[i])
		}
		setColumnsState(columnInfos, model.StatePublic)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != columnInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		asyncNotifyEvent(d, &ddlutil.Event{Tp: model.ActionAddColumns, TableInfo: tblInfo, ColumnInfos: columnInfos})
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", columnInfos[0].State)
	}

	return ver, errors.Trace(err)
}

func onDropColumns(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
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
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != colInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> delete only
		setColumnsState(colInfos, model.StateDeleteOnly)
		if len(idxInfos) > 0 {
			newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
			for _, idx := range tblInfo.Indices {
				if !indexInfoContains(idx.ID, idxInfos) {
					newIndices = append(newIndices, idx)
				}
			}
			tblInfo.Indices = newIndices
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != colInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.Args = append(job.Args, indexInfosToIDList(idxInfos))
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> reorganization
		setColumnsState(colInfos, model.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != colInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteReorganization
	case model.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-delCount]
		setColumnsState(colInfos, model.StateNone)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != colInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, getPartitionIDs(tblInfo))
		}
	default:
		err = dbterror.ErrInvalidDDLJob.GenWithStackByArgs("table", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropColumns(t *meta.Meta, job *model.Job) (*model.TableInfo, []*model.ColumnInfo, int, []*model.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, 0, nil, errors.Trace(err)
	}

	var colNames []model.CIStr
	var ifExists []bool
	// indexIds is used to make sure we don't truncate args when decoding the rawArgs.
	var indexIds []int64
	err = job.DecodeArgs(&colNames, &ifExists, &indexIds)
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
			return nil, nil, 0, nil, dbterror.ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
		}
		if err = isDroppableColumn(job.MultiSchemaInfo != nil, tblInfo, colName); err != nil {
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
	if len(indexIds) > 0 {
		job.Args = append(job.Args, indexIds)
	}
	return tblInfo, colInfos, len(colInfos), indexInfos, nil
}

func checkDropColumnForStatePublic(tblInfo *model.TableInfo, colInfo *model.ColumnInfo) (err error) {
	// Set this column's offset to the last and reset all following columns' offsets.
	adjustColumnInfoInDropColumn(tblInfo, colInfo.Offset)
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

func onDropColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
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
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		colInfo.State = model.StateDeleteOnly
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
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != colInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		// All reorganization jobs are done, drop this column.
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

func checkDropColumn(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ColumnInfo, []*model.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	var colName model.CIStr
	// indexIds is used to make sure we don't truncate args when decoding the rawArgs.
	var indexIds []int64
	err = job.DecodeArgs(&colName, &indexIds)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, errors.Trace(err)
	}

	colInfo := model.FindColumnInfo(tblInfo.Columns, colName.L)
	if colInfo == nil || colInfo.Hidden {
		job.State = model.JobStateCancelled
		return nil, nil, nil, dbterror.ErrCantDropFieldOrKey.GenWithStack("column %s doesn't exist", colName)
	}
	if err = isDroppableColumn(job.MultiSchemaInfo != nil, tblInfo, colName); err != nil {
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

func onSetDefaultValue(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	newCol := &model.ColumnInfo{}
	err := job.DecodeArgs(newCol)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	return updateColumnDefaultValue(d, t, job, newCol, &newCol.Name)
}

func needChangeColumnData(oldCol, newCol *model.ColumnInfo) bool {
	toUnsigned := mysql.HasUnsignedFlag(newCol.GetFlag())
	originUnsigned := mysql.HasUnsignedFlag(oldCol.GetFlag())
	needTruncationOrToggleSign := func() bool {
		return (newCol.GetFlen() > 0 && newCol.GetFlen() < oldCol.GetFlen()) || (toUnsigned != originUnsigned)
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
			return isElemsChangedToModifyColumn(oldCol.GetElems(), newCol.GetElems())
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return toUnsigned != originUnsigned
		case mysql.TypeString:
			// Due to the behavior of padding \x00 at binary type, always change column data when binary length changed
			if types.IsBinaryStr(&oldCol.FieldType) {
				return newCol.GetFlen() != oldCol.GetFlen()
			}
		}

		return needTruncationOrToggleSign()
	}

	if convertBetweenCharAndVarchar(oldCol.GetType(), newCol.GetType()) {
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

// TODO: it is used for plugins. so change plugin's using and remove it.
func convertBetweenCharAndVarchar(oldCol, newCol byte) bool {
	return types.ConvertBetweenCharAndVarchar(oldCol, newCol)
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

type modifyingColInfo struct {
	newCol                *model.ColumnInfo
	oldColName            *model.CIStr
	modifyColumnTp        byte
	updatedAutoRandomBits uint64
	changingCol           *model.ColumnInfo
	changingIdxs          []*model.IndexInfo
	pos                   *ast.ColumnPosition
}

func getModifyColumnInfo(t *meta.Meta, job *model.Job) (*model.DBInfo, *model.TableInfo, *model.ColumnInfo, *modifyingColInfo, error) {
	modifyInfo := &modifyingColInfo{pos: &ast.ColumnPosition{}}
	err := job.DecodeArgs(&modifyInfo.newCol, &modifyInfo.oldColName, modifyInfo.pos, &modifyInfo.modifyColumnTp, &modifyInfo.updatedAutoRandomBits, &modifyInfo.changingCol, &modifyInfo.changingIdxs)
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

// getOriginDefaultValueForModifyColumn gets the original default value for modifying column.
// Since column type change is implemented as adding a new column then substituting the old one.
// Case exists when update-where statement fetch a NULL for not-null column without any default data,
// it will errors.
// So we set original default value here to prevent this error. If the oldCol has the original default value, we use it.
// Otherwise we set the zero value as original default value.
// Besides, in insert & update records, we have already implement using the casted value of relative column to insert
// rather than the original default value.
func getOriginDefaultValueForModifyColumn(d *ddlCtx, changingCol, oldCol *model.ColumnInfo) (interface{}, error) {
	var err error
	originDefVal := oldCol.GetOriginDefaultValue()
	if originDefVal != nil {
		sessCtx := newContext(d.store)
		odv, err := table.CastValue(sessCtx, types.NewDatum(originDefVal), changingCol, false, false)
		if err != nil {
			logutil.BgLogger().Info("[ddl] cast origin default value failed", zap.Error(err))
		}
		if !odv.IsNull() {
			if originDefVal, err = odv.ToString(); err != nil {
				originDefVal = nil
				logutil.BgLogger().Info("[ddl] convert default value to string failed", zap.Error(err))
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

func (w *worker) onModifyColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	dbInfo, tblInfo, oldCol, modifyInfo, err := getModifyColumnInfo(t, job)
	if err != nil {
		return ver, err
	}

	if job.IsRollingback() {
		// For those column-type-change jobs which don't reorg the data.
		if !needChangeColumnData(oldCol, modifyInfo.newCol) {
			return rollbackModifyColumnJob(d, t, tblInfo, job, modifyInfo.newCol, oldCol, modifyInfo.modifyColumnTp)
		}
		// For those column-type-change jobs which reorg the data.
		return rollbackModifyColumnJobWithData(d, t, tblInfo, job, oldCol, modifyInfo)
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
		if val.(bool) {
			if modifyInfo.newCol.State != model.StatePublic {
				failpoint.Return(ver, errors.New("the column state is wrong"))
			}
		}
	})

	err = checkAndApplyAutoRandomBits(d, t, dbInfo, tblInfo, oldCol, modifyInfo.newCol, modifyInfo.updatedAutoRandomBits)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if !needChangeColumnData(oldCol, modifyInfo.newCol) {
		return w.doModifyColumn(d, t, job, dbInfo, tblInfo, modifyInfo.newCol, oldCol, modifyInfo.pos)
	}

	if err = isGeneratedRelatedColumn(tblInfo, modifyInfo.newCol, oldCol); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if modifyInfo.changingCol == nil {
		changingColPos := &ast.ColumnPosition{Tp: ast.ColumnPositionNone}
		newColName := model.NewCIStr(genChangingColumnUniqueName(tblInfo, oldCol))
		if mysql.HasPriKeyFlag(oldCol.GetFlag()) {
			job.State = model.JobStateCancelled
			msg := "this column has primary key flag"
			return ver, dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}

		modifyInfo.changingCol = modifyInfo.newCol.Clone()
		modifyInfo.changingCol.Name = newColName
		modifyInfo.changingCol.ChangeStateInfo = &model.ChangeStateInfo{DependencyColumnOffset: oldCol.Offset}
		originDefVal, err := getOriginDefaultValueForModifyColumn(d, modifyInfo.changingCol, oldCol)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = modifyInfo.changingCol.SetOriginDefaultValue(originDefVal); err != nil {
			return ver, errors.Trace(err)
		}

		_, _, _, err = createColumnInfo(tblInfo, modifyInfo.changingCol, changingColPos)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		idxInfos, offsets := findIndexesByColName(tblInfo.Indices, oldCol.Name.L)
		modifyInfo.changingIdxs = make([]*model.IndexInfo, 0, len(idxInfos))
		for i, idxInfo := range idxInfos {
			newIdxInfo := idxInfo.Clone()
			newIdxInfo.Name = model.NewCIStr(genChangingIndexUniqueName(tblInfo, idxInfo))
			newIdxInfo.ID = allocateIndexID(tblInfo)
			newIdxChangingCol := newIdxInfo.Columns[offsets[i]]
			newIdxChangingCol.Name = newColName
			newIdxChangingCol.Offset = modifyInfo.changingCol.Offset
			canPrefix := types.IsTypePrefixable(modifyInfo.changingCol.GetType())
			if !canPrefix || (canPrefix && modifyInfo.changingCol.GetFlen() < newIdxChangingCol.Length) {
				newIdxChangingCol.Length = types.UnspecifiedLength
			}
			modifyInfo.changingIdxs = append(modifyInfo.changingIdxs, newIdxInfo)
		}
		tblInfo.Indices = append(tblInfo.Indices, modifyInfo.changingIdxs...)
	} else {
		tblInfo.Columns[len(tblInfo.Columns)-1] = modifyInfo.changingCol
		copy(tblInfo.Indices[len(tblInfo.Indices)-len(modifyInfo.changingIdxs):], modifyInfo.changingIdxs)
	}

	return w.doModifyColumnTypeWithData(d, t, job, dbInfo, tblInfo, modifyInfo.changingCol, oldCol, modifyInfo.newCol.Name, modifyInfo.pos, modifyInfo.changingIdxs)
}

// rollbackModifyColumnJobWithData is used to rollback modify-column job which need to reorg the data.
func rollbackModifyColumnJobWithData(d *ddlCtx, t *meta.Meta, tblInfo *model.TableInfo, job *model.Job, oldCol *model.ColumnInfo, modifyInfo *modifyingColInfo) (ver int64, err error) {
	// If the not-null change is included, we should clean the flag info in oldCol.
	if modifyInfo.modifyColumnTp == mysql.TypeNull {
		// Reset NotNullFlag flag.
		tblInfo.Columns[oldCol.Offset].SetFlag(oldCol.GetFlag() &^ mysql.NotNullFlag)
		// Reset PreventNullInsertFlag flag.
		tblInfo.Columns[oldCol.Offset].SetFlag(oldCol.GetFlag() &^ mysql.PreventNullInsertFlag)
	}
	if modifyInfo.changingCol != nil {
		// changingCol isn't nil means the job has been in the mid state. These appended changingCol and changingIndex should
		// be removed from the tableInfo as well.
		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-1]
		tblInfo.Indices = tblInfo.Indices[:len(tblInfo.Indices)-len(modifyInfo.changingIdxs)]
	}
	ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	// Refactor the job args to add the abandoned temporary index ids into delete range table.
	idxIDs := make([]int64, 0, len(modifyInfo.changingIdxs))
	for _, idx := range modifyInfo.changingIdxs {
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
	targetCol := changingCol.Clone()
	targetCol.Name = colName
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
				var ctx sessionctx.Context
				ctx, err := w.sessPool.get()
				if err != nil {
					failpoint.Return(ver, err)
				}
				defer w.sessPool.put(ctx)

				_, _, err = ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(context.Background(), nil, valStr)
				if err != nil {
					job.State = model.JobStateCancelled
					failpoint.Return(ver, err)
				}
			}
		})
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != changingCol.State)
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
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		updateChangingObjState(changingCol, changingIdxs, model.StateWriteReorganization)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != changingCol.State)
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

		var done bool
		done, ver, err = doReorgWorkForModifyColumn(w, d, t, job, tbl, oldCol, changingCol, changingIdxs)
		if !done {
			return ver, err
		}

		oldIdxIDs := getOldIndexIDs(tblInfo, oldCol) // used by GC delete range.

		err = adjustTableInfoAfterModifyColumnWithData(tblInfo, pos, oldCol, changingCol, colName, changingIdxs)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}

		updateChangingObjState(changingCol, changingIdxs, model.StatePublic)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != changingCol.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		// Refactor the job args to add the old index ids into delete range table.
		job.Args = []interface{}{oldIdxIDs, getPartitionIDs(tblInfo)}
		asyncNotifyEvent(d, &ddlutil.Event{Tp: model.ActionModifyColumn, TableInfo: tblInfo, ColumnInfos: []*model.ColumnInfo{changingCol}})
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", changingCol.State)
	}

	return ver, errors.Trace(err)
}

func doReorgWorkForModifyColumn(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job, tbl table.Table,
	oldCol, changingCol *model.ColumnInfo, changingIdxs []*model.IndexInfo) (done bool, ver int64, err error) {
	reorgInfo, err := getReorgInfo(w.JobContext, d, t, job, tbl, BuildElements(changingCol, changingIdxs))
	if err != nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}

	// Inject a failpoint so that we can pause here and do verification on other components.
	// With a failpoint-enabled version of TiDB, you can trigger this failpoint by the following command:
	// enable: curl -X PUT -d "pause" "http://127.0.0.1:10080/fail/github.com/pingcap/tidb/ddl/mockDelayInModifyColumnTypeWithData".
	// disable: curl -X DELETE "http://127.0.0.1:10080/fail/github.com/pingcap/tidb/ddl/mockDelayInModifyColumnTypeWithData"
	failpoint.Inject("mockDelayInModifyColumnTypeWithData", func() {})
	err = w.runReorgJob(t, reorgInfo, tbl.Meta(), d.lease, func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onModifyColumn",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("modify table `%v` column `%v` panic", tbl.Meta().Name, oldCol.Name)
			}, false)
		// Use old column name to generate less confusing error messages.
		changingColCpy := changingCol.Clone()
		changingColCpy.Name = oldCol.Name
		return w.updateColumnAndIndexes(tbl, oldCol, changingColCpy, changingIdxs, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// If timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.IsTxnRetryableError(err) {
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			w.reorgCtx.cleanNotifyReorgCancel()
			return false, ver, errors.Trace(err)
		}
		if err1 := t.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.BgLogger().Warn("[ddl] run modify column job failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
				zap.String("job", job.String()), zap.Error(err1))
		}
		logutil.BgLogger().Warn("[ddl] run modify column job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
		job.State = model.JobStateRollingback
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()
		return false, ver, errors.Trace(err)
	}
	// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
	w.reorgCtx.cleanNotifyReorgCancel()
	return true, ver, nil
}

func adjustTableInfoAfterModifyColumnWithData(tblInfo *model.TableInfo, pos *ast.ColumnPosition,
	oldCol, changingCol *model.ColumnInfo, newName model.CIStr, changingIdxs []*model.IndexInfo) (err error) {
	if pos != nil && pos.RelativeColumn != nil && oldCol.Name.L == pos.RelativeColumn.Name.L {
		// For cases like `modify column b after b`, it should report this error.
		return errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name))
	}
	internalColName := changingCol.Name
	changingCol = replaceOldColumn(tblInfo, oldCol, changingCol, newName)
	if len(changingIdxs) > 0 {
		replaceOldIndexes(tblInfo, changingIdxs)
		updateNewIndexesCols(tblInfo, internalColName, newName, changingCol.Offset)
	}
	// Move the new column to a correct offset.
	destOffset, err := locateOffsetToMove(changingCol.Offset, pos, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}
	tblInfo.MoveColumnInfo(changingCol.Offset, destOffset)
	return nil
}

func replaceOldColumn(tblInfo *model.TableInfo, oldCol, changingCol *model.ColumnInfo,
	newName model.CIStr) *model.ColumnInfo {
	// Replace the old column.
	tblInfo.Columns[oldCol.Offset] = changingCol
	tblInfo.Columns[changingCol.Offset] = nil
	changingCol = updateChangingCol(changingCol, newName, oldCol.Offset)
	// Remove nil column.
	tmp := tblInfo.Columns[:0]
	for _, c := range tblInfo.Columns {
		if c != nil {
			tmp = append(tmp, c)
		}
	}
	tblInfo.Columns = tmp
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
				cIdx.Name = model.NewCIStr(idxName)
				tblInfo.Indices[i] = cIdx
				break
			}
		}
	}
}

func updateNewIndexesCols(tblInfo *model.TableInfo, oldName, newName model.CIStr, newOffset int) {
	for _, idx := range tblInfo.Indices {
		for i, col := range idx.Columns {
			if col.Name.L == oldName.L {
				idx.Columns[i].Name = newName
				idx.Columns[i].Offset = newOffset
				break
			}
		}
	}
}

func updateChangingCol(col *model.ColumnInfo, newName model.CIStr, newOffset int) *model.ColumnInfo {
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

func getOldIndexIDs(tblInfo *model.TableInfo, oldCol *model.ColumnInfo) []int64 {
	var oldIdxIDs []int64
	for _, idx := range tblInfo.Indices {
		for _, idxCol := range idx.Columns {
			if tblInfo.Columns[idxCol.Offset].ID == oldCol.ID {
				oldIdxIDs = append(oldIdxIDs, idx.ID)
				break
			}
		}
	}
	return oldIdxIDs
}

func locateOffsetToMove(currentOffset int, pos *ast.ColumnPosition, tblInfo *model.TableInfo) (destOffset int, err error) {
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

func (w *worker) updatePhysicalTableRow(t table.PhysicalTable, oldColInfo, colInfo *model.ColumnInfo, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[ddl] start to update table row", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writePhysicalTableRecord(t, typeUpdateColumnWorker, nil, oldColInfo, colInfo, reorgInfo)
}

// TestReorgGoroutineRunning is only used in test to indicate the reorg goroutine has been started.
var TestReorgGoroutineRunning = make(chan interface{})

// updateColumnAndIndexes handles the modify column reorganization state for a table.
func (w *worker) updateColumnAndIndexes(t table.Table, oldCol, col *model.ColumnInfo, idxes []*model.IndexInfo, reorgInfo *reorgInfo) error {
	failpoint.Inject("mockInfiniteReorgLogic", func(val failpoint.Value) {
		if val.(bool) {
			a := new(interface{})
			TestReorgGoroutineRunning <- a
			for {
				time.Sleep(30 * time.Millisecond)
				if w.reorgCtx.isReorgCanceled() {
					// Job is cancelled. So it can't be done.
					failpoint.Return(dbterror.ErrCancelledDDLJob)
				}
			}
		}
	})
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
	originalStartHandle, originalEndHandle, err := getTableRange(w.JobContext, reorgInfo.d, t.(table.PhysicalTable), currentVer.Ver, reorgInfo.Job.Priority)
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
		metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("update_col_rate"),
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
	err := iterateSnapshotRows(w.ddlWorker.JobContext, w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startKey, taskRange.endKey,
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
	sysTZ := w.sessCtx.GetSessionVars().StmtCtx.TimeZone
	_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.sessCtx, handle, rawRow, sysTZ, w.rowMap)
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
	oldWarn := w.sessCtx.GetSessionVars().StmtCtx.GetWarnings()
	if oldWarn == nil {
		oldWarn = []stmtctx.SQLWarn{}
	} else {
		oldWarn = oldWarn[:0]
	}
	w.sessCtx.GetSessionVars().StmtCtx.SetWarnings(oldWarn)
	val := w.rowMap[w.oldColInfo.ID]
	col := w.newColInfo
	if val.Kind() == types.KindNull && col.FieldType.GetType() == mysql.TypeTimestamp && mysql.HasNotNullFlag(col.GetFlag()) {
		if v, err := expression.GetTimeCurrentTimestamp(w.sessCtx, col.GetType(), col.GetDecimal()); err == nil {
			// convert null value to timestamp should be substituted with current timestamp if NOT_NULL flag is set.
			w.rowMap[w.oldColInfo.ID] = v
		}
	}
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
				failpoint.Return(errors.Trace(dbterror.ErrWaitReorgTimeout))
			}
		}
	})

	w.rowMap[w.newColInfo.ID] = newColVal
	_, err = w.rowDecoder.EvalRemainedExprColumnMap(w.sessCtx, w.rowMap)
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

// BackfillDataInTxn will backfill the table record in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed.
func (w *updateColumnWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(context.Background(), w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.ddlWorker.getResourceGroupTaggerForTopSQL(); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

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

func updateChangingObjState(changingCol *model.ColumnInfo, changingIdxs []*model.IndexInfo, schemaState model.SchemaState) {
	changingCol.State = schemaState
	for _, idx := range changingIdxs {
		idx.State = schemaState
	}
}

// doModifyColumn updates the column information and reorders all columns. It does not support modifying column data.
func (w *worker) doModifyColumn(
	d *ddlCtx, t *meta.Meta, job *model.Job, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	newCol, oldCol *model.ColumnInfo, pos *ast.ColumnPosition) (ver int64, _ error) {
	if oldCol.ID != newCol.ID {
		job.State = model.JobStateRollingback
		return ver, dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column %s id %d does not exist, this column may have been updated by other DDL ran in parallel", oldCol.Name, newCol.ID)
	}
	// Column from null to not null.
	if !mysql.HasNotNullFlag(oldCol.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag()) {
		noPreventNullFlag := !mysql.HasPreventNullInsertFlag(oldCol.GetFlag())

		// lease = 0 means it's in an integration test. In this case we don't delay so the test won't run too slowly.
		// We need to check after the flag is set
		if d.lease > 0 && !noPreventNullFlag {
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
			return updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
		}
	}

	if err := adjustTableInfoAfterModifyColumn(tblInfo, newCol, oldCol, pos); err != nil {
		job.State = model.JobStateRollingback
		return ver, errors.Trace(err)
	}

	ver, err := updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
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

func adjustTableInfoAfterModifyColumn(
	tblInfo *model.TableInfo, newCol, oldCol *model.ColumnInfo, pos *ast.ColumnPosition) error {
	// We need the latest column's offset and state. This information can be obtained from the store.
	newCol.Offset = oldCol.Offset
	newCol.State = oldCol.State
	if pos != nil && pos.RelativeColumn != nil && oldCol.Name.L == pos.RelativeColumn.Name.L {
		// For cases like `modify column b after b`, it should report this error.
		return errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name))
	}
	destOffset, err := locateOffsetToMove(oldCol.Offset, pos, tblInfo)
	if err != nil {
		return errors.Trace(infoschema.ErrColumnNotExists.GenWithStackByArgs(oldCol.Name, tblInfo.Name))
	}
	tblInfo.Columns[oldCol.Offset] = newCol
	tblInfo.MoveColumnInfo(oldCol.Offset, destOffset)
	updateNewIndexesCols(tblInfo, oldCol.Name, newCol.Name, newCol.Offset)
	return nil
}

func checkAndApplyAutoRandomBits(d *ddlCtx, m *meta.Meta, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo, newCol *model.ColumnInfo, newAutoRandBits uint64) error {
	if newAutoRandBits == 0 {
		return nil
	}
	idAcc := m.GetAutoIDAccessors(dbInfo.ID, tblInfo.ID)
	err := checkNewAutoRandomBits(idAcc, oldCol, newCol, newAutoRandBits, tblInfo.Version)
	if err != nil {
		return err
	}
	return applyNewAutoRandomBits(d, m, dbInfo, tblInfo, oldCol, newAutoRandBits)
}

// checkNewAutoRandomBits checks whether the new auto_random bits number can cause overflow.
func checkNewAutoRandomBits(idAccessors meta.AutoIDAccessors, oldCol *model.ColumnInfo,
	newCol *model.ColumnInfo, newAutoRandBits uint64, tblInfoVer uint16) error {
	newLayout := autoid.NewShardIDLayout(&newCol.FieldType, newAutoRandBits)

	idAcc := idAccessors.RandomID()
	convertedFromAutoInc := mysql.HasAutoIncrementFlag(oldCol.GetFlag())
	if convertedFromAutoInc {
		idAcc = idAccessors.IncrementID(tblInfoVer)
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
	if usedBits > newLayout.IncrementalBits {
		overflowCnt := usedBits - newLayout.IncrementalBits
		errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg, newAutoRandBits-overflowCnt, newAutoRandBits, oldCol.Name.O)
		return dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
	}
	return nil
}

// applyNewAutoRandomBits set auto_random bits to TableInfo and
// migrate auto_increment ID to auto_random ID if possible.
func applyNewAutoRandomBits(d *ddlCtx, m *meta.Meta, dbInfo *model.DBInfo,
	tblInfo *model.TableInfo, oldCol *model.ColumnInfo, newAutoRandBits uint64) error {
	tblInfo.AutoRandomBits = newAutoRandBits
	needMigrateFromAutoIncToAutoRand := mysql.HasAutoIncrementFlag(oldCol.GetFlag())
	if !needMigrateFromAutoIncToAutoRand {
		return nil
	}
	autoRandAlloc := autoid.NewAllocatorsFromTblInfo(d.store, dbInfo.ID, tblInfo).Get(autoid.AutoRandomType)
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
func checkForNullValue(ctx context.Context, sctx sessionctx.Context, isDataTruncated bool, schema, table model.CIStr, newCol *model.ColumnInfo, oldCols ...*model.ColumnInfo) error {
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
	rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, nil, buf.String(), paramsList...)
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

func updateColumnDefaultValue(d *ddlCtx, t *meta.Meta, job *model.Job, newCol *model.ColumnInfo, oldColName *model.CIStr) (ver int64, _ error) {
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, job.SchemaID)
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
	oldCol.SetFlag(newCol.GetFlag())

	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
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

func isColumnCanDropWithIndex(isMultiSchemaChange bool, colName string, indices []*model.IndexInfo) error {
	for _, indexInfo := range indices {
		if indexInfo.Primary || len(indexInfo.Columns) > 1 {
			for _, col := range indexInfo.Columns {
				if col.Name.L == colName {
					return dbterror.ErrCantDropColWithIndex.GenWithStack("can't drop column %s with composite index covered or Primary Key covered now", colName)
				}
			}
		}
		if len(indexInfo.Columns) == 1 && indexInfo.Columns[0].Name.L == colName && !isMultiSchemaChange {
			return dbterror.ErrCantDropColWithIndex.GenWithStack("can't drop column %s with tidb_enable_change_multi_schema is disable", colName)
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
		return dbterror.ErrTooManyFields
	}
	return nil
}

// rollbackModifyColumnJob rollbacks the job when an error occurs.
func rollbackModifyColumnJob(d *ddlCtx, t *meta.Meta, tblInfo *model.TableInfo, job *model.Job, newCol, oldCol *model.ColumnInfo, modifyColumnTp byte) (ver int64, _ error) {
	var err error
	if oldCol.ID == newCol.ID && modifyColumnTp == mysql.TypeNull {
		// field NotNullFlag flag reset.
		tblInfo.Columns[oldCol.Offset].SetFlag(oldCol.GetFlag() &^ mysql.NotNullFlag)
		// field PreventNullInsertFlag flag reset.
		tblInfo.Columns[oldCol.Offset].SetFlag(oldCol.GetFlag() &^ mysql.PreventNullInsertFlag)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
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
func modifyColsFromNull2NotNull(w *worker, dbInfo *model.DBInfo, tblInfo *model.TableInfo, cols []*model.ColumnInfo, newCol *model.ColumnInfo, isDataTruncated bool) error {
	// Get sessionctx from context resource pool.
	var sctx sessionctx.Context
	sctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(sctx)

	skipCheck := false
	failpoint.Inject("skipMockContextDoExec", func(val failpoint.Value) {
		if val.(bool) {
			skipCheck = true
		}
	})
	if !skipCheck {
		// If there is a null value inserted, it cannot be modified and needs to be rollback.
		err = checkForNullValue(w.ddlJobCtx, sctx, isDataTruncated, dbInfo.Name, tblInfo.Name, newCol, cols...)
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

func generateOriginDefaultValue(col *model.ColumnInfo, ctx sessionctx.Context) (interface{}, error) {
	var err error
	odValue := col.GetDefaultValue()
	if odValue == nil && mysql.HasNotNullFlag(col.GetFlag()) {
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
			t, _ = expression.GetStmtTimestamp(ctx)
		}
		if col.GetType() == mysql.TypeTimestamp {
			odValue = types.NewTime(types.FromGoTime(t.UTC()), col.GetType(), col.GetDecimal()).String()
		} else if col.GetType() == mysql.TypeDatetime {
			odValue = types.NewTime(types.FromGoTime(t), col.GetType(), col.GetDecimal()).String()
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

func getChangingIndexOriginName(changingIdx *model.IndexInfo) string {
	idxName := strings.TrimPrefix(changingIdx.Name.O, changingIndexPrefix)
	// Since the unique idxName may contain the suffix number (indexName_num), better trim the suffix.
	var pos int
	if pos = strings.LastIndex(idxName, "_"); pos == -1 {
		return idxName
	}
	return idxName[:pos]
}

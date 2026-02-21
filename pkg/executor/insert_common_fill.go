// Copyright 2018 PingCAP, Inc.
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

package executor

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

func insertRowsFromSelect(ctx context.Context, base insertCommon) error {
	// process `insert|replace into ... select ... from ...`
	e := base.insertCommon()
	selectExec := e.Children(0)
	fields := exec.RetTypes(selectExec)
	chk := exec.TryNewCacheChunk(selectExec)
	iter := chunk.NewIterator4Chunk(chk)
	rows := make([][]types.Datum, 0, chk.Capacity())

	sessVars := e.Ctx().GetSessionVars()
	batchSize := sessVars.DMLBatchSize
	batchInsert := sessVars.BatchInsert && !sessVars.InTxn() && vardef.EnableBatchDML.Load() && batchSize > 0
	memUsageOfRows := int64(0)
	memUsageOfExtraCols := int64(0)
	memTracker := e.memTracker
	extraColsInSel := make([][]types.Datum, 0, chk.Capacity())
	// In order to ensure the correctness of the `transaction write throughput` SLI statistics,
	// just ignore the transaction which contain `insert|replace into ... select ... from ...` statement.
	e.Ctx().GetTxnWriteThroughputSLI().SetInvalid()
	for {
		err := exec.Next(ctx, selectExec, chk)
		if err != nil {
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		chkMemUsage := chk.MemoryUsage()
		memTracker.Consume(chkMemUsage)
		var totalMemDelta int64
		for innerChunkRow := iter.Begin(); innerChunkRow != iter.End(); innerChunkRow = iter.Next() {
			innerRow := innerChunkRow.GetDatumRow(fields)
			e.rowCount++
			row, err := e.getRow(ctx, innerRow)
			if err != nil {
				return err
			}
			extraColsInSel = append(extraColsInSel, innerRow[e.rowLen:])
			rows = append(rows, row)
			if batchInsert && e.rowCount%uint64(batchSize) == 0 {
				memUsageOfRows = types.EstimatedMemUsage(rows[0], len(rows))
				memUsageOfExtraCols = types.EstimatedMemUsage(extraColsInSel[0], len(extraColsInSel))
				totalMemDelta += memUsageOfRows + memUsageOfExtraCols
				e.Ctx().GetSessionVars().CurrInsertBatchExtraCols = extraColsInSel
				if err = base.exec(ctx, rows); err != nil {
					return err
				}
				rows = rows[:0]
				extraColsInSel = extraColsInSel[:0]
				totalMemDelta += -memUsageOfRows - memUsageOfExtraCols
				memUsageOfRows = 0
				if err = e.doBatchInsert(ctx); err != nil {
					return err
				}
			}
		}
		memTracker.Consume(totalMemDelta)

		if len(rows) != 0 {
			memUsageOfRows = types.EstimatedMemUsage(rows[0], len(rows))
			memUsageOfExtraCols = types.EstimatedMemUsage(extraColsInSel[0], len(extraColsInSel))
			memTracker.Consume(memUsageOfRows + memUsageOfExtraCols)
			e.Ctx().GetSessionVars().CurrInsertBatchExtraCols = extraColsInSel
		}
		err = base.exec(ctx, rows)
		if err != nil {
			return err
		}
		rows = rows[:0]
		extraColsInSel = extraColsInSel[:0]
		memTracker.Consume(-memUsageOfRows - memUsageOfExtraCols - chkMemUsage)
	}
	return nil
}

func (e *InsertValues) doBatchInsert(ctx context.Context) error {
	e.Ctx().StmtCommit(ctx)
	if err := sessiontxn.NewTxnInStmt(ctx, e.Ctx()); err != nil {
		// We should return a special error for batch insert.
		return exeerrors.ErrBatchInsertFail.GenWithStack("BatchInsert failed with error: %v", err)
	}
	return nil
}

// getRow gets the row which from `insert into select from` or `load data`.
// The input values from these two statements are datums instead of
// expressions which are used in `insert into set x=y`.
func (e *InsertValues) getRow(ctx context.Context, vals []types.Datum) ([]types.Datum, error) {
	row := make([]types.Datum, len(e.Table.Cols()))
	hasValue := make([]bool, len(e.Table.Cols()))
	sc := e.Ctx().GetSessionVars().StmtCtx
	warnCnt := int(sc.WarningCount())

	inLoadData := e.Ctx().GetSessionVars().StmtCtx.InLoadDataStmt

	for i := range e.rowLen {
		col := e.insertColumns[i].ToInfo()
		casted, err := table.CastValue(e.Ctx(), vals[i], col, false, false)
		if newErr := e.handleErr(e.insertColumns[i], &vals[i], int(e.rowCount), err); newErr != nil {
			if inLoadData {
				return nil, newErr
			}
			return nil, err
		}

		offset := e.insertColumns[i].Offset
		row[offset] = casted
		hasValue[offset] = true

		if inLoadData {
			if newWarnings := sc.TruncateWarnings(warnCnt); len(newWarnings) > 0 {
				for k := range newWarnings {
					newWarnings[k].Err = completeLoadErr(col, int(e.rowCount), newWarnings[k].Err)
				}
				sc.AppendWarnings(newWarnings)
				warnCnt += len(newWarnings)
			}
		}
	}

	return e.fillRow(ctx, row, hasValue, 0)
}

// getColDefaultValue gets the column default value.
func (e *InsertValues) getColDefaultValue(idx int, col *table.Column) (d types.Datum, err error) {
	if !col.DefaultIsExpr && e.colDefaultVals != nil && e.colDefaultVals[idx].valid {
		return e.colDefaultVals[idx].val, nil
	}

	var defaultVal types.Datum
	if col.DefaultIsExpr && col.DefaultExpr != nil {
		defaultVal, err = table.EvalColDefaultExpr(e.Ctx().GetExprCtx(), col.ToInfo(), col.DefaultExpr)
	} else {
		if err := table.CheckNoDefaultValueForInsert(e.Ctx().GetSessionVars().StmtCtx, col.ToInfo()); err != nil {
			return types.Datum{}, err
		}
		defaultVal, err = table.GetColDefaultValue(e.Ctx().GetExprCtx(), col.ToInfo())
	}
	if err != nil {
		return types.Datum{}, err
	}
	if initialized := e.lazilyInitColDefaultValBuf(); initialized && !col.DefaultIsExpr {
		e.colDefaultVals[idx].val = defaultVal
		e.colDefaultVals[idx].valid = true
	}

	return defaultVal, nil
}

// fillColValue fills the column value if it is not set in the insert statement.
func (e *InsertValues) fillColValue(
	ctx context.Context,
	datum types.Datum,
	idx int,
	column *table.Column,
	hasValue bool,
) (types.Datum, error) {
	if mysql.HasAutoIncrementFlag(column.GetFlag()) {
		if !hasValue && mysql.HasNoDefaultValueFlag(column.ToInfo().GetFlag()) {
			vars := e.Ctx().GetSessionVars()
			sc := vars.StmtCtx
			if vars.SQLMode.HasStrictMode() {
				return datum, table.ErrNoDefaultValue.FastGenByArgs(column.ToInfo().Name)
			}
			sc.AppendWarning(table.ErrNoDefaultValue.FastGenByArgs(column.ToInfo().Name))
		}

		if e.lazyFillAutoID {
			// Handle hasValue info in autoIncrement column previously for lazy handle.
			if !hasValue {
				datum.SetNull()
			}
			// Store the plain datum of autoIncrement column directly for lazy handle.
			return datum, nil
		}
		d, err := e.adjustAutoIncrementDatum(ctx, datum, hasValue, column)
		if err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	tblInfo := e.Table.Meta()
	if ddl.IsAutoRandomColumnID(tblInfo, column.ID) {
		d, err := e.adjustAutoRandomDatum(ctx, datum, hasValue, column)
		if err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	if column.ID == model.ExtraHandleID && hasValue {
		d, err := e.adjustImplicitRowID(ctx, datum, hasValue, column)
		if err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	if !hasValue {
		d, err := e.getColDefaultValue(idx, column)
		if e.handleErr(column, &datum, 0, err) != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	return datum, nil
}

// fillRow fills generated columns, auto_increment column and empty column.
// For NOT NULL column, it will return error or use zero value based on sql_mode.
// When lazyFillAutoID is true, fill row will lazily handle auto increment datum for lazy batch allocation.
// `insert|replace values` can guarantee consecutive autoID in a batch.
// Other statements like `insert select from` don't guarantee consecutive autoID.
// https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html
func (e *InsertValues) fillRow(ctx context.Context, row []types.Datum, hasValue []bool, rowIdx int) (
	[]types.Datum, error,
) {
	gCols := make([]*table.Column, 0)
	tCols := e.Table.Cols()
	if e.hasExtraHandle {
		col := &table.Column{}
		col.ColumnInfo = model.NewExtraHandleColInfo()
		col.ColumnInfo.Offset = len(tCols)
		tCols = append(tCols, col)
	}
	rowCntInLoadData := uint64(0)
	if e.Ctx().GetSessionVars().StmtCtx.InLoadDataStmt {
		rowCntInLoadData = e.rowCount
	}

	for i, c := range tCols {
		var err error
		// Evaluate the generated columns later after real columns set
		if c.IsGenerated() {
			gCols = append(gCols, c)
		} else {
			// Get the default value for all no value columns, the auto increment column is different from the others.
			if row[i], err = e.fillColValue(ctx, row[i], i, c, hasValue[i]); err != nil {
				return nil, err
			}
			if !e.lazyFillAutoID || (e.lazyFillAutoID && !mysql.HasAutoIncrementFlag(c.GetFlag())) {
				if err = c.HandleBadNull(e.Ctx().GetSessionVars().StmtCtx.ErrCtx(), &row[i], rowCntInLoadData); err != nil {
					return nil, err
				}
			}
		}
	}

	// Handle exchange partition
	tbl := e.Table.Meta()
	if tbl.ExchangePartitionInfo != nil && tbl.GetPartitionInfo() == nil {
		if err := checkRowForExchangePartition(e.Ctx(), row, tbl); err != nil {
			return nil, err
		}
	}

	sctx := e.Ctx()
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	sc := sctx.GetSessionVars().StmtCtx
	warnCnt := int(sc.WarningCount())
	for i, gCol := range gCols {
		colIdx := gCol.ColumnInfo.Offset
		val, err := e.GenExprs[i].Eval(evalCtx, chunk.MutRowFromDatums(row).ToRow())
		if err != nil && gCol.FieldType.IsArray() {
			return nil, completeError(tbl, gCol.Offset, rowIdx, err)
		}
		if e.Ctx().GetSessionVars().StmtCtx.HandleTruncate(err) != nil {
			return nil, err
		}
		row[colIdx], err = table.CastValue(e.Ctx(), val, gCol.ToInfo(), false, false)
		if err = e.handleErr(gCol, &val, rowIdx, err); err != nil {
			return nil, err
		}
		if newWarnings := sc.TruncateWarnings(warnCnt); len(newWarnings) > 0 {
			for k := range newWarnings {
				newWarnings[k].Err = completeInsertErr(gCol.ColumnInfo, &val, rowIdx, newWarnings[k].Err)
			}
			sc.AppendWarnings(newWarnings)
			warnCnt += len(newWarnings)
		}
		// Handle the bad null error.
		if err = gCol.HandleBadNull(sc.ErrCtx(), &row[colIdx], rowCntInLoadData); err != nil {
			return nil, err
		}
	}
	return row, nil
}

func completeError(tbl *model.TableInfo, offset int, rowIdx int, err error) error {
	name := "expression_index"
	for _, idx := range tbl.Indices {
		for _, column := range idx.Columns {
			if column.Offset == offset {
				name = idx.Name.O
				break
			}
		}
	}

	if expression.ErrInvalidJSONForFuncIndex.Equal(err) {
		return expression.ErrInvalidJSONForFuncIndex.GenWithStackByArgs(name)
	}
	if types.ErrOverflow.Equal(err) {
		return expression.ErrDataOutOfRangeFuncIndex.GenWithStackByArgs(name, rowIdx+1)
	}
	if types.ErrDataTooLong.Equal(err) {
		return expression.ErrFuncIndexDataIsTooLong.GenWithStackByArgs(name)
	}
	return err
}

// isAutoNull can help judge whether a datum is AutoIncrement Null quickly.
// This used to help lazyFillAutoIncrement to find consecutive N datum backwards for batch autoID alloc.
func (e *InsertValues) isAutoNull(_ context.Context, d types.Datum, col *table.Column) bool {
	var err error
	var recordID int64
	if !d.IsNull() {
		recordID, err = getAutoRecordID(d, &col.FieldType, true)
		if err != nil {
			return false
		}
	}
	// Use the value if it's not null and not 0.
	if recordID != 0 {
		return false
	}
	// Change NULL to auto id.
	// Change value 0 to auto id, if NoAutoValueOnZero SQL mode is not set.
	if d.IsNull() || e.Ctx().GetSessionVars().SQLMode&mysql.ModeNoAutoValueOnZero == 0 {
		return true
	}
	return false
}

func findAutoIncrementColumn(t table.Table) (col *table.Column, offsetInRow int, found bool) {
	for i, c := range t.Cols() {
		if mysql.HasAutoIncrementFlag(c.GetFlag()) {
			return c, i, true
		}
	}
	return nil, -1, false
}

func (e *InsertValues) handleWarning(err error) {
	sc := e.Ctx().GetSessionVars().StmtCtx
	sc.AppendWarning(err)
}

func (e *InsertValues) collectRuntimeStatsEnabled() bool {
	if e.RuntimeStats() != nil {
		if e.stats == nil {
			snapshotStats := &txnsnapshot.SnapshotRuntimeStats{}
			e.stats = &InsertRuntimeStat{
				BasicRuntimeStats:     e.RuntimeStats(),
				SnapshotRuntimeStats:  snapshotStats,
				AllocatorRuntimeStats: autoid.NewAllocatorRuntimeStats(),
			}
		}
		return true
	}
	return false
}

// CreateSession will be assigned by session package.
var CreateSession func(ctx sessionctx.Context) (sessionctx.Context, error)

// CloseSession will be assigned by session package.
var CloseSession func(ctx sessionctx.Context)


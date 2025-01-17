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
	"bytes"
	"context"
	"fmt"
	"runtime/trace"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	mmodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// UpdateExec represents a new update executor.
type UpdateExec struct {
	exec.BaseExecutor

	OrderedList         []*expression.Assignment
	assignmentsPerTable map[int][]*expression.Assignment

	// updatedRowKeys is a map for unique (TableAlias, handle) pair.
	// The value is true if the row is changed, or false otherwise
	updatedRowKeys map[int]*kv.MemAwareHandleMap[bool]
	tblID2table    map[int64]table.Table
	// mergedRowData is a map for unique (Table, handle) pair.
	// The value is cached table row
	mergedRowData          map[int64]*kv.MemAwareHandleMap[[]types.Datum]
	multiUpdateOnSameTable map[int64]bool

	matched uint64 // a counter of matched rows during update
	// tblColPosInfos stores relationship between column ordinal to its table handle.
	// the columns ordinals is present in ordinal range format, @see plannercore.TblColPosInfos
	tblColPosInfos            plannercore.TblColPosInfoSlice
	assignFlag                []int
	evalBuffer                chunk.MutRow
	allAssignmentsAreConstant bool
	virtualAssignmentsOffset  int
	drained                   bool
	memTracker                *memory.Tracker

	stats *updateRuntimeStats

	handles        []kv.Handle
	tableUpdatable []bool
	changed        []bool
	matches        []bool
	// fkChecks contains the foreign key checkers. the map is tableID -> []*FKCheckExec
	fkChecks map[int64][]*FKCheckExec
	// fkCascades contains the foreign key cascade. the map is tableID -> []*FKCascadeExec
	fkCascades map[int64][]*FKCascadeExec

	IgnoreError bool
}

// prepare `handles`, `tableUpdatable`, `changed` to avoid re-computations.
func (e *UpdateExec) prepare(row []types.Datum) (err error) {
	if e.updatedRowKeys == nil {
		e.updatedRowKeys = make(map[int]*kv.MemAwareHandleMap[bool])
	}
	e.handles = e.handles[:0]
	e.tableUpdatable = e.tableUpdatable[:0]
	e.changed = e.changed[:0]
	e.matches = e.matches[:0]
	for _, content := range e.tblColPosInfos {
		if e.updatedRowKeys[content.Start] == nil {
			e.updatedRowKeys[content.Start] = kv.NewMemAwareHandleMap[bool]()
		}
		handle, err := content.HandleCols.BuildHandleByDatums(row)
		if err != nil {
			return err
		}
		e.handles = append(e.handles, handle)

		updatable := false
		flags := e.assignFlag[content.Start:content.End]
		for _, flag := range flags {
			if flag >= 0 {
				updatable = true
				break
			}
		}
		if unmatchedOuterRow(content, row) {
			updatable = false
		}
		e.tableUpdatable = append(e.tableUpdatable, updatable)

		changed, ok := e.updatedRowKeys[content.Start].Get(handle)
		if ok {
			e.changed = append(e.changed, changed)
			e.matches = append(e.matches, false)
		} else {
			e.changed = append(e.changed, false)
			e.matches = append(e.matches, true)
		}
	}
	return nil
}

func (e *UpdateExec) mergeNonGenerated(row, newData []types.Datum) error {
	if e.mergedRowData == nil {
		e.mergedRowData = make(map[int64]*kv.MemAwareHandleMap[[]types.Datum])
	}
	var mergedData []types.Datum
	// merge updates from and into mergedRowData
	var totalMemDelta int64
	for i, content := range e.tblColPosInfos {
		if !e.multiUpdateOnSameTable[content.TblID] {
			// No need to merge if not multi-updated
			continue
		}
		if !e.tableUpdatable[i] {
			// If there's nothing to update, we can just skip current row
			continue
		}
		if e.changed[i] {
			// Each matched row is updated once, even if it matches the conditions multiple times.
			continue
		}
		handle := e.handles[i]
		flags := e.assignFlag[content.Start:content.End]

		if e.mergedRowData[content.TblID] == nil {
			e.mergedRowData[content.TblID] = kv.NewMemAwareHandleMap[[]types.Datum]()
		}
		tbl := e.tblID2table[content.TblID]
		oldData := row[content.Start:content.End]
		newTableData := newData[content.Start:content.End]
		if v, ok := e.mergedRowData[content.TblID].Get(handle); ok {
			mergedData = v
			for i, flag := range flags {
				if tbl.WritableCols()[i].IsGenerated() {
					continue
				}
				mergedData[i].Copy(&oldData[i])
				if flag >= 0 {
					newTableData[i].Copy(&mergedData[i])
				} else {
					mergedData[i].Copy(&newTableData[i])
				}
			}
		} else {
			mergedData = append([]types.Datum{}, newTableData...)
		}

		memDelta := e.mergedRowData[content.TblID].Set(handle, mergedData)
		memDelta += types.EstimatedMemUsage(mergedData, 1) + int64(handle.ExtraMemSize())
		totalMemDelta += memDelta
	}
	e.memTracker.Consume(totalMemDelta)
	return nil
}

func (e *UpdateExec) mergeGenerated(row, newData []types.Datum, i int, beforeEval bool) error {
	if e.virtualAssignmentsOffset >= len(e.OrderedList) {
		return nil
	}

	var mergedData []types.Datum
	// merge updates from and into mergedRowData
	var totalMemDelta int64

	content := e.tblColPosInfos[i]

	if !e.multiUpdateOnSameTable[content.TblID] {
		// No need to merge if not multi-updated
		return nil
	}
	if !e.tableUpdatable[i] {
		// If there's nothing to update, we can just skip current row
		return nil
	}
	if e.changed[i] {
		// Each matched row is updated once, even if it matches the conditions multiple times.
		return nil
	}
	handle := e.handles[i]
	flags := e.assignFlag[content.Start:content.End]

	if e.mergedRowData[content.TblID] == nil {
		e.mergedRowData[content.TblID] = kv.NewMemAwareHandleMap[[]types.Datum]()
	}
	tbl := e.tblID2table[content.TblID]
	oldData := row[content.Start:content.End]
	newTableData := newData[content.Start:content.End]

	// We don't check the second return value here because we have already called mergeNonGenerated() before.
	mergedData, _ = e.mergedRowData[content.TblID].Get(handle)
	for i, flag := range flags {
		if !tbl.WritableCols()[i].IsGenerated() {
			continue
		}
		// Before evaluate generated columns,
		// we need to copy new values to both oldData and newData.
		// After evaluation,
		/// we need to copy new generated values into mergedData.
		if beforeEval {
			mergedData[i].Copy(&oldData[i])
			if flag < 0 {
				mergedData[i].Copy(&newTableData[i])
			}
		} else {
			if flag >= 0 {
				newTableData[i].Copy(&mergedData[i])
			}
		}
	}

	memDelta := e.mergedRowData[content.TblID].Set(handle, mergedData)
	memDelta += types.EstimatedMemUsage(mergedData, 1) + int64(handle.ExtraMemSize())
	totalMemDelta += memDelta

	e.memTracker.Consume(totalMemDelta)
	return nil
}

func (e *UpdateExec) exec(
	ctx context.Context,
	_ *expression.Schema,
	rowIdx int, row, newData []types.Datum,
	dupKeyCheck table.DupKeyCheckMode,
) error {
	defer trace.StartRegion(ctx, "UpdateExec").End()
	bAssignFlag := make([]bool, len(e.assignFlag))
	for i, flag := range e.assignFlag {
		bAssignFlag[i] = flag >= 0
	}

	errorHandler := func(sctx sessionctx.Context, assign *expression.Assignment, _ *types.Datum, err error) error {
		return handleUpdateError(sctx, assign.ColName, assign.Col.ToInfo(), rowIdx, err)
	}

	var totalMemDelta int64
	defer func() { e.memTracker.Consume(totalMemDelta) }()

	for i, content := range e.tblColPosInfos {
		if !e.tableUpdatable[i] {
			// If there's nothing to update, we can just skip current row
			continue
		}
		if e.changed[i] {
			// Each matched row is updated once, even if it matches the conditions multiple times.
			continue
		}
		if e.matches[i] {
			// Row is matched for the first time, increment `matched` counter
			e.matched++
		}
		tbl := e.tblID2table[content.TblID]
		handle := e.handles[i]

		oldData := row[content.Start:content.End]
		newTableData := newData[content.Start:content.End]
		flags := bAssignFlag[content.Start:content.End]

		// Evaluate generated columns and write to table.
		// Evaluated values will be stored in newRow.
		var assignments []*expression.Assignment
		if a, ok := e.assignmentsPerTable[i]; ok {
			assignments = a
		}

		// Copy data from merge row to old and new rows
		if err := e.mergeGenerated(row, newData, i, true); err != nil {
			return errors.Trace(err)
		}

		// Update row
		changed, ignored, err := updateRecord(
			ctx, e.Ctx(),
			handle, oldData, newTableData,
			content.Start, assignments, e.evalBuffer, errorHandler,
			flags, tbl, false, e.memTracker,
			e.fkChecks[content.TblID],
			e.fkCascades[content.TblID],
			dupKeyCheck, e.IgnoreError)

		// Copy data from new row to merge row
		if err := e.mergeGenerated(row, newData, i, false); err != nil {
			return errors.Trace(err)
		}

		if ignored {
			continue
		}

		if err == nil {
			_, exist := e.updatedRowKeys[content.Start].Get(handle)
			memDelta := e.updatedRowKeys[content.Start].Set(handle, changed)
			if !exist {
				memDelta += int64(handle.ExtraMemSize())
			}
			totalMemDelta += memDelta
			continue
		}

		if kv.ErrKeyExists.Equal(err) || table.ErrCheckConstraintViolated.Equal(err) {
			ec := e.Ctx().GetSessionVars().StmtCtx.ErrCtx()
			if err = ec.HandleErrorWithAlias(kv.ErrKeyExists, err, err); err != nil {
				return err
			}
			continue
		}
		return err
	}
	if txn, _ := e.Ctx().Txn(false); txn != nil {
		return txn.MayFlush()
	}
	return nil
}

// unmatchedOuterRow checks the tableCols of a record to decide whether that record
// can not be updated. The handle is NULL only when it is the inner side of an
// outer join: the outer row can not match any inner rows, and in this scenario
// the inner handle field is filled with a NULL value.
//
// This fixes: https://github.com/pingcap/tidb/issues/7176.
func unmatchedOuterRow(tblPos plannercore.TblColPosInfo, waitUpdateRow []types.Datum) bool {
	firstHandleIdx := tblPos.HandleCols.GetCol(0)
	return waitUpdateRow[firstHandleIdx.Index].IsNull()
}

// Next implements the Executor Next interface.
func (e *UpdateExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.drained {
		if e.collectRuntimeStatsEnabled() {
			ctx = context.WithValue(ctx, autoid.AllocatorRuntimeStatsCtxKey, e.stats.AllocatorRuntimeStats)
		}
		numRows, err := e.updateRows(ctx)
		if err != nil {
			return err
		}
		e.drained = true
		e.Ctx().GetSessionVars().StmtCtx.AddRecordRows(uint64(numRows))
	}
	return nil
}

func (e *UpdateExec) updateRows(ctx context.Context) (int, error) {
	fields := exec.RetTypes(e.Children(0))
	colsInfo := plannercore.GetUpdateColumnsInfo(e.tblID2table, e.tblColPosInfos, len(fields))
	globalRowIdx := 0
	chk := exec.TryNewCacheChunk(e.Children(0))
	if !e.allAssignmentsAreConstant {
		e.evalBuffer = chunk.MutRowFromTypes(fields)
	}
	composeFunc := e.fastComposeNewRow
	if !e.allAssignmentsAreConstant {
		composeFunc = e.composeNewRow
	}
	memUsageOfChk := int64(0)
	totalNumRows := 0

	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return 0, err
	}

	if e.virtualAssignmentsOffset < len(e.OrderedList) {
		e.assignmentsPerTable = make(map[int][]*expression.Assignment, 0)
		for _, assign := range e.OrderedList[e.virtualAssignmentsOffset:] {
			tblIdx := e.assignFlag[assign.Col.Index]
			if tblIdx < 0 {
				continue
			}
			if _, ok := e.assignmentsPerTable[tblIdx]; !ok {
				e.assignmentsPerTable[tblIdx] = make([]*expression.Assignment, 0)
			}
			e.assignmentsPerTable[tblIdx] = append(e.assignmentsPerTable[tblIdx], assign)
		}
	}

	dupKeyCheck := optimizeDupKeyCheckForUpdate(txn, e.IgnoreError)
	for {
		e.memTracker.Consume(-memUsageOfChk)
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			return 0, err
		}

		if chk.NumRows() == 0 {
			break
		}
		memUsageOfChk = chk.MemoryUsage()
		e.memTracker.Consume(memUsageOfChk)
		if e.collectRuntimeStatsEnabled() {
			if snap := txn.GetSnapshot(); snap != nil {
				snap.SetOption(kv.CollectRuntimeStats, e.stats.SnapshotRuntimeStats)
			}
		}
		// pipelined dml may already flush in background, don't touch it to avoid race.
		if !txn.IsPipelined() {
			sc := e.Ctx().GetSessionVars().StmtCtx
			txn.SetOption(kv.ResourceGroupTagger, sc.GetResourceGroupTagger())
			if sc.KvExecCounter != nil {
				// Bind an interceptor for client-go to count the number of SQL executions of each TiKV.
				txn.SetOption(kv.RPCInterceptor, sc.KvExecCounter.RPCInterceptor())
			}
		}
		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			chunkRow := chk.GetRow(rowIdx)
			datumRow := chunkRow.GetDatumRow(fields)
			// precomputes handles
			if err := e.prepare(datumRow); err != nil {
				return 0, err
			}
			// compose non-generated columns
			newRow, err := composeFunc(globalRowIdx, datumRow, colsInfo)
			if err != nil {
				return 0, err
			}
			// merge non-generated columns
			if err := e.mergeNonGenerated(datumRow, newRow); err != nil {
				return 0, err
			}

			if e.virtualAssignmentsOffset < len(e.OrderedList) {
				e.evalBuffer.SetDatums(newRow...)
			}

			if err := e.exec(
				ctx, e.Children(0).Schema(),
				globalRowIdx, datumRow, newRow, dupKeyCheck); err != nil {
				return 0, err
			}
			globalRowIdx++
		}
		totalNumRows += chk.NumRows()
		chk = chunk.Renew(chk, e.MaxChunkSize())
	}
	return totalNumRows, nil
}

func handleUpdateError(sctx sessionctx.Context, colName ast.CIStr, colInfo *mmodel.ColumnInfo, rowIdx int, err error) error {
	if err == nil {
		return nil
	}

	if types.ErrDataTooLong.Equal(err) {
		return errors.AddStack(resetErrDataTooLong(colName.O, rowIdx+1, err))
	}

	if types.ErrOverflow.Equal(err) {
		return types.ErrWarnDataOutOfRange.GenWithStackByArgs(colName.O, rowIdx+1)
	}

	if types.ErrTruncatedWrongVal.Equal(err) && colInfo != nil && colInfo.GetType() == mysql.TypeTimestamp {
		ec := sctx.GetSessionVars().StmtCtx.ErrCtx()
		return errors.AddStack(ec.HandleErrorWithAlias(kv.ErrKeyExists, err, err))
	}
	return err
}

func (e *UpdateExec) fastComposeNewRow(rowIdx int, oldRow []types.Datum, cols []*table.Column) ([]types.Datum, error) {
	newRowData := types.CloneRow(oldRow)
	for _, assign := range e.OrderedList {
		var colInfo *mmodel.ColumnInfo
		if cols[assign.Col.Index] != nil {
			colInfo = cols[assign.Col.Index].ColumnInfo
		}

		tblIdx := e.assignFlag[assign.Col.Index]
		if tblIdx >= 0 && !e.tableUpdatable[tblIdx] {
			continue
		}
		con := assign.Expr.(*expression.Constant)
		val, err := con.Eval(e.Ctx().GetExprCtx().GetEvalCtx(), emptyRow)
		if err = handleUpdateError(e.Ctx(), assign.ColName, colInfo, rowIdx, err); err != nil {
			return nil, err
		}

		// info of `_tidb_rowid` column is nil.
		// No need to cast `_tidb_rowid` column value.
		if cols[assign.Col.Index] != nil {
			val, err = table.CastValue(e.Ctx(), val, cols[assign.Col.Index].ColumnInfo, false, false)
			if err = handleUpdateError(e.Ctx(), assign.ColName, colInfo, rowIdx, err); err != nil {
				return nil, err
			}
		}

		val.Copy(&newRowData[assign.Col.Index])
	}
	return newRowData, nil
}

func (e *UpdateExec) composeNewRow(rowIdx int, oldRow []types.Datum, cols []*table.Column) ([]types.Datum, error) {
	newRowData := types.CloneRow(oldRow)
	e.evalBuffer.SetDatums(newRowData...)
	for _, assign := range e.OrderedList[:e.virtualAssignmentsOffset] {
		tblIdx := e.assignFlag[assign.Col.Index]
		if tblIdx >= 0 && !e.tableUpdatable[tblIdx] {
			continue
		}
		val, err := assign.Expr.Eval(e.Ctx().GetExprCtx().GetEvalCtx(), e.evalBuffer.ToRow())
		if err != nil {
			return nil, err
		}

		// info of `_tidb_rowid` column is nil.
		// No need to cast `_tidb_rowid` column value.
		if cols[assign.Col.Index] != nil {
			colInfo := cols[assign.Col.Index].ColumnInfo
			val, err = table.CastValue(e.Ctx(), val, colInfo, false, false)
			if err = handleUpdateError(e.Ctx(), assign.ColName, colInfo, rowIdx, err); err != nil {
				return nil, err
			}
		}

		val.Copy(&newRowData[assign.Col.Index])
	}
	return newRowData, nil
}

// Close implements the Executor Close interface.
func (e *UpdateExec) Close() error {
	defer e.memTracker.ReplaceBytesUsed(0)
	e.setMessage()
	if e.RuntimeStats() != nil && e.stats != nil {
		txn, err := e.Ctx().Txn(false)
		if err == nil && txn.Valid() && txn.GetSnapshot() != nil {
			txn.GetSnapshot().SetOption(kv.CollectRuntimeStats, nil)
		}
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	return exec.Close(e.Children(0))
}

// Open implements the Executor Open interface.
func (e *UpdateExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	return exec.Open(ctx, e.Children(0))
}

// setMessage sets info message(ERR_UPDATE_INFO) generated by UPDATE statement
func (e *UpdateExec) setMessage() {
	stmtCtx := e.Ctx().GetSessionVars().StmtCtx
	numMatched := e.matched
	numChanged := stmtCtx.UpdatedRows()
	numWarnings := stmtCtx.WarningCount()
	msg := fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUpdateInfo].Raw, numMatched, numChanged, numWarnings)
	stmtCtx.SetMessage(msg)
}

func (e *UpdateExec) collectRuntimeStatsEnabled() bool {
	if e.RuntimeStats() != nil {
		if e.stats == nil {
			e.stats = &updateRuntimeStats{
				SnapshotRuntimeStats:  &txnsnapshot.SnapshotRuntimeStats{},
				AllocatorRuntimeStats: autoid.NewAllocatorRuntimeStats(),
			}
		}
		return true
	}
	return false
}

// updateRuntimeStats is the execution stats about update statements.
type updateRuntimeStats struct {
	*txnsnapshot.SnapshotRuntimeStats
	*autoid.AllocatorRuntimeStats
}

func (e *updateRuntimeStats) String() string {
	if e.SnapshotRuntimeStats == nil && e.AllocatorRuntimeStats == nil {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	if e.SnapshotRuntimeStats != nil {
		stats := e.SnapshotRuntimeStats.String()
		if stats != "" {
			buf.WriteString(stats)
		}
	}
	if e.AllocatorRuntimeStats != nil {
		stats := e.AllocatorRuntimeStats.String()
		if stats != "" {
			if buf.Len() > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(stats)
		}
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *updateRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := &updateRuntimeStats{}
	if e.SnapshotRuntimeStats != nil {
		snapshotStats := e.SnapshotRuntimeStats.Clone()
		newRs.SnapshotRuntimeStats = snapshotStats
	}
	if e.AllocatorRuntimeStats != nil {
		newRs.AllocatorRuntimeStats = e.AllocatorRuntimeStats.Clone()
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *updateRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*updateRuntimeStats)
	if !ok {
		return
	}
	if tmp.SnapshotRuntimeStats != nil {
		if e.SnapshotRuntimeStats == nil {
			snapshotStats := tmp.SnapshotRuntimeStats.Clone()
			e.SnapshotRuntimeStats = snapshotStats
		} else {
			e.SnapshotRuntimeStats.Merge(tmp.SnapshotRuntimeStats)
		}
	}
	if tmp.AllocatorRuntimeStats != nil {
		if e.AllocatorRuntimeStats == nil {
			e.AllocatorRuntimeStats = tmp.AllocatorRuntimeStats.Clone()
		}
	}
}

// Tp implements the RuntimeStats interface.
func (*updateRuntimeStats) Tp() int {
	return execdetails.TpUpdateRuntimeStats
}

// GetFKChecks implements WithForeignKeyTrigger interface.
func (e *UpdateExec) GetFKChecks() []*FKCheckExec {
	fkChecks := make([]*FKCheckExec, 0, len(e.fkChecks))
	for _, fkc := range e.fkChecks {
		fkChecks = append(fkChecks, fkc...)
	}
	return fkChecks
}

// GetFKCascades implements WithForeignKeyTrigger interface.
func (e *UpdateExec) GetFKCascades() []*FKCascadeExec {
	fkCascades := make([]*FKCascadeExec, 0, len(e.fkChecks))
	for _, fkc := range e.fkCascades {
		fkCascades = append(fkCascades, fkc...)
	}
	return fkCascades
}

// HasFKCascades implements WithForeignKeyTrigger interface.
func (e *UpdateExec) HasFKCascades() bool {
	return len(e.fkCascades) > 0
}

// optimizeDupKeyCheckForUpdate trys to optimize the DupKeyCheckMode for an update statement.
// If the DupKeyCheckMode of the current statement can be optimized, it will return `DupKeyCheckLazy` to avoid the
// redundant requests to TiKV, otherwise, `DupKeyCheckInPlace` will be returned.
// The second argument `ignoreNeedsCheckInPlace` is true if `IGNORE` keyword is used in the update statement.
func optimizeDupKeyCheckForUpdate(txn kv.Transaction, ignoreNeedsCheckInPlace bool) table.DupKeyCheckMode {
	if txn.IsPipelined() {
		// It means `@@tidb_dml_type='bulk'` which indicates to insert rows in "bulk" mode.
		// At this time, `DupKeyCheckLazy` should be used to improve the performance.
		// If "bulk" mode and IGNORE keyword are used together, "bulk" is prior, see:
		// https://github.com/pingcap/tidb/issues/55187#issuecomment-2268356459
		return table.DupKeyCheckLazy
	}

	if ignoreNeedsCheckInPlace {
		// For `UPDATE IGNORE ...` and `INSERT IGNORE ... ON DUPLICATE KEY UPDATE ...` statements,
		// `DupKeyCheckInPlace` should be used to make sure the executor can get the error
		// immediately and ignore it then.
		return table.DupKeyCheckInPlace
	}

	if txn.IsPessimistic() {
		// We can just check duplicated key lazily without keys in storage for the below cases:
		// - `txn.Pipelined()` is true.
		//    It means the user is using `@@tidb_dml_type="bulk"` to insert rows in bulk mode.
		//    DupKeyCheckLazy should be used to improve the performance.
		// - The current transaction is pessimistic.
		//   The duplicate key check can be postponed to the lock stage.
		// Please notice that for optimistic transaction, it always returns `DupKeyCheckInPlace` even if
		// `tidb_constraint_check_in_place` is `OFF`.
		// That is because `tidb_constraint_check_in_place` is only designed for insert cases, see comments in issue:
		// https://github.com/pingcap/tidb/issues/54492#issuecomment-2229941881
		return table.DupKeyCheckLazy
	}

	return table.DupKeyCheckInPlace
}

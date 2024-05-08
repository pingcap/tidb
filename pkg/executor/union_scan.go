// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"runtime/trace"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// UnionScanExec merges the rows from dirty table and the rows from distsql request.
type UnionScanExec struct {
	exec.BaseExecutor

	memBuf     kv.MemBuffer
	memBufSnap kv.Getter

	conditions           []expression.Expression
	conditionsWithVirCol []expression.Expression
	columns              []*model.ColumnInfo
	table                table.Table

	addedRowsIter       memRowsIter
	cursor4AddRows      []types.Datum
	snapshotRows        [][]types.Datum
	cursor4SnapshotRows int
	snapshotChunkBuffer *chunk.Chunk
	mutableRow          chunk.MutRow
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int

	// cacheTable not nil means it's reading from cached table.
	cacheTable kv.MemBuffer

	// If partitioned table and the physical table id is encoded in the chuck at this column index
	// used with dynamic prune mode
	// < 0 if not used.
	physTblIDIdx int

	// partitionIDMap are only required by union scan with global index.
	partitionIDMap map[int64]struct{}

	keepOrder bool
	compareExec
}

// Open implements the Executor Open interface.
func (us *UnionScanExec) Open(ctx context.Context) error {
	r, ctx := tracing.StartRegionEx(ctx, "UnionScanExec.Open")
	defer r.End()

	if err := us.BaseExecutor.Open(ctx); err != nil {
		return err
	}
	return us.open(ctx)
}

func (us *UnionScanExec) open(ctx context.Context) error {
	var err error
	reader := us.Children(0)

	// If the push-downed condition contains virtual column, we may build a selection upon reader. Since unionScanExec
	// has already contained condition, we can ignore the selection.
	if sel, ok := reader.(*SelectionExec); ok {
		reader = sel.Children(0)
	}

	defer trace.StartRegion(ctx, "UnionScanBuildRows").End()
	txn, err := us.Ctx().Txn(false)
	if err != nil {
		return err
	}

	us.physTblIDIdx = -1
	for i := len(us.columns) - 1; i >= 0; i-- {
		if us.columns[i].ID == model.ExtraPhysTblID {
			us.physTblIDIdx = i
			break
		}
	}
	mb := txn.GetMemBuffer()
	mb.RLock()
	defer mb.RUnlock()

	us.memBuf = mb
	us.memBufSnap = mb.SnapshotGetter()

	// 1. select without virtual columns
	// 2. build virtual columns and select with virtual columns
	switch x := reader.(type) {
	case *TableReaderExecutor:
		us.addedRowsIter, err = buildMemTableReader(ctx, us, x.kvRanges).getMemRowsIter(ctx)
	case *IndexReaderExecutor:
		us.addedRowsIter, err = buildMemIndexReader(ctx, us, x).getMemRowsIter(ctx)
	case *IndexLookUpExecutor:
		us.addedRowsIter, err = buildMemIndexLookUpReader(ctx, us, x).getMemRowsIter(ctx)
	case *IndexMergeReaderExecutor:
		us.addedRowsIter, err = buildMemIndexMergeReader(ctx, us, x).getMemRowsIter(ctx)
	case *MPPGather:
		us.addedRowsIter, err = buildMemTableReader(ctx, us, x.kvRanges).getMemRowsIter(ctx)
	default:
		err = fmt.Errorf("unexpected union scan children:%T", reader)
	}
	if err != nil {
		return err
	}
	us.snapshotChunkBuffer = exec.TryNewCacheChunk(us)
	return nil
}

// Next implements the Executor Next interface.
func (us *UnionScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	us.memBuf.RLock()
	defer us.memBuf.RUnlock()

	// Assume req.Capacity() > 0 after GrowAndReset(), if this assumption fail,
	// the for-loop may exit without read one single row!
	req.GrowAndReset(us.MaxChunkSize())

	mutableRow := chunk.MutRowFromTypes(exec.RetTypes(us))
	for batchSize := req.Capacity(); req.NumRows() < batchSize; {
		row, err := us.getOneRow(ctx)
		if err != nil {
			return err
		}
		// no more data.
		if row == nil {
			return nil
		}
		mutableRow.SetDatums(row...)

		sctx := us.Ctx()
		for _, idx := range us.virtualColumnIndex {
			datum, err := us.Schema().Columns[idx].EvalVirtualColumn(sctx.GetExprCtx().GetEvalCtx(), mutableRow.ToRow())
			if err != nil {
				return err
			}
			// Because the expression might return different type from
			// the generated column, we should wrap a CAST on the result.
			castDatum, err := table.CastValue(us.Ctx(), datum, us.columns[idx], false, true)
			if err != nil {
				return err
			}
			// Handle the bad null error.
			if (mysql.HasNotNullFlag(us.columns[idx].GetFlag()) || mysql.HasPreventNullInsertFlag(us.columns[idx].GetFlag())) && castDatum.IsNull() {
				castDatum = table.GetZeroValue(us.columns[idx])
			}
			mutableRow.SetDatum(idx, castDatum)
		}

		matched, _, err := expression.EvalBool(us.Ctx().GetExprCtx().GetEvalCtx(), us.conditionsWithVirCol, mutableRow.ToRow())
		if err != nil {
			return err
		}
		if matched {
			req.AppendRow(mutableRow.ToRow())
		}
	}
	return nil
}

// Close implements the Executor Close interface.
func (us *UnionScanExec) Close() error {
	us.cursor4AddRows = nil
	us.cursor4SnapshotRows = 0
	us.snapshotRows = us.snapshotRows[:0]
	return exec.Close(us.Children(0))
}

// getOneRow gets one result row from dirty table or child.
func (us *UnionScanExec) getOneRow(ctx context.Context) ([]types.Datum, error) {
	snapshotRow, err := us.getSnapshotRow(ctx)
	if err != nil {
		return nil, err
	}
	addedRow, err := us.getAddedRow()
	if err != nil {
		return nil, err
	}

	var row []types.Datum
	var isSnapshotRow bool
	if addedRow == nil {
		row = snapshotRow
		isSnapshotRow = true
	} else if snapshotRow == nil {
		row = addedRow
	} else {
		isSnapshotRowInt, err := us.compare(us.Ctx().GetSessionVars().StmtCtx, snapshotRow, addedRow)
		if err != nil {
			return nil, err
		}
		isSnapshotRow = isSnapshotRowInt < 0
		if isSnapshotRow {
			row = snapshotRow
		} else {
			row = addedRow
		}
	}
	if row == nil {
		return nil, nil
	}

	if isSnapshotRow {
		us.cursor4SnapshotRows++
	} else {
		us.cursor4AddRows = nil
	}
	return row, nil
}

func (us *UnionScanExec) getSnapshotRow(ctx context.Context) ([]types.Datum, error) {
	if us.cacheTable != nil {
		// From cache table, so the snapshot is nil
		return nil, nil
	}
	if us.cursor4SnapshotRows < len(us.snapshotRows) {
		return us.snapshotRows[us.cursor4SnapshotRows], nil
	}
	var err error
	us.cursor4SnapshotRows = 0
	us.snapshotRows = us.snapshotRows[:0]
	for len(us.snapshotRows) == 0 {
		err = exec.Next(ctx, us.Children(0), us.snapshotChunkBuffer)
		if err != nil || us.snapshotChunkBuffer.NumRows() == 0 {
			return nil, err
		}
		iter := chunk.NewIterator4Chunk(us.snapshotChunkBuffer)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			var snapshotHandle kv.Handle
			snapshotHandle, err = us.handleCols.BuildHandle(row)
			if err != nil {
				return nil, err
			}
			var checkKey kv.Key
			if us.physTblIDIdx >= 0 {
				tblID := row.GetInt64(us.physTblIDIdx)
				checkKey = tablecodec.EncodeRowKeyWithHandle(tblID, snapshotHandle)
			} else {
				checkKey = tablecodec.EncodeRecordKey(us.table.RecordPrefix(), snapshotHandle)
			}
			if _, err := us.memBufSnap.Get(context.TODO(), checkKey); err == nil {
				// If src handle appears in added rows, it means there is conflict and the transaction will fail to
				// commit, but for simplicity, we don't handle it here.
				continue
			}
			us.snapshotRows = append(us.snapshotRows, row.GetDatumRow(exec.RetTypes(us.Children(0))))
		}
	}
	return us.snapshotRows[0], nil
}

func (us *UnionScanExec) getAddedRow() ([]types.Datum, error) {
	if us.cursor4AddRows == nil {
		var err error
		us.cursor4AddRows, err = us.addedRowsIter.Next()
		if err != nil {
			return nil, err
		}
	}
	return us.cursor4AddRows, nil
}

type compareExec struct {
	collators []collate.Collator
	// usedIndex is the column offsets of the index which Src executor has used.
	usedIndex []int
	desc      bool
	// handleCols is the handle's position of the below scan plan.
	handleCols plannerutil.HandleCols
}

func (ce compareExec) compare(sctx *stmtctx.StatementContext, a, b []types.Datum) (ret int, err error) {
	var cmp int
	for _, colOff := range ce.usedIndex {
		aColumn := a[colOff]
		bColumn := b[colOff]
		cmp, err = aColumn.Compare(sctx.TypeCtx(), &bColumn, ce.collators[colOff])
		if err != nil {
			return 0, err
		}
		if cmp == 0 {
			continue
		}
		if ce.desc {
			return -cmp, nil
		}
		return cmp, nil
	}
	cmp, err = ce.handleCols.Compare(a, b, ce.collators)
	if ce.desc {
		return -cmp, err
	}
	return cmp, err
}

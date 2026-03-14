// Copyright 2025 PingCAP, Inc.
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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

// IndexOnlyJoinExec combines two indexes: a driver index for ordering
// and a probe index for filtering, then fetches qualifying rows from the table.
type IndexOnlyJoinExec struct {
	exec.BaseExecutor

	table      table.Table
	tableInfo  *model.TableInfo
	probeIndex *model.IndexInfo
	handleCols plannerutil.HandleCols

	// Plans for constructing executors.
	driverPlans  []base.PhysicalPlan
	driverReader *physicalop.PhysicalIndexReader
	probePlans   []base.PhysicalPlan
	probeReader  *physicalop.PhysicalIndexReader
	tblPlans     []base.PhysicalPlan

	// Probe range template and mapping.
	probeRanges   []*ranger.Range
	keyOff2IdxOff []int

	keepOrder bool

	// Builder for constructing probe/table executors per batch.
	*dataReaderBuilder

	// Runtime state.
	driverExec    exec.Executor
	driverStarted bool
	finished      chan struct{}
	resultCh      chan *indexOnlyJoinResult
	workerWg      sync.WaitGroup

	memTracker *memory.Tracker
	startTS    uint64
}

// indexOnlyJoinResult holds a batch of result rows.
type indexOnlyJoinResult struct {
	chk *chunk.Chunk
	err error
}

const indexOnlyJoinBatchSize = 1024

// Open implements the Executor Open interface.
func (e *IndexOnlyJoinExec) Open(_ context.Context) error {
	e.finished = make(chan struct{})
	e.resultCh = make(chan *indexOnlyJoinResult, 2)
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	return nil
}

// Next implements the Executor Next interface.
func (e *IndexOnlyJoinExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.driverStarted {
		e.driverStarted = true
		e.workerWg.Add(1)
		go e.runWorker(ctx)
	}

	result, ok := <-e.resultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		return result.err
	}
	req.SwapColumns(result.chk)
	return nil
}

// Close implements the Executor Close interface.
func (e *IndexOnlyJoinExec) Close() error {
	if e.finished != nil {
		close(e.finished)
	}
	e.workerWg.Wait()
	if e.driverExec != nil {
		terror.Log(exec.Close(e.driverExec))
	}
	return nil
}

// runWorker drives the entire pipeline: read from driver, probe, fetch from table.
func (e *IndexOnlyJoinExec) runWorker(ctx context.Context) {
	defer e.workerWg.Done()
	defer close(e.resultCh)

	// Build and open driver executor.
	var err error
	e.driverExec, err = e.buildDriverExec(ctx)
	if err != nil {
		e.sendResult(ctx, &indexOnlyJoinResult{err: err})
		return
	}

	for {
		// Check if finished.
		select {
		case <-e.finished:
			return
		case <-ctx.Done():
			return
		default:
		}

		// Read a batch of handles from the driver.
		handles, err := e.readDriverBatch(ctx)
		if err != nil {
			e.sendResult(ctx, &indexOnlyJoinResult{err: err})
			return
		}
		if len(handles) == 0 {
			return // driver exhausted
		}

		// Probe: check which handles exist in the probe index.
		qualifiedHandles, err := e.probeHandles(ctx, handles)
		if err != nil {
			e.sendResult(ctx, &indexOnlyJoinResult{err: err})
			return
		}
		if len(qualifiedHandles) == 0 {
			continue
		}

		// Table fetch: read full rows for qualified handles.
		chk, err := e.fetchTableRows(ctx, qualifiedHandles)
		if err != nil {
			e.sendResult(ctx, &indexOnlyJoinResult{err: err})
			return
		}
		if chk.NumRows() == 0 {
			continue
		}

		if !e.sendResult(ctx, &indexOnlyJoinResult{chk: chk}) {
			return
		}
	}
}

func (e *IndexOnlyJoinExec) sendResult(ctx context.Context, result *indexOnlyJoinResult) bool {
	select {
	case e.resultCh <- result:
		return true
	case <-e.finished:
		return false
	case <-ctx.Done():
		return false
	}
}

func (e *IndexOnlyJoinExec) buildDriverExec(ctx context.Context) (exec.Executor, error) {
	driverIS := e.driverPlans[0].(*physicalop.PhysicalIndexScan)
	readerExec, err := buildNoRangeIndexReader(e.dataReaderBuilder.executorBuilder, e.driverReader)
	if err != nil {
		return nil, err
	}
	readerExec.ranges = driverIS.Ranges
	kvRanges, err := readerExec.buildKVRangesForIndexReader()
	if err != nil {
		return nil, err
	}
	if err = readerExec.open(ctx, kvRanges); err != nil {
		return nil, err
	}
	return readerExec, nil
}

// readDriverBatch reads a batch of handles from the driver index scan.
func (e *IndexOnlyJoinExec) readDriverBatch(ctx context.Context) ([]kv.Handle, error) {
	handles := make([]kv.Handle, 0, indexOnlyJoinBatchSize)
	chk := exec.TryNewCacheChunk(e.driverExec)

	for len(handles) < indexOnlyJoinBatchSize {
		err := exec.Next(ctx, e.driverExec, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			break
		}

		sc := e.Ctx().GetSessionVars().StmtCtx
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle, err := e.handleCols.BuildHandleFromIndexRow(sc, row)
			if err != nil {
				return nil, err
			}
			handles = append(handles, handle)
		}
		chk.Reset()
	}
	return handles, nil
}

// probeHandles checks which handles exist in the probe index.
func (e *IndexOnlyJoinExec) probeHandles(ctx context.Context, handles []kv.Handle) ([]kv.Handle, error) {
	// Build IndexJoinLookUpContent for each handle.
	lookUpContents := make([]*join.IndexJoinLookUpContent, 0, len(handles))
	for _, h := range handles {
		var keys []types.Datum
		if h.IsInt() {
			keys = []types.Datum{types.NewIntDatum(h.IntValue())}
		} else {
			// For common handle, decode encoded column bytes to datums.
			numCols := h.NumCols()
			keys = make([]types.Datum, numCols)
			for i := range numCols {
				_, d, err := codec.DecodeOne(h.EncodedCol(i))
				if err != nil {
					return nil, err
				}
				keys[i] = d
			}
		}
		lookUpContents = append(lookUpContents, &join.IndexJoinLookUpContent{
			Keys: keys,
		})
	}

	// Build KV ranges for probe.
	dctx := e.Ctx().GetDistSQLCtx()
	rctx := e.Ctx().GetRangerCtx()
	interruptSignal := &atomic.Value{}
	interruptSignal.Store(false)
	kvRanges, err := buildKvRangesForIndexJoin(
		dctx, rctx,
		e.tableInfo.ID,
		e.probeIndex.ID,
		lookUpContents,
		e.probeRanges,
		e.keyOff2IdxOff,
		nil,
		e.memTracker,
		interruptSignal,
	)
	if err != nil {
		return nil, err
	}

	if len(kvRanges) == 0 {
		return nil, nil
	}

	// Execute probe index read.
	probeExec, err := e.buildProbeExec(ctx, kvRanges)
	if err != nil {
		return nil, err
	}
	defer func() { terror.Log(exec.Close(probeExec)) }()

	// Build a set of all driver handles for lookup.
	handleSet := kv.NewHandleMap()
	for _, h := range handles {
		handleSet.Set(h, true)
	}

	// Collect qualifying handles from probe result.
	qualifiedHandles := make([]kv.Handle, 0, len(handles))
	probeChk := exec.TryNewCacheChunk(probeExec)
	sc := e.Ctx().GetSessionVars().StmtCtx
	for {
		err := exec.Next(ctx, probeExec, probeChk)
		if err != nil {
			return nil, err
		}
		if probeChk.NumRows() == 0 {
			break
		}
		// The probe index contains the handle as part of the index key.
		// Extract handles from the probe result rows (handle is the last column).
		iter := chunk.NewIterator4Chunk(probeChk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			h, err := e.handleCols.BuildHandleFromIndexRow(sc, row)
			if err != nil {
				logutil.BgLogger().Warn("index_only_join: failed to build handle from probe row", zap.Error(err))
				continue
			}
			if _, ok := handleSet.Get(h); ok {
				qualifiedHandles = append(qualifiedHandles, h)
			}
		}
		probeChk.Reset()
	}

	return qualifiedHandles, nil
}

func (e *IndexOnlyJoinExec) buildProbeExec(ctx context.Context, kvRanges []kv.KeyRange) (exec.Executor, error) {
	readerExec, err := buildNoRangeIndexReader(e.dataReaderBuilder.executorBuilder, e.probeReader)
	if err != nil {
		return nil, err
	}
	err = readerExec.open(ctx, kvRanges)
	if err != nil {
		return nil, err
	}
	return readerExec, nil
}

// fetchTableRows builds a table reader from handles and reads full rows.
func (e *IndexOnlyJoinExec) fetchTableRows(ctx context.Context, handles []kv.Handle) (*chunk.Chunk, error) {
	tableReader, err := e.buildTableReaderForIOJ(ctx, handles)
	if err != nil {
		return nil, err
	}
	defer func() { terror.Log(exec.Close(tableReader)) }()

	maxChunkSize := e.Ctx().GetSessionVars().MaxChunkSize
	resultChk := chunk.New(exec.RetTypes(e), maxChunkSize, maxChunkSize)
	for {
		chk := exec.TryNewCacheChunk(tableReader)
		err := exec.Next(ctx, tableReader, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			break
		}
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			resultChk.AppendRow(row)
		}
	}
	return resultChk, nil
}

func (e *IndexOnlyJoinExec) buildTableReaderForIOJ(ctx context.Context, handles []kv.Handle) (exec.Executor, error) {
	dagReq, err := builder.ConstructDAGReq(e.Ctx(), e.tblPlans, kv.TiKV)
	if err != nil {
		return nil, err
	}
	tableReaderExec := &TableReaderExecutor{
		BaseExecutorV2:             exec.NewBaseExecutorV2(e.Ctx().GetSessionVars(), e.Schema(), e.ID()),
		tableReaderExecutorContext: newTableReaderExecutorContext(e.Ctx()),
		table:                      e.table,
		startTS:                    e.startTS,
		dagPB:                      dagReq,
		columns:                    e.tableInfo.Cols(),
		plans:                      e.tblPlans,
		tablePlan:                  e.tblPlans[len(e.tblPlans)-1],
	}
	tableReaderExec.buildVirtualColumnInfo()
	return e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, handles, true)
}

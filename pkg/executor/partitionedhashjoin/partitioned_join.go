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

package partitionedhashjoin

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var (
	_ exec.Executor = &PartitionedHashJoinExec{}
)

// IsSupportedJoin returns true if current join is supported by partitioned hash join
func IsSupportedJoin(v *plannercore.PhysicalHashJoin) bool {
	switch v.JoinType {
	case plannercore.LeftOuterJoin, plannercore.InnerJoin:
		// null aware join is not supported yet
		if len(v.LeftNAJoinKeys) > 0 {
			return false
		}
		// cross join is not supported
		if len(v.LeftJoinKeys) == 0 {
			return false
		}
		// NullEQ is not supported yet
		for _, value := range v.IsNullEQ {
			if value {
				return false
			}
		}
		return true
	default:
		return false
	}
}

type PartitionedHashJoinCtx struct {
	SessCtx   sessionctx.Context
	allocPool chunk.Allocator
	// concurrency is the number of partition, build and join workers.
	Concurrency  uint
	joinResultCh chan *hashjoinWorkerResult
	// closeCh add a lock for closing executor.
	closeCh         chan struct{}
	finished        atomic.Bool
	UseOuterToBuild bool
	buildFinished   chan error
	JoinType        plannercore.JoinType
	stats           *hashJoinRuntimeStats
	probeTypes      []*types.FieldType
	buildTypes      []*types.FieldType
	memTracker      *memory.Tracker // track memory usage.
	diskTracker     *disk.Tracker   // track disk usage.

	filter         expression.CNFExprs
	otherCondition expression.CNFExprs
	joinHashTable  *joinHashTable
	hashTableMeta  *tableMeta
	keyMode        keyMode
}

// probeSideTupleFetcher reads tuples from probeSideExec and send them to probeWorkers.
type probeSideTupleFetcher struct {
	*PartitionedHashJoinCtx

	probeSideExec      exec.Executor
	probeChkResourceCh chan *probeChkResource
	probeResultChs     []chan *chunk.Chunk
	requiredRows       int64
}

type probeWorker struct {
	hashJoinCtx *PartitionedHashJoinCtx
	workerID    uint

	probeKeyColIdx []int

	// We build individual joinProbe for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joinProbe          joinProbe
	rowIters           *chunk.Iterator4Slice
	probeChkResourceCh chan *probeChkResource
	joinChkResourceCh  chan *chunk.Chunk
	probeResultCh      chan *chunk.Chunk
}

type buildWorker struct {
	hashJoinCtx      *PartitionedHashJoinCtx
	buildSideExec    exec.Executor
	buildKeyColIdx   []int
	buildNAKeyColIdx []int
}

// PartitionedHashJoinExec implements the hash join algorithm.
type PartitionedHashJoinExec struct {
	exec.BaseExecutor
	*PartitionedHashJoinCtx

	probeSideTupleFetcher *probeSideTupleFetcher
	probeWorkers          []*probeWorker
	buildWorker           *buildWorker

	workerWg util.WaitGroupWrapper
	waiterWg util.WaitGroupWrapper

	prepared bool
}

// probeChkResource stores the result of the join probe side fetch worker,
// `dest` is for Chunk reuse: after join workers process the probe side chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the probe side fetch worker will put new data into `chk` and write `chk` into dest.
type probeChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

func (e *PartitionedHashJoinExec) Init() error {
	return nil
}

// Close implements the Executor Close interface.
func (e *PartitionedHashJoinExec) Close() error {
	if e.closeCh != nil {
		close(e.closeCh)
	}
	e.finished.Store(true)
	if e.prepared {
		if e.buildFinished != nil {
			channel.Clear(e.buildFinished)
		}
		if e.joinResultCh != nil {
			channel.Clear(e.joinResultCh)
		}
		if e.probeSideTupleFetcher.probeChkResourceCh != nil {
			close(e.probeSideTupleFetcher.probeChkResourceCh)
			channel.Clear(e.probeSideTupleFetcher.probeChkResourceCh)
		}
		for i := range e.probeSideTupleFetcher.probeResultChs {
			channel.Clear(e.probeSideTupleFetcher.probeResultChs[i])
		}
		for i := range e.probeWorkers {
			close(e.probeWorkers[i].joinChkResourceCh)
			channel.Clear(e.probeWorkers[i].joinChkResourceCh)
		}
		e.probeSideTupleFetcher.probeChkResourceCh = nil
		e.waiterWg.Wait()
	}
	for _, w := range e.probeWorkers {
		w.joinChkResourceCh = nil
	}

	if e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	err := e.BaseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *PartitionedHashJoinExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		e.closeCh = nil
		e.prepared = false
		return err
	}
	e.prepared = false
	if e.PartitionedHashJoinCtx.memTracker != nil {
		e.PartitionedHashJoinCtx.memTracker.Reset()
	} else {
		e.PartitionedHashJoinCtx.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.PartitionedHashJoinCtx.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	e.diskTracker = disk.NewTracker(e.ID(), -1)
	e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)

	e.workerWg = util.WaitGroupWrapper{}
	e.waiterWg = util.WaitGroupWrapper{}
	e.closeCh = make(chan struct{})
	e.finished.Store(false)

	if e.RuntimeStats() != nil {
		e.stats = &hashJoinRuntimeStats{
			concurrent: int(e.Concurrency),
		}
	}
	return nil
}

// fetchProbeSideChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (fetcher *probeSideTupleFetcher) fetchProbeSideChunks(ctx context.Context, maxChunkSize int) {
	hasWaitedForBuild := false
	for {
		if fetcher.finished.Load() {
			return
		}

		var probeSideResource *probeChkResource
		var ok bool
		select {
		case <-fetcher.closeCh:
			return
		case probeSideResource, ok = <-fetcher.probeChkResourceCh:
			if !ok {
				return
			}
		}
		probeSideResult := probeSideResource.chk
		/*
			if fetcher.isOuterJoin {
				required := int(atomic.LoadInt64(&fetcher.requiredRows))
				probeSideResult.SetRequiredRows(required, maxChunkSize)
			}
		*/
		err := exec.Next(ctx, fetcher.probeSideExec, probeSideResult)
		failpoint.Inject("ConsumeRandomPanic", nil)
		if err != nil {
			fetcher.joinResultCh <- &hashjoinWorkerResult{
				err: err,
			}
			return
		}
		if !hasWaitedForBuild {
			failpoint.Inject("issue30289", func(val failpoint.Value) {
				if val.(bool) {
					probeSideResult.Reset()
				}
			})
			if probeSideResult.NumRows() == 0 && !fetcher.UseOuterToBuild {
				fetcher.finished.Store(true)
			}
			emptyBuild, buildErr := fetcher.wait4BuildSide()
			if buildErr != nil {
				fetcher.joinResultCh <- &hashjoinWorkerResult{
					err: buildErr,
				}
				return
			} else if emptyBuild {
				return
			}
			hasWaitedForBuild = true
		}

		if probeSideResult.NumRows() == 0 {
			return
		}

		probeSideResource.dest <- probeSideResult
	}
}

func (fetcher *probeSideTupleFetcher) wait4BuildSide() (emptyBuild bool, err error) {
	select {
	case <-fetcher.closeCh:
		return true, nil
	case err := <-fetcher.buildFinished:
		if err != nil {
			return false, err
		}
	}
	/*
		if fetcher.rowContainer.Len() == uint64(0) && (fetcher.joinType == plannercore.InnerJoin || fetcher.joinType == plannercore.SemiJoin) {
			return true, nil
		}
	*/
	return false, nil
}

// fetchBuildSideRows fetches all rows from build side executor, and append them
// to e.buildSideResult.
func (w *buildWorker) fetchBuildSideRows(ctx context.Context, chkCh chan<- *chunk.Chunk, errCh chan<- error, doneCh <-chan struct{}) {
	defer close(chkCh)
	var err error
	failpoint.Inject("issue30289", func(val failpoint.Value) {
		if val.(bool) {
			err = errors.Errorf("issue30289 build return error")
			errCh <- errors.Trace(err)
			return
		}
	})
	failpoint.Inject("issue42662_1", func(val failpoint.Value) {
		if val.(bool) {
			if w.hashJoinCtx.SessCtx.GetSessionVars().ConnectionID != 0 {
				// consume 170MB memory, this sql should be tracked into MemoryTop1Tracker
				w.hashJoinCtx.memTracker.Consume(170 * 1024 * 1024)
			}
			return
		}
	})
	sessVars := w.hashJoinCtx.SessCtx.GetSessionVars()
	for {
		if w.hashJoinCtx.finished.Load() {
			return
		}
		chk := w.hashJoinCtx.allocPool.Alloc(w.buildSideExec.RetFieldTypes(), sessVars.MaxChunkSize, sessVars.MaxChunkSize)
		err = exec.Next(ctx, w.buildSideExec, chk)
		if err != nil {
			errCh <- errors.Trace(err)
			return
		}
		failpoint.Inject("errorFetchBuildSideRowsMockOOMPanic", nil)
		failpoint.Inject("ConsumeRandomPanic", nil)
		if chk.NumRows() == 0 {
			return
		}
		select {
		case <-doneCh:
			return
		case <-w.hashJoinCtx.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

func (e *PartitionedHashJoinExec) initializeForProbe() {
	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.Concurrency+1)

	e.probeSideTupleFetcher.PartitionedHashJoinCtx = e.PartitionedHashJoinCtx
	// e.probeSideTupleFetcher.probeResultChs is for transmitting the chunks which store the data of
	// probeSideExec, it'll be written by probe side worker goroutine, and read by join
	// workers.
	e.probeSideTupleFetcher.probeResultChs = make([]chan *chunk.Chunk, e.Concurrency)
	for i := uint(0); i < e.Concurrency; i++ {
		e.probeSideTupleFetcher.probeResultChs[i] = make(chan *chunk.Chunk, 1)
		e.probeWorkers[i].probeResultCh = e.probeSideTupleFetcher.probeResultChs[i]
	}

	// e.probeChkResourceCh is for transmitting the used probeSideExec chunks from
	// join workers to probeSideExec worker.
	e.probeSideTupleFetcher.probeChkResourceCh = make(chan *probeChkResource, e.Concurrency)
	for i := uint(0); i < e.Concurrency; i++ {
		e.probeSideTupleFetcher.probeChkResourceCh <- &probeChkResource{
			chk:  exec.NewFirstChunk(e.probeSideTupleFetcher.probeSideExec),
			dest: e.probeSideTupleFetcher.probeResultChs[i],
		}
	}

	// e.probeWorker.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to probe worker goroutines.
	for i := uint(0); i < e.Concurrency; i++ {
		e.probeWorkers[i].joinChkResourceCh = make(chan *chunk.Chunk, 1)
		e.probeWorkers[i].joinChkResourceCh <- exec.NewFirstChunk(e)
		e.probeWorkers[i].probeChkResourceCh = e.probeSideTupleFetcher.probeChkResourceCh
	}
}

func (e *PartitionedHashJoinExec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.workerWg.RunWithRecover(func() {
		defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
		e.probeSideTupleFetcher.fetchProbeSideChunks(ctx, e.MaxChunkSize())
	}, e.probeSideTupleFetcher.handleProbeSideFetcherPanic)

	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.workerWg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "HashJoinWorker").End()
			e.probeWorkers[workerID].runJoinWorker()
		}, e.probeWorkers[workerID].handleProbeWorkerPanic)
	}
	e.waiterWg.RunWithRecover(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (fetcher *probeSideTupleFetcher) handleProbeSideFetcherPanic(r any) {
	for i := range fetcher.probeResultChs {
		close(fetcher.probeResultChs[i])
	}
	if r != nil {
		fetcher.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

func (w *probeWorker) handleProbeWorkerPanic(r any) {
	if r != nil {
		w.hashJoinCtx.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

func (e *PartitionedHashJoinExec) handleJoinWorkerPanic(r any) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

func (e *PartitionedHashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.workerWg.Wait()
	if e.probeWorkers[0] != nil && e.probeWorkers[0].joinProbe.needScanHT() {
		for i := uint(0); i < e.Concurrency; i++ {
			var workerID = i
			e.workerWg.RunWithRecover(func() {
				e.probeWorkers[workerID].scanHashTableAfterProbeDone()
			}, e.handleJoinWorkerPanic)
		}
	}
	close(e.joinResultCh)
}

func (w *probeWorker) scanHashTableAfterProbeDone() {
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}
	for !w.joinProbe.isScanHTDone() {
		joinResult = w.joinProbe.scanHT(joinResult)
		if joinResult.err != nil {
			w.hashJoinCtx.joinResultCh <- joinResult
			return
		}
		if joinResult.chk.IsFull() {
			w.hashJoinCtx.joinResultCh <- joinResult
			ok, joinResult = w.getNewJoinResult()
			if !ok {
				return
			}
		}
	}
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.hashJoinCtx.joinResultCh <- joinResult
	}
}

func (w *probeWorker) processOneProbeChunk(probeChunk *chunk.Chunk, joinResult *hashjoinWorkerResult) (ok bool, _ *hashjoinWorkerResult) {
	joinResult.err = w.joinProbe.setChunkForProbe(probeChunk)
	if joinResult.err != nil {
		return false, joinResult
	}
	for !w.joinProbe.isCurrentChunkProbeDone() {
		ok, joinResult = w.joinProbe.probe(joinResult)
		if !ok || joinResult.err != nil {
			return ok, joinResult
		}
		if joinResult.chk.IsFull() {
			w.hashJoinCtx.joinResultCh <- joinResult
			ok, joinResult = w.getNewJoinResult()
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

func (w *probeWorker) runJoinWorker() {
	probeTime := int64(0)
	if w.hashJoinCtx.stats != nil {
		start := time.Now()
		defer func() {
			t := time.Since(start)
			atomic.AddInt64(&w.hashJoinCtx.stats.probe, probeTime)
			atomic.AddInt64(&w.hashJoinCtx.stats.fetchAndProbe, int64(t))
			w.hashJoinCtx.stats.setMaxFetchAndProbeTime(int64(t))
		}()
	}

	var (
		probeSideResult *chunk.Chunk
	)
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: w.probeResultCh,
	}
	for ok := true; ok; {
		if w.hashJoinCtx.finished.Load() {
			break
		}
		select {
		case <-w.hashJoinCtx.closeCh:
			return
		case probeSideResult, ok = <-w.probeResultCh:
		}
		failpoint.Inject("ConsumeRandomPanic", nil)
		if !ok {
			break
		}

		start := time.Now()
		ok, joinResult = w.processOneProbeChunk(probeSideResult, joinResult)
		probeTime += int64(time.Since(start))
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult
		w.probeChkResourceCh <- emptyProbeSideResult
	}
	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.hashJoinCtx.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		w.joinChkResourceCh <- joinResult.chk
	}
}

func (w *probeWorker) getNewJoinResult() (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: w.joinChkResourceCh,
	}
	ok := true
	select {
	case <-w.hashJoinCtx.closeCh:
		ok = false
	case joinResult.chk, ok = <-w.joinChkResourceCh:
	}
	return ok, joinResult
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash table;
// step 2. fetch data from probe child in a background goroutine and probe the hash table in multiple join workers.
func (e *PartitionedHashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	/*
		if !e.prepared {
			e.buildFinished = make(chan error, 1)
			hCtx := &hashContext{
				allTypes:    e.buildTypes,
				keyColIdx:   e.buildWorker.buildKeyColIdx,
				naKeyColIdx: e.buildWorker.buildNAKeyColIdx,
			}
			e.rowContainer = newHashRowContainer(e.Ctx(), hCtx, exec.RetTypes(e.buildWorker.buildSideExec))
			// we shallow copies rowContainer for each probe worker to avoid lock contention
			for i := uint(0); i < e.Concurrency; i++ {
				if i == 0 {
					e.probeWorkers[i].rowContainerForProbe = e.rowContainer
				} else {
					e.probeWorkers[i].rowContainerForProbe = e.rowContainer.ShallowCopy()
				}
			}
			for i := uint(0); i < e.concurrency; i++ {
				e.probeWorkers[i].rowIters = chunk.NewIterator4Slice([]chunk.Row{})
			}
			e.workerWg.RunWithRecover(func() {
				defer trace.StartRegion(ctx, "HashJoinHashTableBuilder").End()
				e.fetchAndBuildHashTable(ctx)
			}, e.handleFetchAndBuildHashTablePanic)
			e.fetchAndProbeHashTable(ctx)
			e.prepared = true
		}
		if e.isOuterJoin {
			atomic.StoreInt64(&e.probeSideTupleFetcher.requiredRows, int64(req.RequiredRows()))
		}
		req.Reset()

		result, ok := <-e.joinResultCh
		if !ok {
			return nil
		}
		if result.err != nil {
			e.finished.Store(true)
			return result.err
		}
		req.SwapColumns(result.chk)
		result.src <- result.chk
	*/
	return nil
}

func (e *PartitionedHashJoinExec) handleFetchAndBuildHashTablePanic(r any) {
	if r != nil {
		e.buildFinished <- util.GetRecoverError(r)
	}
	close(e.buildFinished)
}

func (e *PartitionedHashJoinExec) fetchAndBuildHashTable(ctx context.Context) {
	if e.stats != nil {
		start := time.Now()
		defer func() {
			e.stats.fetchAndBuildHashTable = time.Since(start)
		}()
	}
	// buildSideResultCh transfers build side chunk from build side fetch to build hash table.
	buildSideResultCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	fetchBuildSideRowsOk := make(chan error, 1)
	e.workerWg.RunWithRecover(
		func() {
			defer trace.StartRegion(ctx, "HashJoinBuildSideFetcher").End()
			e.buildWorker.fetchBuildSideRows(ctx, buildSideResultCh, fetchBuildSideRowsOk, doneCh)
		},
		func(r any) {
			if r != nil {
				fetchBuildSideRowsOk <- util.GetRecoverError(r)
			}
			close(fetchBuildSideRowsOk)
		},
	)

	// TODO: Parallel build hash table. Currently not support because `unsafeHashTable` is not thread-safe.
	err := e.buildWorker.buildHashTableForList(buildSideResultCh)
	if err != nil {
		e.buildFinished <- errors.Trace(err)
		close(doneCh)
	}
	// Wait fetchBuildSideRows be finished.
	// 1. if buildHashTableForList fails
	// 2. if probeSideResult.NumRows() == 0, fetchProbeSideChunks will not wait for the build side.
	channel.Clear(buildSideResultCh)
	// Check whether err is nil to avoid sending redundant error into buildFinished.
	if err == nil {
		if err = <-fetchBuildSideRowsOk; err != nil {
			e.buildFinished <- err
		}
	}
}

// buildHashTableForList builds hash table from `list`.
func (w *buildWorker) buildHashTableForList(buildSideResultCh <-chan *chunk.Chunk) error {
	/*
		var err error
		var selected []bool
		rowContainer := w.hashJoinCtx.rowContainer
		rowContainer.GetMemTracker().AttachTo(w.hashJoinCtx.memTracker)
		rowContainer.GetMemTracker().SetLabel(memory.LabelForBuildSideResult)
		rowContainer.GetDiskTracker().AttachTo(w.hashJoinCtx.diskTracker)
		rowContainer.GetDiskTracker().SetLabel(memory.LabelForBuildSideResult)
		if variable.EnableTmpStorageOnOOM.Load() {
			actionSpill := rowContainer.ActionSpill()
			failpoint.Inject("testRowContainerSpill", func(val failpoint.Value) {
				if val.(bool) {
					actionSpill = rowContainer.rowContainer.ActionSpillForTest()
					defer actionSpill.(*chunk.SpillDiskAction).WaitForTest()
				}
			})
			w.hashJoinCtx.sessCtx.GetSessionVars().MemTracker.FallbackOldAndSetNewAction(actionSpill)
		}
		for chk := range buildSideResultCh {
			if w.hashJoinCtx.finished.Load() {
				return nil
			}
			if !w.hashJoinCtx.useOuterToBuild {
				err = rowContainer.PutChunk(chk, w.hashJoinCtx.isNullEQ)
			} else {
				var bitMap = bitmap.NewConcurrentBitmap(chk.NumRows())
				w.hashJoinCtx.outerMatchedStatus = append(w.hashJoinCtx.outerMatchedStatus, bitMap)
				w.hashJoinCtx.memTracker.Consume(bitMap.BytesConsumed())
				if len(w.hashJoinCtx.outerFilter) == 0 {
					err = w.hashJoinCtx.rowContainer.PutChunk(chk, w.hashJoinCtx.isNullEQ)
				} else {
					selected, err = expression.VectorizedFilter(w.hashJoinCtx.sessCtx, w.hashJoinCtx.outerFilter, chunk.NewIterator4Chunk(chk), selected)
					if err != nil {
						return err
					}
					err = rowContainer.PutChunkSelected(chk, selected, w.hashJoinCtx.isNullEQ)
				}
			}
			failpoint.Inject("ConsumeRandomPanic", nil)
			if err != nil {
				return err
			}
		}
	*/
	return nil
}

// cacheInfo is used to save the concurrency information of the executor operator
type cacheInfo struct {
	hitRatio float64
	useCache bool
}

type joinRuntimeStats struct {
	*execdetails.RuntimeStatsWithConcurrencyInfo

	applyCache  bool
	cache       cacheInfo
	hasHashStat bool
	hashStat    hashStatistic
}

func newJoinRuntimeStats() *joinRuntimeStats {
	stats := &joinRuntimeStats{
		RuntimeStatsWithConcurrencyInfo: &execdetails.RuntimeStatsWithConcurrencyInfo{},
	}
	return stats
}

// setCacheInfo sets the cache information. Only used for apply executor.
func (e *joinRuntimeStats) setCacheInfo(useCache bool, hitRatio float64) {
	e.Lock()
	e.applyCache = true
	e.cache.useCache = useCache
	e.cache.hitRatio = hitRatio
	e.Unlock()
}

func (e *joinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteString(e.RuntimeStatsWithConcurrencyInfo.String())
	if e.applyCache {
		if e.cache.useCache {
			fmt.Fprintf(buf, ", cache:ON, cacheHitRatio:%.3f%%", e.cache.hitRatio*100)
		} else {
			buf.WriteString(", cache:OFF")
		}
	}
	if e.hasHashStat {
		buf.WriteString(", " + e.hashStat.String())
	}
	return buf.String()
}

// Tp implements the RuntimeStats interface.
func (*joinRuntimeStats) Tp() int {
	return execdetails.TpJoinRuntimeStats
}

func (e *joinRuntimeStats) Clone() execdetails.RuntimeStats {
	newJRS := &joinRuntimeStats{
		RuntimeStatsWithConcurrencyInfo: e.RuntimeStatsWithConcurrencyInfo,
		applyCache:                      e.applyCache,
		cache:                           e.cache,
		hasHashStat:                     e.hasHashStat,
		hashStat:                        e.hashStat,
	}
	return newJRS
}

type hashJoinRuntimeStats struct {
	fetchAndBuildHashTable time.Duration
	hashStat               hashStatistic
	fetchAndProbe          int64
	probe                  int64
	concurrent             int
	maxFetchAndProbe       int64
}

func (e *hashJoinRuntimeStats) setMaxFetchAndProbeTime(t int64) {
	for {
		value := atomic.LoadInt64(&e.maxFetchAndProbe)
		if t <= value {
			return
		}
		if atomic.CompareAndSwapInt64(&e.maxFetchAndProbe, value, t) {
			return
		}
	}
}

// Tp implements the RuntimeStats interface.
func (*hashJoinRuntimeStats) Tp() int {
	return execdetails.TpHashJoinRuntimeStats
}

func (e *hashJoinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if e.fetchAndBuildHashTable > 0 {
		buf.WriteString("build_hash_table:{total:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable))
		buf.WriteString(", fetch:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable - e.hashStat.buildTableElapse))
		buf.WriteString(", build:")
		buf.WriteString(execdetails.FormatDuration(e.hashStat.buildTableElapse))
		buf.WriteString("}")
	}
	if e.probe > 0 {
		buf.WriteString(", probe:{concurrency:")
		buf.WriteString(strconv.Itoa(e.concurrent))
		buf.WriteString(", total:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe)))
		buf.WriteString(", max:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(atomic.LoadInt64(&e.maxFetchAndProbe))))
		buf.WriteString(", probe:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.probe)))
		buf.WriteString(", fetch:")
		buf.WriteString(execdetails.FormatDuration(time.Duration(e.fetchAndProbe - e.probe)))
		if e.hashStat.probeCollision > 0 {
			buf.WriteString(", probe_collision:")
			buf.WriteString(strconv.FormatInt(e.hashStat.probeCollision, 10))
		}
		buf.WriteString("}")
	}
	return buf.String()
}

func (e *hashJoinRuntimeStats) Clone() execdetails.RuntimeStats {
	return &hashJoinRuntimeStats{
		fetchAndBuildHashTable: e.fetchAndBuildHashTable,
		hashStat:               e.hashStat,
		fetchAndProbe:          e.fetchAndProbe,
		probe:                  e.probe,
		concurrent:             e.concurrent,
		maxFetchAndProbe:       e.maxFetchAndProbe,
	}
}

func (e *hashJoinRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*hashJoinRuntimeStats)
	if !ok {
		return
	}
	e.fetchAndBuildHashTable += tmp.fetchAndBuildHashTable
	e.hashStat.buildTableElapse += tmp.hashStat.buildTableElapse
	e.hashStat.probeCollision += tmp.hashStat.probeCollision
	e.fetchAndProbe += tmp.fetchAndProbe
	e.probe += tmp.probe
	if e.maxFetchAndProbe < tmp.maxFetchAndProbe {
		e.maxFetchAndProbe = tmp.maxFetchAndProbe
	}
}

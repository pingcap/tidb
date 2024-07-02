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
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/applycache"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/bitmap"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var (
	_ exec.Executor = &HashJoinExec{}
	_ exec.Executor = &NestedLoopApplyExec{}
)

type hashJoinCtx struct {
	sessCtx   sessionctx.Context
	allocPool chunk.Allocator
	// concurrency is the number of partition, build and join workers.
	concurrency  uint
	joinResultCh chan *hashjoinWorkerResult
	// closeCh add a lock for closing executor.
	closeCh            chan struct{}
	finished           atomic.Bool
	useOuterToBuild    bool
	isOuterJoin        bool
	isNullEQ           []bool
	buildFinished      chan error
	rowContainer       *hashRowContainer
	joinType           plannercore.JoinType
	outerMatchedStatus []*bitmap.ConcurrentBitmap
	stats              *hashJoinRuntimeStats
	probeTypes         []*types.FieldType
	buildTypes         []*types.FieldType
	outerFilter        expression.CNFExprs
	isNullAware        bool
	memTracker         *memory.Tracker // track memory usage.
	diskTracker        *disk.Tracker   // track disk usage.
}

// probeSideTupleFetcher reads tuples from probeSideExec and send them to probeWorkers.
type probeSideTupleFetcher struct {
	*hashJoinCtx

	probeSideExec      exec.Executor
	probeChkResourceCh chan *probeChkResource
	probeResultChs     []chan *chunk.Chunk
	requiredRows       int64
}

type probeWorker struct {
	hashJoinCtx *hashJoinCtx
	workerID    uint

	probeKeyColIdx   []int
	probeNAKeyColIdx []int
	// We pre-alloc and reuse the Rows and RowPtrs for each probe goroutine, to avoid allocation frequently
	buildSideRows    []chunk.Row
	buildSideRowPtrs []chunk.RowPtr

	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiner               joiner
	rowIters             *chunk.Iterator4Slice
	rowContainerForProbe *hashRowContainer
	// for every naaj probe worker,  pre-allocate the int slice for store the join column index to check.
	needCheckBuildColPos []int
	needCheckProbeColPos []int
	needCheckBuildTypes  []*types.FieldType
	needCheckProbeTypes  []*types.FieldType
	probeChkResourceCh   chan *probeChkResource
	joinChkResourceCh    chan *chunk.Chunk
	probeResultCh        chan *chunk.Chunk
}

type buildWorker struct {
	hashJoinCtx      *hashJoinCtx
	buildSideExec    exec.Executor
	buildKeyColIdx   []int
	buildNAKeyColIdx []int
}

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	exec.BaseExecutor
	*hashJoinCtx

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

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
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
		terror.Call(e.rowContainer.Close)
		e.hashJoinCtx.sessCtx.GetSessionVars().MemTracker.UnbindActionFromHardLimit(e.rowContainer.ActionSpill())
		e.waiterWg.Wait()
	}
	e.outerMatchedStatus = e.outerMatchedStatus[:0]
	for _, w := range e.probeWorkers {
		w.buildSideRows = nil
		w.buildSideRowPtrs = nil
		w.needCheckBuildColPos = nil
		w.needCheckProbeColPos = nil
		w.needCheckBuildTypes = nil
		w.needCheckProbeTypes = nil
		w.joinChkResourceCh = nil
	}

	if e.stats != nil && e.rowContainer != nil {
		e.stats.hashStat = *e.rowContainer.stat
	}
	if e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	err := e.BaseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open(ctx context.Context) error {
	if err := e.BaseExecutor.Open(ctx); err != nil {
		e.closeCh = nil
		e.prepared = false
		return err
	}
	e.prepared = false
	if e.hashJoinCtx.memTracker != nil {
		e.hashJoinCtx.memTracker.Reset()
	} else {
		e.hashJoinCtx.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.hashJoinCtx.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	if e.hashJoinCtx.diskTracker != nil {
		e.hashJoinCtx.diskTracker.Reset()
	} else {
		e.hashJoinCtx.diskTracker = disk.NewTracker(e.ID(), -1)
	}
	e.hashJoinCtx.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)

	e.workerWg = util.WaitGroupWrapper{}
	e.waiterWg = util.WaitGroupWrapper{}
	e.closeCh = make(chan struct{})
	e.finished.Store(false)

	if e.RuntimeStats() != nil {
		e.stats = &hashJoinRuntimeStats{
			concurrent: int(e.concurrency),
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
		if fetcher.isOuterJoin {
			required := int(atomic.LoadInt64(&fetcher.requiredRows))
			probeSideResult.SetRequiredRows(required, maxChunkSize)
		}
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
	if fetcher.rowContainer.Len() == uint64(0) && (fetcher.joinType == plannercore.InnerJoin || fetcher.joinType == plannercore.SemiJoin) {
		return true, nil
	}
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
			if w.hashJoinCtx.sessCtx.GetSessionVars().ConnectionID != 0 {
				// consume 170MB memory, this sql should be tracked into MemoryTop1Tracker
				w.hashJoinCtx.memTracker.Consume(170 * 1024 * 1024)
			}
			return
		}
	})
	sessVars := w.hashJoinCtx.sessCtx.GetSessionVars()
	failpoint.Inject("issue51998", func(val failpoint.Value) {
		if val.(bool) {
			time.Sleep(2 * time.Second)
		}
	})
	for {
		if w.hashJoinCtx.finished.Load() {
			return
		}
		chk := w.hashJoinCtx.allocPool.Alloc(w.buildSideExec.RetFieldTypes(), sessVars.MaxChunkSize, sessVars.MaxChunkSize)
		err = exec.Next(ctx, w.buildSideExec, chk)
		failpoint.Inject("issue51998", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.Errorf("issue51998 build return error")
			}
		})
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

func (e *HashJoinExec) initializeForProbe() {
	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)

	e.probeSideTupleFetcher.hashJoinCtx = e.hashJoinCtx
	// e.probeSideTupleFetcher.probeResultChs is for transmitting the chunks which store the data of
	// probeSideExec, it'll be written by probe side worker goroutine, and read by join
	// workers.
	e.probeSideTupleFetcher.probeResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeSideTupleFetcher.probeResultChs[i] = make(chan *chunk.Chunk, 1)
		e.probeWorkers[i].probeResultCh = e.probeSideTupleFetcher.probeResultChs[i]
	}

	// e.probeChkResourceCh is for transmitting the used probeSideExec chunks from
	// join workers to probeSideExec worker.
	e.probeSideTupleFetcher.probeChkResourceCh = make(chan *probeChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeSideTupleFetcher.probeChkResourceCh <- &probeChkResource{
			chk:  exec.NewFirstChunk(e.probeSideTupleFetcher.probeSideExec),
			dest: e.probeSideTupleFetcher.probeResultChs[i],
		}
	}

	// e.probeWorker.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to probe worker goroutines.
	for i := uint(0); i < e.concurrency; i++ {
		e.probeWorkers[i].joinChkResourceCh = make(chan *chunk.Chunk, 1)
		e.probeWorkers[i].joinChkResourceCh <- exec.NewFirstChunk(e)
		e.probeWorkers[i].probeChkResourceCh = e.probeSideTupleFetcher.probeChkResourceCh
	}
}

func (e *HashJoinExec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.workerWg.RunWithRecover(func() {
		defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
		e.probeSideTupleFetcher.fetchProbeSideChunks(ctx, e.MaxChunkSize())
	}, e.probeSideTupleFetcher.handleProbeSideFetcherPanic)

	for i := uint(0); i < e.concurrency; i++ {
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

func (e *HashJoinExec) handleJoinWorkerPanic(r any) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: util.GetRecoverError(r)}
	}
}

// Concurrently handling unmatched rows from the hash table
func (w *probeWorker) handleUnmatchedRowsFromHashTable() {
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}
	numChks := w.rowContainerForProbe.NumChunks()
	for i := int(w.workerID); i < numChks; i += int(w.hashJoinCtx.concurrency) {
		chk, err := w.rowContainerForProbe.GetChunk(i)
		if err != nil {
			// Catching the error and send it
			joinResult.err = err
			w.hashJoinCtx.joinResultCh <- joinResult
			return
		}
		for j := 0; j < chk.NumRows(); j++ {
			if !w.hashJoinCtx.outerMatchedStatus[i].UnsafeIsSet(j) { // process unmatched outer rows
				w.joiner.onMissMatch(false, chk.GetRow(j), joinResult.chk)
			}
			if joinResult.chk.IsFull() {
				w.hashJoinCtx.joinResultCh <- joinResult
				ok, joinResult = w.getNewJoinResult()
				if !ok {
					return
				}
			}
		}
	}

	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		w.hashJoinCtx.joinResultCh <- joinResult
	}
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.workerWg.Wait()
	if e.useOuterToBuild {
		// Concurrently handling unmatched rows from the hash table at the tail
		for i := uint(0); i < e.concurrency; i++ {
			var workerID = i
			e.workerWg.RunWithRecover(func() { e.probeWorkers[workerID].handleUnmatchedRowsFromHashTable() }, e.handleJoinWorkerPanic)
		}
		e.workerWg.Wait()
	}
	close(e.joinResultCh)
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
		selected        = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: w.probeResultCh,
	}
	hCtx := &hashContext{
		allTypes:    w.hashJoinCtx.probeTypes,
		keyColIdx:   w.probeKeyColIdx,
		naKeyColIdx: w.probeNAKeyColIdx,
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
		// waitTime is the time cost on w.sendingResult(), it should not be added to probe time, because if
		// parent executor does not call `e.Next()`, `sendingResult()` will hang, and this hang has nothing to do
		// with the probe
		waitTime := int64(0)
		if w.hashJoinCtx.useOuterToBuild {
			ok, waitTime, joinResult = w.join2ChunkForOuterHashJoin(probeSideResult, hCtx, joinResult)
		} else {
			ok, waitTime, joinResult = w.join2Chunk(probeSideResult, hCtx, joinResult, selected)
		}
		probeTime += int64(time.Since(start)) - waitTime
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

func (w *probeWorker) joinMatchedProbeSideRow2ChunkForOuterHashJoin(probeKey uint64, probeSideRow chunk.Row, hCtx *hashContext, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var err error
	waitTime := int64(0)
	oneWaitTime := int64(0)
	w.buildSideRows, w.buildSideRowPtrs, err = w.rowContainerForProbe.GetMatchedRowsAndPtrs(probeKey, probeSideRow, hCtx, w.buildSideRows, w.buildSideRowPtrs, true)
	buildSideRows, rowsPtrs := w.buildSideRows, w.buildSideRowPtrs
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) == 0 {
		return true, waitTime, joinResult
	}

	iter := w.rowIters
	iter.Reset(buildSideRows)
	var outerMatchStatus []outerRowStatusFlag
	rowIdx, ok := 0, false
	for iter.Begin(); iter.Current() != iter.End(); {
		outerMatchStatus, err = w.joiner.tryToMatchOuters(iter, probeSideRow, joinResult.chk, outerMatchStatus)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		for i := range outerMatchStatus {
			if outerMatchStatus[i] == outerRowMatched {
				w.hashJoinCtx.outerMatchedStatus[rowsPtrs[rowIdx+i].ChkIdx].Set(int(rowsPtrs[rowIdx+i].RowIdx))
			}
		}
		rowIdx += len(outerMatchStatus)
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	return true, waitTime, joinResult
}

// joinNAALOSJMatchProbeSideRow2Chunk implement the matching logic for NA-AntiLeftOuterSemiJoin
func (w *probeWorker) joinNAALOSJMatchProbeSideRow2Chunk(probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *hashContext, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var (
		err error
		ok  bool
	)
	waitTime := int64(0)
	oneWaitTime := int64(0)
	if probeKeyNullBits == nil {
		// step1: match the same key bucket first.
		// because AntiLeftOuterSemiJoin cares about the scalar value. If we both have a match from null
		// bucket and same key bucket, we should return the result as <rhs-row, 0> from same-key bucket
		// rather than <rhs-row, null> from null bucket.
		w.buildSideRows, err = w.rowContainerForProbe.GetMatchedRows(probeKey, probeSideRow, hCtx, w.buildSideRows)
		buildSideRows := w.buildSideRows
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if len(buildSideRows) != 0 {
			iter1 := w.rowIters
			iter1.Reset(buildSideRows)
			for iter1.Begin(); iter1.Current() != iter1.End(); {
				matched, _, err := w.joiner.tryToMatchInners(probeSideRow, iter1, joinResult.chk, LeftNotNullRightNotNull)
				if err != nil {
					joinResult.err = err
					return false, waitTime, joinResult
				}
				// here matched means: there is a valid same-key bucket row from right side.
				// as said in the comment, once we meet a same key (NOT IN semantic) in CNF, we can determine the result as <rhs, 0>.
				if matched {
					return true, waitTime, joinResult
				}
				if joinResult.chk.IsFull() {
					ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
					waitTime += oneWaitTime
					if !ok {
						return false, waitTime, joinResult
					}
				}
			}
		}
		// step2: match the null bucket secondly.
		w.buildSideRows, err = w.rowContainerForProbe.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
		buildSideRows = w.buildSideRows
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if len(buildSideRows) == 0 {
			// when reach here, it means we couldn't find a valid same key match from same-key bucket yet
			// and the null bucket is empty. so the result should be <rhs, 1>.
			w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
			return true, waitTime, joinResult
		}
		iter2 := w.rowIters
		iter2.Reset(buildSideRows)
		for iter2.Begin(); iter2.Current() != iter2.End(); {
			matched, _, err := w.joiner.tryToMatchInners(probeSideRow, iter2, joinResult.chk, LeftNotNullRightHasNull)
			if err != nil {
				joinResult.err = err
				return false, waitTime, joinResult
			}
			// here matched means: there is a valid null bucket row from right side.
			// as said in the comment, once we meet a null in CNF, we can determine the result as <rhs, null>.
			if matched {
				return true, waitTime, joinResult
			}
			if joinResult.chk.IsFull() {
				ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
		// step3: if we couldn't return it quickly in null bucket and same key bucket, here means two cases:
		// case1: x NOT IN (empty set): if other key bucket don't have the valid rows yet.
		// case2: x NOT IN (l,m,n...): if other key bucket do have the valid rows.
		// both cases mean the result should be <rhs, 1>
		w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	// when left side has null values, all we want is to find a valid build side rows (past other condition)
	// so we can return it as soon as possible. here means two cases:
	// case1: <?, null> NOT IN (empty set):             ----------------------> result is <rhs, 1>.
	// case2: <?, null> NOT IN (at least a valid inner row) ------------------> result is <rhs, null>.
	// Step1: match null bucket (assumption that null bucket is quite smaller than all hash table bucket rows)
	w.buildSideRows, err = w.rowContainerForProbe.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
	buildSideRows := w.buildSideRows
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) != 0 {
		iter1 := w.rowIters
		iter1.Reset(buildSideRows)
		for iter1.Begin(); iter1.Current() != iter1.End(); {
			matched, _, err := w.joiner.tryToMatchInners(probeSideRow, iter1, joinResult.chk, LeftHasNullRightHasNull)
			if err != nil {
				joinResult.err = err
				return false, waitTime, joinResult
			}
			// here matched means: there is a valid null bucket row from right side. (not empty)
			// as said in the comment, once we found at least a valid row, we can determine the result as <rhs, null>.
			if matched {
				return true, waitTime, joinResult
			}
			if joinResult.chk.IsFull() {
				ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
	}
	// Step2: match all hash table bucket build rows (use probeKeyNullBits to filter if any).
	w.buildSideRows, err = w.rowContainerForProbe.GetAllMatchedRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
	buildSideRows = w.buildSideRows
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) == 0 {
		// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
		// which means x NOT IN (empty set) or x NOT IN (l,m,n), the result should be <rhs, 1>
		w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	iter2 := w.rowIters
	iter2.Reset(buildSideRows)
	for iter2.Begin(); iter2.Current() != iter2.End(); {
		matched, _, err := w.joiner.tryToMatchInners(probeSideRow, iter2, joinResult.chk, LeftHasNullRightNotNull)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		// here matched means: there is a valid same key bucket row from right side. (not empty)
		// as said in the comment, once we found at least a valid row, we can determine the result as <rhs, null>.
		if matched {
			return true, waitTime, joinResult
		}
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	// step3: if we couldn't return it quickly in null bucket and all hash bucket, here means only one cases:
	// case1: <?, null> NOT IN (empty set):
	// empty set comes from no rows from all bucket can pass other condition. the result should be <rhs, 1>
	w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
	return true, waitTime, joinResult
}

// joinNAASJMatchProbeSideRow2Chunk implement the matching logic for NA-AntiSemiJoin
func (w *probeWorker) joinNAASJMatchProbeSideRow2Chunk(probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *hashContext, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var (
		err error
		ok  bool
	)
	waitTime := int64(0)
	oneWaitTime := int64(0)
	if probeKeyNullBits == nil {
		// step1: match null bucket first.
		// need fetch the "valid" rows every time. (nullBits map check is necessary)
		w.buildSideRows, err = w.rowContainerForProbe.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
		buildSideRows := w.buildSideRows
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if len(buildSideRows) != 0 {
			iter1 := w.rowIters
			iter1.Reset(buildSideRows)
			for iter1.Begin(); iter1.Current() != iter1.End(); {
				matched, _, err := w.joiner.tryToMatchInners(probeSideRow, iter1, joinResult.chk)
				if err != nil {
					joinResult.err = err
					return false, waitTime, joinResult
				}
				// here matched means: there is a valid null bucket row from right side.
				// as said in the comment, once we meet a rhs null in CNF, we can determine the reject of lhs row.
				if matched {
					return true, waitTime, joinResult
				}
				if joinResult.chk.IsFull() {
					ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
					waitTime += oneWaitTime
					if !ok {
						return false, waitTime, joinResult
					}
				}
			}
		}
		// step2: then same key bucket.
		w.buildSideRows, err = w.rowContainerForProbe.GetMatchedRows(probeKey, probeSideRow, hCtx, w.buildSideRows)
		buildSideRows = w.buildSideRows
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if len(buildSideRows) == 0 {
			// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
			// which means x NOT IN (empty set), accept the rhs row.
			w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
			return true, waitTime, joinResult
		}
		iter2 := w.rowIters
		iter2.Reset(buildSideRows)
		for iter2.Begin(); iter2.Current() != iter2.End(); {
			matched, _, err := w.joiner.tryToMatchInners(probeSideRow, iter2, joinResult.chk)
			if err != nil {
				joinResult.err = err
				return false, waitTime, joinResult
			}
			// here matched means: there is a valid same key bucket row from right side.
			// as said in the comment, once we meet a false in CNF, we can determine the reject of lhs row.
			if matched {
				return true, waitTime, joinResult
			}
			if joinResult.chk.IsFull() {
				ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
		// step3: if we couldn't return it quickly in null bucket and same key bucket, here means two cases:
		// case1: x NOT IN (empty set): if other key bucket don't have the valid rows yet.
		// case2: x NOT IN (l,m,n...): if other key bucket do have the valid rows.
		// both cases should accept the rhs row.
		w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	// when left side has null values, all we want is to find a valid build side rows (passed from other condition)
	// so we can return it as soon as possible. here means two cases:
	// case1: <?, null> NOT IN (empty set):             ----------------------> accept rhs row.
	// case2: <?, null> NOT IN (at least a valid inner row) ------------------> unknown result, refuse rhs row.
	// Step1: match null bucket (assumption that null bucket is quite smaller than all hash table bucket rows)
	w.buildSideRows, err = w.rowContainerForProbe.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
	buildSideRows := w.buildSideRows
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) != 0 {
		iter1 := w.rowIters
		iter1.Reset(buildSideRows)
		for iter1.Begin(); iter1.Current() != iter1.End(); {
			matched, _, err := w.joiner.tryToMatchInners(probeSideRow, iter1, joinResult.chk)
			if err != nil {
				joinResult.err = err
				return false, waitTime, joinResult
			}
			// here matched means: there is a valid null bucket row from right side. (not empty)
			// as said in the comment, once we found at least a valid row, we can determine the reject of lhs row.
			if matched {
				return true, waitTime, joinResult
			}
			if joinResult.chk.IsFull() {
				ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
	}
	// Step2: match all hash table bucket build rows.
	w.buildSideRows, err = w.rowContainerForProbe.GetAllMatchedRows(hCtx, probeSideRow, probeKeyNullBits, w.buildSideRows, w.needCheckBuildColPos, w.needCheckProbeColPos, w.needCheckBuildTypes, w.needCheckProbeTypes)
	buildSideRows = w.buildSideRows
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) == 0 {
		// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
		// which means <?,null> NOT IN (empty set) or <?,null> NOT IN (no valid rows) accept the rhs row.
		w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	iter2 := w.rowIters
	iter2.Reset(buildSideRows)
	for iter2.Begin(); iter2.Current() != iter2.End(); {
		matched, _, err := w.joiner.tryToMatchInners(probeSideRow, iter2, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		// here matched means: there is a valid key row from right side. (not empty)
		// as said in the comment, once we found at least a valid row, we can determine the reject of lhs row.
		if matched {
			return true, waitTime, joinResult
		}
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	// step3: if we couldn't return it quickly in null bucket and all hash bucket, here means only one cases:
	// case1: <?, null> NOT IN (empty set):
	// empty set comes from no rows from all bucket can pass other condition. we should accept the rhs row.
	w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
	return true, waitTime, joinResult
}

// joinNAAJMatchProbeSideRow2Chunk implement the matching priority logic for NA-AntiSemiJoin and NA-AntiLeftOuterSemiJoin
// there are some bucket-matching priority difference between them.
//
//		Since NA-AntiSemiJoin don't need to append the scalar value with the left side row, there is a quick matching path.
//		1: lhs row has null:
//	       lhs row has null can't determine its result in advance, we should judge whether the right valid set is empty
//	       or not. For semantic like x NOT IN(y set), If y set is empty, the scalar result is 1; Otherwise, the result
//	       is 0. Since NA-AntiSemiJoin don't care about the scalar value, we just try to find a valid row from right side,
//	       once we found it then just return the left side row instantly. (same as NA-AntiLeftOuterSemiJoin)
//
//		2: lhs row without null:
//	       same-key bucket and null-bucket which should be the first to match? For semantic like x NOT IN(y set), once y
//	       set has a same key x, the scalar value is 0; else if y set has a null key, then the scalar value is null. Both
//	       of them lead the refuse of the lhs row without any difference. Since NA-AntiSemiJoin don't care about the scalar
//	       value, we can just match the null bucket first and refuse the lhs row as quickly as possible, because a null of
//	       yi in the CNF (x NA-EQ yi) can always determine a negative value (refuse lhs row) in advance here.
//
//	       For NA-AntiLeftOuterSemiJoin, we couldn't match null-bucket first, because once y set has a same key x and null
//	       key, we should return the result as left side row appended with a scalar value 0 which is from same key matching failure.
func (w *probeWorker) joinNAAJMatchProbeSideRow2Chunk(probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *hashContext, joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	naAntiSemiJoin := w.hashJoinCtx.joinType == plannercore.AntiSemiJoin && w.hashJoinCtx.isNullAware
	naAntiLeftOuterSemiJoin := w.hashJoinCtx.joinType == plannercore.AntiLeftOuterSemiJoin && w.hashJoinCtx.isNullAware
	if naAntiSemiJoin {
		return w.joinNAASJMatchProbeSideRow2Chunk(probeKey, probeKeyNullBits, probeSideRow, hCtx, joinResult)
	}
	if naAntiLeftOuterSemiJoin {
		return w.joinNAALOSJMatchProbeSideRow2Chunk(probeKey, probeKeyNullBits, probeSideRow, hCtx, joinResult)
	}
	// shouldn't be here, not a valid NAAJ.
	return false, 0, joinResult
}

func (w *probeWorker) joinMatchedProbeSideRow2Chunk(probeKey uint64, probeSideRow chunk.Row, hCtx *hashContext,
	joinResult *hashjoinWorkerResult) (bool, int64, *hashjoinWorkerResult) {
	var err error
	waitTime := int64(0)
	oneWaitTime := int64(0)
	var buildSideRows []chunk.Row
	if w.joiner.isSemiJoinWithoutCondition() {
		var rowPtr *chunk.Row
		rowPtr, err = w.rowContainerForProbe.GetOneMatchedRow(probeKey, probeSideRow, hCtx)
		if rowPtr != nil {
			buildSideRows = append(buildSideRows, *rowPtr)
		}
	} else {
		w.buildSideRows, err = w.rowContainerForProbe.GetMatchedRows(probeKey, probeSideRow, hCtx, w.buildSideRows)
		buildSideRows = w.buildSideRows
	}

	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}
	if len(buildSideRows) == 0 {
		w.joiner.onMissMatch(false, probeSideRow, joinResult.chk)
		return true, waitTime, joinResult
	}
	iter := w.rowIters
	iter.Reset(buildSideRows)
	hasMatch, hasNull, ok := false, false, false
	for iter.Begin(); iter.Current() != iter.End(); {
		matched, isNull, err := w.joiner.tryToMatchInners(probeSideRow, iter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	if !hasMatch {
		w.joiner.onMissMatch(hasNull, probeSideRow, joinResult.chk)
	}
	return true, waitTime, joinResult
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

func (w *probeWorker) join2Chunk(probeSideChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinWorkerResult,
	selected []bool) (ok bool, waitTime int64, _ *hashjoinWorkerResult) {
	var err error
	waitTime = 0
	oneWaitTime := int64(0)
	selected, err = expression.VectorizedFilter(w.hashJoinCtx.sessCtx.GetExprCtx().GetEvalCtx(), w.hashJoinCtx.sessCtx.GetSessionVars().EnableVectorizedExpression, w.hashJoinCtx.outerFilter, chunk.NewIterator4Chunk(probeSideChk), selected)
	if err != nil {
		joinResult.err = err
		return false, waitTime, joinResult
	}

	numRows := probeSideChk.NumRows()
	hCtx.initHash(numRows)
	// By now, path 1 and 2 won't be conducted at the same time.
	// 1: write the row data of join key to hashVals. (normal EQ key should ignore the null values.) null-EQ for Except statement is an exception.
	for keyIdx, i := range hCtx.keyColIdx {
		ignoreNull := len(w.hashJoinCtx.isNullEQ) > keyIdx && w.hashJoinCtx.isNullEQ[keyIdx]
		err = codec.HashChunkSelected(w.rowContainerForProbe.sc.TypeCtx(), hCtx.hashVals, probeSideChk, hCtx.allTypes[keyIdx], i, hCtx.buf, hCtx.hasNull, selected, ignoreNull)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
	}
	// 2: write the row data of NA join key to hashVals. (NA EQ key should collect all row including null value, store null value in a special position)
	isNAAJ := len(hCtx.naKeyColIdx) > 0
	for keyIdx, i := range hCtx.naKeyColIdx {
		// NAAJ won't ignore any null values, but collect them up to probe.
		err = codec.HashChunkSelected(w.rowContainerForProbe.sc.TypeCtx(), hCtx.hashVals, probeSideChk, hCtx.allTypes[keyIdx], i, hCtx.buf, hCtx.hasNull, selected, false)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		// after fetch one NA column, collect the null value to null bitmap for every row. (use hasNull flag to accelerate)
		// eg: if a NA Join cols is (a, b, c), for every build row here we maintained a 3-bit map to mark which column is null for them.
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if hCtx.hasNull[rowIdx] {
				hCtx.naColNullBitMap[rowIdx].UnsafeSet(keyIdx)
				// clean and try fetch next NA join col.
				hCtx.hasNull[rowIdx] = false
				hCtx.naHasNull[rowIdx] = true
			}
		}
	}

	for i := range selected {
		err := w.hashJoinCtx.sessCtx.GetSessionVars().SQLKiller.HandleSignal()
		failpoint.Inject("killedInJoin2Chunk", func(val failpoint.Value) {
			if val.(bool) {
				err = exeerrors.ErrQueryInterrupted
			}
		})
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		if isNAAJ {
			if !selected[i] {
				// since this is the case of using inner to build, so for an outer row unselected, we should fill the result when it's outer join.
				w.joiner.onMissMatch(false, probeSideChk.GetRow(i), joinResult.chk)
			}
			if hCtx.naHasNull[i] {
				// here means the probe join connecting column has null value in it and this is special for matching all the hash buckets
				// for it. (probeKey is not necessary here)
				probeRow := probeSideChk.GetRow(i)
				ok, oneWaitTime, joinResult = w.joinNAAJMatchProbeSideRow2Chunk(0, hCtx.naColNullBitMap[i].Clone(), probeRow, hCtx, joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			} else {
				// here means the probe join connecting column without null values, where we should match same key bucket and null bucket for it at its order.
				// step1: process same key matched probe side rows
				probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
				ok, oneWaitTime, joinResult = w.joinNAAJMatchProbeSideRow2Chunk(probeKey, nil, probeRow, hCtx, joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		} else {
			// since this is the case of using inner to build, so for an outer row unselected, we should fill the result when it's outer join.
			if !selected[i] || hCtx.hasNull[i] { // process unmatched probe side rows
				w.joiner.onMissMatch(false, probeSideChk.GetRow(i), joinResult.chk)
			} else { // process matched probe side rows
				probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
				ok, oneWaitTime, joinResult = w.joinMatchedProbeSideRow2Chunk(probeKey, probeRow, hCtx, joinResult)
				waitTime += oneWaitTime
				if !ok {
					return false, waitTime, joinResult
				}
			}
		}
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	return true, waitTime, joinResult
}

func (w *probeWorker) sendingResult(joinResult *hashjoinWorkerResult) (ok bool, cost int64, newJoinResult *hashjoinWorkerResult) {
	start := time.Now()
	w.hashJoinCtx.joinResultCh <- joinResult
	ok, newJoinResult = w.getNewJoinResult()
	cost = int64(time.Since(start))
	return ok, cost, newJoinResult
}

// join2ChunkForOuterHashJoin joins chunks when using the outer to build a hash table (refer to outer hash join)
func (w *probeWorker) join2ChunkForOuterHashJoin(probeSideChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinWorkerResult) (ok bool, waitTime int64, _ *hashjoinWorkerResult) {
	waitTime = 0
	oneWaitTime := int64(0)
	hCtx.initHash(probeSideChk.NumRows())
	for keyIdx, i := range hCtx.keyColIdx {
		err := codec.HashChunkColumns(w.rowContainerForProbe.sc.TypeCtx(), hCtx.hashVals, probeSideChk, hCtx.allTypes[keyIdx], i, hCtx.buf, hCtx.hasNull)
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
	}
	for i := 0; i < probeSideChk.NumRows(); i++ {
		err := w.hashJoinCtx.sessCtx.GetSessionVars().SQLKiller.HandleSignal()
		failpoint.Inject("killedInJoin2ChunkForOuterHashJoin", func(val failpoint.Value) {
			if val.(bool) {
				err = exeerrors.ErrQueryInterrupted
			}
		})
		if err != nil {
			joinResult.err = err
			return false, waitTime, joinResult
		}
		probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
		ok, oneWaitTime, joinResult = w.joinMatchedProbeSideRow2ChunkForOuterHashJoin(probeKey, probeRow, hCtx, joinResult)
		waitTime += oneWaitTime
		if !ok {
			return false, waitTime, joinResult
		}
		if joinResult.chk.IsFull() {
			ok, oneWaitTime, joinResult = w.sendingResult(joinResult)
			waitTime += oneWaitTime
			if !ok {
				return false, waitTime, joinResult
			}
		}
	}
	return true, waitTime, joinResult
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash table;
// step 2. fetch data from probe child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.buildFinished = make(chan error, 1)
		hCtx := &hashContext{
			allTypes:    e.buildTypes,
			keyColIdx:   e.buildWorker.buildKeyColIdx,
			naKeyColIdx: e.buildWorker.buildNAKeyColIdx,
		}
		e.rowContainer = newHashRowContainer(e.Ctx(), hCtx, exec.RetTypes(e.buildWorker.buildSideExec))
		// we shallow copies rowContainer for each probe worker to avoid lock contention
		for i := uint(0); i < e.concurrency; i++ {
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
	return nil
}

func (e *HashJoinExec) handleFetchAndBuildHashTablePanic(r any) {
	if r != nil {
		e.buildFinished <- util.GetRecoverError(r)
	}
	close(e.buildFinished)
}

func (e *HashJoinExec) fetchAndBuildHashTable(ctx context.Context) {
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
				selected, err = expression.VectorizedFilter(w.hashJoinCtx.sessCtx.GetExprCtx().GetEvalCtx(), w.hashJoinCtx.sessCtx.GetSessionVars().EnableVectorizedExpression, w.hashJoinCtx.outerFilter, chunk.NewIterator4Chunk(chk), selected)
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
	return nil
}

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	exec.BaseExecutor

	ctx         sessionctx.Context
	innerRows   []chunk.Row
	cursor      int
	innerExec   exec.Executor
	outerExec   exec.Executor
	innerFilter expression.CNFExprs
	outerFilter expression.CNFExprs

	joiner joiner

	cache              *applycache.ApplyCache
	canUseCache        bool
	cacheHitCounter    int
	cacheAccessCounter int

	outerSchema []*expression.CorrelatedColumn

	outerChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	innerList        *chunk.List
	innerChunk       *chunk.Chunk
	innerSelected    []bool
	innerIter        chunk.Iterator
	outerRow         *chunk.Row
	hasMatch         bool
	hasNull          bool

	outer bool

	memTracker *memory.Tracker // track memory usage.
}

// Close implements the Executor interface.
func (e *NestedLoopApplyExec) Close() error {
	e.innerRows = nil
	e.memTracker = nil
	if e.RuntimeStats() != nil {
		runtimeStats := newJoinRuntimeStats()
		if e.canUseCache {
			var hitRatio float64
			if e.cacheAccessCounter > 0 {
				hitRatio = float64(e.cacheHitCounter) / float64(e.cacheAccessCounter)
			}
			runtimeStats.setCacheInfo(true, hitRatio)
		} else {
			runtimeStats.setCacheInfo(false, 0)
		}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", 0))
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), runtimeStats)
	}
	return exec.Close(e.outerExec)
}

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := exec.Open(ctx, e.outerExec)
	if err != nil {
		return err
	}
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.outerChunk = exec.TryNewCacheChunk(e.outerExec)
	e.innerChunk = exec.TryNewCacheChunk(e.innerExec)
	e.innerList = chunk.NewList(exec.RetTypes(e.innerExec), e.InitCap(), e.MaxChunkSize())

	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	e.innerList.GetMemTracker().SetLabel(memory.LabelForInnerList)
	e.innerList.GetMemTracker().AttachTo(e.memTracker)

	if e.canUseCache {
		e.cache, err = applycache.NewApplyCache(e.ctx)
		if err != nil {
			return err
		}
		e.cacheHitCounter = 0
		e.cacheAccessCounter = 0
		e.cache.GetMemTracker().AttachTo(e.memTracker)
	}
	return nil
}

// aggExecutorTreeInputEmpty checks whether the executor tree returns empty if without aggregate operators.
// Note that, the prerequisite is that this executor tree has been executed already and it returns one row.
func aggExecutorTreeInputEmpty(e exec.Executor) bool {
	children := e.AllChildren()
	if len(children) == 0 {
		return false
	}
	if len(children) > 1 {
		_, ok := e.(*UnionExec)
		if !ok {
			// It is a Join executor.
			return false
		}
		for _, child := range children {
			if !aggExecutorTreeInputEmpty(child) {
				return false
			}
		}
		return true
	}
	// Single child executors.
	if aggExecutorTreeInputEmpty(children[0]) {
		return true
	}
	if hashAgg, ok := e.(*aggregate.HashAggExec); ok {
		return hashAgg.IsChildReturnEmpty
	}
	if streamAgg, ok := e.(*aggregate.StreamAggExec); ok {
		return streamAgg.IsChildReturnEmpty
	}
	return false
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(ctx context.Context, chk *chunk.Chunk) (*chunk.Row, error) {
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		if e.outerChunkCursor >= e.outerChunk.NumRows() {
			err := exec.Next(ctx, e.outerExec, e.outerChunk)
			if err != nil {
				return nil, err
			}
			if e.outerChunk.NumRows() == 0 {
				return nil, nil
			}
			e.outerSelected, err = expression.VectorizedFilter(e.ctx.GetExprCtx().GetEvalCtx(), e.ctx.GetSessionVars().EnableVectorizedExpression, e.outerFilter, outerIter, e.outerSelected)
			if err != nil {
				return nil, err
			}
			// For cases like `select count(1), (select count(1) from s where s.a > t.a) as sub from t where t.a = 1`,
			// if outer child has no row satisfying `t.a = 1`, `sub` should be `null` instead of `0` theoretically; however, the
			// outer `count(1)` produces one row <0, null> over the empty input, we should specially mark this outer row
			// as not selected, to trigger the mismatch join procedure.
			if e.outerChunkCursor == 0 && e.outerChunk.NumRows() == 1 && e.outerSelected[0] && aggExecutorTreeInputEmpty(e.outerExec) {
				e.outerSelected[0] = false
			}
			e.outerChunkCursor = 0
		}
		outerRow := e.outerChunk.GetRow(e.outerChunkCursor)
		selected := e.outerSelected[e.outerChunkCursor]
		e.outerChunkCursor++
		if selected {
			return &outerRow, nil
		} else if e.outer {
			e.joiner.onMissMatch(false, outerRow, chk)
			if chk.IsFull() {
				return nil, nil
			}
		}
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(ctx context.Context) error {
	err := exec.Open(ctx, e.innerExec)
	defer func() { terror.Log(exec.Close(e.innerExec)) }()
	if err != nil {
		return err
	}

	if e.canUseCache {
		// create a new one since it may be in the cache
		e.innerList = chunk.NewListWithMemTracker(exec.RetTypes(e.innerExec), e.InitCap(), e.MaxChunkSize(), e.innerList.GetMemTracker())
	} else {
		e.innerList.Reset()
	}
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := exec.Next(ctx, e.innerExec, e.innerChunk)
		if err != nil {
			return err
		}
		if e.innerChunk.NumRows() == 0 {
			return nil
		}

		e.innerSelected, err = expression.VectorizedFilter(e.ctx.GetExprCtx().GetEvalCtx(), e.ctx.GetSessionVars().EnableVectorizedExpression, e.innerFilter, innerIter, e.innerSelected)
		if err != nil {
			return err
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[row.Idx()] {
				e.innerList.AppendRow(row)
			}
		}
	}
}

// Next implements the Executor interface.
func (e *NestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			if e.outerRow != nil && !e.hasMatch {
				e.joiner.onMissMatch(e.hasNull, *e.outerRow, req)
			}
			e.outerRow, err = e.fetchSelectedOuterRow(ctx, req)
			if e.outerRow == nil || err != nil {
				return err
			}
			e.hasMatch = false
			e.hasNull = false

			if e.canUseCache {
				var key []byte
				for _, col := range e.outerSchema {
					*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
					key, err = codec.EncodeKey(e.Ctx().GetSessionVars().StmtCtx.TimeZone(), key, *col.Data)
					err = e.Ctx().GetSessionVars().StmtCtx.HandleError(err)
					if err != nil {
						return err
					}
				}
				e.cacheAccessCounter++
				value, err := e.cache.Get(key)
				if err != nil {
					return err
				}
				if value != nil {
					e.innerList = value
					e.cacheHitCounter++
				} else {
					err = e.fetchAllInners(ctx)
					if err != nil {
						return err
					}
					if _, err := e.cache.Set(key, e.innerList); err != nil {
						return err
					}
				}
			} else {
				for _, col := range e.outerSchema {
					*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
				}
				err = e.fetchAllInners(ctx)
				if err != nil {
					return err
				}
			}
			e.innerIter = chunk.NewIterator4List(e.innerList)
			e.innerIter.Begin()
		}

		matched, isNull, err := e.joiner.tryToMatchInners(*e.outerRow, e.innerIter, req)
		e.hasMatch = e.hasMatch || matched
		e.hasNull = e.hasNull || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
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
		// fetch time is the time wait fetch result from its child executor,
		// wait time is the time wait its parent executor to fetch the joined result
		buf.WriteString(", fetch and wait:")
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

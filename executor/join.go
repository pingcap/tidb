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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/bitmap"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/memory"
)

var (
	_ Executor = &HashJoinExec{}
	_ Executor = &NestedLoopApplyExec{}
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	baseExecutor

	probeSideExec     Executor
	buildSideExec     Executor
	buildSideEstCount float64
	outerFilter       expression.CNFExprs
	probeKeys         []*expression.Column
	probeNAKeys       []*expression.Column
	buildKeys         []*expression.Column
	buildNAKeys       []*expression.Column
	isNullEQ          []bool
	probeTypes        []*types.FieldType
	buildTypes        []*types.FieldType

	// concurrency is the number of partition, build and join workers.
	concurrency   uint
	rowContainer  *hashRowContainer
	buildFinished chan error

	// closeCh add a lock for closing executor.
	closeCh      chan struct{}
	joinType     plannercore.JoinType
	requiredRows int64

	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner

	probeChkResourceCh   chan *probeChkResource
	probeResultChs       []chan *chunk.Chunk
	joinChkResourceCh    []chan *chunk.Chunk
	joinResultCh         chan *hashjoinWorkerResult
	rowContainerForProbe []*hashRowContainer

	memTracker  *memory.Tracker // track memory usage.
	diskTracker *disk.Tracker   // track disk usage.

	outerMatchedStatus []*bitmap.ConcurrentBitmap
	useOuterToBuild    bool

	prepared    bool
	isOuterJoin bool

	// joinWorkerWaitGroup is for sync multiple join workers.
	joinWorkerWaitGroup sync.WaitGroup
	finished            atomic.Value

	stats *hashJoinRuntimeStats

	// We pre-alloc and reuse the Rows and RowPtrs for each probe goroutine, to avoid allocation frequently
	buildSideRows    [][]chunk.Row
	buildSideRowPtrs [][]chunk.RowPtr

	// for every naaj probe worker,  pre-allocate the int slice for store the join column index to check.
	needCheckBuildRowPos [][]int
	needCheckProbeRowPos [][]int
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
			for range e.buildFinished {
			}
		}
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.probeChkResourceCh != nil {
			close(e.probeChkResourceCh)
			for range e.probeChkResourceCh {
			}
		}
		for i := range e.probeResultChs {
			for range e.probeResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
		}
		e.probeChkResourceCh = nil
		e.joinChkResourceCh = nil
		terror.Call(e.rowContainer.Close)
	}
	e.outerMatchedStatus = e.outerMatchedStatus[:0]
	e.buildSideRows = nil
	e.buildSideRowPtrs = nil
	e.needCheckBuildRowPos = nil
	e.needCheckProbeRowPos = nil
	if e.stats != nil && e.rowContainer != nil {
		e.stats.hashStat = *e.rowContainer.stat
	}
	err := e.baseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.diskTracker = disk.NewTracker(e.id, -1)
	e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)

	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.joinWorkerWaitGroup = sync.WaitGroup{}

	if e.probeTypes == nil {
		e.probeTypes = retTypes(e.probeSideExec)
	}
	if e.buildTypes == nil {
		e.buildTypes = retTypes(e.buildSideExec)
	}
	if e.runtimeStats != nil {
		e.stats = &hashJoinRuntimeStats{
			concurrent: cap(e.joiners),
		}
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
	return nil
}

// fetchProbeSideChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchProbeSideChunks(ctx context.Context) {
	hasWaitedForBuild := false
	for {
		if e.finished.Load().(bool) {
			return
		}

		var probeSideResource *probeChkResource
		var ok bool
		select {
		case <-e.closeCh:
			return
		case probeSideResource, ok = <-e.probeChkResourceCh:
			if !ok {
				return
			}
		}
		probeSideResult := probeSideResource.chk
		if e.isOuterJoin {
			required := int(atomic.LoadInt64(&e.requiredRows))
			probeSideResult.SetRequiredRows(required, e.maxChunkSize)
		}
		err := Next(ctx, e.probeSideExec, probeSideResult)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{
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
			if probeSideResult.NumRows() == 0 && !e.useOuterToBuild {
				e.finished.Store(true)
			}
			emptyBuild, buildErr := e.wait4BuildSide()
			if buildErr != nil {
				e.joinResultCh <- &hashjoinWorkerResult{
					err: buildErr,
				}
				return
			} else if emptyBuild {
				return
			}
			// after building is finished. the hash null bucket slice is allocated and determined.
			// copy it for multi probe worker.
			for i := range e.rowContainerForProbe {
				e.rowContainerForProbe[i].hashNANullBucket = e.rowContainer.hashNANullBucket
			}
			hasWaitedForBuild = true
		}

		if probeSideResult.NumRows() == 0 {
			return
		}

		probeSideResource.dest <- probeSideResult
	}
}

func (e *HashJoinExec) wait4BuildSide() (emptyBuild bool, err error) {
	select {
	case <-e.closeCh:
		return true, nil
	case err := <-e.buildFinished:
		if err != nil {
			return false, err
		}
	}
	if e.rowContainer.Len() == uint64(0) && (e.joinType == plannercore.InnerJoin || e.joinType == plannercore.SemiJoin) {
		return true, nil
	}
	return false, nil
}

// fetchBuildSideRows fetches all rows from build side executor, and append them
// to e.buildSideResult.
func (e *HashJoinExec) fetchBuildSideRows(ctx context.Context, chkCh chan<- *chunk.Chunk, doneCh <-chan struct{}) {
	defer close(chkCh)
	var err error
	failpoint.Inject("issue30289", func(val failpoint.Value) {
		if val.(bool) {
			err = errors.Errorf("issue30289 build return error")
			e.buildFinished <- errors.Trace(err)
			return
		}
	})
	for {
		if e.finished.Load().(bool) {
			return
		}
		chk := chunk.NewChunkWithCapacity(e.buildSideExec.base().retFieldTypes, e.ctx.GetSessionVars().MaxChunkSize)
		err = Next(ctx, e.buildSideExec, chk)
		if err != nil {
			e.buildFinished <- errors.Trace(err)
			return
		}
		failpoint.Inject("errorFetchBuildSideRowsMockOOMPanic", nil)
		if chk.NumRows() == 0 {
			return
		}
		select {
		case <-doneCh:
			return
		case <-e.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

func (e *HashJoinExec) initializeForProbe() {
	// e.probeResultChs is for transmitting the chunks which store the data of
	// probeSideExec, it'll be written by probe side worker goroutine, and read by join
	// workers.
	e.probeResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.probeChkResourceCh is for transmitting the used probeSideExec chunks from
	// join workers to probeSideExec worker.
	e.probeChkResourceCh = make(chan *probeChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeChkResourceCh <- &probeChkResource{
			chk:  newFirstChunk(e.probeSideExec),
			dest: e.probeResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)

	e.buildSideRows = make([][]chunk.Row, e.concurrency)
	e.buildSideRowPtrs = make([][]chunk.RowPtr, e.concurrency)
	e.needCheckBuildRowPos = make([][]int, e.concurrency)
	e.needCheckProbeRowPos = make([][]int, e.concurrency)
}

func (e *HashJoinExec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.joinWorkerWaitGroup.Add(1)
	go util.WithRecovery(func() {
		defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
		e.fetchProbeSideChunks(ctx)
	}, e.handleProbeSideFetcherPanic)

	probeKeyColIdx := make([]int, len(e.probeKeys))
	probeNAKeColIdx := make([]int, len(e.probeNAKeys))
	for i := range e.probeKeys {
		probeKeyColIdx[i] = e.probeKeys[i].Index
	}
	for i := range e.probeNAKeys {
		probeNAKeColIdx[i] = e.probeNAKeys[i].Index
	}

	for i := uint(0); i < e.concurrency; i++ {
		e.joinWorkerWaitGroup.Add(1)
		workID := i
		go util.WithRecovery(func() {
			defer trace.StartRegion(ctx, "HashJoinWorker").End()
			e.runJoinWorker(workID, probeKeyColIdx, probeNAKeColIdx)
		}, e.handleJoinWorkerPanic)
	}
	go util.WithRecovery(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (e *HashJoinExec) handleProbeSideFetcherPanic(r interface{}) {
	for i := range e.probeResultChs {
		close(e.probeResultChs[i])
	}
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) handleJoinWorkerPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

// Concurrently handling unmatched rows from the hash table
func (e *HashJoinExec) handleUnmatchedRowsFromHashTable(workerID uint) {
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}
	numChks := e.rowContainer.NumChunks()
	for i := int(workerID); i < numChks; i += int(e.concurrency) {
		chk, err := e.rowContainer.GetChunk(i)
		if err != nil {
			// Catching the error and send it
			joinResult.err = err
			e.joinResultCh <- joinResult
			return
		}
		for j := 0; j < chk.NumRows(); j++ {
			if !e.outerMatchedStatus[i].UnsafeIsSet(j) { // process unmatched outer rows
				e.joiners[workerID].onMissMatch(false, chk.GetRow(j), joinResult.chk)
			}
			if joinResult.chk.IsFull() {
				e.joinResultCh <- joinResult
				ok, joinResult = e.getNewJoinResult(workerID)
				if !ok {
					return
				}
			}
		}
	}

	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.joinResultCh <- joinResult
	}
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.joinWorkerWaitGroup.Wait()
	if e.useOuterToBuild {
		// Concurrently handling unmatched rows from the hash table at the tail
		for i := uint(0); i < e.concurrency; i++ {
			var workerID = i
			e.joinWorkerWaitGroup.Add(1)
			go util.WithRecovery(func() { e.handleUnmatchedRowsFromHashTable(workerID) }, e.handleJoinWorkerPanic)
		}
		e.joinWorkerWaitGroup.Wait()
	}
	close(e.joinResultCh)
}

func (e *HashJoinExec) runJoinWorker(workerID uint, probeKeyColIdx, probeNAKeyColIdx []int) {
	probeTime := int64(0)
	if e.stats != nil {
		start := time.Now()
		defer func() {
			t := time.Since(start)
			atomic.AddInt64(&e.stats.probe, probeTime)
			atomic.AddInt64(&e.stats.fetchAndProbe, int64(t))
			e.stats.setMaxFetchAndProbeTime(int64(t))
		}()
	}

	var (
		probeSideResult *chunk.Chunk
		selected        = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: e.probeResultChs[workerID],
	}
	hCtx := &hashContext{
		allTypes:    e.probeTypes,
		keyColIdx:   probeKeyColIdx,
		naKeyColIdx: probeNAKeyColIdx,
	}
	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case probeSideResult, ok = <-e.probeResultChs[workerID]:
		}
		if !ok {
			break
		}
		start := time.Now()
		if e.useOuterToBuild {
			ok, joinResult = e.join2ChunkForOuterHashJoin(workerID, probeSideResult, hCtx, e.rowContainerForProbe[workerID], joinResult)
		} else {
			ok, joinResult = e.join2Chunk(workerID, probeSideResult, hCtx, e.rowContainerForProbe[workerID], joinResult, selected)
		}
		probeTime += int64(time.Since(start))
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult
		e.probeChkResourceCh <- emptyProbeSideResult
	}
	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		e.joinChkResourceCh[workerID] <- joinResult.chk
	}
}

func (e *HashJoinExec) joinMatchedProbeSideRow2ChunkForOuterHashJoin(workerID uint, probeKey uint64, probeSideRow chunk.Row, hCtx *hashContext, rowContainer *hashRowContainer, joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	var err error
	e.buildSideRows[workerID], e.buildSideRowPtrs[workerID], err = rowContainer.GetMatchedRowsAndPtrs(probeKey, probeSideRow, hCtx, e.buildSideRows[workerID], e.buildSideRowPtrs[workerID], true)
	buildSideRows, rowsPtrs := e.buildSideRows[workerID], e.buildSideRowPtrs[workerID]
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) == 0 {
		return true, joinResult
	}

	iter := chunk.NewIterator4Slice(buildSideRows)
	defer chunk.FreeIterator(iter)
	var outerMatchStatus []outerRowStatusFlag
	rowIdx, ok := 0, false
	for iter.Begin(); iter.Current() != iter.End(); {
		outerMatchStatus, err = e.joiners[workerID].tryToMatchOuters(iter, probeSideRow, joinResult.chk, outerMatchStatus)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		for i := range outerMatchStatus {
			if outerMatchStatus[i] == outerRowMatched {
				e.outerMatchedStatus[rowsPtrs[rowIdx+i].ChkIdx].Set(int(rowsPtrs[rowIdx+i].RowIdx))
			}
		}
		rowIdx += len(outerMatchStatus)
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// joinNAALOSJMatchProbeSideRow2Chunk implement the matching logic for NA-AntiLeftOuterSemiJoin
func (e *HashJoinExec) joinNAALOSJMatchProbeSideRow2Chunk(workerID uint, probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *hashContext,
	rowContainer *hashRowContainer, joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	var (
		err error
		ok  bool
	)
	if probeKeyNullBits == nil {
		// step1: match the same key bucket first.
		// because AntiLeftOuterSemiJoin cares about the scalar value. If we both have a match from null
		// bucket and same key bucket, we should return the result as <rhs-row, 0> from same-key bucket
		// rather than <rhs-row, null> from null bucket.
		e.buildSideRows[workerID], err = rowContainer.GetMatchedRows(probeKey, probeSideRow, hCtx, e.buildSideRows[workerID])
		buildSideRows := e.buildSideRows[workerID]
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		if len(buildSideRows) != 0 {
			iter1 := chunk.NewIterator4Slice(buildSideRows)
			defer chunk.FreeIterator(iter1)
			for iter1.Begin(); iter1.Current() != iter1.End(); {
				matched, _, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter1, joinResult.chk, LeftNotNullRightNotNull)
				if err != nil {
					joinResult.err = err
					return false, joinResult
				}
				// here matched means: there is a valid same-key bucket row from right side.
				// as said in the comment, once we meet a same key (NOT IN semantic) in CNF, we can determine the result as <rhs, 0>.
				if matched {
					return true, joinResult
				}
				if joinResult.chk.IsFull() {
					e.joinResultCh <- joinResult
					ok, joinResult = e.getNewJoinResult(workerID)
					if !ok {
						return false, joinResult
					}
				}
			}
		}
		// step2: match the null bucket secondly.
		e.buildSideRows[workerID], err = rowContainer.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, e.buildSideRows[workerID], e.needCheckBuildRowPos[workerID], e.needCheckProbeRowPos[workerID])
		buildSideRows = e.buildSideRows[workerID]
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		if len(buildSideRows) == 0 {
			// when reach here, it means we couldn't find a valid same key match from same-key bucket yet
			// and the null bucket is empty. so the result should be <rhs, 1>.
			e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
			return true, joinResult
		}
		iter2 := chunk.NewIterator4Slice(buildSideRows)
		defer chunk.FreeIterator(iter2)
		for iter2.Begin(); iter2.Current() != iter2.End(); {
			matched, _, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter2, joinResult.chk, LeftNotNullRightHasNull)
			if err != nil {
				joinResult.err = err
				return false, joinResult
			}
			// here matched means: there is a valid null bucket row from right side.
			// as said in the comment, once we meet a null in CNF, we can determine the result as <rhs, null>.
			if matched {
				return true, joinResult
			}
			if joinResult.chk.IsFull() {
				e.joinResultCh <- joinResult
				ok, joinResult = e.getNewJoinResult(workerID)
				if !ok {
					return false, joinResult
				}
			}
		}
		// step3: if we couldn't return it quickly in null bucket and same key bucket, here means two cases:
		// case1: x NOT IN (empty set): if other key bucket don't have the valid rows yet.
		// case2: x NOT IN (l,m,n...): if other key bucket do have the valid rows.
		// both cases mean the result should be <rhs, 1>
		e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
		return true, joinResult
	}
	// when left side has null values, all we want is to find a valid build side rows (past other condition)
	// so we can return it as soon as possible. here means two cases:
	// case1: <?, null> NOT IN (empty set):             ----------------------> result is <rhs, 1>.
	// case2: <?, null> NOT IN (at least a valid inner row) ------------------> result is <rhs, null>.
	// Step1: match null bucket (assumption that null bucket is quite smaller than all hash table bucket rows)
	e.buildSideRows[workerID], err = rowContainer.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, e.buildSideRows[workerID], e.needCheckBuildRowPos[workerID], e.needCheckProbeRowPos[workerID])
	buildSideRows := e.buildSideRows[workerID]
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) != 0 {
		iter1 := chunk.NewIterator4Slice(buildSideRows)
		defer chunk.FreeIterator(iter1)
		for iter1.Begin(); iter1.Current() != iter1.End(); {
			matched, _, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter1, joinResult.chk, LeftHasNullRightHasNull)
			if err != nil {
				joinResult.err = err
				return false, joinResult
			}
			// here matched means: there is a valid null bucket row from right side. (not empty)
			// as said in the comment, once we found at least a valid row, we can determine the result as <rhs, null>.
			if matched {
				return true, joinResult
			}
			if joinResult.chk.IsFull() {
				e.joinResultCh <- joinResult
				ok, joinResult = e.getNewJoinResult(workerID)
				if !ok {
					return false, joinResult
				}
			}
		}
	}
	// Step2: match all hash table bucket build rows (use probeKeyNullBits to filter if any).
	e.buildSideRows[workerID], err = rowContainer.GetAllMatchedRows(hCtx, probeSideRow, probeKeyNullBits, e.buildSideRows[workerID], e.needCheckBuildRowPos[workerID], e.needCheckProbeRowPos[workerID])
	buildSideRows = e.buildSideRows[workerID]
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) == 0 {
		// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
		// which means x NOT IN (empty set) or x NOT IN (l,m,n), the result should be <rhs, 1>
		e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
		return true, joinResult
	}
	iter2 := chunk.NewIterator4Slice(buildSideRows)
	defer chunk.FreeIterator(iter2)
	for iter2.Begin(); iter2.Current() != iter2.End(); {
		matched, _, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter2, joinResult.chk, LeftHasNullRightNotNull)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		// here matched means: there is a valid same key bucket row from right side. (not empty)
		// as said in the comment, once we found at least a valid row, we can determine the result as <rhs, null>.
		if matched {
			return true, joinResult
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	// step3: if we couldn't return it quickly in null bucket and all hash bucket, here means only one cases:
	// case1: <?, null> NOT IN (empty set):
	// empty set comes from no rows from all bucket can pass other condition. the result should be <rhs, 1>
	e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
	return true, joinResult
}

// joinNAASJMatchProbeSideRow2Chunk implement the matching logic for NA-AntiSemiJoin
func (e *HashJoinExec) joinNAASJMatchProbeSideRow2Chunk(workerID uint, probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *hashContext,
	rowContainer *hashRowContainer, joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	var (
		err error
		ok  bool
	)
	if probeKeyNullBits == nil {
		// step1: match null bucket first.
		// need fetch the "valid" rows every time. (nullBits map check is necessary)
		e.buildSideRows[workerID], err = rowContainer.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, e.buildSideRows[workerID], e.needCheckBuildRowPos[workerID], e.needCheckProbeRowPos[workerID])
		buildSideRows := e.buildSideRows[workerID]
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		if len(buildSideRows) != 0 {
			iter1 := chunk.NewIterator4Slice(buildSideRows)
			defer chunk.FreeIterator(iter1)
			for iter1.Begin(); iter1.Current() != iter1.End(); {
				matched, _, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter1, joinResult.chk)
				if err != nil {
					joinResult.err = err
					return false, joinResult
				}
				// here matched means: there is a valid null bucket row from right side.
				// as said in the comment, once we meet a rhs null in CNF, we can determine the reject of lhs row.
				if matched {
					return true, joinResult
				}
				if joinResult.chk.IsFull() {
					e.joinResultCh <- joinResult
					ok, joinResult = e.getNewJoinResult(workerID)
					if !ok {
						return false, joinResult
					}
				}
			}
		}
		// step2: then same key bucket.
		e.buildSideRows[workerID], err = rowContainer.GetMatchedRows(probeKey, probeSideRow, hCtx, e.buildSideRows[workerID])
		buildSideRows = e.buildSideRows[workerID]
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		if len(buildSideRows) == 0 {
			// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
			// which means x NOT IN (empty set), accept the rhs row.
			e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
			return true, joinResult
		}
		iter2 := chunk.NewIterator4Slice(buildSideRows)
		defer chunk.FreeIterator(iter2)
		for iter2.Begin(); iter2.Current() != iter2.End(); {
			matched, _, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter2, joinResult.chk)
			if err != nil {
				joinResult.err = err
				return false, joinResult
			}
			// here matched means: there is a valid same key bucket row from right side.
			// as said in the comment, once we meet a false in CNF, we can determine the reject of lhs row.
			if matched {
				return true, joinResult
			}
			if joinResult.chk.IsFull() {
				e.joinResultCh <- joinResult
				ok, joinResult = e.getNewJoinResult(workerID)
				if !ok {
					return false, joinResult
				}
			}
		}
		// step3: if we couldn't return it quickly in null bucket and same key bucket, here means two cases:
		// case1: x NOT IN (empty set): if other key bucket don't have the valid rows yet.
		// case2: x NOT IN (l,m,n...): if other key bucket do have the valid rows.
		// both cases should accept the rhs row.
		e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
		return true, joinResult
	}
	// when left side has null values, all we want is to find a valid build side rows (passed from other condition)
	// so we can return it as soon as possible. here means two cases:
	// case1: <?, null> NOT IN (empty set):             ----------------------> accept rhs row.
	// case2: <?, null> NOT IN (at least a valid inner row) ------------------> unknown result, refuse rhs row.
	// Step1: match null bucket (assumption that null bucket is quite smaller than all hash table bucket rows)
	e.buildSideRows[workerID], err = rowContainer.GetNullBucketRows(hCtx, probeSideRow, probeKeyNullBits, e.buildSideRows[workerID], e.needCheckBuildRowPos[workerID], e.needCheckProbeRowPos[workerID])
	buildSideRows := e.buildSideRows[workerID]
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) != 0 {
		iter1 := chunk.NewIterator4Slice(buildSideRows)
		defer chunk.FreeIterator(iter1)
		for iter1.Begin(); iter1.Current() != iter1.End(); {
			matched, _, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter1, joinResult.chk)
			if err != nil {
				joinResult.err = err
				return false, joinResult
			}
			// here matched means: there is a valid null bucket row from right side. (not empty)
			// as said in the comment, once we found at least a valid row, we can determine the reject of lhs row.
			if matched {
				return true, joinResult
			}
			if joinResult.chk.IsFull() {
				e.joinResultCh <- joinResult
				ok, joinResult = e.getNewJoinResult(workerID)
				if !ok {
					return false, joinResult
				}
			}
		}
	}
	// Step2: match all hash table bucket build rows.
	e.buildSideRows[workerID], err = rowContainer.GetAllMatchedRows(hCtx, probeSideRow, probeKeyNullBits, e.buildSideRows[workerID], e.needCheckBuildRowPos[workerID], e.needCheckProbeRowPos[workerID])
	buildSideRows = e.buildSideRows[workerID]
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) == 0 {
		// when reach here, it means we couldn't return it quickly in null bucket, and same-bucket is empty,
		// which means <?,null> NOT IN (empty set) or <?,null> NOT IN (no valid rows) accept the rhs row.
		e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
		return true, joinResult
	}
	iter2 := chunk.NewIterator4Slice(buildSideRows)
	defer chunk.FreeIterator(iter2)
	for iter2.Begin(); iter2.Current() != iter2.End(); {
		matched, _, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter2, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		// here matched means: there is a valid key row from right side. (not empty)
		// as said in the comment, once we found at least a valid row, we can determine the reject of lhs row.
		if matched {
			return true, joinResult
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	// step3: if we couldn't return it quickly in null bucket and all hash bucket, here means only one cases:
	// case1: <?, null> NOT IN (empty set):
	// empty set comes from no rows from all bucket can pass other condition. we should accept the rhs row.
	e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
	return true, joinResult
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
//	       set has a same key x, the scalar value is 0; else if y set has a null key, then the scalar value is null. Since
//	       NA-AntiSemiJoin don't care about the scalar value, we just return the lhs row as quickly as possible, which means
//	       we can match the null bucket first, a null of yi in the CNF (x NA-EQ yi) can always determine a negative value
//	       (null) in advance here.
//
//	       For NA-AntiLeftOuterSemiJoin, we couldn't match null-bucket first, because once y set has a same key x and null
//	       key, we should return the result as left side row appended with a scalar value 0 which is from same key matching failure.
func (e *HashJoinExec) joinNAAJMatchProbeSideRow2Chunk(workerID uint, probeKey uint64, probeKeyNullBits *bitmap.ConcurrentBitmap, probeSideRow chunk.Row, hCtx *hashContext,
	rowContainer *hashRowContainer, joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	NAAntiSemiJoin := e.joinType == plannercore.AntiSemiJoin && len(e.buildNAKeys) > 0
	NAAntiLeftOuterSemiJoin := e.joinType == plannercore.AntiLeftOuterSemiJoin && len(e.buildNAKeys) > 0
	if NAAntiSemiJoin {
		return e.joinNAASJMatchProbeSideRow2Chunk(workerID, probeKey, probeKeyNullBits, probeSideRow, hCtx, rowContainer, joinResult)
	}
	if NAAntiLeftOuterSemiJoin {
		return e.joinNAALOSJMatchProbeSideRow2Chunk(workerID, probeKey, probeKeyNullBits, probeSideRow, hCtx, rowContainer, joinResult)
	}
	// shouldn't be here, not a valid NAAJ.
	return false, joinResult
}

func (e *HashJoinExec) joinMatchedProbeSideRow2Chunk(workerID uint, probeKey uint64, probeSideRow chunk.Row, hCtx *hashContext,
	rowContainer *hashRowContainer, joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	var err error
	e.buildSideRows[workerID], err = rowContainer.GetMatchedRows(probeKey, probeSideRow, hCtx, e.buildSideRows[workerID])
	buildSideRows := e.buildSideRows[workerID]
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideRows) == 0 {
		e.joiners[workerID].onMissMatch(false, probeSideRow, joinResult.chk)
		return true, joinResult
	}
	iter := chunk.NewIterator4Slice(buildSideRows)
	defer chunk.FreeIterator(iter)
	hasMatch, hasNull, ok := false, false, false
	for iter.Begin(); iter.Current() != iter.End(); {
		matched, isNull, err := e.joiners[workerID].tryToMatchInners(probeSideRow, iter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	if !hasMatch {
		e.joiners[workerID].onMissMatch(hasNull, probeSideRow, joinResult.chk)
	}
	return true, joinResult
}

func (e *HashJoinExec) getNewJoinResult(workerID uint) (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: e.joinChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[workerID]:
	}
	return ok, joinResult
}

func (e *HashJoinExec) join2Chunk(workerID uint, probeSideChk *chunk.Chunk, hCtx *hashContext, rowContainer *hashRowContainer, joinResult *hashjoinWorkerResult,
	selected []bool) (ok bool, _ *hashjoinWorkerResult) {
	var err error
	selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(probeSideChk), selected)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}

	numRows := probeSideChk.NumRows()
	hCtx.initHash(numRows)
	// By now, path 1 and 2 won't be conducted at the same time.
	// 1: write the row data of join key to hashVals. (normal EQ key should ignore the null values.) null-EQ for Except statement is an exception.
	for keyIdx, i := range hCtx.keyColIdx {
		ignoreNull := len(e.isNullEQ) > keyIdx && e.isNullEQ[keyIdx]
		err = codec.HashChunkSelected(rowContainer.sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[keyIdx], i, hCtx.buf, hCtx.hasNull, selected, ignoreNull)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
	}
	// 2: write the row data of NA join key to hashVals. (NA EQ key should collect all row including null value, store null value in a special position)
	isNAAJ := len(hCtx.naKeyColIdx) > 0
	for keyIdx, i := range hCtx.naKeyColIdx {
		// NAAJ won't ignore any null values, but collect them up to probe.
		err = codec.HashChunkSelected(rowContainer.sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[keyIdx], i, hCtx.buf, hCtx.hasNull, selected, false)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		// after fetch one NA column, collect the null value to null bitmap for every row. (use hasNull flag to accelerate)
		// eg: if a NA Join cols is (a, b, c), for every build row here we maintained a 3-bit map to mark which column is null for them.
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if hCtx.hasNull[rowIdx] {
				hCtx.naColNullBitMap[rowIdx].Set(keyIdx)
				// clean and try fetch next NA join col.
				hCtx.hasNull[rowIdx] = false
				hCtx.naHasNull[rowIdx] = true
			}
		}
	}

	for i := range selected {
		killed := atomic.LoadUint32(&e.ctx.GetSessionVars().Killed) == 1
		failpoint.Inject("killedInJoin2Chunk", func(val failpoint.Value) {
			if val.(bool) {
				killed = true
			}
		})
		if killed {
			joinResult.err = ErrQueryInterrupted
			return false, joinResult
		}
		if isNAAJ {
			if !selected[i] {
				// since this is the case of using inner to build, so for an outer row unselected, we should fill the result when it's outer join.
				e.joiners[workerID].onMissMatch(false, probeSideChk.GetRow(i), joinResult.chk)
			}
			if hCtx.naHasNull[i] {
				// here means the probe join connecting column has null value in it and this is special for matching all the hash buckets
				// for it. (probeKey is not necessary here)
				probeRow := probeSideChk.GetRow(i)
				ok, joinResult = e.joinNAAJMatchProbeSideRow2Chunk(workerID, 0, hCtx.naColNullBitMap[i].Clone(), probeRow, hCtx, rowContainer, joinResult)
				if !ok {
					return false, joinResult
				}
			} else {
				// here means the probe join connecting column without null values, where we should match same key bucket and null bucket for it at its order.
				probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
				ok, joinResult = e.joinNAAJMatchProbeSideRow2Chunk(workerID, probeKey, nil, probeRow, hCtx, rowContainer, joinResult)
				if !ok {
					return false, joinResult
				}
			}
		} else {
			// since this is the case of using inner to build, so for an outer row unselected, we should fill the result when it's outer join.
			if !selected[i] || hCtx.hasNull[i] { // process unmatched probe side rows
				e.joiners[workerID].onMissMatch(false, probeSideChk.GetRow(i), joinResult.chk)
			} else { // process matched probe side rows
				probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
				ok, joinResult = e.joinMatchedProbeSideRow2Chunk(workerID, probeKey, probeRow, hCtx, rowContainer, joinResult)
				if !ok {
					return false, joinResult
				}
			}
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// join2ChunkForOuterHashJoin joins chunks when using the outer to build a hash table (refer to outer hash join)
func (e *HashJoinExec) join2ChunkForOuterHashJoin(workerID uint, probeSideChk *chunk.Chunk, hCtx *hashContext, rowContainer *hashRowContainer, joinResult *hashjoinWorkerResult) (ok bool, _ *hashjoinWorkerResult) {
	hCtx.initHash(probeSideChk.NumRows())
	for keyIdx, i := range hCtx.keyColIdx {
		err := codec.HashChunkColumns(rowContainer.sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[keyIdx], i, hCtx.buf, hCtx.hasNull)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
	}
	for i := 0; i < probeSideChk.NumRows(); i++ {
		killed := atomic.LoadUint32(&e.ctx.GetSessionVars().Killed) == 1
		failpoint.Inject("killedInJoin2ChunkForOuterHashJoin", func(val failpoint.Value) {
			if val.(bool) {
				killed = true
			}
		})
		if killed {
			joinResult.err = ErrQueryInterrupted
			return false, joinResult
		}
		probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
		ok, joinResult = e.joinMatchedProbeSideRow2ChunkForOuterHashJoin(workerID, probeKey, probeRow, hCtx, rowContainer, joinResult)
		if !ok {
			return false, joinResult
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash table;
// step 2. fetch data from probe child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.buildFinished = make(chan error, 1)
		buildKeyColIdx := make([]int, len(e.buildKeys))
		for i := range e.buildKeys {
			buildKeyColIdx[i] = e.buildKeys[i].Index
		}
		buildNAKeyColIdx := make([]int, len(e.buildNAKeys))
		for i := range e.buildNAKeys {
			buildNAKeyColIdx[i] = e.buildNAKeys[i].Index
		}
		hCtx := &hashContext{
			allTypes:    e.buildTypes,
			keyColIdx:   buildKeyColIdx,
			naKeyColIdx: buildNAKeyColIdx,
		}
		e.rowContainer = newHashRowContainer(e.ctx, int(e.buildSideEstCount), hCtx, retTypes(e.buildSideExec))
		// we shallow copies rowContainer for each probe worker to avoid lock contention
		e.rowContainerForProbe = make([]*hashRowContainer, e.concurrency)
		for i := uint(0); i < e.concurrency; i++ {
			if i == 0 {
				e.rowContainerForProbe[i] = e.rowContainer
			} else {
				e.rowContainerForProbe[i] = e.rowContainer.ShallowCopy()
			}
		}
		go util.WithRecovery(func() {
			defer trace.StartRegion(ctx, "HashJoinHashTableBuilder").End()
			e.fetchAndBuildHashTable(ctx)
		}, e.handleFetchAndBuildHashTablePanic)
		e.fetchAndProbeHashTable(ctx)
		e.prepared = true
	}
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
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

func (e *HashJoinExec) handleFetchAndBuildHashTablePanic(r interface{}) {
	if r != nil {
		e.buildFinished <- errors.Errorf("%v", r)
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
	go util.WithRecovery(
		func() {
			defer trace.StartRegion(ctx, "HashJoinBuildSideFetcher").End()
			e.fetchBuildSideRows(ctx, buildSideResultCh, doneCh)
		},
		func(r interface{}) {
			if r != nil {
				fetchBuildSideRowsOk <- errors.Errorf("%v", r)
			}
			close(fetchBuildSideRowsOk)
		},
	)

	// TODO: Parallel build hash table. Currently not support because `unsafeHashTable` is not thread-safe.
	err := e.buildHashTableForList(buildSideResultCh)
	if err != nil {
		e.buildFinished <- errors.Trace(err)
		close(doneCh)
	}
	// Wait fetchBuildSideRows be finished.
	// 1. if buildHashTableForList fails
	// 2. if probeSideResult.NumRows() == 0, fetchProbeSideChunks will not wait for the build side.
	for range buildSideResultCh {
	}
	// Check whether err is nil to avoid sending redundant error into buildFinished.
	if err == nil {
		if err = <-fetchBuildSideRowsOk; err != nil {
			e.buildFinished <- err
		}
	}
}

// buildHashTableForList builds hash table from `list`.
func (e *HashJoinExec) buildHashTableForList(buildSideResultCh <-chan *chunk.Chunk) error {
	var err error
	var selected []bool
	e.rowContainer.GetMemTracker().AttachTo(e.memTracker)
	e.rowContainer.GetMemTracker().SetLabel(memory.LabelForBuildSideResult)
	e.rowContainer.GetDiskTracker().AttachTo(e.diskTracker)
	e.rowContainer.GetDiskTracker().SetLabel(memory.LabelForBuildSideResult)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		actionSpill := e.rowContainer.ActionSpill()
		failpoint.Inject("testRowContainerSpill", func(val failpoint.Value) {
			if val.(bool) {
				actionSpill = e.rowContainer.rowContainer.ActionSpillForTest()
				defer actionSpill.(*chunk.SpillDiskAction).WaitForTest()
			}
		})
		e.ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}
	for chk := range buildSideResultCh {
		if e.finished.Load().(bool) {
			return nil
		}
		if !e.useOuterToBuild {
			err = e.rowContainer.PutChunk(chk, e.isNullEQ)
		} else {
			var bitMap = bitmap.NewConcurrentBitmap(chk.NumRows())
			e.outerMatchedStatus = append(e.outerMatchedStatus, bitMap)
			e.memTracker.Consume(bitMap.BytesConsumed())
			if len(e.outerFilter) == 0 {
				err = e.rowContainer.PutChunk(chk, e.isNullEQ)
			} else {
				selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(chk), selected)
				if err != nil {
					return err
				}
				err = e.rowContainer.PutChunkSelected(chk, selected, e.isNullEQ)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	baseExecutor

	ctx         sessionctx.Context
	innerRows   []chunk.Row
	cursor      int
	innerExec   Executor
	outerExec   Executor
	innerFilter expression.CNFExprs
	outerFilter expression.CNFExprs

	joiner joiner

	cache              *applyCache
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
	if e.runtimeStats != nil {
		runtimeStats := newJoinRuntimeStats()
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, runtimeStats)
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
	}
	return e.outerExec.Close()
}

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return err
	}
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.outerChunk = newFirstChunk(e.outerExec)
	e.innerChunk = newFirstChunk(e.innerExec)
	e.innerList = chunk.NewList(retTypes(e.innerExec), e.initCap, e.maxChunkSize)

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.innerList.GetMemTracker().SetLabel(memory.LabelForInnerList)
	e.innerList.GetMemTracker().AttachTo(e.memTracker)

	if e.canUseCache {
		e.cache, err = newApplyCache(e.ctx)
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
func aggExecutorTreeInputEmpty(e Executor) bool {
	children := e.base().children
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
	if hashAgg, ok := e.(*HashAggExec); ok {
		return hashAgg.isChildReturnEmpty
	}
	if streamAgg, ok := e.(*StreamAggExec); ok {
		return streamAgg.isChildReturnEmpty
	}
	return false
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(ctx context.Context, chk *chunk.Chunk) (*chunk.Row, error) {
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		if e.outerChunkCursor >= e.outerChunk.NumRows() {
			err := Next(ctx, e.outerExec, e.outerChunk)
			if err != nil {
				return nil, err
			}
			if e.outerChunk.NumRows() == 0 {
				return nil, nil
			}
			e.outerSelected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected)
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
	err := e.innerExec.Open(ctx)
	defer terror.Call(e.innerExec.Close)
	if err != nil {
		return err
	}

	if e.canUseCache {
		// create a new one since it may be in the cache
		e.innerList = chunk.NewList(retTypes(e.innerExec), e.initCap, e.maxChunkSize)
	} else {
		e.innerList.Reset()
	}
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := Next(ctx, e.innerExec, e.innerChunk)
		if err != nil {
			return err
		}
		if e.innerChunk.NumRows() == 0 {
			return nil
		}

		e.innerSelected, err = expression.VectorizedFilter(e.ctx, e.innerFilter, innerIter, e.innerSelected)
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
					key, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, key, *col.Data)
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
			buf.WriteString(fmt.Sprintf(", cache:ON, cacheHitRatio:%.3f%%", e.cache.hitRatio*100))
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
func (e *joinRuntimeStats) Tp() int {
	return execdetails.TpJoinRuntimeStats
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
func (e *hashJoinRuntimeStats) Tp() int {
	return execdetails.TpHashJoinRuntimeStats
}

func (e *hashJoinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if e.fetchAndBuildHashTable > 0 {
		buf.WriteString("build_hash_table:{total:")
		buf.WriteString(execdetails.FormatDuration(e.fetchAndBuildHashTable))
		buf.WriteString(", fetch:")
		buf.WriteString(execdetails.FormatDuration((e.fetchAndBuildHashTable - e.hashStat.buildTableElapse)))
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

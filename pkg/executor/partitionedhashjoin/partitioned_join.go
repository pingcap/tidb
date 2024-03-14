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
	"hash/fnv"
	"math"
	"runtime/trace"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	internalutil "github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/expression"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var (
	_ exec.Executor = &PartitionedHashJoinExec{}
)

type PartitionedHashJoinCtx struct {
	SessCtx   sessionctx.Context
	allocPool chunk.Allocator
	// concurrency is the number of partition, build and join workers.
	Concurrency  uint
	joinResultCh chan *internalutil.HashjoinWorkerResult
	// closeCh add a lock for closing executor.
	closeCh       chan struct{}
	finished      atomic.Bool
	buildFinished chan error
	JoinType      plannercore.JoinType
	stats         *hashJoinRuntimeStats
	ProbeKeyTypes []*types.FieldType
	BuildKeyTypes []*types.FieldType
	memTracker    *memory.Tracker // track memory usage.
	diskTracker   *disk.Tracker   // track disk usage.

	RightAsBuildSide bool
	Filter           expression.CNFExprs
	OtherCondition   expression.CNFExprs
	joinHashTable    *JoinHashTable
	hashTableMeta    *JoinTableMeta
	keyMode          keyMode

	LUsed, RUsed                                 []int
	LUsedInOtherCondition, RUsedInOtherCondition []int
}

// ProbeSideTupleFetcher reads tuples from ProbeSideExec and send them to ProbeWorkers.
type ProbeSideTupleFetcher struct {
	*PartitionedHashJoinCtx

	ProbeSideExec                  exec.Executor
	ProbeChkResourceCh             chan *probeChkResource
	ProbeResultChs                 []chan *chunk.Chunk
	RequiredRows                   int64
	needScanHTAfterProbeDone       bool
	canSkipProbeIfHashTableIsEmpty bool
}

type ProbeWorker struct {
	HashJoinCtx *PartitionedHashJoinCtx
	WorkerID    uint

	// We build individual joinProbe for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	JoinProbe          JoinProbe
	ProbeChkResourceCh chan *probeChkResource
	joinChkResourceCh  chan *chunk.Chunk
	probeResultCh      chan *chunk.Chunk
}

type BuildSideTupleFetcher struct {
	HashJoinCtx   *PartitionedHashJoinCtx
	BuildSideExec exec.Executor
}

type BuildWorker struct {
	HashJoinCtx    *PartitionedHashJoinCtx
	BuildSideExec  exec.Executor
	BuildTypes     []*types.FieldType
	BuildKeyColIdx []int
	WorkerID       uint
	rowTable       *rowTable
	partition      *chunk.List
	rtBuilder      *rowTableBuilder
}

// preBuild split the build side data into multiple partitions.
func (w *BuildWorker) preBuild() {
	if w.partition != nil {
		w.partition = chunk.NewList(w.BuildTypes, w.HashJoinCtx.SessCtx.GetSessionVars().InitChunkSize, w.HashJoinCtx.SessCtx.GetSessionVars().MaxChunkSize)

	}
}

// PartitionedHashJoinExec implements the hash join algorithm.
type PartitionedHashJoinExec struct {
	exec.BaseExecutor
	*PartitionedHashJoinCtx

	ProbeSideTupleFetcher *ProbeSideTupleFetcher
	ProbeWorkers          []*ProbeWorker
	BuildSideTupleFetcher *BuildSideTupleFetcher
	BuildWorkers          []*BuildWorker

	workerWg util.WaitGroupWrapper
	waiterWg util.WaitGroupWrapper

	prepared        bool
	partitionNumber int
}

// probeChkResource stores the result of the join probe side fetch worker,
// `dest` is for Chunk reuse: after join workers process the probe side chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the probe side fetch worker will put new data into `chk` and write `chk` into dest.
type probeChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
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
		if e.ProbeSideTupleFetcher.ProbeChkResourceCh != nil {
			close(e.ProbeSideTupleFetcher.ProbeChkResourceCh)
			channel.Clear(e.ProbeSideTupleFetcher.ProbeChkResourceCh)
		}
		for i := range e.ProbeSideTupleFetcher.ProbeResultChs {
			channel.Clear(e.ProbeSideTupleFetcher.ProbeResultChs[i])
		}
		for i := range e.ProbeWorkers {
			close(e.ProbeWorkers[i].joinChkResourceCh)
			channel.Clear(e.ProbeWorkers[i].joinChkResourceCh)
		}
		e.ProbeSideTupleFetcher.ProbeChkResourceCh = nil
		e.waiterWg.Wait()
	}
	for _, w := range e.ProbeWorkers {
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
	e.partitionNumber = mathutil.Min(int(e.Concurrency), 16)
	if e.RightAsBuildSide {
		e.hashTableMeta = newTableMeta(e.BuildWorkers[0].BuildKeyColIdx, e.BuildWorkers[0].BuildTypes,
			e.BuildKeyTypes, e.ProbeKeyTypes, e.RUsedInOtherCondition, e.RUsed, false)
	} else {
		e.hashTableMeta = newTableMeta(e.BuildWorkers[0].BuildKeyColIdx, e.BuildWorkers[0].BuildTypes,
			e.BuildKeyTypes, e.ProbeKeyTypes, e.LUsedInOtherCondition, e.LUsed, false)
	}
	for _, buildWorker := range e.BuildWorkers {
		buildWorker.rowTable = newRowTable(e.hashTableMeta)
	}
	e.PartitionedHashJoinCtx.allocPool = e.AllocPool
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

func (fetcher *ProbeSideTupleFetcher) shouldLimitProbeFetchSize() bool {
	if fetcher.JoinType == plannercore.LeftOuterJoin && fetcher.RightAsBuildSide {
		return true
	}
	if fetcher.JoinType == plannercore.RightOuterJoin && !fetcher.RightAsBuildSide {
		return true
	}
	return false
}

// fetchProbeSideChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (fetcher *ProbeSideTupleFetcher) fetchProbeSideChunks(ctx context.Context, maxChunkSize int) {
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
		case probeSideResource, ok = <-fetcher.ProbeChkResourceCh:
			if !ok {
				return
			}
		}
		probeSideResult := probeSideResource.chk
		if fetcher.shouldLimitProbeFetchSize() {
			required := int(atomic.LoadInt64(&fetcher.RequiredRows))
			probeSideResult.SetRequiredRows(required, maxChunkSize)
		}
		err := exec.Next(ctx, fetcher.ProbeSideExec, probeSideResult)
		failpoint.Inject("ConsumeRandomPanic", nil)
		if err != nil {
			fetcher.joinResultCh <- &internalutil.HashjoinWorkerResult{
				Err: err,
			}
			return
		}
		if !hasWaitedForBuild {
			failpoint.Inject("issue30289", func(val failpoint.Value) {
				if val.(bool) {
					probeSideResult.Reset()
				}
			})
			if probeSideResult.NumRows() == 0 && !fetcher.needScanHTAfterProbeDone {
				// this is a short path, if current join don't need to scan hash table
				// after probe, then if the probe side is empty, the join result must
				// be empty
				fetcher.finished.Store(true)
			}
			skipProbe, buildErr := fetcher.wait4BuildSide()
			if buildErr != nil {
				fetcher.joinResultCh <- &internalutil.HashjoinWorkerResult{
					Err: buildErr,
				}
				return
			} else if skipProbe {
				// stop probe
				if !fetcher.needScanHTAfterProbeDone {
					fetcher.finished.Store(true)
				}
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

func (fetcher *ProbeSideTupleFetcher) wait4BuildSide() (skipProbe bool, err error) {
	select {
	case <-fetcher.closeCh:
		return true, nil
	case err := <-fetcher.buildFinished:
		if err != nil {
			return false, err
		}
	}
	if fetcher.joinHashTable.isHashTableEmpty() && fetcher.canSkipProbeIfHashTableIsEmpty {
		return true, nil
	}
	return false, nil
}

func (w *BuildWorker) splitPartitionAndAppendToRowTable(typeCtx types.Context, srcChkCh chan *chunk.Chunk) error {
	builder := w.rtBuilder
	partitionNumber := w.HashJoinCtx.joinHashTable.partitionNumber
	hashTableMeta := w.HashJoinCtx.hashTableMeta
	serializedKeyVectorBuffer := make([][]byte, 1024)
	builder.hashValue = make([]uint32, 0, 1024)
	builder.partIdxVector = make([]int, 0, 1024)
	builder.rowTables = make([]*rowTable, partitionNumber)
	for chk := range srcChkCh {
		serializedKeyBuf := serializedKeyVectorBuffer[:chk.NumRows()]
		for i := range serializedKeyBuf {
			serializedKeyBuf[i] = serializedKeyBuf[i][:]
		}
		builder.hashValue = builder.hashValue[:]
		builder.partIdxVector = builder.partIdxVector[:]
		// split partition
		for _, colIdx := range builder.buildKeyIndex {
			err := codec.SerializeKeys(typeCtx, chk, builder.buildSchema.Columns[colIdx].RetType, colIdx, nil /*TODO: @XuHuaiyu filterVector*/, nil /*TODO: @XuHuaiyu nullVector*/, hashTableMeta.ignoreIntegerKeySignFlag[colIdx], serializedKeyBuf)
			if err != nil {
				return err
			}
		}

		builder.partIdxVector = builder.partIdxVector[:]
		h := fnv.New32a()
		for _, key := range serializedKeyBuf {
			h.Write(key)
			hash := h.Sum32()
			builder.hashValue = append(builder.hashValue, hash)
			builder.partIdxVector = append(builder.partIdxVector, int(hash)%int(partitionNumber))
			h.Reset()
		}

		// 2. build rowtable
		builder.appendToRowTable(typeCtx, chk, hashTableMeta)
	}
	return nil
}

// partitionWorker 各自持有一个 channel，等待 fetcher 发送的 chunk，然后处理 chunk
// 1. 从 channel 中获取 chunk
// 2. 切分 partition
// 3. 获得所有数据后，构造 rowtable
// 4. 分发 partition 和 rowtable 到不同的 build worker

// buildWorker 等 partitionWorker 分发的 chunk，然后构造 hashtable

// fetchBuildSideRows fetches all rows from build side executor, and append them
// to e.buildSideResult.
func (w *BuildSideTupleFetcher) fetchBuildSideRows(ctx context.Context, chkCh chan<- *chunk.Chunk, errCh chan<- error, doneCh <-chan struct{}) {
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
			if w.HashJoinCtx.SessCtx.GetSessionVars().ConnectionID != 0 {
				// consume 170MB memory, this sql should be tracked into MemoryTop1Tracker
				w.HashJoinCtx.memTracker.Consume(170 * 1024 * 1024)
			}
			return
		}
	})
	sessVars := w.HashJoinCtx.SessCtx.GetSessionVars()
	for {
		if w.HashJoinCtx.finished.Load() {
			return
		}
		chk := w.HashJoinCtx.allocPool.Alloc(w.BuildSideExec.RetFieldTypes(), sessVars.MaxChunkSize, sessVars.MaxChunkSize)
		err = exec.Next(ctx, w.BuildSideExec, chk)
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
		case <-w.HashJoinCtx.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

func (e *PartitionedHashJoinExec) canSkipProbeIfHashTableIsEmpty() bool {
	switch e.JoinType {
	case plannercore.InnerJoin:
		return true
	case plannercore.LeftOuterJoin:
		return !e.RightAsBuildSide
	case plannercore.RightOuterJoin:
		return e.RightAsBuildSide
	case plannercore.SemiJoin:
		return e.RightAsBuildSide
	default:
		return false
	}
}

func (e *PartitionedHashJoinExec) initializeForProbe() {
	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *internalutil.HashjoinWorkerResult, e.Concurrency+1)

	e.ProbeSideTupleFetcher.PartitionedHashJoinCtx = e.PartitionedHashJoinCtx
	e.ProbeSideTupleFetcher.needScanHTAfterProbeDone = e.ProbeWorkers[0].JoinProbe.NeedScanRowTable()
	e.ProbeSideTupleFetcher.canSkipProbeIfHashTableIsEmpty = e.canSkipProbeIfHashTableIsEmpty()
	// e.ProbeSideTupleFetcher.ProbeResultChs is for transmitting the chunks which store the data of
	// ProbeSideExec, it'll be written by probe side worker goroutine, and read by join
	// workers.
	e.ProbeSideTupleFetcher.ProbeResultChs = make([]chan *chunk.Chunk, e.Concurrency)
	for i := uint(0); i < e.Concurrency; i++ {
		e.ProbeSideTupleFetcher.ProbeResultChs[i] = make(chan *chunk.Chunk, 1)
		e.ProbeWorkers[i].probeResultCh = e.ProbeSideTupleFetcher.ProbeResultChs[i]
	}

	// e.ProbeChkResourceCh is for transmitting the used ProbeSideExec chunks from
	// join workers to ProbeSideExec worker.
	e.ProbeSideTupleFetcher.ProbeChkResourceCh = make(chan *probeChkResource, e.Concurrency)
	for i := uint(0); i < e.Concurrency; i++ {
		e.ProbeSideTupleFetcher.ProbeChkResourceCh <- &probeChkResource{
			chk:  exec.NewFirstChunk(e.ProbeSideTupleFetcher.ProbeSideExec),
			dest: e.ProbeSideTupleFetcher.ProbeResultChs[i],
		}
	}

	// e.ProbeWorker.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to probe worker goroutines.
	for i := uint(0); i < e.Concurrency; i++ {
		e.ProbeWorkers[i].joinChkResourceCh = make(chan *chunk.Chunk, 1)
		e.ProbeWorkers[i].joinChkResourceCh <- exec.NewFirstChunk(e)
		e.ProbeWorkers[i].ProbeChkResourceCh = e.ProbeSideTupleFetcher.ProbeChkResourceCh
	}
}

func (e *PartitionedHashJoinExec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.workerWg.RunWithRecover(func() {
		defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
		e.ProbeSideTupleFetcher.fetchProbeSideChunks(ctx, e.MaxChunkSize())
	}, e.ProbeSideTupleFetcher.handleProbeSideFetcherPanic)

	for i := uint(0); i < e.Concurrency; i++ {
		workerID := i
		e.workerWg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "HashJoinWorker").End()
			e.ProbeWorkers[workerID].runJoinWorker()
		}, e.ProbeWorkers[workerID].handleProbeWorkerPanic)
	}
	e.waiterWg.RunWithRecover(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (fetcher *ProbeSideTupleFetcher) handleProbeSideFetcherPanic(r any) {
	for i := range fetcher.ProbeResultChs {
		close(fetcher.ProbeResultChs[i])
	}
	if r != nil {
		fetcher.joinResultCh <- &internalutil.HashjoinWorkerResult{Err: util.GetRecoverError(r)}
	}
}

func (w *ProbeWorker) handleProbeWorkerPanic(r any) {
	if r != nil {
		w.HashJoinCtx.joinResultCh <- &internalutil.HashjoinWorkerResult{Err: util.GetRecoverError(r)}
	}
}

func (e *PartitionedHashJoinExec) handleJoinWorkerPanic(r any) {
	if r != nil {
		e.joinResultCh <- &internalutil.HashjoinWorkerResult{Err: util.GetRecoverError(r)}
	}
}

func (e *PartitionedHashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.workerWg.Wait()
	if e.ProbeWorkers[0] != nil && e.ProbeWorkers[0].JoinProbe.NeedScanRowTable() {
		for i := uint(0); i < e.Concurrency; i++ {
			var workerID = i
			e.workerWg.RunWithRecover(func() {
				e.ProbeWorkers[workerID].scanRowTableAfterProbeDone()
			}, e.handleJoinWorkerPanic)
		}
	}
	close(e.joinResultCh)
}

func (w *ProbeWorker) scanRowTableAfterProbeDone() {
	ok, joinResult := w.getNewJoinResult()
	if !ok {
		return
	}
	for !w.JoinProbe.IsScanRowTableDone() {
		joinResult = w.JoinProbe.ScanRowTable(joinResult)
		if joinResult.Err != nil {
			w.HashJoinCtx.joinResultCh <- joinResult
			return
		}
		if joinResult.Chk.IsFull() {
			w.HashJoinCtx.joinResultCh <- joinResult
			ok, joinResult = w.getNewJoinResult()
			if !ok {
				return
			}
		}
	}
	if joinResult == nil {
		return
	} else if joinResult.Err != nil || (joinResult.Chk != nil && joinResult.Chk.NumRows() > 0) {
		w.HashJoinCtx.joinResultCh <- joinResult
	}
}

func (w *ProbeWorker) processOneProbeChunk(probeChunk *chunk.Chunk, joinResult *internalutil.HashjoinWorkerResult) (ok bool, _ *internalutil.HashjoinWorkerResult) {
	joinResult.Err = w.JoinProbe.SetChunkForProbe(probeChunk)
	if joinResult.Err != nil {
		return false, joinResult
	}
	for !w.JoinProbe.IsCurrentChunkProbeDone() {
		ok, joinResult = w.JoinProbe.Probe(joinResult)
		if !ok || joinResult.Err != nil {
			return ok, joinResult
		}
		if joinResult.Chk.IsFull() {
			w.HashJoinCtx.joinResultCh <- joinResult
			ok, joinResult = w.getNewJoinResult()
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

func (w *ProbeWorker) runJoinWorker() {
	probeTime := int64(0)
	if w.HashJoinCtx.stats != nil {
		start := time.Now()
		defer func() {
			t := time.Since(start)
			atomic.AddInt64(&w.HashJoinCtx.stats.probe, probeTime)
			atomic.AddInt64(&w.HashJoinCtx.stats.fetchAndProbe, int64(t))
			w.HashJoinCtx.stats.setMaxFetchAndProbeTime(int64(t))
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
		if w.HashJoinCtx.finished.Load() {
			break
		}
		select {
		case <-w.HashJoinCtx.closeCh:
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
		w.ProbeChkResourceCh <- emptyProbeSideResult
	}
	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.Err != nil || (joinResult.Chk != nil && joinResult.Chk.NumRows() > 0) {
		w.HashJoinCtx.joinResultCh <- joinResult
	} else if joinResult.Chk != nil && joinResult.Chk.NumRows() == 0 {
		w.joinChkResourceCh <- joinResult.Chk
	}
}

func (w *ProbeWorker) getNewJoinResult() (bool, *internalutil.HashjoinWorkerResult) {
	joinResult := &internalutil.HashjoinWorkerResult{
		Src: w.joinChkResourceCh,
	}
	ok := true
	select {
	case <-w.HashJoinCtx.closeCh:
		ok = false
	case joinResult.Chk, ok = <-w.joinChkResourceCh:
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
				keyColIdx:   e.BuildWorker.BuildKeyColIdx,
				naKeyColIdx: e.BuildWorker.buildNAKeyColIdx,
			}
			e.rowContainer = newHashRowContainer(e.Ctx(), hCtx, exec.RetTypes(e.BuildWorker.BuildSideExec))
			// we shallow copies rowContainer for each probe worker to avoid lock contention
			for i := uint(0); i < e.Concurrency; i++ {
				if i == 0 {
					e.ProbeWorkers[i].rowContainerForProbe = e.rowContainer
				} else {
					e.ProbeWorkers[i].rowContainerForProbe = e.rowContainer.ShallowCopy()
				}
			}
			for i := uint(0); i < e.concurrency; i++ {
				e.ProbeWorkers[i].rowIters = chunk.NewIterator4Slice([]chunk.Row{})
			}
			e.workerWg.RunWithRecover(func() {
				defer trace.StartRegion(ctx, "HashJoinHashTableBuilder").End()
				e.fetchAndBuildHashTable(ctx)
			}, e.handleFetchAndBuildHashTablePanic)
			e.fetchAndProbeHashTable(ctx)
			e.prepared = true
		}
		if e.isOuterJoin {
			atomic.StoreInt64(&e.ProbeSideTupleFetcher.RequiredRows, int64(req.RequiredRows()))
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

func (e *PartitionedHashJoinExec) mergeRowTables() ([]*rowTable, int) {
	rowTables := make([]*rowTable, e.partitionNumber)
	totalSegmentCnt := 0
	for i := uint(0); i < uint(e.partitionNumber); i++ {
		rowTables[i] = newRowTable(e.hashTableMeta)
	}
	for _, w := range e.BuildWorkers {
		for partIdx, rt := range w.rtBuilder.rowTables {
			if rt == nil {
				continue
			}
			rowTables[partIdx].merge(rt)
			totalSegmentCnt += len(rt.segments)
		}
	}
	return rowTables, totalSegmentCnt
}

// checkBalance checks whether the segment count of each partition is balanced.
func (e *PartitionedHashJoinExec) checkBalance(totalSegmentCnt int) bool {
	isBalanced := e.Concurrency == uint(e.partitionNumber)
	if !isBalanced {
		return false
	}
	avgSegCnt := totalSegmentCnt / e.partitionNumber
	balanceThreshold := int(float64(avgSegCnt) * 0.8)
	subTables := e.PartitionedHashJoinCtx.joinHashTable.tables

	for _, subTable := range subTables {
		if math.Abs(float64(len(subTable.rowData.segments)-avgSegCnt)) > float64(balanceThreshold) {
			isBalanced = false
			break
		}
	}
	return isBalanced
}

func (e *PartitionedHashJoinExec) createTasks(buildTaskCh chan<- *buildTask, totalSegmentCnt int) {
	isBalanced := e.checkBalance(totalSegmentCnt)
	segStep := totalSegmentCnt / int(e.Concurrency)
	subTables := e.PartitionedHashJoinCtx.joinHashTable.tables
	createBuildTask := func(partIdx int, segStartIdx int, segEndIdx int) *buildTask {
		return &buildTask{partitionIdx: partIdx, segStartIdx: segStartIdx, segEndIdx: segEndIdx}
	}

	for partIdx, subTable := range subTables {
		segmentsLen := len(subTable.rowData.segments)
		if isBalanced {
			buildTaskCh <- createBuildTask(partIdx, 0, segmentsLen)
			continue
		}
		for startIdx := 0; startIdx < segmentsLen; startIdx += segStep {
			endIdx := mathutil.Min(startIdx+segStep, segmentsLen)
			buildTaskCh <- createBuildTask(partIdx, startIdx, endIdx)
		}
	}
}

func (e *PartitionedHashJoinExec) fetchAndBuildHashTable(ctx context.Context) {
	if e.stats != nil {
		start := time.Now()
		defer func() {
			e.stats.fetchAndBuildHashTable = time.Since(start)
		}()
	}

	wg := new(sync.WaitGroup)
	errCh := make(chan error, 1+e.Concurrency)
	srcChkCh := e.fetchBuildSideRows(ctx, wg, errCh)
	e.splitAndAppendToRowTable(srcChkCh, wg, errCh)
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		e.buildFinished <- err
	}

	rowTables, totalSegmentCnt := e.mergeRowTables()
	e.joinHashTable = newJoinHashTable(rowTables)

	wg = new(sync.WaitGroup)
	errCh = make(chan error, 1+e.Concurrency)
	buildTaskCh := e.createBuildTasks(totalSegmentCnt, wg, errCh)
	e.buildHashTable(buildTaskCh, wg, errCh)
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		e.buildFinished <- err
	}
}

func (e *PartitionedHashJoinExec) fetchBuildSideRows(ctx context.Context, wg *sync.WaitGroup, errCh chan error) chan *chunk.Chunk {
	srcChkCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	wg.Add(1)
	e.workerWg.RunWithRecover(
		func() {
			defer trace.StartRegion(ctx, "HashJoinBuildSideFetcher").End()
			e.BuildSideTupleFetcher.fetchBuildSideRows(ctx, srcChkCh, errCh, doneCh)
		},
		func(r any) {
			if r != nil {
				errCh <- util.GetRecoverError(r)
			}
		},
	)
	return srcChkCh
}

func (e *PartitionedHashJoinExec) splitAndAppendToRowTable(srcChkCh chan *chunk.Chunk, wg *sync.WaitGroup, errCh chan error) {
	for i := uint(0); i < e.Concurrency; i++ {
		wg.Add(1)
		e.workerWg.RunWithRecover(
			func() {
				err := e.BuildWorkers[i].splitPartitionAndAppendToRowTable(e.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), srcChkCh)
				if err != nil {
					errCh <- err
				}
			},
			func(r any) {
				if r != nil {
					errCh <- util.GetRecoverError(r)
				}
				wg.Done()
			},
		)
	}
}

func (e *PartitionedHashJoinExec) createBuildTasks(totalSegmentCnt int, wg *sync.WaitGroup, errCh chan error) chan *buildTask {
	buildTaskCh := make(chan *buildTask, e.Concurrency)
	wg.Add(1)
	e.workerWg.RunWithRecover(
		func() { e.createTasks(buildTaskCh, totalSegmentCnt) },
		func(r any) {
			if r != nil {
				errCh <- util.GetRecoverError(r)
			}
			close(buildTaskCh)
		},
	)
	return buildTaskCh
}

func (e *PartitionedHashJoinExec) buildHashTable(buildTaskCh chan *buildTask, wg *sync.WaitGroup, errCh chan error) {
	for i := uint(0); i < e.Concurrency; i++ {
		wg.Add(1)
		e.workerWg.RunWithRecover(
			func() {
				err := e.BuildWorkers[i].buildHashTable(buildTaskCh)
				if err != nil {
					errCh <- err
				}
			},
			func(r any) {
				if r != nil {
					errCh <- util.GetRecoverError(r)
				}
				wg.Done()
			},
		)
	}
}

type buildTask struct {
	partitionIdx int
	segStartIdx  int
	segEndIdx    int
}

// buildHashTableForList builds hash table from `list`.
func (w *BuildWorker) buildHashTable(taskCh chan *buildTask) error {
	for task := range taskCh {
		partIdx, segStartIdx, segEndIdx := task.partitionIdx, task.segStartIdx, task.segEndIdx
		w.HashJoinCtx.joinHashTable.tables[partIdx].build(segStartIdx, segEndIdx)
	}

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

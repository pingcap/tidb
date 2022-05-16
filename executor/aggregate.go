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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/set"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

type aggPartialResultMapper map[string][]aggfuncs.PartialResult

// baseHashAggWorker stores the common attributes of HashAggFinalWorker and HashAggPartialWorker.
// nolint:structcheck
type baseHashAggWorker struct {
	ctx          sessionctx.Context
	finishCh     <-chan struct{}
	aggFuncs     []aggfuncs.AggFunc
	maxChunkSize int
	stats        *AggWorkerStat

	memTracker *memory.Tracker
	BInMap     int // indicate there are 2^BInMap buckets in Golang Map.
}

func newBaseHashAggWorker(ctx sessionctx.Context, finishCh <-chan struct{}, aggFuncs []aggfuncs.AggFunc,
	maxChunkSize int, memTrack *memory.Tracker) baseHashAggWorker {
	baseWorker := baseHashAggWorker{
		ctx:          ctx,
		finishCh:     finishCh,
		aggFuncs:     aggFuncs,
		maxChunkSize: maxChunkSize,
		memTracker:   memTrack,
		BInMap:       0,
	}
	return baseWorker
}

// HashAggPartialWorker indicates the partial workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_partial_concurrency`.
type HashAggPartialWorker struct {
	baseHashAggWorker

	inputCh           chan *chunk.Chunk
	outputChs         []chan *HashAggIntermData
	globalOutputCh    chan *AfFinalResult
	giveBackCh        chan<- *HashAggInput
	partialResultsMap aggPartialResultMapper
	groupByItems      []expression.Expression
	groupKey          [][]byte
	// chk stores the input data from child,
	// and is reused by childExec and partial worker.
	chk *chunk.Chunk
}

// HashAggFinalWorker indicates the final workers of parallel hash agg execution,
// the number of the worker can be set by `tidb_hashagg_final_concurrency`.
type HashAggFinalWorker struct {
	baseHashAggWorker

	rowBuffer           []types.Datum
	mutableRow          chunk.MutRow
	partialResultMap    aggPartialResultMapper
	groupSet            set.StringSetWithMemoryUsage
	inputCh             chan *HashAggIntermData
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk
	groupKeys           [][]byte
}

// AfFinalResult indicates aggregation functions final result.
type AfFinalResult struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// HashAggExec deals with all the aggregate functions.
// It is built from the Aggregate Plan. When Next() is called, it reads all the data from Src
// and updates all the items in PartialAggFuncs.
// The parallel execution flow is as the following graph shows:
//
//                            +-------------+
//                            | Main Thread |
//                            +------+------+
//                                   ^
//                                   |
//                                   +
//                              +-+-            +-+
//                              | |    ......   | |  finalOutputCh
//                              +++-            +-+
//                               ^
//                               |
//                               +---------------+
//                               |               |
//                 +--------------+             +--------------+
//                 | final worker |     ......  | final worker |
//                 +------------+-+             +-+------------+
//                              ^                 ^
//                              |                 |
//                             +-+  +-+  ......  +-+
//                             | |  | |          | |
//                             ...  ...          ...    partialOutputChs
//                             | |  | |          | |
//                             +++  +++          +++
//                              ^    ^            ^
//          +-+                 |    |            |
//          | |        +--------o----+            |
// inputCh  +-+        |        +-----------------+---+
//          | |        |                              |
//          ...    +---+------------+            +----+-----------+
//          | |    | partial worker |   ......   | partial worker |
//          +++    +--------------+-+            +-+--------------+
//           |                     ^                ^
//           |                     |                |
//      +----v---------+          +++ +-+          +++
//      | data fetcher | +------> | | | |  ......  | |   partialInputChs
//      +--------------+          +-+ +-+          +-+
type HashAggExec struct {
	baseExecutor

	sc               *stmtctx.StatementContext
	PartialAggFuncs  []aggfuncs.AggFunc
	FinalAggFuncs    []aggfuncs.AggFunc
	partialResultMap aggPartialResultMapper
	bInMap           int64 // indicate there are 2^bInMap buckets in partialResultMap
	groupSet         set.StringSetWithMemoryUsage
	groupKeys        []string
	cursor4GroupKey  int
	GroupByItems     []expression.Expression
	groupKeyBuffer   [][]byte

	finishCh         chan struct{}
	finalOutputCh    chan *AfFinalResult
	partialOutputChs []chan *HashAggIntermData
	inputCh          chan *HashAggInput
	partialInputChs  []chan *chunk.Chunk
	partialWorkers   []HashAggPartialWorker
	finalWorkers     []HashAggFinalWorker
	defaultVal       *chunk.Chunk
	childResult      *chunk.Chunk

	// isChildReturnEmpty indicates whether the child executor only returns an empty input.
	isChildReturnEmpty bool
	// After we support parallel execution for aggregation functions with distinct,
	// we can remove this attribute.
	isUnparallelExec        bool
	parallelExecInitialized bool
	prepared                bool
	executed                bool

	memTracker  *memory.Tracker // track memory usage.
	diskTracker *disk.Tracker

	stats *HashAggRuntimeStats

	// listInDisk is the chunks to store row values for spilled data.
	// The HashAggExec may be set to `spill mode` multiple times, and all spilled data will be appended to ListInDisk.
	listInDisk *chunk.ListInDisk
	// numOfSpilledChks indicates the number of all the spilled chunks.
	numOfSpilledChks int
	// offsetOfSpilledChks indicates the offset of the chunk be read from the disk.
	// In each round of processing, we need to re-fetch all the chunks spilled in the last one.
	offsetOfSpilledChks int
	// inSpillMode indicates whether HashAgg is in `spill mode`.
	// When HashAgg is in `spill mode`, the size of `partialResultMap` is no longer growing and all the data fetched
	// from the child executor is spilled to the disk.
	inSpillMode uint32
	// tmpChkForSpill is the temp chunk for spilling.
	tmpChkForSpill *chunk.Chunk
	// spillAction save the Action for spilling.
	spillAction *AggSpillDiskAction
	// isChildDrained indicates whether the all data from child has been taken out.
	isChildDrained bool
}

// HashAggInput indicates the input of hash agg exec.
type HashAggInput struct {
	chk *chunk.Chunk
	// giveBackCh is bound with specific partial worker,
	// it's used to reuse the `chk`,
	// and tell the data-fetcher which partial worker it should send data to.
	giveBackCh chan<- *chunk.Chunk
}

// HashAggIntermData indicates the intermediate data of aggregation execution.
type HashAggIntermData struct {
	groupKeys        []string
	cursor           int
	partialResultMap aggPartialResultMapper
}

// getPartialResultBatch fetches a batch of partial results from HashAggIntermData.
func (d *HashAggIntermData) getPartialResultBatch(sc *stmtctx.StatementContext, prs [][]aggfuncs.PartialResult, aggFuncs []aggfuncs.AggFunc, maxChunkSize int) (_ [][]aggfuncs.PartialResult, groupKeys []string, reachEnd bool) {
	keyStart := d.cursor
	for ; d.cursor < len(d.groupKeys) && len(prs) < maxChunkSize; d.cursor++ {
		prs = append(prs, d.partialResultMap[d.groupKeys[d.cursor]])
	}
	if d.cursor == len(d.groupKeys) {
		reachEnd = true
	}
	return prs, d.groupKeys[keyStart:d.cursor], reachEnd
}

// Close implements the Executor Close interface.
func (e *HashAggExec) Close() error {
	if e.isUnparallelExec {
		var firstErr error
		e.childResult = nil
		e.groupSet, _ = set.NewStringSetWithMemoryUsage()
		e.partialResultMap = nil
		if e.memTracker != nil {
			e.memTracker.ReplaceBytesUsed(0)
		}
		if e.listInDisk != nil {
			firstErr = e.listInDisk.Close()
		}
		e.spillAction, e.tmpChkForSpill = nil, nil
		if err := e.baseExecutor.Close(); firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	if e.parallelExecInitialized {
		// `Close` may be called after `Open` without calling `Next` in test.
		if !e.prepared {
			close(e.inputCh)
			for _, ch := range e.partialOutputChs {
				close(ch)
			}
			for _, ch := range e.partialInputChs {
				close(ch)
			}
			close(e.finalOutputCh)
		}
		close(e.finishCh)
		for _, ch := range e.partialOutputChs {
			for range ch {
			}
		}
		for _, ch := range e.partialInputChs {
			for range ch {
			}
		}
		for range e.finalOutputCh {
		}
		e.executed = false
		if e.memTracker != nil {
			e.memTracker.ReplaceBytesUsed(0)
		}
	}
	return e.baseExecutor.Close()
}

// Open implements the Executor Open interface.
func (e *HashAggExec) Open(ctx context.Context) error {
	failpoint.Inject("mockHashAggExecBaseExecutorOpenReturnedError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock HashAggExec.baseExecutor.Open returned error"))
		}
	})

	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	// If panic here, the children executor should be closed because they are open.
	defer closeBaseExecutor(&e.baseExecutor)
	e.prepared = false

	e.memTracker = memory.NewTracker(e.id, -1)
	if e.ctx.GetSessionVars().TrackAggregateMemoryUsage {
		e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	}

	if e.isUnparallelExec {
		e.initForUnparallelExec()
		return nil
	}
	e.initForParallelExec(e.ctx)
	return nil
}

func (e *HashAggExec) initForUnparallelExec() {
	var setSize int64
	e.groupSet, setSize = set.NewStringSetWithMemoryUsage()
	e.partialResultMap = make(aggPartialResultMapper)
	e.bInMap = 0
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice*(1<<e.bInMap) + setSize)
	e.groupKeyBuffer = make([][]byte, 0, 8)
	e.childResult = newFirstChunk(e.children[0])
	e.memTracker.Consume(e.childResult.MemoryUsage())

	e.offsetOfSpilledChks, e.numOfSpilledChks = 0, 0
	e.executed, e.isChildDrained = false, false
	e.listInDisk = chunk.NewListInDisk(retTypes(e.children[0]))
	e.tmpChkForSpill = newFirstChunk(e.children[0])
	if e.ctx.GetSessionVars().TrackAggregateMemoryUsage && config.GetGlobalConfig().OOMUseTmpStorage {
		e.diskTracker = disk.NewTracker(e.id, -1)
		e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)
		e.listInDisk.GetDiskTracker().AttachTo(e.diskTracker)
		e.ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewActionForSoftLimit(e.ActionSpill())
	}
}

func closeBaseExecutor(b *baseExecutor) {
	if r := recover(); r != nil {
		// Release the resource, but throw the panic again and let the top level handle it.
		terror.Log(b.Close())
		logutil.BgLogger().Warn("panic in Open(), close base executor and throw exception again")
		panic(r)
	}
}

func (e *HashAggExec) initForParallelExec(ctx sessionctx.Context) {
	sessionVars := e.ctx.GetSessionVars()
	finalConcurrency := sessionVars.HashAggFinalConcurrency()
	partialConcurrency := sessionVars.HashAggPartialConcurrency()
	e.isChildReturnEmpty = true
	e.finalOutputCh = make(chan *AfFinalResult, finalConcurrency+partialConcurrency+1)
	e.inputCh = make(chan *HashAggInput, partialConcurrency)
	e.finishCh = make(chan struct{}, 1)

	e.partialInputChs = make([]chan *chunk.Chunk, partialConcurrency)
	for i := range e.partialInputChs {
		e.partialInputChs[i] = make(chan *chunk.Chunk, 1)
	}
	e.partialOutputChs = make([]chan *HashAggIntermData, finalConcurrency)
	for i := range e.partialOutputChs {
		e.partialOutputChs[i] = make(chan *HashAggIntermData, partialConcurrency)
	}

	e.partialWorkers = make([]HashAggPartialWorker, partialConcurrency)
	e.finalWorkers = make([]HashAggFinalWorker, finalConcurrency)
	e.initRuntimeStats()

	// Init partial workers.
	for i := 0; i < partialConcurrency; i++ {
		w := HashAggPartialWorker{
			baseHashAggWorker: newBaseHashAggWorker(e.ctx, e.finishCh, e.PartialAggFuncs, e.maxChunkSize, e.memTracker),
			inputCh:           e.partialInputChs[i],
			outputChs:         e.partialOutputChs,
			giveBackCh:        e.inputCh,
			globalOutputCh:    e.finalOutputCh,
			partialResultsMap: make(aggPartialResultMapper),
			groupByItems:      e.GroupByItems,
			chk:               newFirstChunk(e.children[0]),
			groupKey:          make([][]byte, 0, 8),
		}
		// There is a bucket in the empty partialResultsMap.
		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << w.BInMap))
		if e.stats != nil {
			w.stats = &AggWorkerStat{}
			e.stats.PartialStats = append(e.stats.PartialStats, w.stats)
		}
		e.memTracker.Consume(w.chk.MemoryUsage())
		e.partialWorkers[i] = w
		input := &HashAggInput{
			chk:        newFirstChunk(e.children[0]),
			giveBackCh: w.inputCh,
		}
		e.memTracker.Consume(input.chk.MemoryUsage())
		e.inputCh <- input
	}

	// Init final workers.
	for i := 0; i < finalConcurrency; i++ {
		groupSet, setSize := set.NewStringSetWithMemoryUsage()
		w := HashAggFinalWorker{
			baseHashAggWorker:   newBaseHashAggWorker(e.ctx, e.finishCh, e.FinalAggFuncs, e.maxChunkSize, e.memTracker),
			partialResultMap:    make(aggPartialResultMapper),
			groupSet:            groupSet,
			inputCh:             e.partialOutputChs[i],
			outputCh:            e.finalOutputCh,
			finalResultHolderCh: make(chan *chunk.Chunk, 1),
			rowBuffer:           make([]types.Datum, 0, e.Schema().Len()),
			mutableRow:          chunk.MutRowFromTypes(retTypes(e)),
			groupKeys:           make([][]byte, 0, 8),
		}
		// There is a bucket in the empty partialResultsMap.
		e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice*(1<<w.BInMap) + setSize)
		if e.stats != nil {
			w.stats = &AggWorkerStat{}
			e.stats.FinalStats = append(e.stats.FinalStats, w.stats)
		}
		e.finalWorkers[i] = w
		e.finalWorkers[i].finalResultHolderCh <- newFirstChunk(e)
	}

	e.parallelExecInitialized = true
}

func (w *HashAggPartialWorker) getChildInput() bool {
	select {
	case <-w.finishCh:
		return false
	case chk, ok := <-w.inputCh:
		if !ok {
			return false
		}
		w.chk.SwapColumns(chk)
		w.giveBackCh <- &HashAggInput{
			chk:        chk,
			giveBackCh: w.inputCh,
		}
	}
	return true
}

func recoveryHashAgg(output chan *AfFinalResult, r interface{}) {
	err := errors.Errorf("%v", r)
	output <- &AfFinalResult{err: errors.Errorf("%v", r)}
	logutil.BgLogger().Error("parallel hash aggregation panicked", zap.Error(err), zap.Stack("stack"))
}

func (w *HashAggPartialWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup, finalConcurrency int) {
	start := time.Now()
	needShuffle, sc := false, ctx.GetSessionVars().StmtCtx
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.globalOutputCh, r)
		}
		if needShuffle {
			w.shuffleIntermData(sc, finalConcurrency)
		}
		w.memTracker.Consume(-w.chk.MemoryUsage())
		if w.stats != nil {
			w.stats.WorkerTime += int64(time.Since(start))
		}
		waitGroup.Done()
	}()
	for {
		waitStart := time.Now()
		ok := w.getChildInput()
		if w.stats != nil {
			w.stats.WaitTime += int64(time.Since(waitStart))
		}
		if !ok {
			return
		}
		execStart := time.Now()
		if err := w.updatePartialResult(ctx, sc, w.chk, len(w.partialResultsMap)); err != nil {
			w.globalOutputCh <- &AfFinalResult{err: err}
			return
		}
		if w.stats != nil {
			w.stats.ExecTime += int64(time.Since(execStart))
			w.stats.TaskNum += 1
		}
		// The intermData can be promised to be not empty if reaching here,
		// so we set needShuffle to be true.
		needShuffle = true
	}
}

func getGroupKeyMemUsage(groupKey [][]byte) int64 {
	mem := int64(0)
	for _, key := range groupKey {
		mem += int64(cap(key))
	}
	mem += 12 * int64(cap(groupKey))
	return mem
}

func (w *HashAggPartialWorker) updatePartialResult(ctx sessionctx.Context, sc *stmtctx.StatementContext, chk *chunk.Chunk, finalConcurrency int) (err error) {
	memSize := getGroupKeyMemUsage(w.groupKey)
	w.groupKey, err = getGroupKey(w.ctx, chk, w.groupKey, w.groupByItems)
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(getGroupKeyMemUsage(w.groupKey) - memSize)
	if err != nil {
		return err
	}

	partialResults := w.getPartialResult(sc, w.groupKey, w.partialResultsMap)
	numRows := chk.NumRows()
	rows := make([]chunk.Row, 1)
	allMemDelta := int64(0)
	for i := 0; i < numRows; i++ {
		for j, af := range w.aggFuncs {
			rows[0] = chk.GetRow(i)
			memDelta, err := af.UpdatePartialResult(ctx, rows, partialResults[i][j])
			if err != nil {
				return err
			}
			allMemDelta += memDelta
		}
	}
	w.memTracker.Consume(allMemDelta)
	return nil
}

// shuffleIntermData shuffles the intermediate data of partial workers to corresponded final workers.
// We only support parallel execution for single-machine, so process of encode and decode can be skipped.
func (w *HashAggPartialWorker) shuffleIntermData(sc *stmtctx.StatementContext, finalConcurrency int) {
	groupKeysSlice := make([][]string, finalConcurrency)
	for groupKey := range w.partialResultsMap {
		finalWorkerIdx := int(murmur3.Sum32([]byte(groupKey))) % finalConcurrency
		if groupKeysSlice[finalWorkerIdx] == nil {
			groupKeysSlice[finalWorkerIdx] = make([]string, 0, len(w.partialResultsMap)/finalConcurrency)
		}
		groupKeysSlice[finalWorkerIdx] = append(groupKeysSlice[finalWorkerIdx], groupKey)
	}

	for i := range groupKeysSlice {
		if groupKeysSlice[i] == nil {
			continue
		}
		w.outputChs[i] <- &HashAggIntermData{
			groupKeys:        groupKeysSlice[i],
			partialResultMap: w.partialResultsMap,
		}
	}
}

// getGroupKey evaluates the group items and args of aggregate functions.
func getGroupKey(ctx sessionctx.Context, input *chunk.Chunk, groupKey [][]byte, groupByItems []expression.Expression) ([][]byte, error) {
	numRows := input.NumRows()
	avlGroupKeyLen := mathutil.Min(len(groupKey), numRows)
	for i := 0; i < avlGroupKeyLen; i++ {
		groupKey[i] = groupKey[i][:0]
	}
	for i := avlGroupKeyLen; i < numRows; i++ {
		groupKey = append(groupKey, make([]byte, 0, 10*len(groupByItems)))
	}

	for _, item := range groupByItems {
		tp := item.GetType()
		buf, err := expression.GetColumn(tp.EvalType(), numRows)
		if err != nil {
			return nil, err
		}

		if err := expression.EvalExpr(ctx, item, tp.EvalType(), input, buf); err != nil {
			expression.PutColumn(buf)
			return nil, err
		}
		// This check is used to avoid error during the execution of `EncodeDecimal`.
		if item.GetType().GetType() == mysql.TypeNewDecimal {
			newTp := *tp
			newTp.SetFlen(0)
			tp = &newTp
		}
		groupKey, err = codec.HashGroupKey(ctx.GetSessionVars().StmtCtx, input.NumRows(), buf, groupKey, tp)
		if err != nil {
			expression.PutColumn(buf)
			return nil, err
		}
		expression.PutColumn(buf)
	}
	return groupKey, nil
}

func (w *baseHashAggWorker) getPartialResult(sc *stmtctx.StatementContext, groupKey [][]byte, mapper aggPartialResultMapper) [][]aggfuncs.PartialResult {
	n := len(groupKey)
	partialResults := make([][]aggfuncs.PartialResult, n)
	allMemDelta := int64(0)
	for i := 0; i < n; i++ {
		var ok bool
		if partialResults[i], ok = mapper[string(groupKey[i])]; ok {
			continue
		}
		for _, af := range w.aggFuncs {
			partialResult, memDelta := af.AllocPartialResult()
			partialResults[i] = append(partialResults[i], partialResult)
			allMemDelta += memDelta
		}
		mapper[string(groupKey[i])] = partialResults[i]
		allMemDelta += int64(len(groupKey[i]))
		// Map will expand when count > bucketNum * loadFactor. The memory usage will doubled.
		if len(mapper) > (1<<w.BInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
			w.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << w.BInMap))
			w.BInMap++
		}
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(allMemDelta)
	return partialResults
}

func (w *HashAggFinalWorker) getPartialInput() (input *HashAggIntermData, ok bool) {
	select {
	case <-w.finishCh:
		return nil, false
	case input, ok = <-w.inputCh:
		if !ok {
			return nil, false
		}
	}
	return
}

func (w *HashAggFinalWorker) consumeIntermData(sctx sessionctx.Context) (err error) {
	var (
		input            *HashAggIntermData
		ok               bool
		intermDataBuffer [][]aggfuncs.PartialResult
		groupKeys        []string
		sc               = sctx.GetSessionVars().StmtCtx
	)
	for {
		waitStart := time.Now()
		input, ok = w.getPartialInput()
		if w.stats != nil {
			w.stats.WaitTime += int64(time.Since(waitStart))
		}
		if !ok {
			return nil
		}
		execStart := time.Now()
		if intermDataBuffer == nil {
			intermDataBuffer = make([][]aggfuncs.PartialResult, 0, w.maxChunkSize)
		}
		// Consume input in batches, size of every batch is less than w.maxChunkSize.
		for reachEnd := false; !reachEnd; {
			intermDataBuffer, groupKeys, reachEnd = input.getPartialResultBatch(sc, intermDataBuffer[:0], w.aggFuncs, w.maxChunkSize)
			groupKeysLen := len(groupKeys)
			memSize := getGroupKeyMemUsage(w.groupKeys)
			w.groupKeys = w.groupKeys[:0]
			for i := 0; i < groupKeysLen; i++ {
				w.groupKeys = append(w.groupKeys, []byte(groupKeys[i]))
			}
			failpoint.Inject("ConsumeRandomPanic", nil)
			w.memTracker.Consume(getGroupKeyMemUsage(w.groupKeys) - memSize)
			finalPartialResults := w.getPartialResult(sc, w.groupKeys, w.partialResultMap)
			allMemDelta := int64(0)
			for i, groupKey := range groupKeys {
				if !w.groupSet.Exist(groupKey) {
					allMemDelta += w.groupSet.Insert(groupKey)
				}
				prs := intermDataBuffer[i]
				for j, af := range w.aggFuncs {
					memDelta, err := af.MergePartialResult(sctx, prs[j], finalPartialResults[i][j])
					if err != nil {
						return err
					}
					allMemDelta += memDelta
				}
			}
			w.memTracker.Consume(allMemDelta)
		}
		if w.stats != nil {
			w.stats.ExecTime += int64(time.Since(execStart))
			w.stats.TaskNum += 1
		}
	}
}

func (w *HashAggFinalWorker) getFinalResult(sctx sessionctx.Context) {
	waitStart := time.Now()
	result, finished := w.receiveFinalResultHolder()
	if w.stats != nil {
		w.stats.WaitTime += int64(time.Since(waitStart))
	}
	if finished {
		return
	}
	execStart := time.Now()
	memSize := getGroupKeyMemUsage(w.groupKeys)
	w.groupKeys = w.groupKeys[:0]
	for groupKey := range w.groupSet.StringSet {
		w.groupKeys = append(w.groupKeys, []byte(groupKey))
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	w.memTracker.Consume(getGroupKeyMemUsage(w.groupKeys) - memSize)
	partialResults := w.getPartialResult(sctx.GetSessionVars().StmtCtx, w.groupKeys, w.partialResultMap)
	for i := 0; i < len(w.groupSet.StringSet); i++ {
		for j, af := range w.aggFuncs {
			if err := af.AppendFinalResult2Chunk(sctx, partialResults[i][j], result); err != nil {
				logutil.BgLogger().Error("HashAggFinalWorker failed to append final result to Chunk", zap.Error(err))
			}
		}
		if len(w.aggFuncs) == 0 {
			result.SetNumVirtualRows(result.NumRows() + 1)
		}
		if result.IsFull() {
			w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
			result, finished = w.receiveFinalResultHolder()
			if finished {
				return
			}
		}
	}
	w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
	if w.stats != nil {
		w.stats.ExecTime += int64(time.Since(execStart))
	}
}

func (w *HashAggFinalWorker) receiveFinalResultHolder() (*chunk.Chunk, bool) {
	select {
	case <-w.finishCh:
		return nil, true
	case result, ok := <-w.finalResultHolderCh:
		return result, !ok
	}
}

func (w *HashAggFinalWorker) run(ctx sessionctx.Context, waitGroup *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.outputCh, r)
		}
		if w.stats != nil {
			w.stats.WorkerTime += int64(time.Since(start))
		}
		waitGroup.Done()
	}()
	if err := w.consumeIntermData(ctx); err != nil {
		w.outputCh <- &AfFinalResult{err: err}
	}
	w.getFinalResult(ctx)
}

// Next implements the Executor Next interface.
func (e *HashAggExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.isUnparallelExec {
		return e.unparallelExec(ctx, req)
	}
	return e.parallelExec(ctx, req)
}

func (e *HashAggExec) fetchChildData(ctx context.Context, waitGroup *sync.WaitGroup) {
	var (
		input *HashAggInput
		chk   *chunk.Chunk
		ok    bool
		err   error
	)
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(e.finalOutputCh, r)
		}
		for i := range e.partialInputChs {
			close(e.partialInputChs[i])
		}
		waitGroup.Done()
	}()
	for {
		select {
		case <-e.finishCh:
			return
		case input, ok = <-e.inputCh:
			if !ok {
				return
			}
			chk = input.chk
		}
		mSize := chk.MemoryUsage()
		err = Next(ctx, e.children[0], chk)
		if err != nil {
			e.finalOutputCh <- &AfFinalResult{err: err}
			e.memTracker.Consume(-mSize)
			return
		}
		if chk.NumRows() == 0 {
			e.memTracker.Consume(-mSize)
			return
		}
		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(chk.MemoryUsage() - mSize)
		input.giveBackCh <- chk
	}
}

func (e *HashAggExec) waitPartialWorkerAndCloseOutputChs(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.inputCh)
	for input := range e.inputCh {
		e.memTracker.Consume(-input.chk.MemoryUsage())
	}
	for _, ch := range e.partialOutputChs {
		close(ch)
	}
}

func (e *HashAggExec) waitAllWorkersAndCloseFinalOutputCh(waitGroups ...*sync.WaitGroup) {
	for _, waitGroup := range waitGroups {
		waitGroup.Wait()
	}
	close(e.finalOutputCh)
}

func (e *HashAggExec) prepare4ParallelExec(ctx context.Context) {
	fetchChildWorkerWaitGroup := &sync.WaitGroup{}
	fetchChildWorkerWaitGroup.Add(1)
	go e.fetchChildData(ctx, fetchChildWorkerWaitGroup)

	partialWorkerWaitGroup := &sync.WaitGroup{}
	partialWorkerWaitGroup.Add(len(e.partialWorkers))
	partialStart := time.Now()
	for i := range e.partialWorkers {
		go e.partialWorkers[i].run(e.ctx, partialWorkerWaitGroup, len(e.finalWorkers))
	}
	go func() {
		e.waitPartialWorkerAndCloseOutputChs(partialWorkerWaitGroup)
		if e.stats != nil {
			atomic.AddInt64(&e.stats.PartialWallTime, int64(time.Since(partialStart)))
		}
	}()
	finalWorkerWaitGroup := &sync.WaitGroup{}
	finalWorkerWaitGroup.Add(len(e.finalWorkers))
	finalStart := time.Now()
	for i := range e.finalWorkers {
		go e.finalWorkers[i].run(e.ctx, finalWorkerWaitGroup)
	}
	go func() {
		finalWorkerWaitGroup.Wait()
		if e.stats != nil {
			atomic.AddInt64(&e.stats.FinalWallTime, int64(time.Since(finalStart)))
		}
	}()

	// All workers may send error message to e.finalOutputCh when they panic.
	// And e.finalOutputCh should be closed after all goroutines gone.
	go e.waitAllWorkersAndCloseFinalOutputCh(fetchChildWorkerWaitGroup, partialWorkerWaitGroup, finalWorkerWaitGroup)
}

// HashAggExec employs one input reader, M partial workers and N final workers to execute parallelly.
// The parallel execution flow is:
// 1. input reader reads data from child executor and send them to partial workers.
// 2. partial worker receives the input data, updates the partial results, and shuffle the partial results to the final workers.
// 3. final worker receives partial results from all the partial workers, evaluates the final results and sends the final results to the main thread.
func (e *HashAggExec) parallelExec(ctx context.Context, chk *chunk.Chunk) error {
	if !e.prepared {
		e.prepare4ParallelExec(ctx)
		e.prepared = true
	}

	failpoint.Inject("parallelHashAggError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("HashAggExec.parallelExec error"))
		}
	})

	if e.executed {
		return nil
	}
	for {
		result, ok := <-e.finalOutputCh
		if !ok {
			e.executed = true
			if e.isChildReturnEmpty && e.defaultVal != nil {
				chk.Append(e.defaultVal, 0, 1)
			}
			return nil
		}
		if result.err != nil {
			return result.err
		}
		chk.SwapColumns(result.chk)
		result.chk.Reset()
		result.giveBackCh <- result.chk
		if chk.NumRows() > 0 {
			e.isChildReturnEmpty = false
			return nil
		}
	}
}

// unparallelExec executes hash aggregation algorithm in single thread.
func (e *HashAggExec) unparallelExec(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for {
		if e.prepared {
			// Since we return e.maxChunkSize rows every time, so we should not traverse
			// `groupSet` because of its randomness.
			for ; e.cursor4GroupKey < len(e.groupKeys); e.cursor4GroupKey++ {
				partialResults := e.getPartialResults(e.groupKeys[e.cursor4GroupKey])
				if len(e.PartialAggFuncs) == 0 {
					chk.SetNumVirtualRows(chk.NumRows() + 1)
				}
				for i, af := range e.PartialAggFuncs {
					if err := af.AppendFinalResult2Chunk(e.ctx, partialResults[i], chk); err != nil {
						return err
					}
				}
				if chk.IsFull() {
					e.cursor4GroupKey++
					return nil
				}
			}
			e.resetSpillMode()
		}
		if e.executed {
			return nil
		}
		if err := e.execute(ctx); err != nil {
			return err
		}
		if (len(e.groupSet.StringSet) == 0) && len(e.GroupByItems) == 0 {
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one row [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.memTracker.Consume(e.groupSet.Insert(""))
			e.groupKeys = append(e.groupKeys, "")
		}
		e.prepared = true
	}
}

func (e *HashAggExec) resetSpillMode() {
	e.cursor4GroupKey, e.groupKeys = 0, e.groupKeys[:0]
	var setSize int64
	e.groupSet, setSize = set.NewStringSetWithMemoryUsage()
	e.partialResultMap = make(aggPartialResultMapper)
	e.bInMap = 0
	e.prepared = false
	e.executed = e.numOfSpilledChks == e.listInDisk.NumChunks() // No data is spilling again, all data have been processed.
	e.numOfSpilledChks = e.listInDisk.NumChunks()
	e.memTracker.ReplaceBytesUsed(setSize)
	atomic.StoreUint32(&e.inSpillMode, 0)
}

// execute fetches Chunks from src and update each aggregate function for each row in Chunk.
func (e *HashAggExec) execute(ctx context.Context) (err error) {
	defer func() {
		if e.tmpChkForSpill.NumRows() > 0 && err == nil {
			err = e.listInDisk.Add(e.tmpChkForSpill)
			e.tmpChkForSpill.Reset()
		}
	}()
	for {
		mSize := e.childResult.MemoryUsage()
		if err := e.getNextChunk(ctx); err != nil {
			return err
		}
		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}

		failpoint.Inject("unparallelHashAggError", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("HashAggExec.unparallelExec error"))
			}
		})

		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
		e.groupKeyBuffer, err = getGroupKey(e.ctx, e.childResult, e.groupKeyBuffer, e.GroupByItems)
		if err != nil {
			return err
		}

		allMemDelta := int64(0)
		sel := make([]int, 0, e.childResult.NumRows())
		for j := 0; j < e.childResult.NumRows(); j++ {
			groupKey := string(e.groupKeyBuffer[j]) // do memory copy here, because e.groupKeyBuffer may be reused.
			if !e.groupSet.Exist(groupKey) {
				if atomic.LoadUint32(&e.inSpillMode) == 1 && e.groupSet.Count() > 0 {
					sel = append(sel, j)
					continue
				}
				allMemDelta += e.groupSet.Insert(groupKey)
				e.groupKeys = append(e.groupKeys, groupKey)
			}
			partialResults := e.getPartialResults(groupKey)
			for i, af := range e.PartialAggFuncs {
				memDelta, err := af.UpdatePartialResult(e.ctx, []chunk.Row{e.childResult.GetRow(j)}, partialResults[i])
				if err != nil {
					return err
				}
				allMemDelta += memDelta
			}
		}

		// spill unprocessed data when exceeded.
		if len(sel) > 0 {
			e.childResult.SetSel(sel)
			err = e.spillUnprocessedData(len(sel) == cap(sel))
			if err != nil {
				return err
			}
		}

		failpoint.Inject("ConsumeRandomPanic", nil)
		e.memTracker.Consume(allMemDelta)
	}
}

func (e *HashAggExec) spillUnprocessedData(isFullChk bool) (err error) {
	if isFullChk {
		return e.listInDisk.Add(e.childResult)
	}
	for i := 0; i < e.childResult.NumRows(); i++ {
		e.tmpChkForSpill.AppendRow(e.childResult.GetRow(i))
		if e.tmpChkForSpill.IsFull() {
			err = e.listInDisk.Add(e.tmpChkForSpill)
			if err != nil {
				return err
			}
			e.tmpChkForSpill.Reset()
		}
	}
	return nil
}

func (e *HashAggExec) getNextChunk(ctx context.Context) (err error) {
	e.childResult.Reset()
	if !e.isChildDrained {
		if err := Next(ctx, e.children[0], e.childResult); err != nil {
			return err
		}
		if e.childResult.NumRows() == 0 {
			e.isChildDrained = true
		} else {
			return nil
		}
	}
	if e.offsetOfSpilledChks < e.numOfSpilledChks {
		e.childResult, err = e.listInDisk.GetChunk(e.offsetOfSpilledChks)
		if err != nil {
			return err
		}
		e.offsetOfSpilledChks++
	}
	return nil
}

func (e *HashAggExec) getPartialResults(groupKey string) []aggfuncs.PartialResult {
	partialResults, ok := e.partialResultMap[groupKey]
	allMemDelta := int64(0)
	if !ok {
		partialResults = make([]aggfuncs.PartialResult, 0, len(e.PartialAggFuncs))
		for _, af := range e.PartialAggFuncs {
			partialResult, memDelta := af.AllocPartialResult()
			partialResults = append(partialResults, partialResult)
			allMemDelta += memDelta
		}
		e.partialResultMap[groupKey] = partialResults
		allMemDelta += int64(len(groupKey))
		// Map will expand when count > bucketNum * loadFactor. The memory usage will doubled.
		if len(e.partialResultMap) > (1<<e.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
			e.memTracker.Consume(hack.DefBucketMemoryUsageForMapStrToSlice * (1 << e.bInMap))
			e.bInMap++
		}
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(allMemDelta)
	return partialResults
}

func (e *HashAggExec) initRuntimeStats() {
	if e.runtimeStats != nil && e.stats == nil {
		stats := &HashAggRuntimeStats{
			PartialConcurrency: e.ctx.GetSessionVars().HashAggPartialConcurrency(),
			FinalConcurrency:   e.ctx.GetSessionVars().HashAggFinalConcurrency(),
		}
		stats.PartialStats = make([]*AggWorkerStat, 0, stats.PartialConcurrency)
		stats.FinalStats = make([]*AggWorkerStat, 0, stats.FinalConcurrency)
		e.stats = stats
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
}

// HashAggRuntimeStats record the HashAggExec runtime stat
type HashAggRuntimeStats struct {
	PartialConcurrency int
	PartialWallTime    int64
	FinalConcurrency   int
	FinalWallTime      int64
	PartialStats       []*AggWorkerStat
	FinalStats         []*AggWorkerStat
}

// AggWorkerInfo contains the agg worker information.
type AggWorkerInfo struct {
	Concurrency int
	WallTime    int64
}

// AggWorkerStat record the AggWorker runtime stat
type AggWorkerStat struct {
	TaskNum    int64
	WaitTime   int64
	ExecTime   int64
	WorkerTime int64
}

// Clone implements the RuntimeStats interface.
func (w *AggWorkerStat) Clone() *AggWorkerStat {
	return &AggWorkerStat{
		TaskNum:    w.TaskNum,
		WaitTime:   w.WaitTime,
		ExecTime:   w.ExecTime,
		WorkerTime: w.WorkerTime,
	}
}

func (e *HashAggRuntimeStats) workerString(buf *bytes.Buffer, prefix string, concurrency int, wallTime int64, workerStats []*AggWorkerStat) {
	var totalTime, totalWait, totalExec, totalTaskNum int64
	for _, w := range workerStats {
		totalTime += w.WorkerTime
		totalWait += w.WaitTime
		totalExec += w.ExecTime
		totalTaskNum += w.TaskNum
	}
	buf.WriteString(prefix)
	buf.WriteString(fmt.Sprintf("_worker:{wall_time:%s, concurrency:%d, task_num:%d, tot_wait:%s, tot_exec:%s, tot_time:%s",
		time.Duration(wallTime), concurrency, totalTaskNum, time.Duration(totalWait), time.Duration(totalExec), time.Duration(totalTime)))
	n := len(workerStats)
	if n > 0 {
		sort.Slice(workerStats, func(i, j int) bool { return workerStats[i].WorkerTime < workerStats[j].WorkerTime })
		buf.WriteString(fmt.Sprintf(", max:%v, p95:%v",
			time.Duration(workerStats[n-1].WorkerTime), time.Duration(workerStats[n*19/20].WorkerTime)))
	}
	buf.WriteString("}")
}

// String implements the RuntimeStats interface.
func (e *HashAggRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	e.workerString(buf, "partial", e.PartialConcurrency, atomic.LoadInt64(&e.PartialWallTime), e.PartialStats)
	buf.WriteString(", ")
	e.workerString(buf, "final", e.FinalConcurrency, atomic.LoadInt64(&e.FinalWallTime), e.FinalStats)
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *HashAggRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := &HashAggRuntimeStats{
		PartialConcurrency: e.PartialConcurrency,
		PartialWallTime:    atomic.LoadInt64(&e.PartialWallTime),
		FinalConcurrency:   e.FinalConcurrency,
		FinalWallTime:      atomic.LoadInt64(&e.FinalWallTime),
		PartialStats:       make([]*AggWorkerStat, 0, e.PartialConcurrency),
		FinalStats:         make([]*AggWorkerStat, 0, e.FinalConcurrency),
	}
	for _, s := range e.PartialStats {
		newRs.PartialStats = append(newRs.PartialStats, s.Clone())
	}
	for _, s := range e.FinalStats {
		newRs.FinalStats = append(newRs.FinalStats, s.Clone())
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *HashAggRuntimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*HashAggRuntimeStats)
	if !ok {
		return
	}
	atomic.AddInt64(&e.PartialWallTime, atomic.LoadInt64(&tmp.PartialWallTime))
	atomic.AddInt64(&e.FinalWallTime, atomic.LoadInt64(&tmp.FinalWallTime))
	e.PartialStats = append(e.PartialStats, tmp.PartialStats...)
	e.FinalStats = append(e.FinalStats, tmp.FinalStats...)
}

// Tp implements the RuntimeStats interface.
func (e *HashAggRuntimeStats) Tp() int {
	return execdetails.TpHashAggRuntimeStat
}

// StreamAggExec deals with all the aggregate functions.
// It assumes all the input data is sorted by group by key.
// When Next() is called, it will return a result for the same group.
type StreamAggExec struct {
	baseExecutor

	executed bool
	// isChildReturnEmpty indicates whether the child executor only returns an empty input.
	isChildReturnEmpty bool
	defaultVal         *chunk.Chunk
	groupChecker       *vecGroupChecker
	inputIter          *chunk.Iterator4Chunk
	inputRow           chunk.Row
	aggFuncs           []aggfuncs.AggFunc
	partialResults     []aggfuncs.PartialResult
	groupRows          []chunk.Row
	childResult        *chunk.Chunk

	memTracker *memory.Tracker // track memory usage.
	// memUsageOfInitialPartialResult indicates the memory usage of all partial results after initialization.
	// All partial results will be reset after processing one group data, and the memory usage should also be reset.
	// We can't get memory delta from ResetPartialResult, so record the memory usage here.
	memUsageOfInitialPartialResult int64
}

// Open implements the Executor Open interface.
func (e *StreamAggExec) Open(ctx context.Context) error {
	failpoint.Inject("mockStreamAggExecBaseExecutorOpenReturnedError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock StreamAggExec.baseExecutor.Open returned error"))
		}
	})

	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	// If panic in Open, the children executor should be closed because they are open.
	defer closeBaseExecutor(&e.baseExecutor)

	e.childResult = newFirstChunk(e.children[0])
	e.executed = false
	e.isChildReturnEmpty = true
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()

	e.partialResults = make([]aggfuncs.PartialResult, 0, len(e.aggFuncs))
	for _, aggFunc := range e.aggFuncs {
		partialResult, memDelta := aggFunc.AllocPartialResult()
		e.partialResults = append(e.partialResults, partialResult)
		e.memUsageOfInitialPartialResult += memDelta
	}

	// bytesLimit <= 0 means no limit, for now we just track the memory footprint
	e.memTracker = memory.NewTracker(e.id, -1)
	if e.ctx.GetSessionVars().TrackAggregateMemoryUsage {
		e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(e.childResult.MemoryUsage() + e.memUsageOfInitialPartialResult)
	return nil
}

// Close implements the Executor Close interface.
func (e *StreamAggExec) Close() error {
	if e.childResult != nil {
		e.memTracker.Consume(-e.childResult.MemoryUsage() - e.memUsageOfInitialPartialResult)
		e.childResult = nil
	}
	e.groupChecker.reset()
	return e.baseExecutor.Close()
}

// Next implements the Executor Next interface.
func (e *StreamAggExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for !e.executed && !req.IsFull() {
		err = e.consumeOneGroup(ctx, req)
		if err != nil {
			e.executed = true
			return err
		}
	}
	return nil
}

func (e *StreamAggExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.groupChecker.isExhausted() {
		if err = e.consumeCurGroupRowsAndFetchChild(ctx, chk); err != nil {
			return err
		}
		if !e.executed {
			_, err := e.groupChecker.splitIntoGroups(e.childResult)
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}
	begin, end := e.groupChecker.getNextGroup()
	for i := begin; i < end; i++ {
		e.groupRows = append(e.groupRows, e.childResult.GetRow(i))
	}

	for meetLastGroup := end == e.childResult.NumRows(); meetLastGroup; {
		meetLastGroup = false
		if err = e.consumeCurGroupRowsAndFetchChild(ctx, chk); err != nil || e.executed {
			return err
		}

		isFirstGroupSameAsPrev, err := e.groupChecker.splitIntoGroups(e.childResult)
		if err != nil {
			return err
		}

		if isFirstGroupSameAsPrev {
			begin, end = e.groupChecker.getNextGroup()
			for i := begin; i < end; i++ {
				e.groupRows = append(e.groupRows, e.childResult.GetRow(i))
			}
			meetLastGroup = end == e.childResult.NumRows()
		}
	}

	err = e.consumeGroupRows()
	if err != nil {
		return err
	}

	return e.appendResult2Chunk(chk)
}

func (e *StreamAggExec) consumeGroupRows() error {
	if len(e.groupRows) == 0 {
		return nil
	}

	allMemDelta := int64(0)
	for i, aggFunc := range e.aggFuncs {
		memDelta, err := aggFunc.UpdatePartialResult(e.ctx, e.groupRows, e.partialResults[i])
		if err != nil {
			return err
		}
		allMemDelta += memDelta
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(allMemDelta)
	e.groupRows = e.groupRows[:0]
	return nil
}

func (e *StreamAggExec) consumeCurGroupRowsAndFetchChild(ctx context.Context, chk *chunk.Chunk) (err error) {
	// Before fetching a new batch of input, we should consume the last group.
	err = e.consumeGroupRows()
	if err != nil {
		return err
	}

	mSize := e.childResult.MemoryUsage()
	err = Next(ctx, e.children[0], e.childResult)
	failpoint.Inject("ConsumeRandomPanic", nil)
	e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
	if err != nil {
		return err
	}

	// No more data.
	if e.childResult.NumRows() == 0 {
		if !e.isChildReturnEmpty {
			err = e.appendResult2Chunk(chk)
		} else if e.defaultVal != nil {
			chk.Append(e.defaultVal, 0, 1)
		}
		e.executed = true
		return err
	}
	// Reach here, "e.childrenResults[0].NumRows() > 0" is guaranteed.
	e.isChildReturnEmpty = false
	e.inputRow = e.inputIter.Begin()
	return nil
}

// appendResult2Chunk appends result of all the aggregation functions to the
// result chunk, and reset the evaluation context for each aggregation.
func (e *StreamAggExec) appendResult2Chunk(chk *chunk.Chunk) error {
	for i, aggFunc := range e.aggFuncs {
		err := aggFunc.AppendFinalResult2Chunk(e.ctx, e.partialResults[i], chk)
		if err != nil {
			return err
		}
		aggFunc.ResetPartialResult(e.partialResults[i])
	}
	failpoint.Inject("ConsumeRandomPanic", nil)
	// All partial results have been reset, so reset the memory usage.
	e.memTracker.ReplaceBytesUsed(e.childResult.MemoryUsage() + e.memUsageOfInitialPartialResult)
	if len(e.aggFuncs) == 0 {
		chk.SetNumVirtualRows(chk.NumRows() + 1)
	}
	return nil
}

// vecGroupChecker is used to split a given chunk according to the `group by` expression in a vectorized manner
// It is usually used for streamAgg
type vecGroupChecker struct {
	ctx          sessionctx.Context
	GroupByItems []expression.Expression

	// groupOffset holds the offset of the last row in each group of the current chunk
	groupOffset []int
	// groupCount is the count of groups in the current chunk
	groupCount int
	// nextGroupID records the group id of the next group to be consumed
	nextGroupID int

	// lastGroupKeyOfPrevChk is the groupKey of the last group of the previous chunk
	lastGroupKeyOfPrevChk []byte
	// firstGroupKey and lastGroupKey are used to store the groupKey of the first and last group of the current chunk
	firstGroupKey []byte
	lastGroupKey  []byte

	// firstRowDatums and lastRowDatums store the results of the expression evaluation for the first and last rows of the current chunk in datum
	// They are used to encode to get firstGroupKey and lastGroupKey
	firstRowDatums []types.Datum
	lastRowDatums  []types.Datum

	// sameGroup is used to check whether the current row belongs to the same group as the previous row
	sameGroup []bool

	// set these functions for testing
	allocateBuffer func(evalType types.EvalType, capacity int) (*chunk.Column, error)
	releaseBuffer  func(buf *chunk.Column)
}

func newVecGroupChecker(ctx sessionctx.Context, items []expression.Expression) *vecGroupChecker {
	return &vecGroupChecker{
		ctx:          ctx,
		GroupByItems: items,
		groupCount:   0,
		nextGroupID:  0,
		sameGroup:    make([]bool, 1024),
	}
}

// splitIntoGroups splits a chunk into multiple groups which the row in the same group have the same groupKey
// `isFirstGroupSameAsPrev` indicates whether the groupKey of the first group of the newly passed chunk is equal to the groupKey of the last group left before
// TODO: Since all the group by items are only a column reference, guaranteed by building projection below aggregation, we can directly compare data in a chunk.
func (e *vecGroupChecker) splitIntoGroups(chk *chunk.Chunk) (isFirstGroupSameAsPrev bool, err error) {
	// The numRows can not be zero. `fetchChild` is called before `splitIntoGroups` is called.
	// if numRows == 0, it will be returned in `fetchChild`. See `fetchChild` for more details.
	numRows := chk.NumRows()

	e.reset()
	e.nextGroupID = 0
	if len(e.GroupByItems) == 0 {
		e.groupOffset = append(e.groupOffset, numRows)
		e.groupCount = 1
		return true, nil
	}

	for _, item := range e.GroupByItems {
		err = e.getFirstAndLastRowDatum(item, chk, numRows)
		if err != nil {
			return false, err
		}
	}
	e.firstGroupKey, err = codec.EncodeValue(e.ctx.GetSessionVars().StmtCtx, e.firstGroupKey, e.firstRowDatums...)
	if err != nil {
		return false, err
	}

	e.lastGroupKey, err = codec.EncodeValue(e.ctx.GetSessionVars().StmtCtx, e.lastGroupKey, e.lastRowDatums...)
	if err != nil {
		return false, err
	}

	if len(e.lastGroupKeyOfPrevChk) == 0 {
		isFirstGroupSameAsPrev = false
	} else {
		if bytes.Equal(e.lastGroupKeyOfPrevChk, e.firstGroupKey) {
			isFirstGroupSameAsPrev = true
		} else {
			isFirstGroupSameAsPrev = false
		}
	}

	if length := len(e.lastGroupKey); len(e.lastGroupKeyOfPrevChk) >= length {
		e.lastGroupKeyOfPrevChk = e.lastGroupKeyOfPrevChk[:length]
	} else {
		e.lastGroupKeyOfPrevChk = make([]byte, length)
	}
	copy(e.lastGroupKeyOfPrevChk, e.lastGroupKey)

	if bytes.Equal(e.firstGroupKey, e.lastGroupKey) {
		e.groupOffset = append(e.groupOffset, numRows)
		e.groupCount = 1
		return isFirstGroupSameAsPrev, nil
	}

	if cap(e.sameGroup) < numRows {
		e.sameGroup = make([]bool, 0, numRows)
	}
	e.sameGroup = append(e.sameGroup, false)
	for i := 1; i < numRows; i++ {
		e.sameGroup = append(e.sameGroup, true)
	}

	for _, item := range e.GroupByItems {
		err = e.evalGroupItemsAndResolveGroups(item, chk, numRows)
		if err != nil {
			return false, err
		}
	}

	for i := 1; i < numRows; i++ {
		if !e.sameGroup[i] {
			e.groupOffset = append(e.groupOffset, i)
		}
	}
	e.groupOffset = append(e.groupOffset, numRows)
	e.groupCount = len(e.groupOffset)
	return isFirstGroupSameAsPrev, nil
}

func (e *vecGroupChecker) getFirstAndLastRowDatum(item expression.Expression, chk *chunk.Chunk, numRows int) (err error) {
	var firstRowDatum, lastRowDatum types.Datum
	tp := item.GetType()
	eType := tp.EvalType()
	switch eType {
	case types.ETInt:
		firstRowVal, firstRowIsNull, err := item.EvalInt(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalInt(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			firstRowDatum.SetInt64(firstRowVal)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			lastRowDatum.SetInt64(lastRowVal)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETReal:
		firstRowVal, firstRowIsNull, err := item.EvalReal(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalReal(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			firstRowDatum.SetFloat64(firstRowVal)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			lastRowDatum.SetFloat64(lastRowVal)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETDecimal:
		firstRowVal, firstRowIsNull, err := item.EvalDecimal(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalDecimal(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			// make a copy to avoid DATA RACE
			firstDatum := types.MyDecimal{}
			err := firstDatum.FromString(firstRowVal.ToString())
			if err != nil {
				return err
			}
			firstRowDatum.SetMysqlDecimal(&firstDatum)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			// make a copy to avoid DATA RACE
			lastDatum := types.MyDecimal{}
			err := lastDatum.FromString(lastRowVal.ToString())
			if err != nil {
				return err
			}
			lastRowDatum.SetMysqlDecimal(&lastDatum)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETDatetime, types.ETTimestamp:
		firstRowVal, firstRowIsNull, err := item.EvalTime(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalTime(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			firstRowDatum.SetMysqlTime(firstRowVal)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			lastRowDatum.SetMysqlTime(lastRowVal)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETDuration:
		firstRowVal, firstRowIsNull, err := item.EvalDuration(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalDuration(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			firstRowDatum.SetMysqlDuration(firstRowVal)
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			lastRowDatum.SetMysqlDuration(lastRowVal)
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETJson:
		firstRowVal, firstRowIsNull, err := item.EvalJSON(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalJSON(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			// make a copy to avoid DATA RACE
			firstRowDatum.SetMysqlJSON(firstRowVal.Copy())
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			// make a copy to avoid DATA RACE
			lastRowDatum.SetMysqlJSON(lastRowVal.Copy())
		} else {
			lastRowDatum.SetNull()
		}
	case types.ETString:
		firstRowVal, firstRowIsNull, err := item.EvalString(e.ctx, chk.GetRow(0))
		if err != nil {
			return err
		}
		lastRowVal, lastRowIsNull, err := item.EvalString(e.ctx, chk.GetRow(numRows-1))
		if err != nil {
			return err
		}
		if !firstRowIsNull {
			// make a copy to avoid DATA RACE
			firstDatum := string([]byte(firstRowVal))
			firstRowDatum.SetString(firstDatum, tp.GetCollate())
		} else {
			firstRowDatum.SetNull()
		}
		if !lastRowIsNull {
			// make a copy to avoid DATA RACE
			lastDatum := string([]byte(lastRowVal))
			lastRowDatum.SetString(lastDatum, tp.GetCollate())
		} else {
			lastRowDatum.SetNull()
		}
	default:
		err = errors.New(fmt.Sprintf("invalid eval type %v", eType))
		return err
	}

	e.firstRowDatums = append(e.firstRowDatums, firstRowDatum)
	e.lastRowDatums = append(e.lastRowDatums, lastRowDatum)
	return err
}

// evalGroupItemsAndResolveGroups evaluates the chunk according to the expression item.
// And resolve the rows into groups according to the evaluation results
func (e *vecGroupChecker) evalGroupItemsAndResolveGroups(item expression.Expression, chk *chunk.Chunk, numRows int) (err error) {
	tp := item.GetType()
	eType := tp.EvalType()
	if e.allocateBuffer == nil {
		e.allocateBuffer = expression.GetColumn
	}
	if e.releaseBuffer == nil {
		e.releaseBuffer = expression.PutColumn
	}
	col, err := e.allocateBuffer(eType, numRows)
	if err != nil {
		return err
	}
	defer e.releaseBuffer(col)
	err = expression.EvalExpr(e.ctx, item, eType, chk, col)
	if err != nil {
		return err
	}

	previousIsNull := col.IsNull(0)
	switch eType {
	case types.ETInt:
		vals := col.Int64s()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETReal:
		vals := col.Float64s()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDecimal:
		vals := col.Decimals()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i].Compare(&vals[i-1]) != 0 {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDatetime, types.ETTimestamp:
		vals := col.Times()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i].Compare(vals[i-1]) != 0 {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDuration:
		vals := col.GoDurations()
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETJson:
		var previousKey, key json.BinaryJSON
		if !previousIsNull {
			previousKey = col.GetJSON(0)
		}
		for i := 1; i < numRows; i++ {
			isNull := col.IsNull(i)
			if !isNull {
				key = col.GetJSON(i)
			}
			if e.sameGroup[i] {
				if isNull == previousIsNull {
					if !isNull && json.CompareBinary(previousKey, key) != 0 {
						e.sameGroup[i] = false
					}
				} else {
					e.sameGroup[i] = false
				}
			}
			if !isNull {
				previousKey = key
			}
			previousIsNull = isNull
		}
	case types.ETString:
		previousKey := codec.ConvertByCollationStr(col.GetString(0), tp)
		for i := 1; i < numRows; i++ {
			key := codec.ConvertByCollationStr(col.GetString(i), tp)
			isNull := col.IsNull(i)
			if e.sameGroup[i] {
				if isNull != previousIsNull || previousKey != key {
					e.sameGroup[i] = false
				}
			}
			previousKey = key
			previousIsNull = isNull
		}
	default:
		err = errors.New(fmt.Sprintf("invalid eval type %v", eType))
	}
	if err != nil {
		return err
	}

	return err
}

func (e *vecGroupChecker) getNextGroup() (begin, end int) {
	if e.nextGroupID == 0 {
		begin = 0
	} else {
		begin = e.groupOffset[e.nextGroupID-1]
	}
	end = e.groupOffset[e.nextGroupID]
	e.nextGroupID++
	return begin, end
}

func (e *vecGroupChecker) isExhausted() bool {
	return e.nextGroupID >= e.groupCount
}

func (e *vecGroupChecker) reset() {
	if e.groupOffset != nil {
		e.groupOffset = e.groupOffset[:0]
	}
	if e.sameGroup != nil {
		e.sameGroup = e.sameGroup[:0]
	}
	if e.firstGroupKey != nil {
		e.firstGroupKey = e.firstGroupKey[:0]
	}
	if e.lastGroupKey != nil {
		e.lastGroupKey = e.lastGroupKey[:0]
	}
	if e.firstRowDatums != nil {
		e.firstRowDatums = e.firstRowDatums[:0]
	}
	if e.lastRowDatums != nil {
		e.lastRowDatums = e.lastRowDatums[:0]
	}
}

// ActionSpill returns a AggSpillDiskAction for spilling intermediate data for hashAgg.
func (e *HashAggExec) ActionSpill() *AggSpillDiskAction {
	if e.spillAction == nil {
		e.spillAction = &AggSpillDiskAction{
			e: e,
		}
	}
	return e.spillAction
}

// maxSpillTimes indicates how many times the data can spill at most.
const maxSpillTimes = 10

// AggSpillDiskAction implements memory.ActionOnExceed for unparalleled HashAgg.
// If the memory quota of a query is exceeded, AggSpillDiskAction.Action is
// triggered.
type AggSpillDiskAction struct {
	memory.BaseOOMAction
	e          *HashAggExec
	spillTimes uint32
}

// Action set HashAggExec spill mode.
func (a *AggSpillDiskAction) Action(t *memory.Tracker) {
	// Guarantee that processed data is at least 20% of the threshold, to avoid spilling too frequently.
	if atomic.LoadUint32(&a.e.inSpillMode) == 0 && a.spillTimes < maxSpillTimes && a.e.memTracker.BytesConsumed() >= t.GetBytesLimit()/5 {
		a.spillTimes++
		logutil.BgLogger().Info("memory exceeds quota, set aggregate mode to spill-mode",
			zap.Uint32("spillTimes", a.spillTimes),
			zap.Int64("consumed", t.BytesConsumed()),
			zap.Int64("quota", t.GetBytesLimit()))
		atomic.StoreUint32(&a.e.inSpillMode, 1)
		return
	}
	if fallback := a.GetFallback(); fallback != nil {
		fallback.Action(t)
	}
}

// GetPriority get the priority of the Action
func (a *AggSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

// SetLogHook sets the hook, it does nothing just to form the memory.ActionOnExceed interface.
func (a *AggSpillDiskAction) SetLogHook(hook func(uint64)) {}

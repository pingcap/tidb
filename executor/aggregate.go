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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
)

type aggCtxsMapper map[string][]*aggregation.AggEvaluateContext

type aggWorker struct {
	*sync.WaitGroup

	finishCh      <-chan struct{}
	aggFuncs      []aggregation.Aggregation
	aggCtxsMap    aggCtxsMapper
	groupByItems  []expression.Expression
	groupKey      []byte
	groupVals     [][]byte
	groupMap      *mvmap.MVMap
	groupIterator *mvmap.Iterator
	maxChunkSize  int
}

// HashAggPartialWorker does the following things:
// 1. accept and handle chunk from the child executor parallelly
// 2. generate and encode the intermediate results
// 3. transfer the encoded result to the final workers
type HashAggPartialWorker struct {
	aggWorker

	inputCh             chan *chunk.Chunk
	outputChs           []chan []*AfInterResult
	interResultHolderCh chan []*AfInterResult
	globalOutputCh      chan *AfFinalResult
	giveBackCh          chan<- *chunk.Chunk
}

// HashAggFinalWorker does the following things:
// 1. accept and decode the encoded intermediate results from partial workers parallelly
// 2. calculate the final result
// 3. transfer the final result to the main thread
type HashAggFinalWorker struct {
	aggWorker

	rowBuffer  []types.Datum
	mutableRow chunk.MutRow

	inputCh             chan []*AfInterResult
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk
	giveBackCh          chan<- []*AfInterResult
}

// AfFinalResult indicates aggregation functions final result.
type AfFinalResult struct {
	chk *chunk.Chunk
	err error

	giveBackCh chan *chunk.Chunk
}

// HashAggExec deals with all the aggregate functions.
// It is built from the Aggregate Plan. When Next() is called, it reads all the data from Src
// and updates all the items in AggFuncs.
type HashAggExec struct {
	baseExecutor

	prepared      bool
	sc            *stmtctx.StatementContext
	AggFuncs      []aggregation.Aggregation
	aggCtxsMap    aggCtxsMapper
	groupMap      *mvmap.MVMap
	groupIterator *mvmap.Iterator
	mutableRow    chunk.MutRow
	rowBuffer     []types.Datum
	GroupByItems  []expression.Expression
	groupKey      []byte
	groupVals     [][]byte

	// After we support parallel execution for aggregation functions with distinct,
	// we can remove this attribute.
	doesUnparallelExec bool

	finishCh                   chan struct{}
	finalOutputCh              chan *AfFinalResult
	partialInterResultHolderCh chan []*AfInterResult
	partialOutputChs           []chan []*AfInterResult
	inputCh                    chan *chunk.Chunk
	partialConcurrency         int
	finalConcurrency           int
	partialWorkers             []HashAggPartialWorker
	finalWorkers               []HashAggFinalWorker
	partialWorkerWaitGroup     *sync.WaitGroup
	finalWorkerWaitGroup       *sync.WaitGroup
	defaultVal                 *chunk.Chunk
	// isInputNull indicates whether the child only returns empty input.
	isInputNull bool
}

// AfInterResult indicates the intermediate result of aggPartialWorker.
type AfInterResult struct {
	GroupKey    []byte
	InterResult [][]byte
}

// ToRows decodes the InterResult into []types.Row.
func (r *AfInterResult) ToRows(fts []*types.FieldType, size int, loc *time.Location) (result []types.Row, err error) {
	// Sql like select 11 from t order by a;
	// If it use hash agg, the agg funcs would be nil, thus fts would be nil.
	if len(fts) == 0 {
		return
	}
	for _, interResult := range r.InterResult {
		row, err := codec.Decode(interResult, size)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row, err = tablecodec.UnflattenDatums(row, fts, loc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result = append(result, row)
	}
	return result, nil
}

// Close implements the Executor Close interface.
func (e *HashAggExec) Close() error {
	if e.doesUnparallelExec {
		e.groupMap = nil
		e.groupIterator = nil
		e.aggCtxsMap = nil
	} else {
		// `Close` may be called after `Open` without calling `Next` in test.
		if !e.prepared {
			close(e.inputCh)
			close(e.partialInterResultHolderCh)
			for _, ch := range e.partialOutputChs {
				close(ch)
			}
			close(e.finalOutputCh)
		}
		close(e.finishCh)
		for _, ch := range e.partialOutputChs {
			for range ch {
			}
		}
		for range e.finalOutputCh {
		}
	}
	return errors.Trace(e.baseExecutor.Close())
}

// Open implements the Executor Open interface.
func (e *HashAggExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.prepared = false

	if e.doesUnparallelExec { // TODO: StatsInfo.Count < e.partialConcurrency * MaxChunkSize
		e.initForUnparallelExec()
		return nil
	}
	e.initForParallelExec()
	return nil
}

func (e *HashAggExec) initForUnparallelExec() {
	e.groupMap = mvmap.NewMVMap()
	e.groupIterator = e.groupMap.NewIterator()
	e.aggCtxsMap = make(aggCtxsMapper, 0)
	e.mutableRow = chunk.MutRowFromTypes(e.retTypes())
	e.rowBuffer = make([]types.Datum, 0, e.Schema().Len())
	e.groupKey = make([]byte, 0, 8)
	e.groupVals = make([][]byte, 0, 8)
}

func (e *HashAggExec) initForParallelExec() {
	e.isInputNull = true
	e.partialWorkerWaitGroup = &sync.WaitGroup{}
	e.finalWorkerWaitGroup = &sync.WaitGroup{}
	e.finalOutputCh = make(chan *AfFinalResult, e.finalConcurrency)
	e.inputCh = make(chan *chunk.Chunk, e.partialConcurrency)
	for i := 0; i < e.partialConcurrency; i++ {
		e.inputCh <- e.children[0].newChunk()
	}
	e.finishCh = make(chan struct{}, 1)

	e.partialOutputChs = make([]chan []*AfInterResult, e.finalConcurrency)
	for i := range e.partialOutputChs {
		e.partialOutputChs[i] = make(chan []*AfInterResult, e.partialConcurrency)
	}

	e.partialInterResultHolderCh = make(chan []*AfInterResult, e.finalConcurrency*e.partialConcurrency)
	for i := 0; i < cap(e.partialInterResultHolderCh); i++ {
		e.partialInterResultHolderCh <- make([]*AfInterResult, 0, e.maxChunkSize)
	}

	e.partialWorkers = make([]HashAggPartialWorker, e.partialConcurrency)
	e.finalWorkers = make([]HashAggFinalWorker, e.finalConcurrency)
	// Init partial workers.
	for i := 0; i < e.partialConcurrency; i++ {
		e.partialWorkers[i] = HashAggPartialWorker{
			aggWorker: aggWorker{
				finishCh:     e.finishCh,
				aggFuncs:     e.AggFuncs,
				groupByItems: e.GroupByItems,
				aggCtxsMap:   make(aggCtxsMapper, 0),
				maxChunkSize: e.maxChunkSize,
				WaitGroup:    e.partialWorkerWaitGroup,
				groupMap:     mvmap.NewMVMap(),
				groupVals:    make([][]byte, 0, 8),
			},
			inputCh:             make(chan *chunk.Chunk, 1),
			interResultHolderCh: e.partialInterResultHolderCh,
			outputChs:           e.partialOutputChs,
			giveBackCh:          e.inputCh,
			globalOutputCh:      e.finalOutputCh,
		}
	}

	// Init final workers.
	finalAggFuncs := e.newFinalAggFuncs()
	for i := 0; i < e.finalConcurrency; i++ {
		e.finalWorkers[i] = HashAggFinalWorker{
			aggWorker: aggWorker{
				finishCh:     e.finishCh,
				aggFuncs:     finalAggFuncs,
				aggCtxsMap:   make(aggCtxsMapper, 0),
				maxChunkSize: e.maxChunkSize,
				WaitGroup:    e.finalWorkerWaitGroup,
				groupMap:     mvmap.NewMVMap(),
				groupVals:    make([][]byte, 0, 8),
			},
			inputCh:             e.partialOutputChs[i],
			outputCh:            e.finalOutputCh,
			finalResultHolderCh: make(chan *chunk.Chunk, 1),
			giveBackCh:          e.partialInterResultHolderCh,
			rowBuffer:           make([]types.Datum, 0, e.Schema().Len()),
			mutableRow:          chunk.MutRowFromTypes(e.retTypes()),
		}
		e.finalWorkers[i].finalResultHolderCh <- e.newChunk()
	}
}

func (e *HashAggExec) newFinalAggFuncs() (newAggFuncs []aggregation.Aggregation) {
	newAggFuncs = make([]aggregation.Aggregation, 0, len(e.AggFuncs))
	idx := 0
	for _, af := range e.AggFuncs {
		var aggFunc aggregation.Aggregation
		idx, aggFunc = af.GetFinalAggFunc(idx)
		newAggFuncs = append(newAggFuncs, aggFunc)
	}
	return
}

// HashAggPartialWorker gets and handles origin data or partial data from inputCh,
// then shuffle the intermediate results to corresponded final workers.
func (w *HashAggPartialWorker) run(ctx sessionctx.Context, finalConcurrency int) {
	defer func() {
		w.WaitGroup.Done()
	}()
	var (
		chk              *chunk.Chunk
		ok               bool
		sc               = ctx.GetSessionVars().StmtCtx
		processedRowsNum = 0
	)
	for {
		select {
		case <-w.finishCh:
			return
		case chk, ok = <-w.inputCh:
		}
		if !ok {
			if processedRowsNum > 0 {
				interResults, ok, err := w.shuffleInterResult(sc, finalConcurrency)
				if !ok || err != nil {
					w.globalOutputCh <- &AfFinalResult{err: errors.Trace(err)}
					return
				}
				for i := range interResults {
					w.outputChs[i] <- interResults[i]
				}
			}
			break
		}
		inputIter := chunk.NewIterator4Chunk(chk)
		for row := inputIter.Begin(); row != inputIter.End(); row = inputIter.Next() {
			groupKey, err := w.getGroupKey(ctx, row)
			if err != nil {
				w.globalOutputCh <- &AfFinalResult{err: errors.Trace(err)}
				return
			}
			if len(w.groupMap.Get(groupKey, w.groupVals[:0])) == 0 {
				w.groupMap.Put(groupKey, []byte{})
			}
			aggEvalCtxs := w.getContext(ctx, groupKey)
			for i, af := range w.aggFuncs {
				err = af.Update(aggEvalCtxs[i], sc, row)
				if err != nil {
					w.globalOutputCh <- &AfFinalResult{err: errors.Trace(err)}
				}
			}
		}
		processedRowsNum += chk.NumRows()
		if chk.NumRows() == 0 || processedRowsNum >= finalConcurrency*w.maxChunkSize {
			processedRowsNum = 0
			interResults, ok, err := w.shuffleInterResult(sc, finalConcurrency)
			if !ok || err != nil {
				w.globalOutputCh <- &AfFinalResult{err: errors.Trace(err)}
				return
			}
			for i := range interResults {
				w.outputChs[i] <- interResults[i]
			}
		}
		w.giveBackCh <- chk
	}
}

// shuffleInterResult shuffles the intermediate results of partial workers to corresponded final workers.
func (w *HashAggPartialWorker) shuffleInterResult(sc *stmtctx.StatementContext, concurrency int) ([][]*AfInterResult, bool, error) {
	result := make([][]*AfInterResult, concurrency)
	ok := false
	for i := range result {
		result[i], ok = <-w.interResultHolderCh
		if !ok {
			return nil, false, nil
		}
		result[i] = result[i][:0]
	}

	groupIter := w.groupMap.NewIterator()
	for groupKey, _ := groupIter.Next(); groupKey != nil; groupKey, _ = groupIter.Next() {
		aggEvalCtx := w.aggCtxsMap[string(groupKey)]
		interResult := &AfInterResult{}
		interResult.GroupKey = groupKey
		var value []byte
		idx := int(murmur3.Sum32(interResult.GroupKey)) % concurrency
		for i, f := range w.aggFuncs {
			// todo: use HashChunkRow
			r, err := f.GetInterResult(aggEvalCtx[i], sc)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			value = append(value, r...)
			f.ResetContext(sc, aggEvalCtx[i])
		}
		interResult.InterResult = append(interResult.InterResult, value)
		result[idx] = append(result[idx], interResult)
	}
	w.groupMap = mvmap.NewMVMap()
	return result, true, nil
}

// getGroupKey evaluates the group items and args of aggregate functions.
func (w aggWorker) getGroupKey(ctx sessionctx.Context, row chunk.Row) ([]byte, error) {
	vals := make([]types.Datum, 0, len(w.groupByItems))
	for _, item := range w.groupByItems {
		v, err := item.Eval(row)
		if item.GetType().Tp == mysql.TypeNewDecimal {
			v.SetLength(0)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	var err error
	w.groupKey, err = codec.EncodeValue(ctx.GetSessionVars().StmtCtx, w.groupKey[:0], vals...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return w.groupKey, nil
}

func (w aggWorker) getContext(ctx sessionctx.Context, groupKey []byte) []*aggregation.AggEvaluateContext {
	groupKeyString := string(groupKey)
	aggCtxs, ok := w.aggCtxsMap[groupKeyString]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(w.aggFuncs))
		for _, af := range w.aggFuncs {
			aggCtxs = append(aggCtxs, af.CreateContext(ctx.GetSessionVars().StmtCtx))
		}
		w.aggCtxsMap[groupKeyString] = aggCtxs
	}
	return aggCtxs
}

func (w *HashAggFinalWorker) run(ctx sessionctx.Context) {
	defer func() {
		w.WaitGroup.Done()
	}()
	var (
		input  []*AfInterResult
		ok     bool
		result *chunk.Chunk
		fts    = make([]*types.FieldType, 0, len(w.aggFuncs))
		sc     = ctx.GetSessionVars().StmtCtx
		loc    = sc.TimeZone
	)
	for _, f := range w.aggFuncs {
		for _, arg := range f.GetArgs() {
			if _, ok := arg.(*expression.Constant); !ok {
				fts = append(fts, arg.GetType())
			}
		}
	}
	for {
		select {
		case <-w.finishCh:
			return
		case input, ok = <-w.inputCh:
		}
		if !ok {
			break
		}

		for _, interResult := range input {
			groupKey := interResult.GroupKey
			if len(w.groupMap.Get(groupKey, w.groupVals[:0])) == 0 {
				w.groupMap.Put(groupKey, []byte{})
			}
			aggEvalCtxs := w.getContext(ctx, groupKey)
			// todo: use new decoder
			rows, err := interResult.ToRows(fts, len(w.aggFuncs), loc)
			if err != nil {
				w.outputCh <- &AfFinalResult{err: errors.Trace(err)}
				return
			}
			for _, row := range rows {
				for i, af := range w.aggFuncs {
					err = af.Update(aggEvalCtxs[i], sc, row)
					if err != nil {
						w.outputCh <- &AfFinalResult{err: errors.Trace(err)}
						return
					}
				}
			}
		}
		w.giveBackCh <- input
	}
	groupIter := w.groupMap.NewIterator()
	result, ok = <-w.finalResultHolderCh
	if !ok {
		return
	}
	result.Reset()
	for {
		groupKey, _ := groupIter.Next()
		if groupKey == nil {
			if result.NumRows() > 0 {
				w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
			}
			return
		}
		aggCtxs := w.getContext(ctx, groupKey)
		w.rowBuffer = w.rowBuffer[:0]
		for i, af := range w.aggFuncs {
			w.rowBuffer = append(w.rowBuffer, af.GetResult(aggCtxs[i]))
		}
		w.mutableRow.SetDatums(w.rowBuffer...)
		result.AppendRow(w.mutableRow.ToRow())
		if result.NumRows() == w.maxChunkSize {
			w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
			result, ok = <-w.finalResultHolderCh
			if !ok {
				return
			}
			result.Reset()
		}
	}
}

// Next implements the Executor Next interface.
func (e *HashAggExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.doesUnparallelExec {
		return errors.Trace(e.unparallelExec(ctx, chk))
	}
	return errors.Trace(e.parallelExec(ctx, chk))
}

func (e *HashAggExec) fetchChildData(ctx context.Context) {
	var (
		chk *chunk.Chunk
		ok  bool
		err error
	)
	defer func() {
		for _, w := range e.partialWorkers {
			close(w.inputCh)
		}
	}()
	for i := 0; ; i++ {
		select {
		case <-e.finishCh:
			return
		case chk, ok = <-e.inputCh:
			if !ok {
				return
			}
		}
		err = e.children[0].Next(ctx, chk)
		if err != nil {
			e.finalOutputCh <- &AfFinalResult{err: errors.Trace(err)}
			return
		}
		if chk.NumRows() == 0 {
			return
		}
		e.partialWorkers[i%e.partialConcurrency].inputCh <- chk
	}
}

func (e *HashAggExec) waitPartialWorkerAndCloseOutputChs() {
	e.partialWorkerWaitGroup.Wait()
	for _, ch := range e.partialOutputChs {
		close(ch)
	}
}

func (e *HashAggExec) waitFinalWorkerAndCloseFinalOutput() {
	e.finalWorkerWaitGroup.Wait()
	close(e.finalOutputCh)
}

// parallelExec executes hash aggregate algorithm parallelly.
func (e *HashAggExec) parallelExec(ctx context.Context, chk *chunk.Chunk) error {
	if !e.prepared {
		go e.fetchChildData(ctx)
		for i := range e.partialWorkers {
			e.partialWorkerWaitGroup.Add(1)
			go e.partialWorkers[i].run(e.ctx, e.finalConcurrency)
		}
		go e.waitPartialWorkerAndCloseOutputChs()
		for i := range e.finalWorkers {
			e.finalWorkerWaitGroup.Add(1)
			go e.finalWorkers[i].run(e.ctx)
		}
		go e.waitFinalWorkerAndCloseFinalOutput()
		e.prepared = true
	}
	for {
		result, ok := <-e.finalOutputCh
		if !ok || result.err != nil || result.chk.NumRows() == 0 {
			if result != nil {
				return errors.Trace(result.err)
			}
			if e.isInputNull && e.defaultVal != nil {
				chk.Append(e.defaultVal, 0, 1)
			}
			e.isInputNull = false
			return nil
		}
		e.isInputNull = false
		chk.SwapColumns(result.chk)
		// Put result.chk back to the corresponded final worker's finalResultHolderCh.
		result.giveBackCh <- result.chk
		// todo: store the result that chk.numrows() < e.maxChunkSize
		if chk.NumRows() > 0 {
			break
		}
	}
	return nil
}

// unparallelExec executes hash aggregation algorithm in single thread.
func (e *HashAggExec) unparallelExec(ctx context.Context, chk *chunk.Chunk) error {
	// In this stage we consider all data from src as a single group.
	if !e.prepared {
		err := e.execute(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if (e.groupMap.Len() == 0) && len(e.GroupByItems) == 0 {
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one row [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.groupMap.Put([]byte{}, []byte{})
		}
		e.prepared = true
	}
	chk.Reset()
	for {
		groupKey, _ := e.groupIterator.Next()
		if groupKey == nil {
			return nil
		}
		aggCtxs := e.getContexts(groupKey)
		e.rowBuffer = e.rowBuffer[:0]
		for i, af := range e.AggFuncs {
			e.rowBuffer = append(e.rowBuffer, af.GetResult(aggCtxs[i]))
		}
		e.mutableRow.SetDatums(e.rowBuffer...)
		chk.AppendRow(e.mutableRow.ToRow())
		if chk.NumRows() == e.maxChunkSize {
			return nil
		}
	}
}

// execute fetches Chunks from src and update each aggregate function for each row in Chunk.
func (e *HashAggExec) execute(ctx context.Context) (err error) {
	inputIter := chunk.NewIterator4Chunk(e.childrenResults[0])
	for {
		err := e.children[0].Next(ctx, e.childrenResults[0])
		if err != nil {
			return errors.Trace(err)
		}
		// no more data.
		if e.childrenResults[0].NumRows() == 0 {
			return nil
		}
		for row := inputIter.Begin(); row != inputIter.End(); row = inputIter.Next() {
			groupKey, err := e.getGroupKey(row)
			if err != nil {
				return errors.Trace(err)
			}
			if len(e.groupMap.Get(groupKey, e.groupVals[:0])) == 0 {
				e.groupMap.Put(groupKey, []byte{})
			}
			aggCtxs := e.getContexts(groupKey)
			for i, af := range e.AggFuncs {
				err = af.Update(aggCtxs[i], e.sc, row)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

func (e *HashAggExec) getGroupKey(row chunk.Row) ([]byte, error) {
	vals := make([]types.Datum, 0, len(e.GroupByItems))
	for _, item := range e.GroupByItems {
		v, err := item.Eval(row)
		if item.GetType().Tp == mysql.TypeNewDecimal {
			v.SetLength(0)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, v)
	}
	var err error
	e.groupKey, err = codec.EncodeValue(e.sc, e.groupKey[:0], vals...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e.groupKey, nil
}

func (e *HashAggExec) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	groupKeyString := string(groupKey)
	aggCtxs, ok := e.aggCtxsMap[groupKeyString]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.AggFuncs))
		for _, af := range e.AggFuncs {
			aggCtxs = append(aggCtxs, af.CreateContext(e.ctx.GetSessionVars().StmtCtx))
		}
		e.aggCtxsMap[groupKeyString] = aggCtxs
	}
	return aggCtxs
}

// StreamAggExec deals with all the aggregate functions.
// It assumes all the input data is sorted by group by key.
// When Next() is called, it will return a result for the same group.
type StreamAggExec struct {
	baseExecutor

	executed     bool
	hasData      bool
	StmtCtx      *stmtctx.StatementContext
	AggFuncs     []aggregation.Aggregation
	aggCtxs      []*aggregation.AggEvaluateContext
	GroupByItems []expression.Expression
	curGroupKey  []types.Datum
	tmpGroupKey  []types.Datum

	// for chunk execution.
	inputIter  *chunk.Iterator4Chunk
	inputRow   chunk.Row
	mutableRow chunk.MutRow
	rowBuffer  []types.Datum
}

// Open implements the Executor Open interface.
func (e *StreamAggExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	e.executed = false
	e.hasData = false
	e.inputIter = chunk.NewIterator4Chunk(e.childrenResults[0])
	e.inputRow = e.inputIter.End()
	e.mutableRow = chunk.MutRowFromTypes(e.retTypes())
	e.rowBuffer = make([]types.Datum, 0, e.Schema().Len())

	e.aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.AggFuncs))
	for _, agg := range e.AggFuncs {
		e.aggCtxs = append(e.aggCtxs, agg.CreateContext(e.ctx.GetSessionVars().StmtCtx))
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *StreamAggExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()

	for !e.executed && chk.NumRows() < e.maxChunkSize {
		err := e.consumeOneGroup(ctx, chk)
		if err != nil {
			e.executed = true
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *StreamAggExec) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) error {
	for !e.executed {
		if err := e.fetchChildIfNecessary(ctx, chk); err != nil {
			return errors.Trace(err)
		}
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			meetNewGroup, err := e.meetNewGroup(e.inputRow)
			if err != nil {
				return errors.Trace(err)
			}
			if meetNewGroup {
				e.appendResult2Chunk(chk)
			}
			for i, af := range e.AggFuncs {
				err := af.Update(e.aggCtxs[i], e.StmtCtx, e.inputRow)
				if err != nil {
					return errors.Trace(err)
				}
			}
			if meetNewGroup {
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
	}
	return nil
}

func (e *StreamAggExec) fetchChildIfNecessary(ctx context.Context, chk *chunk.Chunk) error {
	if e.inputRow != e.inputIter.End() {
		return nil
	}

	err := e.children[0].Next(ctx, e.childrenResults[0])
	if err != nil {
		return errors.Trace(err)
	}
	// No more data.
	if e.childrenResults[0].NumRows() == 0 {
		if e.hasData || len(e.GroupByItems) == 0 {
			e.appendResult2Chunk(chk)
		}
		e.executed = true
		return nil
	}

	// Reach here, "e.childrenResults[0].NumRows() > 0" is guaranteed.
	e.inputRow = e.inputIter.Begin()
	e.hasData = true
	return nil
}

// appendResult2Chunk appends result of all the aggregation functions to the
// result chunk, and reset the evaluation context for each aggregation.
func (e *StreamAggExec) appendResult2Chunk(chk *chunk.Chunk) {
	e.rowBuffer = e.rowBuffer[:0]
	for i, af := range e.AggFuncs {
		e.rowBuffer = append(e.rowBuffer, af.GetResult(e.aggCtxs[i]))
		af.ResetContext(e.ctx.GetSessionVars().StmtCtx, e.aggCtxs[i])
	}
	e.mutableRow.SetDatums(e.rowBuffer...)
	chk.AppendRow(e.mutableRow.ToRow())
}

// meetNewGroup returns a value that represents if the new group is different from last group.
func (e *StreamAggExec) meetNewGroup(row chunk.Row) (bool, error) {
	if len(e.GroupByItems) == 0 {
		return false, nil
	}
	e.tmpGroupKey = e.tmpGroupKey[:0]
	matched, firstGroup := true, false
	if len(e.curGroupKey) == 0 {
		matched, firstGroup = false, true
	}
	for i, item := range e.GroupByItems {
		v, err := item.Eval(row)
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			c, err := v.CompareDatum(e.StmtCtx, &e.curGroupKey[i])
			if err != nil {
				return false, errors.Trace(err)
			}
			matched = c == 0
		}
		e.tmpGroupKey = append(e.tmpGroupKey, v)
	}
	if matched {
		return false, nil
	}
	e.curGroupKey = e.curGroupKey[:0]
	for _, v := range e.tmpGroupKey {
		e.curGroupKey = append(e.curGroupKey, *((&v).Copy()))
	}
	return !firstGroup, nil
}

package executor

import (
	"context"
	"github.com/pingcap/tidb/util/codec"
	"reflect"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var _ Executor = &ExchangeSender{}
var _ Executor = &ExchangeReceiver{}
var _ Executor = &ExchangeReceiverFullMerge{}
var _ Executor = &ExchangeSenderBroadcast{}
var _ Executor = &ExchangeSenderBroadcastHT{}
var _ Executor = &ExchangeReceiverPassThroughHT{}
var _ Executor = &ExchangeReceiverPassThrough{}

func IsExchangeSender(e Executor) bool {
	switch e.(type) {
	case *ExchangeSenderBroadcast, *ExchangeSenderPassThrough, *ExchangeSenderRandom, *ExchangeSenderBroadcastHT, *ExchangeSenderHash:
		return true
	default:
		return false
	}
}

type ExchangeSender struct {
	baseExecutor
	opened bool
}

type ExchangeReceiver struct {
	baseExecutor
	opened bool
}

// ExchangeReceiverFullMerge merge N input streams into one.
type ExchangeReceiverFullMerge struct {
	ExchangeReceiver

	chkChs          []chan *chunk.Chunk
	resChs          []chan *chunk.Chunk
	recvSelectCases []reflect.SelectCase
}

func (e *ExchangeReceiverFullMerge) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.recvSelectCases = make([]reflect.SelectCase, len(e.resChs))
	for i, ch := range e.resChs {
		e.recvSelectCases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	chanSize := cap(e.chkChs[0])
	for i := 0; i < len(e.chkChs); i++ {
		for j := 0; j < chanSize; j++ {
			chk := newFirstChunk(e.children[i])
			e.chkChs[i] <- chk
		}
	}
	return nil
}

func (e *ExchangeReceiverFullMerge) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	for {
		if len(e.recvSelectCases) == 0 {
			break
		}

		chosen, value, ok := reflect.Select(e.recvSelectCases)

		if ok {
			tmpChk := value.Interface().(*chunk.Chunk)
			req.SwapColumns(tmpChk)
			e.chkChs[chosen] <- tmpChk
			break
		}
		e.recvSelectCases = append(e.recvSelectCases[:chosen], e.recvSelectCases[chosen+1:]...)
		clearChan(e.chkChs[chosen])
		close(e.chkChs[chosen])
		e.chkChs = append(e.chkChs[:chosen], e.chkChs[chosen+1:]...)
	}
	return nil
}

func clearChan(ch chan *chunk.Chunk) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func (e *ExchangeReceiverFullMerge) Close() error {
	e.resChs = nil
	e.chkChs = nil
	e.recvSelectCases = nil
	return e.baseExecutor.Close()
}

// ExchangeSenderBroadcast broadcast one input stream to all outputs.
type ExchangeSenderBroadcast struct {
	ExchangeSender

	outputs []chan *chunk.Chunk
}

func (e *ExchangeSenderBroadcast) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeSenderBroadcast) closeOutputs() {
	for _, ch := range e.outputs {
		close(ch)
	}
}

func (e *ExchangeSenderBroadcast) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if err := Next(ctx, e.children[0], req); err != nil {
		e.closeOutputs()
		return err
	}
	if req.NumRows() == 0 {
		e.closeOutputs()
		// TODO: another way to indicates done
		return errors.New("sender broadcast done")
	}
	for i := 1; i < len(e.outputs); i++ {
		// TODO: this is dangerous
		e.outputs[i] <- req.CopyConstruct()
	}
	e.outputs[0] <- req
	return nil
}

func (e *ExchangeSenderBroadcast) Close() error {
	return e.baseExecutor.Close()
}

type ExchangeSenderPassThrough struct {
	ExchangeSender

	resCh chan *chunk.Chunk
	chkCh chan *chunk.Chunk
}

func (e *ExchangeSenderPassThrough) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeSenderPassThrough) Next(ctx context.Context, req *chunk.Chunk) error {
	chk := <-e.chkCh
	if err := Next(ctx, e.children[0], chk); err != nil {
		return err
	}
	if chk.NumRows() == 0 {
		close(e.resCh)
		// TODO: another way
		return errors.New("sender pass through done")
	}
	e.resCh <- chk
	return nil
}

func (e *ExchangeSenderPassThrough) Close() error {
	e.resCh = nil
	e.chkCh = nil
	return e.baseExecutor.Close()
}

type ExchangeReceiverPassThrough struct {
	ExchangeReceiver

	chkCh chan *chunk.Chunk
	resCh chan *chunk.Chunk
}

func (e *ExchangeReceiverPassThrough) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	return nil
}

func (e *ExchangeReceiverPassThrough) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	e.chkCh <- req
	_, ok := <-e.resCh
	if !ok {
		clearChan(e.chkCh)
		close(e.chkCh)
	}
	return nil
}

func (e *ExchangeReceiverPassThrough) Close() error {
	return e.baseExecutor.Close()
}

type ExchangeSenderRandom struct {
	ExchangeSender

	chkChs          []chan *chunk.Chunk
	resChs          []chan *chunk.Chunk
	childResult     *chunk.Chunk
	recvSelectCases []reflect.SelectCase
	// TODO: opened closed, make in base struct
	closed bool
}

func (e *ExchangeSenderRandom) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	e.recvSelectCases = make([]reflect.SelectCase, len(e.chkChs))
	for i, ch := range e.chkChs {
		e.recvSelectCases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	e.childResult = newFirstChunk(e)
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeSenderRandom) closeOutputs() {
	for _, ch := range e.resChs {
		close(ch)
	}
}

func (e *ExchangeSenderRandom) Next(ctx context.Context, _ *chunk.Chunk) error {
	if err := Next(ctx, e.children[0], e.childResult); err != nil {
		e.closeOutputs()
		return err
	}
	if e.childResult.NumRows() == 0 {
		e.closeOutputs()
		return errors.New("sender random done")
	}
	chosen, value, ok := reflect.Select(e.recvSelectCases)
	if ok {
		tmpChk := value.Interface().(*chunk.Chunk)
		e.childResult.SwapColumns(tmpChk)
		e.resChs[chosen] <- tmpChk
	} else {
		panic("ExchangeSenderRandom must be ok to read chkChs")
	}
	return nil
}

func (e *ExchangeSenderRandom) Close() error {
	if !e.closed {
		e.closed = true
		return e.baseExecutor.Close()
	}
	return nil
}

type ExchangeSenderBroadcastHT struct {
	ExchangeSender

	outputs []chan *chunk.Chunk

	ht *HashRowContainer

	buildSideEstCount float64
	buildKeys         []*expression.Column
	buildTypes        []*types.FieldType
	useOuterToBuild   bool
	isNullEQ          []bool
	closed            bool
}

func (e *ExchangeSenderBroadcastHT) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true

	if e.useOuterToBuild {
		panic("not implemented yet")
	}

	buildKeyColIdx := make([]int, len(e.buildKeys))
	for i := range e.buildKeys {
		buildKeyColIdx[i] = e.buildKeys[i].Index
	}
	hCtx := &hashContext{
		allTypes:  e.buildTypes,
		keyColIdx: buildKeyColIdx,
	}
	e.ht = newHashRowContainerMultiple(e.ctx, int(e.buildSideEstCount), hCtx, len(e.outputs))
	htSlice, ok := e.ctx.GetSessionVars().StmtCtx.BroadcastHT.([]*HashRowContainer)
	if !ok {
		panic("unexpected htSlice")
	}
	htSlice = append(htSlice, e.ht)
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeSenderBroadcastHT) sendAndCloseOutputs() {
	for _, ch := range e.outputs {
		ch <- (*chunk.Chunk)(unsafe.Pointer(e.ht))
		close(ch)
	}
}

func (e *ExchangeSenderBroadcastHT) Next(ctx context.Context, req *chunk.Chunk) error {
	chkChs := make([]chan *chunk.Chunk, 0, len(e.children))
	for i := 0; i < len(e.children); i++ {
		chkChs = append(chkChs, make(chan *chunk.Chunk, 100))
	}
	recvSelectCases := make([]reflect.SelectCase, 0, len(e.children))
	for _, ch := range chkChs {
		recvSelectCases = append(recvSelectCases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
	}

	for i := 0; i < len(e.children); i++ {
		go func(childIdx int) {
			for {
				chk := chunk.NewChunkWithCapacity(e.base().retFieldTypes, e.ctx.GetSessionVars().MaxChunkSize)
				if err := Next(ctx, e.children[childIdx], chk); err != nil {
					// TODO: another way to indicates done
					panic("got error in BroadcastHT")
				}
				if chk.NumRows() == 0 {
					close(chkChs[childIdx])
					break
				}

				chkChs[childIdx] <- chk
			}
		}(i)
	}

	for {
		if len(recvSelectCases) == 0 {
			break
		}

		chosen, value, ok := reflect.Select(recvSelectCases)

		if ok {
			tmpChk := value.Interface().(*chunk.Chunk)
			if err := e.ht.PutChunk(tmpChk, e.isNullEQ); err != nil {
				panic("put chunk error")
			}
			continue
		}
		recvSelectCases = append(recvSelectCases[:chosen], recvSelectCases[chosen+1:]...)
	}

	e.sendAndCloseOutputs()
	return errors.New("ExchangeSenderBroadcastHT done")
}

func (e *ExchangeSenderBroadcastHT) Close() error {
	if !e.closed {
		e.closed = true
		return e.baseExecutor.Close()
	}
	return nil
}

type ExchangeReceiverPassThroughHT struct {
	ExchangeReceiver

	// TODO change type to HT
	input chan *chunk.Chunk
}

func (e *ExchangeReceiverPassThroughHT) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeReceiverPassThroughHT) Next(ctx context.Context, req *chunk.Chunk) error {
	chk, ok := <-e.input
	if ok {
		htPtr := (*HashRowContainer)(unsafe.Pointer(chk))
		reqPtr := (**HashRowContainer)(unsafe.Pointer(req))
		*reqPtr = htPtr
	}
	return nil
}

func (e *ExchangeReceiverPassThroughHT) Close() error {
	return e.baseExecutor.Close()
}

type ExchangeSenderHash struct {
	ExchangeSender
	chkChs      []chan *chunk.Chunk
	resChs      []chan *chunk.Chunk
	handleFlags []bool

	childResult        *chunk.Chunk
	hashPartitionChunk []*chunk.Chunk
	hashColumns        []*expression.Column
	hashContext
}

func (e *ExchangeSenderHash) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	e.hashPartitionChunk = make([]*chunk.Chunk, len(e.chkChs))
	e.handleFlags = make([]bool, len(e.chkChs))
	for i := 0; i < len(e.handleFlags); i++ {
		e.handleFlags[i] = true
	}
	e.hashContext.allTypes = e.retFieldTypes
	for _, col := range e.hashColumns {
		e.hashContext.keyColIdx = append(e.hashContext.keyColIdx, col.Index)
	}
	e.childResult = newFirstChunk(e)
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeSenderHash) sendAndCloseOutputs() {
	for _, ch := range e.resChs {
		close(ch)
	}
}

func (e *ExchangeSenderHash) Next(ctx context.Context, _ *chunk.Chunk) error {
	if err := Next(ctx, e.children[0], e.childResult); err != nil {
		e.sendAndCloseOutputs()
		return err
	}
	if e.childResult.NumRows() == 0 {
		e.sendAndCloseOutputs()
		return errors.New("sender hashPartition done")
	}
	e.initHash(e.childResult.NumRows())
	// Hash
	for _, i := range e.hashContext.keyColIdx {
		err := codec.HashChunkColumns(e.ctx.GetSessionVars().StmtCtx, e.hashVals, e.childResult, e.allTypes[i], i, e.buf, e.hasNull)
		if err != nil {
			return err
		}
	}

	for i := 0; i < len(e.chkChs); i++ {
		if e.handleFlags[i] {
			e.hashPartitionChunk[i] = <-e.chkChs[i]
		}
	}

	for i := 0; i < len(e.handleFlags); i++ {
		e.handleFlags[i] = false
	}
	tmp := chunk.NewChunkWithOld(e.hashPartitionChunk[0])

	// Partition
	for i := 0; i < e.childResult.NumRows(); i++ {
		outputIndex := e.hashVals[i].Sum64() % uint64(len(e.resChs))
		e.hashPartitionChunk[outputIndex].AppendRow(e.childResult.GetRow(i))
		e.handleFlags[outputIndex] = true
	}
	for i := 0; i < len(e.resChs); i++ {
		if e.handleFlags[i] {
			e.resChs[i] <- e.hashPartitionChunk[i]
		}
	}
	e.childResult = tmp
	return nil
}

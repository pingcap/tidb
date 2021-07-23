package executor

import (
	"context"
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
	_, ok1 := e.(*ExchangeSenderBroadcast)
	_, ok2 := e.(*ExchangeSenderPassThrough)
	_, ok3 := e.(*ExchangeSenderRandom)
	_, ok4 := e.(*ExchangeSenderBroadcastHT)
	return ok1 || ok2 || ok3 || ok4
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

	inputs          []chan *chunk.Chunk
	sendSelectCases []reflect.SelectCase
}

func (e *ExchangeReceiverFullMerge) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.sendSelectCases = make([]reflect.SelectCase, len(e.inputs))
	for i := 0; i < len(e.inputs); i++ {
		e.sendSelectCases[i] = reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(e.inputs[i])}
	}
	// e.inputs should already be setup when building executor.
	return nil
}

func (e *ExchangeReceiverFullMerge) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	for i := 0; i < len(e.sendSelectCases); i++ {
		e.sendSelectCases[i].Send = reflect.ValueOf(req)
	}
	for {
		if len(e.sendSelectCases) == 0 {
			break
		}

		chosen, _, _ := reflect.Select(e.sendSelectCases)

		tmp := <-e.inputs[chosen]
		if tmp == nil {
			close(e.inputs[chosen])
			// remove channel.
			e.sendSelectCases = append(e.sendSelectCases[:chosen], e.sendSelectCases[chosen+1:]...)
			continue
		}
		break
	}
	return nil
}

func (e *ExchangeReceiverFullMerge) Close() error {
	e.inputs = nil
	e.sendSelectCases = nil
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

	output      chan *chunk.Chunk
	childResult *chunk.Chunk
}

func (e *ExchangeSenderPassThrough) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	e.childResult = newFirstChunk(e)
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeSenderPassThrough) Next(ctx context.Context, req *chunk.Chunk) error {
	if err := Next(ctx, e.children[0], e.childResult); err != nil {
		return err
	}
	if e.childResult.NumRows() == 0 {
		_ = <-e.output
		e.output <- nil
		// TODO: another way
		return errors.New("sender pass through done")
	}
	chk := <-e.output
	chk.SwapColumns(e.childResult)
	e.output <- chk
	return nil
}

func (e *ExchangeSenderPassThrough) Close() error {
	return e.baseExecutor.Close()
}

type ExchangeReceiverPassThrough struct {
	ExchangeReceiver

	input chan *chunk.Chunk
}

func (e *ExchangeReceiverPassThrough) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeReceiverPassThrough) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	chk, ok := <-e.input
	if ok {
		req.SwapColumns(chk)
	}
	return nil
}

func (e *ExchangeReceiverPassThrough) Close() error {
	return e.baseExecutor.Close()
}

type ExchangeSenderRandom struct {
	ExchangeSender

	outputs     []chan *chunk.Chunk
	selectCases []reflect.SelectCase
}

func (e *ExchangeSenderRandom) Open(ctx context.Context) error {
	if e.opened {
		return nil
	}
	e.opened = true
	e.selectCases = make([]reflect.SelectCase, len(e.outputs))
	for i := 0; i < len(e.outputs); i++ {
		e.selectCases[i] = reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(e.outputs[i])}
	}
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeSenderRandom) closeOutputs() {
	for _, ch := range e.outputs {
		close(ch)
	}
}

func (e *ExchangeSenderRandom) Next(ctx context.Context, req *chunk.Chunk) error {
	// req.Reset()
	chk := newFirstChunk(e)
	if err := Next(ctx, e.children[0], chk); err != nil {
		e.closeOutputs()
		return err
	}
	if chk.NumRows() == 0 {
		e.closeOutputs()
		return errors.New("sender random done")
	}
	for i := 0; i < len(e.outputs); i++ {
		e.selectCases[i].Send = reflect.ValueOf(chk)
	}
	_, _, _ = reflect.Select(e.selectCases)
	return nil
}

func (e *ExchangeSenderRandom) Close() error {
	return e.baseExecutor.Close()
}

type ExchangeSenderBroadcastHT struct {
	ExchangeSender

	outputs []chan *chunk.Chunk
	ht      *hashRowContainer

	buildSideEstCount float64
	buildKeys         []*expression.Column
	buildTypes        []*types.FieldType
	useOuterToBuild   bool
	isNullEQ          []bool

	childResult *chunk.Chunk
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
	e.ht = newHashRowContainer(e.ctx, int(e.buildSideEstCount), hCtx)
	e.childResult = newFirstChunk(e)
	return e.baseExecutor.Open(ctx)
}

func (e *ExchangeSenderBroadcastHT) sendAndCloseOutputs() {
	for _, ch := range e.outputs {
		ch <- (*chunk.Chunk)(unsafe.Pointer(e.ht))
		close(ch)
	}
}

func (e *ExchangeSenderBroadcastHT) Next(ctx context.Context, req *chunk.Chunk) error {
	if err := Next(ctx, e.children[0], e.childResult); err != nil {
		// TODO: another way to indicates done
		panic("go error in BroadcastHT")
	}
	if e.childResult.NumRows() == 0 {
		e.sendAndCloseOutputs()
		return errors.New("sender broadcast done")
	}

	tmp := chunk.NewChunkWithOld(e.childResult)
	if err := e.ht.PutChunk(e.childResult, e.isNullEQ); err != nil {
		panic("put chunk error")
	}
	e.childResult = tmp
	return nil
}

func (e *ExchangeSenderBroadcastHT) Close() error {
	return e.baseExecutor.Close()
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
		htPtr := (*hashRowContainer)(unsafe.Pointer(chk))
		reqPtr := (**hashRowContainer)(unsafe.Pointer(req))
		*reqPtr = htPtr
	}
	return nil
}

func (e *ExchangeReceiverPassThroughHT) Close() error {
	return e.baseExecutor.Close()
}

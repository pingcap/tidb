package pipeline

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
)

type onceDoneWaitGroup struct {
	whenDone func()
	once     sync.Once
}

func (od *onceDoneWaitGroup) done() {
	od.once.Do(func() {
		od.whenDone()
	})
}

type globalContext struct {
	context.Context
	wg    *sync.WaitGroup
	errCh chan error
}

func (gcx globalContext) EmitErr(err error) {
	// If it is already canceled, do nothing.
	if gcx.Err() == nil && errors.Cause(err) == context.Canceled {
		return
	}
	gcx.errCh <- err
}

type Manager[In, Out any] struct {
	cancel context.CancelFunc
	global globalContext
	input  chan<- In
	output <-chan Out
}

func Execute[In, Out any](ctx context.Context, pipe Worker[In, Out]) Manager[In, Out] {
	cx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 16)
	inputCh := make(chan In, 128)
	gcx := globalContext{
		Context: cx,
		errCh:   errCh,
		wg:      new(sync.WaitGroup),
	}
	wcx, output := spawnFrom[Out](gcx)
	mgr := Manager[In, Out]{
		cancel: cancel,
		global: gcx,
		input:  inputCh,
		output: output,
	}
	go pipe.MainLoop(wcx, inputCh)
	return mgr
}

func (m Manager[In, Out]) Output() <-chan Out {
	return m.output
}

func (m Manager[In, Out]) Input() chan<- In {
	return m.input
}

func (m Manager[In, Out]) Cancel() {
	m.cancel()
}

func (m Manager[In, Out]) Collect(ctx context.Context) ([]Out, error) {
	var (
		wgChan  = make(chan struct{})
		output  []Out
		outChan = m.Output()
	)

	close(m.Input())
	go func() {
		m.global.wg.Wait()
		close(wgChan)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-m.global.errCh:
			m.Cancel()
			return nil, err
		case <-wgChan:
			return output, nil
		case o, ok := <-outChan:
			if !ok {
				outChan = nil
			}
			output = append(output, o)
		}
	}
}

func spawnFrom[Out any](parent globalContext) (workerContext[Out], <-chan Out) {
	nextCh := make(chan Out, 16)
	parent.wg.Add(1)
	wkr := workerContext[Out]{
		globalContext: parent,
		nextCh:        nextCh,
		id:            uuid.NewString(),
		wgAndDone: &onceDoneWaitGroup{
			whenDone: func() {
				parent.wg.Done()
			},
		},
	}
	return wkr, nextCh
}

type workerContext[Out any] struct {
	globalContext

	id        string
	wgAndDone *onceDoneWaitGroup
	nextCh    chan<- Out
}

func (cx workerContext[Out]) Finish() {
	cx.wgAndDone.done()
	close(cx.nextCh)
}

func (cx workerContext[Out]) Emit(out Out) error {
	select {
	case <-cx.Done():
		return cx.Err()
	case cx.nextCh <- out:
	}
	return nil
}

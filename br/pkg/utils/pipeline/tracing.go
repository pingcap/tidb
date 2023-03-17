package pipeline

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	_ ContextWrapper[any] = traceContext[any]{}
)

type traceContext[Out any] struct {
	Context[Out]
	onProgress func()
	name       string
}

func (t traceContext[Out]) Emit(item Out) error {
	if err := t.Context.Emit(item); err != nil {
		return err
	}
	t.onProgress()
	return nil
}

func (t traceContext[Out]) EmitErr(err error) {
	t.Context.EmitErr(errors.Annotatef(err, "in the part %s", t.name))
}

func (t traceContext[Out]) Unwrap() Context[Out] {
	return t.Context
}

type traced[In, Out any] struct {
	inner Traceable[In, Out]

	current uint64
	max     uint64
	name    string
}

func Traced[In, Out any](t Traceable[In, Out]) Worker[In, Out] {
	return &traced[In, Out]{
		inner:   t,
		current: 0,
		max:     uint64(t.Size()),
		name:    t.Name(),
	}
}

func (t *traced[In, Out]) progressPrinter(ctx context.Context) {
	log.Info("Pipeline progress.", zap.Uint64("max", t.max), zap.Uint64("current", atomic.LoadUint64(&t.current)), zap.String("name", t.name))
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
			log.Info("Pipeline progress.", zap.Uint64("max", t.max), zap.Uint64("current", atomic.LoadUint64(&t.current)), zap.String("name", t.name))
		}
	}
}

func (t *traced[In, Out]) MainLoop(ctx Context[Out], input <-chan In) {
	cx := traceContext[Out]{
		Context: ctx,
		onProgress: func() {
			atomic.AddUint64(&t.current, 1)
		},
		name: t.name,
	}

	go t.progressPrinter(cx)
	t.inner.MainLoop(cx, input)
}

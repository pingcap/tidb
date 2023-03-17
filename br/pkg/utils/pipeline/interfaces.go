package pipeline

import (
	"context"
)

type Context[Out any] interface {
	context.Context

	Emit(out Out) error
	EmitErr(err error)
	Finish()
}

type ContextWrapper[Out any] interface {
	Unwrap() Context[Out]
}

func rootContext[T any](ctx Context[T]) Context[T] {
	if wctx, ok := ctx.(ContextWrapper[T]); ok {
		return rootContext(wctx.Unwrap())
	}
	return ctx
}

type Worker[In, Out any] interface {
	MainLoop(ctx Context[Out], input <-chan In)
}

type Traceable[In, Out any] interface {
	Worker[In, Out]

	Name() string
	Size() int
}

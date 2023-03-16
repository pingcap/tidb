package pipeline

import "context"

type Context[Out any] interface {
	context.Context

	Emit(out Out) error
	EmitErr(err error)
	Finish()
}

type Worker[In, Out any] interface {
	MainLoop(ctx Context[Out], input <-chan In)
}

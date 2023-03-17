package pipeline

func TryEmit[T any](cx Context[T], item T) bool {
	if err := cx.Emit(item); err != nil {
		cx.EmitErr(err)
		return false
	}
	return true
}

type cons[In, Mid, Out any] struct {
	car Worker[In, Mid]
	cdr Worker[Mid, Out]
}

func (l cons[In, Mid, Out]) MainLoop(ctx Context[Out], input <-chan In) {
	midCx, midCh := spawnFrom[Mid](rootContext(ctx).(workerContext[Out]).globalContext)
	go l.car.MainLoop(midCx, input)
	l.cdr.MainLoop(ctx, midCh)
}

func Append[In, Mid, Out any](car Worker[In, Mid], cdr Worker[Mid, Out]) Worker[In, Out] {
	return cons[In, Mid, Out]{
		car: car,
		cdr: cdr,
	}
}

type TransformFunc[In, Out any] func(In) Out

func (f TransformFunc[In, Out]) MainLoop(ctx Context[Out], input <-chan In) {
	cxf := CtxFunc[In, Out](func(ctx Context[Out], i In) (cont bool) {
		return TryEmit(ctx, f(i))
	})
	cxf.MainLoop(ctx, input)
}

func AppendFunc[In, Out, Out2 any](w Worker[In, Out], f func(Out) Out2) Worker[In, Out2] {
	return Append[In, Out, Out2](w, TransformFunc[Out, Out2](f))
}

type CtxFunc[In, Out any] func(Context[Out], In) (cont bool)

func (f CtxFunc[In, Out]) MainLoop(ctx Context[Out], input <-chan In) {
	defer ctx.Finish()

	for {
		select {
		case <-ctx.Done():
			ctx.EmitErr(ctx.Err())
			return
		case item, ok := <-input:
			if !ok {
				return
			}
			if !f(ctx, item) {
				return
			}
		}
	}
}

func AppendCtxFunc[In, Out, Out2 any](w Worker[In, Out], f func(Context[Out2], Out) bool) Worker[In, Out2] {
	return Append[In, Out, Out2](w, CtxFunc[Out, Out2](f))
}

// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package iter

import (
	"context"
	"fmt"
)

// IterResult is the result of try to advancing an impure iterator.
// Generally it is a "sum type", which only one field would be filled.
// You can create it via `Done`, `Emit` and `Throw`.
type IterResult[T any] struct {
	Item     T
	Err      error
	Finished bool
}

func (r IterResult[T]) String() string {
	if r.Err != nil {
		return fmt.Sprintf("IterResult.Throw(%s)", r.Err)
	}
	if r.Finished {
		return "IterResult.Done()"
	}
	return fmt.Sprintf("IterResult.Emit(%v)", r.Item)
}

// TryNextor is the general interface for "impure" iterators:
// which may trigger some error or block the caller when advancing.
type TryNextor[T any] interface {
	TryNext(ctx context.Context) IterResult[T]
}

func (r IterResult[T]) FinishedOrError() bool {
	return r.Err != nil || r.Finished
}

// DoneBy creates a finished or error IterResult by its argument.
func DoneBy[T, O any](r IterResult[O]) IterResult[T] {
	return IterResult[T]{
		Err:      r.Err,
		Finished: r.Finished,
	}
}

// Done creates an IterResult which indices the iteration has finished.
func Done[T any]() IterResult[T] {
	return IterResult[T]{
		Finished: true,
	}
}

// Emit creates an IterResult which contains normal data.
func Emit[T any](t T) IterResult[T] {
	return IterResult[T]{
		Item: t,
	}
}

// Throw creates an IterResult which contains the err.
func Throw[T any](err error) IterResult[T] {
	return IterResult[T]{
		Err: err,
	}
}

// CollectMany collects the first n items of the iterator.
// When the iterator contains less data than N, it emits as many items as it can and won't set `Finished`.
func CollectMany[T any](ctx context.Context, it TryNextor[T], n uint) IterResult[[]T] {
	return CollectAll(ctx, TakeFirst(it, n))
}

// CollectAll fully consumes the iterator, collecting all items the iterator emitted.
// When the iterator has been finished, it emits empty slice and won't set `Finished`.
func CollectAll[T any](ctx context.Context, it TryNextor[T]) IterResult[[]T] {
	r := IterResult[[]T]{}

	for ir := it.TryNext(ctx); !ir.Finished; ir = it.TryNext(ctx) {
		if ir.Err != nil {
			return DoneBy[[]T](ir)
		}
		r.Item = append(r.Item, ir.Item)
	}
	return r
}

type tap[T any] struct {
	inner TryNextor[T]

	tapper func(T)
}

func (t tap[T]) TryNext(ctx context.Context) IterResult[T] {
	n := t.inner.TryNext(ctx)
	if n.FinishedOrError() {
		return n
	}

	t.tapper(n.Item)
	return Emit(n.Item)
}

// Tap adds a hook into the iterator, it would execute the function
// anytime the iterator emits an item.
func Tap[T any](i TryNextor[T], with func(T)) TryNextor[T] {
	return tap[T]{
		inner:  i,
		tapper: with,
	}
}

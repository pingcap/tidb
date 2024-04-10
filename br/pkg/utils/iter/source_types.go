// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package iter

import (
	"context"

	"golang.org/x/exp/constraints"
)

type fromSlice[T any] []T

func (s *fromSlice[T]) TryNext(ctx context.Context) IterResult[T] {
	if s == nil || len(*s) == 0 {
		return Done[T]()
	}

	var item T
	item, *s = (*s)[0], (*s)[1:]
	return Emit(item)
}

type ofRange[T constraints.Integer] struct {
	end          T
	endExclusive bool

	current T
}

func (r *ofRange[T]) TryNext(ctx context.Context) IterResult[T] {
	if r.current > r.end || (r.current == r.end && r.endExclusive) {
		return Done[T]()
	}

	result := Emit(r.current)
	r.current++
	return result
}

type empty[T any] struct{}

func (empty[T]) TryNext(ctx context.Context) IterResult[T] {
	return Done[T]()
}

type failure[T any] struct {
	error
}

func (f failure[T]) TryNext(ctx context.Context) IterResult[T] {
	return Throw[T](f)
}

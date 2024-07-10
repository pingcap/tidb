// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package iter

import (
	"context"

	"github.com/pingcap/tidb/pkg/util"
	"golang.org/x/sync/errgroup"
)

type chunkMappingCfg struct {
	chunkSize uint
	quota     *util.WorkerPool
}

type chunkMapping[T, R any] struct {
	chunkMappingCfg
	inner  TryNextor[T]
	mapper func(context.Context, T) (R, error)

	buffer fromSlice[R]
}

func (m *chunkMapping[T, R]) fillChunk(ctx context.Context) IterResult[fromSlice[R]] {
	eg, cx := errgroup.WithContext(ctx)
	s := CollectMany(ctx, m.inner, m.chunkSize)
	if s.FinishedOrError() {
		return DoneBy[fromSlice[R]](s)
	}
	r := make([]R, len(s.Item))
	for i := 0; i < len(s.Item); i++ {
		i := i
		m.quota.ApplyOnErrorGroup(eg, func() error {
			var err error
			r[i], err = m.mapper(cx, s.Item[i])
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return Throw[fromSlice[R]](err)
	}
	if len(r) > 0 {
		return Emit(fromSlice[R](r))
	}
	return Done[fromSlice[R]]()
}

func (m *chunkMapping[T, R]) TryNext(ctx context.Context) IterResult[R] {
	r := m.buffer.TryNext(ctx)
	if !r.FinishedOrError() {
		return Emit(r.Item)
	}

	r2 := m.fillChunk(ctx)
	if !r2.FinishedOrError() {
		m.buffer = r2.Item
		return m.TryNext(ctx)
	}

	return DoneBy[R](r2)
}

type filter[T any] struct {
	inner       TryNextor[T]
	filterOutIf func(T) bool
}

func (f filter[T]) TryNext(ctx context.Context) IterResult[T] {
	for {
		r := f.inner.TryNext(ctx)
		if r.Err != nil || r.Finished || !f.filterOutIf(r.Item) {
			return r
		}
	}
}

type take[T any] struct {
	n     uint
	inner TryNextor[T]
}

func (t *take[T]) TryNext(ctx context.Context) IterResult[T] {
	if t.n == 0 {
		return Done[T]()
	}

	t.n--
	return t.inner.TryNext(ctx)
}

type join[T any] struct {
	inner TryNextor[TryNextor[T]]

	current TryNextor[T]
}

type pureMap[T, R any] struct {
	inner TryNextor[T]

	mapper func(T) R
}

func (p pureMap[T, R]) TryNext(ctx context.Context) IterResult[R] {
	r := p.inner.TryNext(ctx)

	if r.FinishedOrError() {
		return DoneBy[R](r)
	}
	return Emit(p.mapper(r.Item))
}

func (j *join[T]) TryNext(ctx context.Context) IterResult[T] {
	r := j.current.TryNext(ctx)
	if r.Err != nil {
		j.inner = empty[TryNextor[T]]{}
		return r
	}
	if !r.Finished {
		return r
	}

	nr := j.inner.TryNext(ctx)
	if nr.FinishedOrError() {
		return DoneBy[T](nr)
	}
	j.current = nr.Item
	return j.TryNext(ctx)
}

type withIndex[T any] struct {
	inner TryNextor[T]
	index int
}

func (wi *withIndex[T]) TryNext(ctx context.Context) IterResult[Indexed[T]] {
	r := wi.inner.TryNext(ctx)
	if r.Finished || r.Err != nil {
		return convertDoneOrErrResult[T, Indexed[T]](r)
	}
	res := Emit(Indexed[T]{
		Index: wi.index,
		Item:  r.Item,
	})
	wi.index += 1
	return res
}

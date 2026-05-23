// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package iter

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/pkg/util"
)

type chunkMappingCfg struct {
	chunkSize uint
	quota     *util.WorkerPool
}

type chunkMapping[T, R any] struct {
	chunkMappingCfg
	inner  TryNextor[T]
	mapper func(context.Context, T) (R, error)

	started     bool
	finished    bool
	cancel      context.CancelFunc
	outstanding chan struct{}
	results     chan IterResult[R]
}

func (m *chunkMapping[T, R]) TryNext(ctx context.Context) IterResult[R] {
	if !m.started {
		m.start(ctx)
	}
	for {
		if m.finished {
			return Done[R]()
		}

		select {
		case <-ctx.Done():
			m.cancel()
			m.finished = true
			return Throw[R](ctx.Err())
		case r, ok := <-m.results:
			if !ok {
				m.finished = true
				return Done[R]()
			}
			<-m.outstanding
			if r.Err != nil {
				m.cancel()
				m.finished = true
			}
			return r
		}
	}
}

func (m *chunkMapping[T, R]) start(ctx context.Context) {
	m.started = true
	m.outstanding = make(chan struct{}, m.chunkSize)
	m.results = make(chan IterResult[R], m.chunkSize)
	ctx, m.cancel = context.WithCancel(ctx)

	go func() {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(m.results)
		}()

		for {
			select {
			case m.outstanding <- struct{}{}:
			case <-ctx.Done():
				return
			}

			r := m.inner.TryNext(ctx)
			if r.FinishedOrError() {
				if r.Err != nil {
					m.results <- DoneBy[R](r)
					m.cancel()
				} else {
					<-m.outstanding
				}
				return
			}

			item := r.Item
			wg.Add(1)
			m.quota.Apply(func() {
				defer wg.Done()

				result, err := m.mapper(ctx, item)
				r := Emit(result)
				if err != nil {
					r = Throw[R](err)
				}
				select {
				case m.results <- r:
				case <-ctx.Done():
				}
				if err != nil {
					m.cancel()
				}
			})
		}
	}()
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

type filterMap[T, R any] struct {
	inner TryNextor[T]

	mapper func(T) (R, bool)
}

func (f filterMap[T, R]) TryNext(ctx context.Context) IterResult[R] {
	for {
		r := f.inner.TryNext(ctx)

		if r.FinishedOrError() {
			return DoneBy[R](r)
		}

		res, skip := f.mapper(r.Item)
		if !skip {
			return Emit(res)
		}
	}
}

type tryMap[T, R any] struct {
	inner TryNextor[T]

	mapper func(T) (R, error)
}

func (t tryMap[T, R]) TryNext(ctx context.Context) IterResult[R] {
	r := t.inner.TryNext(ctx)

	if r.FinishedOrError() {
		return DoneBy[R](r)
	}

	res, err := t.mapper(r.Item)
	if err != nil {
		return Throw[R](err)
	}
	return Emit(res)
}

type join[T any] struct {
	inner TryNextor[TryNextor[T]]

	current TryNextor[T]
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

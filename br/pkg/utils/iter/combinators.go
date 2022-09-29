package iter

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type chunkMapping[T, R any] struct {
	inner     TryNextor[T]
	mapper    func(context.Context, T) (R, error)
	chunkSize uint
	quota     *utils.WorkerPool

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

type TransformConfig[T, R any] func(*chunkMapping[T, R])

func WithConcurrency[T, R any](n uint) TransformConfig[T, R] {
	return func(c *chunkMapping[T, R]) {
		c.quota = utils.NewWorkerPool(n, "transforming")
	}
}

func WithChunkSize[T, R any](n uint) TransformConfig[T, R] {
	return func(c *chunkMapping[T, R]) {
		c.chunkSize = n
	}
}

// Transform returns an iterator that performs an impure procedure for each element,
// then emitting the result of that procedure.
// The execution of that procedure can be paralleled with the config `WithConcurrency`.
// You may also need to config the `WithChunkSize`, because the concurrent execution is only available intra-batch.
func Transform[T, R any](it TryNextor[T], with func(context.Context, T) (R, error), cs ...TransformConfig[T, R]) TryNextor[R] {
	r := &chunkMapping[T, R]{
		inner:     it,
		mapper:    with,
		chunkSize: 1,
	}
	for _, c := range cs {
		c(r)
	}
	if r.quota == nil {
		r.quota = utils.NewWorkerPool(r.chunkSize, "max-concurrency")
	}
	if r.quota.Limit() > int(r.chunkSize) {
		r.chunkSize = uint(r.quota.Limit())
	}
	return r
}

type filter[T any] struct {
	inner       TryNextor[T]
	filterOutIf func(T) bool
}

func (f filter[T]) TryNext(ctx context.Context) IterResult[T] {
	r := f.inner.TryNext(ctx)
	if r.Err != nil || r.Finished {
		return r
	}

	if f.filterOutIf(r.Item) {
		return f.TryNext(ctx)
	}

	return r
}

func FilterOut[T any](it TryNextor[T], f func(T) bool) TryNextor[T] {
	return filter[T]{
		inner:       it,
		filterOutIf: f,
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

func TakeFirst[T any](inner TryNextor[T], n uint) TryNextor[T] {
	return &take[T]{
		n:     n,
		inner: inner,
	}
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

func FlatMap[T, R any](it TryNextor[T], mapper func(T) TryNextor[R]) TryNextor[R] {
	return &join[R]{
		inner: pureMap[T, TryNextor[R]]{
			inner:  it,
			mapper: mapper,
		},
		current: empty[R]{},
	}
}

func Map[T, R any](it TryNextor[T], mapper func(T) R) TryNextor[R] {
	return pureMap[T, R]{
		inner:  it,
		mapper: mapper,
	}
}

func ConcatAll[T any](items ...TryNextor[T]) TryNextor[T] {
	return &join[T]{
		inner:   FromSlice(items),
		current: empty[T]{},
	}
}

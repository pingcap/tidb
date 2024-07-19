// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package iter

import (
	"context"

	"github.com/pingcap/tidb/pkg/util"
)

// TransformConfig is the config for the combinator "transform".
type TransformConfig func(*chunkMappingCfg)

func WithConcurrency(n uint) TransformConfig {
	return func(c *chunkMappingCfg) {
		c.quota = util.NewWorkerPool(n, "transforming")
	}
}

func WithChunkSize(n uint) TransformConfig {
	return func(c *chunkMappingCfg) {
		c.chunkSize = n
	}
}

// Transform returns an iterator that performs an impure procedure for each element,
// then emitting the result of that procedure.
// The execution of that procedure can be paralleled with the config `WithConcurrency`.
// You may also need to config the `WithChunkSize`, because the concurrent execution is only available intra-batch.
func Transform[T, R any](it TryNextor[T], with func(context.Context, T) (R, error), cs ...TransformConfig) TryNextor[R] {
	r := &chunkMapping[T, R]{
		inner:  it,
		mapper: with,
		chunkMappingCfg: chunkMappingCfg{
			chunkSize: 1,
		},
	}
	for _, c := range cs {
		c(&r.chunkMappingCfg)
	}
	if r.quota == nil {
		r.quota = util.NewWorkerPool(r.chunkSize, "max-concurrency")
	}
	if r.quota.Limit() > int(r.chunkSize) {
		r.chunkSize = uint(r.quota.Limit())
	}
	return r
}

// FilterOut returns an iterator that yields all elements the original iterator
// generated and DOESN'T satisfies the predicate.
func FilterOut[T any](it TryNextor[T], f func(T) bool) TryNextor[T] {
	return filter[T]{
		inner:       it,
		filterOutIf: f,
	}
}

// TakeFirst takes the first n elements of the iterator.
func TakeFirst[T any](inner TryNextor[T], n uint) TryNextor[T] {
	return &take[T]{
		n:     n,
		inner: inner,
	}
}

// FlapMap applies the mapper over every elements the origin iterator generates,
// then flatten them.
func FlatMap[T, R any](it TryNextor[T], mapper func(T) TryNextor[R]) TryNextor[R] {
	return &join[R]{
		inner: pureMap[T, TryNextor[R]]{
			inner:  it,
			mapper: mapper,
		},
		current: empty[R]{},
	}
}

// Map applies the mapper over every elements the origin iterator yields.
func Map[T, R any](it TryNextor[T], mapper func(T) R) TryNextor[R] {
	return pureMap[T, R]{
		inner:  it,
		mapper: mapper,
	}
}

// ConcatAll concatenates all elements yields by the iterators.
// In another word, it 'chains' all the input iterators.
func ConcatAll[T any](items ...TryNextor[T]) TryNextor[T] {
	return &join[T]{
		inner:   FromSlice(items),
		current: empty[T]{},
	}
}

func Enumerate[T any](it TryNextor[T]) TryNextor[Indexed[T]] {
	return &withIndex[T]{inner: it, index: 0}
}

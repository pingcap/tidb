// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// WorkerPool contains a pool of workers.
type WorkerPool struct {
	limit   uint
	workers chan *Worker
	name    string
}

// Worker identified by ID.
type Worker struct {
	ID uint64
}

type taskFunc func()

type identifiedTaskFunc func(uint64)

// NewWorkerPool returns a WorkPool.
func NewWorkerPool(limit uint, name string) *WorkerPool {
	workers := make(chan *Worker, limit)
	for i := uint(0); i < limit; i++ {
		workers <- &Worker{ID: uint64(i + 1)}
	}
	return &WorkerPool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

// IdleCount counts how many idle workers in the pool.
func (pool *WorkerPool) IdleCount() int {
	return len(pool.workers)
}

// Limit is the limit of the pool
func (pool *WorkerPool) Limit() int {
	return int(pool.limit)
}

// Apply executes a task.
func (pool *WorkerPool) Apply(fn taskFunc) {
	worker, _ := pool.ApplyWorker(context.TODO())
	go func() {
		defer pool.RecycleWorker(worker)
		fn()
	}()
}

// ApplyWithID execute a task and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithID(fn identifiedTaskFunc) {
	worker, _ := pool.ApplyWorker(context.TODO())
	go func() {
		defer pool.RecycleWorker(worker)
		fn(worker.ID)
	}()
}

// ApplyOnErrorGroup executes a task in an errorgroup.
func (pool *WorkerPool) ApplyOnErrorGroup(ctx context.Context, eg *errgroup.Group, fn func() error) error {
	worker, err := pool.ApplyWorker(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	eg.Go(func() error {
		defer pool.RecycleWorker(worker)
		return fn()
	})
	return nil
}

// ApplyWithIDInErrorGroup executes a task in an errorgroup and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithIDInErrorGroup(ctx context.Context, eg *errgroup.Group, fn func(id uint64) error) error {
	worker, err := pool.ApplyWorker(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	eg.Go(func() error {
		defer pool.RecycleWorker(worker)
		return fn(worker.ID)
	})
	return nil
}

// ApplyWorker apply a worker.
func (pool *WorkerPool) ApplyWorker(ctx context.Context) (*Worker, error) {
	var worker *Worker
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case worker = <-pool.workers:
	default:
		log.Debug("wait for workers", zap.String("pool", pool.name))
		worker = <-pool.workers
	}
	return worker, nil
}

// RecycleWorker recycle a worker.
func (pool *WorkerPool) RecycleWorker(worker *Worker) {
	if worker == nil {
		panic("invalid restore worker")
	}
	pool.workers <- worker
}

// HasWorker checks if the pool has unallocated workers.
func (pool *WorkerPool) HasWorker() bool {
	return pool.IdleCount() > 0
}

// Wait waits for all the goroutine to complete and catches the context cancel error.
// workerpool.ApplyWithIDInErrorGroup now can handle the context done, so it can quickly stop the later task.
// If the context is canceled by workerpool itself, it's OK to catch the real error from eg.err.
// However, if the context is canceled by its parent context, the workerpool cannot ensure whether the function
// canceling the parent context can handle the related error (mainly some bugs introduced). By this, the error
// might be leak, and BR finishes the task successfully (it should be failed actually).
func (pool *WorkerPool) Wait(eg *errgroup.Group, ectx context.Context) error {
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(context.Cause(ectx))
}

// PanicToErr recovers when the execution get panicked, and set the error provided by the arg.
// generally, this would be used with named return value and `defer`, like:
//
//	func foo() (err error) {
//	  defer utils.PanicToErr(&err)
//	  return maybePanic()
//	}
//
// Before using this, there are some hints for reducing resource leakage or bugs:
//   - If any of clean work (by `defer`) relies on the error (say, when error happens, rollback some operations.), please
//     place `defer this` AFTER that.
//   - All resources allocated should be freed by the `defer` syntax, or when panicking, they may not be recycled.
func PanicToErr(err *error) {
	item := recover()
	if item != nil {
		*err = errors.Annotatef(berrors.ErrUnknown, "panicked when executing, message: %v", item)
		log.Warn("PanicToErr: panicked, recovering and returning error", zap.StackSkip("stack", 1), logutil.ShortError(*err))
	}
}

// CatchAndLogPanic recovers when the execution get panicked, and log the panic.
// generally, this would be used with `defer`, like:
//
//	func foo() {
//	  defer utils.CatchAndLogPanic()
//	  maybePanic()
//	}
func CatchAndLogPanic() {
	item := recover()
	if item != nil {
		log.Warn("CatchAndLogPanic: panicked, but ignored.", zap.StackSkip("stack", 1), zap.Any("panic", item))
	}
}

type Result[T any] struct {
	Err  error
	Item T
}

func AsyncStreamBy[T any](generator func() (T, error)) <-chan Result[T] {
	out := make(chan Result[T])
	go func() {
		defer close(out)
		for {
			item, err := generator()
			if err != nil {
				out <- Result[T]{Err: err}
				return
			}
			out <- Result[T]{Item: item}
		}
	}()
	return out
}

func BuildWorkerTokenChannel(size uint) chan struct{} {
	ch := make(chan struct{}, size)
	for i := 0; i < int(size); i += 1 {
		ch <- struct{}{}
	}
	return ch
}

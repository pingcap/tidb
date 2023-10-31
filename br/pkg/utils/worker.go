// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metrics"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type RestoreSendType int

const (
	SendSignal RestoreSendType = iota
	SendTable
)

// PipelineChannel is used to record the stats of a channel during restore.
type PipelineChannel[T any] struct {
	name  string
	size  int
	ch    chan T
	errCh chan error
}

func NewPipelineChannel[T any](name string, size int) *PipelineChannel[T] {
	return &PipelineChannel[T]{
		name:  name,
		size:  size,
		ch:    make(chan T, size),
		errCh: make(chan error, 32),
	}
}

func (p *PipelineChannel[T]) Send(item T) {
	metrics.RestoreInFlightCounters.WithLabelValues(p.name).Inc()
	p.ch <- item
}

// we can always send error to next pipeline channel
func (p *PipelineChannel[T]) SendError(e error) {
	p.errCh <- e
}

func (p *PipelineChannel[T]) Recv(ctx context.Context) (item T, ok bool) {
	metrics.RestoreInFlightCounters.WithLabelValues(p.name).Desc()
	select {
	case <-ctx.Done():
		p.errCh <- ctx.Err()
	case item, ok = <-p.ch:
	}
	return item, ok
}

func (p *PipelineChannel[T]) WaitFinish(ctx context.Context) (err error) {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-p.errCh:
			return err
		case _, ok := <-p.ch:
			if !ok {
				// finished
				log.Info("pipeline channel has consume all pending tasks", zap.String("name", p.name))
				return nil
			}
		}
	}
}

func (p *PipelineChannel[T]) Close() {
	close(p.ch)
	log.Info("channel closed", zap.String("name", p.name))
}

func (p *PipelineChannel[T]) Len() int {
	return len(p.ch)
}

// golang does not support additional type parameter in method.
// so we cannot transform T to Other types.
// https://github.com/golang/go/issues/49085
func (p *PipelineChannel[T]) Map(f func(T) T) *PipelineChannel[T] {
	name := fmt.Sprintf("%s_mapped", p.name)
	outCh := NewPipelineChannel[T](name, p.size)
	go func() {
		defer outCh.Close()
		for item := range p.ch {
			outCh.Send(f(item))
		}
	}()
	return outCh
}

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
	worker := pool.ApplyWorker()
	go func() {
		defer pool.RecycleWorker(worker)
		fn()
	}()
}

// ApplyWithID execute a task and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithID(fn identifiedTaskFunc) {
	worker := pool.ApplyWorker()
	go func() {
		defer pool.RecycleWorker(worker)
		fn(worker.ID)
	}()
}

// ApplyOnErrorGroup executes a task in an errorgroup.
func (pool *WorkerPool) ApplyOnErrorGroup(eg *errgroup.Group, fn func() error) {
	worker := pool.ApplyWorker()
	eg.Go(func() error {
		defer pool.RecycleWorker(worker)
		return fn()
	})
}

// ApplyWithIDInErrorGroup executes a task in an errorgroup and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithIDInErrorGroup(eg *errgroup.Group, fn func(id uint64) error) {
	worker := pool.ApplyWorker()
	eg.Go(func() error {
		defer pool.RecycleWorker(worker)
		return fn(worker.ID)
	})
}

// ApplyWorker apply a worker.
func (pool *WorkerPool) ApplyWorker() *Worker {
	var worker *Worker
	select {
	case worker = <-pool.workers:
	default:
		log.Debug("wait for workers", zap.String("pool", pool.name))
		worker = <-pool.workers
	}
	return worker
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

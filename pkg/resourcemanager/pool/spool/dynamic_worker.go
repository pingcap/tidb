// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spool

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/resourcemanager/pool"
	"github.com/pingcap/tidb/pkg/resourcemanager/poolmanager"
)

// RunWithDynamicalConcurrency runs a function in the pool with dynamically concurrency.
func RunWithDynamicalConcurrency[T any, W Worker[T]](ctx context.Context, p *Pool, create func() W, least, concurrency int32) (*DynamicConcurrencyHandle[T, W], error) {
	// Don't need to check the pool capacity by `Pool.checkAndAddRunning` because the pool is not shared now. This avoids mutex lock.
	if p.isStop.Load() {
		return nil, pool.ErrPoolClosed
	}
	if least > concurrency {
		least = concurrency
	}
	taskID := p.GenTaskID()
	exitCh := make(chan struct{})
	// hack: use a closed channel to avoid blocking runTask when tuning.
	close(exitCh)
	meta := poolmanager.NewMeta(taskID, exitCh, nil, least)
	p.taskManager.RegisterTask(meta)
	ctx, cancel := context.WithCancel(ctx)
	handle := &DynamicConcurrencyHandle[T, W]{
		ctx:      ctx,
		cancel:   cancel,
		capacity: concurrency,
		taskID:   taskID,
		p:        p,
		taskCh:   make(chan T, concurrency),
		create:   create,
	}
	for n := int32(0); n < least; n++ {
		p.run(handle.run)
	}
	return handle, nil
}

// Worker interface.
type Worker[task any] interface {
	HandleTask(context.Context, task)
	Close()
}

// DynamicConcurrencyHandle is the handle of dynamic concurrency worker.
type DynamicConcurrencyHandle[T any, W Worker[T]] struct {
	ctx      context.Context
	cancel   context.CancelFunc
	running  atomic.Int32
	capacity int32
	taskID   uint64
	p        *Pool
	taskCh   chan T
	create   func() W
}

// Send a task to the pool, spawn new worker if required.
func (d *DynamicConcurrencyHandle[T, W]) Send(task T) {
	poolSize := d.p.Running()
	spawn := poolSize < d.capacity && len(d.taskCh) > 0 || d.running.Load() == poolSize
	d.taskCh <- task
	if spawn {
		// spawn new runner.
		d.p.run(d.run)
	}
}

// Close stop the tasks, if cancel is true, the queued tasks will be dropped.
func (d *DynamicConcurrencyHandle[T, W]) Close(cancel bool) {
	close(d.taskCh)
	if cancel {
		d.cancel()
	}
}

func (d *DynamicConcurrencyHandle[T, W]) run() {
	d.p.running.Add(1)
	worker := d.create()
	defer worker.Close()
	doneCh := d.ctx.Done()
	for {
		var (
			task T
			ok   bool
		)
		select {
		case task, ok = <-d.taskCh:
		case <-doneCh:
			return
		}
		if !ok {
			return
		}
		d.running.Add(1)
		worker.HandleTask(d.ctx, task)
		d.running.Add(-1)
	}
}

// Volume returns the capacity of the handler.
func (d *DynamicConcurrencyHandle[T, W]) Volume() int {
	if d == nil {
		return 0
	}
	return int(d.p.Running())
}

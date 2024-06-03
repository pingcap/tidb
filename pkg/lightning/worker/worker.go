// Copyright 2019 PingCAP, Inc.
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

package worker

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/metric"
)

// Pool is the worker pool.
type Pool struct {
	limit   int
	workers chan *Worker
	name    string
	metrics *metric.Metrics
}

// Worker is the worker struct.
type Worker struct {
	ID int64
}

// NewPool creates a new worker pool.
func NewPool(ctx context.Context, limit int, name string) *Pool {
	workers := make(chan *Worker, limit)
	for i := 0; i < limit; i++ {
		workers <- &Worker{ID: int64(i + 1)}
	}

	metrics, ok := metric.FromContext(ctx)
	if ok {
		metrics.IdleWorkersGauge.WithLabelValues(name).Set(float64(limit))
	}
	return &Pool{
		limit:   limit,
		workers: workers,
		name:    name,
		metrics: metrics,
	}
}

// Apply gets a worker from the pool.
func (pool *Pool) Apply() *Worker {
	start := time.Now()
	worker := <-pool.workers
	if pool.metrics != nil {
		pool.metrics.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
		pool.metrics.ApplyWorkerSecondsHistogram.WithLabelValues(pool.name).Observe(time.Since(start).Seconds())
	}
	return worker
}

// Recycle puts a worker back to the pool.
func (pool *Pool) Recycle(worker *Worker) {
	if worker == nil {
		panic("invalid restore worker")
	}
	pool.workers <- worker
	if pool.metrics != nil {
		pool.metrics.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
	}
}

// HasWorker returns whether the pool has worker.
func (pool *Pool) HasWorker() bool {
	return len(pool.workers) > 0
}

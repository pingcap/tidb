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
// See the License for the specific language governing permissions and
// limitations under the License.

package worker

import (
	"context"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/metric"
)

type Pool struct {
	limit   int
	workers chan *Worker
	name    string
}

type Worker struct {
	ID int64
}

func NewPool(ctx context.Context, limit int, name string) *Pool {
	workers := make(chan *Worker, limit)
	for i := 0; i < limit; i++ {
		workers <- &Worker{ID: int64(i + 1)}
	}

	metric.IdleWorkersGauge.WithLabelValues(name).Set(float64(limit))
	return &Pool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

func (pool *Pool) Apply() *Worker {
	start := time.Now()
	worker := <-pool.workers
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
	metric.ApplyWorkerSecondsHistogram.WithLabelValues(pool.name).Observe(time.Since(start).Seconds())
	return worker
}

func (pool *Pool) Recycle(worker *Worker) {
	if worker == nil {
		panic("invalid restore worker")
	}
	pool.workers <- worker
	metric.IdleWorkersGauge.WithLabelValues(pool.name).Set(float64(len(pool.workers)))
}

func (pool *Pool) HasWorker() bool {
	return len(pool.workers) > 0
}

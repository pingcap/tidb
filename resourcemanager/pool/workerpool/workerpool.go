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

package workerpool

import (
	"time"

	"github.com/pingcap/tidb/resourcemanager"
	"github.com/pingcap/tidb/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/syncutil"
	atomicutil "go.uber.org/atomic"
)

// Worker is worker interface.
type Worker interface {
	HandleTask(task any)
	Close()
}

// WorkerPool is a pool of workers.
type WorkerPool struct {
	name           string
	numWorkers     int32
	runningWorkers atomicutil.Int32
	taskChan       chan any
	quitChan       chan struct{}
	wg             tidbutil.WaitGroupWrapper
	createWorker   func() Worker
	lastTuneTs     atomicutil.Time
	mu             syncutil.RWMutex
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(name string, component util.Component, numWorkers int, createWorker func() Worker) (*WorkerPool, error) {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	p := &WorkerPool{
		name:         name,
		numWorkers:   int32(numWorkers),
		taskChan:     make(chan any),
		quitChan:     make(chan struct{}),
		createWorker: createWorker,
	}

	err := resourcemanager.InstanceResourceManager.Register(p, name, component)
	if err != nil {
		return nil, err
	}

	// Start default count of workers.
	for i := 0; i < int(p.numWorkers); i++ {
		p.runAWorker()
	}

	return p, nil
}

func (p *WorkerPool) runAWorker() {
	p.wg.Run(func() {
		w := p.createWorker()
		for {
			select {
			case task := <-p.taskChan:
				p.runningWorkers.Add(1)
				w.HandleTask(task)
				p.runningWorkers.Add(-1)
			case <-p.quitChan:
				w.Close()
				return
			}
		}
	})
}

// AddTask adds a task to the pool.
func (p *WorkerPool) AddTask(task any) {
	p.taskChan <- task
}

// Tune tunes the pool to the specified number of workers.
func (p *WorkerPool) Tune(numWorkers int32) {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	p.lastTuneTs.Store(time.Now())
	p.mu.Lock()
	defer p.mu.Unlock()
	diff := numWorkers - p.numWorkers
	if diff > 0 {
		// Add workers
		for i := 0; i < int(diff); i++ {
			p.runAWorker()
		}
	} else if diff < 0 {
		// Remove workers
		for i := 0; i < int(-diff); i++ {
			p.quitChan <- struct{}{}
		}
	}
	p.numWorkers = numWorkers
}

func (p *WorkerPool) LastTunerTs() time.Time {
	return p.lastTuneTs.Load()
}

func (p *WorkerPool) Cap() int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numWorkers
}

func (p *WorkerPool) Running() int32 {
	return p.runningWorkers.Load()
}

func (p *WorkerPool) Name() string {
	return p.name
}

func (p *WorkerPool) ReleaseAndWait() {
	close(p.quitChan)
	p.wg.Wait()
	resourcemanager.InstanceResourceManager.Unregister(p.Name())
}

// Copyright 2015 PingCAP, Inc.
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

package unionexec

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"go.uber.org/zap"
)

var (
	_ exec.Executor = &UnionExec{}
)

// UnionExec pulls all it's children's result and returns to its parent directly.
// A "resultPuller" is started for every child to pull result from that child and push it to the "resultPool", the used
// "Chunk" is obtained from the corresponding "resourcePool". All resultPullers are running concurrently.
//
//	                          +----------------+
//	+---> resourcePool 1 ---> | resultPuller 1 |-----+
//	|                         +----------------+     |
//	|                                                |
//	|                         +----------------+     v
//	+---> resourcePool 2 ---> | resultPuller 2 |-----> resultPool ---+
//	|                         +----------------+     ^               |
//	|                               ......           |               |
//	|                         +----------------+     |               |
//	+---> resourcePool n ---> | resultPuller n |-----+               |
//	|                         +----------------+                     |
//	|                                                                |
//	|                          +-------------+                       |
//	|--------------------------| main thread | <---------------------+
//	                           +-------------+
type UnionExec struct {
	exec.BaseExecutor
	Concurrency int
	childIDChan chan int

	stopFetchData atomic.Value

	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionWorkerResult

	results     []*chunk.Chunk
	wg          sync.WaitGroup
	initialized bool
	mu          struct {
		*syncutil.Mutex
		maxOpenedChildID int
	}

	childInFlightForTest int32
}

// unionWorkerResult stores the result for a union worker.
// A "resultPuller" is started for every child to pull result from that child, unionWorkerResult is used to store that pulled result.
// "src" is used for Chunk reuse: after pulling result from "resultPool", main-thread must push a valid unused Chunk to "src" to
// enable the corresponding "resultPuller" continue to work.
type unionWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

func (e *UnionExec) waitAllFinished() {
	e.wg.Wait()
	close(e.resultPool)
}

// Open implements the Executor Open interface.
func (e *UnionExec) Open(context.Context) error {
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	e.mu.Mutex = &syncutil.Mutex{}
	e.mu.maxOpenedChildID = -1
	return nil
}

func (e *UnionExec) initialize(ctx context.Context) {
	if e.Concurrency > e.ChildrenLen() {
		e.Concurrency = e.ChildrenLen()
	}
	for i := 0; i < e.Concurrency; i++ {
		e.results = append(e.results, exec.NewFirstChunk(e.Children(0)))
	}
	e.resultPool = make(chan *unionWorkerResult, e.Concurrency)
	e.resourcePools = make([]chan *chunk.Chunk, e.Concurrency)
	e.childIDChan = make(chan int, e.ChildrenLen())
	for i := 0; i < e.Concurrency; i++ {
		e.resourcePools[i] = make(chan *chunk.Chunk, 1)
		e.resourcePools[i] <- e.results[i]
		e.wg.Add(1)
		go e.resultPuller(ctx, i)
	}
	for i := 0; i < e.ChildrenLen(); i++ {
		e.childIDChan <- i
	}
	close(e.childIDChan)
	go e.waitAllFinished()
}

func (e *UnionExec) resultPuller(ctx context.Context, workerID int) {
	result := &unionWorkerResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[workerID],
	}
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("resultPuller panicked", zap.Any("recover", r), zap.Stack("stack"))
			result.err = util.GetRecoverError(r)
			e.resultPool <- result
			e.stopFetchData.Store(true)
		}
		e.wg.Done()
	}()
	for childID := range e.childIDChan {
		e.mu.Lock()
		if childID > e.mu.maxOpenedChildID {
			e.mu.maxOpenedChildID = childID
		}
		e.mu.Unlock()
		if err := exec.Open(ctx, e.Children(childID)); err != nil {
			result.err = err
			e.stopFetchData.Store(true)
			e.resultPool <- result
		}
		failpoint.Inject("issue21441", func() {
			atomic.AddInt32(&e.childInFlightForTest, 1)
		})
		for {
			if e.stopFetchData.Load().(bool) {
				return
			}
			select {
			case <-e.finished:
				return
			case result.chk = <-e.resourcePools[workerID]:
			}
			result.err = exec.Next(ctx, e.Children(childID), result.chk)
			if result.err == nil && result.chk.NumRows() == 0 {
				e.resourcePools[workerID] <- result.chk
				break
			}
			failpoint.Inject("issue21441", func() {
				if int(atomic.LoadInt32(&e.childInFlightForTest)) > e.Concurrency {
					panic("the count of child in flight is larger than e.concurrency unexpectedly")
				}
			})
			e.resultPool <- result
			if result.err != nil {
				e.stopFetchData.Store(true)
				return
			}
		}
		failpoint.Inject("issue21441", func() {
			atomic.AddInt32(&e.childInFlightForTest, -1)
		})
	}
}

// Next implements the Executor Next interface.
func (e *UnionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if !e.initialized {
		e.initialize(ctx)
		e.initialized = true
	}
	result, ok := <-e.resultPool
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}

	if result.chk.NumCols() != req.NumCols() {
		return errors.Errorf("Internal error: UnionExec chunk column count mismatch, req: %d, result: %d",
			req.NumCols(), result.chk.NumCols())
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the Executor Close interface.
func (e *UnionExec) Close() error {
	if e.finished != nil {
		close(e.finished)
	}
	e.results = nil
	if e.resultPool != nil {
		channel.Clear(e.resultPool)
	}
	e.resourcePools = nil
	if e.childIDChan != nil {
		channel.Clear(e.childIDChan)
	}
	// We do not need to acquire the e.mu.Lock since all the resultPuller can be
	// promised to exit when reaching here (e.childIDChan been closed).
	var firstErr error
	for i := 0; i <= e.mu.maxOpenedChildID; i++ {
		if err := exec.Close(e.Children(i)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

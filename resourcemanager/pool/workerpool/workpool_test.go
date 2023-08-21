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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var globalCnt atomic.Int64
var cntWg sync.WaitGroup

type MyWorker[T int64, R struct{}] struct {
	id int
}

func (w *MyWorker[T, R]) HandleTask(task int64) struct{} {
	globalCnt.Add(task)
	cntWg.Done()
	logutil.BgLogger().Info("Worker handling task")
	return struct{}{}
}

func (w *MyWorker[T, R]) Close() {
	logutil.BgLogger().Info("Close worker", zap.Any("id", w.id))
}

func createMyWorker() Worker[int64, struct{}] {
	return &MyWorker[int64, struct{}]{}
}

func TestWorkerPool(t *testing.T) {
	// Create a worker pool with 3 workers.
	pool, err := NewWorkerPool[int64]("test", util.UNKNOWN, 3, createMyWorker)
	require.NoError(t, err)
	pool.Start()
	globalCnt.Store(0)

	g := new(errgroup.Group)
	g.Go(func() error {
		// Consume the results.
		for range pool.GetResultChan() {
			// Do nothing.
		}
		return nil
	})
	defer g.Wait()

	// Add some tasks to the pool.
	cntWg.Add(10)
	for i := 0; i < 10; i++ {
		pool.AddTask(int64(i))
	}

	cntWg.Wait()
	require.Equal(t, int32(3), pool.Cap())
	require.Equal(t, int64(45), globalCnt.Load())

	// Enlarge the pool to 5 workers.
	pool.Tune(5)

	// Add some more tasks to the pool.
	cntWg.Add(10)
	for i := 0; i < 10; i++ {
		pool.AddTask(int64(i))
	}

	cntWg.Wait()
	require.Equal(t, int32(5), pool.Cap())
	require.Equal(t, int64(90), globalCnt.Load())

	// Decrease the pool to 2 workers.
	pool.Tune(2)

	// Add some more tasks to the pool.
	cntWg.Add(10)
	for i := 0; i < 10; i++ {
		pool.AddTask(int64(i))
	}

	cntWg.Wait()
	require.Equal(t, int32(2), pool.Cap())
	require.Equal(t, int64(135), globalCnt.Load())

	// Wait for the tasks to be completed.
	pool.ReleaseAndWait()
}

type dummyWorker[T, R any] struct {
}

func (d dummyWorker[T, R]) HandleTask(task T) R {
	var zero R
	return zero
}

func (d dummyWorker[T, R]) Close() {}

func TestWorkerPoolNoneResult(t *testing.T) {
	pool, err := NewWorkerPool[int64, None](
		"test", util.UNKNOWN, 3,
		func() Worker[int64, None] {
			return dummyWorker[int64, None]{}
		})
	require.NoError(t, err)
	pool.Start()
	ch := pool.GetResultChan()
	require.Nil(t, ch)
	pool.ReleaseAndWait()

	pool2, err := NewWorkerPool[int64, int64](
		"test", util.UNKNOWN, 3,
		func() Worker[int64, int64] {
			return dummyWorker[int64, int64]{}
		})
	require.NoError(t, err)
	pool2.Start()
	require.NotNil(t, pool2.GetResultChan())
	pool2.ReleaseAndWait()

	pool3, err := NewWorkerPool[int64, struct{}](
		"test", util.UNKNOWN, 3,
		func() Worker[int64, struct{}] {
			return dummyWorker[int64, struct{}]{}
		})
	require.NoError(t, err)
	pool3.Start()
	require.NotNil(t, pool3.GetResultChan())
	pool3.ReleaseAndWait()
}

func TestWorkerPoolCustomChan(t *testing.T) {
	pool, err := NewWorkerPool[int64, int64](
		"test", util.UNKNOWN, 3,
		func() Worker[int64, int64] {
			return dummyWorker[int64, int64]{}
		})
	require.NoError(t, err)

	taskCh := make(chan int64)
	pool.SetTaskReceiver(taskCh)
	resultCh := make(chan int64)
	pool.SetResultSender(resultCh)
	count := 0
	g := new(errgroup.Group)
	g.Go(func() error {
		for range resultCh {
			count++
		}
		return nil
	})

	pool.Start()
	for i := 0; i < 5; i++ {
		taskCh <- int64(i)
	}
	pool.ReleaseAndWait()
	require.NoError(t, g.Wait())
	require.Equal(t, 5, count)
}

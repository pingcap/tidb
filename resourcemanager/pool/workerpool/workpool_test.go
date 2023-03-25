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
)

var globalCnt atomic.Int64
var cntWg sync.WaitGroup

type MyWorker[T int64] struct {
	id int
}

func (w *MyWorker[T]) HandleTask(task int64) {
	globalCnt.Add(task)
	cntWg.Done()
	logutil.BgLogger().Info("Worker handling task")
}

func (w *MyWorker[T]) Close() {
	logutil.BgLogger().Info("Close worker", zap.Any("id", w.id))
}

func createMyWorker() Worker[int64] {
	return &MyWorker[int64]{}
}

func TestWorkerPool(t *testing.T) {
	// Create a worker pool with 3 workers.
	pool, err := NewWorkerPool[int64]("test", util.UNKNOWN, 3, createMyWorker)
	require.NoError(t, err)
	globalCnt.Store(0)

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

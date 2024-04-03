// Copyright 2024 PingCAP, Inc.
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

package initstats

import (
	"context"
	"runtime"
	"sync"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// Worker is used to load stats concurrently.
type Worker struct {
	taskFunc func(ctx context.Context, req *chunk.Chunk) error
	dealFunc func(is infoschema.InfoSchema, cache statstypes.StatsCache, iter *chunk.Iterator4Chunk)
	mu       sync.Mutex
	wg       util.WaitGroupWrapper
}

// NewWorker creates a new Worker.
func NewWorker(
	taskFunc func(ctx context.Context, req *chunk.Chunk) error,
	dealFunc func(is infoschema.InfoSchema, cache statstypes.StatsCache, iter *chunk.Iterator4Chunk)) *Worker {
	return &Worker{
		taskFunc: taskFunc,
		dealFunc: dealFunc,
	}
}

// LoadStats loads stats concurrently when to init stats
func (ls *Worker) LoadStats(is infoschema.InfoSchema, cache statstypes.StatsCache, rc sqlexec.RecordSet) {
	var concurrency int
	if config.GetGlobalConfig().Performance.ForceInitStats {
		concurrency = min(max(2, runtime.GOMAXPROCS(0)-2), 16)
	} else {
		concurrency = min(max(2, runtime.GOMAXPROCS(0)/2), 16)
	}
	for n := 0; n < concurrency; n++ {
		ls.wg.Run(func() {
			req := rc.NewChunk(nil)
			ls.loadStats(is, cache, req)
		})
	}
}

func (ls *Worker) loadStats(is infoschema.InfoSchema, cache statstypes.StatsCache, req *chunk.Chunk) {
	iter := chunk.NewIterator4Chunk(req)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	for {
		err := ls.getTask(ctx, req)
		if err != nil {
			logutil.StatsLogger().Error("load stats failed", zap.Error(err))
			return
		}
		if req.NumRows() == 0 {
			return
		}
		ls.dealFunc(is, cache, iter)
	}
}

func (ls *Worker) getTask(ctx context.Context, req *chunk.Chunk) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.taskFunc(ctx, req)
}

// Wait closes the load stats worker.
func (ls *Worker) Wait() {
	ls.wg.Wait()
}

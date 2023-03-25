// Copyright 2022 PingCAP, Inc.
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

package ddl

import (
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
)

// workerPool is used to new worker.
type workerPool struct {
	t       jobType
	exit    atomic.Bool
	resPool *pools.ResourcePool
}

func newDDLWorkerPool(resPool *pools.ResourcePool, tp jobType) *workerPool {
	return &workerPool{
		t:       tp,
		exit:    *atomic.NewBool(false),
		resPool: resPool,
	}
}

// get gets workerPool from context resource pool.
// Please remember to call put after you finished using workerPool.
func (wp *workerPool) get() (*worker, error) {
	if wp.resPool == nil {
		return nil, nil
	}

	if wp.exit.Load() {
		return nil, errors.Errorf("workerPool is closed")
	}

	// no need to protect wp.resPool
	resource, err := wp.resPool.TryGet()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resource == nil {
		return nil, nil
	}

	worker := resource.(*worker)
	return worker, nil
}

// put returns workerPool to context resource pool.
func (wp *workerPool) put(wk *worker) {
	if wp.resPool == nil || wp.exit.Load() {
		return
	}

	// no need to protect wp.resPool, even the wp.resPool is closed, the ctx still need to
	// put into resPool, because when resPool is closing, it will wait all the ctx returns, then resPool finish closing.
	wp.resPool.Put(wk)
}

// close clean up the workerPool.
func (wp *workerPool) close() {
	// prevent closing resPool twice.
	if wp.exit.Load() || wp.resPool == nil {
		return
	}
	wp.exit.Store(true)
	logutil.BgLogger().Info("[ddl] closing workerPool")
	wp.resPool.Close()
}

// tp return the type of backfill worker pool.
func (wp *workerPool) tp() jobType {
	return wp.t
}

// backfillCtxPool is used to new backfill context.
type backfillCtxPool struct {
	exit    atomic.Bool
	resPool *pools.ResourcePool
}

func newBackfillContextPool(resPool *pools.ResourcePool) *backfillCtxPool {
	return &backfillCtxPool{
		exit:    *atomic.NewBool(false),
		resPool: resPool,
	}
}

// setCapacity changes the capacity of the pool.
// A setCapacity of 0 is equivalent to closing the backfillCtxPool.
func (bcp *backfillCtxPool) setCapacity(capacity int) error {
	return bcp.resPool.SetCapacity(capacity)
}

// get gets backfillCtxPool from context resource pool.
// Please remember to call put after you finished using backfillCtxPool.
func (bcp *backfillCtxPool) get() (*backfillWorker, error) {
	if bcp.resPool == nil {
		return nil, nil
	}

	if bcp.exit.Load() {
		return nil, errors.Errorf("backfill worker pool is closed")
	}

	// no need to protect bcp.resPool
	resource, err := bcp.resPool.TryGet()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resource == nil {
		return nil, nil
	}

	worker := resource.(*backfillWorker)
	return worker, nil
}

// batchGet gets a batch backfillWorkers from context resource pool.
// Please remember to call batchPut after you finished using backfillWorkerPool.
func (bcp *backfillCtxPool) batchGet(cnt int) ([]*backfillWorker, error) {
	if bcp.resPool == nil {
		return nil, nil
	}

	if bcp.exit.Load() {
		return nil, errors.Errorf("backfill worker pool is closed")
	}

	workers := make([]*backfillWorker, 0, cnt)
	for i := 0; i < cnt; i++ {
		// no need to protect bcp.resPool
		res, err := bcp.resPool.TryGet()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if res == nil {
			if len(workers) == 0 {
				return nil, nil
			}
			return workers, nil
		}
		worker := res.(*backfillWorker)
		workers = append(workers, worker)
	}

	return workers, nil
}

// put returns workerPool to context resource pool.
func (bcp *backfillCtxPool) put(wk *backfillWorker) {
	if bcp.resPool == nil || bcp.exit.Load() {
		return
	}

	// No need to protect bcp.resPool, even the bcp.resPool is closed, the ctx still need to
	// put into resPool, because when resPool is closing, it will wait all the ctx returns, then resPool finish closing.
	bcp.resPool.Put(wk)
}

// close clean up the backfillCtxPool.
func (bcp *backfillCtxPool) close() {
	// Prevent closing resPool twice.
	if bcp.resPool == nil || bcp.exit.Load() {
		return
	}
	bcp.exit.Store(true)
	logutil.BgLogger().Info("[ddl] closing workerPool")
	bcp.resPool.Close()
}

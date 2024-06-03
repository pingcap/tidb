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
	"github.com/pingcap/tidb/pkg/ddl/logutil"
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
	logutil.DDLLogger().Info("closing workerPool")
	wp.resPool.Close()
}

// tp return the type of backfill worker pool.
func (wp *workerPool) tp() jobType {
	return wp.t
}

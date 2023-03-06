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
	"context"
	"testing"

	"github.com/ngaut/pools"
	"github.com/stretchr/testify/require"
)

func TestDDLWorkerPool(t *testing.T) {
	f := func() func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newWorker(nil, addIdxWorker, nil, nil, nil)
			return wk, nil
		}
	}
	pool := newDDLWorkerPool(pools.NewResourcePool(f(), 1, 2, 0), reorg)
	pool.close()
	pool.put(nil)
}

func TestBackfillWorkerPool(t *testing.T) {
	f := func() func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newBackfillWorker(context.Background(), nil)
			return wk, nil
		}
	}
	pool := newBackfillContextPool(pools.NewResourcePool(f(), 3, 4, 0))
	bc, err := pool.get()
	require.NoError(t, err)
	require.NotNil(t, bc)
	require.Nil(t, bc.backfiller)
	bcs, err := pool.batchGet(3)
	require.NoError(t, err)
	require.Len(t, bcs, 2)
	// test it to reach the capacity
	bc1, err := pool.get()
	require.NoError(t, err)
	require.Nil(t, bc1)
	bcs1, err := pool.batchGet(1)
	require.NoError(t, err)
	require.Nil(t, bcs1)

	// test setCapacity
	err = pool.setCapacity(5)
	require.Equal(t, "capacity 5 is out of range", err.Error())
	err = pool.setCapacity(4)
	require.NoError(t, err)
	bc1, err = pool.get()
	require.NoError(t, err)
	require.NotNil(t, bc)
	require.Nil(t, bc.backfiller)
	pool.put(bc)
	pool.put(bc1)
	for _, bc := range bcs {
		pool.put(bc)
	}

	// test close
	pool.close()
	pool.close()
	require.Equal(t, true, pool.exit.Load())
	pool.put(bc1)

	bc, err = pool.get()
	require.Error(t, err)
	require.Equal(t, "backfill worker pool is closed", err.Error())
	require.Nil(t, bc)

	bcs, err = pool.batchGet(1)
	require.Error(t, err)
	require.Equal(t, "backfill worker pool is closed", err.Error())
	require.Nil(t, bcs)
}

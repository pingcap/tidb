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
			wk := newBackfillWorker(context.Background(), 1, nil)
			return wk, nil
		}
	}
	pool := newBackfillWorkerPool(pools.NewResourcePool(f(), 1, 2, 0))
	bwp, err := pool.get()
	require.NoError(t, err)
	require.Equal(t, 1, bwp.id)
	// test it to reach the capacity
	bwp1, err := pool.get()
	require.NoError(t, err)
	require.Nil(t, bwp1)

	// test setCapacity
	err = pool.setCapacity(2)
	require.NoError(t, err)
	bwp1, err = pool.get()
	require.NoError(t, err)
	require.Equal(t, 1, bwp1.id)
	pool.put(bwp)
	pool.put(bwp1)

	// test close
	pool.close()
	pool.close()
	require.Equal(t, true, pool.exit.Load())
	pool.put(bwp1)

	bwp, err = pool.get()
	require.Error(t, err)
	require.Equal(t, "backfill worker pool is closed", err.Error())
	require.Nil(t, bwp)
}

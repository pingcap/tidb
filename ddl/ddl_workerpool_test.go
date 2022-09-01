package ddl

import (
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

func TestBackfillWorkerPool(t *testing.T) {
	reorgInfo := &reorgInfo{Job: &model.Job{ID: 1}}
	f := func() func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newBackfillWorker(nil, 1, nil, reorgInfo)
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

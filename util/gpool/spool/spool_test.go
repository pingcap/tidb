package spool

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/stretchr/testify/require"
)

func TestSPool(t *testing.T) {
	var wg sync.WaitGroup
	p, err := NewPool("TestAntsPoolWaitToGetWorker", PoolCap, util.UNKNOWN, WithExpiryDuration(DefaultExpiredTime))
	require.NoError(t, err)
	defer p.ReleaseAndWait()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		_ = p.Run(func() {
			demoPoolFunc(100)
			wg.Done()
		})
	}
	wg.Wait()
}

package spool

import (
	"runtime"
	"sync"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/resourcemanager/util"
	"go.uber.org/zap"
)

// TestAntsPoolWaitToGetWorker is used to test waiting to get worker.
func TestAntsPoolWaitToGetWorker(t *testing.T) {
	var wg sync.WaitGroup
	p, _ := NewPool("TestAntsPoolWaitToGetWorker", PoolCap, util.UNKNOWN, WithExpiryDuration(DefaultExpiredTime))
	defer p.ReleaseAndWait()

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		log.Info("pool, running workers number:", zap.Int("id", i))
		_ = p.Run(func() {
			demoPoolFunc(100)
			wg.Done()
		})
	}
	wg.Wait()
	t.Logf("pool, running workers number:%d", p.Running())
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
}
